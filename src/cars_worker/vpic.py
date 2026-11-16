"""NHTSA vPIC (Product Information Catalog and Vehicle Listing): makes, models and VIN decoding.

VINs are decoded with ``DecodeVINValuesBatch``, 50 VINs per POST, de-duplicated
within each Arrow batch and memoized per worker process.
"""

from __future__ import annotations

import json
import logging
import re
import threading
from collections import OrderedDict
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar, cast

import pyarrow as pa
import pyarrow.compute as pc
from vgi.arguments import Arg, Attach, Param, Returns, TableInput
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction
from vgi.table_function import (
    BindParams,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)
from vgi.table_in_out_function import OutputCollector as InOutCollector
from vgi.table_in_out_function import TableInOutGenerator

from .common import (
    Endpoints,
    UpstreamError,
    clean_str,
    endpoints_from_options,
    endpoints_of,
    get_json,
    normalize_make,
    path_segment,
    post_form_json,
    to_float,
    to_int,
)
from .meta import column, object_tags, result_columns

log = logging.getLogger(__name__)

VPIC_DOCS = "https://vpic.nhtsa.dot.gov/api/"
BATCH_SIZE = 50
# vPIC vehicle type names accepted by GetMakesForVehicleType / GetModelsForMakeYear (partial match).
VEHICLE_TYPES = ["car", "truck", "mpv", "motorcycle", "bus", "trailer"]
_VIN_OK = re.compile(r"^[A-Z0-9*]{1,17}$")
_memo: OrderedDict[tuple[str, str, int | None], dict[str, Any]] = OrderedDict()
_memo_lock = threading.Lock()
_MEMO_MAX = 20_000


# ---------------------------------------------------------------- VIN decoding


def _vin_fields() -> list[pa.Field[Any]]:
    c = column
    return [
        c("vin", pa.string(), "The VIN as decoded (trimmed, upper-cased)."),
        c("make", pa.string(), "Make, upper-cased join key ('HONDA')."),
        c("model", pa.string(), "Model name as vPIC reports it ('Accord')."),
        c("model_year", pa.int32(), "Model year decoded from the VIN (position 10, plus context)."),
        c("trim", pa.string(), "Trim level ('EX-V6')."),
        c("series", pa.string(), "Series designation."),
        c(
            "body_class",
            pa.string(),
            "Body class ('Sedan/Saloon', 'Pickup', 'Sport Utility Vehicle (SUV)/Multi-Purpose Vehicle (MPV)').",
        ),
        c(
            "vehicle_type",
            pa.string(),
            "NHTSA vehicle type ('PASSENGER CAR', 'TRUCK', 'MULTIPURPOSE PASSENGER VEHICLE (MPV)').",
        ),
        c("doors", pa.int32(), "Number of doors."),
        c("drive_type", pa.string(), "Drive type ('FWD/Front-Wheel Drive', '4WD/4-Wheel Drive/4x4')."),
        c("primary_fuel", pa.string(), "Primary fuel type ('Gasoline', 'Diesel', 'Electric')."),
        c("secondary_fuel", pa.string(), "Secondary fuel type for dual-fuel vehicles."),
        c(
            "electrification_level",
            pa.string(),
            "Electrification ('BEV (Battery Electric Vehicle)', 'HEV (Strong Hybrid)', 'PHEV ...').",
        ),
        c("engine_cylinders", pa.int32(), "Number of engine cylinders."),
        c("displacement_l", pa.float64(), "Engine displacement in litres."),
        c("engine_hp", pa.float64(), "Engine brake horsepower (low end of range when a range is given)."),
        c("turbo", pa.string(), "Turbo indicator as reported ('Yes', 'No'); NULL when unknown."),
        c(
            "transmission_style",
            pa.string(),
            "Transmission style ('Automatic', 'Manual/Standard', 'Continuously Variable Transmission (CVT)').",
        ),
        c("transmission_speeds", pa.int32(), "Number of transmission speeds."),
        c(
            "gvwr_class",
            pa.string(),
            "Gross vehicle weight rating class ('Class 1: 6,000 lb or less (2,722 kg or less)').",
        ),
        c("manufacturer", pa.string(), "Manufacturer of record ('AMERICAN HONDA MOTOR CO., INC.')."),
        c("plant_city", pa.string(), "Assembly plant city."),
        c("plant_state", pa.string(), "Assembly plant state or province."),
        c("plant_country", pa.string(), "Assembly plant country."),
        c("error_code", pa.string(), "vPIC error codes, comma-separated; '0' means decoded cleanly."),
        c("error_text", pa.string(), "vPIC explanation of the error codes."),
        c("suggested_vin", pa.string(), "vPIC's corrected VIN when positions are in error (errors marked '!')."),
    ]


VIN_FIELDS = _vin_fields()
VIN_STRUCT = pa.struct(VIN_FIELDS)

_VPIC_KEYS = {
    "make": "Make",
    "model": "Model",
    "model_year": "ModelYear",
    "trim": "Trim",
    "series": "Series",
    "body_class": "BodyClass",
    "vehicle_type": "VehicleType",
    "doors": "Doors",
    "drive_type": "DriveType",
    "primary_fuel": "FuelTypePrimary",
    "secondary_fuel": "FuelTypeSecondary",
    "electrification_level": "ElectrificationLevel",
    "engine_cylinders": "EngineCylinders",
    "displacement_l": "DisplacementL",
    "engine_hp": "EngineHP",
    "turbo": "Turbo",
    "transmission_style": "TransmissionStyle",
    "transmission_speeds": "TransmissionSpeeds",
    "gvwr_class": "GVWR",
    "manufacturer": "Manufacturer",
    "plant_city": "PlantCity",
    "plant_state": "PlantState",
    "plant_country": "PlantCountry",
    "error_code": "ErrorCode",
    "error_text": "ErrorText",
    "suggested_vin": "SuggestedVIN",
}


def vin_record(result: dict[str, Any], vin: str) -> dict[str, Any]:
    """Map one vPIC ``DecodeVINValues`` result onto the ``decode_vin`` struct fields."""
    row: dict[str, Any] = {"vin": vin}
    for field in VIN_FIELDS[1:]:
        raw = result.get(_VPIC_KEYS[field.name])
        if pa.types.is_integer(field.type):
            row[field.name] = to_int(raw)
        elif pa.types.is_floating(field.type):
            row[field.name] = to_float(raw)
        else:
            row[field.name] = clean_str(raw)
    row["make"] = normalize_make(row["make"])
    if row["displacement_l"] is not None:
        row["displacement_l"] = round(row["displacement_l"], 3)
    return row


def _rejected(vin: str) -> dict[str, Any]:
    row: dict[str, Any] = dict.fromkeys((f.name for f in VIN_FIELDS), None)
    row.update(vin=vin, error_code="400", error_text="Not sent to vPIC: a VIN is 1 to 17 letters, digits or '*'.")
    return row


def decode_vins(ep: Endpoints, items: Iterable[tuple[str, int | None]]) -> dict[tuple[str, int | None], dict[str, Any]]:
    """Decode unique ``(vin, model_year)`` pairs, 50 per vPIC request, using the process memo."""
    wanted = list(dict.fromkeys(items))
    found: dict[tuple[str, int | None], dict[str, Any]] = {}
    todo: list[tuple[str, int | None]] = []
    with _memo_lock:
        for key in wanted:
            hit = _memo.get((ep.vpic_base_url, *key))
            if hit is not None:
                found[key] = hit
            elif not _VIN_OK.match(key[0]):
                found[key] = _rejected(key[0])
            else:
                todo.append(key)
    url = f"{ep.vpic_base_url}/vehicles/DecodeVINValuesBatch/"
    for start in range(0, len(todo), BATCH_SIZE):
        chunk = todo[start : start + BATCH_SIZE]
        data = ";".join(vin if year is None else f"{vin},{year}" for vin, year in chunk)
        body = post_form_json(url, {"format": "json", "data": data})
        results = body.get("Results") or []
        if len(results) != len(chunk):
            raise UpstreamError(f"vPIC returned {len(results)} results for {len(chunk)} VINs")
        with _memo_lock:
            for key, result in zip(chunk, results, strict=True):
                record = vin_record(result, key[0])
                found[key] = record
                _memo[(ep.vpic_base_url, *key)] = record
                if len(_memo) > _MEMO_MAX:
                    _memo.popitem(last=False)
    return found


def _normalize_vins(vins: pa.Array[Any]) -> list[str | None]:
    upper = pc.utf8_upper(pc.utf8_trim_whitespace(pc.cast(vins, pa.string())))
    values: list[str | None] = upper.to_pylist()
    return [v or None for v in values]


def decode_column(ep: Endpoints, vins: pa.Array[Any], years: pa.Array[Any] | None) -> list[dict[str, Any] | None]:
    """Decode a column of VINs (and optional model years); NULL or blank VINs give NULL."""
    keys = _normalize_vins(vins)
    year_list: list[int | None] = (
        cast("list[int | None]", years.to_pylist()) if years is not None else [None] * len(keys)
    )
    pairs = [(k, y) for k, y in zip(keys, year_list, strict=True) if k is not None]
    found = decode_vins(ep, pairs)
    return [found[(k, y)] if k is not None else None for k, y in zip(keys, year_list, strict=True)]


_DECODE_DOC_LLM = (
    "Decode US Vehicle Identification Numbers with NHTSA vPIC and return a `STRUCT` of key fields: vin, "
    "make (upper-cased), model, model_year, trim, series, body_class, vehicle_type, doors, drive_type, "
    "primary_fuel, secondary_fuel, electrification_level, engine_cylinders, displacement_l, engine_hp, "
    "turbo, transmission_style, transmission_speeds, gvwr_class, manufacturer, plant_city, "
    "plant_state, plant_country, error_code ('0' = clean), error_text, suggested_vin. Access fields "
    "with dot notation: `cars.main.decode_vin(vin).make`. Works over a column: VINs are de-duplicated "
    "and sent 50 per request. NULL or blank VIN gives NULL; partial VINs (with '*') decode what they "
    "can; a malformed VIN returns a struct with error_code '400'. Missing values mean NHTSA has no "
    "data, not that a feature is absent. For many columns at once use `decode_vins((SELECT vin ...))`."
)


_VIN_DOC = "Vehicle Identification Number (17 characters; '*' wildcards allowed for partial VINs). NULL in, NULL out."


class DecodeVin(ScalarFunction):
    """``decode_vin(vin)``: decode VINs to a STRUCT of vehicle attributes."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "decode_vin"
        description = "Decode a VIN with NHTSA vPIC into a STRUCT of make, model, year, body, engine and plant"
        tags = {
            **object_tags(
                title="Decode VIN",
                category="vin",
                keywords="vin, vin decoder, vehicle identification number, nhtsa, vpic, make model year",
                doc_llm=_DECODE_DOC_LLM,
                doc_md=(
                    "## decode_vin(vin)\n\n"
                    "Decode VINs with [NHTSA vPIC](https://vpic.nhtsa.dot.gov/api/) into a `STRUCT` "
                    "(make, model, model_year, trim, body, engine, transmission, plant, error code). "
                    "Use `decode_vin(vin, model_year)` when the model year is known, which resolves the "
                    "30-year ambiguity of VIN position 10."
                ),
            ),
            "vgi.source_url": VPIC_DOCS,
            # Deterministic and offline: malformed or missing VINs are answered without calling vPIC.
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "name": "decode_vin_input_checks",
                        "description": "Malformed VINs are rejected locally with error_code 400; NULL in, NULL out",
                        "sql": [
                            {
                                "description": "A VIN with punctuation is not sent to vPIC",
                                "sql": "SELECT cars.main.decode_vin('NOT-A-VIN!').error_code AS error_code",
                                "expected_result": [{"error_code": "400"}],
                            },
                            {
                                "description": "A NULL VIN decodes to NULL",
                                "sql": "SELECT cars.main.decode_vin(NULL::VARCHAR) IS NULL AS is_null",
                                "expected_result": [{"is_null": True}],
                            },
                        ],
                    }
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT d.make, d.model, d.model_year, d.body_class, d.plant_country "
                    "FROM (SELECT cars.main.decode_vin('1HGCM82633A004352') AS d)"
                ),
                description="Decode one VIN and pick out make, model, year, body and plant country",
            ),
        ]

    @classmethod
    def compute(
        cls,
        vin: Annotated[pa.StringArray, Param(doc=_VIN_DOC)],
        options: Annotated[dict[str, Any], Attach()],
    ) -> Annotated[pa.StructArray, Returns(arrow_type=VIN_STRUCT)]:
        """Decode every VIN in the column."""
        ep = endpoints_from_options(options)
        return cast("pa.StructArray", pa.array(decode_column(ep, vin, None), type=VIN_STRUCT))


class DecodeVinWithYear(ScalarFunction):
    """``decode_vin(vin, model_year)``: decode VINs with a known model year."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "decode_vin"
        description = "Decode a VIN with NHTSA vPIC, using a known model year to resolve the year code"
        tags = {
            **object_tags(
                title="Decode VIN (with model year)",
                category="vin",
                keywords="vin, vin decoder, model year, vehicle identification number, nhtsa, vpic",
                doc_llm=_DECODE_DOC_LLM
                + " This overload also sends `model_year` (an integer, 1949-2100, constant or a column), which "
                "vPIC uses because VIN position 10 repeats every 30 years: "
                "`decode_vin(vin, model_year) FROM fleet` decodes each row with its own year.",
                doc_md=(
                    "## decode_vin(vin, model_year)\n\n"
                    "Same `STRUCT` as `decode_vin(vin)`, with the model year passed to vPIC."
                ),
            ),
            "vgi.source_url": VPIC_DOCS,
        }
        examples = [
            FunctionExample(
                sql="SELECT cars.main.decode_vin('1HGCM82633A004352', 2003).trim AS trim",
                description="Decode a 2003 Honda VIN and return its trim",
            ),
        ]

    @classmethod
    def compute(
        cls,
        vin: Annotated[pa.StringArray, Param(doc=_VIN_DOC)],
        model_year: Annotated[
            pa.Int64Array,
            Param(doc="Known model year for each row's VIN; NULL lets vPIC infer it from the VIN."),
        ],
        options: Annotated[dict[str, Any], Attach()],
    ) -> Annotated[pa.StructArray, Returns(arrow_type=VIN_STRUCT)]:
        """Decode every VIN in the column with its row's model year."""
        bad = sorted({y for y in model_year.to_pylist() if y is not None and not 1949 <= y <= 2100})
        if bad:
            raise ValueError(f"decode_vin: model_year must be between 1949 and 2100, got {bad[0]}")
        ep = endpoints_from_options(options)
        return cast("pa.StructArray", pa.array(decode_column(ep, vin, model_year), type=VIN_STRUCT))


# ---------------------------------------------------------------- decode_vins (table in-out)


@dataclass(slots=True, frozen=True, kw_only=True)
class DecodeVinsArgs:
    """``decode_vins((SELECT vin [, model_year] FROM ...))``."""

    data: Annotated[
        TableInput,
        Arg(0, doc="Rows with a text column named `vin` and optionally a whole-number column `model_year`."),
    ]


DECODE_VINS_SCHEMA = pa.schema(VIN_FIELDS)


class DecodeVins(TableInOutGenerator[DecodeVinsArgs]):
    """Decode a query's VINs into one row of vPIC attributes per input row."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "decode_vins"
        description = "Decode the vin column of a query with NHTSA vPIC, one output row per input row"
        tags = {
            **object_tags(
                title="Decode VINs (table)",
                category="vin",
                keywords="vin, bulk vin decode, batch decode, fleet, vehicle identification number, nhtsa, vpic",
                doc_llm=(
                    "Table in-out function: pass a subquery with a `VARCHAR` column `vin` (and optionally "
                    "an `INTEGER` `model_year`), e.g. `decode_vins((SELECT vin FROM fleet))`. Returns one "
                    "row per input row with the same columns as the `decode_vin` `STRUCT`, flattened: "
                    + ", ".join(f.name for f in VIN_FIELDS)
                    + ". Input order is preserved; NULL VINs give a row of NULLs. VINs are decoded 50 "
                    "per vPIC request as the input streams in. Join back to the input on `vin`."
                ),
                doc_md=(
                    "## decode_vins((SELECT vin [, model_year] ...))\n\n"
                    "Bulk VIN decoding with [NHTSA vPIC](https://vpic.nhtsa.dot.gov/api/): one row of "
                    "flattened attributes per input row, decoded 50 VINs per request."
                ),
            ),
            "vgi.result_columns_schema": result_columns(VIN_FIELDS),
            "vgi.source_url": VPIC_DOCS,
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT vin, make, model, model_year, electrification_level "
                    "FROM cars.main.decode_vins((SELECT * FROM (VALUES ('1HGCM82633A004352'), "
                    "('5YJ3E1EA7KF317000')) AS t(vin))) ORDER BY vin"
                ),
                description="Decode a small list of VINs into make, model, year and electrification",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[DecodeVinsArgs]) -> BindResponse:
        """Require a ``vin`` column; output is the flattened decode schema."""
        schema = params.bind_call.input_schema
        if schema is None or "vin" not in schema.names:
            raise ValueError("decode_vins needs an input column named 'vin'")
        return BindResponse(output_schema=DECODE_VINS_SCHEMA)

    @classmethod
    def process(
        cls, params: ProcessParams[DecodeVinsArgs], state: None, batch: pa.RecordBatch, out: InOutCollector
    ) -> None:
        """Decode this batch's VINs and emit one row per input row."""
        years = None
        if "model_year" in batch.schema.names:
            years = pc.cast(batch.column("model_year"), pa.int32())
        records = decode_column(endpoints_of(params), batch.column("vin"), years)
        empty = dict.fromkeys(DECODE_VINS_SCHEMA.names)
        out.emit(pa.RecordBatch.from_pylist([r or empty for r in records], schema=params.output_schema))


# ---------------------------------------------------------------- makes() / models()


def _makes_fields() -> list[pa.Field[Any]]:
    return [
        column("make_id", pa.int32(), "vPIC make id.", nullable=False),
        column("make", pa.string(), "Make name, upper-cased join key ('HONDA')."),
        column("vehicle_type", pa.string(), "vPIC vehicle type when filtered by one ('Passenger Car'); else NULL."),
    ]


MAKES_FIELDS = _makes_fields()


@dataclass(slots=True, frozen=True, kw_only=True)
class MakesArgs:
    """Arguments for ``makes(vehicle_type := NULL)``."""

    vehicle_type: Annotated[
        str | None,
        Arg(
            "vehicle_type",
            default=None,
            doc="Only makes registered for this vPIC vehicle type; NULL (default) lists all ~12,000 registered "
            "makes, most of them trailer and specialty builders.",
            choices=VEHICLE_TYPES,
        ),
    ]


@bind_fixed_schema
@init_single_worker
class Makes(TableFunctionGenerator[MakesArgs, None]):
    """Every make registered with NHTSA vPIC, optionally for one vehicle type."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(MAKES_FIELDS)

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "makes"
        description = "Vehicle makes registered with NHTSA vPIC, optionally for one vehicle type"
        tags = {
            **object_tags(
                title="Vehicle Makes (vPIC)",
                category="vin",
                keywords="makes, brands, manufacturers, nhtsa, vpic, car brands, vehicle type",
                doc_llm=(
                    "Vehicle makes registered with NHTSA vPIC, one row per make with columns make_id, "
                    "make and vehicle_type. With no argument every registered make is returned, about "
                    "12,000, most of them trailer and specialty builders. The named argument "
                    "`vehicle_type` narrows the list to one vehicle type ('car', 'truck', 'mpv', "
                    "'motorcycle', 'bus' or 'trailer') and fills the vehicle_type column. The make "
                    "column is upper-cased to match the other functions."
                ),
                doc_md=(
                    "## makes(vehicle_type := NULL)\n\n"
                    "Makes registered with [NHTSA vPIC](https://vpic.nhtsa.dot.gov/api/), optionally for "
                    "one vehicle type ('car', 'truck', 'mpv', 'motorcycle')."
                ),
            ),
            "vgi.result_columns_schema": result_columns(MAKES_FIELDS),
            "vgi.source_url": VPIC_DOCS,
        }
        examples = [
            FunctionExample(
                sql="SELECT make FROM cars.main.makes(vehicle_type := 'car') WHERE make LIKE 'A%' ORDER BY make",
                description="Passenger-car makes whose names start with A",
            ),
        ]

    @classmethod
    def process(cls, params: ProcessParams[MakesArgs], state: None, out: OutputCollector) -> None:
        """Fetch the make list in one request."""
        ep = endpoints_of(params)
        vtype = clean_str(params.args.vehicle_type)
        if vtype:
            body = get_json(f"{ep.vpic_base_url}/vehicles/GetMakesForVehicleType/{path_segment(vtype)}?format=json")
            rows = [
                {
                    "make_id": r.get("MakeId"),
                    "make": normalize_make(r.get("MakeName")),
                    "vehicle_type": clean_str(r.get("VehicleTypeName")),
                }
                for r in body.get("Results") or []
            ]
        else:
            body = get_json(f"{ep.vpic_base_url}/vehicles/GetAllMakes?format=json")
            rows = [
                {"make_id": r.get("Make_ID"), "make": normalize_make(r.get("Make_Name")), "vehicle_type": None}
                for r in body.get("Results") or []
            ]
        out.emit(pa.RecordBatch.from_pylist(rows, schema=params.output_schema))
        out.finish()


def _models_fields() -> list[pa.Field[Any]]:
    return [
        column("make_id", pa.int32(), "vPIC make id."),
        column("make", pa.string(), "Make, upper-cased join key ('HONDA')."),
        column("model_id", pa.int32(), "vPIC model id.", nullable=False),
        column("model", pa.string(), "Model name as vPIC reports it ('Accord')."),
        column("model_year", pa.int32(), "The model year asked for; NULL when listing all years."),
    ]


MODELS_FIELDS = _models_fields()


@dataclass(slots=True, frozen=True, kw_only=True)
class ModelsArgs:
    """Arguments for ``models(make, year := NULL, vehicle_type := NULL)``."""

    make: Annotated[str, Arg(0, doc="Make name, case-insensitive ('honda'); partial names match in vPIC.")]
    year: Annotated[
        int | None,
        Arg("year", default=None, doc="Only models made in this model year; NULL lists every year.", ge=1900, le=2100),
    ]
    vehicle_type: Annotated[
        str | None,
        Arg(
            "vehicle_type",
            default=None,
            doc="Only models of this vPIC vehicle type; needs `year`.",
            choices=VEHICLE_TYPES,
        ),
    ]


@bind_fixed_schema
@init_single_worker
class Models(TableFunctionGenerator[ModelsArgs, None]):
    """Models of a make from NHTSA vPIC, optionally for one model year."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(MODELS_FIELDS)

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "models"
        description = "Models of a make from NHTSA vPIC, optionally for one model year and vehicle type"
        tags = {
            **object_tags(
                title="Vehicle Models (vPIC)",
                category="vin",
                keywords="models, car models, model lineup, nhtsa, vpic, make, model year",
                doc_llm=(
                    "List the models of a make from NHTSA vPIC as (make_id, make, model_id, model, "
                    "model_year). `make` is positional and case-insensitive ('honda'); vPIC also matches "
                    "partial names, so a short name can return several makes. Named `year := 2020` limits "
                    "to one model year, and `vehicle_type := 'car'|'truck'|'mpv'` (with year) to one type."
                ),
                doc_md=(
                    "## models(make, year := NULL, vehicle_type := NULL)\n\n"
                    "Models of a make from [NHTSA vPIC](https://vpic.nhtsa.dot.gov/api/)."
                ),
            ),
            "vgi.result_columns_schema": result_columns(MODELS_FIELDS),
            "vgi.source_url": VPIC_DOCS,
        }
        examples = [
            FunctionExample(
                sql="SELECT model FROM cars.main.models('honda', year := 2020) WHERE make = 'HONDA' ORDER BY model",
                description="Honda's 2020 model lineup according to NHTSA",
            ),
        ]

    @classmethod
    def process(cls, params: ProcessParams[ModelsArgs], state: None, out: OutputCollector) -> None:
        """Fetch the model list in one request."""
        ep = endpoints_of(params)
        make = clean_str(params.args.make)
        if not make:
            raise ValueError("models() needs a make, e.g. models('honda')")
        year, vtype = params.args.year, clean_str(params.args.vehicle_type)
        if vtype and year is None:
            raise ValueError(
                "models(): vehicle_type needs year as well, e.g. models('honda', year := 2020, vehicle_type := 'car')"
            )
        if year is None:
            url = f"{ep.vpic_base_url}/vehicles/GetModelsForMake/{path_segment(make)}?format=json"
        else:
            url = f"{ep.vpic_base_url}/vehicles/GetModelsForMakeYear/make/{path_segment(make)}/modelyear/{year}"
            if vtype:
                url += f"/vehicletype/{path_segment(vtype)}"
            url += "?format=json"
        body = get_json(url)
        rows = [
            {
                "make_id": r.get("Make_ID"),
                "make": normalize_make(r.get("Make_Name")),
                "model_id": r.get("Model_ID"),
                "model": clean_str(r.get("Model_Name")),
                "model_year": year,
            }
            for r in body.get("Results") or []
        ]
        out.emit(pa.RecordBatch.from_pylist(rows, schema=params.output_schema))
        out.finish()
