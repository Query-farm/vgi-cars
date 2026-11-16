"""NHTSA safety data from api.nhtsa.gov: recalls, owner complaints and NCAP 5-Star ratings."""

from __future__ import annotations

import re
import urllib.parse
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Arg
from vgi.metadata import FunctionExample
from vgi.table_function import (
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from .common import (
    clean_str,
    endpoints_of,
    get_json,
    normalize_make,
    parse_date,
    path_segment,
    to_float,
    to_int,
    to_rating,
)
from .meta import column, object_tags, result_columns

NHTSA_DOCS = "https://www.nhtsa.gov/nhtsa-datasets-and-apis"


@dataclass(slots=True, frozen=True, kw_only=True)
class VehicleArgs:
    """``(make, model, year)``: the vehicle NHTSA is asked about."""

    make: Annotated[str, Arg(0, doc="Make, case-insensitive ('honda').")]
    model: Annotated[
        str, Arg(1, doc="Model as NHTSA names it, case-insensitive; usually the base model, for example accord.")
    ]
    year: Annotated[int, Arg(2, doc="Model year of the vehicle, for example 2020.", ge=1949, le=2100)]


def _query(params: ProcessParams[VehicleArgs]) -> tuple[str, str, int]:
    make, model = clean_str(params.args.make), clean_str(params.args.model)
    if not make or not model:
        raise ValueError("make and model must be non-empty, e.g. ('honda', 'accord', 2020)")
    return make, model, int(params.args.year)


# ---------------------------------------------------------------- recalls


def _recall_fields() -> list[pa.Field[Any]]:
    c = column
    return [
        c("campaign_number", pa.string(), "NHTSA recall campaign number ('19V182000').", nullable=False),
        c("make", pa.string(), "Make, upper-cased join key."),
        c("model", pa.string(), "Model as NHTSA names it (upper-case)."),
        c("model_year", pa.int32(), "Model year."),
        c("manufacturer", pa.string(), "Manufacturer that filed the recall."),
        c("report_received_date", pa.date32(), "Date NHTSA received the manufacturer's defect report."),
        c("component", pa.string(), "Affected component path ('AIR BAGS:FRONTAL:DRIVER SIDE:INFLATOR MODULE')."),
        c("summary", pa.string(), "Description of the defect."),
        c("consequence", pa.string(), "What can happen because of the defect."),
        c("remedy", pa.string(), "How the manufacturer will fix it."),
        c("notes", pa.string(), "Additional notes and contact information."),
        c("action_number", pa.string(), "Related NHTSA investigation (action) number, if any."),
        c("park_it", pa.bool_(), "TRUE when owners are told not to drive the vehicle until repaired."),
        c("park_outside", pa.bool_(), "TRUE when owners are told to park outside (fire risk)."),
        c("over_the_air_update", pa.bool_(), "TRUE when the remedy is an over-the-air software update."),
        c("recall_url", pa.string(), "The recall's page on nhtsa.gov."),
    ]


RECALL_FIELDS = _recall_fields()


@bind_fixed_schema
@init_single_worker
class Recalls(TableFunctionGenerator[VehicleArgs, None]):
    """Safety recalls NHTSA lists for one make, model and model year."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(RECALL_FIELDS)

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "recalls"
        description = "NHTSA safety recalls for a make, model and model year"
        tags = {
            **object_tags(
                title="Safety Recalls (NHTSA)",
                category="safety",
                keywords="recalls, safety recall, defect, nhtsa, campaign, takata, airbag, remedy",
                doc_llm=(
                    "Safety recall campaigns NHTSA lists for one vehicle: `recalls(make, model, year)`, "
                    "all positional and required (make/model case-insensitive, year `INTEGER`). One row per "
                    "campaign: campaign_number, make, model, model_year, manufacturer, "
                    "report_received_date (`DATE`), component, summary, consequence, remedy, notes, "
                    "action_number, park_it, park_outside, over_the_air_update, recall_url. Unknown "
                    "vehicles return zero rows. NHTSA model names are often the base model ('ACCORD', "
                    "'F-150'), not the EPA trim name."
                ),
                doc_md=(
                    "## recalls(make, model, year)\n\n"
                    "Safety recalls from the [NHTSA recalls API](https://www.nhtsa.gov/nhtsa-datasets-and-apis), "
                    "one row per campaign."
                ),
            ),
            "vgi.result_columns_schema": result_columns(RECALL_FIELDS),
            "vgi.source_url": NHTSA_DOCS,
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT campaign_number, report_received_date, component "
                    "FROM cars.main.recalls('honda', 'accord', 2003) "
                    "WHERE component LIKE 'AIR BAGS%' ORDER BY report_received_date, campaign_number"
                ),
                description="Air bag recalls for the 2003 Honda Accord, oldest first",
            ),
        ]

    @classmethod
    def process(cls, params: ProcessParams[VehicleArgs], state: None, out: OutputCollector) -> None:
        """Fetch the recalls in one request."""
        ep = endpoints_of(params)
        make, model, year = _query(params)
        qs = urllib.parse.urlencode({"make": make, "model": model, "modelYear": year})
        body = get_json(f"{ep.nhtsa_base_url}/recalls/recallsByVehicle?{qs}", empty_on_400=True)
        rows = []
        for r in body.get("results") or []:
            campaign = clean_str(r.get("NHTSACampaignNumber"))
            if not campaign:
                continue
            rows.append(
                {
                    "campaign_number": campaign,
                    "make": normalize_make(r.get("Make")) or make.upper(),
                    "model": clean_str(r.get("Model")),
                    "model_year": to_int(r.get("ModelYear")) or year,
                    "manufacturer": clean_str(r.get("Manufacturer")),
                    # NHTSA sends ReportReceivedDate day-first ('21/02/2024').
                    "report_received_date": parse_date(r.get("ReportReceivedDate"), "dmy"),
                    "component": clean_str(r.get("Component")),
                    "summary": clean_str(r.get("Summary")),
                    "consequence": clean_str(r.get("Consequence")),
                    "remedy": clean_str(r.get("Remedy")),
                    "notes": clean_str(r.get("Notes")),
                    "action_number": clean_str(r.get("NHTSAActionNumber")),
                    "park_it": r.get("parkIt"),
                    "park_outside": r.get("parkOutSide"),
                    "over_the_air_update": r.get("overTheAirUpdate"),
                    "recall_url": f"https://www.nhtsa.gov/recalls?nhtsaId={campaign}",
                }
            )
        out.emit(pa.RecordBatch.from_pylist(rows, schema=params.output_schema))
        out.finish()


# ---------------------------------------------------------------- complaints

# Components are comma-joined, but names contain ", " ('SERVICE BRAKES, HYDRAULIC').
_COMPONENT_SPLIT = re.compile(r",(?! )")


def split_components(value: Any) -> list[str]:
    """Split NHTSA's comma-joined component list without breaking 'SERVICE BRAKES, HYDRAULIC'."""
    text = clean_str(value)
    return [part.strip() for part in _COMPONENT_SPLIT.split(text) if part.strip()] if text else []


def _complaint_fields() -> list[pa.Field[Any]]:
    c = column
    return [
        c("odi_number", pa.int64(), "NHTSA Office of Defects Investigation complaint number.", nullable=False),
        c("make", pa.string(), "Make asked for, upper-cased join key."),
        c("model", pa.string(), "Model asked for, upper-cased."),
        c("model_year", pa.int32(), "Model year asked for."),
        c("manufacturer", pa.string(), "Manufacturer of the vehicle."),
        c("incident_date", pa.date32(), "Date of the incident, as reported by the owner."),
        c("complaint_date", pa.date32(), "Date the complaint was filed with NHTSA."),
        c("crash", pa.bool_(), "TRUE when the complaint reports a crash."),
        c("fire", pa.bool_(), "TRUE when the complaint reports a fire."),
        c("injuries", pa.int32(), "Number of injuries reported."),
        c("deaths", pa.int32(), "Number of deaths reported."),
        c("components", pa.list_(pa.string()), "Components the owner named ('AIR BAGS', 'SERVICE BRAKES, HYDRAULIC')."),
        c("summary", pa.string(), "The owner's description of the problem."),
        c("vin_prefix", pa.string(), "First 11 characters of the VIN as published (the rest is withheld)."),
    ]


COMPLAINT_FIELDS = _complaint_fields()


@dataclass(kw_only=True)
class ComplaintsCursor(ArrowSerializableDataclass):
    """Offset into the complaint list (popular vehicles have thousands)."""

    offset: int = 0


COMPLAINT_BATCH = 2000


@bind_fixed_schema
@init_single_worker
class Complaints(TableFunctionGenerator[VehicleArgs, ComplaintsCursor]):
    """Owner complaints NHTSA received about one make, model and model year."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(COMPLAINT_FIELDS)

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "complaints"
        description = "Owner complaints filed with NHTSA for a make, model and model year"
        tags = {
            **object_tags(
                title="Owner Complaints (NHTSA)",
                category="safety",
                keywords="complaints, owner complaints, defects, nhtsa, odi, crash, fire, injuries",
                doc_llm=(
                    "Owner complaints NHTSA's Office of Defects Investigation received about one vehicle: "
                    "`complaints(make, model, year)`, all positional and required. One row per complaint: "
                    "odi_number, make, model, model_year, manufacturer, incident_date, complaint_date "
                    "(`DATE`), crash, fire, injuries, deaths, components (`VARCHAR[]`, e.g. "
                    "['AIR BAGS', 'SERVICE BRAKES, HYDRAULIC']), summary, vin_prefix. Use "
                    "`unnest(components)` to count by component. Unknown vehicles return zero rows; "
                    "complaints are unverified owner reports."
                ),
                doc_md=(
                    "## complaints(make, model, year)\n\n"
                    "Owner complaints from the [NHTSA complaints API](https://www.nhtsa.gov/nhtsa-datasets-and-apis). "
                    "These are unverified reports, useful for spotting patterns."
                ),
            ),
            "vgi.result_columns_schema": result_columns(COMPLAINT_FIELDS),
            "vgi.source_url": NHTSA_DOCS,
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT component, count(*) AS complaints "
                    "FROM (SELECT unnest(components) AS component "
                    "FROM cars.main.complaints('honda', 'accord', 2003)) "
                    "GROUP BY component ORDER BY complaints DESC, component LIMIT 5"
                ),
                description="The five components owners complain about most for the 2003 Honda Accord",
            ),
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[VehicleArgs]) -> ComplaintsCursor:
        """Start at the first complaint."""
        return ComplaintsCursor()

    @classmethod
    def process(cls, params: ProcessParams[VehicleArgs], state: ComplaintsCursor, out: OutputCollector) -> None:
        """Fetch the complaints (one request) and emit them in slices."""
        ep = endpoints_of(params)
        make, model, year = _query(params)
        qs = urllib.parse.urlencode({"make": make, "model": model, "modelYear": year})
        body = get_json(f"{ep.nhtsa_base_url}/complaints/complaintsByVehicle?{qs}", empty_on_400=True)
        results = body.get("results") or []
        chunk = results[state.offset : state.offset + COMPLAINT_BATCH]
        if not chunk:
            out.finish()
            return
        state.offset += len(chunk)
        rows = []
        for r in chunk:
            odi = to_int(r.get("odiNumber"))
            if odi is None:
                continue
            rows.append(
                {
                    "odi_number": odi,
                    "make": make.upper(),
                    "model": model.upper(),
                    "model_year": year,
                    "manufacturer": clean_str(r.get("manufacturer")),
                    # Complaint dates are month-first ('06/18/2026').
                    "incident_date": parse_date(r.get("dateOfIncident"), "mdy"),
                    "complaint_date": parse_date(r.get("dateComplaintFiled"), "mdy"),
                    "crash": r.get("crash"),
                    "fire": r.get("fire"),
                    "injuries": to_int(r.get("numberOfInjuries")),
                    "deaths": to_int(r.get("numberOfDeaths")),
                    "components": split_components(r.get("components")),
                    "summary": clean_str(r.get("summary")),
                    "vin_prefix": clean_str(r.get("vin")),
                }
            )
        out.emit(pa.RecordBatch.from_pylist(rows, schema=params.output_schema))
        if state.offset >= len(results):
            out.finish()


# ---------------------------------------------------------------- safety ratings


def _rating_fields() -> list[pa.Field[Any]]:
    c = column
    stars = "NCAP stars 1-5; NULL when not rated."
    return [
        c("vehicle_id", pa.int64(), "NHTSA NCAP vehicle id (one per tested body style/drive).", nullable=False),
        c("vehicle_description", pa.string(), "Tested variant ('2020 Honda ACCORD 4 DR FWD')."),
        c("make", pa.string(), "Make, upper-cased join key."),
        c("model", pa.string(), "Model as NHTSA names it (upper-case)."),
        c("model_year", pa.int32(), "Model year."),
        c("overall_rating", pa.int32(), f"Overall vehicle score. {stars}"),
        c("overall_front_crash_rating", pa.int32(), f"Overall frontal crash rating. {stars}"),
        c("front_crash_driver_rating", pa.int32(), f"Frontal crash, driver side. {stars}"),
        c("front_crash_passenger_rating", pa.int32(), f"Frontal crash, passenger side. {stars}"),
        c("overall_side_crash_rating", pa.int32(), f"Overall side crash rating. {stars}"),
        c("side_crash_driver_rating", pa.int32(), f"Side barrier crash, driver side. {stars}"),
        c("side_crash_passenger_rating", pa.int32(), f"Side barrier crash, passenger side. {stars}"),
        c("side_pole_crash_rating", pa.int32(), f"Side pole crash. {stars}"),
        c("rollover_rating", pa.int32(), f"Rollover resistance. {stars}"),
        c(
            "rollover_risk",
            pa.float64(),
            "Calculated probability of rollover in a single-vehicle crash (0.093 = 9.3%); NULL when not rated.",
        ),
        c("dynamic_tip_result", pa.string(), "Dynamic rollover test result ('No Tip', 'Tip')."),
        c("electronic_stability_control", pa.string(), "ESC availability ('Standard', 'Optional', 'Not Available')."),
        c("forward_collision_warning", pa.string(), "Forward collision warning availability."),
        c("lane_departure_warning", pa.string(), "Lane departure warning availability."),
        c("complaints_count", pa.int32(), "Complaints NHTSA has on file for this vehicle."),
        c("recalls_count", pa.int32(), "Recalls NHTSA has on file for this vehicle."),
        c("investigations_count", pa.int32(), "Defect investigations NHTSA has on file for this vehicle."),
        c("front_crash_picture_url", pa.string(), "Photo from the frontal crash test."),
        c("side_crash_picture_url", pa.string(), "Photo from the side crash test."),
    ]


RATING_FIELDS = _rating_fields()


def rating_row(r: dict[str, Any], vehicle_id: int) -> dict[str, Any]:
    """Map one ``SafetyRatings/VehicleId/{id}`` result onto the output columns."""
    rated = to_rating(r.get("RolloverRating")) is not None
    return {
        "vehicle_id": vehicle_id,
        "vehicle_description": clean_str(r.get("VehicleDescription")),
        "make": normalize_make(r.get("Make")),
        "model": clean_str(r.get("Model")),
        "model_year": to_int(r.get("ModelYear")),
        "overall_rating": to_rating(r.get("OverallRating")),
        "overall_front_crash_rating": to_rating(r.get("OverallFrontCrashRating")),
        "front_crash_driver_rating": to_rating(r.get("FrontCrashDriversideRating")),
        "front_crash_passenger_rating": to_rating(r.get("FrontCrashPassengersideRating")),
        "overall_side_crash_rating": to_rating(r.get("OverallSideCrashRating")),
        "side_crash_driver_rating": to_rating(r.get("SideCrashDriversideRating")),
        "side_crash_passenger_rating": to_rating(r.get("SideCrashPassengersideRating")),
        "side_pole_crash_rating": to_rating(r.get("SidePoleCrashRating")),
        "rollover_rating": to_rating(r.get("RolloverRating")),
        "rollover_risk": to_float(r.get("RolloverPossibility")) if rated else None,
        "dynamic_tip_result": clean_str(r.get("dynamicTipResult")),
        "electronic_stability_control": clean_str(r.get("NHTSAElectronicStabilityControl")),
        "forward_collision_warning": clean_str(r.get("NHTSAForwardCollisionWarning")),
        "lane_departure_warning": clean_str(r.get("NHTSALaneDepartureWarning")),
        "complaints_count": to_int(r.get("ComplaintsCount")),
        "recalls_count": to_int(r.get("RecallsCount")),
        "investigations_count": to_int(r.get("InvestigationCount")),
        "front_crash_picture_url": clean_str(r.get("FrontCrashPicture")),
        "side_crash_picture_url": clean_str(r.get("SideCrashPicture")),
    }


@bind_fixed_schema
@init_single_worker
class SafetyRatings(TableFunctionGenerator[VehicleArgs, None]):
    """NCAP 5-Star safety ratings for every tested variant of one make, model and model year."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(RATING_FIELDS)

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "safety_ratings"
        description = "NHTSA NCAP 5-Star crash ratings for each tested variant of a make, model and model year"
        tags = {
            **object_tags(
                title="NCAP Safety Ratings (NHTSA)",
                category="safety",
                keywords="safety ratings, crash test, ncap, 5-star, nhtsa, rollover, frontal crash, side crash",
                doc_llm=(
                    "NHTSA New Car Assessment Program (NCAP) 5-Star ratings: `safety_ratings(make, model, "
                    "year)`, all positional and required. One row per tested variant (body style/drive, "
                    "e.g. '2020 Honda ACCORD 4 DR FWD') with vehicle_id, vehicle_description, make, model, "
                    "model_year, overall_rating, overall_front_crash_rating, front_crash_driver_rating, "
                    "front_crash_passenger_rating, overall_side_crash_rating, side_crash_driver_rating, "
                    "side_crash_passenger_rating, side_pole_crash_rating, rollover_rating (`INTEGER` stars "
                    "1-5, NULL = not rated), rollover_risk (probability), dynamic_tip_result, "
                    "electronic_stability_control, forward_collision_warning, lane_departure_warning, "
                    "complaints_count, recalls_count, investigations_count and crash-test photo URLs. "
                    "Vehicles NHTSA did not test return zero rows."
                ),
                doc_md=(
                    "## safety_ratings(make, model, year)\n\n"
                    "[NHTSA 5-Star Safety Ratings](https://www.nhtsa.gov/ratings) for each tested variant."
                ),
            ),
            "vgi.result_columns_schema": result_columns(RATING_FIELDS),
            "vgi.source_url": NHTSA_DOCS,
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT vehicle_description, overall_rating, rollover_rating, rollover_risk "
                    "FROM cars.main.safety_ratings('toyota', 'tundra', 2024) ORDER BY vehicle_description"
                ),
                description="Crash ratings for each tested 2024 Toyota Tundra cab and drive variant",
            ),
        ]

    @classmethod
    def process(cls, params: ProcessParams[VehicleArgs], state: None, out: OutputCollector) -> None:
        """List the tested variants, then fetch each variant's ratings."""
        ep = endpoints_of(params)
        make, model, year = _query(params)
        base = f"{ep.nhtsa_base_url}/SafetyRatings"
        listing = get_json(
            f"{base}/modelyear/{year}/make/{path_segment(make)}/model/{path_segment(model)}", empty_on_400=True
        )
        rows = []
        for variant in listing.get("Results") or []:
            vehicle_id = to_int(variant.get("VehicleId"))
            if vehicle_id is None:
                continue
            detail = get_json(f"{base}/VehicleId/{vehicle_id}", empty_on_400=True)
            for r in detail.get("Results") or []:
                rows.append(rating_row(r, vehicle_id))
        out.emit(pa.RecordBatch.from_pylist(rows, schema=params.output_schema))
        out.finish()
