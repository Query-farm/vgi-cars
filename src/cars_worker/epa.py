"""EPA fuel economy: the fueleconomy.gov bulk vehicle file, cached locally.

``vehicles.csv.zip`` (about 2 MB zipped, 22 MB of CSV, ~50,000 rows covering model
years 1984 to the next model year) is downloaded on first use into a per-user cache
directory, verified, and reused. It is re-checked with a conditional request once
it is older than ``MAX_AGE_DAYS`` (ATTACH option), and a failed re-check keeps
serving the cached copy. Nothing is downloaded at import or ATTACH time.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, ClassVar, cast

import pyarrow as pa
import pyarrow.compute as _pc
import pyarrow.csv as pacsv
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

from .common import TIMEOUT_SECONDS, USER_AGENT, Endpoints, UpstreamError, endpoints_of, utc_now
from .meta import column, object_tags, result_columns

log = logging.getLogger(__name__)
# pyarrow-stubs' compute overloads reject valid ChunkedArray-vs-literal calls; the module is
# used untyped here and every result is pinned to the declared schema with pa.Table.from_arrays.
pc: Any = _pc

ZIP_NAME = "vehicles.csv.zip"
META_NAME = "vehicles.meta.json"
CSV_MEMBER = "vehicles.csv"
REQUIRED_COLUMNS = {"id", "year", "make", "model", "baseModel", "city08", "highway08", "comb08", "VClass"}
BATCH_ROWS = 8192
VEHICLE_PAGE = "https://www.fueleconomy.gov/feg/noframes/{id}.shtml"
SOURCE_PAGE = "https://www.fueleconomy.gov/feg/ws/index.shtml"

_lock = threading.Lock()
_memo: dict[Path, tuple[int, int, pa.Table]] = {}


# ---------------------------------------------------------------- cache management


@dataclass(frozen=True, slots=True)
class CacheState:
    """What the cache holds after an ``ensure`` call."""

    path: Path
    meta: dict[str, Any]
    status: str


def _read_meta(cache_dir: Path) -> dict[str, Any]:
    try:
        loaded = json.loads((cache_dir / META_NAME).read_text())
        return loaded if isinstance(loaded, dict) else {}
    except OSError, ValueError:
        return {}


def _write_meta(cache_dir: Path, meta: dict[str, Any]) -> None:
    tmp = cache_dir / f"{META_NAME}.{os.getpid()}.tmp"
    tmp.write_text(json.dumps(meta, indent=1, sort_keys=True))
    tmp.replace(cache_dir / META_NAME)


def verify_zip(data: bytes) -> None:
    """Check that ``data`` is the EPA vehicles zip: one readable CSV with the expected header.

    Args:
        data: The downloaded bytes.

    Raises:
        UpstreamError: The payload is not a usable vehicles file.

    """
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            names = archive.namelist()
            if CSV_MEMBER not in names:
                raise UpstreamError(f"EPA vehicles zip has no {CSV_MEMBER} (members: {names[:5]})")
            with archive.open(CSV_MEMBER) as handle:
                header = handle.readline().decode("utf-8-sig").strip().split(",")
            if archive.testzip() is not None:
                raise UpstreamError("EPA vehicles zip is corrupt (CRC check failed)")
    except zipfile.BadZipFile as err:
        raise UpstreamError(f"EPA vehicles download is not a zip file: {err}") from err
    missing = REQUIRED_COLUMNS - set(header)
    if missing:
        raise UpstreamError(f"EPA vehicles CSV lacks expected columns: {sorted(missing)}")


def _download(ep: Endpoints, meta: dict[str, Any], conditional: bool) -> tuple[bytes | None, dict[str, str]]:
    """Fetch the bulk file.

    Args:
        ep: Endpoints (the bulk file URL).
        meta: Validators of the cached copy (``etag``, ``last_modified``).
        conditional: Send ``If-None-Match`` / ``If-Modified-Since``.

    Returns:
        The body (``None`` on 304 Not Modified) and the response's validators.

    Raises:
        UpstreamError: The download failed after retries.

    """
    headers = {"User-Agent": USER_AGENT, "Accept": "application/zip, */*"}
    if conditional and meta.get("etag"):
        headers["If-None-Match"] = str(meta["etag"])
    if conditional and meta.get("last_modified"):
        headers["If-Modified-Since"] = str(meta["last_modified"])
    url = ep.epa_vehicles_url
    for attempt in range(1, 5):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=TIMEOUT_SECONDS) as r:
                body: bytes = r.read()
                return body, {
                    "etag": r.headers.get("ETag") or "",
                    "last_modified": r.headers.get("Last-Modified") or "",
                }
        except urllib.error.HTTPError as err:
            if err.code == 304:
                return None, {}
            if (err.code != 429 and err.code < 500) or attempt == 4:
                raise UpstreamError(f"GET {url} failed with HTTP {err.code}") from err
        except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
            if attempt == 4:
                raise UpstreamError(f"GET {url} failed: {getattr(err, 'reason', err)}") from err
        time.sleep(0.5 * 2**attempt)
    raise UpstreamError(f"GET {url}: retries exhausted")


def ensure_cache(ep: Endpoints, *, refresh: bool = False, force: bool = False) -> CacheState:
    """Make sure the bulk file is cached, downloading or re-checking it when due.

    Args:
        ep: Endpoints and cache settings.
        refresh: Re-check upstream now (conditional request) regardless of age.
        force: Download unconditionally, replacing the cached copy.

    Returns:
        The cache path, its metadata and what happened ('cached', 'downloaded',
        'not_modified', 'stale' when a re-check failed, or 'offline').

    Raises:
        UpstreamError: No cached copy exists and it cannot be downloaded.

    """
    cache_dir = ep.cache_dir
    path = cache_dir / ZIP_NAME
    with _lock:
        meta = _read_meta(cache_dir)
        exists = path.is_file()
        if exists and meta.get("source_url") not in (None, ep.epa_vehicles_url):
            # The cache was filled from another URL (e.g. a fixture server); do not mix them.
            exists, meta = False, {}
        if exists and not (refresh or force):
            checked = float(meta.get("checked_epoch", path.stat().st_mtime))
            if time.time() - checked < ep.max_age_days * 86400:
                return CacheState(path, meta, "cached")
        if ep.offline:
            if exists:
                return CacheState(path, meta, "offline")
            raise UpstreamError(
                f"OFFLINE is set and no cached EPA vehicles file exists in {cache_dir}; "
                "attach without OFFLINE once to download it"
            )
        try:
            body, validators = _download(ep, meta, conditional=exists and not force)
        except UpstreamError as err:
            if exists:
                log.warning("EPA vehicles re-check failed, serving the cached copy: %s", err)
                return CacheState(path, meta, "stale")
            raise
        now = utc_now()
        if body is None:
            meta = {**meta, "checked_at": now.isoformat(), "checked_epoch": time.time()}
            _write_meta(cache_dir, meta)
            return CacheState(path, meta, "not_modified")
        verify_zip(body)
        cache_dir.mkdir(parents=True, exist_ok=True)
        tmp = cache_dir / f"{ZIP_NAME}.{os.getpid()}.tmp"
        tmp.write_bytes(body)
        tmp.replace(path)
        meta = {
            "source_url": ep.epa_vehicles_url,
            "etag": validators.get("etag", ""),
            "last_modified": validators.get("last_modified", ""),
            "downloaded_at": now.isoformat(),
            "checked_at": now.isoformat(),
            "checked_epoch": time.time(),
            "sha256": hashlib.sha256(body).hexdigest(),
            "size_bytes": len(body),
        }
        _write_meta(cache_dir, meta)
        log.info("downloaded EPA vehicles file (%d bytes) to %s", len(body), path)
        return CacheState(path, meta, "downloaded")


# ---------------------------------------------------------------- parsing


def _num(table: pa.Table, name: str, type_: pa.DataType, *, null_if: tuple[float, ...] = ()) -> pa.Array[Any]:
    text = table.column(name)
    numeric = pc.match_substring_regex(text, r"^\s*-?\d+(\.\d*)?([eE][-+]?\d+)?\s*$")
    text = pc.if_else(numeric, text, pa.scalar(None, pa.string()))  # 'NA' and other junk -> NULL
    values = pc.cast(text, pa.float64())
    for sentinel in null_if:
        values = pc.if_else(pc.equal(values, sentinel), pa.scalar(None, pa.float64()), values)
    if pa.types.is_integer(type_):
        values = pc.round(values)
    return cast("pa.Array[Any]", pc.cast(values, type_).combine_chunks())


def _text(table: pa.Table, name: str) -> pa.Array[Any]:
    values = pc.utf8_trim_whitespace(table.column(name))
    empty = pc.equal(values, "")
    return cast("pa.Array[Any]", pc.if_else(empty, pa.scalar(None, pa.string()), values).combine_chunks())


def _flag(table: pa.Table, name: str, truthy: tuple[str, ...]) -> pa.Array[Any]:
    """Boolean from a marker column: a truthy marker is TRUE, blank is FALSE."""
    values = pc.fill_null(table.column(name), "")
    return cast("pa.Array[Any]", pc.is_in(values, pa.array(truthy)).combine_chunks())


def _yes_no(table: pa.Table, name: str) -> pa.Array[Any]:
    """'Y' -> TRUE, 'N' -> FALSE, blank -> NULL."""
    values = table.column(name)
    result = pc.if_else(
        pc.equal(values, "Y"), True, pc.if_else(pc.equal(values, "N"), False, pa.scalar(None, pa.bool_()))
    )
    return cast("pa.Array[Any]", result.combine_chunks())


_MONTHS = pa.array(["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])


def _epa_date(table: pa.Table, name: str) -> pa.Array[Any]:
    """'Tue Jan 01 00:00:00 EST 2013' -> DATE 2013-01-01.

    The month name is mapped to a number here because Arrow's strptime on Windows
    does not support ``%b``.
    """
    col = table.column(name)
    pattern = r"^\w{3} (\w{3}) (\d{2}) .* (\d{4})$"
    month = pc.index_in(pc.replace_substring_regex(col, pattern, r"\1"), value_set=_MONTHS)
    month_text = pc.utf8_lpad(pc.cast(pc.add(month, 1), pa.string()), 2, "0")
    text = pc.binary_join_element_wise(
        pc.replace_substring_regex(col, pattern, r"\3"),
        month_text,
        pc.replace_substring_regex(col, pattern, r"\2"),
        "-",
    )
    parsed = pc.strptime(text, format="%Y-%m-%d", unit="s", error_is_null=True)
    return cast("pa.Array[Any]", pc.cast(parsed, pa.date32()).combine_chunks())


def _electric_range(table: pa.Table) -> pa.Array[Any]:
    """All-electric range: ``range`` for EVs, the secondary-fuel range for plug-in hybrids."""
    ev_range = _num(table, "range", pa.int32(), null_if=(0,))
    alt = table.column("rangeA")
    alt_int = pc.cast(
        pc.if_else(pc.match_substring_regex(alt, r"^\d+$"), alt, pa.scalar(None, pa.string())), pa.int32()
    )
    is_ev = pc.equal(pc.fill_null(table.column("atvType"), ""), "EV")
    is_phev = pc.equal(pc.fill_null(table.column("fuelType2"), ""), "Electricity")
    none = pa.scalar(None, pa.int32())
    result = pc.if_else(is_ev, ev_range, pc.if_else(is_phev, alt_int, none))
    return cast("pa.Array[Any]", result.combine_chunks())


def _fields() -> list[pa.Field[Any]]:
    c = column
    return [
        c("id", pa.int64(), "EPA vehicle record id (fueleconomy.gov 'id'); unique per row.", nullable=False),
        c("model_year", pa.int32(), "Model year, 1984 onward; the cross-source join key with NHTSA data."),
        c("make", pa.string(), "Manufacturer brand, upper-cased so it joins with NHTSA/vPIC data ('TOYOTA')."),
        c("model", pa.string(), "EPA model name as published, often with drive or trim ('Tundra 4WD')."),
        c("base_model", pa.string(), "EPA base model name without drive/trim ('Tundra')."),
        c("vehicle_class", pa.string(), "EPA size class ('Midsize Cars', 'Standard Pickup Trucks 4WD')."),
        c("drive", pa.string(), "Drive axle type ('Front-Wheel Drive', 'All-Wheel Drive', '4-Wheel Drive')."),
        c("transmission", pa.string(), "Transmission ('Automatic (S10)', 'Manual 5-spd')."),
        c("transmission_notes", pa.string(), "EPA transmission descriptor codes ('CLKUP', 'SIL'); NULL if none."),
        c("engine_displacement_l", pa.float64(), "Engine displacement in litres; NULL for battery-electric."),
        c("cylinders", pa.int32(), "Number of engine cylinders; NULL for battery-electric."),
        c("turbocharged", pa.bool_(), "TRUE when the engine is turbocharged."),
        c("supercharged", pa.bool_(), "TRUE when the engine is supercharged."),
        c("engine_description", pa.string(), "EPA engine descriptor ('SIDI; PHEV', '(FFS)'); NULL if none."),
        c("electric_motor", pa.string(), "Electric motor/battery description for EVs and hybrids."),
        c(
            "fuel_type",
            pa.string(),
            "Fuel summary ('Regular', 'Premium', 'Electricity', 'Regular Gas and Electricity').",
        ),
        c("primary_fuel", pa.string(), "Primary fuel ('Regular Gasoline', 'Diesel', 'Electricity')."),
        c("secondary_fuel", pa.string(), "Secondary fuel for dual-fuel vehicles ('E85', 'Electricity'); else NULL."),
        c(
            "powertrain",
            pa.string(),
            "Alternative powertrain: 'EV', 'Hybrid', 'Plug-in Hybrid', 'FFV', 'Diesel', 'CNG' ...; "
            "NULL for conventional.",
        ),
        c("city_mpg", pa.int32(), "EPA city fuel economy on the primary fuel, MPG (MPGe for EVs)."),
        c("highway_mpg", pa.int32(), "EPA highway fuel economy on the primary fuel, MPG (MPGe for EVs)."),
        c("combined_mpg", pa.int32(), "EPA combined fuel economy on the primary fuel, MPG (MPGe for EVs)."),
        c(
            "city_mpg_secondary",
            pa.int32(),
            "City MPG on the secondary fuel (MPGe on electricity for PHEVs); NULL if single-fuel.",
        ),
        c("highway_mpg_secondary", pa.int32(), "Highway MPG on the secondary fuel; NULL if single-fuel."),
        c("combined_mpg_secondary", pa.int32(), "Combined MPG on the secondary fuel; NULL if single-fuel."),
        c(
            "combined_kwh_per_100mi",
            pa.float64(),
            "Combined electricity use in kWh per 100 miles; NULL without a plug.",
        ),
        c("electric_range_miles", pa.int32(), "All-electric range in miles (EVs and plug-in hybrids); NULL otherwise."),
        c("co2_grams_per_mile", pa.float64(), "Tailpipe CO2 in grams per mile on the primary fuel (0 for EVs)."),
        c(
            "annual_fuel_cost_usd",
            pa.int32(),
            "EPA estimated annual fuel cost on the primary fuel, US dollars (EPA's current price assumptions).",
        ),
        c(
            "five_year_savings_usd",
            pa.int32(),
            "EPA 'you save/spend' over 5 years versus an average new vehicle; negative means you spend more.",
        ),
        c("fuel_economy_score", pa.int32(), "EPA fuel economy score 1-10 (10 best); NULL before model year 2013."),
        c("ghg_score", pa.int32(), "EPA greenhouse gas score 1-10 (10 best); NULL before model year 2013."),
        c("gas_guzzler", pa.bool_(), "TRUE when the vehicle is subject to the gas guzzler tax."),
        c("start_stop", pa.bool_(), "TRUE when the engine has start-stop; NULL where EPA did not record it."),
        c(
            "charge_time_240v_hours",
            pa.float64(),
            "Hours to charge at 240 V; NULL without a plug or when not reported.",
        ),
        c("manufacturer_code", pa.string(), "EPA 3-letter manufacturer code ('TYX'); NULL for older records."),
        c("created_on", pa.date32(), "Date EPA created the record."),
        c("modified_on", pa.date32(), "Date EPA last modified the record."),
        c("fueleconomy_url", pa.string(), "This vehicle's page on fueleconomy.gov."),
    ]


VEHICLE_FIELDS = _fields()
VEHICLE_SCHEMA = pa.schema(VEHICLE_FIELDS)


def parse_vehicles(data: bytes | Path) -> pa.Table:
    """Parse the EPA vehicles zip into the typed ``vehicles`` table."""
    raw_bytes = data.read_bytes() if isinstance(data, Path) else data
    with zipfile.ZipFile(io.BytesIO(raw_bytes)) as archive, archive.open(CSV_MEMBER) as handle:
        csv_bytes = handle.read()
    header = csv_bytes.split(b"\n", 1)[0].decode("utf-8-sig").strip().split(",")
    raw = pacsv.read_csv(
        io.BytesIO(csv_bytes),
        convert_options=pacsv.ConvertOptions(
            column_types=dict.fromkeys(header, pa.string()), null_values=[""], strings_can_be_null=True
        ),
    )
    ids = _num(raw, "id", pa.int64())
    urls = pc.binary_join_element_wise(
        "https://www.fueleconomy.gov/feg/noframes/", pc.cast(ids, pa.string()), ".shtml", ""
    )
    secondary = pc.is_valid(raw.column("fuelType2"))
    none_i32 = pa.scalar(None, pa.int32())

    def secondary_only(name: str) -> pa.Array[Any]:
        return cast("pa.Array[Any]", pc.if_else(secondary, _num(raw, name, pa.int32()), none_i32))

    plug = pc.greater(pc.cast(pc.fill_null(raw.column("combE"), "0"), pa.float64()), 0)
    columns: list[pa.Array[Any]] = [
        ids,
        _num(raw, "year", pa.int32()),
        pc.utf8_upper(_text(raw, "make")),
        _text(raw, "model"),
        _text(raw, "baseModel"),
        _text(raw, "VClass"),
        _text(raw, "drive"),
        _text(raw, "trany"),
        _text(raw, "trans_dscr"),
        _num(raw, "displ", pa.float64()),
        _num(raw, "cylinders", pa.int32()),
        _flag(raw, "tCharger", ("T",)),
        _flag(raw, "sCharger", ("S",)),
        _text(raw, "eng_dscr"),
        _text(raw, "evMotor"),
        _text(raw, "fuelType"),
        _text(raw, "fuelType1"),
        _text(raw, "fuelType2"),
        _text(raw, "atvType"),
        _num(raw, "city08", pa.int32()),
        _num(raw, "highway08", pa.int32()),
        _num(raw, "comb08", pa.int32()),
        secondary_only("cityA08"),
        secondary_only("highwayA08"),
        secondary_only("combA08"),
        pc.if_else(plug, _num(raw, "combE", pa.float64()), pa.scalar(None, pa.float64())),
        _electric_range(raw),
        _num(raw, "co2TailpipeGpm", pa.float64(), null_if=(-1,)),
        _num(raw, "fuelCost08", pa.int32()),
        _num(raw, "youSaveSpend", pa.int32()),
        _num(raw, "feScore", pa.int32(), null_if=(-1,)),
        _num(raw, "ghgScore", pa.int32(), null_if=(-1,)),
        _flag(raw, "guzzler", ("G", "T", "S")),
        _yes_no(raw, "startStop"),
        _num(raw, "charge240", pa.float64(), null_if=(0,)),
        _text(raw, "mfrCode"),
        _epa_date(raw, "createdOn"),
        _epa_date(raw, "modifiedOn"),
        urls,
    ]
    table = pa.Table.from_arrays(
        [pc.cast(a, f.type) for a, f in zip(columns, VEHICLE_FIELDS, strict=True)], schema=VEHICLE_SCHEMA
    )
    return table.sort_by(
        [("model_year", "ascending"), ("make", "ascending"), ("model", "ascending"), ("id", "ascending")]
    )


def load_vehicles(path: Path) -> pa.Table:
    """Return the parsed table for the cached zip at ``path``, memoized per file version."""
    stat = path.stat()
    with _lock:
        hit = _memo.get(path)
        if hit and hit[0] == stat.st_mtime_ns and hit[1] == stat.st_size:
            return hit[2]
    table = parse_vehicles(path)
    with _lock:
        _memo[path] = (stat.st_mtime_ns, stat.st_size, table)
    return table


# ---------------------------------------------------------------- vehicles()


@dataclass(slots=True, frozen=True, kw_only=True)
class VehiclesArgs:
    """Arguments for ``vehicles(year := NULL, make := NULL, model := NULL)``."""

    year: Annotated[
        int | None,
        Arg("year", default=None, doc="Only this model year (1984 onward); NULL means all years.", ge=1984, le=2100),
    ]
    make: Annotated[
        str | None,
        Arg("make", default=None, doc="Only this make, case-insensitive ('toyota'); NULL means all makes."),
    ]
    model: Annotated[
        str | None,
        Arg(
            "model",
            default=None,
            doc="Only this model, case-insensitive; matches the base model ('Tundra') or the full EPA model "
            "name ('Tundra 4WD'). NULL means all models.",
        ),
    ]


@dataclass(kw_only=True)
class VehiclesCursor(ArrowSerializableDataclass):
    """Scan position over the filtered table, plus the file version it refers to."""

    offset: int = 0
    sha256: str = ""


def filter_vehicles(table: pa.Table, year: int | None, make: str | None, model: str | None) -> pa.Table:
    """Apply the named-argument filters (all optional, case-insensitive for text)."""
    mask = None

    def both(m: Any, cond: Any) -> Any:
        return cond if m is None else pc.and_kleene(m, cond)

    if year is not None:
        mask = both(mask, pc.equal(table.column("model_year"), year))
    if make:
        mask = both(mask, pc.equal(table.column("make"), make.strip().upper()))
    if model:
        wanted = model.strip().lower()
        mask = both(
            mask,
            pc.or_kleene(
                pc.equal(pc.utf8_lower(table.column("base_model")), wanted),
                pc.equal(pc.utf8_lower(table.column("model")), wanted),
            ),
        )
    return table if mask is None else table.filter(pc.fill_null(mask, False))


_VEHICLES_DOC_LLM = (
    "EPA fuel economy and powertrain specifications for every light-duty vehicle sold in the US since "
    "model year 1984 (about 50,000 rows), from the fueleconomy.gov bulk file. One row per EPA vehicle "
    "configuration (model year x make x model x engine x transmission x drive). Optional named filters: "
    "`year := 2024`, `make := 'toyota'` (case-insensitive), `model := 'tundra'` (matches base_model or "
    "the full EPA model name, case-insensitive). Key columns: model_year, make (upper-cased join key), "
    "model, base_model, vehicle_class, drive, transmission, cylinders, engine_displacement_l, "
    "fuel_type, powertrain ('EV', 'Hybrid', 'Plug-in Hybrid', 'FFV', 'Diesel', 'CNG' or NULL for "
    "conventional), city_mpg/highway_mpg/combined_mpg (MPGe for EVs), *_secondary MPG for dual-fuel "
    "vehicles, electric_range_miles, co2_grams_per_mile, annual_fuel_cost_usd, fuel_economy_score, "
    "ghg_score, fueleconomy_url. The file is downloaded once (~2 MB) into a local cache and re-checked "
    "weekly; use `cache_info()` to see its vintage. Join to NHTSA functions on model_year and make; "
    "compare upper(base_model) with NHTSA's upper-case model names. No colors: no free official source."
)


@bind_fixed_schema
@init_single_worker
class Vehicles(TableFunctionGenerator[VehiclesArgs, VehiclesCursor]):
    """EPA vehicle records from the cached fueleconomy.gov bulk file."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = VEHICLE_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "vehicles"
        description = "EPA fuel economy and specs for US vehicles since 1984, filterable by year, make and model"
        projection_pushdown = True
        tags = {
            **object_tags(
                title="EPA Vehicles (Fuel Economy)",
                category="fuel_economy",
                keywords=(
                    "fuel economy, mpg, mpge, epa, fueleconomy.gov, vehicle specs, engine, transmission, "
                    "electric vehicle, ev range, hybrid, co2 emissions, fuel cost, car models"
                ),
                doc_llm=_VEHICLES_DOC_LLM,
                doc_md=(
                    "## vehicles(year := NULL, make := NULL, model := NULL)\n\n"
                    "Every EPA-tested US light-duty vehicle since 1984 with MPG/MPGe, CO2, fuel cost, "
                    "EV range and powertrain details, from the "
                    "[fueleconomy.gov](https://www.fueleconomy.gov/feg/ws/index.shtml) bulk file "
                    "(US government work, public domain).\n\n"
                    "- Named filters are applied before rows leave the worker; `make` and `model` are "
                    "case-insensitive, and `model` matches the base model or full model name.\n"
                    "- `make` is upper-cased to join with the NHTSA functions.\n"
                    "- The ~2 MB file is cached locally (see `cache_info()` / `refresh_cache()`)."
                ),
            ),
            "vgi.result_columns_schema": result_columns(VEHICLE_FIELDS),
            "vgi.source_url": SOURCE_PAGE,
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT model, drive, cylinders, city_mpg, highway_mpg, combined_mpg "
                    "FROM cars.main.vehicles(year := 2024, make := 'toyota', model := 'tundra') "
                    "ORDER BY combined_mpg DESC, model"
                ),
                description="Compare the fuel economy of every 2024 Toyota Tundra configuration",
            ),
            FunctionExample(
                sql=(
                    "SELECT model_year, count(*) AS ev_models, max(electric_range_miles) AS longest_range "
                    "FROM cars.main.vehicles() WHERE powertrain = 'EV' AND model_year >= 2020 "
                    "GROUP BY model_year ORDER BY model_year"
                ),
                description="Count EV models and the longest EPA range per model year since 2020",
            ),
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[VehiclesArgs]) -> VehiclesCursor:
        """Make sure the bulk file is cached and remember which version this scan reads."""
        state = ensure_cache(endpoints_of(params))
        return VehiclesCursor(offset=0, sha256=str(state.meta.get("sha256", "")))

    @classmethod
    def process(cls, params: ProcessParams[VehiclesArgs], state: VehiclesCursor, out: OutputCollector) -> None:
        """Emit the next slice of matching rows, projected to the requested columns."""
        ep = endpoints_of(params)
        cache = ensure_cache(ep)
        if state.sha256 and cache.meta.get("sha256") and cache.meta["sha256"] != state.sha256:
            raise UpstreamError("the cached EPA vehicles file was refreshed during this scan; run the query again")
        table = filter_vehicles(load_vehicles(cache.path), params.args.year, params.args.make, params.args.model)
        chunk = table.slice(state.offset, BATCH_ROWS)
        if chunk.num_rows == 0:
            out.finish()
            return
        state.offset += chunk.num_rows
        names = params.output_schema.names
        out.emit(
            pa.RecordBatch.from_arrays([chunk.column(n).combine_chunks() for n in names], schema=params.output_schema)
        )


# ---------------------------------------------------------------- cache_info() / refresh_cache()


def _cache_fields() -> list[pa.Field[Any]]:
    c = column
    return [
        c(
            "status",
            pa.string(),
            "What this call did: 'cached', 'downloaded', 'not_modified', 'stale', 'offline' or 'missing'.",
            nullable=False,
        ),
        c("cache_path", pa.string(), "Absolute path of the cached vehicles.csv.zip.", nullable=False),
        c("cached", pa.bool_(), "TRUE when a verified copy is present in the cache.", nullable=False),
        c(
            "source_url",
            pa.string(),
            "URL the cached copy was downloaded from (and is refreshed from).",
            nullable=False,
        ),
        c("size_bytes", pa.int64(), "Size of the cached zip in bytes; NULL when not cached."),
        c("sha256", pa.string(), "SHA-256 of the cached zip; NULL when not cached."),
        c("vehicle_rows", pa.int64(), "Number of vehicle records in the cached file; NULL when not cached."),
        c("min_model_year", pa.int32(), "Earliest model year in the cached file."),
        c("max_model_year", pa.int32(), "Latest model year in the cached file."),
        c("upstream_last_modified", pa.string(), "Last-Modified header fueleconomy.gov sent for the cached copy."),
        c("downloaded_at", pa.timestamp("us", tz="UTC"), "When the cached copy was downloaded."),
        c("checked_at", pa.timestamp("us", tz="UTC"), "When upstream was last checked for a newer file."),
        c("max_age_days", pa.int32(), "Re-check interval in days (ATTACH option MAX_AGE_DAYS).", nullable=False),
    ]


CACHE_FIELDS = _cache_fields()
CACHE_SCHEMA = pa.schema(CACHE_FIELDS)


def _ts(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value)) if value else None
    except ValueError:
        return None


def cache_row(ep: Endpoints, status: str) -> pa.RecordBatch:
    """Describe the cache as one row."""
    path = ep.cache_dir / ZIP_NAME
    meta = _read_meta(ep.cache_dir)
    cached = path.is_file() and meta.get("source_url") in (None, ep.epa_vehicles_url)
    rows = min_year = max_year = None
    if cached:
        table = load_vehicles(path)
        rows = table.num_rows
        years = table.column("model_year")
        min_year, max_year = pc.min(years).as_py(), pc.max(years).as_py()
    row = {
        "status": status if cached or status != "cached" else "missing",
        "cache_path": str(path.resolve()),
        "cached": cached,
        "source_url": ep.epa_vehicles_url,
        "size_bytes": path.stat().st_size if cached else None,
        "sha256": meta.get("sha256") if cached else None,
        "vehicle_rows": rows,
        "min_model_year": min_year,
        "max_model_year": max_year,
        "upstream_last_modified": (meta.get("last_modified") or None) if cached else None,
        "downloaded_at": _ts(meta.get("downloaded_at")) if cached else None,
        "checked_at": _ts(meta.get("checked_at")) if cached else None,
        "max_age_days": ep.max_age_days,
    }
    return pa.RecordBatch.from_pylist([row], schema=CACHE_SCHEMA)


@dataclass(slots=True, frozen=True, kw_only=True)
class NoArgs:
    """``cache_info()`` takes no arguments."""


@bind_fixed_schema
@init_single_worker
class CacheInfo(TableFunctionGenerator[NoArgs, None]):
    """Report the state of the local EPA bulk-file cache without touching the network."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = CACHE_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "cache_info"
        description = "Where the EPA vehicles file is cached, its vintage, size and row count (no network)"
        tags = {
            **object_tags(
                title="EPA Cache Info",
                category="cache",
                keywords="cache, vintage, data freshness, download, epa bulk file, provenance",
                doc_llm=(
                    "One row describing the local cache of the EPA fueleconomy.gov bulk file that "
                    "`vehicles()` reads: status ('cached' or 'missing'), cache_path, cached, source_url, "
                    "size_bytes, sha256, vehicle_rows, min_model_year, max_model_year, "
                    "upstream_last_modified, downloaded_at, checked_at, max_age_days. It never downloads; "
                    "use it to report the data vintage. Call `refresh_cache()` to download or re-check."
                ),
                doc_md=(
                    "## cache_info()\n\n"
                    "One row about the cached EPA `vehicles.csv.zip`: path, size, checksum, row count, "
                    "model-year span and when it was downloaded and last checked. Never touches the network."
                ),
            ),
            "vgi.result_columns_schema": result_columns(CACHE_FIELDS),
            "vgi.source_url": SOURCE_PAGE,
        }
        examples = [
            FunctionExample(
                sql="SELECT cached, vehicle_rows, min_model_year, max_model_year FROM cars.main.cache_info()",
                description="Check whether the EPA file is cached and which model years it covers",
            ),
        ]

    @classmethod
    def process(cls, params: ProcessParams[NoArgs], state: None, out: OutputCollector) -> None:
        """Emit the single cache row."""
        out.emit(cache_row(endpoints_of(params), "cached"))
        out.finish()


@dataclass(slots=True, frozen=True, kw_only=True)
class RefreshArgs:
    """Arguments for ``refresh_cache(force := false)``."""

    force: Annotated[
        bool,
        Arg(
            "force",
            default=False,
            doc="TRUE downloads the file again even if upstream reports it unchanged; FALSE (default) sends a "
            "conditional request and keeps the cached copy when it is current.",
        ),
    ]


@bind_fixed_schema
@init_single_worker
class RefreshCache(TableFunctionGenerator[RefreshArgs, None]):
    """Download the EPA bulk file now, or re-check it, and report the cache."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = CACHE_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "refresh_cache"
        description = "Download or re-check the EPA vehicles bulk file now and report the cache"
        tags = {
            **object_tags(
                title="Refresh EPA Cache",
                category="cache",
                keywords="refresh, download, update data, cache, epa bulk file, conditional request",
                doc_llm=(
                    "Re-check fueleconomy.gov for a newer `vehicles.csv.zip` now (conditional request), "
                    "downloading it if it changed or is not cached, and return the same single row as "
                    "`cache_info()` with status 'downloaded', 'not_modified', 'stale' (re-check failed, "
                    "cached copy kept) or 'offline'. `force := true` downloads unconditionally. Normally "
                    "unnecessary: `vehicles()` downloads on first use and re-checks every MAX_AGE_DAYS."
                ),
                doc_md=(
                    "## refresh_cache(force := false)\n\n"
                    "Re-check (or with `force := true`, re-download) the EPA bulk file and report the "
                    "cache, like `cache_info()`."
                ),
            ),
            "vgi.result_columns_schema": result_columns(CACHE_FIELDS),
            "vgi.source_url": SOURCE_PAGE,
        }
        examples = [
            FunctionExample(
                sql="SELECT status, vehicle_rows, max_model_year FROM cars.main.refresh_cache()",
                description="Make sure the newest EPA vehicles file is cached",
            ),
        ]

    @classmethod
    def process(cls, params: ProcessParams[RefreshArgs], state: None, out: OutputCollector) -> None:
        """Refresh, then emit the cache row."""
        ep = endpoints_of(params)
        result = ensure_cache(ep, refresh=True, force=params.args.force)
        out.emit(cache_row(ep, result.status))
        out.finish()
