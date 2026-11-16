"""Unit tests. The SQL-level tests live in test/sql and run through DuckDB."""

from __future__ import annotations

import io
import zipfile
from datetime import date
from pathlib import Path

import pyarrow as pa
import pytest

from cars_worker.common import (
    UpstreamError,
    endpoints_from_options,
    normalize_make,
    parse_date,
    to_int,
    to_rating,
)
from cars_worker.epa import (
    VEHICLE_SCHEMA,
    ensure_cache,
    filter_vehicles,
    load_vehicles,
    parse_vehicles,
    verify_zip,
)
from cars_worker.nhtsa import rating_row, split_components
from cars_worker.vpic import VIN_FIELDS, decode_column, vin_record

FIXTURES = Path(__file__).resolve().parent.parent / "test" / "fixtures"


@pytest.fixture(scope="module")
def vehicles() -> pa.Table:
    return parse_vehicles(FIXTURES / "vehicles.csv.zip")


def test_parse_vehicles_schema_and_rows(vehicles: pa.Table) -> None:
    assert vehicles.schema == VEHICLE_SCHEMA
    assert vehicles.num_rows == 34
    assert vehicles.column("id").null_count == 0
    # Sorted by model year, make, model.
    years = [y for y in vehicles.column("model_year").to_pylist() if y is not None]
    assert years == sorted(years)


def test_parse_vehicles_values(vehicles: pa.Table) -> None:
    rows = {r["id"]: r for r in vehicles.to_pylist()}
    tundra = rows[47130]
    assert tundra["make"] == "TOYOTA"
    assert tundra["base_model"] == "Tundra"
    assert (tundra["city_mpg"], tundra["highway_mpg"], tundra["combined_mpg"]) == (17, 22, 19)
    assert tundra["turbocharged"] is True
    assert tundra["start_stop"] is True
    assert tundra["fueleconomy_url"] == "https://www.fueleconomy.gov/feg/noframes/47130.shtml"
    assert tundra["fuel_economy_score"] == 4
    phev = rows[47274]
    assert phev["powertrain"] == "Plug-in Hybrid"
    assert phev["electric_range_miles"] == 33
    assert phev["combined_mpg_secondary"] == 77
    assert phev["combined_kwh_per_100mi"] == 44.0
    old = rows[1]
    assert old["fuel_economy_score"] is None
    assert old["city_mpg_secondary"] is None
    assert old["created_on"] == date(2013, 1, 1)
    ev = rows[41188]
    assert ev["powertrain"] == "EV"
    assert ev["cylinders"] is None
    assert ev["electric_range_miles"] is not None and ev["electric_range_miles"] > 200


def test_filter_vehicles(vehicles: pa.Table) -> None:
    assert filter_vehicles(vehicles, 2024, "toyota", "tundra").num_rows == 6
    assert filter_vehicles(vehicles, None, None, "Tundra 4WD").num_rows == 3
    assert filter_vehicles(vehicles, 1999, None, None).num_rows == 0
    assert filter_vehicles(vehicles, None, None, None).num_rows == 34


def test_verify_zip_rejects_bad_payloads() -> None:
    with pytest.raises(UpstreamError, match="not a zip"):
        verify_zip(b"nope")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("vehicles.csv", "a,b\n1,2\n")
    with pytest.raises(UpstreamError, match="expected columns"):
        verify_zip(buf.getvalue())
    verify_zip((FIXTURES / "vehicles.csv.zip").read_bytes())


def test_cache_download_and_reuse(tmp_path: Path, mock_api: str) -> None:
    ep = endpoints_from_options(
        {"epa_vehicles_url": f"{mock_api}/feg/epadata/vehicles.csv.zip", "cache_dir": str(tmp_path)}
    )
    first = ensure_cache(ep)
    assert first.status == "downloaded"
    assert len(first.meta["sha256"]) == 64
    assert ensure_cache(ep).status == "cached"
    assert ensure_cache(ep, refresh=True).status == "not_modified"
    assert ensure_cache(ep, force=True).status == "downloaded"
    assert load_vehicles(first.path).num_rows == 34
    # A failed re-check keeps serving the cached copy.
    broken = endpoints_from_options({"epa_vehicles_url": ep.epa_vehicles_url, "cache_dir": str(tmp_path)})
    object.__setattr__(broken, "epa_vehicles_url", "http://127.0.0.1:9/vehicles.csv.zip")
    # The cache was filled from a different URL, so it is not reused for this one.
    with pytest.raises(UpstreamError):
        ensure_cache(broken, refresh=True)


def test_cache_offline_without_copy(tmp_path: Path) -> None:
    ep = endpoints_from_options({"cache_dir": str(tmp_path), "offline": True})
    with pytest.raises(UpstreamError, match="OFFLINE"):
        ensure_cache(ep)


def test_endpoint_defaults_and_overrides() -> None:
    ep = endpoints_from_options({})
    assert ep.vpic_base_url == "https://vpic.nhtsa.dot.gov/api"
    assert ep.nhtsa_base_url == "https://api.nhtsa.gov"
    assert ep.max_age_days == 7
    ep = endpoints_from_options({"VPIC_BASE_URL": "http://x/api/", "max_age_days": 1})
    assert ep.vpic_base_url == "http://x/api"
    assert ep.max_age_days == 1


def test_value_helpers() -> None:
    assert parse_date("21/02/2024", "dmy") == date(2024, 2, 21)
    assert parse_date("06/18/2026", "mdy") == date(2026, 6, 18)
    assert parse_date("31/31/2024", "dmy") is None
    assert parse_date("", "dmy") is None
    assert to_int("6.0") == 6
    assert to_int("Not Applicable") is None
    assert to_rating("Not Rated") is None
    assert to_rating("5") == 5
    assert normalize_make(" Mercedes-Benz ") == "MERCEDES-BENZ"


def test_split_components() -> None:
    assert split_components("SERVICE BRAKES, HYDRAULIC,AIR BAGS") == ["SERVICE BRAKES, HYDRAULIC", "AIR BAGS"]
    assert split_components("") == []
    assert split_components(None) == []


def test_vin_record_mapping() -> None:
    result = {
        "Make": "HONDA",
        "Model": "Accord",
        "ModelYear": "2003",
        "DisplacementL": "2.998832712",
        "EngineCylinders": "6",
        "BodyClass": "Coupe",
        "ErrorCode": "0",
        "Trim": "",
        "BusType": "Not Applicable",
    }
    row = vin_record(result, "1HGCM82633A004352")
    assert set(row) == {f.name for f in VIN_FIELDS}
    assert row["model_year"] == 2003
    assert row["displacement_l"] == 2.999
    assert row["engine_cylinders"] == 6
    assert row["trim"] is None


def test_decode_column_batches_and_dedupes(mock_api: str) -> None:
    ep = endpoints_from_options({"vpic_base_url": f"{mock_api}/api"})
    vins = pa.array(["1HGCM82633A004352", None, "1hgcm82633a004352", "", "BAD;VIN"])
    out = decode_column(ep, vins, None)
    assert out[0] is not None and out[0]["make"] == "HONDA"
    assert out[1] is None
    assert out[2] == out[0]
    assert out[3] is None
    assert out[4] is not None and out[4]["error_code"] == "400"


def test_rating_row() -> None:
    row = rating_row(
        {"OverallRating": "Not Rated", "RolloverRating": "4", "RolloverPossibility": 0.12, "Make": "Toyota"}, 1
    )
    assert row["overall_rating"] is None
    assert row["rollover_rating"] == 4
    assert row["rollover_risk"] == 0.12
    assert row["make"] == "TOYOTA"
    unrated = rating_row({"RolloverRating": "Not Rated", "RolloverPossibility": 0.0}, 2)
    assert unrated["rollover_risk"] is None
