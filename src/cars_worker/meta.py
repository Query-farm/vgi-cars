"""Discovery metadata for the ``cars`` catalog.

DuckDB, coding agents and the ``vgi-lint`` metadata checker all read these tags.
Catalog-level tags live in ``CATALOG_TAGS``; every function carries
``object_tags(...)`` with a category from ``SCHEMA_CATEGORIES``.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

CATALOG_NAME = "cars"

SOURCE_URL = "https://github.com/Query-farm/vgi-cars"

SCHEMA_CATEGORIES = [
    {
        "name": "fuel_economy",
        "title": "Fuel Economy",
        "description": "EPA fuel economy, emissions and powertrain specifications from fueleconomy.gov.",
    },
    {
        "name": "vin",
        "title": "VIN Decoding and Model Lists",
        "description": "NHTSA vPIC: VIN decoding and the registered makes and models.",
    },
    {
        "name": "safety",
        "title": "Safety",
        "description": "NHTSA recalls, owner complaints and NCAP 5-Star crash ratings.",
    },
    {
        "name": "cache",
        "title": "Cache and Provenance",
        "description": "The local cache of the EPA bulk file: vintage, size and refresh.",
    },
]


def keywords_array(keywords: str) -> str:
    """Turn ``"a, b, c"`` into the JSON array string ``vgi.keywords`` requires."""
    return json.dumps([k.strip() for k in keywords.split(",") if k.strip()])


def object_tags(*, title: str, category: str, keywords: str, doc_llm: str, doc_md: str) -> dict[str, str]:
    """Build the standard per-object discovery tags."""
    if category not in {c["name"] for c in SCHEMA_CATEGORIES}:
        raise ValueError(f"unknown category {category!r}; add it to SCHEMA_CATEGORIES")
    return {
        "vgi.title": title,
        "vgi.category": category,
        "vgi.keywords": keywords_array(keywords),
        "vgi.doc_llm": doc_llm,
        "vgi.doc_md": doc_md,
    }


def column(name: str, type_: pa.DataType, comment: str, *, nullable: bool = True) -> pa.Field[Any]:
    """Build an Arrow field whose ``comment`` shows up in DuckDB's ``DESCRIBE`` and agent tools."""
    return pa.field(name, type_, nullable=nullable, metadata={b"comment": comment.encode()})


_DUCKDB_TYPES = {
    pa.int32(): "INTEGER",
    pa.int64(): "BIGINT",
    pa.float64(): "DOUBLE",
    pa.string(): "VARCHAR",
    pa.bool_(): "BOOLEAN",
    pa.date32(): "DATE",
    pa.timestamp("us", tz="UTC"): "TIMESTAMP WITH TIME ZONE",
    pa.list_(pa.string()): "VARCHAR[]",
}


def _short(text: str) -> str:
    """Return the first clause of a column comment (before any parenthesis or semicolon)."""
    for mark in (" (", "; "):
        if mark in text:
            text = text.split(mark, 1)[0]
    return text.rstrip(".") + "."


def result_columns(fields: list[pa.Field[Any]]) -> str:
    """Build the ``vgi.result_columns_schema`` JSON for a table function's output fields."""
    return json.dumps(
        [
            {
                "name": f.name,
                "type": _DUCKDB_TYPES[f.type],
                # Agent tools bound tag size; the full text stays in each column's comment.
                "description": _short((f.metadata or {}).get(b"comment", b"").decode()),
            }
            for f in fields
        ]
    )


CATALOG_DESCRIPTION = (
    "US vehicle specifications, fuel economy and safety from keyless US-government sources: EPA "
    "fueleconomy.gov MPG and powertrain data since 1984, NHTSA vPIC VIN decoding, recalls, owner "
    "complaints and NCAP 5-Star crash ratings."
)

CATALOG_DOC_LLM = (
    "Keyless US-government vehicle data as SQL. EPA fuel economy (fueleconomy.gov) gives one row per "
    "vehicle configuration sold in the US since model year 1984: MPG/MPGe, CO2, fuel cost, EV range, "
    "engine, transmission, drive and size class, read from a ~2 MB bulk file that is cached locally and "
    "re-checked weekly. NHTSA vPIC decodes VINs (single values or whole columns, 50 VINs per request) "
    "and lists registered makes and models. NHTSA's public APIs give safety recalls, owner complaints "
    "and NCAP 5-Star crash ratings for a given make, model and model year. Join across sources on "
    "model_year (`INTEGER`) and make (always upper-cased, e.g. 'TOYOTA'); NHTSA model names are usually "
    "the EPA base_model upper-cased. Vehicle colors are not covered: no free official source exists. "
    "No API key is needed; all data is US-government public domain."
)

CATALOG_DOC_MD = (
    "# Cars: US vehicle specs, fuel economy and safety\n\n"
    "**Keyless US-government vehicle data in DuckDB.** Look up EPA fuel economy and specifications for "
    "every US light-duty vehicle since 1984, decode VINs, and check recalls, owner complaints and crash "
    "ratings, then join them in SQL.\n\n"
    "Sources (all US-government works in the public domain, no key needed):\n\n"
    "- [EPA fueleconomy.gov web services and bulk data](https://www.fueleconomy.gov/feg/ws/index.shtml)\n"
    "- [NHTSA vPIC API](https://vpic.nhtsa.dot.gov/api/) for VIN decoding, makes and models\n"
    "- [NHTSA datasets and APIs](https://www.nhtsa.gov/nhtsa-datasets-and-apis) for recalls, complaints "
    "and 5-Star Safety Ratings\n\n"
    "Join across sources on `model_year` and the upper-cased `make`. Vehicle colors are not included: "
    "there is no free official source for them."
)

SCHEMA_DOC_LLM = (
    "The `main` schema holds every cars function. EPA fuel economy comes from a locally cached bulk "
    "file, so filtering and aggregating it is fast and offline after the first download. NHTSA "
    "functions call the live APIs per make, model and model year; they take those three values "
    "positionally and return zero rows for unknown vehicles. VIN decoding is available as a scalar "
    "returning a `STRUCT` for use inside projections, and as a table in-out function for decoding a "
    "query's VIN column into flat columns. Every result uses model_year `INTEGER` and upper-cased make "
    "as join keys."
)

SCHEMA_DOC_MD = (
    "## main\n\n"
    "Vehicle data functions grouped by source:\n\n"
    "- **Fuel economy** from EPA fueleconomy.gov, served from a local cache of the bulk file.\n"
    "- **VIN decoding and model lists** from NHTSA vPIC.\n"
    "- **Safety** (recalls, complaints, crash ratings) from NHTSA, per make, model and model year.\n"
    "- **Cache** functions report and refresh the EPA file's vintage."
)

AGENT_TEST_TASKS = [
    {
        "name": "best_mpg_tundra_2024",
        "prompt": (
            "Using the cars worker, find the highest EPA combined MPG of any 2024 Toyota Tundra configuration. "
            "Save it as a temporary table tundra_best with one row and one column best_mpg."
        ),
    },
    {
        "name": "ev_range_2019_tesla",
        "prompt": (
            "How many 2019 Tesla Model 3 configurations does the EPA list, and what is the longest electric "
            "range among them? Save a temporary table tesla_2019 with one row and columns configurations "
            "and longest_range_miles."
        ),
    },
    {
        "name": "decode_accord_vin",
        "prompt": (
            "What make, model and model year is VIN 1HGCM82633A004352? Save a temporary table vin_answer "
            "with one row and columns make, model and model_year."
        ),
    },
    {
        "name": "accord_airbag_recalls",
        "prompt": (
            "How many NHTSA recall campaigns for the 2003 Honda Accord involve air bags? Save the number as a "
            "temporary table airbag_recalls with one row and one column recalls."
        ),
    },
    {
        "name": "accord_2020_overall_rating",
        "prompt": (
            "What overall NCAP star rating did NHTSA give the 2020 Honda Accord? Save it as a temporary table "
            "accord_rating with one row and one column stars."
        ),
    },
    {
        "name": "accord_fire_complaints",
        "prompt": (
            "How many owner complaints to NHTSA about the 2003 Honda Accord report a fire? Save the number as a "
            "temporary table fire_complaints with one row and one column complaints."
        ),
    },
    {
        "name": "fleet_vin_years",
        "prompt": (
            "Decode these VINs and report each one's model year: 1HGCM82633A004352, 5YJ3E1EA7KF317000, "
            "1FTFW1ET5DFC10312. Save a temporary table fleet_years with columns vin and model_year."
        ),
    },
    {
        "name": "car_make_count",
        "prompt": (
            "How many makes are registered with NHTSA vPIC as passenger-car makes? Save the number as a "
            "temporary table car_makes with one row and one column makes."
        ),
    },
    {
        "name": "honda_2020_models",
        "prompt": (
            "How many distinct 2020 Honda (make HONDA) models does NHTSA vPIC list? Save the number as a "
            "temporary table honda_models with one row and one column models."
        ),
    },
    {
        "name": "epa_data_vintage",
        "prompt": (
            "Make sure the local copy of the EPA fuel economy file is current, then report the latest model "
            "year it covers. Save it as a temporary table epa_vintage with one row and one column max_model_year."
        ),
    },
]

CATALOG_TAGS = {
    "vgi.title": "US Vehicle Data",
    "vgi.subtitle": "EPA fuel economy, NHTSA VIN decoding, recalls, complaints and crash ratings for US vehicles.",
    "vgi.doc_llm": CATALOG_DOC_LLM,
    "vgi.doc_md": CATALOG_DOC_MD,
    "vgi.source_url": SOURCE_URL,
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.license": "0BSD",
    "vgi.author": "Query.Farm",
    "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
    "vgi.keywords": keywords_array(
        "cars, vehicles, fuel economy, mpg, epa, nhtsa, vin decoder, recalls, crash ratings, complaints, automotive"
    ),
    "vgi.support_contact": f"{SOURCE_URL}/issues",
    "vgi.support_policy_url": f"{SOURCE_URL}#support",
    "vgi.agent_test_tasks": json.dumps(AGENT_TEST_TASKS),
}
