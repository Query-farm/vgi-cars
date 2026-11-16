"""Assemble the ``cars`` catalog and run the worker.

Run it:

    vgi-cars-worker                      # stdio: what DuckDB launches by default
    vgi-cars-worker --http --port 8000   # HTTP: for Docker / hosted deploys

Attach it from DuckDB:

    ATTACH 'cars' (TYPE vgi, LOCATION '/abs/path/to/.venv/bin/vgi-cars-worker');
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from vgi import Worker
from vgi.catalog import Catalog, Schema
from vgi.catalog.attach_option import AttachOption

from .common import DEFAULT_EPA_VEHICLES_URL, DEFAULT_MAX_AGE_DAYS, DEFAULT_NHTSA_BASE_URL, DEFAULT_VPIC_BASE_URL
from .epa import CacheInfo, RefreshCache, Vehicles
from .meta import (
    CATALOG_DESCRIPTION,
    CATALOG_NAME,
    CATALOG_TAGS,
    SCHEMA_CATEGORIES,
    SCHEMA_DOC_LLM,
    SCHEMA_DOC_MD,
    SOURCE_URL,
    keywords_array,
)
from .nhtsa import Complaints, Recalls, SafetyRatings
from .vpic import DecodeVin, DecodeVins, DecodeVinWithYear, Makes, Models

FUNCTIONS: list[type[Any]] = [
    Vehicles,
    CacheInfo,
    RefreshCache,
    Makes,
    Models,
    DecodeVin,
    DecodeVinWithYear,
    DecodeVins,
    Recalls,
    Complaints,
    SafetyRatings,
]


def _stamp_example_queries(functions: list[type[Any]]) -> list[dict[str, str]]:
    """Publish each function's examples as ``vgi.example_queries``; overloads share one SQL name.

    Returns the first example of each function, for the schema-level list.
    """
    by_name: dict[str, list[dict[str, str]]] = {}
    for fn in functions:
        bucket = by_name.setdefault(fn.Meta.name, [])
        for ex in getattr(fn.Meta, "examples", []):
            entry = {"description": ex.description, "sql": ex.sql}
            if entry not in bucket:
                bucket.append(entry)
    for fn in functions:
        fn.Meta.tags = {**fn.Meta.tags, "vgi.example_queries": json.dumps(by_name[fn.Meta.name])}
    return [entries[0] for entries in by_name.values() if entries]


_SCHEMA_EXAMPLES = _stamp_example_queries(FUNCTIONS)


class AttachOptions:
    """ATTACH options: upstream overrides (mirrors, fixtures) and the EPA cache settings."""

    cache_dir: Annotated[
        str | None,
        AttachOption(
            desc="Directory for the cached EPA vehicles.csv.zip (about 2 MB). Default: the per-user cache "
            "directory, e.g. ~/Library/Caches/vgi-cars or ~/.cache/vgi-cars."
        ),
    ] = None
    max_age_days: Annotated[
        int,
        AttachOption(desc="Re-check fueleconomy.gov for a newer bulk file once the cached copy is this many days old."),
    ] = DEFAULT_MAX_AGE_DAYS
    offline: Annotated[
        bool,
        AttachOption(desc="Never download the EPA file; use the cached copy only (fails if there is none)."),
    ] = False
    epa_vehicles_url: Annotated[
        str,
        AttachOption(desc="URL of the EPA bulk file vehicles.csv.zip; override to use a mirror."),
    ] = DEFAULT_EPA_VEHICLES_URL
    vpic_base_url: Annotated[
        str,
        AttachOption(desc="Base URL of the NHTSA vPIC API (VIN decoding, makes, models)."),
    ] = DEFAULT_VPIC_BASE_URL
    nhtsa_base_url: Annotated[
        str,
        AttachOption(desc="Base URL of api.nhtsa.gov (recalls, complaints, safety ratings)."),
    ] = DEFAULT_NHTSA_BASE_URL


class CarsWorker(Worker):
    """The worker process: the ``cars`` catalog with one ``main`` schema."""

    AttachOptions = AttachOptions

    catalog = Catalog(
        name=CATALOG_NAME,
        comment=CATALOG_DESCRIPTION,
        source_url=SOURCE_URL,
        tags=CATALOG_TAGS,
        schemas=[
            Schema(
                path=["main"],
                comment="Vehicle fuel economy, VIN decoding and safety functions from EPA and NHTSA.",
                functions=FUNCTIONS,
                tags={
                    "vgi.title": "Cars Functions",
                    "domain": "automotive",
                    "vgi.keywords": keywords_array(
                        "fuel economy, vin decode, recalls, complaints, crash ratings, makes, models"
                    ),
                    "vgi.doc_llm": SCHEMA_DOC_LLM,
                    "vgi.doc_md": SCHEMA_DOC_MD,
                    "vgi.categories": json.dumps(SCHEMA_CATEGORIES),
                    "vgi.example_queries": json.dumps(_SCHEMA_EXAMPLES),
                },
            )
        ],
    )


def main() -> None:
    """Console-script entry point: parse ``--http`` / ``--unix`` / logging flags and serve."""
    CarsWorker.main()


if __name__ == "__main__":
    main()
