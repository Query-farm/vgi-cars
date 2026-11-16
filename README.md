<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Query US Vehicle Specs, Fuel Economy and Safety Data in DuckDB

> **vgi-cars** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-cars/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-cars/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

Keyless US-government vehicle data as SQL: EPA fuel economy and powertrain specifications for
every light-duty vehicle sold in the US since model year 1984, NHTSA VIN decoding, the makes and
models NHTSA knows about, safety recalls, owner complaints and NCAP 5-Star crash ratings. No API
key, no account. Every result shares two join keys, `model_year` (`INTEGER`) and `make`
(upper-cased, e.g. `'TOYOTA'`), so you can combine the sources in one query.

```sql
-- Fuel economy and crash ratings for the 2020 Honda Accord, side by side.
SELECT v.model, v.combined_mpg, s.vehicle_description, s.overall_rating
FROM cars.main.vehicles(year := 2020, make := 'honda', model := 'accord') v
JOIN cars.main.safety_ratings('honda', 'accord', 2020) s
  ON s.model_year = v.model_year AND s.make = v.make AND s.model = upper(v.base_model)
ORDER BY v.combined_mpg DESC;
```

## Quick start

**DuckDB 2.0:**

```sql
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;

ATTACH 'cars' (TYPE vgi,
  LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-cars@v0.1.1 vgi-cars-worker');

SELECT model, combined_mpg FROM cars.main.vehicles(year := 2024, make := 'toyota', model := 'prius');
```

**Haybarn 2.0** (`uvx haybarn-cli`): use `INSTALL vgi FROM community; LOAD vgi;` and the same
`ATTACH`.

**From a checkout:** run `make build`, then use
`LOCATION '/abs/path/to/vgi-cars/.venv/bin/vgi-cars-worker'`.

## Functions

All functions live in `cars.main`.

| Function | Kind | Source | What it returns |
|---|---|---|---|
| `vehicles(year := , make := , model := )` | table | EPA bulk file (cached) | One row per EPA vehicle configuration since 1984: MPG/MPGe, CO2, fuel cost, EV range, engine, transmission, drive, class, `fueleconomy_url` |
| `cache_info()` | table | local cache | Where the EPA file is cached, its size, SHA-256, row count, model-year span, download/check times (never touches the network) |
| `refresh_cache(force := false)` | table | EPA | Re-check (conditional request) or re-download the EPA file now; same row as `cache_info()` plus what happened |
| `makes(vehicle_type := )` | table | NHTSA vPIC | Registered makes, optionally for `'car'`, `'truck'`, `'mpv'`, `'motorcycle'`, `'bus'`, `'trailer'` |
| `models(make, year := , vehicle_type := )` | table | NHTSA vPIC | Models of a make, optionally for one model year and type |
| `decode_vin(vin)` / `decode_vin(vin, model_year)` | scalar | NHTSA vPIC | `STRUCT` of make, model, year, trim, body, engine, transmission, plant and vPIC error code |
| `decode_vins((SELECT vin [, model_year] ...))` | table in-out | NHTSA vPIC | The same fields as flat columns, one row per input row |
| `recalls(make, model, year)` | table | NHTSA | Recall campaigns: component, summary, consequence, remedy, dates, park-it flags, nhtsa.gov link |
| `complaints(make, model, year)` | table | NHTSA | Owner complaints: dates, crash/fire flags, injuries, deaths, components (`VARCHAR[]`), narrative |
| `safety_ratings(make, model, year)` | table | NHTSA NCAP | 5-Star ratings per tested variant: overall, frontal, side, pole, rollover and risk, ADAS availability |

More examples:

```sql
-- Longest-range EVs per model year since 2020.
SELECT model_year, make, model, electric_range_miles
FROM cars.main.vehicles() WHERE powertrain = 'EV' AND model_year >= 2020
QUALIFY row_number() OVER (PARTITION BY model_year ORDER BY electric_range_miles DESC) = 1
ORDER BY model_year;

-- Decode a fleet table and count vehicles by make and fuel.
SELECT make, primary_fuel, count(*) FROM cars.main.decode_vins((SELECT vin FROM fleet)) GROUP BY ALL;

-- Which components do owners complain about most?
SELECT component, count(*) AS n
FROM (SELECT unnest(components) AS component FROM cars.main.complaints('honda', 'accord', 2003))
GROUP BY component ORDER BY n DESC LIMIT 5;
```

Notes:

- `decode_vin(vin, model_year)` takes the model year as a second argument, a constant or a
  column, so `SELECT decode_vin(vin, model_year) FROM fleet` decodes each row with its own
  year. Scalars have no named arguments in DuckDB.
- NHTSA model names are usually the base model (`'ACCORD'`, `'F-150'`), not EPA's
  drive/trim names (`'Accord'`, `'F150 Pickup 2WD'`); join on `upper(base_model)` where they agree.
- For EVs, EPA's `city_mpg`/`highway_mpg`/`combined_mpg` are MPGe. `-1` sentinels and empty
  strings from upstream become `NULL`.
- **No colors.** Vehicle colors are not part of any free official US data source (VINs do not
  encode them, and EPA/NHTSA do not publish them), so this worker does not offer them.

## What pushes down

- `vehicles()`: the named arguments `year`, `make` and `model` filter rows inside the worker
  before anything is sent to DuckDB; only the columns your query uses are sent (projection
  pushdown). Other `WHERE` clauses are evaluated by DuckDB over the in-memory 50,000-row table.
- `recalls`, `complaints`, `safety_ratings`: make, model and year are the upstream query; NHTSA has
  no further server-side filters.
- `makes(vehicle_type :=)` and `models(make, year :=, vehicle_type :=)` choose the matching vPIC
  endpoint.
- `decode_vin` / `decode_vins`: VINs are normalized (trimmed, upper-cased), de-duplicated within
  each Arrow batch, memoized per worker process, and sent 50 per `DecodeVINValuesBatch` POST.

## The EPA bulk file and its cache

`vehicles()` reads EPA's [`vehicles.csv.zip`](https://www.fueleconomy.gov/feg/epadata/vehicles.csv.zip)
(about 2.2 MB zipped, 50,000 rows). It is downloaded on first use, never at import or `ATTACH`, into
the per-user cache directory (`~/Library/Caches/vgi-cars` on macOS, `~/.cache/vgi-cars` on Linux),
checked (valid zip, CRC, expected CSV header), written atomically and parsed once per worker process.
After `MAX_AGE_DAYS` (default 7) it is re-checked with a conditional request. If the re-check fails,
the worker keeps using the cached copy.

ATTACH options:

| Option | Default | Meaning |
|---|---|---|
| `CACHE_DIR` | per-user cache dir | Where `vehicles.csv.zip` and its metadata are kept |
| `MAX_AGE_DAYS` | `7` | Re-check interval for the EPA file |
| `OFFLINE` | `false` | Never download; use the cached copy only |
| `EPA_VEHICLES_URL` | fueleconomy.gov | Bulk-file URL (mirror) |
| `VPIC_BASE_URL` | `https://vpic.nhtsa.dot.gov/api` | vPIC base URL |
| `NHTSA_BASE_URL` | `https://api.nhtsa.gov` | Recalls/complaints/ratings base URL |

```sql
ATTACH 'cars' (TYPE vgi, LOCATION '...', CACHE_DIR '/data/cars-cache', MAX_AGE_DAYS 30);
SELECT cached, vehicle_rows, max_model_year, downloaded_at FROM cars.main.cache_info();
```

The worker sends a `vgi-cars/0.1 (+https://query.farm)` User-Agent, sets timeouts on every request,
and retries HTTP 429/5xx with backoff, honouring `Retry-After`. NHTSA answers "no such vehicle" with
HTTP 400 and an empty list; the functions return zero rows for those.

## Testing

```bash
make test-unit                 # Python unit tests (offline, fixture server)
make test-sql                  # test/sql/*.test through Haybarn's SQL runner (stdio)
make test-sql TRANSPORT=http   # ...over HTTP (also: launch)
make check                     # everything CI runs, including vgi-lint
make live-check                # opt-in: a few queries against the live EPA/NHTSA services
```

The SQL and unit tests run offline against `test/mock_api/server.py`, which serves recorded
upstream responses from `test/fixtures` on the real services' paths (selected with the ATTACH
options above). It rate-limits every path once so the retry path is always tested. The metadata
check (`make lint-explain`) executes the function examples against the live services; each
execution downloads the 2.2 MB EPA file at most once and makes a handful of NHTSA requests.

## Data terms

The code is under the license below; the data is not. The data comes from US federal agencies:
EPA ([fueleconomy.gov](https://www.fueleconomy.gov/feg/ws/index.shtml)) and NHTSA
([vPIC](https://vpic.nhtsa.dot.gov/api/), [datasets and APIs](https://www.nhtsa.gov/nhtsa-datasets-and-apis)).
US-government works are in the public domain in the United States. The agencies provide
them as is: complaints are unverified owner reports, and a missing vPIC value means NHTSA has no data
(not that a feature is absent). This project is not affiliated with or endorsed by EPA, DOE or NHTSA.
See [DATA-SOURCES.md](DATA-SOURCES.md) for the test fixtures in this repository.

## Support

Issues: <https://github.com/Query-farm/vgi-cars/issues>.

## Authorship & License

Copyright 2026 Query Farm LLC - https://query.farm

Written by [Query.Farm](https://query.farm) and released under the
[BSD Zero Clause License](LICENSE) (0BSD).
