# Changelog

## 0.1.1 (2026-11-16)

- Move to Python 3.14: `requires-python >=3.14`, CI, the Docker image and lint tooling target 3.14.

## 0.1.0 (unreleased)

First release.

- `vehicles()` over the EPA fueleconomy.gov bulk file, with a verified local cache plus `cache_info()` and `refresh_cache()`.
- NHTSA vPIC: `makes()`, `models()`, `decode_vin()` (two overloads) and `decode_vins()`.
- NHTSA: `recalls()`, `complaints()`, `safety_ratings()`.

Targets: vgi-python 0.51 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
