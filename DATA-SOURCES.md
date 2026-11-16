# Data sources

The worker bundles **no** production data. `vehicles()` downloads EPA's bulk file at runtime into
a per-user cache (see README, "The EPA bulk file and its cache"); every other function calls the
live NHTSA APIs.

The repository does contain small **test fixtures** in `test/fixtures/`: trimmed copies of real
upstream responses, recorded on 2026-11-15 with a handful of polite requests
(User-Agent `vgi-cars/0.1 (+https://query.farm)`). They are used only by the offline tests.

| Source | Licence | Used for |
|---|---|---|
| EPA / DOE fueleconomy.gov bulk data, <https://www.fueleconomy.gov/feg/epadata/vehicles.csv.zip> (docs: <https://www.fueleconomy.gov/feg/ws/index.shtml>) | US-government work, public domain (17 U.S.C. § 105) | `vehicles.csv.zip`: 34 of the ~50,600 rows of the 2026-11-04 file (full header kept) |
| NHTSA vPIC API, <https://vpic.nhtsa.dot.gov/api/> | US-government work, public domain | `vpic_*.json`: GetAllMakes and GetMakesForVehicleType/car (trimmed), GetModelsForMakeYear honda/2020, GetModelsForMake honda (trimmed), DecodeVINValuesBatch for 4 VINs plus one with a model year |
| NHTSA recalls API, <https://api.nhtsa.gov/recalls/recallsByVehicle> | US-government work, public domain | `recalls_*.json` (complete responses) |
| NHTSA complaints API, <https://api.nhtsa.gov/complaints/complaintsByVehicle> | US-government work, public domain | `complaints_honda_accord_2003.json`: 6 of 2,013 complaints |
| NHTSA 5-Star Safety Ratings API, <https://api.nhtsa.gov/SafetyRatings> | US-government work, public domain | `safety_*.json` (complete responses) |

The full EPA file retrieved on 2026-11-15 (`Last-Modified: Wed, 04 Nov 2026 14:31:23 GMT`) was
2,228,192 bytes, SHA-256 `1d553896a77898f6cf47128281337af268df053c90dc1365891c7c8abff82c82`.
It is not in the repository.

## Fixture checksums (SHA-256)

```
334f07d927385ea3c9a1e8d9f51bc2e5859f6424ac559452ac47d1994b669e73  complaints_honda_accord_2003.json
50cfcb82f6017474f7bfc8661892a0a070d0f772bdc8190e2ab84b93ce211f78  recalls_honda_accord_2003.json
e0bffcb3f57abb6684267a994b461f24358d49b95d2bab2c9a9af1624fb7634e  recalls_toyota_tundra_2024.json
5ed757459645dff885fd98d08f1105490d50d35c6be150b81a118201b5972fc3  safety_list_2020_honda_accord.json
e7e1c054ddb995d69d86fa0bdccd11dfd8526f1baf9910a253a0281fb0b6ee0a  safety_list_2024_toyota_tundra.json
cee1d5ad3f68b8988988f0527d3b7244b9d139ebf5fa7d1136710dbb993564ca  safety_vehicle_14818.json
f61ef6b829f9410921b7bdf26dd50d592d429ae5f5299d6bc4607c0fe3e4354c  safety_vehicle_19454.json
4a7f0c946ceca32bdcb24c7a18065496af496c9f5ee62fb11adb424b9355d653  safety_vehicle_19455.json
6e9123f6bf08b1ffb5844a49aebc8b67c5d780207e27b788ea9941c7e949422f  safety_vehicle_19456.json
d18c5c2b7775e6712b032ff338be0066bc39b036845a9470cde6bab41ea62778  safety_vehicle_19457.json
077f43d975ed90808c69096a04e3628a94a4372204a29c8b5196523ca86bca65  vehicles.csv.zip
000a72eb1db814583ca11bb222318e2e1e4a39a0da8348ccbda7b1b478bbb9de  vpic_all_makes.json
fde721adb071d0201367413b73007cd24c34e92fb0d0ce8e5d501d85d31b89bd  vpic_decode.json
9ee557eaebbfb8a13e503bf93bb0587c7617a32e210014b14e16c1373ddb8bce  vpic_makes_car.json
68721a0c4323926e371d749ffe9c21181e5102669c1687dcfaa622774fbe4f2b  vpic_models_honda_2020.json
4b9d1d37747455153cc1857c5eea1ab7afce452182a1bebc52a296f022e46b97  vpic_models_honda.json
```

Regenerate the checksums with `shasum -a 256 test/fixtures/*` after re-recording.
