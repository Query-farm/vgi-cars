#!/usr/bin/env python3
"""Offline stand-in for fueleconomy.gov, vpic.nhtsa.dot.gov and api.nhtsa.gov.

Serves the recorded responses in ``test/fixtures`` on the same paths as the real
services, so the worker is pointed at it with ATTACH options::

    EPA_VEHICLES_URL '<base>/feg/epadata/vehicles.csv.zip'
    VPIC_BASE_URL    '<base>/api'
    NHTSA_BASE_URL   '<base>'

Unknown vehicles get the same answer the real APIs give (HTTP 400 with an empty
result list on api.nhtsa.gov, an empty list on vPIC).

Behaviour switches
------------------
* ``MOCK_API_FLAKY=1``: the first request for each distinct path returns ``429``
  with ``Retry-After: 0``, exercising the client's retry path.
* The bulk file is served with an ``ETag`` and honours ``If-None-Match`` (304).

Usage::

    python3 test/mock_api/server.py            # prints PORT:<n> once listening
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
ZIP_ETAG = '"fixture-vehicles-1"'

_flaky_seen: set[str] = set()
_flaky_lock = threading.Lock()


def _fixture(name: str) -> object:
    return json.loads((FIXTURES / name).read_text())


def _slug(*parts: str) -> str:
    return "_".join(re.sub(r"[^a-z0-9]+", "-", p.strip().lower()).strip("-") for p in parts)


class Handler(BaseHTTPRequestHandler):
    """Route the real services' paths to fixture files."""

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - stdlib signature
        """Log to stderr only, so stdout carries nothing except the ``PORT:`` line."""
        sys.stderr.write("mock_api: " + (format % args) + "\n")

    def _send(self, status: int, body: object | bytes, headers: dict[str, str] | None = None) -> None:
        payload = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/zip" if isinstance(body, bytes) else "application/json")
        self.send_header("Content-Length", str(len(payload)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(payload)

    def _flaky(self) -> bool:
        if os.environ.get("MOCK_API_FLAKY") != "1":
            return False
        with _flaky_lock:
            if self.path in _flaky_seen:
                return False
            _flaky_seen.add(self.path)
        self._send(429, {"error": "slow down"}, {"Retry-After": "0"})
        return True

    def do_POST(self) -> None:
        """vPIC DecodeVINValuesBatch."""
        if self._flaky():
            return
        url = urlparse(self.path)
        if url.path.rstrip("/") != "/api/vehicles/DecodeVINValuesBatch":
            self._send(404, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length") or 0)
        form = parse_qs(self.rfile.read(length).decode())
        recorded = _fixture("vpic_decode.json")
        assert isinstance(recorded, dict)
        results = []
        for item in form.get("data", [""])[0].split(";"):
            vin = item.split(",")[0].strip().upper()
            hit = recorded["by_vin_year"].get(item.strip().upper()) or recorded["by_vin"].get(vin)
            if hit is None:
                hit = {**recorded["by_vin"]["BADVIN"], "VIN": vin}
            results.append(hit)
        self._send(200, {"Count": len(results), "Message": recorded["Message"], "Results": results})

    def do_GET(self) -> None:
        """Route GET requests."""
        url = urlparse(self.path)
        path = unquote(url.path)
        query = {k: v[0] for k, v in parse_qs(url.query).items()}
        if path == "/health":
            self._send(200, {"status": "ok"})
            return
        if self._flaky():
            return
        if path == "/feg/epadata/vehicles.csv.zip":
            if self.headers.get("If-None-Match") == ZIP_ETAG:
                self.send_response(304)
                self.end_headers()
                return
            self._send(200, (FIXTURES / "vehicles.csv.zip").read_bytes(), {"ETag": ZIP_ETAG})
            return
        if path == "/feg/epadata/broken.csv.zip":
            self._send(200, b"this is not a zip file")
            return
        if path == "/api/vehicles/GetAllMakes":
            self._send(200, _fixture("vpic_all_makes.json"))
            return
        match = re.fullmatch(r"/api/vehicles/GetMakesForVehicleType/(.+)", path)
        if match:
            name = f"vpic_makes_{_slug(match.group(1))}.json"
            self._send(200, _fixture(name) if (FIXTURES / name).exists() else {"Count": 0, "Results": []})
            return
        match = re.fullmatch(r"/api/vehicles/GetModelsForMake/(.+)", path)
        if match:
            name = f"vpic_models_{_slug(match.group(1))}.json"
            self._send(200, _fixture(name) if (FIXTURES / name).exists() else {"Count": 0, "Results": []})
            return
        match = re.fullmatch(r"/api/vehicles/GetModelsForMakeYear/make/(.+)/modelyear/(\d+)(?:/vehicletype/.+)?", path)
        if match:
            name = f"vpic_models_{_slug(match.group(1), match.group(2))}.json"
            self._send(200, _fixture(name) if (FIXTURES / name).exists() else {"Count": 0, "Results": []})
            return
        if path in ("/recalls/recallsByVehicle", "/complaints/complaintsByVehicle"):
            kind = "recalls" if path.startswith("/recalls") else "complaints"
            name = f"{kind}_{_slug(query.get('make', ''), query.get('model', ''), query.get('modelYear', ''))}.json"
            if (FIXTURES / name).exists():
                self._send(200, _fixture(name))
            else:
                key = "Count" if kind == "recalls" else "count"
                self._send(400, {key: 0, "message": "Results returned successfully", "results": []})
            return
        match = re.fullmatch(r"/SafetyRatings/modelyear/(\d+)/make/(.+)/model/(.+)", path)
        if match:
            name = f"safety_list_{_slug(match.group(1), match.group(2), match.group(3))}.json"
            self._send(200, _fixture(name) if (FIXTURES / name).exists() else {"Count": 0, "Results": []})
            return
        match = re.fullmatch(r"/SafetyRatings/VehicleId/(\d+)", path)
        if match:
            name = f"safety_vehicle_{match.group(1)}.json"
            self._send(200, _fixture(name) if (FIXTURES / name).exists() else {"Count": 0, "Results": []})
            return
        self._send(404, {"error": f"no fixture route for {path}"})


def main() -> None:
    """Start the server and print ``PORT:<n>`` once it is listening."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"PORT:{server.server_address[1]}", flush=True)
    with contextlib.suppress(KeyboardInterrupt):
        server.serve_forever()


if __name__ == "__main__":
    main()
