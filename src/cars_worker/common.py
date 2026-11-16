"""Shared plumbing: upstream endpoints (ATTACH options), polite HTTP, and value helpers.

Every upstream URL can be overridden at ``ATTACH`` time, which is how the test
suite points the worker at recorded fixtures instead of the live services::

    ATTACH 'cars' (TYPE vgi, LOCATION '...',
                   VPIC_BASE_URL 'http://127.0.0.1:8765/api',
                   NHTSA_BASE_URL 'http://127.0.0.1:8765',
                   EPA_VEHICLES_URL 'http://127.0.0.1:8765/feg/epadata/vehicles.csv.zip',
                   CACHE_DIR '/tmp/cars-cache');
"""

from __future__ import annotations

import json
import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import platformdirs

log = logging.getLogger(__name__)

USER_AGENT = "vgi-cars/0.1 (+https://query.farm)"
TIMEOUT_SECONDS = 60.0
MAX_ATTEMPTS = 4

DEFAULT_EPA_VEHICLES_URL = "https://www.fueleconomy.gov/feg/epadata/vehicles.csv.zip"
DEFAULT_VPIC_BASE_URL = "https://vpic.nhtsa.dot.gov/api"
DEFAULT_NHTSA_BASE_URL = "https://api.nhtsa.gov"
DEFAULT_MAX_AGE_DAYS = 7


class UpstreamError(RuntimeError):
    """An upstream service returned an error we will not retry."""


@dataclass(frozen=True, slots=True)
class Endpoints:
    """Where the worker reads from, resolved from the ATTACH options."""

    epa_vehicles_url: str = DEFAULT_EPA_VEHICLES_URL
    vpic_base_url: str = DEFAULT_VPIC_BASE_URL
    nhtsa_base_url: str = DEFAULT_NHTSA_BASE_URL
    cache_dir: Path = Path(platformdirs.user_cache_dir("vgi-cars", appauthor=False))
    max_age_days: int = DEFAULT_MAX_AGE_DAYS
    offline: bool = False


def endpoints_from_options(options: dict[str, Any] | None) -> Endpoints:
    """Build `Endpoints` from ATTACH options (names lower-cased; absent means default)."""
    opts = {k.lower(): v for k, v in (options or {}).items() if v is not None}
    defaults = Endpoints()
    max_age = opts.get("max_age_days", defaults.max_age_days)
    return Endpoints(
        epa_vehicles_url=str(opts.get("epa_vehicles_url") or defaults.epa_vehicles_url),
        vpic_base_url=str(opts.get("vpic_base_url") or defaults.vpic_base_url).rstrip("/"),
        nhtsa_base_url=str(opts.get("nhtsa_base_url") or defaults.nhtsa_base_url).rstrip("/"),
        cache_dir=Path(str(opts["cache_dir"])).expanduser() if opts.get("cache_dir") else defaults.cache_dir,
        max_age_days=int(max_age),
        offline=bool(opts.get("offline", False)),
    )


def endpoints_of(params: Any) -> Endpoints:
    """Return the endpoints for a call, from its ATTACH options when it has any."""
    try:
        options = dict(params.attach_options or {})
    except Exception:  # not invoked through an ATTACHed catalog
        options = {}
    return endpoints_from_options(options)


def _request(
    url: str,
    *,
    data: bytes | None = None,
    headers: dict[str, str] | None = None,
    empty_on_400: bool = False,
) -> bytes:
    """Fetch ``url`` (POST when ``data`` is given), retrying 429/5xx/network errors with backoff.

    Args:
        url: Absolute URL.
        data: Form body for a POST.
        headers: Extra request headers.
        empty_on_400: NHTSA's api.nhtsa.gov answers "no such vehicle" with HTTP 400 and a
            normal empty JSON body; return that body instead of raising.

    Returns:
        The response body.

    Raises:
        UpstreamError: A non-retryable status, or retries exhausted.

    """
    all_headers = {"User-Agent": USER_AGENT, "Accept": "application/json", **(headers or {})}
    for attempt in range(1, MAX_ATTEMPTS + 1):
        request = urllib.request.Request(url, data=data, headers=all_headers, method="POST" if data else "GET")
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
                body: bytes = response.read()
                return body
        except urllib.error.HTTPError as err:
            body = err.read()
            if err.code == 400 and empty_on_400:
                return body
            retryable = err.code == 429 or err.code >= 500
            if not retryable or attempt == MAX_ATTEMPTS:
                snippet = body[:300].decode(errors="replace")
                raise UpstreamError(f"{request.get_method()} {url} failed with HTTP {err.code}: {snippet}") from err
            retry_after = err.headers.get("Retry-After")
            delay = float(retry_after) if retry_after and retry_after.isdigit() else 0.5 * 2**attempt
            log.warning("HTTP %s from %s; retrying in %.1fs", err.code, url, delay)
            time.sleep(min(delay, 30.0))
        except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
            if attempt == MAX_ATTEMPTS:
                reason = getattr(err, "reason", err)
                raise UpstreamError(f"{request.get_method()} {url} failed: {reason}") from err
            time.sleep(0.5 * 2**attempt)
    raise UpstreamError(f"{url}: retries exhausted")


def get_json(url: str, *, empty_on_400: bool = False) -> Any:
    """GET ``url`` and decode its JSON body."""
    return json.loads(_request(url, empty_on_400=empty_on_400) or b"{}")


def post_form_json(url: str, form: dict[str, str]) -> Any:
    """POST ``form`` (urlencoded) to ``url`` and decode its JSON body."""
    data = urllib.parse.urlencode(form).encode()
    return json.loads(_request(url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"}))


def path_segment(value: str) -> str:
    """Percent-encode one URL path segment (makes and models contain spaces and slashes)."""
    return urllib.parse.quote(value.strip(), safe="")


# ---------------------------------------------------------------- value helpers


def clean_str(value: Any) -> str | None:
    """Trim a string; empty, whitespace-only and 'Not Applicable' become NULL."""
    if value is None:
        return None
    text = str(value).strip()
    return text if text and text != "Not Applicable" else None


def to_int(value: Any) -> int | None:
    """Parse an integer leniently ('6', '6.0', ' 6 '); anything else is NULL."""
    text = clean_str(value)
    if text is None:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def to_float(value: Any) -> float | None:
    """Parse a float leniently; anything else is NULL."""
    text = clean_str(value)
    if text is None:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def to_rating(value: Any) -> int | None:
    """NCAP star rating '1'..'5'; 'Not Rated' and blanks are NULL."""
    rating = to_int(value)
    return rating if rating is not None and 1 <= rating <= 5 else None


_DMY = re.compile(r"^(\d{2})/(\d{2})/(\d{4})$")


def parse_date(value: Any, order: str) -> date | None:
    """Parse 'NN/NN/YYYY' as day-first (``order='dmy'``) or month-first (``'mdy'``)."""
    text = clean_str(value)
    if text is None:
        return None
    match = _DMY.match(text)
    if not match:
        return None
    a, b, year = (int(g) for g in match.groups())
    day, month = (a, b) if order == "dmy" else (b, a)
    try:
        return date(year, month, day)
    except ValueError:
        return None


def normalize_make(value: Any) -> str | None:
    """Return the cross-source join key for a make: trimmed and upper-cased ('Toyota' -> 'TOYOTA')."""
    text = clean_str(value)
    return text.upper() if text else None


def utc_now() -> datetime:
    """Return the current time, timezone-aware (UTC)."""
    from datetime import UTC

    return datetime.now(UTC)
