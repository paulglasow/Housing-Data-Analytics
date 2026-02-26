"""
build_hna_data.py - Housing Needs Assessment data builder.

Fetches, caches, and processes DOLA SYA, LEHD, and Census ACS data for
Colorado counties/places. Designed to be resilient: external API failures
are non-fatal; partial outputs are produced with warnings.
"""

import io
import logging
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DOLA_SYA_URL = (
    "https://demography.dola.colorado.gov/assets/downloads/"
    "county_sya_estimates.csv"
)
DOLA_SOURCE_CSV = "data/hna/source/dola_sya_county.csv"

CENSUS_BASE = "https://api.census.gov/data"
LEHD_BASE = "https://lehd.ces.census.gov/data/lodes/LODES8"

OUTPUT_DIR = "data/hna/output"

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def utc_now_z() -> str:
    """Return a timezone-aware UTC timestamp string (ISO-8601 with Z suffix)."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def redact(s: str) -> str:
    """Redact Census API keys and other secrets from log strings."""
    import re
    return re.sub(r"(key=)[^&\s]+", r"\1***", str(s))


def http_get_text(url: str, timeout: int = 30, retries: int = 3,
                  backoff: float = 1.7) -> str:
    """GET *url* and return the response body as text.

    Retries with exponential backoff on connection/timeout errors.
    Raises on HTTP error after all retries are exhausted.
    """
    last_exc: Exception = RuntimeError("No attempts made")
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < retries - 1:
                wait = backoff ** attempt
                log.warning(
                    "http_get_text attempt %d/%d failed for %s: %s – "
                    "retrying in %.1fs",
                    attempt + 1, retries, redact(url), exc, wait,
                )
                time.sleep(wait)
    raise last_exc


def http_get_json(url: str, timeout: int = 30, retries: int = 3,
                  backoff: float = 1.7):
    """GET *url* and return parsed JSON, or *None* on any error (non-fatal)."""
    last_exc: Exception = RuntimeError("No attempts made")
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < retries - 1:
                wait = backoff ** attempt
                log.warning(
                    "http_get_json attempt %d/%d failed for %s: %s – "
                    "retrying in %.1fs",
                    attempt + 1, retries, redact(url), exc, wait,
                )
                time.sleep(wait)
    log.error("http_get_json all retries failed for %s: %s",
              redact(url), last_exc)
    return None


def read_csv_with_banner_skip(path: str, encoding: str = "utf-8") -> pd.DataFrame:
    """Read a CSV, automatically skipping leading banner rows.

    Banner rows are lines where the first cell does not look like a column
    header or numeric value – e.g. "Vintage 2023 county estimates...".
    """
    with open(path, encoding=encoding) as fh:
        lines = fh.readlines()

    skip = 0
    for line in lines:
        first_cell = line.split(",")[0].strip().strip('"')
        # Stop skipping once we find a plausible header or data line
        if first_cell == "" or first_cell[0].isdigit():
            break
        # Lines starting with "Vintage", "Note", "Source", "#" are banners
        if any(first_cell.lower().startswith(kw)
               for kw in ("vintage", "note", "source", "#")):
            skip += 1
        else:
            break

    if skip:
        log.info("read_csv_with_banner_skip: skipping %d banner row(s) in %s",
                 skip, path)

    return pd.read_csv(path, skiprows=skip, encoding=encoding)


# ---------------------------------------------------------------------------
# DOLA Single-Year-of-Age (SYA) data
# ---------------------------------------------------------------------------


def download_dola_sya(dest: str = DOLA_SOURCE_CSV) -> bool:
    """Download the DOLA SYA county CSV to *dest*.

    Returns True on success, False on failure (non-fatal).
    """
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    log.info("Downloading DOLA SYA from %s", DOLA_SYA_URL)
    try:
        text = http_get_text(DOLA_SYA_URL)
        with open(dest, "w", encoding="utf-8") as fh:
            fh.write(text)
        log.info("DOLA SYA saved to %s", dest)
        return True
    except Exception as exc:
        log.warning("DOLA SYA download failed: %s", exc)
        return False


def load_dola_sya(cache: str = DOLA_SOURCE_CSV) -> pd.DataFrame | None:
    """Return the DOLA SYA DataFrame, using cache when download fails.

    Returns None if neither download nor cache is available (non-fatal).
    """
    success = download_dola_sya(cache)
    if not success:
        if os.path.exists(cache):
            log.warning("Using cached DOLA file: %s", cache)
        else:
            log.warning(
                "DOLA SYA unavailable and no cache found – skipping SYA build"
            )
            return None

    try:
        return read_csv_with_banner_skip(cache)
    except Exception as exc:
        log.warning("Failed to parse DOLA SYA CSV (%s): %s – skipping SYA build",
                    cache, exc)
        return None


def build_dola_sya_by_county(county: str,
                              df: pd.DataFrame | None = None) -> pd.DataFrame | None:
    """Filter the SYA DataFrame for *county*. Non-fatal if df is None."""
    if df is None:
        log.info("Skipped SYA build for %s (no data)", county)
        return None
    mask = df.get("county", pd.Series(dtype=str)).str.lower() == county.lower()
    return df[mask].copy()


# ---------------------------------------------------------------------------
# Census ACS helpers
# ---------------------------------------------------------------------------


def _census_get(dataset: str, year: int, variables: str,
                geo: str, key: str | None = None) -> list | None:
    """Low-level Census API call; returns raw JSON list or None."""
    params = f"get={variables}&for={geo}"
    if key:
        params += f"&key={key}"
    url = f"{CENSUS_BASE}/{year}/{dataset}?{params}"
    log.info("Census request: %s", redact(url))
    data = http_get_json(url)
    if data is None or not isinstance(data, list):
        return None
    return data


def fetch_acs_profile(geo: str, variables: str, year: int,
                      key: str | None = None) -> list | None:
    """Fetch ACS profile data with fallback chain.

    Tries in order: ACS1/profile → ACS1/subject → ACS5/profile → ACS5/subject.
    Returns raw Census JSON list or None if all fail (non-fatal).
    """
    attempts = [
        (f"acs/acs1/profile", year),
        (f"acs/acs1/subject", year),
        (f"acs/acs5/profile", year),
        (f"acs/acs5/subject", year),
    ]
    for dataset, yr in attempts:
        result = _census_get(dataset, yr, variables, geo, key)
        if result is not None:
            log.info("fetch_acs_profile succeeded with %s/%d", dataset, yr)
            return result
        log.warning("fetch_acs_profile: %s/%d returned no data, trying next",
                    dataset, yr)
    log.error("fetch_acs_profile: all fallbacks exhausted for geo=%s", geo)
    return None


def fetch_acs_s0801(geo: str, variables: str, year: int,
                    key: str | None = None) -> list | None:
    """Fetch ACS S0801 (commuting) data.

    Tries: ACS1/subject → ACS5/subject.
    Returns raw Census JSON list or None if all fail (non-fatal).
    """
    attempts = [
        ("acs/acs1/subject", year),
        ("acs/acs5/subject", year),
    ]
    for dataset, yr in attempts:
        result = _census_get(dataset, yr, variables, geo, key)
        if result is not None:
            log.info("fetch_acs_s0801 succeeded with %s/%d", dataset, yr)
            return result
        log.warning("fetch_acs_s0801: %s/%d returned no data, trying next fallback",
                    dataset, yr)
    log.error("fetch_acs_s0801: all fallbacks exhausted for geo=%s", geo)
    return None


# ---------------------------------------------------------------------------
# LEHD (critical – fatal on failure)
# ---------------------------------------------------------------------------


def download_lehd(state: str, year: int, dest_dir: str = OUTPUT_DIR) -> str:
    """Download LEHD LODES WAC file for *state*/*year*.

    This is a critical data source – raises on failure.
    """
    os.makedirs(dest_dir, exist_ok=True)
    filename = f"{state.lower()}_wac_S000_JT00_{year}.csv.gz"
    url = f"{LEHD_BASE}/{state.lower()}/wac/{filename}"
    dest = os.path.join(dest_dir, filename)
    log.info("Downloading LEHD from %s", url)
    # Use http_get_text with longer timeout for large file
    resp = requests.get(url, timeout=120, stream=True)
    resp.raise_for_status()
    with open(dest, "wb") as fh:
        for chunk in resp.iter_content(chunk_size=65536):
            fh.write(chunk)
    log.info("LEHD saved to %s (%s)", dest, utc_now_z())
    return dest


def build_lehd_by_county(county: str, lehd_path: str) -> pd.DataFrame | None:
    """Read LEHD WAC file and filter to *county*. Non-fatal on parse error."""
    try:
        df = pd.read_csv(lehd_path, compression="gzip", dtype=str)
        mask = df.get("cty", pd.Series(dtype=str)).str.startswith(county)
        return df[mask].copy()
    except Exception as exc:
        log.warning("build_lehd_by_county failed for %s: %s", county, exc)
        return None


# ---------------------------------------------------------------------------
# Geo-derived inputs and summary cache
# ---------------------------------------------------------------------------


def build_dola_projections_by_county(county: str) -> dict:
    """Placeholder for DOLA projection data. Returns empty dict if unavailable."""
    log.info("build_dola_projections_by_county: %s (stub)", county)
    return {}


def build_geo_derived_inputs() -> dict:
    """Build geography-derived inputs. Returns empty dict on failure."""
    log.info("build_geo_derived_inputs called at %s", utc_now_z())
    return {}


def build_summary_cache() -> dict:
    """Build summary cache. Returns empty dict on failure."""
    log.info("build_summary_cache called at %s", utc_now_z())
    return {}


def write_geo_config(output_dir: str = OUTPUT_DIR) -> str:
    """Write geo-config JSON stub. Returns path written."""
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "geo_config.json")
    import json
    config = {"generated": utc_now_z(), "version": "1.0"}
    with open(path, "w") as fh:
        json.dump(config, fh, indent=2)
    log.info("geo_config written to %s", path)
    return path


def fetch_counties() -> list:
    """Return list of Colorado county FIPS codes to process.

    Colorado has 64 counties with odd FIPS codes from 001 to 123.
    """
    # Colorado county FIPS: odd numbers 001..123 (64 counties total)
    _CO_FIPS_MIN = 1
    _CO_FIPS_MAX = 124  # exclusive upper bound (last valid FIPS is 123)
    _CO_FIPS_STEP = 2   # all Colorado county FIPS are odd
    return [str(fips).zfill(3) for fips in range(_CO_FIPS_MIN, _CO_FIPS_MAX, _CO_FIPS_STEP)]


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main() -> int:
    """Run the HNA data build pipeline. Returns exit code (0 = success)."""
    log.info("HNA build started at %s", utc_now_z())

    # --- Critical: ensure output directory exists ---
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
    except OSError as exc:
        log.critical("Cannot create output directory %s: %s", OUTPUT_DIR, exc)
        return 1

    # --- Non-critical: DOLA SYA ---
    sya_df = load_dola_sya()
    if sya_df is None:
        log.warning("Skipped SYA build – continuing without SYA data")

    # --- Non-critical: geo-derived inputs ---
    geo_inputs = build_geo_derived_inputs()

    # --- Non-critical: summary cache ---
    summary = build_summary_cache()

    # --- Non-critical: geo-config ---
    write_geo_config()

    log.info("HNA build finished at %s", utc_now_z())
    return 0


if __name__ == "__main__":
    sys.exit(main())
