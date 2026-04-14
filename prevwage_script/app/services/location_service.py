import re
import requests
from typing import Dict, Optional
from app.db import get_db_conn
from app.utils.state_abbrev import STATE_ABBR

ZIP_API_URL = "https://api.zippopotam.us/us/{zip_code}"
FCC_AREA_API_URL = "https://geo.fcc.gov/api/census/area"

def extract_zip(text: str) -> str:
    match = re.search(r"\b\d{5}\b", text)
    if not match:
        raise RuntimeError(f"No ZIP code found in '{text}'")
    return match.group(0)

def resolve_location(city_state_zip: str) -> Dict[str, str]:
    parsed = parse_city_state_zip(city_state_zip)
    zip_code = parsed["zip"]

    zip_row = get_zip_from_db(zip_code)

    if not zip_row:
        print(f"[ZIP] ZIP {zip_code} not found in DB. Fetching externally...")
        fetched = fetch_zip_mapping(zip_code=zip_code, fallback_state=parsed["state"])

        if not fetched:
            raise RuntimeError(f"ZIP {zip_code} not found")

        save_zip_mapping(
            zip_code=fetched["zip"],
            state_name=fetched["state_name"],
            county_name=fetched["county_name"],
        )

        zip_row = fetched

    conn = get_db_conn()
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute(
            """
            SELECT fips, county_name
            FROM counties
            WHERE state_name = %s AND county_name = %s
            LIMIT 1
            """,
            (zip_row["state_name"], zip_row["county_name"])
        )
        county_row = cur.fetchone()

        if not county_row:
            raise RuntimeError(
                f"No FIPS match for {zip_row['county_name']}, {zip_row['state_name']}"
            )

        return {
            "county": county_row["county_name"],
            "fips": county_row["fips"],
        }
    finally:
        cur.close()
        conn.close()

def get_county_by_fips(fips: str) -> Dict[str, str]:
    conn = get_db_conn()
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute(
            """
            SELECT state_name, county_name
            FROM counties
            WHERE fips = %s
            LIMIT 1
            """,
            (fips,)
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"No county found for FIPS {fips}")
        return row
    finally:
        cur.close()
        conn.close()

def parse_city_state_zip(text: str) -> Dict[str, str]:
    zip_code = extract_zip(text)

    cleaned = text.replace(zip_code, "").strip().rstrip(",")
    parts = [p.strip() for p in cleaned.split(",") if p.strip()]

    if len(parts) < 2:
        raise RuntimeError(f"Could not parse city/state from '{text}'")

    city = parts[0]
    state = parts[1]

    return {
        "city": city,
        "state": state,
        "zip": zip_code,
    }


def get_zip_from_db(zip_code: str) -> Optional[Dict[str, str]]:
    conn = get_db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT zip, state_name, county_name FROM zips WHERE zip = %s",
            (zip_code,)
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def save_zip_mapping(zip_code: str, state_name: str, county_name: str) -> None:
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO zips (zip, state_name, county_name)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                state_name = VALUES(state_name),
                county_name = VALUES(county_name)
            """,
            (zip_code, state_name, county_name)
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def fetch_zip_mapping(zip_code: str, fallback_state: Optional[str] = None) -> Optional[Dict[str, str]]:
    # Step 1: ZIP -> lat/lon + state via Zippopotam
    zip_resp = requests.get(ZIP_API_URL.format(zip_code=zip_code), timeout=20)
    if zip_resp.status_code == 404:
        return None
    zip_resp.raise_for_status()

    zip_data = zip_resp.json()
    places = zip_data.get("places", [])
    if not places:
        return None

    place = places[0]
    lat = place.get("latitude")
    lon = place.get("longitude")
    state_name = place.get("state")

    # prefer explicit user state if it was an abbreviation we can expand
    if fallback_state:
        state_name = STATE_ABBR.get(fallback_state.upper(), fallback_state)

    # Step 2: lat/lon -> county via FCC
    fcc_resp = requests.get(
        FCC_AREA_API_URL,
        params={
            "format": "json",
            "lat": lat,
            "lon": lon,
        },
        timeout=20,
    )
    fcc_resp.raise_for_status()
    fcc_data = fcc_resp.json()

    county_block = fcc_data.get("results", {}).get("county")
    state_block = fcc_data.get("results", {}).get("state")

    if not county_block or not state_block:
        return None

    county_name = county_block.get("name")
    if county_name and not county_name.lower().endswith("county"):
        county_name = f"{county_name} County"

    return {
        "zip": zip_code,
        "state_name": state_name or state_block.get("name"),
        "county_name": county_name,
    }