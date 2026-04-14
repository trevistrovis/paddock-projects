import re
import requests
from typing import Dict, Optional
from app.db import get_db_conn

CENSUS_GEOCODER_URL = "https://geocoding.geo.census.gov/geocoder/geographies/address"

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
        print(f"[ZIP] ZIP {zip_code} not found in DB. Fetching from Census...")
        fetched = fetch_zip_mapping_from_census(
            city=parsed["city"],
            state=parsed["state"],
            zip_code=zip_code,
        )

        if not fetched:
            raise RuntimeError(f"ZIP {zip_code} not found")

        save_zip_mapping(
            zip_code=fetched["zip"],
            state_name=fetched["state_name"],
            county_name=fetched["county_name"],
        )

        zip_row = {
            "zip": fetched["zip"],
            "state_name": fetched["state_name"],
            "county_name": fetched["county_name"],
        }

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
    """
    Basic parser for inputs like:
    'Rock Hill, SC 29730'
    'Buffalo, NY 14202'
    """
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

def fetch_zip_mapping_from_census(city: str, state: str, zip_code: str) -> Optional[Dict[str, str]]:
    """
    Uses the Census Geocoder API to resolve county geography from city/state/ZIP.
    """
    params = {
        "city": city,
        "state": state,
        "zip": zip_code,
        "benchmark": "Public_AR_Current",
        "vintage": "Current_Current",
        "format": "json",
    }

    resp = requests.get(CENSUS_GEOCODER_URL, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    result_block = data.get("result", {})
    matches = result_block.get("addressMatches", [])

    if not matches:
        return None

    first = matches[0]
    geos = first.get("geographies", {})
    counties = geos.get("Counties", [])

    if not counties:
        return None

    county = counties[0]

    county_name = county.get("NAME")
    state_name = county.get("STATE")
    county_fips = county.get("COUNTY")
    full_fips = f"{state_name}{county_fips}"

    return {
        "zip": zip_code,
        "state_name": first.get("matchedAddress", "").split(",")[-2].strip() if False else state,  # keep caller state name for now
        "county_name": county_name,
        "fips": full_fips,
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