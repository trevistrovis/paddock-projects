import re
from typing import Dict
from app.db import get_db_conn

def extract_zip(text: str) -> str:
    match = re.search(r"\b\d{5}\b", text)
    if not match:
        raise RuntimeError(f"No ZIP code found in '{text}'")
    return match.group(0)

def resolve_location(city_state_zip: str) -> Dict[str, str]:
    zip_code = extract_zip(city_state_zip)

    conn = get_db_conn()
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute(
            "SELECT state_name, county_name FROM zips WHERE zip = %s",
            (zip_code,)
        )
        zip_row = cur.fetchone()

        if not zip_row:
            raise RuntimeError(f"ZIP {zip_code} not found")

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