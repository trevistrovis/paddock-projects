from typing import Optional, Dict, Any

from app.db import get_db_conn
from app.config import DEFAULT_CONSTRUCTION_TYPE
from app.services.location_service import get_county_by_fips
from app.services.sam_service import (
    search_sam_for_wd,
    fetch_wd_detail_from_sam,
    extract_millwright_from_wd,
)

def get_wage_from_db(fips: str, as_of_date: Optional[str] = None) -> Optional[Dict[str, Any]]:
    conn = get_db_conn()
    cur = conn.cursor(dictionary=True)

    try:
        if as_of_date:
            cur.execute(
                """
                SELECT *
                FROM wages
                WHERE fips = %s
                  AND effective_date <= %s
                ORDER BY effective_date DESC
                LIMIT 1
                """,
                (fips, as_of_date)
            )
        else:
            cur.execute(
                """
                SELECT *
                FROM wages
                WHERE fips = %s
                ORDER BY effective_date DESC
                LIMIT 1
                """,
                (fips,)
            )

        row = cur.fetchone()
        if not row:
            return None

        return {
            "base_rate": float(row["base_rate"]),
            "fringe_rate": float(row["fringe_rate"]),
            "effective_date": row["effective_date"].strftime("%Y-%m-%d"),
            "source_note": " ".join(
                part for part in [row.get("source"), row.get("source_id")] if part
            ).strip(),
        }
    finally:
        cur.close()
        conn.close()

def get_cached_wd(fips: str) -> Optional[Dict[str, Any]]:
    conn = get_db_conn()
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute(
            """
            SELECT *
            FROM wage_determination_cache
            WHERE fips = %s
            ORDER BY retrieved_at DESC
            LIMIT 1
            """,
            (fips,)
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()

def save_wd_cache(
    fips: str,
    wd_number: str,
    construction_type: str,
    wd_title: Optional[str] = None,
    source_url: Optional[str] = None,
    effective_date: Optional[str] = None,
) -> None:
    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO wage_determination_cache (
                fips, wd_number, construction_type, wd_title, source_url, effective_date
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                wd_title = VALUES(wd_title),
                source_url = VALUES(source_url),
                effective_date = VALUES(effective_date),
                retrieved_at = CURRENT_TIMESTAMP
            """,
            (fips, wd_number, construction_type, wd_title, source_url, effective_date)
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

def save_wage(
    fips: str,
    base_rate: float,
    fringe_rate: float,
    effective_date: str,
    expiration_date: Optional[str] = None,
    source: str = "Davis-Bacon",
    source_id: Optional[str] = None,
    source_url: Optional[str] = None,
    notes: Optional[str] = None,
) -> None:
    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO wages (
                fips, base_rate, fringe_rate, effective_date, expiration_date,
                source, source_id, source_url, notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                fips,
                base_rate,
                fringe_rate,
                effective_date,
                expiration_date,
                source,
                source_id,
                source_url,
                notes,
            )
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

def fetch_and_store_wage_from_sam(
    fips: str,
    as_of_date: Optional[str] = None,
    construction_type: str = DEFAULT_CONSTRUCTION_TYPE,
    wd_cache: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    county_info = get_county_by_fips(fips)
    state_name = county_info["state_name"]
    county_name = county_info["county_name"]

    print(f"[SAM] Searching SAM for {county_name}, {state_name}, {construction_type}")

    if wd_cache:
        print(f"[SAM] Using cached WD entry for {fips}: {wd_cache['wd_number']}")
        wd_number = wd_cache["wd_number"]
        wd_url = wd_cache.get("source_url")
        wd_data = fetch_wd_detail_from_sam(wd_number=wd_number, wd_url=wd_url)

        if not wd_data:
            print(f"[SAM] No WD detail data returned for cached WD {wd_number}")
            return None
    else:
        search_result = search_sam_for_wd(
            state_name=state_name,
            county_name=county_name,
            construction_type=construction_type,
        )

        print(f"[SAM] search_sam_for_wd result: {search_result}")

        if not search_result:
            print(f"[SAM] No WD search result found for {county_name}, {state_name}")
            return None

        print(f"[SAM] Saving WD cache for FIPS {fips}")
        save_wd_cache(
            fips=fips,
            wd_number=search_result["wd_number"],
            construction_type=construction_type,
            wd_title=search_result.get("wd_title"),
            source_url=search_result.get("source_url"),
            effective_date=search_result.get("effective_date"),
        )
        print(f"[SAM] Cached WD placeholder for {fips}: {search_result['wd_number']}")

        return None

def lookup_millwright_wage(fips: str, as_of_date: Optional[str] = None) -> Dict[str, Any]:
    print(f"[WAGE] Looking up wage for FIPS {fips}")

    wage = get_wage_from_db(fips, as_of_date)
    if wage:
        print(f"[WAGE] Found wage in DB for {fips}")
        return wage

    print(f"[WAGE] No wage in DB for {fips}")

    wd_cache = get_cached_wd(fips)
    if wd_cache:
        print(f"[WAGE] Found cached WD for {fips}: {wd_cache['wd_number']}")
    else:
        print(f"[WAGE] No cached WD for {fips}")

    wage = fetch_and_store_wage_from_sam(
        fips=fips,
        as_of_date=as_of_date,
        construction_type=DEFAULT_CONSTRUCTION_TYPE,
        wd_cache=wd_cache,
    )
    if wage:
        return wage

    print(f"[WAGE] SAM fallback returned nothing for {fips}")
    raise RuntimeError(f"No wage found for FIPS {fips}")