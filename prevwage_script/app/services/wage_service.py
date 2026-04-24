from typing import Optional, Dict, Any
from datetime import date
from app.db import get_db_conn
from app.config import DEFAULT_CONSTRUCTION_TYPE
from app.services.location_service import get_county_by_fips
from app.services.sam_service import (
    search_sam_for_wd,
    fetch_wd_detail_from_sam,
)
import re

WD_NUMBER_RE = re.compile(r"\b([A-Z]{2}\d{8})\b")

def get_wage_from_db(
    fips: str,
    worker_classification: str,
    as_of_date: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    conn = get_db_conn()
    cur = conn.cursor(dictionary=True)

    try:
        if as_of_date:
            cur.execute(
                """
                SELECT *
                FROM wages
                WHERE fips = %s
                AND worker_classification = %s
                AND effective_date <= %s
                ORDER BY effective_date DESC
                LIMIT 1
                """,
                (fips, worker_classification, as_of_date)
            )
        else:
            cur.execute(
                """
                SELECT *
                FROM wages
                WHERE fips = %s
                AND worker_classification = %s
                ORDER BY effective_date DESC
                LIMIT 1
                """,
                (fips, worker_classification)
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
    detail_url: Optional[str] = None,
    effective_date: Optional[str] = None,
) -> None:
    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO wage_determination_cache (
                fips, wd_number, construction_type, wd_title, source_url, detail_url, effective_date
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                wd_title = VALUES(wd_title),
                source_url = VALUES(source_url),
                detail_url = VALUES(detail_url),
                effective_date = VALUES(effective_date),
                retrieved_at = CURRENT_TIMESTAMP
            """,
            (fips, wd_number, construction_type, wd_title, source_url, detail_url, effective_date)
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

def save_wage(
    fips: str,
    worker_classification: str,
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
                fips, worker_classification, base_rate, fringe_rate,
                effective_date, expiration_date,
                source, source_id, source_url, notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                fips,
                worker_classification,
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
    worker_classification: str = "Millwright",
    as_of_date: Optional[str] = None,
    construction_type: str = DEFAULT_CONSTRUCTION_TYPE,
    wd_cache: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    county_info = get_county_by_fips(fips)
    state_name = county_info["state_name"]
    county_name = county_info["county_name"]

    print(f"[SAM] Searching SAM for {county_name}, {state_name}, {construction_type}")

    wd_number = None
    wd_url = None
    detail_url = None
    wd_data = None

    expected_header = f"COUNTY: {county_name.upper()} IN {state_name.upper()}"

    # If cache exists, try it first
    if wd_cache:
        wd_number = wd_cache["wd_number"]
        wd_url = wd_cache.get("source_url") or wd_cache.get("detail_url")
        detail_url = wd_cache.get("detail_url")
        print(
            f"[SAM] Cached WD URLs for {fips}: "
            f"source_url={wd_cache.get('source_url')}, detail_url={wd_cache.get('detail_url')}"
        )

        wd_data = fetch_wd_detail_from_sam(wd_number=wd_number, wd_url=wd_url)

        if wd_data:
            wd_text = wd_data.get("text", "")
            if expected_header in wd_text.upper():
                worker_wage = extract_worker_from_wd(
                    wd_data,
                    worker_classification=worker_classification,
                )
                if worker_wage:
                    effective_date = worker_wage["effective_date"] or date.today().isoformat()

                    save_wage(
                        fips=fips,
                        worker_classification=worker_classification,
                        base_rate=worker_wage["base_rate"],
                        fringe_rate=worker_wage["fringe_rate"],
                        effective_date=effective_date,
                        source="Davis-Bacon",
                        source_id=wd_number,
                        source_url=wd_url,
                        notes=f"Fetched {worker_classification} from SAM for {county_name}, {state_name}, building",
)

                    return {
                        "base_rate": worker_wage["base_rate"],
                        "fringe_rate": worker_wage["fringe_rate"],
                        "effective_date": effective_date,
                        "source_note": f"Davis-Bacon {wd_number}",
                    }

        print(f"[SAM] Cached WD failed for {fips}, falling back to fresh discovery...")

    # Fresh search
    search_result = search_sam_for_wd(
        state_name=state_name,
        county_name=county_name,
        construction_type=construction_type,
    )

    print(f"[SAM] search_sam_for_wd result: {search_result}")

    if not search_result:
        print(f"[SAM] No WD search result found for {county_name}, {state_name}")
        return None

    candidate_urls = search_result.get("candidates", [])

# fallback to single returned WD URL
    if not candidate_urls:
        single_url = search_result.get("source_url") or search_result.get("detail_url")
        if single_url:
            candidate_urls = [single_url]

    print(f"[SAM] Candidate URL count returned from search: {len(candidate_urls)}")

    if not candidate_urls:
        print("[SAM] No candidate URLs returned from search")
        return None

    # Try each candidate until the downloaded WD text matches county/state
    for candidate_url in candidate_urls:
        print(f"[SAM] Trying candidate WD URL: {candidate_url}")

        wd_data = fetch_wd_detail_from_sam(
            wd_number="UNKNOWN",
            wd_url=candidate_url,
        )

        if not wd_data:
            continue

        wd_text = wd_data.get("text", "")
        if expected_header not in wd_text.upper():
            print(f"[SAM] Candidate did not match expected county/state: {candidate_url}")
            continue

        wd_match = WD_NUMBER_RE.search(wd_text.upper())
        wd_number = wd_match.group(1) if wd_match else "UNKNOWN"
        wd_url = candidate_url
        detail_url = candidate_url

        worker_wage = extract_worker_from_wd(
        wd_data,
            worker_classification=worker_classification,
        )

        if not worker_wage:
            print(f"[SAM] No {worker_classification} line found in WD {wd_number}")
            continue

        # Cache only after validation succeeds
        save_wd_cache(
            fips=fips,
            wd_number=wd_number,
            construction_type=construction_type,
            wd_title=f"{county_name}, {state_name} - Building",
            source_url=wd_url,
            detail_url=detail_url,
            effective_date=worker_wage.get("effective_date"),
        )

        effective_date = worker_wage["effective_date"] or date.today().isoformat()

        save_wage(
            fips=fips,
            base_rate=worker_wage["base_rate"],
            fringe_rate=worker_wage["fringe_rate"],
            effective_date=effective_date,
            source="Davis-Bacon",
            source_id=wd_number,
            source_url=wd_url,
            notes=f"Fetched from SAM for {county_name}, {state_name}, {construction_type}",
        )

        return {
            "base_rate": worker_wage["base_rate"],
            "fringe_rate": worker_wage["fringe_rate"],
            "effective_date": effective_date,
            "source_note": f"Davis-Bacon {wd_number}",
        }

    print(f"[SAM] No candidate WD matched {county_name}, {state_name}")
    return None

def lookup_worker_wage(
    fips: str,
    worker_classification: str = "Millwright",
    as_of_date: Optional[str] = None,
) -> Dict[str, Any]:
    print(f"[WAGE] Looking up {worker_classification} wage for FIPS {fips}")

    wage = get_wage_from_db(
        fips=fips,
        worker_classification=worker_classification,
        as_of_date=as_of_date,
    )

    if wage:
        print(f"[WAGE] Found {worker_classification} wage in DB for {fips}")
        return wage

    print(f"[WAGE] No {worker_classification} wage in DB for {fips}")

    wd_cache = get_cached_wd(fips)
    if wd_cache:
        print(f"[WAGE] Found cached WD for {fips}: {wd_cache['wd_number']}")
    else:
        print(f"[WAGE] No cached WD for {fips}")

    wage = fetch_and_store_wage_from_sam(
        fips=fips,
        worker_classification=worker_classification,
        as_of_date=as_of_date,
        construction_type=DEFAULT_CONSTRUCTION_TYPE,
        wd_cache=wd_cache,
    )

    if wage:
        return wage

    print(f"[WAGE] SAM fallback returned nothing for {fips}")
    raise RuntimeError(f"No {worker_classification} wage found for FIPS {fips}")