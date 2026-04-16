import re
from typing import Optional, Dict, Any

import requests
from bs4 import BeautifulSoup

from app.config import SAM_BASE_URL

HEADERS = {
    "User-Agent": "Mozilla/5.0",
}

RATE_LINE_RE = re.compile(
    r"""
    (?P<trade>MILLWRIGHT[^\n\r]*?)
    (?P<base>\d{1,3}(?:\.\d{2})?)
    \s*
    (?P<fringe>\d{1,3}(?:\.\d{2})?|[A-Z].*)
    """,
    re.IGNORECASE | re.VERBOSE,
)

DATE_RE = re.compile(r"(?i)(?:effective|modification|published)\s*(?:date)?\s*[:\-]?\s*([A-Za-z]+\s+\d{1,2},\s+\d{4}|\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2})")


def search_sam_for_wd(
    state_name: str,
    county_name: str,
    construction_type: str,
) -> Optional[Dict[str, Any]]:
    """
    Step 2 still leaves search lightweight.
    For now this returns a search URL you can cache, not a final WD number.
    """
    query = f"{state_name} {county_name} {construction_type} Davis-Bacon wage determination"
    search_url = f"{SAM_BASE_URL}?q={requests.utils.quote(query)}"

    return {
        "wd_number": f"PENDING-{state_name[:2].upper()}-{county_name[:10].upper().replace(' ', '')}",
        "wd_title": f"{county_name}, {state_name} - {construction_type}",
        "source_url": search_url,
        "effective_date": None,
    }


def fetch_wd_detail_from_sam(
    wd_number: str,
    wd_url: Optional[str] = None,
    
) -> Optional[Dict[str, Any]]:
    """
    Step 2 expects wd_url to point to a specific wage-determination detail page.
    If it's still just a search URL, parsing probably won't work yet.
    """
    if not wd_url:
        print(f"[SAM] No wd_url provided for {wd_number}")
        return None

    resp = requests.get(wd_url, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    html = resp.text
    soup = BeautifulSoup(html, "html.parser")

    text = soup.get_text("\n", strip=True)
    
    print(f"[SAM] Fetching WD detail from: {wd_url}")
    print(f"[SAM] HTTP status: {resp.status_code}")
    print(f"[SAM] Page title: {soup.title.get_text(strip=True) if soup.title else 'NO TITLE'}")
    print(f"[SAM] Text sample: {text[:1000]}")
    
    return {
        "wd_number": wd_number,
        "wd_url": wd_url,
        "html": html,
        "text": text,
        "title": soup.title.get_text(strip=True) if soup.title else None,
    }


def _normalize_fringe(fringe_raw: str) -> float:
    """
    Very conservative parser:
    - if fringe starts with a number, use it
    - otherwise default to 0.00 for now
    """
    if not fringe_raw:
        return 0.0

    m = re.search(r"(\d{1,3}(?:\.\d{2})?)", fringe_raw)
    if not m:
        return 0.0

    return float(m.group(1))


def _normalize_effective_date(text: str) -> str:
    """
    Step 2 fallback:
    - try to find a recognizable date in the page
    - if not found, return today's ISO date from the caller layer later if needed
    """
    m = DATE_RE.search(text)
    if not m:
        return ""

    raw = m.group(1).strip()

    # pass through common formats for now
    return raw


def extract_millwright_from_wd(wd_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extracts Millwright line from the raw text.
    This is intentionally simple for step 2.
    """
    text = wd_data.get("text", "")
    if not text:
        return None

    # First pass: line-by-line search
    lines = [line.strip() for line in text.splitlines() if line.strip()]

    for line in lines:
        if "MILLWRIGHT" not in line.upper():
            continue

        match = RATE_LINE_RE.search(line)
        if match:
            base_rate = float(match.group("base"))
            fringe_rate = _normalize_fringe(match.group("fringe"))
            effective_date = _normalize_effective_date(text)
            print("[SAM] Searching for Millwright in WD text")
            
            return {
                "base_rate": base_rate,
                "fringe_rate": fringe_rate,
                "effective_date": effective_date,
                "matched_line": line,
            }
        else:
            print("[SAM] No Millwright match found")
    
    # Second pass: broader text search across wrapped lines
    m = RATE_LINE_RE.search(text)
    if m:
        base_rate = float(m.group("base"))
        fringe_rate = _normalize_fringe(m.group("fringe"))
        effective_date = _normalize_effective_date(text)

        return {
            "base_rate": base_rate,
            "fringe_rate": fringe_rate,
            "effective_date": effective_date,
            "matched_line": m.group(0),
        }

    return None