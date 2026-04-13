from typing import Optional, Dict, Any
from urllib.parse import quote_plus
from app.config import DEFAULT_CONSTRUCTION_TYPE, SAM_BASE_URL

def build_sam_search_url(
    state_name: str,
    county_name: str,
    construction_type: str = DEFAULT_CONSTRUCTION_TYPE,
) -> str:
    """
    For step 1, we build a search URL you can use manually or later automate.
    """
    # This is a placeholder search link pattern for now.
    # We are not relying on a documented direct JSON API yet.
    query = f"{state_name} {county_name} {construction_type} Davis-Bacon wage determination"
    return f"{SAM_BASE_URL}?q={quote_plus(query)}"

def search_sam_for_wd(
    state_name: str,
    county_name: str,
    construction_type: str,
) -> Optional[Dict[str, Any]]:
    """
    Step 1 behavior:
    - build a likely search URL
    - return a cacheable record shape
    - do not try to parse the full WD yet
    """
    print(f"[SAM] Preparing search for {county_name}, {state_name}, {construction_type}")

    search_url = build_sam_search_url(
        state_name=state_name,
        county_name=county_name,
        construction_type=construction_type,
    )

    # Step 1 placeholder result:
    # we don't have a true WD number parser yet, so we cache the search URL only
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
    Step 1: not implemented yet.
    """
    print(f"[SAM] fetch_wd_detail_from_sam not implemented yet for {wd_number}")
    return None

def extract_millwright_from_wd(wd_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Step 1: not implemented yet.
    """
    print("[SAM] extract_millwright_from_wd not implemented yet")
    return None