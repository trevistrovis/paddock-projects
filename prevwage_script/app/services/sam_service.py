from typing import Optional, Dict, Any
from app.config import DEFAULT_CONSTRUCTION_TYPE

def search_sam_for_wd(
    state_name: str,
    county_name: str,
    construction_type: str,
) -> Optional[Dict[str, Any]]:
    print(f"[SAM] search_sam_for_wd not implemented yet: {county_name}, {state_name}, {construction_type}")
    return None

def fetch_wd_detail_from_sam(
    wd_number: str,
    wd_url: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    print(f"[SAM] fetch_wd_detail_from_sam not implemented yet: {wd_number}")
    return None

def extract_millwright_from_wd(wd_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    print("[SAM] extract_millwright_from_wd not implemented yet")
    return None

def fetch_and_store_wage_from_sam(
    fips: str,
    as_of_date: Optional[str] = None,
    construction_type: str = DEFAULT_CONSTRUCTION_TYPE,
    wd_cache: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    print(f"[SAM] Fetch not implemented yet for FIPS {fips}")
    return None