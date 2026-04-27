import os
import re
import tempfile
from typing import Optional, Dict, Any
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from app.config import SAM_BASE_URL

HEADERS = {
    "User-Agent": "Mozilla/5.0",
}

# RATE_LINE_RE = re.compile(
#     r"""
#     (?P<trade>MILLWRIGHT[^\n\r]*?)
#     (?P<base>\d{1,3}(?:\.\d{2})?)
#     \s*
#     (?P<fringe>\d{1,3}(?:\.\d{2})?|[A-Z].*)
#     """,
#     re.IGNORECASE | re.VERBOSE,
# )

TXT_LINK_RE = re.compile(r"\.txt($|\?)", re.IGNORECASE)

DATE_RE = re.compile(
    r"(?i)(?:effective|modification|published)\s*(?:date)?\s*[:\-]?\s*"
    r"([A-Za-z]+\s+\d{1,2},\s+\d{4}|\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2})"
)

WD_NUMBER_RE = re.compile(r"\b([A-Z]{2}\d{8})\b")
TXT_LINK_RE = re.compile(r"\.txt($|\?)", re.IGNORECASE)

CONSTRUCTION_LABELS = {
    "building": "Building",
    "heavy": "Heavy",
    "highway": "Highway",
    "residential": "Residential",
}

WORKER_SEARCH_TERMS = {
    "Millwright": ["MILLWRIGHT"],
    "Plumber": ["PLUMBER", "PIPEFITTER"],
    "Sheet Metal Worker": ["SHEET METAL WORKER", "SHEET METAL"],
}

def normalize_county_for_match(value: str) -> str:
    return (
        value
        .replace(" County", "")
        .replace(".", "")
        .strip()
        .lower()
    )

def fill_autocomplete_field(page, aria_label: str, value: str, debug_name: str) -> bool:
    try:
        field = page.locator(f'input[aria-label="{aria_label}"]').first
        field.wait_for(timeout=5000)

        # Clear existing text and type desired value
        field.click()
        field.fill("")
        field.fill(value)
        page.wait_for_timeout(1500)

        # Try to click the exact option from the dropdown
        option_candidates = [
            page.get_by_role("option", name=re.compile(f"^{re.escape(value)}$", re.I)).first,
            page.get_by_text(re.compile(f"^{re.escape(value)}$", re.I)).first,
            page.locator(f'text="{value}"').first,
        ]

        for option in option_candidates:
            try:
                option.wait_for(timeout=4000)
                option.click(force=True)
                page.wait_for_timeout(1000)
                selected_value = field.input_value()
                print(f"[SAM] Filled {debug_name} with exact match: {selected_value}")
                return True
            except Exception:
                continue

        # Fallback: if exact option click fails, try Enter only if the typed value stuck
        field.press("Enter")
        page.wait_for_timeout(1000)
        selected_value = field.input_value()
        print(f"[SAM] Fallback {debug_name} value after Enter: {selected_value}")

        return selected_value.strip().lower() == value.strip().lower()

    except Exception as exc:
        print(f"[SAM] Failed to fill {debug_name}: {exc}")
        return False

def fill_autocomplete_field(page, aria_label: str, value: str, debug_name: str) -> bool:
    try:
        field = page.locator(f'input[aria-label="{aria_label}"]').first
        field.wait_for(timeout=5000)

        def try_value(candidate_value: str) -> bool:
            field.click()
            field.fill("")
            field.fill(candidate_value)
            page.wait_for_timeout(1500)

            option_candidates = [
                page.get_by_role("option", name=re.compile(f"^{re.escape(candidate_value)}$", re.I)).first,
                page.get_by_text(re.compile(f"^{re.escape(candidate_value)}$", re.I)).first,
                page.locator(f'text="{candidate_value}"').first,
            ]

            for option in option_candidates:
                try:
                    option.wait_for(timeout=4000)
                    option.click(force=True)
                    page.wait_for_timeout(1000)
                    selected_value = field.input_value()
                    print(f"[SAM] Filled {debug_name} with exact match: {selected_value}")
                    return selected_value.strip().lower() == candidate_value.strip().lower()
                except Exception:
                    continue

            field.press("Enter")
            page.wait_for_timeout(1000)
            selected_value = field.input_value()
            print(f"[SAM] Fallback {debug_name} value after Enter: {selected_value}")
            return selected_value.strip().lower() == candidate_value.strip().lower()

        candidate_values = [value]

        if debug_name == "construction":
            extras = ["Building", "Heavy", "Highway", "Residential"]
            for extra in extras:
                if extra not in candidate_values:
                    candidate_values.append(extra)

        for candidate_value in candidate_values:
            if try_value(candidate_value):
                return True

        return False

    except Exception as exc:
        print(f"[SAM] Failed to fill {debug_name}: {exc}")
        return False

def select_exact_county_option(page, county_name: str) -> bool:
    try:
        county_base = (
            county_name
            .replace(" County", "")
            .replace(".", "")
            .strip()
        )

        county_input = page.locator('input[aria-label="wd-county"]').first
        county_input.wait_for(timeout=5000)
        county_input.click()
        county_input.fill("")
        county_input.fill(county_base)
        page.wait_for_timeout(1500)
        option_clicked = False

        options = page.locator('[role="option"]')
        option_count = options.count()

        for i in range(option_count):
            try:
                option = options.nth(i)
                option_text = (option.inner_text() or "").strip()
                option_text_clean = normalize_county_for_match(option_text)

                if county_base == option_text_clean:
                    option.click(force=True)
                    option_clicked = True
                    print(f"[SAM] Clicked county option: {option_text}")
                    break

            except Exception:
                continue

        if not option_clicked:
            county_input.press("ArrowDown")
            page.wait_for_timeout(500)
            county_input.press("Enter")
            page.wait_for_timeout(1000)
            print("[SAM] Used keyboard fallback for county")
            return False

        # Blur/commit the widget, but do not require the input to retain the value
        county_input.press("Tab")
        page.wait_for_timeout(1000)

        selected = county_input.input_value().strip()
        print(f"[SAM] County value now: {selected}")

        # If we clicked the exact visible county option, count that as success.
        return True

    except Exception as exc:
        print(f"[SAM] Failed to select county: {exc}")
        return False

def set_results_per_page_to_100(page) -> None:
    try:
        print("[SAM] Attempting to set results per page to 100")

        # Log visible select/combobox-ish controls first
        try:
            select_count = page.locator("select").count()
            print(f"[SAM] Select count before page-size change: {select_count}")
            for i in range(min(select_count, 5)):
                try:
                    txt = page.locator("select").nth(i).text_content() or ""
                    print(f"[SAM] Select[{i}] text sample: {txt[:300]}")
                except Exception:
                    pass
        except Exception:
            pass

        # First try a real select element if one exists
        try:
            selects = page.locator("select")
            for i in range(selects.count()):
                sel = selects.nth(i)
                txt = (sel.text_content() or "").lower()
                if "25" in txt and "100" in txt:
                    sel.select_option(label="100")
                    page.wait_for_timeout(4000)
                    print(f"[SAM] Set results per page to 100 using select[{i}]")
                    return
        except Exception as exc:
            print(f"[SAM] Select-based page-size change failed: {exc}")

        # Fallback: click the current page-size control, then click 100
        opened = False
        for locator in [
            page.get_by_text(re.compile(r"^25$", re.I)).first,
            page.get_by_role("combobox").first,
            page.get_by_text(re.compile(r"results per page", re.I)).first,
        ]:
            try:
                locator.wait_for(timeout=3000)
                locator.click(force=True)
                page.wait_for_timeout(1000)
                opened = True
                print("[SAM] Opened results-per-page control")
                break
            except Exception:
                continue

        if not opened:
            print("[SAM] Could not open results-per-page control")
            return

        for locator in [
            page.get_by_role("option", name=re.compile(r"^100$", re.I)).first,
            page.get_by_text(re.compile(r"^100$", re.I)).first,
            page.locator('text="100"').first,
        ]:
            try:
                locator.wait_for(timeout=3000)
                locator.click(force=True)
                page.wait_for_timeout(4000)
                print("[SAM] Set results per page to 100 using option click")
                return
            except Exception:
                continue

        print("[SAM] Could not select 100 results per page")

    except Exception as exc:
        print(f"[SAM] Failed to set results per page to 100: {exc}")

def search_sam_for_wd(
    state_name: str,
    county_name: str,
    construction_type: str,  # ignored; Building is hardcoded below
) -> Optional[Dict[str, Any]]:
    print(f"[SAM] WD discovery target: {county_name}, {state_name}, Building")

    def go_to_next_page(page) -> bool:
        try:
            next_candidates = [
                page.get_by_role("button", name=re.compile(r"next", re.I)).first,
                page.get_by_role("link", name=re.compile(r"next", re.I)).first,
                page.locator('[aria-label*="Next"]').first,
                page.get_by_text(re.compile(r"^Next$", re.I)).first,
            ]

            for locator in next_candidates:
                try:
                    locator.wait_for(timeout=3000)
                    disabled = locator.get_attribute("disabled")
                    aria_disabled = locator.get_attribute("aria-disabled")

                    if disabled is not None or aria_disabled == "true":
                        continue

                    locator.click(force=True)
                    page.wait_for_timeout(4000)
                    print("[SAM] Moved to next results page")
                    return True
                except Exception:
                    continue

            print("[SAM] No next page available")
            return False

        except Exception as exc:
            print(f"[SAM] Failed to move to next page: {exc}")
            return False

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            page.goto(SAM_BASE_URL, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(3000)

            body_text = page.locator("body").inner_text(timeout=10000)
            print(f"[SAM] Landing page text sample: {body_text[:1500]}")

            # Enter the DBA path
            pbo_locator = page.get_by_text("Public Buildings or Works", exact=False).first
            pbo_locator.wait_for(timeout=15000)
            pbo_locator.scroll_into_view_if_needed()
            pbo_locator.click(force=True)
            page.wait_for_timeout(5000)

            print(f"[SAM] URL after DBA click: {page.url}")

            page.wait_for_load_state("domcontentloaded", timeout=30000)
            page.wait_for_timeout(5000)

            try:
                page.locator("input").first.wait_for(state="attached", timeout=30000)
            except Exception as exc:
                print(f"[SAM] No input controls appeared after DBA click: {exc}")
                print(f"[SAM] URL after failed DBA click: {page.url}")
                try:
                    body_text = page.locator("body").inner_text(timeout=5000)
                    print(f"[SAM] Body text after failed DBA click: {body_text[:1500]}")
                except Exception:
                    pass
                browser.close()
                return None

            for i in range(min(page.locator("input").count(), 7)):
                try:
                    inp = page.locator("input").nth(i)
                    placeholder = inp.get_attribute("placeholder")
                    aria_label = inp.get_attribute("aria-label")
                    name = inp.get_attribute("name")
                    print(
                        f"[SAM] Input[{i}] "
                        f"placeholder={placeholder} aria-label={aria_label} name={name}"
                    )
                except Exception:
                    pass

            # State selection
            state_filled = fill_autocomplete_field(
                page,
                aria_label="wd-state",
                value=state_name,
                debug_name="state",
            )

            # County selection
            county_filled = select_exact_county_option(
                page,
                county_name=county_name,
            )

            # Hardcode construction to Building
            construction_filled = False
            try:
                construction_input = page.locator('input[aria-label="dba-construction-type"]').first
                construction_input.wait_for(timeout=5000)
                construction_input.click()
                construction_input.fill("")
                construction_input.fill("Building")
                page.wait_for_timeout(1500)

                option_clicked = False

                for option in page.locator('[role="option"]').all():
                    try:
                        text = (option.inner_text() or "").strip()
                        if text.lower() == "building":
                            option.click(force=True)
                            option_clicked = True
                            print("[SAM] Clicked construction option: Building")
                            break
                    except Exception:
                        continue

                if not option_clicked:
                    try:
                        page.get_by_text(re.compile(r"^Building$", re.I)).first.click(force=True)
                        option_clicked = True
                        print("[SAM] Clicked construction text: Building")
                    except Exception:
                        pass

                if not option_clicked:
                    construction_input.press("ArrowDown")
                    page.wait_for_timeout(500)
                    construction_input.press("Enter")
                    page.wait_for_timeout(1000)
                    print("[SAM] Used keyboard fallback for construction")

                selected = construction_input.input_value()
                print(f"[SAM] Construction selected: {selected}")
                construction_filled = selected.strip().lower() == "building"

            except Exception as exc:
                print(f"[SAM] Failed to fill construction: {exc}")

            print(
                f"[SAM] Fill status: "
                f"state={state_filled}, county={county_filled}, construction={construction_filled}"
            )

            if not (state_filled and county_filled and construction_filled):
                print("[SAM] Could not fill structured form completely")
                browser.close()
                return None

            # Submit search
            submitted = False
            for locator in [
                page.get_by_role("button", name=re.compile("search", re.I)).first,
                page.get_by_text(re.compile("search", re.I)).first,
            ]:
                try:
                    locator.wait_for(timeout=4000)
                    locator.click(force=True)
                    submitted = True
                    print("[SAM] Search submitted")
                    break
                except Exception:
                    continue

            if not submitted:
                print("[SAM] Could not submit DBA structured search")
                browser.close()
                return None

            page.wait_for_timeout(5000)

            try:
                result_text = page.locator("body").inner_text(timeout=10000)
                print(f"[SAM] Search results text sample: {result_text[:2500]}")
            except Exception as exc:
                print(f"[SAM] Could not read search results body text: {exc}")
                browser.close()
                return None

            county_base = normalize_county_for_match(county_name)
            state_lower = state_name.strip().lower()
            construction_lower = "building"

            matched_result = None
            max_pages = 10

            for page_num in range(1, max_pages + 1):
                print(f"[SAM] Scanning results page {page_num}")

                links = page.locator('a[href*="/wage-determination/"]')
                link_count = links.count()
                print(f"[SAM] WD result link count on page {page_num}: {link_count}")

                for idx in range(link_count):
                    try:
                        print(f"[SAM] Scanning result card {idx + 1} of {link_count}")

                        link = links.nth(idx)
                        href = link.get_attribute("href")
                        link_text = (link.inner_text() or "").strip()

                        if not href or not link_text:
                            continue

                        full_url = href if href.startswith("http") else "https://sam.gov" + href

                        # Read the full visible card text
                        card_text = ""
                        try:
                            card = link.locator(
                                "xpath=ancestor::div[contains(., 'State') and contains(., 'Counties') and contains(., 'Construction Types')][1]"
                            )
                            if card.count() > 0:
                                card_text = card.inner_text(timeout=3000)
                            else:
                                raise Exception("No rich result-card ancestor found")
                        except Exception:
                            try:
                                card_text = link.locator("xpath=ancestor::div[5]").inner_text(timeout=3000)
                            except Exception:
                                try:
                                    card_text = link.locator("xpath=ancestor::div[4]").inner_text(timeout=3000)
                                except Exception:
                                    card_text = link_text

                        card_text_lower = normalize_county_for_match(card_text)

                        print(f"[SAM] Result card text sample: {card_text[:1000]}")

                        has_state = state_lower in card_text_lower
                        has_county = county_base in card_text_lower
                        has_building = construction_lower in card_text_lower

                        print(
                            f"[SAM] Card match flags: "
                            f"state={has_state}, county={has_county}, construction={has_building}"
                        )

                        if has_state and has_county and has_building:
                            print(f"[SAM] Found matching result card: {link_text} -> {full_url}")
                            matched_result = {
                                "wd_number": link_text,
                                "url": full_url,
                                "text": card_text,
                            }
                            break

                    except Exception as exc:
                        print(f"[SAM] Failed while scanning result card {idx + 1}: {exc}")
                        continue

                if matched_result:
                    break

                if not go_to_next_page(page):
                    break

            browser.close()

            if not matched_result:
                print(f"[SAM] No matching result card found for {county_name}, {state_name}, Building")
                return None

            wd_match = WD_NUMBER_RE.search(matched_result["wd_number"].upper())
            wd_number = wd_match.group(1) if wd_match else "UNKNOWN"

            return {
                "wd_number": wd_number,
                "wd_title": matched_result["wd_number"],
                "source_url": matched_result["url"],
                "detail_url": matched_result["url"],
                "effective_date": None,
            }

        except PlaywrightTimeoutError as exc:
            print(f"[SAM] Playwright timeout during WD discovery: {exc}")
            browser.close()
            return None
        except Exception as exc:
            print(f"[SAM] WD discovery failed: {exc}")
            browser.close()
            return None

def wd_matches_county_state(
    wd_text: str,
    county_name: str,
    state_name: str,
) -> bool:
    text_upper = normalize_county_for_match(wd_text).upper()
    county_base = normalize_county_for_match(county_name).upper()
    state_upper = state_name.strip().upper()

    expected_header = f"COUNTY: {county_name.upper()} IN {state_upper}"

    if expected_header in text_upper:
        return True

    # Some WDs list many counties together instead of using a single County: header.
    if county_base in text_upper and state_upper in text_upper:
        return True

    return False

def fetch_wd_detail_from_sam(
    wd_number: str,
    wd_url: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Supports two cases:
    1. wd_url is already a .txt URL -> fetch directly with requests
    2. wd_url is a SAM detail page -> open in Playwright, click Download, read txt
    """
    if not wd_url:
        print(f"[SAM] No wd_url provided for {wd_number}")
        return None

    # Case 1: direct TXT URL
    if TXT_LINK_RE.search(wd_url):
        print(f"[SAM] Fetching WD TXT directly: {wd_url}")
        resp = requests.get(wd_url, headers=HEADERS, timeout=30)
        resp.raise_for_status()

        text = resp.text
        if not text.strip():
            print("[SAM] Direct TXT response was empty")
            return None

        return {
            "wd_number": wd_number,
            "wd_url": wd_url,
            "text": text,
            "title": wd_number,
        }

    # Case 2: detail page -> use Playwright to click Download
    print(f"[SAM] Opening WD detail page in browser: {wd_url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        try:
            page.goto(wd_url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(3000)

            body_text = page.locator("body").inner_text(timeout=10000)
            print(f"[SAM] Detail page text sample: {body_text[:1500]}")

            download = None

            # Strategy 1: click a visible Download button/link and let Playwright download the file
            download_selectors = [
                lambda: page.get_by_role("button", name=re.compile("download", re.I)).first,
                lambda: page.get_by_role("link", name=re.compile("download", re.I)).first,
                lambda: page.get_by_text(re.compile("download", re.I)).first,
            ]

            for selector_fn in download_selectors:
                try:
                    candidate = selector_fn()
                    candidate.wait_for(timeout=5000)

                    with page.expect_download(timeout=15000) as download_info:
                        candidate.click(force=True)
                    download = download_info.value
                    print("[SAM] Download button clicked successfully")
                    break
                except Exception:
                    continue

            # Strategy 2: if no download event fired, try to find a .txt href on the page
            if download is None:
                print("[SAM] No direct download event; scanning page links for .txt")
                txt_href = None

                for a in page.locator("a").all():
                    try:
                        href = a.get_attribute("href")
                        if href and TXT_LINK_RE.search(href):
                            txt_href = href
                            break
                    except Exception:
                        continue

                if txt_href:
                    print(f"[SAM] Found TXT href in page: {txt_href}")
                    resp = requests.get(txt_href, headers=HEADERS, timeout=30)
                    resp.raise_for_status()

                    text = resp.text
                    if not text.strip():
                        print("[SAM] TXT href response was empty")
                        return None

                    return {
                        "wd_number": wd_number,
                        "wd_url": txt_href,
                        "text": text,
                        "title": wd_number,
                    }

                print("[SAM] No downloadable TXT found on detail page")
                return None

            # Save download to temp path and read it
            print(f"[SAM] Downloaded file: {download.suggested_filename}")

            with tempfile.TemporaryDirectory() as tmpdir:
                save_path = os.path.join(tmpdir, download.suggested_filename)
                download.save_as(save_path)

                with open(save_path, "r", encoding="utf-8", errors="replace") as f:
                    text = f.read()

            if not text.strip():
                print("[SAM] Downloaded file was empty")
                return None

            return {
                "wd_number": wd_number,
                "wd_url": wd_url,
                "text": text,
                "title": wd_number,
            }

        except PlaywrightTimeoutError as exc:
            print(f"[SAM] Playwright timeout while fetching WD detail: {exc}")
            return None
        except Exception as exc:
            print(f"[SAM] WD detail fetch failed: {exc}")
            return None
        finally:
            context.close()
            browser.close()


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

def extract_worker_from_wd(
    wd_data: Dict[str, Any],
    worker_classification: str,
) -> Optional[Dict[str, Any]]:
    text = wd_data.get("text", "")
    if not text:
        return None

    search_terms = WORKER_SEARCH_TERMS.get(
        worker_classification,
        [worker_classification.upper()],
    )

    lines = [line.strip() for line in text.splitlines() if line.strip()]

    for i, line in enumerate(lines):
        upper_line = line.upper()

        if not any(term in upper_line for term in search_terms):
            continue

        print(f"[SAM] Found candidate {worker_classification} line: {line}")

        combined = line
        if i + 1 < len(lines):
            combined = f"{line} {lines[i + 1]}"

        rates = re.findall(r"\d{1,3}\.\d{2}", combined)

        if len(rates) >= 2:
            effective_date = _normalize_effective_date(text)

            return {
                "base_rate": float(rates[0]),
                "fringe_rate": float(rates[1]),
                "effective_date": effective_date,
                "matched_line": combined,
                "worker_classification": worker_classification,
            }

    print(f"[SAM] No {worker_classification} match found")
    return None