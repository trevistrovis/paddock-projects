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

RATE_LINE_RE = re.compile(
    r"""
    (?P<trade>MILLWRIGHT[^\n\r]*?)
    (?P<base>\d{1,3}(?:\.\d{2})?)
    \s*
    (?P<fringe>\d{1,3}(?:\.\d{2})?|[A-Z].*)
    """,
    re.IGNORECASE | re.VERBOSE,
)

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
    """
    SAM county dropdown appears to list county names without the word 'County'.
    Example:
      'Philadelphia County' -> 'Philadelphia'
    """
    try:
        county_base = county_name.replace(" County", "").strip()

        county_input = page.locator('input[aria-label="wd-county"]').first
        county_input.wait_for(timeout=5000)
        county_input.click()
        county_input.fill("")
        county_input.fill(county_base)
        page.wait_for_timeout(1500)

        # Log visible options for debugging
        try:
            options = page.locator('[role="option"]')
            option_count = options.count()
            print(f"[SAM] County option count: {option_count}")

            for i in range(min(option_count, 15)):
                try:
                    opt_text = (options.nth(i).inner_text() or "").strip()
                    print(f"[SAM] County option[{i}]: {opt_text}")
                except Exception:
                    pass
        except Exception:
            pass

        option_clicked = False
        option_candidates = [
            page.get_by_role("option", name=re.compile(rf"^{re.escape(county_base)}$", re.I)).first,
            page.get_by_text(re.compile(rf"^{re.escape(county_base)}$", re.I)).first,
            page.locator(f'text="{county_base}"').first,
        ]

        for option in option_candidates:
            try:
                option.wait_for(timeout=4000)
                option.click(force=True)
                option_clicked = True
                print(f"[SAM] Clicked county option: {county_base}")
                break
            except Exception:
                continue

        if not option_clicked:
            county_input.press("ArrowDown")
            page.wait_for_timeout(500)
            county_input.press("Enter")
            page.wait_for_timeout(1000)
            print("[SAM] Used keyboard fallback for county")

        page.wait_for_timeout(1000)

        selected = county_input.input_value().strip()
        print(f"[SAM] County value now: {selected}")

        # Some SAM widgets clear the input after selection and show the chosen
        # value elsewhere in the UI, so validate using visible page text too.
        page_text = page.locator("body").inner_text(timeout=5000)
        print(f"[SAM] County validation text sample: {page_text[:1000]}")

        if selected.lower() == county_base.lower():
            return True

        if re.search(rf"\b{re.escape(county_base)}\b", page_text, re.I):
            print(f"[SAM] County confirmed in visible page text: {county_base}")
            return True

        return False

    except Exception as exc:
        print(f"[SAM] Failed to select county: {exc}")
        return False

def search_sam_for_wd(
    state_name: str,
    county_name: str,
    construction_type: str,  # ignored; Building is hardcoded below
) -> Optional[Dict[str, Any]]:
    print(f"[SAM] WD discovery target: {county_name}, {state_name}, Building")

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

            select_count = page.locator("select").count()
            input_count = page.locator("input").count()
            button_count = page.locator("button").count()

            print(
                f"[SAM] After DBA click counts: "
                f"selects={select_count}, inputs={input_count}, buttons={button_count}"
            )

            try:
                visible_text = page.locator("main").text_content(timeout=5000)
                print(f"[SAM] Main text sample after DBA click: {(visible_text or '')[:1500]}")
            except Exception as exc:
                print(f"[SAM] Could not read main text after DBA click: {exc}")

            try:
                page.locator("select, input").first.wait_for(timeout=15000)
            except Exception as exc:
                print(f"[SAM] No form controls appeared after DBA click: {exc}")
                browser.close()
                return None

            for i in range(min(page.locator("select").count(), 5)):
                try:
                    select_text = page.locator("select").nth(i).text_content() or ""
                    print(f"[SAM] Select[{i}] text sample: {select_text[:500]}")
                except Exception:
                    pass

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

            # State selection: keep your existing helper if it's working
            state_filled = fill_autocomplete_field(
                page,
                aria_label="wd-state",
                value=state_name,
                debug_name="state",
            )

            # County selection: exact county option, but SAM lists counties without "County"
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

            page.wait_for_timeout(1500)

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

            # Prefer the filtered result if the page narrowed correctly
            if "Showing 1 - 1 of 1 results" in result_text:
                print("[SAM] Search narrowed to exactly 1 result")

            result_url = None
            result_label = None

            links = page.locator('a[href*="/wage-determination/"]')
            link_count = links.count()
            print(f"[SAM] WD result link count on page: {link_count}")

            for i in range(link_count):
                try:
                    link = links.nth(i)
                    href = link.get_attribute("href")
                    text = (link.inner_text() or "").strip()

                    if not href or not text:
                        continue

                    wd_match = WD_NUMBER_RE.search(text.upper())
                    if not wd_match:
                        continue

                    result_url = href if href.startswith("http") else "https://sam.gov" + href
                    result_label = text
                    print(f"[SAM] Selected visible filtered WD result: {result_label} -> {result_url}")
                    break

                except Exception:
                    continue

            browser.close()

            if not result_url:
                print("[SAM] No filtered WD result found")
                return None

            wd_match = WD_NUMBER_RE.search(result_label.upper())
            wd_number = wd_match.group(1) if wd_match else "UNKNOWN"

            return {
                "wd_number": wd_number,
                "wd_title": result_label,
                "source_url": result_url,
                "detail_url": result_url,
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


def extract_millwright_from_wd(wd_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    text = wd_data.get("text", "")
    if not text:
        return None

    lines = [line.strip() for line in text.splitlines() if line.strip()]

    for i, line in enumerate(lines):
        if "MILLWRIGHT" not in line.upper():
            continue

        print(f"[SAM] Found candidate Millwright line: {line}")

        rate_match = re.search(r"(\d{1,3}\.\d{2})", line)
        if not rate_match and i + 1 < len(lines):
            rate_match = re.search(r"(\d{1,3}\.\d{2})", lines[i + 1])

        if not rate_match:
            continue

        base_rate = float(rate_match.group(1))

        fringe_rate = 0.0
        fringe_match = re.search(r"(\d{1,3}\.\d{2}).*?(\d{1,3}\.\d{2})", line)
        if fringe_match:
            base_rate = float(fringe_match.group(1))
            fringe_rate = float(fringe_match.group(2))
        elif i + 1 < len(lines):
            next_line = lines[i + 1]
            next_match = re.search(r"(\d{1,3}\.\d{2})", next_line)
            if next_match and float(next_match.group(1)) != base_rate:
                fringe_rate = float(next_match.group(1))

        effective_date = _normalize_effective_date(text)

        return {
            "base_rate": base_rate,
            "fringe_rate": fringe_rate,
            "effective_date": effective_date,
            "matched_line": line,
        }

    print("[SAM] No Millwright match found")
    return None