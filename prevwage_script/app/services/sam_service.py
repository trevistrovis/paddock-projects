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


def search_sam_for_wd(
    state_name: str,
    county_name: str,
    construction_type: str,
) -> Optional[Dict[str, Any]]:
    expected_header = f"COUNTY: {county_name.upper()} IN {state_name.upper()}"
    print(f"[SAM] WD discovery target: {county_name}, {state_name}, {construction_type}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            page.goto(SAM_BASE_URL, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(3000)

            body_text = page.locator("body").inner_text(timeout=10000)
            print(f"[SAM] Landing page text sample: {body_text[:1500]}")

            # Click DBA path
            pbo_locator = page.get_by_text("Public Buildings or Works", exact=False).first
            pbo_locator.wait_for(timeout=15000)
            pbo_locator.scroll_into_view_if_needed()
            pbo_locator.click(force=True)
            page.wait_for_timeout(5000)

            print(f"[SAM] URL after DBA click: {page.url}")

            # Do not try to read full body immediately; inspect controls
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

            # Wait for likely form controls
            try:
                page.locator("select, input").first.wait_for(timeout=15000)
            except Exception as exc:
                print(f"[SAM] No form controls appeared after DBA click: {exc}")
                browser.close()
                return None

            # Debug what controls exist
            for i in range(min(page.locator("select").count(), 5)):
                try:
                    select_text = page.locator("select").nth(i).text_content() or ""
                    print(f"[SAM] Select[{i}] text sample: {select_text[:500]}")
                except Exception:
                    pass

            for i in range(min(page.locator("input").count(), 5)):
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

            state_filled = False
            county_filled = False
            construction_filled = False

            # Try selects first
            for i in range(page.locator("select").count()):
                sel = page.locator("select").nth(i)
                try:
                    options_text = (sel.text_content() or "").lower()

                    if not state_filled and state_name.lower()[:8] in options_text:
                        sel.select_option(label=state_name)
                        state_filled = True
                        print(f"[SAM] Filled state using select[{i}]")
                        continue

                    if not county_filled and county_name.lower()[:8] in options_text:
                        sel.select_option(label=county_name)
                        county_filled = True
                        print(f"[SAM] Filled county using select[{i}]")
                        continue

                    if not construction_filled and construction_type.lower() in options_text:
                        sel.select_option(label=construction_type.capitalize())
                        construction_filled = True
                        print(f"[SAM] Filled construction using select[{i}]")
                        continue
                except Exception:
                    continue

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

            # Gather candidate WD links
            candidate_urls = []
            seen = set()

            for a in page.locator("a").all():
                try:
                    href = a.get_attribute("href")
                    link_text = (a.inner_text() or "").strip()

                    if not href:
                        continue

                    if "/wage-determination/" in href:
                        full_url = href if href.startswith("http") else "https://sam.gov" + href
                        if full_url not in seen:
                            seen.add(full_url)
                            candidate_urls.append(full_url)
                            print(f"[SAM] Candidate WD link: text='{link_text}' url='{full_url}'")
                except Exception:
                    continue

            print(f"[SAM] Found {len(candidate_urls)} candidate WD URLs")

            if not candidate_urls:
                browser.close()
                return None

            # Validate each candidate by reading its text/TXT and checking county/state
            for candidate_url in candidate_urls[:10]:
                print(f"[SAM] Checking candidate WD detail URL: {candidate_url}")

                try:
                    wd_data = fetch_wd_detail_from_sam(
                        wd_number="UNKNOWN",
                        wd_url=candidate_url,
                    )

                    if not wd_data:
                        continue

                    wd_text = wd_data.get("text", "")
                    if not wd_text:
                        continue

                    wd_text_upper = wd_text.upper()

                    if expected_header not in wd_text_upper:
                        print(
                            f"[SAM] Candidate rejected. "
                            f"Expected header '{expected_header}' not found."
                        )
                        continue

                    wd_match = WD_NUMBER_RE.search(wd_text_upper)
                    wd_number = (
                        wd_match.group(1)
                        if wd_match
                        else f"UNKNOWN-{state_name[:2].upper()}-{county_name[:10].upper().replace(' ', '')}"
                    )

                    print(f"[SAM] Found matching WD for {county_name}, {state_name}")

                    browser.close()
                    return {
                        "wd_number": wd_number,
                        "wd_title": f"{county_name}, {state_name} - {construction_type}",
                        "source_url": candidate_url,
                        "detail_url": candidate_url,
                        "effective_date": None,
                    }

                except Exception as exc:
                    print(f"[SAM] Candidate check failed for {candidate_url}: {exc}")
                    continue

            browser.close()
            print(f"[SAM] No matching WD found for {county_name}, {state_name}")
            return None

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