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
    """
    Browser-automation version:
    - opens SAM wage determinations
    - searches for county/state/construction type
    - tries to capture WD number + detail URL
    - if possible, captures TXT download URL
    """

    search_phrase = f"{state_name} {county_name} {construction_type}"
    print(f"[SAM] WD discovery search phrase: {search_phrase}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            page.goto(SAM_BASE_URL, wait_until="networkidle", timeout=60000)

            # This part may need selector tweaks depending on the page.
            # We intentionally use broader text/role selectors first.
            page.get_by_text("Public Buildings or Works", exact=False).click(timeout=15000)
            page.get_by_text("Get started searching wage determinations", exact=False).click(timeout=15000)

            # Try to find a generic search input on the resulting page
            search_input = None
            for locator in [
                page.locator('input[type="search"]').first,
                page.locator('input[placeholder*="Search"]').first,
                page.locator('input').first,
            ]:
                try:
                    locator.wait_for(timeout=5000)
                    search_input = locator
                    break
                except Exception:
                    pass

            if search_input is None:
                print("[SAM] Could not find a search input")
                return None

            search_input.fill(search_phrase)
            search_input.press("Enter")
            page.wait_for_load_state("networkidle", timeout=30000)

            content = page.content()
            text = page.locator("body").inner_text(timeout=10000)

            # Try to find a WD number in the rendered page text
            wd_match = WD_NUMBER_RE.search(text)
            wd_number = wd_match.group(1) if wd_match else None

            # Try to capture a likely WD detail link
            detail_url = None
            for a in page.locator("a").all():
                try:
                    href = a.get_attribute("href")
                    if href and "/wage-determination/" in href:
                        detail_url = href
                        if detail_url.startswith("/"):
                            detail_url = "https://sam.gov" + detail_url
                        break
                except Exception:
                    continue

            # If we found a detail URL, open it and look for the TXT download link
            txt_url = None
            if detail_url:
                detail_page = browser.new_page()
                detail_page.goto(detail_url, wait_until="networkidle", timeout=60000)

                for a in detail_page.locator("a").all():
                    try:
                        href = a.get_attribute("href")
                        if href and TXT_LINK_RE.search(href):
                            txt_url = href
                            break
                    except Exception:
                        continue

                # Fallback: inspect rendered HTML/text if needed
                detail_text = detail_page.locator("body").inner_text(timeout=10000)
                if not wd_number:
                    wd_match = WD_NUMBER_RE.search(detail_text)
                    wd_number = wd_match.group(1) if wd_match else None

                detail_page.close()

            browser.close()

            if not detail_url:
                print("[SAM] No detail URL found")
                return None

            return {
                "wd_number": wd_number,
                "wd_title": f"{county_name}, {state_name} - {construction_type}",
                "source_url": txt_url or detail_url,
                "detail_url": detail_url,
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

        return {
            "wd_number": wd_number,
            "wd_url": wd_url,
            "text": resp.text,
            "title": wd_number,
        }

    # Case 2: detail page -> use Playwright to click Download
    print(f"[SAM] Opening WD detail page in browser: {wd_url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        try:
            page.goto(wd_url, wait_until="networkidle", timeout=60000)

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
                        candidate.click()
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

                    return {
                        "wd_number": wd_number,
                        "wd_url": txt_href,
                        "text": text,
                        "title": wd_number,
                    }

                print("[SAM] No downloadable TXT found on detail page")
                return None

            # Save download to temp path and read it
            with tempfile.TemporaryDirectory() as tmpdir:
                save_path = os.path.join(tmpdir, download.suggested_filename)
                download.save_as(save_path)

                with open(save_path, "r", encoding="utf-8", errors="replace") as f:
                    text = f.read()

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