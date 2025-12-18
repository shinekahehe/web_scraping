import asyncio
import json
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urldefrag, urljoin

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

from bni_chapters import CHAPTERS


TARGET_SECTIONS = (
    "My Business",
    "Top Product",
    "Ideal Referral",
    "Top Problem Solved",
    "My Favourite BNI Story",
)


@dataclass(frozen=True)
class Chapter:
    name: str
    url: str


def slugify(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "chapter"


def normalize_url(raw: str) -> str:
    # Removes `#...` fragments some of your copied links have.
    return urldefrag(raw.strip())[0]


def normalize_phone(phone_str: Optional[str]) -> Optional[str]:
    """
    Normalizes phone numbers to 10-digit format (removes country codes, spaces, dashes, etc.)
    Returns None if the phone doesn't look valid.
    """
    if not phone_str:
        return None
    # Remove common separators and country codes
    cleaned = re.sub(r"[\s\+\-\(\)\.]", "", str(phone_str))
    # Remove leading country codes (91, 0091, +91, etc.)
    if cleaned.startswith("0091") and len(cleaned) == 13:
        cleaned = cleaned[4:]
    elif cleaned.startswith("91") and len(cleaned) == 12:
        cleaned = cleaned[2:]
    # Extract 10-digit phone starting with 6-9 (Indian mobile)
    if len(cleaned) >= 10 and cleaned[0] in "6789":
        return cleaned[-10:] if len(cleaned) > 10 else cleaned
    return None


async def _block_heavy_resources(route) -> None:
    try:
        rtype = route.request.resource_type
        if rtype in {"image", "media", "font"}:
            await route.abort()
            return
    except Exception:
        pass
    await route.continue_()


async def extract_profile_links_from_memberlist(
    page,
) -> List[Tuple[str, str, str, Optional[str], str]]:
    """
    Returns list of tuples:
    (name, business, category, phone, profile_url)
    """
    results: List[Tuple[str, str, str, Optional[str], str]] = []

    await page.wait_for_selector("table.listtables tbody tr", timeout=60_000)

    while True:
        await page.wait_for_selector("table.listtables tbody tr", timeout=60_000)
        rows = await page.query_selector_all("table.listtables tbody tr")

        for row in rows:
            cells = await row.query_selector_all("td")
            if len(cells) < 3:
                continue

            name_link = await cells[0].query_selector("a")
            if not name_link:
                continue

            name = (await name_link.inner_text()).strip()
            business = (await cells[1].inner_text()).strip()
            category = (await cells[2].inner_text()).strip()

            # Phone can be present in a hidden column (e.g. <td style="display:none"><bdi>...</bdi></td>)
            # Search ALL cells for phone patterns (not just cells[3], as column order may vary)
            phone: Optional[str] = None
            try:
                # First, try to find a <bdi> tag in any cell (common pattern for hidden phone)
                # Use inner_html to get text even from hidden elements
                for cell in cells:
                    bdi = await cell.query_selector("bdi")
                    if bdi:
                        phone_txt = (await bdi.inner_text()).strip()
                        if phone_txt:
                            phone = normalize_phone(phone_txt)
                            if phone:
                                break
                
                # If no <bdi> found via inner_text, try getting raw HTML to catch hidden elements
                if not phone:
                    for cell in cells:
                        cell_html = await cell.inner_html()
                        # Look for <bdi> in raw HTML (works even if display:none)
                        bdi_match = re.search(r'<bdi[^>]*>([^<]+)</bdi>', cell_html, re.IGNORECASE)
                        if bdi_match:
                            phone_txt = bdi_match.group(1).strip()
                            if phone_txt:
                                phone = normalize_phone(phone_txt)
                                if phone:
                                    break
                
                # If still no phone, search all cell text for phone patterns
                if not phone:
                    for cell in cells:
                        cell_text = (await cell.inner_text()).strip()
                        if not cell_text:
                            continue
                        # Match Indian phone patterns: +91, spaces, dashes, 10 digits starting with 6-9
                        # Try multiple patterns to catch various formats
                        patterns = [
                            r"(?:\+?91[-\s]?)?([6-9]\d{9})",  # Standard format
                            r"([6-9]\d{9})",  # Just 10 digits
                        ]
                        for pattern in patterns:
                            m = re.search(pattern, cell_text)
                            if m:
                                candidate = normalize_phone(m.group(1))
                                if candidate:
                                    phone = candidate
                                    break
                        if phone:
                            break
                
                # Last resort: search raw HTML for phone patterns
                if not phone:
                    for cell in cells:
                        cell_html = await cell.inner_html()
                        patterns = [
                            r"(?:\+?91[-\s]?)?([6-9]\d{9})",  # Standard format
                            r"([6-9]\d{9})",  # Just 10 digits
                        ]
                        for pattern in patterns:
                            m = re.search(pattern, cell_html)
                            if m:
                                candidate = normalize_phone(m.group(1))
                                if candidate:
                                    phone = candidate
                                    break
                        if phone:
                            break
            except Exception:
                phone = None

            href = await name_link.get_attribute("href")
            if not href:
                continue

            profile_url = urljoin(page.url, href)
            results.append((name, business, category, phone, profile_url))

        next_button = await page.query_selector('a[title="Next"]')
        if not next_button:
            break
        cls = (await next_button.get_attribute("class")) or ""
        if "disabled" in cls:
            break

        # Click next and wait for table refresh quickly.
        await next_button.click()
        await page.wait_for_timeout(400)

    # De-dupe by profile_url (some pages can repeat due to weird nav)
    deduped: Dict[str, Tuple[str, str, str, Optional[str], str]] = {}
    for t in results:
        deduped[t[4]] = t
    return list(deduped.values())


async def scrape_profile(
    context, base: Tuple[str, str, str, Optional[str], str]
) -> Dict[str, Any]:
    name, business, category, phone_from_list, profile_url = base

    page = await context.new_page()
    try:
        await page.route("**/*", _block_heavy_resources)
        # Some profiles are slow; retry once before giving up.
        last_err: Optional[Exception] = None
        for attempt in range(2):
            try:
                await page.goto(profile_url, timeout=90_000, wait_until="domcontentloaded")
                last_err = None
                break
            except Exception as e:
                last_err = e
                await page.wait_for_timeout(600 * (attempt + 1))
        if last_err:
            data = {
                "name": name,
                "business": business,
                "category": category,
                "phone": phone_from_list or None,
                "top_product": None,
                "ideal_referral": None,
                "top_problem_solved": None,
                "my_favourite_bni_story": None,
                "_error": f"profile_goto_failed: {last_err}",
            }
            # Only include my_business when present (keeps output clean for chapters where it is always empty)
            # (No-op here since it's unset on failures.)
            return data

        # --- PHONE ---
        # Different BNI templates expose phone differently:
        # - Sometimes as a simple <a href="tel:...">
        # - Sometimes inside .memberContactDetails, where the visible text is the phone number
        # - Sometimes only as plain text (no tel: link)
        phone: Optional[str] = normalize_phone(phone_from_list)
        try:
            tel = await page.query_selector('a[href^="tel:"]')
            if tel:
                href = await tel.get_attribute("href")
                if href and href.startswith("tel:"):
                    phone = normalize_phone(href.replace("tel:", "").strip()) or phone
        except Exception:
            pass

        # Fallback 1: `.memberContactDetails a[href^='tel:']` (chapterdetails.py style)
        if not phone:
            try:
                tel2 = await page.query_selector(".memberContactDetails a[href^='tel:']")
                if tel2:
                    txt = (await tel2.inner_text()).strip()
                    if txt:
                        phone = normalize_phone(txt)
                    if not phone:
                        href2 = await tel2.get_attribute("href")
                        if href2 and href2.startswith("tel:"):
                            phone = normalize_phone(href2.replace("tel:", "").strip())
            except Exception:
                pass

        # Fallback 2: regex from contact details text
        if not phone:
            try:
                contact = await page.query_selector(".memberContactDetails")
                if contact:
                    blob = (await contact.inner_text()).strip()
                    # Match common India formats (+91, spaces, dashes)
                    m = re.search(r"(?:\+?91[-\s]?)?([6-9]\d{9})", blob)
                    if m:
                        phone = normalize_phone(m.group(1))
            except Exception:
                pass

        # Extract "My Business" from .widgetMemberTxtVideo section (special case)
        my_business: Optional[str] = None
        try:
            my_business_content = await page.evaluate("""
                () => {
                    const section = document.querySelector('.widgetMemberTxtVideo');
                    if (section) {
                        const h2 = section.querySelector('h2');
                        if (h2 && h2.textContent.includes('My Business')) {
                            const p = section.querySelector('p');
                            return p ? p.textContent.trim() : '';
                        }
                    }
                    return '';
                }
            """)
            my_business = (my_business_content or "").strip() or None
        except Exception:
            pass

        section_data: Dict[str, Optional[str]] = {k: None for k in TARGET_SECTIONS}
        try:
            # Not all profiles have these; don't hard-wait long.
            await page.wait_for_selector(".widgetProfile .rowTwoCol h3", timeout=5_000)
            headings = await page.query_selector_all(".widgetProfile .rowTwoCol h3")
            for h3 in headings:
                title = (await h3.inner_text()).strip()
                if title in section_data:
                    value = await h3.evaluate(
                        "el => el.nextElementSibling ? el.nextElementSibling.innerText : ''"
                    )
                    section_data[title] = (value or "").strip() or None
        except PlaywrightTimeoutError:
            pass

        # If "My Business" wasn't found in widgetMemberTxtVideo, check widgetProfile as fallback
        if not my_business:
            my_business = section_data.get("My Business")

        data = {
            "name": name,
            "business": business,
            "category": category,
            "phone": phone,
            "my_business": my_business,  # Always include, even if null
            "top_product": section_data["Top Product"],
            "ideal_referral": section_data["Ideal Referral"],
            "top_problem_solved": section_data["Top Problem Solved"],
            "my_favourite_bni_story": section_data["My Favourite BNI Story"],
        }
        return data
    finally:
        await page.close()

async def _prepare_member_list_page(page, start_url: str) -> None:
    """
    Navigates to start_url and ensures the member list table is visible.

    Supports:
    - direct /memberlist URLs
    - /index URLs (often have a Members tab or a predictable /memberlist sibling)
    - /chapterdetail URLs (often load Members table after clicking a tab)
    """
    await page.goto(start_url, timeout=90_000, wait_until="domcontentloaded")

    # If already on a member list page, we're done.
    if "/memberlist" in page.url:
        await page.wait_for_selector("table.listtables tbody tr", timeout=60_000)
        return

    # Heuristic: many sites have .../en-IN/index and .../en-IN/memberlist
    if page.url.endswith("/index"):
        guess = page.url[:-len("/index")] + "/memberlist"
        try:
            await page.goto(guess, timeout=60_000, wait_until="domcontentloaded")
            await page.wait_for_selector("table.listtables tbody tr", timeout=10_000)
            return
        except Exception:
            # fall through to clicking "Members"
            await page.goto(start_url, timeout=90_000, wait_until="domcontentloaded")

    # Try clicking a "Members" / "Chapter Members" / "Show Members" tab/link
    # (your CSS snippet shows `.nav-tabs` / `.leadership_tab` patterns).
    # IMPORTANT: On some chapterdetail pages, "Show Members" is NOT an <a>, it's a button/div.
    # We'll try both locator-based clicks and a JS scan (like your chapterdetails.py).
    candidates = [
        "a:has-text('Members')",
        "a:has-text('MEMBERS')",
        "a:has-text('Chapter Members')",
        "a:has-text('CHAPTER MEMBERS')",
        "a:has-text('Show Members')",
        "a:has-text('SHOW MEMBERS')",
        "text=Show Members",
        "text=Chapter Members",
        "li a:has-text('Members')",
        "li a:has-text('Chapter Members')",
        ".nav-tabs a:has-text('Members')",
        ".nav-tabs a:has-text('Chapter Members')",
        ".leadership_tab a:has-text('Members')",
        ".leadership_tab a:has-text('Chapter Members')",
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel).first
            if await loc.count():
                # Many of these trigger a navigation to /memberlist. Try to catch it quickly.
                try:
                    async with page.expect_navigation(timeout=8_000):
                        await loc.click(timeout=5_000)
                except Exception:
                    await loc.click(timeout=5_000)
                await page.wait_for_timeout(800)
                if "/memberlist" in page.url:
                    await page.wait_for_selector("table.listtables tbody tr", timeout=60_000)
                    return
                await page.wait_for_selector("table.listtables tbody tr", timeout=20_000)
                return
        except Exception:
            continue

    # JS scan fallback: find any element whose *exact* text is 'Show Members' and click it.
    # (This mirrors your working chapterdetails.py approach.)
    try:
        before = page.url
        await page.evaluate(
            """
            () => {
              const el = [...document.querySelectorAll('*')]
                .find(e => e && e.textContent && e.textContent.trim() === 'Show Members');
              if (el) el.click();
            }
            """
        )
        # Wait for navigation or table
        await page.wait_for_timeout(1200)
        if page.url != before and "/memberlist" in page.url:
            await page.wait_for_selector("table.listtables tbody tr", timeout=60_000)
            return
        await page.wait_for_selector("table.listtables tbody tr", timeout=20_000)
        return
    except Exception:
        pass

    # Another JS scan: partial match for "Members" in case of different capitalization/whitespace.
    try:
        await page.evaluate(
            """
            () => {
              const txt = (s) => (s || '').replace(/\\s+/g, ' ').trim().toLowerCase();
              const el = [...document.querySelectorAll('a,button,li,div,span')]
                .find(e => {
                  const t = txt(e.textContent);
                  return t === 'show members' || t === 'chapter members' || t === 'members';
                });
              if (el) el.click();
            }
            """
        )
        await page.wait_for_timeout(1200)
        if "/memberlist" in page.url:
            await page.wait_for_selector("table.listtables tbody tr", timeout=60_000)
            return
        await page.wait_for_selector("table.listtables tbody tr", timeout=20_000)
        return
    except Exception:
        pass

    # Fallback: search HTML for a /memberlist link and navigate to it
    html = await page.content()
    # Some sites embed the member list link as plain text or in scripts.
    m = re.search(r"(https?://[^\s\"']*/memberlist[^\s\"']+)", html)
    if m:
        await page.goto(m.group(1), timeout=60_000, wait_until="domcontentloaded")
        await page.wait_for_selector("table.listtables tbody tr", timeout=60_000)
        return
    m = re.search(r'href="([^"]*/memberlist[^"]*)"', html)
    if m:
        await page.goto(urljoin(page.url, m.group(1)), timeout=60_000, wait_until="domcontentloaded")
        await page.wait_for_selector("table.listtables tbody tr", timeout=60_000)
        return

    raise ValueError(f"Could not reach member list table from: {start_url}")


async def scrape_chapter(chapter: Chapter, *, out_dir: str, profile_concurrency: int = 6) -> str:
    """
    Scrapes one chapter and writes a JSON file.
    Returns output file path.
    """
    url = normalize_url(chapter.url)

    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{slugify(chapter.name)}.json")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()

        # Block heavy resources for ALL pages in this context.
        await context.route("**/*", _block_heavy_resources)

        list_page = await context.new_page()
        try:
            await _prepare_member_list_page(list_page, url)
            bases = await extract_profile_links_from_memberlist(list_page)
        finally:
            await list_page.close()

        sem = asyncio.Semaphore(profile_concurrency)

        async def worker(b: Tuple[str, str, str, Optional[str], str]) -> Dict[str, Any]:
            async with sem:
                try:
                    data = await scrape_profile(context, b)
                except Exception as e:
                    name, business, category, phone_from_list, profile_url = b
                    data = {
                        "name": name,
                        "business": business,
                        "category": category,
                        "phone": phone_from_list or None,
                        "top_product": None,
                        "ideal_referral": None,
                        "top_problem_solved": None,
                        "my_favourite_bni_story": None,
                        "_error": f"profile_failed: {e}",
                        "_profile_url": profile_url,
                    }
                data["chapter"] = chapter.name
                return data

        # Don't let one failing profile kill the entire chapter.
        members: List[Dict[str, Any]] = await asyncio.gather(*(worker(b) for b in bases), return_exceptions=False)
        await browser.close()

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(members, f, indent=2, ensure_ascii=False)

    return out_path


async def main() -> None:
    chapters = [Chapter(name=c["chapter"], url=c["url"]) for c in CHAPTERS]
    only_re = os.environ.get("BNI_ONLY_CHAPTER_REGEX")
    if only_re:
        rx = re.compile(only_re, re.IGNORECASE)
        chapters = [c for c in chapters if rx.search(c.name)]

    # Limit across chapters too, so you don't DOS the site or get blocked.
    chapter_concurrency = int(os.environ.get("BNI_CHAPTER_CONCURRENCY", "2"))
    profile_concurrency = int(os.environ.get("BNI_PROFILE_CONCURRENCY", "6"))

    sem = asyncio.Semaphore(chapter_concurrency)

    async def run_one(ch: Chapter) -> Optional[str]:
        async with sem:
            try:
                # Separate folder for chapterdetail/coregroupdetail sources (like your chapterdetails.py flow)
                if "chapterdetail" in ch.url or "coregroupdetail" in ch.url:
                    out_dir = os.path.join(os.getcwd(), "output_bni_chapterdetails")
                else:
                    out_dir = os.path.join(os.getcwd(), "output_bni")

                path = await scrape_chapter(ch, out_dir=out_dir, profile_concurrency=profile_concurrency)
                print(f"[OK] {ch.name} -> {path}")
                return path
            except Exception as e:
                print(f"[SKIP/ERR] {ch.name}: {e}")
                return None

    results = await asyncio.gather(*(run_one(ch) for ch in chapters))
    ok = [r for r in results if r]
    print(f"Done. Wrote {len(ok)} chapter JSON files.")


if __name__ == "__main__":
    asyncio.run(main())


