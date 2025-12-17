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


async def _block_heavy_resources(route) -> None:
    try:
        rtype = route.request.resource_type
        if rtype in {"image", "media", "font"}:
            await route.abort()
            return
    except Exception:
        pass
    await route.continue_()


async def extract_profile_links_from_memberlist(page) -> List[Tuple[str, str, str, str]]:
    """
    Returns list of tuples:
    (name, business, category, profile_url)
    """
    results: List[Tuple[str, str, str, str]] = []

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

            href = await name_link.get_attribute("href")
            if not href:
                continue

            profile_url = urljoin(page.url, href)
            results.append((name, business, category, profile_url))

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
    deduped: Dict[str, Tuple[str, str, str, str]] = {}
    for t in results:
        deduped[t[3]] = t
    return list(deduped.values())


async def scrape_profile(context, base: Tuple[str, str, str, str]) -> Dict[str, Any]:
    name, business, category, profile_url = base

    page = await context.new_page()
    try:
        await page.route("**/*", _block_heavy_resources)
        await page.goto(profile_url, timeout=60_000, wait_until="domcontentloaded")

        phone: Optional[str] = None
        try:
            tel = await page.query_selector('a[href^="tel:"]')
            if tel:
                href = await tel.get_attribute("href")
                if href and href.startswith("tel:"):
                    phone = href.replace("tel:", "").strip()
        except PlaywrightTimeoutError:
            phone = None

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

        return {
            "name": name,
            "business": business,
            "category": category,
            "phone": phone,
            "my_business": section_data["My Business"],
            "top_product": section_data["Top Product"],
            "ideal_referral": section_data["Ideal Referral"],
            "top_problem_solved": section_data["Top Problem Solved"],
            "my_favourite_bni_story": section_data["My Favourite BNI Story"],
        }
    finally:
        await page.close()


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

        async def resolve_to_memberlist(start_url: str) -> str:
            """
            Accepts:
            - /memberlist urls (returns as-is)
            - /index or /chapterdetail urls (tries to find a link containing '/memberlist')
            """
            if "/memberlist" in start_url:
                return start_url

            tmp = await context.new_page()
            try:
                await tmp.goto(start_url, timeout=60_000, wait_until="domcontentloaded")
                # Try common link patterns.
                link = await tmp.query_selector('a[href*="/memberlist"]')
                if link:
                    href = await link.get_attribute("href")
                    if href:
                        return urljoin(tmp.url, href)
                # Fallback: search in page html for /memberlist
                html = await tmp.content()
                m = re.search(r'href="([^"]*/memberlist[^"]*)"', html)
                if m:
                    return urljoin(tmp.url, m.group(1))
            finally:
                await tmp.close()
            raise ValueError(f"Could not resolve to /memberlist from: {start_url}")

        list_page = await context.new_page()
        try:
            memberlist_url = await resolve_to_memberlist(url)
            await list_page.goto(memberlist_url, timeout=60_000, wait_until="domcontentloaded")
            bases = await extract_profile_links_from_memberlist(list_page)
        finally:
            await list_page.close()

        sem = asyncio.Semaphore(profile_concurrency)

        async def worker(b: Tuple[str, str, str, str]) -> Dict[str, Any]:
            async with sem:
                data = await scrape_profile(context, b)
                data["chapter"] = chapter.name
                return data

        members: List[Dict[str, Any]] = await asyncio.gather(*(worker(b) for b in bases))
        await browser.close()

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(members, f, indent=2, ensure_ascii=False)

    return out_path


async def main() -> None:
    chapters = [Chapter(name=c["chapter"], url=c["url"]) for c in CHAPTERS]

    out_dir = os.path.join(os.getcwd(), "output_bni")
    # Limit across chapters too, so you don't DOS the site or get blocked.
    chapter_concurrency = int(os.environ.get("BNI_CHAPTER_CONCURRENCY", "2"))
    profile_concurrency = int(os.environ.get("BNI_PROFILE_CONCURRENCY", "6"))

    sem = asyncio.Semaphore(chapter_concurrency)

    async def run_one(ch: Chapter) -> Optional[str]:
        async with sem:
            try:
                path = await scrape_chapter(ch, out_dir=out_dir, profile_concurrency=profile_concurrency)
                print(f"[OK] {ch.name} -> {path}")
                return path
            except Exception as e:
                print(f"[SKIP/ERR] {ch.name}: {e}")
                return None

    results = await asyncio.gather(*(run_one(ch) for ch in chapters))
    ok = [r for r in results if r]
    print(f"Done. Wrote {len(ok)} chapter JSON files to: {out_dir}")


if __name__ == "__main__":
    asyncio.run(main())


