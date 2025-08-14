import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from playwright.async_api import async_playwright, Browser, Page


BASE_URL = "https://ankergames.net"
GAMES_LIST_URL = f"{BASE_URL}/games-list"


@dataclass
class DownloadItem:
    title: str
    uris: List[str]
    uploadDate: str
    fileSize: str
    repackLinkSource: str


def to_iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def parse_last_updated(text: str) -> Optional[str]:
    # Expect formats like: "Last updated Apr 18, 2025 at 07:20 AM"
    # Extract the date-time segment and parse
    match = re.search(r"([A-Za-z]{3,9} \d{1,2}, \d{4} at \d{1,2}:\d{2} (AM|PM))", text)
    if not match:
        return None
    dt_str = match.group(1)
    for fmt in ("%b %d, %Y at %I:%M %p", "%B %d, %Y at %I:%M %p"):
        try:
            dt = datetime.strptime(dt_str, fmt)
            return to_iso_utc(dt)
        except ValueError:
            continue
    return None


def _days_in_month(year: int, month: int) -> int:
    if month == 12:
        next_month = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        next_month = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    this_month = datetime(year, month, 1, tzinfo=timezone.utc)
    return (next_month - this_month).days


def subtract_months(dt: datetime, months: int) -> datetime:
    year = dt.year
    month = dt.month
    day = dt.day
    month_index = (year * 12 + (month - 1)) - months
    new_year = month_index // 12
    new_month = month_index % 12 + 1
    max_day = _days_in_month(new_year, new_month)
    new_day = min(day, max_day)
    return dt.replace(year=new_year, month=new_month, day=new_day)


def parse_relative_months(text: str) -> Optional[str]:
    # Examples inside a block:
    # "Last Updated - 6 months ago"
    # "Published on, 10 months ago"
    lowered = text.lower()
    now = datetime.now(timezone.utc)

    # Prefer Last Updated
    m = re.search(r"last\s*updated[^\d]*(\d+)\s+month", lowered)
    if m:
        months = int(m.group(1))
        dt = subtract_months(now, months)
        return to_iso_utc(dt)

    # Fallback to Published on
    m = re.search(r"published\s*on[^\d]*(\d+)\s+month", lowered)
    if m:
        months = int(m.group(1))
        dt = subtract_months(now, months)
        return to_iso_utc(dt)

    return None


async def safe_inner_text(el) -> Optional[str]:
    try:
        txt = await el.inner_text()
        return txt.strip()
    except Exception:
        return None


async def get_game_links(page: Page) -> List[str]:
    await page.goto(GAMES_LIST_URL, wait_until="domcontentloaded")

    # Try to click "Load All Games" if present
    try:
        load_all_button = page.locator('button:has-text("Load All Games")')
        if await load_all_button.count() > 0:
            await load_all_button.first.click()
            # Give the Livewire/Alpine stack time to populate
            await page.wait_for_timeout(2000)
            # Wait for network to settle a bit
            with page.expect_network_idle(timeout=15000):
                await page.wait_for_timeout(1000)
    except Exception:
        # Continue even if the button is missing
        pass

    # The grid contains many cards; get all anchors that point to /game/...
    anchors = page.locator('div.grid a[href*="/game/"]')
    count = await anchors.count()
    links = []
    for i in range(count):
        href = await anchors.nth(i).get_attribute("href")
        if not href:
            continue
        if href.startswith("/"):
            href = BASE_URL + href
        if href.startswith(f"{BASE_URL}/game/") and href not in links:
            links.append(href)
    return links


async def extract_title(page: Page) -> str:
    # Part 1: Main title from h1
    title_part_1 = None
    h1 = page.locator("h1")
    if await h1.count() > 0:
        title_part_1 = (await safe_inner_text(h1.first)) or None

    # Part 2: Version badge, supports "V 1.5.0", "v1.0.13", "v 1.0.13"
    version_part = None
    version_pattern = re.compile(r"(?i)^v\s*\d+(?:[._]\d+)*")
    # Prefer animate-glow span
    version_locator = page.locator('span.animate-glow')
    if await version_locator.count() > 0:
        version_text = (await safe_inner_text(version_locator.first)) or ""
        version_text = re.sub(r"\s+", " ", version_text).strip()
        if version_text and (version_pattern.search(version_text) or version_text.upper().startswith("V ")):
            version_part = version_text
    if not version_part:
        # Fallback: scan spans for version-like text
        spans = page.locator('span')
        span_count = await spans.count()
        for i in range(min(span_count, 200)):
            t = (await safe_inner_text(spans.nth(i))) or ""
            t = t.strip()
            if not t:
                continue
            if version_pattern.search(t) or t.upper().startswith("V "):
                version_part = t
                break

    # Part 3: Edition value (e.g., "Multiplayer" or "Complete"). If present, append literal "Edition".
    part3_text = None
    try:
        edition_label = page.locator('span:has-text("Edition")')
        if await edition_label.count() > 0:
            value_span = edition_label.first.locator('xpath=preceding-sibling::span[1]')
            if await value_span.count() > 0:
                part3_text = (await safe_inner_text(value_span.first)) or None
    except Exception:
        pass
    if not part3_text:
        part3_locator = page.locator('span:has-text("Complete")')
        if await part3_locator.count() > 0:
            part3_text = (await safe_inner_text(part3_locator.first)) or None

    # If part3 exists, add literal "Edition"
    parts = [p for p in [title_part_1, version_part, part3_text] if p]
    if part3_text:
        parts.append("Edition")

    return " ".join(parts) if parts else (title_part_1 or "")


async def extract_file_size(page: Page) -> Optional[str]:
    # Look for text like "40.0 GB" or "755.6 MB"
    # Try a few likely containers first to avoid false positives
    candidates = [
        page.locator('div:has(.text-xs):has-text("GB"), div:has(.text-xs):has-text("MB")'),
        page.locator('div:has-text("GB"), div:has-text("MB")'),
        page.locator('span:has-text("GB"), span:has-text("MB")'),
    ]
    pattern = re.compile(r"(\d+(?:\.\d+)?)\s*(GB|MB)")
    for loc in candidates:
        try:
            count = await loc.count()
            for i in range(min(count, 10)):
                txt = await safe_inner_text(loc.nth(i))
                if not txt:
                    continue
                match = pattern.search(txt)
                if match:
                    return f"{match.group(1)} {match.group(2)}"
        except Exception:
            continue
    # Fallback: page-wide search
    try:
        content = await page.content()
        match = pattern.search(content)
        if match:
            return f"{match.group(1)} {match.group(2)}"
    except Exception:
        pass
    return None


async def extract_last_updated_iso(page: Page) -> Optional[str]:
	# По требованию: сначала берём дату из "Last Updated - X months ago";
	# если нет, используем "Published on, Y months ago". Абсолютные даты игнорируем.
	html = await page.content()
	# Сначала Last Updated
	m = re.search(r"last\s*updated[^\d]*(\d+)\s+month", html.lower())
	if m:
		months = int(m.group(1))
		dt = subtract_months(datetime.now(timezone.utc), months)
		return to_iso_utc(dt)
	# Затем Published on
	m = re.search(r"published\s*on[^\d]*(\d+)\s+month", html.lower())
	if m:
		months = int(m.group(1))
		dt = subtract_months(datetime.now(timezone.utc), months)
		return to_iso_utc(dt)
	return None


async def extract_download_link(page: Page) -> Optional[str]:
    # Step 1: Click the main "Download" button to open the modal
    try:
        download_button = page.locator('button:has-text("Download")')
        if await download_button.count() > 0:
            await download_button.first.click()
            await page.wait_for_timeout(500)
    except Exception:
        pass

    # Step 2: Click the token generation button inside the modal
    try:
        generate_button = page.locator('a.download-button:has-text("Download")')
        if await generate_button.count() == 0:
            # Fallback: any button/anchor with Download text inside a dialog
            generate_button = page.locator('a:has-text("Download")')
        if await generate_button.count() > 0:
            await generate_button.first.click()
            # Allow async token creation
            await page.wait_for_timeout(1500)
    except Exception:
        pass

    # Step 3: Wait for the final anchor with "Download Now"
    try:
        final_anchor = page.locator('a:has-text("Download Now")')
        await final_anchor.first.wait_for(state="visible", timeout=15000)
        href = await final_anchor.first.get_attribute("href")
        if href:
            # Normalize whitespace/newlines
            href = re.sub(r"\s+", "", href)
            return href
    except Exception:
        pass
    return None


async def scrape_game(context, game_url: str) -> Optional[DownloadItem]:
    page = await context.new_page()
    try:
        await page.goto(game_url, wait_until="domcontentloaded")
        # Ensure large viewport so lg: elements are visible
        await page.set_viewport_size({"width": 1366, "height": 900})

        title = await extract_title(page)
        upload_iso = await extract_last_updated_iso(page)
        file_size = await extract_file_size(page)
        download_uri = await extract_download_link(page)

        if not title:
            title = ""
        if not upload_iso:
            upload_iso = ""
        if not file_size:
            file_size = ""
        uris = [download_uri] if download_uri else []

        return DownloadItem(
            title=title,
            uris=uris,
            uploadDate=upload_iso,
            fileSize=file_size,
            repackLinkSource=game_url,
        )
    finally:
        await page.close()


async def main(limit: Optional[int] = None, output_path: str = "ankergames.json"):
    async with async_playwright() as p:
        browser: Browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ))
        page = await context.new_page()

        try:
            links = await get_game_links(page)
        finally:
            await page.close()

        if limit is not None:
            links = links[:limit]

        results: List[DownloadItem] = []
        for idx, url in enumerate(links, start=1):
            print(f"[{idx}/{len(links)}] {url}")
            try:
                item = await scrape_game(context, url)
                if item:
                    results.append(item)
            except Exception as e:
                print(f"Failed to scrape {url}: {e}")

        await context.close()
        await browser.close()

        data = {
            "name": "AnkerGames",
            "downloads": [
                {
                    "title": r.title,
                    "uris": r.uris,
                    "uploadDate": r.uploadDate,
                    "fileSize": r.fileSize,
                    "repackLinkSource": r.repackLinkSource,
                }
                for r in results
            ],
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Saved to {output_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="AnkerGames parser")
    parser.add_argument("--limit", type=int, default=None, help="Ограничить количество игр для парсинга")
    parser.add_argument("--output", type=str, default="ankergames.json", help="Путь к выходному JSON")
    args = parser.parse_args()

    asyncio.run(main(limit=args.limit, output_path=args.output))



