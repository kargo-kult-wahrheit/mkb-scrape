"""Scraper for extracting MKB-10 codes from stetoskop.info.

The scraper is intentionally written without external dependencies other than
``requests`` and ``beautifulsoup4`` so that it can easily run inside a Docker
container or a local Python environment.

The website structure may evolve over time. The parsing logic therefore tries
several strategies when extracting the individual codes to remain reasonably
robust against small layout changes.
"""

from __future__ import annotations

import csv
import logging
import re
import time
from collections.abc import Iterable
from dataclasses import dataclass
from html import unescape
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.stetoskop.info"
# Entry point for the ICD-10 catalogue on stetoskop.info. The portal recently
# reorganized its URLs under ``/medjunarodna-klasifikacija-bolesti`` instead of
# the previous ``/mkb`` path, so we default to the new location here.
INDEX_PATH = "/mkb"
CATALOG_PATH_PREFIX = "/medjunarodna-klasifikacija-bolesti"
INDEX_URL = f"{BASE_URL}{INDEX_PATH}"

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class MKBEntry:
    """Represents a single MKB record."""

    code: str
    serbian: str
    latin: str


class MKBScraper:
    """Scrape the stetoskop.info MKB pages into structured entries."""

    def __init__(
        self,
        base_url: str = BASE_URL,
        index_path: str = INDEX_PATH,
        catalog_path_prefix: str = CATALOG_PATH_PREFIX,
        delay: float = 0.2,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.index_path = index_path if index_path.startswith("/") else f"/{index_path}"
        self.index_url = f"{self.base_url}{self.index_path}"
        self.catalog_path_prefix = (
            catalog_path_prefix
            if catalog_path_prefix.startswith("/")
            else f"/{catalog_path_prefix}"
        )
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.setdefault(
            "User-Agent",
            "Mozilla/5.0 (compatible; mkb-scraper/1.0; +https://www.batut.org.rs)",
        )

    def scrape(self) -> list[MKBEntry]:
        """Scrape every discoverable MKB entry from the site."""

        entries: list[MKBEntry] = []
        LOGGER.info("Fetching index page: %s", self.index_url)
        index_html = self._fetch(self.index_url)
        index_soup = BeautifulSoup(index_html, "html.parser")

        index_entries = self._parse_entries(index_soup)
        LOGGER.debug("Found %d entries directly on the index page", len(index_entries))
        entries.extend(index_entries)

        catalog_pages = self._collect_catalog_urls(index_soup)
        LOGGER.info("Discovered %d catalogue pages", len(catalog_pages))

        for idx, page_url in enumerate(catalog_pages, start=1):
            LOGGER.info("Fetching page %d/%d: %s", idx, len(catalog_pages), page_url)
            html = self._fetch(page_url)
            soup = BeautifulSoup(html, "html.parser")

            page_entries = self._parse_entries(soup)
            LOGGER.debug("Found %d entries on %s", len(page_entries), page_url)
            entries.extend(page_entries)

            if self.delay and idx < len(catalog_pages):
                time.sleep(self.delay)

        unique_entries = _deduplicate(entries)
        LOGGER.info("Collected %d unique entries", len(unique_entries))
        return sorted(unique_entries, key=lambda entry: _code_sort_key(entry.code))

    def _fetch(self, url: str) -> str:
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text

    def _collect_catalog_urls(self, soup: BeautifulSoup) -> list[str]:
        catalog_urls: list[str] = []
        seen: set[str] = set()
        base_netloc = urlparse(self.base_url).netloc

        for anchor in soup.find_all("a", href=True):
            href = anchor["href"].strip()
            if not href or href.startswith("#"):
                continue

            absolute = urljoin(f"{self.base_url}/", href)
            parsed = urlparse(absolute)
            if parsed.scheme not in {"http", "https"}:
                continue
            if parsed.netloc != base_netloc:
                continue

            path = parsed.path.rstrip("/")
            if not path or not path.startswith(self.catalog_path_prefix):
                continue

            normalized = f"{parsed.scheme}://{parsed.netloc}{path}"
            if parsed.query:
                normalized = f"{normalized}?{parsed.query}"

            if normalized not in seen:
                catalog_urls.append(normalized)
                seen.add(normalized)

        return sorted(catalog_urls)

    def _parse_entries(self, soup: BeautifulSoup) -> list[MKBEntry]:
        entries: list[MKBEntry] = []
        entries.extend(self._parse_from_tables(soup))
        if entries:
            return entries
        entries.extend(self._parse_from_structured_blocks(soup))
        if entries:
            return entries
        entries.extend(self._parse_from_text_blocks(soup))
        return entries

    def _parse_from_tables(self, soup: BeautifulSoup) -> list[MKBEntry]:
        entries: list[MKBEntry] = []
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cells = [
                    _normalize_text(cell.get_text(" ", strip=True))
                    for cell in row.find_all(["td", "th"])
                ]
                if not cells or any("šifra" in cell.lower() for cell in cells):
                    continue
                if len(cells) < 2:
                    continue
                code = cells[0]
                if not _is_code(code):
                    continue
                serbian = cells[1]
                latin = cells[2] if len(cells) >= 3 else ""
                entries.append(MKBEntry(code=code, serbian=serbian, latin=latin))
        return entries

    def _parse_from_structured_blocks(self, soup: BeautifulSoup) -> list[MKBEntry]:
        entries: list[MKBEntry] = []
        candidates = soup.find_all(
            lambda tag: tag.name in {"div", "li"}
            and tag.get("class")
            and any("mkb" in cls.lower() for cls in tag.get("class", []))
        )
        for container in candidates:
            code_element = _find_first_matching(
                container,
                class_substrings=["sifra", "code", "oznaka"],
            )
            serbian_element = _find_first_matching(
                container,
                class_substrings=["sr", "opis", "naziv"],
                exclude=code_element,
            )
            latin_element = _find_first_matching(
                container,
                class_substrings=["lat", "latin"],
                exclude=code_element,
            )

            code_text = _normalize_text(code_element.get_text(" ", strip=True)) if code_element else ""
            if not _is_code(code_text):
                continue
            serbian_text = (
                _normalize_text(serbian_element.get_text(" ", strip=True))
                if serbian_element
                else ""
            )
            latin_text = (
                _normalize_text(latin_element.get_text(" ", strip=True))
                if latin_element
                else ""
            )
            entries.append(MKBEntry(code_text, serbian_text, latin_text))
        return entries

    def _parse_from_text_blocks(self, soup: BeautifulSoup) -> list[MKBEntry]:
        text = soup.get_text("\n", strip=True)
        entries: list[MKBEntry] = []
        pattern = re.compile(r"^(?P<code>[A-Z]{1,2}\d{2}(?:\.[0-9A-Z]{1,4})?)\s+(?P<rest>.+)$")
        for line in text.splitlines():
            match = pattern.match(line)
            if not match:
                continue
            code = match.group("code")
            rest = match.group("rest")
            parts = [part.strip() for part in re.split(r"\s{2,}\|\s{2,}|\s{2,}", rest) if part.strip()]
            serbian = parts[0] if parts else ""
            latin = parts[1] if len(parts) > 1 else ""
            entries.append(MKBEntry(code, serbian, latin))
        return entries


def scrape_to_csv(output_path: str, *, delay: float = 0.2) -> int:
    """Convenience helper to scrape the site and persist the result to CSV."""

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    scraper = MKBScraper(delay=delay)
    entries = scraper.scrape()
    with open(output_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file, delimiter="|")
        writer.writerow(["code", "description_serbian", "description_latin"])
        for entry in entries:
            writer.writerow([entry.code, entry.serbian, entry.latin])
    LOGGER.info("Written %d entries to %s", len(entries), output_path)
    return len(entries)


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", unescape(value)).strip()


def _is_code(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Z]{1,2}\d{2}(?:\.[0-9A-Z]{1,4})?", value))


def _deduplicate(entries: Iterable[MKBEntry]) -> list[MKBEntry]:
    seen: dict[str, MKBEntry] = {}
    for entry in entries:
        if entry.code not in seen:
            seen[entry.code] = entry
        else:
            existing = seen[entry.code]
            merged = MKBEntry(
                code=entry.code,
                serbian=entry.serbian or existing.serbian,
                latin=entry.latin or existing.latin,
            )
            seen[entry.code] = merged
    return list(seen.values())


def _code_sort_key(code: str) -> tuple:
    match = re.fullmatch(r"([A-Z]+)(\d+)(?:\.([0-9A-Z]+))?", code)
    if match:
        prefix, number, suffix = match.groups()
        number_value = int(number)
        suffix_value = suffix or ""
        return (prefix, number_value, suffix_value)
    return (code, 0, "")


def _find_first_matching(
    container,
    *,
    class_substrings: list[str],
    exclude=None,
):
    if container is None:
        return None
    for descendant in container.find_all(True):
        if exclude is not None and descendant is exclude:
            continue
        class_list = [cls.lower() for cls in descendant.get("class", [])]
        if any(sub in cls for sub in class_substrings for cls in class_list):
            return descendant
    return None


__all__ = ["MKBScraper", "MKBEntry", "scrape_to_csv"]
