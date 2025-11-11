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
from collections import deque
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
INDEX_PATH = "/"
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
        delay: float = 0.2,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.index_path = index_path if index_path.startswith("/") else f"/{index_path}"
        self.index_url = f"{self.base_url}{self.index_path}"
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.setdefault(
            "User-Agent",
            "Mozilla/5.0 (compatible; mkb-scraper/1.0; +https://www.batut.org.rs)",
        )

    def scrape(self) -> list[MKBEntry]:
        """Scrape every discoverable MKB entry from the site."""

        entries: list[MKBEntry] = []
        visited: set[str] = set()
        to_visit: deque[str] = deque([self.index_url])
        discovered: set[str] = {self.index_url.rstrip("/") or self.index_url}

        while to_visit:
            page_url = to_visit.popleft()
            if page_url in visited:
                continue

            LOGGER.info("Fetching page: %s", page_url)
            html = self._fetch(page_url)
            soup = BeautifulSoup(html, "html.parser")

            page_entries = self._parse_entries(soup)
            LOGGER.debug("Found %d entries on %s", len(page_entries), page_url)
            entries.extend(page_entries)

            visited.add(page_url)

            new_links = self._discover_detail_pages(soup, page_url)
            LOGGER.debug(
                "Discovered %d child pages from %s", len(new_links), page_url
            )
            for link in new_links:
                normalized_link = link.rstrip("/") or link
                if normalized_link not in discovered:
                    to_visit.append(link)
                    discovered.add(normalized_link)

            if self.delay and to_visit:
                time.sleep(self.delay)

        unique_entries = _deduplicate(entries)
        LOGGER.info("Collected %d unique entries", len(unique_entries))
        return sorted(unique_entries, key=lambda entry: _code_sort_key(entry.code))

    def _fetch(self, url: str) -> str:
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text

    def _discover_detail_pages(self, soup: BeautifulSoup, current_url: str) -> list[str]:
        links: set[str] = set()
        index_path = self.index_path.rstrip("/")
        base_netloc = urlparse(self.base_url).netloc
        current_normalized = current_url.rstrip("/")

        def normalize(candidate: str) -> str | None:
            parsed = urlparse(candidate)
            if parsed.scheme not in {"http", "https"}:
                return None
            if parsed.netloc != base_netloc:
                return None
            path = parsed.path.rstrip("/")
            if not path:
                return None
            if path == index_path and not parsed.query:
                return None
            if not path.startswith(index_path):
                return None
            normalized_url = f"{parsed.scheme}://{parsed.netloc}{path}"
            if parsed.query:
                normalized_url = f"{normalized_url}?{parsed.query}"
            if normalized_url.rstrip("/") == current_normalized:
                return None
            return normalized_url

        base_candidates: list[str] = []
        seen_bases: set[str] = set()
        for raw_base in (
            current_url,
            f"{self.base_url}{index_path}/",
        ):
            parsed_base = urlparse(raw_base)
            base = f"{parsed_base.scheme}://{parsed_base.netloc}{parsed_base.path}"
            if not base.endswith("/"):
                base = f"{base}/"
            if base not in seen_bases:
                base_candidates.append(base)
                seen_bases.add(base)

        for anchor in soup.find_all("a", href=True):
            href = anchor["href"].strip()
            if not href or href.startswith("#"):
                continue
            for base in base_candidates:
                absolute = urljoin(base, href)
                normalized = normalize(absolute)
                if normalized is not None:
                    links.add(normalized)
        return sorted(links)

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
