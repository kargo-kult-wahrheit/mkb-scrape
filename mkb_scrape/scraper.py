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
from bs4 import BeautifulSoup, NavigableString, Tag

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

        index_entries = self._parse_entries(index_soup, source=self.index_url)
        if index_entries:
            LOGGER.info(
                "Found %d entries directly on the index page", len(index_entries)
            )
            entries.extend(index_entries)
        else:
            LOGGER.info(
                "No entries parsed from the index page; continuing with catalogue pages"
            )

        catalog_pages = self._collect_catalog_urls(index_soup)
        LOGGER.info("Discovered %d catalogue pages", len(catalog_pages))

        for idx, page_url in enumerate(catalog_pages, start=1):
            LOGGER.info("Fetching page %d/%d: %s", idx, len(catalog_pages), page_url)
            html = self._fetch(page_url)
            soup = BeautifulSoup(html, "html.parser")

            page_entries = self._parse_entries(soup, source=page_url)
            if not page_entries:
                raise RuntimeError(
                    f"No entries parsed from {page_url}. Aborting as requested."
                )
            LOGGER.info("Found %d entries on %s", len(page_entries), page_url)
            entries.extend(page_entries)

            if self.delay and idx < len(catalog_pages):
                time.sleep(self.delay)

        unique_entries = _deduplicate(entries)
        LOGGER.info("Collected %d unique entries", len(unique_entries))
        ordered = sorted(unique_entries, key=lambda entry: _code_sort_key(entry.code))
        if ordered:
            sample = ordered[0]
            LOGGER.debug(
                "First entry after sorting: %s | %s | %s",
                sample.code,
                sample.serbian,
                sample.latin,
            )
        return ordered

    def _fetch(self, url: str) -> str:
        LOGGER.debug("Requesting %s", url)
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text

    def _collect_catalog_urls(self, soup: BeautifulSoup) -> list[str]:
        candidates: list[tuple[str, str]] = []
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

            if normalized in seen:
                continue

            seen.add(normalized)
            candidates.append((normalized, path))
            LOGGER.info("Discovered catalogue link: %s", normalized)

        LOGGER.info("Identified %d candidate catalogue links", len(candidates))
        return self._filter_catalog_urls(candidates)

    def _filter_catalog_urls(self, candidates: list[tuple[str, str]]) -> list[str]:
        catalog_urls: list[str] = []
        covered_ranges: list[
            tuple[tuple[str, int, str], tuple[str, int, str], str, str]
        ] = []

        for url, path in candidates:
            should_skip = False
            code_range = _extract_code_range(path, self.catalog_path_prefix)
            if code_range:
                start, end = code_range
                start_key = _code_sort_key(start)
                end_key = _code_sort_key(end)
                if start_key > end_key:
                    start, end = end, start
                    start_key, end_key = end_key, start_key

                for existing_start_key, existing_end_key, existing_start, existing_end in covered_ranges:
                    if existing_start_key <= start_key and end_key <= existing_end_key:
                        LOGGER.info(
                            "Skipping %s because range %s-%s is covered by %s-%s",
                            url,
                            start,
                            end,
                            existing_start,
                            existing_end,
                        )
                        should_skip = True
                        break

                if should_skip:
                    continue

                covered_ranges.append((start_key, end_key, start, end))

            catalog_urls.append(url)
            LOGGER.info("Keeping catalogue URL: %s", url)

        LOGGER.info("Retained %d catalogue URLs after filtering", len(catalog_urls))
        return catalog_urls

    def _parse_entries(self, soup: BeautifulSoup, *, source: str) -> list[MKBEntry]:
        entries: list[MKBEntry] = []
        parsed: list[MKBEntry] = []

        table_entries = self._parse_from_tables(soup)
        LOGGER.info("Parsed %d entries from HTML tables on %s", len(table_entries), source)
        parsed.extend(table_entries)

        structured_entries = self._parse_from_structured_blocks(soup)
        LOGGER.info(
            "Parsed %d entries from structured blocks on %s",
            len(structured_entries),
            source,
        )
        parsed.extend(structured_entries)

        list_group_entries = self._parse_from_list_groups(soup)
        LOGGER.info(
            "Parsed %d entries from list-group blocks on %s",
            len(list_group_entries),
            source,
        )
        parsed.extend(list_group_entries)

        heading_entries = self._parse_from_heading_blocks(soup)
        LOGGER.info(
            "Parsed %d entries from heading blocks on %s",
            len(heading_entries),
            source,
        )
        parsed.extend(heading_entries)

        paragraph_entries = self._parse_from_paragraph_blocks(soup)
        LOGGER.info(
            "Parsed %d entries from paragraph blocks on %s",
            len(paragraph_entries),
            source,
        )
        parsed.extend(paragraph_entries)

        text_entries = self._parse_from_text_blocks(soup)
        LOGGER.info(
            "Parsed %d entries from free text blocks on %s",
            len(text_entries),
            source,
        )
        parsed.extend(text_entries)

        meaningful = [_normalise_entry(entry) for entry in parsed]
        dropped = sum(1 for entry in meaningful if entry is None)
        if dropped:
            LOGGER.info(
                "Dropped %d entries during normalisation on %s", dropped, source
            )
        final_entries = [entry for entry in meaningful if entry]
        for entry in final_entries:
            LOGGER.info(
                "Created entry %s|%s|%s from %s",
                entry.code,
                entry.serbian,
                entry.latin,
                source,
            )
        return final_entries

    def _parse_from_tables(self, soup: BeautifulSoup) -> list[MKBEntry]:
        entries: list[MKBEntry] = []
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cells = [
                    _strip_labels(_normalize_text(cell.get_text(" ", strip=True)))
                    for cell in row.find_all(["td", "th"])
                ]
                if not cells or any("šifra" in cell.lower() for cell in cells):
                    continue
                if len(cells) < 2:
                    continue
                code = cells[0]
                if not _is_code(code):
                    continue
                serbian = _strip_labels(cells[1])
                latin = _strip_labels(cells[2]) if len(cells) >= 3 else ""
                entries.append(MKBEntry(code=code, serbian=serbian, latin=latin))
        return entries

    def _parse_from_structured_blocks(self, soup: BeautifulSoup) -> list[MKBEntry]:
        entries: list[MKBEntry] = []
        candidates = soup.find_all(
            lambda tag: tag.name in {"div", "li"}
            and tag.get("class")
            and any(
                any(key in cls.lower() for key in ("mkb", "icd", "sifra", "šifra"))
                for cls in tag.get("class", [])
            )
        )
        for container in candidates:
            code_element = _find_first_matching(
                container,
                class_substrings=["sifra", "code", "oznaka"],
            )
            serbian_element = _find_first_matching(
                container,
                class_substrings=["sr", "opis", "naziv", "title"],
                exclude=code_element,
            )
            latin_element = _find_first_matching(
                container,
                class_substrings=["lat", "latin"],
                exclude=code_element,
            )

            code_text = (
                _strip_labels(_normalize_text(code_element.get_text(" ", strip=True)))
                if code_element
                else ""
            )
            if not _is_code(code_text):
                continue
            serbian_text = (
                _strip_labels(_normalize_text(serbian_element.get_text(" ", strip=True)))
                if serbian_element
                else ""
            )
            latin_text = (
                _strip_labels(_normalize_text(latin_element.get_text(" ", strip=True)))
                if latin_element
                else ""
            )
            entries.append(MKBEntry(code_text, serbian_text, latin_text))
        return entries

    def _parse_from_list_groups(self, soup: BeautifulSoup) -> list[MKBEntry]:
        entries: list[MKBEntry] = []
        for item in soup.select("li.list-group-item"):
            code_container = item.select_one(".col_first")
            description_container = item.select_one(".col_last")

            if not code_container or not description_container:
                continue

            code_element = code_container.find(["strong", "b"]) or code_container
            code_text = _strip_labels(
                _normalize_text(code_element.get_text(" ", strip=True))
            )
            if not _is_code(code_text):
                continue

            serbian_text = ""
            latin_text = ""

            description_element = description_container.find(["strong", "b"])
            if description_element:
                serbian_text = _strip_labels(
                    _normalize_text(description_element.get_text(" ", strip=True))
                )
                latin_text = _extract_latin_from_siblings(description_element)
            else:
                serbian_text = _strip_labels(
                    _normalize_text(description_container.get_text(" ", strip=True))
                )

            if serbian_text == code_text:
                serbian_text = ""

            entries.append(MKBEntry(code_text, serbian_text, latin_text))

        return entries

    def _parse_from_heading_blocks(self, soup: BeautifulSoup) -> list[MKBEntry]:
        entries: list[MKBEntry] = []
        heading_pattern = re.compile(
            r"^(?P<code>[A-Z]{1,2}\d{2}(?:\.[0-9A-Z]{1,4})?)\s*[-–—:]?\s*(?P<serbian>.+)$"
        )
        for heading in soup.find_all(re.compile(r"^h[1-6]$")):
            text = _normalize_text(heading.get_text(" ", strip=True))
            if not text:
                continue
            match = heading_pattern.match(text)
            if not match:
                continue
            code = match.group("code")
            serbian = match.group("serbian").strip()
            latin = _extract_following_latin(heading)
            entries.append(MKBEntry(code, serbian, latin))
        return entries

    def _parse_from_paragraph_blocks(self, soup: BeautifulSoup) -> list[MKBEntry]:
        entries: list[MKBEntry] = []
        for paragraph in soup.find_all(["p", "li", "div"]):
            strong = paragraph.find(["strong", "b"])
            if not strong:
                continue
            code_text = _strip_labels(_normalize_text(strong.get_text(" ", strip=True)))
            if not _is_code(code_text):
                continue
            serbian_parts: list[str] = []
            latin_text = ""
            for node in strong.next_siblings:
                if isinstance(node, str):
                    candidate = _strip_labels(_normalize_text(node))
                    if candidate:
                        serbian_parts.append(candidate)
                    continue
                if node.name == "br":
                    continue
                text = _strip_labels(_normalize_text(node.get_text(" ", strip=True)))
                if not text:
                    continue
                classes = " ".join(node.get("class", [])).lower()
                if node.name in {"em", "i"} or "latin" in classes:
                    latin_text = text
                    break
                if any(keyword in classes for keyword in ("lat", "latin")):
                    latin_text = text
                    break
                serbian_parts.append(text)
            serbian_text = _normalize_text(" ".join(part for part in serbian_parts if part))
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
            rest = _strip_labels(match.group("rest"))
            parts = [
                part.strip()
                for part in re.split(r"\s{2,}\|\s{2,}|\s{2,}|\s+-\s+|\s+–\s+", rest)
                if part.strip()
            ]
            serbian = parts[0] if parts else rest.strip()
            latin = ""
            if len(parts) > 1:
                latin = parts[1]
            else:
                latin_match = re.search(r"\(([^()]+)\)$", serbian)
                if latin_match:
                    latin = latin_match.group(1).strip()
                    serbian = serbian[: latin_match.start()].strip()
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
            LOGGER.info(
                "Writing entry to CSV: %s|%s|%s", entry.code, entry.serbian, entry.latin
            )
            writer.writerow([entry.code, entry.serbian, entry.latin])
    LOGGER.info("Written %d entries to %s", len(entries), output_path)
    return len(entries)


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", unescape(value)).strip()


def _is_code(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Z]{1,2}\d{2}(?:\.[0-9A-Z]{1,4})?", value))


def _normalise_entry(entry: MKBEntry) -> Optional[MKBEntry]:
    serbian = _strip_labels(_normalize_text(entry.serbian))
    latin = _strip_labels(_normalize_text(entry.latin))

    if serbian and _is_code(serbian):
        serbian = ""
    if latin and _is_code(latin):
        latin = ""

    if not serbian and latin:
        segments = [segment.strip() for segment in re.split(r"\s+-\s+|\s+–\s+", latin) if segment.strip()]
        if len(segments) > 1:
            serbian, latin = segments[0], segments[-1]

    if not serbian:
        return None

    return MKBEntry(entry.code, serbian, latin)


_LABEL_PATTERN = re.compile(
    r"^(?:srpski|serbian|naziv|opis|latinski|latin(?: name)?|šifra|sifra|oznaka|code)\s*[:\-–]?\s*",
    re.IGNORECASE,
)


def _strip_labels(value: str) -> str:
    if not value:
        return ""
    cleaned = _LABEL_PATTERN.sub("", value)
    return cleaned.strip(" -:\u2013")


def _extract_following_latin(element: Tag) -> str:
    for idx, sibling in enumerate(element.next_siblings):
        if idx > 10:
            break
        if isinstance(sibling, NavigableString):
            candidate = _normalize_text(str(sibling))
            if _contains_latin_label(candidate):
                return _strip_labels(candidate)
            continue
        if not isinstance(sibling, Tag):
            continue
        if sibling.name == "br":
            continue
        text = _normalize_text(sibling.get_text(" ", strip=True))
        if not text:
            continue
        classes = " ".join(sibling.get("class", [])).lower()
        if sibling.name in {"em", "i"} or "latin" in classes or _contains_latin_label(text):
            return _strip_labels(text)
        if sibling.name and sibling.name.startswith("h"):
            break
    return ""


def _extract_latin_from_siblings(element: Tag) -> str:
    for sibling in element.next_siblings:
        if isinstance(sibling, str):
            candidate = _strip_labels(_normalize_text(sibling))
            if candidate:
                return candidate
            continue
        if not isinstance(sibling, Tag):
            continue
        if sibling.name == "br":
            continue
        text = _strip_labels(_normalize_text(sibling.get_text(" ", strip=True)))
        if text:
            return text
    return ""


def _contains_latin_label(value: str) -> bool:
    value_lower = value.lower()
    return any(token in value_lower for token in ("latinski", "latin"))


_CODE_IN_PATH_PATTERN = re.compile(r"[A-Z]{1,2}\d{2}(?:\.[0-9A-Z]{1,4})?")


def _extract_code_range(path: str, prefix: str) -> Optional[tuple[str, str]]:
    if not path.startswith(prefix):
        return None

    remainder = path[len(prefix) :].strip("/")
    if not remainder:
        return None

    codes = _CODE_IN_PATH_PATTERN.findall(remainder)
    if not codes:
        return None

    if len(codes) == 1:
        return (codes[0], codes[0])

    return (codes[0], codes[-1])


def _deduplicate(entries: Iterable[MKBEntry]) -> list[MKBEntry]:
    seen: dict[str, MKBEntry] = {}
    for entry in entries:
        if entry.code not in seen:
            seen[entry.code] = entry
            LOGGER.info("Registering new unique entry: %s", entry.code)
        else:
            existing = seen[entry.code]
            merged = MKBEntry(
                code=entry.code,
                serbian=entry.serbian or existing.serbian,
                latin=entry.latin or existing.latin,
            )
            seen[entry.code] = merged
            LOGGER.info("Merged duplicate entry for code %s", entry.code)
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
