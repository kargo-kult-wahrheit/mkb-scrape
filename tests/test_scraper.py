import unittest

from bs4 import BeautifulSoup

from mkb_scrape.scraper import MKBScraper, _strip_labels


class ParseHelpersTests(unittest.TestCase):
    def setUp(self) -> None:
        self.scraper = MKBScraper()

    def _soup(self, html: str) -> BeautifulSoup:
        return BeautifulSoup(html, "html.parser")

    def test_parse_structured_blocks_with_labels(self) -> None:
        html = """
        <div class="mkb-item">
            <span class="sifra">Šifra: A00</span>
            <span class="naziv">Opis: Kolera</span>
            <span class="latin">Latinski: Cholera</span>
        </div>
        """
        soup = self._soup(html)

        entries = self.scraper._parse_from_structured_blocks(soup)

        self.assertEqual(1, len(entries))
        entry = entries[0]
        self.assertEqual("A00", entry.code)
        self.assertEqual("Kolera", entry.serbian)
        self.assertEqual("Cholera", entry.latin)

    def test_parse_tables_strips_labels(self) -> None:
        html = """
        <table>
            <tr>
                <td>Šifra: B00</td>
                <td>Naziv: Herpes simpleks</td>
                <td>Latinski: Herpes simplex</td>
            </tr>
        </table>
        """
        soup = self._soup(html)

        entries = self.scraper._parse_from_tables(soup)

        self.assertEqual(1, len(entries))
        entry = entries[0]
        self.assertEqual("B00", entry.code)
        self.assertEqual("Herpes simpleks", entry.serbian)
        self.assertEqual("Herpes simplex", entry.latin)

    def test_strip_labels_handles_empty_strings(self) -> None:
        self.assertEqual("", _strip_labels(""))
        self.assertEqual("tekst", _strip_labels("Naziv: tekst"))
        self.assertEqual("tekst", _strip_labels("latinski - tekst"))

    def test_parse_list_group_structure(self) -> None:
        html = """
        <ul class="list-group mb-3">
            <li class="list-group-item">
                <div class="col-sm-2 col_first"><strong>A00</strong></div>
                <div class="col-sm-10 col_last">
                    <strong>Kolera NOVA</strong><br>
                    Cholera
                </div>
            </li>
            <li class="list-group-item">
                <div class="col-sm-2 col_first"><strong>A00.0</strong></div>
                <div class="col-sm-10 col_last">
                    <strong>Kolera, uzročnik Vibrio cholerae 01,biotip cholerae</strong><br>
                    Cholera classica
                </div>
            </li>
        </ul>
        """
        soup = self._soup(html)

        entries = self.scraper._parse_from_list_groups(soup)

        self.assertEqual(2, len(entries))
        self.assertEqual("A00", entries[0].code)
        self.assertEqual("Kolera NOVA", entries[0].serbian)
        self.assertEqual("Cholera", entries[0].latin)
        self.assertEqual("A00.0", entries[1].code)
        self.assertEqual(
            "Kolera, uzročnik Vibrio cholerae 01,biotip cholerae",
            entries[1].serbian,
        )
        self.assertEqual("Cholera classica", entries[1].latin)


if __name__ == "__main__":
    unittest.main()
