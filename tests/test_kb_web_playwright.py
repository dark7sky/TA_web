import unittest
from unittest.mock import patch

import KB_web_playwright as kb_web


class ScrapeValidationTests(unittest.TestCase):
    def test_parse_balance_rejects_non_numeric_text(self) -> None:
        with self.assertRaises(ValueError):
            kb_web.parse_balance("loading")

    def test_parse_balance_accepts_formatted_won_value(self) -> None:
        self.assertEqual(kb_web.parse_balance("1,234원"), 1234)

    def test_rejects_large_drop_from_previous_collection(self) -> None:
        previous = [["bank", "type", str(index), "name", 1] for index in range(10)]
        current = [["bank", "type", str(index), "name", 1] for index in range(3)]

        with patch.dict("os.environ", {"MIN_ACCOUNT_COLLECTION_RATIO": "0.8"}):
            with self.assertRaisesRegex(RuntimeError, "coverage dropped"):
                kb_web.validate_collection_results(current, previous)

    def test_accepts_complete_collection(self) -> None:
        previous = [["bank", "type", str(index), "name", 1] for index in range(5)]
        current = [["bank", "type", str(index), "name", 2] for index in range(5)]

        kb_web.validate_collection_results(current, previous)


if __name__ == "__main__":
    unittest.main()
