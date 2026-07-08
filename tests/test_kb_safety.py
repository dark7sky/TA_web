import unittest
from unittest.mock import patch

import KB


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, _query, _params=None):
        return None

    def fetchall(self):
        return self.rows


class CollectionCompletenessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.rules = KB.ExclusionRules(frozenset(), frozenset())

    @staticmethod
    def snapshot(key: str) -> KB.AccountSnapshot:
        return KB.AccountSnapshot(account_key=key, balance=1, company="bank")

    def test_rejects_large_collection_drop(self) -> None:
        cur = FakeCursor([(str(index), "bank") for index in range(10)])
        accounts = {str(index): self.snapshot(str(index)) for index in range(3)}

        with patch.dict("os.environ", {"MIN_ACCOUNT_COLLECTION_RATIO": "0.8"}):
            with self.assertRaisesRegex(RuntimeError, "coverage dropped"):
                KB.validate_collection_completeness(cur, accounts, self.rules)

    def test_accepts_collection_above_threshold(self) -> None:
        cur = FakeCursor([(str(index), "bank") for index in range(10)])
        accounts = {str(index): self.snapshot(str(index)) for index in range(8)}

        with patch.dict("os.environ", {"MIN_ACCOUNT_COLLECTION_RATIO": "0.8"}):
            KB.validate_collection_completeness(cur, accounts, self.rules)


class CardBalanceTests(unittest.TestCase):
    def test_skips_card_merge_when_no_stored_balance_exists(self) -> None:
        cur = FakeCursor([])
        accounts = {}

        with (
            patch.object(KB, "sync_card_excel") as sync_card_excel,
            patch.object(KB, "should_refresh_cards", return_value=False),
            patch.object(KB, "fetch_latest_card_balance", return_value=None),
            patch.object(KB, "merge_snapshot") as merge_snapshot,
        ):
            refreshed = KB.apply_card_balance(cur, {}, accounts)

        self.assertFalse(refreshed)
        sync_card_excel.assert_called_once()
        merge_snapshot.assert_not_called()

    def test_raises_when_refresh_produces_no_card_balance(self) -> None:
        cur = FakeCursor([])
        accounts = {}

        with (
            patch.object(KB, "sync_card_excel") as sync_card_excel,
            patch.object(KB, "should_refresh_cards", return_value=True),
            patch.object(KB, "fetch_latest_card_balance", return_value=None),
            patch.object(KB, "cards") as cards_mod,
        ):
            cards_mod.main.return_value = None
            with self.assertRaisesRegex(RuntimeError, "stored card balance"):
                KB.apply_card_balance(cur, {}, accounts)

        sync_card_excel.assert_called_once()
        cards_mod.main.assert_called_once()


if __name__ == "__main__":
    unittest.main()
