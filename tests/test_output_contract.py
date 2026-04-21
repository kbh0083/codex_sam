from __future__ import annotations

import unittest

from app.output_contract import dedupe_serialized_order_payloads, normalize_counterparty_output_order_payloads


class OutputContractTest(unittest.TestCase):
    def test_metlife_normalization_still_works_with_prompt_name(self) -> None:
        payloads = [
            {
                "fund_code": "ML123",
                "fund_name": "  글로벌채권형  ",
                "settle_class": "2",
                "order_type": "3",
                "base_date": "2026-04-14",
                "t_day": "01",
                "transfer_amount": "10,000",
            }
        ]

        normalized = normalize_counterparty_output_order_payloads(
            payloads,
            prompt_name="메트라이프생명",
        )

        self.assertEqual(normalized[0]["fund_code"], "-")
        self.assertEqual(normalized[0]["fund_name"], "글로벌채권형")

    def test_heungkuk_normalization_accepts_company_name_and_sorts(self) -> None:
        payloads = [
            {
                "fund_code": "450036",
                "fund_name": "임시명",
                "settle_class": "1",
                "order_type": "1",
                "base_date": "2026-04-13",
                "t_day": "03",
                "transfer_amount": "50,000,000",
            },
            {
                "fund_code": "450038",
                "fund_name": "임시명",
                "settle_class": "2",
                "order_type": "3",
                "base_date": "2026-04-13",
                "t_day": "01",
                "transfer_amount": "40,000,000",
            },
        ]

        normalized = normalize_counterparty_output_order_payloads(
            payloads,
            company_name="흥국생명-heungkuklife",
        )

        self.assertEqual(normalized[0]["fund_code"], "450038")
        self.assertEqual(normalized[0]["fund_name"], "-")
        self.assertEqual(normalized[1]["fund_code"], "450036")
        self.assertEqual(normalized[1]["fund_name"], "-")

    def test_cardif_normalization_sorts_by_fund_code_without_rewriting_settle_class(self) -> None:
        payloads = [
            {
                "fund_code": "GBE",
                "fund_name": "Global Bond FoF II",
                "settle_class": "2",
                "order_type": "3",
                "base_date": "2025-11-27",
                "t_day": "01",
                "transfer_amount": "2,420,174",
            },
            {
                "fund_code": "ATE",
                "fund_name": "AI innovative Theme selection",
                "settle_class": "2",
                "order_type": "1",
                "base_date": "2025-11-27",
                "t_day": "01",
                "transfer_amount": "42,346",
            },
        ]

        normalized = normalize_counterparty_output_order_payloads(
            payloads,
            prompt_name="카디프",
        )

        self.assertEqual([row["fund_code"] for row in normalized], ["ATE", "GBE"])
        self.assertEqual([row["settle_class"] for row in normalized], ["2", "2"])

    def test_dedupe_uses_numeric_amount_identity_but_preserves_first_amount_format(self) -> None:
        payloads = [
            {
                "fund_code": "KB001",
                "fund_name": "KB Decimal",
                "settle_class": "2",
                "order_type": "3",
                "base_date": "2025-11-27",
                "t_day": "01",
                "transfer_amount": "23,213.4",
            },
            {
                "fund_code": "KB001",
                "fund_name": "KB Decimal",
                "settle_class": "2",
                "order_type": "3",
                "base_date": "2025-11-27",
                "t_day": "01",
                "transfer_amount": "23,213.40",
            },
        ]

        deduped = dedupe_serialized_order_payloads(payloads)

        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0]["transfer_amount"], "23,213.4")

    def test_dedupe_treats_same_fund_info_with_decimal_scale_difference_as_duplicate(self) -> None:
        payloads = [
            {
                "fund_code": "KB001",
                "fund_name": "KB Decimal",
                "settle_class": "2",
                "order_type": "3",
                "base_date": "2025-11-27",
                "t_day": "01",
                "transfer_amount": "23,213.40",
            },
            {
                "fund_code": "KB001",
                "fund_name": "KB Decimal",
                "settle_class": "2",
                "order_type": "3",
                "base_date": "2025-11-27",
                "t_day": "01",
                "transfer_amount": "23,213.4",
            },
        ]

        deduped = dedupe_serialized_order_payloads(payloads)

        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0]["transfer_amount"], "23,213.40")

    def test_dedupe_still_normalizes_integer_like_decimal_artifact(self) -> None:
        payloads = [
            {
                "fund_code": "F001",
                "fund_name": "Artifact",
                "settle_class": "2",
                "order_type": "3",
                "base_date": "2025-11-27",
                "t_day": "01",
                "transfer_amount": "70,000,000.00000001",
            }
        ]

        deduped = dedupe_serialized_order_payloads(payloads)

        self.assertEqual(deduped[0]["transfer_amount"], "70,000,000")


if __name__ == "__main__":
    unittest.main()
