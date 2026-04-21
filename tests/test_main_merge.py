from __future__ import annotations

import csv
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import main


class MainMergeTests(unittest.TestCase):
    """CLI helper와 merged payload 계약을 검증한다."""

    def test_build_parser_accepts_multiple_files(self) -> None:
        parser = main.build_parser()

        args = parser.parse_args(["-f", "a.pdf", "b.pdf"])

        self.assertEqual(args.file, ["a.pdf", "b.pdf"])
        self.assertFalse(args.use_counterparty_prompt)

    def test_build_parser_accepts_use_counterparty_prompt_flag(self) -> None:
        parser = main.build_parser()

        args = parser.parse_args(["-f", "a.pdf", "--use-counterparty-prompt"])

        self.assertTrue(args.use_counterparty_prompt)

    def test_resolve_output_base_uses_merged_name_for_multiple_files(self) -> None:
        base_path = main.resolve_output_base(
            [Path("/tmp/a.pdf"), Path("/tmp/b.pdf")],
            None,
        )

        self.assertEqual(base_path.name, "merged_orders")

    def test_build_merged_payload_keeps_order_schema_consistent_with_single_payload(self) -> None:
        payload_a = {
            "file_name": "a.pdf",
            "source_path": "/tmp/a.pdf",
            "model_name": "model-x",
            "base_date": "2026-03-16",
            "issues": ["WARN_A"],
            "orders": [
                {
                    "fund_code": "F001",
                    "fund_name": "Alpha",
                    "settle_class": "2",
                    "order_type": "3",
                    "base_date": "2026-03-16",
                    "t_day": "01",
                    "transfer_amount": "100",
                }
            ],
        }
        payload_b = {
            "file_name": "b.pdf",
            "source_path": "/tmp/b.pdf",
            "model_name": "model-x",
            "base_date": "2026-03-17",
            "issues": ["WARN_B"],
            "orders": [
                {
                    "fund_code": "F002",
                    "fund_name": "Beta",
                    "settle_class": "1",
                    "order_type": "1",
                    "base_date": "2026-03-17",
                    "t_day": "02",
                    "transfer_amount": "200",
                }
            ],
        }

        merged = main.build_merged_payload([payload_a, payload_b])

        self.assertEqual(merged["file_count"], 2)
        self.assertEqual(merged["status"], "COMPLETED")
        self.assertIsNone(merged["reason"])
        self.assertEqual(merged["issues"], ["WARN_A", "WARN_B"])
        self.assertEqual(len(merged["documents"]), 2)
        self.assertEqual(len(merged["orders"]), 2)
        self.assertEqual(merged["orders"][0]["fund_code"], "F001")
        self.assertEqual(merged["orders"][1]["fund_code"], "F002")
        self.assertNotIn("file_name", merged["orders"][0])

    def test_build_merged_payload_deduplicates_identical_orders(self) -> None:
        payload_a = {
            "file_name": "a.xlsx",
            "source_path": "/tmp/a.xlsx",
            "model_name": "model-x",
            "base_date": "2025-08-26",
            "issues": [],
            "orders": [
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "settle_class": "2",
                    "order_type": "3",
                    "base_date": "2025-08-26",
                    "t_day": "01",
                    "transfer_amount": "1,053,329",
                }
            ],
        }
        payload_b = {
            "file_name": "b.xlsx",
            "source_path": "/tmp/b.xlsx",
            "model_name": "model-x",
            "base_date": "2025-08-26",
            "issues": [],
            "orders": [
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "settle_class": "2",
                    "order_type": "3",
                    "base_date": "2025-08-26",
                    "t_day": "01",
                    "transfer_amount": "1,053,329",
                }
            ],
        }

        merged = main.build_merged_payload([payload_a, payload_b])
        rows = main.payload_to_csv_rows(merged)

        self.assertEqual(len(merged["orders"]), 1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["file_name"], "a.xlsx")

    def test_build_merged_payload_keeps_sub_and_red_with_same_bucket_separate(self) -> None:
        payload_a = {
            "file_name": "a.xlsx",
            "source_path": "/tmp/a.xlsx",
            "model_name": "model-x",
            "base_date": "2025-08-26",
            "issues": [],
            "orders": [
                {
                    "fund_code": "6101",
                    "fund_name": "혼합성장형",
                    "settle_class": "2",
                    "order_type": "3",
                    "base_date": "2025-08-26",
                    "t_day": "01",
                    "transfer_amount": "20,262,383",
                }
            ],
        }
        payload_b = {
            "file_name": "b.xlsx",
            "source_path": "/tmp/b.xlsx",
            "model_name": "model-x",
            "base_date": "2025-08-26",
            "issues": [],
            "orders": [
                {
                    "fund_code": "6101",
                    "fund_name": "혼합성장형",
                    "settle_class": "2",
                    "order_type": "1",
                    "base_date": "2025-08-26",
                    "t_day": "01",
                    "transfer_amount": "5,000,000",
                }
            ],
        }

        merged = main.build_merged_payload([payload_a, payload_b])
        rows = main.payload_to_csv_rows(merged)

        self.assertEqual(len(merged["orders"]), 2)
        self.assertEqual({order["order_type"] for order in merged["orders"]}, {"3", "1"})
        self.assertEqual({order["transfer_amount"] for order in merged["orders"]}, {"20,262,383", "5,000,000"})
        self.assertEqual(len(rows), 2)
        self.assertEqual([row["file_name"] for row in rows], ["a.xlsx", "b.xlsx"])

    def test_build_merged_payload_does_not_merge_same_code_with_different_name(self) -> None:
        payload_a = {
            "file_name": "a.xlsx",
            "source_path": "/tmp/a.xlsx",
            "model_name": "model-x",
            "base_date": "2025-08-26",
            "issues": [],
            "orders": [
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "settle_class": "2",
                    "order_type": "3",
                    "base_date": "2025-08-26",
                    "t_day": "01",
                    "transfer_amount": "1,053,329",
                }
            ],
        }
        payload_b = {
            "file_name": "b.xlsx",
            "source_path": "/tmp/b.xlsx",
            "model_name": "model-x",
            "base_date": "2025-08-26",
            "issues": [],
            "orders": [
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형(클래스A)",
                    "settle_class": "2",
                    "order_type": "3",
                    "base_date": "2025-08-26",
                    "t_day": "01",
                    "transfer_amount": "1,053,329",
                }
            ],
        }

        merged = main.build_merged_payload([payload_a, payload_b])

        self.assertEqual(len(merged["orders"]), 2)

    def test_build_merged_payload_does_not_merge_same_name_with_different_code(self) -> None:
        payload_a = {
            "file_name": "a.xlsx",
            "source_path": "/tmp/a.xlsx",
            "model_name": "model-x",
            "base_date": "2025-08-26",
            "issues": [],
            "orders": [
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "settle_class": "2",
                    "order_type": "3",
                    "base_date": "2025-08-26",
                    "t_day": "01",
                    "transfer_amount": "1,053,329",
                }
            ],
        }
        payload_b = {
            "file_name": "b.xlsx",
            "source_path": "/tmp/b.xlsx",
            "model_name": "model-x",
            "base_date": "2025-08-26",
            "issues": [],
            "orders": [
                {
                    "fund_code": "6115",
                    "fund_name": "인덱스주식형",
                    "settle_class": "2",
                    "order_type": "3",
                    "base_date": "2025-08-26",
                    "t_day": "01",
                    "transfer_amount": "1,053,329",
                }
            ],
        }

        merged = main.build_merged_payload([payload_a, payload_b])

        self.assertEqual(len(merged["orders"]), 2)

    def test_build_merged_payload_does_not_merge_when_identity_field_differs(self) -> None:
        base_order = {
            "fund_code": "6114",
            "fund_name": "인덱스주식형",
            "settle_class": "2",
            "order_type": "3",
            "base_date": "2025-08-26",
            "t_day": "01",
            "transfer_amount": "1,053,329",
        }
        differing_fields = {
            "settle_class": "1",
            "order_type": "1",
            "base_date": "2025-08-27",
            "t_day": "02",
            "transfer_amount": "1,053,330",
        }

        for field_name, changed_value in differing_fields.items():
            with self.subTest(field=field_name):
                payload_a = {
                    "file_name": "a.xlsx",
                    "source_path": "/tmp/a.xlsx",
                    "model_name": "model-x",
                    "base_date": "2025-08-26",
                    "issues": [],
                    "orders": [dict(base_order)],
                }
                other_order = dict(base_order)
                other_order[field_name] = changed_value
                payload_b = {
                    "file_name": "b.xlsx",
                    "source_path": "/tmp/b.xlsx",
                    "model_name": "model-x",
                    "base_date": "2025-08-26",
                    "issues": [],
                    "orders": [other_order],
                }

                merged = main.build_merged_payload([payload_a, payload_b])

                self.assertEqual(len(merged["orders"]), 2)

    def test_write_orders_csv_supports_merged_payload(self) -> None:
        merged_payload = {
            "file_count": 2,
            "documents": [
                {
                    "file_name": "a.pdf",
                    "source_path": "/tmp/a.pdf",
                    "model_name": "model-x",
                    "base_date": "2026-03-16",
                    "issues": [],
                    "orders": [
                        {
                            "fund_code": "F001",
                            "fund_name": "Alpha",
                            "settle_class": "2",
                            "order_type": "3",
                            "base_date": "2026-03-16",
                            "t_day": "01",
                            "transfer_amount": "100",
                        }
                    ],
                },
                {
                    "file_name": "b.pdf",
                    "source_path": "/tmp/b.pdf",
                    "model_name": "model-x",
                    "base_date": "2026-03-17",
                    "issues": [],
                    "orders": [
                        {
                            "fund_code": "F002",
                            "fund_name": "Beta",
                            "settle_class": "1",
                            "order_type": "1",
                            "base_date": "2026-03-17",
                            "t_day": "02",
                            "transfer_amount": "200",
                        }
                    ],
                },
            ],
            "orders": [
                {
                    "fund_code": "F001",
                    "fund_name": "Alpha",
                    "settle_class": "2",
                    "order_type": "3",
                    "base_date": "2026-03-16",
                    "t_day": "01",
                    "transfer_amount": "100",
                },
                {
                    "fund_code": "F002",
                    "fund_name": "Beta",
                    "settle_class": "1",
                    "order_type": "1",
                    "base_date": "2026-03-17",
                    "t_day": "02",
                    "transfer_amount": "200",
                },
            ],
        }

        with TemporaryDirectory() as tmp_dir:
            csv_path = Path(tmp_dir) / "merged.csv"
            main.write_orders_csv(csv_path, merged_payload)

            with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.DictReader(handle))

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["file_name"], "a.pdf")
        self.assertEqual(rows[1]["file_name"], "b.pdf")


if __name__ == "__main__":
    unittest.main()
