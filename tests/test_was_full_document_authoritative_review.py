from __future__ import annotations

import importlib.util
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "was_full_document_authoritative_review.py"
SPEC = importlib.util.spec_from_file_location("was_full_document_authoritative_review", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
review_script = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(review_script)


class WasFullDocumentAuthoritativeReviewTests(unittest.TestCase):
    def test_db_company_alias_mapping(self) -> None:
        self.assertEqual(
            review_script._db_company_for_prompt_name("IBK", answer_company="IBK"),
            "IBK연금보험",
        )
        self.assertEqual(
            review_script._db_company_for_prompt_name("흥국생명", answer_company="흥국생명"),
            "흥국생명-heungkuklife",
        )
        self.assertEqual(
            review_script._db_company_for_prompt_name("흥국생명-hanais", answer_company="흥국생명"),
            "흥국생명-hanais",
        )

    def test_build_expected_db_rows_contains_required_aliases(self) -> None:
        rows = review_script._build_expected_db_rows()
        rows_by_company = {row["db_company"]: row for row in rows}

        self.assertEqual(len(rows), 17)
        self.assertIn("IBK연금보험", rows_by_company)
        self.assertIn("흥국생명-heungkuklife", rows_by_company)
        self.assertIn("흥국생명-hanais", rows_by_company)
        self.assertTrue(rows_by_company["IBK연금보험"]["use_counterparty_prompt"])
        self.assertFalse(rows_by_company["KDB생명"]["only_pending"])

    def test_write_report_includes_runtime_and_db_sync_summary(self) -> None:
        cases = [
            {
                "case_id": "01_case",
                "document_name": "doc1.eml",
                "answer_company": "흥국생명",
                "db_company": "흥국생명-heungkuklife",
                "only_pending": False,
                "runtime": "was",
                "authority_basis": "business_answer",
                "confidence_tier": "confirmed",
                "human_confirmed": True,
                "status": "COMPLETED",
                "answer_vs_source": {"json_exact": True, "csv_exact": True},
                "extract_vs_source": {"core_exact": True},
                "source_review_status": "BYPASSED_EXTERNAL_AUTHORITY",
                "answer_action": "updated",
                "order_count": 9,
                "retry_counts": "-",
                "elapsed_seconds": 1.2,
                "review_verdict": "PASS",
                "base_date": "2026-04-13",
                "external_reference_path": "/tmp/ref.md",
                "source_review_issues": [],
                "result_json_path": "/tmp/a.json",
                "result_csv_path": "/tmp/a.csv",
                "authoritative_json_path": "/tmp/auth-a.json",
                "debug_root": "/tmp/debug-a",
                "handoff_root": "/tmp/handoff-a",
                "answer_updated_without_business": False,
            }
        ]
        preflight_summary = {
            "llm_base_url": "http://localhost:3900/v1",
            "db_sync_action": "noop",
            "db_sync_summary": {
                "expected_row_count": 17,
                "db_row_count": 17,
                "updated_row_count": 0,
                "missing_rows": [],
                "unexpected_rows": [],
            },
            "llm_tunnel_reconnected": False,
            "db_tunnel_reconnected": False,
        }

        with TemporaryDirectory() as tmp_dir, patch.object(review_script, "REPORT_ROOT", Path(tmp_dir)):
            report_path = review_script._write_report(
                run_root=Path("/tmp/run-root"),
                cases=cases,
                preflight_summary=preflight_summary,
            )
            report_text = report_path.read_text(encoding="utf-8")

        self.assertIn("runtime: `was`", report_text)
        self.assertIn("db_sync_action: `noop`", report_text)
        self.assertIn("db expected rows: `17`", report_text)
        self.assertIn("흥국생명-heungkuklife", report_text)
        self.assertIn("answer_company", report_text)


if __name__ == "__main__":
    unittest.main()
