from __future__ import annotations

import importlib.util
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from app.document_loader import DocumentLoadTaskPayload, TargetFundScope

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "full_document_authoritative_review.py"
SPEC = importlib.util.spec_from_file_location("full_document_authoritative_review", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
review_script = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(review_script)


class FullDocumentAuthoritativeReviewTests(unittest.TestCase):
    def _sample_task_payload(self) -> DocumentLoadTaskPayload:
        return DocumentLoadTaskPayload(
            source_path=str(REPO_ROOT / "document" / "[흥국생명] 설정해지 내역 운용지시건-삼성-0413.eml"),
            file_name="[흥국생명] 설정해지 내역 운용지시건-삼성-0413.eml",
            pdf_password=None,
            content_type="message/rfc822",
            raw_text="raw",
            markdown_text="markdown",
            chunks=("chunk",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=9,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )

    def test_parse_md_table_db_payload_returns_heungkuk_business_answer_payload(self) -> None:
        payload = review_script._parse_md_table_db_payload(
            REPO_ROOT / "참고자료" / "흥국생명_HKlife_0413_정답지.md"
        )

        self.assertEqual(payload["status"], "COMPLETED")
        self.assertEqual(payload["base_date"], "2026-04-13")
        self.assertEqual(len(payload["orders"]), 9)
        self.assertEqual(
            payload["orders"][0],
            {
                "fund_code": "450038",
                "fund_name": "-",
                "base_date": "2026-04-13",
                "t_day": "01",
                "transfer_amount": "40,000,000",
                "settle_class": "2",
                "order_type": "3",
            },
        )
        self.assertEqual(
            payload["orders"][-1],
            {
                "fund_code": "450036",
                "fund_name": "-",
                "base_date": "2026-04-13",
                "t_day": "03",
                "transfer_amount": "50,000,000",
                "settle_class": "1",
                "order_type": "1",
            },
        )

    def test_resolve_authoritative_payload_bypasses_llm_when_business_authority_exists(self) -> None:
        task_payload = self._sample_task_payload()
        case_record = {"db_company": "흥국생명"}
        extraction_candidate = {
            "base_date": "2026-04-13",
            "status": "COMPLETED",
            "reason": None,
            "issues": [],
            "orders": [],
        }
        registry = {
            task_payload.file_name: {
                "document_name": task_payload.file_name,
                "reference_path": REPO_ROOT / "참고자료" / "흥국생명_HKlife_0413_정답지.md",
                "format": "md_table_db_payload",
                "authority_kind": "business_answer",
            }
        }

        with TemporaryDirectory() as tmp_dir, patch.object(
            review_script,
            "_run_authoritative_review_llm",
            side_effect=AssertionError("LLM review must be bypassed for business authority"),
        ):
            payload, source_review_status, source_review_issues, provenance = review_script._resolve_authoritative_payload_for_case(
                case_record=case_record,
                task_payload=task_payload,
                counterparty_guidance=None,
                review_root=Path(tmp_dir),
                extraction_candidate=extraction_candidate,
                answer_candidate=None,
                model_name="test-model",
                external_registry=registry,
            )

        self.assertEqual(source_review_status, "BYPASSED_EXTERNAL_AUTHORITY")
        self.assertEqual(source_review_issues, [])
        self.assertEqual(provenance["authority_basis"], "business_answer")
        self.assertTrue(provenance["human_confirmed"])
        self.assertEqual(provenance["confidence_tier"], "confirmed")
        self.assertEqual(len(payload["orders"]), 9)

    def test_resolve_authoritative_payload_marks_source_review_cases_as_provisional(self) -> None:
        task_payload = self._sample_task_payload()
        case_record = {"db_company": "테스트"}
        extraction_candidate = {
            "base_date": "2026-04-13",
            "status": "COMPLETED",
            "reason": None,
            "issues": [],
            "orders": [
                {
                    "fund_code": "F001",
                    "fund_name": "Alpha",
                    "settle_class": "1",
                    "order_type": "3",
                    "base_date": "2026-04-13",
                    "t_day": "01",
                    "transfer_amount": "100",
                }
            ],
        }

        with TemporaryDirectory() as tmp_dir, patch.object(
            review_script,
            "_run_authoritative_review_llm",
            return_value={"selected_candidate": "extraction"},
        ):
            payload, source_review_status, source_review_issues, provenance = review_script._resolve_authoritative_payload_for_case(
                case_record=case_record,
                task_payload=task_payload,
                counterparty_guidance=None,
                review_root=Path(tmp_dir),
                extraction_candidate=extraction_candidate,
                answer_candidate=None,
                model_name="test-model",
                external_registry={},
            )

        self.assertEqual(source_review_status, "PASS")
        self.assertEqual(source_review_issues, [])
        self.assertEqual(payload["orders"], extraction_candidate["orders"])
        self.assertEqual(provenance["authority_basis"], "source_review")
        self.assertFalse(provenance["human_confirmed"])
        self.assertEqual(provenance["confidence_tier"], "provisional")

    def test_review_provenance_marks_source_review_answer_update(self) -> None:
        provisional = review_script._review_provenance(
            authority_basis="source_review",
            external_reference_path=None,
            answer_action="updated",
        )
        confirmed = review_script._review_provenance(
            authority_basis="business_answer",
            external_reference_path="/tmp/reference.md",
            answer_action="updated",
        )

        self.assertTrue(provisional["answer_updated_without_business"])
        self.assertEqual(provisional["confidence_tier"], "provisional")
        self.assertFalse(confirmed["answer_updated_without_business"])
        self.assertEqual(confirmed["confidence_tier"], "confirmed")

    def test_write_report_includes_provenance_sections_and_counts(self) -> None:
        cases = [
            {
                "case_id": "01_case",
                "document_name": "doc1.eml",
                "db_company": "흥국생명",
                "only_pending": False,
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
                "markdown_loss_detected": False,
                "effective_llm_text_kind": "markdown_text",
                "external_reference_path": "/tmp/ref.md",
                "source_review_issues": [],
                "result_json_path": "/tmp/a.json",
                "result_csv_path": "/tmp/a.csv",
                "authoritative_json_path": "/tmp/auth-a.json",
                "debug_root": "/tmp/debug-a",
                "handoff_root": "/tmp/handoff-a",
                "answer_updated_without_business": False,
            },
            {
                "case_id": "02_case",
                "document_name": "doc2.eml",
                "db_company": "기타",
                "only_pending": True,
                "authority_basis": "source_review",
                "confidence_tier": "provisional",
                "human_confirmed": False,
                "status": "COMPLETED",
                "answer_vs_source": {"json_exact": True, "csv_exact": True},
                "extract_vs_source": {"core_exact": True},
                "source_review_status": "PASS",
                "answer_action": "updated",
                "order_count": 1,
                "retry_counts": {"stage_retry_invocations": {"t_day": 1}, "transport_retry_attempts": {}},
                "elapsed_seconds": 2.3,
                "review_verdict": "PASS",
                "base_date": "2026-04-13",
                "markdown_loss_detected": False,
                "effective_llm_text_kind": "markdown_text",
                "external_reference_path": None,
                "source_review_issues": [],
                "result_json_path": "/tmp/b.json",
                "result_csv_path": "/tmp/b.csv",
                "authoritative_json_path": "/tmp/auth-b.json",
                "debug_root": "/tmp/debug-b",
                "handoff_root": "/tmp/handoff-b",
                "answer_updated_without_business": True,
            },
        ]

        with TemporaryDirectory() as tmp_dir, patch.object(review_script, "REPORT_ROOT", Path(tmp_dir)), patch.object(
            review_script,
            "get_settings",
            return_value=SimpleNamespace(llm_base_url="http://localhost:3910/v1"),
        ):
            report_path = review_script._write_report(run_root=Path("/tmp/run-root"), cases=cases)
            report_text = report_path.read_text(encoding="utf-8")

        self.assertIn("## 보조 판정 요약", report_text)
        self.assertIn("`confirmed=1`", report_text)
        self.assertIn("`provisional=1`", report_text)
        self.assertIn("`human_unconfirmed=1`", report_text)
        self.assertIn("tt0911.md", report_text)
        self.assertIn("tt0923.md", report_text)
        self.assertIn("100% 신뢰 가능", report_text)


if __name__ == "__main__":
    unittest.main()
