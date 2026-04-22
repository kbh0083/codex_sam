from __future__ import annotations

from dataclasses import replace
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from app.config import get_settings
from app.document_loader import DocumentLoadTaskPayload, TargetFundScope
from app.extractor import (
    ExtractionOutcomeError,
    FundOrderExtractor,
    InvalidResponseArtifact,
    LLMExtractionOutcome,
    load_counterparty_guidance,
    resolve_counterparty_prompt_name,
    write_invalid_response_debug_files,
)
from app.schemas import ExtractionResult, OrderExtraction, OrderType, SettleClass
from app.service import ExtractionService, NonInstructionDocumentError


class ServiceGuardTests(unittest.TestCase):
    """service 계층의 경계 책임을 검증하는 테스트 모음.

    여기서는 LLM 품질 자체보다 아래 항목을 중점적으로 본다.
    - 출력 직렬화 규칙
    - task payload JSON 저장/복원
    - coverage/blocking issue 가드
    - invalid artifact 저장
    - service도 handler handoff 파일을 실제로 거치는지
    """

    def test_serialize_order_payload_removes_negative_sign_for_red(self) -> None:
        order = OrderExtraction(
            fund_code="F001",
            fund_name="Alpha",
            settle_class=SettleClass.CONFIRMED,
            order_type=OrderType.RED,
            base_date="2025-11-27",
            t_day=0,
            transfer_amount="-100",
        )

        payload = ExtractionService._serialize_order_payload(order)

        self.assertEqual(payload["transfer_amount"], "100")
        self.assertEqual(payload["settle_class"], "2")
        self.assertEqual(payload["order_type"], "1")

    def test_serialize_order_payload_normalizes_integer_like_decimal_artifact(self) -> None:
        order = OrderExtraction(
            fund_code="F001",
            fund_name="Alpha",
            settle_class=SettleClass.CONFIRMED,
            order_type=OrderType.SUB,
            base_date="2025-11-27",
            t_day=0,
            transfer_amount="70000000.00000001",
        )

        payload = ExtractionService._serialize_order_payload(order)

        self.assertEqual(payload["transfer_amount"], "70,000,000")

    def test_serialize_order_payload_keeps_real_decimal_amount(self) -> None:
        order = OrderExtraction(
            fund_code="KB001",
            fund_name="KB Decimal",
            settle_class=SettleClass.CONFIRMED,
            order_type=OrderType.SUB,
            base_date="2025-11-27",
            t_day=0,
            transfer_amount="50572.49",
        )

        payload = ExtractionService._serialize_order_payload(order)

        self.assertEqual(payload["transfer_amount"], "50,572.49")

    def test_serialize_order_payload_preserves_single_decimal_place(self) -> None:
        order = OrderExtraction(
            fund_code="F001",
            fund_name="Decimal",
            settle_class=SettleClass.CONFIRMED,
            order_type=OrderType.SUB,
            base_date="2025-11-27",
            t_day=0,
            transfer_amount="23213.4",
        )

        payload = ExtractionService._serialize_order_payload(order)

        self.assertEqual(payload["transfer_amount"], "23,213.4")

    def test_serialize_order_payload_preserves_three_decimal_places(self) -> None:
        order = OrderExtraction(
            fund_code="F001",
            fund_name="Decimal",
            settle_class=SettleClass.CONFIRMED,
            order_type=OrderType.SUB,
            base_date="2025-11-27",
            t_day=0,
            transfer_amount="23213.409",
        )

        payload = ExtractionService._serialize_order_payload(order)

        self.assertEqual(payload["transfer_amount"], "23,213.409")

    def test_serialize_order_payload_preserves_small_real_decimal_amount(self) -> None:
        order = OrderExtraction(
            fund_code="F001",
            fund_name="Decimal",
            settle_class=SettleClass.CONFIRMED,
            order_type=OrderType.SUB,
            base_date="2025-11-27",
            t_day=0,
            transfer_amount="0.0019",
        )

        payload = ExtractionService._serialize_order_payload(order)

        self.assertEqual(payload["transfer_amount"], "0.0019")

    def test_serialize_order_payload_preserves_document_two_decimal_places(self) -> None:
        order = OrderExtraction(
            fund_code="F001",
            fund_name="Decimal",
            settle_class=SettleClass.CONFIRMED,
            order_type=OrderType.SUB,
            base_date="2025-11-27",
            t_day=0,
            transfer_amount="23,213.40",
        )

        payload = ExtractionService._serialize_order_payload(order)

        self.assertEqual(payload["transfer_amount"], "23,213.40")

    def test_serialize_order_payload_removes_red_sign_and_preserves_decimal_places(self) -> None:
        order = OrderExtraction(
            fund_code="F001",
            fund_name="Decimal",
            settle_class=SettleClass.CONFIRMED,
            order_type=OrderType.RED,
            base_date="2025-11-27",
            t_day=0,
            transfer_amount="-23,213.40",
        )

        payload = ExtractionService._serialize_order_payload(order)

        self.assertEqual(payload["transfer_amount"], "23,213.40")

    def test_serialize_order_payload_keeps_integer_without_decimal_suffix(self) -> None:
        integer_order = OrderExtraction(
            fund_code="F001",
            fund_name="Integer",
            settle_class=SettleClass.CONFIRMED,
            order_type=OrderType.SUB,
            base_date="2025-11-27",
            t_day=0,
            transfer_amount="18711858",
        )
        integer_decimal_order = OrderExtraction(
            fund_code="F002",
            fund_name="Integer Decimal",
            settle_class=SettleClass.CONFIRMED,
            order_type=OrderType.SUB,
            base_date="2025-11-27",
            t_day=0,
            transfer_amount="18711858.00",
        )

        integer_payload = ExtractionService._serialize_order_payload(integer_order)
        integer_decimal_payload = ExtractionService._serialize_order_payload(integer_decimal_order)

        self.assertEqual(integer_payload["transfer_amount"], "18,711,858")
        self.assertEqual(integer_decimal_payload["transfer_amount"], "18,711,858")

    def test_serialize_order_payload_formats_t_day_codes(self) -> None:
        confirmed = OrderExtraction(
            fund_code="F001",
            fund_name="Alpha",
            settle_class=SettleClass.CONFIRMED,
            order_type=OrderType.SUB,
            base_date="2025-11-27",
            t_day=0,
            transfer_amount="100",
        )
        pending = OrderExtraction(
            fund_code="F002",
            fund_name="Beta",
            settle_class=SettleClass.PENDING,
            order_type=OrderType.SUB,
            base_date="2025-11-27",
            t_day=2,
            transfer_amount="100",
        )

        confirmed_payload = ExtractionService._serialize_order_payload(confirmed)
        pending_payload = ExtractionService._serialize_order_payload(pending)

        self.assertEqual(confirmed_payload["t_day"], "01")
        self.assertEqual(pending_payload["t_day"], "03")
        self.assertEqual(confirmed_payload["settle_class"], "2")
        self.assertEqual(pending_payload["settle_class"], "1")
        self.assertEqual(confirmed_payload["order_type"], "3")
        self.assertEqual(pending_payload["order_type"], "3")

    def test_document_load_task_payload_can_be_written_and_read_as_json(self) -> None:
        payload = DocumentLoadTaskPayload(
            source_path="/tmp/sample.pdf",
            file_name="sample.pdf",
            pdf_password="1234",
            content_type="application/pdf",
            raw_text="Closing Date : 2025-11-26\n결제일 : 2025-11-27\n",
            markdown_text="markdown",
            chunks=("chunk-1", "chunk-2"),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=3,
            target_fund_scope=TargetFundScope(
                manager_column_present=True,
                include_all_funds=False,
                fund_codes=frozenset({"F001"}),
                fund_names=frozenset({"Alpha"}),
                canonical_fund_names=frozenset({"alpha"}),
            ),
        )

        with TemporaryDirectory() as tmp_dir:
            json_path = Path(tmp_dir) / "loader_payload.json"
            payload.write_json(json_path)
            restored = DocumentLoadTaskPayload.read_json(json_path)

        self.assertEqual(restored.source_path, payload.source_path)
        self.assertEqual(restored.chunks, payload.chunks)
        self.assertEqual(restored.target_fund_scope.fund_codes, payload.target_fund_scope.fund_codes)
        self.assertFalse(restored.markdown_loss_detected)
        self.assertEqual(restored.markdown_loss_reasons, ())
        self.assertEqual(restored.effective_llm_text_kind, "markdown_text")

    def test_document_load_task_payload_write_json_creates_parent_directories(self) -> None:
        payload = DocumentLoadTaskPayload(
            source_path="/tmp/sample.pdf",
            file_name="sample.pdf",
            pdf_password=None,
            content_type="application/pdf",
            raw_text="raw",
            markdown_text="markdown",
            chunks=("chunk-1",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=1,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )

        with TemporaryDirectory() as tmp_dir:
            json_path = Path(tmp_dir) / "nested" / "job" / "loader_payload.json"
            payload.write_json(json_path)

            self.assertTrue(json_path.exists())
            restored = DocumentLoadTaskPayload.read_json(json_path)

        self.assertEqual(restored.file_name, payload.file_name)

    def test_load_counterparty_guidance_returns_text_for_known_document(self) -> None:
        guidance = load_counterparty_guidance(
            "/tmp/삼성액티브자산운용_설정_해지_20251128.eml",
            use_counterparty_prompt=True,
        )

        self.assertIsNotNone(guidance)
        self.assertIn("BUY & SELL REPORT", guidance or "")

    def test_load_counterparty_guidance_falls_back_to_none_for_unknown_document(self) -> None:
        guidance = load_counterparty_guidance(
            "/tmp/unknown_counterparty.pdf",
            use_counterparty_prompt=True,
        )

        self.assertIsNone(guidance)

    def test_resolve_counterparty_prompt_name_avoids_unrelated_substring_match(self) -> None:
        self.assertEqual(resolve_counterparty_prompt_name("/tmp/ABL_250826.xlsx"), "ABL")
        self.assertEqual(resolve_counterparty_prompt_name("/tmp/DB_250826.xlsx"), "DB")
        self.assertEqual(resolve_counterparty_prompt_name("/tmp/iM라이프_250826.xls"), "IM")
        self.assertEqual(resolve_counterparty_prompt_name("/tmp/KB라이프_250826.XLS"), "KB")
        self.assertEqual(resolve_counterparty_prompt_name("/tmp/라이나_250826.xlsx"), "라이나")
        self.assertEqual(resolve_counterparty_prompt_name("/tmp/신한라이프_251127_20260116_212233.pdf"), "신한라이프")
        self.assertEqual(resolve_counterparty_prompt_name("/tmp/운용지시서_KDB_20251128.eml"), "KDB")
        self.assertEqual(resolve_counterparty_prompt_name("/tmp/hanhwa_20250826.html"), "한화생명")
        self.assertEqual(resolve_counterparty_prompt_name("/tmp/hanhwa2.html"), "한화생명")
        self.assertEqual(resolve_counterparty_prompt_name("/tmp/자금운용_설정_5440_20260316.mht"), "교보생명")
        self.assertEqual(resolve_counterparty_prompt_name("/tmp/하나생명(액티브)_251127.pdf"), "하나생명")
        self.assertEqual(resolve_counterparty_prompt_name("/tmp/하나생명(액티브)_251127.xlsx"), "하나생명")
        self.assertIsNone(resolve_counterparty_prompt_name("/tmp/메트라이프생명_0408.eml"))
        self.assertIsNone(resolve_counterparty_prompt_name("/tmp/global_ablation.xlsx"))
        self.assertIsNone(resolve_counterparty_prompt_name("/tmp/table_balance.xlsx"))

    def test_resolve_counterparty_prompt_name_can_match_metlife_from_document_text(self) -> None:
        self.assertEqual(
            resolve_counterparty_prompt_name(
                "/tmp/6f3ec0dd-aaaa-bbbb-cccc-1234567890ab.html",
                document_text="메트라이프생명\n펀드설정/해지 운용지시서\n1. 추가설정 및 부분해지 내용",
            ),
            "메트라이프생명",
        )

    def test_resolve_counterparty_prompt_name_prefers_metlife_content_over_other_filename_match(self) -> None:
        self.assertEqual(
            resolve_counterparty_prompt_name(
                "/tmp/db_attachment_uuid.html",
                document_text="메트라이프생명\n펀드설정/해지 운용지시서\n1. 추가설정 및 부분해지 내용",
            ),
            "메트라이프생명",
        )

    def test_resolve_counterparty_prompt_name_can_match_heungkuk_from_document_text(self) -> None:
        self.assertEqual(
            resolve_counterparty_prompt_name(
                "/tmp/3278b79a-1c10-40d0-a9be-7c0ea0ce8a19.xls",
                document_text=(
                    "흥국생명\n설정해지 내역 운용지시건\n"
                    "펀드코드 | 추가설정금액 | 당일인출금액"
                ),
            ),
            "흥국생명",
        )

    def test_resolve_counterparty_prompt_name_matches_ibk_from_filename(self) -> None:
        self.assertEqual(
            resolve_counterparty_prompt_name(
                "/tmp/IBK연금보험_0408_삼성자산운용설정해지지시서.eml",
            ),
            "IBK",
        )

    def test_resolve_counterparty_prompt_name_can_match_ibk_from_document_text(self) -> None:
        self.assertEqual(
            resolve_counterparty_prompt_name(
                "/tmp/4d8b5400-1111-2222-3333-444444444444.eml",
                document_text=(
                    "IBK 연금보험 변액보험 펀드 정산 자료\n"
                    "기준일자 : 2026/04/08\n"
                    "예정 정산액 기준일+1\n"
                    "펀드코드\n"
                ),
            ),
            "IBK",
        )

    def test_load_counterparty_guidance_returns_text_for_ibk_document(self) -> None:
        guidance = load_counterparty_guidance(
            "/tmp/IBK연금보험_0408_삼성자산운용설정해지지시서.eml",
            use_counterparty_prompt=True,
        )

        self.assertIsNotNone(guidance)
        self.assertIn("기준일자", guidance or "")
        self.assertIn("정산액", guidance or "")
        self.assertIn("예정 정산액 기준일+1", guidance or "")
        self.assertIn("only_pending=true", guidance or "")
        self.assertIn("full internal order set", guidance or "")

    def test_resolve_counterparty_prompt_name_matches_heungkuk_hanais_from_xlsx_filename(self) -> None:
        self.assertEqual(
            resolve_counterparty_prompt_name(
                "/tmp/흥국생명-hanais-0407-지시서.xlsx",
            ),
            "흥국생명-hanais",
        )

    def test_resolve_counterparty_prompt_name_matches_heungkuk_hanais_from_pdf_filename(self) -> None:
        self.assertEqual(
            resolve_counterparty_prompt_name(
                "/tmp/흥국생명-hanais-0407-지시서.pdf",
            ),
            "흥국생명-hanais",
        )

    def test_resolve_counterparty_prompt_name_can_match_heungkuk_hanais_from_document_text(self) -> None:
        self.assertEqual(
            resolve_counterparty_prompt_name(
                "/tmp/e900d4d5-d857-4e6c-8b34-d5173e12ffb6.pdf",
                document_text=(
                    "운용사명 | 운용사 펀드코드 | 펀드명 | 설정액 | 해지액 | 설정해지유형구분\n"
                    "흥국생명보험\n"
                ),
            ),
            "흥국생명-hanais",
        )

    def test_load_counterparty_guidance_returns_text_for_heungkuk_hanais_document(self) -> None:
        guidance = load_counterparty_guidance(
            "/tmp/흥국생명-hanais-0407-지시서.xlsx",
            use_counterparty_prompt=True,
        )

        self.assertIsNotNone(guidance)
        self.assertIn("spreadsheet attachment", guidance or "")
        self.assertIn("PDF copy", guidance or "")
        self.assertIn("운용사 펀드코드", guidance or "")
        self.assertIn("거래유형` or `구분`", guidance or "")
        self.assertIn("only_pending=false", guidance or "")
        self.assertIn("keep confirmed orders as confirmed", guidance or "")
        self.assertNotIn("source label/file name", guidance or "")

    def test_resolve_counterparty_prompt_name_can_match_cardif_from_document_text(self) -> None:
        self.assertEqual(
            resolve_counterparty_prompt_name(
                "/tmp/8c18f7aa-f31f-4c84-98a8-1cc66c1b96bb.xlsx",
                document_text=(
                    "BNP Paribas Cardif Life Insurance\n"
                    "The order of Subscription and Redemption\n"
                    "Amount(KRW)\n"
                ),
            ),
            "카디프",
        )

    def test_load_counterparty_guidance_returns_text_for_cardif_document(self) -> None:
        guidance = load_counterparty_guidance(
            "/tmp/카디프_251127.xlsx",
            use_counterparty_prompt=True,
        )

        self.assertIsNotNone(guidance)
        self.assertIn("Use only the page-based PDF document", guidance or "")
        self.assertIn("duplicate XLSX copy", guidance or "")
        self.assertIn("## Page 1", guidance or "")
        self.assertIn("## Sheet", guidance or "")
        self.assertIn("Amount(KRW)", guidance or "")
        self.assertIn("Classify every extracted row as `CONFIRMED` internally", guidance or "")
        self.assertIn("only_pending=true", guidance or "")
        self.assertNotIn("source label/file name", guidance or "")

    def test_load_counterparty_guidance_returns_text_for_hanalife_document(self) -> None:
        guidance = load_counterparty_guidance(
            "/tmp/하나생명(액티브)_251127.xlsx",
            use_counterparty_prompt=True,
        )

        self.assertIsNotNone(guidance)
        self.assertIn("sheet-based XLSX workbook", guidance or "")
        self.assertIn("duplicate PDF copy; use XLSX attachment", guidance or "")
        self.assertIn("거래유형명", guidance or "")
        self.assertIn("설정해지금액", guidance or "")
        self.assertIn("## Sheet", guidance or "")
        self.assertIn("## Page 1", guidance or "")
        self.assertIn("only_pending=true", guidance or "")
        self.assertNotIn("HANAIS-style export sheets", guidance or "")
        self.assertNotIn("source label/file name", guidance or "")

    def test_resolve_counterparty_prompt_name_does_not_overmatch_metlife_from_filename_only(self) -> None:
        self.assertIsNone(resolve_counterparty_prompt_name("/tmp/미래에셋_메트라이프생명_참고.txt"))

    def test_resolve_counterparty_prompt_name_reads_mapping_from_yaml(self) -> None:
        with TemporaryDirectory() as temp_dir:
            mapping_path = Path(temp_dir) / "counterparty_prompt_map.yaml"
            mapping_path.write_text(
                (
                    "mappings:\n"
                    "  - prompt_name: 카디프\n"
                    "    match_tokens:\n"
                    "      - custom_cardif\n"
                    "  - prompt_name: 메트라이프생명\n"
                    "    content_match_tokens:\n"
                    "      - 메트라이프생명\n"
                    "      - 펀드설정/해지 운용지시서\n"
                ),
                encoding="utf-8",
            )

            self.assertEqual(
                resolve_counterparty_prompt_name("/tmp/custom_cardif.pdf", mapping_path=mapping_path),
                "카디프",
            )
            self.assertEqual(
                resolve_counterparty_prompt_name(
                    "/tmp/uuid.html",
                    mapping_path=mapping_path,
                    document_text="메트라이프생명\n펀드설정/해지 운용지시서",
                ),
                "메트라이프생명",
            )

    def test_load_counterparty_guidance_uses_mapping_file(self) -> None:
        with TemporaryDirectory() as temp_dir:
            mapping_path = Path(temp_dir) / "counterparty_prompt_map.yaml"
            mapping_path.write_text(
                (
                    "mappings:\n"
                    "  - prompt_name: 카디프\n"
                    "    match_tokens:\n"
                    "      - custom_cardif\n"
                ),
                encoding="utf-8",
            )

            guidance = load_counterparty_guidance(
                "/tmp/custom_cardif.pdf",
                use_counterparty_prompt=True,
                mapping_path=mapping_path,
            )

            self.assertIsNotNone(guidance)
            self.assertIn("Subscription", guidance or "")

    def test_load_counterparty_guidance_falls_back_to_none_for_invalid_mapping_file(self) -> None:
        with TemporaryDirectory() as temp_dir:
            mapping_path = Path(temp_dir) / "counterparty_prompt_map.yaml"
            mapping_path.write_text("mappings:\n  - prompt_name: 카디프\n", encoding="utf-8")

            guidance = load_counterparty_guidance(
                "/tmp/카디프_251127.pdf",
                use_counterparty_prompt=True,
                mapping_path=mapping_path,
            )

            self.assertIsNone(guidance)

    def test_load_counterparty_guidance_skips_invalid_rows_and_uses_valid_rows(self) -> None:
        with TemporaryDirectory() as temp_dir:
            mapping_path = Path(temp_dir) / "counterparty_prompt_map.yaml"
            mapping_path.write_text(
                (
                    "mappings:\n"
                    "  - prompt_name: \n"
                    "    match_tokens:\n"
                    "      - broken\n"
                    "  - prompt_name: 카디프\n"
                    "    match_tokens:\n"
                    "      - custom_cardif\n"
                ),
                encoding="utf-8",
            )

            guidance = load_counterparty_guidance(
                "/tmp/custom_cardif.pdf",
                use_counterparty_prompt=True,
                mapping_path=mapping_path,
            )

            self.assertIsNotNone(guidance)
            self.assertIn("Subscription", guidance or "")

    def test_load_counterparty_guidance_falls_back_to_none_for_invalid_mapping_top_level(self) -> None:
        with TemporaryDirectory() as temp_dir:
            mapping_path = Path(temp_dir) / "counterparty_prompt_map.yaml"
            mapping_path.write_text("- prompt_name: 카디프\n", encoding="utf-8")

            guidance = load_counterparty_guidance(
                "/tmp/카디프_251127.pdf",
                use_counterparty_prompt=True,
                mapping_path=mapping_path,
            )

            self.assertIsNone(guidance)

    def test_extract_from_task_payload_rejects_blocking_issues(self) -> None:
        extractor = FundOrderExtractor(get_settings())
        task_payload = DocumentLoadTaskPayload(
            source_path="/tmp/sample.pdf",
            file_name="sample.pdf",
            pdf_password=None,
            content_type="application/pdf",
            raw_text="Closing Date : 2025-11-26\n결제일 : 2025-11-27\n",
            markdown_text="markdown",
            chunks=("chunk-1",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=1,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )

        extractor.extract = lambda chunks, raw_text=None, markdown_text=None, target_fund_scope=None, counterparty_guidance=None, expected_order_count=None: LLMExtractionOutcome(  # type: ignore[method-assign]
            result=ExtractionResult(
                orders=[
                    OrderExtraction(
                        fund_code="F001",
                        fund_name="Alpha",
                        settle_class=SettleClass.CONFIRMED,
                        order_type=OrderType.SUB,
                        base_date="2025-11-27",
                        t_day=0,
                        transfer_amount="100",
                    )
                ],
                issues=["TRANSFER_AMOUNT_MISSING"],
            )
        )

        with self.assertRaisesRegex(ValueError, "incomplete"):
            extractor.extract_from_task_payload(task_payload)

    def test_extract_from_task_payload_preemptively_rejects_cardif_duplicate_xlsx_copy(self) -> None:
        extractor = FundOrderExtractor(get_settings())
        task_payload = DocumentLoadTaskPayload(
            source_path="/tmp/카디프_251127.xlsx",
            file_name="카디프_251127.xlsx",
            pdf_password=None,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            raw_text="[SHEET Instruction]\nAmount(KRW)\n",
            markdown_text="## Sheet Instruction\n\n| Amount(KRW) |\n| --- |\n| 1 |\n",
            chunks=("chunk-1",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=1,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )
        extractor.extract = lambda *args, **kwargs: self.fail("duplicate copy should not reach LLM extraction")  # type: ignore[method-assign]

        with self.assertRaisesRegex(ValueError, r"duplicate XLSX copy; use PDF attachment"):
            extractor.extract_from_task_payload(task_payload)

    def test_extract_from_task_payload_preemptively_rejects_hanalife_legacy_pdf_copy(self) -> None:
        extractor = FundOrderExtractor(get_settings())
        task_payload = DocumentLoadTaskPayload(
            source_path="/tmp/하나생명(액티브)_251127.pdf",
            file_name="하나생명(액티브)_251127.pdf",
            pdf_password=None,
            content_type="application/pdf",
            raw_text="[PAGE 1]\n거래유형명\n설정해지금액\n",
            markdown_text="## Page 1\n\n거래유형명\n설정해지금액\n",
            chunks=("chunk-1",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=1,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )
        extractor.extract = lambda *args, **kwargs: self.fail("duplicate copy should not reach LLM extraction")  # type: ignore[method-assign]

        with self.assertRaisesRegex(ValueError, r"duplicate PDF copy; use XLSX attachment"):
            extractor.extract_from_task_payload(task_payload)

    def test_extract_from_task_payload_preemptively_rejects_heungkuk_hanais_duplicate_pdf_hint(self) -> None:
        extractor = FundOrderExtractor(get_settings())
        task_payload = DocumentLoadTaskPayload(
            source_path="/tmp/흥국생명-hanais-0407-지시서.pdf",
            file_name="흥국생명-hanais-0407-지시서.pdf",
            pdf_password=None,
            content_type="application/pdf",
            raw_text=(
                "Heungkuk Life HANAIS duplicate PDF copy\n"
                "Use XLSX attachment for extraction\n"
                "Same instruction content as spreadsheet attachment\n"
            ),
            markdown_text="Heungkuk Life HANAIS duplicate PDF copy\nUse XLSX attachment for extraction\n",
            chunks=("chunk-1",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=1,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )
        extractor.extract = lambda *args, **kwargs: self.fail("duplicate copy should not reach LLM extraction")  # type: ignore[method-assign]

        with self.assertRaisesRegex(ValueError, r"duplicate PDF copy; use XLSX attachment"):
            extractor.extract_from_task_payload(task_payload)

    def test_extract_from_task_payload_ignores_precheck_prompt_mapping_failure(self) -> None:
        extractor = FundOrderExtractor(get_settings())
        task_payload = DocumentLoadTaskPayload(
            source_path="/tmp/sample.pdf",
            file_name="sample.pdf",
            pdf_password=None,
            content_type="application/pdf",
            raw_text="Closing Date : 2025-11-26\n결제일 : 2025-11-27\n",
            markdown_text="markdown",
            chunks=("chunk-1",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=1,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )
        extractor.extract = lambda chunks, raw_text=None, markdown_text=None, target_fund_scope=None, counterparty_guidance=None, expected_order_count=None, markdown_loss_detected=False: LLMExtractionOutcome(  # type: ignore[method-assign]
            result=ExtractionResult(
                orders=[
                    OrderExtraction(
                        fund_code="F001",
                        fund_name="Alpha",
                        settle_class=SettleClass.CONFIRMED,
                        order_type=OrderType.SUB,
                        base_date="2025-11-27",
                        t_day=0,
                        transfer_amount="100",
                    )
                ],
                issues=[],
            )
        )

        with patch("app.extractor.resolve_counterparty_prompt_name", side_effect=RuntimeError("broken mapping")):
            outcome = extractor.extract_from_task_payload(task_payload)

        self.assertEqual(len(outcome.result.orders), 1)
        self.assertEqual(outcome.result.orders[0].fund_code, "F001")

    def test_extract_from_task_payload_rejects_llm_classified_non_instruction_document(self) -> None:
        extractor = FundOrderExtractor(get_settings())
        task_payload = DocumentLoadTaskPayload(
            source_path="/tmp/wrapper.eml",
            file_name="wrapper.eml",
            pdf_password=None,
            content_type="message/rfc822",
            raw_text="wrapper",
            markdown_text="wrapper",
            chunks=("chunk-1",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=0,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )

        extractor.extract = lambda chunks, raw_text=None, markdown_text=None, target_fund_scope=None, counterparty_guidance=None, expected_order_count=None: LLMExtractionOutcome(  # type: ignore[method-assign]
            result=ExtractionResult(
                orders=[],
                issues=["DOCUMENT_NOT_INSTRUCTION: cover email only"],
            )
        )

        with self.assertRaisesRegex(ValueError, "not a variable-annuity order instruction"):
            extractor.extract_from_task_payload(task_payload)

    def test_extract_from_task_payload_allows_empty_result_for_no_order_document(self) -> None:
        extractor = FundOrderExtractor(get_settings())
        task_payload = DocumentLoadTaskPayload(
            source_path="/tmp/sample.pdf",
            file_name="sample.pdf",
            pdf_password=None,
            content_type="application/pdf",
            raw_text="raw",
            markdown_text="markdown",
            chunks=("chunk-1",),
            non_instruction_reason=None,
            allow_empty_result=True,
            scope_excludes_all_funds=False,
            expected_order_count=0,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )

        outcome = extractor.extract_from_task_payload(task_payload)

        self.assertEqual(outcome.result.orders, [])
        self.assertEqual(outcome.result.issues, [])

    def test_extract_from_task_payload_keeps_coverage_mismatch_as_warning(self) -> None:
        extractor = FundOrderExtractor(get_settings())
        task_payload = DocumentLoadTaskPayload(
            source_path="/tmp/sample.pdf",
            file_name="sample.pdf",
            pdf_password=None,
            content_type="application/pdf",
            raw_text="Closing Date : 2025-11-26\n결제일 : 2025-11-27\n",
            markdown_text="markdown",
            chunks=("chunk-1",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=3,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )

        extractor.extract = lambda chunks, raw_text=None, markdown_text=None, target_fund_scope=None, counterparty_guidance=None, expected_order_count=None: LLMExtractionOutcome(  # type: ignore[method-assign]
            result=ExtractionResult(
                orders=[
                    OrderExtraction(
                        fund_code="F001",
                        fund_name="Alpha",
                        settle_class=SettleClass.CONFIRMED,
                        order_type=OrderType.SUB,
                        base_date="2025-11-27",
                        t_day=0,
                        transfer_amount="100",
                    )
                ],
                issues=[],
            )
        )

        outcome = extractor.extract_from_task_payload(task_payload)

        self.assertEqual(len(outcome.result.orders), 1)
        self.assertEqual(outcome.result.issues, ["ORDER_COVERAGE_MISMATCH"])

    def test_extract_from_task_payload_still_rejects_other_blocking_issue_even_with_coverage_mismatch(self) -> None:
        extractor = FundOrderExtractor(get_settings())
        task_payload = DocumentLoadTaskPayload(
            source_path="/tmp/sample.pdf",
            file_name="sample.pdf",
            pdf_password=None,
            content_type="application/pdf",
            raw_text="raw",
            markdown_text="markdown",
            chunks=("chunk-1",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=2,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )

        extractor.extract = lambda chunks, raw_text=None, markdown_text=None, target_fund_scope=None, counterparty_guidance=None, expected_order_count=None: LLMExtractionOutcome(  # type: ignore[method-assign]
            result=ExtractionResult(
                orders=[
                    OrderExtraction(
                        fund_code="F001",
                        fund_name="Alpha",
                        settle_class=SettleClass.CONFIRMED,
                        order_type=OrderType.SUB,
                        base_date="2025-11-27",
                        t_day=0,
                        transfer_amount="100",
                    )
                ],
                issues=["TRANSFER_AMOUNT_MISSING"],
            )
        )

        with self.assertRaisesRegex(ValueError, "TRANSFER_AMOUNT_MISSING"):
            extractor.extract_from_task_payload(task_payload)

    def test_extract_from_task_payload_downgrades_coverage_mismatch_when_deterministic_orders_match(self) -> None:
        extractor = FundOrderExtractor(get_settings())
        expected_order = OrderExtraction(
            fund_code="F001",
            fund_name="Alpha",
            settle_class=SettleClass.CONFIRMED,
            order_type=OrderType.SUB,
            base_date="2025-11-27",
            t_day=0,
            transfer_amount="100",
        )
        task_payload = DocumentLoadTaskPayload(
            source_path="/tmp/sample.pdf",
            file_name="sample.pdf",
            pdf_password=None,
            content_type="application/pdf",
            raw_text="raw",
            markdown_text="markdown",
            chunks=("chunk-1",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=2,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )

        extractor.extract = lambda chunks, raw_text=None, markdown_text=None, target_fund_scope=None, counterparty_guidance=None, expected_order_count=None: LLMExtractionOutcome(  # type: ignore[method-assign]
            result=ExtractionResult(
                orders=[expected_order],
                issues=[],
            )
        )
        extractor._build_deterministic_markdown_orders = lambda markdown_text, raw_text=None, target_fund_scope=None: [expected_order]  # type: ignore[method-assign]

        outcome = extractor.extract_from_task_payload(task_payload)

        self.assertEqual(outcome.result.orders, [expected_order])
        self.assertEqual(outcome.result.issues, [])

    def test_extract_from_task_payload_downgrades_preexisting_coverage_mismatch_when_deterministic_orders_match(self) -> None:
        extractor = FundOrderExtractor(get_settings())
        expected_order = OrderExtraction(
            fund_code="F001",
            fund_name="Alpha",
            settle_class=SettleClass.CONFIRMED,
            order_type=OrderType.SUB,
            base_date="2025-11-27",
            t_day=0,
            transfer_amount="100",
        )
        task_payload = DocumentLoadTaskPayload(
            source_path="/tmp/sample.pdf",
            file_name="sample.pdf",
            pdf_password=None,
            content_type="application/pdf",
            raw_text="raw",
            markdown_text="markdown",
            chunks=("chunk-1",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=2,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )

        extractor.extract = lambda chunks, raw_text=None, markdown_text=None, target_fund_scope=None, counterparty_guidance=None, expected_order_count=None: LLMExtractionOutcome(  # type: ignore[method-assign]
            result=ExtractionResult(
                orders=[expected_order],
                issues=["ORDER_COVERAGE_MISMATCH"],
            )
        )
        extractor._build_deterministic_markdown_orders = lambda markdown_text, raw_text=None, target_fund_scope=None: [expected_order]  # type: ignore[method-assign]

        outcome = extractor.extract_from_task_payload(task_payload)

        self.assertEqual(outcome.result.orders, [expected_order])
        self.assertEqual(outcome.result.issues, [])

    def test_extract_from_task_payload_downgrades_coverage_mismatch_when_corroboration_has_duplicates_and_reference_date(self) -> None:
        extractor = FundOrderExtractor(get_settings())
        extracted_order = OrderExtraction(
            fund_code="F001",
            fund_name="Alpha",
            settle_class=SettleClass.CONFIRMED,
            order_type=OrderType.SUB,
            base_date="2025-11-27",
            t_day=0,
            transfer_amount="100",
        )
        task_payload = DocumentLoadTaskPayload(
            source_path="/tmp/sample.pdf",
            file_name="sample.pdf",
            pdf_password=None,
            content_type="application/pdf",
            raw_text="Closing Date : 2025-11-26\n결제일 : 2025-11-27\n",
            markdown_text="markdown",
            chunks=("chunk-1",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=3,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )

        extractor.extract = lambda chunks, raw_text=None, markdown_text=None, target_fund_scope=None, counterparty_guidance=None, expected_order_count=None: LLMExtractionOutcome(  # type: ignore[method-assign]
            result=ExtractionResult(
                orders=[extracted_order],
                issues=[],
            )
        )
        extractor._build_deterministic_markdown_orders = lambda markdown_text, raw_text=None, target_fund_scope=None: [  # type: ignore[method-assign]
            OrderExtraction(
                fund_code="F001",
                fund_name="Alpha",
                settle_class=SettleClass.CONFIRMED,
                order_type=OrderType.SUB,
                base_date="2025-11-26",
                t_day=0,
                transfer_amount="100",
            ),
            OrderExtraction(
                fund_code="F001",
                fund_name="Alpha",
                settle_class=SettleClass.CONFIRMED,
                order_type=OrderType.SUB,
                base_date="2025-11-26",
                t_day=0,
                transfer_amount="100",
            ),
        ]

        outcome = extractor.extract_from_task_payload(task_payload)

        self.assertEqual(outcome.result.orders, [extracted_order])
        self.assertEqual(outcome.result.issues, [])

    def test_extract_from_task_payload_keeps_coverage_mismatch_when_base_date_mismatch_has_no_reference_date_proof(self) -> None:
        extractor = FundOrderExtractor(get_settings())
        extracted_order = OrderExtraction(
            fund_code="F001",
            fund_name="Alpha",
            settle_class=SettleClass.CONFIRMED,
            order_type=OrderType.SUB,
            base_date="2025-11-27",
            t_day=0,
            transfer_amount="100",
        )
        task_payload = DocumentLoadTaskPayload(
            source_path="/tmp/sample.pdf",
            file_name="sample.pdf",
            pdf_password=None,
            content_type="application/pdf",
            raw_text="2025-11-26\n2025-11-27\n",
            markdown_text="markdown",
            chunks=("chunk-1",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=3,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )

        extractor.extract = lambda chunks, raw_text=None, markdown_text=None, target_fund_scope=None, counterparty_guidance=None, expected_order_count=None: LLMExtractionOutcome(  # type: ignore[method-assign]
            result=ExtractionResult(
                orders=[extracted_order],
                issues=[],
            )
        )
        extractor._build_deterministic_markdown_orders = lambda markdown_text, raw_text=None, target_fund_scope=None: [  # type: ignore[method-assign]
            OrderExtraction(
                fund_code="F001",
                fund_name="Alpha",
                settle_class=SettleClass.CONFIRMED,
                order_type=OrderType.SUB,
                base_date="2025-11-26",
                t_day=0,
                transfer_amount="100",
            ),
            OrderExtraction(
                fund_code="F001",
                fund_name="Alpha",
                settle_class=SettleClass.CONFIRMED,
                order_type=OrderType.SUB,
                base_date="2025-11-26",
                t_day=0,
                transfer_amount="100",
            ),
        ]

        outcome = extractor.extract_from_task_payload(task_payload)

        self.assertEqual(outcome.result.orders, [extracted_order])
        self.assertEqual(outcome.result.issues, ["ORDER_COVERAGE_MISMATCH"])

    def test_extract_from_task_payload_removes_transfer_amount_conflict_when_independent_orders_match(self) -> None:
        extractor = FundOrderExtractor(get_settings())
        expected_order = OrderExtraction(
            fund_code="F001",
            fund_name="Alpha",
            settle_class=SettleClass.CONFIRMED,
            order_type=OrderType.SUB,
            base_date="2025-11-27",
            t_day=0,
            transfer_amount="100",
        )
        task_payload = DocumentLoadTaskPayload(
            source_path="/tmp/sample.pdf",
            file_name="sample.pdf",
            pdf_password=None,
            content_type="application/pdf",
            raw_text="raw",
            markdown_text="markdown",
            chunks=("chunk-1",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=1,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )

        extractor.extract = lambda chunks, raw_text=None, markdown_text=None, target_fund_scope=None, counterparty_guidance=None, expected_order_count=None: LLMExtractionOutcome(  # type: ignore[method-assign]
            result=ExtractionResult(
                orders=[expected_order],
                issues=["TRANSFER_AMOUNT_CONFLICT"],
            )
        )
        extractor._build_deterministic_markdown_orders = lambda markdown_text, raw_text=None, target_fund_scope=None: [expected_order]  # type: ignore[method-assign]

        outcome = extractor.extract_from_task_payload(task_payload)

        self.assertEqual(outcome.result.orders, [expected_order])
        self.assertEqual(outcome.result.issues, [])

    def test_extract_from_task_payload_keeps_transfer_amount_conflict_when_other_blocking_issue_remains(self) -> None:
        extractor = FundOrderExtractor(get_settings())
        expected_order = OrderExtraction(
            fund_code="F001",
            fund_name="Alpha",
            settle_class=SettleClass.CONFIRMED,
            order_type=OrderType.SUB,
            base_date="2025-11-27",
            t_day=0,
            transfer_amount="100",
        )
        task_payload = DocumentLoadTaskPayload(
            source_path="/tmp/sample.pdf",
            file_name="sample.pdf",
            pdf_password=None,
            content_type="application/pdf",
            raw_text="raw",
            markdown_text="markdown",
            chunks=("chunk-1",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=1,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )

        extractor.extract = lambda chunks, raw_text=None, markdown_text=None, target_fund_scope=None, counterparty_guidance=None, expected_order_count=None: LLMExtractionOutcome(  # type: ignore[method-assign]
            result=ExtractionResult(
                orders=[expected_order],
                issues=["TRANSFER_AMOUNT_CONFLICT", "ORDER_TYPE_MISSING"],
            )
        )
        extractor._build_deterministic_markdown_orders = lambda markdown_text, raw_text=None, target_fund_scope=None: [expected_order]  # type: ignore[method-assign]

        with self.assertRaisesRegex(ValueError, "ORDER_TYPE_MISSING"):
            extractor.extract_from_task_payload(task_payload)

    def test_validate_source_path_resolves_filename_inside_document_input_dir(self) -> None:
        service = ExtractionService()

        with TemporaryDirectory() as tmp_dir:
            document_dir = Path(tmp_dir)
            target_file = document_dir / "sample.pdf"
            target_file.write_text("dummy", encoding="utf-8")

            object.__setattr__(
                service,
                "settings",
                replace(service.settings, document_input_dir=document_dir),
            )

            resolved = service._validate_source_path(Path("sample.pdf"))

        self.assertEqual(resolved, target_file)

    def test_validate_source_path_prefers_document_input_dir_for_bare_filename(self) -> None:
        service = ExtractionService()

        with TemporaryDirectory() as tmp_dir:
            temp_root = Path(tmp_dir)
            document_dir = temp_root / "documents"
            document_dir.mkdir()
            env_file = document_dir / "sample.pdf"
            env_file.write_text("from-env", encoding="utf-8")

            cwd_file = temp_root / "sample.pdf"
            cwd_file.write_text("from-cwd", encoding="utf-8")

            object.__setattr__(
                service,
                "settings",
                replace(service.settings, document_input_dir=document_dir),
            )

            original_cwd = Path.cwd()
            try:
                os.chdir(temp_root)
                resolved = service._validate_source_path(Path("sample.pdf"))
            finally:
                os.chdir(original_cwd)

        self.assertEqual(resolved, env_file)

    def test_extract_file_path_rejects_non_instruction_document(self) -> None:
        service = ExtractionService()
        payload = DocumentLoadTaskPayload(
            source_path="/tmp/non_instruction.xls",
            file_name="non_instruction.xls",
            pdf_password=None,
            content_type="application/vnd.ms-excel",
            raw_text="dummy",
            markdown_text="dummy",
            chunks=("chunk-1",),
            non_instruction_reason="비지시서",
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=0,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )
        service._build_task_payload = lambda file_path, pdf_password=None: (  # type: ignore[method-assign]
            payload,
            Path("/tmp/non_instruction.xls"),
        )

        with self.assertRaisesRegex(NonInstructionDocumentError, "not a variable-annuity order instruction"):
            service.extract_file_path(Path("/tmp/non_instruction.xls"))

    def test_extract_file_path_wraps_llm_classified_non_instruction_document(self) -> None:
        service = ExtractionService()
        payload = DocumentLoadTaskPayload(
            source_path="/tmp/wrapper.eml",
            file_name="wrapper.eml",
            pdf_password=None,
            content_type="message/rfc822",
            raw_text="wrapper",
            markdown_text="wrapper",
            chunks=("chunk-1",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=0,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )
        service._build_task_payload = lambda file_path, pdf_password=None: (  # type: ignore[method-assign]
            payload,
            Path("/tmp/wrapper.eml"),
        )
        service.extractor.extract_from_task_payload = lambda task_payload, *, counterparty_guidance=None, extract_log_path=None: (_ for _ in ()).throw(  # type: ignore[method-assign]
            ValueError(
                "Document is not a variable-annuity order instruction. "
                "path=/tmp/wrapper.eml reason=DOCUMENT_NOT_INSTRUCTION: cover email only"
            )
        )

        with self.assertRaisesRegex(NonInstructionDocumentError, "not a variable-annuity order instruction"):
            service.extract_file_path(Path("/tmp/wrapper.eml"))

    def test_extract_file_path_reloads_task_payload_from_handoff_file(self) -> None:
        service = ExtractionService()
        payload = DocumentLoadTaskPayload(
            source_path="/tmp/sample.pdf",
            file_name="sample.pdf",
            pdf_password=None,
            content_type="application/pdf",
            raw_text="raw",
            markdown_text="markdown",
            chunks=("chunk-1",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=1,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )
        service._build_task_payload = lambda file_path, pdf_password=None: (  # type: ignore[method-assign]
            payload,
            Path("/tmp/sample.pdf"),
        )

        seen: dict[str, object] = {}

        def _capture(
            task_payload: DocumentLoadTaskPayload,
            *,
            counterparty_guidance: str | None = None,
            extract_log_path: Path | None = None,
        ) -> LLMExtractionOutcome:
            """service가 handoff JSON 재로딩 후 새 payload 인스턴스를 넘기는지 확인한다."""
            # service가 handoff JSON을 거친 뒤 다시 읽은 payload를 넘겨주는지 검증한다.
            # 같은 내용이어야 하지만, 같은 인스턴스일 필요는 없고 오히려 새 인스턴스여야 한다.
            seen["task_payload"] = task_payload
            return LLMExtractionOutcome(
                result=ExtractionResult(
                    orders=[
                        OrderExtraction(
                            fund_code="F001",
                            fund_name="Alpha",
                            settle_class=SettleClass.CONFIRMED,
                            order_type=OrderType.SUB,
                            base_date="2025-11-27",
                            t_day=0,
                            transfer_amount="100",
                        )
                    ],
                    issues=[],
                )
            )

        service.extractor.extract_from_task_payload = _capture  # type: ignore[method-assign]

        result = service.extract_file_path(Path("/tmp/sample.pdf"))
        reloaded_payload = seen["task_payload"]

        self.assertEqual(len(result.orders), 1)
        self.assertIsInstance(reloaded_payload, DocumentLoadTaskPayload)
        self.assertIsNot(reloaded_payload, payload)
        self.assertEqual(reloaded_payload.source_path, payload.source_path)
        self.assertEqual(reloaded_payload.markdown_text, payload.markdown_text)

    def test_extract_file_path_keeps_handoff_file_when_deletion_disabled(self) -> None:
        service = ExtractionService()
        payload = DocumentLoadTaskPayload(
            source_path="/tmp/sample.pdf",
            file_name="sample.pdf",
            pdf_password=None,
            content_type="application/pdf",
            raw_text="raw",
            markdown_text="markdown",
            chunks=("chunk-1",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=1,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )
        service._build_task_payload = lambda file_path, pdf_password=None: (  # type: ignore[method-assign]
            payload,
            Path("/tmp/sample.pdf"),
        )
        service.extractor.extract_from_task_payload = lambda task_payload, counterparty_guidance=None, extract_log_path=None: LLMExtractionOutcome(  # type: ignore[method-assign]
            result=ExtractionResult(
                orders=[
                    OrderExtraction(
                        fund_code="F001",
                        fund_name="Alpha",
                        settle_class=SettleClass.CONFIRMED,
                        order_type=OrderType.SUB,
                        base_date="2025-11-27",
                        t_day=0,
                        transfer_amount="100",
                    )
                ],
                issues=[],
            )
        )

        with TemporaryDirectory() as tmp_dir:
            object.__setattr__(
                service,
                "settings",
                replace(
                    service.settings,
                    debug_output_dir=Path(tmp_dir) / "debug",
                    task_payload_output_dir=Path(tmp_dir) / "handoff",
                    delete_task_payload_files=False,
                ),
            )
            service.extract_file_path(Path("/tmp/sample.pdf"))
            handoff_files = sorted((Path(tmp_dir) / "handoff").glob("**/*_task_payload.json"))

        self.assertEqual(len(handoff_files), 1)
        self.assertTrue(handoff_files[0].name.endswith("_task_payload.json"))

    def test_extract_file_path_writes_invalid_response_artifacts_before_raising(self) -> None:
        service = ExtractionService()
        payload = DocumentLoadTaskPayload(
            source_path="/tmp/sample.pdf",
            file_name="sample.pdf",
            pdf_password=None,
            content_type="application/pdf",
            raw_text="raw",
            markdown_text="markdown",
            chunks=("chunk-1",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=1,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )
        service._build_task_payload = lambda file_path, pdf_password=None: (  # type: ignore[method-assign]
            payload,
            Path("/tmp/sample.pdf"),
        )

        outcome = LLMExtractionOutcome(
            result=ExtractionResult(issues=["LLM_INVALID_RESPONSE_FORMAT"]),
            invalid_response_artifacts=[
                InvalidResponseArtifact(
                    chunk_index=1,
                    raw_response='{"broken": ',
                    stage_name="fund_inventory",
                )
            ],
        )

        def _raise_failure(
            task_payload: DocumentLoadTaskPayload,
            *,
            counterparty_guidance: str | None = None,
            extract_log_path: Path | None = None,
        ) -> LLMExtractionOutcome:
            """invalid artifact를 실은 실패 예외를 의도적으로 발생시킨다."""
            raise ExtractionOutcomeError(
                "Extraction result is incomplete and was not stored. blocking_issues=['LLM_INVALID_RESPONSE_FORMAT']",
                outcome,
            )

        service.extractor.extract_from_task_payload = _raise_failure  # type: ignore[method-assign]

        with TemporaryDirectory() as tmp_dir:
            object.__setattr__(
                service,
                "settings",
                replace(service.settings, debug_output_dir=Path(tmp_dir)),
            )

            with self.assertRaisesRegex(ValueError, "incomplete"):
                service.extract_file_path(Path("/tmp/sample.pdf"))

            debug_files = list(Path(tmp_dir).glob("*llm_invalid_response.txt"))
            self.assertEqual(len(debug_files), 1)
            debug_content = debug_files[0].read_text(encoding="utf-8")

        self.assertEqual(debug_content, '{"broken": ')

    def test_extract_file_path_applies_only_pending_filter(self) -> None:
        service = ExtractionService()
        payload = DocumentLoadTaskPayload(
            source_path="/tmp/sample.pdf",
            file_name="sample.pdf",
            pdf_password=None,
            content_type="application/pdf",
            raw_text="raw",
            markdown_text="markdown",
            chunks=("chunk-1",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=2,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )
        service._build_task_payload = lambda file_path, pdf_password=None: (  # type: ignore[method-assign]
            payload,
            Path("/tmp/sample.pdf"),
        )
        service.extractor.extract_from_task_payload = lambda task_payload, counterparty_guidance=None, extract_log_path=None: LLMExtractionOutcome(  # type: ignore[method-assign]
            result=ExtractionResult(
                orders=[
                    OrderExtraction(
                        fund_code="F001",
                        fund_name="Alpha",
                        settle_class=SettleClass.CONFIRMED,
                        order_type=OrderType.SUB,
                        base_date="2025-11-27",
                        t_day=0,
                        transfer_amount="100",
                    ),
                    OrderExtraction(
                        fund_code="F002",
                        fund_name="Beta",
                        settle_class=SettleClass.PENDING,
                        order_type=OrderType.RED,
                        base_date="2025-11-27",
                        t_day=1,
                        transfer_amount="-40",
                    ),
                ],
                issues=[],
            )
        )

        result = service.extract_file_path(Path("/tmp/sample.pdf"), only_pending=True)

        self.assertEqual(len(result.orders), 1)
        self.assertEqual(result.orders[0].fund_code, "F001")
        self.assertEqual(result.orders[0].settle_class, SettleClass.PENDING)

    def test_extract_file_path_to_payload_serializes_output_codes(self) -> None:
        service = ExtractionService()

        with TemporaryDirectory() as tmp_dir:
            sample_path = Path(tmp_dir) / "sample.pdf"
            sample_path.write_text("dummy", encoding="utf-8")
            payload = DocumentLoadTaskPayload(
                source_path=str(sample_path),
                file_name="sample.pdf",
                pdf_password=None,
                content_type="application/pdf",
                raw_text="raw",
                markdown_text="markdown",
                chunks=("chunk-1",),
                non_instruction_reason=None,
                allow_empty_result=False,
                scope_excludes_all_funds=False,
                expected_order_count=1,
                target_fund_scope=TargetFundScope(manager_column_present=False),
            )
            service._build_task_payload = lambda file_path, pdf_password=None: (  # type: ignore[method-assign]
                payload,
                sample_path,
            )
            service.extractor.extract_from_task_payload = lambda task_payload, counterparty_guidance=None, extract_log_path=None: LLMExtractionOutcome(  # type: ignore[method-assign]
                result=ExtractionResult(
                    orders=[
                        OrderExtraction(
                            fund_code="F001",
                            fund_name="Alpha",
                            settle_class=SettleClass.CONFIRMED,
                            order_type=OrderType.RED,
                            base_date="2025-11-27",
                            t_day=0,
                            transfer_amount="-100",
                        )
                    ],
                    issues=[],
                )
            )

            result_payload = service.extract_file_path_to_payload(sample_path)

        self.assertEqual(result_payload["status"], "COMPLETED")
        self.assertIsNone(result_payload["reason"])
        self.assertEqual(result_payload["orders"][0]["settle_class"], "2")
        self.assertEqual(result_payload["orders"][0]["order_type"], "1")
        self.assertEqual(result_payload["orders"][0]["transfer_amount"], "100")

    def test_extract_file_path_to_payload_returns_skipped_payload_for_non_instruction(self) -> None:
        service = ExtractionService()

        with TemporaryDirectory() as tmp_dir:
            sample_path = Path(tmp_dir) / "sample.pdf"
            sample_path.write_text("dummy", encoding="utf-8")
            service._extract_file_path_internal = lambda *args, **kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
                NonInstructionDocumentError("Document is not a variable-annuity order instruction. path=/tmp/sample.pdf reason=cover email")
            )

            result_payload = service.extract_file_path_to_payload(sample_path)

        self.assertEqual(result_payload["status"], "SKIPPED")
        self.assertIn("not a variable-annuity order instruction", result_payload["reason"])
        self.assertEqual(result_payload["orders"], [])
        self.assertEqual(result_payload["issues"], [])

    def test_extract_file_path_to_payload_returns_failed_payload_for_extraction_failure(self) -> None:
        service = ExtractionService()

        with TemporaryDirectory() as tmp_dir:
            sample_path = Path(tmp_dir) / "sample.pdf"
            sample_path.write_text("dummy", encoding="utf-8")
            outcome = LLMExtractionOutcome(
                result=ExtractionResult(issues=["TRANSFER_AMOUNT_MISSING"]),
                invalid_response_artifacts=[],
            )
            service._extract_file_path_internal = lambda *args, **kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
                ExtractionOutcomeError("failed", outcome)
            )

            result_payload = service.extract_file_path_to_payload(sample_path)

        self.assertEqual(result_payload["status"], "FAILED")
        self.assertEqual(result_payload["reason"], "failed")
        self.assertEqual(result_payload["issues"], ["TRANSFER_AMOUNT_MISSING"])
        self.assertEqual(result_payload["orders"], [])

    def test_extract_file_path_to_payload_preserves_allow_empty_result_skip_reason(self) -> None:
        service = ExtractionService()

        with TemporaryDirectory() as tmp_dir:
            sample_path = Path(tmp_dir) / "sample.pdf"
            sample_path.write_text("dummy", encoding="utf-8")
            task_payload = DocumentLoadTaskPayload(
                source_path=str(sample_path),
                file_name="sample.pdf",
                pdf_password=None,
                content_type="application/pdf",
                raw_text="raw",
                markdown_text="markdown",
                chunks=("chunk-1",),
                non_instruction_reason=None,
                allow_empty_result=True,
                scope_excludes_all_funds=False,
                expected_order_count=0,
                target_fund_scope=TargetFundScope(manager_column_present=False),
            )
            service._extract_file_path_internal = lambda *args, **kwargs: (  # type: ignore[method-assign]
                ExtractionResult(orders=[], issues=[]),
                task_payload,
                sample_path,
            )

            result_payload = service.extract_file_path_to_payload(sample_path)

        self.assertEqual(result_payload["status"], "SKIPPED")
        self.assertEqual(result_payload["reason"], "거래가 없는 문서")
        self.assertEqual(result_payload["orders"], [])

    def test_extract_file_path_to_payload_normalizes_metlife_final_orders(self) -> None:
        service = ExtractionService()

        with TemporaryDirectory() as tmp_dir:
            sample_path = Path(tmp_dir) / "메트라이프생명_0408.eml"
            sample_path.write_text("dummy", encoding="utf-8")
            task_payload = DocumentLoadTaskPayload(
                source_path=str(sample_path),
                file_name=sample_path.name,
                pdf_password=None,
                content_type="message/rfc822",
                raw_text="raw",
                markdown_text="메트라이프생명 펀드설정/해지 운용지시서",
                chunks=("chunk-1",),
                non_instruction_reason=None,
                allow_empty_result=False,
                scope_excludes_all_funds=False,
                expected_order_count=1,
                target_fund_scope=TargetFundScope(manager_column_present=False),
            )
            service._extract_file_path_internal = lambda *args, **kwargs: (  # type: ignore[method-assign]
                ExtractionResult(
                    orders=[
                        OrderExtraction(
                            fund_code="METLIFE_TMP_001",
                            fund_name="  MyFund VUL 혼합성장형  ",
                            settle_class=SettleClass.PENDING,
                            order_type=OrderType.SUB,
                            base_date="2026-04-08",
                            t_day=0,
                            transfer_amount="80,617,172",
                        )
                    ],
                    issues=[],
                ),
                task_payload,
                sample_path,
            )

            result_payload = service.extract_file_path_to_payload(sample_path)

        self.assertEqual(result_payload["status"], "COMPLETED")
        self.assertEqual(result_payload["orders"][0]["fund_code"], "-")
        self.assertEqual(result_payload["orders"][0]["fund_name"], "MyFund VUL 혼합성장형")

    def test_extract_file_path_to_payload_sorts_heungkuk_final_orders(self) -> None:
        service = ExtractionService()

        with TemporaryDirectory() as tmp_dir:
            sample_path = Path(tmp_dir) / "[흥국생명] 설정해지 내역 운용지시건-삼성-0413.eml"
            sample_path.write_text("dummy", encoding="utf-8")
            task_payload = DocumentLoadTaskPayload(
                source_path=str(sample_path),
                file_name=sample_path.name,
                pdf_password=None,
                content_type="message/rfc822",
                raw_text="raw",
                markdown_text=(
                    "흥국생명\n"
                    "설정해지 내역 운용지시건\n"
                    "펀드코드 | 추가설정금액 | 당일인출금액\n"
                ),
                chunks=("chunk-1",),
                non_instruction_reason=None,
                allow_empty_result=False,
                scope_excludes_all_funds=False,
                expected_order_count=9,
                target_fund_scope=TargetFundScope(manager_column_present=False),
            )
            service._extract_file_path_internal = lambda *args, **kwargs: (  # type: ignore[method-assign]
                ExtractionResult(
                    orders=[
                        OrderExtraction(
                            fund_code="450033",
                            fund_name="ignored",
                            settle_class=SettleClass.CONFIRMED,
                            order_type=OrderType.RED,
                            base_date="2026-04-13",
                            t_day=0,
                            transfer_amount="-30,000,000",
                        ),
                        OrderExtraction(
                            fund_code="450036",
                            fund_name="ignored",
                            settle_class=SettleClass.PENDING,
                            order_type=OrderType.RED,
                            base_date="2026-04-13",
                            t_day=2,
                            transfer_amount="-50,000,000",
                        ),
                        OrderExtraction(
                            fund_code="450038",
                            fund_name="ignored",
                            settle_class=SettleClass.CONFIRMED,
                            order_type=OrderType.SUB,
                            base_date="2026-04-13",
                            t_day=0,
                            transfer_amount="40,000,000",
                        ),
                    ],
                    issues=[],
                ),
                task_payload,
                sample_path,
            )

            result_payload = service.extract_file_path_to_payload(sample_path)

        self.assertEqual(
            [order["fund_code"] for order in result_payload["orders"]],
            ["450038", "450033", "450036"],
        )
        self.assertEqual(
            [order["fund_name"] for order in result_payload["orders"]],
            ["-", "-", "-"],
        )

    def test_extraction_outcome_error_exposes_outcome_shortcuts(self) -> None:
        outcome = LLMExtractionOutcome(
            result=ExtractionResult(issues=["LLM_INVALID_RESPONSE_FORMAT"]),
            invalid_response_artifacts=[
                InvalidResponseArtifact(
                    chunk_index=2,
                    raw_response="broken-response",
                    stage_name="transfer_amount",
                )
            ],
        )

        exc = ExtractionOutcomeError("failed", outcome)

        self.assertIs(exc.outcome, outcome)
        self.assertIs(exc.result, outcome.result)
        self.assertIs(exc.invalid_response_artifacts, outcome.invalid_response_artifacts)

    def test_write_invalid_response_debug_files_can_be_used_without_service(self) -> None:
        artifacts = [
            InvalidResponseArtifact(
                chunk_index=1,
                raw_response='{"bad": ',
                stage_name="fund_inventory",
            )
        ]

        with TemporaryDirectory() as tmp_dir:
            written_paths = write_invalid_response_debug_files(
                debug_output_dir=Path(tmp_dir),
                source_name="sample.pdf",
                artifacts=artifacts,
            )

            self.assertEqual(len(written_paths), 1)
            self.assertTrue(written_paths[0].exists())
            self.assertEqual(written_paths[0].read_text(encoding="utf-8"), '{"bad": ')


if __name__ == "__main__":
    unittest.main()
