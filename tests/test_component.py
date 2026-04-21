from __future__ import annotations

import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory

from app.component import (
    DocumentExtractionRequest,
    ExtractionComponent,
    build_merged_payload,
)
from app.config import get_settings
from app.document_loader import DocumentLoadTaskPayload, TargetFundScope
from app.extractor import LLMExtractionOutcome
from app.extractor import ExtractionOutcomeError
from app.schemas import ExtractionResult, OrderExtraction, OrderType, SettleClass


class _FakeDocumentLoader:
    """component의 handler A 동작만 검증하기 위한 최소 fake loader.

    이 fake는 실제 문서 파싱 품질을 검증하지 않는다.
    여기서 확인하고 싶은 것은
    - 어떤 입력 경로가 loader로 전달됐는지
    - handler A가 task payload를 파일로 쓰는지
    두 가지뿐이다.
    """

    def __init__(self) -> None:
        """호출 경로 기록용 리스트를 초기화한다."""
        self.loaded_paths: list[Path] = []

    def build_task_payload(
        self,
        file_path: Path,
        *,
        chunk_size_chars: int,
        pdf_password: str | None = None,
    ) -> DocumentLoadTaskPayload:
        """입력 경로를 기록하고 고정 task payload를 반환한다."""
        # fake loader는 실제 문서 내용을 파싱하지 않고,
        # "어떤 경로가 handler A에 전달됐는지"와 "task payload 파일이 만들어지는지"만 검증한다.
        self.loaded_paths.append(file_path)
        markdown_text = "markdown"
        if file_path.name == "메트라이프생명_0408.eml":
            markdown_text = "메트라이프생명 펀드설정/해지 운용지시서"
        return DocumentLoadTaskPayload(
            source_path=str(file_path),
            file_name=file_path.name,
            pdf_password=pdf_password,
            content_type="application/pdf",
            raw_text="raw",
            markdown_text=markdown_text,
            chunks=("chunk-1",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=1,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )


class _FakeExtractor:
    """component의 handler B 동작만 검증하기 위한 최소 fake extractor.

    실제 LLM 호출 대신, handoff 파일에서 다시 읽힌 payload의 source_path만 확인하고
    고정된 ExtractionResult를 반환한다.
    즉 이 fake의 목적은 "추출 품질"이 아니라 "실행 경로" 검증이다.
    """

    def __init__(self) -> None:
        """handler B에서 다시 읽힌 source_path를 기록할 저장소를 만든다."""
        self.payload_paths: list[str] = []
        self.counterparty_guidances: list[str | None] = []
        self.extract_log_paths: list[Path | None] = []

    def extract_from_task_payload(
        self,
        task_payload: DocumentLoadTaskPayload,
        *,
        counterparty_guidance: str | None = None,
        extract_log_path: Path | None = None,
    ) -> LLMExtractionOutcome:
        """handoff JSON 재로딩 경로만 검증할 수 있게 고정 결과를 반환한다."""
        # fake extractor는 handler B가 handoff 파일을 다시 읽어
        # 올바른 source_path를 복원했는지만 확인하면 충분하다.
        self.payload_paths.append(task_payload.source_path)
        self.counterparty_guidances.append(counterparty_guidance)
        self.extract_log_paths.append(extract_log_path)
        payloads = {
            "a.pdf": OrderExtraction(
                fund_code="F001",
                fund_name="Alpha",
                settle_class=SettleClass.CONFIRMED,
                order_type=OrderType.SUB,
                base_date="2026-03-16",
                t_day=0,
                transfer_amount="100",
            ),
            "b.pdf": OrderExtraction(
                fund_code="F001",
                fund_name="Alpha",
                settle_class=SettleClass.CONFIRMED,
                order_type=OrderType.RED,
                base_date="2026-03-16",
                t_day=0,
                transfer_amount="-40",
            ),
            "카디프_251127.pdf": OrderExtraction(
                fund_code="AEE",
                fund_name="Asia(ex Japan) Index FoF",
                settle_class=SettleClass.CONFIRMED,
                order_type=OrderType.SUB,
                base_date="2025-11-27",
                t_day=0,
                transfer_amount="668,326",
            ),
            "unknown_counterparty.pdf": OrderExtraction(
                fund_code="F999",
                fund_name="Unknown",
                settle_class=SettleClass.CONFIRMED,
                order_type=OrderType.SUB,
                base_date="2025-11-27",
                t_day=0,
                transfer_amount="1",
            ),
            "메트라이프생명_0408.eml": OrderExtraction(
                fund_code="METLIFE_TMP_001",
                fund_name="  MyFund VUL 혼합성장형  ",
                settle_class=SettleClass.PENDING,
                order_type=OrderType.SUB,
                base_date="2026-04-08",
                t_day=0,
                transfer_amount="80,617,172",
            ),
        }
        return LLMExtractionOutcome(
            result=ExtractionResult(
                orders=[payloads[Path(task_payload.source_path).name]],
                issues=[],
            )
        )


class _FakeOnlyPendingExtractor:
    """only_pending 후처리 규칙을 검증하기 위한 fake extractor."""

    def extract_from_task_payload(
        self,
        task_payload: DocumentLoadTaskPayload,
        *,
        counterparty_guidance: str | None = None,
        extract_log_path: Path | None = None,
    ) -> LLMExtractionOutcome:
        """`CONFIRMED + PENDING` 조합 결과를 고정 반환한다."""
        return LLMExtractionOutcome(
            result=ExtractionResult(
                orders=[
                    OrderExtraction(
                        fund_code="F001",
                        fund_name="Alpha",
                        settle_class=SettleClass.CONFIRMED,
                        order_type=OrderType.SUB,
                        base_date="2026-03-16",
                        t_day=0,
                        transfer_amount="100",
                    ),
                    OrderExtraction(
                        fund_code="F002",
                        fund_name="Beta",
                        settle_class=SettleClass.PENDING,
                        order_type=OrderType.RED,
                        base_date="2026-03-16",
                        t_day=1,
                        transfer_amount="-40",
                    ),
                ],
                issues=[],
            )
        )


class _FakeOnlyPendingEmptyAfterFilterExtractor:
    """only_pending 적용 후 최종 row가 0건이 되는 경로를 검증하기 위한 fake extractor."""

    def extract_from_task_payload(
        self,
        task_payload: DocumentLoadTaskPayload,
        *,
        counterparty_guidance: str | None = None,
        extract_log_path: Path | None = None,
    ) -> LLMExtractionOutcome:
        """내부 추출은 성공했지만 only_pending 후처리 후 저장 row가 0건이 되도록 반환한다."""
        return LLMExtractionOutcome(
            result=ExtractionResult(
                orders=[
                    OrderExtraction(
                        fund_code="F009",
                        fund_name="Gamma",
                        settle_class=SettleClass.PENDING,
                        order_type=OrderType.RED,
                        base_date="2026-03-16",
                        t_day=1,
                        transfer_amount="-10",
                    )
                ],
                issues=["ORDER_COVERAGE_ESTIMATE_MISMATCH"],
            )
        )


class _FakeExtractionOutcomeErrorExtractor:
    """실제 추출 실패를 FAILED payload로 정규화하는지 검증하는 fake extractor."""

    def extract_from_task_payload(
        self,
        task_payload: DocumentLoadTaskPayload,
        *,
        counterparty_guidance: str | None = None,
        extract_log_path: Path | None = None,
    ) -> LLMExtractionOutcome:
        raise ExtractionOutcomeError(
            "stage failure",
            LLMExtractionOutcome(result=ExtractionResult(issues=["TRANSFER_AMOUNT_MISSING"])),
        )


class _FakeGenericFailureExtractor:
    """예상 밖 일반 예외도 FAILED payload로 정규화하는지 검증하는 fake extractor."""

    def extract_from_task_payload(
        self,
        task_payload: DocumentLoadTaskPayload,
        *,
        counterparty_guidance: str | None = None,
        extract_log_path: Path | None = None,
    ) -> LLMExtractionOutcome:
        raise ValueError("unexpected local failure")


class ExtractionComponentTests(unittest.TestCase):
    """`ExtractionComponent`의 handler A/B 경로와 최종 payload 계약을 검증한다."""

    def _make_component(self, document_dir: Path) -> tuple[ExtractionComponent, _FakeDocumentLoader, _FakeExtractor]:
        """테스트용 component를 만든다.

        실제 settings를 그대로 쓰면 로컬 환경의 document 경로에 의존할 수 있으므로,
        문서 루트만 임시 디렉터리로 치환한 settings 복사본을 주입한다.
        """
        fake_loader = _FakeDocumentLoader()
        fake_extractor = _FakeExtractor()
        settings = replace(
            get_settings(),
            document_input_dir=document_dir,
        )
        component = ExtractionComponent(
            settings=settings,
            document_loader=fake_loader,
            extractor=fake_extractor,
        )
        return component, fake_loader, fake_extractor

    def test_extract_document_payloads_returns_document_payloads_in_input_order(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            temp_root = Path(tmp_dir)
            # 실제 문서는 필요 없고 "존재하는 파일 경로"만 있으면 되므로 더미 파일만 만든다.
            (temp_root / "a.pdf").write_text("dummy", encoding="utf-8")
            (temp_root / "b.pdf").write_text("dummy", encoding="utf-8")
            component, fake_loader, fake_extractor = self._make_component(temp_root)
            handoff_dir = temp_root / "handoff"
            payloads = component.extract_document_payloads(
                [
                    DocumentExtractionRequest("a.pdf"),
                    DocumentExtractionRequest("b.pdf"),
                ],
                handoff_dir=handoff_dir,
            )
            # handoff 파일 이름까지 확인하는 이유는,
            # component가 정말 handler A 파일 저장 경로를 거쳤는지 보려는 것이다.
            handoff_files = sorted(path.name for path in handoff_dir.glob("*_task_payload.json"))

        self.assertEqual([payload["file_name"] for payload in payloads], ["a.pdf", "b.pdf"])
        self.assertEqual([path.name for path in fake_loader.loaded_paths], ["a.pdf", "b.pdf"])
        self.assertEqual([Path(path).name for path in fake_extractor.payload_paths], ["a.pdf", "b.pdf"])
        self.assertEqual(handoff_files, ["01_a_task_payload.json", "02_b_task_payload.json"])

    def test_run_handler_b_passes_document_scoped_extract_log_path(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            temp_root = Path(tmp_dir)
            source_path = temp_root / "a.pdf"
            source_path.write_text("dummy", encoding="utf-8")
            component, _, fake_extractor = self._make_component(temp_root)
            handoff_path = temp_root / "01_a_task_payload.json"
            component.run_handler_a(DocumentExtractionRequest(source_path), handoff_path)

            component.run_handler_b(handoff_path)

        self.assertEqual(len(fake_extractor.extract_log_paths), 1)
        logged_path = fake_extractor.extract_log_paths[0]
        self.assertIsNotNone(logged_path)
        assert logged_path is not None
        self.assertEqual(logged_path.parent, component.settings.debug_output_dir)
        self.assertTrue(logged_path.name.startswith("a_"))
        self.assertTrue(logged_path.name.endswith("_llm_pipeline.log"))

    def test_extract_document_payload_keeps_internal_handoff_files_when_deletion_disabled(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            temp_root = Path(tmp_dir)
            (temp_root / "a.pdf").write_text("dummy", encoding="utf-8")
            fake_loader = _FakeDocumentLoader()
            fake_extractor = _FakeExtractor()
            settings = replace(
                get_settings(),
                document_input_dir=temp_root,
                task_payload_output_dir=temp_root / "handoff_store",
                delete_task_payload_files=False,
            )
            component = ExtractionComponent(
                settings=settings,
                document_loader=fake_loader,
                extractor=fake_extractor,
            )

            payload = component.extract_document_payload(DocumentExtractionRequest("a.pdf"))
            handoff_files = sorted((temp_root / "handoff_store").glob("**/*_task_payload.json"))

        self.assertEqual(payload["file_name"], "a.pdf")
        self.assertEqual(payload["status"], "COMPLETED")
        self.assertIsNone(payload["reason"])
        self.assertEqual(len(handoff_files), 1)
        self.assertEqual(handoff_files[0].name, "01_a_task_payload.json")
        self.assertEqual(payload["orders"][0]["settle_class"], "2")
        self.assertEqual(payload["orders"][0]["order_type"], "3")

    def test_extract_document_payload_passes_counterparty_guidance_when_enabled(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            temp_root = Path(tmp_dir)
            source_file = temp_root / "카디프_251127.pdf"
            source_file.write_text("dummy", encoding="utf-8")
            fake_loader = _FakeDocumentLoader()
            fake_extractor = _FakeExtractor()
            settings = replace(
                get_settings(),
                document_input_dir=temp_root,
            )
            component = ExtractionComponent(
                settings=settings,
                document_loader=fake_loader,
                extractor=fake_extractor,
            )

            payload = component.extract_document_payload(
                DocumentExtractionRequest("카디프_251127.pdf", use_counterparty_prompt=True),
                handoff_dir=temp_root / "handoff",
            )

        self.assertEqual(payload["file_name"], "카디프_251127.pdf")
        self.assertEqual(len(fake_extractor.counterparty_guidances), 1)
        self.assertIn("Subscription", fake_extractor.counterparty_guidances[0] or "")

    def test_extract_document_payload_falls_back_when_counterparty_guidance_is_unknown(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            temp_root = Path(tmp_dir)
            source_file = temp_root / "unknown_counterparty.pdf"
            source_file.write_text("dummy", encoding="utf-8")
            fake_loader = _FakeDocumentLoader()
            fake_extractor = _FakeExtractor()
            settings = replace(
                get_settings(),
                document_input_dir=temp_root,
            )
            component = ExtractionComponent(
                settings=settings,
                document_loader=fake_loader,
                extractor=fake_extractor,
            )

            payload = component.extract_document_payload(
                DocumentExtractionRequest("unknown_counterparty.pdf", use_counterparty_prompt=True),
                handoff_dir=temp_root / "handoff",
            )

        self.assertEqual(payload["file_name"], "unknown_counterparty.pdf")
        self.assertEqual(fake_extractor.counterparty_guidances, [None])

    def test_extract_document_payload_normalizes_metlife_final_orders(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            temp_root = Path(tmp_dir)
            source_file = temp_root / "메트라이프생명_0408.eml"
            source_file.write_text("dummy", encoding="utf-8")
            fake_loader = _FakeDocumentLoader()
            fake_extractor = _FakeExtractor()
            settings = replace(
                get_settings(),
                document_input_dir=temp_root,
            )
            component = ExtractionComponent(
                settings=settings,
                document_loader=fake_loader,
                extractor=fake_extractor,
            )

            payload = component.extract_document_payload(
                DocumentExtractionRequest("메트라이프생명_0408.eml", use_counterparty_prompt=True),
                handoff_dir=temp_root / "handoff",
            )

        self.assertEqual(payload["status"], "COMPLETED")
        self.assertEqual(payload["orders"][0]["fund_code"], "-")
        self.assertEqual(payload["orders"][0]["fund_name"], "MyFund VUL 혼합성장형")

    def test_extract_document_payload_applies_only_pending_filter(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            temp_root = Path(tmp_dir)
            (temp_root / "a.pdf").write_text("dummy", encoding="utf-8")
            fake_loader = _FakeDocumentLoader()
            settings = replace(
                get_settings(),
                document_input_dir=temp_root,
            )
            component = ExtractionComponent(
                settings=settings,
                document_loader=fake_loader,
                extractor=_FakeOnlyPendingExtractor(),
            )

            payload = component.extract_document_payload(
                DocumentExtractionRequest("a.pdf", only_pending=True),
                handoff_dir=temp_root / "handoff",
            )

        self.assertEqual(len(payload["orders"]), 1)
        self.assertEqual(payload["orders"][0]["fund_code"], "F001")
        self.assertEqual(payload["orders"][0]["settle_class"], "1")
        self.assertEqual(payload["orders"][0]["order_type"], "3")

    def test_extract_document_payload_marks_warning_only_zero_rows_as_skipped(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            temp_root = Path(tmp_dir)
            (temp_root / "a.pdf").write_text("dummy", encoding="utf-8")
            fake_loader = _FakeDocumentLoader()
            settings = replace(
                get_settings(),
                document_input_dir=temp_root,
            )
            component = ExtractionComponent(
                settings=settings,
                document_loader=fake_loader,
                extractor=_FakeOnlyPendingEmptyAfterFilterExtractor(),
            )

            payload = component.extract_document_payload(
                DocumentExtractionRequest("a.pdf", only_pending=True),
                handoff_dir=temp_root / "handoff",
            )

        self.assertEqual(payload["status"], "SKIPPED")
        self.assertEqual(payload["orders"], [])
        self.assertEqual(payload["reason"], "추출된 주문 데이터 없음")
        self.assertEqual(payload["issues"], ["ORDER_COVERAGE_ESTIMATE_MISMATCH"])

    def test_extract_document_payload_returns_failed_payload_for_extraction_outcome_error(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            temp_root = Path(tmp_dir)
            (temp_root / "a.pdf").write_text("dummy", encoding="utf-8")
            component = ExtractionComponent(
                settings=replace(get_settings(), document_input_dir=temp_root),
                document_loader=_FakeDocumentLoader(),
                extractor=_FakeExtractionOutcomeErrorExtractor(),
            )

            payload = component.extract_document_payload(
                DocumentExtractionRequest("a.pdf"),
                handoff_dir=temp_root / "handoff",
            )

        self.assertEqual(payload["status"], "FAILED")
        self.assertEqual(payload["orders"], [])
        self.assertEqual(payload["issues"], ["TRANSFER_AMOUNT_MISSING"])
        self.assertEqual(payload["reason"], "stage failure")

    def test_extract_document_payload_returns_failed_payload_for_generic_failure(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            temp_root = Path(tmp_dir)
            (temp_root / "a.pdf").write_text("dummy", encoding="utf-8")
            component = ExtractionComponent(
                settings=replace(get_settings(), document_input_dir=temp_root),
                document_loader=_FakeDocumentLoader(),
                extractor=_FakeGenericFailureExtractor(),
            )

            payload = component.extract_document_payload(
                DocumentExtractionRequest("a.pdf"),
                handoff_dir=temp_root / "handoff",
            )

        self.assertEqual(payload["status"], "FAILED")
        self.assertEqual(payload["orders"], [])
        self.assertEqual(payload["issues"], ["unexpected local failure"])
        self.assertEqual(payload["reason"], "unexpected local failure")

    def test_extract_document_payload_dedupes_serialized_orders_after_amount_canonicalization(self) -> None:
        class _ArtifactDuplicateExtractor:
            def extract_from_task_payload(
                self,
                task_payload: DocumentLoadTaskPayload,
                *,
                counterparty_guidance: str | None = None,
                extract_log_path: Path | None = None,
            ) -> LLMExtractionOutcome:
                return LLMExtractionOutcome(
                    result=ExtractionResult(
                        orders=[
                            OrderExtraction(
                                fund_code="450042",
                                fund_name="변액유니버셜 주식",
                                settle_class=SettleClass.CONFIRMED,
                                order_type=OrderType.SUB,
                                base_date="2026-04-15",
                                t_day=0,
                                transfer_amount="70000000.00000001",
                            ),
                            OrderExtraction(
                                fund_code="450042",
                                fund_name="변액유니버셜 주식",
                                settle_class=SettleClass.CONFIRMED,
                                order_type=OrderType.SUB,
                                base_date="2026-04-15",
                                t_day=0,
                                transfer_amount="70,000,000",
                            ),
                        ],
                        issues=[],
                    )
                )

        with TemporaryDirectory() as tmp_dir:
            temp_root = Path(tmp_dir)
            (temp_root / "artifact.xlsx").write_text("dummy", encoding="utf-8")
            component = ExtractionComponent(
                settings=replace(get_settings(), document_input_dir=temp_root),
                document_loader=_FakeDocumentLoader(),
                extractor=_ArtifactDuplicateExtractor(),
            )

            payload = component.extract_document_payload(
                DocumentExtractionRequest("artifact.xlsx"),
                handoff_dir=temp_root / "handoff",
            )

        self.assertEqual(len(payload["orders"]), 1)
        self.assertEqual(payload["orders"][0]["transfer_amount"], "70,000,000")

    def test_extract_merged_payload_keeps_opposite_order_types_separate(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            temp_root = Path(tmp_dir)
            # a.pdf와 b.pdf는 같은 펀드/같은 버킷에 대해 SUB/RED를 각각 반환하도록 fake extractor가 설계돼 있다.
            # 현재 규칙은 이를 순액으로 합치지 않고 두 주문을 그대로 유지하는 것이다.
            (temp_root / "a.pdf").write_text("dummy", encoding="utf-8")
            (temp_root / "b.pdf").write_text("dummy", encoding="utf-8")
            component, _, _ = self._make_component(temp_root)
            merged_payload = component.extract_merged_payload(
                [
                    DocumentExtractionRequest("a.pdf"),
                    DocumentExtractionRequest("b.pdf"),
                ],
                handoff_dir=temp_root / "handoff",
            )

        self.assertEqual(merged_payload["file_count"], 2)
        self.assertEqual(merged_payload["status"], "COMPLETED")
        self.assertIsNone(merged_payload["reason"])
        self.assertEqual(len(merged_payload["documents"]), 2)
        self.assertEqual(len(merged_payload["orders"]), 2)
        self.assertEqual({order["order_type"] for order in merged_payload["orders"]}, {"3", "1"})
        self.assertEqual({order["transfer_amount"] for order in merged_payload["orders"]}, {"100", "40"})

    def test_build_merged_payload_dedupes_orders_with_only_artifact_amount_difference(self) -> None:
        merged_payload = build_merged_payload(
            [
                {
                    "file_name": "a.xlsx",
                    "source_path": "/tmp/a.xlsx",
                    "model_name": "test-model",
                    "base_date": "2026-04-15",
                    "status": "COMPLETED",
                    "reason": None,
                    "issues": [],
                    "orders": [
                        {
                            "fund_code": "450042",
                            "fund_name": "변액유니버셜 주식",
                            "settle_class": "2",
                            "order_type": "3",
                            "base_date": "2026-04-15",
                            "t_day": "01",
                            "transfer_amount": "70,000,000.00000001",
                        }
                    ],
                },
                {
                    "file_name": "b.xlsx",
                    "source_path": "/tmp/b.xlsx",
                    "model_name": "test-model",
                    "base_date": "2026-04-15",
                    "status": "COMPLETED",
                    "reason": None,
                    "issues": [],
                    "orders": [
                        {
                            "fund_code": "450042",
                            "fund_name": "변액유니버셜 주식",
                            "settle_class": "2",
                            "order_type": "3",
                            "base_date": "2026-04-15",
                            "t_day": "01",
                            "transfer_amount": "70,000,000",
                        }
                    ],
                },
            ]
        )

        self.assertEqual(len(merged_payload["orders"]), 1)
        self.assertEqual(merged_payload["orders"][0]["transfer_amount"], "70,000,000")

    def test_extract_document_payload_returns_skipped_payload_for_non_instruction_document(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            temp_root = Path(tmp_dir)
            source_file = temp_root / "cover_email.eml"
            source_file.write_text("dummy", encoding="utf-8")
            fake_extractor = _FakeExtractor()
            settings = replace(
                get_settings(),
                document_input_dir=temp_root,
            )

            class _NonInstructionLoader(_FakeDocumentLoader):
                def build_task_payload(self, file_path: Path, *, chunk_size_chars: int, pdf_password: str | None = None) -> DocumentLoadTaskPayload:  # type: ignore[override]
                    payload = super().build_task_payload(
                        file_path,
                        chunk_size_chars=chunk_size_chars,
                        pdf_password=pdf_password,
                    )
                    return DocumentLoadTaskPayload(
                        source_path=payload.source_path,
                        file_name=payload.file_name,
                        pdf_password=payload.pdf_password,
                        content_type=payload.content_type,
                        raw_text=payload.raw_text,
                        markdown_text=payload.markdown_text,
                        chunks=payload.chunks,
                        non_instruction_reason="cover email only",
                        allow_empty_result=False,
                        scope_excludes_all_funds=False,
                        expected_order_count=0,
                        target_fund_scope=payload.target_fund_scope,
                    )

            component = ExtractionComponent(
                settings=settings,
                document_loader=_NonInstructionLoader(),
                extractor=fake_extractor,
            )

            payload = component.extract_document_payload(
                DocumentExtractionRequest("cover_email.eml"),
                handoff_dir=temp_root / "handoff",
            )

        self.assertEqual(payload["status"], "SKIPPED")
        self.assertEqual(payload["orders"], [])
        self.assertIn("not a variable-annuity order instruction", payload["reason"])

    def test_component_payload_to_csv_rows_supports_merged_payload(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            temp_root = Path(tmp_dir)
            # CSV 경로는 검수/엑셀 연동에서 많이 보므로,
            # merged payload도 별도 분기 없이 동일 helper로 평탄화되는지 확인한다.
            (temp_root / "a.pdf").write_text("dummy", encoding="utf-8")
            (temp_root / "b.pdf").write_text("dummy", encoding="utf-8")
            component, _, _ = self._make_component(temp_root)
            merged_payload = component.extract_merged_payload(
                [
                    DocumentExtractionRequest("a.pdf"),
                    DocumentExtractionRequest("b.pdf"),
                ],
                handoff_dir=temp_root / "handoff",
            )

        rows = component.payload_to_csv_rows(merged_payload)

        self.assertEqual(len(rows), 2)
        self.assertEqual([row["file_name"] for row in rows], ["a.pdf", "b.pdf"])
        self.assertEqual({row["transfer_amount"] for row in rows}, {"100", "40"})


if __name__ == "__main__":
    unittest.main()
