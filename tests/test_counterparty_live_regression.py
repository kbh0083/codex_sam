from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from openpyxl import Workbook

from app.component import DocumentExtractionRequest, ExtractionComponent
from app.config import get_settings
from app.document_loader import DocumentLoader
from app.extractor import FundOrderExtractor, load_counterparty_guidance
from app.schemas import OrderType, SettleClass


RUN_LIVE_COUNTERPARTY_REGRESSION = os.getenv("RUN_LIVE_COUNTERPARTY_REGRESSION") == "1"
REPO_ROOT = Path(__file__).resolve().parents[1]
DOCUMENT_DIR = REPO_ROOT / "document"
CORRECT_RESULT_DIR = REPO_ROOT / "output" / "correct_result"
IBK_DOCUMENT_PATH = DOCUMENT_DIR / "IBK연금보험_0408_삼성자산운용설정해지지시서.eml"
HANAIS_XLSX_PATH = DOCUMENT_DIR / "흥국생명-hanais-0407-지시서.xlsx"
HANAIS_PDF_PATH = DOCUMENT_DIR / "흥국생명-hanais-0407-지시서.pdf"
CARDIF_PDF_PATH = CORRECT_RESULT_DIR / "11_카디프__카디프_251127.pdf"
CARDIF_BASELINE_PATH = CORRECT_RESULT_DIR / "11_카디프.json"
HANA_LIFE_XLSX_PATH = DOCUMENT_DIR / "하나생명-0415-지시서.xlsx"
HANA_LIFE_LEGACY_PDF_PATH = DOCUMENT_DIR / "하나생명(액티브)_251127.pdf"
HANA_LIFE_BASELINE_PATH = CORRECT_RESULT_DIR / "12_하나생명.json"


def _write_minimal_text_pdf(path: Path, lines: list[str]) -> None:
    """pdfplumber가 읽을 수 있는 최소 텍스트 PDF를 외부 의존성 없이 만든다."""

    def _escape_pdf_text(value: str) -> str:
        return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

    content_lines = ["BT", "/F1 12 Tf", "72 760 Td"]
    first_line = True
    for line in lines:
        if not first_line:
            content_lines.append("0 -16 Td")
        content_lines.append(f"({_escape_pdf_text(line)}) Tj")
        first_line = False
    content_lines.append("ET")
    content = ("\n".join(content_lines) + "\n").encode("latin-1")

    objects: list[bytes] = []

    def _add_object(body: bytes) -> None:
        objects.append(body)

    _add_object(b"<< /Type /Catalog /Pages 2 0 R >>")
    _add_object(b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
    _add_object(
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R "
        b"/Resources << /Font << /F1 5 0 R >> >> >>"
    )
    _add_object(b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content))
    _add_object(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

    pdf = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for index, body in enumerate(objects, start=1):
        offsets.append(len(pdf))
        pdf.extend(f"{index} 0 obj\n".encode("ascii"))
        pdf.extend(body)
        pdf.extend(b"\nendobj\n")

    xref_offset = len(pdf)
    pdf.extend(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    pdf.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        pdf.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    pdf.extend(
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_offset}\n%%EOF\n".encode(
            "ascii"
        )
    )
    path.write_bytes(pdf)


def _write_minimal_xlsx(path: Path, rows: list[list[str]]) -> None:
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "Instruction"
    for row_index, values in enumerate(rows, start=1):
        for column_index, value in enumerate(values, start=1):
            worksheet.cell(row=row_index, column=column_index, value=value)
    workbook.save(path)


@unittest.skipUnless(
    RUN_LIVE_COUNTERPARTY_REGRESSION,
    "set RUN_LIVE_COUNTERPARTY_REGRESSION=1 to run live counterparty regression tests",
)
class CounterpartyLiveRegressionTests(unittest.TestCase):
    def test_cardif_counterparty_prompt_extracts_expected_pdf_orders(self) -> None:
        if not CARDIF_PDF_PATH.exists():
            self.skipTest(f"missing actual Cardif PDF fixture: {CARDIF_PDF_PATH}")
        if not CARDIF_BASELINE_PATH.exists():
            self.skipTest(f"missing Cardif baseline payload: {CARDIF_BASELINE_PATH}")

        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        temp_root = Path(temp_dir.name)
        settings = replace(
            get_settings(),
            document_input_dir=REPO_ROOT,
            task_payload_output_dir=temp_root / "handoff",
            debug_output_dir=temp_root / "debug",
        )
        component = ExtractionComponent(settings=settings)

        payload = component.extract_document_payload(
            DocumentExtractionRequest(
                CARDIF_PDF_PATH,
                use_counterparty_prompt=True,
                only_pending=True,
            ),
        )
        baseline_payload = json.loads(CARDIF_BASELINE_PATH.read_text(encoding="utf-8"))

        self.assertEqual(payload["status"], "COMPLETED")
        self.assertEqual(payload["reason"], baseline_payload.get("reason"))
        self.assertEqual(payload["base_date"], baseline_payload["base_date"])
        self.assertEqual(payload["issues"], baseline_payload["issues"])
        self.assertEqual(payload["orders"], baseline_payload["orders"])

    def test_cardif_counterparty_prompt_skips_duplicate_xlsx_copy(self) -> None:
        settings = get_settings()
        loader = DocumentLoader()
        extractor = FundOrderExtractor(settings)
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        xlsx_path = Path(temp_dir.name) / "cardif_duplicate_copy.xlsx"
        _write_minimal_xlsx(
            xlsx_path,
            [
                ["BNP Paribas Cardif Life Insurance"],
                ["The order of Subscription and Redemption"],
                ["1. Subscription"],
                ["Fund Name", "Code", "Amount(KRW)", "Bank"],
                ["Future Mobility Active ETF FoF", "FME", "21,427,754", "HANA"],
                ["Global Bond FoF II", "GBE", "2,420,174", "HANA"],
                ["2. Redemption"],
                ["Fund Name", "Code", "Amount(KRW)", "Bank"],
                ["Bond ETF FoF", "BEE", "208,169,495", "SCFB"],
            ],
        )

        task_payload = loader.build_task_payload(xlsx_path, chunk_size_chars=settings.llm_chunk_size_chars)
        guidance = load_counterparty_guidance(
            xlsx_path,
            use_counterparty_prompt=True,
            document_text=task_payload.markdown_text,
        )

        with self.assertRaisesRegex(ValueError, r"duplicate XLSX copy; use PDF attachment"):
            extractor.extract_from_task_payload(task_payload, counterparty_guidance=guidance)

    def test_hanalife_counterparty_prompt_extracts_expected_internal_orders(self) -> None:
        if not HANA_LIFE_XLSX_PATH.exists():
            self.skipTest(f"missing actual Hana Life XLSX fixture: {HANA_LIFE_XLSX_PATH}")

        settings = get_settings()
        loader = DocumentLoader()
        extractor = FundOrderExtractor(settings)
        task_payload = loader.build_task_payload(HANA_LIFE_XLSX_PATH, chunk_size_chars=settings.llm_chunk_size_chars)
        guidance = load_counterparty_guidance(
            HANA_LIFE_XLSX_PATH,
            use_counterparty_prompt=True,
            document_text=task_payload.markdown_text,
        )

        outcome = extractor.extract_from_task_payload(task_payload, counterparty_guidance=guidance)

        self.assertEqual(outcome.result.issues, [])
        self.assertEqual(
            [
                (
                    order.fund_code,
                    order.fund_name,
                    order.settle_class,
                    order.order_type,
                    order.base_date,
                    order.t_day,
                    order.transfer_amount,
                )
                for order in outcome.result.orders
            ],
            [
                (
                    "BBA933",
                    "VA 이머징프릭스주식성장형",
                    SettleClass.CONFIRMED,
                    OrderType.RED,
                    "2026-04-15",
                    0,
                    "-5,351",
                ),
                (
                    "BBC143",
                    "글로벌혼합형(1형)",
                    SettleClass.CONFIRMED,
                    OrderType.SUB,
                    "2026-04-15",
                    0,
                    "2,906,740",
                ),
                (
                    "BBC152",
                    "아태브릭스주식성장형",
                    SettleClass.CONFIRMED,
                    OrderType.SUB,
                    "2026-04-15",
                    0,
                    "4,128,002",
                ),
                (
                    "BBC170",
                    "차이나주식성장형",
                    SettleClass.CONFIRMED,
                    OrderType.RED,
                    "2026-04-15",
                    0,
                    "-689",
                ),
                (
                    "BBC170",
                    "차이나주식성장형",
                    SettleClass.CONFIRMED,
                    OrderType.SUB,
                    "2026-04-15",
                    0,
                    "100,000",
                ),
                (
                    "BBC180",
                    "유럽주식성장형",
                    SettleClass.CONFIRMED,
                    OrderType.RED,
                    "2026-04-15",
                    0,
                    "-21,514,177",
                ),
                (
                    "BBC180",
                    "유럽주식성장형",
                    SettleClass.CONFIRMED,
                    OrderType.SUB,
                    "2026-04-15",
                    0,
                    "22,684,941",
                ),
            ],
        )

    def test_hanalife_counterparty_prompt_extracts_expected_xlsx_payload(self) -> None:
        if not HANA_LIFE_XLSX_PATH.exists():
            self.skipTest(f"missing actual Hana Life XLSX fixture: {HANA_LIFE_XLSX_PATH}")
        if not HANA_LIFE_BASELINE_PATH.exists():
            self.skipTest(f"missing Hana Life baseline payload: {HANA_LIFE_BASELINE_PATH}")

        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        temp_root = Path(temp_dir.name)
        settings = replace(
            get_settings(),
            document_input_dir=REPO_ROOT,
            task_payload_output_dir=temp_root / "handoff",
            debug_output_dir=temp_root / "debug",
        )
        component = ExtractionComponent(settings=settings)

        payload = component.extract_document_payload(
            DocumentExtractionRequest(
                HANA_LIFE_XLSX_PATH,
                use_counterparty_prompt=True,
                only_pending=True,
            ),
        )
        baseline_payload = json.loads(HANA_LIFE_BASELINE_PATH.read_text(encoding="utf-8"))

        self.assertEqual(payload["status"], "COMPLETED")
        self.assertEqual(payload["reason"], baseline_payload.get("reason"))
        self.assertEqual(payload["base_date"], baseline_payload["base_date"])
        self.assertEqual(payload["issues"], baseline_payload["issues"])
        self.assertEqual(payload["orders"], baseline_payload["orders"])

    def test_hanalife_counterparty_prompt_rejects_legacy_pdf_copy(self) -> None:
        if not HANA_LIFE_LEGACY_PDF_PATH.exists():
            self.skipTest(f"missing actual Hana Life legacy PDF fixture: {HANA_LIFE_LEGACY_PDF_PATH}")

        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        temp_root = Path(temp_dir.name)
        settings = replace(
            get_settings(),
            document_input_dir=REPO_ROOT,
            task_payload_output_dir=temp_root / "handoff",
            debug_output_dir=temp_root / "debug",
        )
        component = ExtractionComponent(settings=settings)

        payload = component.extract_document_payload(
            DocumentExtractionRequest(
                HANA_LIFE_LEGACY_PDF_PATH,
                use_counterparty_prompt=True,
                only_pending=True,
            ),
        )

        self.assertEqual(payload["status"], "SKIPPED")
        self.assertIn("duplicate PDF copy; use XLSX attachment", payload.get("reason", ""))

    def test_ibk_counterparty_prompt_extracts_expected_internal_orders(self) -> None:
        settings = get_settings()
        loader = DocumentLoader()
        extractor = FundOrderExtractor(settings)
        task_payload = loader.build_task_payload(IBK_DOCUMENT_PATH, chunk_size_chars=settings.llm_chunk_size_chars)
        guidance = load_counterparty_guidance(
            IBK_DOCUMENT_PATH,
            use_counterparty_prompt=True,
            document_text=task_payload.markdown_text,
        )

        outcome = extractor.extract_from_task_payload(task_payload, counterparty_guidance=guidance)

        self.assertEqual(outcome.result.issues, [])
        self.assertEqual(
            [
                (
                    order.fund_code,
                    order.fund_name,
                    order.settle_class,
                    order.order_type,
                    order.base_date,
                    order.t_day,
                    order.transfer_amount,
                )
                for order in outcome.result.orders
            ],
            [
                (
                    "BBCA00",
                    "AI 글로벌자산배분형",
                    SettleClass.CONFIRMED,
                    OrderType.RED,
                    "2026-04-08",
                    0,
                    "-54,788,126",
                ),
                (
                    "BBCA00",
                    "AI 글로벌자산배분형",
                    SettleClass.PENDING,
                    OrderType.RED,
                    "2026-04-08",
                    1,
                    "-95,003,244",
                ),
                (
                    "BBCA00",
                    "AI 글로벌자산배분형",
                    SettleClass.PENDING,
                    OrderType.SUB,
                    "2026-04-08",
                    2,
                    "2,794,977",
                ),
            ],
        )

    def test_ibk_counterparty_prompt_only_pending_contract_outputs_single_pending_row(self) -> None:
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        temp_root = Path(temp_dir.name)
        settings = replace(
            get_settings(),
            document_input_dir=REPO_ROOT,
            task_payload_output_dir=temp_root / "handoff",
            debug_output_dir=temp_root / "debug",
        )
        component = ExtractionComponent(settings=settings)

        payload = component.extract_document_payload(
            DocumentExtractionRequest(
                IBK_DOCUMENT_PATH,
                use_counterparty_prompt=True,
                only_pending=True,
            ),
        )

        self.assertEqual(payload["status"], "COMPLETED")
        self.assertEqual(payload["base_date"], "2026-04-08")
        self.assertEqual(payload["issues"], [])
        self.assertEqual(
            payload["orders"],
            [
                {
                    "fund_code": "BBCA00",
                    "fund_name": "AI 글로벌자산배분형",
                    "settle_class": "1",
                    "order_type": "1",
                    "base_date": "2026-04-08",
                    "t_day": "01",
                    "transfer_amount": "54,788,126",
                }
            ],
        )

    def test_heungkuk_hanais_counterparty_prompt_extracts_expected_xlsx_orders(self) -> None:
        settings = get_settings()
        loader = DocumentLoader()
        extractor = FundOrderExtractor(settings)
        task_payload = loader.build_task_payload(HANAIS_XLSX_PATH, chunk_size_chars=settings.llm_chunk_size_chars)
        guidance = load_counterparty_guidance(
            HANAIS_XLSX_PATH,
            use_counterparty_prompt=True,
            document_text=task_payload.markdown_text,
        )

        outcome = extractor.extract_from_task_payload(task_payload, counterparty_guidance=guidance)

        self.assertEqual(outcome.result.issues, [])
        self.assertEqual(
            [
                (
                    order.fund_code,
                    order.fund_name,
                    order.settle_class,
                    order.order_type,
                    order.base_date,
                    order.t_day,
                    order.transfer_amount,
                )
                for order in outcome.result.orders
            ],
            [
                (
                    "450046",
                    "변액유니버셜 선진국주식형(삼성)",
                    SettleClass.CONFIRMED,
                    OrderType.SUB,
                    "2026-04-07",
                    0,
                    "100,000,000",
                ),
                (
                    "450039",
                    "변액유니버셜 중국본토주식형(삼성)",
                    SettleClass.CONFIRMED,
                    OrderType.RED,
                    "2026-04-07",
                    0,
                    "-50,000,000",
                ),
            ],
        )

    def test_heungkuk_hanais_counterparty_prompt_only_pending_false_preserves_confirmed_output(self) -> None:
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        temp_root = Path(temp_dir.name)
        settings = replace(
            get_settings(),
            document_input_dir=REPO_ROOT,
            task_payload_output_dir=temp_root / "handoff",
            debug_output_dir=temp_root / "debug",
        )
        component = ExtractionComponent(settings=settings)

        payload = component.extract_document_payload(
            DocumentExtractionRequest(
                HANAIS_XLSX_PATH,
                use_counterparty_prompt=True,
                only_pending=False,
            ),
        )

        self.assertEqual(payload["status"], "COMPLETED")
        self.assertEqual(payload["base_date"], "2026-04-07")
        self.assertEqual(payload["issues"], [])
        self.assertEqual(
            payload["orders"],
            [
                {
                    "fund_code": "450046",
                    "fund_name": "변액유니버셜 선진국주식형(삼성)",
                    "settle_class": "2",
                    "order_type": "3",
                    "base_date": "2026-04-07",
                    "t_day": "01",
                    "transfer_amount": "100,000,000",
                },
                {
                    "fund_code": "450039",
                    "fund_name": "변액유니버셜 중국본토주식형(삼성)",
                    "settle_class": "2",
                    "order_type": "1",
                    "base_date": "2026-04-07",
                    "t_day": "01",
                    "transfer_amount": "50,000,000",
                },
            ],
        )

    def test_heungkuk_hanais_counterparty_prompt_skips_actual_duplicate_pdf_copy(self) -> None:
        settings = get_settings()
        loader = DocumentLoader()
        extractor = FundOrderExtractor(settings)
        if HANAIS_PDF_PATH.exists():
            pdf_path = HANAIS_PDF_PATH
        else:
            temp_dir = TemporaryDirectory()
            self.addCleanup(temp_dir.cleanup)
            pdf_path = Path(temp_dir.name) / HANAIS_PDF_PATH.name
            _write_minimal_text_pdf(
                pdf_path,
                [
                    "Heungkuk Life HANAIS duplicate PDF copy",
                    "Use XLSX attachment for extraction",
                    "Same instruction content as spreadsheet attachment",
                    "Samsung Asset Management",
                    "Fund code 450046 amount 100000000",
                    "Fund code 450039 amount 50000000",
                ],
            )

        task_payload = loader.build_task_payload(pdf_path, chunk_size_chars=settings.llm_chunk_size_chars)
        guidance = load_counterparty_guidance(
            pdf_path,
            use_counterparty_prompt=True,
            document_text=task_payload.markdown_text,
        )

        with self.assertRaisesRegex(ValueError, r"duplicate PDF copy; use XLSX attachment"):
            extractor.extract_from_task_payload(task_payload, counterparty_guidance=guidance)


if __name__ == "__main__":
    unittest.main()
