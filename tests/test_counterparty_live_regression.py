from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory, mkdtemp
import unittest

from openpyxl import Workbook

from app.config import get_settings
from app.document_loader import DocumentLoader
from app.extraction import load_counterparty_guidance
from app.extractor import FundOrderExtractor
from app.schemas import OrderType, SettleClass


RUN_LIVE_COUNTERPARTY_REGRESSION = os.getenv("RUN_LIVE_COUNTERPARTY_REGRESSION") == "1"
LIVE_REGRESSION_TIMEOUT_SECONDS = int(os.getenv("LIVE_REGRESSION_TIMEOUT_SECONDS", "480"))
REPO_ROOT = Path(__file__).resolve().parents[1]
DOCUMENT_DIR = REPO_ROOT / "document"
ANSWER_SET_DIR = REPO_ROOT / "거래처별_문서_정답"
KDB_XLSX_PATH = DOCUMENT_DIR / "운용지시서(KDB생명)_20251128.xlsx"
KDB_PDF_PATH = DOCUMENT_DIR / "운용지시서(KDB생명)_20251128.pdf"
KDB_EML_PATH = DOCUMENT_DIR / "운용지시서_KDB_20251128.eml"
KDB_ANSWER_DIR = ANSWER_SET_DIR / "KDB생명"
KDB_XLSX_BASELINE_PATH = KDB_ANSWER_DIR / "운용지시서(KDB생명)_20251128.json"
KDB_PDF_BASELINE_PATH = KDB_ANSWER_DIR / "운용지시서(KDB생명)_20251128.pdf.json"
KDB_EML_BASELINE_PATH = KDB_ANSWER_DIR / "운용지시서_KDB_20251128.json"
IBK_DOCUMENT_PATH = DOCUMENT_DIR / "IBK연금보험_0408_삼성자산운용설정해지지시서.eml"
HANAIS_XLSX_PATH = DOCUMENT_DIR / "흥국생명-hanais-0407-지시서.xlsx"
HANAIS_PDF_PATH = DOCUMENT_DIR / "흥국생명-hanais-0407-지시서.pdf"
CARDIF_PDF_PATH = DOCUMENT_DIR / "카디프_251127.pdf"
CARDIF_ANSWER_DIR = ANSWER_SET_DIR / "카디프생명"
CARDIF_BASELINE_PATH = CARDIF_ANSWER_DIR / "카디프_251127.json"
DONGYANG_20260318_PATH = DOCUMENT_DIR / "동양생명_20260318.html"
DONGYANG_ANSWER_DIR = ANSWER_SET_DIR / "동양생명"
DONGYANG_20260318_BASELINE_PATH = DONGYANG_ANSWER_DIR / "동양생명_20260318.json"
DONGYANG_20260413_PATH = DOCUMENT_DIR / "동양생명_20260413.html"
DONGYANG_20260413_BASELINE_PATH = DONGYANG_ANSWER_DIR / "동양생명_20260413.json"
HANHWA_20250826_PATH = DOCUMENT_DIR / "hanhwa_20250826.html"
HANHWA_ANSWER_DIR = ANSWER_SET_DIR / "한화생명"
HANHWA_20250826_BASELINE_PATH = HANHWA_ANSWER_DIR / "hanhwa_20250826.json"
SHINHAN_20260116_PATH = DOCUMENT_DIR / "신한라이프_251127_20260116_212233.pdf"
SHINHAN_ANSWER_DIR = ANSWER_SET_DIR / "신한라이프"
SHINHAN_20260116_BASELINE_PATH = SHINHAN_ANSWER_DIR / "신한라이프_251127_20260116_212233.json"
KYOBO_MHT_PATH = DOCUMENT_DIR / "자금운용_해지_5440_20260316.mht"
KYOBO_ANSWER_DIR = ANSWER_SET_DIR / "교보생명"
KYOBO_MHT_BASELINE_PATH = KYOBO_ANSWER_DIR / "자금운용_해지_5440_20260316.json"
HANA_LIFE_XLSX_PATH = DOCUMENT_DIR / "하나생명-0415-지시서.xlsx"
HANA_LIFE_LEGACY_PDF_PATH = DOCUMENT_DIR / "하나생명(액티브)_251127.pdf"
HANA_LIFE_ANSWER_DIR = ANSWER_SET_DIR / "하나생명"
HANA_LIFE_BASELINE_PATH = HANA_LIFE_ANSWER_DIR / "하나생명-0415-지시서.json"
METLIFE_BASE_PATH = DOCUMENT_DIR / "메트라이프생명_0408.eml"
METLIFE_ANSWER_DIR = ANSWER_SET_DIR / "메트라이프생명"
METLIFE_BASELINE_PATH = METLIFE_ANSWER_DIR / "메트라이프생명_0408.json"
METLIFE_ADDITIONAL_SUB_PATH = DOCUMENT_DIR / "메트라이프생명_0408_추가_추가설정.eml"
METLIFE_ADDITIONAL_RED_PATH = DOCUMENT_DIR / "메트라이프생명_추가설정해지_0408.eml"


class LiveRegressionTimeoutError(TimeoutError):
    """live regression이 backend stall로 과도하게 오래 걸릴 때 쓰는 명시적 오류다."""


@contextmanager
def _live_timeout_guard(seconds: int, *, label: str):
    """live regression 1건이 무한 대기하지 않도록 wall-clock 상한을 건다."""
    if seconds <= 0 or not hasattr(signal, "SIGALRM") or not hasattr(signal, "setitimer"):
        yield
        return

    def _raise_timeout(signum, frame):
        raise LiveRegressionTimeoutError(f"live regression timed out after {seconds}s: {label}")

    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_timer = signal.getitimer(signal.ITIMER_REAL)
    signal.signal(signal.SIGALRM, _raise_timeout)
    signal.setitimer(signal.ITIMER_REAL, float(seconds))
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_timer != (0.0, 0.0):
            signal.setitimer(signal.ITIMER_REAL, *previous_timer)


class LiveRegressionTimeoutGuardTests(unittest.TestCase):
    @unittest.skipUnless(
        hasattr(signal, "SIGALRM") and hasattr(signal, "setitimer"),
        "requires SIGALRM-based timer support",
    )
    def test_live_timeout_guard_raises_after_deadline(self) -> None:
        with self.assertRaises(LiveRegressionTimeoutError):
            with _live_timeout_guard(1, label="timeout-guard-smoke"):
                time.sleep(2)


class _LiveRegressionTimeoutMixin:
    """각 live regression test method에 timeout guard를 공통 적용한다."""

    def _callTestMethod(self, method):
        label = f"{self.__class__.__name__}.{self._testMethodName}"
        try:
            with _live_timeout_guard(LIVE_REGRESSION_TIMEOUT_SECONDS, label=label):
                return super()._callTestMethod(method)
        except LiveRegressionTimeoutError as exc:
            self.fail(str(exc))


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
class CounterpartyLiveRegressionTests(_LiveRegressionTimeoutMixin, unittest.TestCase):
    def _extract_document_payload_via_subprocess(
        self,
        source_path: Path,
        *,
        only_pending: bool,
        include_debug_meta: bool = False,
    ) -> dict[str, object]:
        temp_root = Path(mkdtemp(prefix="live_counterparty_regression_"))
        child_script = """
import json
import sys
from dataclasses import replace
from pathlib import Path

from app.component import DocumentExtractionRequest, ExtractionComponent
from app.config import get_settings

source_path = Path(sys.argv[1])
only_pending = sys.argv[2] == "1"
repo_root = Path(sys.argv[3])
temp_root = Path(sys.argv[4])

settings = replace(
    get_settings(),
    document_input_dir=repo_root,
    task_payload_output_dir=temp_root / "handoff",
    debug_output_dir=temp_root / "debug",
)
component = ExtractionComponent(settings=settings)
payload = component.extract_document_payload(
    DocumentExtractionRequest(
        source_path,
        use_counterparty_prompt=True,
        only_pending=only_pending,
    ),
)
print(
    json.dumps(
        {
            "payload": payload,
            "debug_root": str(temp_root / "debug"),
            "handoff_root": str(temp_root / "handoff"),
        },
        ensure_ascii=False,
    )
)
"""
        previous_handler = None
        previous_timer = None
        timer_was_suspended = False
        if hasattr(signal, "SIGALRM") and hasattr(signal, "setitimer"):
            previous_handler = signal.getsignal(signal.SIGALRM)
            previous_timer = signal.getitimer(signal.ITIMER_REAL)
            signal.setitimer(signal.ITIMER_REAL, 0.0)
            timer_was_suspended = True
        try:
            try:
                completed = subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        child_script,
                        str(source_path),
                        "1" if only_pending else "0",
                        str(REPO_ROOT),
                        str(temp_root),
                    ],
                    cwd=REPO_ROOT,
                    capture_output=True,
                    text=True,
                    timeout=LIVE_REGRESSION_TIMEOUT_SECONDS,
                    check=False,
                )
            except subprocess.TimeoutExpired:
                self.fail(
                    f"live regression timed out after {LIVE_REGRESSION_TIMEOUT_SECONDS}s: "
                    f"{source_path} | debug_root={temp_root / 'debug'}"
                )
        except subprocess.TimeoutExpired as exc:
            self.fail(
                f"live regression timed out after {LIVE_REGRESSION_TIMEOUT_SECONDS}s: "
                f"{source_path} | debug_root={temp_root / 'debug'}"
            )
        finally:
            if timer_was_suspended and previous_handler is not None and previous_timer is not None:
                signal.signal(signal.SIGALRM, previous_handler)
                if previous_timer != (0.0, 0.0):
                    signal.setitimer(signal.ITIMER_REAL, *previous_timer)

        stdout_lines = [line for line in completed.stdout.splitlines() if line.strip()]
        if completed.returncode != 0:
            failure_detail = completed.stderr.strip() or completed.stdout.strip() or "subprocess returned non-zero exit status"
            self.fail(
                f"live regression subprocess failed for {source_path}: {failure_detail} "
                f"| debug_root={temp_root / 'debug'}"
            )
        if not stdout_lines:
            self.fail(f"live regression subprocess produced no JSON output for {source_path}: debug_root={temp_root / 'debug'}")

        try:
            result = json.loads(stdout_lines[-1])
        except json.JSONDecodeError as exc:
            self.fail(
                f"live regression subprocess returned invalid JSON for {source_path}: {exc} "
                f"| debug_root={temp_root / 'debug'}"
            )
        if include_debug_meta:
            return result
        return result["payload"]

    def _load_single_metrics_sidecar(self, debug_root: Path) -> dict[str, object]:
        metrics_paths = sorted(debug_root.glob("*_llm_metrics.json"))
        if not metrics_paths:
            self.fail(f"missing extract metrics sidecar under debug_root={debug_root}")
        if len(metrics_paths) != 1:
            self.fail(
                f"expected exactly one extract metrics sidecar under debug_root={debug_root}, "
                f"found {len(metrics_paths)}"
            )
        return json.loads(metrics_paths[0].read_text(encoding="utf-8"))

    def _assert_payload_matches_baseline(
        self,
        source_path: Path,
        baseline_path: Path,
        *,
        only_pending: bool,
        include_debug_meta: bool = False,
    ) -> dict[str, object] | None:
        if not source_path.exists():
            self.skipTest(f"missing actual fixture: {source_path}")
        if not baseline_path.exists():
            self.skipTest(f"missing baseline payload: {baseline_path}")

        payload_result = self._extract_document_payload_via_subprocess(
            source_path,
            only_pending=only_pending,
            include_debug_meta=include_debug_meta,
        )
        payload = payload_result["payload"] if include_debug_meta else payload_result
        baseline_payload = json.loads(baseline_path.read_text(encoding="utf-8"))

        self.assertEqual(payload["status"], "COMPLETED")
        self.assertEqual(payload["reason"], baseline_payload.get("reason"))
        self.assertEqual(payload["base_date"], baseline_payload["base_date"])
        self.assertEqual(payload["issues"], baseline_payload["issues"])
        self.assertEqual(payload["orders"], baseline_payload["orders"])
        return payload_result if include_debug_meta else None

    def test_cardif_counterparty_prompt_extracts_expected_pdf_orders(self) -> None:
        if not CARDIF_PDF_PATH.exists():
            self.skipTest(f"missing actual Cardif PDF fixture: {CARDIF_PDF_PATH}")
        if not CARDIF_BASELINE_PATH.exists():
            self.skipTest(f"missing Cardif baseline payload: {CARDIF_BASELINE_PATH}")

        payload = self._extract_document_payload_via_subprocess(
            CARDIF_PDF_PATH,
            only_pending=True,
        )
        baseline_payload = json.loads(CARDIF_BASELINE_PATH.read_text(encoding="utf-8"))

        self.assertEqual(payload["status"], "COMPLETED")
        self.assertEqual(payload["reason"], baseline_payload.get("reason"))
        self.assertEqual(payload["base_date"], baseline_payload["base_date"])
        self.assertEqual(payload["issues"], baseline_payload["issues"])
        self.assertEqual(payload["orders"], baseline_payload["orders"])

    def test_dongyang_20260318_counterparty_prompt_extracts_expected_html_payload(self) -> None:
        result = self._assert_payload_matches_baseline(
            DONGYANG_20260318_PATH,
            DONGYANG_20260318_BASELINE_PATH,
            only_pending=False,
            include_debug_meta=True,
        )
        assert result is not None
        metrics_payload = self._load_single_metrics_sidecar(Path(result["debug_root"]))
        self.assertEqual(metrics_payload.get("llm_batches_started", {}).get("t_day", 0), 0)
        self.assertEqual(metrics_payload.get("llm_batches_started", {}).get("transfer_amount", 0), 0)

    def test_dongyang_20260413_counterparty_prompt_extracts_expected_html_payload(self) -> None:
        result = self._assert_payload_matches_baseline(
            DONGYANG_20260413_PATH,
            DONGYANG_20260413_BASELINE_PATH,
            only_pending=False,
            include_debug_meta=True,
        )
        assert result is not None
        metrics_payload = self._load_single_metrics_sidecar(Path(result["debug_root"]))
        self.assertEqual(metrics_payload.get("llm_batches_started", {}).get("t_day", 0), 0)
        self.assertEqual(metrics_payload.get("llm_batches_started", {}).get("transfer_amount", 0), 0)

    def test_hanhwa_counterparty_prompt_extracts_expected_html_payload(self) -> None:
        self._assert_payload_matches_baseline(
            HANHWA_20250826_PATH,
            HANHWA_20250826_BASELINE_PATH,
            only_pending=False,
        )

    def test_shinhan_counterparty_prompt_extracts_expected_pdf_payload(self) -> None:
        self._assert_payload_matches_baseline(
            SHINHAN_20260116_PATH,
            SHINHAN_20260116_BASELINE_PATH,
            only_pending=False,
        )

    def test_kyobo_mht_counterparty_prompt_ignores_total_row_and_matches_baseline(self) -> None:
        result = self._assert_payload_matches_baseline(
            KYOBO_MHT_PATH,
            KYOBO_MHT_BASELINE_PATH,
            only_pending=True,
            include_debug_meta=True,
        )
        assert result is not None
        payload = result["payload"]
        self.assertEqual(len(payload["orders"]), 2)
        self.assertEqual(
            [order["fund_code"] for order in payload["orders"]],
            ["D706", "Y318"],
        )
        self.assertNotIn("ORDER_COVERAGE_MISMATCH", payload["issues"])

    def test_kdb_counterparty_prompt_extracts_expected_xlsx_payload(self) -> None:
        self._assert_payload_matches_baseline(
            KDB_XLSX_PATH,
            KDB_XLSX_BASELINE_PATH,
            only_pending=False,
        )

    def test_kdb_counterparty_prompt_extracts_expected_pdf_payload(self) -> None:
        self._assert_payload_matches_baseline(
            KDB_PDF_PATH,
            KDB_PDF_BASELINE_PATH,
            only_pending=False,
        )

    def test_kdb_counterparty_prompt_extracts_expected_eml_payload(self) -> None:
        self._assert_payload_matches_baseline(
            KDB_EML_PATH,
            KDB_EML_BASELINE_PATH,
            only_pending=False,
        )

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

        payload = self._extract_document_payload_via_subprocess(
            HANA_LIFE_XLSX_PATH,
            only_pending=True,
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

        payload = self._extract_document_payload_via_subprocess(
            HANA_LIFE_LEGACY_PDF_PATH,
            only_pending=True,
        )

        self.assertEqual(payload["status"], "SKIPPED")
        self.assertTrue(payload.get("reason"))

    def test_metlife_counterparty_prompt_extracts_expected_internal_subscription_order(self) -> None:
        if not METLIFE_ADDITIONAL_SUB_PATH.exists():
            self.skipTest(f"missing actual MetLife fixture: {METLIFE_ADDITIONAL_SUB_PATH}")

        settings = get_settings()
        loader = DocumentLoader()
        extractor = FundOrderExtractor(settings)
        task_payload = loader.build_task_payload(METLIFE_ADDITIONAL_SUB_PATH, chunk_size_chars=settings.llm_chunk_size_chars)
        guidance = load_counterparty_guidance(
            METLIFE_ADDITIONAL_SUB_PATH,
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
                    "METLIFE_TMP_001",
                    "MyFund Vul 혼합안정형 (주식)-삼성",
                    SettleClass.CONFIRMED,
                    OrderType.SUB,
                    "2026-04-09",
                    0,
                    "12,082,790",
                )
            ],
        )

    def test_metlife_counterparty_prompt_extracts_expected_internal_redemption_order(self) -> None:
        if not METLIFE_ADDITIONAL_RED_PATH.exists():
            self.skipTest(f"missing actual MetLife fixture: {METLIFE_ADDITIONAL_RED_PATH}")

        settings = get_settings()
        loader = DocumentLoader()
        extractor = FundOrderExtractor(settings)
        task_payload = loader.build_task_payload(METLIFE_ADDITIONAL_RED_PATH, chunk_size_chars=settings.llm_chunk_size_chars)
        guidance = load_counterparty_guidance(
            METLIFE_ADDITIONAL_RED_PATH,
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
                    "METLIFE_TMP_001",
                    "MyFund Vul 혼합안정형 (주식)-삼성",
                    SettleClass.CONFIRMED,
                    OrderType.RED,
                    "2026-04-08",
                    0,
                    "-23,182,592",
                )
            ],
        )

    def test_metlife_counterparty_prompt_subscription_payload_completes_for_only_pending_contract(self) -> None:
        if not METLIFE_ADDITIONAL_SUB_PATH.exists():
            self.skipTest(f"missing actual MetLife fixture: {METLIFE_ADDITIONAL_SUB_PATH}")

        payload = self._extract_document_payload_via_subprocess(
            METLIFE_ADDITIONAL_SUB_PATH,
            only_pending=True,
        )

        self.assertEqual(payload["status"], "COMPLETED")
        self.assertEqual(payload["base_date"], "2026-04-09")
        self.assertEqual(payload["issues"], [])
        self.assertEqual(
            payload["orders"],
            [
                {
                    "fund_code": "-",
                    "fund_name": "MyFund Vul 혼합안정형 (주식)-삼성",
                    "settle_class": "1",
                    "order_type": "3",
                    "base_date": "2026-04-09",
                    "t_day": "01",
                    "transfer_amount": "12,082,790",
                }
            ],
        )

    def test_metlife_counterparty_prompt_redemption_payload_completes_for_only_pending_contract(self) -> None:
        if not METLIFE_ADDITIONAL_RED_PATH.exists():
            self.skipTest(f"missing actual MetLife fixture: {METLIFE_ADDITIONAL_RED_PATH}")

        payload = self._extract_document_payload_via_subprocess(
            METLIFE_ADDITIONAL_RED_PATH,
            only_pending=True,
        )

        self.assertEqual(payload["status"], "COMPLETED")
        self.assertEqual(payload["base_date"], "2026-04-08")
        self.assertEqual(payload["issues"], [])
        self.assertEqual(
            payload["orders"],
            [
                {
                    "fund_code": "-",
                    "fund_name": "MyFund Vul 혼합안정형 (주식)-삼성",
                    "settle_class": "1",
                    "order_type": "1",
                    "base_date": "2026-04-08",
                    "t_day": "01",
                    "transfer_amount": "23,182,592",
                }
            ],
        )

    def test_metlife_counterparty_prompt_base_payload_matches_accepted_baseline(self) -> None:
        result = self._assert_payload_matches_baseline(
            METLIFE_BASE_PATH,
            METLIFE_BASELINE_PATH,
            only_pending=True,
            include_debug_meta=True,
        )
        assert result is not None
        metrics_payload = self._load_single_metrics_sidecar(Path(result["debug_root"]))
        self.assertEqual(metrics_payload.get("llm_batches_started", {}).get("t_day", 0), 0)
        self.assertEqual(metrics_payload.get("llm_batches_started", {}).get("transfer_amount", 0), 0)

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
                    OrderType.SUB,
                    "2026-04-08",
                    0,
                    "35,595,984",
                ),
                (
                    "BBCA00",
                    "AI 글로벌자산배분형",
                    SettleClass.CONFIRMED,
                    OrderType.RED,
                    "2026-04-08",
                    0,
                    "-90,384,110",
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

    def test_ibk_counterparty_prompt_only_pending_contract_outputs_same_day_split_rows(self) -> None:
        payload = self._extract_document_payload_via_subprocess(
            IBK_DOCUMENT_PATH,
            only_pending=True,
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
                    "order_type": "3",
                    "base_date": "2026-04-08",
                    "t_day": "01",
                    "transfer_amount": "35,595,984",
                },
                {
                    "fund_code": "BBCA00",
                    "fund_name": "AI 글로벌자산배분형",
                    "settle_class": "1",
                    "order_type": "1",
                    "base_date": "2026-04-08",
                    "t_day": "01",
                    "transfer_amount": "90,384,110",
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
        payload = self._extract_document_payload_via_subprocess(
            HANAIS_XLSX_PATH,
            only_pending=False,
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
        if not HANAIS_PDF_PATH.exists():
            self.skipTest(f"missing actual Heungkuk HANAIS PDF fixture: {HANAIS_PDF_PATH}")

        payload = self._extract_document_payload_via_subprocess(
            HANAIS_PDF_PATH,
            only_pending=True,
        )

        self.assertEqual(payload["status"], "SKIPPED")
        self.assertTrue(payload.get("reason"))


if __name__ == "__main__":
    unittest.main()
