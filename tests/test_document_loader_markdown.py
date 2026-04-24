from __future__ import annotations

import base64
from email.message import EmailMessage
import struct
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

from openpyxl import Workbook
from app.document_loader import DocumentLoadTaskPayload, DocumentLoader, TargetFundScope
from pdfminer.pdfdocument import PDFPasswordIncorrect
from pdfplumber.utils.exceptions import PdfminerException

REPO_ROOT = Path(__file__).resolve().parents[1]
DOCUMENT_DIR = REPO_ROOT / "document"
METLIFE_ADDITIONAL_SUB_PATH = DOCUMENT_DIR / "메트라이프생명_0408_추가_추가설정.eml"
METLIFE_ADDITIONAL_RED_PATH = DOCUMENT_DIR / "메트라이프생명_추가설정해지_0408.eml"


class DocumentLoaderMarkdownTests(unittest.TestCase):
    """`DocumentLoader`의 markdown/coverage 변환 규칙을 회귀 테스트한다."""

    def setUp(self) -> None:
        """각 테스트에서 재사용할 loader 인스턴스를 준비한다."""
        self.loader = DocumentLoader()

    def test_build_markdown_renders_sheet_as_table(self) -> None:
        raw_text = (
            "[SHEET Sheet1]\n"
            "펀드 | 펀드명 | 설정금액 | 해지금액\n"
            "4070003 | Alpha | 4260262 | -18053366\n"
            "7030802 | Beta | 3282353 | -43457625"
        )

        markdown = self.loader.build_markdown(raw_text)

        self.assertIn("## Sheet Sheet1", markdown)
        self.assertIn("| 펀드 | 펀드명 | 설정금액 | 해지금액 |", markdown)
        self.assertIn("| 4070003 | Alpha | 4260262 | -18053366 |", markdown)

    def test_task_payload_write_json_returns_metadata(self) -> None:
        payload = DocumentLoadTaskPayload(
            source_path="/tmp/sample.pdf",
            file_name="sample.pdf",
            pdf_password=None,
            content_type="application/pdf",
            raw_text="raw",
            markdown_text="markdown",
            chunks=("markdown",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=1,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )

        with TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / "nested" / "payload.json"
            write_result = payload.write_json(target)

            self.assertEqual(
                write_result,
                {
                    "ok": True,
                    "path": str(target),
                    "size_bytes": len(target.read_bytes()),
                },
            )
            self.assertTrue(target.exists())

    def test_build_markdown_renders_page_text_block(self) -> None:
        raw_text = "[PAGE 1]\n기준일자 : 2025-11-27\n수신처 : 삼성자산운용"

        markdown = self.loader.build_markdown(raw_text)

        self.assertIn("## Page 1", markdown)
        self.assertIn("```text", markdown)
        self.assertIn("기준일자 : 2025-11-27", markdown)

    def test_excel_format_cell_collapses_embedded_newlines(self) -> None:
        self.assertEqual(
            self.loader._format_cell("T일 순유입 \n확정금액"),
            "T일 순유입 확정금액",
        )

    def test_load_xlsx_normalizes_integer_like_float_tail_but_keeps_real_decimals(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            workbook = Workbook()
            sheet = workbook.active
            sheet.title = "Sheet1"
            sheet.append(["펀드코드", "artifact_amount", "decimal_amount", "ratio"])
            sheet.append(["450042", 70000000.00000001, 50572.49, 0.0019])
            path = Path(tmp_dir) / "artifact.xlsx"
            workbook.save(path)

            extracted = self.loader.load(path)

        self.assertIn("70000000", extracted.raw_text)
        self.assertNotIn("70000000.00000001", extracted.raw_text)
        self.assertIn("50572.49", extracted.raw_text)
        self.assertIn("0.0019", extracted.raw_text)

    def test_split_for_llm_supports_markdown_section_delimiter(self) -> None:
        markdown = (
            "## Page 1\n\n```text\nA\n```"
            + DocumentLoader.MARKDOWN_SECTION_DELIMITER
            + "## Page 2\n\n```text\nB\n```"
        )

        chunks = self.loader.split_for_llm(markdown, chunk_size_chars=1000)

        self.assertEqual(len(chunks), 1)
        self.assertIn("## Page 1", chunks[0])
        self.assertIn("## Page 2", chunks[0])

    def test_build_markdown_preserves_preamble_before_real_header(self) -> None:
        raw_text = (
            "[SHEET Sheet1]\n"
            "클라이언트 설정해지지시서 |  |  | \n"
            "수 신: 호스트 |  |  | \n"
            "펀드 | 펀드명 | 설정금액 | 해지금액\n"
            "10205 | Alpha | 100 | -20"
        )

        markdown = self.loader.build_markdown(raw_text)

        self.assertIn("```text\n클라이언트 설정해지지시서", markdown)
        self.assertIn("| 펀드 | 펀드명 | 설정금액 | 해지금액 |", markdown)
        self.assertIn("| 10205 | Alpha | 100 | -20 |", markdown)

    def test_build_markdown_collapses_multi_row_header(self) -> None:
        raw_text = (
            "[SHEET 호스트]\n"
            "특별계정_VL & VUL 자금 운용 현황 |  |  |  |\n"
            "운용지시펀드코드 | 펀드명 | T일 | T+1일\n"
            "펀드코드_KASS |  | 투입금액 | 인출금액\n"
            "C1005 | 글로벌리츠(VUL) | 920315 | 269"
        )

        markdown = self.loader.build_markdown(raw_text)

        self.assertIn("| 운용지시펀드코드 / 펀드코드_KASS | 펀드명 | T일 / 투입금액 | T+1일 / 인출금액 |", markdown)
        self.assertIn("| C1005 | 글로벌리츠(VUL) | 920315 | 269 |", markdown)

    def test_build_markdown_drops_summary_and_ratio_columns_from_structured_table(self) -> None:
        raw_text = (
            "[SHEET 호스트]\n"
            "펀드코드 | 펀드명 | T일 | T일 | T+1일 | 5영업일 순유입 합산 (예상) | NAV대비\n"
            " |  | T일 순유입 확정금액 | T일 순유입 확정좌수 | T+1일 순유입예상금액 | 순유입(예상) | NAV대비\n"
            "C1005 | 글로벌리츠(VUL) | 920315 | 478728 | 278625 | 6801078 | 0.0019"
        )

        markdown = self.loader.build_markdown(raw_text)

        self.assertIn("T일 / T일 순유입 확정금액", markdown)
        self.assertIn("T+1일 / T+1일 순유입예상금액", markdown)
        self.assertNotIn("5영업일 순유입 합산", markdown)
        self.assertNotIn("NAV대비", markdown)
        self.assertNotIn("확정좌수", markdown)

    def test_build_markdown_prefers_unmixed_amount_columns_when_header_mixes_units_and_amounts(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "거래유형 |  | 펀드코드 |  | 사무관리사 | 결제일 | 설정(해지)전좌수 | 설정(해지)좌수 | 펀드납입(인출)금액\n"
            "판매사 |  | 펀드명 |  | 펀드평가사 | 통화코드 | 설정(해지)후좌수 | 설정(해지)금액 | 판매회사분결제액\n"
            "설정 |  | BBC13F |  | 하나펀드서비스 | 2025-11-27 | 18,577,453,950 | 3,406,206 | 7,108,103\n"
            "하나생명 |  | VUL 주식성장형(1형)_SamsungActive |  |  |  | 18,580,860,156 | 7,108,103 | 7,108,103"
        )

        markdown = self.loader.build_markdown(raw_text)

        self.assertIn("펀드납입(인출)금액 / 판매회사분결제액", markdown)
        self.assertNotIn("설정(해지)좌수 / 설정(해지)금액", markdown)
        self.assertIn("| 설정 | BBC13F | 하나펀드서비스 | 2025-11-27 |", markdown)
        self.assertIn("7,108,103", markdown)

    def test_estimate_order_cell_count_supports_spaced_context_and_amount_labels(self) -> None:
        raw_text = (
            "[SHEET Sheet1]\n"
            " |  | 구 분 |  |  | 펀 드 명 |  | 금 액\n"
            " |  | 설정 |  |  | A01450 | 액티브형 | 81,393,887\n"
            " |  | 해지 |  |  | A01550 | 밸런스형 | 4,947,720\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 2)

    def test_estimate_order_cell_count_supports_generic_amount_with_unit_header(self) -> None:
        raw_text = (
            "[MHT sample]\n"
            "수탁사 / 펀드코드 | 교보생명 / 펀드코드 | 펀드명 | 금액 ( 원 ) | 운용사\n"
            "D706 | D006 | 인덱스혼합형 | 500,000,000 | 삼성자산운용\n"
            "합 계 | 합 계 | 500,000,000 | 500,000,000 | 500,000,000\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 1)

    def test_currency_decorated_amount_tokens_are_recognized_for_document_side_parsing(self) -> None:
        self.assertTrue(self.loader._is_amount_string("₩12,082,790"))
        self.assertTrue(self.loader._is_amount_string("-₩23 ,182,592"))
        self.assertTrue(self.loader._is_amount_string("KRW\t100"))
        self.assertTrue(self.loader._is_amount_string("₩\u00a0123"))
        self.assertEqual(self.loader._parse_numeric_amount_text("₩12,082,790"), 12082790.0)
        self.assertEqual(self.loader._parse_numeric_amount_text("-₩23 ,182,592"), -23182592.0)
        self.assertEqual(self.loader._parse_numeric_amount_text("KRW\t100"), 100.0)
        self.assertEqual(self.loader._parse_numeric_amount_text("₩\u00a0123"), 123.0)
        self.assertTrue(self.loader._is_zero_amount("₩0"))
        self.assertEqual(self.loader._normalize_pipe_cell("₩\u00a0123"), "123")
        self.assertEqual(self.loader._normalize_pipe_cell("-₩23 ,182,592"), "-23,182,592")

    def test_prefer_plain_pdf_text_uses_table_when_table_coverage_is_better(self) -> None:
        plain_text = (
            "하나펀드서비스(주)\n"
            "운 용 지 시 서 화면번호 : 13001\n"
            "거래종류 : 펀드설정(수탁은행용)\n"
            "설정 BBC13F 하나펀드서비스 2025-11-27 18,577,453,950 3,406,206 7,108,103\n"
            "하나생명 VUL 주식성장형(1형)_SamsungActive 18,580,860,156 7,108,103 7,108,103\n"
        )
        table_text = (
            "거래유형 |  | 펀드코드 |  | 사무관리사 | 결제일 | 설정(해지)전좌수 | 설정(해지)좌수 | 펀드납입(인출)금액\n"
            "판매사 |  | 펀드명 |  | 펀드평가사 | 통화코드 | 설정(해지)후좌수 | 설정(해지)금액 | 판매회사분결제액\n"
            "설정 |  | BBC13F |  | 하나펀드서비스 | 2025-11-27 | 18,577,453,950 | 3,406,206 | 7,108,103\n"
            "하나생명 |  | VUL 주식성장형(1형)_SamsungActive |  |  |  | 18,580,860,156 | 7,108,103 | 7,108,103\n"
        )

        self.assertFalse(self.loader._prefer_plain_pdf_text(plain_text, table_text))

    def test_build_markdown_drops_bracket_aggregate_rows_from_pdf_table(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "거래유형 |  | 펀드코드 |  | 사무관리사 | 결제일 | 설정(해지)전좌수 | 설정(해지)좌수 | 펀드납입(인출)금액\n"
            "판매사 |  | 펀드명 |  | 펀드평가사 | 통화코드 | 설정(해지)후좌수 | 설정(해지)금액 | 판매회사분결제액\n"
            "설정 |  | BBC13F |  | 하나펀드서비스 | 2025-11-27 | 18,577,453,950 | 3,406,206 | 7,108,103\n"
            "하나생명 |  | VUL 주식성장형(1형)_SamsungActive |  |  |  | 18,580,860,156 | 7,108,103 | 7,108,103\n"
            "|  | [매매처계] |  |  |  | 18,577,453,950 | 3,406,206 | 7,108,103\n"
            "[수탁사계] |  |  |  |  |  | 18,577,453,950 | 3,406,206 | 7,108,103\n"
        )

        markdown = self.loader.build_markdown(raw_text)

        self.assertIn("BBC13F", markdown)
        self.assertIn("VUL 주식성장형(1형)_SamsungActive", markdown)
        self.assertNotIn("[매매처계]", markdown)
        self.assertNotIn("[수탁사계]", markdown)

    def test_is_total_like_text_recognizes_bracket_aggregate_labels(self) -> None:
        self.assertTrue(self.loader._is_total_like_text("[매매처계]"))
        self.assertTrue(self.loader._is_total_like_text("[결제일계]"))
        self.assertTrue(self.loader._is_total_like_text("[수탁사계]"))
        self.assertTrue(self.loader._is_total_like_text("[거래종류계]"))
        self.assertTrue(self.loader._is_total_like_text("[7개 펀드]"))
        self.assertTrue(self.loader._is_total_like_text("7개 펀드 합계"))

    def test_load_html_extracts_text_and_expands_rowspan_cells(self) -> None:
        html = """<!doctype html>
<html lang="ko">
  <body>
    <div>기준일 : 20250826</div>
    <div>삼성액티브자산운영</div>
    <table>
      <tr>
        <th>펀드코드</th>
        <th>펀드명</th>
        <th>구분</th>
        <th>설정금액</th>
      </tr>
      <tr>
        <td rowspan="2">V2201S</td>
        <td rowspan="2">VUL적립 성장주식형</td>
        <td>입금</td>
        <td>21,187,951</td>
      </tr>
      <tr>
        <td>출금</td>
        <td>0</td>
      </tr>
    </table>
  </body>
</html>
"""
        with TemporaryDirectory() as tmp_dir:
            html_path = Path(tmp_dir) / "sample.html"
            html_path.write_text(html, encoding="utf-8")

            extracted = self.loader.load(html_path)

        self.assertEqual(extracted.content_type, "text/html")
        self.assertIn("[HTML sample.html]", extracted.raw_text)
        self.assertIn("기준일 : 20250826", extracted.raw_text)
        self.assertIn("펀드코드 | 펀드명 | 구분 | 설정금액", extracted.raw_text)
        self.assertIn("V2201S | VUL적립 성장주식형 | 입금 | 21,187,951", extracted.raw_text)
        self.assertIn("V2201S | VUL적립 성장주식형 | 출금 | 0", extracted.raw_text)
        self.assertIn("## HTML sample.html", extracted.markdown_text)

    def test_load_html_preserves_numeric_rowspan_fund_code(self) -> None:
        html = """<!doctype html>
<html lang="ko">
  <body>
    <table>
      <tr>
        <th>펀드코드</th>
        <th>펀드명</th>
        <th>구분</th>
        <th>설정금액</th>
      </tr>
      <tr>
        <td rowspan="2">10205</td>
        <td rowspan="2">인덱스혼합형</td>
        <td>입금</td>
        <td>100,000</td>
      </tr>
      <tr>
        <td>출금</td>
        <td>0</td>
      </tr>
    </table>
  </body>
</html>
"""
        with TemporaryDirectory() as tmp_dir:
            html_path = Path(tmp_dir) / "numeric-code.html"
            html_path.write_text(html, encoding="utf-8")

            extracted = self.loader.load(html_path)

        self.assertIn("10205 | 인덱스혼합형 | 입금 | 100,000", extracted.raw_text)
        self.assertIn("10205 | 인덱스혼합형 | 출금 | 0", extracted.raw_text)

    def test_load_html_preserves_rowspan_amount_context_for_child_rows(self) -> None:
        html = """<!doctype html>
<html lang="ko">
  <body>
    <table>
      <tr>
        <th>펀드코드</th>
        <th>펀드명</th>
        <th>구분</th>
        <th>펀드계</th>
        <th>익영업일 이체예상금액</th>
      </tr>
      <tr>
        <td rowspan="3">V2201S</td>
        <td rowspan="3">VUL적립 성장주식형</td>
        <td>입금</td>
        <td rowspan="3">21,187,951</td>
        <td rowspan="3">-36,400,000</td>
      </tr>
      <tr>
        <td>출금</td>
      </tr>
      <tr>
        <td>환매 / 신청</td>
      </tr>
    </table>
  </body>
</html>
"""
        with TemporaryDirectory() as tmp_dir:
            html_path = Path(tmp_dir) / "hanhwa-style.html"
            html_path.write_text(html, encoding="utf-8")

            extracted = self.loader.load(html_path)

        self.assertIn("V2201S | VUL적립 성장주식형 | 입금 | 21,187,951 | -36,400,000", extracted.raw_text)
        self.assertIn("V2201S | VUL적립 성장주식형 | 출금 | 21,187,951 | -36,400,000", extracted.raw_text)
        self.assertIn("V2201S | VUL적립 성장주식형 | 환매 / 신청 | 21,187,951 | -36,400,000", extracted.raw_text)

    def test_build_markdown_keeps_order_context_column_for_html_rowspan_rows(self) -> None:
        html = """<!doctype html>
<html lang="ko">
  <body>
    <table>
      <tr>
        <th>펀드코드</th>
        <th>펀드명</th>
        <th>설정(예탁) 및 해지금액</th>
        <th>설정(예탁) 및 해지금액</th>
        <th>익영업일 이체예상금액</th>
      </tr>
      <tr>
        <th>펀드코드</th>
        <th>펀드명</th>
        <th>투자일임/수익증권</th>
        <th>펀드계</th>
        <th>익영업일 이체예상금액</th>
      </tr>
      <tr>
        <td rowspan="3">V2201S</td>
        <td rowspan="3">VUL적립 성장주식형</td>
        <td>입금</td>
        <td rowspan="3">21,187,951</td>
        <td rowspan="3">-36,400,000</td>
      </tr>
      <tr>
        <td>출금</td>
      </tr>
      <tr>
        <td>환매 / 신청</td>
      </tr>
    </table>
  </body>
</html>
"""
        with TemporaryDirectory() as tmp_dir:
            html_path = Path(tmp_dir) / "hanhwa-style-markdown.html"
            html_path.write_text(html, encoding="utf-8")

            extracted = self.loader.load(html_path)
            markdown = extracted.markdown_text

        self.assertIn("투자일임/수익증권 / 구분", markdown)
        self.assertIn("| V2201S | VUL적립 성장주식형 | 입금 | 21,187,951 | -36,400,000 |", markdown)
        self.assertIn("| V2201S | VUL적립 성장주식형 | 출금 | 21,187,951 | -36,400,000 |", markdown)
        self.assertIn("| V2201S | VUL적립 성장주식형 | 환매 / 신청 | 21,187,951 | -36,400,000 |", markdown)
        self.assertFalse(extracted.markdown_loss_detected)
        self.assertEqual(extracted.effective_llm_text_kind, "markdown_text")

    def test_build_markdown_does_not_blank_same_amount_rows_without_html_inheritance_hints(self) -> None:
        raw_text = (
            "[HTML same-amount.html]\n"
            "펀드코드 | 펀드명 | 구분 | 설정금액 | 해지금액\n"
            "F001 | Alpha Fund | 입금 | 10,000 | 0\n"
            "F001 | Alpha Fund | 출금 | 10,000 | 0\n"
        )

        markdown = self.loader.build_markdown(raw_text)

        self.assertIn("| F001 | Alpha Fund | 입금 | 10,000 |", markdown)
        self.assertIn("| F001 | Alpha Fund | 출금 | 10,000 |", markdown)

    def test_column_has_order_context_values_requires_all_values_to_be_row_kinds(self) -> None:
        body_rows = [
            ["펀드A", "입금", "보험료입금"],
            ["펀드A", "출금", "기타"],
            ["펀드A", "환매 / 신청", "특별계정운용보수"],
        ]

        self.assertTrue(self.loader._column_has_order_context_values(body_rows, 1))
        self.assertFalse(self.loader._column_has_order_context_values(body_rows, 2))

    def test_load_html_supports_cp949_encoded_document(self) -> None:
        html = """<!doctype html>
<html lang="ko">
  <head><meta charset="euc-kr" /></head>
  <body>
    <div>기준일 : 20250826</div>
    <table>
      <tr><th>펀드코드</th><th>펀드명</th><th>설정금액</th></tr>
      <tr><td>V2201S</td><td>성장주식형</td><td>21,187,951</td></tr>
    </table>
  </body>
</html>
"""
        with TemporaryDirectory() as tmp_dir:
            html_path = Path(tmp_dir) / "cp949.html"
            html_path.write_bytes(html.encode("cp949"))

            extracted = self.loader.load(html_path)

        self.assertIn("기준일 : 20250826", extracted.raw_text)
        self.assertIn("V2201S | 성장주식형 | 21,187,951", extracted.raw_text)

    def test_load_eml_prefers_html_body_and_keeps_headers(self) -> None:
        message = EmailMessage()
        message["Subject"] = "삼성액티브자산운용 설정/해지"
        message["From"] = "bhkim@minisoft.co.kr"
        message["To"] = "heejin.minisoft@gmail.com"
        message["Date"] = "Mon, 16 Mar 2026 06:43:45 +0000"
        message.set_content("BUY & SELL REPORT\nplain fallback body")
        message.add_alternative(
            """<html><body>
            <div>BUY &amp; SELL REPORT</div>
            <table>
              <tr><th>Date</th><th>Fund Code</th><th>Fund Name</th><th>Buy</th><th>Sell</th></tr>
              <tr><td>11-28-2025</td><td>151128</td><td>AIA VUL Alpha</td><td>1,311,285</td><td>55,901,379</td></tr>
            </table>
            </body></html>""",
            subtype="html",
        )

        with TemporaryDirectory() as tmp_dir:
            eml_path = Path(tmp_dir) / "sample.eml"
            eml_path.write_bytes(message.as_bytes())

            extracted = self.loader.load(eml_path)

        self.assertEqual(extracted.content_type, "message/rfc822")
        self.assertIn("[EML sample.eml]", extracted.raw_text)
        self.assertIn("Subject: 삼성액티브자산운용 설정/해지", extracted.raw_text)
        self.assertIn("BUY & SELL REPORT", extracted.raw_text)
        self.assertIn("Date | Fund Code | Fund Name | Buy | Sell", extracted.raw_text)
        self.assertIn("11-28-2025 | 151128 | AIA VUL Alpha | 1,311,285 | 55,901,379", extracted.raw_text)
        self.assertIn("## EML sample.eml", extracted.markdown_text)

    def test_load_eml_html_body_preserves_hanhwa_rowspan_context_in_markdown(self) -> None:
        message = EmailMessage()
        message["Subject"] = "한화 html body"
        message.add_alternative(
            """<html><body>
            <table>
              <tr><th>펀드코드</th><th>펀드명</th><th>구분</th><th>펀드계</th><th>익영업일 이체예상금액</th></tr>
              <tr><td rowspan="3">V2201S</td><td rowspan="3">VUL적립 성장주식형</td><td>입금</td><td rowspan="3">21,187,951</td><td rowspan="3">-36,400,000</td></tr>
              <tr><td>출금</td></tr>
              <tr><td>환매 / 신청</td></tr>
            </table>
            </body></html>""",
            subtype="html",
        )

        with TemporaryDirectory() as tmp_dir:
            eml_path = Path(tmp_dir) / "hanhwa-html-body.eml"
            eml_path.write_bytes(message.as_bytes())

            extracted = self.loader.load(eml_path)

        self.assertIn("V2201S | VUL적립 성장주식형 | 입금 | 21,187,951 | -36,400,000", extracted.raw_text)
        self.assertIn("V2201S | VUL적립 성장주식형 | 출금 | 21,187,951 | -36,400,000", extracted.raw_text)
        self.assertIn("| V2201S | VUL적립 성장주식형 | 입금 | 21,187,951 | -36,400,000 |", extracted.markdown_text)
        self.assertIn("| V2201S | VUL적립 성장주식형 | 출금 | 21,187,951 | -36,400,000 |", extracted.markdown_text)
        self.assertFalse(extracted.markdown_loss_detected)

    def test_load_mht_extracts_main_html_body_with_unicode_charset(self) -> None:
        html = """<html><body>
        <div>자금운용 해지</div>
        <table>
          <tr><th>펀드코드</th><th>펀드명</th><th>해지금액</th></tr>
          <tr><td>5440</td><td>삼성혼합형</td><td>12,345,678</td></tr>
        </table>
        </body></html>"""
        html_bytes = html.encode("utf-16-le")
        raw_mht = (
            b"MIME-Version: 1.0\r\n"
            b"Content-Type: multipart/related; boundary=\"BOUNDARY\"\r\n"
            b"\r\n"
            b"--BOUNDARY\r\n"
            b"Content-Location: file:///C:/sample.htm\r\n"
            b"Content-Transfer-Encoding: base64\r\n"
            b"Content-Type: text/html; charset=\"unicode\"\r\n"
            b"\r\n"
            + base64.b64encode(html_bytes)
            + b"\r\n"
            b"--BOUNDARY--\r\n"
        )

        with TemporaryDirectory() as tmp_dir:
            mht_path = Path(tmp_dir) / "sample.mht"
            mht_path.write_bytes(raw_mht)

            extracted = self.loader.load(mht_path)

        self.assertEqual(extracted.content_type, "multipart/related")
        self.assertIn("[MHT sample.mht]", extracted.raw_text)
        self.assertIn("자금운용 해지", extracted.raw_text)
        self.assertIn("5440 | 삼성혼합형 | 12,345,678", extracted.raw_text)
        self.assertIn("## MHT sample.mht", extracted.markdown_text)

    def test_load_mht_preserves_html_rowspan_context_in_markdown(self) -> None:
        html = """<html><body>
        <table>
          <tr><th>펀드코드</th><th>펀드명</th><th>구분</th><th>펀드계</th><th>익영업일 이체예상금액</th></tr>
          <tr><td rowspan="3">V2201S</td><td rowspan="3">VUL적립 성장주식형</td><td>입금</td><td rowspan="3">21,187,951</td><td rowspan="3">-36,400,000</td></tr>
          <tr><td>출금</td></tr>
          <tr><td>환매 / 신청</td></tr>
        </table>
        </body></html>"""
        raw_mht = (
            b"MIME-Version: 1.0\r\n"
            b"Content-Type: multipart/related; boundary=\"BOUNDARY\"\r\n"
            b"\r\n"
            b"--BOUNDARY\r\n"
            b"Content-Location: file:///C:/sample.htm\r\n"
            b"Content-Type: text/html; charset=\"utf-8\"\r\n"
            b"Content-Transfer-Encoding: quoted-printable\r\n"
            b"\r\n"
            + html.encode("utf-8")
            + b"\r\n"
            b"--BOUNDARY--\r\n"
        )

        with TemporaryDirectory() as tmp_dir:
            mht_path = Path(tmp_dir) / "rowspan.mht"
            mht_path.write_bytes(raw_mht)

            extracted = self.loader.load(mht_path)

        self.assertIn("V2201S | VUL적립 성장주식형 | 입금 | 21,187,951 | -36,400,000", extracted.raw_text)
        self.assertIn("V2201S | VUL적립 성장주식형 | 출금 | 21,187,951 | -36,400,000", extracted.raw_text)
        self.assertIn("| V2201S | VUL적립 성장주식형 | 입금 | 21,187,951 | -36,400,000 |", extracted.markdown_text)
        self.assertIn("| V2201S | VUL적립 성장주식형 | 출금 | 21,187,951 | -36,400,000 |", extracted.markdown_text)
        self.assertFalse(extracted.markdown_loss_detected)

    def test_build_task_payload_falls_back_to_raw_chunks_when_html_markdown_audit_fails(self) -> None:
        html = """<!doctype html>
<html lang="ko">
  <body>
    <table>
      <tr><th>펀드코드</th><th>펀드명</th><th>구분</th><th>설정금액</th></tr>
      <tr><td>V2201S</td><td>VUL적립 성장주식형</td><td>입금</td><td>21,187,951</td></tr>
    </table>
  </body>
</html>
"""
        with TemporaryDirectory() as tmp_dir:
            html_path = Path(tmp_dir) / "audit-fail.html"
            html_path.write_text(html, encoding="utf-8")

            with patch.object(self.loader, "_audit_html_markdown_loss", return_value=["html_label_missing"]):
                payload = self.loader.build_task_payload(html_path, chunk_size_chars=12000)

        self.assertTrue(payload.markdown_loss_detected)
        self.assertEqual(payload.markdown_loss_reasons, ("html_label_missing",))
        self.assertEqual(payload.effective_llm_text_kind, "raw_text")
        self.assertEqual(
            payload.chunks,
            tuple(self.loader.split_for_llm(payload.raw_text, chunk_size_chars=12000)),
        )

    def test_load_html_preserves_nested_table_fund_rows_in_raw_and_markdown(self) -> None:
        html = """<!doctype html>
<html lang="ko">
  <body>
    <div>특별계정사업부 실적배당형 펀드별 설정 및 해지내역서</div>
    <table class="layout">
      <tr>
        <td>
          <div>수탁은행 수령금액내역</div>
          <table class="fund-data">
            <tr><th>펀드코드</th><th>펀드명</th><th>구분</th><th>수탁은행</th><th>판매사</th></tr>
            <tr><td>V3301P</td><td>변액연금 혼합형</td><td>입금</td><td>0</td><td>0</td></tr>
            <tr><td>V530318</td><td>변액연금 성장주혼합형</td><td>입금</td><td>0</td><td>0</td></tr>
            <tr><td>V230428</td><td>변액연금 가치주식형</td><td>입금</td><td>0</td><td>0</td></tr>
            <tr><td>V430234</td><td>변액연금 배당주혼합형</td><td>입금</td><td>0</td><td>0</td></tr>
            <tr><td>V530239</td><td>변액연금 액티브주식형</td><td>입금</td><td>0</td><td>0</td></tr>
            <tr><td>V630116</td><td>변액연금 글로벌채권형</td><td>입금</td><td>0</td><td>0</td></tr>
          </table>
        </td>
      </tr>
    </table>
    <p>익영업일 이체예상금액은 공시기준가를 적용합니다.</p>
  </body>
</html>
"""
        with TemporaryDirectory() as tmp_dir:
            html_path = Path(tmp_dir) / "nested_wrapper.html"
            html_path.write_text(html, encoding="utf-8")

            extracted = self.loader.load(html_path)

        for fund_code in ("V3301P", "V530318", "V230428", "V430234", "V530239", "V630116"):
            self.assertIn(fund_code, extracted.raw_text)
            self.assertIn(fund_code, extracted.markdown_text)
        self.assertFalse(extracted.markdown_loss_detected)

    def test_load_html_falls_back_to_regex_when_bs4_parser_fails(self) -> None:
        html = """<!doctype html>
<html><body><table><tr><th>펀드코드</th><th>펀드명</th></tr><tr><td>V3301P</td><td>변액연금 혼합형</td></tr></table></body></html>
"""
        with TemporaryDirectory() as tmp_dir:
            html_path = Path(tmp_dir) / "bs4-fallback.html"
            html_path.write_text(html, encoding="utf-8")

            with patch("app.document_loaders.html_loader.BeautifulSoup", side_effect=RuntimeError("parser unavailable")):
                extracted = self.loader.load(html_path)

        self.assertIn("V3301P", extracted.raw_text)
        self.assertIn("V3301P", extracted.markdown_text)

    def test_build_task_payload_falls_back_to_raw_chunks_when_html_markdown_audit_is_missing(self) -> None:
        html = """<!doctype html><html><body><div>테스트 헤더</div></body></html>"""
        with TemporaryDirectory() as tmp_dir:
            html_path = Path(tmp_dir) / "audit-missing.html"
            html_path.write_text(html, encoding="utf-8")

            with patch.object(
                self.loader,
                "_load_html_with_render_hints",
                return_value=(
                    "[HTML audit-missing.html]\n\n테스트 헤더\nV3301P | 변액연금 혼합형 | 입금 | 0 | 0",
                    {"preferred_markdown_text": "## HTML audit-missing.html\n\n```text\n테스트 헤더\n```"},
                ),
            ):
                payload = self.loader.build_task_payload(html_path, chunk_size_chars=12000)

        self.assertTrue(payload.markdown_loss_detected)
        self.assertEqual(payload.markdown_loss_reasons, ("html_markdown_unverified",))
        self.assertEqual(payload.effective_llm_text_kind, "raw_text")
        self.assertEqual(
            payload.chunks,
            tuple(self.loader.split_for_llm(payload.raw_text, chunk_size_chars=12000)),
        )

    def test_load_xls_uses_biff_parser_and_skips_hidden_rows_cols(self) -> None:
        fake_sheet = MagicMock()
        fake_sheet.visibility = 0
        fake_sheet.nrows = 3
        fake_sheet.ncols = 3
        fake_sheet.rowinfo_map = {2: MagicMock(hidden=1)}
        fake_sheet.colinfo_map = {2: MagicMock(hidden=1)}
        fake_sheet.cell_type.side_effect = lambda row, col: 1
        fake_sheet.cell_value.side_effect = lambda row, col: (
            ("펀드코드", "설정금액", "숨김열"),
            ("6101", "1000", "HIDDEN"),
            ("6102", "2000", "HIDDEN2"),
        )[row][col]

        fake_workbook = MagicMock()
        fake_workbook.datemode = 0
        fake_workbook.sheet_names.return_value = ["호스트"]
        fake_workbook.sheet_by_name.return_value = fake_sheet

        with TemporaryDirectory() as tmp_dir:
            xls_path = Path(tmp_dir) / "sample.xls"
            xls_path.write_bytes(b"A" * 32)

            with patch("app.document_loaders.excel_loader.xlrd.open_workbook", return_value=fake_workbook):
                extracted = self.loader.load(xls_path)

        self.assertIn("[SHEET 호스트]", extracted.raw_text)
        self.assertIn("6101 | 1000", extracted.raw_text)
        self.assertNotIn("HIDDEN", extracted.raw_text)
        self.assertNotIn("6102", extracted.raw_text)

    def test_load_xls_keeps_direct_biff_path_when_workbook_opens_normally(self) -> None:
        fake_sheet = MagicMock()
        fake_sheet.visibility = 0
        fake_sheet.nrows = 2
        fake_sheet.ncols = 2
        fake_sheet.rowinfo_map = {}
        fake_sheet.colinfo_map = {}
        fake_sheet.cell_type.side_effect = lambda row, col: 1
        fake_sheet.cell_value.side_effect = lambda row, col: (("펀드", "금액"), ("6101", "1000"))[row][col]

        fake_workbook = MagicMock()
        fake_workbook.datemode = 0
        fake_workbook.sheet_names.return_value = ["Sheet1"]
        fake_workbook.sheet_by_name.return_value = fake_sheet

        with TemporaryDirectory() as tmp_dir:
            xls_path = Path(tmp_dir) / "direct.xls"
            xls_path.write_bytes(b"A" * 32)

            with patch("app.document_loaders.excel_loader.xlrd.open_workbook", return_value=fake_workbook):
                with patch.object(self.loader, "_load_legacy_workbook_via_parser_fallback") as fallback:
                    extracted = self.loader.load(xls_path)

        fallback.assert_not_called()
        self.assertIn("[SHEET Sheet1]", extracted.raw_text)
        self.assertIn("6101 | 1000", extracted.raw_text)

    def test_load_xls_uses_parser_style_fallback_only_for_biff_related_errors(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            xls_path = Path(tmp_dir) / "fallback.xls"
            xls_path.write_bytes(b"A" * 32)

            with patch(
                "app.document_loaders.excel_loader.xlrd.open_workbook",
                side_effect=struct.error("unpack requires a buffer of 2 bytes"),
            ):
                with patch.object(
                    self.loader,
                    "_load_legacy_workbook_via_parser_fallback",
                    return_value="[SHEET 복구]\n6101 | 1000",
                ) as fallback:
                    extracted = self.loader.load(xls_path)

        fallback.assert_called_once_with(xls_path)
        self.assertIn("[SHEET 복구]", extracted.raw_text)
        self.assertIn("6101 | 1000", extracted.raw_text)

    def test_load_xls_falls_back_to_xml_spreadsheet_when_not_biff(self) -> None:
        if self.loader.__class__.__module__ is None:  # pragma: no cover - defensive
            self.skipTest("loader unavailable")

        xlrderror = getattr(getattr(__import__("xlrd"), "biffh"), "XLRDError")

        with TemporaryDirectory() as tmp_dir:
            xls_path = Path(tmp_dir) / "sample.xls"
            xls_path.write_text(
                """<?xml version="1.0"?>
                <Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet">
                  <Worksheet ss:Name="Sheet1" xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">
                    <Table>
                      <Row><Cell><Data>펀드</Data></Cell><Cell><Data>금액</Data></Cell></Row>
                      <Row><Cell><Data>6101</Data></Cell><Cell><Data>1000</Data></Cell></Row>
                    </Table>
                  </Worksheet>
                </Workbook>""",
                encoding="utf-8",
            )

            with patch(
                "app.document_loaders.excel_loader.xlrd.open_workbook",
                side_effect=xlrderror("Unsupported format"),
            ):
                extracted = self.loader.load(xls_path)

        self.assertIn("[SHEET Sheet1]", extracted.raw_text)
        self.assertIn("6101 | 1000", extracted.raw_text)

    def test_load_xls_falls_back_to_xlsx_loader_when_not_biff(self) -> None:
        xlrderror = getattr(getattr(__import__("xlrd"), "biffh"), "XLRDError")

        with TemporaryDirectory() as tmp_dir:
            xls_path = Path(tmp_dir) / "sample.xls"
            xls_path.write_bytes(b"not-biff")

            with patch(
                "app.document_loaders.excel_loader.xlrd.open_workbook",
                side_effect=xlrderror("Unsupported format"),
            ):
                with patch.object(self.loader, "_is_xml_spreadsheet", return_value=False):
                    with patch.object(self.loader, "_load_workbook", return_value="[SHEET 호스트]\n6101 | 1000"):
                        extracted = self.loader.load(xls_path)

        self.assertIn("[SHEET 호스트]", extracted.raw_text)
        self.assertIn("6101 | 1000", extracted.raw_text)

    def test_build_task_payload_normalizes_legacy_xls_rows_for_imlife_style_table(self) -> None:
        rows = [
            ("", "", "", "", "설정해지금액통보", "", "", "", "", "", "", "", "", "", ""),
            ("", "", "수신 : ", "호스트", "", "", "", "", "", "", "", "", "", "", ""),
            ("", "", "일자 : ", "2025-08-26", "", "", "", "", "", "", "", "", "", "(단위 : 원)", ""),
            ("", "", "구  분", "", "", "펀 드 명", "", "", "", "", "", "", "", "금  액", ""),
            ("", "", "설정", "", "", "A01450", "", "VA글로벌AI플랫폼액티브형[호스트]", "", "", "", "", "", 81393887.0, ""),
            ("", "", "", "", "", "A01550", "", "VA글로벌AI플랫폼밸런스형[호스트]", "", "", "", "", "", 668237.0, ""),
            ("", "", "", "", "", "A01650", "", "VA글로벌AI플랫폼세이프형[호스트]", "", "", "", "", "", 66380.0, ""),
            ("", "", "", "", "", "A01950", "", "VA_AI글로벌멀티에셋[호스트]", "", "", "", "", "", 96578445.0, ""),
            ("", "", "소 계", "", "", "", "", "", "", "", "", "", "", 178706949.0, ""),
            ("", "", "해지", "", "", "A01450", "", "VA글로벌AI플랫폼액티브형[호스트]", "", "", "", "", "", 446512197.0, ""),
            ("", "", "", "", "", "A01550", "", "VA글로벌AI플랫폼밸런스형[호스트]", "", "", "", "", "", 4947720.0, ""),
            ("", "", "", "", "", "A01650", "", "VA글로벌AI플랫폼세이프형[호스트]", "", "", "", "", "", 1791926.0, ""),
            ("", "", "", "", "", "A01950", "", "VA_AI글로벌멀티에셋[호스트]", "", "", "", "", "", 51531801.0, ""),
            ("", "", "소 계", "", "", "", "", "", "", "", "", "", "", 504783644.0, ""),
            ("", "", "", "", "", "", "(주)iM라이프생명보험\n변액운용부", "", "", "", "", "", "", "", ""),
            ("", "", "", "", "", "", "", "", "", "1/1", "", "", "", "", ""),
        ]

        fake_sheet = MagicMock()
        fake_sheet.visibility = 0
        fake_sheet.nrows = len(rows)
        fake_sheet.ncols = len(rows[0])
        fake_sheet.rowinfo_map = {}
        fake_sheet.colinfo_map = {}
        fake_sheet.cell_type.side_effect = lambda row, col: 1 if rows[row][col] != "" else 0
        fake_sheet.cell_value.side_effect = lambda row, col: rows[row][col]

        fake_workbook = MagicMock()
        fake_workbook.datemode = 0
        fake_workbook.sheet_names.return_value = ["Sheet1"]
        fake_workbook.sheet_by_name.return_value = fake_sheet

        with TemporaryDirectory() as tmp_dir:
            xls_path = Path(tmp_dir) / "imlife.xls"
            xls_path.write_bytes(b"A" * 32)

            with patch("app.document_loaders.excel_loader.xlrd.open_workbook", return_value=fake_workbook):
                payload = self.loader.build_task_payload(xls_path, chunk_size_chars=12000)

        self.assertIsNone(payload.non_instruction_reason)
        self.assertEqual(payload.expected_order_count, 8)
        self.assertIn("[SHEET Sheet1]", payload.raw_text)
        self.assertIn("설정 |  |  | A01450", payload.raw_text)
        self.assertIn("81393887", payload.raw_text)
        self.assertNotIn("81393887.0 |", payload.raw_text)
        self.assertNotIn("(주)iM라이프생명보험\n변액운용부", payload.raw_text)
        self.assertIn("(주)iM라이프생명보험 변액운용부", payload.raw_text)
        self.assertIn("| 구 분 | 펀 드 명 | 금 액 |", payload.markdown_text)
        self.assertIn("| 설정 | A01450 | 81393887 |", payload.markdown_text)
        self.assertIn("| 해지 | A01450 | 446512197 |", payload.markdown_text)

    def test_load_mht_does_not_trim_html_body_on_reply_like_markers(self) -> None:
        valid_html = """<html><body>
        <div>--------- Original Message ---------</div>
        <div>Sender : system@example.com</div>
        <div>Date : 2026-03-16 09:00</div>
        <div>Title : 자금운용 해지</div>
        <table>
          <tr><th>펀드코드</th><th>펀드명</th><th>해지금액</th></tr>
          <tr><td>5440</td><td>삼성혼합형</td><td>12,345,678</td></tr>
        </table>
        </body></html>"""
        raw_mht = (
            b"MIME-Version: 1.0\r\n"
            b"Content-Type: multipart/related; boundary=\"BOUNDARY\"\r\n"
            b"\r\n"
            b"--BOUNDARY\r\n"
            b"Content-Type: text/html; charset=\"utf-8\"\r\n"
            b"\r\n"
            b"<html><body></body></html>\r\n"
            b"--BOUNDARY\r\n"
            b"Content-Type: text/html; charset=\"utf-8\"\r\n"
            b"\r\n"
            + valid_html.encode("utf-8")
            + b"\r\n"
            b"--BOUNDARY--\r\n"
        )

        with TemporaryDirectory() as tmp_dir:
            mht_path = Path(tmp_dir) / "reply-like-body.mht"
            mht_path.write_bytes(raw_mht)

            extracted = self.loader.load(mht_path)

        self.assertIn("[MHT reply-like-body.mht]", extracted.raw_text)
        self.assertIn("Original Message", extracted.raw_text)
        self.assertIn("5440 | 삼성혼합형 | 12,345,678", extracted.raw_text)
        self.assertIn("## MHT reply-like-body.mht", extracted.markdown_text)

    def test_mht_body_scoring_does_not_penalize_reply_like_markers(self) -> None:
        body_lines = [
            "BUY & SELL REPORT",
            "From: system@example.com",
            "Subject: 운용 보고",
            "11-28-2025 | 151128 | Alpha | 1,000 | 2,000",
        ]

        penalized = self.loader._score_eml_body_lines(
            body_lines,
            subtype="html",
            apply_quoted_thread_penalty=True,
        )
        unpenalized = self.loader._score_eml_body_lines(
            body_lines,
            subtype="html",
            apply_quoted_thread_penalty=False,
        )

        self.assertGreater(unpenalized, penalized)

    def test_looks_like_no_order_document_recognizes_no_instruction_marker(self) -> None:
        raw_text = (
            "[MHT sample.mht]\n\n"
            "수신 : 삼성자산운용\n"
            "설정지시서\n"
            "2026-03-16\n\n"
            "수탁사 / 펀드코드 | 교보생명 / 펀드코드 | 펀드명 | 금액 ( 원 )\n"
            " |  | - 자금설정해지 지시없음 - | 0\n"
        )

        self.assertTrue(self.loader.looks_like_no_order_document(raw_text))

    def test_looks_like_non_instruction_document_recognizes_notice_title_without_order_evidence(self) -> None:
        raw_text = (
            "[SHEET Sheet1]\n"
            "설정해지금액통보\n"
            "수신 : 호스트\n"
            "안내사항 : 첨부 금액을 참고하시기 바랍니다.\n"
        )

        reason = self.loader.looks_like_non_instruction_document(raw_text)

        self.assertIsNotNone(reason)
        self.assertIn("통보", reason)

    def test_looks_like_non_instruction_document_does_not_reject_notice_title_with_real_order_rows(self) -> None:
        raw_text = (
            "[SHEET Sheet1]\n"
            "설정해지금액통보\n"
            "수신 : 호스트\n"
            "구 분 | 펀 드 명 | 금 액\n"
            "설정 | A01450 | 81,393,887\n"
            "해지 | A01450 | 446,512,197\n"
        )

        reason = self.loader.looks_like_non_instruction_document(raw_text)

        self.assertIsNone(reason)

    def test_looks_like_non_instruction_document_recognizes_cover_email_without_order_body(self) -> None:
        raw_text = (
            "[EML test.eml]\n"
            "Subject: [iM라이프] 설정해지서 송부\n"
            "From: a@example.com\n"
            "To: b@example.com\n"
            "Date: Wed, 18 Mar 2026 08:36:48 +0000\n\n"
            "안녕하세요. iM라이프입니다.\n"
            "첨부파일과 같이 펀드설정해지서를 송부드리니 업무에 참고하시기 바랍니다.\n"
            "감사합니다.\n"
        )

        reason = self.loader.looks_like_non_instruction_document(raw_text)

        self.assertIsNotNone(reason)
        self.assertIn("메일 안내문", reason)

    def test_load_eml_adds_seoul_date_header_for_utc_date(self) -> None:
        message = EmailMessage()
        message["Subject"] = "[흥국생명] 설정해지 내역 운용지시건-삼성"
        message["From"] = "a@example.com"
        message["To"] = "b@example.com"
        message["Date"] = "Sun, 12 Apr 2026 23:41:44 +0000"
        message.set_content("펀드코드 | 추가설정금액 | 당일인출금액 | 해지신청 | 비고\n450038 | 0.4억 | / | / | /")

        with TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "sample.eml"
            path.write_bytes(message.as_bytes())

            raw_text = self.loader._load_eml(path)

        self.assertIn("Date: Sun, 12 Apr 2026 23:41:44 +0000", raw_text)
        self.assertIn("Date (Asia/Seoul): 2026-04-13", raw_text)

    def test_looks_like_non_instruction_document_recognizes_short_attachment_wrapper_with_instruction_word(self) -> None:
        raw_text = (
            "[EML email.eml]\n"
            "Subject: 동양생명 변액자금 2026.0403[삼성액티브자산운용/00001]\n"
            "From: a@example.com\n"
            "To: b@example.com\n"
            "Date: Fri, 03 Apr 2026 10:53:51 +0000\n\n"
            "본 메일에는 운용지시서 내역이 첨부로 되어 있습니다.\n"
            "받기\n"
            "Mac용 Outlook\n"
        )

        reason = self.loader.looks_like_non_instruction_document(raw_text)

        self.assertIsNotNone(reason)
        self.assertIn("메일 안내문", reason)

    def test_looks_like_non_instruction_document_recognizes_short_cover_note_without_attachment_phrase(self) -> None:
        raw_text = (
            "[EML email.eml]\n"
            "Subject: 한화생명 특별계정사업부 실적배당형 펀드별 설정 및 해지 내역서_4차\n"
            "From: a@example.com\n"
            "To: b@example.com\n"
            "Date: Fri, 03 Apr 2026 10:55:11 +0000\n\n"
            "[한화생명] 특별계정사업부\n"
            "To. 삼성자산운용\n"
            "2025년 08월 26일 펀드별 자금운용현황 입니다.\n"
            "From. 한화생명 특별계정사업부\n"
        )

        reason = self.loader.looks_like_non_instruction_document(raw_text)

        self.assertIsNotNone(reason)
        self.assertIn("메일 안내문", reason)

    def test_looks_like_non_instruction_document_does_not_reject_real_instruction_document(self) -> None:
        raw_text = (
            "[SHEET Sheet1]\n"
            "운용지시서\n"
            "2025-11-28\n"
            "펀드명 | 운용사 | 수탁코드 | 구분 | 내용 | 금액 | D+1(예상금액)\n"
            "액티브배당성장70혼합형 | 삼성액티브 자산운용 | 492007 | 입금 | 보험료입금 | 1605698 | 41258553\n"
        )

        reason = self.loader.looks_like_non_instruction_document(raw_text)

        self.assertIsNone(reason)

    def test_build_task_payload_prefers_allow_empty_result_over_non_instruction_reason(self) -> None:
        message = EmailMessage()
        message["Subject"] = "설정지시서"
        message["From"] = "a@example.com"
        message["To"] = "b@example.com"
        message["Date"] = "Wed, 18 Mar 2026 08:36:48 +0000"
        message.set_content("- 자금설정해지 지시없음 -")

        with TemporaryDirectory() as tmp_dir:
            email_path = Path(tmp_dir) / "no_order.eml"
            email_path.write_bytes(message.as_bytes())

            payload = self.loader.build_task_payload(email_path, chunk_size_chars=1000)

        self.assertTrue(payload.allow_empty_result)
        self.assertIsNone(payload.non_instruction_reason)

    def test_build_task_payload_counts_metlife_additional_subscription_email_as_one_order(self) -> None:
        if not METLIFE_ADDITIONAL_SUB_PATH.exists():
            self.skipTest(f"missing actual MetLife fixture: {METLIFE_ADDITIONAL_SUB_PATH}")

        payload = self.loader.build_task_payload(METLIFE_ADDITIONAL_SUB_PATH, chunk_size_chars=12000)

        self.assertEqual(payload.expected_order_count, 1)
        self.assertIsNone(payload.non_instruction_reason)
        self.assertFalse(payload.allow_empty_result)
        self.assertIn("| / | MyFund Vul 혼합안정형 (주식)-삼성 |", payload.markdown_text)
        self.assertIn("| 현금 | 12,082,790 |", payload.markdown_text)

    def test_build_task_payload_counts_metlife_additional_redemption_email_as_one_order(self) -> None:
        if not METLIFE_ADDITIONAL_RED_PATH.exists():
            self.skipTest(f"missing actual MetLife fixture: {METLIFE_ADDITIONAL_RED_PATH}")

        payload = self.loader.build_task_payload(METLIFE_ADDITIONAL_RED_PATH, chunk_size_chars=12000)

        self.assertEqual(payload.expected_order_count, 1)
        self.assertIsNone(payload.non_instruction_reason)
        self.assertFalse(payload.allow_empty_result)
        self.assertIn("| / | MyFund Vul 혼합안정형 (주식)-삼성 |", payload.markdown_text)
        self.assertIn("| 현금 | -23,182,592 |", payload.markdown_text)

    def test_load_eml_prefers_plain_body_when_html_preview_is_weaker(self) -> None:
        message = EmailMessage()
        message["Subject"] = "plain body wins"
        message.set_content(
            "BUY & SELL REPORT\n"
            "\n"
            "Date\n"
            "Buy&Sell\n"
            "External Fund Manager\n"
            "Fund Code\n"
            "Fund Name\n"
            "Fund Price\n"
            "Buy\n"
            "\n"
            "Sell\n"
            "\n"
            "Custodian Bank\n"
            "\n"
            "11-28-2025\n"
            "Buy&Sell\n"
            "삼성액티브\n"
            "151128\n"
            "AIA VUL Alpha\n"
            "1785.89\n"
            "1,311,285\n"
            "\n"
            "55,901,379\n"
            "\n"
            "하나은행\n"
        )
        message.add_alternative(
            """<html><body>
            <div>안녕하세요.</div>
            <div>메일 미리보기입니다.</div>
            </body></html>""",
            subtype="html",
        )

        with TemporaryDirectory() as tmp_dir:
            eml_path = Path(tmp_dir) / "prefer-plain.eml"
            eml_path.write_bytes(message.as_bytes())

            extracted = self.loader.load(eml_path)

        self.assertIn("BUY & SELL REPORT", extracted.raw_text)
        self.assertIn("Date | Buy&Sell | External Fund Manager", extracted.raw_text)
        self.assertIn("11-28-2025 | Buy&Sell | 삼성액티브 | 151128 | AIA VUL Alpha", extracted.raw_text)
        self.assertNotIn("메일 미리보기입니다.", extracted.raw_text)

    def test_load_eml_html_part_handles_wrong_declared_charset(self) -> None:
        html = """<html><body>
        <div>기준일 : 20251128</div>
        <table>
          <tr><th>펀드코드</th><th>펀드명</th><th>설정금액</th></tr>
          <tr><td>151128</td><td>주식형(삼성)</td><td>1,311,285</td></tr>
        </table>
        </body></html>"""
        html_bytes = html.encode("cp949")
        raw_eml = (
            b"Subject: charset fallback\r\n"
            b"MIME-Version: 1.0\r\n"
            b"Content-Type: text/html; charset=utf-8\r\n"
            b"Content-Transfer-Encoding: base64\r\n"
            b"\r\n"
            + base64.b64encode(html_bytes)
            + b"\r\n"
        )

        with TemporaryDirectory() as tmp_dir:
            eml_path = Path(tmp_dir) / "wrong-charset.eml"
            eml_path.write_bytes(raw_eml)

            extracted = self.loader.load(eml_path)

        self.assertIn("기준일 : 20251128", extracted.raw_text)
        self.assertIn("151128 | 주식형(삼성) | 1,311,285", extracted.raw_text)

    def test_load_eml_plain_only_buy_sell_report_is_restructured_as_table(self) -> None:
        plain_body = (
            "BUY & SELL REPORT\n"
            "\n"
            "AIA생명보험\n"
            "\n"
            "Date\n"
            "Buy&Sell\n"
            "External Fund Manager\n"
            "Fund Code\n"
            "Fund Name\n"
            "Fund Price\n"
            "Buy\n"
            "\n"
            "Sell\n"
            "\n"
            "Custodian Bank\n"
            "\n"
            "\n"
            "\n"
            "\n"
            "\n"
            "\n"
            "Amount\n"
            "Unit\n"
            "Amount\n"
            "Unit\n"
            "\n"
            "11-28-2025\n"
            "Buy&Sell\n"
            "삼성액티브\n"
            "151128\n"
            "AIA VUL Alpha\n"
            "1785.89\n"
            "1,311,285\n"
            "\n"
            "55,901,379\n"
            "\n"
            "\n"
            "\n"
            "삼성액티브\n"
            "151161\n"
            "AIA VUL Beta\n"
            "2588.67\n"
            "3,597,188\n"
            "\n"
            "9,770,019\n"
            "\n"
            "\n"
            "\n"
            "Total\n"
        )
        message = EmailMessage()
        message["Subject"] = "plain only buy sell"
        message.set_content(plain_body, charset="utf-8")

        with TemporaryDirectory() as tmp_dir:
            eml_path = Path(tmp_dir) / "plain-only.eml"
            eml_path.write_bytes(message.as_bytes())

            extracted = self.loader.load(eml_path)

        self.assertIn("BUY & SELL REPORT", extracted.raw_text)
        self.assertIn("AIA생명보험", extracted.raw_text)
        self.assertIn("Date | Buy&Sell | External Fund Manager | Fund Code | Fund Name | Fund Price | Buy |  | Sell |  | Custodian Bank", extracted.raw_text)
        self.assertIn("11-28-2025 | Buy&Sell | 삼성액티브 | 151128 | AIA VUL Alpha | 1785.89 | 1,311,285 |  | 55,901,379 |  |", extracted.raw_text)
        self.assertIn(" |  | 삼성액티브 | 151161 | AIA VUL Beta | 2588.67 | 3,597,188 |  | 9,770,019 |  |", extracted.raw_text)

    def test_load_eml_plain_only_buy_sell_report_handles_trimmed_trailing_blanks(self) -> None:
        plain_body = (
            "BUY & SELL REPORT\n"
            "AIA생명보험\n"
            "Date\n"
            "Buy&Sell\n"
            "External Fund Manager\n"
            "Fund Code\n"
            "Fund Name\n"
            "Fund Price\n"
            "Buy\n"
            "Sell\n"
            "Custodian Bank\n"
            "Amount\n"
            "Unit\n"
            "Amount\n"
            "Unit\n"
            "11-28-2025\n"
            "Buy&Sell\n"
            "삼성액티브\n"
            "151128\n"
            "AIA VUL Alpha\n"
            "1785.89\n"
            "1,311,285\n"
            "55,901,379\n"
            "11-29-2025\n"
            "Buy&Sell\n"
            "삼성액티브\n"
            "151161\n"
            "AIA VUL Beta\n"
            "2588.67\n"
            "3,597,188\n"
            "9,770,019\n"
            "Total\n"
        )
        message = EmailMessage()
        message["Subject"] = "plain trimmed rows"
        message.set_content(plain_body, charset="utf-8")

        with TemporaryDirectory() as tmp_dir:
            eml_path = Path(tmp_dir) / "plain-trimmed.eml"
            eml_path.write_bytes(message.as_bytes())

            extracted = self.loader.load(eml_path)

        self.assertIn("11-28-2025 | Buy&Sell | 삼성액티브 | 151128 | AIA VUL Alpha | 1785.89 | 1,311,285 |  | 55,901,379 |  |", extracted.raw_text)
        self.assertIn("11-29-2025 | Buy&Sell | 삼성액티브 | 151161 | AIA VUL Beta | 2588.67 | 3,597,188 |  | 9,770,019 |  |", extracted.raw_text)

    def test_load_eml_prefers_richer_html_body_over_short_preview(self) -> None:
        raw_eml = (
            b"Subject: multi html\r\n"
            b"MIME-Version: 1.0\r\n"
            b"Content-Type: multipart/alternative; boundary=\"ALT\"\r\n"
            b"\r\n"
            b"--ALT\r\n"
            b"Content-Type: text/html; charset=utf-8\r\n"
            b"\r\n"
            b"<html><body><div>preview</div></body></html>\r\n"
            b"--ALT\r\n"
            b"Content-Type: text/html; charset=utf-8\r\n"
            b"\r\n"
            b"<html><body><div>BUY &amp; SELL REPORT</div><table><tr><th>Date</th><th>Fund Code</th><th>Fund Name</th><th>Buy</th><th>Sell</th></tr><tr><td>11-28-2025</td><td>151128</td><td>AIA VUL Alpha</td><td>1,311,285</td><td>55,901,379</td></tr></table></body></html>\r\n"
            b"--ALT--\r\n"
        )

        with TemporaryDirectory() as tmp_dir:
            eml_path = Path(tmp_dir) / "multi-html.eml"
            eml_path.write_bytes(raw_eml)

            extracted = self.loader.load(eml_path)

        self.assertIn("BUY & SELL REPORT", extracted.raw_text)
        self.assertIn("11-28-2025 | 151128 | AIA VUL Alpha | 1,311,285 | 55,901,379", extracted.raw_text)
        self.assertNotIn("preview\n", extracted.raw_text)

    def test_load_eml_falls_back_to_plain_when_html_has_no_extractable_text(self) -> None:
        message = EmailMessage()
        message["Subject"] = "html parse fallback"
        message.set_content(
            "BUY & SELL REPORT\n"
            "AIA생명보험\n"
            "Date\n"
            "Buy&Sell\n"
            "External Fund Manager\n"
            "Fund Code\n"
            "Fund Name\n"
            "Fund Price\n"
            "Buy\n"
            "Sell\n"
            "Custodian Bank\n"
            "Amount\n"
            "Unit\n"
            "Amount\n"
            "Unit\n"
            "11-28-2025\n"
            "Buy&Sell\n"
            "삼성액티브\n"
            "151128\n"
            "AIA VUL Alpha\n"
            "1785.89\n"
            "1,311,285\n"
            "55,901,379\n"
            "Total\n",
            charset="utf-8",
        )
        message.add_alternative("<html><body><img src='cid:logo'/></body></html>", subtype="html")

        with TemporaryDirectory() as tmp_dir:
            eml_path = Path(tmp_dir) / "html-empty.eml"
            eml_path.write_bytes(message.as_bytes())

            extracted = self.loader.load(eml_path)

        self.assertIn("BUY & SELL REPORT", extracted.raw_text)
        self.assertIn("11-28-2025 | Buy&Sell | 삼성액티브 | 151128 | AIA VUL Alpha | 1785.89 | 1,311,285 |  | 55,901,379 |  |", extracted.raw_text)

    def test_load_eml_prefers_actual_report_over_large_quoted_html_thread(self) -> None:
        quoted_html = (
            "<html><body>"
            "<div>Forwarded message</div>"
            "<div>From: somebody@example.com</div>"
            "<div>Subject: previous thread</div>"
            "<table>"
            + "".join(f"<tr><td>history {index}</td><td>value {index}</td></tr>" for index in range(40))
            + "</table>"
            "</body></html>"
        )
        actual_report_html = (
            "<html><body>"
            "<div>BUY &amp; SELL REPORT</div>"
            "<table><tr><th>Date</th><th>Fund Code</th><th>Fund Name</th><th>Buy</th><th>Sell</th></tr>"
            "<tr><td>11-28-2025</td><td>151128</td><td>AIA VUL Alpha</td><td>1,311,285</td><td>55,901,379</td></tr>"
            "</table>"
            "</body></html>"
        )
        raw_eml = (
            b"Subject: quoted html\r\n"
            b"MIME-Version: 1.0\r\n"
            b"Content-Type: multipart/alternative; boundary=\"ALT\"\r\n"
            b"\r\n"
            b"--ALT\r\n"
            b"Content-Type: text/html; charset=utf-8\r\n"
            b"\r\n"
            + quoted_html.encode("utf-8")
            + b"\r\n"
            b"--ALT\r\n"
            b"Content-Type: text/html; charset=utf-8\r\n"
            b"\r\n"
            + actual_report_html.encode("utf-8")
            + b"\r\n"
            b"--ALT--\r\n"
        )

        with TemporaryDirectory() as tmp_dir:
            eml_path = Path(tmp_dir) / "quoted-thread.eml"
            eml_path.write_bytes(raw_eml)

            extracted = self.loader.load(eml_path)

        self.assertIn("BUY & SELL REPORT", extracted.raw_text)
        self.assertIn("11-28-2025 | 151128 | AIA VUL Alpha | 1,311,285 | 55,901,379", extracted.raw_text)
        self.assertNotIn("history 0", extracted.raw_text)

    def test_load_eml_discards_previous_plain_message_thread(self) -> None:
        plain_body = (
            "BUY & SELL REPORT\n"
            "AIA생명보험\n"
            "Date\n"
            "Buy&Sell\n"
            "External Fund Manager\n"
            "Fund Code\n"
            "Fund Name\n"
            "Fund Price\n"
            "Buy\n"
            "Sell\n"
            "Custodian Bank\n"
            "Amount\n"
            "Unit\n"
            "Amount\n"
            "Unit\n"
            "11-28-2025\n"
            "Buy&Sell\n"
            "삼성액티브\n"
            "151128\n"
            "AIA VUL Alpha\n"
            "1785.89\n"
            "1,311,285\n"
            "55,901,379\n"
            "\n"
            "-----Original Message-----\n"
            "From: old@example.com\n"
            "Sent: Tue, 27 Nov 2025 10:00:00 +0900\n"
            "Subject: previous thread\n"
            "BUY & SELL REPORT\n"
            "11-27-2025\n"
            "Buy&Sell\n"
            "삼성액티브\n"
            "999999\n"
            "Legacy Fund\n"
            "1450.11\n"
            "9,999\n"
            "8,888\n"
        )
        message = EmailMessage()
        message["Subject"] = "trim previous plain thread"
        message.set_content(plain_body, charset="utf-8")

        with TemporaryDirectory() as tmp_dir:
            eml_path = Path(tmp_dir) / "trim-plain-thread.eml"
            eml_path.write_bytes(message.as_bytes())

            extracted = self.loader.load(eml_path)

        self.assertIn("151128 | AIA VUL Alpha", extracted.raw_text)
        self.assertNotIn("999999", extracted.raw_text)
        self.assertNotIn("previous thread", extracted.raw_text)

    def test_load_eml_discards_previous_plain_message_thread_without_separator(self) -> None:
        plain_body = (
            "BUY & SELL REPORT\n"
            "Date\n"
            "Buy&Sell\n"
            "External Fund Manager\n"
            "Fund Code\n"
            "Fund Name\n"
            "Fund Price\n"
            "Buy\n"
            "Sell\n"
            "Custodian Bank\n"
            "Amount\n"
            "Unit\n"
            "Amount\n"
            "Unit\n"
            "11-28-2025\n"
            "Buy&Sell\n"
            "삼성액티브\n"
            "151128\n"
            "AIA VUL Alpha\n"
            "1785.89\n"
            "1,311,285\n"
            "55,901,379\n"
            "From: old@example.com\n"
            "Sent: Tue, 27 Nov 2025 10:00:00 +0900\n"
            "Subject: previous thread\n"
            "11-27-2025\n"
            "Buy&Sell\n"
            "삼성액티브\n"
            "999999\n"
            "Legacy Fund\n"
            "1450.11\n"
            "9,999\n"
            "8,888\n"
        )
        message = EmailMessage()
        message["Subject"] = "trim previous plain thread without separator"
        message.set_content(plain_body, charset="utf-8")

        with TemporaryDirectory() as tmp_dir:
            eml_path = Path(tmp_dir) / "trim-plain-thread-no-separator.eml"
            eml_path.write_bytes(message.as_bytes())

            extracted = self.loader.load(eml_path)

        self.assertIn("151128 | AIA VUL Alpha", extracted.raw_text)
        self.assertNotIn("999999", extracted.raw_text)
        self.assertNotIn("old@example.com", extracted.raw_text)

    def test_load_eml_discards_previous_plain_message_thread_without_separator_with_long_buy_sell_header(self) -> None:
        plain_body = (
            "11-28-2025\n"
            "Buy&Sell\n"
            "삼성액티브\n"
            "151128\n"
            "Alpha Fund\n"
            "1785.89\n"
            "1,311,285\n"
            "55,901,379\n"
            "From: old@example.com\n"
            "Sent: Tue, 27 Nov 2025 10:00:00 +0900\n"
            "Subject: previous thread\n"
            "BUY & SELL REPORT\n"
            "Date\n"
            "Buy&Sell\n"
            "External Fund Manager\n"
            "Fund Code\n"
            "Fund Name\n"
            "Fund Price\n"
            "Buy\n"
            "Sell\n"
            "Custodian Bank\n"
            "Amount\n"
            "Unit\n"
            "Amount\n"
            "Unit\n"
            "11-27-2025\n"
            "Buy&Sell\n"
            "삼성액티브\n"
            "999999\n"
            "Legacy Fund\n"
            "1450.11\n"
            "9,999\n"
            "8,888\n"
        )
        message = EmailMessage()
        message["Subject"] = "trim previous plain thread long buy sell header"
        message.set_content(plain_body, charset="utf-8")

        with TemporaryDirectory() as tmp_dir:
            eml_path = Path(tmp_dir) / "trim-plain-thread-long-buy-sell-header.eml"
            eml_path.write_bytes(message.as_bytes())

            extracted = self.loader.load(eml_path)

        self.assertNotIn("999999", extracted.raw_text)
        self.assertNotIn("Legacy Fund", extracted.raw_text)
        self.assertIn("11-28-2025", extracted.raw_text)

    def test_load_eml_discards_previous_plain_thread_for_original_message_sender_date_title_block(self) -> None:
        plain_body = (
            "11-28-2025 | 151128 | Alpha Fund | 1,000 | 2,000\n"
            "안내드립니다.\n"
            "--------- Original Message ---------\n"
            "Sender : 김병훈 프로 <bhkim@minisoft.co.kr>\n"
            "Date : 2026-02-05 14:35 (GMT+9)\n"
            "Title : Re: Re: [AI포털구축] 변액일임펀드 설정/해지 지시서 자동입력 기능 개발건으로 문의드립니다.\n"
            "11-27-2025 | 999999 | Legacy Fund | 9,999 | 8,888\n"
        )
        message = EmailMessage()
        message["Subject"] = "trim original message sender date title"
        message.set_content(plain_body, charset="utf-8")

        with TemporaryDirectory() as tmp_dir:
            eml_path = Path(tmp_dir) / "trim-original-message-sender-title.eml"
            eml_path.write_bytes(message.as_bytes())

            extracted = self.loader.load(eml_path)

        self.assertIn("11-28-2025 | 151128 | Alpha Fund | 1,000 | 2,000", extracted.raw_text)
        self.assertNotIn("999999", extracted.raw_text)
        self.assertNotIn("김병훈 프로", extracted.raw_text)
        self.assertNotIn("AI포털구축", extracted.raw_text)

    def test_load_eml_keeps_current_plain_metadata_example_lines(self) -> None:
        plain_body = (
            "BUY & SELL REPORT\n"
            "Date\n"
            "Buy&Sell\n"
            "External Fund Manager\n"
            "Fund Code\n"
            "Fund Name\n"
            "Fund Price\n"
            "Buy\n"
            "Sell\n"
            "Custodian Bank\n"
            "Amount\n"
            "Unit\n"
            "Amount\n"
            "Unit\n"
            "11-28-2025\n"
            "Buy&Sell\n"
            "삼성액티브\n"
            "151128\n"
            "AIA VUL Alpha\n"
            "1785.89\n"
            "1,311,285\n"
            "55,901,379\n"
            "안내: 아래 메타데이터 예시를 참고하세요.\n"
            "From: 시스템 생성값\n"
            "Sent: 2025-11-28 09:00\n"
            "Subject: 운용 보고\n"
            "11-29-2025\n"
            "Buy&Sell\n"
            "삼성액티브\n"
            "151161\n"
            "Beta Fund\n"
            "1450.11\n"
            "9,999\n"
            "8,888\n"
        )
        message = EmailMessage()
        message["Subject"] = "keep current plain metadata"
        message.set_content(plain_body, charset="utf-8")

        with TemporaryDirectory() as tmp_dir:
            eml_path = Path(tmp_dir) / "keep-current-plain-metadata.eml"
            eml_path.write_bytes(message.as_bytes())

            extracted = self.loader.load(eml_path)

        self.assertIn("151128 | AIA VUL Alpha", extracted.raw_text)
        self.assertIn("151161 | Beta Fund", extracted.raw_text)
        self.assertIn("From: 시스템 생성값", extracted.raw_text)

    def test_load_eml_keeps_current_plain_metadata_example_with_realistic_header_values(self) -> None:
        plain_body = (
            "11-28-2025 | 151128 | Alpha Fund | 1,000 | 2,000\n"
            "안내: 아래는 메타데이터 예시입니다.\n"
            "From: report@example.com\n"
            "Sent: Tue, 27 Nov 2025 10:00:00 +0900\n"
            "Subject: 운용 보고 예시\n"
            "11-29-2025 | 151161 | Beta Fund | 3,000 | 4,000\n"
        )
        message = EmailMessage()
        message["Subject"] = "keep realistic plain metadata example"
        message.set_content(plain_body, charset="utf-8")

        with TemporaryDirectory() as tmp_dir:
            eml_path = Path(tmp_dir) / "keep-realistic-plain-metadata.eml"
            eml_path.write_bytes(message.as_bytes())

            extracted = self.loader.load(eml_path)

        self.assertIn("11-28-2025 | 151128 | Alpha Fund | 1,000 | 2,000", extracted.raw_text)
        self.assertIn("11-29-2025 | 151161 | Beta Fund | 3,000 | 4,000", extracted.raw_text)
        self.assertIn("From: report@example.com", extracted.raw_text)

    def test_load_eml_discards_previous_html_message_thread(self) -> None:
        html_body = (
            "<html><body>"
            "<div>BUY &amp; SELL REPORT</div>"
            "<table><tr><th>Date</th><th>Fund Code</th><th>Fund Name</th><th>Buy</th><th>Sell</th></tr>"
            "<tr><td>11-28-2025</td><td>151128</td><td>AIA VUL Alpha</td><td>1,311,285</td><td>55,901,379</td></tr>"
            "</table>"
            "<div>-----Original Message-----</div>"
            "<div>From: old@example.com</div>"
            "<div>Subject: previous html thread</div>"
            "<table><tr><th>Date</th><th>Fund Code</th><th>Fund Name</th><th>Buy</th><th>Sell</th></tr>"
            "<tr><td>11-27-2025</td><td>999999</td><td>Legacy Fund</td><td>9,999</td><td>8,888</td></tr>"
            "</table>"
            "</body></html>"
        )
        message = EmailMessage()
        message["Subject"] = "trim previous html thread"
        message.add_alternative(html_body, subtype="html")

        with TemporaryDirectory() as tmp_dir:
            eml_path = Path(tmp_dir) / "trim-html-thread.eml"
            eml_path.write_bytes(message.as_bytes())

            extracted = self.loader.load(eml_path)

        self.assertIn("151128 | AIA VUL Alpha | 1,311,285 | 55,901,379", extracted.raw_text)
        self.assertNotIn("999999", extracted.raw_text)
        self.assertNotIn("old@example.com", extracted.raw_text)

    def test_load_eml_discards_previous_html_reply_header_block_without_separator(self) -> None:
        html_body = (
            "<html><body>"
            "<div>BUY &amp; SELL REPORT</div>"
            "<table><tr><th>Date</th><th>Fund Code</th><th>Fund Name</th><th>Buy</th><th>Sell</th></tr>"
            "<tr><td>11-28-2025</td><td>151128</td><td>AIA VUL Alpha</td><td>1,311,285</td><td>55,901,379</td></tr>"
            "</table>"
            "<div>From: old@example.com</div>"
            "<div>Sent: Tue, 27 Nov 2025 10:00:00 +0900</div>"
            "<div>Subject: previous html thread</div>"
            "<table><tr><th>Date</th><th>Fund Code</th><th>Fund Name</th><th>Buy</th><th>Sell</th></tr>"
            "<tr><td>11-27-2025</td><td>999999</td><td>Legacy Fund</td><td>9,999</td><td>8,888</td></tr>"
            "</table>"
            "</body></html>"
        )
        message = EmailMessage()
        message["Subject"] = "trim previous html reply headers"
        message.add_alternative(html_body, subtype="html")

        with TemporaryDirectory() as tmp_dir:
            eml_path = Path(tmp_dir) / "trim-html-reply-block.eml"
            eml_path.write_bytes(message.as_bytes())

            extracted = self.loader.load(eml_path)

        self.assertIn("151128 | AIA VUL Alpha | 1,311,285 | 55,901,379", extracted.raw_text)
        self.assertNotIn("999999", extracted.raw_text)
        self.assertNotIn("old@example.com", extracted.raw_text)
        self.assertNotIn("previous html thread", extracted.raw_text)

    def test_load_eml_discards_previous_html_original_message_sender_date_title_block(self) -> None:
        html_body = (
            "<html><body>"
            "<table><tr><th>Date</th><th>Fund Code</th><th>Fund Name</th><th>Buy</th><th>Sell</th></tr>"
            "<tr><td>11-28-2025</td><td>151128</td><td>Alpha</td><td>1,000</td><td>2,000</td></tr></table>"
            "<div>안내드립니다.</div>"
            "<div>--------- Original Message ---------</div>"
            "<div>Sender : 김병훈 프로 &lt;bhkim@minisoft.co.kr&gt;</div>"
            "<div>Date : 2026-02-05 14:35 (GMT+9)</div>"
            "<div>Title : Re: Re: [AI포털구축] 변액일임펀드 설정/해지 지시서 자동입력 기능 개발건으로 문의드립니다.</div>"
            "<table><tr><th>Date</th><th>Fund Code</th><th>Fund Name</th><th>Buy</th><th>Sell</th></tr>"
            "<tr><td>11-27-2025</td><td>999999</td><td>Legacy Fund</td><td>9,999</td><td>8,888</td></tr></table>"
            "</body></html>"
        )
        message = EmailMessage()
        message["Subject"] = "trim html original message sender date title"
        message.add_alternative(html_body, subtype="html")

        with TemporaryDirectory() as tmp_dir:
            eml_path = Path(tmp_dir) / "trim-html-original-message-sender-title.eml"
            eml_path.write_bytes(message.as_bytes())

            extracted = self.loader.load(eml_path)

        self.assertIn("11-28-2025 | 151128 | Alpha | 1,000 | 2,000", extracted.raw_text)
        self.assertNotIn("999999", extracted.raw_text)
        self.assertNotIn("김병훈 프로", extracted.raw_text)
        self.assertNotIn("AI포털구축", extracted.raw_text)

    def test_load_eml_keeps_current_html_original_message_example_block(self) -> None:
        html_body = (
            "<html><body>"
            "<div>안내: 아래 문자열은 예시입니다.</div>"
            "<div>--------- Original Message ---------</div>"
            "<div>Sender : sample@example.com</div>"
            "<div>Date : 2026-02-05 14:35 (GMT+9)</div>"
            "<div>Title : example only</div>"
            "<table><tr><th>Date</th><th>Fund Code</th><th>Fund Name</th><th>Buy</th><th>Sell</th></tr>"
            "<tr><td>11-28-2025</td><td>151128</td><td>Alpha</td><td>1,000</td><td>2,000</td></tr></table>"
            "</body></html>"
        )
        message = EmailMessage()
        message["Subject"] = "keep current html original message example"
        message.add_alternative(html_body, subtype="html")

        with TemporaryDirectory() as tmp_dir:
            eml_path = Path(tmp_dir) / "keep-current-html-original-message-example.eml"
            eml_path.write_bytes(message.as_bytes())

            extracted = self.loader.load(eml_path)

        self.assertIn("안내: 아래 문자열은 예시입니다.", extracted.raw_text)
        self.assertIn("11-28-2025 | 151128 | Alpha | 1,000 | 2,000", extracted.raw_text)

    def test_load_eml_keeps_current_html_metadata_example_lines(self) -> None:
        html_body = (
            "<html><body>"
            "<div>BUY &amp; SELL REPORT</div>"
            "<div>아래 메타데이터를 확인하세요.</div>"
            "<div>From: 시스템 생성값</div>"
            "<div>Subject: 운용 보고</div>"
            "<table><tr><th>Date</th><th>Fund Code</th><th>Fund Name</th><th>Buy</th><th>Sell</th></tr>"
            "<tr><td>11-28-2025</td><td>151128</td><td>Alpha</td><td>1,000</td><td>2,000</td></tr>"
            "</table>"
            "</body></html>"
        )
        message = EmailMessage()
        message["Subject"] = "keep metadata example"
        message.add_alternative(html_body, subtype="html")

        with TemporaryDirectory() as tmp_dir:
            eml_path = Path(tmp_dir) / "keep-current-html-metadata.eml"
            eml_path.write_bytes(message.as_bytes())

            extracted = self.loader.load(eml_path)

        self.assertIn("From: 시스템 생성값", extracted.raw_text)
        self.assertIn("Subject: 운용 보고", extracted.raw_text)
        self.assertIn("11-28-2025 | 151128 | Alpha | 1,000 | 2,000", extracted.raw_text)

    def test_load_eml_keeps_current_html_metadata_example_with_second_generic_table(self) -> None:
        html_body = (
            "<html><body>"
            "<div>BUY &amp; SELL REPORT</div>"
            "<table><tr><th>Date</th><th>Fund Code</th></tr>"
            "<tr><td>11-28-2025</td><td>151128</td></tr></table>"
            "<div>안내: 아래는 메타데이터 예시입니다.</div>"
            "<div>From: 시스템 생성값</div>"
            "<div>Subject: 운용 보고</div>"
            "<table><tr><th>Key</th><th>Value</th></tr>"
            "<tr><td>A</td><td>B</td></tr></table>"
            "</body></html>"
        )
        message = EmailMessage()
        message["Subject"] = "keep metadata example with second table"
        message.add_alternative(html_body, subtype="html")

        with TemporaryDirectory() as tmp_dir:
            eml_path = Path(tmp_dir) / "keep-current-html-metadata-two-tables.eml"
            eml_path.write_bytes(message.as_bytes())

            extracted = self.loader.load(eml_path)

        self.assertIn("From: 시스템 생성값", extracted.raw_text)
        self.assertIn("Subject: 운용 보고", extracted.raw_text)
        self.assertIn("11-28-2025 | 151128", extracted.raw_text)
        self.assertIn("Key | Value", extracted.raw_text)

    def test_load_eml_keeps_current_html_metadata_example_with_second_order_table(self) -> None:
        html_body = (
            "<html><body>"
            "<div>BUY &amp; SELL REPORT</div>"
            "<table><tr><th>Date</th><th>Fund Code</th><th>Fund Name</th><th>Buy</th><th>Sell</th></tr>"
            "<tr><td>11-28-2025</td><td>151128</td><td>Alpha</td><td>1,000</td><td>2,000</td></tr></table>"
            "<div>안내: 아래 메타데이터 예시를 참고하세요.</div>"
            "<div>From: 시스템 생성값</div>"
            "<div>Sent: 2025-11-28 09:00</div>"
            "<div>Subject: 운용 보고</div>"
            "<table><tr><th>Date</th><th>Fund Code</th><th>Fund Name</th><th>Buy</th><th>Sell</th></tr>"
            "<tr><td>11-29-2025</td><td>151161</td><td>Beta</td><td>3,000</td><td>4,000</td></tr></table>"
            "</body></html>"
        )
        message = EmailMessage()
        message["Subject"] = "keep current html metadata with order table"
        message.add_alternative(html_body, subtype="html")

        with TemporaryDirectory() as tmp_dir:
            eml_path = Path(tmp_dir) / "keep-current-html-metadata-order-table.eml"
            eml_path.write_bytes(message.as_bytes())

            extracted = self.loader.load(eml_path)

        self.assertIn("From: 시스템 생성값", extracted.raw_text)
        self.assertIn("Subject: 운용 보고", extracted.raw_text)
        self.assertIn("11-28-2025 | 151128 | Alpha | 1,000 | 2,000", extracted.raw_text)
        self.assertIn("11-29-2025 | 151161 | Beta | 3,000 | 4,000", extracted.raw_text)

    def test_load_eml_keeps_current_html_metadata_example_with_realistic_header_values(self) -> None:
        html_body = (
            "<html><body>"
            "<div>BUY &amp; SELL REPORT</div>"
            "<table><tr><th>Date</th><th>Fund Code</th><th>Fund Name</th><th>Buy</th><th>Sell</th></tr>"
            "<tr><td>11-28-2025</td><td>151128</td><td>Alpha</td><td>1,000</td><td>2,000</td></tr></table>"
            "<div>안내: 아래는 메타데이터 예시입니다.</div>"
            "<div>From: report@example.com</div>"
            "<div>Sent: Tue, 27 Nov 2025 10:00:00 +0900</div>"
            "<div>Subject: 운용 보고 예시</div>"
            "<table><tr><th>Date</th><th>Fund Code</th><th>Fund Name</th><th>Buy</th><th>Sell</th></tr>"
            "<tr><td>11-29-2025</td><td>151161</td><td>Beta</td><td>3,000</td><td>4,000</td></tr></table>"
            "</body></html>"
        )
        message = EmailMessage()
        message["Subject"] = "keep realistic html metadata example"
        message.add_alternative(html_body, subtype="html")

        with TemporaryDirectory() as tmp_dir:
            eml_path = Path(tmp_dir) / "keep-realistic-html-metadata.eml"
            eml_path.write_bytes(message.as_bytes())

            extracted = self.loader.load(eml_path)

        self.assertIn("From: report@example.com", extracted.raw_text)
        self.assertIn("11-28-2025 | 151128 | Alpha | 1,000 | 2,000", extracted.raw_text)
        self.assertIn("11-29-2025 | 151161 | Beta | 3,000 | 4,000", extracted.raw_text)

    def test_build_markdown_aliases_korean_business_day_headers(self) -> None:
        raw_text = (
            "[HTML sample.html]\n"
            "펀드코드 | 펀드명 | 익영업일 | 익익영업일 | 제3영업일\n"
            "V2201S | Alpha | 100 | 200 | 300"
        )

        markdown = self.loader.build_markdown(raw_text)

        self.assertIn("익영업일(T+1)", markdown)
        self.assertIn("익익영업일(T+2)", markdown)
        self.assertIn("제3영업일(T+3)", markdown)

    def test_estimate_order_cell_count_counts_settlement_buckets(self) -> None:
        raw_text = (
            "[SHEET Sheet1]\n"
            "펀드 | 펀드명 | 설정금액 | 해지금액 | 설정좌수\n"
            "10205 | Alpha | 100 | -20 | 10\n"
            "11400 | Beta | 0 | -30 | 20"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 3)

    def test_estimate_order_cell_count_counts_name_only_rows_without_fund_code(self) -> None:
        raw_text = (
            "[SHEET Sheet1]\n"
            "펀드명 | 설정금액 | 해지금액 | 설정좌수\n"
            "알파혼합형 | 100 | -20 | 10\n"
            "베타주식형 | 0 | -30 | 20"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 3)

    def test_estimate_order_cell_count_ignores_name_only_subtotal_rows(self) -> None:
        raw_text = (
            "[SHEET Sheet1]\n"
            "펀드명 | 설정금액 | 해지금액 | 설정좌수\n"
            "알파혼합형 | 100 | 0 | 10\n"
            "입금소계 | 100 | 0 | 10\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 1)

    def test_estimate_order_cell_count_handles_name_only_minimal_three_column_table(self) -> None:
        raw_text = (
            "[SHEET Sheet1]\n"
            "펀드명 | 설정금액 | 해지금액\n"
            "알파혼합형 | 100 | 0\n"
            "베타주식형 | 0 | 30\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 2)

    def test_is_likely_data_row_ignores_boolean_marker_rows(self) -> None:
        row = ["", "", "TRUE", "", "", "22673164", "7824196"]

        self.assertFalse(self.loader._is_likely_data_row(row))

    def test_is_likely_data_row_ignores_two_cell_metadata_rows(self) -> None:
        row = ["화면번호 :", "13001"]

        self.assertFalse(self.loader._is_likely_data_row(row))

    def test_is_likely_data_row_accepts_code_only_amount_rows(self) -> None:
        row = ["450038", "0.4억", "/", "/", "/"]

        self.assertTrue(self.loader._is_likely_data_row(row))

    def test_build_markdown_keeps_heungkuk_amount_columns_and_code_only_rows(self) -> None:
        raw_text = (
            "[SHEET Sheet1]\n"
            "펀드코드 | 추가설정금액 | 당일인출금액 | 해지신청 | 비고\n"
            "450038 | 0.4억 | / | / | /\n"
            "450033 | / | 0.3억 | / | 10일 신청건\n"
            "[7개 펀드] | 0.4억 | 0.3억 | / | /\n"
            "7개 펀드 합계 | 0.4억 | 0.3억 | / | /\n"
        )

        markdown = self.loader.build_markdown(raw_text)

        self.assertIn("| 펀드코드 | 추가설정금액 | 당일인출금액 | 비고 |", markdown)
        self.assertIn("| 450038 | 0.4억 | / | / |", markdown)
        self.assertIn("| 450033 | / | 0.3억 | 10일 신청건 |", markdown)
        self.assertNotIn("| [7개 펀드] |", markdown)
        self.assertNotIn("| 7개 펀드 합계 |", markdown)

    def test_order_bucket_keys_for_row_uses_note_date_for_pending_request_amount(self) -> None:
        header = ["펀드코드", "추가설정금액", "당일인출금액", "해지신청", "비고"]
        row = ["450036", "/", "0.8억", "0.5억", "0.8억 > 10일 신청건 0.5억 > 15일 인출예정"]

        bucket_keys = self.loader._order_bucket_keys_for_row(row, header)

        self.assertEqual(bucket_keys, {"T0_RED", "DAY15_RED"})

    def test_estimate_order_cell_count_counts_heungkuk_pending_request_separately(self) -> None:
        raw_text = (
            "[EML sample.eml]\n"
            "펀드코드 | 추가설정금액 | 당일인출금액 | 해지신청 | 비고\n"
            "450033 | / | 0.3억 | / | 10일 신청건\n"
            "450034 | / | 1.8억 | / | 10일 신청건\n"
            "450045 | / | 1.7억 | / | 0.8억>10일 신청건 0.9억 > 당일 신청\n"
            "450036 | / | 0.8억 | 0.5억 | 0.8억 > 10일 신청건 0.5억 > 15일 인출예정\n"
            "450038 | 0.4억 | / | / | /\n"
            "450044 | / | 1.0억 | / | 10일 신청건\n"
            "450037 | / | 0.1억 | / | 10일 신청건\n"
            "450040 | / | 2.7억 | / | 10일 신청건\n"
            "7개 펀드 | 0.4억 | 8.4억 | / | /\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 9)

    def test_extract_target_fund_scope_filters_to_samsung_manager_rows(self) -> None:
        raw_text = (
            "[SHEET Sheet1]\n"
            "운용사명 | 펀드코드 | 펀드명 | 설정금액 | 해지금액\n"
            "삼성자산 | 6104 | 채권형 | 1243750 | 39961885\n"
            "신한BNP | 6105 | 해외혼합형 | 264750 | 4939\n"
            "삼성자산,한투 | 6101 | 혼합성장형 | 25915800 | 5653417\n"
        )

        scope = self.loader.extract_target_fund_scope(raw_text)

        self.assertTrue(scope.manager_column_present)
        self.assertFalse(scope.include_all_funds)
        self.assertEqual(scope.fund_codes, frozenset({"6104", "6101"}))
        self.assertEqual(scope.fund_names, frozenset({"채권형", "혼합성장형"}))

    def test_extract_target_fund_scope_prefers_manager_name_over_manager_code_column(self) -> None:
        raw_text = (
            "[SHEET Sheet1]\n"
            "이체일자 | 상품명 | 펀드명 | 운용사코드 | 운용사명 | 유입금액 | 유출금액 | 증감금액\n"
            "2025-08-26 | 변액종신 | 변액종신아시아혼합형 | D32160 | 삼성자산운용 | 112135 | 1324595 | -1212460\n"
            "2025-08-26 | 변액방카연금 | 차이나혼합형 | D32100 | 삼성자산운용 | 5617925 | 9624766 | -4006841\n"
            "2025-08-26 | 변액연금 | 변액연금채권형 | D32010 | DB자산운용 | 186710 | 45191 | 141519\n"
        )

        scope = self.loader.extract_target_fund_scope(raw_text)
        estimated = self.loader.estimate_order_cell_count(raw_text, target_fund_scope=scope)

        self.assertTrue(scope.manager_column_present)
        self.assertFalse(scope.include_all_funds)
        self.assertEqual(scope.fund_codes, frozenset({"D32160", "D32100"}))
        self.assertEqual(scope.fund_names, frozenset({"변액종신아시아혼합형", "차이나혼합형"}))
        self.assertEqual(estimated, 2)

    def test_extract_target_fund_scope_keeps_all_when_manager_column_absent(self) -> None:
        raw_text = (
            "[SHEET Sheet1]\n"
            "펀드코드 | 펀드명 | 설정금액 | 해지금액\n"
            "6104 | 채권형 | 1243750 | 39961885\n"
        )

        scope = self.loader.extract_target_fund_scope(raw_text)

        self.assertFalse(scope.manager_column_present)
        self.assertTrue(scope.include_all_funds)
        self.assertEqual(scope.fund_codes, frozenset())
        self.assertEqual(scope.fund_names, frozenset())

    def test_extract_target_fund_scope_uses_samsung_manager_header_when_column_absent(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "운용사: 삼성자산운용\n"
            + DocumentLoader.RAW_SECTION_DELIMITER
            + "[SHEET Sheet1]\n"
            "펀드코드 | 펀드명 | 설정금액 | 해지금액\n"
            "6104 | 채권형 | 1243750 | 39961885\n"
        )

        scope = self.loader.extract_target_fund_scope(raw_text)

        self.assertTrue(scope.manager_column_present)
        self.assertFalse(scope.include_all_funds)
        self.assertEqual(scope.fund_codes, frozenset({"6104"}))
        self.assertEqual(scope.fund_names, frozenset({"채권형"}))
        self.assertEqual(scope.canonical_fund_names, frozenset({"채권형"}))

    def test_extract_target_fund_scope_excludes_all_when_non_samsung_manager_header_present(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "운용사명: 한국투신\n"
            + DocumentLoader.RAW_SECTION_DELIMITER
            + "[SHEET Sheet1]\n"
            "펀드코드 | 펀드명 | 설정금액 | 해지금액\n"
            "6104 | 채권형 | 1243750 | 39961885\n"
        )

        scope = self.loader.extract_target_fund_scope(raw_text)
        estimated = self.loader.estimate_order_cell_count(raw_text, target_fund_scope=scope)

        self.assertTrue(scope.manager_column_present)
        self.assertFalse(scope.include_all_funds)
        self.assertEqual(scope.fund_codes, frozenset())
        self.assertEqual(scope.fund_names, frozenset())
        self.assertEqual(scope.canonical_fund_names, frozenset())
        self.assertEqual(estimated, 0)

    def test_extract_target_fund_scope_prefers_richer_later_samsung_header_over_early_preview_header(self) -> None:
        raw_text = (
            "[EML preview]\n"
            "운용사명: 한국투신\n"
            "안내 미리보기\n"
            + DocumentLoader.RAW_SECTION_DELIMITER
            + "[SHEET Sheet1]\n"
            "운용사: 삼성자산운용\n"
            "펀드코드 | 펀드명 | 설정금액 | 해지금액\n"
            "6104 | 채권형 | 1243750 | 39961885\n"
        )

        scope = self.loader.extract_target_fund_scope(raw_text)
        estimated = self.loader.estimate_order_cell_count(raw_text, target_fund_scope=scope)

        self.assertTrue(scope.manager_column_present)
        self.assertFalse(scope.include_all_funds)
        self.assertEqual(scope.fund_codes, frozenset({"6104"}))
        self.assertEqual(estimated, 2)

    def test_extract_target_fund_scope_ignores_reference_table_with_manager_column(self) -> None:
        raw_text = (
            "[SHEET 참고]\n"
            "구분 | 운용사 | 비고\n"
            "안내 | 한국투신 | 메타데이터\n"
            + DocumentLoader.RAW_SECTION_DELIMITER
            + "[SHEET Sheet1]\n"
            "운용사: 삼성자산운용\n"
            "펀드코드 | 펀드명 | 설정금액 | 해지금액\n"
            "6104 | 채권형 | 1243750 | 39961885\n"
        )

        scope = self.loader.extract_target_fund_scope(raw_text)
        estimated = self.loader.estimate_order_cell_count(raw_text, target_fund_scope=scope)

        self.assertTrue(scope.manager_column_present)
        self.assertFalse(scope.include_all_funds)
        self.assertEqual(scope.fund_codes, frozenset({"6104"}))
        self.assertEqual(scope.fund_names, frozenset({"채권형"}))
        self.assertEqual(scope.canonical_fund_names, frozenset({"채권형"}))
        self.assertEqual(estimated, 2)

    def test_extract_target_fund_scope_recognizes_product_name_column(self) -> None:
        raw_text = (
            "[SHEET Sheet1]\n"
            "운용사 | 상품명 | 설정금액 | 해지금액\n"
            "한국투신 | 알파혼합형 | 100 | 0\n"
        )

        scope = self.loader.extract_target_fund_scope(raw_text)
        estimated = self.loader.estimate_order_cell_count(raw_text, target_fund_scope=scope)

        self.assertTrue(scope.manager_column_present)
        self.assertFalse(scope.include_all_funds)
        self.assertEqual(estimated, 0)
        self.assertEqual(scope.canonical_fund_names, frozenset())

    def test_extract_target_fund_scope_ignores_blank_manager_values(self) -> None:
        raw_text = (
            "[SHEET Sheet1]\n"
            "운용사 | 펀드코드 | 펀드명 | 설정금액 | 해지금액\n"
            " | 6104 | 채권형 | 100 | 0\n"
        )

        scope = self.loader.extract_target_fund_scope(raw_text)
        estimated = self.loader.estimate_order_cell_count(raw_text, target_fund_scope=scope)

        self.assertFalse(scope.manager_column_present)
        self.assertTrue(scope.include_all_funds)
        self.assertEqual(estimated, 1)

    def test_extract_target_fund_scope_does_not_let_nontarget_manager_section_block_later_samsung_header(self) -> None:
        raw_text = (
            "[SHEET 참고]\n"
            "운용사 | 상품명 | 설정금액 | 해지금액\n"
            "한국투신 | 알파혼합형 | 100 | 0\n"
            + DocumentLoader.RAW_SECTION_DELIMITER
            + "[SHEET Sheet1]\n"
            "운용사: 삼성자산운용\n"
            "펀드코드 | 펀드명 | 설정금액 | 해지금액\n"
            "6104 | 채권형 | 1243750 | 39961885\n"
        )

        scope = self.loader.extract_target_fund_scope(raw_text)

        self.assertTrue(scope.manager_column_present)
        self.assertFalse(scope.include_all_funds)
        self.assertEqual(scope.fund_codes, frozenset({"6104"}))

    def test_extract_target_fund_scope_merges_manager_column_and_samsung_header_only_sections(self) -> None:
        raw_text = (
            "[SHEET A]\n"
            "운용사 | 펀드코드 | 펀드명 | 설정금액 | 해지금액\n"
            "삼성자산운용 | 6104 | 채권형 | 100 | 0\n"
            + DocumentLoader.RAW_SECTION_DELIMITER
            + "[SHEET B]\n"
            "운용사: 삼성자산운용\n"
            "펀드코드 | 펀드명 | 설정금액 | 해지금액\n"
            "6108 | 유럽주식형 | 200 | 0\n"
        )

        scope = self.loader.extract_target_fund_scope(raw_text)
        estimated = self.loader.estimate_order_cell_count(raw_text, target_fund_scope=scope)

        self.assertTrue(scope.manager_column_present)
        self.assertFalse(scope.include_all_funds)
        self.assertEqual(scope.fund_codes, frozenset({"6104", "6108"}))
        self.assertEqual(scope.fund_names, frozenset({"채권형", "유럽주식형"}))
        self.assertEqual(estimated, 2)

    def test_extract_target_fund_scope_carries_samsung_header_only_section_to_next_table_section(self) -> None:
        raw_text = (
            "[SHEET A]\n"
            "운용사 | 펀드코드 | 펀드명 | 설정금액 | 해지금액\n"
            "삼성자산운용 | 6104 | 채권형 | 100 | 0\n"
            + DocumentLoader.RAW_SECTION_DELIMITER
            + "[SHEET B]\n"
            "운용사: 삼성자산운용\n"
            + DocumentLoader.RAW_SECTION_DELIMITER
            + "[SHEET C]\n"
            "펀드코드 | 펀드명 | 설정금액 | 해지금액\n"
            "6108 | 유럽주식형 | 200 | 0\n"
        )

        scope = self.loader.extract_target_fund_scope(raw_text)
        estimated = self.loader.estimate_order_cell_count(raw_text, target_fund_scope=scope)

        self.assertTrue(scope.manager_column_present)
        self.assertFalse(scope.include_all_funds)
        self.assertEqual(scope.fund_codes, frozenset({"6104", "6108"}))
        self.assertEqual(scope.fund_names, frozenset({"채권형", "유럽주식형"}))
        self.assertEqual(estimated, 2)

    def test_extract_target_fund_scope_does_not_include_non_target_section_before_split_samsung_header(self) -> None:
        raw_text = (
            "[SHEET A]\n"
            "운용사 | 상품명 | 설정금액 | 해지금액\n"
            "한국투신 | 알파혼합형 | 100 | 0\n"
            + DocumentLoader.RAW_SECTION_DELIMITER
            + "[SHEET B]\n"
            "운용사: 삼성자산운용\n"
            + DocumentLoader.RAW_SECTION_DELIMITER
            + "[SHEET C]\n"
            "펀드코드 | 펀드명 | 설정금액 | 해지금액\n"
            "6104 | 채권형 | 200 | 0\n"
        )

        scope = self.loader.extract_target_fund_scope(raw_text)
        estimated = self.loader.estimate_order_cell_count(raw_text, target_fund_scope=scope)

        self.assertTrue(scope.manager_column_present)
        self.assertFalse(scope.include_all_funds)
        self.assertEqual(scope.fund_codes, frozenset({"6104"}))
        self.assertEqual(scope.fund_names, frozenset({"채권형"}))
        self.assertEqual(estimated, 1)

    def test_estimate_order_cell_count_applies_target_scope_and_dedupes_duplicate_sections(self) -> None:
        raw_text = (
            "[SHEET 당일]\n"
            "운용사명 | 펀드코드 | 펀드명 | 설정금액 | 해지금액\n"
            "삼성자산 | 6104 | 채권형 | 1243750 | 39961885\n"
            "신한BNP | 6105 | 해외혼합형 | 264750 | 4939\n"
            + DocumentLoader.RAW_SECTION_DELIMITER
            + "[SHEET 당일(2)]\n"
            "운용사명 | 펀드코드 | 펀드명 | 설정금액 | 해지금액\n"
            "삼성자산 | 6104 | 채권형 | 1243750 | 39961885\n"
        )

        scope = self.loader.extract_target_fund_scope(raw_text)
        estimated = self.loader.estimate_order_cell_count(raw_text, target_fund_scope=scope)

        self.assertEqual(estimated, 2)

    def test_estimate_order_cell_count_ignores_unit_columns_and_summary_rows_for_lina_style_table(self) -> None:
        raw_text = (
            "[SHEET 당일]\n"
            "운용사명 | 펀드코드 | 펀드명 | 수탁사명 | 설정신청좌수 | 설정금액 | 해지금액\n"
            "삼성자산 | 6104 | 채권형 | KB은행 | 718947 | 1243750 | 39961885\n"
            "삼성자산 | 6115 | 삼성그룹주형 | KB은행 | 0 | 146250 | 4485\n"
            "삼성자산 | 삼성자산_합계 |  |  | 1265800 | 2515500 | 106151097\n"
            "삼성자산,한투 | 6101 | 혼합성장형 | KB은행 | 5687732 | 25915800 | 5653417\n"
            "삼성자산,한투 | 삼성자산_한투_합계 |  |  | 5687732 | 25915800 | 5653417\n"
            + DocumentLoader.RAW_SECTION_DELIMITER
            + "[SHEET 당일(2)]\n"
            "운용사명 | 펀드코드 | 펀드명 | 수탁사명 | 설정신청좌수 | 설정금액 | 해지금액\n"
            "삼성자산 | 6115 | 삼성그룹주형 | KB은행 | 0 | 146250 | 4485\n"
            "삼성자산,한투 | 6101 | 혼합성장형 | KB은행 | 5687732 | 25915800 | 5653417\n"
        )

        scope = self.loader.extract_target_fund_scope(raw_text)
        estimated = self.loader.estimate_order_cell_count(raw_text, target_fund_scope=scope)

        self.assertEqual(
            scope.fund_codes,
            frozenset({"6104", "6115", "6101"}),
        )
        self.assertEqual(estimated, 6)

    def test_estimate_order_cell_count_handles_pdf_multi_header_schedule_columns(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "기준일자 : 2025-11-27 수신처 : 삼성자산운용 CUTOFF대상여부 : N\n"
            "통합 / 펀드코드 | 서브 / 펀드코드 |  | 펀드명 |  |  | 운용사 | 입금액 |  | 출금액 |  |  | 당일이체좌수 |  | 당일이체금액 |  | 이체 예정금액 |  |  |  |  |  |\n"
            "|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2025-11-28 |  | 2025-12-01 |  | 2025-12-02 |  | 2025-12-03\n"
            "V0043 |  |  | 글로벌멀티에셋자산배분형 |  |  | 삼성자산운용 | 8,251,635 |  | 16,627,375 |  |  | -5,578,407 |  | -8,375,740 |  | 451,845 |  | -1,858,420 |  | -12,733,690 |  | 1,984,387\n"
            "V0058 |  |  | 글로벌헷지펀드배분형 |  |  | 삼성자산운용 | 23,358,300 |  | 73,058,898 |  |  | -41,809,346 |  | -49,700,598 |  | -15,323,373 |  | 9,178,360 |  | 38,169,802 |  | -166,292,156"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 10)

    def test_build_markdown_keeps_confirmed_and_scheduled_amount_columns_for_kdb_style_table(self) -> None:
        raw_text = (
            "[EML sample.eml]\n"
            "운용지시서 | 운용지시서 | 운용지시서 | 운용지시서 | 운용지시서 | 운용지시서 | 운용지시서 | 운용지시서\n"
            "2025-11-28 | 2025-11-28 | 2025-11-28 | 2025-11-28 | 2025-11-28 | 2025-11-28 | 2025-11-28 | 2025-11-28\n"
            "펀드명 | 운용사 | 수탁코드 | 구분 | 내용 | 금액 | D+1(예상금액) | 당일좌수\n"
            "액티브배당성장70혼합형 | 삼성액티브자산운용 | 492007 | 입금 | 보험료입금 | 1,605,698 | 41,258,553 | 606,688\n"
            "액티브배당성장70혼합형 | 삼성액티브자산운용 | 492007 | 출금 | 출금소계 | - | - | -\n"
            "글로벌 / 자산배분안정형 | 삼성자산운용 | F00432 | 출금 | 기타 | 2,819,320 | - | 1,904,145"
        )

        markdown = self.loader.build_markdown(raw_text)

        self.assertIn("구분", markdown)
        self.assertIn("내용", markdown)
        self.assertIn("금액", markdown)
        self.assertIn("D+1(예상금액)", markdown)
        self.assertIn("2,819,320", markdown)

    def test_build_markdown_propagates_identity_into_kdb_subtotal_rows(self) -> None:
        raw_text = (
            "[SHEET Sheet1]\n"
            "운용지시서 |  |  |  |  |  |  |  |\n"
            "2025-11-28 |  |  |  |  |  |  |  |\n"
            "펀드명 | 운용사 | 수탁코드 | 구분 | 내용 | 금액 | D+1(예상금액) | 당일좌수\n"
            "글로벌 자산배분안정형 | 삼성자산운용 | F00432 | 입금 | 보험료입금 | - | 48,912,891 | -\n"
            " |  |  |  | 입금소계 | - | 48,912,891 | -\n"
            " |  |  | 출금 | 특별계정운용보수 | - | - | -\n"
            " |  |  |  | 기타 | 2,819,320 | - | 1,904,145\n"
            " |  |  |  | 출금소계 | 2,819,320 | - | 1,904,145"
        )

        markdown = self.loader.build_markdown(raw_text)

        self.assertIn("금액", markdown)
        self.assertIn("| 글로벌 자산배분안정형 | 삼성자산운용 | F00432 | 입금 | 입금소계 | - | 48,912,891 |", markdown)
        self.assertIn("| 글로벌 자산배분안정형 | 삼성자산운용 | F00432 | 출금 | 출금소계 | 2,819,320 | - |", markdown)

    def test_build_markdown_renders_flattened_schedule_text_block_as_table(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "퇴직연금(실적배당형) 펀드별 설정/해지 내역\n"
            "기준일자 : 2025-11-27 수신처 : 삼성액티브자산운용 CUTOFF대상여부 : N\n"
            "이체 예정금액\n"
            "통합 서브\n"
            "펀드명 운용사 입금액 출금액 당일이체좌수 당일이체금액\n"
            "펀드코드 펀드코드 2025-11-28 2025-12-01 2025-12-02 2025-12-03\n"
            "DV004 M0402 퇴직혼합형-혼합형 삼성액티브자산운용 0 0 0 -19,267,686 846,032 -121,762,204 0 0\n"
            "DV005 M0501 퇴직 주식형-주식형1 삼성액티브자산운용 0 0 0 0 1,157,261 0 0 0"
        )

        markdown = self.loader.build_markdown(raw_text)
        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertIn("| 통합펀드코드 | 서브펀드코드 | 펀드명 | 운용사 | 당일이체금액 | 2025-11-28 예정금액 | 2025-12-01 예정금액 | 2025-12-02 예정금액 | 2025-12-03 예정금액 |", markdown)
        self.assertIn("| DV004 | M0402 | 퇴직혼합형-혼합형 | 삼성액티브자산운용 | -19,267,686 | 846,032 | -121,762,204 | 0 | 0 |", markdown)
        self.assertEqual(estimated, 4)

    def test_build_markdown_renders_flattened_schedule_row_without_sub_code(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "변액 펀드별 설정/해지 내역\n"
            "기준일자 : 2025-11-27 수신처 : 삼성액티브자산운용 CUTOFF대상여부 : N\n"
            "이체 예정금액\n"
            "통합 서브\n"
            "펀드명 운용사 입금액 출금액 당일이체좌수 당일이체금액\n"
            "펀드코드 펀드코드 2025-11-28 2025-12-01 2025-12-02 2025-12-03\n"
            "V0053 주식형 삼성액티브자산운용 58,812 8,100 20,568 50,712 -10,763 -5,711 0 0"
        )

        markdown = self.loader.build_markdown(raw_text)
        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertIn("| V0053 |  | 주식형 | 삼성액티브자산운용 | 50,712 | -10,763 | -5,711 | 0 | 0 |", markdown)
        self.assertEqual(estimated, 3)

    def test_estimate_order_cell_count_handles_split_row_pdf_orders(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "거래유형 |  | 펀드코드 |  | 사무관리사 | 결제일 | 설정(해지)전좌수 | 설정(해지)좌수 | 펀드납입(인출)금액\n"
            "판매사 |  | 펀드명 |  | 펀드평가사 | 통화코드 | 설정(해지)후좌수 | 설정(해지)금액 | 판매회사분결제액\n"
            "설정 |  | BBC13F |  | 하나펀드서비스 | 2025-11-27 | 18,577,453,950 | 3,406,206 | 7,108,103\n"
            "하나생명 |  | VUL 주식성장형(1형)_SamsungActive |  |  |  | 18,580,860,156 | 7,108,103 | 7,108,103\n"
            "|  | [매매처계] |  |  |  | 18,577,453,950 | 3,406,206 | 7,108,103\n"
            "[수탁사계] |  |  |  |  |  | 18,577,453,950 | 3,406,206 | 7,108,103\n"
            + DocumentLoader.RAW_SECTION_DELIMITER
            + "[PAGE 2]\n"
            "거래유형 |  | 펀드코드 |  | 사무관리사 | 결제일 | 설정(해지)전좌수 | 설정(해지)좌수 | 펀드납입(인출)금액\n"
            "판매사 |  | 펀드명 |  | 펀드평가사 | 통화코드 | 설정(해지)후좌수 | 설정(해지)금액 | 판매회사분결제액\n"
            "해지 |  | BBA72G |  | 하나펀드서비스 | 2025-11-27 | 3,921,557,603 | 651,189 | 1,288,090\n"
            "하나생명 |  | VA 안정성장형 I_SamsungActive |  |  |  | 3,920,906,414 | 1,288,090 | 1,288,090\n"
            "|  | [결제일계] |  |  |  | 3,921,557,603 | 651,189 | 1,288,090\n"
            "[거래종류계] |  |  |  |  |  | 3,921,557,603 | 651,189 | 1,288,090\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 2)

    def test_estimate_order_cell_count_handles_plain_text_pdf_order_sections(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "The order of Subscription and Redemption\n"
            "1. Subscription\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Asia(ex Japan) Index FoF AEE 373,629.5985 27-Nov-25 1,788.74 668,326 HANA\n"
            "AI innovative Theme selection ATE - 27-Nov-25 1,566.73 - CITI\n"
            "2. Redemption\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Bond ETF FoF BEE 185,714,727.6942 27-Nov-25 1,120.91 208,169,495 SCFB\n"
            "TOTAL 186,088,356.2927 208,837,821\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 2)

    def test_estimate_order_cell_count_handles_plain_text_pdf_order_sections_across_pages(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "1. Subscription\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Asia(ex Japan) Index FoF AEE 373,629.5985 27-Nov-25 1,788.74 668,326 HANA\n"
            "2. Redemption\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            + DocumentLoader.RAW_SECTION_DELIMITER
            + "[PAGE 2]\n"
            "2. Redemption\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Bond ETF FoF BEE 185,714,727.6942 27-Nov-25 1,120.91 208,169,495 SCFB\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 2)

    def test_estimate_order_cell_count_handles_plain_text_pdf_names_with_embedded_numbers(self) -> None:
        raw_text = (
            "[PAGE 2]\n"
            "2. Redemption\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Gold ETF FoF 2 GLE 14,110,053.8966 27-Nov-25 3,285.17 46,353,926 SCFB\n"
            "S&P 500 ETF FoF UEE2 3,555,633.6745 27-Nov-25 2,889.84 10,275,212 SCFB\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 2)

    def test_estimate_order_cell_count_handles_plain_text_pdf_korean_sections(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "1. 설정\n"
            "펀드명 코드 날짜 금액\n"
            "알파혼합형 AEE 2025-11-27 668,326\n"
            "2. 해지\n"
            "펀드명 코드 날짜 금액\n"
            "베타주식형 BEE 2025-11-27 208,169,495\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 2)

    def test_estimate_order_cell_count_handles_plain_text_pdf_iso_date_without_nav(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "Subscription\n"
            "Fund Name Code Date Amount\n"
            "Alpha Growth Fund AEE 2025-11-27 668,326\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 1)

    def test_estimate_order_cell_count_ignores_plain_text_pdf_iso_date_nav_only_line(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "Subscription\n"
            "Fund Name Code Date NAV Bank\n"
            "Alpha Growth Fund AEE 2025-11-27 1,200.00 HANA\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 0)

    def test_estimate_order_cell_count_distinguishes_plain_text_pdf_duplicate_amounts_by_tail_context(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "Subscription\n"
            "Fund Name Code Date Amount Bank\n"
            "Alpha Fund AEE 2025-11-27 100,000 HANA\n"
            "Alpha Fund AEE 2025-11-27 100,000 KB\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 2)

    def test_estimate_order_cell_count_handles_short_continuation_row_with_name_and_amount(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "거래유형 | 펀드코드 | 펀드명 | 결제일 | 설정금액\n"
            "설정 | BBC13F |  | 2025-11-27 | 0\n"
            " |  | VUL 주식성장형 |  | 7,108,103\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 1)

    def test_estimate_order_cell_count_ignores_unit_only_value_in_mixed_unit_amount_header(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "거래유형 |  | 펀드코드 |  | 사무관리사 | 결제일 | 설정(해지)전좌수 | 설정(해지)좌수 | 펀드납입(인출)금액\n"
            "판매사 |  | 펀드명 |  | 펀드평가사 | 통화코드 | 설정(해지)후좌수 | 설정(해지)금액 | 판매회사분결제액\n"
            "설정 |  | BBC13F |  | 하나펀드서비스 | 2025-11-27 | 18,577,453,950 | 3,406,206 | 0\n"
            "하나생명 |  | VUL 주식성장형(1형)_SamsungActive |  |  |  | 18,580,860,156 | 0 | 0\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 0)

    def test_estimate_order_cell_count_counts_mixed_header_when_it_is_only_amount_column(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "펀드코드 | 펀드명 | 설정(해지)좌수 / 설정(해지)금액\n"
            "F001 | Alpha | 4,567\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 1)

    def test_estimate_order_cell_count_handles_sparse_split_row_with_mixed_header_amount(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "펀드코드 | 펀드명 | 설정금액 | 설정(해지)좌수 / 설정(해지)금액\n"
            "F001 |  | 0 | 0\n"
            " | Alpha | 0 | 4,567\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 1)

    def test_build_markdown_keeps_sparse_split_row_seed_out_of_header(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "펀드코드 | 펀드명 | 설정금액 | 설정(해지)좌수 / 설정(해지)금액\n"
            "F001 |  | 0 | 0\n"
            " | Alpha | 0 | 4,567\n"
        )

        markdown = self.loader.build_markdown(raw_text)

        self.assertIn("| 펀드코드 | 펀드명 | 설정(해지)좌수 / 설정(해지)금액 |", markdown)
        self.assertIn("| F001 |  | 0 |", markdown)
        self.assertNotIn("펀드코드 / F001", markdown)

    def test_estimate_order_cell_count_prefers_settlement_amount_column(self) -> None:
        raw_text = (
            "[SHEET Sheet1]\n"
            "펀드 | 펀드명 | 설정금액 | 해지금액 | 결제금액\n"
            "10205 | Alpha | 100 | 40 | 60\n"
            "11400 | Beta | 0 | 30 | -30"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 2)

    def test_estimate_order_cell_count_prefers_net_settlement_amount_column(self) -> None:
        raw_text = (
            "[SHEET Sheet1]\n"
            "펀드 | 펀드명 | 설정액 | 해지액 | 정산액\n"
            "BALX00 | Alpha | 100 | 40 | 60\n"
            "BLAX10 | Beta | 0 | 30 | -30"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 2)

    def test_estimate_order_cell_count_splits_rows_without_execution_value_even_if_execution_column_exists(self) -> None:
        raw_text = (
            "[SHEET Sheet1]\n"
            "펀드 | 펀드명 | 설정금액 | 해지금액 | 결제금액\n"
            "F001 | Alpha | 100 | 40 | 60\n"
            "F002 | Beta | 100 | 40 | 0\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 3)

    def test_estimate_order_cell_count_splits_explicit_t0_even_when_future_execution_bucket_exists(self) -> None:
        raw_text = (
            "[SHEET Sheet1]\n"
            "펀드 | 펀드명 | 설정금액 | 해지금액 | T+1일 순유입예상금액\n"
            "F001 | Alpha | 100 | 40 | 60\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 3)

    def test_estimate_order_cell_count_prefers_hanalife_row_context_amount_column(self) -> None:
        raw_text = (
            "[SHEET Sheet1]\n"
            "거래일자 | 거래종류 | 거래유형명 | 펀드코드 | 펀드명 | 설정해지금액 / 최저연금보증해징 | 펀드납입출금액 / 최저적립보증비용 | 판매회사분결제금액 / 최저보장보증비용 | 운용사회사명\n"
            "2026-04-15 | 펀드해지(수탁은행용) | 해지 | BBA933 | VA 이머징프릭스주식성장형 | 5,351 | 5,351 | 5,351 | 삼성자산운용\n"
            "2026-04-15 | 펀드해지(판매사용) | 해지 | BBA933 | VA 이머징프릭스주식성장형 | 5,351 | 5,351 | 5,351 | 삼성자산운용\n"
            "2026-04-15 | 펀드해지(수탁은행용) | 설정 | BBC143 | 글로벌혼합형(1형) | 2,906,740 | 2,906,740 | 2,906,740 | 삼성자산운용\n"
            "2026-04-15 | 펀드해지(판매사용) | 설정 | BBC143 | 글로벌혼합형(1형) | 2,906,740 | 2,906,740 | 2,906,740 | 삼성자산운용\n"
            "2026-04-15 | 펀드해지(수탁은행용) | 설정 | BBC152 | 아태브릭스주식성장형 | 4,128,002 | 4,128,002 | 4,128,002 | 삼성자산운용\n"
            "2026-04-15 | 펀드해지(판매사용) | 설정 | BBC152 | 아태브릭스주식성장형 | 4,128,002 | 4,128,002 | 4,128,002 | 삼성자산운용\n"
            "2026-04-15 | 펀드해지(수탁은행용) | 해지 | BBC170 | 차이나주식성장형 | 689 | 689 | 689 | 삼성자산운용\n"
            "2026-04-15 | 펀드해지(판매사용) | 해지 | BBC170 | 차이나주식성장형 | 689 | 689 | 689 | 삼성자산운용\n"
            "2026-04-15 | 펀드해지(수탁은행용) | 설정 | BBC170 | 차이나주식성장형 | 100,000 | 100,000 | 100,000 | 삼성자산운용\n"
            "2026-04-15 | 펀드해지(판매사용) | 설정 | BBC170 | 차이나주식성장형 | 100,000 | 100,000 | 100,000 | 삼성자산운용\n"
            "2026-04-15 | 펀드해지(수탁은행용) | 해지 | BBC180 | 유럽주식성장형 | 21,514,177 | 21,514,177 | 21,514,177 | 삼성자산운용\n"
            "2026-04-15 | 펀드해지(판매사용) | 해지 | BBC180 | 유럽주식성장형 | 21,514,177 | 21,514,177 | 21,514,177 | 삼성자산운용\n"
            "2026-04-15 | 펀드해지(수탁은행용) | 설정 | BBC180 | 유럽주식성장형 | 22,684,941 | 22,684,941 | 22,684,941 | 삼성자산운용\n"
            "2026-04-15 | 펀드해지(판매사용) | 설정 | BBC180 | 유럽주식성장형 | 22,684,941 | 22,684,941 | 22,684,941 | 삼성자산운용"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 7)

    def test_build_markdown_preserves_blank_line_separated_tables_in_same_section(self) -> None:
        raw_text = (
            "[HTML sample.html]\n"
            "정산내역\n"
            "\n"
            "펀드코드 | 펀드명 | 설정액 | 해지액 | 정산액\n"
            "F001 | Alpha | 100 | -40 | 60\n"
            "\n"
            "펀드코드 | 펀드명 | 설정금액 | 설정금액 | 해지금액 | 해지금액\n"
            "펀드코드 | 펀드명 | 3월19일 | 3월20일 | 3월19일 | 3월20일\n"
            "F001 | Alpha | 10 | 20 | 30 | 40\n"
        )

        markdown = self.loader.build_markdown(raw_text)

        self.assertIn("| 펀드코드 | 펀드명 | 설정액 | 해지액 | 정산액 |", markdown)
        self.assertIn("| 펀드코드 | 펀드명 | 설정금액 / 3월19일 | 설정금액 / 3월20일 | 해지금액 / 3월19일 | 해지금액 / 3월20일 |", markdown)
        self.assertLess(
            markdown.find("| 펀드코드 | 펀드명 | 설정액 | 해지액 | 정산액 |"),
            markdown.find("| 펀드코드 | 펀드명 | 설정금액 / 3월19일 | 설정금액 / 3월20일 | 해지금액 / 3월19일 | 해지금액 / 3월20일 |",),
        )

    def test_estimate_order_cell_count_distinguishes_month_day_schedule_columns(self) -> None:
        raw_text = (
            "[HTML sample.html]\n"
            "예상내역\n"
            "\n"
            "펀드코드 | 펀드명 | 설정금액 | 설정금액 | 해지금액 | 해지금액\n"
            "펀드코드 | 펀드명 | 3월19일 | 3월20일 | 3월19일 | 3월20일\n"
            "F001 | Alpha | 100 | 50 | 40 | 30\n"
            "F002 | Beta | 0 | 0 | 20 | 0\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 5)

    def test_estimate_order_cell_count_splits_setting_and_redemption_rows_when_no_net_column(self) -> None:
        raw_text = (
            "[SHEET Sheet1]\n"
            "구 분 | 펀 드 명 | 금 액\n"
            "설정 | A01450 | 81393887\n"
            "설정 | A01550 | 668237\n"
            "해지 | A01450 | 446512197\n"
            "해지 | A01550 | 4947720\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 4)

    def test_estimate_order_cell_count_splits_buy_and_sell_columns_when_no_net_column(self) -> None:
        raw_text = (
            "[EML sample.eml]\n"
            "BUY & SELL REPORT |  |  |  |  |  |  |  |  |  |\n"
            "Date | Buy&Sell | External Fund Manager | Fund Code | Fund Name | Fund Price | Buy |  | Sell |  | Custodian Bank\n"
            " |  |  |  |  |  | Amount | Unit | Amount | Unit |\n"
            "11-28-2025 | Buy&Sell | 삼성액티브 | 151128 | AIA VUL Alpha | 1785.89 | 1,311,285 |  | 55,901,379 |  | \n"
            " |  | 삼성액티브 | 151161 | AIA VUL Beta | 2588.67 | 3,597,188 |  | 9,770,019 |  | "
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 4)

    def test_estimate_order_cell_count_splits_explicit_setting_and_redemption_labels(self) -> None:
        raw_text = (
            "[SHEET 당일]\n"
            "운용사명 | 펀드코드 | 펀드명 | 설정금액 | 해지금액\n"
            "삼성자산 | 6104 | 채권형 | 1243750 | 39961885\n"
            "삼성자산 | 6108 | 유럽주식형 | 19500 | 66132056\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 4)

    def test_estimate_order_cell_count_dedupes_detail_and_subtotal_rows(self) -> None:
        raw_text = (
            "[EML kdb.eml]\n"
            "운용지시서 | 운용지시서 | 운용지시서 | 운용지시서 | 운용지시서 | 운용지시서 | 운용지시서 | 운용지시서\n"
            "2025-11-28 | 2025-11-28 | 2025-11-28 | 2025-11-28 | 2025-11-28 | 2025-11-28 | 2025-11-28 | 2025-11-28\n"
            "펀드명 | 운용사 | 수탁코드 | 구분 | 내용 | 금액 | D+1(예상금액) | 당일좌수\n"
            "액티브배당성장70혼합형 | 삼성액티브 / 자산운용 | 492007 | 입금 | 보험료입금 | 1,605,698 | 41,258,553 | 606,688\n"
            "액티브배당성장70혼합형 | 삼성액티브 / 자산운용 | 492007 | 입금 | 입금소계 | 1,605,698 | 41,258,553 | 606,688\n"
            "글로벌 / 자산배분안정형 | 삼성자산운용 | F00432 | 입금 | 입금소계 | - | 48,912,891 | -\n"
            "글로벌 / 자산배분안정형 | 삼성자산운용 | F00432 | 출금 | 기타 | 2,819,320 | - | 1,904,145\n"
            "글로벌 / 자산배분안정형 | 삼성자산운용 | F00432 | 출금 | 출금소계 | 2,819,320 | - | 1,904,145\n"
            "글로벌 / 자산배분적극형 | 삼성자산운용 | 822337 | 입금 | 입금소계 | - | 236,310,331 | -\n"
            "글로벌 / 자산배분적극형 | 삼성자산운용 | 822337 | 출금 | 출금소계 | 11,628,301 | - | 8,424,209\n"
            "미국고배당포커스주식형 | 삼성자산운용 | 004434 | 입금 | 보험료입금 | 23,123,105 | 310,859,543 | 21,284,905\n"
            "미국고배당포커스주식형 | 삼성자산운용 | 004434 | 입금 | 입금소계 | 23,123,105 | 310,859,543 | 21,284,905\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 8)

    def test_estimate_order_cell_count_handles_blank_identity_kdb_subtotal_rows(self) -> None:
        raw_text = (
            "[SHEET Sheet1]\n"
            "운용지시서 |  |  |  |  |  |  |  |\n"
            "2025-11-28 |  |  |  |  |  |  |  |\n"
            "펀드명 | 운용사 | 수탁코드 | 구분 | 내용 | 금액 | D+1(예상금액) | 당일좌수\n"
            "액티브배당성장70혼합형 | 삼성액티브 / 자산운용 | 492007 | 입금 | 보험료입금 | 1,605,698 | 41,258,553 | 606,688\n"
            " |  |  |  | 입금소계 | 1,605,698 | 41,258,553 | 606,688\n"
            "글로벌 / 자산배분안정형 | 삼성자산운용 | F00432 | 입금 | 보험료입금 | - | 48,912,891 | -\n"
            " |  |  |  | 입금소계 | - | 48,912,891 | -\n"
            " |  |  | 출금 | 특별계정운용보수 | - | - | -\n"
            " |  |  |  | 기타 | 2,819,320 | - | 1,904,145\n"
            " |  |  |  | 출금소계 | 2,819,320 | - | 1,904,145\n"
            "글로벌 / 자산배분적극형 | 삼성자산운용 | 822337 | 입금 | 보험료입금 | - | 236,310,331 | -\n"
            " |  |  |  | 입금소계 | - | 236,310,331 | -\n"
            " |  |  | 출금 | 특별계정운용보수 | - | - | -\n"
            " |  |  |  | 기타 | 11,628,301 | - | 8,424,209\n"
            " |  |  |  | 출금소계 | 11,628,301 | - | 8,424,209\n"
            "미국고배당포커스주식형 | 삼성자산운용 | 004434 | 입금 | 보험료입금 | 23,123,105 | 310,859,543 | 21,284,905\n"
            " |  |  |  | 입금소계 | 23,123,105 | 310,859,543 | 21,284,905\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 8)

    def test_estimate_order_cell_count_ignores_amount_like_aux_columns_in_identity_key(self) -> None:
        raw_text = (
            "[HTML hanhwa.html]\n"
            "펀드코드 | 펀드명 | 설정(예탁) 및 해지금액 / 투자일임/수익증권 | 설정(예탁) 및 해지금액 / 펀드계 | 보수출금 | 예탁금 / 이용료 | 수탁은행 / 수령금액 | 익영업일(T+1) / 이체예상금액 | 수탁은행 / 판매사\n"
            "V5203F | 변액연금Ⅱ 가치주식형Ⅱ(삼성액티브주식) | 입금 | -47,580,112 | 0 | 0 | -47,580,112 | -55,500,000 | 하나은행\n"
            "V5203F | 변액연금Ⅱ 가치주식형Ⅱ(삼성액티브주식) | 출금 | -47,580,112 |  |  |  |  | 하나은행\n"
        )

        estimated = self.loader.estimate_order_cell_count(raw_text)

        self.assertEqual(estimated, 2)

    @patch("app.document_loaders.pdf_loader.pdfplumber.open")
    def test_load_pdf_uses_pdfplumber_text_extraction(self, open_mock) -> None:
        first_page = MagicMock()
        first_page.extract_tables.return_value = []
        first_page.extract_text.return_value = "첫 페이지 본문"
        second_page = MagicMock()
        second_page.extract_tables.return_value = []
        second_page.extract_text.return_value = "둘째 페이지 본문"
        document = MagicMock()
        document.pages = [first_page, second_page]
        open_mock.return_value.__enter__.return_value = document

        raw_text = self.loader._load_pdf(Path("/tmp/sample.pdf"))

        open_mock.assert_called_once_with(Path("/tmp/sample.pdf"), password=None)
        self.assertIn("[PAGE 1]\n첫 페이지 본문", raw_text)
        self.assertIn("[PAGE 2]\n둘째 페이지 본문", raw_text)

    def test_extract_pdf_page_text_prefers_table_rows_when_available(self) -> None:
        page = MagicMock()
        page.extract_tables.side_effect = [
            [
                [
                    ["Fund Name", "Code", "Amount(KRW)"],
                    ["Asia FoF", "AEE", "668,326"],
                ]
            ],
            [],
            [],
        ]
        page.extract_text.return_value = "flattened text"

        page_text, header_lines, carry_lines = self.loader._extract_pdf_page_text(page)

        self.assertIn("Fund Name | Code | Amount(KRW)", page_text)
        self.assertIn("Asia FoF | AEE | 668,326", page_text)
        self.assertNotIn("flattened text", page_text)
        self.assertEqual(header_lines, ["Fund Name | Code | Amount(KRW)"])
        self.assertIsNone(carry_lines)

    def test_extract_pdf_page_text_prefers_plain_text_when_table_rows_are_fragmented(self) -> None:
        page = MagicMock()
        page.extract_tables.side_effect = [
            [
                [
                    ["Fund Name", "Code", "No. of Unit", "NAV Date", "NAV", "Amount(KRW)", "Bank"],
                    ["A", "sia(ex Japan) Inde", "x FoF", "AEE", "373,629.5985", "27-Nov-25", "1,788.74", "668,326", "HANA"],
                    ["A", "ctive", "solution asset allo", "cation FoF", "ARF", "121,148.7113", "27-Nov-25", "3,210.25", "388,918", "HANA"],
                ]
            ],
            [],
            [],
        ]
        page.extract_text.return_value = (
            "The order of Subscription and Redemption\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Asia(ex Japan) Index FoF AEE 373,629.5985 27-Nov-25 1,788.74 668,326 HANA\n"
            "Active solution asset allocation FoF ARF 121,148.7113 27-Nov-25 3,210.25 388,918 HANA"
        )

        page_text, header_lines, carry_lines = self.loader._extract_pdf_page_text(page)

        self.assertIn("Asia(ex Japan) Index FoF AEE 373,629.5985 27-Nov-25 1,788.74 668,326 HANA", page_text)
        self.assertNotIn("| A | sia(ex Japan) Inde | x FoF |", page_text)
        self.assertIsNone(header_lines)
        self.assertIsNone(carry_lines)

    def test_extract_pdf_page_text_reconstructs_kdb_broken_plain_text_into_pipe_rows(self) -> None:
        page = MagicMock()
        page.extract_tables.side_effect = [[], [], []]
        page.extract_text.return_value = (
            "운용지시서\n"
            "2025-11-28\n"
            "펀드명 운용사 수탁코드구분 내용 금액 D+1(예상금액)당일좌수\n"
            "보험료입금 1,605,698 41,258,553 606,688\n"
            "입금 기타 - - -\n"
            "액티브배당성장70혼 삼성액티브 입금소계 1,605,698 41,258,553 606,688\n"
            "492007\n"
            "합형 자산운용 특별계정운용보수 - - -\n"
            "출금 기타 - - -\n"
            "출금소계 - - -\n"
            "보험료입금 - 48,912,891 -\n"
            "입금 기타 - - -\n"
            "글로벌 입금소계 - 48,912,891 -\n"
            "삼성자산운용 F00432\n"
            "자산배분안정형 특별계정운용보수 - - -\n"
            "출금 기타 2,819,320 - 1,904,145\n"
            "출금소계 2,819,320 - 1,904,145"
        )

        page_text, header_lines, carry_lines = self.loader._extract_pdf_page_text(page)
        estimated = self.loader.estimate_order_cell_count(f"[PAGE 1]\n{page_text}")

        self.assertIn("펀드명 | 운용사 | 수탁코드 | 구분 | 내용 | 금액 | D+1(예상금액)", page_text)
        self.assertIn("492007 | 입금 | 입금소계 | 1,605,698 | 41,258,553", page_text)
        self.assertIn("F00432 | 출금 | 출금소계 | 2,819,320 | -", page_text)
        self.assertEqual(estimated, 4)
        self.assertIsNone(header_lines)
        self.assertIsNone(carry_lines)

    def test_extract_pdf_page_text_repeats_header_after_section_marker(self) -> None:
        page = MagicMock()
        page.extract_tables.side_effect = [
            [
                [
                    ["Fund Name", "Code", "Amount(KRW)"],
                    ["Asia FoF", "AEE", "668,326"],
                    ["2. Redemption", "", ""],
                    ["Asia FoF", "AEE", "145,737"],
                ]
            ],
            [],
            [],
        ]
        page.extract_text.return_value = "The order of Subscription and Redemption"

        page_text, _, carry_lines = self.loader._extract_pdf_page_text(page)

        self.assertIn("2. Redemption", page_text)
        self.assertEqual(page_text.count("Fund Name | Code | Amount(KRW)"), 2)
        self.assertIn("Asia FoF | AEE | 145,737", page_text)
        self.assertIsNone(carry_lines)

    def test_extract_pdf_page_text_carries_plain_section_header_to_next_page(self) -> None:
        first_page = MagicMock()
        first_page.extract_tables.side_effect = [[], [], []]
        first_page.extract_text.return_value = (
            "The order of Subscription and Redemption\n"
            "2. Redemption\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Asia(ex Japan) Index FoF AEE - 27-Nov-25 1,788.74 - SCFB"
        )

        second_page = MagicMock()
        second_page.extract_tables.side_effect = [[], [], []]
        second_page.extract_text.return_value = (
            "Global Theme Active ETF FoF ASE - 27-Nov-25 1,101.80 - SCFB\n"
            "Bond ETF FoF BEE 185,714,727.6942 27-Nov-25 1,120.91 208,169,495 SCFB"
        )

        _, _, carry_lines = self.loader._extract_pdf_page_text(first_page)
        page_text, header_lines, next_carry_lines = self.loader._extract_pdf_page_text(
            second_page,
            previous_plain_carry_lines=carry_lines,
        )

        self.assertTrue(page_text.startswith("2. Redemption\nFund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank"))
        self.assertIn("Bond ETF FoF BEE 185,714,727.6942 27-Nov-25 1,120.91 208,169,495 SCFB", page_text)
        self.assertIsNone(header_lines)
        self.assertEqual(next_carry_lines, ["2. Redemption", "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank"])

    @patch("app.document_loaders.pdf_loader.pdfplumber.open")
    def test_load_pdf_requires_password_for_encrypted_pdf(self, open_mock) -> None:
        open_mock.side_effect = PdfminerException(PDFPasswordIncorrect())

        with self.assertRaisesRegex(ValueError, "Encrypted PDF requires pdf_password."):
            self.loader._load_pdf(Path("/tmp/locked.pdf"))

    @patch("app.document_loaders.pdf_loader.pdfplumber.open")
    def test_load_pdf_rejects_invalid_password_for_encrypted_pdf(self, open_mock) -> None:
        open_mock.side_effect = PdfminerException(PDFPasswordIncorrect())

        with self.assertRaisesRegex(ValueError, "Invalid pdf_password for encrypted PDF."):
            self.loader._load_pdf(Path("/tmp/locked.pdf"), pdf_password="bad-password")


if __name__ == "__main__":
    unittest.main()
