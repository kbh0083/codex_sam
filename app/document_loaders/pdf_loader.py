from __future__ import annotations

import logging
import re
from pathlib import Path

import pdfplumber
from pdfminer.pdfdocument import PDFPasswordIncorrect
from pdfplumber.utils.exceptions import PdfminerException

logger = logging.getLogger(__name__)


class PdfDocumentLoaderMixin:
    """PDF 원문을 raw_text 로 바꾸기 위한 전용 보조 로직.

    PDF는 같은 "텍스트 추출"이라도 문서마다 레이아웃 손상이 크게 다르다.
    그래서 이 로더는 단순 `extract_text()`만 쓰지 않고:
    - 표 감지
    - 헤더 carry-over
    - 표 바깥 문맥 보존
    - 섹션 마커 분리
    를 함께 수행해서 LLM 입력 품질을 최대한 안정화한다.
    """

    PDF_CONTEXT_KEYWORDS = (
        "subscription",
        "redemption",
        "approved",
        "to :",
        "from :",
        "fax",
        "date :",
        "기준일자",
        "수신처",
        "cutoff",
        "대상여부",
        "회계처리",
    )

    def _load_pdf(self, file_path: Path, pdf_password: str | None = None) -> str:
        """pdfplumber 로 페이지별 raw_text 를 추출한다.

        암호화 PDF도 여기서 처리한다. 결과는 페이지 경계를 `[PAGE N]` 으로 남겨서
        후단에서 페이지 단위 검수와 section 분할이 가능하게 한다.
        """
        pages: list[str] = []
        previous_table_header_lines: list[str] | None = None
        previous_plain_carry_lines: list[str] | None = None
        try:
            if pdf_password is not None:
                logger.info("Attempting PDF authentication")
            with pdfplumber.open(file_path, password=pdf_password) as document:
                logger.info("Opened PDF via pdfplumber: pages=%s", len(document.pages))
                if pdf_password is not None:
                    logger.info("PDF authentication succeeded")
                for page_number, page in enumerate(document.pages, start=1):
                    page_text, previous_table_header_lines, previous_plain_carry_lines = self._extract_pdf_page_text(
                        page,
                        previous_table_header_lines=previous_table_header_lines,
                        previous_plain_carry_lines=previous_plain_carry_lines,
                    )
                    if page_text:
                        pages.append(f"[PAGE {page_number}]\n{page_text}")
                logger.info("Extracted text from %s PDF page(s)", len(pages))
        except PdfminerException as exc:
            if self._is_pdf_password_error(exc):
                if pdf_password is None:
                    raise ValueError("Encrypted PDF requires pdf_password.") from exc
                raise ValueError("Invalid pdf_password for encrypted PDF.") from exc
            raise ValueError(f"Failed to extract text from PDF: {exc}") from exc
        if not pages:
            raise ValueError("No extractable text found in PDF. OCR is not included in this service.")
        return self.RAW_SECTION_DELIMITER.join(pages)

    def _extract_pdf_page_text(
        self,
        page: pdfplumber.page.Page,
        previous_table_header_lines: list[str] | None = None,
        previous_plain_carry_lines: list[str] | None = None,
    ) -> tuple[str, list[str] | None, list[str] | None]:
        """페이지 1장을 raw_text 후보 문자열로 복원하고 carry 정보를 함께 돌려준다.

        반환값은 단순한 page text가 아니라,
        - 현재 페이지의 최종 raw_text
        - 다음 페이지로 넘길 table header carry
        - 다음 페이지로 넘길 plain-text carry
        세 가지를 한 번에 제공한다.
        """
        # 표를 감지한 경우에도, 항상 table 기반 raw_text 가 더 좋은 것은 아니다.
        # 카드프처럼 표 추출이 글자를 잘게 쪼개면 오히려 page.extract_text()가
        # 더 읽기 좋은 본문이 된다. 그래서 두 후보를 비교해 더 품질 좋은 쪽을 고른다.
        plain_text = (page.extract_text() or "").strip()
        reconstructed_plain_text = self._reconstruct_kdb_instruction_plain_text(plain_text)
        if reconstructed_plain_text:
            return reconstructed_plain_text, None, None
        table_lines, next_table_header_lines = self._extract_pdf_table_lines(
            page,
            previous_table_header_lines=previous_table_header_lines,
        )
        if table_lines:
            context_lines = self._extract_pdf_context_lines(page, table_lines)
            merged_lines = self._merge_unique_lines(context_lines, table_lines)
            table_text = "\n".join(merged_lines).strip()
            if self._prefer_plain_pdf_text(plain_text, table_text):
                plain_with_carry = self._apply_plain_pdf_carry(plain_text, previous_plain_carry_lines)
                return plain_with_carry, None, self._extract_plain_pdf_carry_lines(plain_with_carry)
            return table_text, next_table_header_lines, None
        plain_with_carry = self._apply_plain_pdf_carry(plain_text, previous_plain_carry_lines)
        return plain_with_carry, None, self._extract_plain_pdf_carry_lines(plain_with_carry)

    def _reconstruct_kdb_instruction_plain_text(self, plain_text: str) -> str | None:
        """KDB생명형 깨진 plain-text PDF를 pipe table로 복원한다.

        이 양식은 pdfplumber plain text에서 한 펀드의 정보가 여러 줄로 갈라져
        `펀드명/운용사/수탁코드`와 `입금소계/출금소계`가 서로 다른 줄에 흩어진다.
        그대로 LLM에 넘기면 T+1 금액과 일부 펀드명이 누락되기 쉬우므로,
        subtotal row 기준으로 핵심 주문 정보만 재조립한다.
        """
        if not plain_text:
            return None

        lines = [line.strip() for line in plain_text.splitlines() if line.strip()]
        header_index = next((index for index, line in enumerate(lines) if self._looks_like_kdb_pdf_header_line(line)), None)
        if header_index is None:
            return None

        body_lines = lines[header_index + 1 :]
        blocks = self._split_kdb_pdf_blocks(body_lines)
        if not blocks:
            return None

        rendered_rows: list[str] = []
        for block in blocks:
            rendered_rows.extend(self._render_kdb_pdf_block(block))
        if not rendered_rows:
            return None

        preamble_lines = lines[:header_index]
        reconstructed_lines = [
            *preamble_lines,
            "펀드명 | 운용사 | 수탁코드 | 구분 | 내용 | 금액 | D+1(예상금액)",
            *rendered_rows,
        ]
        return "\n".join(reconstructed_lines)

    @staticmethod
    def _looks_like_kdb_pdf_header_line(line: str) -> bool:
        """KDB생명 PDF의 특수 원장형 헤더 줄인지 판별한다."""
        normalized = re.sub(r"\s+", "", line).lower()
        return all(
            token in normalized
            for token in (
                "펀드명",
                "운용사",
                "수탁코드",
                "구분",
                "내용",
                "금액",
                "d+1(예상금액)",
                "당일좌수",
            )
        )

    @staticmethod
    def _split_kdb_pdf_blocks(lines: list[str]) -> list[list[str]]:
        """KDB PDF 본문을 `보험료입금` 단위 block으로 나눈다."""
        blocks: list[list[str]] = []
        current: list[str] = []
        for line in lines:
            if line.startswith("보험료입금"):
                if current:
                    blocks.append(current)
                current = [line]
                continue
            if current:
                current.append(line)
        if current:
            blocks.append(current)
        return blocks

    def _render_kdb_pdf_block(self, lines: list[str]) -> list[str]:
        """KDB PDF block 하나를 pipe row 1~2개로 렌더링한다."""
        fund_code = ""
        manager_fragments: list[str] = []
        name_fragments: list[str] = []
        inflow_amount = "-"
        scheduled_amount = "-"
        outflow_amount = "-"

        for line in lines:
            self._collect_kdb_pdf_identity_fragments(
                line=line,
                name_fragments=name_fragments,
                manager_fragments=manager_fragments,
            )

            if not fund_code:
                fund_code = self._extract_kdb_pdf_fund_code(line) or fund_code

            if "입금소계" in line:
                inflow_amount, scheduled_amount = self._extract_kdb_pdf_amount_pair(line, "입금소계")
            elif "출금소계" in line:
                outflow_amount, _ = self._extract_kdb_pdf_amount_pair(line, "출금소계")

        fund_name = self._normalize_kdb_pdf_name(name_fragments)
        manager_name = self._normalize_kdb_pdf_manager(manager_fragments)
        if not fund_code or not fund_name:
            return []

        rows: list[str] = []
        if inflow_amount != "-" or scheduled_amount != "-":
            rows.append(
                f"{fund_name} | {manager_name} | {fund_code} | 입금 | 입금소계 | {inflow_amount} | {scheduled_amount}"
            )
        if outflow_amount != "-":
            rows.append(
                f"{fund_name} | {manager_name} | {fund_code} | 출금 | 출금소계 | {outflow_amount} | -"
            )
        return rows

    def _collect_kdb_pdf_identity_fragments(
        self,
        *,
        line: str,
        name_fragments: list[str],
        manager_fragments: list[str],
    ) -> None:
        """KDB PDF 한 줄에서 펀드명/운용사명 조각을 분리 수집한다."""
        label_patterns = (
            "보험료입금",
            "입금소계",
            "출금소계",
            "특별계정운용보수",
            "입금 기타",
            "출금 기타",
        )
        prefix = line
        for label in label_patterns:
            if label in line:
                prefix = line.split(label, 1)[0].strip()
                break
        if not prefix:
            return

        code_match = re.search(r"\b([A-Z]\d{4,6}|\d{6})\b", prefix)
        if code_match:
            prefix = prefix.replace(code_match.group(1), " ").strip()

        tokens = prefix.split()
        if not tokens:
            return

        manager_start: int | None = None
        for index, token in enumerate(tokens):
            if "삼성" in token:
                manager_start = index
                break
        if manager_start is None:
            for index, token in enumerate(tokens):
                if "자산운용" in token and manager_fragments:
                    manager_start = index
                    break

        if manager_start is None:
            name_part = "".join(tokens).strip()
            if name_part:
                name_fragments.append(name_part)
            return

        name_part = "".join(tokens[:manager_start]).strip()
        manager_part = " ".join(tokens[manager_start:]).strip()
        if name_part:
            name_fragments.append(name_part)
        if manager_part:
            manager_fragments.append(manager_part)

    @staticmethod
    def _extract_kdb_pdf_fund_code(line: str) -> str | None:
        """KDB PDF 한 줄에서 수탁코드/펀드코드 후보를 찾는다.

        어떤 줄은 코드만 단독으로 내려오고, 어떤 줄은 펀드명/운용사 사이에
        inline 으로 섞여 있기 때문에 두 패턴을 모두 본다.
        """
        code_match = re.fullmatch(r"(?:[A-Z]\d{4,6}|\d{6})", line.strip())
        if code_match:
            return code_match.group(0)
        inline_match = re.search(r"\b([A-Z]\d{4,6}|\d{6})\b", line)
        if inline_match:
            return inline_match.group(1)
        return None

    @staticmethod
    def _extract_kdb_pdf_amount_pair(line: str, marker: str) -> tuple[str, str]:
        """`입금소계/출금소계` 뒤의 금액 2개를 `(T0, T+1)` 형태로 분리한다."""
        tail = line.split(marker, 1)[1].strip() if marker in line else ""
        if not tail:
            return "-", "-"
        tokens = re.findall(r"-?\d[\d,]*|-", tail)
        if not tokens:
            return "-", "-"
        first = tokens[0] if len(tokens) >= 1 else "-"
        second = tokens[1] if len(tokens) >= 2 else "-"
        return first, second

    @staticmethod
    def _normalize_kdb_pdf_name(fragments: list[str]) -> str:
        """분할된 펀드명 조각을 다시 하나의 이름으로 합친다."""
        joined = "".join(fragment.strip() for fragment in fragments if fragment.strip())
        joined = re.sub(r"\s+", " ", joined).strip()
        return joined

    @staticmethod
    def _normalize_kdb_pdf_manager(fragments: list[str]) -> str:
        """분할된 운용사명 조각을 dedupe 하면서 사람이 읽는 이름으로 정리한다."""
        if not fragments:
            return ""
        normalized: list[str] = []
        for fragment in fragments:
            text = re.sub(r"\s+", " ", fragment).strip()
            if text and text not in normalized:
                normalized.append(text)
        manager = " ".join(normalized)
        manager = manager.replace("삼성액티브 자산운용", "삼성액티브자산운용")
        return manager.strip()

    def _prefer_plain_pdf_text(self, plain_text: str, table_text: str) -> bool:
        """plain-text 후보와 table 후보 중 어느 쪽을 raw_text로 채택할지 결정한다.

        판단 기준은 단순 문자 수가 아니라 아래 신호의 조합이다.
        - coverage가 살아 있는지
        - 데이터행/헤더가 충분히 보존되는지
        - 금액 컬럼과 좌수 컬럼이 함께 있을 때 table 쪽이 더 구조를 잘 살리는지

        즉 "보기 좋은 텍스트"보다 "후단 LLM/coverage가 더 잘 동작하는 텍스트"를
        우선 선택한다.
        """
        if not plain_text:
            return False
        plain_coverage = 0
        table_coverage = 0
        plain_score = self._score_pdf_text_candidate(plain_text)
        table_score = self._score_pdf_text_candidate(table_text)
        if plain_text:
            plain_coverage = self.estimate_order_cell_count(f"[PAGE]\n{plain_text}")
        if table_text:
            table_coverage = self.estimate_order_cell_count(f"[PAGE]\n{table_text}")
        table_preserves_structured_order_columns = (
            "|" in table_text
            and any(keyword in table_text for keyword in ("설정(해지)금액", "판매회사분결제액", "펀드납입(인출)금액", "결제액"))
            and any(keyword in table_text for keyword in ("전좌수", "후좌수", "좌수"))
        )
        if plain_coverage == 0 and table_coverage > 0 and table_preserves_structured_order_columns:
            return False
        if plain_coverage == 0 and table_coverage > 0 and plain_score[0] <= 0:
            return False
        if table_coverage == 0 and plain_coverage > 0:
            return True
        return plain_score > table_score

    def _apply_plain_pdf_carry(
        self,
        plain_text: str,
        previous_plain_carry_lines: list[str] | None,
    ) -> str:
        """이전 페이지의 plain-text 섹션 헤더를 현재 페이지 선두에 이어 붙인다."""
        if not plain_text or not previous_plain_carry_lines:
            return plain_text

        lines = [line.strip() for line in plain_text.splitlines() if line.strip()]
        if not lines:
            return plain_text
        if self._is_pdf_section_marker_line(lines[0]) or self._looks_like_pdf_table_header_line(lines[0]):
            return plain_text
        if not self._looks_like_pdf_data_line(lines[0]):
            return plain_text

        return "\n".join(previous_plain_carry_lines + lines)

    def _extract_plain_pdf_carry_lines(self, plain_text: str) -> list[str] | None:
        """다음 페이지로 이어 붙일 섹션 헤더/표 헤더를 현재 페이지에서 추출한다."""
        lines = [line.strip() for line in plain_text.splitlines() if line.strip()]
        latest: list[str] | None = None
        for index, line in enumerate(lines):
            if not self._is_pdf_section_marker_line(line):
                continue
            candidate = [line]
            if index + 1 < len(lines) and self._looks_like_pdf_table_header_line(lines[index + 1]):
                candidate.append(lines[index + 1])
            latest = candidate
        return latest

    def _score_pdf_text_candidate(self, text: str) -> tuple[int, int, int, int]:
        """PDF 페이지 텍스트 후보의 품질을 상대 비교용으로 점수화한다."""
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        data_like_lines = sum(1 for line in lines if self._looks_like_pdf_data_line(line))
        header_like_lines = sum(1 for line in lines if self._looks_like_pdf_table_header_line(line))
        fragmented_penalty = sum(self._fragmented_pdf_line_penalty(line) for line in lines)
        quality_score = data_like_lines - fragmented_penalty
        return (quality_score, data_like_lines, header_like_lines, len(lines))

    @staticmethod
    def _looks_like_pdf_table_header_line(line: str) -> bool:
        """plain-text 줄이 표 헤더처럼 보이는지 휴리스틱으로 판별한다."""
        lower_line = line.lower()
        compact_line = re.sub(r"\s+", "", lower_line)
        keyword_hits = sum(
            1
            for keyword in (
                "fund",
                "code",
                "amount",
                "nav",
                "bank",
                "unit",
                "date",
                "펀드",
                "코드",
                "금액",
                "좌수",
                "결제일",
                "기준일",
            )
            if keyword in lower_line or keyword in compact_line
        )
        return keyword_hits >= 3

    @staticmethod
    def _is_pdf_section_marker_line(line: str) -> bool:
        """`1. Subscription`, `2. Redemption` 같은 섹션 마커 줄인지 본다."""
        return bool(re.fullmatch(r"\d+\.\s*(subscription|redemption)", line.strip(), flags=re.IGNORECASE))

    @staticmethod
    def _looks_like_pdf_data_line(line: str) -> bool:
        """plain-text 줄이 날짜+금액을 가진 주문 데이터행인지 대략 판별한다."""
        lower_line = line.lower()
        if re.search(r"\d{1,2}-[a-z]{3}-\d{2}", lower_line) and re.search(r"\d[\d,]*(?:\.\d+)?", lower_line):
            return True
        if re.search(r"\d{4}-\d{2}-\d{2}", lower_line) and re.search(r"\d[\d,]*(?:\.\d+)?", lower_line):
            return True
        return False

    def _fragmented_pdf_line_penalty(self, line: str) -> int:
        """셀 분절이 심한 줄일수록 패널티를 준다.

        PDF 표 추출이 실패하면 한 글자/두 글자 단위 셀이 많이 생기는데,
        이런 후보는 사람이 보기에도 불편하고 후단 파싱 품질도 낮다.
        """
        if " | " not in line:
            return 0
        cells = [cell.strip() for cell in line.split(" | ") if cell.strip()]
        return sum(1 for cell in cells if self._is_fragmented_pdf_cell(cell))

    def _extract_pdf_table_lines(
        self,
        page: pdfplumber.page.Page,
        previous_table_header_lines: list[str] | None = None,
    ) -> tuple[list[str], list[str] | None]:
        """한 페이지의 표를 pipe row 목록으로 뽑고 다음 페이지용 header carry를 계산한다."""
        # 한 페이지 안에 여러 표가 있을 수 있고, 다음 페이지로 이어지는 표도 있다.
        # 그래서 "현재 페이지 헤더"와 "다음 페이지로 넘길 헤더"를 별도로 관리한다.
        lines: list[str] = []
        latest_header_lines = previous_table_header_lines
        for table in self._extract_pdf_tables(page):
            normalized_rows = self._normalize_pdf_table(table)
            if not normalized_rows:
                continue
            table_structure = self._infer_table_structure(normalized_rows)
            preamble_rows: list[list[str]] = []
            header_rows: list[list[str]] = []
            body_rows = normalized_rows
            if table_structure is not None:
                _, header_start, data_start = table_structure
                # preamble_rows 는 표처럼 보이지만 실제로는 제목/수신처/안내 문구일 수 있다.
                # 이 행들을 table header 로 올리면 전체 컬럼 스키마가 깨지므로 별도 분리한다.
                if data_start > header_start >= 0:
                    preamble_rows = normalized_rows[:header_start]
                    header_rows = normalized_rows[header_start:data_start]
                    body_rows = normalized_rows[data_start:]

            preamble_lines = [
                self._render_pdf_preamble_row(row)
                for row in preamble_rows
                if self._render_pdf_preamble_row(row)
            ]
            current_header_lines = self._dedupe_lines([" | ".join(row) for row in header_rows]) or None
            if not current_header_lines and previous_table_header_lines and self._looks_like_data_only_table(body_rows):
                current_header_lines = previous_table_header_lines
            row_lines = self._render_pdf_body_rows(
                body_rows,
                current_header_lines=current_header_lines,
            )
            has_inline_header = (
                bool(current_header_lines)
                and row_lines[: len(current_header_lines)] == current_header_lines
            )
            if lines:
                lines.append("")
            lines.extend(preamble_lines)
            if current_header_lines and not has_inline_header:
                lines.extend(current_header_lines)
                latest_header_lines = current_header_lines
            elif current_header_lines:
                latest_header_lines = current_header_lines
            lines.extend(row_lines)
        return [line for line in lines if line.strip()], latest_header_lines

    def _render_pdf_body_rows(
        self,
        body_rows: list[list[str]],
        current_header_lines: list[str] | None,
    ) -> list[str]:
        """정규화된 표 body를 raw_text용 줄 목록으로 렌더링한다."""
        # body row 처리에서 중요한 건 두 가지다.
        # 1. 총계/합계는 개별 주문이 아니므로 버린다.
        # 2. "1. Subscription" 같은 섹션 마커는 데이터행이 아니라 문맥 정보로 유지한다.
        rendered_lines: list[str] = []
        for row in body_rows:
            if self._is_total_row(row):
                continue
            if self._is_pdf_section_marker_row(row):
                marker = self._render_pdf_preamble_row(row)
                if marker:
                    rendered_lines.append(marker)
                if current_header_lines:
                    rendered_lines.extend(current_header_lines)
                continue
            row_line = " | ".join(row)
            if current_header_lines and row_line in current_header_lines:
                if rendered_lines and rendered_lines[-1] == row_line:
                    continue
                if not rendered_lines:
                    continue
            rendered_lines.append(row_line)
        return rendered_lines

    def _extract_pdf_tables(self, page: pdfplumber.page.Page) -> list[list[list[str | None]]]:
        """`pdfplumber` table 후보들을 여러 설정으로 시도하고 최적 set을 고른다."""
        # pdfplumber 는 table_settings 에 따라 결과 품질 차이가 크다.
        # 그래서 후보를 여러 개 시도한 뒤 가장 덜 쪼개진 table set 을 고른다.
        candidates: list[list[list[str | None]]] = []
        seen_signatures: set[str] = set()
        for table_settings in self._pdf_table_setting_candidates():
            extracted = page.extract_tables(table_settings=table_settings)
            if not extracted:
                continue
            signature = repr(extracted)
            if signature in seen_signatures:
                continue
            seen_signatures.add(signature)
            candidates.append(extracted)
        if not candidates:
            return []
        return max(candidates, key=self._score_pdf_table_set)

    def _score_pdf_table_set(self, tables: list[list[list[str | None]]]) -> tuple[int, int, int, int]:
        """table 후보 set을 상대 비교하기 위한 품질 점수를 계산한다."""
        # 점수는 "실제 데이터행이 많고(fragment 적고), 헤더가 살아 있는가"에 초점을 둔다.
        # 이 값은 절대적 의미보다 후보 간 상대 비교용이다.
        normalized_tables = [self._normalize_pdf_table(table) for table in tables]
        meaningful_rows = sum(
            1
            for table in normalized_tables
            for row in table
            if sum(1 for cell in row if cell) >= 3
        )
        data_rows = sum(
            1
            for table in normalized_tables
            for row in table
            if self._is_likely_data_row(row)
        )
        header_rows = sum(
            1
            for table in normalized_tables
            for row in table
            if self._header_score(row) > 0
        )
        fragmented_cells = sum(
            1
            for table in normalized_tables
            for row in table
            for cell in row
            if self._is_fragmented_pdf_cell(cell)
        )
        return (data_rows, -fragmented_cells, header_rows, meaningful_rows)

    def _normalize_pdf_table(self, table: list[list[str | None]]) -> list[list[str]]:
        """pdfplumber 테이블을 빈 행 제거 + 셀 정규화된 2차원 배열로 바꾼다."""
        rows: list[list[str]] = []
        for row in table:
            if row is None:
                continue
            normalized = [self._normalize_pdf_cell(cell) for cell in row]
            if any(normalized):
                rows.append(normalized)
        return rows

    def _looks_like_data_only_table(self, rows: list[list[str]]) -> bool:
        """현재 페이지 표가 헤더 없이 body만 이어진 continuation table인지 본다."""
        if not rows:
            return False
        data_rows = sum(1 for row in rows[:5] if self._is_likely_data_row(row))
        return data_rows >= min(3, len(rows))

    def _is_pdf_section_marker_row(self, row: list[str]) -> bool:
        """table cell 배열 안에 들어온 섹션 마커 행을 판별한다."""
        non_empty = [cell for cell in row if cell]
        if len(non_empty) != 1:
            return False
        marker = non_empty[0].strip().lower()
        return bool(re.fullmatch(r"\d+\.\s*(subscription|redemption)", marker))

    def _extract_pdf_context_lines(self, page: pdfplumber.page.Page, table_lines: list[str]) -> list[str]:
        """표 바깥 본문에서 order_type/settle_class 판단에 도움이 되는 문맥 줄만 남긴다."""
        # 표로 이미 잡힌 줄은 제외하고, 표 바깥에서 order_type / settle_class 판단에
        # 도움이 되는 문맥 라인만 추가로 보존한다.
        page_text = (page.extract_text() or "").strip()
        if not page_text:
            return []
        normalized_table_text = " ".join(table_lines).lower()
        context_lines: list[str] = []
        for line in page_text.splitlines():
            normalized_line = re.sub(r"\s+", " ", line).strip()
            if not normalized_line:
                continue
            if normalized_line.lower() in normalized_table_text:
                continue
            if self._is_pdf_context_line(normalized_line):
                context_lines.append(normalized_line)
        return context_lines

    def _is_pdf_context_line(self, line: str) -> bool:
        """표 바깥 줄 중 order_type/settle_class 판단에 도움이 되는 줄만 남긴다."""
        lower_line = line.lower()
        return any(keyword in lower_line for keyword in self.PDF_CONTEXT_KEYWORDS)

    @staticmethod
    def _merge_unique_lines(prefix_lines: list[str], suffix_lines: list[str]) -> list[str]:
        """context line과 table line을 중복 없이 합친다."""
        merged_prefix: list[str] = []
        suffix_normalized = {line.strip() for line in suffix_lines if line.strip()}
        seen_prefix: set[str] = set()

        for line in prefix_lines:
            normalized = line.strip()
            if not normalized or normalized in suffix_normalized or normalized in seen_prefix:
                continue
            seen_prefix.add(normalized)
            merged_prefix.append(normalized)

        merged_suffix = [line.strip() for line in suffix_lines if line.strip()]
        return merged_prefix + merged_suffix

    @staticmethod
    def _render_pdf_preamble_row(row: list[str]) -> str:
        """표 preamble 행을 사람이 읽을 수 있는 일반 텍스트 한 줄로 렌더링한다."""
        return " ".join(cell for cell in row if cell).strip()

    @staticmethod
    def _dedupe_lines(lines: list[str]) -> list[str]:
        """순서를 유지하면서 완전히 같은 줄을 제거한다."""
        deduped: list[str] = []
        seen: set[str] = set()
        for line in lines:
            normalized = line.strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(normalized)
        return deduped

    @staticmethod
    def _normalize_pdf_cell(value: str | None) -> str:
        """PDF 셀 내부 줄바꿈과 중복 공백을 후단 파싱 친화적으로 정리한다."""
        if value is None:
            return ""
        collapsed = re.sub(r"\s*\n\s*", " / ", value)
        return re.sub(r"\s+", " ", collapsed).strip()

    def _is_fragmented_pdf_cell(self, value: str) -> bool:
        """셀 값이 비정상적으로 잘게 쪼개진 fragment인지 판단한다."""
        if not value:
            return False
        if self._is_amount_string(value):
            return False
        if self._looks_like_fund_code(value):
            return False
        if len(value) > 3:
            return False
        return bool(re.fullmatch(r"[A-Za-z가-힣]{1,3}", value))

    @staticmethod
    def _pdf_table_setting_candidates() -> list[dict[str, object] | None]:
        """pdfplumber table 추출에 시도할 설정 후보를 반환한다."""
        return [
            None,
            {
                "vertical_strategy": "lines",
                "horizontal_strategy": "lines",
                "intersection_tolerance": 5,
                "snap_tolerance": 3,
                "join_tolerance": 3,
            },
            {
                "vertical_strategy": "text",
                "horizontal_strategy": "text",
                "min_words_vertical": 2,
                "min_words_horizontal": 1,
                "text_x_tolerance": 2,
                "text_y_tolerance": 2,
            },
        ]

    @staticmethod
    def _is_pdf_password_error(exc: PdfminerException) -> bool:
        """pdfminer 예외 체인 안에 실제 비밀번호 오류가 들어 있는지 확인한다."""
        return any(isinstance(arg, PDFPasswordIncorrect) for arg in exc.args)
