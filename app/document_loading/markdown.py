from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


class DocumentLoaderMarkdownMixin:
    """DocumentLoader markdown/section rendering helpers."""

    def build_markdown(self, raw_text: str, render_hints: dict[str, Any] | None = None) -> str:
        """raw_text 를 LLM 입력용 markdown 으로 결정론적으로 변환한다.

        이 단계는 LLM에게 다시 레이아웃을 재구성시키지 않기 위한 것이다.
        표는 markdown table 로, 일반 텍스트는 fenced block 으로 유지해서
        "사람이 읽는 구조"와 "코드가 재현 가능한 구조"를 동시에 맞춘다.
        """
        preferred_markdown_text = render_hints.get("preferred_markdown_text") if render_hints else None
        if isinstance(preferred_markdown_text, str) and preferred_markdown_text.strip():
            return preferred_markdown_text.strip()

        sections = [section.strip() for section in raw_text.split(self.RAW_SECTION_DELIMITER) if section.strip()]
        rendered_sections: list[str] = []
        render_hint_state = {
            "pipe_table_hints": list(render_hints.get("pipe_table_hints", [])) if render_hints else [],
            "pipe_table_cursor": 0,
        }

        for section_index, section in enumerate(sections, start=1):
            # 빈 줄은 "같은 section 안의 별도 표/본문 블록 경계"로 자주 쓰인다.
            # 특히 HTML 메일/페이지는 두 개 이상의 table을 하나의 section에 이어 붙인 뒤
            # 빈 줄만 사이에 두는 경우가 많다. 여기서 blank line을 미리 제거하면
            # 서로 다른 표가 한 markdown table로 합쳐져 LLM과 coverage가 함께 흔들린다.
            lines = [line.rstrip() for line in section.splitlines()]
            if not lines:
                continue
            heading = self._format_markdown_heading(lines[0], section_index)
            body = self._render_markdown_body(lines[1:], render_hint_state=render_hint_state)
            if body:
                rendered_sections.append(f"{heading}\n\n{body}")
            else:
                rendered_sections.append(heading)

        markdown_text = self.MARKDOWN_SECTION_DELIMITER.join(rendered_sections).strip()
        logger.info("Built markdown text for LLM: chars=%s", len(markdown_text))
        return markdown_text or raw_text

    def _audit_html_markdown_loss(
        self,
        markdown_text: str,
        render_hints: dict[str, Any] | None = None,
    ) -> list[str]:
        audit_payload = render_hints.get("html_markdown_audit") if render_hints else None
        if not isinstance(audit_payload, dict):
            return []

        reasons: list[str] = []
        label_tokens = [str(token).strip() for token in audit_payload.get("label_tokens", []) if str(token).strip()]
        table_row_tokens = [
            str(token).strip() for token in audit_payload.get("table_row_tokens", []) if str(token).strip()
        ]
        inherited_row_tokens = [
            str(token).strip() for token in audit_payload.get("inherited_row_tokens", []) if str(token).strip()
        ]
        row_kind_tokens = [
            str(token).strip() for token in audit_payload.get("row_kind_tokens", []) if str(token).strip()
        ]

        if label_tokens and any(token not in markdown_text for token in label_tokens):
            reasons.append("html_label_missing")
        if table_row_tokens and any(token not in markdown_text for token in table_row_tokens):
            reasons.append("html_table_shape_collapsed")
        if inherited_row_tokens and any(token not in markdown_text for token in inherited_row_tokens):
            reasons.append("html_inherited_context_missing")
        if row_kind_tokens and any(token not in markdown_text for token in row_kind_tokens):
            reasons.append("html_row_kind_missing")
        return reasons

    def _detect_section_delimiter(self, document_text: str) -> str:
        """입력 본문이 raw section 형식인지 markdown section 형식인지 판별한다."""
        if self.MARKDOWN_SECTION_DELIMITER in document_text:
            return self.MARKDOWN_SECTION_DELIMITER
        return self.RAW_SECTION_DELIMITER

    def _format_markdown_heading(self, header_line: str, section_index: int) -> str:
        """raw section marker를 사람이 읽는 markdown heading으로 치환한다."""
        page_match = header_line.strip().removeprefix("[PAGE ").removesuffix("]")
        if header_line.startswith("[PAGE ") and header_line.endswith("]"):
            return f"## Page {page_match}"
        sheet_match = header_line.strip().removeprefix("[SHEET ").removesuffix("]")
        if header_line.startswith("[SHEET ") and header_line.endswith("]"):
            return f"## Sheet {sheet_match}"
        html_match = header_line.strip().removeprefix("[HTML ").removesuffix("]")
        if header_line.startswith("[HTML ") and header_line.endswith("]"):
            return f"## HTML {html_match}"
        eml_match = header_line.strip().removeprefix("[EML ").removesuffix("]")
        if header_line.startswith("[EML ") and header_line.endswith("]"):
            return f"## EML {eml_match}"
        mht_match = header_line.strip().removeprefix("[MHT ").removesuffix("]")
        if header_line.startswith("[MHT ") and header_line.endswith("]"):
            return f"## MHT {mht_match}"
        return f"## Section {section_index}"

    def _render_markdown_body(
        self,
        lines: list[str],
        *,
        render_hint_state: dict[str, Any] | None = None,
    ) -> str:
        """section 본문을 text block 과 table block 의 교차 구조로 렌더링한다.

        이 함수가 중요한 이유는, 한 section 안에서도
        - 상단 안내 문구
        - 표 헤더/본문
        - 표 아래 메모
        가 섞여 있기 때문이다. 이를 한 덩어리 텍스트로 합치면 LLM이 컬럼 관계를
        놓치기 쉬워서 블록 단위로 분리한다.
        """
        if not lines:
            return ""

        blocks: list[str] = []
        cursor = 0
        while cursor < len(lines):
            if self._is_pipe_row(lines[cursor]):
                next_cursor = cursor
                while next_cursor < len(lines) and self._is_pipe_row(lines[next_cursor]):
                    next_cursor += 1
                pipe_table_hint = None
                if render_hint_state is not None:
                    pipe_table_hints = render_hint_state.get("pipe_table_hints", [])
                    pipe_table_cursor = int(render_hint_state.get("pipe_table_cursor", 0))
                    if pipe_table_cursor < len(pipe_table_hints):
                        pipe_table_hint = pipe_table_hints[pipe_table_cursor]
                    render_hint_state["pipe_table_cursor"] = pipe_table_cursor + 1
                blocks.extend(self._render_pipe_table(lines[cursor:next_cursor], pipe_table_hint=pipe_table_hint))
                cursor = next_cursor
                continue

            next_cursor = cursor
            while next_cursor < len(lines) and not self._is_pipe_row(lines[next_cursor]):
                next_cursor += 1
            text_block_lines = lines[cursor:next_cursor]
            flattened_schedule_table = self._render_flattened_schedule_table(text_block_lines)
            if flattened_schedule_table:
                blocks.append(flattened_schedule_table)
            else:
                blocks.append(self._render_text_block(text_block_lines))
            cursor = next_cursor

        return "\n\n".join(block for block in blocks if block.strip())

    @staticmethod
    def _is_pipe_row(line: str) -> bool:
        """공통 표 로직이 읽을 수 있는 pipe-delimited row인지 빠르게 판별한다."""
        return " | " in line

    def _render_pipe_table(
        self,
        rows: list[str],
        *,
        pipe_table_hint: dict[str, Any] | None = None,
    ) -> list[str]:
        """pipe 형태 블록에서 실제 표 구조를 추론해 markdown table 로 바꾼다.

        핵심은 "첫 줄이 무조건 헤더는 아니다"라는 점이다.
        보험사 문서에는 제목행/메모행/수신처행 뒤에 실제 헤더가 나오는 경우가 많아서,
        헤더 시작점과 데이터 시작점을 따로 추론한다.
        """
        parsed_rows = [self._split_pipe_row(row) for row in rows]
        if len(parsed_rows) == 1:
            return [self._render_text_block(rows)]

        normalized_rows = self._normalize_pipe_rows(parsed_rows)
        table_structure = self._infer_table_structure(normalized_rows)
        if table_structure is None:
            return [self._render_text_block(rows)]

        blocks: list[str] = []
        preamble_start, header_start, data_start = table_structure
        if header_start > preamble_start:
            blocks.append(self._render_text_block(rows[preamble_start:header_start]))

        header_rows = normalized_rows[header_start:data_start]
        body_rows = normalized_rows[data_start:]
        inherited_body_rows: list[list[bool]] | None = None
        if pipe_table_hint is not None:
            inherited_rows_payload = pipe_table_hint.get("inherited_rows")
            if isinstance(inherited_rows_payload, list):
                normalized_inherited_rows: list[list[bool]] = []
                for inherited_row in inherited_rows_payload:
                    if not isinstance(inherited_row, list):
                        normalized_inherited_rows = []
                        break
                    normalized_inherited_rows.append(
                        [bool(cell) for cell in inherited_row] + [False] * (len(normalized_rows[0]) - len(inherited_row))
                    )
                if normalized_inherited_rows:
                    inherited_body_rows = normalized_inherited_rows[data_start:]
        if not header_rows or not body_rows:
            blocks.append(self._render_text_block(rows))
            return blocks

        markdown_table = self._render_structured_table(
            header_rows,
            body_rows,
            inherited_body_rows=inherited_body_rows,
        )
        if markdown_table:
            blocks.append(markdown_table)
        else:
            blocks.append(self._render_text_block(rows))
        return blocks

    @staticmethod
    def _split_pipe_row(row: str) -> list[str]:
        """pipe row를 셀 리스트로 나누고 각 셀의 좌우 공백을 정리한다."""
        return [cell.strip() for cell in row.split(" | ")]

    def _normalize_pipe_rows(self, rows: list[list[str]]) -> list[list[str]]:
        """행 길이를 맞추고 셀 값 표기를 정규화해 2차원 테이블로 만든다."""
        column_count = max(len(row) for row in rows)
        return [[self._normalize_pipe_cell(cell) for cell in row + [""] * (column_count - len(row))] for row in rows]

    def _infer_table_structure(self, rows: list[list[str]]) -> tuple[int, int, int] | None:
        """정규화된 표 블록에서 preamble / header / body 경계를 찾는다.

        반환값은 `(preamble_start, header_start, data_start)` 이다.
        이 정보가 있어야 markdown 변환 시 제목행은 text block 으로 빼고,
        실제 컬럼 헤더만 table header 로 올릴 수 있다.
        """
        data_start = self._find_first_data_row(rows)
        if data_start is None:
            header_index = self._find_best_header_row(rows)
            if header_index is None or header_index >= len(rows) - 1:
                return None
            return (0, header_index, header_index + 1)

        header_start = self._find_header_start(rows, data_start)
        if header_start >= data_start:
            header_start = max(0, data_start - 1)
        return (0, header_start, data_start)

    def _find_first_data_row(self, rows: list[list[str]]) -> int | None:
        """표 블록에서 실제 body가 시작되는 첫 행을 찾는다."""
        for index, row in enumerate(rows):
            if self._is_likely_data_row(row) or self._is_total_row(row) or self._is_sparse_split_row_seed(rows, index):
                return index
        return None

    def _is_sparse_split_row_seed(self, rows: list[list[str]], row_index: int) -> bool:
        """첫 행이 식별자만 갖고, 다음 행이 continuation인 split-row seed를 잡는다.

        일부 PDF/HTML 표는 첫 데이터 행에 펀드코드나 거래유형만 있고 실제 금액은
        바로 다음 continuation row에만 적힌다. 이런 경우 현재 행을 body 시작으로
        보지 않으면 첫 데이터 행이 header에 흡수되어 markdown이 오염된다.
        """
        if row_index < 0 or row_index >= len(rows) - 1:
            return False

        row = rows[row_index]
        next_row = rows[row_index + 1]
        if self._is_total_row(row) or self._is_metadata_preamble_row(row):
            return False
        if self._is_total_row(next_row) or self._is_metadata_preamble_row(next_row):
            return False
        if self._is_likely_data_row(row):
            return False

        leading_cells = row[:4]
        has_code = any(
            self._looks_like_fund_code(cell) and cell.strip().upper() not in {"TRUE", "FALSE"}
            for cell in leading_cells
        )
        text_identity_cells = [cell for cell in leading_cells if cell and self._looks_like_text_cell(cell)]
        text_identity_cells = [cell for cell in text_identity_cells if cell.strip().upper() not in {"TRUE", "FALSE"}]
        if not has_code and len(text_identity_cells) < 2:
            return False
        if not any(self._is_amount_string(cell) for cell in row[1:] if cell):
            return False

        non_zero_amount_cells = sum(1 for cell in row[1:] if self._is_amount_string(cell) and not self._is_zero_amount(cell))
        if non_zero_amount_cells > 0:
            return False

        return self._looks_like_sparse_continuation_without_header(next_row)

    def _find_best_header_row(self, rows: list[list[str]]) -> int | None:
        """헤더 점수가 가장 높은 행을 찾아 단일 헤더 후보로 사용한다."""
        best_index: int | None = None
        best_score = 0
        for index, row in enumerate(rows):
            score = self._header_score(row)
            if score > best_score:
                best_score = score
                best_index = index
        return best_index

    def _find_header_start(self, rows: list[list[str]], data_start: int) -> int:
        """body 시작점 바로 위에서 연속된 header 구간의 시작 인덱스를 찾는다."""
        header_start = max(0, data_start - 1)
        while (
            header_start > 0
            and self._is_header_row(rows[header_start - 1])
            and not self._is_metadata_preamble_row(rows[header_start - 1])
        ):
            header_start -= 1
        if self._header_score(rows[header_start]) == 0 and header_start < data_start - 1:
            header_start += 1
        return header_start

    def _render_structured_table(
        self,
        header_rows: list[list[str]],
        body_rows: list[list[str]],
        *,
        inherited_body_rows: list[list[bool]] | None = None,
    ) -> str:
        """헤더/본문 행렬을 LLM 입력용 markdown table 문자열로 렌더링한다."""
        header = self._collapse_header_rows(header_rows)
        if not any(header):
            return ""

        propagated_body_rows = self._propagate_contextual_body_rows(header, body_rows)

        filtered_indices = [
            index
            for index, row in enumerate(body_rows)
            if self._is_likely_data_row(row)
            or self._is_short_continuation_row(row, header)
            or self._is_sparse_split_seed_row(body_rows, index, header)
            or self._is_order_subtotal_row(row, propagated_body_rows[index], header)
        ]
        filtered_body_rows = [propagated_body_rows[index] for index in filtered_indices]
        if filtered_body_rows:
            body_rows = filtered_body_rows
            if inherited_body_rows is not None:
                inherited_body_rows = [
                    inherited_body_rows[index]
                    for index in filtered_indices
                    if index < len(inherited_body_rows)
                ]

        keep_indices = self._select_core_markdown_columns(header, body_rows)
        if keep_indices:
            header = [header[index] for index in keep_indices]
            body_rows = [
                [row[index] if index < len(row) else "" for index in keep_indices]
                for row in body_rows
            ]
            if inherited_body_rows is not None:
                inherited_body_rows = [
                    [row[index] if index < len(row) else False for index in keep_indices]
                    for row in inherited_body_rows
                ]
            header = self._normalize_markdown_headers(header, body_rows)

        markdown_lines = [
            f"| {' | '.join(self._escape_markdown_cell(cell or f'col_{index + 1}') for index, cell in enumerate(header))} |",
            f"| {' | '.join('---' for _ in range(len(header)))} |",
        ]
        for row in body_rows:
            markdown_lines.append(f"| {' | '.join(self._escape_markdown_cell(cell) for cell in row)} |")
        return "\n".join(markdown_lines)

    def _select_core_markdown_columns(self, header: list[str], body_rows: list[list[str]]) -> list[int]:
        """LLM 입력에 남길 핵심 컬럼 인덱스를 고른다.

        목적은 "주문을 직접 판단하는 정보"만 구조화 표에 남기고,
        좌수/NAV대비/합산/보조 수치처럼 혼선을 주는 컬럼은 markdown 단계에서 제거하는 것이다.
        raw_text 백업은 그대로 함께 전달되므로, 정보 자체가 사라지는 것은 아니다.
        """
        keep_indices: list[int] = []
        has_order_context_header = any(self._is_order_context_label(label) for label in header)
        has_non_mixed_order_amount_column = any(
            not self._is_mixed_unit_amount_label(label)
            and (
                self._is_markdown_relevant_amount_label(label)
                or self._is_contextual_generic_amount_label(label, has_order_context_header)
            )
            and self._column_has_non_zero_amount(body_rows, index)
            for index, label in enumerate(header)
        )
        for index, label in enumerate(header):
            if self._is_identity_label(label):
                keep_indices.append(index)
                continue
            if self._is_order_context_label(label) and self._column_has_meaningful_text(body_rows, index):
                keep_indices.append(index)
                continue
            if self._column_has_order_context_values(body_rows, index):
                keep_indices.append(index)
                continue
            if self._is_mixed_unit_amount_label(label) and has_non_mixed_order_amount_column:
                continue
            if (
                self._is_markdown_relevant_amount_label(label)
                or self._is_contextual_generic_amount_label(label, has_order_context_header)
            ) and self._column_has_non_zero_amount(body_rows, index):
                keep_indices.append(index)
        return keep_indices

    def _normalize_markdown_headers(self, header: list[str], body_rows: list[list[str]]) -> list[str]:
        """본문 값으로만 문맥이 드러나는 열은 markdown 에서 의미 있는 이름으로 보정한다."""
        normalized = list(header)
        for index, label in enumerate(header):
            if self._is_order_context_label(label):
                continue
            if not self._column_has_order_context_values(body_rows, index):
                continue
            normalized[index] = "구분"
        return normalized

    def _is_markdown_relevant_amount_label(self, label: str) -> bool:
        """LLM용 markdown 표에 남길 가치가 있는 금액 컬럼인지 판별한다."""
        lower_label = label.lower()
        if any(keyword in lower_label for keyword in ("합산", "누계", "총합", "subtotal", "total", "ratio", "nav대비", "nav ratio")):
            return False
        if self._is_plain_generic_amount_label(label):
            return True
        if self._is_mixed_unit_amount_label(label):
            return any("금액" in segment or "amount" in segment for segment in self._label_segments(label))
        if any(keyword in lower_label for keyword in self.NON_AMOUNT_KEYWORDS):
            return False
        return (
            self._is_core_order_amount_label(label)
            or self._is_explicit_order_amount_label(label)
            or self._is_execution_amount_label(label)
            or self._is_scheduled_amount_label(label)
            or self._is_order_amount_label(label)
        )

    def _is_order_context_label(self, label: str) -> bool:
        """`구분`, `내용`, `transaction type` 같은 주문 문맥 컬럼인지 본다."""
        lower_label = label.lower()
        compact_label = self._compact_label(label)
        return any(
            keyword in lower_label
            for keyword in (
                "transaction",
                "type",
                "content",
                "detail",
                "description",
                "memo",
            )
        ) or any(keyword in compact_label for keyword in ("구분", "내용", "적요", "사유", "비고")) or compact_label in {
            "거래유형",
            "거래유형명",
        }

    def _column_has_meaningful_text(self, body_rows: list[list[str]], column_index: int) -> bool:
        """열 안에 금액/합계가 아닌 실제 설명 텍스트가 있는지 확인한다."""
        for row in body_rows:
            if column_index >= len(row):
                continue
            value = row[column_index].strip()
            if not value or self._is_total_like_text(value):
                continue
            if self._is_amount_string(value):
                continue
            return True
        return False

    def _column_has_order_context_values(self, body_rows: list[list[str]], column_index: int) -> bool:
        """헤더가 약해도 본문 값이 "행 구분" 열인지 아주 보수적으로 판단한다."""
        values: list[str] = []
        for row in body_rows:
            if column_index >= len(row):
                continue
            value = row[column_index].strip()
            if not value:
                continue
            if self._is_amount_string(value):
                continue
            values.append(value)
        if len(values) < 2:
            return False
        return all(self._looks_like_order_context_value(value) for value in values)

    @staticmethod
    def _looks_like_order_context_value(value: str) -> bool:
        """본문 셀 값이 row-kind(입금/출금/환매 신청) 계열인지 좁게 본다."""
        compact_value = re.sub(r"[^0-9a-z가-힣]+", "", value).lower()
        if not compact_value:
            return False
        return compact_value in {
            "입금",
            "출금",
            "환매신청",
            "환매",
            "설정",
            "해지",
            "buy",
            "sell",
            "subscription",
            "redemption",
        }

    def _column_has_non_zero_amount(self, body_rows: list[list[str]], column_index: int) -> bool:
        """열 안에 0이 아닌 금액이 실제로 존재하는지 확인한다."""
        for row in body_rows:
            if column_index >= len(row):
                continue
            value = row[column_index].strip()
            if self._looks_like_non_zero_amount_value(value):
                return True
        return False

    def _is_core_order_amount_label(self, label: str) -> bool:
        """주문 금액 판단에 직접 필요한 amount 컬럼인지 판정한다."""
        if not label:
            return False
        lower_label = label.lower()
        if any(keyword in lower_label for keyword in ("합산", "누계", "총합", "subtotal", "total", "ratio", "nav대비", "nav ratio")):
            return False
        for segment in self._label_segments(label):
            if any(keyword in segment for keyword in self.NON_AMOUNT_KEYWORDS):
                continue
            if any(
                keyword in segment
                for keyword in (
                    "보수",
                    "fee",
                    "이용료",
                    "수령",
                    "receipt",
                    "회계",
                    "초기자금",
                    "발생",
                    "계정",
                )
            ):
                continue
            if self._is_explicit_order_amount_label(segment):
                return True
            if self._is_execution_amount_label(segment):
                return True
            if self._is_scheduled_amount_label(segment):
                return True
            if self._looks_like_generic_order_amount_segment(segment):
                return True
            if self._is_order_amount_label(segment):
                return True
        return False

    @staticmethod
    def _looks_like_generic_order_amount_segment(segment: str) -> bool:
        """정확한 키워드 사전에 없더라도 주문 금액성 헤더를 넓게 인식한다."""
        if "금액" not in segment and "amount" not in segment:
            return False
        if any(
            keyword in segment
            for keyword in (
                "설정",
                "해지",
                "입금",
                "출금",
                "투입",
                "인출",
                "납입",
                "결제",
                "buy",
                "sell",
                "subscription",
                "redemption",
            )
        ):
            return True
        return False

    @staticmethod
    def _is_plain_generic_amount_label(label: str) -> bool:
        """맥락이 있을 때 주문 금액으로 재해석 가능한 generic header를 찾는다."""
        normalized = re.sub(r"[^0-9a-z가-힣]+", "", label).lower()
        return normalized in {"금액", "금액원", "amount", "amountkrw", "amountwon"}

    def _is_contextual_generic_amount_label(self, label: str, has_order_context_header: bool) -> bool:
        """주문 문맥 컬럼이 있을 때만 generic amount를 주문 금액으로 인정한다."""
        if not has_order_context_header:
            return False
        if any(keyword in label.lower() for keyword in self.NON_AMOUNT_KEYWORDS):
            return False
        return self._is_plain_generic_amount_label(label)

    def _collapse_header_rows(self, header_rows: list[list[str]]) -> list[str]:
        """Merge multi-row table headers into one logical header row."""
        collapsed: list[str] = []
        expanded_header_rows = self._expand_header_rows(header_rows)
        column_count = max(len(row) for row in expanded_header_rows)

        for column_index in range(column_count):
            tokens: list[str] = []
            for row in expanded_header_rows:
                cell = row[column_index] if column_index < len(row) else ""
                if not cell:
                    continue
                if cell not in tokens:
                    tokens.append(cell)
            collapsed.append(" / ".join(tokens))
        return collapsed

    def _expand_header_rows(self, header_rows: list[list[str]]) -> list[list[str]]:
        """Forward-fill grouped header labels into blank cells where appropriate."""
        expanded_rows: list[list[str]] = []
        for row in header_rows:
            expanded = list(row)
            carry = ""
            for index, cell in enumerate(expanded):
                if cell:
                    carry = cell
                    continue
                if carry and self._should_forward_fill_header_value(carry):
                    expanded[index] = carry
            expanded_rows.append(expanded)
        return expanded_rows

    def _should_forward_fill_header_value(self, value: str) -> bool:
        """상위 그룹 헤더를 빈 셀에 전파해도 되는지 판단한다."""
        normalized = value.lower()
        if any(keyword in normalized for keyword in self.NON_AMOUNT_KEYWORDS):
            return False
        if self._is_order_amount_label(value):
            return True
        if bool(re.search(r"\d{4}-\d{2}-\d{2}", normalized)):
            return True
        if any(keyword in normalized for keyword in ("예정", "청구", "예상", "t+", "t일", "execution", "실행")):
            return True
        return False

    def _render_flattened_schedule_table(self, lines: list[str]) -> str | None:
        """flattened schedule 본문을 markdown table 문자열로 렌더링한다."""
        parsed = self._parse_flattened_schedule_block(lines)
        if parsed is None:
            return None

        header, rows = parsed
        if not header or not rows:
            return None

        markdown_lines = [
            f"| {' | '.join(self._escape_markdown_cell(cell or f'col_{index + 1}') for index, cell in enumerate(header))} |",
            f"| {' | '.join('---' for _ in header)} |",
        ]
        for row in rows:
            markdown_lines.append(f"| {' | '.join(self._escape_markdown_cell(cell) for cell in row)} |")
        return "\n".join(markdown_lines)
