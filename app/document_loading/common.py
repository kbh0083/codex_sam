from __future__ import annotations

import re


class DocumentLoaderCommonMixin:
    SUPPORTED_EXTENSIONS = {".pdf", ".xlsx", ".xlsm", ".xls", ".html", ".htm", ".eml", ".mht", ".mhtml"}
    RAW_SECTION_DELIMITER = "\n\n==== SECTION ====\n\n"
    MARKDOWN_SECTION_DELIMITER = "\n\n<!-- SECTION -->\n\n"
    HEADER_KEYWORDS = (
        "펀드",
        "fund",
        "설정",
        "해지",
        "입금",
        "출금",
        "투입",
        "인출",
        "subscription",
        "redemption",
        "transaction",
        "구분",
        "date",
        "기준일",
        "결제일",
        "settlement",
        "당일",
        "예정",
        "청구",
        "예상",
        "t+",
        "nav",
        "순유입",
        "이체",
        "amount",
    )
    ORDER_AMOUNT_KEYWORDS = (
        "설정금액",
        "해지금액",
        "추가설정금액",
        "당일인출금액",
        "해지신청",
        "설정신청",
        "입금액",
        "출금액",
        "투입금액",
        "인출금액",
        "매입금액",
        "환매금액",
        "buy",
        "sell",
        "subscription",
        "redemption",
        "execution amount",
        "당일이체금액",
        "amount",
    )
    TARGET_MANAGER_KEYWORD = "삼성"
    MANAGER_HEADER_KEYWORDS = ("운용사", "manager")
    NON_AMOUNT_KEYWORDS = (
        "좌수",
        "unit",
        "units",
        "share",
        "shares",
        "누적좌수",
        "전일좌수",
        "변경 후 누적좌수",
        "순투입좌수",
        "증감좌수",
    )
    NO_ORDER_MARKERS = (
        "지시없음",
        "지시 없음",
        "거래없음",
        "거래 없음",
        "설정해지 지시없음",
        "자금설정해지 지시없음",
        "no instruction",
        "no instructions",
        "no order",
        "no orders",
        "no transaction",
        "no transactions",
    )
    NON_INSTRUCTION_TITLE_MARKERS = (
        "설정해지금액통보",
        "설정/해지금액통보",
        "설정 해지 금액 통보",
    )
    NON_INSTRUCTION_MAIL_MARKERS = (
        "첨부파일과 같이",
        "송부드리니",
        "참고하시기 바랍니다",
        "업무에 참고",
        "감사합니다",
    )
    NON_INSTRUCTION_ATTACHMENT_MARKERS = (
        "첨부",
        "attached",
        "attachment",
        "붙임",
    )
    NON_INSTRUCTION_MAIL_FOOTER_MARKERS = (
        "받기",
        "mac용 outlook",
        "outlook for mac",
    )
    HTML_MARKDOWN_LOSS_REASON_CODES = (
        "html_inherited_context_missing",
        "html_table_shape_collapsed",
        "html_label_missing",
        "html_row_kind_missing",
    )
    STRONG_INSTRUCTION_MARKERS = (
        "운용지시서",
        "지시서",
        "order of subscription and redemption",
        "buy & sell report",
        "buy&sell report",
        "설정 및 해지내역",
        "설정/해지 내역",
        "설정해지내역서",
        "실적배당형 펀드별 설정 및 해지내역",
    )

    def _order_bucket_key(self, label: str, column_index: int) -> str:
        """헤더 라벨을 coverage용 결제 버킷 키로 정규화한다."""
        lower_label = label.lower()
        t_plus_match = re.search(r"t\s*\+\s*(\d+)", lower_label)
        if t_plus_match:
            return f"T+{t_plus_match.group(1)}"

        d_plus_match = re.search(r"d\s*\+\s*(\d+)", lower_label)
        if d_plus_match:
            return f"T+{d_plus_match.group(1)}"

        business_day_match = re.search(r"(익+)영업일", lower_label)
        if business_day_match:
            return f"T+{len(business_day_match.group(1))}"

        numbered_business_day_match = re.search(r"제\s*(\d+)\s*영업일", lower_label)
        if numbered_business_day_match:
            return f"T+{numbered_business_day_match.group(1)}"

        if "t일" in lower_label:
            return "T0"

        date_match = re.search(r"\d{4}-\d{2}-\d{2}", lower_label)
        if date_match:
            return date_match.group(0)

        month_day_match = re.search(r"(\d{1,2})\s*월\s*(\d{1,2})\s*일", lower_label)
        if month_day_match:
            month = int(month_day_match.group(1))
            day = int(month_day_match.group(2))
            return f"M{month:02d}D{day:02d}"

        if any(
            keyword in lower_label
            for keyword in (
                "당일",
                "확정",
                "실행",
                "당일이체",
                "설정금액",
                "해지금액",
                "입금액",
                "출금액",
                "투입금액",
                "인출금액",
                "순유입",
                "순투입",
                "결제금액",
                "buy",
                "sell",
                "settlement amount",
                "execution amount",
                "subscription",
                "redemption",
            )
        ):
            return "T0"

        if self._is_plain_generic_amount_label(label):
            return "T0"

        if self._is_core_order_amount_label(label):
            return "T0"

        return f"COL_{column_index}"

    def _is_header_row(self, row: list[str]) -> bool:
        """행이 헤더처럼 보이면 True를 반환한다."""
        return self._header_score(row) > 0

    def _is_metadata_preamble_row(self, row: list[str]) -> bool:
        """`화면번호 : 13001` 같은 메타 preamble 행인지 판별한다."""
        non_empty = [cell for cell in row if cell]
        if not non_empty:
            return False
        colon_cells = sum(1 for cell in non_empty if ":" in cell)
        if colon_cells == 0:
            return False
        return len(non_empty) <= 4 and not any(self._is_order_amount_label(cell) for cell in non_empty)

    def _header_score(self, row: list[str]) -> int:
        """행이 표 헤더답게 보이는 정도를 점수화한다."""
        non_empty = [cell for cell in row if cell]
        if len(non_empty) < 2:
            return 0
        score = 0
        joined = " ".join(non_empty).lower()
        for keyword in self.HEADER_KEYWORDS:
            if keyword in joined:
                score += 2
        text_cells = sum(1 for cell in non_empty if self._looks_like_text_cell(cell))
        if text_cells >= 2:
            score += text_cells
        return score

    def _is_likely_data_row(self, row: list[str]) -> bool:
        """행이 실제 주문 데이터행처럼 보이는지 판별한다."""
        non_empty = [cell for cell in row if cell]
        if len(non_empty) < 3:
            return False
        if self._is_total_row(row):
            return False
        if self._is_metadata_preamble_row(row):
            return False

        amount_indices = [
            index
            for index, cell in enumerate(row)
            if self._is_amount_string(cell) and not self._looks_like_fund_code(cell)
        ]
        if not amount_indices:
            amount_indices = [index for index, cell in enumerate(row) if self._is_amount_string(cell)]
        identity_prefix = row[: amount_indices[0]] if amount_indices else row[:4]
        if not identity_prefix:
            identity_prefix = row[:4]
        leading_cells = identity_prefix[:8]
        has_code = any(
            self._looks_like_fund_code(cell) and cell.strip().upper() not in {"TRUE", "FALSE"}
            for cell in leading_cells
        )
        text_identity_cells = [cell for cell in leading_cells if cell and self._looks_like_text_cell(cell)]
        text_identity_cells = [cell for cell in text_identity_cells if cell.strip().upper() not in {"TRUE", "FALSE"}]
        non_zero_amount_cells = sum(1 for cell in row[1:] if self._is_amount_string(cell) and not self._is_zero_amount(cell))
        if non_zero_amount_cells < 1:
            return False

        if has_code:
            return True
        return bool(text_identity_cells)

    def _is_short_continuation_row(self, row: list[str], header: list[str]) -> bool:
        """PDF split-row 표의 짧은 continuation row를 별도로 판정한다."""
        if not self._is_short_continuation_shape(row, header):
            return False

        return any(
            self._is_amount_string(cell)
            and not self._is_zero_amount(cell)
            and index < len(header)
            and (
                self._is_core_order_amount_label(header[index])
                or self._is_explicit_order_amount_label(header[index])
                or self._is_execution_amount_label(header[index])
                or self._is_scheduled_amount_label(header[index])
                or self._is_order_amount_label(header[index])
            )
            for index, cell in enumerate(row)
        )

    def _is_short_continuation_shape(self, row: list[str], header: list[str]) -> bool:
        """짧은 split-row continuation의 형상만 따로 판별한다."""
        non_empty = [cell for cell in row if cell]
        if len(non_empty) < 2:
            return False
        if self._is_total_row(row) or self._is_metadata_preamble_row(row):
            return False

        text_indices = [index for index, cell in enumerate(row) if cell and self._looks_like_text_cell(cell)]
        amount_indices = [index for index, cell in enumerate(row) if self._is_amount_string(cell)]
        non_zero_amount_indices = [
            index
            for index, cell in enumerate(row)
            if self._is_amount_string(cell) and not self._is_zero_amount(cell)
        ]
        if len(text_indices) != 1 or len(non_zero_amount_indices) != 1:
            return False

        text_index = text_indices[0]
        text_label = header[text_index] if text_index < len(header) else ""
        if not text_label:
            return False
        if not (self._is_identity_label(text_label) or self._is_strong_identity_label(text_label)):
            return False

        allowed_indices = set(text_indices + amount_indices)
        for index, cell in enumerate(row):
            if not cell:
                continue
            if index in allowed_indices:
                continue
            return False

        return True

    def _looks_like_sparse_continuation_without_header(self, row: list[str]) -> bool:
        """헤더가 없어도 continuation 후보인지 거칠게 판정한다."""
        non_empty = [cell for cell in row if cell]
        if len(non_empty) < 2:
            return False
        if self._is_total_row(row) or self._is_metadata_preamble_row(row):
            return False

        text_cells = [cell for cell in row if cell and self._looks_like_text_cell(cell)]
        amount_cells = [cell for cell in row if self._is_amount_string(cell)]
        non_zero_amount_cells = [cell for cell in row if self._is_amount_string(cell) and not self._is_zero_amount(cell)]
        if len(text_cells) != 1 or len(non_zero_amount_cells) != 1:
            return False

        return len(text_cells) + len(amount_cells) == len(non_empty)

    def _propagate_contextual_body_rows(self, header: list[str], body_rows: list[list[str]]) -> list[list[str]]:
        """rowspan/병합 셀처럼 비어 있는 body 값을 같은 주문 문맥으로 보완한다."""
        carry_indices = [
            index
            for index, label in enumerate(header)
            if self._is_strong_identity_label(label) or self._is_grouping_context_label(label)
        ]
        propagated_rows: list[list[str]] = []
        previous_row: list[str] | None = None
        for row in body_rows:
            propagated_row = list(row)
            if previous_row is not None:
                for index in carry_indices:
                    if index >= len(propagated_row):
                        continue
                    if propagated_row[index].strip():
                        continue
                    if index >= len(previous_row):
                        continue
                    previous_value = previous_row[index].strip()
                    if previous_value:
                        propagated_row[index] = previous_value
            propagated_rows.append(propagated_row)
            previous_row = propagated_row
        return propagated_rows

    def _is_grouping_context_label(self, label: str) -> bool:
        """rowspan carry 대상인 구분/transaction type 컬럼인지 판별한다."""
        lower_label = label.lower()
        compact_label = self._compact_label(label)
        return "구분" in compact_label or compact_label in {"거래유형", "거래유형명"} or any(
            keyword in lower_label for keyword in ("transaction", "type")
        )

    def _is_order_subtotal_row(self, row: list[str], propagated_row: list[str], header: list[str]) -> bool:
        """입금소계/출금소계 같은 subtotal 행을 주문 근거로 유지할지 판별한다."""
        subtotal_labels = [cell.strip() for cell in row if cell and self._is_total_like_text(cell)]
        if not subtotal_labels:
            return False
        if not any(self._is_order_subtotal_label(label) for label in subtotal_labels):
            return False
        if not any(self._is_amount_string(cell) and not self._is_zero_amount(cell) for cell in row):
            return False
        return self._is_likely_data_row(propagated_row)

    @staticmethod
    def _is_order_subtotal_label(value: str) -> bool:
        """총계 중에서도 실제 주문 성격을 가진 소계 라벨만 골라낸다."""
        lower_value = value.strip().lower()
        return any(
            keyword in lower_value
            for keyword in (
                "입금소계",
                "출금소계",
                "설정소계",
                "해지소계",
                "buy subtotal",
                "sell subtotal",
                "subscription subtotal",
                "redemption subtotal",
            )
        )

    def _is_sparse_split_seed_row(
        self,
        body_rows: list[list[str]],
        row_index: int,
        header: list[str],
    ) -> bool:
        """body 영역 안의 sparse split-row seed를 유지할지 판정한다."""
        if row_index < 0 or row_index >= len(body_rows) - 1:
            return False

        row = body_rows[row_index]
        next_row = body_rows[row_index + 1]
        if self._is_likely_data_row(row) or self._is_total_row(row) or self._is_metadata_preamble_row(row):
            return False

        leading_cells = row[:4]
        has_code = any(self._looks_like_fund_code(cell) for cell in leading_cells)
        text_identity_cells = [cell for cell in leading_cells if cell and self._looks_like_text_cell(cell)]
        if not has_code and len(text_identity_cells) < 2:
            return False
        if not any(self._is_amount_string(cell) for cell in row[1:] if cell):
            return False

        non_zero_amount_cells = sum(1 for cell in row[1:] if self._is_amount_string(cell) and not self._is_zero_amount(cell))
        if non_zero_amount_cells > 0:
            return False

        return self._is_short_continuation_row(next_row, header)

    def _normalize_pipe_cell(self, value: str) -> str:
        """pipe table 셀의 공백과 business-day 별칭을 정리한다."""
        cell = value.strip()
        if cell in {"|", "\\|"}:
            return ""
        normalized_amount = self._normalize_document_amount_token(cell)
        if normalized_amount is not None:
            return normalized_amount
        return self._alias_business_day_label(cell)

    @staticmethod
    def _alias_business_day_label(value: str) -> str:
        """Add explicit T+N aliases to Korean business-day headers."""
        normalized = value

        def replace_repeated_ik(match: re.Match[str]) -> str:
            count = len(match.group(1))
            original = match.group(0)
            return f"{original}(T+{count})"

        normalized = re.sub(r"\b(익+)영업일\b(?!\s*\(T\+\d+\))", replace_repeated_ik, normalized)
        normalized = re.sub(
            r"\b제\s*(\d+)\s*영업일\b(?!\s*\(T\+\d+\))",
            lambda match: f"{match.group(0)}(T+{int(match.group(1))})",
            normalized,
        )
        return normalized

    @staticmethod
    def _is_amount_string(value: str) -> bool:
        """문자열이 금액/수량처럼 보이는 숫자 표현인지 판별한다."""
        if not value:
            return False
        return DocumentLoaderCommonMixin._normalize_document_amount_token(value) is not None

    def _is_order_amount_label(self, label: str) -> bool:
        """헤더가 넓은 의미의 주문 금액 컬럼인지 판별한다."""
        for segment in self._label_segments(label):
            if any(keyword in segment for keyword in self.NON_AMOUNT_KEYWORDS):
                continue
            if segment.startswith("순투입") or segment.startswith("순유입"):
                continue
            if any(keyword in segment for keyword in self.ORDER_AMOUNT_KEYWORDS):
                return True
        return False

    def _is_explicit_order_amount_label(self, label: str) -> bool:
        """헤더가 설정/해지/입출금처럼 명시적 금액 컬럼인지 판별한다."""
        explicit_keywords = (
            "설정금액",
            "해지금액",
            "추가설정금액",
            "당일인출금액",
            "해지신청",
            "설정신청",
            "입금액",
            "출금액",
            "투입금액",
            "인출금액",
            "매입금액",
            "환매금액",
            "buy",
            "sell",
            "subscription",
            "redemption",
        )
        for segment in self._label_segments(label):
            if any(keyword in segment for keyword in self.NON_AMOUNT_KEYWORDS):
                continue
            if any(keyword in segment for keyword in explicit_keywords):
                return True
        return False

    def _is_execution_amount_label(self, label: str) -> bool:
        """헤더가 순유입/결제/정산처럼 net execution 컬럼인지 판별한다."""
        execution_keywords = (
            "순유입",
            "순투입",
            "증감금액",
            "증감액",
            "정산액",
            "당일이체금액",
            "이체예정금액",
            "실행금액",
            "결제금액",
            "settlement amount",
            "execution amount",
        )
        for segment in self._label_segments(label):
            if any(keyword in segment for keyword in execution_keywords):
                return True
        return False

    def _is_scheduled_amount_label(self, label: str) -> bool:
        """헤더가 T+N/예정/청구 계열 미래 결제 컬럼인지 판별한다."""
        lower_label = label.lower()
        if any(keyword in lower_label for keyword in self.NON_AMOUNT_KEYWORDS):
            return False
        has_schedule_keyword = any(keyword in lower_label for keyword in ("예정", "청구", "예상", "t+", "t일"))
        has_d_plus_keyword = bool(re.search(r"d\s*\+\s*\d+", lower_label))
        has_business_day_keyword = bool(re.search(r"익+영업일|제\s*\d+\s*영업일", lower_label))
        has_date = bool(re.search(r"\d{4}-\d{2}-\d{2}", lower_label))
        has_amount_keyword = any(
            keyword in lower_label
            for keyword in (
                "금액",
                "amount",
                "투입",
                "인출",
                "입금",
                "출금",
                "buy",
                "sell",
                "subscription",
                "redemption",
            )
        )
        if has_date and not (has_amount_keyword or has_schedule_keyword or has_business_day_keyword or has_d_plus_keyword):
            return False
        return (has_date or has_schedule_keyword or has_business_day_keyword or has_d_plus_keyword) and has_amount_keyword

    @staticmethod
    def _label_segments(label: str) -> list[str]:
        """멀티 헤더를 `/` 기준으로 분해해 비교 친화적인 segment 목록을 만든다."""
        return [part.strip().lower() for part in label.split("/") if part.strip()]

    def _is_zero_amount(self, value: str) -> bool:
        """문자열 금액이 수치적으로 0인지 확인한다."""
        parsed = self._parse_numeric_amount_text(value)
        return parsed == 0.0 if parsed is not None else False

    @staticmethod
    def _parse_numeric_amount_text(value: str) -> float | None:
        """문자열 금액을 숫자로 해석한다."""
        normalized = DocumentLoaderCommonMixin._normalize_document_amount_token(value)
        if not normalized or normalized in {"-", "/", "--"}:
            return None
        try:
            return float(normalized.replace(",", ""))
        except ValueError:
            pass

        for pattern, multiplier in (
            (r"([+-]?\d+(?:\.\d+)?)억", 100_000_000.0),
            (r"([+-]?\d+(?:\.\d+)?)만", 10_000.0),
            (r"([+-]?\d+(?:\.\d+)?)천", 1_000.0),
        ):
            match = re.fullmatch(pattern, normalized)
            if match:
                return float(match.group(1)) * multiplier
        return None

    @staticmethod
    def _normalize_document_amount_token(value: str | None) -> str | None:
        """문서 셀에서 금액 해석에 불필요한 통화 표기를 제거한다."""
        if value is None:
            return None

        normalized = str(value).strip()
        if not normalized:
            return None

        normalized = re.sub(r"(?i)krw", "", normalized)
        normalized = normalized.replace("₩", "").replace("￦", "").replace("원", "")
        normalized = re.sub(r"\s+", "", normalized)
        if not normalized or normalized in {"-", "/", "--"}:
            return None

        if re.fullmatch(r"[+-]?\d[\d,]*(?:\.\d+)?", normalized):
            return normalized
        if re.fullmatch(r"[+-]?\d[\d,]*(?:\.\d+)?(?:억|만|천)", normalized):
            return normalized
        return None

    def _looks_like_non_zero_amount_value(self, value: str) -> bool:
        parsed = self._parse_numeric_amount_text(value)
        return parsed is not None and parsed != 0.0

    @staticmethod
    def _render_text_block(lines: list[str]) -> str:
        """일반 텍스트 블록을 fenced code block 형태로 렌더링한다."""
        text = "\n".join(lines).strip()
        if not text:
            return ""
        return f"```text\n{text}\n```"

    @staticmethod
    def _escape_markdown_cell(value: str) -> str:
        """markdown table cell 안에서 `|`가 깨지지 않도록 escape 한다."""
        return value.replace("|", "\\|").strip()

    @classmethod
    def _is_total_row(cls, row: list[str]) -> bool:
        """행 전체가 총계/소계 성격이면 True를 반환한다."""
        non_empty = [cell for cell in row if cell]
        if not non_empty:
            return False
        leading_cells = [cell.strip().lower() for cell in non_empty[:3]]
        return any(cls._is_total_like_text(cell) for cell in leading_cells)

    @staticmethod
    def _is_total_like_text(value: str) -> bool:
        """문자열이 총계/합계/소계류 라벨인지 확인한다."""
        normalized = value.strip().lower()
        if not normalized:
            return False
        if normalized in {"total", "총 계", "총계", "설정 합계", "해지 합계"}:
            return True
        if "subtotal" in normalized:
            return True
        if normalized.endswith("소계") or normalized.endswith("합계"):
            return True
        if re.search(r"\d+\s*개\s*펀드", normalized):
            return True

        bracket_stripped = normalized.strip("[](){}<>")
        if bracket_stripped and bracket_stripped.endswith("계"):
            return True

        return False
