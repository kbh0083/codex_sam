from __future__ import annotations

import logging
import re

from .dto import TargetFundScope
from .scope import normalize_fund_name_key

logger = logging.getLogger(__name__)


class DocumentLoaderCoverageMixin:
    """DocumentLoader coverage/order-bucket helpers."""

    def estimate_order_cell_count(
        self,
        raw_text: str,
        target_fund_scope: TargetFundScope | None = None,
    ) -> int:
        """원문에서 금액이 있는 주문 버킷 수를 대략 추정한다."""
        sections = [section.strip() for section in raw_text.split(self.RAW_SECTION_DELIMITER) if section.strip()]
        bucket_pairs: set[tuple[tuple[str, ...], str]] = set()

        for section in sections:
            lines = [line.rstrip() for line in section.splitlines()]
            cursor = 1 if lines and lines[0].startswith("[") else 0
            section_has_pipe_block = False

            while cursor < len(lines):
                if not self._is_pipe_row(lines[cursor]):
                    cursor += 1
                    continue

                section_has_pipe_block = True
                next_cursor = cursor
                while next_cursor < len(lines) and self._is_pipe_row(lines[next_cursor]):
                    next_cursor += 1
                bucket_pairs.update(
                    self._collect_pipe_block_order_buckets(
                        lines[cursor:next_cursor],
                        target_fund_scope=target_fund_scope,
                    )
                )
                cursor = next_cursor

            if not section_has_pipe_block:
                bucket_pairs.update(
                    self._collect_plain_text_order_buckets(
                        lines,
                        target_fund_scope=target_fund_scope,
                    )
                )

        estimated = len(bucket_pairs)
        logger.info("Estimated %s amount-bearing order cell(s) from raw text", estimated)
        return estimated

    def _collect_pipe_block_order_buckets(
        self,
        rows: list[str],
        target_fund_scope: TargetFundScope | None = None,
    ) -> set[tuple[tuple[str, ...], str]]:
        """Count settlement buckets inside one pipe-delimited block."""
        parsed_rows = [self._split_pipe_row(row) for row in rows]
        if len(parsed_rows) <= 1:
            return set()

        normalized_rows = self._normalize_pipe_rows(parsed_rows)
        table_structure = self._infer_table_structure(normalized_rows)
        if table_structure is None:
            return set()

        _, header_start, data_start = table_structure
        if data_start <= header_start:
            return set()
        if not normalized_rows[header_start:data_start]:
            return set()
        header = self._collapse_header_rows(normalized_rows[header_start:data_start])
        body_rows = normalized_rows[data_start:]
        propagated_body_rows = self._propagate_contextual_body_rows(header, body_rows)
        identity_indices = self._identity_column_indices(header, body_rows)
        bucket_pairs: set[tuple[tuple[str, ...], str]] = set()
        code_index = self._fund_code_column_index(header, body_rows)
        name_index = self._fund_name_column_index(header)

        for row_index, row in enumerate(body_rows):
            propagated_row = propagated_body_rows[row_index]
            if not (
                self._is_likely_data_row(row)
                or self._is_short_continuation_row(row, header)
                or self._is_order_subtotal_row(row, propagated_row, header)
            ):
                continue
            bucket_keys = self._order_bucket_keys_for_row(propagated_row, header, original_row=row)
            if not bucket_keys:
                continue

            fund_code = self._row_fund_code_value(propagated_row, code_index)
            fund_name = self._row_fund_name_value(propagated_row, name_index)
            merged_identity_key, merged_code, merged_name = self._coverage_identity_key(
                body_rows=propagated_body_rows,
                row_index=row_index,
                row=propagated_row,
                header=header,
                identity_indices=identity_indices,
                code_index=code_index,
                name_index=name_index,
                bucket_keys=bucket_keys,
            )

            if target_fund_scope is not None and target_fund_scope.manager_column_present:
                if not self._matches_target_fund_scope(fund_code, fund_name, target_fund_scope):
                    if not self._matches_target_fund_scope(merged_code, merged_name, target_fund_scope):
                        continue

            identity_key = merged_identity_key
            if identity_key is None:
                continue

            for bucket_key in bucket_keys:
                bucket_pairs.add((identity_key, bucket_key))

        return bucket_pairs

    def _collect_plain_text_order_buckets(
        self,
        lines: list[str],
        target_fund_scope: TargetFundScope | None = None,
    ) -> set[tuple[tuple[str, ...], str]]:
        """plain-text PDF 본문에서 section + fund_code + amount 기준 coverage를 센다."""
        flattened_schedule_pairs = self._collect_flattened_schedule_order_buckets(
            lines,
            target_fund_scope=target_fund_scope,
        )
        if flattened_schedule_pairs:
            return flattened_schedule_pairs

        bucket_pairs: set[tuple[tuple[str, ...], str]] = set()
        current_section_key: str | None = None
        current_header_line: str | None = None

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue

            section_key = self._plain_text_order_section_key(stripped)
            if section_key is not None:
                current_section_key = section_key
                current_header_line = None
                continue

            if current_section_key is None:
                continue
            if self._is_plain_text_order_header_line(stripped):
                current_header_line = stripped
                continue
            if self._is_plain_text_total_line(stripped):
                continue

            parsed_line = self._parse_plain_text_order_line(stripped, header_line=current_header_line)
            if parsed_line is None:
                continue

            fund_code, fund_name, amount_value, tail_context = parsed_line
            if target_fund_scope is not None and target_fund_scope.manager_column_present:
                if not self._matches_target_fund_scope(fund_code, fund_name, target_fund_scope):
                    continue

            identity_key = self._plain_text_order_identity_key(
                fund_code,
                fund_name,
                amount_value,
                tail_context=tail_context,
            )
            if identity_key is None:
                continue
            bucket_pairs.add((identity_key, current_section_key))

        return bucket_pairs

    def _collect_flattened_schedule_order_buckets(
        self,
        lines: list[str],
        *,
        target_fund_scope: TargetFundScope | None = None,
    ) -> set[tuple[tuple[str, ...], str]]:
        """flattened schedule 표에서 coverage용 주문 버킷을 수집한다."""
        parsed = self._parse_flattened_schedule_block(lines)
        if parsed is None:
            return set()

        header, rows = parsed
        code_index = self._fund_code_column_index(header, rows)
        name_index = self._fund_name_column_index(header)
        bucket_pairs: set[tuple[tuple[str, ...], str]] = set()

        for row in rows:
            fund_code = self._row_fund_code_value(row, code_index)
            fund_name = self._row_fund_name_value(row, name_index)
            if target_fund_scope is not None and target_fund_scope.manager_column_present:
                if not self._matches_target_fund_scope(fund_code, fund_name, target_fund_scope):
                    continue

            identity_key = tuple(value for value in (fund_code, fund_name) if value)
            if not identity_key:
                continue

            for column_index, cell in enumerate(row):
                if column_index >= len(header):
                    continue
                label = header[column_index]
                if not self._is_markdown_relevant_amount_label(label):
                    continue
                if not self._is_amount_string(cell) or self._is_zero_amount(cell):
                    continue
                bucket_pairs.add((identity_key, self._order_bucket_key(label, column_index)))

        return bucket_pairs

    def _parse_flattened_schedule_block(self, lines: list[str]) -> tuple[list[str], list[list[str]]] | None:
        """plain-text로 풀린 schedule 표를 markdown/coverage용 행렬로 복원한다."""
        value_header_index: int | None = None
        date_header_index: int | None = None
        for index in range(len(lines) - 1):
            if not self._looks_like_flattened_schedule_value_header(lines[index]):
                continue
            if not self._looks_like_flattened_schedule_date_header(lines[index + 1]):
                continue
            value_header_index = index
            date_header_index = index + 1
            break
        if value_header_index is None or date_header_index is None:
            return None

        amount_headers = self._flattened_schedule_amount_headers(lines[value_header_index])
        date_headers = re.findall(r"\d{4}-\d{2}-\d{2}", lines[date_header_index])
        if not amount_headers or not date_headers:
            return None

        selected_amount_headers = list(amount_headers)
        if any(
            self._is_execution_amount_label(label) or self._is_scheduled_amount_label(label)
            for label in amount_headers
        ):
            selected_amount_headers = [
                label
                for label in amount_headers
                if self._is_execution_amount_label(label) or self._is_scheduled_amount_label(label)
            ]

        scheduled_headers = [f"{date_label} 예정금액" for date_label in date_headers]
        header = ["통합펀드코드", "서브펀드코드", "펀드명", "운용사", *selected_amount_headers, *scheduled_headers]
        selected_amount_indices = [amount_headers.index(label) for label in selected_amount_headers]
        numeric_tail_count = len(amount_headers) + len(date_headers)
        rows: list[list[str]] = []

        for line in lines[date_header_index + 1 :]:
            parsed_row = self._parse_flattened_schedule_data_row(
                line,
                numeric_tail_count=numeric_tail_count,
                amount_headers=amount_headers,
                selected_amount_indices=selected_amount_indices,
                date_count=len(date_headers),
            )
            if parsed_row is None:
                continue
            rows.append(parsed_row)

        if not rows:
            return None
        return header, rows

    def _looks_like_flattened_schedule_value_header(self, line: str) -> bool:
        """flattened schedule의 값 헤더 줄인지 판별한다."""
        lowered = line.lower()
        return (
            "펀드명" in line
            and "운용사" in line
            and any(keyword in lowered for keyword in ("당일이체금액", "입금액", "출금액", "settlement", "amount"))
        )

    def _looks_like_flattened_schedule_date_header(self, line: str) -> bool:
        """flattened schedule의 날짜 헤더 줄인지 판별한다."""
        return line.count("펀드코드") >= 1 and len(re.findall(r"\d{4}-\d{2}-\d{2}", line)) >= 1

    def _flattened_schedule_amount_headers(self, line: str) -> list[str]:
        """값 헤더 줄에서 실제 금액성 컬럼 이름만 순서대로 추출한다."""
        pattern = re.compile(
            r"(입금액|출금액|설정금액|해지금액|당일이체좌수|당일이체금액|이체예정금액|결제금액|순유입금액|순투입금액|증감금액|증감액)",
            flags=re.IGNORECASE,
        )
        return [match.group(1) for match in pattern.finditer(line)]

    def _parse_flattened_schedule_data_row(
        self,
        line: str,
        *,
        numeric_tail_count: int,
        amount_headers: list[str],
        selected_amount_indices: list[int],
        date_count: int,
    ) -> list[str] | None:
        """flattened schedule 데이터 줄을 표 행 형태로 복원한다."""
        tokens = line.split()
        if len(tokens) < numeric_tail_count + 3:
            return None
        if self._is_plain_text_total_line(line):
            return None
        if not self._looks_like_fund_code(tokens[0]):
            return None

        numeric_tail = tokens[-numeric_tail_count:]
        if not all(self._is_amount_string(token) for token in numeric_tail):
            return None

        prefix_tokens = tokens[:-numeric_tail_count]
        if len(prefix_tokens) < 3:
            return None

        integrated_code = prefix_tokens[0]
        manager = prefix_tokens[-1]
        has_sub_code = len(prefix_tokens) >= 4 and self._looks_like_fund_code(prefix_tokens[1])
        sub_code = prefix_tokens[1] if has_sub_code else ""
        name_start = 2 if has_sub_code else 1
        fund_name = " ".join(prefix_tokens[name_start:-1]).strip()
        if not fund_name:
            return None

        amount_values = numeric_tail[: len(amount_headers)]
        date_values = numeric_tail[len(amount_headers) : len(amount_headers) + date_count]
        selected_amount_values = [amount_values[index] for index in selected_amount_indices]
        return [integrated_code, sub_code, fund_name, manager, *selected_amount_values, *date_values]

    def _plain_text_order_section_key(self, line: str) -> str | None:
        """plain-text 줄이 어떤 주문 섹션(T0 SUB/RED)을 뜻하는지 정규화한다."""
        stripped = line.strip()
        normalized = self._normalize_plain_text_section_label(stripped)
        if normalized in {"subscription", "buy", "설정", "입금", "투입", "매입"}:
            return "T0_SUB"
        if normalized in {"redemption", "sell", "해지", "출금", "인출", "환매"}:
            return "T0_RED"
        return None

    def _is_plain_text_order_header_line(self, line: str) -> bool:
        """plain-text 주문 본문의 헤더 줄인지 휴리스틱으로 판별한다."""
        lower_line = line.lower()
        if "the order of subscription and redemption" in lower_line:
            return True
        keyword_hits = sum(
            1
            for keyword in (
                "fund",
                "code",
                "unit",
                "nav",
                "date",
                "amount",
                "bank",
                "펀드",
                "코드",
                "좌수",
                "날짜",
                "금액",
                "결제일",
                "기준일",
                "은행",
            )
            if keyword in lower_line
        )
        return keyword_hits >= 4

    def _is_plain_text_total_line(self, line: str) -> bool:
        """plain-text 본문의 합계/총계 줄인지 확인한다."""
        tokens = line.split()
        if not tokens:
            return False
        return self._is_total_like_text(tokens[0])

    def _parse_plain_text_order_line(
        self,
        line: str,
        *,
        header_line: str | None = None,
    ) -> tuple[str, str, str, tuple[str, ...]] | None:
        """plain-text 주문 데이터 줄을 `(code, name, amount, tail_context)`로 파싱한다."""
        if not self._looks_like_plain_text_order_data_line(line):
            return None

        tokens = line.split()
        if len(tokens) < 4:
            return None

        amount_index = self._plain_text_order_amount_index(tokens, header_line=header_line)
        if amount_index is None:
            return None

        amount_value = tokens[amount_index]
        if not self._is_amount_string(amount_value) or self._is_zero_amount(amount_value):
            return None

        code_index = self._plain_text_fund_code_index(tokens, amount_index)
        if code_index is None:
            return None
        fund_code = tokens[code_index]

        fund_name = " ".join(tokens[:code_index]).strip()
        if not fund_name or self._is_total_like_text(fund_name):
            return None

        tail_context = self._plain_text_tail_context(tokens, amount_index)
        return fund_code, fund_name, amount_value, tail_context

    def _looks_like_plain_text_order_data_line(self, line: str) -> bool:
        """plain-text 한 줄이 실제 주문 데이터행처럼 보이는지 판별한다."""
        if self._looks_like_pdf_data_line(line):
            return True

        if self._is_plain_text_total_line(line) or self._is_plain_text_order_header_line(line):
            return False

        tokens = line.split()
        if len(tokens) < 3:
            return False

        has_code = any(self._looks_like_fund_code(token) for token in tokens)
        if not has_code:
            return False

        has_non_zero_amount = any(self._is_amount_string(token) and not self._is_zero_amount(token) for token in tokens)
        if not has_non_zero_amount:
            return False

        return self._plain_text_first_date_token_index(tokens) is not None or has_non_zero_amount

    def _plain_text_fund_code_index(self, tokens: list[str], amount_index: int) -> int | None:
        """plain-text 주문 줄에서 fund code가 위치한 인덱스를 추정한다."""
        date_index = self._plain_text_first_date_token_index(tokens)
        search_end = date_index if date_index is not None else amount_index

        for index in range(search_end - 1, -1, -1):
            token = tokens[index]
            if self._looks_like_fund_code(token):
                return index

        first_numeric_index = self._plain_text_first_numeric_token_index(tokens)
        if first_numeric_index is None or first_numeric_index <= 0:
            return None
        fallback_index = first_numeric_index - 1
        return fallback_index if self._looks_like_fund_code(tokens[fallback_index]) else None

    def _plain_text_order_amount_index(
        self,
        tokens: list[str],
        *,
        header_line: str | None = None,
    ) -> int | None:
        """plain-text 주문 줄에서 실제 주문 금액 토큰 인덱스를 찾는다."""
        date_index = self._plain_text_first_date_token_index(tokens)
        if date_index is not None:
            post_date_numeric_indices = [
                index
                for index in range(date_index + 1, len(tokens))
                if self._is_amount_string(tokens[index]) and not self._is_zero_amount(tokens[index])
            ]
            header_has_nav = self._plain_text_header_mentions_nav(header_line)
            header_has_amount = self._plain_text_header_mentions_amount(header_line)
            if len(post_date_numeric_indices) >= 2:
                return post_date_numeric_indices[-1]
            if len(post_date_numeric_indices) == 1:
                if self._is_iso_like_date_token(tokens[date_index]) and header_has_amount and not header_has_nav:
                    return post_date_numeric_indices[0]
                return None

        for index in range(len(tokens) - 1, -1, -1):
            token = tokens[index]
            if not self._is_amount_string(token):
                continue
            if self._is_zero_amount(token):
                continue
            return index
        return None

    def _plain_text_first_numeric_token_index(self, tokens: list[str]) -> int | None:
        """plain-text 토큰 목록에서 첫 숫자/날짜 토큰 위치를 찾는다."""
        for index, token in enumerate(tokens):
            if self._is_amount_string(token) or re.fullmatch(r"\d{1,2}-[A-Za-z]{3}-\d{2}", token):
                return index
        return None

    @staticmethod
    def _plain_text_first_date_token_index(tokens: list[str]) -> int | None:
        """토큰 목록에서 첫 날짜 토큰 위치를 찾는다."""
        for index, token in enumerate(tokens):
            if (
                re.fullmatch(r"\d{1,2}-[A-Za-z]{3}-\d{2}", token)
                or re.fullmatch(r"\d{4}[-/.]\d{2}[-/.]\d{2}", token)
            ):
                return index
        return None

    @staticmethod
    def _is_iso_like_date_token(token: str) -> bool:
        """토큰이 `YYYY-MM-DD` 계열 날짜 형식인지 확인한다."""
        return bool(re.fullmatch(r"\d{4}[-/.]\d{2}[-/.]\d{2}", token))

    @staticmethod
    def _normalize_plain_text_section_label(value: str) -> str:
        """번호/구두점을 제거한 plain-text 섹션 비교용 키를 만든다."""
        stripped = value.strip()
        numbered_match = re.fullmatch(r"\d+\.\s*(.+)", stripped)
        if numbered_match:
            stripped = numbered_match.group(1).strip()
        lowered = stripped.lower()
        return re.sub(r"[\s\[\](){}<>:.\-_/]+", "", lowered)

    @staticmethod
    def _plain_text_order_identity_key(
        fund_code: str,
        fund_name: str,
        amount_value: str,
        *,
        tail_context: tuple[str, ...] = (),
    ) -> tuple[str, ...] | None:
        """plain-text coverage dedupe용 identity key를 만든다."""
        if fund_code:
            return (fund_code, amount_value, *tail_context)
        normalized_name = normalize_fund_name_key(fund_name)
        if normalized_name:
            return (normalized_name, amount_value, *tail_context)
        return None

    @staticmethod
    def _plain_text_tail_context(tokens: list[str], amount_index: int) -> tuple[str, ...]:
        """금액 뒤 후행 토큰을 identity 보조 문맥으로 보관한다."""
        trailing_tokens = [token.strip() for token in tokens[amount_index + 1 :] if token.strip()]
        if not trailing_tokens:
            return ()
        return tuple(trailing_tokens)

    @staticmethod
    def _plain_text_header_mentions_nav(header_line: str | None) -> bool:
        """헤더가 NAV/기준가 열을 명시하는지 확인한다."""
        if not header_line:
            return False
        lower_header = header_line.lower()
        return any(keyword in lower_header for keyword in ("nav", "기준가", "price"))

    @staticmethod
    def _plain_text_header_mentions_amount(header_line: str | None) -> bool:
        """헤더가 주문 금액 열을 명시하는지 확인한다."""
        if not header_line:
            return False
        lower_header = header_line.lower()
        return any(keyword in lower_header for keyword in ("amount", "금액", "settlement", "execution", "결제"))

    def _order_bucket_keys_for_row(
        self,
        row: list[str],
        header: list[str],
        *,
        original_row: list[str] | None = None,
    ) -> set[str]:
        """한 행이 대표하는 주문 버킷(T0, T+1, T0_SUB 등)을 계산한다."""
        bucket_keys: set[str] = set()
        has_order_context_header = any(self._is_order_context_label(label) for label in header)
        reference_row = original_row if original_row is not None else row
        preferred_contextual_columns = self._preferred_row_context_amount_columns_for_row(
            row=row,
            header=header,
            has_order_context_header=has_order_context_header,
        )
        execution_bucket_keys = self._execution_bucket_keys_for_row(
            row=row,
            header=header,
            ignored_buckets=set(preferred_contextual_columns),
        )
        table_has_unmixed_amount_label = any(
            (
                self._is_core_order_amount_label(label)
                or self._is_explicit_order_amount_label(label)
                or self._is_execution_amount_label(label)
                or self._is_scheduled_amount_label(label)
                or self._is_order_amount_label(label)
                or self._is_plain_generic_amount_label(label)
                or self._is_contextual_generic_amount_label(label, has_order_context_header)
            )
            and not self._is_mixed_unit_amount_label(label)
            for label in header
        )
        for column_index, cell in enumerate(row):
            if not self._looks_like_non_zero_amount_value(cell):
                continue
            label = header[column_index] if column_index < len(header) else ""
            if not label:
                continue
            bucket_key = self._order_bucket_key(label, column_index)
            bucket_key = self._pending_request_bucket_key_for_row(
                label=label,
                row=row,
                header=header,
                column_index=column_index,
            ) or bucket_key
            preferred_indices = preferred_contextual_columns.get(bucket_key)
            if preferred_indices is not None and column_index not in preferred_indices:
                continue
            if not (
                self._is_core_order_amount_label(label)
                or self._is_explicit_order_amount_label(label)
                or self._is_execution_amount_label(label)
                or self._is_scheduled_amount_label(label)
                or self._is_order_amount_label(label)
                or self._is_plain_generic_amount_label(label)
                or self._is_contextual_generic_amount_label(label, has_order_context_header)
            ):
                continue
            if self._is_mixed_unit_amount_label(label) and not self._row_has_supporting_amount_signal(
                row=row,
                header=header,
                exclude_index=column_index,
            ):
                if table_has_unmixed_amount_label and not self._is_short_continuation_shape(reference_row, header):
                    continue
            if bucket_key not in execution_bucket_keys:
                explicit_direction = self._explicit_bucket_direction(label=label, row=row, header=header)
                if explicit_direction is not None:
                    bucket_key = f"{bucket_key}_{explicit_direction}"
            bucket_keys.add(bucket_key)
        return bucket_keys

    def _execution_bucket_keys_for_row(
        self,
        *,
        row: list[str],
        header: list[str],
        ignored_buckets: set[str] | None = None,
    ) -> set[str]:
        """현재 행에서 실제 non-zero execution/net 금액이 있는 결제 버킷만 모은다."""
        bucket_keys: set[str] = set()
        ignored_buckets = ignored_buckets or set()
        for column_index, cell in enumerate(row):
            if not self._looks_like_non_zero_amount_value(cell):
                continue
            if column_index >= len(header):
                continue
            label = header[column_index]
            if self._is_execution_amount_label(label):
                bucket_key = self._pending_request_bucket_key_for_row(
                    label=label,
                    row=row,
                    header=header,
                    column_index=column_index,
                ) or self._order_bucket_key(label, column_index)
                if bucket_key in ignored_buckets:
                    continue
                bucket_keys.add(bucket_key)
        return bucket_keys

    def _preferred_row_context_amount_columns_for_row(
        self,
        *,
        row: list[str],
        header: list[str],
        has_order_context_header: bool,
    ) -> dict[str, set[int]]:
        """행 구분 컬럼이 있을 때 authoritative mixed amount 열만 남긴다."""
        if not has_order_context_header:
            return {}

        best_priority_by_bucket: dict[str, int] = {}
        preferred_indices_by_bucket: dict[str, set[int]] = {}

        for column_index, cell in enumerate(row):
            if column_index >= len(header) or not self._looks_like_non_zero_amount_value(cell):
                continue
            label = header[column_index]
            priority = self._row_context_amount_priority(label)
            if priority is None:
                continue
            bucket_key = self._pending_request_bucket_key_for_row(
                label=label,
                row=row,
                header=header,
                column_index=column_index,
            ) or self._order_bucket_key(label, column_index)
            current_best = best_priority_by_bucket.get(bucket_key)
            if current_best is None or priority < current_best:
                best_priority_by_bucket[bucket_key] = priority
                preferred_indices_by_bucket[bucket_key] = {column_index}
            elif priority == current_best:
                preferred_indices_by_bucket.setdefault(bucket_key, set()).add(column_index)

        return preferred_indices_by_bucket

    def _row_context_amount_priority(self, label: str) -> int | None:
        """row-level direction 문맥과 함께 읽어야 하는 mixed amount 헤더 우선순위."""
        best_priority: int | None = None
        normalized_label = self._normalize_plain_text_section_label(label)
        whole_label_adjustment = 0
        if "펀드계" in normalized_label:
            whole_label_adjustment = -10
        elif "투자일임" in normalized_label or "수익증권" in normalized_label:
            whole_label_adjustment = 1
        for segment in self._label_segments(label):
            if not (
                self._looks_like_generic_order_amount_segment(segment)
                or self._is_order_amount_label(segment)
                or self._is_plain_generic_amount_label(segment)
            ):
                continue

            has_sub = any(token in segment for token in ("설정", "입금", "투입", "납입", "매입", "buy", "subscription"))
            has_red = any(token in segment for token in ("해지", "출금", "인출", "환매", "sell", "redemption"))

            priority: int | None = None
            if has_sub and has_red:
                if "설정" in segment and "해지" in segment:
                    priority = 0
                elif "납입" in segment and ("출금" in segment or "인출" in segment):
                    priority = 1
                elif "입금" in segment and ("출금" in segment or "인출" in segment):
                    priority = 2
                elif "투입" in segment and ("출금" in segment or "인출" in segment):
                    priority = 3
                else:
                    priority = 4
            elif self._is_plain_generic_amount_label(segment):
                priority = 10

            if priority is None:
                continue
            priority += whole_label_adjustment
            if best_priority is None or priority < best_priority:
                best_priority = priority

        return best_priority

    def _explicit_bucket_direction(
        self,
        *,
        label: str,
        row: list[str],
        header: list[str],
    ) -> str | None:
        """coverage 집계용 명시적 주문 방향(SUB/RED)을 찾는다."""
        label_direction = self._explicit_direction_from_text(label)
        if label_direction is not None:
            return label_direction

        for column_index, column_label in enumerate(header):
            if not self._is_order_context_label(column_label) or column_index >= len(row):
                continue
            row_direction = self._explicit_direction_from_text(row[column_index])
            if row_direction is not None:
                return row_direction
        return None

    @staticmethod
    def _explicit_direction_from_text(value: str) -> str | None:
        """설정/입금 계열은 SUB, 해지/출금 계열은 RED로 정규화한다."""
        normalized = DocumentLoaderCoverageMixin._normalize_plain_text_section_label(value)
        sub_tokens = ("subscription", "buy", "설정", "입금", "투입", "매입", "납입")
        red_tokens = ("redemption", "sell", "해지", "출금", "인출", "환매")
        has_sub = any(token in normalized for token in sub_tokens)
        has_red = any(token in normalized for token in red_tokens)

        if has_sub and has_red:
            return None
        if has_sub:
            return "SUB"
        if has_red:
            return "RED"
        return None

    def _pending_request_bucket_key_for_row(
        self,
        *,
        label: str,
        row: list[str],
        header: list[str],
        column_index: int,
    ) -> str | None:
        """해지/설정 신청 컬럼의 future bucket을 비고 문맥으로 보정한다."""
        if not self._is_pending_request_label(label):
            return None
        if column_index >= len(row):
            return None
        note_index = self._remarks_column_index(header)
        if note_index is None or note_index >= len(row):
            return None
        note_text = row[note_index].strip()
        if not note_text:
            return None
        amount_value = self._parse_numeric_amount_text(row[column_index])
        return self._bucket_key_from_pending_note(note_text, amount_value)

    def _bucket_key_from_pending_note(self, note_text: str, amount_value: float | None) -> str | None:
        """비고 문구에서 pending amount가 속한 예정일 bucket을 추론한다."""
        matches = list(
            re.finditer(
                r"(?:(?P<amount>[+-]?\d[\d,]*(?:\.\d+)?(?:억|만|천)?)\s*>\s*)?(?P<day>\d{1,2})일",
                note_text,
            )
        )
        if not matches:
            return None

        if amount_value is not None:
            for match in matches:
                amount_text = match.group("amount")
                if not amount_text:
                    continue
                parsed_amount = self._parse_numeric_amount_text(amount_text)
                if parsed_amount is None:
                    continue
                if abs(parsed_amount - amount_value) < 0.5:
                    return f"DAY{int(match.group('day')):02d}"

        unique_days = {int(match.group("day")) for match in matches}
        if len(unique_days) == 1:
            day_value = next(iter(unique_days))
            return f"DAY{day_value:02d}"
        return None

    def _remarks_column_index(self, header: list[str]) -> int | None:
        """헤더에서 비고/remarks 컬럼 위치를 찾는다."""
        for index, label in enumerate(header):
            compact_label = self._compact_label(label)
            lower_label = label.lower()
            if "비고" in compact_label or "remark" in lower_label or "note" in lower_label:
                return index
        return None

    def _is_pending_request_label(self, label: str) -> bool:
        """설정/해지 신청처럼 청구분 future slot을 의미하는 컬럼인지 판별한다."""
        return any(
            any(keyword in segment for keyword in ("해지신청", "설정신청"))
            for segment in self._label_segments(label)
        )

    def _is_mixed_unit_amount_label(self, label: str) -> bool:
        """하나의 헤더에 좌수와 금액이 함께 섞인 mixed 컬럼인지 판별한다."""
        has_unit_segment = any(any(keyword in segment for keyword in self.NON_AMOUNT_KEYWORDS) for segment in self._label_segments(label))
        if not has_unit_segment:
            return False
        return self._is_core_order_amount_label(label) or self._is_explicit_order_amount_label(label) or self._is_execution_amount_label(label) or self._is_scheduled_amount_label(label) or self._is_order_amount_label(label)

    def _row_has_supporting_amount_signal(
        self,
        *,
        row: list[str],
        header: list[str],
        exclude_index: int,
    ) -> bool:
        """mixed 컬럼 외에 실제 금액 컬럼이 같은 행에 더 있는지 확인한다."""
        for column_index, cell in enumerate(row):
            if column_index == exclude_index:
                continue
            if not self._is_amount_string(cell) or self._is_zero_amount(cell):
                continue
            label = header[column_index] if column_index < len(header) else ""
            if not label or self._is_mixed_unit_amount_label(label):
                continue
            if (
                self._is_core_order_amount_label(label)
                or self._is_explicit_order_amount_label(label)
                or self._is_execution_amount_label(label)
                or self._is_scheduled_amount_label(label)
                or self._is_order_amount_label(label)
            ):
                return True
        return False

    def _coverage_identity_key(
        self,
        *,
        body_rows: list[list[str]],
        row_index: int,
        row: list[str],
        header: list[str],
        identity_indices: list[int],
        code_index: int | None,
        name_index: int | None,
        bucket_keys: set[str],
    ) -> tuple[tuple[str, ...] | None, str, str]:
        """coverage dedupe용 identity를 인접 행 보완까지 포함해 계산한다."""
        direct_identity = self._row_identity_key(row, identity_indices)
        code_value = self._row_fund_code_value(row, code_index)
        name_value = self._row_fund_name_value(row, name_index)

        merged_code = code_value
        merged_name = name_value

        if not (merged_code and merged_name):
            for neighbor_index in (row_index - 1, row_index + 1):
                if neighbor_index < 0 or neighbor_index >= len(body_rows):
                    continue
                neighbor_row = body_rows[neighbor_index]
                if self._is_total_row(neighbor_row):
                    continue
                if self._order_bucket_keys_for_row(neighbor_row, header) != bucket_keys:
                    continue
                neighbor_code = self._row_fund_code_value(neighbor_row, code_index)
                neighbor_name = self._row_fund_name_value(neighbor_row, name_index)
                if not neighbor_code and not neighbor_name:
                    continue
                if not merged_code and neighbor_code:
                    merged_code = neighbor_code
                if not merged_name and neighbor_name:
                    merged_name = neighbor_name
                if merged_code and merged_name:
                    break

        if merged_code or merged_name:
            merged_identity = tuple(value for value in (merged_code, merged_name) if value)
            if merged_identity:
                return merged_identity, merged_code, merged_name

        return direct_identity, merged_code, merged_name

    def _identity_column_indices(self, header: list[str], body_rows: list[list[str]]) -> list[int]:
        """주문 버킷 dedupe에 쓸 식별자 컬럼 인덱스를 추론한다."""
        indices = [index for index, label in enumerate(header) if self._is_identity_label(label)]
        indices = self._prune_identity_indices_by_body_content(header, body_rows, indices)
        if indices:
            return indices

        fallback_indices: list[int] = []
        for index, label in enumerate(header):
            if self._is_explicit_order_amount_label(label) or self._is_execution_amount_label(label) or self._is_scheduled_amount_label(label):
                break
            if label and not self._is_order_amount_label(label) and not any(keyword in label.lower() for keyword in self.NON_AMOUNT_KEYWORDS):
                fallback_indices.append(index)
        return fallback_indices

    def _prune_identity_indices_by_body_content(
        self,
        header: list[str],
        body_rows: list[list[str]],
        indices: list[int],
    ) -> list[int]:
        """본문 값이 대부분 금액인 컬럼은 identity 후보에서 제외한다."""
        pruned: list[int] = []
        for index in indices:
            label = header[index] if index < len(header) else ""
            if self._is_strong_identity_label(label):
                pruned.append(index)
                continue

            non_empty_values = [row[index].strip() for row in body_rows if index < len(row) and row[index].strip()]
            if not non_empty_values:
                continue

            amount_like_count = sum(1 for value in non_empty_values if self._is_amount_string(value))
            if amount_like_count / len(non_empty_values) >= 0.6:
                continue

            pruned.append(index)
        return pruned

    def _is_strong_identity_label(self, label: str) -> bool:
        """본문 값이 숫자여도 유지해야 하는 강한 식별자 컬럼인지 판단한다."""
        lower_label = label.lower()
        compact_label = self._compact_label(label)
        if self._contains_auxiliary_amount_keyword(lower_label):
            return False
        return any(
            keyword in lower_label
            for keyword in (
                "fund code",
                "fund name",
                "product name",
                "asset name",
                "manager",
                "custodian",
            )
        ) or any(
            keyword in compact_label
            for keyword in (
                "펀드코드",
                "수탁코드",
                "펀드명",
                "상품명",
                "자산명",
                "운용사",
                "수탁은행",
                "판매사",
                "환매사",
            )
        )

    def _is_identity_label(self, label: str) -> bool:
        """헤더가 주문 dedupe용 식별자 컬럼인지 휴리스틱으로 판별한다."""
        if not label:
            return False
        lower_label = self._semantic_label_for_identity(label)
        compact_label = self._compact_label(lower_label)
        if self._is_explicit_order_amount_label(label) or self._is_execution_amount_label(label) or self._is_scheduled_amount_label(label):
            return False
        if self._is_order_amount_label(label):
            return False
        if self._contains_auxiliary_amount_keyword(label.lower()):
            return False
        if any(keyword in lower_label for keyword in self.NON_AMOUNT_KEYWORDS):
            return False
        if any(
            keyword in lower_label
            for keyword in (
                "transaction",
                "type",
                "content",
                "detail",
                "description",
                "memo",
                "date",
                "settlement",
                "nav",
                "price",
                "custodian",
                "buy&sell",
            )
        ):
            return False
        if any(
            keyword in compact_label
            for keyword in (
                "구분",
                "내용",
                "적요",
                "사유",
                "비고",
                "기준일",
                "결제일",
                "기준가",
                "수탁은행",
                "수익은행",
            )
        ):
            return False
        return True

    @staticmethod
    def _contains_auxiliary_amount_keyword(lower_label: str) -> bool:
        """식별자 컬럼으로 쓰면 안 되는 보조 금액/수수료성 라벨을 걸러낸다."""
        return any(
            keyword in lower_label
            for keyword in (
                "금액",
                "amount",
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
        )

    def _semantic_label_for_identity(self, label: str) -> str:
        """멀티 헤더 collapse 후에도 실제 의미를 가진 마지막 라벨 조각을 고른다."""
        for segment in reversed(self._label_segments(label)):
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", segment):
                continue
            if segment in {"운용지시서", "buy&sell"}:
                continue
            return segment
        return label.lower()

    def _row_identity_key(self, row: list[str], identity_indices: list[int]) -> tuple[str, ...] | None:
        """현재 행에서 dedupe용 identity tuple을 만든다."""
        values = tuple(row[index].strip() for index in identity_indices if index < len(row) and row[index].strip())
        if values:
            return values

        leading_values = tuple(
            cell.strip()
            for cell in row[:5]
            if cell.strip() and not self._is_amount_string(cell) and not any(keyword in cell.lower() for keyword in self.NON_AMOUNT_KEYWORDS)
        )
        return leading_values or None
