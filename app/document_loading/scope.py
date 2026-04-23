from __future__ import annotations

import re

from .dto import TargetFundScope


def normalize_fund_name_key(value: str | None) -> str:
    """펀드명 비교용 느슨한 정규화 키를 만든다."""
    if value is None:
        return ""
    lowered = value.lower()
    return re.sub(r"[^0-9a-z가-힣]+", "", lowered)


class DocumentLoaderScopeMixin:
    @staticmethod
    def scope_excludes_all_funds(target_fund_scope: TargetFundScope) -> bool:
        return (
            target_fund_scope.manager_column_present
            and not target_fund_scope.include_all_funds
            and not target_fund_scope.fund_codes
            and not target_fund_scope.fund_names
        )

    def extract_target_fund_scope(self, raw_text: str) -> TargetFundScope:
        sections = [section.strip() for section in raw_text.split(self.RAW_SECTION_DELIMITER) if section.strip()]
        header_scope_candidates: list[tuple[int, int, bool]] = []
        manager_column_present = False
        manager_info_present = False
        fund_codes: set[str] = set()
        fund_names: set[str] = set()
        canonical_fund_names: set[str] = set()
        pending_header_scope: bool | None = None

        for section_index, section in enumerate(sections, start=1):
            lines = [line.rstrip() for line in section.splitlines()]
            header_scope_is_target = self._extract_manager_scope_from_lines(lines)
            if header_scope_is_target is not None:
                manager_info_present = True
                header_scope_candidates.append(
                    (
                        self._score_manager_scope_section(lines),
                        section_index,
                        header_scope_is_target,
                    )
                )
            effective_header_scope = header_scope_is_target if header_scope_is_target is not None else pending_header_scope
            cursor = 1 if lines and lines[0].startswith("[") else 0
            section_had_pipe_block = False

            while cursor < len(lines):
                if not self._is_pipe_row(lines[cursor]):
                    cursor += 1
                    continue

                section_had_pipe_block = True
                next_cursor = cursor
                while next_cursor < len(lines) and self._is_pipe_row(lines[next_cursor]):
                    next_cursor += 1
                block_has_manager = self._collect_target_funds_from_pipe_block(
                    lines[cursor:next_cursor],
                    fund_codes=fund_codes,
                    fund_names=fund_names,
                    canonical_fund_names=canonical_fund_names,
                )
                manager_column_present = manager_column_present or block_has_manager
                manager_info_present = manager_info_present or block_has_manager
                if effective_header_scope and not block_has_manager:
                    self._collect_all_funds_from_pipe_block(
                        lines[cursor:next_cursor],
                        fund_codes=fund_codes,
                        fund_names=fund_names,
                        canonical_fund_names=canonical_fund_names,
                    )
                cursor = next_cursor

            if header_scope_is_target is not None:
                pending_header_scope = header_scope_is_target if not section_had_pipe_block else None
            elif section_had_pipe_block:
                pending_header_scope = None

        if fund_codes or fund_names or canonical_fund_names:
            return TargetFundScope(
                manager_column_present=manager_info_present,
                include_all_funds=False,
                fund_codes=frozenset(fund_codes),
                fund_names=frozenset(fund_names),
                canonical_fund_names=frozenset(canonical_fund_names),
            )

        if header_scope_candidates:
            _, _, selected_scope = max(header_scope_candidates)
            return TargetFundScope(
                manager_column_present=True,
                include_all_funds=selected_scope,
                fund_codes=frozenset(),
                fund_names=frozenset(),
                canonical_fund_names=frozenset(),
            )

        if manager_column_present:
            return TargetFundScope(
                manager_column_present=True,
                include_all_funds=False,
                fund_codes=frozenset(),
                fund_names=frozenset(),
                canonical_fund_names=frozenset(),
            )

        return TargetFundScope(
            manager_column_present=manager_info_present,
            include_all_funds=True,
            fund_codes=frozenset(fund_codes),
            fund_names=frozenset(fund_names),
            canonical_fund_names=frozenset(canonical_fund_names),
        )

    def _extract_manager_scope_from_lines(self, lines: list[str]) -> bool | None:
        pattern = re.compile(
            r"^\s*(?:external fund manager|운용사명|운용사|manager)\s*[:：=]\s*(.+?)\s*$",
            flags=re.IGNORECASE,
        )
        for line in lines:
            if self._is_pipe_row(line):
                continue
            match = pattern.match(line.strip())
            if not match:
                continue
            manager_value = match.group(1).strip()
            if not manager_value:
                continue
            return self.TARGET_MANAGER_KEYWORD in manager_value
        return None

    def _score_manager_scope_section(self, lines: list[str]) -> int:
        joined = "\n".join(lines).lower()
        pipe_row_bonus = sum(4 for line in lines if self._is_pipe_row(line))
        keyword_bonus = sum(
            3
            for keyword in (
                "설정",
                "해지",
                "입금",
                "출금",
                "투입",
                "인출",
                "buy",
                "sell",
                "subscription",
                "redemption",
                "결제",
                "settlement",
                "기준일",
                "date",
            )
            if keyword in joined
        )
        date_bonus = len(re.findall(r"\d{4}[-./]\d{1,2}[-./]\d{1,2}", joined)) * 2
        quoted_penalty = sum(
            15
            for keyword in (
                "forwarded message",
                "original message",
                "subject:",
                "from:",
                "sent:",
                "to:",
            )
            if keyword in joined
        )
        return pipe_row_bonus + keyword_bonus + min(date_bonus, 12) - quoted_penalty

    def _collect_target_funds_from_pipe_block(
        self,
        rows: list[str],
        *,
        fund_codes: set[str],
        fund_names: set[str],
        canonical_fund_names: set[str],
    ) -> bool:
        parsed_rows = [self._split_pipe_row(row) for row in rows]
        if len(parsed_rows) <= 1:
            return False

        normalized_rows = self._normalize_pipe_rows(parsed_rows)
        table_structure = self._infer_table_structure(normalized_rows)
        if table_structure is None:
            return False

        _, header_start, data_start = table_structure
        if data_start <= header_start or not normalized_rows[header_start:data_start]:
            return False

        header = self._collapse_header_rows(normalized_rows[header_start:data_start])
        body_rows = normalized_rows[data_start:]
        manager_index = self._manager_column_index(header)
        if manager_index is None:
            return False

        code_index = self._fund_code_column_index(header, body_rows)
        name_index = self._fund_name_column_index(header)
        if code_index is None and name_index is None:
            return False
        if not any(
            self._is_explicit_order_amount_label(label)
            or self._is_execution_amount_label(label)
            or self._is_scheduled_amount_label(label)
            or self._is_order_amount_label(label)
            for label in header
        ):
            return False

        has_likely_data_row = False
        has_meaningful_manager_value = False

        for row in body_rows:
            if not self._is_likely_data_row(row):
                continue
            has_likely_data_row = True
            manager_value = row[manager_index].strip() if manager_index < len(row) else ""
            if self._has_meaningful_manager_value(manager_value):
                has_meaningful_manager_value = True
            if self.TARGET_MANAGER_KEYWORD not in manager_value:
                continue

            fund_code = row[code_index].strip() if code_index is not None and code_index < len(row) else ""
            fund_name = row[name_index].strip() if name_index is not None and name_index < len(row) else ""
            if self._is_total_like_text(fund_code) or self._is_total_like_text(fund_name):
                continue
            if fund_code:
                fund_codes.add(fund_code)
            if fund_name:
                fund_names.add(fund_name)
                canonical_fund_names.add(normalize_fund_name_key(fund_name))

        return has_likely_data_row and has_meaningful_manager_value

    def _collect_all_funds_from_pipe_block(
        self,
        rows: list[str],
        *,
        fund_codes: set[str],
        fund_names: set[str],
        canonical_fund_names: set[str],
    ) -> None:
        parsed_rows = [self._split_pipe_row(row) for row in rows]
        if len(parsed_rows) <= 1:
            return

        normalized_rows = self._normalize_pipe_rows(parsed_rows)
        table_structure = self._infer_table_structure(normalized_rows)
        if table_structure is None:
            return

        _, header_start, data_start = table_structure
        if data_start <= header_start or not normalized_rows[header_start:data_start]:
            return

        header = self._collapse_header_rows(normalized_rows[header_start:data_start])
        body_rows = normalized_rows[data_start:]
        code_index = self._fund_code_column_index(header, body_rows)
        name_index = self._fund_name_column_index(header)
        if code_index is None and name_index is None:
            return
        if not any(
            self._is_explicit_order_amount_label(label)
            or self._is_execution_amount_label(label)
            or self._is_scheduled_amount_label(label)
            or self._is_order_amount_label(label)
            for label in header
        ):
            return

        for row in body_rows:
            if not self._is_likely_data_row(row):
                continue
            fund_code = row[code_index].strip() if code_index is not None and code_index < len(row) else ""
            fund_name = row[name_index].strip() if name_index is not None and name_index < len(row) else ""
            if self._is_total_like_text(fund_code) or self._is_total_like_text(fund_name):
                continue
            if fund_code:
                fund_codes.add(fund_code)
            if fund_name:
                fund_names.add(fund_name)
                canonical_fund_names.add(normalize_fund_name_key(fund_name))

    def _row_fund_code_value(self, row: list[str], code_index: int | None) -> str:
        if code_index is None or code_index >= len(row):
            return ""
        value = row[code_index].strip()
        if not value or self._is_total_like_text(value):
            return ""
        return value if self._looks_like_fund_code(value) else ""

    def _row_fund_name_value(self, row: list[str], name_index: int | None) -> str:
        if name_index is None or name_index >= len(row):
            return ""
        value = row[name_index].strip()
        if not value or self._looks_like_fund_code(value) or self._is_total_like_text(value):
            return ""
        return value

    def _manager_column_index(self, header: list[str]) -> int | None:
        preferred_keywords = (
            "운용사명",
            "external fund manager",
            "manager name",
        )
        excluded_keywords = ("코드", "code", "id")

        for index, label in enumerate(header):
            segments = self._label_segments(label)
            if any(any(self._segment_contains_keyword(segment, keyword) for keyword in preferred_keywords) for segment in segments):
                return index

        for index, label in enumerate(header):
            segments = self._label_segments(label)
            if not any(any(self._segment_contains_keyword(segment, keyword) for keyword in self.MANAGER_HEADER_KEYWORDS) for segment in segments):
                continue
            if any(any(self._segment_contains_keyword(segment, keyword) for keyword in excluded_keywords) for segment in segments):
                continue
            return index
        return None

    def _fund_code_column_index(self, header: list[str], body_rows: list[list[str]] | None = None) -> int | None:
        preferred_keywords = ("펀드코드", "fund code")
        fallback_keywords = ("수탁코드", "code")

        for keywords in (preferred_keywords, fallback_keywords):
            for index, label in enumerate(header):
                segments = self._label_segments(label)
                if any(any(self._segment_contains_keyword(segment, keyword) for keyword in keywords) for segment in segments):
                    return index

        if body_rows:
            manager_index = self._manager_column_index(header)
            candidate_indices: list[int] = []
            if manager_index is not None and manager_index > 0:
                candidate_indices.append(manager_index - 1)
            candidate_indices.extend(
                index
                for index, label in enumerate(header)
                if index not in candidate_indices
                and ("코드" in label or "code" in label.lower())
            )
            for index in candidate_indices:
                if index >= len(header):
                    continue
                values = [row[index].strip() for row in body_rows if index < len(row) and row[index].strip()]
                if values and all(self._looks_like_fund_code(value) for value in values[: min(len(values), 10)]):
                    return index
        return None

    def _fund_name_column_index(self, header: list[str]) -> int | None:
        preferred_keywords = (
            "펀드명",
            "fund name",
        )
        fallback_keywords = (
            "상품명",
            "product name",
            "자산명",
            "asset name",
        )
        for keywords in (preferred_keywords, fallback_keywords):
            for index, label in enumerate(header):
                segments = self._label_segments(label)
                if any(any(self._segment_contains_keyword(segment, keyword) for keyword in keywords) for segment in segments):
                    return index
        return None

    @staticmethod
    def _compact_label(value: str) -> str:
        return re.sub(r"[^0-9a-z가-힣]+", "", value).lower()

    def _segment_contains_keyword(self, segment: str, keyword: str) -> bool:
        lower_segment = segment.lower()
        lower_keyword = keyword.lower()
        if lower_keyword in lower_segment:
            return True
        return self._compact_label(lower_keyword) in self._compact_label(lower_segment)

    @staticmethod
    def _has_meaningful_manager_value(value: str) -> bool:
        normalized = value.strip()
        return bool(normalized and normalized not in {"-", "--", "—", "n/a", "na", "null"})

    @staticmethod
    def _matches_target_fund_scope(
        fund_code: str,
        fund_name: str,
        target_fund_scope: TargetFundScope,
    ) -> bool:
        if not target_fund_scope.manager_column_present or target_fund_scope.include_all_funds:
            return True
        if fund_code and fund_code in target_fund_scope.fund_codes:
            return True
        if fund_name and fund_name in target_fund_scope.fund_names:
            return True
        if fund_name and normalize_fund_name_key(fund_name) in target_fund_scope.canonical_fund_names:
            return True
        return False

    @staticmethod
    def _looks_like_fund_code(value: str) -> bool:
        if not value:
            return False
        if value != value.strip() or " " in value:
            return False
        return bool(re.fullmatch(r"[A-Z0-9]{3,}", value))

    def _looks_like_text_cell(self, value: str) -> bool:
        if not value:
            return False
        if value.strip().lower() in {"true", "false"}:
            return False
        if self._is_amount_string(value):
            return False
        return not self._looks_like_fund_code(value)
