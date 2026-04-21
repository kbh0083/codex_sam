from __future__ import annotations

import codecs
import logging
import re
from email import policy
from email.header import decode_header, make_header
from email.message import Message
from email.utils import parsedate_to_datetime
from email.parser import BytesParser
from pathlib import Path
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


class EmlDocumentLoaderMixin:
    """EML 파일에서 메일 본문을 추출해 공통 raw_text 형식으로 변환한다.

    현재 서비스에서 필요한 것은 메일 헤더 전체가 아니라 "지시서가 실제로 적혀 있는 본문"이다.
    따라서 첨부파일까지 재귀적으로 여는 범용 메일 아카이버가 아니라,
    본문 `text/html` 또는 `text/plain` 파트를 안정적으로 추출하는 데 집중한다.
    """

    EML_HEADER_FIELDS = (
        ("Subject", "Subject"),
        ("From", "From"),
        ("To", "To"),
        ("Date", "Date"),
    )
    BUY_SELL_REPORT_TITLE_RE = re.compile(r"buy\s*&\s*sell\s*report", flags=re.IGNORECASE)
    BUY_SELL_DATE_RE = re.compile(r"\d{1,2}-\d{1,2}-\d{4}|\d{4}-\d{1,2}-\d{1,2}")
    BUY_SELL_STOP_TOKENS = ("total", "감사합니다.", "감사합니다", "best regards")
    BUY_SELL_HEADER_HINTS = (
        "date",
        "buy&sell",
        "external fund manager",
        "fund code",
        "fund name",
        "fund price",
        "buy",
        "sell",
        "custodian bank",
        "amount",
        "unit",
    )
    BUY_SELL_FUND_CODE_RE = re.compile(r"[A-Za-z]?\d[\dA-Za-z-]{2,}")
    ORDER_REPORT_KEYWORDS = (
        "buy & sell report",
        "buy&sell",
        "fund code",
        "fund name",
        "subscription",
        "redemption",
        "설정",
        "해지",
        "입금",
        "출금",
        "금액",
        "amount",
    )
    QUOTED_THREAD_KEYWORDS = (
        "original message",
        "forwarded message",
        "from:",
        "sent:",
        "subject:",
        "mailto:",
    )
    # 이전 메일 스레드 경계로 자주 보이는 명시적 separator 들이다.
    # 요구사항상 EML은 "현재 메시지"만 추출 대상이므로, 이런 경계가 보이면
    # 이후 내용은 전부 버린다.
    EML_THREAD_BOUNDARY_MARKERS = (
        "forwarded message",
        "original message",
        "원본 메시지",
        "전달된 메시지",
        "이전 메시지",
    )
    # separator 없이 바로 메일 헤더 블록이 이어지는 경우를 잡기 위한 prefix 다.
    # `From:/Sent:/Subject:` 조합이 반복되면 보통 이전 메시지 시작으로 볼 수 있다.
    EML_REPLY_HEADER_PREFIXES = (
        "from:",
        "sender:",
        "sent:",
        "to:",
        "subject:",
        "title:",
        "date:",
        "cc:",
        "보낸 사람:",
        "보낸사람:",
        "받는 사람:",
        "받는사람:",
        "참조:",
        "제목:",
        "날짜:",
    )
    EML_THREAD_SEPARATOR_RE = re.compile(r"^-{2,}\s*(?:original message|forwarded message).*-{2,}$", flags=re.IGNORECASE)
    EML_REPLY_WROTE_RE = re.compile(r"^on .+ wrote:\s*$", flags=re.IGNORECASE)
    EML_REPLY_DAY_DATE_RE = re.compile(
        r"(mon|tue|wed|thu|fri|sat|sun),?\s+\d{1,2}\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)",
        flags=re.IGNORECASE,
    )
    EML_REPLY_TZ_RE = re.compile(r"[+-]\d{4}")
    EML_REPLY_LOCAL_DATE_RE = re.compile(
        r"\d{4}[-./]\d{1,2}[-./]\d{1,2}(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?",
        flags=re.IGNORECASE,
    )
    EML_REPLY_GMT_RE = re.compile(r"\(?gmt[+-]\d+\)?", flags=re.IGNORECASE)
    # 현재 메시지 안에서 `From:/Sent:/Subject:` 값을 "예시"로 설명하는 경우를 구분하기 위한 단서다.
    # 이런 힌트가 경계 직전에 보이면, 값이 실제 메일 헤더처럼 보여도 이전 메시지 경계로 자르지 않는다.
    EML_METADATA_EXAMPLE_HINTS = (
        "예시",
        "예를 들어",
        "example",
        "sample",
        "metadata",
        "메타데이터",
    )

    def _load_eml(self, file_path: Path) -> str:
        """EML 본문에서 HTML 또는 plain text 지시서를 추출한다."""
        raw_text, _ = self._load_eml_with_render_hints(file_path)
        return raw_text

    def _load_eml_with_render_hints(self, file_path: Path) -> tuple[str, dict[str, object] | None]:
        """EML 본문과 HTML markdown render hint를 함께 추출한다."""
        message = BytesParser(policy=policy.default).parsebytes(file_path.read_bytes())

        header_lines = [f"[EML {file_path.name}]"]
        for header_name, label in self.EML_HEADER_FIELDS:
            header_value = self._decode_eml_header(message.get(header_name))
            if header_value:
                header_lines.append(f"{label}: {header_value}")
                if header_name == "Date":
                    seoul_date = self._normalize_eml_date_to_seoul_date(header_value)
                    if seoul_date:
                        header_lines.append(f"Date (Asia/Seoul): {seoul_date}")

        body_candidates = self._collect_eml_body_candidates(message, source_label=file_path.name)
        if body_candidates:
            _, body_lines, render_hints = max(body_candidates, key=lambda item: item[0])
            return "\n".join(header_lines + [""] + body_lines).strip(), render_hints

        raise ValueError("No extractable text found in EML document.")

    @staticmethod
    def _normalize_eml_date_to_seoul_date(header_value: str) -> str | None:
        """메일 Date 헤더를 한국 시간 기준 YYYY-MM-DD로 정규화한다."""
        if not header_value:
            return None
        try:
            parsed = parsedate_to_datetime(header_value)
        except (TypeError, ValueError, IndexError, OverflowError):
            return None
        if parsed is None:
            return None
        if parsed.tzinfo is None:
            return parsed.date().isoformat()
        return parsed.astimezone(ZoneInfo("Asia/Seoul")).date().isoformat()

    def _collect_eml_body_candidates(
        self,
        message: Message,
        source_label: str,
    ) -> list[tuple[int, list[str], dict[str, object] | None]]:
        """EML 전용 본문 후보를 수집한다.

        EML은 현재 메시지 본문만 추출해야 하므로 quoted thread trimming을 활성화한다.
        """
        return self._collect_message_body_candidates(
            message,
            source_label=source_label,
            section_label="EML",
            trim_previous_thread=True,
            apply_quoted_thread_penalty=True,
        )

    def _collect_mht_body_candidates(
        self,
        message: Message,
        source_label: str,
    ) -> list[tuple[int, list[str], dict[str, object] | None]]:
        """MHT 전용 본문 후보를 수집한다.

        MHT는 메일 스레드보다 HTML 아카이브에 가깝다. 따라서 EML용 previous-thread
        trimming을 적용하지 않고, 원문 HTML을 그대로 raw_text 후보로 평가한다.
        """
        return self._collect_message_body_candidates(
            message,
            source_label=source_label,
            section_label="MHT",
            trim_previous_thread=False,
            apply_quoted_thread_penalty=False,
        )

    def _collect_message_body_candidates(
        self,
        message: Message,
        *,
        source_label: str,
        section_label: str,
        trim_previous_thread: bool,
        apply_quoted_thread_penalty: bool,
    ) -> list[tuple[int, list[str], dict[str, object] | None]]:
        """HTML/plain 본문 후보를 한 곳에 모아 품질 점수로 비교한다.

        예전에는 HTML이 조금이라도 읽히면 plain 본문을 아예 보지 않았는데,
        실제 메일에서는 반대로 plain 본문이 더 충실한 경우가 있다.
        따라서 이제는 subtype 우선순위보다 "실제로 지시서 정보가 얼마나 살아 있는지"를
        점수로 비교한 뒤 가장 좋은 후보를 선택한다.
        """
        candidates: list[tuple[int, list[str], dict[str, object] | None]] = []

        for html_body in self._extract_eml_bodies(message, "html"):
            candidate_html = (
                self._trim_eml_previous_message_html(html_body) if trim_previous_thread else html_body
            )
            try:
                html_raw_text, render_hints = self._extract_html_raw_text_with_render_hints(
                    candidate_html,
                    source_label=source_label,
                    section_label=section_label,
                )
            except ValueError:
                logger.warning("Skipping HTML body candidate that produced no extractable text.")
                continue
            body_lines = self._strip_leading_section_label(html_raw_text.splitlines())
            if trim_previous_thread:
                body_lines = self._trim_eml_previous_message_lines(body_lines)
            if not body_lines:
                continue
            score = self._score_eml_body_lines(
                body_lines,
                subtype="html",
                apply_quoted_thread_penalty=apply_quoted_thread_penalty,
            )
            candidates.append((score, body_lines, render_hints))

        for plain_body in self._extract_eml_bodies(message, "plain"):
            plain_lines = self._normalize_eml_plain_lines(plain_body)
            if not plain_lines:
                continue
            if trim_previous_thread:
                plain_lines = self._trim_eml_previous_message_lines(plain_lines)
            if not plain_lines:
                continue
            score = self._score_eml_body_lines(
                plain_lines,
                subtype="plain",
                apply_quoted_thread_penalty=apply_quoted_thread_penalty,
            )
            candidates.append((score, plain_lines, None))

        return candidates

    def _score_eml_body_lines(
        self,
        body_lines: list[str],
        *,
        subtype: str,
        apply_quoted_thread_penalty: bool = True,
    ) -> int:
        """후보 본문을 실제 raw_text 품질 기준으로 평가한다.

        HTML이든 plain이든 downstream으로 들어가는 형태는 결국 line list 이므로,
        subtype 자체보다 "주문서 키워드, 표 구조, 날짜/금액 문맥"이 얼마나 살아 있는지로
        점수를 매겨야 더 안정적으로 본문을 고를 수 있다.
        """
        joined = "\n".join(body_lines)
        lowered = joined.lower()
        keyword_hits = sum(1 for keyword in self.ORDER_REPORT_KEYWORDS if keyword in lowered)
        keyword_bonus = keyword_hits * 12
        pipe_row_bonus = min(20, sum(2 for line in body_lines if " | " in line))
        date_bonus = min(12, len(re.findall(r"\d{1,4}[-./]\d{1,2}[-./]\d{1,4}", joined)) * 3)
        length_bonus = min(10, len(joined) // 500)
        quoted_penalty = 0
        if apply_quoted_thread_penalty:
            quoted_penalty = sum(20 for keyword in self.QUOTED_THREAD_KEYWORDS if keyword in lowered)
        subtype_bonus = 2 if subtype == "html" else 0
        if keyword_hits == 0 and apply_quoted_thread_penalty:
            quoted_penalty += 30
        return keyword_bonus + pipe_row_bonus + date_bonus + length_bonus + subtype_bonus - quoted_penalty

    def _trim_eml_previous_message_html(self, html_text: str) -> str:
        """HTML 원문에서 이전 메시지 스레드를 먼저 잘라낸다.

        HTML은 텍스트 블록과 테이블을 다시 조립하는 과정에서 line 순서가 바뀔 수 있다.
        그래서 `Original Message` marker 를 line 기반으로 자르면 현재 표까지 함께 날릴 수 있다.
        이 문제를 피하려고 HTML은 raw HTML 문자열 단계에서 먼저 cut 한다.
        """
        lowered_html = html_text.lower()
        boundary_candidates: list[int] = []

        for marker in self.EML_THREAD_BOUNDARY_MARKERS:
            cursor = 0
            while True:
                marker_index = lowered_html.find(marker, cursor)
                if marker_index < 0:
                    break
                if not self._looks_like_html_metadata_example_context(lowered_html, marker_index):
                    boundary_candidates.append(marker_index)
                cursor = marker_index + 1

        separator_match = self.EML_THREAD_SEPARATOR_RE.search(lowered_html)
        if separator_match and not self._looks_like_html_metadata_example_context(lowered_html, separator_match.start()):
            boundary_candidates.append(separator_match.start())

        wrote_match = self.EML_REPLY_WROTE_RE.search(lowered_html)
        if wrote_match and not self._looks_like_html_metadata_example_context(lowered_html, wrote_match.start()):
            boundary_candidates.append(wrote_match.start())

        reply_header_boundary = self._find_eml_reply_header_boundary_in_html(lowered_html)
        if reply_header_boundary is not None:
            boundary_candidates.append(reply_header_boundary)

        if not boundary_candidates:
            return html_text

        boundary_index = min(boundary_candidates)
        # marker 텍스트가 `<div>-----Original Message-----</div>` 같은 블록 안에 있으면
        # marker 문자열 위치에서 자를 경우 열린 태그 조각이 남을 수 있다.
        # 가능한 경우 해당 블록의 시작 태그까지 되감아서 더 깨끗하게 잘라낸다.
        block_start_index = html_text.rfind("<", 0, boundary_index)
        if block_start_index >= 0:
            boundary_index = block_start_index
        trimmed_html = html_text[:boundary_index]
        logger.info(
            "Trimmed previous EML HTML thread content: kept_chars=%s dropped_chars=%s",
            len(trimmed_html),
            len(html_text) - len(trimmed_html),
        )
        return trimmed_html

    def _find_eml_reply_header_boundary_in_html(self, lowered_html: str) -> int | None:
        """HTML 안의 reply header block 시작 위치를 찾는다.

        일부 HTML 메일은 `-----Original Message-----` 같은 separator 없이 바로
        `From:/Sent:/Subject:` 블록이 이어진다. 이 경우도 이전 메시지 시작으로 봐야
        하므로, 짧은 구간 안에 메일 헤더 prefix가 연속 등장하는 패턴을 찾는다.

        다만 현재 메시지 본문이 `From: ...`, `Subject: ...` 같은 메타데이터 예시를
        설명할 수도 있으므로,
        - header block 자체가 실제 메일 헤더 값처럼 보이고
        - 앞뒤에 현재 표/이전 표 구조가 함께 있을 때만
        이전 메시지 경계로 인정한다.
        """
        prefix_hits: list[tuple[int, str]] = []
        for prefix in self.EML_REPLY_HEADER_PREFIXES:
            cursor = 0
            while True:
                found_index = lowered_html.find(prefix, cursor)
                if found_index < 0:
                    break
                prefix_hits.append((found_index, prefix))
                cursor = found_index + 1

        if not prefix_hits:
            return None

        prefix_hits.sort()
        sender_prefixes = {"from:", "sender:", "보낸 사람:", "보낸사람:"}
        timing_prefixes = {"sent:", "date:", "날짜:"}
        subject_prefixes = {"subject:", "title:", "제목:"}

        for hit_index, (start_index, first_prefix) in enumerate(prefix_hits):
            matched_prefixes = {first_prefix}
            for next_index in range(hit_index + 1, len(prefix_hits)):
                candidate_index, candidate_prefix = prefix_hits[next_index]
                if candidate_index - start_index > 500:
                    break
                matched_prefixes.add(candidate_prefix)
                has_sender_or_time = bool(matched_prefixes & sender_prefixes) or bool(matched_prefixes & timing_prefixes)
                has_subject = bool(matched_prefixes & subject_prefixes)
                if (
                    has_sender_or_time
                    and has_subject
                    and len(matched_prefixes) >= 2
                    and not self._looks_like_html_metadata_example_context(lowered_html, start_index)
                    and self._looks_like_reply_header_values_in_html(lowered_html, start_index)
                    and self._looks_like_html_reply_boundary_context(lowered_html, start_index)
                ):
                    return start_index

        return None

    def _looks_like_html_metadata_example_context(self, lowered_html: str, boundary_index: int) -> bool:
        """HTML 경계 직전이 "현재 메시지의 메타데이터 예시" 설명인지 본다.

        최근 요구사항 때문에 separator 없는 reply header block 도 잘라내야 하지만,
        현재 메시지가 `From:/Sent:/Subject:` 값을 설명하는 문단을 포함할 수도 있다.
        이 경우는 이전 스레드가 아니라 현재 메시지 본문 일부이므로 보존해야 한다.
        """
        context_start = max(0, boundary_index - 240)
        context_fragment = lowered_html[context_start:boundary_index]
        return any(hint in context_fragment for hint in self.EML_METADATA_EXAMPLE_HINTS)

    def _looks_like_reply_header_values_in_html(self, lowered_html: str, boundary_index: int) -> bool:
        """HTML 경계 후보 주변의 값이 실제 메일 헤더처럼 보이는지 판별한다.

        현재 메시지의 예시 텍스트는 `From: 시스템 생성값`, `Subject: 운용 보고`처럼
        prefix 는 있어도 값이 "실제 메일 헤더"답지 않은 경우가 많다.
        반대로 이전 메시지 header block 은 이메일 주소, RFC2822 스타일 날짜,
        timezone offset 같은 메일 헤더다운 단서를 갖는 경우가 많다.
        """
        fragment = lowered_html[boundary_index : boundary_index + 800]
        return self._has_strong_reply_header_value_signal(fragment)

    @staticmethod
    def _looks_like_html_reply_boundary_context(lowered_html: str, boundary_index: int) -> bool:
        """HTML reply header block 주변에 "현재 표 + 이전 표" 구조가 있는지 본다.

        separator 없는 HTML 회신은 보통
        - 현재 메시지의 주문 표
        - `From:/Sent:/Subject:` 블록
        - 이전 메시지의 주문 표
        순서로 나타난다.

        반대로 현재 메시지 본문이 메타데이터 예시를 설명하는 경우에는 header block 뒤쪽에
        generic key/value table 이 붙을 수 있다. 따라서 단순히 `<table>` 유무만 보지 않고,
        경계 뒤쪽 테이블이 실제 주문 표처럼 보이는지까지 확인해야 오탐을 줄일 수 있다.
        """
        before_html = lowered_html[:boundary_index]
        after_html = lowered_html[boundary_index:]
        return "<table" in before_html and EmlDocumentLoaderMixin._looks_like_html_order_table_context(after_html)

    @classmethod
    def _looks_like_html_order_table_context(cls, html_fragment: str) -> bool:
        """HTML 조각 안의 다음 테이블이 주문 표 문맥인지 추정한다.

        이전 메시지의 quoted table 은 보통 날짜와 주문 헤더(`fund code`, `buy`, `sell` 등)를
        같이 갖는다. 반면 메타데이터 예시 뒤의 generic table은 이런 신호가 약하다.
        """
        if "<table" not in html_fragment:
            return False

        first_table_start = html_fragment.find("<table")
        first_table_end = html_fragment.find("</table>", first_table_start)
        if first_table_end < 0:
            table_fragment = html_fragment[first_table_start:]
        else:
            table_fragment = html_fragment[first_table_start : first_table_end + len("</table>")]

        order_keyword_hits = sum(
            1
            for keyword in ("fund code", "fund name", "buy", "sell", "subscription", "redemption", "설정", "해지", "입금", "출금", "금액")
            if keyword in table_fragment
        )
        has_date = bool(re.search(r"\d{1,4}[-./]\d{1,2}[-./]\d{1,4}", table_fragment))
        return order_keyword_hits >= 2 or (order_keyword_hits >= 1 and has_date)

    def _trim_eml_previous_message_lines(self, body_lines: list[str]) -> list[str]:
        """현재 메시지 이후에 붙은 이전 메일 스레드를 잘라낸다.

        EML은 하나의 body part 안에 현재 메시지와 이전 메시지가 같이 들어오는 경우가 많다.
        이번 요구사항은 "현재 메시지에서만 추출"이므로, 이전 스레드 경계가 보이면
        그 이후 내용은 후보 평가 전에 전부 버린다.
        """
        boundary_index = self._find_eml_previous_message_boundary(body_lines)
        if boundary_index is None:
            return body_lines

        trimmed = body_lines[:boundary_index]
        while trimmed and not trimmed[-1].strip():
            trimmed.pop()
        logger.info(
            "Trimmed previous EML thread content: kept_lines=%s dropped_lines=%s",
            len(trimmed),
            len(body_lines) - len(trimmed),
        )
        return trimmed

    def _find_eml_previous_message_boundary(self, body_lines: list[str]) -> int | None:
        """이전 메시지가 시작되는 line index를 찾는다.

        경계 판정은 두 부류를 함께 본다.
        1. `-----Original Message-----`, `Forwarded message` 같은 명시적 구분선
        2. 빈 줄 뒤에 `From:/Sent:/Subject:` 헤더 블록이 다시 시작되는 패턴
        """
        for index, raw_line in enumerate(body_lines):
            normalized_line = self._normalize_eml_plain_line(raw_line)
            if not normalized_line:
                continue

            lowered_line = normalized_line.lower()
            if self._looks_like_eml_thread_separator(lowered_line):
                # 현재 메시지 안에서 separator 자체를 설명하는 예시 문단은 보존한다.
                if self._looks_like_plain_metadata_example_context(body_lines, index):
                    continue
                return index
            if self._looks_like_eml_reply_header_block(body_lines, index):
                return index
        return None

    def _looks_like_eml_thread_separator(self, lowered_line: str) -> bool:
        """이 줄이 이전 메시지 시작을 알리는 explicit marker 인지 본다."""
        if any(marker in lowered_line for marker in self.EML_THREAD_BOUNDARY_MARKERS):
            return True
        if self.EML_THREAD_SEPARATOR_RE.fullmatch(lowered_line):
            return True
        if self.EML_REPLY_WROTE_RE.fullmatch(lowered_line):
            return True
        return False

    def _looks_like_eml_reply_header_block(self, body_lines: list[str], start_index: int) -> bool:
        """`From:/Sent:/Subject:` 형태의 이전 메일 헤더 블록 시작인지 추정한다.

        현재 메시지 본문에 단일 `Subject:` 문자열이 들어갈 수도 있으므로,
        한 줄만 보고 자르지 않고
        - 메일 헤더가 2개 이상 연속되고
        - 그 값이 실제 reply header 처럼 보이며
        - 앞뒤에 주문 흔적이 있는 경우
        에만 이전 메시지 경계로 인정한다.
        """
        current_line = self._normalize_eml_plain_line(body_lines[start_index]).lower()
        if not self._is_eml_reply_header_line(current_line):
            return False

        # `Original Message` 블록 자체를 현재 본문 예시로 설명하는 경우에는
        # separator 줄을 건너뛴 뒤의 `Sender:/Date:/Title:`도 그대로 보존해야 한다.
        if self._looks_like_plain_metadata_example_context(body_lines, start_index):
            return False

        if start_index > 0:
            previous_line = self._normalize_eml_plain_line(body_lines[start_index - 1]).lower()
            if previous_line and not self._looks_like_eml_thread_separator(previous_line):
                # plain text EML은 separator 없이 바로 reply header block이 붙는 경우가 있다.
                # 이때는 "앞에 현재 주문 흔적이 있고, 뒤에도 추가 주문 흔적이 이어진다"는
                # 구조가 있을 때만 이전 메시지로 본다.
                #
                # 단, 직전에 `메타데이터 예시`, `안내`, `example` 같은 설명 문구가 있으면
                # 현재 메시지 안에서 헤더 값을 소개하는 경우로 보는 편이 안전하다.
                if not (
                    self._has_order_like_lines_before(body_lines, start_index)
                    and self._has_order_like_lines_after_reply_headers(body_lines, start_index)
                ):
                    return False

        header_hits = 0
        scanned_non_empty = 0
        for lookahead_index in range(start_index, min(len(body_lines), start_index + 5)):
            line = self._normalize_eml_plain_line(body_lines[lookahead_index]).lower()
            if not line:
                continue
            scanned_non_empty += 1
            if self._is_eml_reply_header_line(line):
                header_hits += 1
            if header_hits >= 2:
                header_block_end = lookahead_index + 1
                return self._looks_like_reply_header_values_in_lines(body_lines[start_index:header_block_end])
            if scanned_non_empty >= 4:
                break
        return False

    def _looks_like_plain_metadata_example_context(self, body_lines: list[str], start_index: int) -> bool:
        """plain 본문에서 reply header 직전이 메타데이터 예시 설명인지 본다."""
        context_lines: list[str] = []
        for cursor in range(max(0, start_index - 3), start_index):
            line = self._normalize_eml_plain_line(body_lines[cursor]).lower()
            if line:
                context_lines.append(line)
        context_text = "\n".join(context_lines)
        return any(hint in context_text for hint in self.EML_METADATA_EXAMPLE_HINTS)

    def _has_order_like_lines_before(self, body_lines: list[str], start_index: int) -> bool:
        """reply header block 앞쪽에 이미 현재 주문 흔적이 있는지 본다."""
        for lookback_index in range(max(0, start_index - 6), start_index):
            line = self._normalize_eml_plain_line(body_lines[lookback_index])
            if self._looks_like_eml_order_content_line(line):
                return True
        return False

    def _has_order_like_lines_after_reply_headers(self, body_lines: list[str], start_index: int) -> bool:
        """reply header block 뒤쪽에 추가 주문 흔적이 이어지는지 본다.

        separator 없는 quoted thread 는 `From:/Sent:/Subject:` 바로 다음에 실제 주문행이
        나오지 않고, 세로형 `BUY & SELL REPORT` 제목/헤더가 몇 줄 더 이어지는 경우가 많다.
        그래서 단순히 5~6줄만 훑으면 quoted thread 를 놓칠 수 있어, 제목/헤더 라인도
        약한 주문 신호로 보고 조금 더 길게 확인한다.
        """
        non_empty_seen = 0
        after_start_index = start_index
        for cursor in range(start_index, min(len(body_lines), start_index + 8)):
            line = self._normalize_eml_plain_line(body_lines[cursor])
            if not line:
                continue
            non_empty_seen += 1
            if self._is_eml_reply_header_line(line.lower()):
                after_start_index = cursor + 1
                continue
            # 헤더 블록이 끝나는 첫 비헤더 line부터 이후 내용으로 본다.
            after_start_index = cursor
            break

        buy_sell_header_hits = 0
        for cursor in range(after_start_index, min(len(body_lines), after_start_index + 18)):
            line = self._normalize_eml_plain_line(body_lines[cursor])
            if self._looks_like_eml_order_content_line(line):
                return True
            lowered_line = line.lower()
            if self.BUY_SELL_REPORT_TITLE_RE.fullmatch(line):
                return True
            if lowered_line in self.BUY_SELL_HEADER_HINTS:
                buy_sell_header_hits += 1
                if buy_sell_header_hits >= 3:
                    return True
        return False

    def _looks_like_eml_order_content_line(self, line: str) -> bool:
        """이 line이 주문 표/금액 문맥처럼 보이는지 대략 판정한다."""
        if not line:
            return False
        if " | " in line and any(ch.isdigit() for ch in line):
            return True
        if self.BUY_SELL_DATE_RE.fullmatch(line):
            return True
        if self.BUY_SELL_FUND_CODE_RE.fullmatch(line):
            return True
        normalized = line.replace(",", "")
        if re.fullmatch(r"-?\d+(?:\.\d+)?", normalized):
            return True
        return False

    def _looks_like_reply_header_values_in_lines(self, lines: list[str]) -> bool:
        """plain line 기준으로 reply header 값이 실제 메일 헤더처럼 보이는지 본다."""
        fragment = "\n".join(self._normalize_eml_plain_line(line).lower() for line in lines if line)
        return self._has_strong_reply_header_value_signal(fragment)

    def _has_strong_reply_header_value_signal(self, text: str) -> bool:
        """reply header 후보 값이 실제 메일 헤더처럼 보이는 강한 신호를 찾는다.

        여기서 말하는 강한 신호는 "현재 메시지의 예시 텍스트"와 구분해 주는 값이다.
        예:
        - 이메일 주소 (`@`)
        - mailto
        - `Tue, 27 Nov 2025` 같은 메일 날짜 형식
        - `+0900` 같은 timezone offset
        """
        lowered = text.lower()
        if "@" in lowered or "mailto:" in lowered:
            return True
        if self.EML_REPLY_DAY_DATE_RE.search(lowered):
            return True
        if self.EML_REPLY_LOCAL_DATE_RE.search(lowered):
            return True
        if self.EML_REPLY_TZ_RE.search(lowered):
            return True
        if self.EML_REPLY_GMT_RE.search(lowered):
            return True
        return False

    def _is_eml_reply_header_line(self, lowered_line: str) -> bool:
        """메일 헤더처럼 보이는 줄인지 prefix 기준으로 판별한다."""
        normalized_line = re.sub(r"\s*:\s*", ":", lowered_line, count=1)
        return any(normalized_line.startswith(prefix) for prefix in self.EML_REPLY_HEADER_PREFIXES)

    def _extract_eml_bodies(self, message: Message, subtype: str) -> list[str]:
        """우선순위가 높은 순서대로 text/{subtype} 본문 후보를 반환한다.

        HTML 파트는 파일형 HTML과 같은 인코딩 방어를 재사용하기 위해 raw bytes 기반으로
        직접 디코딩한다. 메일 본문은 MIME charset 선언이 틀린 경우가 실제로 자주 있어
        `part.get_content()` 결과만 믿으면 mojibake가 그대로 downstream으로 전파된다.
        """
        candidates = self._find_eml_body_parts(message, subtype)
        if not candidates:
            return []

        scored_candidates: list[tuple[int, int, str]] = []
        for index, part in enumerate(candidates):
            if subtype == "html":
                decoded = self._decode_eml_html_part(part)
                score = self._score_eml_html_body(decoded)
            else:
                decoded = self._decode_eml_plain_part(part)
                score = self._score_eml_plain_body(decoded)
            if not decoded:
                continue
            scored_candidates.append((score, -index, decoded))

        scored_candidates.sort(reverse=True)
        return [decoded for _, _, decoded in scored_candidates]

    def _find_eml_body_parts(self, message: Message, subtype: str) -> list[Message]:
        """첨부파일을 제외한 text/{subtype} 파트 목록을 수집한다."""
        parts: list[Message] = []
        if message.is_multipart():
            for part in message.walk():
                if part.is_multipart():
                    continue
                if part.get_content_disposition() == "attachment":
                    continue
                if part.get_content_type() == f"text/{subtype}":
                    parts.append(part)
            return parts
        if message.get_content_type() == f"text/{subtype}":
            parts.append(message)
        return parts

    def _score_eml_html_body(self, html_text: str | None) -> int:
        """여러 HTML body part 중 실제 지시서에 가까운 본문에 높은 점수를 준다."""
        if not html_text:
            return -10_000
        html_text = self._trim_eml_previous_message_html(html_text)
        try:
            raw_text = self._extract_html_raw_text(html_text, source_label="EML-CANDIDATE", section_label="EML")
        except ValueError:
            return -10_000

        html_score = self._score_decoded_html(html_text)
        lowered_raw_text = raw_text.lower()
        keyword_hits = sum(1 for keyword in self.ORDER_REPORT_KEYWORDS if keyword in lowered_raw_text)
        keyword_bonus = keyword_hits * 12
        date_bonus = min(12, len(re.findall(r"\d{1,4}[-./]\d{1,2}[-./]\d{1,4}", raw_text)) * 4)
        pipe_row_bonus = min(12, sum(1 for line in raw_text.splitlines() if "|" in line))
        length_bonus = min(8, len(raw_text) // 600)
        quoted_penalty = sum(20 for keyword in self.QUOTED_THREAD_KEYWORDS if keyword in lowered_raw_text)
        if keyword_hits == 0:
            quoted_penalty += 40
        return html_score + keyword_bonus + date_bonus + pipe_row_bonus + length_bonus - quoted_penalty

    def _score_eml_plain_body(self, text: str | None) -> int:
        """여러 plain body part 중 실제 지시서 본문에 가까운 텍스트를 우선한다."""
        if not text:
            return -10_000
        base_score = self._score_decoded_eml_text(text)
        line_bonus = min(15, len([line for line in text.splitlines() if line.strip()]) // 6)
        return base_score + line_bonus

    def _decode_eml_html_part(self, part: Message) -> str | None:
        """HTML 파트를 raw bytes 기반으로 안전하게 복원한다."""
        raw_bytes = part.get_payload(decode=True)
        declared_charset = part.get_content_charset()
        if raw_bytes:
            html_text = self._decode_html_bytes(raw_bytes, declared_charset=declared_charset)
            if html_text.strip():
                return html_text

        content = part.get_content()
        if isinstance(content, str) and content.strip():
            return content
        return None

    def _decode_eml_plain_part(self, part: Message) -> str | None:
        """plain text 파트를 charset fallback과 함께 복원한다."""
        raw_bytes = part.get_payload(decode=True)
        declared_charset = part.get_content_charset()
        if raw_bytes:
            text = self._decode_eml_text_bytes(raw_bytes, declared_charset=declared_charset)
            if text.strip():
                return text

        content = part.get_content()
        if isinstance(content, str) and content.strip():
            return content
        return None

    @staticmethod
    def _decode_eml_header(value: object) -> str:
        """RFC2047 인코딩 헤더를 사람이 읽을 수 있는 문자열로 복원한다."""
        if value is None:
            return ""
        text = str(make_header(decode_header(str(value))))
        return re.sub(r"\s+", " ", text).strip()

    def _decode_eml_text_bytes(self, raw_bytes: bytes, declared_charset: str | None) -> str:
        """plain text EML bytes를 가장 자연스러운 인코딩으로 복원한다."""
        decoded_candidates: list[tuple[int, int, str, str]] = []
        for index, encoding in enumerate(self._eml_text_encoding_candidates(raw_bytes, declared_charset)):
            try:
                text = raw_bytes.decode(encoding)
            except (LookupError, UnicodeDecodeError):
                continue
            normalized_text = text.lstrip("\ufeff")
            score = self._score_decoded_eml_text(normalized_text)
            decoded_candidates.append((score, -index, encoding, normalized_text))

        if not decoded_candidates:
            raise ValueError("Failed to decode EML text body with supported encodings.")

        best_score, _, best_encoding, best_text = max(decoded_candidates)
        logger.info("Decoded EML text using encoding=%s score=%s", best_encoding, best_score)
        return best_text

    def _eml_text_encoding_candidates(self, raw_bytes: bytes, declared_charset: str | None) -> list[str]:
        """EML text 파트 디코딩에 시도할 인코딩 후보를 우선순위대로 만든다.

        우선순위는 대략 다음과 같다.
        1. BOM 이 명시하는 실제 인코딩
        2. MIME 파트가 선언한 charset
        3. 한국어 업무 메일에서 자주 나오는 fallback 후보

        반환값 자체는 단순 문자열 목록이지만, 이 순서가 곧 `_decode_eml_text_bytes()`의
        tie-break 기준이 되므로 "무엇을 먼저 시도할지"가 중요하다.
        """
        candidates: list[str] = []
        bom_encoding = self._detect_eml_bom_encoding(raw_bytes)
        if bom_encoding:
            candidates.append(bom_encoding)
        if declared_charset:
            candidates.extend(self._expand_html_encoding_aliases(declared_charset))
        candidates.extend(["utf-8-sig", "utf-8", "cp949", "euc-kr", "utf-16", "utf-16-le", "utf-16-be", "latin-1"])

        deduped: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            normalized = candidate.strip().lower()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(normalized)
        return deduped

    @staticmethod
    def _detect_eml_bom_encoding(raw_bytes: bytes) -> str | None:
        """BOM 시그니처를 보고 확정 가능한 텍스트 인코딩을 빠르게 판별한다."""
        if raw_bytes.startswith(codecs.BOM_UTF8):
            return "utf-8-sig"
        if raw_bytes.startswith(codecs.BOM_UTF16_LE) or raw_bytes.startswith(codecs.BOM_UTF16_BE):
            return "utf-16"
        return None

    @staticmethod
    def _score_decoded_eml_text(text: str) -> int:
        """디코딩 결과가 사람 눈에 자연스러운지 점수화한다.

        charset fallback은 여러 후보 중 "가장 그럴듯한 텍스트"를 고르는 문제다.
        여기서는 한글 비율, 업무 키워드, 깨진 문자(`�`), 널 바이트 등을 보고
        상대적인 품질 점수를 만든다.
        """
        score = 0
        hangul_count = len(re.findall(r"[가-힣]", text))
        score += min(20, hangul_count)
        if EmlDocumentLoaderMixin.BUY_SELL_REPORT_TITLE_RE.search(text):
            score += 10
        for keyword in ("fund code", "fund name", "buy", "sell", "설정", "해지", "입금", "출금"):
            if keyword in text.lower():
                score += 3
        score -= text.count("\ufffd") * 20
        score -= text.count("\x00") * 10
        if hangul_count == 0 and re.search(r"[À-ÿ]{4,}", text):
            score -= 8
        return score

    def _normalize_eml_plain_lines(self, text: str) -> list[str]:
        """plain text 본문을 raw_text style line 목록으로 정규화한다.

        일반 메일은 빈 줄을 제거한 line list면 충분하지만, BUY & SELL REPORT처럼
        셀 하나가 한 줄씩 내려오는 세로형 리포트는 별도 표 복원이 필요하다.
        """
        normalized_lines = [self._normalize_eml_plain_line(raw_line) for raw_line in text.splitlines()]
        # plain 본문은 구조 복원 전에 이전 메시지를 먼저 잘라야 한다.
        # 그렇지 않으면 과거 thread 의 BUY&SELL 행까지 현재 메시지 데이터로 흡수될 수 있다.
        normalized_lines = self._trim_eml_previous_message_lines(normalized_lines)
        restructured_buy_sell = self._restructure_buy_sell_plain_lines(normalized_lines)
        if restructured_buy_sell:
            return restructured_buy_sell
        return [line for line in normalized_lines if line]

    @staticmethod
    def _normalize_eml_plain_line(raw_line: str) -> str:
        """plain text 한 줄의 공백/특수 스페이스를 정리한다."""
        normalized = raw_line.replace("\u3000", " ").replace("\xa0", " ")
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized

    def _restructure_buy_sell_plain_lines(self, normalized_lines: list[str]) -> list[str] | None:
        """세로형 BUY & SELL REPORT plain text를 pipe table 형태로 복원한다."""
        title_index = next(
            (index for index, line in enumerate(normalized_lines) if self.BUY_SELL_REPORT_TITLE_RE.fullmatch(line)),
            None,
        )
        if title_index is None:
            return None

        date_header_index = next(
            (index for index in range(title_index, len(normalized_lines)) if normalized_lines[index] == "Date"),
            None,
        )
        if date_header_index is None:
            return None

        first_data_index = next(
            (
                index
                for index in range(date_header_index, len(normalized_lines))
                if self.BUY_SELL_DATE_RE.fullmatch(normalized_lines[index])
            ),
            None,
        )
        if first_data_index is None:
            return None

        # 표 헤더 자체는 곧바로 pipe table로 다시 만들기 때문에, preamble에는
        # 제목/발신사 같은 "표 바깥 문맥"만 남기고 세로형 헤더 토큰은 제거한다.
        preamble = [line for line in normalized_lines[title_index:date_header_index] if line]
        lines: list[str] = preamble[:]
        lines.append("Date | Buy&Sell | External Fund Manager | Fund Code | Fund Name | Fund Price | Buy |  | Sell |  | Custodian Bank")
        lines.append(" |  |  |  |  |  | Amount | Unit | Amount | Unit | ")

        cursor = first_data_index
        while cursor < len(normalized_lines):
            while cursor < len(normalized_lines) and not normalized_lines[cursor]:
                cursor += 1
            if cursor >= len(normalized_lines):
                break
            token = normalized_lines[cursor]
            if token.lower() in self.BUY_SELL_STOP_TOKENS:
                break

            if self.BUY_SELL_DATE_RE.fullmatch(token):
                cells, cursor = self._consume_eml_buy_sell_row(normalized_lines, cursor, has_date=True)
            else:
                cells, cursor = self._consume_eml_buy_sell_row(normalized_lines, cursor, has_date=False)

            if len(cells) < 11:
                break
            lines.append(" | ".join(cells[:11]))

        # 제목만 있고 실제 데이터 행을 하나도 못 만들었다면, 오검출로 보고 일반 plain 처리로 되돌린다.
        if len(lines) <= len(preamble) + 2:
            return None
        return lines

    def _consume_eml_buy_sell_row(
        self,
        normalized_lines: list[str],
        start_index: int,
        *,
        has_date: bool,
    ) -> tuple[list[str], int]:
        """plain text BUY&SELL report 한 행을 소비한다.

        메일 클라이언트에 따라 행 끝의 빈 셀이 잘려서 다음 행의 시작 토큰이 바로 이어질 수 있다.
        그래서 단순히 `expected_cells` 개수만 채우지 않고, 충분한 필수 셀을 읽은 뒤
        다음 행 시작 신호(Date 또는 manager/fund code/fund name/fund price 조합)를 보면 멈춘다.
        """
        prefix_cells: list[str] = [] if has_date else ["", ""]
        cursor = start_index
        while cursor < len(normalized_lines) and len(prefix_cells) < 6:
            token = normalized_lines[cursor]
            if len(prefix_cells) < (0 if has_date else 2) and not token:
                cursor += 1
                continue
            if token.lower() in self.BUY_SELL_STOP_TOKENS:
                break
            prefix_cells.append(token)
            cursor += 1

        if len(prefix_cells) < 6:
            prefix_cells.extend([""] * (6 - len(prefix_cells)))
            return prefix_cells + [""] * 5, cursor

        tail_tokens: list[str] = []
        while cursor < len(normalized_lines) and len(tail_tokens) < 5:
            token = normalized_lines[cursor]
            if token.lower() in self.BUY_SELL_STOP_TOKENS:
                break
            if self._looks_like_next_buy_sell_row(normalized_lines, cursor):
                break
            tail_tokens.append(token)
            cursor += 1

        cells = prefix_cells + self._map_buy_sell_tail_tokens(tail_tokens)
        if len(cells) < 11:
            cells.extend([""] * (11 - len(cells)))
        return cells[:11], cursor

    @staticmethod
    def _map_buy_sell_tail_tokens(tail_tokens: list[str]) -> list[str]:
        """tail 토큰을 Buy/Sell/Custodian 하위 컬럼에 배치한다.

        plain text 메일은 빈 셀이 통째로 잘려 나가는 경우가 흔해서
        `buy amount, sell amount` 두 값만 붙어 있는 패턴이 자주 나온다.
        길이에 따라 가장 흔한 실무 패턴으로 매핑해 행 정합성을 높인다.
        """
        if not tail_tokens:
            return ["", "", "", "", ""]
        if len(tail_tokens) == 1:
            return [tail_tokens[0], "", "", "", ""]
        if len(tail_tokens) == 2:
            return [tail_tokens[0], "", tail_tokens[1], "", ""]
        if len(tail_tokens) == 3:
            return [tail_tokens[0], tail_tokens[1], tail_tokens[2], "", ""]
        if len(tail_tokens) == 4:
            return [tail_tokens[0], tail_tokens[1], tail_tokens[2], tail_tokens[3], ""]
        return tail_tokens[:5]

    def _looks_like_next_buy_sell_row(
        self,
        normalized_lines: list[str],
        cursor: int,
    ) -> bool:
        """현재 토큰이 다음 BUY&SELL 행의 시작인지 추정한다."""
        token = normalized_lines[cursor]
        if not token:
            return False
        if token.lower() in self.BUY_SELL_STOP_TOKENS:
            return True
        if self.BUY_SELL_DATE_RE.fullmatch(token):
            return True

        # 첫 행(date 포함)을 읽는 중에는 다음 행이 date 없이 manager부터 시작할 수 있다.
        # continuation 행은 보통 manager -> fund_code -> fund_name -> fund_price 형태가 이어진다.
        lookahead = normalized_lines[cursor : cursor + 4]
        if len(lookahead) < 4:
            return False
        manager, fund_code, fund_name, fund_price = lookahead
        if not manager or not fund_name:
            return False
        if not self.BUY_SELL_FUND_CODE_RE.fullmatch(fund_code):
            return False
        if self.BUY_SELL_DATE_RE.fullmatch(manager):
            return False
        return self._looks_like_amount_or_price(fund_price)

    @staticmethod
    def _looks_like_amount_or_price(token: str) -> bool:
        """BUY&SELL 행의 금액/가격 셀처럼 보이는 토큰인지 간단히 판별한다."""
        normalized = token.replace(",", "")
        return bool(re.fullmatch(r"-?\d+(?:\.\d+)?", normalized))

    @staticmethod
    def _strip_leading_section_label(lines: list[str]) -> list[str]:
        """재사용한 HTML 로더가 만든 첫 줄 `[EML ...]` 라벨을 제거한다."""
        if lines and re.fullmatch(r"\[[A-Z ]+ .+\]", lines[0].strip()):
            return lines[1:]
        return lines
