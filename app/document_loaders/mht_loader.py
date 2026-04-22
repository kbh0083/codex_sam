from __future__ import annotations

import email
import logging
from email import policy
from pathlib import Path

logger = logging.getLogger(__name__)


class MhtDocumentLoaderMixin:
    """MHT/MHTML 웹 아카이브를 parser-style에 가까운 안정 경로로 추출한다.

    운영 WAS의 공통 `DocumentParser._extract_mht()`는 교보 MHT 연속 추출에서
    더 안정적으로 동작한다. 로컬 repo에는 공통 parser 모듈이 없으므로,
    동일한 HTML-part 순회와 본문 결합 규칙을 이 mixin 안에 최소한으로 재현한다.
    """

    def _load_mht(self, file_path: Path) -> str:
        """MHT 파일에서 본문 후보를 골라 공통 raw_text로 변환한다.

        `.mht`는 사실상 HTML 문서 하나가 아니라 관련 리소스를 포함한 메일 아카이브다.
        그래서 "MHT 전용 파서"를 따로 만드는 대신, 이미 운영에서 검증된
        EML 본문 선택 로직을 재사용해 본문 품질을 맞춘다.
        """
        raw_text, _ = self._load_mht_with_render_hints(file_path)
        return raw_text

    def _load_mht_with_render_hints(self, file_path: Path) -> tuple[str, dict[str, object] | None]:
        """MHT 본문을 parser-style 텍스트로 추출하고 raw_text 형식으로 감싼다."""
        text_content, render_hints = self._extract_mht_via_parser_style(file_path)
        if not text_content:
            raise ValueError("No extractable text found in MHT document.")

        header_lines = [f"[MHT {file_path.name}]"]
        return "\n".join(header_lines + ["", text_content]).strip(), render_hints

    def _extract_mht_via_parser_style(self, file_path: Path) -> tuple[str, dict[str, object] | None]:
        """WAS 공통 parser `_extract_mht()`와 같은 순서로 텍스트를 만든다."""
        raw_data = file_path.read_bytes()
        message = email.message_from_bytes(raw_data, policy=policy.default)

        html_parts: list[str] = []

        if message.is_multipart():
            for part in message.walk():
                if part.get_content_type() != "text/html":
                    continue
                payload = part.get_payload(decode=True)
                if not payload:
                    continue
                charset = part.get_content_charset()
                html_parts.append(self._decode_mht_bytes(payload, charset))
        else:
            if message.get_content_type() == "text/html":
                payload = message.get_payload(decode=True)
                if payload:
                    charset = message.get_content_charset()
                    html_parts.append(self._decode_mht_bytes(payload, charset))

        text_parts: list[str] = []
        pipe_table_hints: list[dict[str, object]] = []
        markdown_blocks: list[str] = []
        label_tokens: list[str] = []
        table_row_tokens: list[str] = []
        inherited_row_tokens: list[str] = []
        row_kind_tokens: list[str] = []
        for html in html_parts:
            if not html:
                continue
            try:
                html_raw_text, render_hints = self._extract_html_raw_text_with_render_hints(
                    html,
                    source_label=file_path.name,
                    section_label="MHT",
                )
            except ValueError:
                continue
            if isinstance(render_hints, dict):
                raw_pipe_table_hints = render_hints.get("pipe_table_hints")
                if isinstance(raw_pipe_table_hints, list):
                    pipe_table_hints.extend(
                        table_hint
                        for table_hint in raw_pipe_table_hints
                        if isinstance(table_hint, dict)
                    )
                raw_markdown_blocks = render_hints.get("markdown_blocks")
                if isinstance(raw_markdown_blocks, list):
                    markdown_blocks.extend(str(block) for block in raw_markdown_blocks if str(block).strip())
                raw_audit = render_hints.get("html_markdown_audit")
                if isinstance(raw_audit, dict):
                    label_tokens.extend(
                        str(token) for token in raw_audit.get("label_tokens", []) if str(token).strip()
                    )
                    table_row_tokens.extend(
                        str(token) for token in raw_audit.get("table_row_tokens", []) if str(token).strip()
                    )
                    inherited_row_tokens.extend(
                        str(token) for token in raw_audit.get("inherited_row_tokens", []) if str(token).strip()
                    )
                    row_kind_tokens.extend(
                        str(token) for token in raw_audit.get("row_kind_tokens", []) if str(token).strip()
                    )
            body_text = self._strip_mht_header_from_raw_text(html_raw_text, file_path.name)
            if body_text:
                text_parts.append(body_text)
        normalized_text = "\n\n".join(part for part in text_parts if part).strip()
        if not normalized_text:
            return "", None
        if pipe_table_hints or markdown_blocks:
            render_hints: dict[str, object] = {
                "pipe_table_hints": pipe_table_hints,
                "markdown_blocks": markdown_blocks,
                "html_markdown_audit": {
                    "label_tokens": self._dedupe_preserve_order(label_tokens),
                    "table_row_tokens": self._dedupe_preserve_order(table_row_tokens),
                    "inherited_row_tokens": self._dedupe_preserve_order(inherited_row_tokens),
                    "row_kind_tokens": self._dedupe_preserve_order(row_kind_tokens),
                },
            }
            if markdown_blocks:
                render_hints["preferred_markdown_text"] = self._compose_html_section_markdown(
                    section_label="MHT",
                    source_label=file_path.name,
                    preamble_lines=[],
                    markdown_blocks=markdown_blocks,
                )
            return normalized_text, render_hints
        return normalized_text, None

    @staticmethod
    def _strip_mht_header_from_raw_text(raw_text: str, source_label: str) -> str:
        lines = raw_text.splitlines()
        expected_header = f"[MHT {source_label}]"
        if lines and lines[0].strip() == expected_header:
            lines = lines[1:]
        while lines and not lines[0].strip():
            lines = lines[1:]
        return "\n".join(lines).strip()

    @staticmethod
    def _decode_mht_bytes(data: bytes, charset: str | None) -> str:
        """공통 parser의 `decode_bytes()`와 유사한 strict-first decode."""
        normalized = (charset or "").strip().lower()
        encodings: list[str] = []
        if normalized in {"unicode", "utf16", "utf-16"}:
            encodings.extend(["utf-16-le", "utf-16"])
        elif charset:
            encodings.append(charset)
        encodings.extend(["utf-8", "euc-kr", "cp949", "utf-16", "utf-16-le"])

        seen: set[str] = set()
        for encoding in encodings:
            if encoding in seen:
                continue
            seen.add(encoding)
            try:
                return data.decode(encoding, errors="strict")
            except (LookupError, UnicodeDecodeError):
                continue

        return data.decode("utf-8", errors="ignore")
