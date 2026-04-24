from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Literal

from .dto import DocumentLoadTaskPayload, ExtractedDocumentText

logger = logging.getLogger(__name__)


class DocumentLoaderCoreMixin:
    def load(self, file_path: Path, pdf_password: str | None = None) -> ExtractedDocumentText:
        suffix = file_path.suffix.lower()
        logger.info("Loading document: path=%s suffix=%s", file_path, suffix)
        if suffix not in self.SUPPORTED_EXTENSIONS:
            raise ValueError(f"Unsupported file type: {suffix}")
        if suffix == ".pdf":
            raw_text = self._load_pdf(file_path, pdf_password=pdf_password)
            return self._build_extracted_document(
                raw_text=raw_text,
                content_type="application/pdf",
            )
        if suffix == ".eml":
            raw_text, render_hints = self._load_eml_with_render_hints(file_path)
            return self._build_extracted_document(
                raw_text=raw_text,
                content_type="message/rfc822",
                render_hints=render_hints,
            )
        if suffix in {".mht", ".mhtml"}:
            raw_text, render_hints = self._load_mht_with_render_hints(file_path)
            return self._build_extracted_document(
                raw_text=raw_text,
                content_type="multipart/related",
                render_hints=render_hints,
            )
        if suffix in {".html", ".htm"}:
            raw_text, render_hints = self._load_html_with_render_hints(file_path, html_password=pdf_password)
            return self._build_extracted_document(
                raw_text=raw_text,
                content_type="text/html",
                render_hints=render_hints,
            )
        if suffix == ".xls":
            raw_text = self._load_legacy_workbook(file_path)
            return self._build_extracted_document(
                raw_text=raw_text,
                content_type="application/vnd.ms-excel",
            )
        raw_text = self._load_workbook(file_path)
        return self._build_extracted_document(
            raw_text=raw_text,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    def _build_extracted_document(
        self,
        *,
        raw_text: str,
        content_type: str,
        render_hints: dict[str, Any] | None = None,
    ) -> ExtractedDocumentText:
        markdown_text = self.build_markdown(raw_text, render_hints=render_hints)
        markdown_loss_reasons = self._resolve_markdown_loss_reasons(
            markdown_text=markdown_text,
            content_type=content_type,
            render_hints=render_hints,
        )
        effective_llm_text_kind: Literal["markdown_text", "raw_text"] = (
            "raw_text" if markdown_loss_reasons else "markdown_text"
        )
        return ExtractedDocumentText(
            raw_text=raw_text,
            markdown_text=markdown_text,
            content_type=content_type,
            markdown_loss_detected=bool(markdown_loss_reasons),
            markdown_loss_reasons=markdown_loss_reasons,
            effective_llm_text_kind=effective_llm_text_kind,
        )

    def _resolve_markdown_loss_reasons(
        self,
        *,
        markdown_text: str,
        content_type: str,
        render_hints: dict[str, Any] | None = None,
    ) -> tuple[str, ...]:
        reasons = list(self._audit_html_markdown_loss(markdown_text, render_hints))
        if content_type in {"text/html", "multipart/related"}:
            audit_payload = render_hints.get("html_markdown_audit") if isinstance(render_hints, dict) else None
            if not isinstance(audit_payload, dict):
                reasons.append("html_markdown_unverified")
            else:
                token_keys = (
                    "label_tokens",
                    "table_row_tokens",
                    "inherited_row_tokens",
                    "row_kind_tokens",
                    "fund_code_tokens",
                )
                audit_tokens = [
                    str(token).strip()
                    for key in token_keys
                    for token in audit_payload.get(key, [])
                    if str(token).strip()
                ]
                if not audit_tokens:
                    reasons.append("html_markdown_unverified")
        return tuple(dict.fromkeys(reasons))

    def split_for_llm(self, document_text: str, chunk_size_chars: int) -> list[str]:
        delimiter = self._detect_section_delimiter(document_text)
        sections = [section.strip() for section in document_text.split(delimiter) if section.strip()]
        if not sections:
            logger.info("No section delimiter found. Using single chunk for LLM.")
            return [document_text]

        chunks: list[str] = []
        current = ""
        for section in sections:
            candidate = f"{current}{delimiter}{section}".strip() if current else section
            if current and len(candidate) > chunk_size_chars:
                chunks.append(current)
                current = section
            else:
                current = candidate
        if current:
            chunks.append(current)
        logger.info("Split document into %s chunk(s) for LLM", len(chunks))
        return chunks

    def looks_like_no_order_document(self, raw_text: str) -> bool:
        lowered = raw_text.lower()
        return any(marker in lowered for marker in self.NO_ORDER_MARKERS)

    def looks_like_non_instruction_document(self, raw_text: str) -> str | None:
        lowered = raw_text.lower()
        compact = re.sub(r"\s+", "", lowered)
        estimated_order_count = self.estimate_order_cell_count(raw_text)

        has_notice_title = any(marker.replace(" ", "") in compact for marker in self.NON_INSTRUCTION_TITLE_MARKERS)
        if has_notice_title and estimated_order_count == 0:
            return "문서 제목/헤더가 '설정해지금액통보' 성격의 통보 문서이고 실제 주문 표/금액 근거가 없습니다."

        has_mail_headers = all(header in lowered for header in ("subject:", "from:", "to:", "date:"))
        cover_marker_count = sum(marker in lowered for marker in self.NON_INSTRUCTION_MAIL_MARKERS)
        has_strong_instruction_marker = any(marker in lowered for marker in self.STRONG_INSTRUCTION_MARKERS)
        if (
            has_mail_headers
            and cover_marker_count >= 2
            and not has_strong_instruction_marker
            and estimated_order_count == 0
        ):
            return "메일 안내문만 있고 실제 주문 표/금액 근거가 없습니다."

        if has_mail_headers and estimated_order_count == 0:
            body_lines = self._extract_mail_body_lines(raw_text)
            has_attachment_reference = any(marker in lowered for marker in self.NON_INSTRUCTION_ATTACHMENT_MARKERS)
            has_mail_footer = any(marker in lowered for marker in self.NON_INSTRUCTION_MAIL_FOOTER_MARKERS)
            cover_header_line_count = sum(
                line.lower().startswith(("to.", "from."))
                for line in body_lines
            )
            if body_lines and len(body_lines) <= 6 and (
                has_attachment_reference
                or has_mail_footer
                or cover_header_line_count >= 2
            ):
                return "메일 안내문만 있고 실제 주문 표/금액 근거가 없습니다."

        return None

    @staticmethod
    def _extract_mail_body_lines(raw_text: str) -> list[str]:
        body_started = False
        body_lines: list[str] = []
        for line in raw_text.splitlines():
            if not body_started:
                if not line.strip():
                    body_started = True
                continue
            stripped = line.strip()
            if stripped:
                body_lines.append(stripped)
        return body_lines

    def build_task_payload(
        self,
        file_path: Path,
        *,
        chunk_size_chars: int,
        pdf_password: str | None = None,
    ) -> DocumentLoadTaskPayload:
        normalized_path = file_path.expanduser()
        loaded = self.load(normalized_path, pdf_password=pdf_password)
        target_fund_scope = self.extract_target_fund_scope(loaded.raw_text)
        allow_empty_result = self.looks_like_no_order_document(loaded.raw_text)
        expected_order_count = self.estimate_order_cell_count(
            loaded.raw_text,
            target_fund_scope=target_fund_scope,
        )
        llm_source_text = loaded.raw_text if loaded.markdown_loss_detected else loaded.markdown_text
        return DocumentLoadTaskPayload(
            source_path=str(normalized_path),
            file_name=normalized_path.name,
            pdf_password=pdf_password,
            content_type=loaded.content_type,
            raw_text=loaded.raw_text,
            markdown_text=loaded.markdown_text,
            chunks=tuple(self.split_for_llm(llm_source_text, chunk_size_chars=chunk_size_chars)),
            non_instruction_reason=None if allow_empty_result else self.looks_like_non_instruction_document(loaded.raw_text),
            allow_empty_result=allow_empty_result,
            scope_excludes_all_funds=self.scope_excludes_all_funds(target_fund_scope),
            expected_order_count=expected_order_count,
            target_fund_scope=target_fund_scope,
            markdown_loss_detected=loaded.markdown_loss_detected,
            markdown_loss_reasons=loaded.markdown_loss_reasons,
            effective_llm_text_kind=loaded.effective_llm_text_kind,
        )
