from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal


@dataclass(slots=True)
class ExtractedDocumentText:
    """문서를 두 가지 표현으로 동시에 보관하는 컨테이너."""

    raw_text: str
    markdown_text: str
    content_type: str
    markdown_loss_detected: bool = False
    markdown_loss_reasons: tuple[str, ...] = ()
    effective_llm_text_kind: Literal["markdown_text", "raw_text"] = "markdown_text"


@dataclass(frozen=True, slots=True)
class TargetFundScope:
    """운용사 필터링에 사용할 대상 펀드 범위."""

    manager_column_present: bool
    include_all_funds: bool = True
    fund_codes: frozenset[str] = frozenset()
    fund_names: frozenset[str] = frozenset()
    canonical_fund_names: frozenset[str] = frozenset()


@dataclass(frozen=True, slots=True)
class DocumentLoadTaskPayload:
    """Handler A가 파일로 저장하고 Handler B가 다시 읽어 쓸 수 있는 로딩 결과 DTO."""

    source_path: str
    file_name: str
    pdf_password: str | None
    content_type: str
    raw_text: str
    markdown_text: str
    chunks: tuple[str, ...]
    non_instruction_reason: str | None
    allow_empty_result: bool
    scope_excludes_all_funds: bool
    expected_order_count: int
    target_fund_scope: TargetFundScope
    markdown_loss_detected: bool = False
    markdown_loss_reasons: tuple[str, ...] = ()
    effective_llm_text_kind: Literal["markdown_text", "raw_text"] = "markdown_text"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["chunks"] = list(self.chunks)
        payload["target_fund_scope"] = {
            "manager_column_present": self.target_fund_scope.manager_column_present,
            "include_all_funds": self.target_fund_scope.include_all_funds,
            "fund_codes": sorted(self.target_fund_scope.fund_codes),
            "fund_names": sorted(self.target_fund_scope.fund_names),
            "canonical_fund_names": sorted(self.target_fund_scope.canonical_fund_names),
        }
        payload["markdown_loss_reasons"] = list(self.markdown_loss_reasons)
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> DocumentLoadTaskPayload:
        scope_payload = dict(payload.get("target_fund_scope") or {})
        target_scope = TargetFundScope(
            manager_column_present=bool(scope_payload.get("manager_column_present", False)),
            include_all_funds=bool(scope_payload.get("include_all_funds", True)),
            fund_codes=frozenset(str(value) for value in scope_payload.get("fund_codes", [])),
            fund_names=frozenset(str(value) for value in scope_payload.get("fund_names", [])),
            canonical_fund_names=frozenset(str(value) for value in scope_payload.get("canonical_fund_names", [])),
        )
        return cls(
            source_path=str(payload.get("source_path", "")),
            file_name=str(payload.get("file_name", "")),
            pdf_password=payload.get("pdf_password"),
            content_type=str(payload.get("content_type", "")),
            raw_text=str(payload.get("raw_text", "")),
            markdown_text=str(payload.get("markdown_text", "")),
            chunks=tuple(str(chunk) for chunk in payload.get("chunks", [])),
            non_instruction_reason=payload.get("non_instruction_reason"),
            allow_empty_result=bool(payload.get("allow_empty_result", False)),
            scope_excludes_all_funds=bool(payload.get("scope_excludes_all_funds", False)),
            expected_order_count=int(payload.get("expected_order_count", 0)),
            target_fund_scope=target_scope,
            markdown_loss_detected=bool(payload.get("markdown_loss_detected", False)),
            markdown_loss_reasons=tuple(str(reason) for reason in payload.get("markdown_loss_reasons", [])),
            effective_llm_text_kind=str(payload.get("effective_llm_text_kind", "markdown_text"))
            if str(payload.get("effective_llm_text_kind", "markdown_text")) in {"markdown_text", "raw_text"}
            else "markdown_text",
        )

    def write_json(self, file_path: Path) -> dict[str, Any]:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        serialized = json.dumps(self.to_dict(), ensure_ascii=False, indent=2)
        file_path.write_text(serialized, encoding="utf-8")
        return {
            "ok": True,
            "path": str(file_path),
            "size_bytes": len(serialized.encode("utf-8")),
        }

    @classmethod
    def read_json(cls, file_path: Path) -> DocumentLoadTaskPayload:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"Document load task payload is not an object: {file_path}")
        return cls.from_dict(payload)
