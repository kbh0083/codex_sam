from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import Literal

# `DocumentLoader`는 두 가지 배포 형태를 동시에 지원해야 한다.
#
# 1. 현재 저장소 내부 사용
#    - `from app.document_loader import DocumentLoader`
#    - `document_loaders/`는 `app` 패키지의 하위 패키지다.
#
# 2. WAS 포팅용 standalone 복사
#    - `document_loader.py + document_loaders/`만 다른 서버로 복사
#    - `from document_loader import DocumentLoader`
#    - 이 경우에는 `app` 패키지 자체가 존재하지 않을 수 있다.
#
# 여기서 중요한 점은 "standalone import 경로"와 "실제 import 오류"를 구분하는 것이다.
# 단순 `except ImportError` fallback을 쓰면 아래 두 상황이 뒤섞인다.
# - 정상적인 standalone 사용: 상위 패키지가 없어서 상대 import를 못 하는 경우
# - 실제 버그/환경 오류: mixin 내부 오타, 선택 라이브러리 누락, 의존성 설치 누락
#
# 우리는 두 번째 경우의 진짜 오류를 숨기고 싶지 않다. 그래서 예외를 넓게 잡지 않고,
# 현재 모듈이 "패키지 없이 직접 import된 상태"인지 (`__package__ in {None, ""}`)를
# 먼저 확인한 다음에만 standalone 경로를 사용한다.
#
# 정리하면:
# - 패키지 문맥이 있으면 상대 import
# - 패키지 문맥이 없으면 sibling 패키지 import
# - 실제 import 오류는 그대로 surface 해서 원인을 추적할 수 있게 유지
if __package__ in {None, ""}:  # pragma: no cover - standalone copy 경로
    from document_loaders import (
        EmlDocumentLoaderMixin,
        ExcelDocumentLoaderMixin,
        HtmlDocumentLoaderMixin,
        MhtDocumentLoaderMixin,
        PdfDocumentLoaderMixin,
    )
else:
    from .document_loaders import (
        EmlDocumentLoaderMixin,
        ExcelDocumentLoaderMixin,
        HtmlDocumentLoaderMixin,
        MhtDocumentLoaderMixin,
        PdfDocumentLoaderMixin,
    )

logger = logging.getLogger(__name__)


def normalize_fund_name_key(value: str | None) -> str:
    """펀드명 비교용 느슨한 정규화 키를 만든다.

    운용사 scope 필터는 원문 표의 펀드명과 LLM이 재구성한 펀드명을 비교해야 한다.
    이때 괄호 앞뒤 공백, 슬래시 주변 공백, 하이픈 표기 차이만으로 정상 펀드가
    빠지지 않도록, 영문/숫자/한글만 남긴 비교용 키를 별도로 만든다.
    """
    if value is None:
        return ""
    lowered = value.lower()
    return re.sub(r"[^0-9a-z가-힣]+", "", lowered)


@dataclass(slots=True)
class ExtractedDocumentText:
    """문서를 두 가지 표현으로 동시에 보관하는 컨테이너.

    raw_text:
        원문 보존용 텍스트. 검수, coverage 계산, markdown 변환 실패 추적에 사용한다.
    markdown_text:
        LLM 입력용 구조화 표현. 표/섹션 관계를 더 잘 전달하기 위해 코드가 결정론적으로 만든다.
    content_type:
        현재는 주로 로깅/출력 메타데이터 용도다.
    """

    raw_text: str
    markdown_text: str
    content_type: str
    markdown_loss_detected: bool = False
    markdown_loss_reasons: tuple[str, ...] = ()
    effective_llm_text_kind: Literal["markdown_text", "raw_text"] = "markdown_text"


@dataclass(frozen=True, slots=True)
class TargetFundScope:
    """운용사 필터링에 사용할 대상 펀드 범위.

    manager_column_present:
        원문에 운용사 정보(헤더 또는 컬럼)가 실제로 있었는지 여부.
    include_all_funds:
        운용사 정보는 있었지만, 그 정보만으로 "문서 전체가 삼성 대상"이라고 판단되는 경우
        True 다. 예: 상단 헤더에 `운용사: 삼성자산운용`만 있고 표에는 운용사 컬럼이 없는 문서.
        반대로 운용사 정보가 명시적으로 비삼성이라면 False + 빈 식별자 집합이 된다.
    fund_codes, fund_names:
        삼성 계열 운용사로 판정된 행에서 수집한 식별자들.
        코드가 있는 문서는 코드 기준으로, 코드가 없는 문서는 이름 기준으로 필터링한다.
    """

    manager_column_present: bool
    include_all_funds: bool = True
    fund_codes: frozenset[str] = frozenset()
    fund_names: frozenset[str] = frozenset()
    canonical_fund_names: frozenset[str] = frozenset()


@dataclass(frozen=True, slots=True)
class DocumentLoadTaskPayload:
    """Handler A가 파일로 저장하고 Handler B가 다시 읽어 쓸 수 있는 로딩 결과 DTO.

    이 객체는 queue나 파일 저장을 전제로 만든 "로더 단계 산출물"이다.
    핵심 의도는 두 가지다.

    1. Handler B가 원문 파일을 다시 열지 않아도 되게 한다.
       즉 Handler A가 만든 raw/markdown/chunk/scope/coverage 결과를 그대로 재사용한다.

    2. WAS 서비스 계층을 얇게 유지한다.
       서비스/핸들러는 이 DTO를 넘기기만 하고, 문서 해석은 `DocumentLoader`,
       실제 추출은 `FundOrderExtractor`가 담당하도록 책임을 분리한다.
    """

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
        """JSON 직렬화 가능한 dict로 변환한다.

        `TargetFundScope`는 dataclass + frozenset 구조라 그대로는 JSON으로 저장하기 불편하다.
        queue/file 저장용 payload에서는 집합을 정렬된 list로 바꿔 두면
        다른 언어/다른 프로세스에서도 예측 가능한 형태로 다룰 수 있다.
        """
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
        """파일/queue에서 읽은 dict를 다시 strongly-typed DTO로 복원한다."""
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
        """Handler A 산출물을 JSON 파일로 저장한다.

        queue broker 대신 파일 기반 handoff를 먼저 붙이거나,
        운영 중 문제 문서를 재현하기 위해 중간 산출물을 남길 때 바로 사용할 수 있다.

        WAS/배치 환경에서는 job별 하위 폴더나 날짜별 폴더를 함께 쓰는 경우가 많다.
        호출자가 `.../20260319/job-001/payload.json` 같은 중첩 경로를 넘길 수 있으므로,
        이 메서드 안에서 부모 디렉터리를 먼저 만들어 두는 편이 안전하다.
        이렇게 해 두면 Handler A는 "저장 경로를 정한다"까지만 신경 쓰고,
        실제 디렉터리 생성 책임은 DTO 저장 메서드가 공통으로 처리한다.
        """
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
        """Handler B가 Handler A 산출물 파일을 다시 읽어 DTO로 복원한다."""
        payload = json.loads(file_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"Document load task payload is not an object: {file_path}")
        return cls.from_dict(payload)


class DocumentLoader(
    PdfDocumentLoaderMixin,
    ExcelDocumentLoaderMixin,
    HtmlDocumentLoaderMixin,
    EmlDocumentLoaderMixin,
    MhtDocumentLoaderMixin,
):
    """문서 형식별 로더를 조합하고 공통 정규화를 수행하는 진입점.

    형식별 구현은 mixin 으로 분리되어 있고, 이 클래스는 세 가지 공통 책임만 가진다.
    1. 확장자에 맞는 raw_text 로더 선택
    2. raw_text -> markdown_text 변환
    3. 원문에 보이는 주문 버킷 수 추정(coverage 검증용)

    즉 "파일 형식 해석"과 "LLM 친화적 구조화"가 만나는 지점이다.
    """

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
    # 아래 문구들은 "통보성 제목"에서 자주 보이는 약한 힌트다.
    # 다만 iM라이프 예시처럼 제목이 `설정해지금액통보`여도 실제 주문 표와 금액이
    # 함께 있으면 처리 대상인 지시서일 수 있다.
    # 그래서 이 값만으로는 즉시 차단하지 않고, "실제 주문 버킷이 전혀 없는지"를
    # 추가로 확인한 뒤 비지시서 여부를 결정한다.
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

    def load(self, file_path: Path, pdf_password: str | None = None) -> ExtractedDocumentText:
        """로컬 문서를 읽어 raw_text/markdown_text 를 함께 반환한다.

        포인트는 어떤 형식이든 최종적으로는 같은 `ExtractedDocumentText` 계약으로
        맞춘다는 점이다. 이후 service/extractor 는 파일 형식을 몰라도 된다.
        """
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
        markdown_loss_reasons = tuple(self._audit_html_markdown_loss(markdown_text, render_hints))
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

    def split_for_llm(self, document_text: str, chunk_size_chars: int) -> list[str]:
        """정규화된 문서를 section 경계를 보존하면서 chunk 로 나눈다.

        단순 길이 기준으로 자르면 표 헤더/본문이 찢어져 stage 정확도가 떨어질 수 있다.
        그래서 먼저 section delimiter 기준으로 나누고, 그 다음에만 길이를 본다.
        """
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

    def looks_like_no_order_document(self, raw_text: str) -> bool:
        """문서가 '정상적인 0건' 인지 빠르게 판별한다.

        일부 지시서는 실제 주문 행 대신 `- 자금설정해지 지시없음 -` 같은 문구만 포함한다.
        이런 문서는 추출 실패가 아니라 empty success 로 취급하는 편이 운영상 자연스럽다.
        """
        lowered = raw_text.lower()
        return any(marker in lowered for marker in self.NO_ORDER_MARKERS)

    def looks_like_non_instruction_document(self, raw_text: str) -> str | None:
        """문서가 애초에 "변액일임펀드 설정/해지 지시서"가 아닌지 판별한다.

        `지시없음` 문서는 "주문이 없는 정상 지시서"이므로 empty success 로 처리해야 한다.
        반면 이 함수가 잡아내는 경우는 "처리 대상 문서가 아님"에 가깝다.

        예:
        - `설정해지금액통보` 제목은 있지만 실제 주문 표/금액 근거가 없는 통보성 문서
        - 첨부파일 송부 안내만 있고 실제 지시 표는 없는 cover email

        반환값:
        - `None`: 지시서로 계속 처리해도 됨
        - 문자열: 비지시서로 판단한 이유. 상위 계층은 이 이유를 예외 메시지에 포함한다.
        """
        lowered = raw_text.lower()
        compact = re.sub(r"\s+", "", lowered)
        estimated_order_count = self.estimate_order_cell_count(raw_text)

        # 1) `설정해지금액통보` 같은 제목은 "비지시서일 가능성"을 보여 주는 약한 신호다.
        #    하지만 실제 주문 표와 금액 버킷이 있으면 지시서로 처리해야 하므로,
        #    coverage 가 0일 때만 비지시서로 판정한다.
        has_notice_title = any(marker.replace(" ", "") in compact for marker in self.NON_INSTRUCTION_TITLE_MARKERS)
        if has_notice_title and estimated_order_count == 0:
            return "문서 제목/헤더가 '설정해지금액통보' 성격의 통보 문서이고 실제 주문 표/금액 근거가 없습니다."

        # 2) cover email은 메일 헤더와 안내 문구는 있지만 실제 주문 표/금액 버킷은 없다.
        #    이 경우는 LLM이 FUND_DISCOVERY_EMPTY 로 실패하기 전에 명확한 업무 예외로
        #    알려 주는 편이 운영자가 이해하기 쉽다.
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

        # 3) 일부 wrapper mail은 본문이 매우 짧고 "첨부 안내" 또는 To./From. cover note만 있다.
        #    이런 메일은 `운용지시서` 단어가 한 번 들어 있어도 본문 자체에는 주문 표가 없으므로
        #    LLM 분류 단계까지 보내지 말고 로더 단계에서 먼저 걸러 false-fail/timeout을 줄인다.
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
        """EML raw_text에서 메일 본문 비공백 라인만 추출한다.

        wrapper mail 판정은 제목/헤더가 아니라 실제 메일 본문 길이와 내용에 더 민감하다.
        이 helper는 첫 blank line 뒤의 non-empty body line만 뽑아 짧은 cover note인지
        판단할 때 사용한다.
        """
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

    @staticmethod
    def scope_excludes_all_funds(target_fund_scope: TargetFundScope) -> bool:
        """운용사 필터 결과가 '명시적 0건'인지 판정한다.

        이 판단은 예전에는 service 계층에 있었지만, 의미상으로는 문서 해석 결과에 가깝다.
        즉 표/헤더를 읽고 삼성 대상이 전혀 없다는 사실은 `DocumentLoader`가 가장 잘 안다.
        """
        return (
            target_fund_scope.manager_column_present
            and not target_fund_scope.include_all_funds
            and not target_fund_scope.fund_codes
            and not target_fund_scope.fund_names
        )

    def build_task_payload(
        self,
        file_path: Path,
        *,
        chunk_size_chars: int,
        pdf_password: str | None = None,
    ) -> DocumentLoadTaskPayload:
        """Handler A가 queue/file에 넘길 중간 산출물 DTO를 만든다.

        이 메서드는 단순히 `load()` 결과만 돌려주지 않는다.
        WAS의 다음 단계가 바로 추출을 시작할 수 있도록 아래 메타데이터까지 함께 계산한다.

        - 비지시서 판정 결과
        - 정상 0건 문서 여부
        - 삼성 대상 scope
        - 명시적 0건(scope excludes all) 여부
        - coverage 기준값
        - LLM chunk

        즉 queue 경계를 고려한 "DocumentLoader 단계의 완성형 결과"라고 보면 된다.
        """
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
            # `지시없음` 문서는 정상 0건 지시서이므로 비지시서 판정보다 allow-empty가 우선한다.
            non_instruction_reason=None if allow_empty_result else self.looks_like_non_instruction_document(loaded.raw_text),
            allow_empty_result=allow_empty_result,
            scope_excludes_all_funds=self.scope_excludes_all_funds(target_fund_scope),
            expected_order_count=expected_order_count,
            target_fund_scope=target_fund_scope,
            markdown_loss_detected=loaded.markdown_loss_detected,
            markdown_loss_reasons=loaded.markdown_loss_reasons,
            effective_llm_text_kind=loaded.effective_llm_text_kind,
        )

    def estimate_order_cell_count(
        self,
        raw_text: str,
        target_fund_scope: TargetFundScope | None = None,
    ) -> int:
        """원문에서 금액이 있는 주문 버킷 수를 대략 추정한다.

        여기서 세는 값은 최종 orders 와 1:1 비교되는 coverage 기준값이다.
        완벽한 의미 해석은 아니지만, 추출이 절반만 됐는지/과다 생성됐는지 잡는 데
        매우 효과적이다.
        """
        sections = [section.strip() for section in raw_text.split(self.RAW_SECTION_DELIMITER) if section.strip()]
        bucket_pairs: set[tuple[tuple[str, ...], str]] = set()

        for section in sections:
            # coverage도 markdown과 같은 표 경계를 봐야 한다.
            # blank line을 지워 버리면 서로 다른 table이 하나의 pipe block으로 이어져
            # 헤더가 잘못 합쳐지고 bucket dedupe가 과하게 일어난다.
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

            # PDF plain-text 본문은 pipe table로 정규화되지 않아도 실제 주문 행이 남는다.
            # 이 경로까지 coverage에 포함해야 카드프 같은 문서에서
            # ORDER_COVERAGE_MISMATCH 안전장치가 다시 살아난다.
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

    def extract_target_fund_scope(self, raw_text: str) -> TargetFundScope:
        """원문 표에서 삼성 계열 운용사에 해당하는 펀드 식별자만 수집한다.

        요구사항:
        - 운용사 헤더 또는 컬럼이 있으면 '삼성' 키워드가 포함된 운용사만 대상
        - 운용사 정보가 없으면 모든 펀드를 대상으로 간주

        이 함수는 문서 형식별 rule-base가 아니라, 공통 pipe table 구조와 헤더 의미만으로
        대상 scope를 계산한다.
        """
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

            # 운용사 헤더만 있고 실제 표가 다음 section으로 밀리는 문서를 위해
            # "표를 아직 만나지 못한 헤더 문맥"만 다음 section까지 한 번 전달한다.
            # 반대로 현재 section에서 표를 이미 처리했다면 그 헤더는 여기서 소진된 것으로 본다.
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
        """비표 영역의 운용사 헤더에서 삼성 대상 여부를 읽는다.

        예:
        - `운용사: 삼성자산운용` -> True
        - `운용사명 : 한국투신` -> False
        - 관련 헤더가 없으면 None
        """
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
        """운용사 헤더가 나온 section 중 실제 주문 본문에 가까운 쪽을 우선한다.

        preview/quoted section 도 `운용사:` 같은 문구를 포함할 수 있기 때문에,
        단순히 "가장 먼저 본 헤더"를 쓰면 실제 본문보다 약한 문맥을 고를 수 있다.
        여기서는 표 수, 주문 키워드, 날짜/금액 문맥, quoted-thread 패널티를 조합해서
        상대적으로 더 주문서다운 section 을 선택한다.
        """
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
        """한 pipe table에서 삼성 계열 운용사 펀드 식별자를 수집한다.

        반환값은 "실제 주문 표 수준으로 신뢰할 수 있는 운용사 컬럼이 있었는지" 여부다.
        """
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
        """운용사 헤더가 삼성으로 명시된 section 안의 모든 펀드 식별자를 수집한다.

        manager 컬럼이 없는 표에서는 section 헤더의 운용사 문맥이 유일한 단서일 수 있다.
        이런 경우 문서 전체를 `include_all_funds=True`로 풀어버리면 다른 section의
        비삼성 펀드까지 섞일 수 있으므로, section 내부의 식별자만 미리 수집해 scope로 쓴다.
        """
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
                    normalized_inherited_rows.append([bool(cell) for cell in inherited_row] + [False] * (len(normalized_rows[0]) - len(inherited_row)))
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

        # 총계/보조행까지 structured table에 섞이면 LLM이 요약값을 주문으로 오인하기 쉽다.
        # 실제 데이터 행만 추려서 표를 단순화한다.
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

        # LLM에는 "실제 주문 판단에 필요한 컬럼"만 주는 편이 더 안정적이다.
        # 좌수/NAV대비/합산/보수 같은 보조 컬럼까지 같이 남겨두면, 모델이 요약값이나
        # 비거래 수치를 주문 슬롯으로 오인할 수 있다.
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
            # HTML rowspan 표는 header 가 애매해도 body 값 자체가 `입금/출금/환매 신청`
            # 같은 주문 문맥인 경우가 있다. 이 열을 markdown 에서 버리면 child row 가
            # 거의 동일한 금액 행으로만 보여 중복 거래처럼 오해될 수 있으므로,
            # 값 기반으로 order-context 열을 살린다.
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

    def _suppress_shared_amounts_in_order_context_markdown(
        self,
        header: list[str],
        body_rows: list[list[str]],
        *,
        inherited_body_rows: list[list[bool]] | None = None,
    ) -> list[list[str]]:
        """같은 fund group에 공유되는 amount는 markdown에서 한 번만 노출한다.

        한화 HTML처럼 rowspan 확장으로 같은 펀드의 `입금/출금/환매 신청` 행에
        공통 `펀드계`, `T+1`, `T+2` 금액이 반복되면 raw_text 기준으로는 정보 손실을
        막을 수 있지만, markdown에서는 각 행의 독립 근거처럼 보일 수 있다.

        raw_text는 LLM backup으로 그대로 두고, structured markdown만 보수적으로 정리한다.
        같은 identity group 안에서 모든 row-kind에 동일한 non-zero amount가 반복되는
        컬럼은 첫 행에만 남기고 이후 행에서는 비운다.
        """
        if len(body_rows) < 2 or inherited_body_rows is None:
            return body_rows

        context_indices = [index for index, label in enumerate(header) if self._is_order_context_label(label)]
        if not context_indices:
            return body_rows

        code_index = self._fund_code_column_index(header, body_rows)
        name_index = self._fund_name_column_index(header)
        if code_index is None and name_index is None:
            return body_rows

        identity_indices_to_keep = {index for index in (code_index, name_index) if index is not None}
        amount_indices = [
            index
            for index, _label in enumerate(header)
            if index not in context_indices and index not in identity_indices_to_keep
        ]
        if not amount_indices:
            return body_rows

        display_rows = [list(row) for row in body_rows]
        group_start = 0
        while group_start < len(body_rows):
            group_identity = self._markdown_identity_key(
                body_rows[group_start],
                code_index=code_index,
                name_index=name_index,
            )
            if group_identity is None:
                group_start += 1
                continue

            group_end = group_start + 1
            while group_end < len(body_rows):
                if self._markdown_identity_key(
                    body_rows[group_end],
                    code_index=code_index,
                    name_index=name_index,
                ) != group_identity:
                    break
                group_end += 1

            if group_end - group_start >= 2:
                group_rows = body_rows[group_start:group_end]
                context_values = {
                    self._compact_label(row[index])
                    for row in group_rows
                    for index in context_indices
                    if index < len(row) and row[index].strip()
                }
                if len(context_values) >= 2:
                    for column_index in amount_indices:
                        values = [
                            row[column_index].strip()
                            for row in group_rows
                            if column_index < len(row)
                        ]
                        inherited_values = [
                            inherited_body_rows[row_index][column_index]
                            if row_index < len(inherited_body_rows) and column_index < len(inherited_body_rows[row_index])
                            else False
                            for row_index in range(group_start, group_end)
                        ]
                        if len(values) != len(group_rows):
                            continue
                        if not values or any(not value for value in values):
                            continue
                        if any(not self._is_amount_string(value) or self._is_zero_amount(value) for value in values):
                            continue
                        if len(set(values)) != 1:
                            continue
                        if not inherited_values or not all(inherited_values[1:]):
                            continue
                        for row_index in range(group_start + 1, group_end):
                            display_rows[row_index][column_index] = ""

            group_start = group_end

        return display_rows

    @staticmethod
    def _markdown_identity_key(
        row: list[str],
        *,
        code_index: int | None,
        name_index: int | None,
    ) -> tuple[str, ...] | None:
        """markdown row grouping용 최소 identity를 계산한다."""
        code = row[code_index].strip() if code_index is not None and code_index < len(row) else ""
        name = row[name_index].strip() if name_index is not None and name_index < len(row) else ""
        identity = tuple(value for value in (code, name) if value)
        return identity or None

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
        """헤더가 약해도 본문 값이 "행 구분" 열인지 아주 보수적으로 판단한다.

        이 helper는 한화 HTML처럼 rowspan 확장 후 `입금/출금/환매 신청` 값만 본문에
        남는 weak-header 열을 살리기 위한 특례다. raw text 정보 손실을 markdown에서
        보완하려는 목적이므로, `보험료입금`, `입금소계`, `특별계정운용보수` 같은
        detail/사유 컬럼까지 `구분`으로 오인하면 안 된다.

        따라서 아래 조건을 모두 만족할 때만 True를 반환한다.
        - 비어 있지 않은 비금액 값이 최소 2개 이상 존재
        - 그 값이 모두 "행 구분"으로 인정되는 좁은 어휘 집합에 속함
        """
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
        """정확한 키워드 사전에 없더라도 주문 금액성 헤더를 넓게 인식한다.

        예: `설정(해지)금액`, `펀드납입(인출)금액`, `판매회사분결제액`
        """
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

    def _estimate_pipe_block_order_cells(self, rows: list[str]) -> int:
        """한 pipe block 안의 주문 버킷 수만 빠르게 계산하는 얇은 래퍼다."""
        return len(self._collect_pipe_block_order_buckets(rows))

    def _collect_pipe_block_order_buckets(
        self,
        rows: list[str],
        target_fund_scope: TargetFundScope | None = None,
    ) -> set[tuple[tuple[str, ...], str]]:
        """Count settlement buckets inside one pipe-delimited block.

        핵심 아이디어는 "금액 셀 수"가 아니라 "같은 식별자 + 같은 결제 버킷"을 세는 것이다.
        실무 문서는 한 거래를
        - 상세행
        - 소계행
        - 순액행
        으로 반복해 적는 경우가 많아서, 단순 amount cell 개수는 과대 계상되기 쉽다.
        이 함수는 행 단위 상세표가 있더라도 최종 주문 버킷 수에 더 가깝게 추정하도록
        식별자 컬럼과 결제 버킷을 기준으로 dedupe 한다.
        """
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

        # PDF 표는 한 주문이 여러 행으로 쪼개져 나오는 경우가 흔하다.
        # 예를 들어 첫 행에는 거래유형/펀드코드, 다음 행에는 판매사/펀드명만 있고
        # 금액 열은 두 행 모두 채워지는 식이다. coverage는 "최종 주문 버킷 수"를
        # 세는 용도이므로, 인접 행이 같은 버킷을 공유하면서 코드/이름이 상호 보완되면
        # 하나의 identity로 합쳐서 dedupe 한다.
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
        """plain-text PDF 본문에서 section + fund_code + amount 기준 coverage를 센다.

        pipe table로 복원되지 않은 PDF는 `Fund Name Code ... Amount Bank` 같은 줄이 그대로 남는다.
        이 경우에도 섹션(Subscription/Redemption)과 펀드코드, 마지막 금액 토큰을 잡으면
        최종 주문 건수를 꽤 안정적으로 추정할 수 있다.
        """
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
        """plain-text로 풀린 schedule 표를 markdown/coverage용 행렬로 복원한다.

        주로 PDF 텍스트 레이어가 표를 완전히 보존하지 못할 때 나타나는 패턴이다.
        대표 형태:
        - 제목/안내 문구
        - 값 컬럼 헤더 (`펀드명 운용사 ... 당일이체금액`)
        - 날짜 헤더 (`펀드코드 펀드코드 2025-11-28 ...`)
        - 데이터 행
        """
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

        # execution/scheduled 금액이 있으면 입금액/출금액/좌수 같은 보조 열은 markdown에서 뺀다.
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
        lower_line = stripped.lower()
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
        """plain-text 주문 줄에서 실제 주문 금액 토큰 인덱스를 찾는다.

        NAV와 주문금액이 함께 있는 행, 주문금액만 있는 행, ISO 날짜 뒤 숫자 하나만 있는 행을
        구분해야 해서 헤더 문맥까지 같이 본다.
        """
        date_index = self._plain_text_first_date_token_index(tokens)
        if date_index is not None:
            post_date_numeric_indices = [
                index
                for index in range(date_index + 1, len(tokens))
                if self._is_amount_string(tokens[index]) and not self._is_zero_amount(tokens[index])
            ]
            header_has_nav = self._plain_text_header_mentions_nav(header_line)
            header_has_amount = self._plain_text_header_mentions_amount(header_line)
            # plain-text PDF 주문표는 보통 `... Date NAV Amount Bank` 구조다.
            # 날짜 뒤 숫자가 1개뿐이면 NAV만 있고 실제 주문금액은 비어 있는 줄일 가능성이 높다.
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
        """한 행이 대표하는 주문 버킷(T0, T+1, T0_SUB 등)을 계산한다.

        coverage와 extractor가 서로 어긋나지 않도록,
        net execution 컬럼은 같은 결제 버킷 안에서만 explicit SUB/RED를 대체하고
        net 값이 없는 bucket은 방향별 주문을 그대로 유지하는 현재 규칙을 반영한다.
        """
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
                # execution/net 금액은 "같은 결제 버킷"에 대해서만 explicit SUB/RED 분리를
                # 대체해야 한다. 예를 들어 한 행에
                # - T0 설정금액 / 해지금액
                # - T+1 순유입예상금액
                # 이 함께 있으면, T+1 bucket만 net 1건으로 보고 T0 bucket은 여전히
                # SUB/RED 2건으로 세어야 extractor와 coverage가 일치한다.
                #
                # 따라서 행 전체에 execution 값이 있는지만 보면 안 되고,
                # 현재 amount column이 속한 bucket에 non-zero execution evidence가
                # 실제로 존재하는지 확인한 뒤에만 explicit direction suffix를 생략한다.
                #
                # 방향 bucket은 원본 행이 아니라 propagated row 기준으로 읽는다.
                # 그래야 `구분`이 첫 행에만 적히고 아래 행/소계행은 비어 있는 표에서도
                # 같은 입금/출금 문맥을 유지한 채 dedupe 할 수 있다.
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
        """현재 행에서 실제 non-zero execution/net 금액이 있는 결제 버킷만 모은다.

        최근 규칙에서는 execution/net 컬럼이 "행 어딘가에 하나라도 존재"한다고 해서
        모든 bucket을 net 1건으로 보면 안 된다.

        예:
        - `설정금액`, `해지금액`은 T0 bucket
        - `T+1일 순유입예상금액`은 T+1 bucket

        이 상황에서 T+1 net 값이 있다고 T0 explicit 금액까지 합쳐 세면
        extractor가 `T0 SUB`, `T0 RED`, `T+1 SUB`로 해석하는 규칙과 어긋난다.

        그래서 coverage 단계에서도 "현재 행에 execution 값이 있는가"가 아니라
        "현재 행의 어느 bucket에 execution 값이 있는가"를 계산해서,
        같은 bucket에 대해서만 explicit SUB/RED 분리를 생략한다.
        """
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
        """행 구분 컬럼이 있을 때 authoritative mixed amount 열만 남긴다.

        하나생명 XLSX처럼 같은 logical order를
        - `설정해지금액`
        - `펀드납입출금액`
        - `판매회사분결제금액`
        으로 반복 표기하는 표는 execution/settlement 열을 그대로 세면
        SUB/RED 분리가 사라져 coverage가 1건으로 뭉개진다.

        이 helper는 row-level order context가 있는 표에서만 작동한다.
        같은 결제 bucket 안에 mixed-direction amount 열이 여러 개 있으면,
        가장 authoritative한 열만 남기고 나머지는 duplicate evidence로 간주한다.
        """
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
        """coverage 집계용 명시적 주문 방향(SUB/RED)을 찾는다.

        최근 규칙에서는
        - explicit 설정/해지 금액만 있고
        - 순유입/결제/당일이체 같은 net execution 컬럼이 없으면
        설정과 해지를 separate order로 세야 한다.

        따라서 coverage 단계도 `T0` 하나로 합치지 않고,
        가능한 경우 `T0_SUB`, `T0_RED`처럼 방향을 구분한 bucket으로 나눠야 한다.
        """
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
        """설정/입금 계열은 SUB, 해지/출금 계열은 RED로 정규화한다.

        coverage 단계는 extractor보다 앞에서 실행되므로, 여기서 방향을 놓치면
        extractor가 실제로 `SUB`와 `RED`를 각각 2건으로 만드는 문서도
        loader coverage는 `T0` 1건으로만 세어 `ORDER_COVERAGE_MISMATCH`가 난다.

        특히 최근 요구사항에서는
        - `설정금액` / `해지금액`
        - `입금액` / `출금액`
        - `Buy / Amount` / `Sell / Amount`
        처럼 라벨 자체에 방향이 직접 적힌 경우를 separate order로 세야 한다.

        반대로 `설정(예탁) 및 해지금액`처럼 한 라벨에 양방향 단어가 모두 섞인 경우는
        어느 한쪽으로 단정하면 오탐이 되므로 방향을 돌려주지 않는다.
        """
        normalized = DocumentLoader._normalize_plain_text_section_label(value)
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
        """해지/설정 신청 컬럼의 future bucket을 비고 문맥으로 보정한다.

        흥국생명처럼 same-day 확정 금액과 청구분 신청 금액이 같은 행에 함께 있는 표는
        `해지신청`, `설정신청` 컬럼을 그대로 T0로 세면 coverage가 과소 계상된다.
        신청 컬럼일 때만 같은 행의 `비고`에서 금액별 예정일을 읽어 future bucket을 만든다.
        """
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

        # 인접 행 중 같은 버킷을 공유하는 행이 코드/이름을 보완하면 함께 묶는다.
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
        """주문 버킷 dedupe에 쓸 식별자 컬럼 인덱스를 추론한다.

        일반적으로 펀드코드/펀드명/운용사/수탁코드 같은 컬럼은 유지하고,
        구분/내용/상세/날짜/금액/좌수 같은 컬럼은 제외한다.
        """
        indices = [index for index, label in enumerate(header) if self._is_identity_label(label)]
        indices = self._prune_identity_indices_by_body_content(header, body_rows, indices)
        if indices:
            return indices

        # 헤더가 불완전한 문서에서는 첫 번째 amount 컬럼 전까지의 non-amount 텍스트 셀을
        # 식별자로 간주한다. 이렇게 하면 양식 이름에 기대지 않고도 기본적인 dedupe가 가능하다.
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
        """본문 값이 대부분 금액인 컬럼은 identity 후보에서 제외한다.

        일부 양식은 rowspan 때문에 첫 행에만 순액/보수/이용료 컬럼이 채워지고
        다음 행은 비게 된다. 이 컬럼들이 identity에 섞이면 같은 펀드의 상세행이
        서로 다른 거래처럼 집계될 수 있다.

        헤더만 보고 identity를 고르면 도메인별 예외가 너무 많아지므로,
        본문에서 실제 값의 성격을 함께 보고 금액성 보조 컬럼을 제거한다.
        """
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

        # 마지막 fallback: 선행 컬럼 중 "금액/좌수"가 아닌 값을 식별자로 사용한다.
        leading_values = tuple(
            cell.strip()
            for cell in row[:5]
            if cell.strip() and not self._is_amount_string(cell) and not any(keyword in cell.lower() for keyword in self.NON_AMOUNT_KEYWORDS)
        )
        return leading_values or None

    def _row_fund_code_value(self, row: list[str], code_index: int | None) -> str:
        """row에서 실제 fund code로 쓸 수 있는 값을 안전하게 꺼낸다."""
        if code_index is None or code_index >= len(row):
            return ""
        value = row[code_index].strip()
        if not value or self._is_total_like_text(value):
            return ""
        return value if self._looks_like_fund_code(value) else ""

    def _row_fund_name_value(self, row: list[str], name_index: int | None) -> str:
        """row에서 실제 fund name으로 쓸 수 있는 값을 안전하게 꺼낸다."""
        if name_index is None or name_index >= len(row):
            return ""
        value = row[name_index].strip()
        if not value or self._looks_like_fund_code(value) or self._is_total_like_text(value):
            return ""
        return value

    def _manager_column_index(self, header: list[str]) -> int | None:
        """헤더에서 운용사 컬럼 인덱스를 찾는다."""
        # 운용사 컬럼은 "운용사명"을 가장 강한 신호로 본다.
        # DB_250826.xlsx 같은 표는 `운용사코드 | 운용사명`이 같이 나오는데,
        # 단순 부분일치로 보면 운용사코드를 manager 컬럼으로 잘못 잡을 수 있다.
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
        """헤더와 본문을 함께 보고 실제 fund code 컬럼 인덱스를 찾는다."""
        preferred_keywords = ("펀드코드", "fund code")
        fallback_keywords = ("수탁코드", "code")

        for keywords in (preferred_keywords, fallback_keywords):
            for index, label in enumerate(header):
                segments = self._label_segments(label)
                if any(any(self._segment_contains_keyword(segment, keyword) for keyword in keywords) for segment in segments):
                    return index

        # 일부 원장은 실제 펀드코드 컬럼을 `운용사코드`처럼 잘못 라벨링한다.
        # 이런 경우에도 manager 컬럼 바로 앞의 코드형 열이 실제 펀드 식별자인 경우가 많아,
        # 본문 값까지 보고 fallback 후보를 고른다.
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
        """헤더에서 펀드명/상품명/자산명 계열 컬럼 위치를 찾는다."""
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
        """공백/구두점을 제거한 헤더 비교용 소문자 키를 만든다."""
        return re.sub(r"[^0-9a-z가-힣]+", "", value).lower()

    def _segment_contains_keyword(self, segment: str, keyword: str) -> bool:
        """공백·구두점 차이를 무시하고 헤더 segment에 keyword가 들어 있는지 본다."""
        lower_segment = segment.lower()
        lower_keyword = keyword.lower()
        if lower_keyword in lower_segment:
            return True
        return self._compact_label(lower_keyword) in self._compact_label(lower_segment)

    @staticmethod
    def _has_meaningful_manager_value(value: str) -> bool:
        """운용사 값이 비어 있지 않고 실제 의미 있는 텍스트인지 판단한다."""
        normalized = value.strip()
        return bool(normalized and normalized not in {"-", "--", "—", "n/a", "na", "null"})

    @staticmethod
    def _matches_target_fund_scope(
        fund_code: str,
        fund_name: str,
        target_fund_scope: TargetFundScope,
    ) -> bool:
        """현재 행/주문이 삼성 대상 scope에 포함되는지 판정한다."""
        if not target_fund_scope.manager_column_present or target_fund_scope.include_all_funds:
            return True
        if fund_code and fund_code in target_fund_scope.fund_codes:
            return True
        if fund_name and fund_name in target_fund_scope.fund_names:
            return True
        if fund_name and normalize_fund_name_key(fund_name) in target_fund_scope.canonical_fund_names:
            return True
        return False

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

        # split-row PDF처럼 헤더가 `설정(해지)좌수 / 설정(해지)금액`,
        # `펀드납입(인출)금액 / 판매회사분결제액`처럼 합쳐진 경우는
        # 명시적인 T+N 정보가 없어도 같은 T0 주문 버킷으로 보는 편이 맞다.
        # 그렇지 않으면 한 주문이 금액 컬럼 수만큼 여러 bucket으로 쪼개져
        # coverage가 과대 계상될 수 있다.
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

        # coverage는 "코드가 있는 표"뿐 아니라 "펀드명만 있는 표"도 다뤄야 한다.
        # fund_code가 실제로 보이고 비영 금액이 있으면 텍스트 identity가 없어도
        # 데이터 행으로 본다. 흥국생명처럼 `펀드코드 + 금액`만 있는 표가 여기에 해당한다.
        if has_code:
            return True
        return bool(text_identity_cells)

    def _is_short_continuation_row(self, row: list[str], header: list[str]) -> bool:
        """PDF split-row 표의 짧은 continuation row를 별도로 판정한다.

        최근 `_is_likely_data_row()`를 보수적으로 유지하면 `펀드명 + 금액`처럼
        2셀만 남는 continuation row가 coverage와 markdown에서 빠질 수 있다.
        반대로 이 로직을 너무 완화하면 `화면번호 : | 13001` 같은 메타데이터 행이
        데이터 행으로 오인된다. 따라서 헤더 문맥을 함께 보면서,
        identity 컬럼에 텍스트가 있고 amount 컬럼에 비영 금액이 있는 2셀 행만
        continuation row로 인정한다.
        """
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
        """rowspan/병합 셀처럼 비어 있는 body 값을 같은 주문 문맥으로 보완한다.

        KDB생명 원장처럼 `입금소계/출금소계`가 빈 identity 셀 아래에 이어지는 문서는
        원문 raw_text만 보면 정보가 충분하지만, markdown 구조화 단계에서는
        펀드명/코드/구분이 사라져 LLM이 실제 주문 버킷을 놓칠 수 있다.

        여기서는 강한 식별자와 `구분` 컬럼만 바로 위 행 값으로 carry 해서
        소계행이 같은 펀드의 T0/T+N 근거로 남도록 만든다.
        """
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

    @staticmethod
    def _looks_like_fund_code(value: str) -> bool:
        """값이 펀드코드 형태인지 간단히 판별한다."""
        if not value:
            return False
        if value != value.strip() or " " in value:
            return False
        return bool(re.fullmatch(r"[A-Z0-9]{3,}", value))

    def _looks_like_text_cell(self, value: str) -> bool:
        """값이 설명 텍스트 셀인지, 숫자/코드 셀인지 거칠게 구분한다."""
        if not value:
            return False
        if value.strip().lower() in {"true", "false"}:
            return False
        if self._is_amount_string(value):
            return False
        return not self._looks_like_fund_code(value)

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
            """`익익영업일` 같은 표현을 `T+2` 별칭으로 바꾼다."""
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
        return DocumentLoader._normalize_document_amount_token(value) is not None

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
        """문자열 금액을 숫자로 해석한다.

        coverage/markdown 단계에서 `0.3억`, `1.8억`, `283,660` 같은 표기까지
        비영 금액으로 인식하기 위한 최소 보강이다.
        """
        normalized = DocumentLoader._normalize_document_amount_token(value)
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
        """문서 셀에서 금액 해석에 불필요한 통화 표기를 제거한다.

        raw_text는 원문을 그대로 보존하고, markdown/coverage/deterministic parser 같은
        document-side 판단에서만 아래 표현을 순수 numeric token으로 본다.

        예:
        - `₩ 12,082,790` -> `12,082,790`
        - `-₩23 ,182,592` -> `-23,182,592`
        - `₩0` -> `0`
        - `1,234원` -> `1,234`
        """
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
