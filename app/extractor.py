from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
import re
import time
import unicodedata
from contextvars import ContextVar, copy_context
from datetime import datetime
from threading import RLock
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from string import Formatter
from typing import Any, ClassVar, TypeVar

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
import yaml

from app.amount_normalization import canonicalize_transfer_amount, format_source_transfer_amount
from app.config import Settings
from app.document_loader import DocumentLoadTaskPayload, DocumentLoader, TargetFundScope, normalize_fund_name_key
from app.schemas import ExtractionResult, OrderExtraction, OrderType, SettleClass

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

# queue 기반 WAS에서는 handler B가 `FundOrderExtractor`를 직접 호출한 뒤
# "이 결과를 저장해도 되는지"를 바로 판단할 수 있어야 한다.
# 그래서 저장 차단이 필요한 issue 집합을 service 바깥에서도 재사용할 수 있도록
# extractor 모듈 수준 상수로 둔다.
#
# 주의:
# - `ORDER_COVERAGE_MISMATCH`는 loader 추정 과대 때문에 false-fail을 만들기 쉬워서
#   현재는 hard blocker 집합에 넣지 않는다.
# - coverage 문제는 최종 orders와 독립 parser corroboration을 본 뒤
#   `ORDER_COVERAGE_ESTIMATE_MISMATCH` warning으로만 남길 수 있다.
BLOCKING_EXTRACTION_ISSUES = {
    "LLM_INVALID_RESPONSE_FORMAT",
    "FUND_DISCOVERY_EMPTY",
    "TRANSACTION_SLOT_EMPTY",
    "TRANSFER_AMOUNT_EMPTY",
    "SETTLE_CLASS_EMPTY",
    "FUND_METADATA_INCOMPLETE",
    "TRANSFER_AMOUNT_MISSING",
    "TRANSFER_AMOUNT_CONFLICT",
    "SETTLE_CLASS_MISSING",
    "ORDER_TYPE_MISSING",
    "T_DAY_MISSING",
    "BASE_DATE_STAGE_PARTIAL",
    "T_DAY_STAGE_PARTIAL",
    "TRANSFER_AMOUNT_STAGE_PARTIAL",
    "SETTLE_CLASS_STAGE_PARTIAL",
    "ORDER_TYPE_STAGE_PARTIAL",
}

SOFT_COVERAGE_WARNING = "ORDER_COVERAGE_ESTIMATE_MISMATCH"
# 변액일임 추출기는 다른 서비스와 같은 전역 Settings 기본값을 공유하지 않는다.
# 실제 운영에서 필요한 LLM 설정은 이 모듈 안에서만 고정/제어한다.
VA_LLM_TEMPERATURE = 0.0
VA_LLM_MAX_TOKENS = 16384
VA_LLM_TIMEOUT_SECONDS = 120
VA_LLM_STAGE_BATCH_SIZE = 12
VA_LLM_RETRY_ATTEMPTS = 3
VA_LLM_RETRY_BACKOFF_SECONDS = 1.5
VA_LLM_CHUNK_SIZE_CHARS = 12000
# stage issue retry는 단계별 코드 정책으로 고정한다.
# 1단계(instruction_document)는 최대 1회, 나머지 단계는 최대 2회 재호출한다.
INSTRUCTION_DOCUMENT_STAGE_ISSUE_RETRY_ATTEMPTS = 1
OTHER_STAGE_ISSUE_RETRY_ATTEMPTS = 2
MAX_PARALLEL_LLM_BATCH_WORKERS = 1
STAGE_SPECIFIC_MAX_TOKENS: dict[str, int] = {
    "instruction_document": 4096,
    "base_date": 4096,
    "settle_class": 8192,
    "order_type": 4096,
}
T_DAY_STAGE_BATCH_SIZE = 6
INTERNAL_RETRY_FINDING_PREFIX = "_RETRY_"
_ACTIVE_EXTRACT_LOG_PATH: ContextVar[Path | None] = ContextVar(
    "_ACTIVE_EXTRACT_LOG_PATH",
    default=None,
)
_ACTIVE_EXTRACT_METRICS: ContextVar[Any | None] = ContextVar(
    "_ACTIVE_EXTRACT_METRICS",
    default=None,
)
_ACTIVE_STAGE_RETRY_CONTEXT: ContextVar[dict[str, Any] | None] = ContextVar(
    "_ACTIVE_STAGE_RETRY_CONTEXT",
    default=None,
)
REASON_SUMMARY_MAX_CHARS = 160
STAGE_ITEM_METADATA_FIELDS = frozenset({"reason_code"})
STAGE_RESULT_METADATA_FIELDS = frozenset({"reason_summary"})
INSTRUCTION_DOCUMENT_REASON_CODES = frozenset(
    {
        "INSTR_ORDER_ROWS_PRESENT",
        "INSTR_NET_OR_SCHEDULE_PRESENT",
        "INSTR_OUTFLOW_AMOUNT_PRESENT",
        "NONINSTR_COVER_EMAIL",
        "NONINSTR_ATTACHMENT_WRAPPER",
        "NONINSTR_NO_ACTIONABLE_ROWS",
        "NONINSTR_APPROVAL_NOTICE",
    }
)
FUND_INVENTORY_REASON_CODES = frozenset(
    {
        "FUND_MANAGER_SCOPED",
        "FUND_NO_MANAGER_SCOPE",
        "FUND_NET_AMOUNT_ROW",
        "FUND_EXPLICIT_SUB_ROW",
        "FUND_EXPLICIT_RED_ROW",
        "FUND_CODE_ONLY_ROW",
        "FUND_OTHER_VERIFIED",
    }
)
BASE_DATE_REASON_CODES = frozenset(
    {
        "DATE_DOC_HEADER",
        "DATE_GRID_ALIGNED",
        "DATE_SETTLEMENT_LABEL",
        "DATE_SECTION_ROW",
        "DATE_EMAIL_FALLBACK",
        "DATE_OTHER_VERIFIED",
    }
)
T_DAY_REASON_CODES = frozenset(
    {
        "SLOT_T0_NET",
        "SLOT_T0_EXPLICIT_SUB",
        "SLOT_T0_EXPLICIT_RED",
        "SLOT_TPLUS_SCHEDULE",
        "SLOT_BUSINESS_DAY_HEADER",
        "SLOT_SECTION_DIRECTION",
        "SLOT_OTHER_VERIFIED",
    }
)
TRANSFER_AMOUNT_REASON_CODES = frozenset(
    {
        "AMOUNT_NET_COLUMN",
        "AMOUNT_EXPLICIT_SUB",
        "AMOUNT_EXPLICIT_RED",
        "AMOUNT_SCHEDULE_BUCKET",
        "AMOUNT_SIGNED_OUTFLOW",
        "AMOUNT_OTHER_VERIFIED",
    }
)
SETTLE_CLASS_REASON_CODES = frozenset(
    {
        "SETTLE_SAME_DAY_LABEL",
        "SETTLE_FUTURE_LABEL",
        "SETTLE_BUSINESS_DAY_HEADER",
        "SETTLE_PROCESSED_RESULT",
        "SETTLE_TDAY_FALLBACK",
        "SETTLE_OTHER_VERIFIED",
    }
)
ORDER_TYPE_REASON_CODES = frozenset(
    {
        "ORDER_SIGN_POSITIVE",
        "ORDER_SIGN_NEGATIVE",
        "ORDER_LABEL_SUB",
        "ORDER_LABEL_RED",
        "ORDER_SECTION_SUB",
        "ORDER_SECTION_RED",
        "ORDER_TRANSACTION_TYPE",
        "ORDER_OTHER_VERIFIED",
    }
)
COUNTERPARTY_DUPLICATE_COPY_REASONS = {
    "카디프": "duplicate XLSX copy; use PDF attachment",
    "하나생명": "duplicate PDF copy; use XLSX attachment",
    "흥국생명-hanais": "duplicate PDF copy; use XLSX attachment",
}
HANAIS_DUPLICATE_PDF_HINT_TOKENS = (
    "duplicate pdf copy",
    "use xlsx attachment",
    "xlsx attachment for extraction",
    "xlsx 첨부",
    "xlsx attachment",
    "pdf 사본",
)


def _normalize_reason_code_value(value: object) -> str | None:
    """자유서술이 섞여도 내부 reason_code는 짧은 상수형 태그로 정규화한다."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = unicodedata.normalize("NFC", text).upper()
    text = re.sub(r"[\s\-]+", "_", text)
    text = re.sub(r"[^A-Z0-9_]+", "", text)
    text = re.sub(r"_+", "_", text).strip("_")
    if not text or len(text) > 64:
        return None
    return text


def _normalize_reason_summary_value(value: object) -> str:
    """stage-level 요약은 한 줄 factual summary만 유지한다."""
    if value is None:
        return ""
    text = unicodedata.normalize("NFC", " ".join(str(value).split()))
    if not text:
        return ""
    return text[:REASON_SUMMARY_MAX_CHARS].rstrip()


def apply_only_pending_filter(result: ExtractionResult, *, only_pending: bool) -> ExtractionResult:
    """최종 추출 결과에 `only_pending` 출력 규칙을 적용한다.

    요구사항은 이름과 달리 "예정분만 남긴다"가 아니라 아래 순서를 강제한다.

    - `only_pending=False`
      - 결과를 그대로 둔다.
    - `only_pending=True`
      1. `settle_class == PENDING` 인 항목을 제거한다.
      2. 남은 항목(대체로 CONFIRMED)의 `settle_class`를 모두 `PENDING` 으로 바꾼다.

    이 규칙은 추출/검증 로직 자체를 바꾸는 것이 아니라,
    "검증까지 끝난 최종 결과를 어떤 계약으로 외부에 내보낼지"를 바꾸는 후처리다.
    그래서 coverage, blocking issue 판단이 끝난 뒤 마지막 결과에만 적용한다.
    """
    if not only_pending:
        return result

    filtered_orders = [
        order.model_copy(update={"settle_class": SettleClass.PENDING})
        for order in result.orders
        if order.settle_class is not SettleClass.PENDING
    ]
    return ExtractionResult(orders=filtered_orders, issues=list(result.issues))


class StageModel(BaseModel):
    """각 stage 응답 모델의 공통 기반 클래스.

    LLM 응답에는 불필요한 필드가 섞일 수 있으므로 `extra="ignore"` 로 두고,
    우리가 정의한 계약 필드만 읽는다.
    """
    model_config = ConfigDict(extra="ignore")


class StageItemModel(StageModel):
    """다음 stage에 전파되는 item-level reason metadata를 공통 처리한다."""
    allowed_reason_codes: ClassVar[frozenset[str] | None] = None
    reason_code: str | None = None

    @field_validator("reason_code", mode="before")
    @classmethod
    def _normalize_reason_code(cls, value: object) -> str | None:
        normalized = _normalize_reason_code_value(value)
        allowed_reason_codes = getattr(cls, "allowed_reason_codes", None)
        if normalized is not None and allowed_reason_codes and normalized not in allowed_reason_codes:
            raise ValueError(f"Invalid reason_code for {cls.__name__}: {normalized}")
        return normalized

    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """reason_code가 비어 있으면 item JSON에서 생략한다."""
        payload = super().model_dump(*args, **kwargs)
        if payload.get("reason_code") is None:
            payload.pop("reason_code", None)
        return payload


class StageResultModel(StageModel):
    """stage-level reason summary와 issue 목록을 공통 처리한다."""
    reason_summary: str = ""
    issues: list[str] = Field(default_factory=list)

    @field_validator("reason_summary", mode="before")
    @classmethod
    def _normalize_reason_summary(cls, value: object) -> str:
        return _normalize_reason_summary_value(value)


class FundSeedItem(StageItemModel):
    """Stage 1이 발견한 펀드 inventory 후보 1건이다."""
    allowed_reason_codes: ClassVar[frozenset[str]] = FUND_INVENTORY_REASON_CODES
    fund_code: str = ""
    fund_name: str = ""

    @field_validator("fund_code", "fund_name", mode="before")
    @classmethod
    def _strip_text(cls, value: object) -> str:
        """seed 텍스트 필드의 공백/None을 stage 공통 규칙으로 정리한다."""
        if value is None:
            return ""
        return str(value).strip()


class FundSeedResult(StageResultModel):
    """Stage 1 `fund_inventory` 응답 전체 계약이다."""
    items: list[FundSeedItem] = Field(default_factory=list)


class InstructionDocumentItem(StageItemModel):
    """문서 자체가 실제 지시서인지 판단한 결과 1건이다."""
    allowed_reason_codes: ClassVar[frozenset[str]] = INSTRUCTION_DOCUMENT_REASON_CODES
    is_instruction_document: bool | None = None
    reason: str = ""

    @field_validator("reason", mode="before")
    @classmethod
    def _strip_reason(cls, value: object) -> str:
        """사유 필드는 빈값/공백을 정리해 비교 가능하게 만든다."""
        if value is None:
            return ""
        return str(value).strip()


class InstructionDocumentResult(StageResultModel):
    """사전 문서 판별 stage 응답 전체 계약이다."""
    items: list[InstructionDocumentItem] = Field(default_factory=list)


class FundBaseDateItem(StageItemModel):
    """Stage 2가 펀드별 기준일을 붙인 결과 1건이다."""
    allowed_reason_codes: ClassVar[frozenset[str]] = BASE_DATE_REASON_CODES
    fund_code: str = ""
    fund_name: str = ""
    base_date: str | None = None

    @field_validator("fund_code", "fund_name", mode="before")
    @classmethod
    def _strip_text(cls, value: object) -> str:
        """기준일 stage의 식별자 텍스트를 공통 규칙으로 정리한다."""
        if value is None:
            return ""
        return str(value).strip()


class FundBaseDateResult(StageResultModel):
    """Stage 2 `base_date` 응답 전체 계약이다."""
    items: list[FundBaseDateItem] = Field(default_factory=list)


class FundSlotItem(StageItemModel):
    """Stage 3이 확정한 거래 slot 후보 1건이다."""
    allowed_reason_codes: ClassVar[frozenset[str]] = T_DAY_REASON_CODES
    fund_code: str = ""
    fund_name: str = ""
    base_date: str | None = None
    t_day: int | None = None
    slot_id: str = ""
    evidence_label: str | None = None

    @field_validator("fund_code", "fund_name", "slot_id", mode="before")
    @classmethod
    def _strip_required_text(cls, value: object) -> str:
        """slot stage의 필수 식별자 문자열을 빈 문자열/공백 없이 정리한다."""
        if value is None:
            return ""
        return str(value).strip()

    @field_validator("evidence_label", mode="before")
    @classmethod
    def _strip_optional_text(cls, value: object) -> str | None:
        """선택 필드는 비어 있으면 None으로 정규화한다."""
        if value is None:
            return None
        text = str(value).strip()
        return text or None


class FundSlotResult(StageResultModel):
    """Stage 3 `t_day` 응답 전체 계약이다."""
    items: list[FundSlotItem] = Field(default_factory=list)


class FundAmountItem(StageItemModel):
    """Stage 4가 slot별 금액을 붙인 결과 1건이다."""
    allowed_reason_codes: ClassVar[frozenset[str]] = TRANSFER_AMOUNT_REASON_CODES
    fund_code: str = ""
    fund_name: str = ""
    base_date: str | None = None
    t_day: int | None = None
    slot_id: str = ""
    evidence_label: str | None = None
    transfer_amount: str | None = None

    @field_validator("fund_code", "fund_name", "slot_id", mode="before")
    @classmethod
    def _strip_required_text(cls, value: object) -> str:
        """금액 stage의 필수 문자열 필드를 정리한다."""
        if value is None:
            return ""
        return str(value).strip()

    @field_validator("evidence_label", "transfer_amount", mode="before")
    @classmethod
    def _strip_optional_text(cls, value: object) -> str | None:
        """선택 필드는 비어 있으면 None으로 정규화한다."""
        if value is None:
            return None
        text = str(value).strip()
        return text or None


class FundAmountResult(StageResultModel):
    """Stage 4 `transfer_amount` 응답 전체 계약이다."""
    items: list[FundAmountItem] = Field(default_factory=list)


class FundSettleItem(StageItemModel):
    """Stage 5가 settle_class를 붙인 결과 1건이다."""
    allowed_reason_codes: ClassVar[frozenset[str]] = SETTLE_CLASS_REASON_CODES
    fund_code: str = ""
    fund_name: str = ""
    base_date: str | None = None
    t_day: int | None = None
    slot_id: str = ""
    evidence_label: str | None = None
    transfer_amount: str | None = None
    settle_class: str | None = None

    @field_validator("fund_code", "fund_name", "slot_id", mode="before")
    @classmethod
    def _strip_required_text(cls, value: object) -> str:
        """settle stage의 필수 문자열 필드를 정리한다."""
        if value is None:
            return ""
        return str(value).strip()

    @field_validator("evidence_label", "transfer_amount", "settle_class", mode="before")
    @classmethod
    def _strip_optional_text(cls, value: object) -> str | None:
        """선택 필드는 비어 있으면 None으로 정규화한다."""
        if value is None:
            return None
        text = str(value).strip()
        return text or None


class FundSettleResult(StageResultModel):
    """Stage 5 `settle_class` 응답 전체 계약이다."""
    items: list[FundSettleItem] = Field(default_factory=list)


class FundResolvedItem(StageItemModel):
    """Stage 6까지 완료된 주문 후보 1건이다."""
    allowed_reason_codes: ClassVar[frozenset[str]] = ORDER_TYPE_REASON_CODES
    fund_code: str = ""
    fund_name: str = ""
    base_date: str | None = None
    t_day: int | None = None
    slot_id: str = ""
    evidence_label: str | None = None
    transfer_amount: str | None = None
    settle_class: str | None = None
    order_type: str | None = None

    @field_validator("fund_code", "fund_name", "slot_id", mode="before")
    @classmethod
    def _strip_required_text(cls, value: object) -> str:
        """최종 resolved stage의 필수 문자열 필드를 정리한다."""
        if value is None:
            return ""
        return str(value).strip()

    @field_validator("evidence_label", "transfer_amount", "settle_class", "order_type", mode="before")
    @classmethod
    def _strip_optional_text(cls, value: object) -> str | None:
        """선택 필드는 비어 있으면 None으로 정규화한다."""
        if value is None:
            return None
        text = str(value).strip()
        return text or None


class FundResolvedResult(StageResultModel):
    """Stage 6 `order_type` 응답 전체 계약이다."""
    items: list[FundResolvedItem] = Field(default_factory=list)


@dataclass(slots=True)
class InvalidResponseArtifact:
    """JSON 파싱에 실패한 stage raw response 1건을 보관한다."""
    chunk_index: int
    raw_response: str
    stage_name: str


@dataclass(slots=True)
class LLMExtractionOutcome:
    """추출 결과와 invalid raw response artifact를 함께 담는 반환 객체다."""
    result: ExtractionResult
    invalid_response_artifacts: list[InvalidResponseArtifact] = field(default_factory=list)


@dataclass(slots=True)
class _ExtractMetricsState:
    """문서 1건 추출 동안 쌓는 내부 관측성 메타데이터."""
    started_at_monotonic: float
    stage_retry_invocations: dict[str, int] = field(default_factory=dict)
    transport_retry_attempts: dict[str, int] = field(default_factory=dict)
    llm_batches_started: dict[str, int] = field(default_factory=dict)
    lock: RLock = field(default_factory=RLock, repr=False)

    def increment_bucket(self, bucket_name: str, stage_name: str, increment: int = 1) -> None:
        """stage별 카운터 버킷을 thread-safe 하게 증가시킨다."""
        if increment <= 0:
            return
        with self.lock:
            bucket = getattr(self, bucket_name)
            bucket[stage_name] = bucket.get(stage_name, 0) + increment

    def snapshot(self) -> dict[str, Any]:
        """sidecar 저장용 직렬화 가능한 metrics 스냅샷을 만든다."""
        with self.lock:
            return {
                "total_elapsed_seconds": round(max(0.0, time.monotonic() - self.started_at_monotonic), 3),
                "stage_retry_invocations": dict(sorted(self.stage_retry_invocations.items())),
                "transport_retry_attempts": dict(sorted(self.transport_retry_attempts.items())),
                "llm_batches_started": dict(sorted(self.llm_batches_started.items())),
            }


class ExtractionOutcomeError(ValueError):
    """추출 실패와 함께 stage 산출물을 상위 계층으로 전달하는 예외.

    queue 기반 WAS에서는 "추출이 실패했다"는 사실만큼
    "어떤 원문 응답이 실패 원인이었는지"가 중요하다.
    그런데 `extract_from_task_payload()`가 `ValueError`만 던지면,
    service/handler는 실패 사실만 알고 invalid raw response artifact는 잃어버리게 된다.

    그래서 이 예외는 일반 `ValueError`처럼 사용할 수 있으면서도,
    실패 시점의 `LLMExtractionOutcome`을 함께 싣고 올라가도록 만든다.
    상위 계층은 이 예외를 받아 debug 파일을 저장한 뒤 다시 예외를 올리면 된다.
    """

    def __init__(self, message: str, outcome: LLMExtractionOutcome) -> None:
        """예외 메시지와 함께 실패 시점 산출물을 보관한다."""
        super().__init__(message)
        # 내부 저장소는 private 속성으로 두고, 외부 호출자는 `exc.outcome` 속성으로 읽게 한다.
        # 이렇게 해 두면 "이 속성은 public API"라는 의도를 문서와 코드에서 함께 드러낼 수 있다.
        self._outcome = outcome

    @property
    def outcome(self) -> LLMExtractionOutcome:
        """실패 시점의 전체 추출 산출물.

        Handler B가 `FundOrderExtractor`를 직접 사용할 때는 이 속성이 가장 중요하다.
        - `exc.outcome.result`로 현재까지 조립된 결과/issue를 볼 수 있고
        - `exc.outcome.invalid_response_artifacts`로 깨진 LLM 원문 응답도 저장할 수 있다.
        """
        return self._outcome

    @property
    def result(self) -> ExtractionResult:
        """실패 시점까지 만들어진 최종 결과 스냅샷에 바로 접근하는 shortcut."""
        return self._outcome.result

    @property
    def invalid_response_artifacts(self) -> list[InvalidResponseArtifact]:
        """실패 시점에 확보된 invalid raw response 목록에 바로 접근하는 shortcut."""
        return self._outcome.invalid_response_artifacts


def write_invalid_response_debug_files(
    *,
    debug_output_dir: Path,
    source_name: str,
    artifacts: list[InvalidResponseArtifact],
) -> list[Path]:
    """JSON 파싱에 실패한 LLM 원문 응답을 디버그 파일로 저장한다.

    이 함수는 `ExtractionService` 내부 전용 로직이 아니라,
    queue 기반 WAS의 handler B가 `FundOrderExtractor`를 직접 호출할 때도 그대로 재사용할 수 있게
    extractor 모듈 쪽에 둔다.

    반환값으로 저장된 파일 경로 목록을 돌려주는 이유:
    - 호출자가 로그/DB/모니터링에 그대로 기록하기 쉽고
    - 테스트에서도 "정말 파일이 만들어졌는지"를 곧바로 검증할 수 있기 때문이다.
    """
    if not artifacts:
        return []

    debug_output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    source_stem = Path(source_name).stem or "document"
    written_paths: list[Path] = []

    for artifact in artifacts:
        debug_path = (
            debug_output_dir
            / (
                f"{source_stem}_{timestamp}_{artifact.stage_name}_"
                f"chunk{artifact.chunk_index:02d}_llm_invalid_response.txt"
            )
        )
        debug_path.write_text(artifact.raw_response, encoding="utf-8")
        logger.warning("Saved invalid LLM raw response to debug file: %s", debug_path)
        written_paths.append(debug_path)

    return written_paths


def build_extract_llm_log_path(*, debug_output_dir: Path, source_name: str) -> Path:
    """문서 1건 추출에 대응하는 로컬 LLM 로그 파일 경로를 만든다.

    로컬은 WAS처럼 task temp 디렉터리가 없으므로, debug 출력 루트 아래에
    문서별 timestamped 로그 파일을 만든다. 실행을 반복해도 이전 로그를 덮지 않게
    타임스탬프를 파일명에 포함한다.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    source_stem = Path(source_name).stem or "document"
    return debug_output_dir / f"{source_stem}_{timestamp}_llm_pipeline.log"


def build_extract_llm_metrics_path(*, log_path: Path) -> Path:
    """문서 1건 추출 로그 옆에 저장할 metrics sidecar 경로를 만든다."""
    if log_path.name.endswith("_llm_pipeline.log"):
        stem = log_path.name.removesuffix("_llm_pipeline.log")
    else:
        stem = log_path.stem
    return log_path.with_name(f"{stem}_llm_metrics.json")


def _record_extract_metric(bucket_name: str, stage_name: str, increment: int = 1) -> None:
    """현재 활성 추출 metrics 상태에 stage별 카운터를 기록한다."""
    state = _ACTIVE_EXTRACT_METRICS.get()
    if not isinstance(state, _ExtractMetricsState):
        return
    state.increment_bucket(bucket_name, stage_name, increment)


def _record_stage_retry_invocation(stage_name: str) -> None:
    _record_extract_metric("stage_retry_invocations", stage_name)


def _record_transport_retry_attempt(stage_name: str) -> None:
    _record_extract_metric("transport_retry_attempts", stage_name)


def _record_llm_batch_started(stage_name: str) -> None:
    _record_extract_metric("llm_batches_started", stage_name)


def _write_extract_metrics_sidecar(
    *,
    log_path: Path | None,
    metrics_state: _ExtractMetricsState,
) -> None:
    """현재 추출 metrics 스냅샷을 로그 옆 JSON sidecar 로 남긴다."""
    if log_path is None:
        return
    metrics_path = build_extract_llm_metrics_path(log_path=log_path)
    try:
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        metrics_path.write_text(
            json.dumps(metrics_state.snapshot(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:  # pragma: no cover - observability helper only
        logger.warning("Failed to write extract metrics sidecar %s: %s", metrics_path, exc)


def _append_local_llm_prompt_log(
    log_path: Path | None,
    *,
    header: str,
    system_prompt: str,
    user_prompt: str,
) -> None:
    """로컬 debug 출력 디렉터리에 LLM 프롬프트 블록을 append 한다."""
    if log_path is None:
        return
    log_path.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().isoformat()
    with log_path.open("a", encoding="utf-8") as f:
        f.write(f"{ts} {header}\n")
        f.write(f"{ts} [System]\n")
        for line in system_prompt.splitlines():
            f.write(f"{ts} {line}\n")
        f.write(f"{ts} [User]\n")
        for line in user_prompt.splitlines():
            f.write(f"{ts} {line}\n")
        f.write(f"{ts} --- end LLM prompt ---\n")


def _append_local_llm_response_log(
    log_path: Path | None,
    *,
    header: str,
    response_text: str,
) -> None:
    """로컬 debug 출력 디렉터리에 LLM 응답 블록을 append 한다."""
    if log_path is None:
        return
    log_path.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().isoformat()
    with log_path.open("a", encoding="utf-8") as f:
        f.write(f"{ts} {header}\n")
        f.write(f"{ts} [Response]\n")
        for line in response_text.splitlines():
            f.write(f"{ts} {line}\n")
        f.write(f"{ts} --- end LLM response ---\n")


@dataclass(slots=True)
class _OrderCandidate:
    """최종 집계 전 intermediate 주문 후보와 evidence 메타데이터를 묶는다."""
    order: OrderExtraction
    evidence_label: str
    evidence_kind: str


@dataclass(frozen=True, slots=True)
class StageDefinition:
    """YAML에서 읽은 stage 1개 정의를 런타임 구조로 고정한 객체다."""
    number: int
    name: str
    goal: str
    instructions: str
    output_contract: str
    retry_instructions: str = ""


@dataclass(frozen=True, slots=True)
class PromptBundle:
    """현재 요청에 사용되는 system/user/stage prompt 묶음이다."""
    system_prompt: str
    user_prompt_template: str
    retry_user_prompt_template: str
    stages: dict[str, StageDefinition]

REQUIRED_USER_PROMPT_FIELDS = {
    "stage_number",
    "total_stage_count",
    "stage_goal",
    "stage_name",
    "stage_instructions",
    "output_contract",
    "input_items_json",
    "document_text",
}

OPTIONAL_USER_PROMPT_FIELDS = {
    "counterparty_guidance",
}

REQUIRED_RETRY_USER_PROMPT_FIELDS = {
    "stage_number",
    "total_stage_count",
    "stage_goal",
    "stage_name",
    "stage_instructions",
    "retry_instructions",
    "output_contract",
    "retry_target_issues_json",
    "previous_output_items_json",
    "retry_focus_items_json",
    "input_items_json",
    "document_text",
    "retry_attempt_number",
    "retry_max_attempts",
}

OPTIONAL_RETRY_USER_PROMPT_FIELDS = {
    "counterparty_guidance",
    "previous_reason_summary_text",
}

SENSITIVE_COUNTERPARTY_GUIDANCE_STAGES = frozenset({"base_date", "transfer_amount", "order_type"})


def _default_prompt_path() -> Path:
    """기본 prompt YAML 위치를 반환한다."""
    return Path(__file__).resolve().parent / "prompts" / "extraction_prompts.yaml"


def _default_counterparty_prompt_map_path() -> Path:
    """거래처 프롬프트 매핑 설정 파일 위치를 반환한다."""
    return _default_prompt_path().parent / "counterparty_prompt_map.yaml"


def _normalize_counterparty_token(text: str) -> str:
    """파일명/토큰 비교용 문자열을 NFC + 소문자로 정규화한다."""
    return unicodedata.normalize("NFC", text).casefold()


def _tokenize_counterparty_name(text: str) -> tuple[str, ...]:
    """파일명에서 거래처 식별에 쓸 의미 있는 토큰만 뽑는다.

    영문 약어(AIA, ABL)는 substring 포함 여부로 보면 unrelated 단어에도 쉽게 오인식된다.
    그래서 파일명을 정규화한 뒤 영문/숫자/한글 덩어리만 토큰으로 잘라 exact match에 쓴다.
    """
    normalized_text = _normalize_counterparty_token(text)
    return tuple(re.findall(r"[0-9a-z가-힣]+", normalized_text))


def _counterparty_token_matches(
    *,
    normalized_name: str,
    name_tokens: tuple[str, ...],
    match_token: str,
) -> bool:
    """거래처 매핑 토큰이 현재 파일명과 맞는지 보수적으로 판단한다.

    - 영문 약어/숫자 토큰은 exact token match만 허용한다.
    - 한글/복합 명칭은 파일명 안의 실제 phrase 포함 여부를 본다.

    이렇게 나누면 `ABL`이 `global_ablation.xlsx`에 잘못 붙는 문제를 피하면서,
    `삼성액티브자산운용_...` 같은 한글 문서명은 자연스럽게 인식할 수 있다.
    """
    normalized_match = _normalize_counterparty_token(match_token)
    if re.fullmatch(r"[0-9a-z]+", normalized_match):
        return normalized_match in name_tokens
    return normalized_match in normalized_name


def _load_counterparty_prompt_matchers(
    mapping_path: Path | None = None,
) -> tuple[tuple[str, tuple[str, ...], tuple[str, ...]], ...]:
    """거래처 프롬프트 매핑 설정을 읽어 `(prompt_name, path_tokens, content_tokens)`로 변환한다."""
    mapping_path = mapping_path or _default_counterparty_prompt_map_path()
    if not mapping_path.exists():
        logger.warning(
            "Counterparty prompt mapping file does not exist: %s; all documents will fall back to the general prompt",
            mapping_path,
        )
        return ()

    try:
        payload = yaml.safe_load(mapping_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        logger.warning(
            "Failed to read counterparty prompt mapping file: %s; "
            "all documents will fall back to the general prompt (%s)",
            mapping_path,
            exc,
        )
        return ()

    if not isinstance(payload, dict):
        logger.warning(
            "Counterparty prompt mapping file is not an object: %s; "
            "all documents will fall back to the general prompt",
            mapping_path,
        )
        return ()

    raw_mappings = payload.get("mappings")
    if not isinstance(raw_mappings, list):
        logger.warning(
            "Counterparty prompt mapping file has no 'mappings' list: %s; "
            "all documents will fall back to the general prompt",
            mapping_path,
        )
        return ()

    mappings: list[tuple[str, tuple[str, ...], tuple[str, ...]]] = []
    for index, item in enumerate(raw_mappings, start=1):
        if not isinstance(item, dict):
            logger.warning(
                "Counterparty prompt mapping #%s is not an object and will be ignored: %s",
                index,
                mapping_path,
            )
            continue

        prompt_name = str(item.get("prompt_name", "")).strip()
        raw_tokens = item.get("match_tokens")
        raw_content_tokens = item.get("content_match_tokens")
        if not prompt_name:
            logger.warning(
                "Counterparty prompt mapping #%s has an empty prompt_name and will be ignored: %s",
                index,
                mapping_path,
            )
            continue
        if raw_tokens is not None and not isinstance(raw_tokens, list):
            logger.warning(
                "Counterparty prompt mapping #%s has invalid match_tokens and will be ignored: %s",
                index,
                mapping_path,
            )
            continue
        if raw_content_tokens is not None and not isinstance(raw_content_tokens, list):
            logger.warning(
                "Counterparty prompt mapping #%s has invalid content_match_tokens and will be ignored: %s",
                index,
                mapping_path,
            )
            continue

        match_tokens = tuple(str(token).strip() for token in (raw_tokens or []) if str(token).strip())
        content_match_tokens = tuple(
            str(token).strip() for token in (raw_content_tokens or []) if str(token).strip()
        )
        if not match_tokens and not content_match_tokens:
            logger.warning(
                "Counterparty prompt mapping #%s must define match_tokens and/or content_match_tokens and will be ignored: %s",
                index,
                mapping_path,
            )
            continue
        mappings.append((prompt_name, match_tokens, content_match_tokens))

    return tuple(mappings)


def _counterparty_content_matches(
    *,
    document_text: str,
    content_match_tokens: tuple[str, ...],
) -> bool:
    """문서 본문에 content 토큰이 모두 존재할 때만 content match를 허용한다."""
    if not content_match_tokens:
        return False
    normalized_text = _normalize_counterparty_token(document_text)
    return all(_normalize_counterparty_token(token) in normalized_text for token in content_match_tokens)


def resolve_counterparty_prompt_name(
    source_path: str | Path,
    *,
    mapping_path: Path | None = None,
    document_text: str | None = None,
) -> str | None:
    """문서 경로에서 거래처 특징 프롬프트 파일명을 결정한다.

    로컬 프로젝트에는 DB가 없으므로 YAML 설정 파일로 매핑을 관리한다.
    WAS에서는 이 계약을 유지한 채 내부 구현만 DB 조회로 바꾸면 된다.
    """
    source_name = Path(source_path).stem
    normalized_name = _normalize_counterparty_token(source_name)
    name_tokens = _tokenize_counterparty_name(source_name)
    matchers = _load_counterparty_prompt_matchers(mapping_path)
    if document_text:
        for prompt_name, _match_tokens, content_match_tokens in matchers:
            if _counterparty_content_matches(
                document_text=document_text,
                content_match_tokens=content_match_tokens,
            ):
                return prompt_name
    for prompt_name, match_tokens, _content_match_tokens in matchers:
        if match_tokens and any(
            _counterparty_token_matches(
                normalized_name=normalized_name,
                name_tokens=name_tokens,
                match_token=token,
            )
            for token in match_tokens
        ):
            return prompt_name
    return None


def load_counterparty_guidance(
    source_path: str | Path,
    *,
    use_counterparty_prompt: bool,
    mapping_path: Path | None = None,
    document_text: str | None = None,
) -> str | None:
    """문서 경로에 대응하는 거래처 특징 프롬프트를 선택적으로 로드한다.

    `use_counterparty_prompt=False`면 현재와 완전히 같은 일반 추출 경로를 유지한다.
    `True`일 때는 설정 파일 기반 매핑으로 거래처 프롬프트 파일을 찾아 보되,
    매핑이 없거나 파일이 비어 있으면 "특징이 없는 거래처"로 간주하고
    일반 추출 경로로 자연스럽게 fallback 한다.
    """
    if not use_counterparty_prompt:
        return None

    try:
        prompt_name = resolve_counterparty_prompt_name(
            source_path,
            mapping_path=mapping_path,
            document_text=document_text,
        )
    except Exception as exc:
        logger.warning(
            "Failed to load counterparty prompt mapping for source=%s; "
            "falling back to the general extraction prompt: %s",
            Path(source_path).name,
            exc,
        )
        return None

    if prompt_name is None:
        logger.info(
            "Counterparty prompt was requested, but no mapping was found for source=%s; "
            "falling back to the general extraction prompt",
            Path(source_path).name,
        )
        return None

    prompt_path = _default_prompt_path().parent / f"{prompt_name}.txt"
    if not prompt_path.exists():
        logger.warning(
            "Counterparty prompt mapping matched %s, but the prompt file does not exist: %s; "
            "falling back to the general extraction prompt",
            prompt_name,
            prompt_path,
        )
        return None

    guidance = prompt_path.read_text(encoding="utf-8").strip()
    if not guidance:
        logger.warning(
            "Counterparty prompt file is empty for %s: %s; falling back to the general extraction prompt",
            prompt_name,
            prompt_path,
        )
        return None
    return guidance


def _load_prompt_bundle(prompt_path: Path | None = None) -> PromptBundle:
    """YAML에서 system/user template와 stage 정의를 읽어 PromptBundle로 만든다."""
    prompt_path = prompt_path or _default_prompt_path()
    payload = yaml.safe_load(prompt_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Prompt file is invalid: {prompt_path}")

    system_prompt = str(payload.get("system_prompt", "")).strip()
    user_prompt_template = str(payload.get("user_prompt_template", "")).strip()
    retry_user_prompt_template = str(payload.get("retry_user_prompt_template", "")).strip()
    stage_payloads = payload.get("stages")
    if not system_prompt or not user_prompt_template or not retry_user_prompt_template or not isinstance(stage_payloads, list):
        raise ValueError(f"Prompt file is missing required fields: {prompt_path}")

    _validate_prompt_template(
        template_name="user prompt",
        prompt_template=user_prompt_template,
        prompt_path=prompt_path,
        required_fields=REQUIRED_USER_PROMPT_FIELDS,
        optional_fields=OPTIONAL_USER_PROMPT_FIELDS,
    )
    _validate_prompt_template(
        template_name="retry prompt",
        prompt_template=retry_user_prompt_template,
        prompt_path=prompt_path,
        required_fields=REQUIRED_RETRY_USER_PROMPT_FIELDS,
        optional_fields=OPTIONAL_RETRY_USER_PROMPT_FIELDS,
    )

    stages: dict[str, StageDefinition] = {}
    for stage_payload in stage_payloads:
        if not isinstance(stage_payload, dict):
            raise ValueError(f"Prompt stage entry is invalid: {stage_payload!r}")
        stage = StageDefinition(
            number=int(stage_payload["number"]),
            name=str(stage_payload["name"]).strip(),
            goal=str(stage_payload["goal"]).strip(),
            instructions=str(stage_payload["instructions"]).strip(),
            output_contract=str(stage_payload["output_contract"]).strip(),
            retry_instructions=str(stage_payload.get("retry_instructions", "")).strip(),
        )
        if stage.name in stages:
            raise ValueError(f"Prompt file contains duplicate stage name: {stage.name}")
        stages[stage.name] = stage

    required_stage_names = {
        "instruction_document",
        "fund_inventory",
        "base_date",
        "t_day",
        "transfer_amount",
        "settle_class",
        "order_type",
    }
    missing_stage_names = required_stage_names.difference(stages)
    if missing_stage_names:
        raise ValueError(f"Prompt file is missing stages: {sorted(missing_stage_names)}")

    return PromptBundle(
        system_prompt=system_prompt,
        user_prompt_template=user_prompt_template,
        retry_user_prompt_template=retry_user_prompt_template,
        stages=stages,
    )


def _validate_prompt_template(
    *,
    template_name: str,
    prompt_template: str,
    prompt_path: Path,
    required_fields: set[str],
    optional_fields: set[str],
) -> None:
    """prompt template placeholder 오타를 로드 시점에 즉시 검증한다."""
    formatter = Formatter()
    field_names = {
        field_name
        for _, field_name, _, _ in formatter.parse(prompt_template)
        if field_name is not None and field_name != ""
    }
    allowed_fields = required_fields.union(optional_fields)
    unknown_fields = sorted(field_names.difference(allowed_fields))
    if unknown_fields:
        raise ValueError(
            f"Prompt file has unknown {template_name} placeholders {unknown_fields}: {prompt_path}"
        )
    missing_fields = sorted(required_fields.difference(field_names))
    if missing_fields:
        raise ValueError(
            f"Prompt file is missing {template_name} placeholders {missing_fields}: {prompt_path}"
        )


def _langchain_message_content_to_text(content: Any) -> str:
    """LangChain message content를 로그용 평문으로 변환한다."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
                continue
            if isinstance(item, dict):
                text = item.get("text")
                if text:
                    parts.append(str(text))
        return "\n".join(part for part in parts if part)
    return str(content)


def _split_system_user_from_messages(messages: list[Any]) -> tuple[str, str]:
    """LLM 호출 메시지 묶음에서 system/user 프롬프트를 분리한다."""
    system_parts: list[str] = []
    user_parts: list[str] = []
    for msg in messages:
        text = _langchain_message_content_to_text(getattr(msg, "content", ""))
        if isinstance(msg, SystemMessage):
            system_parts.append(text)
        elif isinstance(msg, HumanMessage):
            user_parts.append(text)
        else:
            user_parts.append(text)
    return "\n\n".join(part for part in system_parts if part), "\n\n".join(part for part in user_parts if part)


class FundOrderExtractor:
    """문서 판별 + staged LLM 추출 파이프라인의 핵심 실행기.

    왜 한 번에 전체 orders 를 뽑지 않고 여러 단계로 쪼개는가:
    - 먼저 "이 문서가 실제 지시서인지"를 보수적으로 확인해 cover email 같은 문서를 분기할 수 있다.
    - 펀드 inventory 를 먼저 고정해야 이후 단계가 누락/오추출을 줄일 수 있다.
    - base_date, t_day, amount, settle_class, order_type 는 서로 의존하지만
      한 번에 모두 요구하면 모델이 표의 여러 컬럼을 섞어 버리기 쉽다.
    - 따라서 "좁은 JSON 계약"을 단계별로 검증하면서 누적하는 구조가 더 안정적이다.
    """

    def __init__(self, settings: Settings) -> None:
        """LLM 클라이언트와 prompt reload 상태를 포함한 추출기 인스턴스를 만든다."""
        self.settings = settings
        # prompt 는 운영 중 YAML 수정으로 대응하는 구조이므로, 파일 경로와 mtime 을
        # 인스턴스 상태로 보관한다.
        self.prompt_path = _default_prompt_path()
        self._prompt_mtime_ns = -1
        # 장기 실행 프로세스에서 동시 요청이 들어와도 prompt 교체 상태가 꼬이지 않게 락을 둔다.
        self._prompt_lock = RLock()
        # __init__ 직후에는 비어 있는 bundle 로 시작하고, 마지막에 정상 YAML 을 강제로 로드한다.
        self.prompt_bundle = PromptBundle(system_prompt="", user_prompt_template="", retry_user_prompt_template="", stages={})
        self.llm_temperature = float(getattr(settings, "llm_temperature", VA_LLM_TEMPERATURE))
        self.llm_max_tokens = int(getattr(settings, "llm_max_tokens", VA_LLM_MAX_TOKENS))
        self.llm_timeout_seconds = int(getattr(settings, "llm_timeout_seconds", VA_LLM_TIMEOUT_SECONDS))
        self.llm = ChatOpenAI(
            model=settings.llm_model,
            base_url=settings.llm_base_url,
            api_key=settings.llm_api_key,
            temperature=self.llm_temperature,
            max_tokens=self.llm_max_tokens,
            timeout=self.llm_timeout_seconds,
        )
        self.stage_batch_size = max(1, int(getattr(settings, "llm_stage_batch_size", VA_LLM_STAGE_BATCH_SIZE)))
        self.llm_parallel_workers = max(
            1,
            min(
                MAX_PARALLEL_LLM_BATCH_WORKERS,
                int(getattr(settings, "llm_parallel_workers", MAX_PARALLEL_LLM_BATCH_WORKERS)),
            ),
        )
        self.llm_retry_attempts = max(1, int(getattr(settings, "llm_retry_attempts", VA_LLM_RETRY_ATTEMPTS)))
        self.llm_retry_backoff_seconds = max(
            0.0,
            float(getattr(settings, "llm_retry_backoff_seconds", VA_LLM_RETRY_BACKOFF_SECONDS)),
        )
        self.llm_chunk_size_chars = max(1, int(getattr(settings, "llm_chunk_size_chars", VA_LLM_CHUNK_SIZE_CHARS)))
        # stage issue retry는 운영 설정이 아니라 단계별 코드 상수로 고정한다.
        self.llm_instruction_document_retry_attempts = INSTRUCTION_DOCUMENT_STAGE_ISSUE_RETRY_ATTEMPTS
        self.llm_stage_issue_retry_attempts = OTHER_STAGE_ISSUE_RETRY_ATTEMPTS
        self.system_prompt = ""
        self._refresh_prompt_bundle(force=True)

    @staticmethod
    def _document_has_sheet_loader_shape(
        *,
        raw_text: str | None,
        markdown_text: str | None,
    ) -> bool:
        """loader 산출물이 sheet/spreadsheet 기반 문서인지 본다."""
        combined = "\n".join(part for part in (markdown_text, raw_text) if part)
        if not combined:
            return False
        return "[SHEET " in combined or "## Sheet " in combined

    @staticmethod
    def _document_has_page_loader_shape(
        *,
        raw_text: str | None,
        markdown_text: str | None,
    ) -> bool:
        """loader 산출물이 page/PDF 기반 문서인지 본다."""
        combined = "\n".join(part for part in (markdown_text, raw_text) if part)
        if not combined:
            return False
        return "[PAGE " in combined or "## Page " in combined

    @staticmethod
    def _document_has_hanais_duplicate_pdf_hint(
        *,
        raw_text: str | None,
        markdown_text: str | None,
    ) -> bool:
        """흥국생명-hanais duplicate PDF 안내 문구를 deterministic 하게 감지한다."""
        normalized_text = " ".join(
            unicodedata.normalize("NFC", part).lower()
            for part in (markdown_text, raw_text)
            if part
        )
        if not normalized_text:
            return False
        compact_text = re.sub(r"\s+", " ", normalized_text)
        return any(token in compact_text for token in HANAIS_DUPLICATE_PDF_HINT_TOKENS)

    def _detect_pre_llm_non_instruction_reason(
        self,
        task_payload: DocumentLoadTaskPayload,
    ) -> str | None:
        """LLM stage 1 이전에 deterministic duplicate-copy guard를 먼저 적용한다."""
        if task_payload.non_instruction_reason:
            return task_payload.non_instruction_reason

        try:
            prompt_name = resolve_counterparty_prompt_name(
                task_payload.source_path,
                document_text=task_payload.markdown_text or task_payload.raw_text,
            )
        except Exception as exc:
            logger.warning(
                "Failed to resolve counterparty prompt during pre-LLM duplicate-copy guard for source=%s; "
                "skipping deterministic duplicate-copy precheck: %s",
                task_payload.file_name,
                exc,
            )
            return None
        if prompt_name is None:
            return None

        if prompt_name == "카디프" and self._document_has_sheet_loader_shape(
            raw_text=task_payload.raw_text,
            markdown_text=task_payload.markdown_text,
        ):
            return COUNTERPARTY_DUPLICATE_COPY_REASONS[prompt_name]
        if prompt_name == "하나생명" and self._document_has_page_loader_shape(
            raw_text=task_payload.raw_text,
            markdown_text=task_payload.markdown_text,
        ):
            return COUNTERPARTY_DUPLICATE_COPY_REASONS[prompt_name]
        if prompt_name == "흥국생명-hanais" and (
            self._document_has_page_loader_shape(
                raw_text=task_payload.raw_text,
                markdown_text=task_payload.markdown_text,
            )
            or self._document_has_hanais_duplicate_pdf_hint(
                raw_text=task_payload.raw_text,
                markdown_text=task_payload.markdown_text,
            )
        ):
            return COUNTERPARTY_DUPLICATE_COPY_REASONS[prompt_name]
        return None

    def extract(
        self,
        chunks: list[str],
        raw_text: str | None = None,
        markdown_text: str | None = None,
        target_fund_scope: TargetFundScope | None = None,
        counterparty_guidance: str | None = None,
        expected_order_count: int | None = None,
        markdown_loss_detected: bool = False,
    ) -> LLMExtractionOutcome:
        """문서 1건에 대한 end-to-end staged extraction 을 수행한다.

        여기서 가장 중요한 설계 포인트는 "요청 시작 시점의 prompt snapshot 고정"이다.
        long-running WAS 환경에서는 요청 도중 YAML 이 바뀔 수 있는데,
        stage들이 서로 다른 prompt 버전을 섞어 쓰면 결과 재현성이 무너진다.
        그래서 시작 시 bundle 을 하나 확정하고, 이번 요청 전체가 그 snapshot 만 사용한다.
        """
        prompt_bundle = self._refresh_prompt_bundle()
        if not chunks:
            return LLMExtractionOutcome(result=ExtractionResult(issues=["DOCUMENT_EMPTY"]))

        logger.info("Starting staged LLM extraction for %s chunk(s)", len(chunks))
        full_text = self._compose_document_context(
            primary_text="\n\n==== SECTION ====\n\n".join(chunk for chunk in chunks if chunk.strip()).strip(),
            raw_text=raw_text,
        )
        structured_markdown_text = None if markdown_loss_detected else (markdown_text or self._structured_markdown_view(full_text))
        issues: list[str] = []
        artifacts: list[InvalidResponseArtifact] = []

        is_instruction_document, non_instruction_reason = self._classify_instruction_document(
            prompt_bundle=prompt_bundle,
            document_text=full_text,
            artifacts=artifacts,
            counterparty_guidance=counterparty_guidance,
        )
        if is_instruction_document is False:
            issue = "DOCUMENT_NOT_INSTRUCTION"
            if non_instruction_reason:
                issue = f"{issue}: {non_instruction_reason}"
            self._append_unique(issues, issue)
            logger.info("Document was classified as non-instruction by LLM: %s", non_instruction_reason or "no reason")
            return LLMExtractionOutcome(
                result=ExtractionResult(issues=self._unique_preserve_order(issues)),
                invalid_response_artifacts=artifacts,
            )

        # fund inventory stage는 chunk 단위로 수행한다. 문서 전체를 다 본 결과가 필요해서
        # chunk별 seed 를 모은 뒤 dedupe 한다. 그 다음에는 target_fund_scope 필터와
        # structured markdown 기반 deterministic seed augmentation 을 연달아 적용해,
        # 한쪽 금액만 0인 라이나 row 같은 seed 누락을 줄인다.
        seeds = self._extract_fund_seeds(
            prompt_bundle,
            chunks,
            raw_text,
            issues,
            artifacts,
            target_fund_scope=target_fund_scope,
            counterparty_guidance=counterparty_guidance,
        )
        seeds = self._filter_fund_seeds_by_scope(seeds, target_fund_scope)
        if not markdown_loss_detected:
            seeds = self._augment_fund_seeds_from_document(
                document_text=full_text,
                seeds=seeds,
                target_fund_scope=target_fund_scope,
            )
        if not seeds:
            direct_result = self._recover_direct_orders_from_document(
                document_text=full_text,
                issues=issues,
                target_fund_scope=target_fund_scope,
            )
            if direct_result is not None:
                return LLMExtractionOutcome(result=direct_result, invalid_response_artifacts=artifacts)
            self._append_unique(issues, "FUND_DISCOVERY_EMPTY")
            return LLMExtractionOutcome(result=ExtractionResult(issues=self._unique_preserve_order(issues)), invalid_response_artifacts=artifacts)
        logger.info("Stage %s completed: discovered %s unique fund(s)", self._stage(prompt_bundle, "fund_inventory").number, len(seeds))

        base_date_stage = self._stage(prompt_bundle, "base_date")
        base_dates = self._resolve_document_base_date_for_seeds(
            prompt_bundle=prompt_bundle,
            base_date_stage=base_date_stage,
            seeds=seeds,
            document_text=full_text,
            raw_text=raw_text,
            artifacts=artifacts,
            counterparty_guidance=counterparty_guidance,
        )
        if base_dates is None:
            logger.info("Falling back to legacy per-fund base_date batches")
            # base_date~order_type stage는 "문서 전체 + 이전 단계 결과 batch" 형태로 수행한다.
            # 문서 전체 문맥은 유지하되, input_items 수를 제한해서 token 폭주와 응답 누락을 막는다.
            base_dates = self._run_batched_stage(
                prompt_bundle=prompt_bundle,
                stage=base_date_stage,
                document_text=full_text,
                input_items=[item.model_dump(mode="json") for item in seeds],
                response_model=FundBaseDateResult,
                issues=issues,
                artifacts=artifacts,
                counterparty_guidance=counterparty_guidance,
            )
        if not base_dates:
            direct_result = self._recover_direct_orders_from_document(
                document_text=full_text,
                issues=issues,
                target_fund_scope=target_fund_scope,
            )
            if direct_result is not None:
                return LLMExtractionOutcome(result=direct_result, invalid_response_artifacts=artifacts)
            return LLMExtractionOutcome(result=ExtractionResult(issues=self._unique_preserve_order(issues)), invalid_response_artifacts=artifacts)
        logger.info("Stage %s completed: resolved %s base-date item(s)", base_date_stage.number, len(base_dates))

        structure_shortcut_orders = self._build_structure_shortcut_orders_after_base_date(
            base_dates=base_dates,
            raw_text=raw_text or full_text,
            markdown_text=structured_markdown_text,
            target_fund_scope=target_fund_scope,
            expected_order_count=expected_order_count,
        )
        if structure_shortcut_orders:
            result = ExtractionResult(
                orders=self._filter_orders_by_scope(structure_shortcut_orders, target_fund_scope),
                issues=self._unique_preserve_order(list(issues)),
            )
            self._reconcile_final_issues(
                [],
                result,
                markdown_text=structured_markdown_text,
                document_text=full_text,
                raw_text=raw_text,
                target_fund_scope=target_fund_scope,
                expected_order_count=expected_order_count,
            )
            logger.info(
                "Recovered extraction from structure shortcut after base_date: orders=%s issues=%s",
                len(result.orders),
                result.issues,
            )
            return LLMExtractionOutcome(result=result, invalid_response_artifacts=artifacts)

        markdown_shortcut_orders: list[OrderExtraction] = []
        if structured_markdown_text:
            markdown_shortcut_orders = self._build_markdown_shortcut_orders_after_base_date(
                base_dates=base_dates,
                markdown_text=structured_markdown_text,
                raw_text=raw_text or full_text,
                target_fund_scope=target_fund_scope,
                expected_order_count=expected_order_count,
            )
        if markdown_shortcut_orders:
            shortcut_issues = self._remove_issues_by_code(list(issues), "DUPLICATE_FUND_CODE_IN_OUTPUT")
            shortcut_issues = self._remove_issues_by_code(shortcut_issues, "AMBIGUOUS_PENDING_DATE_IN_BIGO")
            result = ExtractionResult(
                orders=self._filter_orders_by_scope(markdown_shortcut_orders, target_fund_scope),
                issues=self._unique_preserve_order(shortcut_issues),
            )
            self._reconcile_final_issues(
                [],
                result,
                markdown_text=structured_markdown_text,
                document_text=full_text,
                raw_text=raw_text,
                target_fund_scope=target_fund_scope,
                expected_order_count=expected_order_count,
            )
            logger.info(
                "Recovered extraction from markdown shortcut after base_date: orders=%s issues=%s",
                len(result.orders),
                result.issues,
            )
            return LLMExtractionOutcome(result=result, invalid_response_artifacts=artifacts)

        t_day_stage = self._stage(prompt_bundle, "t_day")
        slots = self._run_batched_stage(
            prompt_bundle=prompt_bundle,
            stage=t_day_stage,
            document_text=full_text,
            input_items=[item.model_dump(mode="json") for item in base_dates],
            response_model=FundSlotResult,
            issues=issues,
            artifacts=artifacts,
            counterparty_guidance=counterparty_guidance,
        )
        slots = self._augment_t_day_items_from_document(
            document_text=full_text,
            input_items=[item.model_dump(mode="json") for item in base_dates],
            output_items=slots,
        )
        slots = self._replace_incomplete_t_day_items_with_deterministic_document_slots(
            document_text=full_text,
            input_items=[item.model_dump(mode="json") for item in base_dates],
            output_items=slots,
        )
        slots, forced_t_day_issues = self._reconcile_t_day_stage_output(
            document_text=full_text,
            input_items=[item.model_dump(mode="json") for item in base_dates],
            output_items=slots,
        )
        for issue in forced_t_day_issues:
            self._append_unique(issues, issue)
        if slots and self._t_day_document_slots_are_complete(
            document_text=full_text,
            input_items=[item.model_dump(mode="json") for item in base_dates],
            output_items=slots,
        ):
            issues = self._remove_issues_by_code(issues, "T_DAY_MISSING")
            issues = self._remove_issues_by_code(issues, "T_DAY_STAGE_PARTIAL")
        if not slots:
            direct_result = self._recover_direct_orders_from_document(
                document_text=full_text,
                issues=issues,
                target_fund_scope=target_fund_scope,
            )
            if direct_result is not None:
                return LLMExtractionOutcome(result=direct_result, invalid_response_artifacts=artifacts)
            self._append_unique(issues, "TRANSACTION_SLOT_EMPTY")
            return LLMExtractionOutcome(result=ExtractionResult(issues=self._unique_preserve_order(issues)), invalid_response_artifacts=artifacts)
        logger.info("Stage %s completed: resolved %s slot(s)", t_day_stage.number, len(slots))

        transfer_amount_stage = self._stage(prompt_bundle, "transfer_amount")
        amounts = self._run_batched_stage(
            prompt_bundle=prompt_bundle,
            stage=transfer_amount_stage,
            document_text=full_text,
            input_items=[item.model_dump(mode="json") for item in slots],
            response_model=FundAmountResult,
            issues=issues,
            artifacts=artifacts,
            counterparty_guidance=counterparty_guidance,
        )
        amounts = self._post_validate_transfer_amount_items(amounts)
        if not amounts:
            direct_result = self._recover_direct_orders_from_document(
                document_text=full_text,
                issues=issues,
                target_fund_scope=target_fund_scope,
            )
            if direct_result is not None:
                return LLMExtractionOutcome(result=direct_result, invalid_response_artifacts=artifacts)
            self._append_unique(issues, "TRANSFER_AMOUNT_EMPTY")
            return LLMExtractionOutcome(result=ExtractionResult(issues=self._unique_preserve_order(issues)), invalid_response_artifacts=artifacts)
        amounts = self._drop_zero_amount_stage_items(amounts, stage_name="settle_class")
        if not amounts:
            direct_result = self._recover_direct_orders_from_document(
                document_text=full_text,
                issues=issues,
                target_fund_scope=target_fund_scope,
            )
            if direct_result is not None:
                return LLMExtractionOutcome(result=direct_result, invalid_response_artifacts=artifacts)
            self._append_unique(issues, "TRANSFER_AMOUNT_EMPTY")
            return LLMExtractionOutcome(result=ExtractionResult(issues=self._unique_preserve_order(issues)), invalid_response_artifacts=artifacts)
        logger.info("Stage %s completed: resolved %s amount item(s)", transfer_amount_stage.number, len(amounts))

        settle_class_stage = self._stage(prompt_bundle, "settle_class")
        deterministic_settle_items = self._build_deterministic_settle_items_from_amount_items(amounts)
        if deterministic_settle_items is not None:
            settle_items = deterministic_settle_items
            logger.info(
                "Recovered settle_class for %s item(s) deterministically from amount items; skipped Stage %s LLM batches",
                len(settle_items),
                settle_class_stage.number,
            )
        else:
            settle_items = self._run_batched_stage(
                prompt_bundle=prompt_bundle,
                stage=settle_class_stage,
                document_text=full_text,
                input_items=[item.model_dump(mode="json") for item in amounts],
                response_model=FundSettleResult,
                issues=issues,
                artifacts=artifacts,
                counterparty_guidance=counterparty_guidance,
            )
        if not settle_items:
            direct_result = self._recover_direct_orders_from_document(
                document_text=full_text,
                issues=issues,
                target_fund_scope=target_fund_scope,
            )
            if direct_result is not None:
                return LLMExtractionOutcome(result=direct_result, invalid_response_artifacts=artifacts)
            self._append_unique(issues, "SETTLE_CLASS_EMPTY")
            return LLMExtractionOutcome(result=ExtractionResult(issues=self._unique_preserve_order(issues)), invalid_response_artifacts=artifacts)
        logger.info("Stage %s completed: resolved %s settle item(s)", settle_class_stage.number, len(settle_items))

        order_type_stage = self._stage(prompt_bundle, "order_type")
        deterministic_resolved_items = self._build_deterministic_resolved_items_from_settle_items(
            settle_items,
            document_text=full_text,
            raw_text=raw_text,
            target_fund_scope=target_fund_scope,
        )
        if deterministic_resolved_items is not None:
            resolved_items = deterministic_resolved_items
            logger.info(
                "Recovered order_type for %s item(s) deterministically from settle items; skipped Stage %s LLM batches",
                len(resolved_items),
                order_type_stage.number,
            )
        else:
            resolved_items = self._run_batched_stage(
                prompt_bundle=prompt_bundle,
                stage=order_type_stage,
                document_text=full_text,
                input_items=[item.model_dump(mode="json") for item in settle_items],
                response_model=FundResolvedResult,
                issues=issues,
                artifacts=artifacts,
                counterparty_guidance=counterparty_guidance,
            )
            resolved_items = self._replace_incomplete_order_type_items_with_deterministic_values(
                input_items=settle_items,
                output_items=resolved_items,
            )
        logger.info("Stage %s completed: resolved %s final item(s)", order_type_stage.number, len(resolved_items))

        # 여기부터는 LLM 결과를 바로 믿지 않고, 결정론적 후처리로 최종 domain order 를 만든다.
        result = self._build_result(resolved_items, issues, document_text=full_text)
        result.orders = self._filter_orders_by_scope(result.orders, target_fund_scope)
        self._reconcile_final_issues(
            resolved_items,
            result,
            markdown_text=structured_markdown_text,
            document_text=full_text,
            raw_text=raw_text,
            target_fund_scope=target_fund_scope,
            expected_order_count=expected_order_count,
        )
        logger.info("Staged LLM extraction finished: orders=%s issues=%s", len(result.orders), result.issues)
        return LLMExtractionOutcome(result=result, invalid_response_artifacts=artifacts)

    def _recover_direct_orders_from_document(
        self,
        *,
        document_text: str,
        issues: list[str],
        target_fund_scope: TargetFundScope | None,
    ) -> ExtractionResult | None:
        """Stage 조기 종료 전에 direct parser로 최종 주문을 바로 복구할 수 있으면 채택한다.

        Stage 1~5가 비어서 staged chain이 일찍 끝나더라도, `BUY & SELL REPORT`처럼
        문서 자체에서 최종 주문을 직접 읽을 수 있는 형식이면 false-fail로 끝낼 이유가 없다.
        이 helper는 그런 형식을 위한 마지막 안전망이다.
        """
        direct_result = self._build_result([], list(issues), document_text=document_text)
        if not direct_result.orders:
            return None
        direct_result.orders = self._filter_orders_by_scope(direct_result.orders, target_fund_scope)
        if not direct_result.orders:
            return None
        logger.info(
            "Recovered extraction from direct document parser before staged completion: orders=%s issues=%s",
            len(direct_result.orders),
            direct_result.issues,
        )
        return direct_result

    def extract_from_task_payload(
        self,
        task_payload: DocumentLoadTaskPayload,
        *,
        counterparty_guidance: str | None = None,
        extract_log_path: Path | None = None,
    ) -> LLMExtractionOutcome:
        """Handler A가 만든 파일/queue 산출물을 받아 최종 추출까지 수행한다.

        이 메서드는 queue 기반 WAS를 위한 진입점이다.
        Handler B는 원문 파일을 다시 열 필요 없이, 저장된 `DocumentLoadTaskPayload`만 읽어서
        이 메서드에 넘기면 된다.

        책임:
        - 비지시서 차단
        - 정상 0건 문서 short-circuit
        - 실제 LLM 추출 수행
        - 필요 시 direct/deterministic fallback으로 최종 주문 보강
        - 독립 corroboration이 있으면 stale missing issue와 coverage false-fail 완화
        - 최종 결과와 모순되는 stale blocking issue 정리
        - 실제로 저장을 막아야 하는 blocking issue만 검사
        """
        metrics_state = _ExtractMetricsState(started_at_monotonic=time.monotonic())
        log_token = _ACTIVE_EXTRACT_LOG_PATH.set(extract_log_path)
        metrics_token = _ACTIVE_EXTRACT_METRICS.set(metrics_state)
        try:
            pre_llm_non_instruction_reason = self._detect_pre_llm_non_instruction_reason(task_payload)
            if pre_llm_non_instruction_reason:
                raise ValueError(
                    "Document is not a variable-annuity order instruction. "
                    f"path={task_payload.source_path} reason={pre_llm_non_instruction_reason}"
                )

            if task_payload.allow_empty_result or task_payload.scope_excludes_all_funds:
                return LLMExtractionOutcome(result=ExtractionResult(orders=[], issues=[]))

            extract_kwargs: dict[str, Any] = {
                "chunks": list(task_payload.chunks),
                "raw_text": task_payload.raw_text,
                "markdown_text": task_payload.markdown_text,
                "target_fund_scope": task_payload.target_fund_scope,
                "counterparty_guidance": counterparty_guidance,
                "expected_order_count": task_payload.expected_order_count,
            }
            if task_payload.markdown_loss_detected:
                extract_kwargs["markdown_loss_detected"] = True
            extraction_outcome = self.extract(
                **extract_kwargs,
            )
            extraction_result = extraction_outcome.result

            if self._issues_include_code(extraction_result.issues, "DOCUMENT_NOT_INSTRUCTION"):
                non_instruction_issue = next(
                    (
                        issue
                        for issue in extraction_result.issues
                        if self._issue_has_code(issue, "DOCUMENT_NOT_INSTRUCTION")
                    ),
                    "DOCUMENT_NOT_INSTRUCTION",
                )
                raise ValueError(
                    "Document is not a variable-annuity order instruction. "
                    f"path={task_payload.source_path} reason={non_instruction_issue}"
                )

            recovered_result = self._recover_result_from_structured_markdown(
                task_payload=task_payload,
                extraction_result=extraction_result,
            )
            if recovered_result is not None:
                extraction_result = recovered_result
                extraction_outcome = LLMExtractionOutcome(
                    result=recovered_result,
                    invalid_response_artifacts=extraction_outcome.invalid_response_artifacts,
                )

            has_coverage_mismatch = (
                task_payload.expected_order_count > 0
                and len(extraction_result.orders) != task_payload.expected_order_count
            )
            if has_coverage_mismatch:
                if self._can_downgrade_coverage_mismatch(
                    task_payload=task_payload,
                    extraction_result=extraction_result,
                ):
                    extraction_result.issues = self._remove_issues_by_code(
                        extraction_result.issues,
                        "ORDER_COVERAGE_MISMATCH",
                    )
                    extraction_result.issues = self._remove_issues_by_code(
                        extraction_result.issues,
                        SOFT_COVERAGE_WARNING,
                    )
                    logger.warning(
                        "Suppressed stale coverage mismatch after deterministic corroboration: expected=%s actual=%s",
                        task_payload.expected_order_count,
                        len(extraction_result.orders),
                    )
                else:
                    self._append_unique(extraction_result.issues, "ORDER_COVERAGE_MISMATCH")
                    logger.warning(
                        "Order coverage mismatch detected from task payload: expected=%s actual=%s",
                        task_payload.expected_order_count,
                        len(extraction_result.orders),
                    )

            if self._issues_include_code(extraction_result.issues, "TRANSFER_AMOUNT_CONFLICT"):
                blocking_without_amount_conflict = [
                    issue
                    for issue in extraction_result.issues
                    if any(
                        self._issue_has_code(issue, code)
                        for code in BLOCKING_EXTRACTION_ISSUES
                        if code != "TRANSFER_AMOUNT_CONFLICT"
                    )
                ]
                if (
                    not blocking_without_amount_conflict
                    and self._can_independently_corroborate_final_orders(
                        task_payload=task_payload,
                        extraction_result=extraction_result,
                    )
                ):
                    extraction_result.issues = self._remove_issues_by_code(
                        extraction_result.issues,
                        "TRANSFER_AMOUNT_CONFLICT",
                    )
                    logger.warning(
                        "Removed stale transfer amount conflict after independent corroboration: orders=%s",
                        len(extraction_result.orders),
                    )

            blocking_issues = [
                issue
                for issue in extraction_result.issues
                if any(self._issue_has_code(issue, code) for code in BLOCKING_EXTRACTION_ISSUES)
            ]
            if not extraction_result.orders and not blocking_issues:
                raise ExtractionOutcomeError("Extraction produced no storable orders.", extraction_outcome)
            if blocking_issues:
                raise ExtractionOutcomeError(
                    "Extraction result is incomplete and was not stored."
                    f" blocking_issues={blocking_issues}",
                    extraction_outcome,
                )
            return extraction_outcome
        finally:
            _write_extract_metrics_sidecar(
                log_path=extract_log_path,
                metrics_state=metrics_state,
            )
            _ACTIVE_EXTRACT_METRICS.reset(metrics_token)
            _ACTIVE_EXTRACT_LOG_PATH.reset(log_token)

    def _can_downgrade_coverage_mismatch(
        self,
        *,
        task_payload: DocumentLoadTaskPayload,
        extraction_result: ExtractionResult,
    ) -> bool:
        """coverage mismatch가 loader 추정 오차로 보일 때는 soft warning으로 내린다.

        매우 보수적으로 동작한다.
        - coverage mismatch 외 다른 blocking issue가 남아 있으면 절대 완화하지 않는다.
        - structured markdown/raw text에서 독립적으로 읽은 deterministic orders가
          현재 최종 orders와 정확히 일치할 때만 loader 추정 과대로 본다.
        """
        if task_payload.expected_order_count <= 0 or not extraction_result.orders:
            return False

        blocking_without_coverage = [
            issue
            for issue in extraction_result.issues
            if any(
                self._issue_has_code(issue, code)
                for code in BLOCKING_EXTRACTION_ISSUES
                if code != "ORDER_COVERAGE_MISMATCH"
            )
        ]
        if blocking_without_coverage:
            return False
        return self._can_independently_corroborate_final_orders(
            task_payload=task_payload,
            extraction_result=extraction_result,
        )

    def _can_independently_corroborate_final_orders(
        self,
        *,
        task_payload: DocumentLoadTaskPayload,
        extraction_result: ExtractionResult,
    ) -> bool:
        """문서의 독립 경로가 현재 최종 주문을 다시 재현하는지 확인한다.

        이 helper는 정확한 final orders가 이미 만들어졌는지 보수적으로 확인하는 용도다.
        결과는 아래 두 군데에서만 사용한다.
        - `ORDER_COVERAGE_MISMATCH`를 soft warning으로 낮출지 판단
        - `TRANSFER_AMOUNT_MISSING`, `ORDER_TYPE_MISSING`, `TRANSFER_AMOUNT_CONFLICT`
          같은 stale blocking issue를 제거할지 판단
        """
        return self._can_independently_corroborate_final_orders_from_text(
            markdown_text=None if task_payload.markdown_loss_detected else task_payload.markdown_text,
            raw_text=task_payload.raw_text,
            target_fund_scope=task_payload.target_fund_scope,
            extraction_result=extraction_result,
        )

    def _can_independently_corroborate_final_orders_from_text(
        self,
        *,
        markdown_text: str | None,
        raw_text: str | None,
        target_fund_scope: TargetFundScope | None,
        extraction_result: ExtractionResult,
        ignore_schedule: bool = False,
    ) -> bool:
        """문서 텍스트만으로 현재 최종 주문을 독립 재현할 수 있는지 확인한다."""
        if not extraction_result.orders:
            return False
        normalized_markdown_text = markdown_text or ""
        normalized_raw_text = raw_text or ""

        corroborated_orders = self._build_deterministic_markdown_orders(
            markdown_text=normalized_markdown_text,
            raw_text=normalized_raw_text,
            target_fund_scope=target_fund_scope,
        )
        if not corroborated_orders:
            document_parts: list[str] = []
            if normalized_markdown_text.strip():
                document_parts.append(normalized_markdown_text.strip())
            if normalized_raw_text.strip() and normalized_raw_text.strip() not in normalized_markdown_text:
                document_parts.append(normalized_raw_text.strip())
            document_text = "\n\n".join(document_parts)
            direct_orders = self._build_buy_sell_report_orders(document_text)
            corroborated_orders = self._filter_orders_by_scope(direct_orders, target_fund_scope)
        if not corroborated_orders:
            return False

        corroborated_orders = self._normalize_corroboration_orders_for_comparison(
            corroborated_orders=corroborated_orders,
            reference_orders=extraction_result.orders,
            raw_text=normalized_raw_text,
        )
        return self._orders_have_same_signatures(
            corroborated_orders,
            extraction_result.orders,
            ignore_schedule=ignore_schedule,
        )

    def _build_independent_corroboration_orders(
        self,
        task_payload: DocumentLoadTaskPayload,
    ) -> list[OrderExtraction]:
        """LLM stage chain과 독립적인 문서 파서로 최종 주문을 재구성한다."""
        corroborated_orders = self._build_deterministic_markdown_orders(
            markdown_text="" if task_payload.markdown_loss_detected else task_payload.markdown_text,
            raw_text=task_payload.raw_text,
            target_fund_scope=task_payload.target_fund_scope,
        )
        if corroborated_orders:
            return corroborated_orders

        document_text = self._compose_document_context(
            primary_text="\n\n==== SECTION ====\n\n".join(chunk for chunk in task_payload.chunks if chunk.strip()).strip(),
            raw_text=task_payload.raw_text,
        )
        direct_orders = self._build_buy_sell_report_orders(document_text)
        if not direct_orders:
            return []
        return self._filter_orders_by_scope(direct_orders, task_payload.target_fund_scope)

    def _normalize_corroboration_orders_for_comparison(
        self,
        *,
        corroborated_orders: list[OrderExtraction],
        reference_orders: list[OrderExtraction],
        raw_text: str | None = None,
    ) -> list[OrderExtraction]:
        """독립 corroboration 주문을 coverage 비교용으로만 보수적으로 정규화한다.

        structured markdown 파서는 같은 표 section이 반복되면 exact duplicate order를
        중복 생성할 수 있고, `Closing Date` 같은 reference date를 base_date로 읽을 때도 있다.
        coverage 완화는 "실제 거래가 같은지"를 확인하는 용도이므로, 아래 두 경우만 좁게 보정한다.
        - exact duplicate corroboration order 제거
        - 모든 거래가 base_date만 다르게 같고, corroborated date가 문서상 reference artifact로
          보일 때만 corroboration base_date를 reference 쪽으로 맞춤
        """
        if not corroborated_orders:
            return []

        normalized_orders = self._dedupe_orders_by_signature(corroborated_orders)
        reference_base_dates = {
            self._normalize_date(order.base_date)
            for order in reference_orders
            if self._normalize_date(order.base_date) is not None
        }
        corroborated_base_dates = {
            self._normalize_date(order.base_date)
            for order in normalized_orders
            if self._normalize_date(order.base_date) is not None
        }
        if (
            len(reference_base_dates) == 1
            and len(corroborated_base_dates) == 1
            and reference_base_dates != corroborated_base_dates
            and self._can_normalize_corroboration_base_date_mismatch(
                raw_text=raw_text or "",
                corroborated_base_date=next(iter(corroborated_base_dates)),
                reference_base_date=next(iter(reference_base_dates)),
            )
            and self._orders_have_same_signatures(
                [
                    order.model_copy(update={"base_date": next(iter(reference_base_dates))})
                    for order in normalized_orders
                ],
                reference_orders,
                ignore_base_date=True,
            )
        ):
            target_base_date = next(iter(reference_base_dates))
            normalized_orders = [
                order.model_copy(update={"base_date": target_base_date})
                for order in normalized_orders
            ]
        return normalized_orders

    def _can_normalize_corroboration_base_date_mismatch(
        self,
        *,
        raw_text: str,
        corroborated_base_date: str,
        reference_base_date: str,
    ) -> bool:
        """coverage 비교에서 base_date mismatch를 좁게 보정해도 되는지 확인한다."""
        if not raw_text:
            return False
        if not self._date_looks_like_reference_only_in_text(raw_text, corroborated_base_date):
            return False
        return self._date_looks_like_actual_base_date_hint_in_text(raw_text, reference_base_date)

    def _dedupe_orders_by_signature(self, orders: list[OrderExtraction]) -> list[OrderExtraction]:
        """독립 corroboration 결과의 exact duplicate order만 제거한다."""
        seen: set[tuple[Any, ...]] = set()
        deduped: list[OrderExtraction] = []
        for order in orders:
            signature = self._order_signature(order)
            if signature in seen:
                continue
            seen.add(signature)
            deduped.append(order)
        return deduped

    def _drop_zero_amount_stage_items(
        self,
        items: list[FundAmountItem | FundSettleItem | FundResolvedItem],
        *,
        stage_name: str,
    ) -> list[FundAmountItem | FundSettleItem | FundResolvedItem]:
        """다음 stage로 넘기기 전에 0원 slot을 제거한다."""
        kept_items: list[FundAmountItem | FundSettleItem | FundResolvedItem] = []
        removed_count = 0

        for item in items:
            normalized_amount = self._normalize_amount(getattr(item, "transfer_amount", None))
            if normalized_amount is not None and self._is_effectively_zero_amount(normalized_amount):
                removed_count += 1
                continue
            kept_items.append(item)

        if removed_count:
            logger.info(
                "Pruned %s zero-amount item(s) before %s stage",
                removed_count,
                stage_name,
            )
        return kept_items

    def _recover_result_from_structured_markdown(
        self,
        *,
        task_payload: DocumentLoadTaskPayload,
        extraction_result: ExtractionResult,
    ) -> ExtractionResult | None:
        """LLM 결과가 coverage를 못 맞출 때 structured markdown 표로 직접 복구를 시도한다.

        동양생명처럼 표 구조가 매우 명확한 문서는 stage 3~6이 대부분의 slot을 찾더라도
        마지막 방향 판정에서 일부를 놓칠 수 있다. 이때 structured markdown의 collapsed
        header(`설정금액 / 3월19일`, `해지금액 / 3월20일`, `정산액`)를 직접 읽으면
        order_type/settle_class/t_day를 결정론적으로 복구할 수 있다.

        fallback은 다음 조건을 모두 만족할 때만 채택한다.
        - structured markdown에서 직접 읽은 주문 수가 expected coverage와 정확히 일치한다.
        - 기존 LLM 결과가 expected coverage를 못 맞췄거나,
          같은 거래 묶음을 더 정확한 내용으로 교정한다.
        """
        if task_payload.markdown_loss_detected:
            return None
        if task_payload.expected_order_count <= 0:
            return None

        recovered_orders = self._build_deterministic_markdown_orders(
            markdown_text=task_payload.markdown_text,
            raw_text=task_payload.raw_text,
            target_fund_scope=task_payload.target_fund_scope,
        )
        if len(recovered_orders) != task_payload.expected_order_count:
            return None
        recovered_has_compatible_families = self._orders_have_compatible_transaction_families(
            recovered_orders,
            extraction_result.orders,
            document_text=task_payload.raw_text,
        )
        recovered_matches_expected_better = (
            len(extraction_result.orders) != task_payload.expected_order_count
            and recovered_has_compatible_families
        )
        recovered_is_strictly_more_complete = (
            len(recovered_orders) > len(extraction_result.orders)
            and recovered_has_compatible_families
        )
        recovered_is_content_fix = (
            len(recovered_orders) == len(extraction_result.orders)
            and not self._orders_have_same_signatures(recovered_orders, extraction_result.orders)
            and self._orders_share_same_transaction_core(recovered_orders, extraction_result.orders)
        )
        recovered_is_document_backed_replacement = (
            len(recovered_orders) == len(extraction_result.orders)
            and not self._orders_have_same_signatures(recovered_orders, extraction_result.orders)
            and recovered_has_compatible_families
        )
        if not (
            recovered_matches_expected_better
            or recovered_is_strictly_more_complete
            or recovered_is_content_fix
            or recovered_is_document_backed_replacement
        ):
            return None

        logger.info(
            "Recovered extraction from structured markdown fallback: expected=%s previous=%s recovered=%s mode=%s",
            task_payload.expected_order_count,
            len(extraction_result.orders),
            len(recovered_orders),
            (
                "content_fix"
                if recovered_is_content_fix
                else "replacement"
                if recovered_is_document_backed_replacement
                else "coverage"
            ),
        )
        # fallback은 "최종 주문을 복구"하는 역할이다.
        # 따라서 더 이상 저장을 막지 않아야 하는 blocking issue는 제거하되,
        # 삼성 필터 적용처럼 운영자가 알아야 하는 비차단 issue는 그대로 남긴다.
        preserved_issues = [
            issue
            for issue in extraction_result.issues
            if not any(self._issue_has_code(issue, code) for code in BLOCKING_EXTRACTION_ISSUES)
        ]
        preserved_issues = self._remove_issues_by_code(preserved_issues, "ORDER_COVERAGE_MISMATCH")
        # fallback이 채택됐다는 것은 structured markdown에서 최종 주문을 다시 읽어
        # 필요한 필드를 결정론적으로 복구했다는 뜻이다. 이때 recovered order 모두에
        # base_date가 들어 있다면, LLM stage 중간에 남았던 `BASE_DATE_MISSING`은
        # 더 이상 현재 결과를 설명하지 못하는 stale issue다.
        #
        # 반대로 삼성 필터 적용 같은 비차단 운영 메모는 여전히 유효하므로 유지한다.
        if recovered_orders and all(order.base_date for order in recovered_orders):
            preserved_issues = self._remove_issues_by_code(preserved_issues, "BASE_DATE_MISSING")
        return ExtractionResult(orders=recovered_orders, issues=self._unique_preserve_order(preserved_issues))

    def _extract_fund_seeds(
        self,
        prompt_bundle: PromptBundle,
        chunks: list[str],
        raw_text: str | None,
        issues: list[str],
        artifacts: list[InvalidResponseArtifact],
        *,
        target_fund_scope: TargetFundScope | None = None,
        counterparty_guidance: str | None = None,
    ) -> list[FundSeedItem]:
        """Stage 1을 chunk 단위로 실행해 전체 펀드 inventory를 수집한다."""
        # fund inventory 는 chunk 단위로 독립 실행 가능하다.
        # 이후 단계처럼 batch input 이 없고, 각 chunk 에서 찾은 fund 를 마지막에 합친다.
        stage = self._stage(prompt_bundle, "fund_inventory")
        total_stage_count = len(prompt_bundle.stages)
        all_items: list[FundSeedItem] = []

        def _invoke_chunk(chunk_index: int, chunk: str) -> tuple[_StageInvocation, list[str], str | None]:
            logger.info(
                "Stage %s/%s: extracting fund seeds from chunk %s/%s",
                stage.number,
                total_stage_count,
                chunk_index,
                len(chunks),
            )
            return self._invoke_stage_with_issue_retry(
                prompt_bundle=prompt_bundle,
                stage=stage,
                document_text=self._compose_document_context(chunk, raw_text),
                input_items=None,
                batch_index=chunk_index,
                response_model=FundSeedResult,
                target_fund_scope=target_fund_scope,
                counterparty_guidance=counterparty_guidance,
            )

        for chunk_index, chunk_result in self._execute_indexed_llm_tasks(
            list(enumerate(chunks, start=1)),
            _invoke_chunk,
        ):
            response, stage_issues, stage_partial_issue = chunk_result
            if response.parsed is None:
                self._append_unique(issues, "LLM_INVALID_RESPONSE_FORMAT")
                if response.raw_response:
                    artifacts.append(
                        InvalidResponseArtifact(
                            chunk_index=chunk_index,
                            raw_response=response.raw_response,
                            stage_name=stage.name,
                        )
                    )
                continue
            issues.extend(self._public_stage_findings(stage_issues))
            if stage_partial_issue is not None:
                self._append_unique(issues, stage_partial_issue)
            all_items.extend(response.parsed.items)

        return self._dedupe_fund_seeds(all_items)

    def _augment_fund_seeds_from_document(
        self,
        *,
        document_text: str,
        seeds: list[FundSeedItem],
        target_fund_scope: TargetFundScope | None,
    ) -> list[FundSeedItem]:
        """Structured markdown 근거로 non-zero amount 펀드 seed를 보강한다.

        Lina처럼 `설정금액=0, 해지금액>0`인 행은 stage 1 LLM이 "0이 보인다"는 이유로
        펀드 자체를 통째로 누락할 수 있다. 하지만 stage 2 input은 펀드 inventory seed를
        그대로 쓰므로, 이런 누락은 뒤 단계에서 복구되지 않는다.

        여기서는 문서 표에서 "해당 펀드 row에 실제 non-zero amount cell이 하나라도 있는가"
        를 기준으로 deterministic seed를 보강한다. 0/0 행과 합계행은 제외하고, 삼성 scope는
        기존 `target_fund_scope` 규칙을 그대로 따른다. 즉 mixed-manager 문서에서도
        out-of-scope fund 는 augmentation 대상에 넣지 않는다.
        """
        current_signatures = {self._fund_seed_signature(item) for item in seeds}
        augmented = list(seeds)
        added_count = 0

        for item in self._derive_document_fund_seed_items(
            document_text=document_text,
            target_fund_scope=target_fund_scope,
        ):
            signature = self._fund_seed_signature(item)
            if signature in current_signatures:
                continue
            current_signatures.add(signature)
            augmented.append(item)
            added_count += 1

        if added_count:
            logger.info("Augmented %s fund seed(s) from structured markdown evidence", added_count)
        return self._dedupe_fund_seeds(augmented)

    def _derive_document_fund_seed_items(
        self,
        *,
        document_text: str,
        target_fund_scope: TargetFundScope | None,
    ) -> list[FundSeedItem]:
        """Structured markdown에서 non-zero amount 근거가 있는 fund seed를 읽는다.

        규칙:
        - 합계/총계 행은 제외
        - visible order amount cell 중 하나라도 non-zero 여야 함
        - `target_fund_scope`가 있으면 삼성 대상 fund만 남김
        - 한 방향만 0이어도 다른 방향이 살아 있으면 seed는 유지
        """
        markdown_view = self._structured_markdown_view(document_text)
        derived: list[FundSeedItem] = []
        seen: set[tuple[str, str]] = set()

        for table_lines in self._iter_markdown_table_blocks(markdown_view):
            rows = [self._markdown_table_cells(line) for line in table_lines]
            if len(rows) < 3:
                continue
            header = rows[0]
            if self._is_markdown_separator_row(header):
                continue

            fund_code_index = self._markdown_fund_code_index(header, body_rows=rows[1:])
            fund_name_index = self._markdown_fund_name_index(header)
            if fund_code_index is None or fund_name_index is None:
                continue
            has_amount_header = any(self._looks_like_document_amount_label(label) for label in header)

            for row in rows[1:]:
                if self._is_markdown_separator_row(row):
                    continue
                if fund_code_index >= len(row) or fund_name_index >= len(row):
                    continue

                fund_code = self._normalize_text(row[fund_code_index])
                fund_name = self._normalize_text(row[fund_name_index])
                if not fund_code or not fund_name or self._looks_like_total_row(fund_code, fund_name):
                    continue
                if target_fund_scope is not None and not self._is_target_fund(
                    fund_code=fund_code,
                    fund_name=fund_name,
                    target_fund_scope=target_fund_scope,
                ):
                    continue
                if not self._row_has_non_zero_document_amount(header=header, row=row):
                    continue

                signature = (fund_code, fund_name)
                if signature in seen:
                    continue
                seen.add(signature)
                derived.append(
                    FundSeedItem(
                        fund_code=fund_code,
                        fund_name=fund_name,
                        reason_code=self._derive_document_fund_seed_reason_code(
                            header=header,
                            row=row,
                            target_fund_scope=target_fund_scope,
                        ),
                    )
                )

        return derived

    def _filter_fund_seeds_by_scope(
        self,
        seeds: list[FundSeedItem],
        target_fund_scope: TargetFundScope | None,
    ) -> list[FundSeedItem]:
        """Stage 1 inventory 결과를 삼성 대상 scope에 맞게 필터링한다."""
        if target_fund_scope is None or not target_fund_scope.manager_column_present:
            return seeds

        filtered = [
            item
            for item in seeds
            if self._is_target_fund(
                fund_code=self._normalize_text(item.fund_code),
                fund_name=self._normalize_text(item.fund_name),
                target_fund_scope=target_fund_scope,
            )
        ]
        logger.info("Applied Samsung manager filter to fund inventory: before=%s after=%s", len(seeds), len(filtered))
        return filtered

    def _filter_orders_by_scope(
        self,
        orders: list[OrderExtraction],
        target_fund_scope: TargetFundScope | None,
    ) -> list[OrderExtraction]:
        """최종 주문 목록을 삼성 대상 scope에 맞게 필터링한다."""
        if target_fund_scope is None or not target_fund_scope.manager_column_present:
            return orders
        filtered = [
            order
            for order in orders
            if self._is_target_fund(
                fund_code=self._normalize_text(order.fund_code),
                fund_name=self._normalize_text(order.fund_name),
                target_fund_scope=target_fund_scope,
            )
        ]
        logger.info("Applied Samsung manager filter to final orders: before=%s after=%s", len(orders), len(filtered))
        return filtered

    @staticmethod
    def _is_target_fund(
        fund_code: str,
        fund_name: str,
        target_fund_scope: TargetFundScope,
    ) -> bool:
        """현재 fund_code/fund_name이 target scope에 포함되는지 판정한다."""
        if not target_fund_scope.manager_column_present or target_fund_scope.include_all_funds:
            return True
        if fund_code and fund_code in target_fund_scope.fund_codes:
            return True
        if fund_name and fund_name in target_fund_scope.fund_names:
            return True
        if fund_name and normalize_fund_name_key(fund_name) in target_fund_scope.canonical_fund_names:
            return True
        return False

    def _run_batched_stage(
        self,
        prompt_bundle: PromptBundle,
        stage: StageDefinition,
        document_text: str,
        input_items: list[dict[str, Any]],
        response_model: type[T],
        issues: list[str],
        artifacts: list[InvalidResponseArtifact],
        *,
        counterparty_guidance: str | None = None,
    ) -> list[Any]:
        """이전 단계 산출물을 batch 로 나눠 현재 stage 를 수행한다.

        stage 2~6 은 항상 문서 전체와 함께 실행한다.
        이유는 개별 item 만 주면 문맥을 잃고, 문서 전체를 매번 너무 많이 주면 응답이 흔들리기 때문이다.
        따라서 "문서 전체 + 적당한 크기의 item batch" 균형을 잡는 것이 핵심이다.
        """
        collected: list[Any] = []
        effective_batch_size = self._effective_stage_batch_size(stage.name, input_items)
        batches = list(self._chunk_list(input_items, effective_batch_size))
        total_stage_count = len(prompt_bundle.stages)

        def _invoke_batch(batch_index: int, batch: list[dict[str, Any]]) -> tuple[_StageInvocation, list[str], str | None]:
            logger.info(
                "Stage %s/%s: %s batch %s/%s (items=%s)",
                stage.number,
                total_stage_count,
                stage.name,
                batch_index,
                len(batches),
                len(batch),
            )
            return self._invoke_stage_with_issue_retry(
                prompt_bundle=prompt_bundle,
                stage=stage,
                document_text=document_text,
                input_items=batch,
                batch_index=batch_index,
                response_model=response_model,
                counterparty_guidance=counterparty_guidance,
            )

        for batch_index, batch_result in self._execute_indexed_llm_tasks(
            list(enumerate(batches, start=1)),
            _invoke_batch,
        ):
            response, stage_issues, stage_partial_issue = batch_result
            if response.parsed is None:
                self._append_unique(issues, "LLM_INVALID_RESPONSE_FORMAT")
                if response.raw_response:
                    artifacts.append(
                        InvalidResponseArtifact(
                            chunk_index=batch_index,
                            raw_response=response.raw_response,
                            stage_name=stage.name,
                        )
                    )
                continue
            issues.extend(self._public_stage_findings(stage_issues))
            if stage_partial_issue is not None:
                self._append_unique(issues, stage_partial_issue)
            collected.extend(response.parsed.items)

        return self._dedupe_stage_items(collected)

    @staticmethod
    def _run_llm_task_in_context(
        context: Any,
        worker: Callable[[int, Any], T],
        task_index: int,
        payload: Any,
    ) -> T:
        """worker thread 안에서도 호출 시점의 contextvars를 유지한다."""
        return context.run(worker, task_index, payload)

    def _effective_parallel_llm_workers(self) -> int:
        """실제 동시 LLM 호출 수는 최대 2개로 제한한다."""
        configured = getattr(self, "llm_parallel_workers", 1)
        try:
            return max(1, min(MAX_PARALLEL_LLM_BATCH_WORKERS, int(configured)))
        except (TypeError, ValueError):
            return 1

    def _execute_indexed_llm_tasks(
        self,
        indexed_payloads: list[tuple[int, Any]],
        worker: Callable[[int, Any], T],
    ) -> list[tuple[int, T]]:
        """독립적인 LLM batch/chunk 호출을 최대 2개까지 병렬 실행하고 순서를 복원한다."""
        if not indexed_payloads:
            return []

        max_workers = min(self._effective_parallel_llm_workers(), len(indexed_payloads))
        if max_workers <= 1:
            return [(task_index, worker(task_index, payload)) for task_index, payload in indexed_payloads]

        results_by_index: dict[int, T] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_index = {
                executor.submit(
                    self._run_llm_task_in_context,
                    copy_context(),
                    worker,
                    task_index,
                    payload,
                ): task_index
                for task_index, payload in indexed_payloads
            }
            for future in as_completed(future_to_index):
                task_index = future_to_index[future]
                results_by_index[task_index] = future.result()

        return [(task_index, results_by_index[task_index]) for task_index, _ in indexed_payloads]

    def _resolve_document_base_date_for_seeds(
        self,
        *,
        prompt_bundle: PromptBundle,
        base_date_stage: StageDefinition,
        seeds: list[FundSeedItem],
        document_text: str,
        raw_text: str | None,
        artifacts: list[InvalidResponseArtifact],
        counterparty_guidance: str | None = None,
    ) -> list[FundBaseDateItem] | None:
        """문서 단일 base_date를 우선 찾고, 성공 시 seed 전체에 fan-out 한다."""
        if not seeds:
            return None

        reference_text = raw_text or document_text
        deterministic_base_date = self._detect_document_base_date_from_text(reference_text)
        if deterministic_base_date is not None and not self._date_looks_like_reference_only_in_text(
            reference_text,
            deterministic_base_date,
        ):
            logger.info("Resolved document base_date deterministically: %s", deterministic_base_date)
            base_dates = self._fan_out_document_base_date_to_funds(
                seeds=seeds,
                base_date=deterministic_base_date,
                reason_code=self._derive_document_base_date_reason_code(reference_text, deterministic_base_date),
            )
            logger.info("Fanned out document base_date to %s fund seed(s)", len(base_dates))
            return base_dates

        llm_base_date, llm_reason_code = self._resolve_document_base_date_via_one_shot_llm(
            prompt_bundle=prompt_bundle,
            base_date_stage=base_date_stage,
            seeds=seeds,
            document_text=document_text,
            raw_text=reference_text,
            artifacts=artifacts,
            counterparty_guidance=counterparty_guidance,
        )
        if llm_base_date is None:
            return None

        logger.info("Resolved document base_date via one-shot LLM: %s", llm_base_date)
        base_dates = self._fan_out_document_base_date_to_funds(
            seeds=seeds,
            base_date=llm_base_date,
            reason_code=llm_reason_code or self._derive_document_base_date_reason_code(reference_text, llm_base_date),
        )
        logger.info("Fanned out document base_date to %s fund seed(s)", len(base_dates))
        return base_dates

    def _resolve_document_base_date_via_one_shot_llm(
        self,
        *,
        prompt_bundle: PromptBundle,
        base_date_stage: StageDefinition,
        seeds: list[FundSeedItem],
        document_text: str,
        raw_text: str,
        artifacts: list[InvalidResponseArtifact],
        counterparty_guidance: str | None = None,
    ) -> tuple[str | None, str | None]:
        """대표 seed 1건만 넣어 문서 단일 base_date를 1회 LLM으로 확인한다."""
        representative_seed = next(
            (
                seed
                for seed in seeds
                if self._normalize_text(seed.fund_code) or self._normalize_text(seed.fund_name)
            ),
            None,
        )
        if representative_seed is None:
            return None, None

        total_stage_count = len(prompt_bundle.stages)
        logger.info(
            "Stage %s/%s: resolving document base_date with one-shot LLM",
            base_date_stage.number,
            total_stage_count,
        )
        invocation, stage_issues, partial_issue = self._invoke_stage_with_issue_retry(
            prompt_bundle=prompt_bundle,
            stage=base_date_stage,
            document_text=document_text,
            input_items=[representative_seed.model_dump(mode="json")],
            batch_index=1,
            response_model=FundBaseDateResult,
            counterparty_guidance=counterparty_guidance,
        )
        if invocation.parsed is None:
            if invocation.raw_response:
                artifacts.append(
                    InvalidResponseArtifact(
                        chunk_index=1,
                        raw_response=invocation.raw_response,
                        stage_name=base_date_stage.name,
                    )
                )
            return None, None
        if stage_issues or partial_issue is not None:
            return None, None

        normalized_base_dates = {
            self._normalize_date(item.base_date)
            for item in invocation.parsed.items
            if self._normalize_date(item.base_date) is not None
        }
        if len(normalized_base_dates) != 1:
            return None, None

        candidate = next(iter(normalized_base_dates))
        if self._date_looks_like_reference_only_in_text(raw_text, candidate):
            return None, None

        item_reason_code = next(
            (
                _normalize_reason_code_value(getattr(item, "reason_code", None))
                for item in invocation.parsed.items
                if self._normalize_date(item.base_date) == candidate
                and _normalize_reason_code_value(getattr(item, "reason_code", None)) is not None
            ),
            None,
        )
        return candidate, item_reason_code

    def _fan_out_document_base_date_to_funds(
        self,
        *,
        seeds: list[FundSeedItem],
        base_date: str,
        reason_code: str | None = None,
    ) -> list[FundBaseDateItem]:
        """문서 단일 base_date를 Stage 2 seed 전체에 동일하게 부여한다."""
        normalized_base_date = self._normalize_date(base_date)
        if normalized_base_date is None:
            return []
        normalized_reason_code = _normalize_reason_code_value(reason_code) or "DATE_OTHER_VERIFIED"
        return [
            FundBaseDateItem(
                fund_code=seed.fund_code,
                fund_name=seed.fund_name,
                base_date=normalized_base_date,
                reason_code=normalized_reason_code,
            )
            for seed in seeds
        ]

    @classmethod
    def _detect_document_base_date_from_text(cls, raw_text: str) -> str | None:
        """문서 단일 base_date를 상단/header 규칙으로 우선 탐지한다."""
        if not raw_text:
            return None

        title_adjacent_base_date = cls._detect_title_adjacent_base_date_from_text(raw_text)
        if title_adjacent_base_date is not None:
            return title_adjacent_base_date

        section_nav_dates = cls._collect_section_nav_dates_from_text(raw_text)
        if section_nav_dates is not None:
            if len(section_nav_dates) == 1:
                return section_nav_dates[0]
            return None

        transaction_row_dates = cls._collect_transaction_row_dates_from_text(raw_text)
        if transaction_row_dates is not None:
            if len(transaction_row_dates) == 1:
                return transaction_row_dates[0]
            return None

        generic_base_date = cls._detect_base_date_from_text(raw_text, include_asia_seoul=False)
        if generic_base_date is not None:
            return generic_base_date

        return cls._detect_asia_seoul_base_date_from_text(raw_text)

    @staticmethod
    def _section_order_row_pattern() -> re.Pattern[str]:
        """section ledger 원시 행을 읽는 공통 row pattern을 반환한다."""
        return re.compile(
            r"^(?P<fund_name>.+?)\s+"
            r"(?P<fund_code>[A-Z0-9]+)\s+"
            r"(?P<units>-|[\d,]+\.\d+)\s+"
            r"(?P<nav_date>\d{1,2}-[A-Za-z]{3}-\d{2})\s+"
            r"(?P<nav>[\d,]+\.\d+)\s+"
            r"(?P<amount>-|[\d,]+)\s+"
            r"(?P<bank>[A-Z]{2,})$"
        )

    @classmethod
    def _collect_section_nav_dates_from_text(cls, raw_text: str) -> list[str] | None:
        """section ledger row에서 읽은 normalized NAV Date 목록을 수집한다."""
        lowered_text = raw_text.lower()
        if "subscription" not in lowered_text or "redemption" not in lowered_text:
            return None
        if "fund name code no. of unit nav date nav amount(krw) bank" not in lowered_text:
            return None

        row_pattern = cls._section_order_row_pattern()
        normalized_dates: list[str] = []
        current_order_type: OrderType | None = None
        in_amount_section = False

        for raw_line in raw_text.splitlines():
            line = cls._normalize_text(raw_line)
            if not line:
                continue

            section_order_type = cls._section_order_type_hint(line)
            if section_order_type is not None:
                current_order_type = section_order_type
                in_amount_section = False
                continue

            lowered_line = line.lower()
            if "fund name code no. of unit nav date nav amount(krw) bank" in lowered_line:
                in_amount_section = True
                continue
            if lowered_line.startswith("total "):
                continue
            if line.startswith("[PAGE "):
                continue
            if current_order_type is None or not in_amount_section:
                continue

            match = row_pattern.match(line)
            if match is None:
                continue
            normalized_date = (
                cls._normalize_short_english_date(match.group("nav_date"))
                or cls._normalize_date(match.group("nav_date"))
            )
            if normalized_date is None:
                continue
            normalized_dates.append(normalized_date)

        return list(dict.fromkeys(normalized_dates))

    @classmethod
    def _collect_transaction_row_dates_from_text(cls, raw_text: str) -> list[str] | None:
        """거래 표의 선두 date column에서 읽은 normalized 날짜 목록을 수집한다."""
        header_found = False
        normalized_dates: list[str] = []

        for raw_line in raw_text.splitlines():
            line = cls._normalize_text(raw_line)
            if not line:
                continue

            lowered_line = line.lower()
            if (
                "date | buy&sell | external fund manager | fund code | fund name" in lowered_line
                or "date | buy&sell | fund code | fund name" in lowered_line
            ):
                header_found = True
                continue
            if not header_found:
                continue

            if line.startswith("[PAGE "):
                continue
            if lowered_line.startswith("total"):
                continue

            cells = [cls._normalize_text(cell) for cell in line.split("|")]
            if not cells:
                continue

            first_cell = next((cell for cell in cells if cell), "")
            if not first_cell:
                continue

            normalized_date = (
                cls._normalize_buy_sell_report_date(first_cell)
                or cls._normalize_date(first_cell)
                or cls._normalize_short_english_date(first_cell)
            )
            if normalized_date is None:
                continue
            normalized_dates.append(normalized_date)

        if not header_found:
            return None
        return list(dict.fromkeys(normalized_dates))

    @classmethod
    def _base_date_header_lines(cls, raw_text: str, *, max_lines: int = 16) -> list[str]:
        """문서 상단에서 base_date 후보를 찾기 위한 header line window를 만든다."""
        meaningful_lines = [cls._normalize_text(line) for line in raw_text.splitlines()]
        meaningful_lines = [line for line in meaningful_lines if line]

        header_lines: list[str] = []
        for line in meaningful_lines:
            lowered = line.lower()
            if re.search(r"(?:^|\b)\d+\.\s*(subscription|redemption)\b", lowered):
                break
            if "fund name code no. of unit nav date nav amount(krw) bank" in lowered:
                break
            header_lines.append(line)
            if len(header_lines) >= max_lines:
                break
        return header_lines

    @classmethod
    def _detect_asia_seoul_base_date_from_text(cls, raw_text: str) -> str | None:
        """EML loader가 추가한 `Date (Asia/Seoul)` 헤더를 최우선 기준일로 읽는다."""
        match = re.search(
            r"Date\s*\(Asia/Seoul\)\s*:\s*(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})",
            raw_text,
            flags=re.IGNORECASE,
        )
        if match is None:
            return None
        year, month, day = match.groups()
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"

    def _classify_instruction_document(
        self,
        *,
        prompt_bundle: PromptBundle,
        document_text: str,
        artifacts: list[InvalidResponseArtifact],
        counterparty_guidance: str | None = None,
    ) -> tuple[bool | None, str | None]:
        """문서가 실제 지시서인지 먼저 보수적으로 판별한다.

        이 단계는 false-fail을 늘리면 안 되므로 아래 원칙을 따른다.
        - 명확히 `false`일 때만 비지시서로 차단한다.
        - parsed 없음/빈 items/애매한 값은 모두 `None`으로 보고 기존 추출을 계속한다.
        - 이전 응답을 재프롬프트로 넣지 않고 같은 문서로만 재호출한다.
        - cover email / wrapper는 여기서 먼저 빠지지만, "주문 없음" 문서는 이후 no-order 규칙이나
          direct parser 판단이 우선할 수 있다.
        """
        stage = self._stage(prompt_bundle, "instruction_document")
        total_stage_count = len(prompt_bundle.stages)
        logger.info("Stage %s/%s: classifying instruction document", stage.number, total_stage_count)
        response, _, _ = self._invoke_stage_with_issue_retry(
            prompt_bundle=prompt_bundle,
            stage=stage,
            document_text=document_text,
            input_items=None,
            batch_index=1,
            response_model=InstructionDocumentResult,
            counterparty_guidance=counterparty_guidance,
        )
        if response.parsed is None:
            if response.raw_response:
                artifacts.append(
                    InvalidResponseArtifact(
                        chunk_index=1,
                        raw_response=response.raw_response,
                        stage_name=stage.name,
                    )
                )
            return None, None

        items = list(response.parsed.items)
        if not items:
            return None, None

        first_item = items[0]
        reason_summary = self._normalize_text(getattr(response.parsed, "reason_summary", "")) or None
        if first_item.is_instruction_document is True:
            return True, None
        if first_item.is_instruction_document is False:
            return False, reason_summary or self._normalize_text(first_item.reason) or None
        return None, None

    def _invoke_stage_with_issue_retry(
        self,
        *,
        prompt_bundle: PromptBundle,
        stage: StageDefinition,
        document_text: str,
        input_items: list[dict[str, Any]] | None,
        batch_index: int,
        response_model: type[T],
        target_fund_scope: TargetFundScope | None = None,
        counterparty_guidance: str | None = None,
    ) -> tuple["_StageInvocation", list[str], str | None]:
        """Stage 응답에 issue가 남으면 이슈-지향 retry prompt로 같은 stage를 다시 호출한다.

        transport/JSON 포맷 재시도는 `_invoke_stage()`가 이미 담당한다.
        이 helper는 그보다 한 단계 위에서, "유효한 JSON은 받았지만 stage issue 또는
        partial 결과가 남은 경우" retry prompt로 같은 stage를 다시 호출해 더 깨끗한
        응답을 우선 채택한다.

        중요한 점:
        - 1차 호출은 기존 prompt를 그대로 사용한다.
        - retry는 원문 문서와 원래 input batch는 유지하되, 직전 best parsed output과
          normalized retry target issues를 별도 retry prompt에 넣는다.
        - 추가 재호출 횟수는 단계별 코드 상수로 고정한다.
          `instruction_document`는 1회, 나머지 stage는 2회다.
        - retry 선택은 "issue 수가 적다"만 보지 않고,
          same-slot missing field 복구는 허용하면서 ghost/extra output은 차단한다.
        """
        best_invocation: _StageInvocation | None = None
        best_stage_issues: list[str] = []
        best_partial_issue: str | None = None
        best_score: tuple[int, int, int, int] | None = None
        _record_llm_batch_started(stage.name)

        stage_retry_attempts = self._stage_issue_retry_attempt_limit(stage.name)
        total_attempts = stage_retry_attempts + 1

        for attempt in range(1, total_attempts + 1):
            retry_context = None
            if attempt > 1:
                _record_stage_retry_invocation(stage.name)
            if (
                attempt > 1
                and best_invocation is not None
                and best_invocation.parsed is not None
                and (best_stage_issues or best_partial_issue is not None)
            ):
                normalized_best_parsed = self._normalize_stage_retry_parsed(
                    stage_name=stage.name,
                    document_text=document_text,
                    input_items=input_items or [],
                    parsed=best_invocation.parsed,
                )
                retry_context = self._build_stage_retry_context(
                    stage=stage,
                    document_text=document_text,
                    input_items=input_items,
                    previous_parsed=normalized_best_parsed,
                    stage_issues=best_stage_issues,
                    partial_issue=best_partial_issue,
                    attempt_number=attempt - 1,
                    target_fund_scope=target_fund_scope,
                )
                if retry_context is not None:
                    logger.info(
                        "retry stage=%s batch=%s attempt=%s/%s target_issues=%s",
                        stage.name,
                        batch_index,
                        attempt - 1,
                        stage_retry_attempts,
                        json.dumps(retry_context["target_issues"], ensure_ascii=False),
                    )

            retry_context_token = _ACTIVE_STAGE_RETRY_CONTEXT.set(retry_context)
            try:
                invocation = self._invoke_stage(
                    prompt_bundle=prompt_bundle,
                    stage=stage,
                    document_text=document_text,
                    input_items=input_items,
                    batch_index=batch_index,
                    response_model=response_model,
                    counterparty_guidance=counterparty_guidance,
                )
            finally:
                _ACTIVE_STAGE_RETRY_CONTEXT.reset(retry_context_token)

            invocation = self._reconcile_stage_retry_invocation_output(
                stage_name=stage.name,
                document_text=document_text,
                input_items=input_items or [],
                invocation=invocation,
            )
            invocation = self._merge_focused_stage_retry_invocation_output(
                stage_name=stage.name,
                document_text=document_text,
                input_items=input_items or [],
                current_parsed=best_invocation.parsed if best_invocation is not None else None,
                invocation=invocation,
                retry_context=retry_context,
            )
            stage_issues, partial_issue = self._collect_stage_findings(
                stage=stage,
                document_text=document_text,
                input_items=input_items,
                parsed=invocation.parsed,
                target_fund_scope=target_fund_scope,
            )
            score = self._score_stage_invocation(
                parsed=invocation.parsed,
                stage_issues=stage_issues,
                partial_issue=partial_issue,
            )
            can_supersede_best = self._can_supersede_stage_retry_result(
                stage=stage,
                document_text=document_text,
                input_items=input_items,
                current_parsed=best_invocation.parsed if best_invocation is not None else None,
                current_stage_issues=best_stage_issues,
                current_partial_issue=best_partial_issue,
                candidate_parsed=invocation.parsed,
            )
            if best_score is None or (can_supersede_best and score <= best_score):
                best_invocation = invocation
                best_stage_issues = stage_issues
                best_partial_issue = partial_issue
                best_score = score

            if invocation.parsed is None:
                if best_invocation is None or attempt >= total_attempts:
                    break
                logger.info(
                    "retry stage=%s batch=%s attempt=%s/%s after parsed=None stage response",
                    stage.name,
                    batch_index,
                    attempt,
                    stage_retry_attempts,
                )
                continue
            if not stage_issues and partial_issue is None:
                if can_supersede_best:
                    if retry_context is not None:
                        logger.info(
                            "retry_resolved stage=%s batch=%s attempt=%s/%s",
                            stage.name,
                            batch_index,
                            attempt - 1,
                            stage_retry_attempts,
                        )
                    self._log_stage_reason_summary(stage_name=stage.name, batch_index=batch_index, parsed=invocation.parsed)
                    return invocation, [], None
                if attempt >= total_attempts:
                    break
                continue
            if attempt >= total_attempts:
                break

            logger.info(
                "Re-invoking stage=%s batch=%s due to stage issues=%s partial_issue=%s (attempt %s/%s)",
                stage.name,
                batch_index,
                stage_issues,
                partial_issue,
                attempt + 1,
                total_attempts,
            )

        assert best_invocation is not None  # _invoke_stage() always returns an invocation object.
        self._log_stage_reason_summary(stage_name=stage.name, batch_index=batch_index, parsed=best_invocation.parsed)
        return best_invocation, best_stage_issues, best_partial_issue

    def _collect_stage_findings(
        self,
        *,
        stage: StageDefinition,
        document_text: str | None,
        input_items: list[dict[str, Any]] | None,
        parsed: BaseModel | None,
        target_fund_scope: TargetFundScope | None = None,
    ) -> tuple[list[str], str | None]:
        """단일 stage 응답에서 재호출 판단에 쓸 finding 목록을 계산한다."""
        if parsed is None:
            return [], None

        stage_issues = self._unique_preserve_order(
            [self._normalize_text(issue) for issue in getattr(parsed, "issues", []) if self._normalize_text(issue)]
        )
        stage_issues = self._unique_preserve_order(
            stage_issues
            + self._derive_retry_findings_from_stage_items(
                stage_name=stage.name,
                document_text=document_text,
                parsed=parsed,
                target_fund_scope=target_fund_scope,
            )
        )
        partial_issue = self._stage_partial_issue_code(
            stage=stage,
            document_text=document_text,
            input_items=input_items,
            output_items=list(getattr(parsed, "items", [])),
        )
        return stage_issues, partial_issue

    def _build_stage_retry_context(
        self,
        *,
        stage: StageDefinition,
        document_text: str,
        input_items: list[dict[str, Any]] | None,
        previous_parsed: BaseModel | None,
        stage_issues: list[str],
        partial_issue: str | None,
        attempt_number: int,
        target_fund_scope: TargetFundScope | None = None,
    ) -> dict[str, Any] | None:
        """Retry prompt에 넣을 최소 context를 만든다."""
        if previous_parsed is None:
            return None

        previous_parsed = self._normalize_stage_retry_parsed(
            stage_name=stage.name,
            document_text=document_text,
            input_items=input_items or [],
            parsed=previous_parsed,
        )
        previous_output_items = [
            item.model_dump(mode="json") if hasattr(item, "model_dump") else item
            for item in getattr(previous_parsed, "items", [])
        ]
        previous_output_items = self._normalize_stage_retry_previous_output_items(
            stage_name=stage.name,
            document_text=document_text,
            input_items=input_items or [],
            previous_output_items=previous_output_items,
        )
        raw_issues = list(stage_issues)
        if partial_issue is not None:
            raw_issues.append(partial_issue)
        if not raw_issues:
            return None

        target_issues = self._normalize_stage_retry_issues(
            stage_name=stage.name,
            issues=raw_issues,
            input_items=input_items,
            previous_output_items=previous_output_items,
        )
        focus_items = self._derive_retry_focus_items(
            stage_name=stage.name,
            document_text=document_text,
            input_items=input_items,
            previous_output_items=previous_output_items,
            retry_target_issues=target_issues,
            target_fund_scope=target_fund_scope,
        )
        target_issues = self._refine_retry_target_issues_with_focus_items(
            stage_name=stage.name,
            target_issues=target_issues,
            focus_items=focus_items,
        )
        focus_items = self._derive_retry_focus_items(
            stage_name=stage.name,
            document_text=document_text,
            input_items=input_items,
            previous_output_items=previous_output_items,
            retry_target_issues=target_issues,
            target_fund_scope=target_fund_scope,
        )
        previous_output_items = self._normalize_stage_retry_context_previous_output_items(
            stage_name=stage.name,
            document_text=document_text,
            input_items=input_items or [],
            previous_output_items=previous_output_items,
            focus_items=focus_items,
        )
        previous_output_items = self._subset_retry_previous_output_items(
            stage_name=stage.name,
            previous_output_items=previous_output_items,
            focus_items=focus_items,
        )
        previous_output_items = self._normalize_stage_retry_context_previous_output_items(
            stage_name=stage.name,
            document_text=document_text,
            input_items=input_items or [],
            previous_output_items=previous_output_items,
            focus_items=focus_items,
        )
        previous_reason_summary = _normalize_reason_summary_value(getattr(previous_parsed, "reason_summary", ""))
        return {
            "attempt_number": attempt_number,
            "max_attempts": self._stage_issue_retry_attempt_limit(stage.name),
            "target_issues": target_issues,
            "previous_output_items": previous_output_items,
            "focus_items": focus_items,
            "previous_reason_summary": previous_reason_summary,
        }

    def _effective_stage_batch_size(
        self,
        stage_name: str,
        input_items: list[dict[str, Any]] | None,
    ) -> int:
        """Stage와 입력 shape를 기준으로 effective batch size를 고른다."""
        if stage_name == "t_day" and len(input_items or []) > T_DAY_STAGE_BATCH_SIZE:
            return T_DAY_STAGE_BATCH_SIZE
        return self.stage_batch_size

    def _stage_max_tokens(self, stage_name: str) -> int:
        """Stage별 응답 상한을 고른다.

        기본값은 전역 `VA_LLM_MAX_TOKENS`를 그대로 쓰고, 응답 shape가 단순한 stage만
        더 낮은 상한을 사용한다. 이 최적화는 거래처명이 아니라 stage 성격에만 의존한다.
        """
        llm_max_tokens = int(getattr(self, "llm_max_tokens", VA_LLM_MAX_TOKENS))
        configured = STAGE_SPECIFIC_MAX_TOKENS.get(stage_name)
        if configured is None:
            return llm_max_tokens
        return min(llm_max_tokens, int(configured))

    def _subset_retry_previous_output_items(
        self,
        *,
        stage_name: str,
        previous_output_items: list[dict[str, Any]],
        focus_items: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Retry prompt에 넣을 previous output을 focus subset으로 줄인다."""
        if not previous_output_items or not focus_items:
            return previous_output_items

        focus_signatures = {
            self._stage_item_signature(stage_name, item)
            for item in focus_items
        }
        focus_family_signatures = {
            self._stage_partial_item_signature(stage_name, item)
            for item in focus_items
        }

        narrowed = [
            item
            for item in previous_output_items
            if self._stage_item_signature(stage_name, item) in focus_signatures
            or self._stage_partial_item_signature(stage_name, item) in focus_family_signatures
        ]
        if not narrowed or len(narrowed) >= len(previous_output_items):
            return previous_output_items
        return self._dedupe_retry_focus_items(narrowed)

    def _stage_issue_retry_attempt_limit(self, stage_name: str) -> int:
        """Stage 이름에 따라 추가 retry 최대 횟수를 반환한다."""
        if stage_name == "instruction_document":
            return int(
                getattr(
                    self,
                    "llm_instruction_document_retry_attempts",
                    INSTRUCTION_DOCUMENT_STAGE_ISSUE_RETRY_ATTEMPTS,
                )
            )
        return int(
            getattr(
                self,
                "llm_stage_issue_retry_attempts",
                OTHER_STAGE_ISSUE_RETRY_ATTEMPTS,
            )
        )

    def _refine_retry_target_issues_with_focus_items(
        self,
        *,
        stage_name: str,
        target_issues: list[dict[str, Any]],
        focus_items: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """generic stage issue를 exact focus item 기반 retry target으로 좁힌다.

        동양생명처럼 한 batch 안에서 fund 하나가 여러 future cell로 분해되는 문서는
        `T_DAY_STAGE_PARTIAL` 같은 generic issue만으로 retry를 걸면 범위가 너무 넓다.
        이 helper는 이미 문서 기반으로 찾아낸 exact focus item이 있을 때에만
        target issue를 해당 fund/slot/evidence 단위로 세분화해 retry prompt의 scope를
        좁힌다. focus item이 없으면 기존 generic issue를 유지한다.
        """
        if not target_issues or not focus_items:
            return target_issues

        refined: list[dict[str, Any]] = []
        for issue in target_issues:
            if self._is_internal_retry_finding(self._normalize_text(issue.get("issue_code"))):
                refined.append(issue)
                continue
            if stage_name == "fund_inventory" and (
                self._normalize_text(issue.get("fund_code")) or self._normalize_text(issue.get("fund_name"))
            ):
                refined.append(issue)
                continue
            if (
                self._normalize_text(issue.get("slot_id"))
                or self._normalize_text(issue.get("evidence_label"))
            ):
                refined.append(issue)
                continue

            expanded = self._expand_generic_retry_issue_with_focus_items(
                stage_name=stage_name,
                issue=issue,
                focus_items=focus_items,
            )
            if expanded:
                refined.extend(expanded)
                continue
            refined.append(issue)

        return self._dedupe_retry_target_issues(refined)

    def _expand_generic_retry_issue_with_focus_items(
        self,
        *,
        stage_name: str,
        issue: dict[str, Any],
        focus_items: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """generic retry issue를 focus item별 issue로 세분화한다."""
        expanded: list[dict[str, Any]] = []
        for item in focus_items:
            fund_code = self._normalize_text(item.get("fund_code")) or None
            fund_name = self._normalize_text(item.get("fund_name")) or None
            slot_id = self._normalize_text(item.get("slot_id")) or None
            evidence_label = self._normalize_text(item.get("evidence_label")) or None
            if not fund_code and not fund_name:
                continue
            if stage_name != "fund_inventory" and not slot_id and not evidence_label:
                continue
            expanded.append(
                {
                    "raw_issue": issue.get("raw_issue", ""),
                    "issue_code": issue.get("issue_code", ""),
                    "fund_code": fund_code,
                    "fund_name": fund_name,
                    "slot_id": slot_id,
                    "evidence_label": evidence_label,
                    "stage_name": issue.get("stage_name", stage_name),
                }
            )
        return expanded

    @staticmethod
    def _dedupe_retry_target_issues(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """retry target issue JSON을 등장 순서 그대로 dict 단위 dedupe 한다."""
        seen: set[str] = set()
        deduped: list[dict[str, Any]] = []
        for item in items:
            key = json.dumps(item, ensure_ascii=False, sort_keys=True)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    def _normalize_stage_retry_issues(
        self,
        *,
        stage_name: str,
        issues: list[str],
        input_items: list[dict[str, Any]] | None,
        previous_output_items: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """raw issue 문자열을 retry prompt용 구조체로 정규화한다."""
        normalized: list[dict[str, Any]] = []
        for issue in issues:
            normalized_issue = self._normalize_stage_retry_issue(
                stage_name=stage_name,
                issue=issue,
                input_items=input_items,
                previous_output_items=previous_output_items,
            )
            if normalized_issue is None:
                continue
            normalized.append(normalized_issue)
        return normalized

    def _normalize_stage_retry_issue(
        self,
        *,
        stage_name: str,
        issue: str,
        input_items: list[dict[str, Any]] | None,
        previous_output_items: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """retry target issue 1건을 구조화한다."""
        raw_issue = self._normalize_text(issue)
        if not raw_issue:
            return None

        issue_code = self._normalize_stage_retry_issue_code(raw_issue)
        fund_code: str | None = None
        fund_name: str | None = None
        slot_id: str | None = None
        evidence_label: str | None = None

        match = re.search(r"_FOR_([A-Za-z0-9]+)$", raw_issue)
        if match is not None:
            fund_code = self._normalize_text(match.group(1)) or None
        should_infer_slot_scope = bool(match) or not self._is_generic_stage_retry_issue_code(issue_code)

        matched_items: list[dict[str, Any]] = []
        matched_previous_output_items: list[dict[str, Any]] = []
        matched_input_items: list[dict[str, Any]] = []
        if fund_code is not None:
            matched_previous_output_items = [
                item
                for item in previous_output_items
                if self._normalize_text(item.get("fund_code")) == fund_code
            ]
            matched_input_items = [
                item for item in list(input_items or []) if self._normalize_text(item.get("fund_code")) == fund_code
            ]
            matched_items = matched_previous_output_items + matched_input_items
        else:
            candidate_items = previous_output_items + list(input_items or [])
            unique_fund_codes = {
                self._normalize_text(item.get("fund_code"))
                for item in candidate_items
                if self._normalize_text(item.get("fund_code"))
            }
            if len(unique_fund_codes) == 1:
                only_fund_code = next(iter(unique_fund_codes))
                matched_previous_output_items = [
                    item
                    for item in previous_output_items
                    if self._normalize_text(item.get("fund_code")) == only_fund_code
                ]
                matched_input_items = [
                    item for item in list(input_items or []) if self._normalize_text(item.get("fund_code")) == only_fund_code
                ]
                matched_items = matched_previous_output_items + matched_input_items
                fund_code = only_fund_code
            elif len(candidate_items) == 1:
                matched_items = candidate_items

        if matched_items:
            reference_items = matched_previous_output_items or matched_items
            richest_item = max(
                reference_items,
                key=lambda item: (
                    1 if self._normalize_text(item.get("slot_id")) else 0,
                    1 if self._normalize_text(item.get("evidence_label")) else 0,
                    1 if self._normalize_text(item.get("fund_name")) else 0,
                ),
            )
            first_item = matched_items[0]
            fund_code = fund_code or (self._normalize_text(first_item.get("fund_code")) or None)
            fund_name = self._normalize_text(richest_item.get("fund_name")) or None
            if should_infer_slot_scope:
                slot_id = self._normalize_text(richest_item.get("slot_id")) or None
                evidence_label = self._normalize_text(richest_item.get("evidence_label")) or None

        return {
            "raw_issue": raw_issue,
            "issue_code": issue_code,
            "fund_code": fund_code,
            "fund_name": fund_name,
            "slot_id": slot_id,
            "evidence_label": evidence_label,
            "stage_name": stage_name,
        }

    @staticmethod
    def _is_generic_stage_retry_issue_code(issue_code: str) -> bool:
        """특정 row/slot을 직접 가리키지 않는 generic stage issue인지 본다."""
        if not issue_code:
            return False
        if issue_code.endswith("_STAGE_PARTIAL"):
            return True
        return issue_code in {
            "BASE_DATE_MISSING",
            "T_DAY_MISSING",
            "TRANSFER_AMOUNT_MISSING",
            "SETTLE_CLASS_MISSING",
            "ORDER_TYPE_MISSING",
        }

    def _normalize_stage_retry_issue_code(self, issue: str) -> str:
        """상세 issue 문자열을 stage retry용 코드로 정규화한다."""
        normalized_issue = self._normalize_text(issue)
        if not normalized_issue:
            return ""
        if self._is_internal_retry_finding(normalized_issue):
            return normalized_issue

        suffix_match = re.match(r"(.+)_FOR_[A-Za-z0-9]+$", normalized_issue)
        if suffix_match is not None:
            return self._normalize_text(suffix_match.group(1))

        if ":" in normalized_issue:
            prefix = self._normalize_text(normalized_issue.split(":", 1)[0])
            if prefix:
                return prefix
        return normalized_issue

    def _derive_retry_focus_items(
        self,
        *,
        stage_name: str,
        document_text: str,
        input_items: list[dict[str, Any]] | None,
        previous_output_items: list[dict[str, Any]],
        retry_target_issues: list[dict[str, Any]],
        target_fund_scope: TargetFundScope | None = None,
    ) -> list[dict[str, Any]]:
        """retry prompt에서 다시 보게 할 missing/incomplete item 범위를 좁힌다.

        blind rerun의 핵심 한계는 "빠진 row는 previous_output 안에 존재하지 않는다"는 점이다.
        그래서 retry focus는 현재 output만 좁히는 것으로 끝나면 안 되고,
        input 대비 빠진 signature나 structured markdown가 직접 보여주는 missing seed/slot도
        같이 강조해야 실제 누락 복구 확률이 올라간다.
        """
        input_item_list = list(input_items or [])
        candidate_items = previous_output_items or input_item_list
        if not candidate_items:
            return []

        missing_input_items = self._derive_missing_input_retry_focus_items(
            stage_name=stage_name,
            input_items=input_item_list,
            previous_output_items=previous_output_items,
        )
        stage_specific_focus_items: list[dict[str, Any]] = []
        if stage_name == "fund_inventory" and document_text:
            stage_specific_focus_items = self._derive_missing_document_fund_seed_focus_items(
                document_text=document_text,
                previous_output_items=previous_output_items,
                target_fund_scope=target_fund_scope,
            )
        elif stage_name == "t_day" and document_text and input_item_list:
            stage_specific_focus_items = self._derive_missing_document_t_day_focus_items(
                document_text=document_text,
                input_items=input_item_list,
                previous_output_items=previous_output_items,
            )
        if stage_specific_focus_items:
            incomplete_items = [
                item
                for item in previous_output_items
                if not self._stage_items_are_complete(stage_name, [item])
            ]
            return self._dedupe_retry_focus_items(stage_specific_focus_items + incomplete_items)

        incomplete_items = [
            item
            for item in previous_output_items
            if not self._stage_items_are_complete(stage_name, [item])
        ]
        has_exact_retry_scope = any(
            self._normalize_text(issue.get("slot_id")) or self._normalize_text(issue.get("evidence_label"))
            for issue in retry_target_issues
        )
        if missing_input_items and not has_exact_retry_scope:
            return self._dedupe_retry_focus_items(missing_input_items + incomplete_items)

        fund_codes = {
            self._normalize_text(issue.get("fund_code"))
            for issue in retry_target_issues
            if self._normalize_text(issue.get("fund_code"))
        }
        slot_ids = {
            self._normalize_text(issue.get("slot_id"))
            for issue in retry_target_issues
            if self._normalize_text(issue.get("slot_id"))
        }
        evidence_labels = {
            self._normalize_text(issue.get("evidence_label"))
            for issue in retry_target_issues
            if self._normalize_text(issue.get("evidence_label"))
        }

        narrowed = list(candidate_items)
        narrowing_pool = self._dedupe_retry_focus_items(previous_output_items + missing_input_items) or list(candidate_items)
        if fund_codes:
            by_fund_code = [
                item for item in narrowing_pool if self._normalize_text(item.get("fund_code")) in fund_codes
            ]
            if by_fund_code:
                narrowed = by_fund_code
        if slot_ids:
            by_slot = [item for item in narrowed if self._normalize_text(item.get("slot_id")) in slot_ids]
            if by_slot:
                narrowed = by_slot
        if evidence_labels:
            by_evidence = [
                item for item in narrowed if self._normalize_text(item.get("evidence_label")) in evidence_labels
            ]
            if by_evidence:
                narrowed = by_evidence

        if narrowed != candidate_items:
            return narrowed

        incomplete_items = [
            item
            for item in candidate_items
            if not self._stage_items_are_complete(stage_name, [item])
        ]
        if incomplete_items:
            return self._dedupe_retry_focus_items(incomplete_items + missing_input_items)

        if missing_input_items:
            return missing_input_items

        return candidate_items

    def _derive_missing_input_retry_focus_items(
        self,
        *,
        stage_name: str,
        input_items: list[dict[str, Any]],
        previous_output_items: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """input 대비 조용히 사라진 stage identity를 retry focus로 끌어온다."""
        if not input_items or not previous_output_items:
            return []

        previous_signatures = {
            self._stage_item_signature(stage_name, item)
            for item in previous_output_items
        }
        missing_items: list[dict[str, Any]] = []
        for item in input_items:
            if self._stage_item_signature(stage_name, item) in previous_signatures:
                continue
            missing_items.append(dict(item))
        return missing_items

    def _derive_missing_document_fund_seed_focus_items(
        self,
        *,
        document_text: str,
        previous_output_items: list[dict[str, Any]],
        target_fund_scope: TargetFundScope | None,
    ) -> list[dict[str, Any]]:
        """문서 표가 보여주는데 stage 1 output에 없는 seed를 retry focus로 만든다."""
        expected_items = self._derive_document_fund_seed_items(
            document_text=document_text,
            target_fund_scope=target_fund_scope,
        )
        if not expected_items:
            return []

        current_signatures = {
            self._fund_seed_signature(item)
            for item in previous_output_items
            if self._fund_seed_signature(item)[0] or self._fund_seed_signature(item)[1]
        }
        missing_items: list[dict[str, Any]] = []
        for item in expected_items:
            item_dict = item.model_dump(mode="json")
            if self._fund_seed_signature(item_dict) in current_signatures:
                continue
            missing_items.append(item_dict)
        return missing_items

    def _derive_missing_document_t_day_focus_items(
        self,
        *,
        document_text: str,
        input_items: list[dict[str, Any]],
        previous_output_items: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """문서 표가 요구하는 missing sibling slot을 retry focus item으로 만든다."""
        expected_items = self._build_deterministic_t_day_items(
            document_text=document_text,
            input_items=input_items,
        )
        if not expected_items:
            return []

        expected_family_slots = self._derive_expected_t_day_family_slots(document_text, input_items)
        current_signatures = {
            self._stage_item_signature("t_day", item)
            for item in previous_output_items
        }
        current_actual_slots = self._derive_actual_t_day_family_slots(previous_output_items)
        expected_signatures_by_family: dict[tuple[Any, ...], set[tuple[Any, ...]]] = {}
        for item in expected_items:
            item_dict = item.model_dump(mode="json")
            expected_signatures_by_family.setdefault(
                self._stage_partial_item_signature("t_day", item_dict),
                set(),
            ).add(self._stage_item_signature("t_day", item_dict))
        missing_items: list[dict[str, Any]] = []

        for item in expected_items:
            item_dict = item.model_dump(mode="json")
            signature = self._stage_item_signature("t_day", item_dict)
            if signature in current_signatures:
                continue
            family_key = self._stage_partial_item_signature("t_day", item_dict)
            family_expected_slots = expected_family_slots.get(family_key, set())
            family_actual_slots = current_actual_slots.get(family_key, set())
            if family_expected_slots and family_expected_slots == family_actual_slots:
                continue
            item_dict["retry_expected_family_slots"] = [
                self._deterministic_t_day_slot_id(t_day=t_day, slot_kind=slot_kind)
                for t_day, slot_kind in sorted(family_expected_slots)
            ]
            missing_items.append(item_dict)

        if missing_items:
            return missing_items

        problematic_expected_items: list[dict[str, Any]] = []
        current_signatures_by_family: dict[tuple[Any, ...], set[tuple[Any, ...]]] = {}
        for item in previous_output_items:
            current_signatures_by_family.setdefault(
                self._stage_partial_item_signature("t_day", item),
                set(),
            ).add(self._stage_item_signature("t_day", item))

        problematic_families = {
            family_key
            for family_key, expected_signatures in expected_signatures_by_family.items()
            if current_signatures_by_family.get(family_key, set()) - expected_signatures
        }
        if problematic_families:
            for item in expected_items:
                item_dict = item.model_dump(mode="json")
                family_key = self._stage_partial_item_signature("t_day", item_dict)
                if family_key not in problematic_families:
                    continue
                family_expected_slots = expected_family_slots.get(family_key, set())
                item_dict["retry_expected_family_slots"] = [
                    self._deterministic_t_day_slot_id(t_day=t_day, slot_kind=slot_kind)
                    for t_day, slot_kind in sorted(family_expected_slots)
                ]
                problematic_expected_items.append(item_dict)
        if problematic_expected_items:
            return self._dedupe_retry_focus_items(problematic_expected_items)

        current_families = {
            self._stage_partial_item_signature("t_day", item)
            for item in previous_output_items
        }
        fallback_missing_families: list[dict[str, Any]] = []
        for item in input_items:
            family_key = self._stage_partial_item_signature("t_day", item)
            if family_key in current_families:
                continue
            fallback_missing_families.append(dict(item))
        return fallback_missing_families

    def _dedupe_retry_focus_items(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Retry focus item JSON은 등장 순서를 유지한 채 dict 단위로 dedupe 한다."""
        index_by_dump: dict[str, int] = {}
        deduped: list[dict[str, Any]] = []
        for item in items:
            key = json.dumps(
                self._stage_item_payload_without_metadata(item),
                ensure_ascii=False,
                sort_keys=True,
            )
            existing_index = index_by_dump.get(key)
            if existing_index is not None:
                deduped[existing_index] = self._merge_stage_item_reason_code(deduped[existing_index], item)
                continue
            index_by_dump[key] = len(deduped)
            deduped.append(dict(item))
        return deduped

    def _derive_retry_findings_from_stage_items(
        self,
        *,
        stage_name: str,
        document_text: str | None,
        parsed: BaseModel,
        target_fund_scope: TargetFundScope | None = None,
    ) -> list[str]:
        """후처리에서만 드러나는 invalid/missing 신호를 stage retry용 finding으로 끌어올린다.

        일부 응답은 `issues=[]` 이지만 값이 비정상 포맷이라 `_build_result()` 단계에서야
        `BASE_DATE_MISSING`, `TRANSFER_AMOUNT_MISSING`, `SETTLE_CLASS_MISSING` 같은
        blocking issue로 바뀐다. 이런 케이스는 같은 stage를 다시 부르는 편이 더 맞기 때문에,
        여기서 stage-level finding으로 미리 승격한다.

        `fund_inventory`는 예외적으로 item 필드 invalid 보다 "조용한 seed 누락"이 더 치명적이다.
        그래서 문서에서 실제로 보이는 non-zero fund 집합과 stage 1 결과를 비교해
        내부 retry-only finding(`_RETRY_*`)을 만들고, 이 신호는 재호출 판단에만 쓰고
        최종 public issue로는 노출하지 않는다.
        """
        items = list(getattr(parsed, "items", []))
        derived: list[str] = []

        if stage_name == "fund_inventory":
            expected_signatures = self._derive_document_fund_seed_signatures(
                document_text or "",
                target_fund_scope=target_fund_scope,
            )
            actual_signatures = {
                self._fund_seed_signature(item)
                for item in items
                if self._fund_seed_signature(item)[0] or self._fund_seed_signature(item)[1]
            }
            if expected_signatures and not expected_signatures.issubset(actual_signatures):
                derived.append(f"{INTERNAL_RETRY_FINDING_PREFIX}FUND_DISCOVERY_PARTIAL")
            return derived

        if stage_name == "base_date":
            if any(self._normalize_date(getattr(item, "base_date", None)) is None for item in items):
                derived.append("BASE_DATE_MISSING")
            return derived

        if stage_name == "transfer_amount":
            if any(self._normalize_amount(getattr(item, "transfer_amount", None)) is None for item in items):
                derived.append("TRANSFER_AMOUNT_MISSING")
            return derived

        if stage_name == "settle_class":
            for item in items:
                evidence_label = self._normalize_text(getattr(item, "evidence_label", None))
                if (
                    self._normalize_settle_class(
                        getattr(item, "settle_class", None),
                        getattr(item, "t_day", None),
                        evidence_label,
                    )
                    is None
                ):
                    derived.append("SETTLE_CLASS_MISSING")
                    break
            return derived

        if stage_name == "order_type":
            for item in items:
                amount = self._normalize_amount(getattr(item, "transfer_amount", None))
                if amount is None:
                    derived.append("TRANSFER_AMOUNT_MISSING")
                    break
                evidence_label = self._normalize_text(getattr(item, "evidence_label", None))
                if self._normalize_order_type(getattr(item, "order_type", None), amount, evidence_label) is None:
                    derived.append("ORDER_TYPE_MISSING")
                    break
            return derived

        return derived

    def _score_stage_invocation(
        self,
        *,
        parsed: BaseModel | None,
        stage_issues: list[str],
        partial_issue: str | None,
    ) -> tuple[int, int, int, int]:
        """여러 stage 호출 결과 중 더 나은 응답을 고르기 위한 점수다."""
        output_count = len(getattr(parsed, "items", [])) if parsed is not None else 0
        return (
            1 if parsed is None else 0,
            len(stage_issues) + (1 if partial_issue is not None else 0),
            1 if partial_issue is not None else 0,
            -output_count,
        )

    def _can_supersede_stage_retry_result(
        self,
        *,
        stage: StageDefinition,
        document_text: str,
        input_items: list[dict[str, Any]] | None,
        current_parsed: BaseModel | None,
        current_stage_issues: list[str],
        current_partial_issue: str | None,
        candidate_parsed: BaseModel | None,
    ) -> bool:
        """재호출 응답이 기존 parsed 결과를 안전하게 대체할 수 있는지 판단한다.

        원칙은 보수적이다.
        - partial 해소처럼 coverage가 늘어난 경우는 허용한다.
        - 같은 item에 대한 기존 non-empty 값을 뒤집는 응답은 허용하지 않는다.
        - 같은 content를 더 깨끗한 issue 상태로 돌려준 경우만 메타 정보 개선으로 본다.

        이렇게 해야 "issue를 없앤 hallucination"이 기존 응답을 덮어쓰는 경로를 막을 수 있다.
        """
        if current_parsed is None:
            return True
        if candidate_parsed is None:
            return False
        if stage.name == "t_day":
            current_parsed = self._normalize_stage_retry_parsed(
                stage_name=stage.name,
                document_text=document_text,
                input_items=input_items or [],
                parsed=current_parsed,
            )
            candidate_parsed = self._normalize_stage_retry_parsed(
                stage_name=stage.name,
                document_text=document_text,
                input_items=input_items or [],
                parsed=candidate_parsed,
            )

        current_items = self._stage_items_by_signature(stage.name, getattr(current_parsed, "items", []))
        candidate_items = self._stage_items_by_signature(stage.name, getattr(candidate_parsed, "items", []))
        current_signatures = set(current_items)
        candidate_signatures = set(candidate_items)
        allowed_families = self._stage_allowed_output_families(stage.name, input_items)
        current_families = {
            self._stage_partial_item_signature(stage.name, item)
            for item in current_items.values()
        }
        candidate_families = {
            self._stage_partial_item_signature(stage.name, item)
            for item in candidate_items.values()
        }

        # fund_inventory는 입력 batch identity가 없어서 extra seed를 문서 근거로 직접 검증한다.
        # retry가 새 seed를 추가하더라도, 그 seed의 fund_code/fund_name이 현재 chunk 안에
        # 실제로 보이는 경우에만 legitimate recovery로 허용한다.
        if allowed_families is None and candidate_signatures > current_signatures:
            added_signatures = candidate_signatures - current_signatures
            if not all(
                self._fund_seed_has_document_evidence(document_text, candidate_items[signature])
                for signature in added_signatures
            ):
                return False

        if allowed_families is not None and not candidate_families.issubset(allowed_families):
            return False

        if not current_families.issubset(candidate_families):
            return False

        if stage.name == "t_day":
            return self._can_supersede_t_day_retry_result(
                document_text=document_text,
                current_items=current_items,
                candidate_items=candidate_items,
                current_stage_issues=current_stage_issues,
                current_partial_issue=current_partial_issue,
            )

        if not current_signatures.issubset(candidate_signatures):
            return False

        shared_has_improvement = False
        for signature in current_signatures:
            comparison = self._classify_stage_retry_item_delta(
                current_item=current_items[signature],
                candidate_item=candidate_items[signature],
            )
            if comparison == "conflict":
                return False
            if comparison == "improved":
                shared_has_improvement = True

        # shared content가 동일하고 missing signature만 복구했다면 안전한 개선이다.
        if candidate_signatures > current_signatures:
            return True

        # 같은 slot/signature에서 비어 있던 필드만 채워진 경우도
        # legitimate recovery로 본다.
        if shared_has_improvement:
            return True

        # same-content 응답이라도 기존 결과가 아직 incomplete 하면
        # 단순 "issues 제거"는 honest warning을 숨길 수 있으므로 허용하지 않는다.
        if not self._stage_items_are_complete(stage.name, current_items.values()):
            return False

        # content는 동일하고 기존 결과도 complete 한 경우에만
        # partial/issue가 줄어든 cleaner response를 채택할 수 있다.
        return bool(current_stage_issues or current_partial_issue is not None)

    def _fund_seed_has_document_evidence(self, document_text: str, item: dict[str, Any]) -> bool:
        """Stage 1 retry가 추가한 seed가 현재 chunk 안에 실제 근거를 갖는지 본다."""
        text = document_text or ""
        fund_code = self._normalize_text(item.get("fund_code"))
        fund_name = self._normalize_text(item.get("fund_name"))
        if fund_code and self._contains_loose_document_term(text, fund_code):
            return True
        if fund_name and self._contains_loose_document_term(text, fund_name):
            return True
        return False

    @staticmethod
    def _fund_seed_signature(item: Any) -> tuple[str, str]:
        """fund seed item을 `(fund_code, fund_name)` 시그니처로 정규화한다."""
        if hasattr(item, "fund_code") or hasattr(item, "fund_name"):
            return FundOrderExtractor._canonical_fund_identity(
                getattr(item, "fund_code", None),
                getattr(item, "fund_name", None),
            )
        if isinstance(item, dict):
            return FundOrderExtractor._canonical_fund_identity(
                item.get("fund_code"),
                item.get("fund_name"),
            )
        return "", ""

    def _derive_document_fund_seed_signatures(
        self,
        document_text: str,
        *,
        target_fund_scope: TargetFundScope | None = None,
    ) -> set[tuple[str, str]]:
        """Structured markdown에서 문서가 실제로 보여주는 펀드 seed 집합을 보수적으로 읽는다."""
        markdown_view = self._structured_markdown_view(document_text)
        signatures: set[tuple[str, str]] = set()

        for table_lines in self._iter_markdown_table_blocks(markdown_view):
            rows = [self._markdown_table_cells(line) for line in table_lines]
            if len(rows) < 3:
                continue
            header = rows[0]
            if self._is_markdown_separator_row(header):
                continue

            fund_code_index = self._markdown_fund_code_index(header, body_rows=rows[1:])
            fund_name_index = self._markdown_fund_name_index(header)
            if fund_code_index is None or fund_name_index is None:
                continue
            has_amount_header = any(self._looks_like_document_amount_label(label) for label in header)

            for row in rows[1:]:
                if self._is_markdown_separator_row(row):
                    continue
                if fund_code_index >= len(row) or fund_name_index >= len(row):
                    continue
                fund_code = self._normalize_text(row[fund_code_index])
                fund_name = self._normalize_text(row[fund_name_index])
                if not fund_code or not fund_name or self._looks_like_total_row(fund_code, fund_name):
                    continue
                if fund_code_index == fund_name_index and not self._looks_like_fund_code(fund_code):
                    continue
                if target_fund_scope is not None and not self._is_target_fund(
                    fund_code=fund_code,
                    fund_name=fund_name,
                    target_fund_scope=target_fund_scope,
                ):
                    continue
                if has_amount_header and not self._row_has_non_zero_document_amount(header=header, row=row):
                    continue
                signatures.add(self._canonical_fund_identity(fund_code, fund_name))

        return signatures

    @staticmethod
    def _contains_loose_document_term(document_text: str, term: str) -> bool:
        """문서 안에 target term이 독립 근거로 보이는지 느슨하게 확인한다."""
        normalized_term = normalize_fund_name_key(term)
        if not normalized_term:
            return False

        pieces = re.findall(r"[0-9A-Za-z가-힣]+", term)
        if not pieces:
            return False
        escaped_pieces = [re.escape(piece) for piece in pieces]
        boundary = r"(?<![0-9A-Za-z가-힣])"
        separator = r"[^0-9A-Za-z가-힣]*"
        pattern = boundary + separator.join(escaped_pieces) + r"(?![0-9A-Za-z가-힣])"
        return re.search(pattern, document_text, flags=re.IGNORECASE) is not None

    def _reconcile_stage_retry_invocation_output(
        self,
        *,
        stage_name: str,
        document_text: str,
        input_items: list[dict[str, Any]],
        invocation: "_StageInvocation",
    ) -> "_StageInvocation":
        """Retry 판단 전에 stage output을 stage-specific invariant에 맞게 정리한다."""
        if invocation.parsed is None:
            return invocation

        if stage_name == "t_day":
            parsed = self._normalize_stage_retry_parsed(
                stage_name=stage_name,
                document_text=document_text,
                input_items=input_items,
                parsed=invocation.parsed,
            )
        elif stage_name == "transfer_amount":
            parsed = invocation.parsed.model_copy(
                update={
                    "items": self._post_validate_transfer_amount_items(
                        list(getattr(invocation.parsed, "items", []))
                    )
                }
            )
        else:
            return invocation
        return _StageInvocation(parsed=parsed, raw_response=invocation.raw_response)

    def _merge_focused_stage_retry_invocation_output(
        self,
        *,
        stage_name: str,
        document_text: str,
        input_items: list[dict[str, Any]],
        current_parsed: BaseModel | None,
        invocation: "_StageInvocation",
        retry_context: dict[str, Any] | None,
    ) -> "_StageInvocation":
        """Focused retry가 subset만 반환해도 현재 best 위에 안전하게 overlay 한다."""
        if (
            invocation.parsed is None
            or current_parsed is None
            or retry_context is None
            or stage_name != "t_day"
            or not list(getattr(invocation.parsed, "items", []))
        ):
            return invocation

        current_parsed = self._normalize_stage_retry_parsed(
            stage_name=stage_name,
            document_text=document_text,
            input_items=input_items,
            parsed=current_parsed,
        )
        merged_items = self._merge_t_day_retry_items(
            current_items=list(getattr(current_parsed, "items", [])),
            candidate_items=list(getattr(invocation.parsed, "items", [])),
        )
        merged_items, forced_issues = self._reconcile_t_day_stage_output(
            document_text=document_text,
            input_items=input_items,
            output_items=merged_items,
        )
        merged_issues = self._unique_preserve_order(
            [self._normalize_text(issue) for issue in getattr(invocation.parsed, "issues", []) if self._normalize_text(issue)]
            + forced_issues
        )
        parsed = invocation.parsed.model_copy(
            update={
                "items": merged_items,
                "issues": merged_issues,
            }
        )
        return _StageInvocation(parsed=parsed, raw_response=invocation.raw_response)

    def _normalize_stage_retry_parsed(
        self,
        *,
        stage_name: str,
        document_text: str,
        input_items: list[dict[str, Any]],
        parsed: BaseModel | None,
    ) -> BaseModel | None:
        """Retry 판단/구성에 쓰는 parsed payload를 stage invariant에 맞게 정규화한다."""
        if parsed is None:
            return parsed
        if stage_name == "transfer_amount":
            return parsed.model_copy(
                update={
                    "items": self._post_validate_transfer_amount_items(
                        list(getattr(parsed, "items", []))
                    )
                }
            )
        if stage_name != "t_day":
            return parsed

        normalized_items, forced_issues = self._normalize_stage_retry_previous_output_items_with_issues(
            stage_name=stage_name,
            document_text=document_text,
            input_items=input_items,
            previous_output_items=[
                item.model_dump(mode="json") if hasattr(item, "model_dump") else item
                for item in getattr(parsed, "items", [])
            ],
        )
        normalized_issues = self._unique_preserve_order(
            [self._normalize_text(issue) for issue in getattr(parsed, "issues", []) if self._normalize_text(issue)]
            + forced_issues
        )
        return parsed.model_copy(
            update={
                "items": [FundSlotItem.model_validate(item) for item in normalized_items],
                "issues": normalized_issues,
            }
        )

    def _normalize_stage_retry_previous_output_items(
        self,
        *,
        stage_name: str,
        document_text: str,
        input_items: list[dict[str, Any]],
        previous_output_items: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Retry prompt용 previous output을 stage-specific invariant에 맞게 다시 정리한다."""
        normalized_items, _ = self._normalize_stage_retry_previous_output_items_with_issues(
            stage_name=stage_name,
            document_text=document_text,
            input_items=input_items,
            previous_output_items=previous_output_items,
        )
        return normalized_items

    def _normalize_stage_retry_context_previous_output_items(
        self,
        *,
        stage_name: str,
        document_text: str,
        input_items: list[dict[str, Any]],
        previous_output_items: list[dict[str, Any]],
        focus_items: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Retry prompt용 previous output을 focus repair scope까지 반영해 정리한다."""
        normalized_items, _ = self._normalize_stage_retry_previous_output_items_with_issues(
            stage_name=stage_name,
            document_text=document_text,
            input_items=input_items,
            previous_output_items=previous_output_items,
        )
        if stage_name != "t_day" or not normalized_items or not focus_items:
            return normalized_items

        current_items = [FundSlotItem.model_validate(item) for item in normalized_items]
        candidate_items: list[FundSlotItem] = []
        expected_slot_ids_by_family: dict[tuple[Any, ...], set[str]] = {}

        for item in focus_items:
            family_key = self._stage_partial_item_signature("t_day", item)
            expected_slot_ids = {
                self._normalize_text(slot_id)
                for slot_id in list(item.get("retry_expected_family_slots") or [])
                if self._normalize_text(slot_id)
            }
            if expected_slot_ids:
                expected_slot_ids_by_family.setdefault(family_key, set()).update(expected_slot_ids)

            slot_id = self._normalize_text(item.get("slot_id"))
            t_day = item.get("t_day")
            if not slot_id or not isinstance(t_day, int):
                continue
            candidate_items.append(FundSlotItem.model_validate(item))

        if candidate_items:
            current_items = self._merge_t_day_retry_items(
                current_items=current_items,
                candidate_items=candidate_items,
            )

        if expected_slot_ids_by_family:
            filtered_items: list[FundSlotItem] = []
            for item in current_items:
                item_dict = item.model_dump(mode="json")
                family_key = self._stage_partial_item_signature("t_day", item_dict)
                expected_slot_ids = expected_slot_ids_by_family.get(family_key)
                if expected_slot_ids is not None and self._normalize_text(item_dict.get("slot_id")) not in expected_slot_ids:
                    continue
                filtered_items.append(item)
            current_items = filtered_items

        reconciled_items, _ = self._reconcile_t_day_stage_output(
            document_text=document_text,
            input_items=input_items,
            output_items=current_items,
        )
        return self._dedupe_retry_focus_items([item.model_dump(mode="json") for item in reconciled_items])

    def _normalize_stage_retry_previous_output_items_with_issues(
        self,
        *,
        stage_name: str,
        document_text: str,
        input_items: list[dict[str, Any]],
        previous_output_items: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """Retry prompt로 다시 넣기 전 previous output을 stage별 계약으로 정규화한다."""
        normalized_items = [dict(item) for item in previous_output_items]
        if not normalized_items:
            return normalized_items, []
        if stage_name == "transfer_amount":
            hydrated_items = [FundAmountItem.model_validate(item) for item in normalized_items]
            post_validated_items = self._post_validate_transfer_amount_items(hydrated_items)
            return (
                self._dedupe_retry_focus_items([item.model_dump(mode="json") for item in post_validated_items]),
                [],
            )
        if stage_name != "t_day":
            return self._dedupe_retry_focus_items(normalized_items), []

        hydrated_items = [FundSlotItem.model_validate(item) for item in normalized_items]
        reconciled_items, forced_issues = self._reconcile_t_day_stage_output(
            document_text=document_text,
            input_items=input_items,
            output_items=hydrated_items,
        )
        return (
            self._dedupe_retry_focus_items([item.model_dump(mode="json") for item in reconciled_items]),
            forced_issues,
        )

    def _reconcile_t_day_stage_output(
        self,
        *,
        document_text: str,
        input_items: list[dict[str, Any]],
        output_items: list[FundSlotItem],
    ) -> tuple[list[FundSlotItem], list[str]]:
        """같은 logical evidence cell을 가리키는 stale t_day sibling을 하나로 정리한다."""
        if not output_items:
            return output_items, []

        _ = input_items
        grouped_indexes: dict[tuple[tuple[Any, ...], str], list[int]] = {}
        for index, item in enumerate(output_items):
            item_dict = item.model_dump(mode="json")
            evidence_label = self._normalize_text(item_dict.get("evidence_label"))
            if not evidence_label:
                continue
            family_key = self._stage_partial_item_signature("t_day", item_dict)
            grouped_indexes.setdefault((family_key, evidence_label), []).append(index)

        winner_by_group: dict[tuple[tuple[Any, ...], str], int] = {}
        forced_issues: list[str] = []
        removed_count = 0
        ambiguous_groups = 0
        for group_key, indexes in grouped_indexes.items():
            signatures = {
                self._stage_item_signature("t_day", output_items[index].model_dump(mode="json"))
                for index in indexes
            }
            if len(signatures) <= 1:
                continue

            scored_indexes = [
                (
                    self._t_day_retry_evidence_item_score(
                        document_text,
                        output_items[index].model_dump(mode="json"),
                    ),
                    index,
                )
                for index in indexes
            ]
            best_score = max(score for score, _ in scored_indexes)
            winner_candidates = [index for score, index in scored_indexes if score == best_score]
            winner_by_group[group_key] = winner_candidates[0]
            removed_count += len(indexes) - 1
            if len(winner_candidates) > 1:
                ambiguous_groups += 1
                self._append_unique(forced_issues, "T_DAY_STAGE_PARTIAL")

        if not winner_by_group:
            return self._dedupe_stage_items(output_items), forced_issues

        reconciled: list[FundSlotItem] = []
        emitted_groups: set[tuple[tuple[Any, ...], str]] = set()
        for index, item in enumerate(output_items):
            item_dict = item.model_dump(mode="json")
            evidence_label = self._normalize_text(item_dict.get("evidence_label"))
            if not evidence_label:
                reconciled.append(item)
                continue
            group_key = (self._stage_partial_item_signature("t_day", item_dict), evidence_label)
            winner_index = winner_by_group.get(group_key)
            if winner_index is None:
                reconciled.append(item)
                continue
            if group_key in emitted_groups:
                continue
            reconciled.append(output_items[winner_index])
            emitted_groups.add(group_key)

        logger.info(
            "Reconciled %s conflicting t_day evidence group(s); removed %s stale item(s)%s",
            len(winner_by_group),
            removed_count,
            f"; ambiguous_groups={ambiguous_groups}" if ambiguous_groups else "",
        )
        return self._dedupe_stage_items(reconciled), forced_issues

    def _merge_t_day_retry_items(
        self,
        *,
        current_items: list[FundSlotItem],
        candidate_items: list[FundSlotItem],
    ) -> list[FundSlotItem]:
        """Retry subset를 기존 t_day family 위에 overlay 해 stale evidence를 교체한다."""
        if not current_items:
            return candidate_items
        if not candidate_items:
            return current_items

        merged_by_family: dict[tuple[Any, ...], list[FundSlotItem]] = {}
        candidate_families: set[tuple[Any, ...]] = set()
        current_families: dict[tuple[Any, ...], list[FundSlotItem]] = {}
        for item in current_items:
            family_key = self._stage_partial_item_signature("t_day", item.model_dump(mode="json"))
            current_families.setdefault(family_key, []).append(item)

        for family_key, family_items in current_families.items():
            merged_by_family[family_key] = list(family_items)

        for candidate_item in candidate_items:
            candidate_dict = candidate_item.model_dump(mode="json")
            family_key = self._stage_partial_item_signature("t_day", candidate_dict)
            candidate_families.add(family_key)
            family_items = merged_by_family.setdefault(family_key, [])
            family_items = [
                existing_item
                for existing_item in family_items
                if self._stage_items_are_complete("t_day", [existing_item.model_dump(mode="json")])
            ]

            candidate_signature = self._stage_item_signature("t_day", candidate_dict)
            candidate_label = self._normalize_text(candidate_dict.get("evidence_label"))
            family_items = [
                existing_item
                for existing_item in family_items
                if (
                    self._stage_item_signature("t_day", existing_item.model_dump(mode="json")) != candidate_signature
                    and (
                        not candidate_label
                        or self._normalize_text(existing_item.evidence_label) != candidate_label
                    )
                )
            ]
            family_items.append(candidate_item)
            merged_by_family[family_key] = family_items

        merged: list[FundSlotItem] = []
        emitted_families: set[tuple[Any, ...]] = set()
        for item in current_items:
            family_key = self._stage_partial_item_signature("t_day", item.model_dump(mode="json"))
            if family_key not in candidate_families:
                merged.append(item)
                continue
            if family_key in emitted_families:
                continue
            merged.extend(merged_by_family.get(family_key, []))
            emitted_families.add(family_key)

        for family_key in candidate_families - emitted_families:
            merged.extend(merged_by_family.get(family_key, []))

        return self._dedupe_stage_items(merged)

    def _t_day_retry_evidence_item_score(
        self,
        document_text: str,
        item: dict[str, Any],
    ) -> tuple[int, int, int, int]:
        """같은 evidence cell 안에서 더 신뢰할 수 있는 t_day item을 고른다."""
        evidence_label = self._normalize_text(item.get("evidence_label"))
        t_day = item.get("t_day")
        explicit_t_day = (
            self._explicit_t_day_from_evidence_label(
                evidence_label,
                self._normalize_date(item.get("base_date")),
            )
            if evidence_label
            else None
        )
        explicit_match = int(explicit_t_day is not None and isinstance(t_day, int) and t_day == explicit_t_day)
        has_document_evidence = int(self._t_day_item_has_document_evidence(document_text, item))
        slot_is_consistent = int(self._t_day_slot_id_matches_t_day(item))
        populated_fields = self._t_day_item_populated_field_count(item)
        return (explicit_match, has_document_evidence, slot_is_consistent, populated_fields)

    def _t_day_slot_id_matches_t_day(self, item: dict[str, Any]) -> bool:
        """slot_id가 현재 t_day/slot kind와 자기 일관성을 가지는지 본다."""
        t_day = item.get("t_day")
        if not isinstance(t_day, int):
            return False
        slot_id = self._normalize_text(item.get("slot_id"))
        if not slot_id:
            return False
        slot_kind = self._actual_t_day_slot_kind(item)
        return slot_id == self._deterministic_t_day_slot_id(t_day=t_day, slot_kind=slot_kind)

    def _t_day_item_populated_field_count(self, item: dict[str, Any]) -> int:
        """동점 해소용으로 t_day item의 populated field 수를 센다."""
        populated = 0
        if self._normalize_text(item.get("fund_code")):
            populated += 1
        if self._normalize_text(item.get("fund_name")):
            populated += 1
        if self._normalize_date(item.get("base_date")) is not None:
            populated += 1
        if isinstance(item.get("t_day"), int):
            populated += 1
        if self._normalize_text(item.get("slot_id")):
            populated += 1
        if self._normalize_text(item.get("evidence_label")):
            populated += 1
        return populated

    def _candidate_supersedes_current_t_day_evidence(
        self,
        *,
        document_text: str,
        current_item: dict[str, Any],
        candidate_item: dict[str, Any],
    ) -> bool:
        """같은 evidence cell의 corrected retry item이 stale current item을 덮을 수 있는지 본다."""
        current_label = self._normalize_text(current_item.get("evidence_label"))
        candidate_label = self._normalize_text(candidate_item.get("evidence_label"))
        if not current_label or current_label != candidate_label:
            return False
        if self._stage_partial_item_signature("t_day", current_item) != self._stage_partial_item_signature(
            "t_day",
            candidate_item,
        ):
            return False
        if self._stage_item_signature("t_day", current_item) == self._stage_item_signature("t_day", candidate_item):
            return False

        current_score = self._t_day_retry_evidence_item_score(document_text, current_item)
        candidate_score = self._t_day_retry_evidence_item_score(document_text, candidate_item)
        if candidate_score <= current_score:
            return False
        return any(candidate_score[:3])

    def _can_supersede_t_day_retry_result(
        self,
        *,
        document_text: str,
        current_items: dict[tuple[Any, ...], dict[str, Any]],
        candidate_items: dict[tuple[Any, ...], dict[str, Any]],
        current_stage_issues: list[str],
        current_partial_issue: str | None,
    ) -> bool:
        """Stage 3은 같은 fund/base_date family에서 slot이 분화될 수 있어 별도 비교가 필요하다."""
        if self._t_day_output_has_duplicate_evidence(list(candidate_items.values())):
            return False

        current_families = self._group_stage_items_by_family("t_day", current_items.values())
        candidate_families = self._group_stage_items_by_family("t_day", candidate_items.values())
        expected_family_slots = self._derive_expected_t_day_family_slots(
            document_text,
            list(current_items.values()) + list(candidate_items.values()),
        )
        current_actual_slots = self._derive_actual_t_day_family_slots(list(current_items.values()))

        shared_has_improvement = False
        for family_key, current_family_items in current_families.items():
            candidate_family_items = candidate_families.get(family_key, [])
            if not candidate_family_items:
                return False

            current_family_complete = self._stage_items_are_complete("t_day", current_family_items)
            required_family_slots = expected_family_slots.get(family_key, set())
            if required_family_slots and not required_family_slots.issubset(current_actual_slots.get(family_key, set())):
                current_family_complete = False
            if current_family_complete:
                current_family_items_by_signature = {
                    self._stage_item_signature("t_day", item): item
                    for item in current_family_items
                }
                candidate_family_items_by_signature = {
                    self._stage_item_signature("t_day", item): item
                    for item in candidate_family_items
                }
                if set(current_family_items_by_signature) != set(candidate_family_items_by_signature):
                    return False
                for signature, current_item in current_family_items_by_signature.items():
                    # complete t_day family에서는 persisted identity/signature가 같다면
                    # evidence_label 흔들림만으로 cleaner retry를 버릴 필요는 없다.
                    # evidence_label은 최종 저장 필드가 아니고, 같은 slot/t_day가 유지되면
                    # 최종 order는 동일하다.
                    current_item_for_compare = dict(current_item)
                    candidate_item_for_compare = dict(candidate_family_items_by_signature[signature])
                    current_item_for_compare["evidence_label"] = ""
                    candidate_item_for_compare["evidence_label"] = ""
                    comparison = self._classify_stage_retry_item_delta(
                        current_item=current_item_for_compare,
                        candidate_item=candidate_item_for_compare,
                    )
                    if comparison == "conflict":
                        return False
                continue

            current_family_signatures = {
                self._stage_item_signature("t_day", item)
                for item in current_family_items
            }
            for candidate_item in candidate_family_items:
                candidate_signature = self._stage_item_signature("t_day", candidate_item)
                if candidate_signature in current_family_signatures:
                    continue
                if not self._t_day_item_has_document_evidence(document_text, candidate_item):
                    return False

            for current_item in current_family_items:
                if not any(
                    self._classify_stage_retry_item_delta(
                        current_item=current_item,
                        candidate_item=candidate_item,
                    ) in {"same", "improved"}
                    or self._candidate_supersedes_current_t_day_evidence(
                        document_text=document_text,
                        current_item=current_item,
                        candidate_item=candidate_item,
                    )
                    for candidate_item in candidate_family_items
                ):
                    return False
            shared_has_improvement = True

        if shared_has_improvement:
            return True
        return bool(current_stage_issues or current_partial_issue is not None)

    def _t_day_item_has_document_evidence(self, document_text: str, item: dict[str, Any]) -> bool:
        """Stage 3 retry가 추가한 slot이 문서 evidence와 의미적으로 맞는지 본다."""
        evidence_label = self._normalize_text(item.get("evidence_label"))
        if not evidence_label or not self._contains_loose_document_term(document_text, evidence_label):
            return False

        t_day = item.get("t_day")
        if not isinstance(t_day, int) or t_day < 0:
            return False

        explicit_t_day = self._explicit_t_day_from_evidence_label(
            evidence_label,
            self._normalize_date(item.get("base_date")),
        )
        if explicit_t_day is not None:
            return t_day == explicit_t_day

        if self._evidence_implies_schedule(evidence_label):
            return t_day > 0

        return t_day == 0

    def _derive_expected_t_day_family_slots(
        self,
        document_text: str,
        input_items: list[dict[str, Any]],
    ) -> dict[tuple[Any, ...], set[tuple[int, str]]]:
        """Structured markdown가 요구하는 t_day family별 slot 집합을 보수적으로 계산한다."""
        markdown_view = self._structured_markdown_view(document_text)
        family_base_dates = {
            self._stage_partial_item_signature("t_day", item): self._normalize_date(item.get("base_date"))
            for item in input_items
            if self._normalize_text(item.get("fund_code"))
        }
        family_lookup: dict[tuple[str, str], list[tuple[Any, ...]]] = {}
        for family_key in family_base_dates:
            family_lookup.setdefault((family_key[0], family_key[1]), []).append(family_key)

        expected_slots: dict[tuple[Any, ...], set[tuple[int, str]]] = {}
        for table_lines in self._iter_markdown_table_blocks(markdown_view):
            rows = [self._markdown_table_cells(line) for line in table_lines]
            if len(rows) < 3:
                continue
            header = rows[0]
            if self._is_markdown_separator_row(header):
                continue

            fund_code_index = self._markdown_fund_code_index(header, body_rows=rows[1:])
            fund_name_index = self._markdown_fund_name_index(header)
            if fund_code_index is None:
                continue

            column_specs_by_base_date: dict[str, list[tuple[int, SettleClass, int, OrderType | None, bool, bool]]] = {}
            for row in rows[1:]:
                if self._is_markdown_separator_row(row):
                    continue
                if fund_code_index >= len(row):
                    continue
                fund_code = self._normalize_text(row[fund_code_index])
                fund_name = (
                    self._normalize_text(row[fund_name_index])
                    if fund_name_index is not None and fund_name_index < len(row)
                    else "-"
                )
                fund_name = fund_name or "-"
                if not fund_code or self._looks_like_total_row(fund_code, fund_name):
                    continue
                matching_families = family_lookup.get(
                    self._canonical_fund_identity(fund_code, fund_name),
                    [],
                )
                if not matching_families:
                    continue
                for family_key in matching_families:
                    base_date = family_base_dates.get(family_key)
                    if base_date is None:
                        continue
                    column_specs = column_specs_by_base_date.setdefault(
                        base_date,
                        self._deterministic_order_columns(header, base_date),
                    )
                    for (
                        column_index,
                        _,
                        t_day,
                        explicit_order_type,
                        uses_signed_amount,
                        uses_row_context_order_type,
                    ) in column_specs:
                        if column_index >= len(row):
                            continue
                        amount = self._normalize_amount_for_deterministic(row[column_index])
                        if amount is None or self._is_effectively_zero_amount(amount):
                            continue
                        label = self._normalize_text(header[column_index])
                        if self._is_pending_request_label_for_deterministic(label):
                            pending_t_day = self._pending_request_t_day_from_row_for_deterministic(
                                label=label,
                                row=row,
                                header=header,
                                column_index=column_index,
                                base_date=base_date,
                            )
                            if pending_t_day is None:
                                continue
                            t_day = pending_t_day
                        slot_order_type = explicit_order_type
                        if uses_row_context_order_type:
                            slot_order_type = self._row_context_order_type_from_markdown_row(
                                header=header,
                                row=row,
                            )
                        slot_kind = self._expected_t_day_slot_kind(slot_order_type, uses_signed_amount)
                        if slot_kind is None:
                            continue
                        expected_slots.setdefault(family_key, set()).add((t_day, slot_kind))

        return expected_slots

    def _build_deterministic_t_day_items(
        self,
        *,
        document_text: str,
        input_items: list[dict[str, Any]],
    ) -> list[FundSlotItem]:
        """Structured markdown에서 stage 3용 slot을 직접 읽어 보강용 후보를 만든다.

        explicit direction column 구조를 읽을 수 있을 때 surviving direction slot만
        deterministic 하게 만든다. 예를 들어 `설정금액=0, 해지금액>0`이면 `T0_RED`만 만들고,
        zero side에 대한 ghost slot은 만들지 않는다.
        """
        markdown_view = self._structured_markdown_view(document_text)
        family_base_dates = {
            self._stage_partial_item_signature("t_day", item): self._normalize_date(item.get("base_date"))
            for item in input_items
            if self._normalize_text(item.get("fund_code"))
        }
        family_lookup: dict[tuple[str, str], list[tuple[Any, ...]]] = {}
        for family_key in family_base_dates:
            family_lookup.setdefault((family_key[0], family_key[1]), []).append(family_key)

        results: dict[tuple[Any, ...], FundSlotItem] = {}

        for table_lines in self._iter_markdown_table_blocks(markdown_view):
            rows = [self._markdown_table_cells(line) for line in table_lines]
            if len(rows) < 3:
                continue
            header = rows[0]
            if self._is_markdown_separator_row(header):
                continue

            fund_code_index = self._markdown_fund_code_index(header, body_rows=rows[1:])
            fund_name_index = self._markdown_fund_name_index(header)
            if fund_code_index is None:
                continue

            column_specs_by_base_date: dict[str, list[tuple[int, SettleClass, int, OrderType | None, bool, bool]]] = {}
            for row in rows[1:]:
                if self._is_markdown_separator_row(row):
                    continue
                if fund_code_index >= len(row):
                    continue

                fund_code = self._normalize_text(row[fund_code_index])
                fund_name = (
                    self._normalize_text(row[fund_name_index])
                    if fund_name_index is not None and fund_name_index < len(row)
                    else "-"
                )
                fund_name = fund_name or "-"
                if not fund_code or self._looks_like_total_row(fund_code, fund_name):
                    continue

                matching_families = family_lookup.get(
                    self._canonical_fund_identity(fund_code, fund_name),
                    [],
                )
                if not matching_families:
                    continue

                for family_key in matching_families:
                    base_date = family_base_dates.get(family_key)
                    if base_date is None:
                        continue
                    column_specs = column_specs_by_base_date.setdefault(
                        base_date,
                        self._deterministic_order_columns(header, base_date),
                    )
                    for (
                        column_index,
                        _,
                        t_day,
                        explicit_order_type,
                        uses_signed_amount,
                        uses_row_context_order_type,
                    ) in column_specs:
                        if column_index >= len(row):
                            continue
                        amount = self._normalize_amount_for_deterministic(row[column_index])
                        if amount is None or self._is_effectively_zero_amount(amount):
                            continue
                        label = self._normalize_text(header[column_index])
                        if self._is_pending_request_label_for_deterministic(label):
                            pending_t_day = self._pending_request_t_day_from_row_for_deterministic(
                                label=label,
                                row=row,
                                header=header,
                                column_index=column_index,
                                base_date=base_date,
                            )
                            if pending_t_day is None:
                                continue
                            t_day = pending_t_day
                        slot_order_type = explicit_order_type
                        if uses_row_context_order_type:
                            slot_order_type = self._row_context_order_type_from_markdown_row(
                                header=header,
                                row=row,
                            )
                        slot_kind = self._expected_t_day_slot_kind(slot_order_type, uses_signed_amount)
                        if slot_kind is None:
                            continue

                        evidence_label = label
                        slot_id = self._deterministic_t_day_slot_id(t_day=t_day, slot_kind=slot_kind)
                        signature = (fund_code, fund_name, base_date, t_day, slot_id)
                        results.setdefault(
                            signature,
                            FundSlotItem(
                                fund_code=fund_code,
                                fund_name=fund_name,
                                base_date=base_date,
                                t_day=t_day,
                                slot_id=slot_id,
                                evidence_label=evidence_label,
                                reason_code=self._deterministic_t_day_reason_code(
                                    t_day=t_day,
                                    slot_id=slot_id,
                                    evidence_label=evidence_label,
                                ),
                            ),
                        )

        return list(results.values())

    def _augment_t_day_items_from_document(
        self,
        *,
        document_text: str,
        input_items: list[dict[str, Any]],
        output_items: list[FundSlotItem],
    ) -> list[FundSlotItem]:
        """Stage 3 output이 surviving explicit slot을 놓쳤을 때 문서 근거로 보강한다.

        목적은 "빠진 실제 slot"을 채우는 것이지 stage 3 결과를 무조건 덮어쓰는 것이 아니다.
        기존 signature row는 유지하고, 문서 표가 요구하는 deterministic slot만 추가한다.
        """
        current = {
            self._stage_item_signature("t_day", item.model_dump(mode="json")): item
            for item in output_items
        }
        added_count = 0

        for item in self._build_deterministic_t_day_items(
            document_text=document_text,
            input_items=input_items,
        ):
            signature = self._stage_item_signature("t_day", item.model_dump(mode="json"))
            if signature in current:
                continue
            current[signature] = item
            added_count += 1

        if added_count:
            logger.info("Augmented %s t_day slot(s) from structured markdown evidence", added_count)
        return self._dedupe_stage_items(list(current.values()))

    def _replace_incomplete_t_day_items_with_deterministic_document_slots(
        self,
        *,
        document_text: str,
        input_items: list[dict[str, Any]],
        output_items: list[FundSlotItem],
    ) -> list[FundSlotItem]:
        """incomplete t_day output을 document-backed deterministic slot 집합으로 교정한다.

        목적은 broad retry가 남긴 ghost/duplicate slot을 없애는 것이다.
        다만 deterministic parser가 모든 family를 설명하지 못하면 안 되므로,
        아래 조건이 모두 맞을 때만 교체한다.
        - 현재 output은 document 기준 complete 하지 않다.
        - deterministic slot 집합은 document 기준 complete 하다.
        - deterministic family 집합이 input family 집합과 정확히 같다.
        - 현재 output family도 deterministic family 바깥으로 벗어나지 않는다.
        """
        if not output_items:
            return output_items
        if self._t_day_document_slots_are_complete(
            document_text=document_text,
            input_items=input_items,
            output_items=output_items,
        ):
            return output_items

        deterministic_items = self._build_deterministic_t_day_items(
            document_text=document_text,
            input_items=input_items,
        )
        if not deterministic_items:
            return output_items

        expected_family_slots = self._derive_expected_t_day_family_slots(document_text, input_items)
        deterministic_items_by_family = self._group_stage_items_by_family(
            "t_day",
            [item.model_dump(mode="json") for item in deterministic_items],
        )
        current_items_by_family = self._group_stage_items_by_family(
            "t_day",
            [item.model_dump(mode="json") for item in output_items],
        )
        deterministic_actual_slots = self._derive_actual_t_day_family_slots(
            [item.model_dump(mode="json") for item in deterministic_items]
        )
        current_actual_slots = self._derive_actual_t_day_family_slots(
            [item.model_dump(mode="json") for item in output_items]
        )
        if expected_family_slots:
            replaced_family_count = 0
            partially_replaced_items: list[FundSlotItem] = []
            emitted_families: set[tuple[Any, ...]] = set()
            for item in output_items:
                family_key = self._stage_partial_item_signature("t_day", item.model_dump(mode="json"))
                if family_key in emitted_families:
                    continue
                emitted_families.add(family_key)
                expected_slots = expected_family_slots.get(family_key)
                deterministic_family = deterministic_items_by_family.get(family_key, [])
                if (
                    expected_slots
                    and deterministic_family
                    and deterministic_actual_slots.get(family_key, set()) == expected_slots
                    and current_actual_slots.get(family_key, set()) != expected_slots
                ):
                    partially_replaced_items.extend(
                        FundSlotItem.model_validate(deterministic_item)
                        for deterministic_item in deterministic_family
                    )
                    replaced_family_count += 1
                    continue
                partially_replaced_items.extend(
                    FundSlotItem.model_validate(output_family_item)
                    for output_family_item in current_items_by_family.get(family_key, [])
                )

            if replaced_family_count:
                logger.info(
                    "Replaced %s incomplete t_day family/families with deterministic slot(s) from structured markdown evidence",
                    replaced_family_count,
                )
                output_items = self._dedupe_stage_items(partially_replaced_items)
                if self._t_day_document_slots_are_complete(
                    document_text=document_text,
                    input_items=input_items,
                    output_items=output_items,
                ):
                    return output_items

        if not self._t_day_document_slots_are_complete(
            document_text=document_text,
            input_items=input_items,
            output_items=deterministic_items,
        ):
            return output_items

        input_families = self._stage_allowed_output_families("t_day", input_items)
        deterministic_families = {
            self._stage_partial_item_signature("t_day", item.model_dump(mode="json"))
            for item in deterministic_items
        }
        current_families = {
            self._stage_partial_item_signature("t_day", item.model_dump(mode="json"))
            for item in output_items
        }
        if input_families is not None and deterministic_families != input_families:
            return output_items
        if not current_families.issubset(deterministic_families):
            return output_items

        logger.info(
            "Replaced incomplete t_day output with %s deterministic slot(s) from structured markdown evidence",
            len(deterministic_items),
        )
        return self._dedupe_stage_items(deterministic_items)

    def _build_deterministic_resolved_items_from_settle_items(
        self,
        input_items: list[FundSettleItem],
        *,
        document_text: str | None = None,
        raw_text: str | None = None,
        target_fund_scope: TargetFundScope | None = None,
    ) -> list[FundResolvedItem] | None:
        """settle items만으로 order_type을 모두 결정할 수 있으면 resolved items를 만든다."""
        document_backed_hints: dict[tuple[str, str, str | None, int | None, str, str], OrderType] = {}
        if document_text and raw_text:
            deterministic_orders = self._build_deterministic_markdown_orders(
                markdown_text=self._structured_markdown_view(document_text),
                raw_text=raw_text,
                target_fund_scope=target_fund_scope,
            )
            hinted_order_types: dict[tuple[str, str, str | None, int | None, str, str], set[OrderType]] = {}
            for order in deterministic_orders:
                amount = self._canonicalize_amount_text(order.transfer_amount)
                if amount is None:
                    continue
                key = (
                    self._normalize_text(order.fund_code),
                    self._normalize_text(order.fund_name),
                    self._normalize_date(order.base_date),
                    order.t_day,
                    self._normalize_text(order.settle_class.value),
                    amount.lstrip("+-"),
                )
                hinted_order_types.setdefault(key, set()).add(order.order_type)
            document_backed_hints = {
                key: next(iter(order_types))
                for key, order_types in hinted_order_types.items()
                if len(order_types) == 1
            }

        deterministic_items: list[FundResolvedItem] = []
        for item in input_items:
            amount = self._canonicalize_amount_text(item.transfer_amount)
            evidence_label = self._normalize_text(item.evidence_label)
            if amount is None:
                return None
            settle_class = self._normalize_settle_class(item.settle_class, item.t_day, evidence_label)
            if settle_class is None:
                return None
            order_type = self._normalize_order_type(None, amount, evidence_label)
            if order_type is None and document_backed_hints:
                key = (
                    self._normalize_text(item.fund_code),
                    self._normalize_text(item.fund_name),
                    self._normalize_date(item.base_date),
                    item.t_day,
                    self._normalize_text(settle_class.value),
                    amount.lstrip("+-"),
                )
                order_type = document_backed_hints.get(key)
            if order_type is None:
                return None
            deterministic_items.append(
                FundResolvedItem(
                    fund_code=item.fund_code,
                    fund_name=item.fund_name,
                    base_date=item.base_date,
                    t_day=item.t_day,
                    slot_id=item.slot_id,
                    evidence_label=item.evidence_label,
                    transfer_amount=item.transfer_amount,
                    settle_class=item.settle_class,
                    order_type=order_type.value,
                    reason_code=self._deterministic_order_type_reason_code(
                        transfer_amount=item.transfer_amount or "",
                        evidence_label=item.evidence_label or "",
                    ),
                )
            )

        deterministic_dump = [item.model_dump(mode="json") for item in deterministic_items]
        if not self._stage_items_are_complete("order_type", deterministic_dump):
            return None
        return self._dedupe_stage_items(deterministic_items)

    def _build_deterministic_settle_items_from_amount_items(
        self,
        input_items: list[FundAmountItem],
    ) -> list[FundSettleItem] | None:
        """amount items만으로 settle_class를 모두 결정할 수 있으면 settle items를 만든다."""
        deterministic_items: list[FundSettleItem] = []
        for item in input_items:
            amount = self._normalize_amount(item.transfer_amount)
            evidence_label = self._normalize_text(item.evidence_label)
            if amount is None:
                return None
            settle_class = self._normalize_settle_class(None, item.t_day, evidence_label)
            if settle_class is None:
                return None
            deterministic_items.append(
                FundSettleItem(
                    fund_code=item.fund_code,
                    fund_name=item.fund_name,
                    base_date=item.base_date,
                    t_day=item.t_day,
                    slot_id=item.slot_id,
                    evidence_label=item.evidence_label,
                    transfer_amount=item.transfer_amount,
                    settle_class=settle_class.value,
                    reason_code=self._deterministic_settle_reason_code(
                        t_day=item.t_day,
                        evidence_label=item.evidence_label or "",
                    ),
                )
            )

        deterministic_dump = [item.model_dump(mode="json") for item in deterministic_items]
        if not self._stage_items_are_complete("settle_class", deterministic_dump):
            return None
        return self._dedupe_stage_items(deterministic_items)

    def _replace_incomplete_order_type_items_with_deterministic_values(
        self,
        *,
        input_items: list[FundSettleItem],
        output_items: list[FundResolvedItem],
    ) -> list[FundResolvedItem]:
        """Stage 7 output이 incomplete하면 input settle items에서 direction을 복구한다.

        evidence_label 또는 signed amount만으로 방향을 안전하게 결정할 수 있는 문서는
        Stage 7 LLM variance보다 settle item 기반 deterministic 보정이 더 안정적이다.
        """
        normalized_output_items = [item.model_dump(mode="json") for item in output_items]
        if normalized_output_items and self._stage_items_are_complete("order_type", normalized_output_items):
            return output_items

        deterministic_items = self._build_deterministic_resolved_items_from_settle_items(input_items)
        if deterministic_items is None:
            return output_items
        deterministic_dump = [item.model_dump(mode="json") for item in deterministic_items]

        input_dump = [item.model_dump(mode="json") for item in input_items]
        input_signatures = {
            self._stage_partial_item_signature("order_type", item)
            for item in input_dump
        }
        deterministic_signatures = {
            self._stage_partial_item_signature("order_type", item)
            for item in deterministic_dump
        }
        current_signatures = {
            self._stage_partial_item_signature("order_type", item)
            for item in normalized_output_items
        }
        if deterministic_signatures != input_signatures:
            return output_items
        if current_signatures and not current_signatures.issubset(input_signatures):
            return output_items

        logger.info(
            "Replaced incomplete order_type output with %s deterministic value(s) from settle items",
            len(deterministic_items),
        )
        return self._dedupe_stage_items(deterministic_items)

    def _t_day_document_slots_are_complete(
        self,
        *,
        document_text: str,
        input_items: list[dict[str, Any]],
        output_items: list[FundSlotItem],
    ) -> bool:
        """문서 표가 요구하는 t_day slot 집합이 현재 output과 정확히 일치하는지 본다.

        stale `T_DAY_MISSING` / `T_DAY_STAGE_PARTIAL` 제거 전에 쓰는 검사이므로,
        expected slot이 모두 있다는 이유만으로는 충분하지 않다. ghost slot 없이
        actual slot 집합이 expected slot 집합과 같아야만 complete 로 본다.
        """
        normalized_output_items = [item.model_dump(mode="json") for item in output_items]
        stage = StageDefinition(
            number=0,
            name="t_day",
            goal="",
            instructions="",
            output_contract="",
        )
        if (
            self._stage_partial_issue_code(
                stage=stage,
                document_text=document_text,
                input_items=input_items,
                output_items=normalized_output_items,
            )
            is not None
        ):
            return False

        expected_items = self._build_deterministic_t_day_items(
            document_text=document_text,
            input_items=input_items,
        )
        if not expected_items:
            return False
        expected_signatures = {
            self._stage_item_signature("t_day", item.model_dump(mode="json"))
            for item in expected_items
        }
        actual_signatures = {
            self._stage_item_signature("t_day", item)
            for item in normalized_output_items
        }
        return actual_signatures == expected_signatures

    def _derive_actual_t_day_family_slots(
        self,
        output_items: list[dict[str, Any]],
    ) -> dict[tuple[Any, ...], set[tuple[int, str]]]:
        """Stage 3 output에서 family별 실제 slot 집합을 계산한다."""
        actual_slots: dict[tuple[Any, ...], set[tuple[int, str]]] = {}
        for item in output_items:
            family_key = self._stage_partial_item_signature("t_day", item)
            t_day = item.get("t_day")
            if not isinstance(t_day, int):
                continue
            slot_kind = self._actual_t_day_slot_kind(item)
            actual_slots.setdefault(family_key, set()).add((t_day, slot_kind))
        return actual_slots

    @staticmethod
    def _expected_t_day_slot_kind(
        explicit_order_type: OrderType | None,
        uses_signed_amount: bool,
    ) -> str | None:
        """문서 컬럼 스펙을 sibling-slot 비교용 slot kind로 정규화한다."""
        if uses_signed_amount or explicit_order_type is None:
            return "NET"
        return explicit_order_type.value

    def _actual_t_day_slot_kind(self, item: dict[str, Any]) -> str:
        """Stage 3 output item을 sibling-slot 비교용 slot kind로 정규화한다."""
        slot_id = self._normalize_text(item.get("slot_id"))
        if slot_id.endswith("_SUB"):
            return OrderType.SUB.value
        if slot_id.endswith("_RED"):
            return OrderType.RED.value
        if slot_id.endswith("_NET"):
            return "NET"

        evidence_label = self._normalize_text(item.get("evidence_label"))
        order_type = self._order_type_hint_from_header_label(evidence_label)
        if order_type is not None:
            return order_type.value
        return "NET"

    @staticmethod
    def _deterministic_t_day_slot_id(*, t_day: int, slot_kind: str) -> str:
        """Deterministic t_day 복구에서 slot_id를 안정적으로 만든다."""
        return f"T{t_day}_{slot_kind}"

    def _group_stage_items_by_family(
        self,
        stage_name: str,
        items: Any,
    ) -> dict[tuple[Any, ...], list[dict[str, Any]]]:
        grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for item in items:
            family_key = self._stage_partial_item_signature(stage_name, item)
            grouped.setdefault(family_key, []).append(item)
        return grouped

    @staticmethod
    def _classify_stage_retry_item_delta(
        *,
        current_item: dict[str, Any],
        candidate_item: dict[str, Any],
    ) -> str:
        """같은 signature row에 대한 retry 결과가 안전한 개선인지 분류한다.

        반환값:
        - `same`: 내용이 동일
        - `improved`: 기존 empty/null 값을 candidate가 채움
        - `conflict`: 기존 non-empty 값을 candidate가 바꿈
        """
        improved = False
        for field_name in set(current_item) | set(candidate_item):
            current_value = current_item.get(field_name)
            candidate_value = candidate_item.get(field_name)
            if current_value == candidate_value:
                continue
            if FundOrderExtractor._is_empty_stage_retry_value(current_value) and not FundOrderExtractor._is_empty_stage_retry_value(candidate_value):
                improved = True
                continue
            return "conflict"
        return "improved" if improved else "same"

    @staticmethod
    def _is_empty_stage_retry_value(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            return not value.strip()
        return False

    def _stage_items_by_signature(
        self,
        stage_name: str,
        items: list[Any],
    ) -> dict[tuple[Any, ...], dict[str, Any]]:
        normalized: dict[tuple[Any, ...], dict[str, Any]] = {}
        for item in items:
            item_dict = item.model_dump(mode="json") if hasattr(item, "model_dump") else dict(item)
            normalized[self._stage_item_signature(stage_name, item_dict)] = item_dict
        return normalized

    def _stage_allowed_output_families(
        self,
        stage_name: str,
        input_items: list[dict[str, Any]] | None,
    ) -> set[tuple[Any, ...]] | None:
        """해당 stage가 출력해도 되는 batch identity 집합을 계산한다.

        stage 2~6은 입력 batch 범위 밖의 row를 새로 만들면 안 된다.
        반면 stage 1은 discovery 단계라 입력 batch가 없으므로 현재는 별도 허용 집합을 만들지 않는다.
        """
        if not input_items:
            return None
        return {self._stage_partial_item_signature(stage_name, item) for item in input_items}

    def _stage_items_are_complete(
        self,
        stage_name: str,
        items: Any,
    ) -> bool:
        required_fields = self._stage_required_fields(stage_name)
        item_list = list(items)
        if not item_list:
            return False

        for item in item_list:
            for field_name in required_fields:
                value = item.get(field_name)
                if field_name == "base_date":
                    if self._normalize_date(value) is None:
                        return False
                    continue
                if field_name == "transfer_amount":
                    if self._normalize_amount(value) is None:
                        return False
                    continue
                if field_name == "settle_class":
                    evidence_label = self._normalize_text(item.get("evidence_label"))
                    if (
                        self._normalize_settle_class(
                            value,
                            item.get("t_day"),
                            evidence_label,
                        )
                        is None
                    ):
                        return False
                    continue
                if field_name == "order_type":
                    amount = self._normalize_amount(item.get("transfer_amount"))
                    evidence_label = self._normalize_text(item.get("evidence_label"))
                    if amount is None or self._normalize_order_type(value, amount, evidence_label) is None:
                        return False
                    continue
                if value is None:
                    return False
                if isinstance(value, str) and not value.strip():
                    return False
        return True

    @staticmethod
    def _stage_required_fields(stage_name: str) -> tuple[str, ...]:
        field_map = {
            "fund_inventory": ("fund_code", "fund_name"),
            "base_date": ("fund_code", "fund_name", "base_date"),
            "t_day": ("fund_code", "fund_name", "base_date", "t_day", "slot_id"),
            "transfer_amount": ("fund_code", "fund_name", "base_date", "t_day", "slot_id", "transfer_amount"),
            "settle_class": (
                "fund_code",
                "fund_name",
                "base_date",
                "t_day",
                "slot_id",
                "transfer_amount",
                "settle_class",
            ),
            "order_type": (
                "fund_code",
                "fund_name",
                "base_date",
                "t_day",
                "slot_id",
                "transfer_amount",
                "settle_class",
                "order_type",
            ),
        }
        return field_map.get(stage_name, ())

    def _build_result(
        self,
        resolved_items: list[FundResolvedItem],
        issues: list[str],
        document_text: str | None = None,
    ) -> ExtractionResult:
        """마지막 stage 결과를 실제 저장 가능한 domain order 로 변환한다.

        이 함수부터는 LLM 추론이 아니라 코드 규칙이 우선한다.
        핵심 역할:
        - 문자열/날짜/금액 정규화
        - 좌수/비금액 evidence 제거
        - 같은 fund + 같은 결제 버킷의 후보를 규칙에 따라 최종 주문으로 집계
        - explicit 설정/해지 금액이 같은 버킷에 함께 있으면 SUB/RED를 분리 유지
        - 그렇지 않으면 순유입/결제금액 같은 net 컬럼을 보조적으로 활용
        """
        direct_orders = self._build_buy_sell_report_orders(document_text)
        if direct_orders:
            filtered_issues = [
                issue
                for issue in self._unique_preserve_order(issues)
                if not any(self._issue_has_code(issue, code) for code in BLOCKING_EXTRACTION_ISSUES)
                and issue not in {"MULTIPLE_SLOTS_PER_FUND_WITH_SAME_TDAY", "ORDER_TYPE_AMBIGUOUS"}
                and not issue.startswith("EVIDENCE_LABEL_AMBIGUOUS")
            ]
            return ExtractionResult(orders=direct_orders, issues=filtered_issues)

        document_order_type_hints = self._build_document_order_type_hints(document_text, resolved_items)
        candidates: list[_OrderCandidate] = []

        for item in resolved_items:
            fund_code = self._normalize_text(item.fund_code)
            fund_name = self._normalize_text(item.fund_name)
            evidence_label = self._normalize_text(item.evidence_label)
            if not fund_code or not fund_name:
                self._append_unique(issues, "FUND_METADATA_INCOMPLETE")
                continue
            if self._is_non_amount_evidence(evidence_label):
                self._append_unique(issues, "NON_AMOUNT_EVIDENCE_DROPPED")
                continue

            base_date = self._normalize_date(item.base_date)
            if base_date is None:
                self._append_unique(issues, "BASE_DATE_MISSING")

            canonical_amount = self._canonicalize_amount_text(item.transfer_amount)
            amount = self._format_source_amount_text(item.transfer_amount)
            if canonical_amount is None or amount is None:
                self._append_unique(issues, "TRANSFER_AMOUNT_MISSING")
                continue
            if self._is_effectively_zero_amount(canonical_amount):
                continue

            settle_class = self._normalize_settle_class(item.settle_class, item.t_day, evidence_label)
            if settle_class is None:
                self._append_unique(issues, "SETTLE_CLASS_MISSING")
                continue

            t_day = self._normalize_t_day(item.t_day, settle_class)

            document_order_type = document_order_type_hints.get((fund_code, canonical_amount.lstrip("+-")))
            if document_order_type is not None:
                amount = self._align_amount_with_document_order_type(amount, document_order_type)
            order_type = document_order_type or self._normalize_order_type(item.order_type, amount, evidence_label)
            if order_type is None:
                self._append_unique(issues, "ORDER_TYPE_MISSING")
                continue

            # 아직 최종 orders 가 아니라 "후보(candidate)" 단계다.
            # 같은 결제 버킷에서도 evidence가 여러 개 나올 수 있으므로 일단 후보를 다 모은다.
            # 마지막 집계에서는
            # - explicit SUB/RED가 함께 있으면 방향별 주문을 그대로 유지하고
            # - 그렇지 않으면 execution/net 컬럼을 우선 반영한다.
            candidates.append(
                _OrderCandidate(
                    order=OrderExtraction(
                        fund_code=fund_code,
                        fund_name=fund_name,
                        settle_class=settle_class,
                        order_type=order_type,
                        base_date=base_date,
                        t_day=t_day,
                        transfer_amount=amount,
                    ),
                    evidence_label=evidence_label,
                    evidence_kind=self._classify_evidence_kind(evidence_label),
                )
            )

        orders = self._aggregate_candidates(candidates, issues)
        return ExtractionResult(orders=orders, issues=self._unique_preserve_order(issues))

    def _build_buy_sell_report_orders(self, document_text: str | None) -> list[OrderExtraction]:
        """BUY & SELL REPORT 형식은 표를 직접 읽어 buy/sell 주문을 각각 확정한다.

        AIA 계열 EML은 한 행 안에 `Buy Amount`와 `Sell Amount`가 함께 있고,
        최근 요구사항에서는 이를 순액 1건으로 합치지 않고
        SUB/RED 주문 2건으로 유지해야 한다.
        이 패턴은 표 구조가 매우 명확하므로, raw backup 안의 pipe row를 직접 읽어
        buy와 sell을 각각 개별 주문으로 확정하는 편이 더 안정적이다.
        """
        if not document_text or "buy & sell report" not in document_text.lower():
            return []

        rows = self._parse_buy_sell_report_rows(document_text)
        if not rows:
            return []

        orders: list[OrderExtraction] = []
        for row in rows:
            fund_code = self._normalize_text(row["fund_code"])
            fund_name = self._normalize_text(row["fund_name"])
            base_date = self._normalize_buy_sell_report_date(row["trade_date"]) or self._normalize_date(row["trade_date"])
            buy_amount = self._normalize_amount(row["buy_amount"])
            sell_amount = self._normalize_amount(row["sell_amount"])
            if not fund_code or not fund_name or base_date is None:
                continue

            if buy_amount is not None:
                buy_decimal = Decimal(buy_amount.replace(",", ""))
                if buy_decimal != 0:
                    orders.append(
                        OrderExtraction(
                            fund_code=fund_code,
                            fund_name=fund_name,
                            settle_class=SettleClass.CONFIRMED,
                            order_type=OrderType.SUB,
                            base_date=base_date,
                            t_day=0,
                            transfer_amount=self._format_decimal_amount(buy_decimal),
                        )
                    )
            if sell_amount is not None:
                sell_decimal = Decimal(sell_amount.replace(",", ""))
                if sell_decimal != 0:
                    orders.append(
                        OrderExtraction(
                            fund_code=fund_code,
                            fund_name=fund_name,
                            settle_class=SettleClass.CONFIRMED,
                            order_type=OrderType.RED,
                            base_date=base_date,
                            t_day=0,
                            transfer_amount=self._format_decimal_amount(-sell_decimal),
                        )
                    )

        return orders

    def _parse_buy_sell_report_rows(self, document_text: str) -> list[dict[str, str]]:
        """BUY & SELL REPORT raw pipe rows에서 거래 행만 뽑아낸다."""
        rows: list[dict[str, str]] = []
        in_table = False
        current_date = ""

        for raw_line in document_text.splitlines():
            line = raw_line.strip()
            lowered = line.lower()
            if "date | buy&sell | external fund manager | fund code | fund name | fund price | buy" in lowered:
                in_table = True
                continue
            if not in_table:
                continue
            if not line or line.startswith("```"):
                continue
            if self._is_buy_sell_report_total_line(line):
                break
            if "|" not in line:
                if rows:
                    break
                continue

            cells = [self._normalize_text(part) for part in line.split("|")]
            if len(cells) < 10:
                continue
            if any(token in lowered for token in ("fund code", "fund name", "external fund manager", "custodian bank")):
                continue

            trade_date = cells[0] or current_date
            if self._normalize_date(trade_date) is None:
                continue
            current_date = trade_date

            manager = cells[2]
            fund_code = cells[3]
            fund_name = cells[4]
            buy_amount = cells[6]
            sell_amount = cells[8]
            if not fund_code or not fund_name:
                continue
            if buy_amount and self._normalize_amount(buy_amount) is None:
                continue
            if sell_amount and self._normalize_amount(sell_amount) is None:
                continue

            rows.append(
                {
                    "trade_date": trade_date,
                    "manager": manager,
                    "fund_code": fund_code,
                    "fund_name": fund_name,
                    "buy_amount": buy_amount,
                    "sell_amount": sell_amount,
                }
            )

        return rows

    @staticmethod
    def _is_buy_sell_report_total_line(line: str) -> bool:
        """BUY&SELL 표의 합계/총계 행을 판별한다."""
        stripped = line.strip().lower()
        return stripped.startswith("total") or stripped.startswith("합계") or stripped.startswith("총계")

    @staticmethod
    def _normalize_buy_sell_report_date(value: str | None) -> str | None:
        """BUY&SELL 표의 `MM-DD-YYYY` 날짜를 표준 `YYYY-MM-DD`로 바꾼다."""
        if value is None:
            return None
        match = re.search(r"(\d{1,2})-(\d{1,2})-(\d{4})", value.strip())
        if not match:
            return None
        month, day, year = match.groups()
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"

    @staticmethod
    def _normalize_short_english_date(value: str | None) -> str | None:
        """`27-Nov-25` 같은 영문 축약 날짜를 `YYYY-MM-DD`로 바꾼다."""
        if value is None:
            return None
        text = value.strip()
        if not text:
            return None
        try:
            parsed = datetime.strptime(text, "%d-%b-%y")
        except ValueError:
            return None
        return parsed.strftime("%Y-%m-%d")

    def _build_document_order_type_hints(
        self,
        document_text: str | None,
        resolved_items: list[FundResolvedItem],
    ) -> dict[tuple[str, str], OrderType]:
        """섹션 문서에서 fund_code+amount 가 속한 실제 section 을 찾아 order_type 힌트를 만든다.

        카드프처럼 `1. Subscription`, `2. Redemption`으로 명확히 분리된 문서는
        LLM stage 6 이 일부 행의 SUB/RED 를 흔들릴 때가 있다.
        이 경우 amount-bearing row 가 실제로 어느 section 에 있었는지를
        결정론적으로 다시 확인해 후처리에서 우선 적용한다.
        """
        if not document_text:
            return {}

        targets = {
            (self._normalize_text(item.fund_code), self._unsigned_amount_identity(item.transfer_amount) or "")
            for item in resolved_items
            if self._normalize_text(item.fund_code) and self._unsigned_amount_identity(item.transfer_amount)
        }
        if not targets:
            return {}

        hints: dict[tuple[str, str], set[OrderType]] = {}
        current_section: OrderType | None = None
        lines = [line.strip() for line in document_text.splitlines() if line.strip()]
        for line in lines:
            section = self._section_order_type_hint(line)
            if section is not None:
                current_section = section
                continue
            if current_section is None:
                continue
            for fund_code, amount in targets:
                if not self._line_matches_fund_amount(line, fund_code, amount):
                    continue
                hints.setdefault((fund_code, amount), set()).add(current_section)

        # 섹션 제목이 없는 표형 문서도 많다. 예를 들어 동양생명 `예상내역`은
        # `설정금액 / 3월19일`, `해지금액 / 3월20일`처럼 header 안에 방향 정보가
        # 들어 있는데, stage 6이 evidence_label을 날짜만 남기면 SUB/RED를 잃을 수 있다.
        # structured markdown의 collapsed header를 다시 읽어서 amount-bearing cell의
        # 방향을 결정론적으로 복구하면 이런 표형 문서도 안정적으로 보정할 수 있다.
        for key, order_types in self._build_markdown_table_order_type_hints(document_text, targets).items():
            hints.setdefault(key, set()).update(order_types)

        return {
            key: next(iter(order_types))
            for key, order_types in hints.items()
            if len(order_types) == 1
        }

    def _build_markdown_table_order_type_hints(
        self,
        document_text: str,
        targets: set[tuple[str, str]],
    ) -> dict[tuple[str, str], set[OrderType]]:
        """Structured markdown 표를 다시 읽어 `(fund_code, amount)`별 방향 힌트를 모은다."""
        if not targets:
            return {}

        hints: dict[tuple[str, str], set[OrderType]] = {}
        markdown_view = self._structured_markdown_view(document_text)

        for table_lines in self._iter_markdown_table_blocks(markdown_view):
            rows = [self._markdown_table_cells(line) for line in table_lines]
            if len(rows) < 3:
                continue

            header = rows[0]
            if self._is_markdown_separator_row(header):
                continue

            fund_code_index = self._markdown_fund_code_index(header, body_rows=rows[1:])
            if fund_code_index is None:
                continue

            row_context_priorities = {
                index: self._row_context_amount_priority_for_deterministic(label)
                for index, label in enumerate(header)
            }
            preferred_row_context_priority = min(
                (priority for priority in row_context_priorities.values() if priority is not None),
                default=None,
            )
            explicit_columns = {
                index: self._order_type_hint_from_header_label(label)
                for index, label in enumerate(header)
            }
            if preferred_row_context_priority is None and not any(
                order_type is not None for order_type in explicit_columns.values()
            ):
                continue

            for row in rows[1:]:
                if self._is_markdown_separator_row(row) or fund_code_index >= len(row):
                    continue
                fund_code = self._normalize_text(row[fund_code_index])
                if not fund_code:
                    continue
                row_context_order_type = self._row_context_order_type_from_markdown_row(header=header, row=row)

                for column_index, explicit_order_type in explicit_columns.items():
                    order_type = explicit_order_type
                    if (
                        order_type is None
                        and row_context_order_type is not None
                        and row_context_priorities.get(column_index) == preferred_row_context_priority
                    ):
                        order_type = row_context_order_type
                    if order_type is None or column_index >= len(row):
                        continue
                    amount = self._canonicalize_amount_text(row[column_index])
                    if amount is None or self._is_effectively_zero_amount(amount):
                        continue
                    key = (fund_code, amount.lstrip("+-"))
                    if key not in targets:
                        continue
                    hints.setdefault(key, set()).add(order_type)

        return hints

    @staticmethod
    def _structured_markdown_view(document_text: str) -> str:
        """LLM 전체 문맥에서 structured markdown 본문만 잘라낸다."""
        marker = "\n\nRaw text backup:\n```text\n"
        if marker in document_text:
            return document_text.split(marker, 1)[0]
        return document_text

    @staticmethod
    def _iter_markdown_table_blocks(markdown_text: str) -> list[list[str]]:
        """연속된 markdown table line 묶음을 추출한다."""
        blocks: list[list[str]] = []
        current: list[str] = []
        for raw_line in markdown_text.splitlines():
            line = raw_line.strip()
            if line.startswith("|") and line.endswith("|"):
                current.append(line)
                continue
            if current:
                blocks.append(current)
                current = []
        if current:
            blocks.append(current)
        return blocks

    @staticmethod
    def _markdown_table_cells(line: str) -> list[str]:
        """markdown table line을 셀 리스트로 평탄화한다."""
        stripped = line.strip().strip("|")
        return [part.strip() for part in stripped.split("|")]

    @staticmethod
    def _is_markdown_separator_row(cells: list[str]) -> bool:
        """`| --- | --- |` 형태의 separator row인지 확인한다."""
        if not cells:
            return False
        non_empty_cells = [cell for cell in cells if cell]
        if not non_empty_cells:
            return False
        return all(bool(re.fullmatch(r":?-{3,}:?", cell)) for cell in non_empty_cells)

    def _markdown_fund_code_index(
        self,
        header: list[str],
        *,
        body_rows: list[list[str]] | None = None,
    ) -> int | None:
        """header에서 펀드코드 열 인덱스를 찾는다."""
        def sampled_column_values_look_like_fund_codes(index: int, *, sample_size: int) -> bool:
            values = [
                self._normalize_text(row[index])
                for row in body_rows or []
                if (
                    not self._is_markdown_separator_row(row)
                    and index < len(row)
                    and self._normalize_text(row[index])
                )
            ]
            sample = values[: min(len(values), sample_size)]
            return bool(sample) and all(self._looks_like_fund_code(value) for value in sample)

        for index, label in enumerate(header):
            lowered = label.lower()
            if "펀드코드" in label or "fund code" in lowered:
                return index
        has_fund_name = any("펀드명" in label or "fund name" in label.lower() for label in header)
        has_execution_amount = any(self._is_execution_evidence(self._normalize_text(label)) for label in header)
        if has_fund_name and has_execution_amount and body_rows:
            for index, label in enumerate(header):
                lowered = label.lower().replace(" ", "")
                if "운용사코드" in label or "managercode" in lowered:
                    if sampled_column_values_look_like_fund_codes(index, sample_size=10):
                        return index
            for index, label in enumerate(header):
                lowered = self._normalize_text(label).lower().replace(" ", "")
                if lowered in {"펀드", "fund"} and sampled_column_values_look_like_fund_codes(index, sample_size=5):
                    return index
        return None

    @staticmethod
    def _looks_like_fund_code(value: str) -> bool:
        """값이 fund code처럼 보이는지 간단히 판별한다."""
        if not value:
            return False
        if value != value.strip() or " " in value:
            return False
        return bool(re.fullmatch(r"[A-Z0-9]{3,}", value))

    @staticmethod
    def _looks_like_non_amount_metadata_label(label: str) -> bool:
        """방향 단어가 있어도 실제 주문 컬럼이 아닌 메타데이터 헤더를 걸러낸다."""
        lowered = label.lower()
        compact_label = re.sub(r"\s+", "", lowered)
        if any(keyword in lowered for keyword in ("manager", "custodian", "bank")):
            return True
        return any(
            keyword in compact_label
            for keyword in ("운용사", "수탁은행", "판매사", "환매사")
        )

    @staticmethod
    def _order_type_hint_from_header_label(label: str) -> OrderType | None:
        """collapsed markdown header label이 SUB/RED 방향을 직접 가리키는지 본다."""
        if FundOrderExtractor._looks_like_non_amount_metadata_label(label):
            return None
        lowered = label.lower()
        sub_tokens = ["투입", "설정", "입금", "납입", "매입", "buy", "subscription"]
        red_tokens = ["인출", "해지", "출금", "환매", "sell", "redemption"]
        has_sub = any(token in lowered for token in sub_tokens)
        has_red = any(token in lowered for token in red_tokens)
        if has_sub and has_red:
            return None
        if has_sub:
            return OrderType.SUB
        if has_red:
            return OrderType.RED
        return None

    @staticmethod
    def _is_order_context_header_for_deterministic(label: str) -> bool:
        """header가 row-level 주문 방향 문맥을 제공하는지 본다."""
        lowered = FundOrderExtractor._normalize_text(label).lower()
        compact = re.sub(r"[^0-9a-z가-힣]+", "", lowered)
        return any(
            keyword in lowered
            for keyword in (
                "transaction",
                "type",
                "content",
                "detail",
                "description",
                "memo",
            )
        ) or any(keyword in compact for keyword in ("구분", "내용", "적요", "사유", "비고")) or compact in {
            "거래유형",
            "거래유형명",
        }

    @staticmethod
    def _row_context_amount_priority_for_deterministic(label: str) -> int | None:
        """row-level direction으로 해석해야 하는 mixed amount 열의 우선순위."""
        best_priority: int | None = None
        normalized_label = FundOrderExtractor._normalize_text(label).lower()
        whole_label_adjustment = 0
        compact_label = re.sub(r"[^0-9a-z가-힣]+", "", normalized_label)
        if "펀드계" in compact_label:
            whole_label_adjustment = -10
        elif "투자일임" in compact_label or "수익증권" in compact_label:
            whole_label_adjustment = 1
        for raw_segment in normalized_label.split("/"):
            segment = raw_segment.strip()
            if not segment:
                continue
            if ("금액" not in segment and "amount" not in segment) and not FundOrderExtractor._is_plain_generic_amount_segment(segment):
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
            elif FundOrderExtractor._is_plain_generic_amount_segment(segment):
                priority = 10

            if priority is None:
                continue
            priority += whole_label_adjustment
            if best_priority is None or priority < best_priority:
                best_priority = priority

        return best_priority

    @staticmethod
    def _plain_generic_amount_segment(segment: str) -> str:
        return re.sub(r"[^0-9a-z가-힣]+", "", segment).lower()

    @staticmethod
    def _is_plain_generic_amount_segment(segment: str) -> bool:
        normalized = FundOrderExtractor._plain_generic_amount_segment(segment)
        return normalized in {"금액", "금액원", "amount", "amountkrw", "amountwon"}

    def _row_context_order_type_from_markdown_row(
        self,
        *,
        header: list[str],
        row: list[str],
    ) -> OrderType | None:
        """행 구분 컬럼에서 SUB/RED를 직접 복구한다."""
        for column_index, label in enumerate(header):
            if column_index >= len(row):
                continue
            if not self._is_order_context_header_for_deterministic(label):
                continue
            order_type = self._order_type_hint_from_header_label(row[column_index])
            if order_type is not None:
                return order_type
        return None

    @staticmethod
    def _remarks_column_index_for_deterministic(header: list[str]) -> int | None:
        """header에서 비고/remarks 컬럼 위치를 찾는다."""
        for index, label in enumerate(header):
            normalized = FundOrderExtractor._normalize_text(label).lower().replace(" ", "")
            if "비고" in normalized or "remark" in normalized or "note" in normalized:
                return index
        return None

    @staticmethod
    def _is_pending_request_label_for_deterministic(label: str) -> bool:
        """설정/해지 신청처럼 청구분 future slot을 의미하는 컬럼인지 판별한다."""
        lowered = FundOrderExtractor._normalize_text(label).lower().replace(" ", "")
        return "해지신청" in lowered or "설정신청" in lowered

    @staticmethod
    def _parse_numeric_amount_text_for_deterministic(value: str | None) -> Decimal | None:
        """deterministic markdown 경로에서 한글 단위 금액까지 안정적으로 읽는다."""
        normalized_token = DocumentLoader._normalize_document_amount_token(value)
        if normalized_token is None:
            return None
        normalized = normalized_token.replace(",", "")
        if not normalized or normalized in {"-", "/", "--"}:
            return None
        try:
            return Decimal(normalized)
        except Exception:
            pass

        for pattern, multiplier in (
            (r"([+-]?\d+(?:\.\d+)?)억", Decimal("100000000")),
            (r"([+-]?\d+(?:\.\d+)?)만", Decimal("10000")),
            (r"([+-]?\d+(?:\.\d+)?)천", Decimal("1000")),
        ):
            match = re.fullmatch(pattern, normalized)
            if match:
                return Decimal(match.group(1)) * multiplier
        return None

    def _normalize_amount_for_deterministic(self, value: str | None) -> str | None:
        """deterministic markdown 경로에서 금액을 내부 표준 문자열로 정규화한다."""
        parsed = self._parse_numeric_amount_text_for_deterministic(value)
        if parsed is None:
            return None
        return self._format_decimal_amount(parsed)

    def _bucket_key_from_pending_note_for_deterministic(
        self,
        note_text: str,
        amount_value: Decimal | None,
    ) -> str | None:
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
                parsed_amount = self._parse_numeric_amount_text_for_deterministic(amount_text)
                if parsed_amount is None:
                    continue
                if abs(parsed_amount - amount_value) < Decimal("0.5"):
                    return f"DAY{int(match.group('day')):02d}"

        unique_days = {int(match.group("day")) for match in matches}
        if len(unique_days) == 1:
            day_value = next(iter(unique_days))
            return f"DAY{day_value:02d}"
        return None

    def _pending_request_t_day_from_row_for_deterministic(
        self,
        *,
        label: str,
        row: list[str],
        header: list[str],
        column_index: int,
        base_date: str | None,
    ) -> int | None:
        """청구분 신청 컬럼은 같은 row의 `비고`에서 예정일을 읽어 future bucket을 정한다."""
        if not self._is_pending_request_label_for_deterministic(label):
            return None
        if column_index >= len(row):
            return None
        note_index = self._remarks_column_index_for_deterministic(header)
        if note_index is None or note_index >= len(row):
            return None
        note_text = self._normalize_text(row[note_index])
        if not note_text:
            return None
        amount_value = self._parse_numeric_amount_text_for_deterministic(row[column_index])
        bucket_key = self._bucket_key_from_pending_note_for_deterministic(note_text, amount_value)
        if not bucket_key or not bucket_key.startswith("DAY"):
            return None
        if base_date is None:
            return None
        try:
            _, base_day = self._base_month_day(base_date)
        except Exception:
            return None
        try:
            day_value = int(bucket_key[3:])
        except ValueError:
            return None
        delta = day_value - base_day
        if delta <= 0:
            return None
        return delta

    def _build_deterministic_markdown_table_orders(
        self,
        *,
        markdown_text: str,
        raw_text: str,
        target_fund_scope: TargetFundScope | None,
    ) -> list[OrderExtraction]:
        """Structured markdown table만 읽어 결정론적으로 주문을 복구한다."""
        base_date = self._detect_document_base_date_from_text(raw_text)

        markdown_orders: list[OrderExtraction] = []
        if base_date is not None:
            for table_lines in self._iter_markdown_table_blocks(markdown_text):
                rows = [self._markdown_table_cells(line) for line in table_lines]
                if len(rows) < 3:
                    continue

                header = rows[0]
                if self._is_markdown_separator_row(header):
                    continue

                fund_code_index = self._markdown_fund_code_index(header, body_rows=rows[1:])
                fund_name_index = self._markdown_fund_name_index(header)
                if fund_code_index is None:
                    continue

                column_specs = self._deterministic_order_columns(header, base_date)
                if not column_specs:
                    continue

                for row in rows[1:]:
                    if self._is_markdown_separator_row(row):
                        continue
                    if fund_code_index >= len(row):
                        continue

                    fund_code = self._normalize_text(row[fund_code_index])
                    fund_name = (
                        self._normalize_text(row[fund_name_index])
                        if fund_name_index is not None and fund_name_index < len(row)
                        else "-"
                    )
                    fund_name = fund_name or "-"
                    if not fund_code or self._looks_like_total_row(fund_code, fund_name):
                        continue
                    if target_fund_scope is not None and not self._is_target_fund(
                        fund_code=fund_code,
                        fund_name=fund_name,
                        target_fund_scope=target_fund_scope,
                    ):
                        continue

                    for (
                        column_index,
                        settle_class,
                        t_day,
                        explicit_order_type,
                        uses_signed_amount,
                        uses_row_context_order_type,
                    ) in column_specs:
                        if column_index >= len(row):
                            continue
                        amount = self._normalize_amount_for_deterministic(row[column_index])
                        if amount is None or self._is_effectively_zero_amount(amount):
                            continue
                        label = self._normalize_text(header[column_index])
                        if self._is_pending_request_label_for_deterministic(label):
                            pending_t_day = self._pending_request_t_day_from_row_for_deterministic(
                                label=label,
                                row=row,
                                header=header,
                                column_index=column_index,
                                base_date=base_date,
                            )
                            if pending_t_day is None:
                                continue
                            t_day = pending_t_day
                            settle_class = SettleClass.PENDING

                        amount_decimal = Decimal(amount.replace(",", ""))
                        order_type = explicit_order_type
                        if uses_row_context_order_type:
                            order_type = self._row_context_order_type_from_markdown_row(
                                header=header,
                                row=row,
                            )
                            if order_type is None:
                                continue
                            signed_amount = abs(amount_decimal)
                            if order_type is OrderType.RED:
                                signed_amount = -signed_amount
                            transfer_amount = self._format_decimal_amount(signed_amount)
                        elif uses_signed_amount:
                            order_type = self._order_type_from_signed_amount(amount_decimal)
                            if order_type is None:
                                continue
                            transfer_amount = self._format_decimal_amount(amount_decimal)
                        else:
                            if order_type is None:
                                continue
                            signed_amount = abs(amount_decimal)
                            if order_type is OrderType.RED:
                                signed_amount = -signed_amount
                            transfer_amount = self._format_decimal_amount(signed_amount)

                        markdown_orders.append(
                            OrderExtraction(
                                fund_code=fund_code,
                                fund_name=fund_name,
                                settle_class=settle_class,
                                order_type=order_type,
                                base_date=base_date,
                                t_day=t_day,
                                transfer_amount=transfer_amount,
                            )
                        )

        return markdown_orders

    @staticmethod
    def _row_context_authoritative_column_uses_signed_amount(label: str) -> bool:
        """row-context 우선 컬럼 중에서도 net total은 부호로 방향을 해석한다."""
        compact_label = re.sub(r"[^0-9a-z가-힣]+", "", FundOrderExtractor._normalize_text(label).lower())
        return "펀드계" in compact_label

    def _build_deterministic_markdown_orders(
        self,
        *,
        markdown_text: str,
        raw_text: str,
        target_fund_scope: TargetFundScope | None,
    ) -> list[OrderExtraction]:
        """Structured markdown와 section raw text를 함께 읽어 결정론적으로 주문을 복구한다."""
        markdown_orders = self._build_deterministic_markdown_table_orders(
            markdown_text=markdown_text,
            raw_text=raw_text,
            target_fund_scope=target_fund_scope,
        )
        base_date = self._detect_document_base_date_from_text(raw_text)

        # Cardif 같은 영문 sectioned PDF는 markdown pipe table이 아니라 raw text 행으로만
        # 남는 경우가 있다. 이 경우에도 Subscription/Redemption section과 amount row를
        # 직접 읽으면 LLM 누락분을 결정론적으로 복구할 수 있다.
        #
        # 중요:
        # section 복구는 "markdown 복구가 0건일 때만" 시도하면 안 된다.
        # 실제로는 markdown 경로가 일부 행만 복구하고, section raw text 경로가
        # 나머지 행까지 더 완전하게 읽는 경우가 있다. 따라서 두 전략을 모두 시도한 뒤
        # 더 많은 주문을 복구한 쪽을 채택한다.
        section_orders = self._build_deterministic_section_orders(
            raw_text=raw_text,
            base_date=base_date,
            target_fund_scope=target_fund_scope,
        )
        return self._select_preferred_deterministic_orders(
            markdown_orders=markdown_orders,
            section_orders=section_orders,
        )

    def _canonical_fund_families_from_base_dates(
        self,
        base_dates: list[FundBaseDateItem],
    ) -> set[tuple[str, str]]:
        """base_date stage가 발견한 fund family 집합을 canonical form으로 만든다."""
        return {
            self._canonical_fund_identity(item.fund_code, item.fund_name)
            for item in base_dates
            if self._normalize_text(item.fund_code) or self._normalize_text(item.fund_name)
        }

    def _canonical_fund_families_from_orders(
        self,
        orders: list[OrderExtraction],
    ) -> set[tuple[str, str]]:
        """결정론적으로 복구한 order list를 family 집합으로 축약한다."""
        return {
            self._canonical_fund_identity(order.fund_code, order.fund_name)
            for order in orders
            if self._normalize_text(order.fund_code) or self._normalize_text(order.fund_name)
        }

    def _shortcut_active_fund_families_from_document(
        self,
        *,
        document_text: str | None,
        target_fund_scope: TargetFundScope | None,
        fallback_orders: list[OrderExtraction],
    ) -> set[tuple[str, str]]:
        """문서에 실제 non-zero transaction evidence가 있는 family 집합을 구한다."""
        if document_text:
            derived_families = {
                self._canonical_fund_identity(item.fund_code, item.fund_name)
                for item in self._derive_document_fund_seed_items(
                    document_text=document_text,
                    target_fund_scope=target_fund_scope,
                )
                if self._normalize_text(item.fund_code) or self._normalize_text(item.fund_name)
            }
            if derived_families:
                return derived_families
        return self._canonical_fund_families_from_orders(fallback_orders)

    def _build_structure_shortcut_orders_after_base_date(
        self,
        *,
        base_dates: list[FundBaseDateItem],
        raw_text: str,
        markdown_text: str | None,
        target_fund_scope: TargetFundScope | None,
        expected_order_count: int | None,
    ) -> list[OrderExtraction]:
        """문서 구조가 충분히 명확하면 base_date 이후 LLM stage를 생략한다.

        현재는 아래 조건을 모두 만족하는 sectioned ledger 문서만 shortcut 대상이다.
        - base_date가 모든 item에서 하나로 수렴한다.
        - raw text에서 deterministic section parser가 주문을 완전히 복구한다.
        - 복구 결과가 전부 same-day confirmed다.
        - family 집합과 expected_order_count가 base_date stage 결과와 모순되지 않는다.

        이 조건이 약하면 곧바로 빈 목록을 반환해 기존 staged LLM 경로를 유지한다.
        """
        if (
            not base_dates
            or not raw_text
            or expected_order_count is None
            or expected_order_count <= 0
        ):
            return []

        normalized_base_dates = {
            self._normalize_date(item.base_date)
            for item in base_dates
            if self._normalize_date(item.base_date) is not None
        }
        if len(normalized_base_dates) != 1:
            return []
        base_date = next(iter(normalized_base_dates))

        expected_families = self._canonical_fund_families_from_base_dates(base_dates)
        if not expected_families:
            return []

        structure_orders = self._dedupe_orders_by_signature(
            self._build_deterministic_section_orders(
                raw_text=raw_text,
                base_date=base_date,
                target_fund_scope=target_fund_scope,
            )
        )
        if not structure_orders:
            return []
        if len(structure_orders) != expected_order_count:
            return []
        if any(
            order.settle_class is not SettleClass.CONFIRMED
            or order.t_day != 0
            or self._normalize_date(order.base_date) != base_date
            for order in structure_orders
        ):
            return []

        actual_families = self._canonical_fund_families_from_orders(structure_orders)
        if not actual_families or not actual_families.issubset(expected_families):
            return []
        active_families = self._shortcut_active_fund_families_from_document(
            document_text=markdown_text,
            target_fund_scope=target_fund_scope,
            fallback_orders=structure_orders,
        )
        if actual_families != active_families:
            return []
        return structure_orders

    def _build_markdown_shortcut_orders_after_base_date(
        self,
        *,
        base_dates: list[FundBaseDateItem],
        markdown_text: str,
        raw_text: str,
        target_fund_scope: TargetFundScope | None,
        expected_order_count: int | None,
    ) -> list[OrderExtraction]:
        """Structured markdown table이 coverage를 증명할 때만 후반 stage를 생략한다."""
        if (
            not base_dates
            or not markdown_text
            or not raw_text
            or expected_order_count is None
            or expected_order_count <= 0
        ):
            return []

        expected_families = self._canonical_fund_families_from_base_dates(base_dates)
        if not expected_families:
            return []

        normalized_base_dates = {
            self._normalize_date(item.base_date)
            for item in base_dates
            if self._normalize_date(item.base_date) is not None
        }

        markdown_orders = self._dedupe_orders_by_signature(
            self._build_deterministic_markdown_table_orders(
                markdown_text=markdown_text,
                raw_text=raw_text,
                target_fund_scope=target_fund_scope,
            )
        )
        if not markdown_orders:
            return []
        if len(markdown_orders) != expected_order_count:
            return []
        if any(
            not self._normalize_text(order.fund_code)
            or self._normalize_date(order.base_date) is None
            or order.t_day is None
            or order.settle_class is None
            or order.order_type is None
            or self._normalize_amount(order.transfer_amount) is None
            for order in markdown_orders
        ):
            return []
        if normalized_base_dates and any(
            self._normalize_date(order.base_date) not in normalized_base_dates
            for order in markdown_orders
        ):
            return []

        actual_families = self._canonical_fund_families_from_orders(markdown_orders)
        if not actual_families or not actual_families.issubset(expected_families):
            return []
        active_families = self._shortcut_active_fund_families_from_document(
            document_text=markdown_text,
            target_fund_scope=target_fund_scope,
            fallback_orders=markdown_orders,
        )
        if actual_families != active_families:
            return []
        return markdown_orders

    def _select_preferred_deterministic_orders(
        self,
        *,
        markdown_orders: list[OrderExtraction],
        section_orders: list[OrderExtraction],
    ) -> list[OrderExtraction]:
        """Deterministic 복구 결과 중 더 안전한 쪽을 선택한다.

        원칙:
        - section이 더 많은 주문을 복구하면 section을 채택한다.
        - 같은 건수면 기본값은 markdown이다.
        - 단, 같은 거래 core(펀드/기준일/t_day/settle/절대금액)는 같은데
          방향/부호만 다르면 section raw-text의 직접 section evidence를 더 신뢰한다.
        """
        if not section_orders:
            return markdown_orders
        if len(section_orders) > len(markdown_orders):
            return section_orders
        if len(section_orders) < len(markdown_orders):
            return markdown_orders
        if self._orders_have_same_signatures(markdown_orders, section_orders):
            return markdown_orders
        if self._orders_share_same_transaction_core(markdown_orders, section_orders):
            return section_orders
        return markdown_orders

    def _build_deterministic_section_orders(
        self,
        *,
        raw_text: str,
        base_date: str | None,
        target_fund_scope: TargetFundScope | None,
    ) -> list[OrderExtraction]:
        """pipe table이 없는 sectioned instruction raw text에서 주문을 직접 복구한다.

        지원 대상은 `1. Subscription` / `2. Redemption` 같은 section heading과
        `Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank` 헤더 아래에
        거래 행이 이어지는 영문 PDF 평탄화 텍스트다.
        """
        lowered_text = raw_text.lower()
        if "subscription" not in lowered_text or "redemption" not in lowered_text:
            return []
        if "fund name code no. of unit nav date nav amount(krw) bank" not in lowered_text:
            return []

        row_pattern = self._section_order_row_pattern()

        orders: list[OrderExtraction] = []
        current_order_type: OrderType | None = None
        in_amount_section = False

        for raw_line in raw_text.splitlines():
            line = self._normalize_text(raw_line)
            if not line:
                continue

            section_order_type = self._section_order_type_hint(line)
            if section_order_type is not None:
                current_order_type = section_order_type
                in_amount_section = False
                continue

            lowered_line = line.lower()
            if "fund name code no. of unit nav date nav amount(krw) bank" in lowered_line:
                in_amount_section = True
                continue
            if lowered_line.startswith("total "):
                continue
            if line.startswith("[PAGE "):
                continue
            if current_order_type is None or not in_amount_section:
                continue

            match = row_pattern.match(line)
            if match is None:
                continue

            fund_code = self._normalize_text(match.group("fund_code"))
            fund_name = self._normalize_text(match.group("fund_name"))
            amount = self._normalize_amount(match.group("amount"))
            if not fund_code or not fund_name or amount is None:
                continue
            row_base_date = (
                self._normalize_short_english_date(match.group("nav_date"))
                or self._normalize_date(match.group("nav_date"))
                or base_date
            )
            if row_base_date is None:
                continue
            if self._looks_like_total_row(fund_code, fund_name) or self._is_effectively_zero_amount(amount):
                continue
            if target_fund_scope is not None and not self._is_target_fund(
                fund_code=fund_code,
                fund_name=fund_name,
                target_fund_scope=target_fund_scope,
            ):
                continue

            signed_amount = Decimal(amount.replace(",", ""))
            if current_order_type is OrderType.RED:
                signed_amount = -signed_amount

            orders.append(
                OrderExtraction(
                    fund_code=fund_code,
                    fund_name=fund_name,
                    settle_class=SettleClass.CONFIRMED,
                    order_type=current_order_type,
                    base_date=row_base_date,
                    t_day=0,
                    transfer_amount=self._format_decimal_amount(signed_amount),
                )
            )

        return orders

    def _orders_have_same_signatures(
        self,
        left_orders: list[OrderExtraction],
        right_orders: list[OrderExtraction],
        *,
        ignore_base_date: bool = False,
        ignore_schedule: bool = False,
    ) -> bool:
        """두 주문 목록이 최종 저장 기준으로 완전히 같은지 비교한다."""
        return self._sorted_order_signatures(
            left_orders,
            ignore_base_date=ignore_base_date,
            ignore_schedule=ignore_schedule,
        ) == self._sorted_order_signatures(
            right_orders,
            ignore_base_date=ignore_base_date,
            ignore_schedule=ignore_schedule,
        )

    def _orders_share_same_transaction_core(
        self,
        left_orders: list[OrderExtraction],
        right_orders: list[OrderExtraction],
    ) -> bool:
        """두 주문 목록이 같은 거래 묶음을 가리키는지 비교한다.

        이 비교는 direction(sign/order_type)은 제외하고,
        fund/base_date/t_day/settle/절대금액이 같은지만 본다.
        즉 같은 거래를 서로 다른 방향으로 읽은 경우를 좁게 식별하기 위한 용도다.
        """
        return self._sorted_order_signatures(
            left_orders,
            ignore_direction=True,
        ) == self._sorted_order_signatures(
            right_orders,
            ignore_direction=True,
        )

    def _orders_have_compatible_transaction_families(
        self,
        recovered_orders: list[OrderExtraction],
        current_orders: list[OrderExtraction],
        *,
        document_text: str | None = None,
    ) -> bool:
        """structured fallback이 현재 결과와 같은 거래 family를 다루는지 본다."""
        if not current_orders:
            return True
        recovered_families = set(self._sorted_order_family_signatures(recovered_orders))
        current_families = set(self._sorted_order_family_signatures(current_orders))
        if recovered_families == current_families:
            return True

        shared_families = recovered_families & current_families
        if not shared_families:
            return False

        replaced_families = recovered_families - current_families
        missing_families = current_families - recovered_families
        if len(replaced_families) != len(missing_families):
            return False

        max_family_count = max(len(recovered_families), len(current_families))
        if max_family_count <= 0 or len(shared_families) < max_family_count - 1:
            return False

        for fund_code, fund_name, _base_date, _t_day, _settle_class in replaced_families:
            if not self._document_has_non_zero_amount_evidence(
                document_text=document_text,
                fund_code=fund_code,
                fund_name=fund_name,
            ):
                return False
        return True

    def _sorted_order_family_signatures(self, orders: list[OrderExtraction]) -> list[tuple[Any, ...]]:
        """방향/금액과 무관한 거래 family 시그니처를 만든다."""
        return sorted(
            {
                (
                    self._normalize_text(order.fund_code),
                    self._normalize_text(order.fund_name),
                    self._normalize_date(order.base_date),
                    order.t_day,
                    self._normalize_text(order.settle_class),
                )
                for order in orders
            }
        )

    def _sorted_order_signatures(
        self,
        orders: list[OrderExtraction],
        *,
        ignore_direction: bool = False,
        ignore_base_date: bool = False,
        ignore_schedule: bool = False,
    ) -> list[tuple[Any, ...]]:
        """주문 목록을 비교 가능한 정규화 signature 리스트로 만든다."""
        return sorted(
            self._order_signature(
                order,
                ignore_direction=ignore_direction,
                ignore_base_date=ignore_base_date,
                ignore_schedule=ignore_schedule,
            )
            for order in orders
        )

    def _order_signature(
        self,
        order: OrderExtraction,
        *,
        ignore_direction: bool = False,
        ignore_base_date: bool = False,
        ignore_schedule: bool = False,
    ) -> tuple[Any, ...]:
        """주문 1건을 비교용 signature로 정규화한다."""
        normalized_amount = self._canonicalize_amount_text(order.transfer_amount)
        normalized_decimal_amount: Decimal | None = None
        if normalized_amount is not None:
            try:
                normalized_decimal_amount = Decimal(normalized_amount.replace(",", ""))
            except InvalidOperation:
                normalized_decimal_amount = None

        if ignore_direction:
            normalized_order_type = None
            if normalized_decimal_amount is not None:
                normalized_decimal_amount = abs(normalized_decimal_amount)
        else:
            normalized_order_type = self._normalize_text(order.order_type)

        normalized_amount_text = (
            self._format_decimal_amount(normalized_decimal_amount)
            if normalized_decimal_amount is not None
            else normalized_amount
        )

        return (
            self._normalize_text(order.fund_code),
            self._normalize_text(order.fund_name),
            None if ignore_base_date else self._normalize_date(order.base_date),
            None if ignore_schedule else order.t_day,
            None if ignore_schedule else self._normalize_text(order.settle_class),
            normalized_order_type,
            normalized_amount_text,
        )

    def _deterministic_order_columns(
        self,
        header: list[str],
        base_date: str,
    ) -> list[tuple[int, SettleClass, int, OrderType | None, bool, bool]]:
        """header를 읽어 amount-bearing column의 방향/settle/t_day를 계산한다."""
        raw_specs: list[tuple[int, int, OrderType | None, bool, int | None, bool]] = []
        future_bucket_order: dict[str, int] = {}
        has_order_context_header = any(self._is_order_context_header_for_deterministic(label) for label in header)

        for index, label in enumerate(header):
            normalized_label = self._normalize_text(label)
            if not normalized_label:
                continue

            future_bucket_key = self._deterministic_future_bucket_key(normalized_label, base_date)
            row_context_priority = (
                self._row_context_amount_priority_for_deterministic(normalized_label)
                if has_order_context_header
                else None
            )
            uses_signed_amount = False if row_context_priority is not None else self._is_execution_evidence(normalized_label)
            explicit_order_type = None if uses_signed_amount else self._order_type_hint_from_header_label(normalized_label)

            # 신한라이프처럼 `2025-11-28 예정금액`처럼 날짜/예정 bucket만 있고
            # 방향은 부호로만 주는 열이 있다. 이런 열은 explicit SUB/RED가 없더라도
            # signed execution-like amount column으로 취급해야 미래 주문 슬롯을 만들 수 있다.
            if explicit_order_type is None and not uses_signed_amount:
                if row_context_priority is not None:
                    pass
                elif future_bucket_key is None:
                    continue
                else:
                    uses_signed_amount = True

            t_day = self._explicit_t_day_from_label(normalized_label)
            if t_day is None:
                if future_bucket_key is None:
                    t_day = 0
                else:
                    # 날짜형/영업일형 bucket은 달력 차이가 아니라 "문서의 열 순서"가 기준이다.
                    # 예를 들어 기준일이 금요일이고 다음 열이 월요일이어도 보통 T+1 의미이므로,
                    # future bucket은 왼쪽에서 오른쪽으로 처음 등장한 순서대로 1, 2, 3...을 배정한다.
                    if future_bucket_key not in future_bucket_order:
                        future_bucket_order[future_bucket_key] = len(future_bucket_order) + 1
                    t_day = future_bucket_order[future_bucket_key]
            raw_specs.append(
                (
                    index,
                    t_day,
                    explicit_order_type,
                    uses_signed_amount,
                    row_context_priority,
                    self._row_context_authoritative_column_uses_signed_amount(normalized_label),
                )
            )

        contextual_best_by_bucket: dict[int, int] = {}
        for _index, t_day, _explicit_order_type, _uses_signed_amount, row_context_priority, _row_context_uses_signed in raw_specs:
            if row_context_priority is None:
                continue
            current_best = contextual_best_by_bucket.get(t_day)
            if current_best is None or row_context_priority < current_best:
                contextual_best_by_bucket[t_day] = row_context_priority

        # 같은 결제 bucket 안에 net execution 컬럼이 있으면, explicit SUB/RED 컬럼은
        # 그 bucket의 보조 정보일 뿐 최종 주문 금액이 아니다. 이 경우 net 컬럼만 남겨야
        # `정산액`, `결제금액`, `순유입금액`이 있는 표를 올바르게 1건으로 읽을 수 있다.
        execution_buckets = {
            t_day
            for _index, t_day, _explicit_order_type, uses_signed_amount, _row_context_priority, _row_context_uses_signed in raw_specs
            if uses_signed_amount and t_day not in contextual_best_by_bucket
        }

        specs: list[tuple[int, SettleClass, int, OrderType | None, bool, bool]] = []
        for index, t_day, explicit_order_type, uses_signed_amount, row_context_priority, row_context_uses_signed in raw_specs:
            contextual_best = contextual_best_by_bucket.get(t_day)
            if contextual_best is not None:
                if row_context_priority is None or row_context_priority != contextual_best:
                    continue
                specs.append(
                    (
                        index,
                        SettleClass.CONFIRMED if t_day == 0 else SettleClass.PENDING,
                        t_day,
                        None,
                        row_context_uses_signed,
                        not row_context_uses_signed,
                    )
                )
                continue
            if t_day in execution_buckets and not uses_signed_amount:
                continue
            specs.append(
                (
                    index,
                    SettleClass.CONFIRMED if t_day == 0 else SettleClass.PENDING,
                    t_day,
                    explicit_order_type,
                    uses_signed_amount,
                    False,
                )
            )
        return specs

    @staticmethod
    def _explicit_t_day_from_label(label: str) -> int | None:
        """label에 `T+N`이 명시돼 있으면 그 값을 그대로 사용한다."""
        match = re.search(r"t\s*\+\s*(\d+)", label.lower())
        if not match:
            return None
        return int(match.group(1))

    def _explicit_t_day_from_evidence_label(
        self,
        label: str,
        base_date: str | None,
    ) -> int | None:
        """evidence label이 직접 가리키는 future bucket을 보수적으로 계산한다."""
        explicit_t_day = self._explicit_t_day_from_label(label)
        if explicit_t_day is not None:
            return explicit_t_day
        if base_date is None:
            return None

        month_day = self._extract_month_day_from_label(label)
        if month_day is None:
            return None

        normalized_base_date = self._normalize_date(base_date)
        if normalized_base_date is None:
            return None
        try:
            base_datetime = datetime.strptime(normalized_base_date, "%Y-%m-%d")
            target_datetime = datetime(base_datetime.year, month_day[0], month_day[1])
        except ValueError:
            return None

        delta = (target_datetime.date() - base_datetime.date()).days
        if delta <= 0:
            return None
        return delta

    def _deterministic_future_bucket_key(self, label: str, base_date: str) -> str | None:
        """date/business-day label을 future bucket 식별자로 정규화한다.

        반환값은 실제 일수 차이가 아니라 "같은 bucket끼리 묶기 위한 키"다.
        이후 t_day는 이 키들이 문서에 등장한 순서대로 1, 2, 3...을 배정한다.
        """
        month_day = self._extract_month_day_from_label(label)
        if month_day is not None:
            base_month, base_day = self._base_month_day(base_date)
            if month_day == (base_month, base_day):
                return None
            return f"DATE:{month_day[0]:02d}-{month_day[1]:02d}"

        lowered = label.lower()
        business_day_match = re.search(r"(익+영업일|제\s*\d+\s*영업일)", lowered)
        if business_day_match:
            return f"BUSINESS:{business_day_match.group(1)}"

        if self._evidence_implies_schedule(lowered):
            generic_key = lowered
            for token in ("투입", "설정", "입금", "매입", "인출", "해지", "출금", "환매", "buy", "sell", "subscription", "redemption"):
                generic_key = generic_key.replace(token, "")
            generic_key = re.sub(r"\s+", "", generic_key)
            return f"SCHEDULE:{generic_key}"
        return None

    @staticmethod
    def _extract_month_day_from_label(label: str) -> tuple[int, int] | None:
        """label에서 `3월19일` 같은 month/day 표기를 추출한다."""
        match = re.search(r"(\d{1,2})\s*월\s*(\d{1,2})\s*일", label)
        if not match:
            return None
        return int(match.group(1)), int(match.group(2))

    @staticmethod
    def _base_month_day(base_date: str) -> tuple[int, int]:
        """`YYYY-MM-DD` 기준일에서 month/day만 추출한다."""
        base = datetime.strptime(base_date, "%Y-%m-%d")
        return base.month, base.day

    @staticmethod
    def _detect_base_date_from_text(raw_text: str, *, include_asia_seoul: bool = True) -> str | None:
        """raw_text 앞부분에서 문서 기준일을 추출한다."""
        if include_asia_seoul:
            seoul_match = re.search(
                r"Date\s*\(Asia/Seoul\)\s*:\s*(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})",
                raw_text,
                flags=re.IGNORECASE,
            )
            if seoul_match:
                year, month, day = seoul_match.groups()
                return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"

        korean_match = re.search(r"(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일", raw_text)
        if korean_match:
            year, month, day = korean_match.groups()
            return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"

        header_lines = FundOrderExtractor._base_date_header_lines(raw_text)
        header_text = "\n".join(header_lines)
        compact_labeled = re.search(
            r"(?:기준일(?:자)?|결제일|document\s*date|settlement\s*date)\s*[:：]?\s*(\d{4})\s*[./-]?\s*(\d{1,2})\s*[./-]?\s*(\d{1,2})",
            header_text,
            flags=re.IGNORECASE,
        )
        if compact_labeled:
            year, month, day = compact_labeled.groups()
            return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"

        line_start_pattern = (
            r"(?im)^\s*date(?:\s*\(asia/seoul\))?\s*[:：]?\s*(\d{4})\s*[./-]?\s*(\d{1,2})\s*[./-]?\s*(\d{1,2})\s*$"
            if include_asia_seoul
            else r"(?im)^\s*date\s*[:：]?\s*(\d{4})\s*[./-]?\s*(\d{1,2})\s*[./-]?\s*(\d{1,2})\s*$"
        )
        line_start_date = re.search(line_start_pattern, header_text)
        if line_start_date:
            year, month, day = line_start_date.groups()
            return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"

        for line in header_lines:
            lowered_line = line.lower()
            if re.search(r"(closing\s*date|prior\s*date|nav\s*date)", lowered_line, flags=re.IGNORECASE):
                continue
            short_match = re.search(r"\b(\d{1,2}-[A-Za-z]{3}-\d{2})\b", line)
            if short_match:
                normalized = FundOrderExtractor._normalize_short_english_date(short_match.group(1))
                if normalized is not None:
                    return normalized

        digits = re.search(r"(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})", raw_text)
        if digits:
            year, month, day = digits.groups()
            return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
        return None

    @classmethod
    def _detect_title_adjacent_base_date_from_text(cls, raw_text: str) -> str | None:
        """문서 상단 제목 바로 아래의 단독 날짜를 강한 기준일 힌트로 인식한다.

        ABL처럼 제목 아래에 문서 기준일이 단독으로 있고, 표 헤더 안에는 `T-1 NAV`
        날짜가 별도로 들어가는 양식이 있다. 이 경우 stage 2가 표 헤더의 날짜를
        잘못 고를 수 있으므로, 문서 상단의 standalone date를 별도 high-confidence
        hint로 보관해 최종 결과를 보정한다.
        """
        meaningful_lines = [cls._normalize_text(line) for line in raw_text.splitlines()]
        meaningful_lines = [line for line in meaningful_lines if line][:12]
        if len(meaningful_lines) < 3:
            return None

        for index, line in enumerate(meaningful_lines[:6]):
            cells = [cls._normalize_text(cell) for cell in line.split("|")]
            non_empty_cells = [cell for cell in cells if cell]
            if len(non_empty_cells) != 1:
                continue

            date_match = re.fullmatch(r"(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})", non_empty_cells[0])
            if not date_match:
                continue

            prev_line = meaningful_lines[index - 1] if index > 0 else ""
            if not prev_line or re.search(r"(기준일|결제일|trade\s*date|settlement\s*date)", prev_line, flags=re.IGNORECASE):
                continue

            following_context = " ".join(meaningful_lines[index + 1 : index + 6]).lower()
            if not any(token in following_context for token in ("t일", "t+1", "순유입", "nav", "운용지시", "펀드코드")):
                continue

            year, month, day = date_match.groups()
            return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
        return None

    @classmethod
    def _date_looks_like_prior_reference_in_text(cls, raw_text: str, date_value: str | None) -> bool:
        """특정 날짜가 문서에서 `T-1 NAV` 같은 참고용 날짜로만 보이는지 확인한다.

        ABL 문서는 제목 아래 단독 날짜가 실제 기준일이고, 표 헤더 안의 별도 날짜는
        `T-1 NAV` 참고 날짜였다. 이런 경우에만 상단 날짜 보정을 허용해야지,
        임의의 보고서 작성일/안내용 날짜가 실제 결제일을 덮어쓰면 안 된다.
        """
        if not raw_text or not date_value:
            return False

        normalized_lines = [cls._normalize_text(line) for line in raw_text.splitlines()]
        for index, line in enumerate(normalized_lines):
            if date_value not in line:
                continue
            window = " ".join(normalized_lines[max(0, index - 1) : min(len(normalized_lines), index + 3)]).lower()
            if re.search(r"t\s*-\s*1", window) and "nav" in window:
                return True
            if "전일" in window and "nav" in window:
                return True
        return False

    @classmethod
    def _date_looks_like_reference_only_in_text(cls, raw_text: str, date_value: str | None) -> bool:
        """특정 날짜가 `Closing Date` 같은 reference artifact로만 보이는지 본다."""
        if not raw_text or not date_value:
            return False
        if cls._date_looks_like_prior_reference_in_text(raw_text, date_value):
            return True

        normalized_lines = [cls._normalize_text(line) for line in raw_text.splitlines()]
        saw_reference_label = False
        for index, line in enumerate(normalized_lines):
            if date_value not in line:
                continue
            same_line = line.lower()
            if re.search(r"(결제일|settlement\s*date|기준일(?:자)?|document\s*date)", same_line, flags=re.IGNORECASE):
                return False
            if re.search(r"(closing\s*date|전일|prior\s*date|nav\s*date)", same_line, flags=re.IGNORECASE):
                saw_reference_label = True
                continue
            window = " ".join(normalized_lines[max(0, index - 1) : min(len(normalized_lines), index + 3)]).lower()
            if re.search(r"(결제일|settlement\s*date|기준일(?:자)?|document\s*date)", window, flags=re.IGNORECASE):
                return False
            if re.search(r"(closing\s*date|전일|prior\s*date|nav\s*date)", window, flags=re.IGNORECASE):
                saw_reference_label = True
        return saw_reference_label

    @classmethod
    def _date_looks_like_actual_base_date_hint_in_text(cls, raw_text: str, date_value: str | None) -> bool:
        """특정 날짜가 실제 기준일 힌트로 문서 안에 보이는지 확인한다."""
        if not raw_text or not date_value:
            return False
        title_adjacent_base_date = cls._detect_title_adjacent_base_date_from_text(raw_text)
        if title_adjacent_base_date == date_value:
            return True

        normalized_lines = [cls._normalize_text(line) for line in raw_text.splitlines()]
        for index, line in enumerate(normalized_lines):
            if date_value not in line:
                continue
            same_line = line.lower()
            if re.search(r"(결제일|settlement\s*date|기준일(?:자)?|document\s*date)", same_line, flags=re.IGNORECASE):
                return True
            window = " ".join(normalized_lines[max(0, index - 1) : min(len(normalized_lines), index + 3)]).lower()
            if re.search(r"(결제일|settlement\s*date|기준일(?:자)?|document\s*date)", window, flags=re.IGNORECASE):
                return True
        return False

    def _markdown_fund_name_index(self, header: list[str]) -> int | None:
        """header에서 펀드명 열 인덱스를 찾는다."""
        for index, label in enumerate(header):
            lowered = label.lower()
            if "펀드명" in label or "fund name" in lowered:
                return index
        return None

    @staticmethod
    def _looks_like_total_row(fund_code: str, fund_name: str) -> bool:
        """합계/총계 행처럼 실제 주문이 아닌 행을 건너뛴다."""
        lowered_code = fund_code.lower()
        lowered_name = fund_name.lower()
        total_tokens = ("합계", "총계", "subtotal", "total")
        if any(token in lowered_code for token in total_tokens) or any(token in lowered_name for token in total_tokens):
            return True
        count_summary_pattern = re.compile(r"\d+\s*개\s*펀드")
        return bool(count_summary_pattern.search(lowered_code) or count_summary_pattern.search(lowered_name))

    @staticmethod
    def _align_amount_with_document_order_type(amount: str, order_type: OrderType) -> str:
        """문서 section 힌트가 명확할 때는 금액 부호도 그 방향에 맞춰 정리한다.

        sectioned 문서의 원문 금액은 보통 절대값인데, LLM stage 4가 잘못된 section 문맥을
        참조해 음수로 반환하는 경우가 있다. 이때는 문서 section 힌트가 더 신뢰할 만하므로
        부호를 제거한 절대값 문자열로 되돌리고, 최종 signed amount 는 order_type 로 다시 만든다.
        """
        text = amount.strip()
        if text.startswith(("+", "-")):
            return text[1:]
        return text

    @staticmethod
    def _section_order_type_hint(line: str) -> OrderType | None:
        """섹션 제목/헤더 줄에서 SUB/RED 방향 힌트를 추출한다."""
        normalized = line.strip().lower()
        if re.fullmatch(r"(?:\d+\.\s*)?subscription", normalized):
            return OrderType.SUB
        if re.fullmatch(r"(?:\d+\.\s*)?redemption", normalized):
            return OrderType.RED
        compact = re.sub(r"\s+", "", normalized)
        if "거래종류" in compact:
            if "펀드설정" in compact:
                return OrderType.SUB
            if "펀드해지" in compact:
                return OrderType.RED
        return None

    @staticmethod
    def _line_matches_fund_amount(line: str, fund_code: str, amount: str) -> bool:
        """한 줄이 특정 `fund_code + amount` 후보를 실제로 포함하는지 확인한다."""
        code_pattern = rf"(?<![A-Z0-9]){re.escape(fund_code)}(?![A-Z0-9])"
        if not re.search(code_pattern, line):
            return False

        target_amount = canonicalize_transfer_amount(amount)
        if target_amount is None:
            return False

        for match in re.finditer(r"[+-]?\d[\d,]*(?:\.\d+)?", line):
            matched_amount = canonicalize_transfer_amount(match.group(0))
            if matched_amount == target_amount:
                return True
        return False

    def _invoke_stage(
        self,
        prompt_bundle: PromptBundle,
        stage: StageDefinition,
        document_text: str,
        input_items: list[dict[str, Any]] | None,
        batch_index: int,
        response_model: type[T],
        *,
        counterparty_guidance: str | None = None,
    ) -> "_StageInvocation":
        """stage 1회분 LLM 호출을 수행한다.

        우선 provider 의 JSON mode 를 사용하고, 실패하면 prompt-only 로 한 번 더 시도한다.
        두 번 다 실패하면 parsed=None 으로 반환하고, 상위 호출부가 issue/artifact 처리한다.
        """
        retry_context = _ACTIVE_STAGE_RETRY_CONTEXT.get()
        user_prompt = (
            self._build_retry_user_prompt(
                prompt_bundle=prompt_bundle,
                stage=stage,
                document_text=document_text,
                input_items=input_items,
                retry_context=retry_context,
                counterparty_guidance=counterparty_guidance,
            )
            if retry_context is not None
            else self._build_user_prompt(
                prompt_bundle=prompt_bundle,
                stage=stage,
                document_text=document_text,
                input_items=input_items,
                counterparty_guidance=counterparty_guidance,
            )
        )
        messages = [
            SystemMessage(content=prompt_bundle.system_prompt),
            HumanMessage(content=user_prompt),
        ]

        raw_response = ""
        json_response, json_error = self._invoke_with_retries(
            stage_name=stage.name,
            batch_index=batch_index,
            messages=messages,
            use_json_mode=True,
        )
        if json_response is not None:
            raw_response = json_response
            parsed = self._parse_stage_response(raw_response, response_model)
            if parsed is not None:
                return _StageInvocation(parsed=parsed, raw_response=raw_response)
            logger.warning(
                "JSON response mode returned an invalid payload for stage=%s batch=%s; falling back to prompt-only mode",
                stage.name,
                batch_index,
            )
        elif json_error is not None:
            logger.warning(
                "JSON response mode failed for stage=%s batch=%s after %s attempt(s): %s",
                stage.name,
                batch_index,
                self.llm_retry_attempts,
                json_error,
            )

        prompt_response, prompt_error = self._invoke_with_retries(
            stage_name=stage.name,
            batch_index=batch_index,
            messages=messages,
            use_json_mode=False,
        )
        if prompt_response is None:
            if prompt_error is not None:
                logger.warning(
                    "Prompt-only mode failed for stage=%s batch=%s after %s attempt(s): %s",
                    stage.name,
                    batch_index,
                    self.llm_retry_attempts,
                    prompt_error,
                )
            return _StageInvocation(parsed=None, raw_response=raw_response)

        raw_response = prompt_response

        parsed = self._parse_stage_response(raw_response, response_model)
        return _StageInvocation(parsed=parsed, raw_response=raw_response)

    def _invoke_with_retries(
        self,
        *,
        stage_name: str,
        batch_index: int,
        messages: list[Any],
        use_json_mode: bool,
    ) -> tuple[str | None, Exception | None]:
        """LLM 호출을 짧게 재시도해 일시적인 연결 흔들림을 흡수한다."""
        mode_label = "JSON response mode" if use_json_mode else "prompt-only mode"
        last_error: Exception | None = None
        log_path = _ACTIVE_EXTRACT_LOG_PATH.get()

        for attempt in range(1, self.llm_retry_attempts + 1):
            if attempt > 1:
                _record_transport_retry_attempt(stage_name)
            try:
                if attempt == 1:
                    if use_json_mode:
                        logger.info("Calling LLM for stage=%s batch=%s in %s", stage_name, batch_index, mode_label)
                    else:
                        logger.info("Retrying LLM for stage=%s batch=%s in %s", stage_name, batch_index, mode_label)
                else:
                    logger.info(
                        "Retrying LLM for stage=%s batch=%s in %s (attempt %s/%s)",
                        stage_name,
                        batch_index,
                        mode_label,
                        attempt,
                        self.llm_retry_attempts,
                    )

                system_prompt, user_prompt = _split_system_user_from_messages(messages)
                mode_tag = "json_object" if use_json_mode else "prompt_only"
                _append_local_llm_prompt_log(
                    log_path,
                    header=(
                        f"LLM prompt stage={stage_name} batch={batch_index} "
                        f"mode={mode_tag} attempt={attempt}"
                    ),
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                )

                stage_llm = self.llm.bind(max_tokens=self._stage_max_tokens(stage_name))
                if use_json_mode:
                    message = stage_llm.bind(response_format={"type": "json_object"}).invoke(messages)
                else:
                    message = stage_llm.invoke(messages)
                response_text = self._message_to_text(message)
                _append_local_llm_response_log(
                    log_path,
                    header=(
                        f"LLM response stage={stage_name} batch={batch_index} "
                        f"mode={mode_tag} attempt={attempt}"
                    ),
                    response_text=response_text,
                )
                return response_text, None
            except Exception as exc:  # pragma: no cover - provider dependent
                last_error = exc
                if attempt >= self.llm_retry_attempts:
                    break
                delay_seconds = self.llm_retry_backoff_seconds * attempt
                logger.warning(
                    "%s failed for stage=%s batch=%s attempt=%s/%s: %s; retrying in %.1fs",
                    mode_label,
                    stage_name,
                    batch_index,
                    attempt,
                    self.llm_retry_attempts,
                    exc,
                    delay_seconds,
                )
                if delay_seconds > 0:
                    time.sleep(delay_seconds)

        return None, last_error

    def _parse_stage_response(self, raw_response: str, response_model: type[T]) -> T | None:
        """LLM raw response를 stage 모델로 파싱하고, 실패 시 None을 돌려준다."""
        payload = self._load_json_payload(raw_response)
        if payload is None:
            return None
        try:
            return response_model.model_validate(payload)
        except ValidationError:
            return None

    def _stage(self, prompt_bundle: PromptBundle, stage_name: str) -> StageDefinition:
        """현재 요청의 prompt bundle 에서 stage 정의를 가져온다."""
        try:
            return prompt_bundle.stages[stage_name]
        except KeyError as exc:
            raise ValueError(f"Unknown prompt stage: {stage_name}") from exc

    def _ensure_prompt_lock(self) -> RLock:
        """테스트 환경(object.__new__)에서도 안전하게 prompt lock 을 보장한다."""
        prompt_lock = getattr(self, "_prompt_lock", None)
        if prompt_lock is None:
            prompt_lock = RLock()
            self._prompt_lock = prompt_lock
        return prompt_lock

    def _has_valid_prompt_bundle(self) -> bool:
        """마지막 정상 prompt bundle이 현재 인스턴스에 살아 있는지 확인한다."""
        # 마지막 정상 prompt bundle 이 있는지 확인하는 함수다.
        # reload 실패 시 fallback 가능 여부를 판단하는 데 사용한다.
        prompt_bundle = getattr(self, "prompt_bundle", None)
        return isinstance(prompt_bundle, PromptBundle) and bool(
            prompt_bundle.system_prompt
            and prompt_bundle.user_prompt_template
            and prompt_bundle.retry_user_prompt_template
            and prompt_bundle.stages
        )

    def _refresh_prompt_bundle(self, force: bool = False) -> PromptBundle:
        """필요할 때 YAML prompt 를 다시 읽고, 실패 시 마지막 정상 버전으로 버틴다.

        운영 환경에서는 YAML 수정 중간 상태가 잠깐 깨질 수 있다.
        그때 매 요청이 바로 죽어 버리면 운영성이 너무 나빠지므로,
        이미 정상 bundle 이 있다면 warning 만 남기고 그 버전으로 계속 처리한다.
        """
        with self._ensure_prompt_lock():
            try:
                current_mtime_ns = self.prompt_path.stat().st_mtime_ns
            except OSError as exc:
                if self._has_valid_prompt_bundle():
                    logger.warning(
                        "Failed to stat extraction prompts from %s; continuing with last known good prompts: %s",
                        self.prompt_path,
                        exc,
                    )
                    return self.prompt_bundle
                raise

            # 파일 수정 시각이 같고 현재 bundle 이 유효하면 재로딩하지 않는다.
            if not force and current_mtime_ns == self._prompt_mtime_ns and self._has_valid_prompt_bundle():
                return self.prompt_bundle

            try:
                prompt_bundle = _load_prompt_bundle(self.prompt_path)
            except Exception as exc:
                # prompt 수정이 잘못된 상태여도, 마지막 정상 bundle 이 있으면 추출 자체는
                # 계속 가능하게 두는 편이 운영상 안전하다.
                if self._has_valid_prompt_bundle():
                    logger.warning(
                        "Failed to reload extraction prompts from %s; continuing with last known good prompts: %s",
                        self.prompt_path,
                        exc,
                    )
                    return self.prompt_bundle
                raise

            self.prompt_bundle = prompt_bundle
            self.system_prompt = prompt_bundle.system_prompt
            self._prompt_mtime_ns = current_mtime_ns
            logger.info("Loaded extraction prompts from %s", self.prompt_path)
            return prompt_bundle

    def _build_user_prompt(
        self,
        prompt_bundle: PromptBundle,
        stage: StageDefinition,
        document_text: str,
        input_items: list[dict[str, Any]] | None,
        counterparty_guidance: str | None = None,
    ) -> str:
        """YAML template 에 현재 stage 정보와 input_items/document_text 를 주입한다.

        공통 wrapper 와 stage 지시문이 모두 YAML 로 빠져 있기 때문에,
        이 함수는 문구를 생성한다기보다 "현재 요청 데이터로 template 를 렌더링"하는 역할이다.
        """
        input_block = "null" if input_items is None else json.dumps(input_items, ensure_ascii=False, indent=2)
        rendered_counterparty_guidance = self._render_counterparty_guidance_for_stage(
            stage_name=stage.name,
            counterparty_guidance=counterparty_guidance,
        )
        try:
            rendered_prompt = prompt_bundle.user_prompt_template.format(
                stage_number=stage.number,
                total_stage_count=len(prompt_bundle.stages),
                stage_goal=stage.goal,
                stage_name=stage.name,
                counterparty_guidance=rendered_counterparty_guidance,
                stage_instructions=stage.instructions,
                output_contract=stage.output_contract,
                input_items_json=input_block,
                document_text=document_text,
            )
            return self._prepend_prompt_task_trace(rendered_prompt)
        except (KeyError, ValueError) as exc:
            raise ValueError(f"Prompt template rendering failed: {exc}") from exc

    def _build_retry_user_prompt(
        self,
        prompt_bundle: PromptBundle,
        stage: StageDefinition,
        document_text: str,
        input_items: list[dict[str, Any]] | None,
        retry_context: dict[str, Any],
        counterparty_guidance: str | None = None,
    ) -> str:
        """Retry 전용 template에 issue/직전 응답/focus item을 주입한다."""
        input_block = "null" if input_items is None else json.dumps(input_items, ensure_ascii=False, indent=2)
        rendered_counterparty_guidance = self._render_counterparty_guidance_for_stage(
            stage_name=stage.name,
            counterparty_guidance=counterparty_guidance,
        )
        try:
            rendered_prompt = prompt_bundle.retry_user_prompt_template.format(
                stage_number=stage.number,
                total_stage_count=len(prompt_bundle.stages),
                stage_goal=stage.goal,
                stage_name=stage.name,
                counterparty_guidance=rendered_counterparty_guidance,
                stage_instructions=stage.instructions,
                retry_instructions=stage.retry_instructions,
                output_contract=stage.output_contract,
                retry_target_issues_json=json.dumps(
                    retry_context.get("target_issues", []),
                    ensure_ascii=False,
                    indent=2,
                ),
                previous_output_items_json=json.dumps(
                    retry_context.get("previous_output_items", []),
                    ensure_ascii=False,
                    indent=2,
                ),
                retry_focus_items_json=json.dumps(
                    retry_context.get("focus_items", []),
                    ensure_ascii=False,
                    indent=2,
                ),
                previous_reason_summary_text=retry_context.get("previous_reason_summary", ""),
                input_items_json=input_block,
                document_text=document_text,
                retry_attempt_number=retry_context.get("attempt_number", 1),
                retry_max_attempts=retry_context.get("max_attempts", self.llm_stage_issue_retry_attempts),
            )
            return self._prepend_prompt_task_trace(rendered_prompt)
        except (KeyError, ValueError) as exc:
            raise ValueError(f"Retry prompt template rendering failed: {exc}") from exc

    def _prepend_prompt_task_trace(self, rendered_prompt: str) -> str:
        """로컬 user prompt에도 추적용 task_id 헤더를 추가한다."""
        trace_task_id = self._prompt_trace_task_id()
        if not trace_task_id:
            return rendered_prompt
        return f"Trace task_id: {trace_task_id}\n\n{rendered_prompt}"

    def _prompt_trace_task_id(self) -> str | None:
        """로컬 실행에서 프롬프트 추적에 사용할 task_id 문자열을 만든다."""
        active_log_path = _ACTIVE_EXTRACT_LOG_PATH.get()
        if active_log_path is None:
            return None
        stem = active_log_path.stem
        if stem.endswith("_llm_pipeline"):
            stem = stem[: -len("_llm_pipeline")]
        stem = stem.strip()
        if not stem:
            return None
        return stem

    @staticmethod
    def _render_counterparty_guidance_for_stage(
        *,
        stage_name: str,
        counterparty_guidance: str | None,
    ) -> str:
        """거래처 특징 힌트를 stage 민감도에 맞게 렌더링한다.

        기준일, 금액, 방향 판단 단계는 거래처별 양식 차이의 영향을 가장 크게 받는다.
        그래서 이 단계에서는 단순 참고 문구가 아니라 "해석 우선순위"에 가깝게 읽히도록
        짧은 강조 래퍼를 붙인다. 다만 원문 근거보다 우선하면 안 되므로 그 제한도 함께 준다.
        """
        guidance = (counterparty_guidance or "").strip()
        if not guidance:
            return ""
        if stage_name not in SENSITIVE_COUNTERPARTY_GUIDANCE_STAGES:
            return guidance
        return (
            "This counterparty-specific guidance is especially important for this stage.\n"
            "- Prefer these counterparty-specific rules when multiple interpretations are plausible.\n"
            "- Do not override direct source evidence.\n\n"
            f"{guidance}"
        )

    @staticmethod
    def _message_to_text(message: Any) -> str:
        """provider message 객체를 최종 텍스트 본문으로 평탄화한다."""
        content = getattr(message, "content", message)
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                if isinstance(item, str):
                    parts.append(item)
                    continue
                if isinstance(item, dict):
                    text = item.get("text")
                    if text:
                        parts.append(str(text))
            return "\n".join(part for part in parts if part)
        return str(content)

    def _load_json_payload(self, raw_response: str) -> dict[str, Any] | None:
        """LLM raw response 안에서 JSON object payload를 찾아 dict로 로드한다."""
        candidate = self._extract_json_candidate(raw_response)
        if candidate is None:
            return None
        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError:
            return None
        if isinstance(payload, dict):
            return payload
        return None

    def _extract_json_candidate(self, raw_response: str) -> str | None:
        """fenced code block/잡음 섞인 응답에서 가장 그럴듯한 JSON object를 꺼낸다."""
        stripped = self._strip_reasoning(raw_response).strip()
        if not stripped:
            return None

        fenced_match = re.search(r"```(?:json)?\s*(\{.*\})\s*```", stripped, flags=re.DOTALL)
        if fenced_match:
            stripped = fenced_match.group(1).strip()

        if stripped.startswith("{") and stripped.endswith("}"):
            return stripped

        candidates = self._balanced_json_objects(stripped)
        if not candidates:
            return None
        return max(candidates, key=len)

    @staticmethod
    def _strip_reasoning(text: str) -> str:
        """`<think>`나 fenced code block wrapper를 제거해 JSON 추출 전 본문만 남긴다."""
        stripped = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL | re.IGNORECASE).strip()
        stripped = re.sub(r"^```json\s*", "", stripped, flags=re.IGNORECASE)
        stripped = re.sub(r"^```\s*", "", stripped)
        stripped = re.sub(r"\s*```$", "", stripped)
        return stripped.strip()

    @staticmethod
    def _balanced_json_objects(text: str) -> list[str]:
        """문자열 안에서 균형 잡힌 JSON object 후보들을 모두 찾는다."""
        # 모델이 JSON 앞뒤에 잡음을 붙였을 때를 대비해,
        # 문자열 내부 brace 는 무시하면서 균형 잡힌 JSON object 후보를 찾는다.
        results: list[str] = []
        for start_index, char in enumerate(text):
            if char != "{":
                continue
            depth = 0
            in_string = False
            escape = False
            for end_index in range(start_index, len(text)):
                current = text[end_index]
                if in_string:
                    if escape:
                        escape = False
                    elif current == "\\":
                        escape = True
                    elif current == '"':
                        in_string = False
                    continue
                if current == '"':
                    in_string = True
                    continue
                if current == "{":
                    depth += 1
                elif current == "}":
                    depth -= 1
                    if depth == 0:
                        candidate = text[start_index : end_index + 1]
                        try:
                            json.loads(candidate)
                        except json.JSONDecodeError:
                            break
                        results.append(candidate)
                        break
        return results

    def _compose_document_context(self, primary_text: str, raw_text: str | None) -> str:
        """markdown 본문에 raw_text 백업을 함께 붙여 LLM 입력 문맥을 만든다.

        markdown 은 레이아웃 전달에 좋고, raw_text 는 원문 보존에 좋다.
        둘을 같이 주면 표 구조는 markdown 에서 읽고, 애매한 줄바꿈/헤더는 raw_text 로
        보완할 수 있다.
        """
        primary = primary_text.strip()
        if not raw_text or not raw_text.strip():
            return primary

        raw_backup = raw_text.strip()
        if len(raw_backup) > self.llm_chunk_size_chars:
            raw_backup = f"{raw_backup[: self.llm_chunk_size_chars]}\n...\n{raw_backup[-2000:]}"

        if raw_backup in primary:
            return primary

        return (
            "Structured markdown view:\n"
            f"{primary}\n\n"
            "Raw text backup:\n"
            f"```text\n{raw_backup}\n```"
        )

    def _append_stage_partial_issue(
        self,
        stage: StageDefinition,
        document_text: str | None,
        input_items: list[dict[str, Any]],
        output_items: list[Any],
        issues: list[str],
    ) -> None:
        """Mark stages that returned only part of the requested batch.

        A valid JSON payload is not enough by itself. If a stage receives 12 input
        items and comes back with only 9 resolved items, we keep that as a partial
        failure signal so the service can block persistence.
        """
        issue_code = self._stage_partial_issue_code(
            stage=stage,
            document_text=document_text,
            input_items=input_items,
            output_items=output_items,
        )
        if issue_code is not None:
            self._append_unique(issues, issue_code)

    def _stage_partial_issue_code(
        self,
        *,
        stage: StageDefinition,
        document_text: str | None = None,
        input_items: list[dict[str, Any]] | None,
        output_items: list[Any],
    ) -> str | None:
        """stage partial 여부를 코드로 계산한다.

        partial은 "일부 row가 빠졌다"만 의미하지 않는다.
        - expected row 누락
        - 요청하지 않은 extra output
        - 같은 stage identity의 충돌 duplicate
        - `t_day` family 안의 duplicate evidence ghost slot
        모두 같은 stage incomplete 신호로 본다.
        """
        if not input_items:
            return None

        issue_map = {
            "base_date": "BASE_DATE_STAGE_PARTIAL",
            "t_day": "T_DAY_STAGE_PARTIAL",
            "transfer_amount": "TRANSFER_AMOUNT_STAGE_PARTIAL",
            "settle_class": "SETTLE_CLASS_STAGE_PARTIAL",
            "order_type": "ORDER_TYPE_STAGE_PARTIAL",
        }
        issue_code = issue_map.get(stage.name)
        if issue_code is None:
            return None

        normalized_output_items = [
            item.model_dump(mode="json") if hasattr(item, "model_dump") else item
            for item in output_items
        ]
        expected = {self._stage_partial_item_signature(stage.name, item) for item in input_items}
        actual = {
            self._stage_partial_item_signature(
                stage.name,
                item,
            )
            for item in normalized_output_items
        }
        if not expected.issubset(actual):
            return issue_code
        if not actual.issubset(expected):
            return issue_code
        if not self._stage_items_are_complete(stage.name, normalized_output_items):
            return issue_code
        if self._stage_output_has_duplicate_signature(stage.name, normalized_output_items):
            return issue_code
        if stage.name == "t_day" and self._t_day_output_has_duplicate_evidence(normalized_output_items):
            return issue_code
        if (
            stage.name == "t_day"
            and document_text
            and self._t_day_has_missing_document_sibling_slots(
                document_text=document_text,
                input_items=input_items,
                output_items=normalized_output_items,
            )
        ):
            return issue_code
        return None

    def _t_day_has_missing_document_sibling_slots(
        self,
        *,
        document_text: str,
        input_items: list[dict[str, Any]],
        output_items: list[dict[str, Any]],
    ) -> bool:
        """문서 표가 요구하는 sibling slot이 stage 3 output에서 조용히 누락됐는지 본다."""
        expected_slots = self._derive_expected_t_day_family_slots(document_text, input_items)
        if not expected_slots:
            return False
        actual_slots = self._derive_actual_t_day_family_slots(output_items)
        for family_key, family_expected_slots in expected_slots.items():
            if not family_expected_slots:
                continue
            if not family_expected_slots.issubset(actual_slots.get(family_key, set())):
                return True
        return False

    def _stage_output_has_duplicate_signature(self, stage_name: str, output_items: list[Any]) -> bool:
        """동일 stage identity가 충돌하는 output 안에서 중복되면 partial로 본다."""
        seen: dict[tuple[Any, ...], dict[str, Any]] = {}
        for item in output_items:
            normalized_item = item.model_dump(mode="json") if hasattr(item, "model_dump") else item
            signature = self._stage_item_signature(stage_name, normalized_item)
            previous_item = seen.get(signature)
            if previous_item is not None:
                if previous_item != normalized_item:
                    return True
                continue
            seen[signature] = normalized_item
        return False

    def _t_day_output_has_duplicate_evidence(self, output_items: list[Any]) -> bool:
        """Stage 3 output 안에서 같은 family의 duplicate slot hallucination을 감지한다."""
        grouped: dict[tuple[Any, ...], dict[str, tuple[Any, ...]]] = {}
        for item in output_items:
            normalized_item = item.model_dump(mode="json") if hasattr(item, "model_dump") else item
            family_key = self._stage_partial_item_signature("t_day", normalized_item)
            evidence_label = self._normalize_text(normalized_item.get("evidence_label"))
            if not evidence_label:
                continue
            signature = self._stage_item_signature("t_day", normalized_item)
            seen = grouped.setdefault(family_key, {})
            previous_signature = seen.get(evidence_label)
            if previous_signature is not None and previous_signature != signature:
                return True
            seen[evidence_label] = signature
        return False

    @staticmethod
    def _normalize_text(value: str | None) -> str:
        """공백 흔들림을 줄이기 위한 가장 기본적인 텍스트 정규화다."""
        if value is None:
            return ""
        return re.sub(r"\s+", " ", value).strip()

    @staticmethod
    def _normalize_date(value: str | None) -> str | None:
        """여러 날짜 표현을 `YYYY-MM-DD` 형식으로 정규화한다."""
        if value is None:
            return None
        text = value.strip()
        if not text:
            return None
        digits = re.sub(r"[^\d]", "", text)
        if len(digits) == 8:
            return f"{digits[0:4]}-{digits[4:6]}-{digits[6:8]}"
        match = re.search(r"(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})", text)
        if match:
            year, month, day = match.groups()
            return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
        return None

    @staticmethod
    def _normalize_amount(value: str | None) -> str | None:
        """금액 문자열을 내부 표준 표현으로 정리하고, 무효값은 None으로 만든다."""
        if value is None:
            return None
        text = value.strip()
        if not text or text in {"-", "--", "N/A", "n/a", "null", "None"}:
            return None
        text = text.replace(" ", "")
        if not re.fullmatch(r"[+-]?\d[\d,]*(?:\.\d+)?", text):
            return None
        return text

    @staticmethod
    def _canonicalize_amount_text(value: str | None) -> str | None:
        """금액 문자열을 artifact-aware canonical text로 정규화한다."""
        normalized = FundOrderExtractor._normalize_amount(value)
        if normalized is None:
            return None
        return canonicalize_transfer_amount(normalized) or normalized

    @staticmethod
    def _format_source_amount_text(value: str | None) -> str | None:
        """지시서 표기 소수 자릿수를 보존한 저장용 금액 문자열로 정리한다."""
        normalized = FundOrderExtractor._normalize_amount(value)
        if normalized is None:
            return None
        return format_source_transfer_amount(normalized) or normalized

    @staticmethod
    def _unsigned_amount_identity(value: str | None) -> str | None:
        """금액 identity 비교용 unsigned canonical text를 만든다."""
        canonical_amount = FundOrderExtractor._canonicalize_amount_text(value)
        if canonical_amount is None:
            return None
        return canonical_amount.lstrip("+-")

    @staticmethod
    def _is_effectively_zero_amount(value: str) -> bool:
        """문자열 금액이 수치적으로 0과 같은지 확인한다."""
        normalized = value.replace(",", "")
        try:
            return float(normalized) == 0.0
        except ValueError:
            return False

    @staticmethod
    def _normalize_settle_class(value: str | None, t_day: int | None, evidence_label: str) -> SettleClass | None:
        """LLM 값, evidence label, t_day를 종합해 최종 settle_class를 정한다."""
        lower_label = evidence_label.lower()
        if FundOrderExtractor._evidence_implies_schedule(lower_label):
            return SettleClass.PENDING
        if any(token in lower_label for token in ["당일", "확정", "실행", "당일이체", "당일투입", "당일인출", "설정금액", "해지금액", "입금액", "출금액", "buy", "sell"]):
            return SettleClass.CONFIRMED
        if any(token in lower_label for token in ["subscription", "redemption", "order", "instruction", "운용지시", "지시서"]):
            return SettleClass.CONFIRMED
        if value:
            upper = value.strip().upper()
            if upper == SettleClass.CONFIRMED.value:
                return SettleClass.CONFIRMED
            if upper == SettleClass.PENDING.value:
                return SettleClass.PENDING
        if t_day == 0:
            return SettleClass.CONFIRMED
        if t_day is not None and t_day >= 1:
            return SettleClass.PENDING
        return None

    @staticmethod
    def _normalize_order_type(value: str | None, amount: str, evidence_label: str) -> OrderType | None:
        """label/value/금액 부호를 종합해 최종 order_type을 정한다."""
        # Header-label 기반 방향 추론은 deterministic markdown fallback과 동일한 규칙을
        # 재사용해, "펀드납입(인출)금액" 같은 hybrid label이 마지막 fallback 단계에서
        # 한쪽 방향으로 다시 치우치지 않도록 유지한다.
        label_order_type = FundOrderExtractor._order_type_hint_from_header_label(evidence_label)

        value_order_type: OrderType | None = None
        if value:
            upper = value.strip().upper()
            if upper == OrderType.SUB.value:
                value_order_type = OrderType.SUB
            elif upper == OrderType.RED.value:
                value_order_type = OrderType.RED

        # section 문서(Cardif 등)는 evidence_label 이 실제 amount-bearing section 을
        # 가리키도록 prompt 를 설계해 두었다. 따라서 "Redemption" / "Subscription" 같은
        # 명시적 문맥이 있으면 모델이 잘못 SUB/RED 를 준 경우보다 이를 우선 신뢰한다.
        if label_order_type is not None:
            return label_order_type
        if value_order_type is not None:
            return value_order_type

        normalized_amount = amount.replace(",", "")
        if normalized_amount.startswith("-"):
            return OrderType.RED
        if normalized_amount.startswith("+"):
            return OrderType.SUB
        return None

    @staticmethod
    def _order_type_from_signed_amount(amount: Decimal) -> OrderType | None:
        """signed Decimal의 부호만 보고 SUB/RED를 결정한다."""
        if amount > 0:
            return OrderType.SUB
        if amount < 0:
            return OrderType.RED
        return None

    @staticmethod
    def _to_signed_decimal(amount: str, order_type: OrderType) -> Decimal | None:
        """unsigned 금액과 order_type 조합을 signed Decimal로 변환한다."""
        normalized = amount.replace(",", "")
        try:
            value = Decimal(normalized)
        except InvalidOperation:
            return None
        if normalized.startswith(("+", "-")):
            return value
        if order_type is OrderType.RED:
            return -value
        return value

    @staticmethod
    def _format_decimal_amount(amount: Decimal) -> str:
        """Decimal을 저장용 문자열 금액 형식으로 포맷한다."""
        text = format(amount, "f")
        sign = ""
        if text.startswith("-"):
            sign = "-"
            text = text[1:]
        whole, dot, fraction = text.partition(".")
        whole = f"{int(whole or '0'):,}"
        formatted = f"{sign}{whole}.{fraction}" if fraction else f"{sign}{whole}"
        return format_source_transfer_amount(formatted) or formatted

    @staticmethod
    def _is_non_amount_evidence(evidence_label: str) -> bool:
        """evidence label이 좌수/NAV/합산 등 비금액 근거인지 판별한다."""
        lower_label = evidence_label.lower()
        return any(
            token in lower_label
            for token in [
                "좌수",
                "unit",
                "units",
                "share",
                "shares",
                "누적좌수",
                "전일좌수",
                "순투입좌수",
                "증감좌수",
                "nav대비",
                "nav ratio",
                "ratio",
                "%",
                "합산",
                "누계",
                "총합",
                "subtotal",
                "total",
            ]
        )

    @staticmethod
    def _normalize_t_day(value: int | None, settle_class: SettleClass) -> int | None:
        """settle_class 기준으로 내부 `t_day`를 정리한다."""
        # Pending instruction documents without explicit T+N buckets are allowed
        # to leave t_day empty. We only coerce confirmed rows to T+0.
        if settle_class is SettleClass.CONFIRMED:
            return 0
        return value

    @staticmethod
    def _chunk_list(items: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
        """배치 호출을 위해 리스트를 고정 크기 chunk로 나눈다."""
        return [items[index : index + size] for index in range(0, len(items), size)]

    @staticmethod
    def _append_unique(items: list[str], value: str) -> None:
        """중복 issue 코드를 한 번만 추가한다."""
        if value not in items:
            items.append(value)

    @staticmethod
    def _issue_has_code(issue: str, code: str) -> bool:
        """상세 설명이 붙은 issue 문자열도 동일한 코드로 간주한다.

        extractor 는 상황에 따라 `T_DAY_MISSING` 같은 코드만 남기기도 하고,
        `T_DAY_MISSING: 492006 has no non-zero monetary value ...`나
        `T_DAY_MISSING_FOR_6114`처럼 사람이 읽기 쉬운 설명을 덧붙이기도 한다. 후속 정리와 저장 가드는
        둘 다 같은 issue code 로 읽어야 하므로 prefix-aware 비교를 공통 helper 로 둔다.
        """
        normalized_issue = issue.strip()
        normalized_code = code.strip()
        return (
            normalized_issue == normalized_code
            or normalized_issue.startswith(f"{normalized_code}:")
            or normalized_issue.startswith(f"{normalized_code}_")
        )

    @classmethod
    def _issues_include_code(cls, issues: list[str], code: str) -> bool:
        """issue 목록에 특정 코드가 하나라도 존재하는지 확인한다."""
        return any(cls._issue_has_code(issue, code) for issue in issues)

    @classmethod
    def _remove_issues_by_code(cls, issues: list[str], code: str) -> list[str]:
        """특정 코드와 그 상세 변형을 issue 목록에서 모두 제거한다."""
        return [issue for issue in issues if not cls._issue_has_code(issue, code)]

    @staticmethod
    def _is_internal_retry_finding(issue: str) -> bool:
        """retry 판단 전용 내부 finding은 최종 extractor issue로 노출하지 않는다."""
        return issue.startswith(INTERNAL_RETRY_FINDING_PREFIX)

    @classmethod
    def _public_stage_findings(cls, issues: list[str]) -> list[str]:
        """retry-only 내부 finding을 제외한 public issue만 남긴다."""
        return [issue for issue in issues if not cls._is_internal_retry_finding(issue)]

    @staticmethod
    def _unique_preserve_order(items: list[str]) -> list[str]:
        """등장 순서를 유지한 채 문자열 목록을 dedupe 한다."""
        seen: set[str] = set()
        result: list[str] = []
        for item in items:
            if item in seen:
                continue
            seen.add(item)
            result.append(item)
        return result

    @staticmethod
    def _stage_item_payload_without_metadata(item: Any) -> dict[str, Any]:
        """stage item identity 비교 시 reason metadata는 제외한다."""
        payload = item.model_dump(mode="json") if hasattr(item, "model_dump") else dict(item)
        return {
            key: value
            for key, value in payload.items()
            if key not in STAGE_ITEM_METADATA_FIELDS and key not in STAGE_RESULT_METADATA_FIELDS
        }

    @staticmethod
    def _stage_item_reason_code(item: Any) -> str | None:
        """stage item에서 내부 reason_code를 읽어 정규화한다."""
        if hasattr(item, "reason_code"):
            return _normalize_reason_code_value(getattr(item, "reason_code"))
        if isinstance(item, dict):
            return _normalize_reason_code_value(item.get("reason_code"))
        return None

    def _merge_stage_item_reason_code(self, existing_item: Any, candidate_item: Any) -> Any:
        """logical duplicate item 병합 시 첫 non-empty reason_code를 유지한다."""
        merged_reason_code = self._stage_item_reason_code(existing_item) or self._stage_item_reason_code(candidate_item)
        if hasattr(existing_item, "model_copy"):
            return existing_item.model_copy(update={"reason_code": merged_reason_code})
        merged_item = dict(existing_item)
        if merged_reason_code is None:
            merged_item.pop("reason_code", None)
        else:
            merged_item["reason_code"] = merged_reason_code
        return merged_item

    @staticmethod
    def _log_stage_reason_summary(*, stage_name: str, batch_index: int, parsed: BaseModel | None) -> None:
        """stage-level reason_summary를 로그로 남긴다."""
        if parsed is None:
            return
        reason_summary = _normalize_reason_summary_value(getattr(parsed, "reason_summary", ""))
        if not reason_summary:
            return
        logger.info(
            "Stage %s batch %s reason_summary: %s",
            stage_name,
            batch_index,
            reason_summary,
        )

    def _derive_document_fund_seed_reason_code(
        self,
        *,
        header: list[str],
        row: list[str],
        target_fund_scope: TargetFundScope | None,
    ) -> str:
        """Structured markdown로 보강한 stage 1 seed의 primary reason_code를 정한다."""
        amount_headers: list[str] = []
        for index, cell in enumerate(row):
            amount = self._normalize_amount(cell)
            if amount is None or self._is_effectively_zero_amount(amount):
                continue
            label = self._normalize_text(header[index]) if index < len(header) else ""
            if label and not self._looks_like_document_amount_label(label):
                continue
            if label:
                amount_headers.append(label)

        if any(self._is_execution_evidence(label) for label in amount_headers):
            return "FUND_NET_AMOUNT_ROW"

        order_hints = {
            self._order_type_hint_from_header_label(label)
            for label in amount_headers
            if self._order_type_hint_from_header_label(label) is not None
        }
        if order_hints == {OrderType.SUB}:
            return "FUND_EXPLICIT_SUB_ROW"
        if order_hints == {OrderType.RED}:
            return "FUND_EXPLICIT_RED_ROW"
        if target_fund_scope is not None and target_fund_scope.manager_column_present:
            return "FUND_MANAGER_SCOPED"
        if target_fund_scope is not None:
            return "FUND_NO_MANAGER_SCOPE"
        return "FUND_OTHER_VERIFIED"

    @classmethod
    def _derive_document_base_date_reason_code(cls, raw_text: str, base_date: str | None) -> str:
        """문서 단일 base_date가 어떤 근거에서 왔는지 짧은 reason_code로 요약한다."""
        normalized_base_date = cls._normalize_date(base_date)
        if not raw_text or normalized_base_date is None:
            return "DATE_OTHER_VERIFIED"
        if cls._detect_title_adjacent_base_date_from_text(raw_text) == normalized_base_date:
            return "DATE_DOC_HEADER"
        section_nav_dates = cls._collect_section_nav_dates_from_text(raw_text)
        if section_nav_dates == [normalized_base_date]:
            return "DATE_SECTION_ROW"
        transaction_row_dates = cls._collect_transaction_row_dates_from_text(raw_text)
        if transaction_row_dates == [normalized_base_date]:
            return "DATE_GRID_ALIGNED"
        if cls._detect_asia_seoul_base_date_from_text(raw_text) == normalized_base_date:
            return "DATE_EMAIL_FALLBACK"
        if cls._date_looks_like_actual_base_date_hint_in_text(raw_text, normalized_base_date):
            return "DATE_SETTLEMENT_LABEL"
        if cls._detect_base_date_from_text(raw_text, include_asia_seoul=False) == normalized_base_date:
            return "DATE_DOC_HEADER"
        return "DATE_OTHER_VERIFIED"

    def _deterministic_t_day_reason_code(
        self,
        *,
        t_day: int,
        slot_id: str,
        evidence_label: str,
    ) -> str:
        """Deterministic t_day slot 생성 시 primary reason_code를 정한다."""
        normalized_label = self._normalize_text(evidence_label)
        lowered_label = normalized_label.lower()
        if t_day >= 1:
            if bool(re.search(r"익+영업일", lowered_label)) or bool(re.search(r"제\s*\d+\s*영업일", lowered_label)):
                return "SLOT_BUSINESS_DAY_HEADER"
            return "SLOT_TPLUS_SCHEDULE"
        if self._is_execution_evidence(normalized_label) or self._normalize_text(slot_id).endswith("_NET"):
            return "SLOT_T0_NET"
        section_order_type = self._section_order_type_hint(normalized_label)
        if section_order_type is not None and not self._is_explicit_order_evidence(normalized_label):
            return "SLOT_SECTION_DIRECTION"
        header_order_type = self._order_type_hint_from_header_label(normalized_label)
        if header_order_type is OrderType.SUB:
            return "SLOT_T0_EXPLICIT_SUB"
        if header_order_type is OrderType.RED:
            return "SLOT_T0_EXPLICIT_RED"
        return "SLOT_OTHER_VERIFIED"

    def _deterministic_settle_reason_code(
        self,
        *,
        t_day: int | None,
        evidence_label: str,
    ) -> str:
        """Deterministic settle_class 생성 시 primary reason_code를 정한다."""
        normalized_label = self._normalize_text(evidence_label)
        lowered_label = normalized_label.lower()
        if self._evidence_implies_schedule(normalized_label):
            if bool(re.search(r"익+영업일", lowered_label)) or bool(re.search(r"제\s*\d+\s*영업일", lowered_label)):
                return "SETTLE_BUSINESS_DAY_HEADER"
            return "SETTLE_FUTURE_LABEL"
        if any(
            token in lowered_label
            for token in ("정산액", "결제금액", "실행금액", "execution", "settlement", "결제일", "processed")
        ):
            return "SETTLE_PROCESSED_RESULT"
        if any(
            token in lowered_label
            for token in (
                "당일",
                "확정",
                "당일이체",
                "당일투입",
                "당일인출",
                "설정금액",
                "해지금액",
                "입금액",
                "출금액",
                "buy",
                "sell",
                "subscription",
                "redemption",
            )
        ):
            return "SETTLE_SAME_DAY_LABEL"
        if t_day is not None:
            return "SETTLE_TDAY_FALLBACK"
        return "SETTLE_OTHER_VERIFIED"

    def _deterministic_order_type_reason_code(
        self,
        *,
        transfer_amount: str,
        evidence_label: str,
    ) -> str:
        """Deterministic order_type 생성 시 primary reason_code를 정한다."""
        normalized_amount = self._canonicalize_amount_text(transfer_amount) or ""
        if normalized_amount.startswith("-"):
            return "ORDER_SIGN_NEGATIVE"
        if normalized_amount.startswith("+"):
            return "ORDER_SIGN_POSITIVE"

        normalized_label = self._normalize_text(evidence_label)
        section_order_type = self._section_order_type_hint(normalized_label)
        if section_order_type is not None and not self._is_explicit_order_evidence(normalized_label):
            return "ORDER_SECTION_SUB" if section_order_type is OrderType.SUB else "ORDER_SECTION_RED"

        header_order_type = self._order_type_hint_from_header_label(normalized_label)
        if header_order_type is OrderType.SUB:
            return "ORDER_LABEL_SUB"
        if header_order_type is OrderType.RED:
            return "ORDER_LABEL_RED"

        lowered_label = normalized_label.lower()
        if "transaction type" in lowered_label or "buy&sell" in lowered_label or lowered_label == "구분":
            return "ORDER_TRANSACTION_TYPE"
        return "ORDER_OTHER_VERIFIED"

    def _infer_transfer_amount_reason_code(self, item: FundAmountItem | dict[str, Any]) -> str:
        """Stage 4 item의 primary amount evidence source를 deterministic 하게 분류한다."""
        if isinstance(item, dict):
            evidence_label = self._normalize_text(item.get("evidence_label"))
            t_day_value = item.get("t_day")
            transfer_amount = item.get("transfer_amount")
        else:
            evidence_label = self._normalize_text(item.evidence_label)
            t_day_value = item.t_day
            transfer_amount = item.transfer_amount

        t_day = t_day_value if isinstance(t_day_value, int) else None
        normalized_amount = self._canonicalize_amount_text(transfer_amount) or ""

        if self._is_execution_evidence(evidence_label):
            return "AMOUNT_NET_COLUMN"
        if (t_day is not None and t_day >= 1) or self._evidence_implies_schedule(evidence_label):
            return "AMOUNT_SCHEDULE_BUCKET"

        header_order_type = self._order_type_hint_from_header_label(evidence_label)
        if header_order_type is OrderType.SUB:
            return "AMOUNT_EXPLICIT_SUB"
        if header_order_type is OrderType.RED:
            return "AMOUNT_EXPLICIT_RED"
        if normalized_amount.startswith("-"):
            return "AMOUNT_SIGNED_OUTFLOW"
        return "AMOUNT_OTHER_VERIFIED"

    def _post_validate_transfer_amount_items(self, items: list[FundAmountItem]) -> list[FundAmountItem]:
        """Stage 4 output의 reason_code를 deterministic rule로 보정/보강한다."""
        post_validated: list[FundAmountItem] = []
        for item in items:
            inferred_reason_code = self._infer_transfer_amount_reason_code(item)
            current_reason_code = _normalize_reason_code_value(item.reason_code)
            if current_reason_code is None or current_reason_code == "AMOUNT_OTHER_VERIFIED":
                reason_code = inferred_reason_code
            else:
                reason_code = current_reason_code
            post_validated.append(item.model_copy(update={"reason_code": reason_code}))
        return post_validated

    def _dedupe_fund_seeds(self, items: list[FundSeedItem]) -> list[FundSeedItem]:
        """Stage 1 fund seed를 `(fund_code, fund_name)` 기준으로 중복 제거한다."""
        result: list[FundSeedItem] = []
        index_by_key: dict[tuple[str, str], int] = {}
        for item in items:
            key = (self._normalize_text(item.fund_code), self._normalize_text(item.fund_name))
            if not all(key):
                continue
            normalized_item = item.model_copy(update={"fund_code": key[0], "fund_name": key[1]})
            existing_index = index_by_key.get(key)
            if existing_index is not None:
                result[existing_index] = self._merge_stage_item_reason_code(result[existing_index], normalized_item)
                continue
            index_by_key[key] = len(result)
            result.append(normalized_item)
        return result

    def _dedupe_stage_items(self, items: list[Any]) -> list[Any]:
        """같은 stage item이 여러 batch/chunk에서 반복될 때 하나만 남긴다."""
        result: list[Any] = []
        index_by_dump: dict[str, int] = {}
        for item in items:
            dumped = json.dumps(
                self._stage_item_payload_without_metadata(item),
                ensure_ascii=False,
                sort_keys=True,
            )
            existing_index = index_by_dump.get(dumped)
            if existing_index is not None:
                result[existing_index] = self._merge_stage_item_reason_code(result[existing_index], item)
                continue
            index_by_dump[dumped] = len(result)
            result.append(item)
        return result

    def _aggregate_candidates(self, candidates: list[_OrderCandidate], issues: list[str]) -> list[OrderExtraction]:
        """후보들을 결제 버킷 단위로 묶어 최종 order 1건으로 만든다.

        현재 규칙은 세 갈래다.

        1. explicit 설정/해지 금액이 같은 버킷에 함께 있으면
           execution(net) evidence 존재 여부를 먼저 본다.
           - net evidence가 있으면 순유입/결제/당일이체 금액 1건을 최종값으로 사용한다.
           - net evidence가 없으면 SUB와 RED를 순액으로 합치지 않고 방향별 주문을 그대로 유지한다.
        2. explicit 방향이 하나뿐이면 `결제금액`, `당일이체금액`, `순유입금액` 같은
           execution(net) 컬럼을 우선 사용한다.
        3. execution 컬럼도 없으면 같은 방향 explicit evidence만 집계한다.

        따라서 1차 grouping key 는 공통 stage identity와 같은 기준을 따라
        `(canonical_fund_code, canonical_fund_name, settle_class, base_date, t_day)` 이고,
        explicit SUB/RED가 모두 존재하더라도 net evidence가 있으면 한 번 더 나누지 않는다.
        """
        grouped: dict[tuple[str, str, SettleClass, str | None, int | None], list[_OrderCandidate]] = {}
        for candidate in candidates:
            order = candidate.order
            canonical_fund_code, canonical_fund_name = self._canonical_fund_identity(
                order.fund_code,
                order.fund_name,
            )
            key = (
                canonical_fund_code,
                canonical_fund_name,
                order.settle_class,
                order.base_date,
                order.t_day,
            )
            grouped.setdefault(key, []).append(candidate)

        orders: list[OrderExtraction] = []
        for group_candidates in grouped.values():
            group_candidates = self._dedupe_group_candidates(group_candidates)
            explicit_candidates = [candidate for candidate in group_candidates if candidate.evidence_kind != "execution"]
            explicit_order_types = {candidate.order.order_type for candidate in explicit_candidates}
            execution_candidates = [candidate for candidate in group_candidates if candidate.evidence_kind == "execution"]

            if len(explicit_order_types) > 1:
                # 같은 펀드 버킷에 입금/출금이 모두 있고, 순유입/결제/당일이체 금액도 있으면
                # 최종 주문은 net evidence 1건으로 보는 것이 현재 요구사항이다.
                # 예: iM/신한 계열 문서처럼 `입금액`, `출금액`, `증감금액`이 함께 있는 경우.
                if execution_candidates:
                    aggregated_order = self._build_aggregated_order(group_candidates, issues)
                    if aggregated_order is not None:
                        orders.append(aggregated_order)
                    continue

                # 반대로 net evidence가 없고 explicit 입금/출금만 둘 다 있다면
                # 최근 요구사항에 따라 두 주문을 합치지 않고 방향별로 그대로 유지한다.
                candidates_by_order_type: dict[OrderType, list[_OrderCandidate]] = {}
                for candidate in explicit_candidates:
                    candidates_by_order_type.setdefault(candidate.order.order_type, []).append(candidate)

                for typed_candidates in candidates_by_order_type.values():
                    aggregated_order = self._build_aggregated_order(typed_candidates, issues)
                    if aggregated_order is not None:
                        orders.append(aggregated_order)
                continue

            aggregated_order = self._build_aggregated_order(group_candidates, issues)
            if aggregated_order is not None:
                orders.append(aggregated_order)
        return orders

    def _dedupe_group_candidates(self, candidates: list[_OrderCandidate]) -> list[_OrderCandidate]:
        """같은 logical evidence가 display-only 차이로 중복된 후보를 하나로 접는다.

        최근 동양생명처럼 같은 fund_code에 fund_name 공백 차이만 있는 행이
        동시에 들어오면, 최종 집계 전까지는 같은 주문 후보를 두 번 보게 된다.
        이때 같은 settle bucket 안에서 evidence_label/order_type/amount/evidence_kind가
        모두 같은 후보는 같은 logical evidence로 보고 첫 후보만 남긴다.
        """
        deduped: list[_OrderCandidate] = []
        seen: set[tuple[Any, ...]] = set()
        for candidate in candidates:
            order = candidate.order
            canonical_fund_code, canonical_fund_name = self._canonical_fund_identity(
                order.fund_code,
                order.fund_name,
            )
            key = (
                canonical_fund_code,
                canonical_fund_name,
                order.settle_class,
                order.base_date,
                order.t_day,
                order.order_type,
                self._canonicalize_amount_text(order.transfer_amount) or order.transfer_amount,
                self._normalize_text(candidate.evidence_label),
                candidate.evidence_kind,
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(candidate)
        return deduped

    def _build_aggregated_order(
        self,
        group_candidates: list[_OrderCandidate],
        issues: list[str],
    ) -> OrderExtraction | None:
        """동일 후보 그룹에서 최종 주문 1건을 만든다."""
        template = group_candidates[0].order
        signed_amount = self._select_group_transfer_amount(group_candidates, issues)
        if signed_amount is None:
            self._append_unique(issues, "TRANSFER_AMOUNT_MISSING")
            return None

        amount_text = self._format_decimal_amount(signed_amount)
        if self._is_effectively_zero_amount(amount_text):
            return None

        order_type = self._order_type_from_signed_amount(signed_amount)
        if order_type is None:
            self._append_unique(issues, "ORDER_TYPE_MISSING")
            return None

        return OrderExtraction(
            fund_code=template.fund_code,
            fund_name=template.fund_name,
            settle_class=template.settle_class,
            order_type=order_type,
            base_date=template.base_date,
            t_day=template.t_day,
            transfer_amount=amount_text,
        )

    def _select_group_transfer_amount(
        self,
        group_candidates: list[_OrderCandidate],
        issues: list[str],
    ) -> Decimal | None:
        """후보 그룹에서 최종 transfer_amount로 사용할 signed 금액을 고른다."""
        # 우선순위 규칙:
        # 1. 결제금액/순유입금액/당일이체금액 같은 execution evidence 가 있으면 우선 사용
        # 2. execution 값이 여러 개 충돌하면 explicit 합산값과 일치하는지 본다
        # 3. 그래도 충돌이면 issue 를 남기고 첫 execution 값을 택한다
        # 4. execution 이 전혀 없으면 explicit inflow/outflow 를 합산한다
        execution_amounts = self._collect_signed_amounts(
            [candidate for candidate in group_candidates if candidate.evidence_kind == "execution"]
        )
        if execution_amounts:
            unique_execution_amounts = {amount for amount in execution_amounts}
            if len(unique_execution_amounts) == 1:
                return execution_amounts[0]

            explicit_amounts = self._collect_signed_amounts(
                [candidate for candidate in group_candidates if candidate.evidence_kind != "execution"]
            )
            explicit_net = sum(explicit_amounts, Decimal("0")) if explicit_amounts else None
            if explicit_net is not None and explicit_net in unique_execution_amounts:
                return explicit_net

            self._append_unique(issues, "TRANSFER_AMOUNT_CONFLICT")
            return execution_amounts[0]

        explicit_amounts = self._collect_signed_amounts(group_candidates)
        if not explicit_amounts:
            return None
        return sum(explicit_amounts, Decimal("0"))

    def _collect_signed_amounts(self, candidates: list[_OrderCandidate]) -> list[Decimal]:
        """후보 목록의 `transfer_amount + order_type`를 signed Decimal 목록으로 변환한다."""
        amounts: list[Decimal] = []
        for candidate in candidates:
            signed_amount = self._to_signed_decimal(
                candidate.order.transfer_amount,
                candidate.order.order_type,
            )
            if signed_amount is None:
                continue
            amounts.append(signed_amount)
        return amounts

    @staticmethod
    def _stage_item_signature(stage_name: str, item: dict[str, Any]) -> tuple[Any, ...]:
        """stage별 partial 검증에 사용할 최소 식별 시그니처를 만든다."""
        fund_code, fund_name = FundOrderExtractor._canonical_fund_identity(
            item.get("fund_code"),
            item.get("fund_name"),
        )
        if stage_name == "base_date":
            return (fund_code, fund_name)
        return (
            fund_code,
            fund_name,
            item.get("base_date"),
            item.get("t_day"),
            item.get("slot_id"),
        )

    @staticmethod
    def _stage_partial_item_signature(stage_name: str, item: dict[str, Any]) -> tuple[Any, ...]:
        """input/output partial 비교에 사용할 stage별 identity key다."""
        fund_code, fund_name = FundOrderExtractor._canonical_fund_identity(
            item.get("fund_code"),
            item.get("fund_name"),
        )
        if stage_name == "base_date":
            return (fund_code, fund_name)
        if stage_name == "t_day":
            return (fund_code, fund_name, item.get("base_date"))
        return (
            fund_code,
            fund_name,
            item.get("base_date"),
            item.get("t_day"),
            item.get("slot_id"),
        )

    @staticmethod
    def _canonical_fund_identity(fund_code: Any, fund_name: Any) -> tuple[str, str]:
        """공통 stage identity 비교용 `(fund_code, fund_name)`를 만든다.

        fund_code가 있는 문서는 code를 1차 identity로 보고, signature 비교에서도
        code만 사용한다. 실제 문서에서는 같은 fund_code에 fund_name 공백/구두점 차이,
        continuation row, mirrored page 표현 차이가 섞여 있을 수 있는데, 이런 차이를
        signature에 남기면 같은 logical fund가 다른 family로 분리되어 retry를 늘린다.

        반대로 fund_code가 없는 문서는 이름 정보 자체가 identity이므로 raw 공백 정규화만
        적용하고, `normalize_fund_name_key`까지는 쓰지 않는다.
        """
        normalized_code = FundOrderExtractor._normalize_text(fund_code)
        normalized_name = FundOrderExtractor._normalize_text(fund_name)
        if normalized_code:
            return normalized_code, ""
        return normalized_code, normalized_name

    def _classify_evidence_kind(self, evidence_label: str) -> str:
        """evidence label을 explicit/execution/other 세 그룹으로 분류한다."""
        if self._is_explicit_order_evidence(evidence_label):
            return "explicit"
        if self._is_execution_evidence(evidence_label):
            return "execution"
        return "other"

    def _reconcile_final_issues(
        self,
        resolved_items: list[FundResolvedItem],
        result: ExtractionResult,
        markdown_text: str | None = None,
        document_text: str | None = None,
        raw_text: str | None = None,
        target_fund_scope: TargetFundScope | None = None,
        expected_order_count: int | None = None,
    ) -> None:
        """최종 결과와 모순되는 stage-level warning 을 정리한다.

        staged prompt 는 보수적으로 warning 을 남길 수 있다.
        예를 들어 중간 stage 에서는 `T_DAY_MISSING` 이었지만,
        최종적으로 CONFIRMED T+0 로 충분히 정리된 경우가 있다.
        그런 warning 까지 그대로 남기면 저장 가드가 불필요하게 결과를 막을 수 있어서
        마지막에 한 번 더 현실 결과 기준으로 정리한다.

        정리 기준은 "warning을 무조건 지운다"가 아니라 아래처럼 보수적이다.
        - final orders가 이미 complete하다
        - complete sibling이나 독립 corroboration으로 ghost/stale row임이 설명된다
        - 반대로 실제 누락/충돌 가능성이 남으면 blocking issue를 유지한다
        """
        title_adjacent_base_date = self._detect_title_adjacent_base_date_from_text(raw_text or "")
        if title_adjacent_base_date and result.orders:
            observed_base_dates = {order.base_date for order in result.orders if order.base_date}
            observed_base_date = next(iter(observed_base_dates), None)
            if (
                len(observed_base_dates) <= 1
                and title_adjacent_base_date not in observed_base_dates
                and self._date_looks_like_prior_reference_in_text(raw_text or "", observed_base_date)
            ):
                for order in result.orders:
                    order.base_date = title_adjacent_base_date

        all_final_orders_have_amount = bool(result.orders) and all(order.transfer_amount for order in result.orders)
        all_final_orders_have_order_type = bool(result.orders) and all(order.order_type for order in result.orders)
        all_final_orders_have_base_date = bool(result.orders) and all(order.base_date for order in result.orders)
        all_final_orders_have_t_day = bool(result.orders) and all(order.t_day is not None for order in result.orders)
        all_final_orders_have_settle_class = bool(result.orders) and all(order.settle_class for order in result.orders)
        all_final_orders_have_metadata = bool(result.orders) and all(
            self._normalize_text(order.fund_code) and self._normalize_text(order.fund_name)
            for order in result.orders
        )
        ambiguity_issue_codes = (
            "FUND_NAME_AMBIGUOUS_IN_RAW_TEXT",
            "FUND_NAME_AMBIGUOUS_IN_MARKDOWN",
        )
        independently_corroborated_final_orders = self._can_independently_corroborate_final_orders_from_text(
            markdown_text=markdown_text,
            raw_text=raw_text,
            target_fund_scope=target_fund_scope,
            extraction_result=result,
        )
        independently_corroborated_fund_identity = independently_corroborated_final_orders
        if not independently_corroborated_fund_identity and any(
            self._issues_include_code(result.issues, issue_code)
            for issue_code in ambiguity_issue_codes
        ):
            independently_corroborated_fund_identity = self._can_independently_corroborate_final_orders_from_text(
                markdown_text=markdown_text,
                raw_text=raw_text,
                target_fund_scope=target_fund_scope,
                extraction_result=result,
                ignore_schedule=True,
            )

        if self._issues_include_code(result.issues, "T_DAY_MISSING"):
            null_t_day_items = [item for item in resolved_items if item.t_day is None]
            if not null_t_day_items:
                result.issues = self._remove_issues_by_code(result.issues, "T_DAY_MISSING")
            elif all(not self._evidence_implies_schedule(self._normalize_text(item.evidence_label)) for item in null_t_day_items):
                result.issues = self._remove_issues_by_code(result.issues, "T_DAY_MISSING")
            else:
                # zero-only 펀드는 stage 중간에서 "t_day를 정할 정보가 없다"는 경고가 남을 수 있다.
                # 하지만 실제 비영 금액 근거가 전혀 없는 항목이라면 최종 주문에도 포함되지 않으므로
                # 저장을 막는 actionable issue로 볼 필요가 없다.
                actionable_null_t_day_items = [
                    item
                    for item in null_t_day_items
                    if self._document_has_non_zero_amount_evidence(
                        document_text=document_text,
                        fund_code=self._normalize_text(item.fund_code),
                        fund_name=self._normalize_text(item.fund_name),
                    )
                    and not self._has_complete_resolved_sibling_for_t_day(resolved_items, item)
                ]
                if not actionable_null_t_day_items:
                    result.issues = self._remove_issues_by_code(result.issues, "T_DAY_MISSING")

        if self._issues_include_code(result.issues, "TRANSFER_AMOUNT_MISSING"):
            null_amount_items = [item for item in resolved_items if self._normalize_amount(item.transfer_amount) is None]
            actionable_null_amount_items = [
                item
                for item in null_amount_items
                if self._document_has_non_zero_amount_evidence(
                    document_text=document_text,
                    fund_code=self._normalize_text(item.fund_code),
                    fund_name=self._normalize_text(item.fund_name),
                )
                and not self._has_complete_resolved_sibling_for_missing_field(
                    resolved_items,
                    item,
                    field_name="transfer_amount",
                )
            ]
            if all_final_orders_have_amount and not actionable_null_amount_items:
                result.issues = self._remove_issues_by_code(result.issues, "TRANSFER_AMOUNT_MISSING")

        if self._issues_include_code(result.issues, "ORDER_TYPE_MISSING"):
            null_order_type_items = [item for item in resolved_items if not self._normalize_text(item.order_type)]
            actionable_null_order_type_items = [
                item
                for item in null_order_type_items
                if self._document_has_non_zero_amount_evidence(
                    document_text=document_text,
                    fund_code=self._normalize_text(item.fund_code),
                    fund_name=self._normalize_text(item.fund_name),
                )
                and not self._has_complete_resolved_sibling_for_missing_field(
                    resolved_items,
                    item,
                    field_name="order_type",
                )
            ]
            if all_final_orders_have_order_type and not actionable_null_order_type_items:
                result.issues = self._remove_issues_by_code(result.issues, "ORDER_TYPE_MISSING")

        if self._issues_include_code(result.issues, "SETTLE_CLASS_MISSING"):
            null_settle_items = [item for item in resolved_items if not self._normalize_text(item.settle_class)]
            actionable_missing_settle_items = []
            for item in null_settle_items:
                evidence_label = self._normalize_text(item.evidence_label)
                amount = self._normalize_amount(item.transfer_amount)
                if amount is None or self._is_effectively_zero_amount(amount):
                    continue
                if self._is_non_amount_evidence(evidence_label):
                    continue
                if self._normalize_settle_class(None, item.t_day, evidence_label) is not None:
                    continue
                if self._has_complete_resolved_sibling_for_missing_field(
                    resolved_items,
                    item,
                    field_name="settle_class",
                ):
                    continue
                actionable_missing_settle_items.append(item)

            if not actionable_missing_settle_items and all(order.settle_class for order in result.orders):
                result.issues = self._remove_issues_by_code(result.issues, "SETTLE_CLASS_MISSING")

        if self._issues_include_code(result.issues, "BASE_DATE_MISSING"):
            if result.orders and all(order.base_date for order in result.orders):
                result.issues = self._remove_issues_by_code(result.issues, "BASE_DATE_MISSING")

        if self._issues_include_code(result.issues, "FUND_METADATA_INCOMPLETE"):
            incomplete_metadata_items = [
                item
                for item in resolved_items
                if not self._normalize_text(item.fund_code) or not self._normalize_text(item.fund_name)
            ]
            actionable_incomplete_metadata_items = []
            for item in incomplete_metadata_items:
                evidence_label = self._normalize_text(item.evidence_label)
                amount = self._normalize_amount(item.transfer_amount)
                if amount is None or self._is_effectively_zero_amount(amount):
                    continue
                if self._is_non_amount_evidence(evidence_label):
                    continue
                if self._has_complete_resolved_sibling_for_metadata(resolved_items, item):
                    continue
                actionable_incomplete_metadata_items.append(item)

            if not actionable_incomplete_metadata_items and result.orders and all(
                self._normalize_text(order.fund_code) and self._normalize_text(order.fund_name)
                for order in result.orders
            ):
                result.issues = self._remove_issues_by_code(result.issues, "FUND_METADATA_INCOMPLETE")

        if self._issues_include_code(result.issues, "BASE_DATE_STAGE_PARTIAL"):
            if all_final_orders_have_base_date and not self._issues_include_code(result.issues, "BASE_DATE_MISSING"):
                result.issues = self._remove_issues_by_code(result.issues, "BASE_DATE_STAGE_PARTIAL")

        if self._issues_include_code(result.issues, "T_DAY_STAGE_PARTIAL"):
            if all_final_orders_have_t_day and not self._issues_include_code(result.issues, "T_DAY_MISSING"):
                result.issues = self._remove_issues_by_code(result.issues, "T_DAY_STAGE_PARTIAL")

        if self._issues_include_code(result.issues, "TRANSFER_AMOUNT_STAGE_PARTIAL"):
            if all_final_orders_have_amount and not self._issues_include_code(result.issues, "TRANSFER_AMOUNT_MISSING"):
                result.issues = self._remove_issues_by_code(result.issues, "TRANSFER_AMOUNT_STAGE_PARTIAL")

        if self._issues_include_code(result.issues, "SETTLE_CLASS_STAGE_PARTIAL"):
            if all_final_orders_have_settle_class and not self._issues_include_code(result.issues, "SETTLE_CLASS_MISSING"):
                result.issues = self._remove_issues_by_code(result.issues, "SETTLE_CLASS_STAGE_PARTIAL")

        if self._issues_include_code(result.issues, "ORDER_TYPE_STAGE_PARTIAL"):
            if all_final_orders_have_order_type and not self._issues_include_code(result.issues, "ORDER_TYPE_MISSING"):
                result.issues = self._remove_issues_by_code(result.issues, "ORDER_TYPE_STAGE_PARTIAL")

        if self._issues_include_code(result.issues, "LLM_INVALID_RESPONSE_FORMAT"):
            remaining_blocking_without_invalid_format = [
                issue
                for issue in result.issues
                if any(
                    self._issue_has_code(issue, code)
                    for code in BLOCKING_EXTRACTION_ISSUES
                    if code != "LLM_INVALID_RESPONSE_FORMAT"
                )
            ]
            if (
                all_final_orders_have_metadata
                and all_final_orders_have_base_date
                and all_final_orders_have_t_day
                and all_final_orders_have_amount
                and all_final_orders_have_settle_class
                and all_final_orders_have_order_type
                and not remaining_blocking_without_invalid_format
            ):
                result.issues = self._remove_issues_by_code(result.issues, "LLM_INVALID_RESPONSE_FORMAT")

        if self._issues_include_code(result.issues, "TRANSFER_AMOUNT_ZERO"):
            if result.orders and all(
                order.transfer_amount and not self._is_effectively_zero_amount(order.transfer_amount)
                for order in result.orders
            ):
                result.issues = self._remove_issues_by_code(result.issues, "TRANSFER_AMOUNT_ZERO")

        if self._issues_include_code(result.issues, "NO_MANAGER_FILTER_APPLIED"):
            if (
                target_fund_scope is not None
                and target_fund_scope.manager_column_present
                and result.orders
                and all(
                    self._is_target_fund(
                        fund_code=self._normalize_text(order.fund_code),
                        fund_name=self._normalize_text(order.fund_name),
                        target_fund_scope=target_fund_scope,
                    )
                    for order in result.orders
                )
            ):
                result.issues = self._remove_issues_by_code(result.issues, "NO_MANAGER_FILTER_APPLIED")

        if (
            target_fund_scope is not None
            and target_fund_scope.manager_column_present
            and result.orders
            and all(
                self._is_target_fund(
                    fund_code=self._normalize_text(order.fund_code),
                    fund_name=self._normalize_text(order.fund_name),
                    target_fund_scope=target_fund_scope,
                )
                for order in result.orders
            )
        ):
            result.issues = self._remove_issues_by_code(result.issues, "NO_MANAGER_INFO")
            result.issues = self._remove_issues_by_code(result.issues, "MANAGER_MISSING")
            result.issues = self._remove_issues_by_code(result.issues, "MANAGER_MISSING_IN_RAW_TEXT")

        if self._issues_include_code(result.issues, "DUPLICATE_FUND_CODE_NAME_PAIRS"):
            seen_pairs: set[tuple[str, str]] = set()
            duplicate_pair_present = False
            for order in result.orders:
                pair = (self._normalize_text(order.fund_code), self._normalize_text(order.fund_name))
                if pair in seen_pairs:
                    duplicate_pair_present = True
                    break
                seen_pairs.add(pair)
            if not duplicate_pair_present:
                result.issues = self._remove_issues_by_code(result.issues, "DUPLICATE_FUND_CODE_NAME_PAIRS")

        if independently_corroborated_final_orders:
            result.issues = self._remove_issues_by_code(result.issues, SOFT_COVERAGE_WARNING)

        if (
            independently_corroborated_fund_identity
            and all_final_orders_have_metadata
            and any(
                self._issues_include_code(result.issues, issue_code)
                for issue_code in ambiguity_issue_codes
            )
        ):
            for issue_code in ambiguity_issue_codes:
                result.issues = self._remove_issues_by_code(result.issues, issue_code)

    def _has_complete_resolved_sibling_for_missing_field(
        self,
        resolved_items: list[FundResolvedItem],
        target_item: FundResolvedItem,
        *,
        field_name: str,
    ) -> bool:
        """같은 버킷/evidence에서 빠진 값을 대체하는 완전한 sibling item이 있는지 본다.

        ghost slot은 종종 같은 펀드/기준일/T-day/evidence label에서 한 항목만 null로 남고,
        실제 주문 값은 같은 버킷의 다른 sibling item에 이미 채워져 있다.
        반대로 실제 누락 주문은 같은 버킷이라고 해도 evidence가 다르거나 완전한 sibling이 없다.
        그래서 coverage 숫자만 보는 대신, 같은 문맥의 sibling이 실제로 있는지를 직접 확인한다.
        """
        target_key = self._missing_field_sibling_key(target_item, field_name=field_name)
        for item in resolved_items:
            if item is target_item:
                continue
            item_key = self._missing_field_sibling_key(item, field_name=field_name)
            if item_key != target_key:
                continue

            if field_name == "transfer_amount":
                amount = self._normalize_amount(item.transfer_amount)
                if amount is not None and not self._is_effectively_zero_amount(amount):
                    return True
                continue

            if field_name == "order_type" and self._normalize_text(item.order_type):
                return True

            if field_name == "settle_class" and self._normalize_text(item.settle_class):
                return True

        return False

    def _missing_field_sibling_key(
        self,
        item: FundResolvedItem,
        *,
        field_name: str,
    ) -> tuple[object, ...]:
        """missing-field sibling 비교에 쓰는 버킷 키를 만든다."""
        base_key = (
            self._normalize_text(item.fund_code),
            self._normalize_text(item.fund_name),
            self._normalize_date(item.base_date),
            item.t_day,
            self._normalize_text(item.evidence_label),
        )
        if field_name == "settle_class":
            return base_key
        return (*base_key, self._normalize_text(item.settle_class))

    def _has_complete_resolved_sibling_for_t_day(
        self,
        resolved_items: list[FundResolvedItem],
        target_item: FundResolvedItem,
    ) -> bool:
        """같은 버킷/evidence에서 t_day만 빠진 ghost row를 대체하는 sibling이 있는지 본다."""
        target_key = (
            self._normalize_text(target_item.fund_code),
            self._normalize_text(target_item.fund_name),
            self._normalize_date(target_item.base_date),
            self._normalize_text(target_item.evidence_label),
            self._normalize_text(target_item.settle_class),
        )
        for item in resolved_items:
            if item is target_item:
                continue
            item_key = (
                self._normalize_text(item.fund_code),
                self._normalize_text(item.fund_name),
                self._normalize_date(item.base_date),
                self._normalize_text(item.evidence_label),
                self._normalize_text(item.settle_class),
            )
            if item_key != target_key:
                continue
            if item.t_day is not None:
                return True
        return False

    def _has_complete_resolved_sibling_for_metadata(
        self,
        resolved_items: list[FundResolvedItem],
        target_item: FundResolvedItem,
    ) -> bool:
        """metadata만 빠진 ghost row를 대체하는 complete sibling이 있는지 본다."""
        target_code = self._normalize_text(target_item.fund_code)
        target_name = self._normalize_text(target_item.fund_name)

        target_amount = self._canonicalize_amount_text(target_item.transfer_amount)
        target_key = (
            self._normalize_date(target_item.base_date),
            target_item.t_day,
            self._normalize_text(target_item.evidence_label),
            self._normalize_text(target_item.settle_class),
            self._normalize_text(target_item.order_type),
            target_amount,
        )
        matching_complete_siblings = 0
        for item in resolved_items:
            if item is target_item:
                continue

            item_code = self._normalize_text(item.fund_code)
            item_name = self._normalize_text(item.fund_name)
            if not item_code or not item_name:
                continue

            item_key = (
                self._normalize_date(item.base_date),
                item.t_day,
                self._normalize_text(item.evidence_label),
                self._normalize_text(item.settle_class),
                self._normalize_text(item.order_type),
                self._canonicalize_amount_text(item.transfer_amount),
            )
            if item_key != target_key:
                continue
            if target_code and item_code != target_code:
                continue
            if target_name and item_name != target_name:
                continue
            matching_complete_siblings += 1
            if target_code or target_name:
                return True
        if not target_code and not target_name:
            return matching_complete_siblings == 1
        return False

    def _document_has_non_zero_amount_evidence(
        self,
        *,
        document_text: str | None,
        fund_code: str,
        fund_name: str,
    ) -> bool:
        """원문에 실제 비영 금액 근거가 있는 항목인지 확인한다."""
        if not document_text:
            return True

        normalized_code = self._normalize_text(fund_code)
        canonical_name = normalize_fund_name_key(self._normalize_text(fund_name))
        if not normalized_code and not canonical_name:
            return True

        current_header: list[str] = []
        for line in document_text.splitlines():
            if " | " not in line:
                continue
            cells = [self._normalize_text(cell) for cell in line.split(" | ")]
            if not any(cells):
                continue

            if self._looks_like_document_pipe_header_row(cells):
                current_header = cells
                continue

            line_matches_code = bool(normalized_code and normalized_code in cells)
            line_matches_name = bool(
                canonical_name and canonical_name in normalize_fund_name_key(" ".join(cells))
            )
            if not (line_matches_code or line_matches_name):
                continue

            for index, cell in enumerate(cells):
                header_label = current_header[index] if index < len(current_header) else ""
                if header_label and not self._looks_like_document_amount_label(header_label):
                    continue
                amount = self._normalize_amount(cell)
                if amount is None:
                    continue
                if not self._is_effectively_zero_amount(amount):
                    return True

            if not current_header:
                for cell in cells:
                    if not any(token in cell for token in (",", ".", "+", "-")):
                        continue
                    amount = self._normalize_amount(cell)
                    if amount is None:
                        continue
                    if not self._is_effectively_zero_amount(amount):
                        return True

        return False

    @staticmethod
    def _looks_like_document_pipe_header_row(cells: list[str]) -> bool:
        """raw backup 안의 pipe row가 헤더인지 본다."""
        joined = " ".join(cell.lower() for cell in cells if cell)
        if not joined:
            return False
        return any(
            keyword in joined
            for keyword in (
                "펀드명",
                "fund name",
                "수탁코드",
                "fund code",
                "운용사",
                "manager",
                "구분",
                "transaction",
                "내용",
                "detail",
                "금액",
                "amount",
                "d+",
                "t+",
                "예상",
                "buy",
                "sell",
            )
        )

    @staticmethod
    def _looks_like_document_amount_label(label: str) -> bool:
        """헤더 라벨이 실제 금액 컬럼을 뜻하는지 확인한다."""
        lower_label = label.lower()
        if any(keyword in lower_label for keyword in ("좌수", "unit", "share")):
            return False
        return any(
            keyword in lower_label
            for keyword in (
                "금액",
                "amount",
                "예상",
                "결제",
                "execution",
                "settlement",
                "입금",
                "출금",
                "설정",
                "해지",
                "buy",
                "sell",
                "subscription",
                "redemption",
            )
        )

    def _row_has_non_zero_document_amount(self, *, header: list[str], row: list[str]) -> bool:
        """Structured markdown row에 실제 non-zero amount 근거가 있는지 확인한다."""
        for index, cell in enumerate(row):
            amount = self._normalize_amount(cell)
            if amount is None or self._is_effectively_zero_amount(amount):
                continue
            header_label = header[index] if index < len(header) else ""
            if header_label and not self._looks_like_document_amount_label(header_label):
                continue
            return True
        return False

    @staticmethod
    def _is_explicit_order_evidence(evidence_label: str) -> bool:
        """label이 설정/해지/입출금 같은 명시적 evidence인지 판별한다."""
        lower_label = evidence_label.lower()
        return any(
            token in lower_label
            for token in [
                "설정금액",
                "해지금액",
                "입금액",
                "출금액",
                "투입금액",
                "인출금액",
                "buy",
                "sell",
                "subscription",
                "redemption",
            ]
        )

    @staticmethod
    def _is_execution_evidence(evidence_label: str) -> bool:
        """label이 순유입/결제/정산 같은 net evidence인지 판별한다."""
        lower_label = evidence_label.lower()
        return any(
            token in lower_label
            for token in [
                "순유입",
                "순투입",
                "증감금액",
                "증감액",
                "펀드계",
                "정산액",
                "당일이체금액",
                "이체예상금액",
                "이체예정금액",
                "실행금액",
                "결제금액",
                "settlement amount",
                "execution amount",
                "execution",
            ]
        )

    @staticmethod
    def _evidence_implies_schedule(evidence_label: str) -> bool:
        """label만 보고도 미래 예정 슬롯임을 강하게 시사하는지 확인한다."""
        lower_label = evidence_label.lower()
        if any(token in lower_label for token in ["예정", "청구", "예상", "t+"]):
            return True
        if bool(re.search(r"익+영업일", lower_label)):
            return True
        if bool(re.search(r"제\s*\d+\s*영업일", lower_label)):
            return True
        if bool(re.search(r"\d{1,2}\s*월\s*\d{1,2}\s*일", lower_label)):
            return True
        if bool(re.search(r"\d{1,2}[/-]\d{1,2}", lower_label)):
            return True
        return bool(re.search(r"\d{4}-\d{2}-\d{2}", lower_label))


@dataclass(slots=True)
class _StageInvocation:
    """LLM stage 1회 호출의 raw response와 parsed result를 함께 담는다."""
    parsed: BaseModel | None
    raw_response: str
