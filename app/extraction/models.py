from __future__ import annotations

from dataclasses import dataclass, field
from threading import RLock
import time
from typing import Any, ClassVar
import re
import unicodedata

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.schemas import ExtractionResult, OrderExtraction, SettleClass

from .constants import (
    BASE_DATE_REASON_CODES,
    FUND_INVENTORY_REASON_CODES,
    INSTRUCTION_DOCUMENT_REASON_CODES,
    ORDER_TYPE_REASON_CODES,
    REASON_SUMMARY_MAX_CHARS,
    SETTLE_CLASS_REASON_CODES,
    T_DAY_REASON_CODES,
    TRANSFER_AMOUNT_REASON_CODES,
)


def _normalize_reason_code_value(value: object) -> str | None:
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
    if value is None:
        return ""
    text = unicodedata.normalize("NFC", " ".join(str(value).split()))
    if not text:
        return ""
    return text[:REASON_SUMMARY_MAX_CHARS].rstrip()


def apply_only_pending_filter(result: ExtractionResult, *, only_pending: bool) -> ExtractionResult:
    if not only_pending:
        return result

    filtered_orders = [
        order.model_copy(update={"settle_class": SettleClass.PENDING})
        for order in result.orders
        if order.settle_class is not SettleClass.PENDING
    ]
    return ExtractionResult(orders=filtered_orders, issues=list(result.issues))


class StageModel(BaseModel):
    model_config = ConfigDict(extra="ignore")


class StageItemModel(StageModel):
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
        payload = super().model_dump(*args, **kwargs)
        if payload.get("reason_code") is None:
            payload.pop("reason_code", None)
        return payload


class StageResultModel(StageModel):
    reason_summary: str = ""
    issues: list[str] = Field(default_factory=list)

    @field_validator("reason_summary", mode="before")
    @classmethod
    def _normalize_reason_summary(cls, value: object) -> str:
        return _normalize_reason_summary_value(value)


class FundSeedItem(StageItemModel):
    allowed_reason_codes: ClassVar[frozenset[str]] = FUND_INVENTORY_REASON_CODES
    fund_code: str = ""
    fund_name: str = ""

    @field_validator("fund_code", "fund_name", mode="before")
    @classmethod
    def _strip_text(cls, value: object) -> str:
        if value is None:
            return ""
        return str(value).strip()


class FundSeedResult(StageResultModel):
    items: list[FundSeedItem] = Field(default_factory=list)


class InstructionDocumentItem(StageItemModel):
    allowed_reason_codes: ClassVar[frozenset[str]] = INSTRUCTION_DOCUMENT_REASON_CODES
    is_instruction_document: bool | None = None
    reason: str = ""

    @field_validator("reason", mode="before")
    @classmethod
    def _strip_reason(cls, value: object) -> str:
        if value is None:
            return ""
        return str(value).strip()


class InstructionDocumentResult(StageResultModel):
    items: list[InstructionDocumentItem] = Field(default_factory=list)


class FundBaseDateItem(StageItemModel):
    allowed_reason_codes: ClassVar[frozenset[str]] = BASE_DATE_REASON_CODES
    fund_code: str = ""
    fund_name: str = ""
    base_date: str | None = None

    @field_validator("fund_code", "fund_name", mode="before")
    @classmethod
    def _strip_text(cls, value: object) -> str:
        if value is None:
            return ""
        return str(value).strip()


class FundBaseDateResult(StageResultModel):
    items: list[FundBaseDateItem] = Field(default_factory=list)


class FundSlotItem(StageItemModel):
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
        if value is None:
            return ""
        return str(value).strip()

    @field_validator("evidence_label", mode="before")
    @classmethod
    def _strip_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None


class FundSlotResult(StageResultModel):
    items: list[FundSlotItem] = Field(default_factory=list)


class FundAmountItem(StageItemModel):
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
        if value is None:
            return ""
        return str(value).strip()

    @field_validator("evidence_label", "transfer_amount", mode="before")
    @classmethod
    def _strip_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None


class FundAmountResult(StageResultModel):
    items: list[FundAmountItem] = Field(default_factory=list)


class FundSettleItem(StageItemModel):
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
        if value is None:
            return ""
        return str(value).strip()

    @field_validator("evidence_label", "transfer_amount", "settle_class", mode="before")
    @classmethod
    def _strip_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None


class FundSettleResult(StageResultModel):
    items: list[FundSettleItem] = Field(default_factory=list)


class FundResolvedItem(StageItemModel):
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
        if value is None:
            return ""
        return str(value).strip()

    @field_validator("evidence_label", "transfer_amount", "settle_class", "order_type", mode="before")
    @classmethod
    def _strip_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None


class FundResolvedResult(StageResultModel):
    items: list[FundResolvedItem] = Field(default_factory=list)


@dataclass(slots=True)
class InvalidResponseArtifact:
    chunk_index: int
    raw_response: str
    stage_name: str


@dataclass(slots=True)
class LLMExtractionOutcome:
    result: ExtractionResult
    invalid_response_artifacts: list[InvalidResponseArtifact] = field(default_factory=list)


@dataclass(slots=True)
class _ExtractMetricsState:
    started_at_monotonic: float
    stage_retry_invocations: dict[str, int] = field(default_factory=dict)
    transport_retry_attempts: dict[str, int] = field(default_factory=dict)
    llm_batches_started: dict[str, int] = field(default_factory=dict)
    lock: RLock = field(default_factory=RLock, repr=False)

    def increment_bucket(self, bucket_name: str, stage_name: str, increment: int = 1) -> None:
        if increment <= 0:
            return
        with self.lock:
            bucket = getattr(self, bucket_name)
            bucket[stage_name] = bucket.get(stage_name, 0) + increment

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "total_elapsed_seconds": round(max(0.0, time.monotonic() - self.started_at_monotonic), 3),
                "stage_retry_invocations": dict(sorted(self.stage_retry_invocations.items())),
                "transport_retry_attempts": dict(sorted(self.transport_retry_attempts.items())),
                "llm_batches_started": dict(sorted(self.llm_batches_started.items())),
            }


class ExtractionOutcomeError(ValueError):
    def __init__(self, message: str, outcome: LLMExtractionOutcome) -> None:
        super().__init__(message)
        self._outcome = outcome

    @property
    def outcome(self) -> LLMExtractionOutcome:
        return self._outcome

    @property
    def result(self) -> ExtractionResult:
        return self._outcome.result

    @property
    def invalid_response_artifacts(self) -> list[InvalidResponseArtifact]:
        return self._outcome.invalid_response_artifacts


@dataclass(slots=True)
class _OrderCandidate:
    order: OrderExtraction
    evidence_label: str
    evidence_kind: str


@dataclass(frozen=True, slots=True)
class StageDefinition:
    number: int
    name: str
    goal: str
    instructions: str
    output_contract: str
    retry_instructions: str = ""


@dataclass(frozen=True, slots=True)
class PromptBundle:
    system_prompt: str
    user_prompt_template: str
    retry_user_prompt_template: str
    stages: dict[str, StageDefinition]
