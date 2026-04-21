from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field, field_validator


class SettleClass(str, Enum):
    """결제 상태 분류.

    - `CONFIRMED`: 당일 확정분
    - `PENDING`: 미래 예정분
    """
    CONFIRMED = "CONFIRMED"
    PENDING = "PENDING"


class OrderType(str, Enum):
    """최종 주문 방향.

    내부 계산은 signed amount도 같이 보지만, 외부 계약은
    `SUB`/`RED` 두 값으로만 전달한다.
    """
    SUB = "SUB"
    RED = "RED"


class OrderExtraction(BaseModel):
    """문서 1건에서 최종 확정된 주문 1건의 데이터 계약.

    이 모델은 LLM 중간 산출물이 아니라 "후처리와 검증을 모두 통과한 결과"를
    표현한다. 따라서 이후 JSON/CSV 저장, merge, 외부 서버 연동은 모두
    이 모델을 기준으로 동작한다.
    """
    fund_code: str = ""
    fund_name: str = ""
    settle_class: SettleClass
    order_type: OrderType
    base_date: str | None = None
    t_day: int | None = None
    transfer_amount: str

    @field_validator("fund_code", "fund_name", "transfer_amount", mode="before")
    @classmethod
    def strip_text(cls, value: object) -> str:
        """문자열 필드를 공통 방식으로 정리한다.

        후단 merge나 비교 로직은 공백 차이에 민감하므로,
        모델에 들어오는 시점에 한 번 정리해 두는 편이 안전하다.
        """
        if value is None:
            return ""
        return str(value).strip()


class ExtractionResult(BaseModel):
    """문서 1건의 추출 결과 전체.

    `orders`는 저장 가능한 최종 주문 목록이고, `issues`는 추출 과정에서
    감지된 경고/오류 코드다. 서비스 계층은 이 `issues`를 보고
    저장 차단 여부를 판단한다.
    """
    orders: list[OrderExtraction] = Field(default_factory=list)
    issues: list[str] = Field(default_factory=list)
