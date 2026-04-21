from __future__ import annotations

from typing import Any

from app.amount_normalization import canonicalize_transfer_amount, format_source_transfer_amount


# 최종 외부 출력 계약에서만 쓰는 코드 치환 규칙이다.
# 내부 모델/LLM stage는 기존 문자열 enum(CONFIRMED/PENDING, SUB/RED)을 유지하고,
# JSON/CSV로 내보내는 마지막 순간에만 아래 코드값으로 바꾼다.
SETTLE_CLASS_OUTPUT_CODES = {
    "PENDING": "1",
    "CONFIRMED": "2",
}

ORDER_TYPE_OUTPUT_CODES = {
    "RED": "1",
    "SUB": "3",
}


def format_output_transfer_amount(order_type: str | None, transfer_amount: str | None) -> str | None:
    """내부 signed amount를 최종 출력 계약 문자열로 바꾼다.

    내부 계산은 signed amount가 가장 다루기 쉽다.
    다만 외부 JSON/CSV는 RED도 절대값 문자열로 내보내야 하므로,
    마지막 직렬화 단계에서만 부호를 정리한다.
    """
    if transfer_amount is None:
        return None
    text = str(transfer_amount).strip()
    if not text:
        return text
    formatted_text = format_source_transfer_amount(text)
    if formatted_text is not None:
        text = formatted_text
    if text.startswith("+"):
        text = text[1:]
    if order_type == "RED" and text.startswith("-"):
        text = text[1:]
    return text


def format_output_t_day(settle_class: str | None, t_day: Any) -> str | None:
    """내부 정수 `t_day`를 외부 계약 문자열(`01`, `02`...)로 변환한다."""
    if settle_class == "CONFIRMED":
        return "01"
    if t_day is None:
        return None
    try:
        numeric = int(str(t_day).strip())
    except ValueError:
        return str(t_day).strip() or None
    return f"{numeric + 1:02d}"


def map_output_settle_class(settle_class: str | None) -> str | None:
    """최종 출력 계약용 settle_class 문자열 코드를 반환한다."""
    if settle_class is None:
        return None
    text = str(settle_class).strip()
    return SETTLE_CLASS_OUTPUT_CODES.get(text, text or None)


def map_output_order_type(order_type: str | None) -> str | None:
    """최종 출력 계약용 order_type 문자열 코드를 반환한다."""
    if order_type is None:
        return None
    text = str(order_type).strip()
    return ORDER_TYPE_OUTPUT_CODES.get(text, text or None)


def serialize_order_payload(order: Any) -> dict[str, Any]:
    """최종 내부 모델을 외부 JSON/CSV 계약 dict로 직렬화한다.

    적용 순서는 항상 아래와 같다.
    1. 모델을 dict로 변환
    2. transfer_amount, t_day 표현 보정
    3. settle_class, order_type를 문자열 코드로 치환

    이 순서를 한 helper에 고정해 두면
    - component
    - service
    - 향후 WAS adapter
    가 모두 동일한 최종 출력 계약을 공유할 수 있다.
    """
    payload = order.model_dump(mode="json")
    payload["transfer_amount"] = format_output_transfer_amount(
        order_type=payload.get("order_type"),
        transfer_amount=payload.get("transfer_amount"),
    )
    payload["t_day"] = format_output_t_day(
        settle_class=payload.get("settle_class"),
        t_day=payload.get("t_day"),
    )
    payload["settle_class"] = map_output_settle_class(payload.get("settle_class"))
    payload["order_type"] = map_output_order_type(payload.get("order_type"))
    return payload


def serialized_order_payload_identity(payload: dict[str, Any]) -> tuple[str, str, str, str, str, str, str]:
    """최종 직렬화 payload의 dedupe identity key를 만든다."""
    canonical_amount = canonicalize_transfer_amount(payload.get("transfer_amount"))
    transfer_amount = (
        canonical_amount
        if canonical_amount is not None
        else _identity_text(payload.get("transfer_amount"))
    )
    return (
        _identity_text(payload.get("fund_code")),
        _identity_text(payload.get("fund_name")),
        _identity_text(payload.get("settle_class")),
        _identity_text(payload.get("order_type")),
        _identity_text(payload.get("base_date")),
        _identity_text(payload.get("t_day")),
        transfer_amount,
    )


def dedupe_serialized_order_payloads(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """최종 직렬화 payload 목록에서 완전히 같은 주문을 1건만 남긴다."""
    deduped: list[dict[str, Any]] = []
    seen: dict[tuple[str, str, str, str, str, str, str], int] = {}

    for payload in payloads:
        normalized_payload = dict(payload)
        formatted_amount = format_source_transfer_amount(normalized_payload.get("transfer_amount"))
        if formatted_amount is not None:
            normalized_payload["transfer_amount"] = formatted_amount

        identity = serialized_order_payload_identity(normalized_payload)
        existing_index = seen.get(identity)
        if existing_index is None:
            seen[identity] = len(deduped)
            deduped.append(normalized_payload)
            continue
        if _is_preferred_order_representation(normalized_payload, deduped[existing_index]):
            normalized_payload["transfer_amount"] = deduped[existing_index].get("transfer_amount")
            deduped[existing_index] = normalized_payload

    return deduped


def normalize_counterparty_output_order_payload(
    payload: dict[str, Any],
    *,
    prompt_name: str | None = None,
    company_name: str | None = None,
) -> dict[str, Any]:
    """거래처별 최종 저장 계약 차이를 마지막 직렬화 단계에서만 반영한다.

    내부 추출 단계는 fund_code를 식별자로 계속 써야 하므로,
    메트라이프처럼 최종 저장에서만 코드 치환이 필요한 경우도
    여기서만 후처리한다.
    """
    normalized = dict(payload)
    normalized_company_name = _normalize_counterparty_name(
        prompt_name=prompt_name,
        company_name=company_name,
    )
    if normalized_company_name == "메트라이프생명":
        normalized["fund_code"] = "-"
        fund_name = normalized.get("fund_name")
        if fund_name is not None:
            normalized["fund_name"] = str(fund_name).strip()
    elif normalized_company_name == "흥국생명-heungkuklife":
        normalized["fund_name"] = "-"
    return normalized


def normalize_counterparty_output_order_payloads(
    payloads: list[dict[str, Any]],
    *,
    prompt_name: str | None = None,
    company_name: str | None = None,
) -> list[dict[str, Any]]:
    """거래처별 최종 저장 계약 후처리를 order payload 목록에 일괄 적용한다."""
    normalized_company_name = _normalize_counterparty_name(
        prompt_name=prompt_name,
        company_name=company_name,
    )
    normalized_payloads = [
        normalize_counterparty_output_order_payload(
            payload,
            prompt_name=prompt_name,
            company_name=normalized_company_name,
        )
        for payload in payloads
    ]
    if normalized_company_name == "흥국생명-heungkuklife":
        normalized_payloads.sort(key=_heungkuk_output_order_sort_key)
    elif normalized_company_name == "카디프":
        normalized_payloads.sort(key=_cardif_output_order_sort_key)
    return normalized_payloads


def _identity_text(value: Any) -> str:
    """serialized payload dedupe 비교용 문자열을 만든다."""
    return str(value if value is not None else "").strip()


def _is_preferred_order_representation(candidate: dict[str, Any], current: dict[str, Any]) -> bool:
    """완전히 같은 주문이 중복될 때 더 풍부한 펀드명 표현을 남긴다."""
    candidate_name = _identity_text(candidate.get("fund_name"))
    current_name = _identity_text(current.get("fund_name"))
    if len(candidate_name) != len(current_name):
        return len(candidate_name) > len(current_name)
    return candidate_name > current_name


def _normalize_counterparty_name(
    *,
    prompt_name: str | None = None,
    company_name: str | None = None,
) -> str:
    normalized_company_name = (company_name or "").strip()
    if normalized_company_name:
        return normalized_company_name
    normalized_prompt_name = (prompt_name or "").strip()
    if normalized_prompt_name == "흥국생명":
        return "흥국생명-heungkuklife"
    return normalized_prompt_name


def _heungkuk_output_order_sort_key(payload: dict[str, Any]) -> tuple[Any, ...]:
    settle_class = str(payload.get("settle_class") or "").strip()
    order_type = str(payload.get("order_type") or "").strip()
    fund_code_text = str(payload.get("fund_code") or "").strip()
    t_day_text = str(payload.get("t_day") or "").strip()
    transfer_amount_text = str(payload.get("transfer_amount") or "").replace(",", "").strip()

    settle_priority = {"2": 0, "1": 1}.get(settle_class, 9)
    order_priority = {"3": 0, "1": 1}.get(order_type, 9)

    if fund_code_text.isdigit():
        fund_code_key: Any = (0, int(fund_code_text))
    else:
        fund_code_key = (1, fund_code_text)

    if t_day_text.isdigit():
        t_day_key: Any = (0, int(t_day_text))
    else:
        t_day_key = (1, t_day_text)

    if transfer_amount_text.isdigit():
        transfer_amount_key: Any = (0, int(transfer_amount_text))
    else:
        transfer_amount_key = (1, transfer_amount_text)

    return (
        settle_priority,
        order_priority,
        fund_code_key,
        t_day_key,
        transfer_amount_key,
    )


def _cardif_output_order_sort_key(payload: dict[str, Any]) -> tuple[Any, ...]:
    fund_code_text = str(payload.get("fund_code") or "").strip()
    base_date_text = str(payload.get("base_date") or "").strip()
    order_type_text = str(payload.get("order_type") or "").strip()
    transfer_amount_text = str(payload.get("transfer_amount") or "").replace(",", "").strip()

    if transfer_amount_text.isdigit():
        transfer_amount_key: Any = (0, int(transfer_amount_text))
    else:
        transfer_amount_key = (1, transfer_amount_text)

    return (
        fund_code_text,
        base_date_text,
        order_type_text,
        transfer_amount_key,
    )
