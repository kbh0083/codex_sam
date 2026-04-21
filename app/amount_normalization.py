from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any
import re


_EMPTY_AMOUNT_TOKENS = {"-", "--", "N/A", "n/a", "null", "None"}
_AMOUNT_PATTERN = re.compile(r"[+-]?\d[\d,]*(?:\.\d+)?")
_INTEGER_ARTIFACT_TOLERANCE = Decimal("0.000001")


def _format_decimal_text(value: Decimal) -> str:
    """Decimal을 comma-separated plain text 금액 문자열로 바꾼다."""
    if value == 0:
        value = Decimal("0")

    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")

    sign = ""
    if text.startswith("-"):
        sign = "-"
        text = text[1:]
    elif text.startswith("+"):
        text = text[1:]

    whole, dot, fraction = text.partition(".")
    whole = f"{int(whole or '0'):,}"
    if fraction:
        return f"{sign}{whole}.{fraction}"
    return f"{sign}{whole}"


def _format_decimal_plain_text(value: Decimal) -> str:
    """Decimal을 comma 없이 plain text 문자열로 바꾼다."""
    if value == 0:
        value = Decimal("0")

    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    if text in {"-0", "+0"}:
        return "0"
    return text


def _nearest_integer_if_artifact(value: Decimal) -> Decimal:
    """정수에 매우 가까운 값은 xlsx float artifact로 보고 최근접 정수로 맞춘다."""
    nearest_integer = value.to_integral_value()
    if abs(value - nearest_integer) < _INTEGER_ARTIFACT_TOLERANCE:
        if nearest_integer == 0:
            return Decimal("0")
        return nearest_integer
    return value


def _parse_amount_decimal(value: str | None) -> Decimal | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text or text in _EMPTY_AMOUNT_TOKENS:
        return None

    text = text.replace(" ", "")
    if not _AMOUNT_PATTERN.fullmatch(text):
        return None

    try:
        return Decimal(text.replace(",", ""))
    except InvalidOperation:
        return None


def _parse_amount_text_parts(value: str | None) -> tuple[str, str, str] | None:
    """금액 문자열을 sign, whole digits, original fraction으로 분해한다."""
    if value is None:
        return None

    text = str(value).strip()
    if not text or text in _EMPTY_AMOUNT_TOKENS:
        return None

    text = text.replace(" ", "")
    if not _AMOUNT_PATTERN.fullmatch(text):
        return None

    sign = ""
    if text.startswith("-"):
        sign = "-"
        text = text[1:]
    elif text.startswith("+"):
        text = text[1:]

    whole, dot, fraction = text.partition(".")
    return sign, whole.replace(",", ""), fraction if dot else ""


def canonicalize_transfer_amount(value: str | None) -> str | None:
    """금액 문자열을 canonical form으로 정규화한다.

    - empty/null-like token은 None
    - 실제 decimal amount는 보존
    - 정수에 매우 가까운 decimal tail만 제거
    - 출력은 comma-separated plain text
    """
    decimal_value = _parse_amount_decimal(value)
    if decimal_value is None:
        return None

    normalized = _nearest_integer_if_artifact(decimal_value)
    return _format_decimal_text(normalized)


def format_source_transfer_amount(value: str | None) -> str | None:
    """지시서 표기 소수 자릿수를 보존하는 금액 문자열을 만든다.

    실제 소수부는 원문 자릿수를 유지한다. 단, 소수부가 모두 0인 정수형 소수와
    xlsx float-tail artifact는 기존처럼 정수 표현으로 정리한다.
    """
    decimal_value = _parse_amount_decimal(value)
    amount_parts = _parse_amount_text_parts(value)
    if decimal_value is None or amount_parts is None:
        return None

    normalized = _nearest_integer_if_artifact(decimal_value)
    if normalized != decimal_value:
        return _format_decimal_text(normalized)

    sign, whole_digits, fraction = amount_parts
    if not fraction or set(fraction) <= {"0"}:
        return _format_decimal_text(decimal_value)

    whole = f"{int(whole_digits or '0'):,}"
    return f"{sign}{whole}.{fraction}"


def format_final_transfer_amount(value: str | None) -> str | None:
    """이전 함수명 호환용 wrapper. 현재 최종 출력은 지시서 표기 스케일을 보존한다."""
    return format_source_transfer_amount(value)


def normalize_excel_numeric_cell(value: Any) -> str:
    """xlsx numeric cell 값을 artifact-aware 문자열로 렌더링한다."""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"

    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return str(value)

    return _format_decimal_plain_text(_nearest_integer_if_artifact(decimal_value))
