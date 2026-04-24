from __future__ import annotations

_PAGE_SHAPE_MARKERS = ("## Page ", "[PAGE ")
_SHEET_SHAPE_MARKERS = ("## Sheet ", "[SHEET ")

_DUPLICATE_COPY_RULES = (
    (
        "duplicate PDF copy; use XLSX attachment",
        _PAGE_SHAPE_MARKERS,
        ("page-based", "pdf copy"),
    ),
    (
        "duplicate XLSX copy; use PDF attachment",
        _SHEET_SHAPE_MARKERS,
        ("sheet-based", "spreadsheet"),
    ),
)


def detect_counterparty_guidance_non_instruction_reason(
    *,
    counterparty_guidance: str | None,
    markdown_text: str,
    raw_text: str,
) -> str | None:
    """Prompt에 명시된 attachment precedence를 실행 경로에서 강제한다.

    extractor core에 거래처명을 하드코딩하지 않기 위해, 회사명 대신
    prompt 본문에 적힌 duplicate-copy reason과 loader shape marker만 본다.
    """

    guidance = (counterparty_guidance or "").strip()
    if not guidance:
        return None

    combined_text = f"{markdown_text}\n{raw_text}".strip()
    if not combined_text:
        return None

    lowered_guidance = guidance.casefold()
    lowered_text = combined_text.casefold()
    for reason, loader_markers, guidance_hints in _DUPLICATE_COPY_RULES:
        if reason.casefold() not in lowered_guidance:
            continue
        if not any(hint in lowered_guidance for hint in guidance_hints):
            continue
        if any(marker.casefold() in lowered_text for marker in loader_markers):
            return reason
    return None
