from __future__ import annotations

import argparse
import csv
import json
import os
from collections import deque
from dataclasses import replace
from datetime import datetime
import filecmp
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
import unicodedata

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from app.amount_normalization import format_source_transfer_amount
from app.component import DocumentExtractionRequest, ExtractionComponent
from app.config import Settings, get_settings
from app.document_loader import DocumentLoadTaskPayload, DocumentLoader
from app.extraction import (
    detect_counterparty_guidance_non_instruction_reason,
    load_counterparty_guidance,
    parse_counterparty_guidance,
    resolve_counterparty_prompt_name,
)
from app.extraction.models import apply_only_pending_filter
from app.output_contract import (
    dedupe_serialized_order_payloads,
    normalize_counterparty_output_order_payloads,
    serialize_order_payload,
)
from app.schemas import ExtractionResult, OrderExtraction

DOCUMENT_DIR = REPO_ROOT / "document"
ANSWER_ROOT = REPO_ROOT / "거래처별_문서_정답"
OUTPUT_ROOT = REPO_ROOT / "output" / "test"
REPORT_ROOT = REPO_ROOT / "test_report"
REFERENCE_ROOT = REPO_ROOT / "참고자료"
AUTHORITY_REGISTRY_PATH = REFERENCE_ROOT / "authoritative_answer_registry.json"

EXCLUDED_DOCUMENT_NAMES = {".DS_Store", "흥국생명_HKlife_0413_정답지.md"}
CSV_FIELDNAMES = [
    "fund_code",
    "fund_name",
    "base_date",
    "t_day",
    "transfer_amount",
    "settle_class",
    "order_type",
]
CSV_SETTLE_LABELS = {"1": "청구", "2": "확정"}
CSV_ORDER_LABELS = {"1": "해지", "3": "설정"}
CSV_SETTLE_CODES = {label: code for code, label in CSV_SETTLE_LABELS.items()}
CSV_ORDER_CODES = {label: code for code, label in CSV_ORDER_LABELS.items()}
ONLY_PENDING_FALSE_COMPANIES = {"동양생명", "신한라이프", "한화생명", "흥국생명", "KDB생명"}
PROMPT_NAME_TO_COMPANY = {
    "ABL": "알리안츠(ABL)생명",
    "AIA": "AIA생명",
    "DB": "DB생명",
    "IBK": "IBK",
    "IM": "iM라이프",
    "KB": "KB라이프",
    "KDB": "KDB생명",
    "교보생명": "교보생명",
    "동양생명": "동양생명",
    "라이나": "라이나생명",
    "메트라이프생명": "메트라이프생명",
    "신한라이프": "신한라이프",
    "카디프": "카디프생명",
    "하나생명": "하나생명",
    "한화생명": "한화생명",
    "흥국생명": "흥국생명",
    "흥국생명-hanais": "흥국생명",
}
TARGETED_OUTPUT_POLICY_COMPANIES = {"동양생명", "한화생명", "신한라이프"}
BUSINESS_AUTHORITY_KIND = "business_answer"
SOURCE_REVIEW_AUTHORITY_KIND = "source_review"
CONFIDENCE_CONFIRMED = "confirmed"
CONFIDENCE_PROVISIONAL = "provisional"


def _normalize_text(value: str) -> str:
    return unicodedata.normalize("NFC", value)


def _now_kst() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _default_pdf_password(document_path: Path) -> str | None:
    if document_path.suffix.lower() != ".pdf":
        return None
    if "신한라이프" in _normalize_text(document_path.name):
        return "345678"
    return None


def _build_settings_for_case(*, case_root: Path) -> Settings:
    return replace(
        get_settings(),
        document_input_dir=REPO_ROOT,
        task_payload_output_dir=case_root / "handoff",
        debug_output_dir=case_root / "debug",
        delete_task_payload_files=False,
    )


def _slugify_document_name(document_path: Path) -> str:
    stem = _normalize_text(document_path.stem)
    slug = re.sub(r"[^0-9A-Za-z가-힣]+", "_", stem).strip("_")
    return slug or "document"


def _case_id(index: int, document_path: Path) -> str:
    suffix = document_path.suffix.lower().lstrip(".") or "file"
    return f"{index:02d}_{_slugify_document_name(document_path)}_{suffix}"


def _non_instruction_reason(source_path: str, reason: str) -> str:
    return f"Document is not a variable-annuity order instruction. path={source_path} reason={reason}"


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _canonical_csv_rows_from_payload(payload: dict[str, Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for order in payload.get("orders", []):
        rows.append(
            {
                "fund_code": str(order.get("fund_code", "")),
                "fund_name": str(order.get("fund_name", "")),
                "base_date": str(order.get("base_date", "")),
                "t_day": str(order.get("t_day", "")),
                "transfer_amount": str(order.get("transfer_amount", "")),
                "settle_class": CSV_SETTLE_LABELS.get(str(order.get("settle_class", "")), str(order.get("settle_class", ""))),
                "order_type": CSV_ORDER_LABELS.get(str(order.get("order_type", "")), str(order.get("order_type", ""))),
            }
        )
    return rows


def _write_canonical_csv(path: Path, payload: dict[str, Any]) -> None:
    rows = _canonical_csv_rows_from_payload(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def _read_canonical_csv(path: Path) -> tuple[bool, list[dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != CSV_FIELDNAMES:
            return False, []
        rows: list[dict[str, str]] = []
        for row in reader:
            rows.append({key: str(row.get(key, "")) for key in CSV_FIELDNAMES})
        return True, rows


def _extract_compare_core(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "base_date": payload.get("base_date"),
        "status": payload.get("status"),
        "reason": payload.get("reason"),
        "issues": payload.get("issues", []),
        "orders": payload.get("orders", []),
    }


def _payload_core_exact(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return _extract_compare_core(left) == _extract_compare_core(right)


def _load_external_authority_registry() -> dict[str, dict[str, Any]]:
    if not AUTHORITY_REGISTRY_PATH.exists():
        raise FileNotFoundError(f"missing authority registry: {AUTHORITY_REGISTRY_PATH}")
    payload = _read_json(AUTHORITY_REGISTRY_PATH)
    if not isinstance(payload, dict):
        raise ValueError("authority registry must be a JSON object")

    registry: dict[str, dict[str, Any]] = {}
    for document_name, entry in payload.items():
        if not isinstance(entry, dict):
            raise ValueError(f"authority registry entry must be an object: {document_name}")
        reference_path_text = str(entry.get("reference_path", "")).strip()
        format_name = str(entry.get("format", "")).strip()
        authority_kind = str(entry.get("authority_kind", BUSINESS_AUTHORITY_KIND)).strip() or BUSINESS_AUTHORITY_KIND
        if not reference_path_text:
            raise ValueError(f"authority registry reference_path is required: {document_name}")
        if format_name not in {"md_table_db_payload", "canonical_csv", "full_payload_json"}:
            raise ValueError(f"unsupported authority registry format for {document_name}: {format_name}")
        resolved_reference_path = Path(reference_path_text)
        if not resolved_reference_path.is_absolute():
            resolved_reference_path = REPO_ROOT / resolved_reference_path
        registry[_normalize_text(document_name)] = {
            "document_name": str(document_name),
            "reference_path": resolved_reference_path,
            "format": format_name,
            "authority_kind": authority_kind,
        }
    return registry


def _external_authority_entry_for_document(
    *,
    document_name: str,
    registry: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    active_registry = registry if registry is not None else _load_external_authority_registry()
    return active_registry.get(_normalize_text(document_name))


def _markdown_table_cells(line: str) -> list[str]:
    stripped = line.strip()
    if not stripped.startswith("|"):
        return []
    parts = stripped.strip("|").split("|")
    return [part.strip() for part in parts]


def _is_markdown_separator_row(cells: list[str]) -> bool:
    if not cells:
        return False
    return all(re.fullmatch(r":?-{3,}:?", cell.replace(" ", "")) for cell in cells)


def _canonicalize_reference_order_row(row: dict[str, Any]) -> dict[str, str]:
    fund_code = str(row.get("fund_code", "")).strip()
    fund_name = str(row.get("fund_name", "")).strip()
    base_date = str(row.get("base_date", "")).strip()
    t_day = str(row.get("t_day", "")).strip().zfill(2)
    settle_class = str(row.get("settle_class", "")).strip()
    order_type = str(row.get("order_type", "")).strip()
    transfer_amount = format_source_transfer_amount(row.get("transfer_amount")) or str(row.get("transfer_amount", "")).strip()

    if settle_class in CSV_SETTLE_CODES:
        settle_class = CSV_SETTLE_CODES[settle_class]
    if order_type in CSV_ORDER_CODES:
        order_type = CSV_ORDER_CODES[order_type]
    if settle_class not in {"1", "2"}:
        raise ValueError(f"invalid settle_class in reference order row: {settle_class}")
    if order_type not in {"1", "3"}:
        raise ValueError(f"invalid order_type in reference order row: {order_type}")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", base_date):
        raise ValueError(f"invalid base_date in reference order row: {base_date}")
    if not re.fullmatch(r"\d{2}", t_day):
        raise ValueError(f"invalid t_day in reference order row: {t_day}")
    if not fund_code:
        raise ValueError("fund_code is required in reference order row")
    if not transfer_amount:
        raise ValueError("transfer_amount is required in reference order row")
    return {
        "fund_code": fund_code,
        "fund_name": fund_name,
        "base_date": base_date,
        "t_day": t_day,
        "transfer_amount": transfer_amount,
        "settle_class": settle_class,
        "order_type": order_type,
    }


def _payload_from_reference_orders(orders: list[dict[str, Any]]) -> dict[str, Any]:
    if not orders:
        return {
            "base_date": None,
            "status": "SKIPPED",
            "reason": "거래가 없는 문서",
            "issues": [],
            "orders": [],
        }
    canonical_orders = [_canonicalize_reference_order_row(order) for order in orders]
    base_dates = {order["base_date"] for order in canonical_orders}
    base_date = canonical_orders[0]["base_date"] if len(base_dates) == 1 else None
    return {
        "base_date": base_date,
        "status": "COMPLETED",
        "reason": None,
        "issues": [],
        "orders": canonical_orders,
    }


def _parse_md_table_db_payload(reference_path: Path) -> dict[str, Any]:
    markdown_text = reference_path.read_text(encoding="utf-8")
    lines = markdown_text.splitlines()
    rows: list[dict[str, Any]] = []
    cursor = 0
    expected_header_fields = set(CSV_FIELDNAMES)

    while cursor < len(lines):
        cells = _markdown_table_cells(lines[cursor])
        if len(cells) != len(CSV_FIELDNAMES) or set(cells) != expected_header_fields:
            cursor += 1
            continue
        if cursor + 1 >= len(lines):
            break
        separator_cells = _markdown_table_cells(lines[cursor + 1])
        if not _is_markdown_separator_row(separator_cells):
            cursor += 1
            continue
        header = cells
        cursor += 2
        while cursor < len(lines):
            row_cells = _markdown_table_cells(lines[cursor])
            if not row_cells:
                break
            if _is_markdown_separator_row(row_cells):
                cursor += 1
                continue
            if len(row_cells) != len(header):
                break
            rows.append(dict(zip(header, row_cells, strict=True)))
            cursor += 1
        continue

    if not rows:
        raise ValueError(f"no canonical markdown answer table found: {reference_path}")
    return _payload_from_reference_orders(rows)


def _parse_canonical_csv_payload(reference_path: Path) -> dict[str, Any]:
    header_ok, rows = _read_canonical_csv(reference_path)
    if not header_ok:
        raise ValueError(f"reference csv does not match canonical header: {reference_path}")
    return _payload_from_reference_orders(rows)


def _parse_full_payload_json(reference_path: Path) -> dict[str, Any]:
    payload = _read_json(reference_path)
    if not isinstance(payload, dict):
        raise ValueError(f"reference full payload must be a JSON object: {reference_path}")
    core = _extract_compare_core(payload)
    orders = [_canonicalize_reference_order_row(order) for order in core.get("orders", [])]
    return {
        "base_date": core.get("base_date"),
        "status": core.get("status"),
        "reason": core.get("reason"),
        "issues": list(core.get("issues", [])),
        "orders": orders,
    }


def _load_external_authority_payload(
    *,
    task_payload: DocumentLoadTaskPayload,
    model_name: str,
    registry: dict[str, dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    entry = _external_authority_entry_for_document(
        document_name=task_payload.file_name,
        registry=registry,
    )
    if entry is None:
        return None

    reference_path = Path(entry["reference_path"])
    if not reference_path.exists():
        raise FileNotFoundError(f"missing external authority reference: {reference_path}")

    format_name = entry["format"]
    if format_name == "md_table_db_payload":
        reference_payload = _parse_md_table_db_payload(reference_path)
    elif format_name == "canonical_csv":
        reference_payload = _parse_canonical_csv_payload(reference_path)
    elif format_name == "full_payload_json":
        reference_payload = _parse_full_payload_json(reference_path)
    else:  # pragma: no cover - guarded by registry validation
        raise ValueError(f"unsupported external authority format: {format_name}")

    authoritative_payload = {
        "file_name": task_payload.file_name,
        "source_path": task_payload.source_path,
        "model_name": model_name,
        "base_date": reference_payload.get("base_date"),
        "status": reference_payload.get("status"),
        "reason": reference_payload.get("reason"),
        "issues": list(reference_payload.get("issues", [])),
        "orders": list(reference_payload.get("orders", [])),
    }
    provenance = {
        "authority_basis": str(entry.get("authority_kind", BUSINESS_AUTHORITY_KIND)),
        "confidence_tier": CONFIDENCE_CONFIRMED,
        "human_confirmed": True,
        "external_reference_path": str(reference_path),
    }
    return authoritative_payload, provenance


def _answer_action_updates_payload(action: str) -> bool:
    return action in {"new", "updated", "renamed+updated"}


def _review_provenance(
    *,
    authority_basis: str,
    external_reference_path: str | None,
    answer_action: str,
) -> dict[str, Any]:
    human_confirmed = authority_basis == BUSINESS_AUTHORITY_KIND
    confidence_tier = CONFIDENCE_CONFIRMED if human_confirmed else CONFIDENCE_PROVISIONAL
    return {
        "authority_basis": authority_basis,
        "confidence_tier": confidence_tier,
        "human_confirmed": human_confirmed,
        "external_reference_path": external_reference_path,
        "answer_updated_without_business": not human_confirmed and _answer_action_updates_payload(answer_action),
    }


def _sorted_documents() -> list[Path]:
    documents = [
        path
        for path in sorted(DOCUMENT_DIR.iterdir(), key=lambda item: _normalize_text(item.name))
        if path.name not in EXCLUDED_DOCUMENT_NAMES
    ]
    return documents


def _find_normalized_path(parent: Path, target_name: str) -> Path | None:
    normalized_target = _normalize_text(target_name)
    for child in parent.iterdir():
        if _normalize_text(child.name) == normalized_target:
            return child
    return None


def _build_answer_source_index() -> dict[str, list[tuple[str, Path]]]:
    index: dict[str, list[tuple[str, Path]]] = {}
    if not ANSWER_ROOT.exists():
        return index
    for company_dir in sorted(ANSWER_ROOT.iterdir(), key=lambda item: _normalize_text(item.name)):
        if not company_dir.is_dir():
            continue
        for child in company_dir.iterdir():
            if child.suffix.lower() in {".json", ".csv"}:
                continue
            index.setdefault(_normalize_text(child.name), []).append((company_dir.name, child))
    return index


def _resolve_company_name(
    *,
    document_path: Path,
    answer_source_index: dict[str, list[tuple[str, Path]]],
    loader: DocumentLoader,
    settings: Settings,
) -> str:
    normalized_name = _normalize_text(document_path.name)
    existing = answer_source_index.get(normalized_name, [])
    if len(existing) == 1:
        return existing[0][0]
    if len(existing) > 1:
        unique_companies = list(dict.fromkeys(company for company, _path in existing))
        if len(unique_companies) == 1:
            return unique_companies[0]

    task_payload = loader.build_task_payload(
        document_path,
        chunk_size_chars=settings.llm_chunk_size_chars,
        pdf_password=_default_pdf_password(document_path),
    )
    prompt_name = resolve_counterparty_prompt_name(
        document_path,
        document_text=task_payload.markdown_text or task_payload.raw_text,
    )
    company_name = PROMPT_NAME_TO_COMPANY.get(prompt_name or "")
    if company_name:
        return company_name
    if existing:
        return existing[0][0]
    raise ValueError(f"Could not resolve company for document: {document_path}")


def _stem_uses_exact_name_json(document_path: Path, stem_counts: dict[str, int]) -> bool:
    return stem_counts.get(_normalize_text(document_path.stem), 0) > 1


def _build_answer_targets(
    *,
    document_path: Path,
    company_name: str,
    stem_counts: dict[str, int],
) -> dict[str, Path]:
    answer_dir = ANSWER_ROOT / company_name
    answer_dir.mkdir(parents=True, exist_ok=True)
    if _stem_uses_exact_name_json(document_path, stem_counts):
        json_name = f"{document_path.name}.json"
        csv_name = f"{document_path.name}.csv"
    else:
        json_name = f"{document_path.stem}.json"
        csv_name = f"{document_path.stem}.csv"
    return {
        "answer_dir": answer_dir,
        "source_path": answer_dir / document_path.name,
        "json_path": answer_dir / json_name,
        "csv_path": answer_dir / csv_name,
    }


def _strip_json_fence(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```(?:json)?\s*", "", stripped)
        stripped = re.sub(r"\s*```$", "", stripped)
    return stripped.strip()


def _extract_json_object_text(text: str) -> str:
    stripped = _strip_json_fence(text)
    if stripped.startswith("{") and stripped.endswith("}"):
        return stripped
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start != -1 and end != -1 and start < end:
        return stripped[start : end + 1]
    return stripped


def _coerce_string_content(response_content: Any) -> str:
    if isinstance(response_content, str):
        return response_content
    if isinstance(response_content, list):
        parts: list[str] = []
        for item in response_content:
            if isinstance(item, dict) and "text" in item:
                parts.append(str(item.get("text", "")))
            else:
                parts.append(str(item))
        return "\n".join(parts)
    return str(response_content)


def _authoritative_review_system_prompt() -> str:
    return (
        "You audit final variable-annuity payloads against the original source document.\n"
        "Return exactly one JSON object and nothing else.\n"
        "The source document is the truth. Candidate payloads are hypotheses only.\n"
        "Prefer selecting an existing candidate unchanged unless the source document proves a concrete mismatch.\n"
        "Return the FINAL DB-storable payload review decision using this schema:\n"
        "{\n"
        '  "selected_candidate": "extraction or answer or corrected",\n'
        '  "payload": {\n'
        '    "base_date": "YYYY-MM-DD or null",\n'
        '    "status": "COMPLETED or SKIPPED or FAILED",\n'
        '    "reason": "string or null",\n'
        '    "issues": [],\n'
        '    "orders": [\n'
        "      {\n"
        '        "fund_code": "string",\n'
        '        "fund_name": "string",\n'
        '        "settle_class": "1 or 2",\n'
        '        "order_type": "1 or 3",\n'
        '        "base_date": "YYYY-MM-DD",\n'
        '        "t_day": "two-digit string",\n'
        '        "transfer_amount": "unsigned numeric string"\n'
        "      }\n"
        "    ]\n"
        "  }\n"
        "}\n"
        "Rules:\n"
        '- settle_class "1" means pending/청구 and "2" means confirmed/확정.\n'
        '- order_type "1" means redemption/해지 and "3" means subscription/설정.\n'
        "- transfer_amount in the final payload must be unsigned and formatted as a document-faithful numeric string.\n"
        "- Preserve leading zeros in fund_code and preserve meaningful spaces inside fund_name.\n"
        '- If one candidate is exactly correct, set selected_candidate to that candidate and return that payload unchanged under "payload".\n'
        '- Use selected_candidate="corrected" only when the source document proves a concrete mismatch in both candidates or when one candidate is missing.\n'
        "- If neither candidate is exactly correct, correct only the rows proven by the source document.\n"
        "- Do not convert a final saved payload candidate back into an internal representation.\n"
        "- Never invent rows that are not supported by the document.\n"
        '- Use [] for issues unless the final payload is impossible to determine from the source.\n'
    )


def _authoritative_review_user_prompt(
    *,
    case_record: dict[str, Any],
    task_payload: DocumentLoadTaskPayload,
    counterparty_guidance: str | None,
    extraction_candidate: dict[str, Any],
    answer_candidate: dict[str, Any] | None,
) -> str:
    visible_guidance = parse_counterparty_guidance(counterparty_guidance).visible_guidance
    output_policy_note = "None."
    if case_record["db_company"] in TARGETED_OUTPUT_POLICY_COMPANIES:
        output_policy_note = (
            "The final saved payload for this company keeps only pending rows whose final output t_day is 03. "
            "Candidates may already reflect this; use the source document to judge the final saved payload."
        )
    return (
        f"Document name: {task_payload.file_name}\n"
        f"Source path: {task_payload.source_path}\n"
        f"Company: {case_record['db_company']}\n"
        f"only_pending downstream contract: {str(case_record['only_pending']).lower()}\n"
        f"Counterparty output policy note: {output_policy_note}\n"
        "Final payload reminders:\n"
        "- The candidates below already try to represent the FINAL DB-storable payload, not internal extraction rows.\n"
        "- If only_pending=true, the final saved payload may legitimately keep settle_class='1' and t_day='01' for same-day rows. Do not convert such rows back to settle_class='2'.\n"
        "- For 메트라이프생명 final saved payload, fund_code is expected to be '-'.\n"
        "- For 흥국생명 final saved payload, fund_name may legitimately be '-'.\n"
        "- If both candidates are identical and the source shows no concrete mismatch, select extraction and return the same payload unchanged.\n"
        "- If answer candidate is null, choose extraction unless the source proves it wrong.\n"
        "\n"
        f"Counterparty guidance:\n{visible_guidance or 'None.'}\n\n"
        f"Candidate extraction payload core:\n{json.dumps(_extract_compare_core(extraction_candidate), ensure_ascii=False, indent=2)}\n\n"
        f"Candidate answer payload core:\n{json.dumps(_extract_compare_core(answer_candidate) if answer_candidate else None, ensure_ascii=False, indent=2)}\n\n"
        "Document text:\n"
        "Structured markdown view:\n"
        f"```text\n{task_payload.markdown_text}\n```\n\n"
        "Raw text backup:\n"
        f"```text\n{task_payload.raw_text}\n```\n"
    )


def _validate_authoritative_payload(payload: dict[str, Any]) -> None:
    selected_candidate = str(payload.get("selected_candidate", "")).strip()
    if selected_candidate not in {"extraction", "answer", "corrected"}:
        raise ValueError("selected_candidate must be extraction, answer, or corrected")
    if not isinstance(payload.get("payload"), dict):
        raise ValueError("payload field must be an object")
    payload = payload["payload"]
    if payload.get("base_date") is not None and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(payload.get("base_date")).strip()):
        raise ValueError("authoritative base_date must be YYYY-MM-DD or null")
    if str(payload.get("status", "")).strip() not in {"COMPLETED", "SKIPPED", "FAILED"}:
        raise ValueError("authoritative status must be COMPLETED, SKIPPED, or FAILED")
    if payload.get("issues") is not None and not isinstance(payload.get("issues"), list):
        raise ValueError("authoritative issues must be a list")
    orders = payload.get("orders")
    if not isinstance(orders, list):
        raise ValueError("authoritative orders must be a list")
    for index, order in enumerate(orders, start=1):
        if not isinstance(order, dict):
            raise ValueError(f"order #{index} is not an object")
        settle_class = str(order.get("settle_class", "")).strip()
        order_type = str(order.get("order_type", "")).strip()
        base_date = str(order.get("base_date", "")).strip()
        t_day = str(order.get("t_day", "")).strip()
        amount = str(order.get("transfer_amount", "")).strip()
        if settle_class not in {"1", "2"}:
            raise ValueError(f"order #{index} settle_class must be 1 or 2")
        if order_type not in {"1", "3"}:
            raise ValueError(f"order #{index} order_type must be 1 or 3")
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", base_date):
            raise ValueError(f"order #{index} base_date must be YYYY-MM-DD")
        if not re.fullmatch(r"\d{2}", t_day):
            raise ValueError(f"order #{index} t_day must be two digits")
        if not amount or amount.startswith("-") or amount.startswith("+"):
            raise ValueError(f"order #{index} transfer_amount must be unsigned")


def _canonicalize_authoritative_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized_choice = str(payload.get("selected_candidate", "")).strip()
    payload = dict(payload.get("payload", {}))
    normalized = {
        "selected_candidate": normalized_choice,
        "base_date": payload.get("base_date"),
        "status": payload.get("status"),
        "reason": payload.get("reason"),
        "issues": list(payload.get("issues", [])),
        "orders": [],
    }
    for order in payload.get("orders", []):
        order_dict = dict(order)
        formatted_amount = format_source_transfer_amount(order_dict.get("transfer_amount"))
        if formatted_amount is not None:
            order_dict["transfer_amount"] = formatted_amount
        order_dict["fund_code"] = str(order_dict.get("fund_code", "")).strip()
        order_dict["fund_name"] = str(order_dict.get("fund_name", "")).strip()
        order_dict["base_date"] = str(order_dict.get("base_date", "")).strip()
        order_dict["t_day"] = str(order_dict.get("t_day", "")).strip()
        order_dict["settle_class"] = str(order_dict.get("settle_class", "")).strip()
        order_dict["order_type"] = str(order_dict.get("order_type", "")).strip()
        normalized["orders"].append(order_dict)
    return normalized


def _run_authoritative_review_llm(
    *,
    case_record: dict[str, Any],
    task_payload: DocumentLoadTaskPayload,
    counterparty_guidance: str | None,
    review_root: Path,
    extraction_candidate: dict[str, Any],
    answer_candidate: dict[str, Any] | None,
) -> dict[str, Any]:
    settings = get_settings()
    llm = ChatOpenAI(
        model=settings.llm_model,
        base_url=settings.llm_base_url,
        api_key=settings.llm_api_key,
        temperature=0,
        max_tokens=settings.llm_max_tokens,
        timeout=settings.llm_timeout_seconds,
    )
    system_prompt = _authoritative_review_system_prompt()
    user_prompt = _authoritative_review_user_prompt(
        case_record=case_record,
        task_payload=task_payload,
        counterparty_guidance=counterparty_guidance,
        extraction_candidate=extraction_candidate,
        answer_candidate=answer_candidate,
    )
    prompt_path = review_root / "authoritative_review_prompt.txt"
    prompt_path.write_text(f"[System]\n{system_prompt}\n\n[User]\n{user_prompt}\n", encoding="utf-8")

    last_error: Exception | None = None
    for attempt in range(1, 4):
        response = llm.invoke([SystemMessage(content=system_prompt), HumanMessage(content=user_prompt)])
        response_text = _coerce_string_content(response.content)
        (review_root / f"authoritative_review_response_attempt{attempt}.txt").write_text(response_text, encoding="utf-8")
        try:
            payload = json.loads(_extract_json_object_text(response_text))
            _validate_authoritative_payload(payload)
            return _canonicalize_authoritative_payload(payload)
        except Exception as exc:  # pragma: no cover - runtime robustness
            last_error = exc
            time.sleep(1.0)
    raise ValueError(f"authoritative review JSON parse failed: {last_error}")


def _load_single_metrics(debug_root: Path) -> tuple[dict[str, Any] | None, float | None]:
    metrics_paths = sorted(debug_root.glob("*_llm_metrics.json"))
    if metrics_paths:
        metrics = _read_json(metrics_paths[0])
        return {
            "stage_retry_invocations": metrics.get("stage_retry_invocations", {}),
            "transport_retry_attempts": metrics.get("transport_retry_attempts", {}),
        }, metrics.get("total_elapsed_seconds")

    log_paths = sorted(debug_root.glob("*_llm_pipeline.log"))
    if not log_paths:
        return None, None
    return {"stage_retry_invocations": {}, "transport_retry_attempts": {}}, None


def _prepare_answer_artifacts(
    *,
    document_path: Path,
    answer_targets: dict[str, Path],
) -> set[str]:
    actions: set[str] = set()
    answer_dir = answer_targets["answer_dir"]
    answer_dir.mkdir(parents=True, exist_ok=True)

    for key, target_path in (
        ("source_path", answer_targets["source_path"]),
        ("json_path", answer_targets["json_path"]),
        ("csv_path", answer_targets["csv_path"]),
    ):
        existing_path = _find_normalized_path(answer_dir, target_path.name)
        if existing_path is not None and existing_path != target_path and not target_path.exists():
            existing_path.rename(target_path)
            actions.add("renamed")

    if not answer_targets["source_path"].exists() or not filecmp.cmp(document_path, answer_targets["source_path"], shallow=False):
        answer_targets["source_path"].parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(document_path, answer_targets["source_path"])
        actions.add("updated" if answer_targets["source_path"].exists() else "new")

    return actions


def _write_answer_set(
    *,
    authoritative_payload: dict[str, Any],
    answer_targets: dict[str, Path],
    document_path: Path,
) -> str:
    preexisting = {
        "source": answer_targets["source_path"].exists(),
        "json": answer_targets["json_path"].exists(),
        "csv": answer_targets["csv_path"].exists(),
    }
    actions = _prepare_answer_artifacts(document_path=document_path, answer_targets=answer_targets)

    json_payload = dict(authoritative_payload)
    json_payload["file_name"] = document_path.name
    json_payload["source_path"] = str(document_path)
    json_payload["model_name"] = get_settings().llm_model

    _write_json(answer_targets["json_path"], json_payload)
    _write_canonical_csv(answer_targets["csv_path"], authoritative_payload)

    if not preexisting["json"] or not preexisting["csv"] or not preexisting["source"]:
        actions.add("new")
    if not actions:
        return "none"
    if "new" in actions:
        return "new"
    if "updated" in actions and "renamed" in actions:
        return "renamed+updated"
    if "updated" in actions:
        return "updated"
    if "renamed" in actions:
        return "renamed"
    return "none"


def _compare_answer_set(
    *,
    authoritative_payload: dict[str, Any],
    answer_targets: dict[str, Path],
    document_path: Path,
) -> dict[str, Any]:
    source_copy_exact = answer_targets["source_path"].exists() and filecmp.cmp(
        document_path,
        answer_targets["source_path"],
        shallow=False,
    )
    answer_json = None
    if answer_targets["json_path"].exists():
        try:
            answer_json = _read_json(answer_targets["json_path"])
        except Exception:
            answer_json = None
    answer_json_exact = isinstance(answer_json, dict) and _payload_core_exact(answer_json, authoritative_payload)
    answer_csv_header_ok = False
    answer_csv_exact = False
    if answer_targets["csv_path"].exists():
        try:
            answer_csv_header_ok, answer_csv_rows = _read_canonical_csv(answer_targets["csv_path"])
            answer_csv_exact = answer_csv_header_ok and answer_csv_rows == _canonical_csv_rows_from_payload(authoritative_payload)
        except Exception:
            answer_csv_header_ok = False
            answer_csv_exact = False
    return {
        "source_copy_exact": source_copy_exact,
        "json_exact": answer_json_exact,
        "csv_header_ok": answer_csv_header_ok,
        "csv_exact": answer_csv_exact,
    }


def _resolve_authoritative_payload_for_case(
    *,
    case_record: dict[str, Any],
    task_payload: DocumentLoadTaskPayload,
    counterparty_guidance: str | None,
    review_root: Path,
    extraction_candidate: dict[str, Any],
    answer_candidate: dict[str, Any] | None,
    model_name: str,
    external_registry: dict[str, dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], str, list[str], dict[str, Any]]:
    external_authority = _load_external_authority_payload(
        task_payload=task_payload,
        model_name=model_name,
        registry=external_registry,
    )
    if external_authority is not None:
        authoritative_payload, provenance = external_authority
        return authoritative_payload, "BYPASSED_EXTERNAL_AUTHORITY", [], provenance

    if task_payload.non_instruction_reason:
        return (
            {
                "file_name": task_payload.file_name,
                "source_path": task_payload.source_path,
                "model_name": model_name,
                "base_date": None,
                "status": "SKIPPED",
                "reason": _non_instruction_reason(task_payload.source_path, task_payload.non_instruction_reason),
                "issues": [],
                "orders": [],
            },
            "PASS",
            [],
            {
                "authority_basis": SOURCE_REVIEW_AUTHORITY_KIND,
                "confidence_tier": CONFIDENCE_PROVISIONAL,
                "human_confirmed": False,
                "external_reference_path": None,
            },
        )

    if task_payload.allow_empty_result:
        return (
            {
                "file_name": task_payload.file_name,
                "source_path": task_payload.source_path,
                "model_name": model_name,
                "base_date": None,
                "status": "SKIPPED",
                "reason": "거래가 없는 문서",
                "issues": [],
                "orders": [],
            },
            "PASS",
            [],
            {
                "authority_basis": SOURCE_REVIEW_AUTHORITY_KIND,
                "confidence_tier": CONFIDENCE_PROVISIONAL,
                "human_confirmed": False,
                "external_reference_path": None,
            },
        )

    if task_payload.scope_excludes_all_funds:
        return (
            {
                "file_name": task_payload.file_name,
                "source_path": task_payload.source_path,
                "model_name": model_name,
                "base_date": None,
                "status": "SKIPPED",
                "reason": "대상 거래처 scope에 해당하는 주문 없음",
                "issues": [],
                "orders": [],
            },
            "PASS",
            [],
            {
                "authority_basis": SOURCE_REVIEW_AUTHORITY_KIND,
                "confidence_tier": CONFIDENCE_PROVISIONAL,
                "human_confirmed": False,
                "external_reference_path": None,
            },
        )

    prompt_directed_reason = detect_counterparty_guidance_non_instruction_reason(
        counterparty_guidance=counterparty_guidance,
        markdown_text=task_payload.markdown_text,
        raw_text=task_payload.raw_text,
    )
    if prompt_directed_reason:
        return (
            {
                "file_name": task_payload.file_name,
                "source_path": task_payload.source_path,
                "model_name": model_name,
                "base_date": None,
                "status": "SKIPPED",
                "reason": _non_instruction_reason(task_payload.source_path, prompt_directed_reason),
                "issues": [],
                "orders": [],
            },
            "PASS",
            [],
            {
                "authority_basis": SOURCE_REVIEW_AUTHORITY_KIND,
                "confidence_tier": CONFIDENCE_PROVISIONAL,
                "human_confirmed": False,
                "external_reference_path": None,
            },
        )

    try:
        authoritative_core = _run_authoritative_review_llm(
            case_record=case_record,
            task_payload=task_payload,
            counterparty_guidance=counterparty_guidance,
            review_root=review_root,
            extraction_candidate=extraction_candidate,
            answer_candidate=answer_candidate,
        )
        selected_candidate = authoritative_core.get("selected_candidate")
        if selected_candidate == "extraction":
            authoritative_core = _extract_compare_core(extraction_candidate)
        elif selected_candidate == "answer" and isinstance(answer_candidate, dict):
            authoritative_core = _extract_compare_core(answer_candidate)
        else:
            authoritative_core = {
                "base_date": authoritative_core.get("base_date"),
                "status": authoritative_core.get("status"),
                "reason": authoritative_core.get("reason"),
                "issues": authoritative_core.get("issues", []),
                "orders": authoritative_core.get("orders", []),
            }
        return (
            {
                "file_name": task_payload.file_name,
                "source_path": task_payload.source_path,
                "model_name": model_name,
                "base_date": authoritative_core.get("base_date"),
                "status": authoritative_core.get("status"),
                "reason": authoritative_core.get("reason"),
                "issues": authoritative_core.get("issues", []),
                "orders": authoritative_core.get("orders", []),
            },
            "PASS",
            [],
            {
                "authority_basis": SOURCE_REVIEW_AUTHORITY_KIND,
                "confidence_tier": CONFIDENCE_PROVISIONAL,
                "human_confirmed": False,
                "external_reference_path": None,
            },
        )
    except Exception as exc:  # pragma: no cover - runtime robustness
        return (
            {
                "file_name": task_payload.file_name,
                "source_path": task_payload.source_path,
                "model_name": model_name,
                "base_date": None,
                "status": "FAILED",
                "reason": str(exc),
                "issues": [str(exc)],
                "orders": [],
            },
            "BLOCKED",
            [str(exc)],
            {
                "authority_basis": SOURCE_REVIEW_AUTHORITY_KIND,
                "confidence_tier": CONFIDENCE_PROVISIONAL,
                "human_confirmed": False,
                "external_reference_path": None,
            },
        )


def _build_case_manifest() -> list[dict[str, Any]]:
    settings = get_settings()
    loader = DocumentLoader()
    documents = _sorted_documents()
    stem_counts: dict[str, int] = {}
    for document_path in documents:
        normalized_stem = _normalize_text(document_path.stem)
        stem_counts[normalized_stem] = stem_counts.get(normalized_stem, 0) + 1

    answer_source_index = _build_answer_source_index()
    manifest: list[dict[str, Any]] = []
    for index, document_path in enumerate(documents, start=1):
        company_name = _resolve_company_name(
            document_path=document_path,
            answer_source_index=answer_source_index,
            loader=loader,
            settings=settings,
        )
        answer_targets = _build_answer_targets(
            document_path=document_path,
            company_name=company_name,
            stem_counts=stem_counts,
        )
        manifest.append(
            {
                "case_index": index,
                "case_id": _case_id(index, document_path),
                "document_name": document_path.name,
                "source_path": str(document_path),
                "db_company": company_name,
                "only_pending": company_name not in ONLY_PENDING_FALSE_COMPANIES,
                "answer_source_path": str(answer_targets["source_path"]),
                "answer_json_path": str(answer_targets["json_path"]),
                "answer_csv_path": str(answer_targets["csv_path"]),
            }
        )
    return manifest


def _preflight(*, run_root: Path) -> dict[str, Any]:
    settings = get_settings()
    preflight_root = run_root / "preflight"
    preflight_root.mkdir(parents=True, exist_ok=True)
    registry_ok = True
    registry_error = ""
    registry_entry_count = 0
    try:
        registry_entry_count = len(_load_external_authority_registry())
    except Exception as exc:
        registry_ok = False
        registry_error = str(exc)
    py_compile_cmd = [
        sys.executable,
        "-m",
        "py_compile",
        "app/config.py",
        "app/document_loader.py",
        "app/extractor.py",
        "app/component.py",
        str(Path(__file__).relative_to(REPO_ROOT)),
    ]
    py_compile_run = subprocess.run(
        py_compile_cmd,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    git_status_run = subprocess.run(
        ["git", "status", "--short", "--untracked-files=all"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    lsof_run = subprocess.run(
        ["lsof", "-nP", "-iTCP:3910", "-sTCP:LISTEN"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    curl_run = subprocess.run(
        [
            "curl",
            "-sS",
            "-H",
            f"Authorization: Bearer {settings.llm_api_key}",
            f"{settings.llm_base_url}/models",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    tunnel_reconnected = False
    if curl_run.returncode != 0:
        ssh_run = subprocess.run(
            [
                "ssh",
                "-f",
                "-N",
                "-o",
                "BatchMode=yes",
                "-o",
                "ExitOnForwardFailure=yes",
                "-L",
                "3910:localhost:3950",
                "minisoft@1.241.20.229",
                "-p",
                "2194",
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        time.sleep(2.0)
        curl_run = subprocess.run(
            [
                "curl",
                "-sS",
                "-H",
                f"Authorization: Bearer {settings.llm_api_key}",
                f"{settings.llm_base_url}/models",
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        tunnel_reconnected = ssh_run.returncode == 0 and curl_run.returncode == 0

    prompt_map_ok = (REPO_ROOT / "app" / "prompts" / "counterparty_prompt_map.yaml").exists()
    preflight_summary = {
        "started_at": _now_kst(),
        "llm_base_url": settings.llm_base_url,
        "llm_model": settings.llm_model,
        "python_executable": sys.executable,
        "py_compile_ok": py_compile_run.returncode == 0,
        "py_compile_stdout": py_compile_run.stdout,
        "py_compile_stderr": py_compile_run.stderr,
        "git_status_returncode": git_status_run.returncode,
        "git_status_stdout": git_status_run.stdout,
        "git_status_stderr": git_status_run.stderr,
        "listener_returncode": lsof_run.returncode,
        "listener_stdout": lsof_run.stdout,
        "listener_stderr": lsof_run.stderr,
        "models_returncode": curl_run.returncode,
        "models_stdout": curl_run.stdout,
        "models_stderr": curl_run.stderr,
        "tunnel_reconnected": tunnel_reconnected,
        "prompt_map_exists": prompt_map_ok,
        "authority_registry_path": str(AUTHORITY_REGISTRY_PATH),
        "authority_registry_ok": registry_ok,
        "authority_registry_error": registry_error,
        "authority_registry_entry_count": registry_entry_count,
        "blocked": py_compile_run.returncode != 0 or curl_run.returncode != 0 or not prompt_map_ok or not registry_ok,
    }
    _write_json(preflight_root / "preflight_summary.json", preflight_summary)
    return preflight_summary


def _run_case_child(case_file: Path) -> int:
    case_record = _read_json(case_file)
    case_root = Path(case_record["case_root"])
    result_root = case_root / "result"
    review_root = case_root / "review"
    result_root.mkdir(parents=True, exist_ok=True)
    review_root.mkdir(parents=True, exist_ok=True)

    settings = _build_settings_for_case(case_root=case_root)
    component = ExtractionComponent(settings=settings)
    request = DocumentExtractionRequest(
        case_record["source_path"],
        pdf_password=_default_pdf_password(Path(case_record["source_path"])),
        only_pending=bool(case_record["only_pending"]),
        use_counterparty_prompt=True,
    )
    started_at = time.time()

    payload = component.extract_document_payload(request)
    result_json_path = result_root / f"{case_record['case_id']}.json"
    result_csv_path = result_root / f"{case_record['case_id']}.csv"
    _write_json(result_json_path, payload)
    _write_canonical_csv(result_csv_path, payload)

    handoff_files = sorted((case_root / "handoff").rglob("*_task_payload.json"))
    if not handoff_files:
        raise ValueError(f"missing handoff JSON for {case_record['case_id']}")
    task_payload_path = handoff_files[0]
    task_payload = DocumentLoadTaskPayload.read_json(task_payload_path)
    counterparty_guidance = load_counterparty_guidance(
        task_payload.source_path,
        use_counterparty_prompt=True,
        document_text=task_payload.markdown_text,
    )
    external_registry = _load_external_authority_registry()

    answer_candidate: dict[str, Any] | None = None
    answer_json_path = Path(case_record["answer_json_path"])
    if answer_json_path.exists():
        try:
            loaded_answer_candidate = _read_json(answer_json_path)
            if isinstance(loaded_answer_candidate, dict):
                answer_candidate = loaded_answer_candidate
        except Exception:
            answer_candidate = None
    authoritative_payload, source_review_status, source_review_issues, authority_resolution = _resolve_authoritative_payload_for_case(
        case_record=case_record,
        task_payload=task_payload,
        counterparty_guidance=counterparty_guidance,
        review_root=review_root,
        extraction_candidate=payload,
        answer_candidate=answer_candidate,
        model_name=settings.llm_model,
        external_registry=external_registry,
    )

    authoritative_json_path = review_root / "authoritative_payload.json"
    _write_json(authoritative_json_path, authoritative_payload)

    extraction_exact = _payload_core_exact(payload, authoritative_payload)
    answer_targets = {
        "answer_dir": Path(case_record["answer_json_path"]).parent,
        "source_path": Path(case_record["answer_source_path"]),
        "json_path": Path(case_record["answer_json_path"]),
        "csv_path": Path(case_record["answer_csv_path"]),
    }
    before_answer_compare = _compare_answer_set(
        authoritative_payload=authoritative_payload,
        answer_targets=answer_targets,
        document_path=Path(case_record["source_path"]),
    )
    if not (before_answer_compare["source_copy_exact"] and before_answer_compare["json_exact"] and before_answer_compare["csv_exact"]):
        answer_action = _write_answer_set(
            authoritative_payload=authoritative_payload,
            answer_targets=answer_targets,
            document_path=Path(case_record["source_path"]),
        )
    else:
        _prepare_answer_artifacts(document_path=Path(case_record["source_path"]), answer_targets=answer_targets)
        answer_action = "none"

    after_answer_compare = _compare_answer_set(
        authoritative_payload=authoritative_payload,
        answer_targets=answer_targets,
        document_path=Path(case_record["source_path"]),
    )
    provenance = _review_provenance(
        authority_basis=str(authority_resolution["authority_basis"]),
        external_reference_path=authority_resolution.get("external_reference_path"),
        answer_action=answer_action,
    )
    metrics, elapsed_seconds = _load_single_metrics(case_root / "debug")
    review_verdict = "PASS"
    if source_review_status == "BLOCKED":
        review_verdict = "BLOCKED"
    elif not extraction_exact:
        review_verdict = "FAIL"
    elif not (after_answer_compare["source_copy_exact"] and after_answer_compare["json_exact"] and after_answer_compare["csv_exact"]):
        review_verdict = "FAIL"

    case_summary = {
        "case_id": case_record["case_id"],
        "company": case_record["db_company"],
        "db_company": case_record["db_company"],
        "document_name": case_record["document_name"],
        "source_path": case_record["source_path"],
        "answer_json_path": case_record["answer_json_path"],
        "answer_source_path": case_record["answer_source_path"],
        "answer_csv_path": case_record["answer_csv_path"],
        "only_pending": bool(case_record["only_pending"]),
        "wave": case_record.get("wave", 1),
        "status": authoritative_payload.get("status"),
        "reason": authoritative_payload.get("reason"),
        "issues": authoritative_payload.get("issues", []),
        "base_date": authoritative_payload.get("base_date"),
        "order_count": len(authoritative_payload.get("orders", [])),
        "exact_json": extraction_exact,
        "exact_csv": _canonical_csv_rows_from_payload(payload) == _canonical_csv_rows_from_payload(authoritative_payload),
        "exact_same": extraction_exact,
        "source_review_status": source_review_status,
        "source_review_issues": source_review_issues,
        "retry_counts": metrics or "-",
        "elapsed_seconds": elapsed_seconds,
        "answer_action": answer_action,
        "answer_vs_source": after_answer_compare,
        "authority_basis": provenance["authority_basis"],
        "confidence_tier": provenance["confidence_tier"],
        "human_confirmed": provenance["human_confirmed"],
        "external_reference_path": provenance["external_reference_path"],
        "answer_updated_without_business": provenance["answer_updated_without_business"],
        "extract_vs_source": {
            "core_exact": extraction_exact,
            "result_core": _extract_compare_core(payload),
            "authoritative_core": _extract_compare_core(authoritative_payload),
        },
        "review_verdict": review_verdict,
        "result_json_path": str(result_json_path),
        "result_csv_path": str(result_csv_path),
        "debug_root": str(case_root / "debug"),
        "handoff_root": str(case_root / "handoff"),
        "task_payload_path": str(task_payload_path),
        "authoritative_json_path": str(authoritative_json_path),
        "markdown_loss_detected": task_payload.markdown_loss_detected,
        "effective_llm_text_kind": task_payload.effective_llm_text_kind,
        "stdout_preview": "",
        "stderr_preview": "",
        "returncode": 0,
        "elapsed_wall_seconds": round(time.time() - started_at, 3),
    }
    _write_json(review_root / "case_summary.json", case_summary)
    return 0


def _start_case_process(*, manifest_entry: dict[str, Any], script_path: Path) -> subprocess.Popen[str]:
    case_root = Path(manifest_entry["case_root"])
    case_root.mkdir(parents=True, exist_ok=True)
    case_file = case_root / "review" / "case_manifest_entry.json"
    _write_json(case_file, manifest_entry)
    return subprocess.Popen(
        [sys.executable, str(script_path), "case", "--case-file", str(case_file)],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _load_case_summary(case_root: Path) -> dict[str, Any] | None:
    summary_path = case_root / "review" / "case_summary.json"
    if not summary_path.exists():
        return None
    return _read_json(summary_path)


def _make_blocked_case_summary(manifest_entry: dict[str, Any], *, returncode: int | None, stdout: str, stderr: str) -> dict[str, Any]:
    case_root = Path(manifest_entry["case_root"])
    result_json_path = case_root / "result" / f"{manifest_entry['case_id']}.json"
    result_csv_path = case_root / "result" / f"{manifest_entry['case_id']}.csv"
    reason = stderr.strip() or stdout.strip() or "case subprocess failed"
    provenance = _review_provenance(
        authority_basis=SOURCE_REVIEW_AUTHORITY_KIND,
        external_reference_path=None,
        answer_action="none",
    )
    return {
        "case_id": manifest_entry["case_id"],
        "company": manifest_entry["db_company"],
        "db_company": manifest_entry["db_company"],
        "document_name": manifest_entry["document_name"],
        "source_path": manifest_entry["source_path"],
        "answer_json_path": manifest_entry["answer_json_path"],
        "answer_source_path": manifest_entry["answer_source_path"],
        "answer_csv_path": manifest_entry["answer_csv_path"],
        "only_pending": bool(manifest_entry["only_pending"]),
        "wave": manifest_entry.get("wave", 1),
        "status": "FAILED",
        "reason": reason,
        "issues": [reason],
        "base_date": None,
        "order_count": 0,
        "exact_json": False,
        "exact_csv": False,
        "exact_same": False,
        "source_review_status": "BLOCKED",
        "source_review_issues": [reason],
        "retry_counts": "-",
        "elapsed_seconds": None,
        "answer_action": "none",
        "answer_vs_source": {
            "source_copy_exact": False,
            "json_exact": False,
            "csv_header_ok": False,
            "csv_exact": False,
        },
        "authority_basis": provenance["authority_basis"],
        "confidence_tier": provenance["confidence_tier"],
        "human_confirmed": provenance["human_confirmed"],
        "external_reference_path": provenance["external_reference_path"],
        "answer_updated_without_business": provenance["answer_updated_without_business"],
        "extract_vs_source": {"core_exact": False},
        "review_verdict": "BLOCKED",
        "result_json_path": str(result_json_path),
        "result_csv_path": str(result_csv_path),
        "debug_root": str(case_root / "debug"),
        "handoff_root": str(case_root / "handoff"),
        "task_payload_path": "",
        "authoritative_json_path": str(case_root / "review" / "authoritative_payload.json"),
        "markdown_loss_detected": False,
        "effective_llm_text_kind": "",
        "stdout_preview": stdout[-2000:],
        "stderr_preview": stderr[-2000:],
        "returncode": returncode,
    }


def _write_report(*, run_root: Path, cases: list[dict[str, Any]]) -> Path:
    now = datetime.now()
    report_dir = REPORT_ROOT / now.strftime("%Y%m%d")
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"tt{now.strftime('%H%M')}.md"

    pass_count = sum(1 for case in cases if case["review_verdict"] == "PASS")
    fail_count = sum(1 for case in cases if case["review_verdict"] == "FAIL")
    blocked_count = sum(1 for case in cases if case["review_verdict"] == "BLOCKED")
    confirmed_count = sum(1 for case in cases if case.get("confidence_tier") == CONFIDENCE_CONFIRMED)
    provisional_count = sum(1 for case in cases if case.get("confidence_tier") == CONFIDENCE_PROVISIONAL)
    human_unconfirmed_count = sum(1 for case in cases if not bool(case.get("human_confirmed")))
    source_review_updates = sum(1 for case in cases if bool(case.get("answer_updated_without_business")))
    retry_cases = []
    for case in cases:
        retry_counts = case.get("retry_counts")
        if retry_counts is None or retry_counts == "-" or retry_counts == "":
            continue
        retry_dict = dict(retry_counts)
        if any(bool(value) for value in retry_dict.values()):
            retry_cases.append(case)
    absorbed_reports = [
        REPORT_ROOT / "20260428" / "tt0911.md",
        REPORT_ROOT / "20260428" / "tt0923.md",
    ]

    lines = [
        "# document 전체 지시서 전수 추출 + 정답지 독립 검증 보고서",
        "",
        "## 개요",
        "",
        f"- 실행 root: [{run_root}]({run_root})",
        f"- 보고서 경로: [{report_path}]({report_path})",
        f"- 대상 건수: {len(cases)}",
        f"- LLM base URL: `{get_settings().llm_base_url}`",
        f"- retained 실행 기준: `{run_root.name}`",
        "",
        "## 최종 요약",
        "",
        f"- `PASS={pass_count}`",
        f"- `FAIL={fail_count}`",
        f"- `BLOCKED={blocked_count}`",
        f"- `confirmed={confirmed_count}`",
        f"- `provisional={provisional_count}`",
        f"- `human_unconfirmed={human_unconfirmed_count}`",
        "",
        "## 판정 기준",
        "",
        "- 현업 확정 정답이 있는 문서는 그 정답을 최우선 authority로 사용했다.",
        "- 현업 정답이 없는 문서는 원문 독립 검수로 authoritative record를 만들었고, 결과를 `provisional / human-unconfirmed`로 표기했다.",
        "- extraction compare는 `base_date`, `status`, `reason`, `issues`, `orders` core exact compare 기준이다.",
        "- answer set compare는 source copy byte-compare, JSON core exact compare, CSV 7컬럼 canonical compare 기준이다.",
        "- `provisional` 케이스는 현재 기준 정합성이 높더라도 `100% 신뢰 가능`으로 보지 않는다.",
        "",
        "## 케이스 결과 표",
        "",
        "| Case | 문서 | 거래처 | only_pending | Authority | Confidence | Human Confirmed | Status | Extract Core Exact | Answer JSON | Answer CSV | Source Review | Answer Action | Orders | Retry Counts | Elapsed(s) | Verdict |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | ---: | --- | ---: | --- |",
    ]
    for case in cases:
        retry_counts = case["retry_counts"]
        retry_text = "-" if retry_counts == "-" else json.dumps(retry_counts, ensure_ascii=False, separators=(",", ":"))
        elapsed_seconds = case.get("elapsed_seconds")
        elapsed_text = "-" if elapsed_seconds in {None, ""} else str(elapsed_seconds)
        lines.append(
            "| "
            + " | ".join(
                [
                    f"`{case['case_id']}`",
                    f"`{case['document_name']}`",
                    f"`{case['db_company']}`",
                    f"`{str(case['only_pending']).lower()}`",
                    f"`{case.get('authority_basis')}`",
                    f"`{case.get('confidence_tier')}`",
                    f"`{str(case.get('human_confirmed')).lower()}`",
                    f"`{case['status']}`",
                    f"`{case['extract_vs_source'].get('core_exact')}`",
                    f"`{case['answer_vs_source'].get('json_exact')}`",
                    f"`{case['answer_vs_source'].get('csv_exact')}`",
                    f"`{case['source_review_status']}`",
                    f"`{case['answer_action']}`",
                    str(case["order_count"]),
                    f"`{retry_text}`",
                    elapsed_text,
                    f"`{case['review_verdict']}`",
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## 원본 대조 메모",
            "",
        ]
    )
    for case in cases:
        lines.append(
            f"- `{case['case_id']}`: company=`{case['db_company']}`, base_date=`{case['base_date']}`, "
            f"order_count=`{case['order_count']}`, markdown_loss_detected=`{case['markdown_loss_detected']}`, "
            f"effective_llm_text_kind=`{case['effective_llm_text_kind']}`, "
            f"authority_basis=`{case.get('authority_basis')}`, confidence_tier=`{case.get('confidence_tier')}`, "
            f"external_reference_path=`{case.get('external_reference_path')}`, "
            f"source_review_issues={case['source_review_issues']}"
        )

    lines.extend(
        [
            "",
            "## 보조 판정 요약",
            "",
            f"- confirmed authority 문서 수: `{confirmed_count}`",
            f"- provisional 문서 수: `{provisional_count}`",
            f"- source review만으로 answer set이 갱신된 문서 수: `{source_review_updates}`",
            f"- 재시도 이력: `{len(retry_cases)}`개 케이스에서 retry/meters가 기록됐다.",
            "- 흡수된 이전 실행 목록:",
        ]
    )
    for absorbed_report in absorbed_reports:
        lines.append(f"- [{absorbed_report}]({absorbed_report})")

    if retry_cases:
        lines.append("- 재시도 발생 케이스:")
        for case in retry_cases:
            retry_text = json.dumps(case["retry_counts"], ensure_ascii=False, separators=(",", ":"))
            lines.append(f"- `{case['case_id']}`: `{retry_text}`")

    lines.extend(
        [
            "- 해석 메모: `business_answer`가 없는 케이스는 현재 round에서 `PASS`여도 human-unconfirmed provisional 판정이다.",
            "",
            "## 산출물 링크",
            "",
            f"- preflight: [{run_root / 'preflight' / 'preflight_summary.json'}]({run_root / 'preflight' / 'preflight_summary.json'})",
            f"- case manifest: [{run_root / 'review' / 'case_manifest.json'}]({run_root / 'review' / 'case_manifest.json'})",
            f"- validation summary: [{run_root / 'review' / 'validation_summary.json'}]({run_root / 'review' / 'validation_summary.json'})",
            f"- authority registry: [{AUTHORITY_REGISTRY_PATH}]({AUTHORITY_REGISTRY_PATH})",
        ]
    )
    for case in cases:
        lines.append(
            f"- `{case['case_id']}`: "
            f"[json]({case['result_json_path']}) / "
            f"[csv]({case['result_csv_path']}) / "
            f"[authoritative]({case['authoritative_json_path']}) / "
            f"[debug]({case['debug_root']}) / "
            f"[handoff]({case['handoff_root']})"
        )

    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path


def _print_progress(*, queued: int, running: list[str], completed: int, pass_count: int, fail_count: int, blocked_count: int, answer_updated: int, answer_new: int, answer_renamed: int, tunnel_state: str) -> None:
    running_text = ", ".join(running) if running else "-"
    print(
        f"[{_now_kst()}] progress queued={queued} running={len(running)} completed={completed} "
        f"pass={pass_count} fail={fail_count} blocked={blocked_count} "
        f"answer_updated={answer_updated} answer_new={answer_new} answer_renamed={answer_renamed} "
        f"running_cases={running_text} tunnel={tunnel_state}",
        flush=True,
    )


def _run_full_review(parallelism: int) -> int:
    started = datetime.now()
    run_root = OUTPUT_ROOT / started.strftime("%Y%m%d") / started.strftime("%H%M%S")
    (run_root / "review").mkdir(parents=True, exist_ok=True)

    preflight_summary = _preflight(run_root=run_root)
    manifest = _build_case_manifest()
    for entry in manifest:
        entry["case_root"] = str(run_root / "cases" / entry["case_id"])
    _write_json(run_root / "review" / "case_manifest.json", manifest)

    if preflight_summary["blocked"]:
        blocked_cases = [
            _make_blocked_case_summary(
                entry,
                returncode=None,
                stdout="",
                stderr="preflight blocked the run",
            )
            for entry in manifest
        ]
        _write_json(run_root / "review" / "validation_summary.json", blocked_cases)
        report_path = _write_report(run_root=run_root, cases=blocked_cases)
        print(f"preflight blocked; report={report_path}", flush=True)
        return 1

    queue = deque(manifest)
    running: dict[str, dict[str, Any]] = {}
    completed_cases: list[dict[str, Any]] = []
    last_progress_at = 0.0
    script_path = Path(__file__).resolve()
    tunnel_state = "connected"

    while queue or running:
        while queue and len(running) < parallelism:
            entry = queue.popleft()
            process = _start_case_process(manifest_entry=entry, script_path=script_path)
            running[entry["case_id"]] = {
                "manifest": entry,
                "process": process,
                "started_at": time.time(),
            }

        finished_case_ids: list[str] = []
        for case_id, state in list(running.items()):
            process: subprocess.Popen[str] = state["process"]
            if process.poll() is None:
                continue
            stdout, stderr = process.communicate()
            case_root = Path(state["manifest"]["case_root"])
            (case_root / "review" / "worker_stdout.log").write_text(stdout, encoding="utf-8")
            (case_root / "review" / "worker_stderr.log").write_text(stderr, encoding="utf-8")
            summary = _load_case_summary(case_root)
            if summary is None:
                summary = _make_blocked_case_summary(
                    state["manifest"],
                    returncode=process.returncode,
                    stdout=stdout,
                    stderr=stderr,
                )
                _write_json(case_root / "review" / "case_summary.json", summary)
            summary["stdout_preview"] = stdout[-2000:]
            summary["stderr_preview"] = stderr[-2000:]
            summary["returncode"] = process.returncode
            completed_cases.append(summary)
            finished_case_ids.append(case_id)

        for case_id in finished_case_ids:
            running.pop(case_id, None)

        while queue and len(running) < parallelism:
            entry = queue.popleft()
            process = _start_case_process(manifest_entry=entry, script_path=script_path)
            running[entry["case_id"]] = {
                "manifest": entry,
                "process": process,
                "started_at": time.time(),
            }

        now = time.time()
        if last_progress_at == 0.0 or now - last_progress_at >= 60.0 or finished_case_ids:
            pass_count = sum(1 for case in completed_cases if case["review_verdict"] == "PASS")
            fail_count = sum(1 for case in completed_cases if case["review_verdict"] == "FAIL")
            blocked_count = sum(1 for case in completed_cases if case["review_verdict"] == "BLOCKED")
            answer_updated = sum(1 for case in completed_cases if "updated" in str(case["answer_action"]))
            answer_new = sum(1 for case in completed_cases if case["answer_action"] == "new")
            answer_renamed = sum(1 for case in completed_cases if "renamed" in str(case["answer_action"]))
            _print_progress(
                queued=len(queue),
                running=sorted(running.keys()),
                completed=len(completed_cases),
                pass_count=pass_count,
                fail_count=fail_count,
                blocked_count=blocked_count,
                answer_updated=answer_updated,
                answer_new=answer_new,
                answer_renamed=answer_renamed,
                tunnel_state=tunnel_state,
            )
            last_progress_at = now
            _write_json(run_root / "review" / "validation_summary.json", sorted(completed_cases, key=lambda item: item["case_id"]))

        if running:
            time.sleep(1.0)

    completed_cases.sort(key=lambda item: item["case_id"])
    final_manifest: list[dict[str, Any]] = []
    completed_by_case_id = {case["case_id"]: case for case in completed_cases}
    for entry in manifest:
        merged_entry = dict(entry)
        case_summary = completed_by_case_id.get(entry["case_id"])
        if case_summary is not None:
            merged_entry["result_json_path"] = case_summary.get("result_json_path")
            merged_entry["result_csv_path"] = case_summary.get("result_csv_path")
            merged_entry["review_verdict"] = case_summary.get("review_verdict")
            merged_entry["authoritative_json_path"] = case_summary.get("authoritative_json_path")
            merged_entry["authority_basis"] = case_summary.get("authority_basis")
            merged_entry["confidence_tier"] = case_summary.get("confidence_tier")
            merged_entry["external_reference_path"] = case_summary.get("external_reference_path")
        final_manifest.append(merged_entry)
    _write_json(run_root / "review" / "case_manifest.json", final_manifest)
    _write_json(run_root / "review" / "validation_summary.json", completed_cases)
    report_path = _write_report(run_root=run_root, cases=completed_cases)
    print(
        f"[{_now_kst()}] completed run_root={run_root} report={report_path} "
        f"pass={sum(1 for case in completed_cases if case['review_verdict'] == 'PASS')} "
        f"fail={sum(1 for case in completed_cases if case['review_verdict'] == 'FAIL')} "
        f"blocked={sum(1 for case in completed_cases if case['review_verdict'] == 'BLOCKED')}",
        flush=True,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run full document extraction + authoritative answer-set validation.")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="run the full review")
    run_parser.add_argument("--parallelism", type=int, default=3)

    case_parser = subparsers.add_parser("case", help="run one case worker")
    case_parser.add_argument("--case-file", required=True)

    args = parser.parse_args(argv)
    command = args.command or "run"
    if command == "run":
        return _run_full_review(parallelism=max(1, int(args.parallelism)))
    if command == "case":
        return _run_case_child(Path(args.case_file))
    raise ValueError(f"Unsupported command: {command}")


if __name__ == "__main__":
    raise SystemExit(main())
