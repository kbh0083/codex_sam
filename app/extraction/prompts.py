from __future__ import annotations

from pathlib import Path
from string import Formatter
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
import yaml

from .models import PromptBundle, StageDefinition

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
OPTIONAL_USER_PROMPT_FIELDS = {"counterparty_guidance"}
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
SENSITIVE_COUNTERPARTY_GUIDANCE_STAGES = frozenset(
    {"fund_inventory", "base_date", "t_day", "transfer_amount", "order_type"}
)


def _default_prompt_path() -> Path:
    return Path(__file__).resolve().parent.parent / "prompts" / "extraction_prompts.yaml"


def _default_counterparty_prompt_map_path() -> Path:
    return _default_prompt_path().parent / "counterparty_prompt_map.yaml"


def _load_prompt_bundle(prompt_path: Path | None = None) -> PromptBundle:
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
