from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import logging
from pathlib import Path
import re
import unicodedata

import yaml

from .prompts import _default_counterparty_prompt_map_path, _default_prompt_path

logger = logging.getLogger(__name__)

_COUNTERPARTY_PROMPT_META_BEGIN = "[[COUNTERPARTY_PROMPT_META]]"
_COUNTERPARTY_PROMPT_META_END = "[[/COUNTERPARTY_PROMPT_META]]"


@dataclass(frozen=True, slots=True)
class CounterpartyStageColumnPolicy:
    include: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CounterpartyPromptMetadata:
    visible_guidance: str = ""
    fixed_stage_columns: dict[str, CounterpartyStageColumnPolicy] | None = None

    def stage_policy(self, stage_name: str) -> CounterpartyStageColumnPolicy | None:
        if not self.fixed_stage_columns:
            return None
        return self.fixed_stage_columns.get(str(stage_name).strip())


def _normalize_counterparty_token(text: str) -> str:
    return unicodedata.normalize("NFC", text).casefold()


def _normalize_prompt_column_labels(raw_labels: object) -> tuple[str, ...]:
    if raw_labels is None:
        return ()
    if not isinstance(raw_labels, list):
        return ()
    labels: list[str] = []
    seen: set[str] = set()
    for raw_label in raw_labels:
        label = str(raw_label).strip()
        if not label or label in seen:
            continue
        seen.add(label)
        labels.append(label)
    return tuple(labels)


def _split_counterparty_prompt_meta_block(guidance: str) -> tuple[str, str | None]:
    start = guidance.find(_COUNTERPARTY_PROMPT_META_BEGIN)
    end = guidance.find(_COUNTERPARTY_PROMPT_META_END)
    if start == -1 or end == -1 or end < start:
        return guidance.strip(), None

    meta_text = guidance[start + len(_COUNTERPARTY_PROMPT_META_BEGIN) : end].strip()
    prefix = guidance[:start].strip()
    suffix = guidance[end + len(_COUNTERPARTY_PROMPT_META_END) :].strip()
    visible_guidance = "\n\n".join(part for part in (prefix, suffix) if part).strip()
    return visible_guidance, meta_text or None


@lru_cache(maxsize=128)
def _parse_counterparty_prompt_metadata_cached(guidance: str) -> CounterpartyPromptMetadata:
    visible_guidance, meta_text = _split_counterparty_prompt_meta_block(guidance)
    if not meta_text:
        return CounterpartyPromptMetadata(
            visible_guidance=visible_guidance,
            fixed_stage_columns={},
        )

    try:
        payload = yaml.safe_load(meta_text)
    except yaml.YAMLError as exc:
        logger.warning("Failed to parse counterparty prompt meta block; ignoring structured guidance (%s)", exc)
        return CounterpartyPromptMetadata(
            visible_guidance=visible_guidance,
            fixed_stage_columns={},
        )

    if not isinstance(payload, dict):
        logger.warning("Counterparty prompt meta block is not an object; ignoring structured guidance")
        return CounterpartyPromptMetadata(
            visible_guidance=visible_guidance,
            fixed_stage_columns={},
        )

    raw_stage_columns = payload.get("fixed_stage_columns")
    if not isinstance(raw_stage_columns, dict):
        return CounterpartyPromptMetadata(
            visible_guidance=visible_guidance,
            fixed_stage_columns={},
        )

    fixed_stage_columns: dict[str, CounterpartyStageColumnPolicy] = {}
    for raw_stage_name, raw_policy in raw_stage_columns.items():
        stage_name = str(raw_stage_name).strip()
        if not stage_name or not isinstance(raw_policy, dict):
            continue
        policy = CounterpartyStageColumnPolicy(
            include=_normalize_prompt_column_labels(raw_policy.get("include")),
            exclude=_normalize_prompt_column_labels(raw_policy.get("exclude")),
        )
        if not policy.include and not policy.exclude:
            continue
        fixed_stage_columns[stage_name] = policy

    return CounterpartyPromptMetadata(
        visible_guidance=visible_guidance,
        fixed_stage_columns=fixed_stage_columns,
    )


def parse_counterparty_guidance(counterparty_guidance: str | None) -> CounterpartyPromptMetadata:
    guidance = (counterparty_guidance or "").strip()
    if not guidance:
        return CounterpartyPromptMetadata(visible_guidance="", fixed_stage_columns={})
    return _parse_counterparty_prompt_metadata_cached(guidance)


def _tokenize_counterparty_name(text: str) -> tuple[str, ...]:
    normalized_text = _normalize_counterparty_token(text)
    return tuple(re.findall(r"[0-9a-z가-힣]+", normalized_text))


def _counterparty_token_matches(
    *,
    normalized_name: str,
    name_tokens: tuple[str, ...],
    match_token: str,
) -> bool:
    normalized_match = _normalize_counterparty_token(match_token)
    if re.fullmatch(r"[0-9a-z]+", normalized_match):
        return normalized_match in name_tokens
    return normalized_match in normalized_name


def _load_counterparty_prompt_matchers(
    mapping_path: Path | None = None,
) -> tuple[tuple[str, tuple[str, ...], tuple[str, ...]], ...]:
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
