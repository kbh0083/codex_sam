from __future__ import annotations

import logging
from pathlib import Path
import re
import unicodedata

import yaml

from .prompts import _default_counterparty_prompt_map_path, _default_prompt_path

logger = logging.getLogger(__name__)


def _normalize_counterparty_token(text: str) -> str:
    return unicodedata.normalize("NFC", text).casefold()


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

