from __future__ import annotations

import json
import logging
from contextvars import ContextVar
from datetime import datetime
from pathlib import Path
from typing import Any

from .models import InvalidResponseArtifact, _ExtractMetricsState

logger = logging.getLogger(__name__)

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


def write_invalid_response_debug_files(
    *,
    debug_output_dir: Path,
    source_name: str,
    artifacts: list[InvalidResponseArtifact],
) -> list[Path]:
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
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    source_stem = Path(source_name).stem or "document"
    return debug_output_dir / f"{source_stem}_{timestamp}_llm_pipeline.log"


def build_extract_llm_metrics_path(*, log_path: Path) -> Path:
    if log_path.name.endswith("_llm_pipeline.log"):
        stem = log_path.name.removesuffix("_llm_pipeline.log")
    else:
        stem = log_path.stem
    return log_path.with_name(f"{stem}_llm_metrics.json")


def _record_extract_metric(bucket_name: str, stage_name: str, increment: int = 1) -> None:
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

