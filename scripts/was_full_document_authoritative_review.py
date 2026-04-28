from __future__ import annotations

import argparse
from collections import deque
from datetime import datetime
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
BASE_SCRIPT_PATH = REPO_ROOT / "scripts" / "full_document_authoritative_review.py"
WAS_REPO_ROOT = Path("/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend")
WAS_VENV_PYTHON = WAS_REPO_ROOT / ".venv" / "bin" / "python"
WAS_RUNNER_PATH = WAS_REPO_ROOT / "scripts" / "va_extract_case_runner.py"
WAS_ENV_PATH = WAS_REPO_ROOT / ".env"

BASE_SPEC = importlib.util.spec_from_file_location("full_document_authoritative_review", BASE_SCRIPT_PATH)
if BASE_SPEC is None or BASE_SPEC.loader is None:  # pragma: no cover - bootstrap guard
    raise RuntimeError(f"failed to load base review script: {BASE_SCRIPT_PATH}")
base_review = importlib.util.module_from_spec(BASE_SPEC)
BASE_SPEC.loader.exec_module(base_review)

OUTPUT_ROOT = REPO_ROOT / "output" / "test"
REPORT_ROOT = REPO_ROOT / "test_report"

PROMPT_NAME_TO_DB_COMPANY = dict(base_review.PROMPT_NAME_TO_COMPANY)
PROMPT_NAME_TO_DB_COMPANY.update(
    {
        "IBK": "IBK연금보험",
        "흥국생명": "흥국생명-heungkuklife",
        "흥국생명-hanais": "흥국생명-hanais",
    }
)
ANSWER_COMPANY_TO_DB_COMPANY = {
    "IBK": "IBK연금보험",
    "흥국생명": "흥국생명-heungkuklife",
}
ONLY_PENDING_FALSE_DB_COMPANIES = {
    "동양생명",
    "신한라이프",
    "한화생명",
    "KDB생명",
    "흥국생명-heungkuklife",
    "흥국생명-hanais",
}


def _load_was_env() -> dict[str, str]:
    env: dict[str, str] = {}
    if not WAS_ENV_PATH.exists():
        raise FileNotFoundError(f"missing WAS env file: {WAS_ENV_PATH}")
    for raw_line in WAS_ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        env[key.strip()] = value
    return env


def _configure_local_llm_from_was_env(was_env: dict[str, str]) -> None:
    for key in ("LLM_BASE_URL", "LLM_API_KEY", "LLM_MODEL"):
        value = was_env.get(key)
        if value:
            os.environ[key] = value
    base_review.get_settings.cache_clear()


def _curl_models(*, base_url: str, api_key: str) -> dict[str, Any]:
    run = subprocess.run(
        [
            "curl",
            "-sS",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "-H",
            f"Authorization: Bearer {api_key}",
            f"{base_url.rstrip('/')}/models",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return {
        "returncode": run.returncode,
        "http_code": run.stdout.strip(),
        "stderr": run.stderr,
    }


def _was_subprocess_env(was_env: dict[str, str]) -> dict[str, str]:
    env = os.environ.copy()
    for key in (
        "DATABASE_URL",
        "DATABASE_POOL_SIZE",
        "DATABASE_MAX_OVERFLOW",
        "DATABASE_POOL_TIMEOUT",
        "DEBUG",
        "LLM_BASE_URL",
        "LLM_API_KEY",
        "LLM_MODEL",
        "LLM_TEMPERATURE",
        "LLM_MAX_TOKENS",
        "REDIS_URL",
    ):
        value = was_env.get(key)
        if value is not None:
            env[key] = value
    return env


def _run_shell(command: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> dict[str, Any]:
    run = subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    return {
        "command": command,
        "returncode": run.returncode,
        "stdout": run.stdout,
        "stderr": run.stderr,
    }


def _ensure_llm_tunnel(*, base_url: str, api_key: str) -> tuple[dict[str, Any], bool]:
    probe = _curl_models(base_url=base_url, api_key=api_key)
    if probe["returncode"] == 0 and probe["http_code"] == "200":
        return probe, False

    askpass_path = REPO_ROOT / "output" / "tmp" / "ssh_askpass_3900.sh"
    askpass_path.parent.mkdir(parents=True, exist_ok=True)
    askpass_path.write_text("#!/bin/sh\necho 'Mini1234!'\n", encoding="utf-8")
    askpass_path.chmod(0o700)
    llm_tunnel_env = os.environ.copy()
    llm_tunnel_env.update(
        {
            "SSH_ASKPASS": str(askpass_path),
            "SSH_ASKPASS_REQUIRE": "force",
            "DISPLAY": ":0",
        }
    )
    ssh_run = _run_shell(
        [
            "nohup",
            "ssh",
            "-f",
            "-N",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "ExitOnForwardFailure=yes",
            "-L",
            "3900:localhost:3950",
            "minisoft@1.241.20.229",
            "-p",
            "2194",
        ],
        cwd=REPO_ROOT,
        env=llm_tunnel_env,
    )
    time.sleep(2.0)
    rechecked = _curl_models(base_url=base_url, api_key=api_key)
    rechecked["ssh_stdout"] = ssh_run["stdout"]
    rechecked["ssh_stderr"] = ssh_run["stderr"]
    rechecked["ssh_returncode"] = ssh_run["returncode"]
    return rechecked, ssh_run["returncode"] == 0 and rechecked["returncode"] == 0 and rechecked["http_code"] == "200"


def _ensure_db_tunnel() -> tuple[dict[str, Any], bool]:
    lsof = _run_shell(["lsof", "-nP", "-iTCP:5432", "-sTCP:LISTEN"], cwd=REPO_ROOT)
    if lsof["returncode"] == 0:
        return lsof, False
    ssh_run = _run_shell(
        [
            "ssh",
            "-f",
            "-N",
            "-i",
            str(WAS_REPO_ROOT / ".ssh" / "samsung_ai_portal_dbs"),
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "ServerAliveInterval=60",
            "-o",
            "ServerAliveCountMax=3",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "ExitOnForwardFailure=yes",
            "-p",
            "2194",
            "-L",
            "5432:localhost:5432",
            "-L",
            "6379:localhost:6379",
            "samsung_ai_dbs@1.241.20.229",
        ],
        cwd=WAS_REPO_ROOT,
    )
    time.sleep(2.0)
    rechecked = _run_shell(["lsof", "-nP", "-iTCP:5432", "-sTCP:LISTEN"], cwd=REPO_ROOT)
    rechecked["ssh_stdout"] = ssh_run["stdout"]
    rechecked["ssh_stderr"] = ssh_run["stderr"]
    rechecked["ssh_returncode"] = ssh_run["returncode"]
    return rechecked, ssh_run["returncode"] == 0 and rechecked["returncode"] == 0


def _db_company_for_prompt_name(prompt_name: str | None, *, answer_company: str) -> str:
    normalized_prompt_name = (prompt_name or "").strip()
    if normalized_prompt_name:
        company_name = PROMPT_NAME_TO_DB_COMPANY.get(normalized_prompt_name)
        if company_name:
            return company_name
    aliased_company = ANSWER_COMPANY_TO_DB_COMPANY.get(answer_company)
    if aliased_company:
        return aliased_company
    return answer_company


def _resolve_prompt_name_for_document(
    *,
    document_path: Path,
    loader: Any,
    settings: Any,
) -> str | None:
    task_payload = loader.build_task_payload(
        document_path,
        chunk_size_chars=settings.llm_chunk_size_chars,
        pdf_password=base_review._default_pdf_password(document_path),
    )
    return base_review.resolve_counterparty_prompt_name(
        document_path,
        document_text=task_payload.markdown_text or task_payload.raw_text,
    )


def _build_expected_db_rows() -> list[dict[str, Any]]:
    expected_rows: list[dict[str, Any]] = []
    prompt_dir = REPO_ROOT / "app" / "prompts"
    for prompt_path in sorted(prompt_dir.glob("*.txt"), key=lambda item: base_review._normalize_text(item.name)):
        prompt_name = prompt_path.stem
        db_company = _db_company_for_prompt_name(prompt_name, answer_company=prompt_name)
        expected_rows.append(
            {
                "prompt_name": prompt_name,
                "db_company": db_company,
                "prompt_path": str(prompt_path),
                "prompt_text": prompt_path.read_text(encoding="utf-8"),
                "use_counterparty_prompt": True,
                "only_pending": db_company not in ONLY_PENDING_FALSE_DB_COMPANIES,
            }
        )
    return expected_rows


def _build_case_manifest() -> list[dict[str, Any]]:
    settings = base_review.get_settings()
    loader = base_review.DocumentLoader()
    documents = base_review._sorted_documents()
    stem_counts: dict[str, int] = {}
    for document_path in documents:
        normalized_stem = base_review._normalize_text(document_path.stem)
        stem_counts[normalized_stem] = stem_counts.get(normalized_stem, 0) + 1

    answer_source_index = base_review._build_answer_source_index()
    manifest: list[dict[str, Any]] = []
    for index, document_path in enumerate(documents, start=1):
        answer_company = base_review._resolve_company_name(
            document_path=document_path,
            answer_source_index=answer_source_index,
            loader=loader,
            settings=settings,
        )
        prompt_name = _resolve_prompt_name_for_document(
            document_path=document_path,
            loader=loader,
            settings=settings,
        )
        db_company = _db_company_for_prompt_name(prompt_name, answer_company=answer_company)
        answer_targets = base_review._build_answer_targets(
            document_path=document_path,
            company_name=answer_company,
            stem_counts=stem_counts,
        )
        manifest.append(
            {
                "runtime": "was",
                "case_index": index,
                "case_id": base_review._case_id(index, document_path),
                "document_name": document_path.name,
                "source_path": str(document_path),
                "answer_company": answer_company,
                "db_company": db_company,
                "prompt_name": prompt_name,
                "only_pending": db_company not in ONLY_PENDING_FALSE_DB_COMPANIES,
                "answer_source_path": str(answer_targets["source_path"]),
                "answer_json_path": str(answer_targets["json_path"]),
                "answer_csv_path": str(answer_targets["csv_path"]),
            }
        )
    return manifest


def _build_db_sync_spec(*, run_root: Path) -> Path:
    spec_path = run_root / "preflight" / "db_sync_spec.json"
    payload = {
        "expected_rows": _build_expected_db_rows(),
    }
    base_review._write_json(spec_path, payload)
    return spec_path


def _preflight(*, run_root: Path) -> dict[str, Any]:
    preflight_root = run_root / "preflight"
    preflight_root.mkdir(parents=True, exist_ok=True)
    was_env = _load_was_env()
    _configure_local_llm_from_was_env(was_env)
    was_subprocess_env = _was_subprocess_env(was_env)

    local_git = _run_shell(["git", "status", "--short", "--untracked-files=all"], cwd=REPO_ROOT)
    was_git = _run_shell(["git", "status", "--short", "--untracked-files=all"], cwd=WAS_REPO_ROOT)
    py_compile = _run_shell(
        [
            str(WAS_VENV_PYTHON),
            "-m",
            "py_compile",
            str(WAS_RUNNER_PATH),
            "src/app/services/variable_annuity/extract/extractor.py",
            "src/app/services/variable_annuity/extract/document_loader.py",
            "src/app/services/variable_annuity/extract/output_contract.py",
        ],
        cwd=WAS_REPO_ROOT,
        env=was_subprocess_env,
    )
    port_3900 = _run_shell(["lsof", "-nP", "-iTCP:3900", "-sTCP:LISTEN"], cwd=REPO_ROOT)
    port_5432, db_tunnel_reconnected = _ensure_db_tunnel()
    models_probe, llm_tunnel_reconnected = _ensure_llm_tunnel(
        base_url=was_env.get("LLM_BASE_URL", "http://localhost:3900/v1"),
        api_key=was_env.get("LLM_API_KEY", "EMPTY"),
    )

    db_sync_spec_path = _build_db_sync_spec(run_root=run_root)
    db_sync_output_path = preflight_root / "db_sync_summary.json"
    db_sync_run = _run_shell(
        [
            str(WAS_VENV_PYTHON),
            str(WAS_RUNNER_PATH),
            "sync-db",
            "--spec-file",
            str(db_sync_spec_path),
            "--output",
            str(db_sync_output_path),
        ],
        cwd=WAS_REPO_ROOT,
        env=was_subprocess_env,
    )
    if db_sync_run["returncode"] != 0 and db_tunnel_reconnected is False:
        port_5432_retry, db_tunnel_reconnected_retry = _ensure_db_tunnel()
        port_5432 = port_5432_retry
        db_tunnel_reconnected = db_tunnel_reconnected_retry
        db_sync_run = _run_shell(
            [
                str(WAS_VENV_PYTHON),
                str(WAS_RUNNER_PATH),
                "sync-db",
                "--spec-file",
                str(db_sync_spec_path),
                "--output",
                str(db_sync_output_path),
            ],
            cwd=WAS_REPO_ROOT,
            env=was_subprocess_env,
        )

    db_sync_summary = {}
    if db_sync_output_path.exists():
        db_sync_summary = base_review._read_json(db_sync_output_path)

    authority_registry_ok = True
    authority_registry_error = ""
    authority_registry_count = 0
    try:
        authority_registry_count = len(base_review._load_external_authority_registry())
    except Exception as exc:
        authority_registry_ok = False
        authority_registry_error = str(exc)

    blocked = any(
        (
            py_compile["returncode"] != 0,
            models_probe["returncode"] != 0,
            models_probe["http_code"] != "200",
            db_sync_run["returncode"] != 0,
            bool(db_sync_summary.get("blocked")),
            not authority_registry_ok,
        )
    )
    summary = {
        "started_at": base_review._now_kst(),
        "runtime": "was",
        "was_repo_root": str(WAS_REPO_ROOT),
        "was_runner_path": str(WAS_RUNNER_PATH),
        "was_python": str(WAS_VENV_PYTHON),
        "llm_base_url": was_env.get("LLM_BASE_URL"),
        "llm_model": was_env.get("LLM_MODEL"),
        "local_git_status": local_git,
        "was_git_status": was_git,
        "py_compile": py_compile,
        "listener_3900": port_3900,
        "listener_5432": port_5432,
        "models_probe": models_probe,
        "llm_tunnel_reconnected": llm_tunnel_reconnected,
        "db_tunnel_reconnected": db_tunnel_reconnected,
        "db_sync_spec_path": str(db_sync_spec_path),
        "db_sync_action": db_sync_summary.get("db_sync_action"),
        "db_sync_summary": db_sync_summary,
        "db_sync_stdout": db_sync_run["stdout"],
        "db_sync_stderr": db_sync_run["stderr"],
        "authority_registry_path": str(base_review.AUTHORITY_REGISTRY_PATH),
        "authority_registry_ok": authority_registry_ok,
        "authority_registry_error": authority_registry_error,
        "authority_registry_entry_count": authority_registry_count,
        "blocked": blocked,
    }
    base_review._write_json(preflight_root / "preflight_summary.json", summary)
    return summary


def _start_case_process(*, manifest_entry: dict[str, Any]) -> subprocess.Popen[str]:
    case_root = Path(manifest_entry["case_root"])
    case_root.mkdir(parents=True, exist_ok=True)
    case_file = case_root / "review" / "case_manifest_entry.json"
    base_review._write_json(case_file, manifest_entry)
    was_subprocess_env = _was_subprocess_env(_load_was_env())
    return subprocess.Popen(
        [
            str(WAS_VENV_PYTHON),
            str(WAS_RUNNER_PATH),
            "run-case",
            "--case-file",
            str(case_file),
        ],
        cwd=WAS_REPO_ROOT,
        env=was_subprocess_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _build_case_summary(
    *,
    manifest_entry: dict[str, Any],
    preflight_summary: dict[str, Any],
    returncode: int,
    stdout: str,
    stderr: str,
) -> dict[str, Any]:
    case_root = Path(manifest_entry["case_root"])
    review_root = case_root / "review"
    result_root = case_root / "result"
    result_json_path = result_root / f"{manifest_entry['case_id']}.json"
    result_csv_path = result_root / f"{manifest_entry['case_id']}.csv"
    authoritative_json_path = review_root / "authoritative_payload.json"
    metadata_path = review_root / "was_case_metadata.json"
    if not result_json_path.exists() or not metadata_path.exists():
        summary = base_review._make_blocked_case_summary(
            manifest_entry,
            returncode=returncode,
            stdout=stdout,
            stderr=stderr,
        )
        summary["runtime"] = "was"
        summary["answer_company"] = manifest_entry["answer_company"]
        summary["db_sync_action"] = preflight_summary.get("db_sync_action")
        summary["db_sync_summary"] = preflight_summary.get("db_sync_summary")
        return summary

    result_payload = base_review._read_json(result_json_path)
    metadata = base_review._read_json(metadata_path)
    task_payload_path = Path(metadata["task_payload_path"])
    task_payload = base_review.DocumentLoadTaskPayload.read_json(task_payload_path)
    counterparty_guidance = metadata.get("counterparty_guidance")
    external_registry = base_review._load_external_authority_registry()

    answer_candidate: dict[str, Any] | None = None
    answer_json_path = Path(manifest_entry["answer_json_path"])
    if answer_json_path.exists():
        try:
            loaded_answer_candidate = base_review._read_json(answer_json_path)
            if isinstance(loaded_answer_candidate, dict):
                answer_candidate = loaded_answer_candidate
        except Exception:
            answer_candidate = None

    authoritative_payload, source_review_status, source_review_issues, authority_resolution = (
        base_review._resolve_authoritative_payload_for_case(
            case_record=manifest_entry,
            task_payload=task_payload,
            counterparty_guidance=counterparty_guidance,
            review_root=review_root,
            extraction_candidate=result_payload,
            answer_candidate=answer_candidate,
            model_name=str(metadata.get("model_name", preflight_summary.get("llm_model", ""))),
            external_registry=external_registry,
        )
    )
    base_review._write_json(authoritative_json_path, authoritative_payload)

    extraction_exact = base_review._payload_core_exact(result_payload, authoritative_payload)
    answer_targets = {
        "answer_dir": Path(manifest_entry["answer_json_path"]).parent,
        "source_path": Path(manifest_entry["answer_source_path"]),
        "json_path": Path(manifest_entry["answer_json_path"]),
        "csv_path": Path(manifest_entry["answer_csv_path"]),
    }
    before_answer_compare = base_review._compare_answer_set(
        authoritative_payload=authoritative_payload,
        answer_targets=answer_targets,
        document_path=Path(manifest_entry["source_path"]),
    )
    if not (
        before_answer_compare["source_copy_exact"]
        and before_answer_compare["json_exact"]
        and before_answer_compare["csv_exact"]
    ):
        answer_action = base_review._write_answer_set(
            authoritative_payload=authoritative_payload,
            answer_targets=answer_targets,
            document_path=Path(manifest_entry["source_path"]),
        )
    else:
        base_review._prepare_answer_artifacts(
            document_path=Path(manifest_entry["source_path"]),
            answer_targets=answer_targets,
        )
        answer_action = "none"
    after_answer_compare = base_review._compare_answer_set(
        authoritative_payload=authoritative_payload,
        answer_targets=answer_targets,
        document_path=Path(manifest_entry["source_path"]),
    )
    provenance = base_review._review_provenance(
        authority_basis=str(authority_resolution["authority_basis"]),
        external_reference_path=authority_resolution.get("external_reference_path"),
        answer_action=answer_action,
    )
    metrics, elapsed_seconds = base_review._load_single_metrics(case_root / "debug")
    review_verdict = "PASS"
    if source_review_status == "BLOCKED":
        review_verdict = "BLOCKED"
    elif not extraction_exact:
        review_verdict = "FAIL"
    elif not (
        after_answer_compare["source_copy_exact"]
        and after_answer_compare["json_exact"]
        and after_answer_compare["csv_exact"]
    ):
        review_verdict = "FAIL"

    summary = {
        "runtime": "was",
        "case_id": manifest_entry["case_id"],
        "company": manifest_entry["db_company"],
        "answer_company": manifest_entry["answer_company"],
        "db_company": manifest_entry["db_company"],
        "prompt_name": manifest_entry.get("prompt_name"),
        "document_name": manifest_entry["document_name"],
        "source_path": manifest_entry["source_path"],
        "answer_json_path": manifest_entry["answer_json_path"],
        "answer_source_path": manifest_entry["answer_source_path"],
        "answer_csv_path": manifest_entry["answer_csv_path"],
        "only_pending": bool(manifest_entry["only_pending"]),
        "wave": manifest_entry.get("wave", 1),
        "status": authoritative_payload.get("status"),
        "reason": authoritative_payload.get("reason"),
        "issues": authoritative_payload.get("issues", []),
        "base_date": authoritative_payload.get("base_date"),
        "order_count": len(authoritative_payload.get("orders", [])),
        "exact_json": extraction_exact,
        "exact_csv": base_review._canonical_csv_rows_from_payload(result_payload)
        == base_review._canonical_csv_rows_from_payload(authoritative_payload),
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
            "result_core": base_review._extract_compare_core(result_payload),
            "authoritative_core": base_review._extract_compare_core(authoritative_payload),
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
        "stdout_preview": stdout[-2000:],
        "stderr_preview": stderr[-2000:],
        "returncode": returncode,
        "elapsed_wall_seconds": metadata.get("elapsed_wall_seconds"),
        "db_sync_action": preflight_summary.get("db_sync_action"),
        "db_sync_summary": preflight_summary.get("db_sync_summary"),
    }
    return summary


def _print_progress(
    *,
    queued: int,
    running: list[str],
    completed: int,
    pass_count: int,
    fail_count: int,
    blocked_count: int,
    answer_updated: int,
    answer_new: int,
    answer_renamed: int,
    tunnel_state: str,
) -> None:
    running_text = ", ".join(running) if running else "-"
    print(
        f"[{base_review._now_kst()}] progress queued={queued} running={len(running)} completed={completed} "
        f"pass={pass_count} fail={fail_count} blocked={blocked_count} "
        f"answer_updated={answer_updated} answer_new={answer_new} answer_renamed={answer_renamed} "
        f"running_cases={running_text} tunnel={tunnel_state}",
        flush=True,
    )


def _write_report(*, run_root: Path, cases: list[dict[str, Any]], preflight_summary: dict[str, Any]) -> Path:
    now = datetime.now()
    report_dir = REPORT_ROOT / now.strftime("%Y%m%d")
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"tt{now.strftime('%H%M')}.md"

    pass_count = sum(1 for case in cases if case["review_verdict"] == "PASS")
    fail_count = sum(1 for case in cases if case["review_verdict"] == "FAIL")
    blocked_count = sum(1 for case in cases if case["review_verdict"] == "BLOCKED")
    confirmed_count = sum(1 for case in cases if case.get("confidence_tier") == base_review.CONFIDENCE_CONFIRMED)
    provisional_count = sum(1 for case in cases if case.get("confidence_tier") == base_review.CONFIDENCE_PROVISIONAL)
    human_unconfirmed_count = sum(1 for case in cases if not bool(case.get("human_confirmed")))
    answer_updated = sum(1 for case in cases if "updated" in str(case.get("answer_action", "")))
    answer_new = sum(1 for case in cases if case.get("answer_action") == "new")
    answer_renamed = sum(1 for case in cases if "renamed" in str(case.get("answer_action", "")))
    absorbed_reports = [
        REPORT_ROOT / "20260428" / "tt1013.md",
        REPORT_ROOT / "20260428" / "tt1310.md",
    ]
    retry_cases = []
    for case in cases:
        retry_counts = case.get("retry_counts")
        if retry_counts is None or retry_counts == "-" or retry_counts == "":
            continue
        retry_dict = dict(retry_counts)
        if any(bool(value) for value in retry_dict.values()):
            retry_cases.append(case)

    db_sync_summary = preflight_summary.get("db_sync_summary") or {}
    lines = [
        "# WAS document 전체 지시서 전수 추출 + 원본 대조 검수 보고서",
        "",
        "## 개요",
        "",
        f"- 실행 root: [{run_root}]({run_root})",
        f"- 보고서 경로: [{report_path}]({report_path})",
        f"- 대상 건수: {len(cases)}",
        f"- runtime: `was`",
        f"- LLM base URL: `{preflight_summary.get('llm_base_url')}`",
        f"- retained 실행 기준: `{run_root.name}`",
        "",
        "## Preflight / DB Sync",
        "",
        f"- db_sync_action: `{preflight_summary.get('db_sync_action')}`",
        f"- db expected rows: `{db_sync_summary.get('expected_row_count')}`",
        f"- db actual rows: `{db_sync_summary.get('db_row_count')}`",
        f"- db updated rows: `{db_sync_summary.get('updated_row_count')}`",
        f"- db missing rows: `{len(db_sync_summary.get('missing_rows', []))}`",
        f"- db unexpected rows: `{len(db_sync_summary.get('unexpected_rows', []))}`",
        f"- llm tunnel reconnected: `{preflight_summary.get('llm_tunnel_reconnected')}`",
        f"- db tunnel reconnected: `{preflight_summary.get('db_tunnel_reconnected')}`",
        "",
        "## 최종 요약",
        "",
        f"- `PASS={pass_count}`",
        f"- `FAIL={fail_count}`",
        f"- `BLOCKED={blocked_count}`",
        f"- `confirmed={confirmed_count}`",
        f"- `provisional={provisional_count}`",
        f"- `human_unconfirmed={human_unconfirmed_count}`",
        f"- `answer_updated={answer_updated}`",
        f"- `answer_new={answer_new}`",
        f"- `answer_renamed={answer_renamed}`",
        "",
        "## 판정 기준",
        "",
        "- extraction runtime은 WAS `DocumentLoader + exact DB row prompt/use_counterparty_prompt/only_pending + FundOrderExtractor` 경로를 사용했다.",
        "- source of truth는 원본 지시서이며, 현업 확정 정답이 registry에 있으면 그 정답을 최우선 authority로 사용했다.",
        "- extraction compare는 `base_date`, `status`, `reason`, `issues`, `orders` core exact compare 기준이다.",
        "- answer set compare는 source copy byte-compare, JSON core exact compare, CSV 7컬럼 canonical compare 기준이다.",
        "- `provisional` 케이스는 이번 라운드에서도 `100% 신뢰 가능`으로 보지 않는다.",
        "",
        "## 케이스 결과 표",
        "",
        "| Case | 문서 | answer_company | db_company | only_pending | runtime | Authority | Confidence | Human Confirmed | Status | Extract Core Exact | Answer JSON | Answer CSV | Source Review | Answer Action | Orders | Retry Counts | Elapsed(s) | Verdict |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | ---: | --- | ---: | --- |",
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
                    f"`{case.get('answer_company')}`",
                    f"`{case['db_company']}`",
                    f"`{str(case['only_pending']).lower()}`",
                    f"`{case.get('runtime')}`",
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
            f"- `{case['case_id']}`: answer_company=`{case.get('answer_company')}`, db_company=`{case['db_company']}`, "
            f"base_date=`{case['base_date']}`, order_count=`{case['order_count']}`, "
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
            f"- source review만으로 answer set이 갱신된 문서 수: `{sum(1 for case in cases if case.get('answer_updated_without_business'))}`",
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
            f"- authority registry: [{base_review.AUTHORITY_REGISTRY_PATH}]({base_review.AUTHORITY_REGISTRY_PATH})",
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


def _run_full_review(parallelism: int) -> int:
    started = datetime.now()
    run_root = OUTPUT_ROOT / started.strftime("%Y%m%d") / started.strftime("%H%M%S")
    (run_root / "review").mkdir(parents=True, exist_ok=True)

    preflight_summary = _preflight(run_root=run_root)
    manifest = _build_case_manifest()
    for entry in manifest:
        entry["case_root"] = str(run_root / "cases" / entry["case_id"])
    base_review._write_json(run_root / "review" / "case_manifest.json", manifest)

    if preflight_summary["blocked"]:
        blocked_cases = []
        for entry in manifest:
            summary = base_review._make_blocked_case_summary(
                entry,
                returncode=None,
                stdout="",
                stderr="preflight blocked the run",
            )
            summary["runtime"] = "was"
            summary["answer_company"] = entry["answer_company"]
            summary["db_sync_action"] = preflight_summary.get("db_sync_action")
            summary["db_sync_summary"] = preflight_summary.get("db_sync_summary")
            blocked_cases.append(summary)
        base_review._write_json(run_root / "review" / "validation_summary.json", blocked_cases)
        report_path = _write_report(run_root=run_root, cases=blocked_cases, preflight_summary=preflight_summary)
        print(f"preflight blocked; report={report_path}", flush=True)
        return 1

    queue = deque(manifest)
    running: dict[str, dict[str, Any]] = {}
    completed_cases: list[dict[str, Any]] = []
    last_progress_at = 0.0
    tunnel_state = "connected"

    while queue or running:
        while queue and len(running) < parallelism:
            entry = queue.popleft()
            process = _start_case_process(manifest_entry=entry)
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
            try:
                summary = _build_case_summary(
                    manifest_entry=state["manifest"],
                    preflight_summary=preflight_summary,
                    returncode=process.returncode or 0,
                    stdout=stdout,
                    stderr=stderr,
                )
            except Exception as exc:  # pragma: no cover - runtime robustness
                summary = base_review._make_blocked_case_summary(
                    state["manifest"],
                    returncode=process.returncode,
                    stdout=stdout,
                    stderr=f"{stderr}\n{exc}".strip(),
                )
                summary["runtime"] = "was"
                summary["answer_company"] = state["manifest"]["answer_company"]
                summary["db_sync_action"] = preflight_summary.get("db_sync_action")
                summary["db_sync_summary"] = preflight_summary.get("db_sync_summary")
            base_review._write_json(case_root / "review" / "case_summary.json", summary)
            completed_cases.append(summary)
            finished_case_ids.append(case_id)
            if "Connection refused" in stderr or "Multiple exceptions" in stderr:
                tunnel_state = "reconnect-needed"

        for case_id in finished_case_ids:
            running.pop(case_id, None)

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
            base_review._write_json(
                run_root / "review" / "validation_summary.json",
                sorted(completed_cases, key=lambda item: item["case_id"]),
            )

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
            merged_entry["runtime"] = case_summary.get("runtime")
            merged_entry["answer_company"] = case_summary.get("answer_company")
            merged_entry["db_sync_action"] = case_summary.get("db_sync_action")
        final_manifest.append(merged_entry)
    base_review._write_json(run_root / "review" / "case_manifest.json", final_manifest)
    base_review._write_json(run_root / "review" / "validation_summary.json", completed_cases)
    report_path = _write_report(run_root=run_root, cases=completed_cases, preflight_summary=preflight_summary)
    print(
        f"[{base_review._now_kst()}] completed run_root={run_root} report={report_path} "
        f"pass={sum(1 for case in completed_cases if case['review_verdict'] == 'PASS')} "
        f"fail={sum(1 for case in completed_cases if case['review_verdict'] == 'FAIL')} "
        f"blocked={sum(1 for case in completed_cases if case['review_verdict'] == 'BLOCKED')}",
        flush=True,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run full authoritative document review with WAS extraction runtime.")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="run the WAS full review")
    run_parser.add_argument("--parallelism", type=int, default=3)

    args = parser.parse_args(argv)
    command = args.command or "run"
    if command == "run":
        return _run_full_review(parallelism=max(1, int(args.parallelism)))
    raise ValueError(f"Unsupported command: {command}")


if __name__ == "__main__":
    raise SystemExit(main())
