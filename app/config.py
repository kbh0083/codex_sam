from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


def _load_dotenv() -> None:
    """프로젝트 루트의 `.env`를 읽어 현재 프로세스 환경변수에 주입한다.

    왜 직접 파싱하나:
    - 이 프로젝트는 의존성을 최소화하려고 `python-dotenv` 같은 패키지에 의존하지 않는다.
    - 설정 로드는 매우 이른 시점에 일어나므로, 실패 지점을 단순하게 유지하는 편이 좋다.

    구현 의도:
    - 이미 셸에서 지정된 환경변수는 덮어쓰지 않는다. (`setdefault`)
    - `export KEY=VALUE` 형태도 허용한다.
    - 따옴표로 감싼 값은 바깥 따옴표만 벗긴다.
    """
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            stripped = stripped[7:].strip()
        if "=" not in stripped:
            continue

        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip()

        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]

        os.environ.setdefault(key, value)


_load_dotenv()


def _get_env_bool(name: str, default: bool) -> bool:
    """환경변수 문자열을 bool로 해석한다.

    `.env`에서는 보통 `true/false`, `1/0`, `yes/no`, `on/off`가 섞여 들어온다.
    설정을 읽는 쪽마다 제각각 파싱하면 실수하기 쉬우므로 여기서 한 번만 통일한다.
    """
    raw = os.getenv(name)
    if raw is None:
        return default

    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return default


@dataclass(frozen=True)
class Settings:
    """런타임 전역 설정 묶음.

    이 프로젝트는 설정을 여기 한 곳으로 모아 두고, 나머지 모듈은
    `Settings` 인스턴스만 주입받아 사용한다. 이렇게 해 두면
    - 테스트에서 설정을 바꿔 넣기 쉽고
    - 환경변수 의존이 코드 전역으로 흩어지지 않으며
    - 어떤 값이 운영 계약인지 한눈에 볼 수 있다.
    """
    debug_output_dir: Path
    document_input_dir: Path
    task_payload_output_dir: Path
    delete_task_payload_files: bool
    llm_model: str
    llm_base_url: str
    llm_api_key: str
    llm_temperature: float
    llm_max_tokens: int
    llm_timeout_seconds: int
    llm_retry_attempts: int
    llm_retry_backoff_seconds: float
    llm_chunk_size_chars: int
    llm_stage_batch_size: int


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """환경변수에서 `Settings`를 한 번만 구성해 재사용한다.

    설정은 실행 중에 거의 바뀌지 않는 값이므로 매 호출마다 다시 파싱할 이유가 없다.
    `lru_cache`를 씌워 두면 서비스/추출기/CLI가 각각 호출하더라도
    항상 같은 객체를 공유하게 되고, 테스트에서는 프로세스 단위로만 초기화하면 된다.
    """
    return Settings(
        debug_output_dir=Path(os.getenv("DEBUG_OUTPUT_DIR", "./output/debug")),
        document_input_dir=Path(os.getenv("DOCUMENT_INPUT_DIR", "./document")),
        task_payload_output_dir=Path(os.getenv("TASK_PAYLOAD_OUTPUT_DIR", "./output/handoff")),
        delete_task_payload_files=_get_env_bool("DELETE_TASK_PAYLOAD_FILES", True),
        llm_model=os.getenv("LLM_MODEL", "qwen3-next-80B-A3B-instruct"),
        llm_base_url=os.getenv("LLM_BASE_URL", "http://localhost:8000/v1"),
        llm_api_key=os.getenv("LLM_API_KEY", "dummy"),
        llm_temperature=float(os.getenv("LLM_TEMPERATURE", "0")),
        llm_max_tokens=int(os.getenv("LLM_MAX_TOKENS", "16384")),
        llm_timeout_seconds=int(os.getenv("LLM_TIMEOUT_SECONDS", "120")),
        llm_retry_attempts=int(os.getenv("LLM_RETRY_ATTEMPTS", "3")),
        llm_retry_backoff_seconds=float(os.getenv("LLM_RETRY_BACKOFF_SECONDS", "1.5")),
        llm_chunk_size_chars=int(os.getenv("LLM_CHUNK_SIZE_CHARS", "12000")),
        llm_stage_batch_size=int(os.getenv("LLM_STAGE_BATCH_SIZE", "12")),
    )
