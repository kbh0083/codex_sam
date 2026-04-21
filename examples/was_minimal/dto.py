from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


class RequestValidationError(ValueError):
    """컨트롤러 입력이 서비스 계약에 맞지 않을 때 사용하는 예외."""


@dataclass(frozen=True, slots=True)
class ExtractOneRequest:
    """문서 1건 추출 요청 DTO.

    WAS에서 들어오는 원본 request body는 프레임워크별로 제각각이다.
    서비스 계층은 그런 세부사항을 모르고, "추출에 필요한 필드가 이미 정리된 객체"만
    받는 편이 훨씬 단순해진다.
    """

    source_path: str
    pdf_password: str | None = None

    def as_path(self) -> Path:
        """문자열 경로를 `Path` 객체로 정규화한다."""
        return Path(self.source_path).expanduser()


@dataclass(frozen=True, slots=True)
class ExtractManyRequest:
    """문서 여러 건 병합 추출 요청 DTO."""

    source_paths: tuple[str, ...]
    pdf_password: str | None = None

    def as_paths(self) -> list[Path]:
        """문자열 경로 목록을 `Path` 목록으로 정규화한다.

        현재 샘플에서는 이 메서드를 직접 많이 쓰지 않더라도,
        상위 계층이 "입력은 문자열, 내부는 Path" 규칙을 일관되게 유지하는 데 도움을 준다.
        """
        return [Path(value).expanduser() for value in self.source_paths]


def parse_extract_one_request(payload: Mapping[str, Any]) -> ExtractOneRequest:
    """컨트롤러 입력 payload를 1건 추출 요청 DTO로 변환한다.

    실제 WAS에서는 프레임워크별 request object가 제각각이므로,
    컨트롤러에서 곧바로 서비스에 넘기지 않고 먼저 DTO로 정규화하는 편이 안전하다.
    """
    # 컨트롤러에서는 입력 검증을 최대한 빨리 끝내고,
    # 서비스는 "필수 필드가 이미 존재한다"는 가정 아래 동작하게 만든다.
    source_path = str(payload.get("source_path", "")).strip()
    if not source_path:
        raise RequestValidationError("source_path is required.")

    pdf_password = payload.get("pdf_password")
    if pdf_password is not None:
        pdf_password = str(pdf_password).strip() or None

    return ExtractOneRequest(
        source_path=source_path,
        pdf_password=pdf_password,
    )


def parse_extract_many_request(payload: Mapping[str, Any]) -> ExtractManyRequest:
    """컨트롤러 입력 payload를 다건 병합 요청 DTO로 변환한다.

    다건 병합은 입력 형식이 조금 더 엄격하다.
    `source_paths`가 빈 리스트면 병합 의미가 없으므로 여기서 바로 차단한다.
    """
    raw_source_paths = payload.get("source_paths")
    if not isinstance(raw_source_paths, list) or not raw_source_paths:
        raise RequestValidationError("source_paths must be a non-empty list.")

    source_paths = tuple(str(value).strip() for value in raw_source_paths if str(value).strip())
    if not source_paths:
        raise RequestValidationError("source_paths must contain at least one valid path.")

    pdf_password = payload.get("pdf_password")
    if pdf_password is not None:
        pdf_password = str(pdf_password).strip() or None

    return ExtractManyRequest(
        source_paths=source_paths,
        pdf_password=pdf_password,
    )
