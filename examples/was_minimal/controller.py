from __future__ import annotations

from typing import Any, Mapping

from examples.was_minimal.dto import (
    RequestValidationError,
    parse_extract_many_request,
    parse_extract_one_request,
)
from examples.was_minimal.service import FundOrderApplicationService


class FundOrderController:
    """프레임워크에 덜 묶인 컨트롤러 예시.

    실제 WAS에서는 Flask/FastAPI/Django/사내 프레임워크 등 구현이 다를 수 있으므로,
    여기서는 `dict -> (response_dict, status_code)` 형태로 최소 책임만 예시로 보여 준다.
    핵심 의도는 "HTTP 프레임워크 문법"과 "추출 엔진 호출"을 분리하는 것이다.
    즉 이 클래스는 추출 규칙을 직접 알지 않고, 요청/응답 변환과 예외 매핑만 담당한다.
    """

    def __init__(self, service: FundOrderApplicationService | None = None) -> None:
        # 서비스 주입을 허용해 두면 실제 WAS에서는 DI 컨테이너나 팩토리와 쉽게 연결할 수 있다.
        # 별도 주입이 없으면 샘플이 바로 동작하도록 기본 서비스 인스턴스를 내부에서 생성한다.
        self.service = service or FundOrderApplicationService()

    def extract_one(self, request_json: Mapping[str, Any]) -> tuple[dict[str, Any], int]:
        """단건 추출 엔드포인트 예시.

        반환 타입을 `(response_dict, status_code)`로 고정한 이유는,
        Flask/FastAPI/Django 어느 쪽으로 옮겨도 핵심 제어 흐름이 거의 그대로 유지되게 하려는 것이다.
        """
        try:
            request = parse_extract_one_request(request_json)
            payload = self.service.extract_one(request)
            return {"success": True, "data": payload}, 200
        except RequestValidationError as exc:
            # 요청 JSON 구조가 잘못된 경우다.
            # 예: source_path 누락, file_paths가 빈 배열 등
            return {
                "success": False,
                "error_type": "REQUEST_VALIDATION_ERROR",
                "message": str(exc),
            }, 400
        except ValueError as exc:
            # 추출은 실행됐지만 저장 가능한 결과가 아니라고 엔진이 판정한 경우다.
            # coverage mismatch, blocking issue 같은 "업무 검증 실패"를 이 범주로 본다.
            return {
                "success": False,
                "error_type": "EXTRACTION_VALIDATION_ERROR",
                "message": str(exc),
            }, 400
        except Exception as exc:  # pragma: no cover
            # 샘플 구조에서는 예상하지 못한 모든 런타임 오류를 500으로 모아 준다.
            # 실제 WAS에서는 여기서 로깅, 알림, 에러 코드 매핑을 더 붙이면 된다.
            return {
                "success": False,
                "error_type": "EXTRACTION_UNEXPECTED_ERROR",
                "message": str(exc),
            }, 500

    def extract_many_and_merge(self, request_json: Mapping[str, Any]) -> tuple[dict[str, Any], int]:
        """다건 병합 추출 엔드포인트 예시.

        여러 문서를 한 번에 받아 문서별 추출을 수행한 뒤,
        `ExtractionComponent`가 제공하는 merge 규칙으로 최종 payload를 만든다.
        컨트롤러는 문서 수에 따라 응답 형식을 바꾸지 않고, 항상 동일한 envelope를 유지한다.
        """
        try:
            request = parse_extract_many_request(request_json)
            payload = self.service.extract_many_and_merge(request)
            return {"success": True, "data": payload}, 200
        except RequestValidationError as exc:
            return {
                "success": False,
                "error_type": "REQUEST_VALIDATION_ERROR",
                "message": str(exc),
            }, 400
        except ValueError as exc:
            return {
                "success": False,
                "error_type": "EXTRACTION_VALIDATION_ERROR",
                "message": str(exc),
            }, 400
        except Exception as exc:  # pragma: no cover
            return {
                "success": False,
                "error_type": "EXTRACTION_UNEXPECTED_ERROR",
                "message": str(exc),
            }, 500
