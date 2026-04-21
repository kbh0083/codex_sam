from __future__ import annotations

import unittest

from app.component import ExtractionComponent
from examples.was_minimal.controller import FundOrderController
from examples.was_minimal.dto import (
    RequestValidationError,
    parse_extract_many_request,
    parse_extract_one_request,
)
from examples.was_minimal.service import FundOrderApplicationService


class _FakeComponent(ExtractionComponent):
    """WAS 샘플 계층 테스트용 최소 fake component다."""

    def __init__(self) -> None:
        """실제 component 초기화 없이 테스트에 필요한 인터페이스만 남긴다."""
        pass

    def extract_document_payload(self, request):  # type: ignore[override]
        """단건 추출 예시 응답을 고정 값으로 돌려준다."""
        return {
            "file_name": "sample.pdf",
            "source_path": str(request.source_path),
            "model_name": "model-x",
            "base_date": "2026-03-18",
            "issues": [],
            "orders": [
                {
                    "fund_code": "F001",
                    "fund_name": "Alpha",
                    "settle_class": "2",
                    "order_type": "3",
                    "base_date": "2026-03-18",
                    "t_day": "01",
                    "transfer_amount": "100",
                }
            ],
        }

    def extract_merged_payload(self, requests):  # type: ignore[override]
        """다건 merge 예시 응답을 고정 값으로 돌려준다."""
        return {
            "file_count": len(requests),
            "documents": [],
            "issues": [],
            "orders": [
                {
                    "fund_code": "F001",
                    "fund_name": "Alpha",
                    "settle_class": "2",
                    "order_type": "3",
                    "base_date": "2026-03-18",
                    "t_day": "01",
                    "transfer_amount": "100",
                }
            ],
        }


class WasMinimalSampleTests(unittest.TestCase):
    """WAS 최소 샘플 DTO/서비스/컨트롤러 경로를 검증한다."""

    def test_parse_extract_one_request_requires_source_path(self) -> None:
        with self.assertRaises(RequestValidationError):
            parse_extract_one_request({})

    def test_parse_extract_many_request_requires_non_empty_list(self) -> None:
        with self.assertRaises(RequestValidationError):
            parse_extract_many_request({"source_paths": []})

    def test_application_service_extract_one_uses_component(self) -> None:
        service = FundOrderApplicationService(component=_FakeComponent())

        payload = service.extract_one(parse_extract_one_request({"source_path": "/tmp/sample.pdf"}))

        self.assertEqual(payload["file_name"], "sample.pdf")
        self.assertEqual(payload["orders"][0]["fund_code"], "F001")

    def test_controller_extract_one_returns_200_response(self) -> None:
        controller = FundOrderController(service=FundOrderApplicationService(component=_FakeComponent()))

        response, status_code = controller.extract_one({"source_path": "/tmp/sample.pdf"})

        self.assertEqual(status_code, 200)
        self.assertTrue(response["success"])
        self.assertEqual(response["data"]["orders"][0]["fund_code"], "F001")

    def test_controller_extract_many_and_merge_returns_200_response(self) -> None:
        controller = FundOrderController(service=FundOrderApplicationService(component=_FakeComponent()))

        response, status_code = controller.extract_many_and_merge(
            {"source_paths": ["/tmp/a.pdf", "/tmp/b.pdf"]}
        )

        self.assertEqual(status_code, 200)
        self.assertTrue(response["success"])
        self.assertEqual(response["data"]["file_count"], 2)


if __name__ == "__main__":
    unittest.main()
