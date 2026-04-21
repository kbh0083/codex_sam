from __future__ import annotations

from app.component import DocumentExtractionRequest, ExtractionComponent

from examples.was_minimal.dto import ExtractManyRequest, ExtractOneRequest


class FundOrderApplicationService:
    """WAS에서 직접 호출하는 애플리케이션 서비스 예시.

    이 계층의 역할은 "HTTP/프레임워크 관심사"와 "추출 엔진"을 분리하는 것이다.
    컨트롤러는 request parsing과 status code만 알고,
    실제 문서 추출과 병합 규칙은 이 서비스와 `ExtractionComponent`가 맡는다.
    """

    def __init__(self, component: ExtractionComponent | None = None) -> None:
        # 실제 WAS에서는 DI 컨테이너나 앱 초기화 시점에서 component를 주입해도 되고,
        # 이 샘플처럼 기본 생성자를 써도 된다.
        self.component = component or ExtractionComponent()

    def extract_one(self, request: ExtractOneRequest) -> dict:
        """문서 1건을 추출해 현재 외부 JSON 계약 payload를 반환한다.

        이 계층의 장점은 컨트롤러가 `DocumentExtractionRequest` 같은 내부 구현 상세를
        몰라도 된다는 점이다. 즉 WAS 애플리케이션의 public contract와 추출 엔진의
        internal contract 사이를 연결해 주는 adapter 역할을 한다.
        """
        return self.component.extract_document_payload(
            DocumentExtractionRequest(
                source_path=request.source_path,
                pdf_password=request.pdf_password,
            )
        )

    def extract_many_and_merge(self, request: ExtractManyRequest) -> dict:
        """문서 여러 건을 각각 추출한 뒤 merge 규칙까지 적용한 payload를 반환한다.

        이 메서드는 문서들을 먼저 "각각" 추출한 뒤 마지막에만 병합한다.
        이렇게 해야 특정 문서가 실패했을 때 어느 입력이 문제인지 추적하기 쉽고,
        병합 규칙도 단일 문서 추출 규칙과 자연스럽게 분리된다.
        """
        extraction_requests = [
            DocumentExtractionRequest(
                source_path=source_path,
                pdf_password=request.pdf_password,
            )
            for source_path in request.source_paths
        ]
        return self.component.extract_merged_payload(extraction_requests)
