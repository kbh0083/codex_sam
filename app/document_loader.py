from __future__ import annotations

# `DocumentLoader`는 두 가지 배포 형태를 동시에 지원해야 한다.
#
# 1. 현재 저장소 내부 사용
#    - `from app.document_loader import DocumentLoader`
#    - `document_loaders/`는 `app` 패키지의 하위 패키지다.
#
# 2. WAS 포팅용 standalone 복사
#    - `document_loader.py + document_loading/ + document_loaders/`만 다른 서버로 복사
#    - `from document_loader import DocumentLoader`
#    - 이 경우에는 `app` 패키지 자체가 존재하지 않을 수 있다.
#
# 여기서 중요한 점은 "standalone import 경로"와 "실제 import 오류"를 구분하는 것이다.
# 단순 `except ImportError` fallback을 쓰면 아래 두 상황이 뒤섞인다.
# - 정상적인 standalone 사용: 상위 패키지가 없어서 상대 import를 못 하는 경우
# - 실제 버그/환경 오류: mixin 내부 오타, 선택 라이브러리 누락, 의존성 설치 누락
#
# 우리는 두 번째 경우의 진짜 오류를 숨기고 싶지 않다. 그래서 예외를 넓게 잡지 않고,
# 현재 모듈이 "패키지 없이 직접 import된 상태"인지 (`__package__ in {None, ""}`)를
# 먼저 확인한 다음에만 standalone 경로를 사용한다.
#
# 정리하면:
# - 패키지 문맥이 있으면 상대 import
# - 패키지 문맥이 없으면 sibling 패키지 import
# - 실제 import 오류는 그대로 surface 해서 원인을 추적할 수 있게 유지
if __package__ in {None, ""}:  # pragma: no cover - standalone copy 경로
    from document_loading import (
        DocumentLoadTaskPayload,
        DocumentLoaderCommonMixin,
        DocumentLoaderCoreMixin,
        DocumentLoaderCoverageMixin,
        DocumentLoaderMarkdownMixin,
        DocumentLoaderScopeMixin,
        ExtractedDocumentText,
        TargetFundScope,
        normalize_fund_name_key,
    )
    from document_loaders import (
        EmlDocumentLoaderMixin,
        ExcelDocumentLoaderMixin,
        HtmlDocumentLoaderMixin,
        MhtDocumentLoaderMixin,
        PdfDocumentLoaderMixin,
    )
else:
    from .document_loading import (
        DocumentLoadTaskPayload,
        DocumentLoaderCommonMixin,
        DocumentLoaderCoreMixin,
        DocumentLoaderCoverageMixin,
        DocumentLoaderMarkdownMixin,
        DocumentLoaderScopeMixin,
        ExtractedDocumentText,
        TargetFundScope,
        normalize_fund_name_key,
    )
    from .document_loaders import (
        EmlDocumentLoaderMixin,
        ExcelDocumentLoaderMixin,
        HtmlDocumentLoaderMixin,
        MhtDocumentLoaderMixin,
        PdfDocumentLoaderMixin,
    )


class DocumentLoader(
    DocumentLoaderCommonMixin,
    DocumentLoaderCoreMixin,
    DocumentLoaderScopeMixin,
    DocumentLoaderMarkdownMixin,
    DocumentLoaderCoverageMixin,
    PdfDocumentLoaderMixin,
    ExcelDocumentLoaderMixin,
    HtmlDocumentLoaderMixin,
    EmlDocumentLoaderMixin,
    MhtDocumentLoaderMixin,
):
    """문서 형식별 로더를 조합하고 공통 정규화를 수행하는 facade entrypoint."""

