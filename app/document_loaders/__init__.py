"""형식별 문서 로더 mixin 묶음.

이 패키지의 역할은 `DocumentLoader` 본체가 파일 형식별 세부 구현을 한 번에
끌어다 쓸 수 있게 mixin을 모아 export 하는 것이다.

포팅 관점에서 중요한 점:
- 현재 저장소 내부에서는 `app.document_loaders` 패키지로 import 된다.
- standalone 복사 환경에서는 `document_loader.py` 옆의 sibling 패키지로 import 된다.

여기서는 상대 import만 사용한다. 이렇게 해 두면 디렉터리 구조만 유지해서 복사했을 때
현재 저장소 내부와 standalone 복사본이 같은 파일 집합을 그대로 재사용할 수 있다.
"""

# 이 패키지는 `DocumentLoader`의 public dependency 집합이다.
# 즉 외부에서는 보통 `DocumentLoader`만 import 하고, 이 패키지는 그 뒤에서
# "어떤 형식을 어떤 mixin이 처리하는지"를 한 곳에서 묶어 주는 역할만 한다.
from .eml_loader import EmlDocumentLoaderMixin
from .excel_loader import ExcelDocumentLoaderMixin
from .html_loader import HtmlDocumentLoaderMixin
from .mht_loader import MhtDocumentLoaderMixin
from .pdf_loader import PdfDocumentLoaderMixin

__all__ = [
    "EmlDocumentLoaderMixin",
    "ExcelDocumentLoaderMixin",
    "HtmlDocumentLoaderMixin",
    "MhtDocumentLoaderMixin",
    "PdfDocumentLoaderMixin",
]
