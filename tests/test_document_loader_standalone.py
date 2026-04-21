from __future__ import annotations

import importlib
import shutil
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

class DocumentLoaderStandaloneImportTests(unittest.TestCase):
    """`DocumentLoader` standalone import 시나리오를 검증한다."""

    def test_document_loader_can_be_imported_as_standalone_package_copy(self) -> None:
        """`document_loader.py + document_loaders/`만 복사된 환경에서도 import 되는지 본다.

        실제 WAS 포팅에서는 `app` 패키지 전체를 옮기지 않고, DocumentLoader 관련 코드만
        떼어내는 경우가 있다. 이 테스트는 그런 배포 형태를 임시 디렉터리에서 재현해서
        import 경로 정리가 실제로 동작하는지 확인한다.
        """
        # standalone 포팅 검증에서 중요한 것은 "DocumentLoader 소스가 어디에 있나"와
        # "샘플 입력 문서가 외부 환경에 얼마나 덜 의존하나"를 함께 보는 것이다.
        #
        # - 소스 경로는 항상 현재 테스트 파일이 들어 있는 저장소를 기준으로 계산해야 한다.
        #   그래야 DOCUMENT_INPUT_DIR가 외부 공유 디렉터리로 바뀌어도 테스트가 깨지지 않는다.
        # - 샘플 문서는 테스트가 직접 임시 디렉터리에 만든다.
        #   이 테스트의 목적은 "standalone import + 기본 로딩 가능 여부" 확인이지
        #   운영 문서 저장소가 어떤 상태인지 확인하는 것이 아니기 때문이다.
        # - 다시 말해 이 테스트는 "외부 서버에서 DocumentLoader만 복사했을 때 import가 되나"를
        #   확인하는 계약 테스트이지, 실제 운영 문서 샘플 회귀 테스트는 아니다.
        project_root = Path(__file__).resolve().parents[1]
        source_module = project_root / "app" / "document_loader.py"
        source_package = project_root / "app" / "document_loaders"

        with TemporaryDirectory() as tmp_dir:
            temp_root = Path(tmp_dir)
            shutil.copy2(source_module, temp_root / "document_loader.py")
            shutil.copytree(source_package, temp_root / "document_loaders")

            # fixture를 테스트 안에서 직접 만드는 이유:
            # - 다른 서버/CI에서는 DOCUMENT_INPUT_DIR가 비어 있을 수 있다.
            # - 그래도 standalone import와 최소 로딩 동작은 항상 검증 가능해야 한다.
            # - 따라서 가장 작은 형태의 "지시서다운 HTML"만 직접 작성해 사용한다.
            sample_document = temp_root / "sample_instruction.html"
            sample_document.write_text(
                """
                <html>
                  <body>
                    <h1>변액일임펀드 설정/해지 지시서</h1>
                    <table>
                      <tr>
                        <th>펀드코드</th>
                        <th>펀드명</th>
                        <th>설정금액</th>
                        <th>해지금액</th>
                      </tr>
                      <tr>
                        <td>V2201S</td>
                        <td>샘플성장형</td>
                        <td>100,000</td>
                        <td>0</td>
                      </tr>
                    </table>
                  </body>
                </html>
                """.strip(),
                encoding="utf-8",
            )

            added_path = str(temp_root)
            sys.path.insert(0, added_path)
            # 이미 import 된 `document_loader*` 모듈이 남아 있으면,
            # 현재 저장소의 모듈이 재사용되어 standalone import를 제대로 검증하지 못한다.
            # 그래서 임시 sys.path를 넣기 전에 관련 모듈을 비워 두고,
            # 테스트가 끝나면 원래 캐시를 복구한다.
            module_names_to_clear = [
                module_name
                for module_name in list(sys.modules)
                if module_name == "document_loader" or module_name.startswith("document_loaders")
            ]
            cached_modules = {module_name: sys.modules.pop(module_name) for module_name in module_names_to_clear}
            try:
                standalone_module = importlib.import_module("document_loader")
                loader = standalone_module.DocumentLoader()
                loaded = loader.load(sample_document)
            finally:
                sys.path.remove(added_path)
                for module_name in [name for name in list(sys.modules) if name == "document_loader" or name.startswith("document_loaders")]:
                    sys.modules.pop(module_name, None)
                sys.modules.update(cached_modules)

        self.assertEqual(loader.__class__.__name__, "DocumentLoader")
        self.assertEqual(loaded.content_type, "text/html")
        self.assertIn("V2201S", loaded.raw_text)
        self.assertGreater(loader.estimate_order_cell_count(loaded.raw_text), 0)
