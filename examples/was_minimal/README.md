# WAS 최소 샘플 구조

이 폴더는 현재 추출 엔진을 개발 중인 WAS 코드에 붙일 때 참고할 수 있는 최소 예시 구조다.

구성:
- `dto.py`
  - 컨트롤러 입력을 서비스 계약으로 정규화하는 DTO
- `service.py`
  - `ExtractionComponent`를 감싼 애플리케이션 서비스
- `controller.py`
  - 프레임워크에 덜 묶인 컨트롤러 예시

읽는 순서:
1. `dto.py`
2. `service.py`
3. `controller.py`

이 순서가 좋은 이유는, 실제 WAS에서도
"입력 정규화 -> 애플리케이션 서비스 -> HTTP 어댑터"
순서로 책임을 나누는 것이 가장 유지보수하기 쉽기 때문이다.

권장 호출 흐름:

```text
Controller
  -> FundOrderApplicationService
    -> ExtractionComponent
      -> ExtractionService
```

핵심 포인트:
- 컨트롤러는 HTTP와 예외 응답만 신경 쓴다.
- 추출/병합 규칙은 `ExtractionComponent`를 통해 공통 재사용한다.
- WAS로 포팅할 때 CLI 로직을 다시 옮기지 않아도 된다.

자세한 설명은 [WAS_포팅_가이드.md](/Users/bhkim/Documents/codex_prj_sam_asset/WAS_포팅_가이드.md)를 함께 보면 된다.
