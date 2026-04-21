# DocumentLoader 가이드

이 문서는 `DocumentLoader`만 다른 서버나 다른 프로젝트로 옮겨서 쓰고 싶은 개발자를 위한 안내서다.  
목표는 단순하다. **이 문서 하나만 읽어도 `DocumentLoader`를 복사하고, 설치하고, import 하고, 실제 문서를 읽어볼 수 있게 만드는 것**이다.

> 2026-04-20 기준 보강.
> 공통 최신 상태와 테스트 보고 원칙은 [처음읽는_개발자용_흐름.md](/Users/bhkim/Documents/codex_prj_sam_asset/readme/처음읽는_개발자용_흐름.md)를 출처로 둔다.
> 이 문서는 `DocumentLoader` 전용 규칙만 유지한다. 하나생명 XLSX 전환의 loader 상세 근거는 [merge_report/merge_result_report_20260420_01.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/merge_result_report_20260420_01.md)를 따른다.

즉 아래 질문에 이 문서 안에서 바로 답을 찾을 수 있도록 구성했다.

- `DocumentLoader`는 정확히 무엇을 하는가
- 어떤 파일을 복사해야 하는가
- 어떤 패키지를 설치해야 하는가
- 다른 서버에서 문서 저장 경로가 달라도 괜찮은가
- 최소 예제를 어떻게 실행하는가
- 실패하면 어디를 먼저 봐야 하는가

## 0. 가장 빠른 시작 방법

아무 설명보다 먼저 "가장 짧은 성공 경로"를 보고 싶다면 아래만 따라 하면 된다.

### 0-1. 파일 복사

다른 서버의 작업 디렉터리에 아래 구조를 만든다.

```text
your_project/
  document_loader.py
  document_loaders/
    __init__.py
    pdf_loader.py
    excel_loader.py
    html_loader.py
    eml_loader.py
    mht_loader.py
```

### 0-2. 패키지 설치

```bash
pip install pdfplumber openpyxl xlrd
# secure HTML/ZIP 메일까지 standalone으로 재현하려면 추가
pip install cryptography
```

### 0-3. 실행 예제

```python
from pathlib import Path
from document_loader import DocumentLoader

loader = DocumentLoader()
loaded = loader.load(Path("/data/orders/sample.pdf"))

print("content_type =", loaded.content_type)
print("coverage =", loader.estimate_order_cell_count(loaded.raw_text))
print("raw preview =", loaded.raw_text[:300])
print("markdown preview =", loaded.markdown_text[:300])
```

여기까지 성공하면 포팅 자체는 완료된 것이다.

## 1. `DocumentLoader`는 무엇을 하는가

`DocumentLoader`는 문서를 읽어서 **추출기(LLM)** 가 이해하기 쉬운 텍스트로 정리해 주는 모듈이다.

정확히는 아래 일을 담당한다.

- 파일 형식에 따라 문서를 읽는다.
  - PDF
  - XLS / XLSX
  - HTML
  - EML
  - MHT
- 순수 `.csv` 파일은 현재 직접 지원하지 않는다.
- 원문 보존용 `raw_text`를 만든다.
- LLM 입력용 구조화 텍스트 `markdown_text`를 만든다.
- 원문 기준 기대 주문 건수(coverage)를 계산한다.
- 삼성 계열 운용사 대상 문서인 경우, 어떤 펀드를 추출 대상으로 볼지 scope를 계산한다.

즉 `DocumentLoader`는 **"문서를 열고 구조를 정리하는 단계"까지만 담당한다.**

## 1-1. 2026-04-20 기준으로 추가로 기억할 규칙

최근 회귀와 WAS 병합에서 실제로 중요했던 loader 규칙은 아래다.

- 합계/소계 행은 더 공격적으로 제외한다.
  - `합계`, `총계`, `소계`뿐 아니라 `[7개 펀드]`, `7개 펀드 합계`, 장식 문자가 붙은 total row도 coverage와 markdown 후보에서 뺀다.
- `fund_name`이 비어 있어도 `fund_code + 금액`이 있으면 주문 row 후보로 남긴다.
  - 흥국생명처럼 코드 중심 문서에서 중요하다.
- `결제금액`, `순유입금액`, `정산액` 같은 net/settlement 컬럼이 있으면 해당 버킷을 1건으로 센다.
- net 컬럼이 없고 `설정/해지`, `입금/출금`, `Buy/Sell`만 있으면 `SUB`와 `RED` 버킷을 분리해 센다.
- `비고`에 예정/T+N 성격이 드러나는 문서는 planned bucket 후보로 인정한다.
- 숫자 판별은 한국 금액 단위(`억`, `만`, `천`)와 쉼표를 허용하되, `TRUE/FALSE` 같은 boolean 노이즈는 금액으로 보지 않는다.
- EML은 메일 헤더의 날짜가 있으면 `Date (Asia/Seoul): YYYY-MM-DD`를 `raw_text` 상단에 추가한다.
- EML은 현재 메시지 본문만 남기고 이전 스레드를 잘라내며, BUY & SELL REPORT 계열 plain text는 세로형 표를 다시 읽기 쉽게 복원한다.
- HTML/MHT는 parser-first 경로를 우선 유지하고, 보안 메일 HTML은 호출자가 준 암호를 써서 복호화할 수 있게 유지한다.
  - standalone 공개 API에서는 HTML도 `load(path, pdf_password="...")` 또는 `build_task_payload(..., pdf_password="...")`로 암호를 전달한다.
  - 이 경로를 실제로 쓰려면 `cryptography` 설치가 권장된다. 없으면 일부 보안 메일은 browser fallback에만 의존하게 된다.
- 하나생명은 PDF가 아니라 XLSX workbook을 권위 있는 원문으로 본다.
  - legacy PDF는 duplicate copy로 reject하고 XLSX attachment를 사용한다.
  - `거래유형/거래유형명`은 order-context header로 인정한다.
  - `설정해지금액` header segment가 있으면 canonical amount header로 본다.
  - 같은 logical bucket 안의 `펀드납입출금액`, `판매회사분결제금액`은 보조/중복 금액 컬럼으로 보고 별도 bucket을 만들지 않는다.

쉽게 말하면:
- `DocumentLoader`
  - 문서를 읽는다
  - 표와 본문을 정리한다
  - 사람이 보기 좋은 `raw_text`
  - LLM이 보기 좋은 `markdown_text`
  를 만든다
- 그 다음 단계
  - 실제 주문 JSON 추출
  - 후처리
  - 다건 병합
  은 다른 모듈이 맡는다

coverage를 볼 때도 최근 규칙을 같이 기억하면 좋다.
- `결제금액`, `순유입금액`, `정산액` 같은 settlement/net 컬럼이 있으면 그 버킷은 1건으로 센다.
- settlement/net 컬럼이 없고 `설정금액/해지금액`, `입금액/출금액`, `Buy/Sell`만 있으면 `SUB`와 `RED`를 분리해서 센다.
- 장식된 합계행과 summary 텍스트는 버킷 후보에서 제외한다.
- `fund_name`이 없어도 코드와 금액이 살아 있는 row는 coverage 후보로 남길 수 있다.
- `only_pending=true` 거래처에서는 loader count가 최종 output row count와 다를 수 있다.
  - 이 경우 sign-off는 loader count 단독이 아니라 internal exactness, final payload exactness, 원본 지시서 검수로 판단한다.

하지만 처음 포팅하거나 문서 품질을 확인할 때는 대부분 `DocumentLoader`만으로도 충분하다.

## 2. 언제 `DocumentLoader`만 따로 쓰면 좋은가

아래 경우에는 전체 추출 엔진보다 `DocumentLoader`만 따로 가져다 쓰는 편이 좋다.

- 다른 서버에서 문서 내용을 먼저 구조화해서 저장하고 싶을 때
- 원문 검수용 `raw_text`와 `markdown_text`만 필요할 때
- LLM 추출은 다른 시스템에서 하고, 문서 파싱만 재사용하고 싶을 때
- 문서 형식별 파싱 품질을 독립적으로 테스트하고 싶을 때

반대로 아래 경우에는 `DocumentLoader`만으로는 부족하다.

- 최종 주문 JSON/CSV까지 바로 만들고 싶을 때
- 여러 문서를 병합하고 싶을 때
- 삼성 대상 필터, LLM 추출, 후처리, CSV 평탄화까지 한 번에 쓰고 싶을 때

이 경우에는 전체 추출 컴포넌트를 쓰는 편이 더 안전하다.  
다만 이 문서는 `DocumentLoader` 단독 사용에만 집중한다.

## 3. 다른 서버로 옮길 때 어떤 파일을 복사해야 하나

`DocumentLoader` standalone 사용에 필요한 최소 파일은 아래와 같다.

- [app/document_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loader.py)
- [app/document_loaders/__init__.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/__init__.py)
- [app/document_loaders/pdf_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/pdf_loader.py)
- [app/document_loaders/excel_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/excel_loader.py)
- [app/document_loaders/html_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/html_loader.py)
- [app/document_loaders/eml_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/eml_loader.py)
- [app/document_loaders/mht_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/mht_loader.py)

복사 후 디렉터리 구조 예시는 아래처럼 맞추면 된다.

```text
your_project/
  document_loader.py
  document_loaders/
    __init__.py
    pdf_loader.py
    excel_loader.py
    html_loader.py
    eml_loader.py
    mht_loader.py
```

이 구조를 유지하면 아래 import가 그대로 동작한다.

```python
from document_loader import DocumentLoader
```

현재 저장소 안에서는 기존처럼 아래 import도 계속 쓸 수 있다.

```python
from app.document_loader import DocumentLoader
```

## 4. 필요한 Python 패키지

형식별 파싱에 필요한 라이브러리는 아래와 같다.

- `pdfplumber`
- `openpyxl`
- `xlrd` (`.xls`를 읽을 때 필요)
- `cryptography` (보안 HTML/ZIP 메일 복호화를 standalone으로 재현할 때 권장)

최소 예시:

```bash
pip install pdfplumber openpyxl xlrd
pip install cryptography
```

주의:
- `DocumentLoader`만 따로 쓸 때는 `.env`, `LLM`, `api key`가 없어도 된다.
- 즉 이 모듈은 문서를 읽고 정리하는 데 집중한 로더이기 때문에, LLM endpoint 설정에는 직접 의존하지 않는다.

즉 초보 개발자 기준으로는 이렇게 이해하면 된다.

- 필요한 것
  - 문서 파일
  - 위 7개 소스 파일
  - 파싱 라이브러리 3개
- 필요 없는 것
  - OpenAI 키
  - LLM 서버
  - `.env`
  - DB

## 5. 문서 저장 경로가 다른 서버마다 달라도 괜찮은가

괜찮다. 이 부분이 가장 중요하다.

`DocumentLoader`는 **문서 저장 루트를 내부에 고정하지 않는다.**  
즉 아래처럼 호출하는 쪽이 넘기는 `Path`를 그대로 사용한다.

```python
from pathlib import Path
from document_loader import DocumentLoader

loader = DocumentLoader()
loaded = loader.load(Path("/new/server/storage/orders/sample.pdf"))
```

정리하면:

- `DocumentLoader` standalone 사용
  - 문서 경로는 호출자가 직접 넘긴다.
  - 서버마다 저장 위치가 달라도 코드 수정이 거의 필요 없다.
- CLI / 서비스 사용
  - [app/config.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/config.py) 의 `DOCUMENT_INPUT_DIR`를 참고할 수 있다.
  - 이 설정은 "파일명만 입력했을 때 어디서 찾을지"를 정하는 용도다.

즉 **`DOCUMENT_INPUT_DIR`는 서비스/CLI 편의 기능이고, `DocumentLoader` 자체의 필수 계약은 아니다.**

## 6. 가장 간단한 사용 예시

```python
from pathlib import Path
from document_loader import DocumentLoader

loader = DocumentLoader()
loaded = loader.load(Path("/data/orders/hanhwa2.html"))

print(loaded.content_type)
print(loaded.raw_text[:500])
print(loaded.markdown_text[:500])
print(loader.estimate_order_cell_count(loaded.raw_text))
```

여기서 `loaded`는 `ExtractedDocumentText` 객체다.

조금 더 실무적으로 쓰면 보통 아래처럼 감싼다.

```python
from pathlib import Path
from document_loader import DocumentLoader


def load_document_for_review(file_path: str) -> dict:
    loader = DocumentLoader()
    loaded = loader.load(Path(file_path))
    expected_count = loader.estimate_order_cell_count(loaded.raw_text)

    return {
        "content_type": loaded.content_type,
        "raw_text": loaded.raw_text,
        "markdown_text": loaded.markdown_text,
        "expected_order_count": expected_count,
    }
```

이 함수 하나만 있어도 다른 서버에서 아래와 같은 용도로 바로 쓸 수 있다.

- 원문 텍스트 저장
- markdown 저장
- 사람이 검수하는 화면에 표시
- 다음 단계 추출기 입력값 생성

## 6-1. `llm_chunk_size_chars`는 무엇이고, 왜 기본값이 12000인가

`DocumentLoader`를 단독으로 `load()`만 쓸 때는 chunk를 직접 신경 쓰지 않아도 된다.  
하지만 아래처럼 `build_task_payload()`까지 쓰거나, 다음 단계에서 LLM 추출기로 넘길 계획이라면
`chunk_size_chars`가 중요한 설정이 된다.

예:

```python
task_payload = loader.build_task_payload(
    Path("/data/orders/sample.pdf"),
    chunk_size_chars=12000,
)
```

이 값은 "문서를 몇 글자 단위로 잘라서 LLM에 넘길지"를 정한다.

중요한 점은 이 프로젝트가 **아무 데서나 12000자씩 잘라 버리는 방식이 아니라는 것**이다.  
[app/document_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loader.py)의 `split_for_llm()`는
먼저 section 경계를 최대한 보존하고, 그 다음에만 길이를 본다.

즉 목적은 단순하다.

- 너무 작게 자르면
  - 표 헤더와 본문이 찢어진다
  - 한 chunk 안에 펀드/금액/결제 버킷 정보가 같이 안 들어올 수 있다
  - stage 1 펀드 탐색 누락이 늘 수 있다
- 너무 크게 자르면
  - 응답 시간이 길어진다
  - JSON 형식이 깨질 가능성이 커질 수 있다
  - LLM 서버 부하와 실패 확률이 올라갈 수 있다

### 6-1-1. 그러면 12000이 “최적값”인가

정답은 **아니다**.

`12000`은 현재 프로젝트에서 쓰는 **무난한 기본값**이다.
즉 “절대 최적값”이라기보다 아래 균형을 맞춘 실무형 시작점에 가깝다.

- 표/섹션 문맥을 너무 잘게 자르지 않기
- 응답 시간이 과도하게 길어지지 않기
- stage 1~7을 안정적으로 돌릴 수 있는 크기 유지하기

그래서 이렇게 이해하면 된다.

- `8000` 정도가 더 나을 수 있는 경우
  - LLM 응답이 자주 깨진다
  - timeout이 난다
  - 서버가 긴 입력에서 불안정하다
- `12000`이 무난한 경우
  - 현재처럼 표 구조와 section 보존이 중요하다
  - 문서 길이가 중간 정도다
  - 특별한 실패 신호가 없다
- `16000` 이상이 나을 수 있는 경우
  - chunk가 너무 잘게 쪼개져 펀드 inventory가 자주 누락된다
  - LLM 서버가 긴 입력을 안정적으로 처리한다

### 6-1-2. 초보 개발자는 어떻게 시작하면 되나

처음에는 아래처럼 하는 것이 가장 안전하다.

1. 기본값 `12000`으로 시작한다.
2. 대표 문서 5~10개를 돌려 본다.
3. 아래 문제가 자주 보이면 값을 조정한다.

- 펀드 누락
- coverage mismatch
- 응답 시간 과다
- invalid response 증가

즉 초보 개발자 기준으로는:
- **처음에는 12000으로 시작**
- **문서군 특성에 따라 나중에 조정**
이 가장 현실적이다.

### 6-1-3. `DocumentLoader`만 단독 사용하면 꼭 알아야 하나

꼭 그렇지는 않다.

정리하면:
- `load()`만 쓸 때
  - 크게 신경 쓰지 않아도 된다
- `build_task_payload()`까지 쓰거나
- `FundOrderExtractor`와 연결해서 쓸 때
  - 이 값이 추출 품질과 안정성에 영향을 준다

즉 `chunk_size_chars`는 `DocumentLoader`의 내부 구현 세부값이 아니라,
**다음 단계 LLM 추출 품질을 좌우하는 실전 파라미터**라고 보면 된다.

## 7. 반환값은 어떤 구조인가

`DocumentLoader.load()`는 `ExtractedDocumentText`를 반환한다.

주요 필드는 아래와 같다.

### 7-1. `raw_text`

원문 보존용 텍스트다.

주로 이런 용도로 쓴다.

- 사람 검수
- coverage 계산
- 원문 근거 추적
- markdown 변환 실패 분석

### 7-2. `markdown_text`

LLM 입력용 구조화 텍스트다.

특징:
- 표를 markdown table로 정리한다.
- 섹션 구조를 드러낸다.
- 원문보다 기계적으로 읽기 쉽다.

즉 추출 모델에게는 `raw_text`보다 이 값이 더 중요할 때가 많다.

### 7-3. `content_type`

현재 문서 형식을 설명하는 메타 정보다.

예:
- `application/pdf`
- `text/html`
- `message/rfc822`

로그나 분기 처리에서 참고할 수 있다.

실제로는 아래처럼 생각하면 된다.

```python
loaded = loader.load(path)

loaded.raw_text        # 원문 보존용
loaded.markdown_text   # 구조화 텍스트
loaded.content_type    # 파일 형식
```

반환 객체는 단순해서, 초보 개발자도 `print()`로 먼저 확인해 보기 좋다.

## 8. `DocumentLoader`에서 자주 같이 쓰는 함수

### 8-1. `estimate_order_cell_count(raw_text)`

원문 기준 기대 주문 건수를 추정한다.

이 값은 왜 중요하냐면:
- 실제 추출 결과가 너무 적거나 많을 때 이상 신호가 된다.
- coverage 검증의 기준값으로 쓰인다.

예시:

```python
expected = loader.estimate_order_cell_count(loaded.raw_text)
print("expected order cells:", expected)
```

### 8-2. `extract_target_fund_scope(raw_text)`

삼성 계열 운용사 대상 scope를 계산한다.

예시:

```python
scope = loader.extract_target_fund_scope(loaded.raw_text)
print(scope.manager_column_present)
print(scope.include_all_funds)
print(scope.fund_codes)
```

이 값은 주로 서비스/추출기가 사용하지만, 디버깅할 때 직접 보면 매우 도움이 된다.

처음에는 이 함수까지 한 번에 다 쓰려고 하기보다 아래 순서가 좋다.

1. `load()`
2. `raw_text` 확인
3. `markdown_text` 확인
4. `estimate_order_cell_count()`
5. 필요할 때만 `extract_target_fund_scope()`

## 9. 초보 개발자 기준 추천 사용 순서

처음에는 아래 순서로 접근하면 덜 헷갈린다.

1. `DocumentLoader()` 생성
2. `load(path)` 호출
3. `raw_text`와 `markdown_text`를 둘 다 출력해 보기
4. `estimate_order_cell_count(raw_text)` 확인
5. 필요하면 그 다음에 `ExtractionComponent`나 `ExtractionService`로 올라가기

추천 디버깅 순서:

1. 문서가 열리는지 확인
2. `content_type`이 예상과 맞는지 확인
3. `raw_text`에 핵심 주문 데이터가 남는지 확인
4. `markdown_text`에 표/섹션이 잘 정리되는지 확인
5. coverage 값이 0이 아닌지 확인

## 10. 형식별로 주의할 점

### 10-1. PDF

- split-row 표가 있을 수 있다.
- plain text와 table text 중 어느 쪽이 더 좋은지 문서마다 다르다.
- coverage가 낮게 잡히면 `raw_text`부터 먼저 확인하는 것이 좋다.

### 10-2. XLS / XLSX

- 병합 셀, 소계행, 빈 식별자 행 때문에 표 구조가 흔들릴 수 있다.
- `입금소계/출금소계`처럼 ID가 비어 있는 행을 어떻게 carry 하는지 확인이 중요하다.
- 최근에는 `추가설정금액`, `당일인출금액`, `해지신청`, `설정신청` 같은 label과 `억/만/천` 단위 금액도 coverage 후보로 본다.

### 10-3. EML

- 현재 메시지와 이전 스레드가 섞일 수 있다.
- 현재 구현은 이전 메시지를 잘라내고 현재 메시지만 남기도록 보강되어 있다.
- 메일 헤더 날짜가 있으면 `Date (Asia/Seoul)`가 상단에 추가되어 extractor의 document-level 기준일 fallback으로 쓰인다.

### 10-4. HTML / MHT

- 표 구조는 좋지만, 보조 컬럼의 0 값이 같이 들어갈 수 있다.
- 실제 주문 금액 컬럼과 보조 컬럼을 구분해서 봐야 한다.
- MHT는 가능하면 HTML 렌더링 전에 parser-first로 읽고, 필요할 때만 render hint에 기대는 편이 현재 기준이다.

## 10-5. 형식별 추천 첫 테스트 문서

처음 포팅한 뒤에는 아래 순서로 테스트하면 좋다.

1. HTML
   - 가장 구조가 단순한 편
   - 표가 바로 보이므로 성공 여부를 판단하기 쉽다
2. XLSX
   - 엑셀 파서와 표 정리가 동작하는지 확인하기 좋다
3. PDF
   - 가장 변수가 많다
4. EML / MHT
   - 이전 메시지 제거, HTML/plain 후보 선택 같은 추가 로직이 있다

## 11. 자주 하는 실수

### 11-1. `DocumentLoader`만 복사하고 `document_loaders/`를 안 복사하는 경우

이 경우 import는 되어도 형식별 mixin이 없어 바로 실패한다.

### 11-2. 문서 경로를 문자열만 넘기고 상대경로 기준을 헷갈리는 경우

가능하면 항상 `Path`로 명시하는 것이 좋다.

```python
from pathlib import Path

loader.load(Path("/absolute/path/to/file.pdf"))
```

### 11-3. `DocumentLoader`로 최종 주문 JSON까지 바로 나올 거라고 기대하는 경우

`DocumentLoader`는 문서를 읽고 구조를 정리하는 단계다.  
즉 아래는 가능하다.

- 원문 읽기
- markdown 만들기
- coverage 계산

반대로 아래는 `DocumentLoader`만으로 끝나지 않는다.

- 최종 주문 JSON 만들기
- 다건 병합
- CSV 저장

## 12. 다른 서버에서 빠르게 smoke test 하는 방법

아래처럼 아주 작은 HTML fixture를 하나 만들어 바로 확인할 수 있다.

```python
from pathlib import Path
from document_loader import DocumentLoader

sample = Path("/tmp/sample_instruction.html")
sample.write_text(
    \"\"\"
    <html>
      <body>
        <h1>변액일임펀드 설정/해지 지시서</h1>
        <table>
          <tr><th>펀드코드</th><th>펀드명</th><th>설정금액</th><th>해지금액</th></tr>
          <tr><td>V2201S</td><td>샘플성장형</td><td>100,000</td><td>0</td></tr>
        </table>
      </body>
    </html>
    \"\"\".strip(),
    encoding=\"utf-8\",
)

loader = DocumentLoader()
loaded = loader.load(sample)
print(loaded.content_type)
print(loader.estimate_order_cell_count(loaded.raw_text))
```

정상이라면 최소한 아래를 기대할 수 있다.

- `content_type == "text/html"`
- `raw_text` 안에 `V2201S` 존재
- `estimate_order_cell_count(...) > 0`

추가로 보면 좋은 것:
- `markdown_text` 안에 markdown table 형태가 보이는지
- `content_type`이 `text/html`로 잡히는지

## 13. 문제 생기면 어디를 먼저 보면 되나

가장 자주 보는 파일은 아래 순서다.

1. [app/document_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loader.py)
2. [app/document_loaders/pdf_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/pdf_loader.py)
3. [app/document_loaders/excel_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/excel_loader.py)
4. [app/document_loaders/eml_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/eml_loader.py)
5. [tests/test_document_loader_standalone.py](/Users/bhkim/Documents/codex_prj_sam_asset/tests/test_document_loader_standalone.py)

문제 유형별 첫 확인 포인트:

- import 실패
  - `document_loader.py` 상단 import shim
- 특정 형식만 실패
  - 해당 형식 mixin 파일
- coverage가 0
  - `raw_text`와 `estimate_order_cell_count()`
- EML이 이전 메시지까지 읽음
  - `eml_loader.py`의 thread trimming 로직

초보 개발자 기준으로는 아래처럼 접근하면 된다.

### 13-1. import 자체가 안 된다

확인할 것:
- `document_loader.py`
- `document_loaders/` 폴더
- `__init__.py`
- `pip install pdfplumber openpyxl xlrd`
- secure HTML/ZIP 메일까지 보려면 `pip install cryptography`

### 13-2. 문서는 열리는데 내용이 비어 있다

확인할 것:
- 파일 경로가 맞는지
- `loaded.content_type`
- `loaded.raw_text[:500]`

### 13-3. coverage가 0이다

확인할 것:
- `loaded.raw_text` 안에 금액/코드/표가 실제로 보이는지
- 해당 형식별 loader가 원문을 잘 읽었는지

### 13-4. PDF만 유독 이상하다

확인할 것:
- 텍스트 레이어가 있는 PDF인지
- 표가 split-row 형태인지
- `pdf_loader.py` 쪽 파싱 결과

## 14. 실제 포팅 절차를 한 번에 따라 하기

아래 순서대로 하면 된다.

1. 새 서버에 작업 폴더 생성
2. `document_loader.py`와 `document_loaders/` 복사
3. `pip install pdfplumber openpyxl xlrd`
   - secure HTML/ZIP 메일까지 필요하면 `cryptography` 추가 설치
4. 샘플 HTML 문서로 smoke test
5. 실제 운영 문서 1건으로 `raw_text`, `markdown_text`, `coverage` 확인
6. 그 다음에만 상위 서비스 코드와 연결

처음부터 PDF/EML 실문서로 들어가면 원인 분리가 어려우니, 꼭 작은 HTML로 먼저 성공을 만드는 것이 좋다.

## 15. 한 줄 요약

`DocumentLoader`는 **문서를 읽고 구조를 정리하는 독립형 로더**다.  
다른 서버로 포팅할 때는 `document_loader.py + document_loaders/`를 함께 복사하고,  
문서 경로는 호출하는 쪽에서 `Path(...)`로 직접 넘기면 된다.

## 2026-04-16 업데이트 메모

- `.xlsx/.xlsm` numeric cell은 이제 `normalize_excel_numeric_cell()` helper를 거쳐 loader raw/markdown 문자열로 렌더링한다.
- 정수에 매우 가까운 float-tail artifact는 정수 문자열로 정리한다.
  - loader text 예: `70,000,000.00000001 -> 70000000`
  - final payload 예: `70,000,000.00000001 -> 70,000,000`
- true decimal 값은 보존한다.
  - 예: `50,572.49`, `0.0019`
- 최종 payload에서 소수 자릿수를 강제로 2자리로 맞추는 일은 loader 책임이 아니다.
  - 지시서/추출 문자열에 `23,213.40`이 남아 있으면 후단 formatter가 `.40`을 유지한다.
  - `23,213.4`를 `23,213.40`으로 padding하거나 `23,213.409`를 `23,213.40`으로 절삭하지 않는다.
- `.xls` legacy BIFF direct path와 parser fallback 계약은 그대로 유지한다.
- 최근 대표 회귀 확인 케이스는 `ABL`, `DB`, `iM`, `KB`, `KDB`, `라이나`, `흥국생명-hanais`다.
- 테스트 결과 보고 기준은 [test_report/20260420_로컬_WAS_테스트_수행_방안.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260420_로컬_WAS_테스트_수행_방안.md)를 출처로 둔다.

## 2026-04-20 업데이트 메모

- 하나생명 workbook coverage의 상세 규칙과 검증 결과는 [merge_report/merge_result_report_20260420_01.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/merge_result_report_20260420_01.md)를 출처로 둔다.
- 로컬/WAS 테스트 재수행 절차와 보고서 필수 항목은 [test_report/20260420_로컬_WAS_테스트_수행_방안.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260420_로컬_WAS_테스트_수행_방안.md)를 출처로 둔다.
