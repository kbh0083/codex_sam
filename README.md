# Fund Order Extractor

변액일임 설정/해지 지시서(PDF/HTML/EML/MHT/XLS/XLSX/XLSM)에서 거래 주문 데이터를 추출하는 서비스입니다.  
현재 runtime은 거래처별 rule-base를 사용하지 않고, `LLM + 결정론적 전처리/검증` 구조로 동작합니다.

## 핵심 요약

- PDF는 `pdfplumber`, HTML은 내장 파서, 엑셀은 `openpyxl`/`xlrd`로 읽습니다.
- EML은 현재 메시지만 남기고, 이전 메시지 스레드는 로더 단계에서 제거합니다.
- 문서는 항상 두 형태로 관리됩니다.
  - `raw_text`: 원문 보존용
  - `markdown_text`: LLM 입력용 구조화 표현
- 추출은 항상 `6단계 staged LLM extraction`으로 수행합니다.
- 결과는 저장 전에 `coverage 검증`과 `blocking issue 검증`을 통과해야 합니다.
- 통보문/안내 메일처럼 지시서가 아닌 문서는 추출 전에 명시적 예외로 차단합니다.
- CLI는 검증을 통과한 결과만 JSON/CSV로 생성합니다.
- 다른 서버에서 재사용할 때는 `ExtractionComponent`를 진입점으로 사용하면 같은 검증/병합/CSV 규칙과 handler A/B 경로를 그대로 공유할 수 있습니다.
- `ExtractionService`를 직접 쓰더라도 내부적으로는 동일하게 `DocumentLoader 결과 JSON 저장 -> 재로딩 -> FundOrderExtractor` 경로를 탑니다.
- handler A 산출물(`DocumentLoadTaskPayload`)의 저장 경로와 삭제 여부는 env로 제어합니다.
  - `TASK_PAYLOAD_OUTPUT_DIR`
  - `DELETE_TASK_PAYLOAD_FILES`
- `only_pending` 옵션을 주면 최종 결과에서 기존 `PENDING` 주문은 제거되고,
  남은 주문은 내부적으로 `PENDING`으로 맞춘 뒤 최종 출력에서는 pending 코드 `"1"`로 기록됩니다.
- 최종 JSON/CSV 출력 계약에서는 상태값을 문자열 코드로 치환합니다.
  - `settle_class`: `PENDING -> "1"`, `CONFIRMED -> "2"`
  - `order_type`: `RED -> "1"`, `SUB -> "3"`

## 저장소 구성 안내

- 이 저장소에는 코드, 테스트, 가이드 문서만 포함합니다.
- 실제 거래처 원문 문서와 정답/검수 산출물은 git에 포함하지 않습니다.
  - 제외 대상 예: `document/`, `거래처별_문서_정답/`, `output/`, `merge_report/`, `test_report/`
- 로컬 실행용 비밀값은 `.env`로 관리하며 git에는 포함하지 않습니다.
- 따라서 일부 live regression이나 실문서 기반 검증은 로컬 fixture를 별도로 준비해야 재현할 수 있습니다.

## 유지보수 메모

최근에는 처음 합류한 개발자도 빠르게 흐름을 따라갈 수 있도록
핵심 helper 레벨까지 한국어 주석을 자세히 보강했다.

처음 보는 개발자를 위한 개별 가이드는 아래 문서를 보면 된다.

- [DocumentLoader 가이드](/Users/bhkim/Documents/codex_prj_sam_asset/readme/DocumentLoader_가이드.md)
- [FundOrderExtractor 가이드](/Users/bhkim/Documents/codex_prj_sam_asset/readme/FundOrderExtractor_가이드.md)

- [app/document_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loader.py)
  - markdown 변환, coverage, identity dedupe가 왜 그렇게 동작하는지 설명
- [app/document_loaders/pdf_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/pdf_loader.py)
  - table/plain 후보 비교와 carry 로직의 목적 설명
- [app/document_loaders/html_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/html_loader.py)
  - rowspan/colspan 전개와 repeated value blanking 이유 설명
- [app/extractor.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extractor.py)
  - 6단계 stage 모델, evidence 집계, final issue 정리 의도 설명

새 문서 변형이 들어왔을 때는 함수 이름만 보기보다,
해당 helper 위 주석에서 "어떤 오탐/누락을 막기 위해 존재하는지"를 먼저 읽는 편이
원인 추적 속도가 훨씬 빠르다.

## 코드 읽기 순서

새로 합류한 개발자는 아래 순서로 보면 전체 흐름이 빠르게 잡힙니다.

1. [main.py](/Users/bhkim/Documents/codex_prj_sam_asset/main.py)
   - CLI 진입점
   - 입력 파일, 출력 파일, 로그 흐름 확인
2. [app/component.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/component.py)
   - 외부 시스템(WAS/배치)이 직접 재사용할 권장 파사드
   - `DocumentLoader -> handoff file -> FundOrderExtractor` 경로를 실제로 수행
   - 문서별 추출, 병합, CSV 평탄화 책임
3. [app/service.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/service.py)
   - 단건 추출 호환용 계층
   - 다만 내부적으로는 component와 같은 handoff 파일 경로를 사용
   - 즉 service를 직접 써도 `DocumentLoader 결과 파일 저장 -> 재로딩 -> 추출` 흐름이 유지됨
4. [app/document_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loader.py)
   - 공통 문서 정규화 진입점
   - `raw_text` -> `markdown_text`
   - coverage 계산
5. 형식별 로더
   - [app/document_loaders/pdf_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/pdf_loader.py)
   - [app/document_loaders/html_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/html_loader.py)
   - [app/document_loaders/excel_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/excel_loader.py)
6. [app/extractor.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extractor.py)
   - 6단계 staged LLM extraction
   - YAML prompt 로드, 응답 파싱, 정규화, 충돌 해소
7. [app/schemas.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/schemas.py)
   - 최종 데이터 계약
8. [app/prompts/extraction_prompts.yaml](/Users/bhkim/Documents/codex_prj_sam_asset/app/prompts/extraction_prompts.yaml)
   - system prompt, 공통 user prompt template, 6단계 stage prompt 정의

## 런타임 아키텍처

```text
CLI / Host Application
  -> ExtractionComponent
    -> Handler A
      -> DocumentLoader
        -> raw_text
        -> markdown_text
        -> DocumentLoadTaskPayload(JSON handoff)
    -> Handler B
      -> DocumentLoadTaskPayload(JSON reload)
      -> FundOrderExtractor
        -> Stage 1: fund_inventory
        -> Stage 2: base_date
        -> Stage 3: t_day / slot_id / evidence_label
        -> Stage 4: transfer_amount
        -> Stage 5: settle_class
        -> Stage 6: order_type
      -> deterministic normalization
      -> coverage validation
      -> blocking issue validation
    -> JSON/CSV output or host application 전달
```

## 처리 프로세스

### 1. 문서 로드

[app/document_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loader.py) 는 공통 진입점이고, 형식별 원문 추출은 아래 파일로 분리되어 있습니다.

- PDF: [app/document_loaders/pdf_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/pdf_loader.py)
- HTML: [app/document_loaders/html_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/html_loader.py) (`utf-8/cp949/euc-kr` 지원)
- XLSX/XLSM/XLS: [app/document_loaders/excel_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/excel_loader.py)

출력은 `ExtractedDocumentText` 입니다.

```python
ExtractedDocumentText(
    raw_text="원문 텍스트",
    markdown_text="LLM 입력용 markdown",
    content_type="..."
)
```

### 2. raw_text -> markdown_text

LLM이 직접 문서를 재정리하지 않도록, 코드가 먼저 `markdown_text`를 만듭니다.

- 페이지/시트 단위로 section heading 생성
- 파이프 형태 표는 markdown table로 변환
- 일반 텍스트는 fenced code block으로 유지
- 원문 `raw_text`는 그대로 보존

이 단계의 목적은 두 가지입니다.

- 표 헤더와 데이터 행의 관계를 LLM에 더 명확하게 전달
- 검수/디버깅 시 raw_text를 기준으로 역추적 가능하게 유지

### 3. PDF 정규화 방식

PDF는 단순 `extract_text()`만으로는 품질이 부족해서, 추가 보정 로직이 들어 있습니다.

- 여러 `pdfplumber.extract_tables()` 설정을 시도
- 가장 덜 fragment 된 후보 table set 선택
- 표 안에서 `preamble / header / body` 분리
- `2. Redemption` 같은 섹션 마커를 데이터행이 아니라 텍스트 문맥으로 분리
- 멀티페이지 표의 경우 이전 페이지 헤더 carry-over
- 표 주변의 문맥 라인 (`Subscription`, `Redemption`, `기준일자`, `수신처` 등) 보존

즉 PDF 로더는 단순 텍스트 추출기가 아니라, **LLM이 읽기 좋은 거래 지시서 표현을 만드는 전처리기** 역할을 합니다.

### 4. 6단계 staged LLM extraction

[app/extractor.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extractor.py) 는 한 번에 전체 주문을 만들지 않고, 다음 6단계로 나눠 처리합니다.

프롬프트 원문은 코드에 직접 하드코딩하지 않고 [app/prompts/extraction_prompts.yaml](/Users/bhkim/Documents/codex_prj_sam_asset/app/prompts/extraction_prompts.yaml) 에서 읽습니다.
즉 system prompt, 공통 user wrapper, stage별 지시문 수정은 YAML 파일만 변경하면 됩니다.
YAML 로드 시에는 공통 user prompt template의 placeholder를 바로 검증하므로, 오타가 있으면 추출 단계 진입 전에 명확한 예외로 실패합니다.
또한 long-running 프로세스에서도 YAML 파일 수정 시 다음 추출 요청에서 자동으로 다시 로드합니다.
운영 중 YAML 파일이 일시적으로 깨져도, 이미 로드된 마지막 정상 프롬프트가 있으면 그 버전으로 계속 처리하고 경고 로그만 남깁니다.
각 추출 요청은 시작 시점의 프롬프트 스냅샷을 고정해서 stage 1~6 전체를 같은 버전으로 처리합니다.

### 4-1. 현재 CLI와 WAS가 같은 경로를 타는 이유

예전에는 CLI가 service를 통해 바로 최종 결과를 만들었고,
WAS에서는 queue 기반으로 handler A/B를 따로 구현해야 해서 실행 경로가 어긋날 수 있었습니다.

현재는 [main.py](/Users/bhkim/Documents/codex_prj_sam_asset/main.py) 도 내부적으로 아래 순서를 그대로 따릅니다.

1. `DocumentLoader.build_task_payload(...)`
2. `DocumentLoadTaskPayload.write_json(...)`
3. `DocumentLoadTaskPayload.read_json(...)`
4. `FundOrderExtractor.extract_from_task_payload(...)`

즉 로컬 CLI 검수와 WAS 운영 경로가 같은 구조를 타므로,
로컬에서 재현된 문제가 WAS에서도 거의 같은 방식으로 재현됩니다.
같은 이유로 `ExtractionService.extract_file_path()`도 내부적으로는 이 handoff 경로를 한 번 거칩니다.
즉 단건 service 호출도 운영 queue 경로와 최대한 같은 방식으로 동작하게 맞춰져 있습니다.

현재 기본 `.env`는 아래처럼 설정되어 있습니다.
- `TASK_PAYLOAD_OUTPUT_DIR=./output/handoff`
- `DELETE_TASK_PAYLOAD_FILES=false`

즉 기본값은 handoff 파일을 삭제하지 않고 남겨 두는 쪽입니다.

또한 CLI에서는 `--only-pending` 옵션을 지원합니다.
- 기본값: `False`
- `True`일 때 동작:
  1. 최종 추출 결과에서 `settle_class == PENDING` 인 주문 제거
  2. 남은 주문의 `settle_class`를 모두 `PENDING`으로 변경

그리고 최종 JSON/CSV 저장 시에는 아래 문자열 코드 규칙이 적용됩니다.
- `settle_class`
  - `PENDING -> "1"`
  - `CONFIRMED -> "2"`
- `order_type`
  - `RED -> "1"`
  - `SUB -> "3"`

예:

```bash
./.venv/bin/python main.py -f /path/to/order.pdf --only-pending
```

1. `fund_inventory`
   - `fund_code`, `fund_name` 전수 수집
2. `base_date`
   - 각 fund의 기준일 추출
3. `t_day`
   - 거래 slot 나열
   - `t_day`, `slot_id`, `evidence_label` 생성
4. `transfer_amount`
   - slot별 금액 확정
5. `settle_class`
   - `CONFIRMED` / `PENDING`
6. `order_type`
   - `SUB` / `RED`

### 5. evidence_label의 역할

`evidence_label`은 최종 응답에는 나오지 않는 내부 필드입니다.

예시:

- `설정금액`
- `해지금액`
- `입금액`
- `출금액`
- `당일이체금액`
- `2025-11-28`
- `T+1 투입금액`
- `Redemption`

이 값을 stage 3에서 만들고 stage 4~6까지 끌고 가는 이유는,
LLM이 “어느 컬럼을 근거로 이 거래 슬롯을 만들었는지”를 잃지 않게 하기 위해서입니다.

### 6. 정규화와 충돌 해소

LLM 결과는 바로 저장하지 않습니다. 먼저 결정론적 후처리를 거칩니다.

- 금액 문자열 정규화
- 날짜 문자열 정규화
- enum 정규화
- `좌수 / unit / share` 계열 evidence 제거
- `0`, `-`, 빈칸 금액 제거

그리고 확정 거래에서 다음 충돌을 정리합니다.

- explicit amount:
  - `설정금액`, `해지금액`, `입금액`, `출금액`
- execution amount:
  - `당일이체금액`, `실행금액`

문서에 두 스키마가 같이 있을 수 있기 때문에,  
먼저 candidate를 모두 만들고, 마지막에 문서 전체 경향을 보고 충돌을 해소합니다.

## 검증과 저장 정책

### coverage 검증

[app/document_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loader.py) 의 `estimate_order_cell_count()`가 원문에서 보이는 금액 셀 수를 추정합니다.

- 예상 버킷 수와 최종 `orders` 수가 다르면 `ORDER_COVERAGE_MISMATCH`
- 결과 반환 전 차단

이 검증은 “의미적으로 100% 맞다”를 보장하지는 않지만,
문서 구조 붕괴나 stage 누락으로 인한 과소/과다 추출을 강하게 걸러냅니다.

### blocking issue

다음 이슈가 남아 있으면 저장하지 않습니다.

- `LLM_INVALID_RESPONSE_FORMAT`
- `FUND_DISCOVERY_EMPTY`
- `TRANSACTION_SLOT_EMPTY`
- `TRANSFER_AMOUNT_EMPTY`
- `SETTLE_CLASS_EMPTY`
- `FUND_METADATA_INCOMPLETE`
- `TRANSFER_AMOUNT_MISSING`
- `SETTLE_CLASS_MISSING`
- `ORDER_TYPE_MISSING`
- `T_DAY_MISSING`
- `BASE_DATE_STAGE_PARTIAL`
- `TRANSFER_AMOUNT_STAGE_PARTIAL`
- `SETTLE_CLASS_STAGE_PARTIAL`
- `ORDER_TYPE_STAGE_PARTIAL`
- `ORDER_COVERAGE_MISMATCH`

`BASE_DATE_MISSING`는 비차단 이슈입니다.  
즉 기준일이 없더라도 나머지 주문이 완전하면 저장은 가능합니다.

### 비지시서 문서 예외

일부 입력은 파일 형식상 읽을 수는 있어도, 업무적으로는 변액일임펀드 설정/해지 지시서가 아닐 수 있습니다.

예:
- `설정해지금액통보` 제목은 있지만 실제 주문 표/금액 근거가 없는 통보성 문서
- 첨부파일 송부 안내만 있고 실제 주문 표는 없는 cover email

이 경우는 LLM을 태워 `FUND_DISCOVERY_EMPTY` 같은 간접 실패를 만드는 대신,
서비스가 먼저 비지시서로 판정해서 `ValueError` 계열 예외를 올립니다.

## CLI

빠른 단건 검증:

```bash
python main.py -f /Users/bhkim/20_code_test/documents/sample_variable_annuity/카디프_251127.pdf
```

암호화 PDF:

```bash
python main.py -f /path/to/locked-order.pdf -p your-password
```

출력 경로 지정:

```bash
python main.py -f /path/to/order.pdf -o /path/to/result.json
```

CLI 동작 특징:

- 검증 통과 시에만 JSON/CSV 생성
- 파일명은 자동으로 `YYYYMMDD_HHMMSS` 타임스탬프 부여

## 다른 서버에서 재사용하기

현재 저장소에는 FastAPI 엔트리가 없습니다.  
기존 개발 서버나 별도 WAS에서 추출 기능만 이관하려면 [app/component.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/component.py)의 `ExtractionComponent`를 호출하는 쪽이 가장 안전합니다.
이 컴포넌트가 문서별 추출, 다건 병합, CSV 평탄화 규칙까지 감싸고 있기 때문입니다.

```python
from app.component import DocumentExtractionRequest, ExtractionComponent

component = ExtractionComponent()
payload = component.extract_document_payload(
    DocumentExtractionRequest(
        source_path="/path/to/order.pdf",
        pdf_password=None,
    )
)
```

WAS 포팅 상세 설명과 최소 샘플 구조는 아래 문서를 같이 보면 됩니다.

- [readme/WAS_포팅_가이드.md](/Users/bhkim/Documents/codex_prj_sam_asset/readme/WAS_포팅_가이드.md)
- [readme/DocumentLoader_가이드.md](/Users/bhkim/Documents/codex_prj_sam_asset/readme/DocumentLoader_가이드.md)
- [examples/was_minimal/README.md](/Users/bhkim/Documents/codex_prj_sam_asset/examples/was_minimal/README.md)

`DocumentLoader`만 따로 떼어 쓰고 싶다면,
[readme/DocumentLoader_가이드.md](/Users/bhkim/Documents/codex_prj_sam_asset/readme/DocumentLoader_가이드.md)를 먼저 보면 된다.

포팅 시 기준을 짧게 정리하면 이렇다.
- `DocumentLoader` 소스 위치는 복사한 패키지 기준으로 결정된다.
- 실제 문서 저장 경로는 호출하는 쪽이 `Path(...)`로 넘긴 값에 따라 달라진다.
- 즉 다른 서버에서 문서 보관 디렉터리가 바뀌어도, `DocumentLoader` 자체는 그 경로를 고정하지 않는다.
- `DOCUMENT_INPUT_DIR`는 CLI/서비스 레벨에서 "파일명만 받았을 때 기본으로 어디서 찾을지"를 정하는 env이다.

## .env 설정

루트의 [.env](/Users/bhkim/Documents/codex_prj_sam_asset/.env) 를 기본 설정으로 읽습니다.

예시:

```env
DEBUG_OUTPUT_DIR=./output/debug
DOCUMENT_INPUT_DIR=./document
LLM_MODEL=qwen3-next-80B-A3B-instruct
LLM_BASE_URL=http://localhost:8000/v1
LLM_API_KEY=dummy
LLM_TEMPERATURE=0
LLM_MAX_TOKENS=16384
LLM_TIMEOUT_SECONDS=120
LLM_CHUNK_SIZE_CHARS=12000
LLM_STAGE_BATCH_SIZE=12
```

같은 이름의 OS 환경변수가 있으면 `.env`보다 우선합니다.

참고:

- 현재 런타임은 JSON/CSV 출력 전용입니다.
- 따라서 DB 연결이나 업로드 저장 관련 설정은 더 이상 필요하지 않습니다.

## 디버깅

LLM stage 응답이 JSON으로 파싱되지 않으면 raw 응답을 별도 파일로 저장합니다.

- 위치: `DEBUG_OUTPUT_DIR`
- 파일명 예시:
  - `<문서명>_<timestamp>_<stage>_chunk01_llm_invalid_response.txt`

로그는 다음 순서로 읽으면 됩니다.

1. 파일 로드
2. markdown 생성
3. chunk 분할
4. stage 1~6
5. coverage 검증
6. 저장 또는 차단

## 테스트

문법 검증:

```bash
python3 -m compileall app tests main.py
```

단위 테스트:

```bash
./.venv/bin/python -m unittest discover -s tests
```

권장 회귀 테스트:

```bash
python main.py -f /path/to/sample.pdf
python main.py -f /path/to/encrypted.pdf -p password
python main.py -f /path/to/sample.xlsx
```

## 현재 런타임에서 사용하지 않는 코드

저장소에는 과거 실험/이행용 코드가 일부 남아 있을 수 있습니다.  
하지만 현재 runtime에서 실제로 사용하는 경로는 다음 두 모듈이 중심입니다.

- [app/document_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loader.py)
- [app/document_loaders/pdf_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/pdf_loader.py)
- [app/document_loaders/html_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/html_loader.py)
- [app/document_loaders/excel_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/excel_loader.py)
- [app/extractor.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extractor.py)

운영 동작을 이해하거나 수정할 때는 이 경로를 기준으로 보는 것이 맞습니다.

## 한계

- OCR은 포함하지 않습니다.
- 스캔 이미지 PDF는 처리할 수 없습니다.
- coverage 검증은 건수 기준 방어선이지, 최종 의미 검수 자체는 아닙니다.
- 새로운 양식은 프롬프트 보강으로 대응하는 구조이지만, 심한 레이아웃 붕괴가 있으면 loader 튜닝이 여전히 필요할 수 있습니다.
