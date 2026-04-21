# FundOrderExtractor 가이드

이 문서는 `FundOrderExtractor`를 처음 접하는 개발자를 위한 안내서다.  
목표는 단순하다. **이 문서 하나만 읽어도 `FundOrderExtractor`가 무엇을 하는지 이해하고, 실제 코드에서 안전하게 호출해 볼 수 있게 만드는 것**이다.

> 2026-04-20 기준 보강.
> 공통 최신 상태와 테스트 보고 원칙은 [처음읽는_개발자용_흐름.md](/Users/bhkim/Documents/codex_prj_sam_asset/readme/처음읽는_개발자용_흐름.md)를 출처로 둔다.
> 이 문서는 `FundOrderExtractor` 전용 규칙만 유지한다. 2026-04-20 extractor 병합 근거는 [merge_report/merge_result_report_20260420_01.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/merge_result_report_20260420_01.md)를 따른다.

즉 아래 질문에 이 문서 안에서 바로 답을 찾을 수 있도록 구성했다.

- `FundOrderExtractor`는 정확히 무엇을 하는가
- `DocumentLoader`와는 어떻게 연결되는가
- 어떤 파일을 같이 가져가야 하는가
- 어떤 환경변수가 필요한가
- 가장 간단한 실행 예제는 무엇인가
- `only_pending`은 어디에서 적용해야 하는가
- 최종 출력 코드 치환은 어디에서 해야 하는가
- 실패하면 무엇을 먼저 봐야 하는가

## 0. 가장 빠른 시작 방법

아무 설명보다 먼저 "가장 짧은 성공 경로"를 보고 싶다면 아래 순서로 실행하면 된다.

### 0-1. 가장 간단한 예제

```python
from pathlib import Path

from app.config import get_settings
from app.document_loader import DocumentLoader
from app.extractor import FundOrderExtractor, apply_only_pending_filter
from app.output_contract import serialize_order_payload

settings = get_settings()
loader = DocumentLoader()
extractor = FundOrderExtractor(settings)

task_payload = loader.build_task_payload(
    Path("/data/orders/sample.pdf"),
    chunk_size_chars=settings.llm_chunk_size_chars,
)

outcome = extractor.extract_from_task_payload(task_payload)
result = apply_only_pending_filter(outcome.result, only_pending=False)

payload = {
    "file_name": task_payload.file_name,
    "source_path": task_payload.source_path,
    "model_name": settings.llm_model,
    "base_date": next((order.base_date for order in result.orders if order.base_date), None),
    "issues": result.issues,
    "orders": [serialize_order_payload(order) for order in result.orders],
}

print(payload["orders"][0])
```

여기까지 성공하면 extractor 사용 자체는 완료된 것이다.

### 0-2. 이 예제가 하는 일

위 예제는 아래 순서대로 동작한다.

1. `DocumentLoader`가 문서를 읽어 `DocumentLoadTaskPayload`를 만든다.
2. `FundOrderExtractor`가 그 payload를 받아 7-stage LLM 추출을 수행한다.
3. `apply_only_pending_filter()`가 최종 결과 후처리를 적용한다.
4. `serialize_order_payload()`가 외부 출력 계약에 맞게 문자열 코드로 바꾼다.

중요한 점은 이 두 단계가 서로 다르다는 것이다.

- extractor 내부 결과
  - `settle_class = PENDING / CONFIRMED`
  - `order_type = SUB / RED`
- 최종 외부 출력
  - `settle_class = "1" / "2"`
  - `order_type = "1" / "3"`

즉 `FundOrderExtractor`는 **주문을 뽑는 역할**을 하고,  
최종 코드 치환은 **출력 계약 단계**에서 별도로 한다.

## 1. `FundOrderExtractor`는 무엇을 하는가

`FundOrderExtractor`는 `DocumentLoader`가 만든 문서 텍스트를 받아,  
**최종 주문 데이터(`OrderExtraction`)를 만드는 모듈**이다.

정확히는 아래 일을 담당한다.

- LLM 7-stage 추출 수행
- 각 단계 응답 JSON 검증
- stage partial issue 기록
- evidence 기반 후처리
- 금액 충돌 해소
- `settle_class`, `order_type`, `t_day`, `transfer_amount` 정규화
- coverage mismatch 반영
- blocking issue 검사
- invalid LLM raw response artifact 보존

쉽게 말하면:

- `DocumentLoader`
  - 문서를 읽고 구조를 정리한다
- `FundOrderExtractor`
  - 그 구조화된 텍스트에서 실제 주문을 뽑는다

즉 extractor는 **"문서 읽기"가 아니라 "주문 해석"** 쪽 책임을 가진다.

## 1-1. 2026-04-20 현재 기준으로 먼저 알아둘 점

최근 세션을 반영하면 extractor를 이해할 때 아래 차이를 먼저 기억하는 편이 빠르다.

- 기본 뼈대는 `instruction_document -> fund_inventory -> base_date -> t_day -> transfer_amount -> settle_class -> order_type`의 7-stage LLM 추출이지만, `base_date` 이후에는 구조 기반 shortcut과 markdown shortcut으로 나머지 stage를 건너뛸 수 있다.
- `Date (Asia/Seoul)`는 강한 document-level 기준일 fallback이지만, 행/섹션에서 더 구체적인 날짜 증거가 있으면 그 날짜가 우선한다.
  - AIA BUY & SELL REPORT처럼 문서 상단 메일 날짜보다 표 안의 uniform transaction date가 더 정확한 케이스가 있다.
- `transfer_amount` 증거가 충분하면 코드가 `settle_class`를 결정론적으로 복구할 수 있다.
- decorated total row, `7개 펀드` summary 같은 행은 extractor 후반부에서도 다시 걸러서 coverage inflate를 막는다.
- Local standalone 실행에서는 [app/prompts/counterparty_prompt_map.yaml](/Users/bhkim/Documents/codex_prj_sam_asset/app/prompts/counterparty_prompt_map.yaml)의 `content_match_tokens`까지 써서 거래처 prompt를 보조 매핑할 수 있다.
- 하지만 WAS에서는 DB prompt/설정값이 source of truth이므로, 이 YAML helper를 그대로 포팅 기준으로 삼으면 안 된다.
- 하나생명은 XLSX workbook을 authoritative instruction으로 본다.
  - 방향은 page-level legacy PDF hint가 아니라 row-level `거래유형명/구분`에서 `SUB/RED`를 결정한다.
  - 금액은 normalized header segment 중 `설정해지금액`을 최우선으로 사용한다.
  - `펀드납입출금액`, `판매회사분결제금액`은 같은 값이어도 primary evidence로 쓰지 않는다.
  - 현재 저장본 기준 exact order list와 금액은 [merge_report/merge_result_report_20260420_01.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/merge_result_report_20260420_01.md)를 출처로 둔다.
- false-warning cleanup은 "결과가 맞으니 무조건 무시"가 아니라, deterministic corroboration과 baseline/original-doc exactness가 충족될 때만 diagnostic을 제거하는 방향으로 유지한다.

## 2. `FundOrderExtractor`가 하지 않는 일

처음 보면 extractor가 많은 일을 하므로, 여기서 하지 않는 일을 같이 구분하는 게 중요하다.

`FundOrderExtractor`는 아래 일을 직접 하지 않는다.

- 파일 경로를 직접 읽지 않는다
- PDF/HTML/XLSX를 직접 파싱하지 않는다
- 여러 문서를 병합하지 않는다
- 최종 JSON/CSV 파일을 직접 쓰지 않는다
- 최종 외부 출력 코드 치환을 직접 하지 않는다
- `only_pending`을 자동 적용하지 않는다

정리하면:

- 문서 읽기: `DocumentLoader`
- 주문 추출: `FundOrderExtractor`
- 최종 출력 계약(JSON/CSV): `output_contract`, `ExtractionComponent`, `ExtractionService`

그래서 **"문서 파일 하나를 바로 최종 결과 JSON으로 만들고 싶다"**면  
대부분은 `FundOrderExtractor` 단독보다 [app/component.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/component.py)의 `ExtractionComponent`를 쓰는 편이 더 쉽다.

## 3. 내부 동작 방식

`FundOrderExtractor`는 한 번에 전체 주문을 만들지 않고, 아래 7단계로 나눠서 추출한다.

1. `instruction_document`
   - 실제 지시서인지 판별
2. `fund_inventory`
   - 펀드 목록 찾기
3. `base_date`
   - 각 펀드의 기준일 찾기
4. `t_day`
   - 거래 슬롯(`slot_id`, `t_day`, `evidence_label`) 찾기
5. `transfer_amount`
   - 슬롯별 금액 찾기
6. `settle_class`
   - `PENDING / CONFIRMED`
7. `order_type`
   - `SUB / RED`

이렇게 나누는 이유는 한 번에 다 시키면 모델이 아래 문제를 일으키기 쉽기 때문이다.

- 펀드 누락
- 기준일과 금액 컬럼 혼선
- `SUB/RED` 뒤바뀜
- `PENDING/CONFIRMED` 오판정

즉 extractor는 "한 번에 크게"가 아니라 **"작게 나눠서 단계별로 검증하는 구조"**라고 이해하면 된다.

### 참고. 실제 런타임에서는 shortcut 경로가 추가로 있다

2026-04-16 기준 현재 extractor는 `base_date` stage가 끝난 뒤 아래 순서로 shortcut을 시도한다.

1. 구조 기반 shortcut
   - 표/섹션 구조만으로 주문이 충분히 복구되면 `t_day -> transfer_amount -> settle_class -> order_type` 일부를 생략한다.
   - 흥국생명처럼 `fund_name`이 비어 있고 `fund_code + 금액`만 남는 문서도 이 경로에서 처리할 수 있다.
2. markdown shortcut
   - markdown table이 충분히 균일하면 planned/confirmed 슬롯을 결정론적으로 복원한다.
3. 일반 7-stage 계속 진행
   - shortcut으로 충분하지 않은 경우에만 남은 stage를 LLM으로 끝까지 수행한다.

즉 "항상 stage 1~7을 모두 돈다"라고 이해하면 현재 구현과 어긋난다.

### 3-1. 프롬프트는 어디에 있나

`FundOrderExtractor`는 프롬프트를 코드에 하드코딩하지 않는다.  
아래 YAML 파일에서 읽는다.

- [app/prompts/extraction_prompts.yaml](/Users/bhkim/Documents/codex_prj_sam_asset/app/prompts/extraction_prompts.yaml)

추가로 로컬 standalone 경로에는 아래 보조 매핑이 있다.

- [app/prompts/counterparty_prompt_map.yaml](/Users/bhkim/Documents/codex_prj_sam_asset/app/prompts/counterparty_prompt_map.yaml)

이 파일은 파일명/title 토큰뿐 아니라 `content_match_tokens`로도 prompt 후보를 고를 수 있다.
다만 이 로직은 로컬 helper이며, WAS 이식 시 source of truth는 DB 값이어야 한다.

즉 추출 규칙을 조정할 때는 아래 둘을 구분해서 생각하면 된다.

- 코드 수정이 필요한 경우
  - 집계 규칙
  - coverage 규칙
  - 후처리/정규화 규칙
- 프롬프트 수정이 필요한 경우
  - stage별 지시문
  - 출력 JSON 계약
  - 모델이 자주 혼동하는 문서 읽기 지침

처음에는 "문제가 생기면 무조건 extractor.py를 수정해야 하나?"라고 생각하기 쉬운데,
실제로는 **문제의 절반 이상은 YAML 프롬프트 수정만으로 해결되는 경우가 많다.**

### 3-2. 프롬프트는 4층 구조로 되어 있다

프롬프트 파일은 크게 4부분으로 나뉜다.

#### 1. `system_prompt`

가장 바깥쪽 공통 규칙이다.

이 프롬프트의 역할:

- 이 작업이 "변액일임 주문 추출"이라는 점을 고정
- JSON만 반환하게 강제
- 마크다운, 코드펜스, prose, `<think>` 금지
- 값 추측 금지
- `좌수`를 `transfer_amount`로 쓰지 말라고 강제
- `0`, `-`, blank는 거래가 아니라고 강제

쉽게 말하면:
- **모든 stage가 공통으로 따라야 하는 안전 규칙**이다.

따라서 초보 개발자에게는 보통 이렇게 권장하면 된다.

- stage별 추출 실수가 있으면 먼저 stage prompt를 본다
- JSON 형식 자체가 깨지거나, 값 추측이 심하면 `system_prompt`를 본다

원문 프롬프트 전문:

```text
You extract variable-annuity fund order data from Korean or English instruction documents.
You must follow the requested stage only and never skip steps.
Return exactly one JSON object and nothing else.
Do not output markdown, code fences, prose, XML, or <think> tags.
Never invent funds, dates, amounts, t_day values, settle classes, or order types.
If a value cannot be found, use null or an empty list and add a concise issue code.
Preserve transfer_amount as a numeric string from the document. Keep real decimal amounts when the document actually uses them, but remove spreadsheet float-tail artifacts such as 70,000,000.00000001 by normalizing them to the nearest integer amount.
Units or shares (좌수, units, shares) are not transfer_amount.
Rows whose amount cell is '-', blank, or zero are not transactions and must be ignored.
Document context may contain both a structured markdown view and a raw text backup. Use the raw text backup to resolve ambiguities created by formatting.
Ignore total rows, summary rows, signature blocks, and approval-only pages.
Only include rows or cells with factual evidence in the document.
```

한국어 번역:

```text
당신은 한국어 또는 영어 지시서 문서에서 변액일임 펀드 주문 데이터를 추출한다.
항상 현재 요청된 stage만 수행하고, 단계를 건너뛰지 않는다.
반드시 JSON 객체 하나만 반환하고 그 외의 내용은 출력하지 않는다.
마크다운, 코드 펜스, 설명 문장, XML, <think> 태그를 출력하지 않는다.
펀드, 날짜, 금액, t_day, settle_class, order_type를 추측해서 만들지 않는다.
값을 찾을 수 없으면 null 또는 빈 리스트를 사용하고, 짧은 issue code를 추가한다.
transfer_amount는 문서에 적힌 숫자 문자열을 기준으로 유지한다. 문서에 실제 소수 금액이 있으면 보존하되, 70,000,000.00000001 같은 spreadsheet float-tail artifact는 가장 가까운 정수 금액으로 정규화한다.
좌수나 unit/share 값은 transfer_amount가 아니다.
금액 셀이 '-', 빈 값, 0인 행은 거래가 아니므로 무시해야 한다.
문서 본문에는 구조화된 markdown view와 raw text backup이 함께 들어올 수 있다. 레이아웃이 애매하면 raw text backup으로 확인한다.
총계 행, 요약 행, 서명 블록, 승인 전용 페이지는 무시한다.
문서 안에 사실 근거가 있는 행과 셀만 포함한다.
```

금액 소수 자릿수는 아래처럼 해석한다.

- `23,213.40`처럼 문서/추출 문자열에 2자리 소수가 있으면 `23,213.40`으로 유지한다.
- `23,213.4`처럼 1자리 소수가 있으면 `23,213.4`로 유지한다.
- `23,213.409`처럼 3자리 이상 소수가 있으면 절삭하지 않고 유지한다.
- `18,711,858.00`처럼 소수부가 모두 0이면 정수 `18,711,858`로 정리한다.
- backend는 최종 출력에서 소수 2자리 padding/truncation을 하지 않는다.

핵심 해석:

- 이 프롬프트는 **모든 stage 공통 안전장치**다.
- JSON 형식을 강하게 고정하고, 추측 금지와 non-transaction 무시 규칙을 준다.
- 특히 `좌수는 금액이 아니다`, `0원은 거래가 아니다`가 매우 중요하다.

#### 2. `user_prompt_template`

모든 stage에 공통으로 쓰이는 wrapper다.

역할:

- 현재 stage 번호/이름/목표를 넣는다
- `document_text`를 넣는다
- 이전 stage 결과(`input_items_json`)를 넣는다
- output contract를 같이 보여 준다

즉 이 템플릿은 "이번 요청이 무슨 stage인지"와 "무슨 JSON을 내야 하는지"를
한 번에 조립하는 틀이다.

이 템플릿 덕분에 extractor는 stage마다 프롬프트 문자열을 코드에서 직접 만들 필요가 없다.

원문 프롬프트 전문:

```text
Stage {stage_number}/6: {stage_goal}
Stage name: {stage_name}

Task plan:
1. Read the document carefully.
2. Prefer the structured markdown view for layout, but check the raw text backup when the layout looks flattened or ambiguous.
3. Use the provided input items as the source of truth when they exist.
4. Resolve only the fields required for this stage.
5. Do not emit any field that is not in the schema.
6. If evidence is missing, leave the field null and add an issue code.

Stage instructions:
{stage_instructions}

Output JSON contract:
{output_contract}

Input items JSON:
{input_items_json}

Document text:
{document_text}
```

한국어 번역:

```text
현재 stage {stage_number}/6: {stage_goal}
stage 이름: {stage_name}

작업 계획:
1. 문서를 주의 깊게 읽는다.
2. 레이아웃은 structured markdown view를 우선 참고하되, 표가 납작하게 풀렸거나 애매하면 raw text backup도 확인한다.
3. input items가 있으면 그것을 현재 stage의 기준 입력값으로 사용한다.
4. 이번 stage에서 필요한 필드만 해석한다.
5. 스키마에 없는 필드는 출력하지 않는다.
6. 근거가 없으면 해당 필드는 null로 두고 issue code를 추가한다.

stage 지시문:
{stage_instructions}

출력 JSON 계약:
{output_contract}

입력 items JSON:
{input_items_json}

문서 본문:
{document_text}
```

핵심 해석:

- `user_prompt_template`는 모든 stage의 **공통 껍데기**다.
- stage마다 다른 것은 `{stage_instructions}`와 `{output_contract}`이고,
  나머지는 동일하다.
- 즉 “이번 stage가 무엇을 받아 무엇을 출력해야 하는가”를 일정한 형식으로 유지해 준다.

#### 3. `stage.instructions`

실제 도메인 규칙은 대부분 여기 있다.

예:

- `fund_inventory`
  - 삼성 운용사 필터를 어떻게 보나
- `base_date`
  - 기준일이 여러 개면 무엇을 우선하나
- `t_day`
  - 어떤 컬럼을 slot으로 보나
- `transfer_amount`
  - net execution 컬럼과 explicit inflow/outflow 중 무엇을 우선하나

즉 **문서 해석 규칙의 대부분은 stage.instructions에 들어 있다.**

#### 4. `stage.output_contract`

각 stage가 반드시 지켜야 하는 JSON 스키마다.

예:

- `fund_inventory`
  - `fund_code`, `fund_name`
- `t_day`
  - `slot_id`, `t_day`, `evidence_label`
- `order_type`
  - `order_type`

이 스키마가 좁을수록 stage가 안정적이다.

왜냐하면:

- stage 1에서는 펀드 목록만 찾으면 되고
- stage 4에서는 금액만 찾으면 되며
- stage 6에서는 방향만 결정하면 되기 때문이다

즉 output contract는 **"이번 단계에서 무엇만 답해야 하는가"를 제한하는 장치**다.

### 3-3. stage별 프롬프트 역할을 자세히 보면

아래는 초보 개발자가 가장 많이 헷갈리는 부분을 기준으로 정리한 설명이다.

#### Stage 1. `fund_inventory`

역할:
- 문서에 등장하는 펀드 목록을 전수 수집

중요한 규칙:
- `fund_code`, `fund_name` 쌍을 distinct하게 모은다
- 문서에 운용사 컬럼이 있으면 `삼성` 운용사만 남긴다
- manager 정보가 없으면 모든 펀드를 포함한다
- totals, blank rows, metadata rows는 제외한다

왜 따로 빼나:
- 이후 stage는 이 inventory를 기준으로만 움직인다
- stage 1이 흔들리면 뒤 단계는 아무리 잘해도 누락이 생긴다

이 stage가 틀릴 때 보통 보이는 현상:
- `FUND_DISCOVERY_EMPTY`
- 삼성 외 운용사 펀드가 섞여 나옴
- 펀드코드 trailing digit 누락

원문 프롬프트 전문:

```text
Stage name: fund_inventory
Goal: Extract all distinct fund_code and fund_name pairs from the document.

Instructions:
- Extract every distinct fund shown in the document.
- If the document has an 운용사, 운용사명, or manager column, include only funds whose manager value contains the keyword 삼성.
- If the document has no manager information, include all distinct funds.
- Use exact fund codes and names from the source text.
- If multiple fund-code columns exist, keep the full code from the dedicated fund-code cell and never shorten trailing digits.
- Ignore totals, blank rows, and non-fund metadata rows.
- Do not infer or translate missing codes or names.

Output contract:
{"items":[{"fund_code":"string","fund_name":"string"}],"issues":["ISSUE_CODE"]}
```

한국어 번역:

```text
stage 이름: fund_inventory
목표: 문서에서 서로 다른 fund_code와 fund_name 쌍을 모두 추출한다.

지시문:
- 문서에 나타나는 모든 고유 펀드를 추출한다.
- 문서에 운용사, 운용사명, manager 컬럼이 있으면 운용사 값에 삼성이라는 키워드가 들어 있는 펀드만 포함한다.
- manager 정보가 없으면 모든 고유 펀드를 포함한다.
- fund code와 fund name은 원문 그대로 사용한다.
- fund-code 컬럼이 여러 개 있으면 전용 fund-code 셀의 전체 코드를 유지하고, 뒤 숫자를 잘라내지 않는다.
- 총계, 빈 행, 펀드가 아닌 메타데이터 행은 무시한다.
- 누락된 코드나 이름을 추론하거나 번역하지 않는다.

출력 계약:
items 안에 fund_code, fund_name만 담고 issues 배열을 함께 반환한다.
```

핵심 해석:

- 이 stage는 **펀드 목록을 좁히는 단계**다.
- 이후 stage는 이 inventory에만 의존하므로, 여기서 빠진 펀드는 뒤에서 복구하기 어렵다.

#### Stage 2. `base_date`

역할:
- 각 펀드의 기준일을 찾는다

중요한 규칙:
- `기준일`, `기준일자`, `T일`, `결제일`, `settlement date`, 문서 날짜를 사용
- schedule 표에 여러 날짜가 있으면 `T일`과 정렬된 날짜를 우선
- 결과는 반드시 `YYYY-MM-DD`

왜 따로 빼나:
- 기준일은 문서 전체에서 한 번에 찾는 게 아니라,
  펀드별 문맥과 함께 봐야 안정적이기 때문이다

이 stage가 틀릴 때 보통 보이는 현상:
- `BASE_DATE_MISSING`
- sheet 제목 날짜와 실제 거래 기준일이 뒤바뀜

원문 프롬프트 전문:

```text
Stage name: base_date
Goal: Resolve base_date for each previously extracted fund.

Instructions:
- For each input fund, resolve the document base date.
- Use 기준일, 기준일자, T일, 결제일, settlement date, or document date when it governs the fund rows.
- If a schedule table shows both a sheet date and a date aligned to the T일 transaction columns, prefer the date aligned to T일 or the transaction grid.
- Return YYYY-MM-DD.
- If not found, set base_date to null and add BASE_DATE_MISSING.

Output contract:
{"items":[{"fund_code":"string","fund_name":"string","base_date":"YYYY-MM-DD or null"}],"issues":["ISSUE_CODE"]}
```

한국어 번역:

```text
stage 이름: base_date
목표: 앞 단계에서 추출한 각 펀드의 기준일을 찾는다.

지시문:
- 각 입력 펀드에 대해 문서 기준일을 찾는다.
- 펀드 행을 지배하는 기준일, 기준일자, T일, 결제일, settlement date, 문서 날짜를 사용한다.
- schedule 표에 시트 날짜와 T일 거래 컬럼에 정렬된 날짜가 함께 있으면, T일 또는 거래 그리드에 맞춘 날짜를 우선한다.
- 결과는 YYYY-MM-DD 형식으로 반환한다.
- 찾지 못하면 base_date를 null로 두고 BASE_DATE_MISSING을 추가한다.
```

핵심 해석:

- 같은 문서 안에 여러 날짜가 있을 수 있으므로,
  단순히 “제일 먼저 보이는 날짜”를 쓰라는 프롬프트가 아니다.
- 거래 컬럼과 실제로 정렬된 날짜를 우선하라는 점이 핵심이다.

#### Stage 3. `t_day`

역할:
- 각 펀드에서 실제 거래 슬롯을 찾는다

이 stage에서 만드는 핵심 필드:
- `t_day`
- `slot_id`
- `evidence_label`

가장 중요한 이유:
- extractor는 이후 모든 단계를 `slot` 단위로 진행한다
- 즉 여기서 slot이 잘못 잡히면 금액/상태/방향이 모두 꼬인다

중요한 규칙:
- non-zero monetary cell이 있는 bucket만 slot으로 만든다
- `설정/해지`, `입금/출금`, `Buy/Sell`이 같은 bucket에 모두 있더라도
  일단 slot은 bucket 1개로 잡는다
- `evidence_label`은 실제 컬럼 이름 또는 cue를 그대로 보관한다
- `익영업일`, `익익영업일`, `제3영업일`은 `T+1`, `T+2`, `T+3`로 본다
- `좌수` 컬럼은 slot 근거가 아니다

초보 개발자가 기억할 점:
- `evidence_label`은 최종 출력 필드가 아니다
- 하지만 뒤 stage들이 이 값을 보고 판단하므로 매우 중요하다

원문 프롬프트 전문:

```text
Stage name: t_day
Goal: Enumerate distinct transaction slots for each fund and assign t_day values.

Instructions:
- For each input fund, enumerate every transaction slot that has a non-empty and non-zero monetary cell.
- A slot is one settlement bucket per fund and per 결제일(base_date or t_day).
- If the same fund and same bucket contain both inflow and outflow columns, emit one slot for that bucket instead of separate SUB and RED slots.
- Include evidence_label as the exact source column or cue, such as 설정금액/해지금액, 입금액/출금액, 결제금액, 당일이체금액, 이체예정금액, 2025-11-28, T+1 투입금액/T+1 인출금액, or Redemption.
- Korean business-day schedule headers such as 익영업일, 익익영업일, 익익익영업일, or 제3영업일 mean future buckets and should be treated as T+1, T+2, T+3 in left-to-right order.
- Determine the primary monetary schema for the row or table before emitting slots.
- Do not hard-code that explicit order columns always win over execution columns, or vice versa.
- Choose only the columns that represent the actual order amounts for that row, and ignore auxiliary monetary columns.
- If a row has 결제금액, 순유입금액, 순투입금액, 당일이체금액, 실행금액, or 이체예정금액 for that bucket, that bucket still gets only one slot.
- If the same fund appears in multiple sections but one occurrence has '-' or blank amount and another occurrence has a numeric amount, keep only the amount-bearing occurrence.
- For sectioned documents such as Subscription / Redemption lists, evidence_label must come from the section that contains the numeric amount for that fund.
- When grouped headers define T일 and T+N buckets, keep each amount in the bucket shown directly above that column. Do not slide a value into an adjacent T+N bucket.
- Never use columns whose header contains 좌수, unit, shares, 누적좌수, 전일좌수, 변경 후 누적좌수, 순투입좌수, 증감좌수.
- Ignore 합계, 총계, summary, total, subtotal, and row labels that are not individual fund rows.
- Use t_day=0 for 당일, 확정, 실행, 당일이체, 당일투입, 당일인출.
- Use t_day=1..N for 예정, 청구, 예상, T+N columns strictly by visual column order. Do not calculate date differences.
- If the document has no 예정, 청구, 예상, T+N, 미래일자별 컬럼 and only document/base/NAV date exists, treat the slot as t_day=0.
- slot_id must be stable within the batch, such as T0_NET, T1_NET, or T2_NET.
- Add T_DAY_MISSING only when the document clearly implies a schedule bucket but you still cannot map the slot.

Output contract:
{"items":[{"fund_code":"string","fund_name":"string","base_date":"YYYY-MM-DD or null","t_day":0,"slot_id":"T0_1","evidence_label":"source column or cue"}],"issues":["ISSUE_CODE"]}
```

한국어 번역:

```text
stage 이름: t_day
목표: 각 펀드별 거래 슬롯을 나열하고 t_day 값을 정한다.

지시문:
- 비어 있지 않고 0이 아닌 금액 셀이 있는 거래 슬롯만 찾는다.
- 슬롯은 펀드별, 결제 버킷별 1건이다.
- 같은 펀드와 같은 버킷에 `결제금액`, `순유입금액`, `순투입금액`, `정산액`, `당일이체금액`, `실행금액`, `이체예정금액` 같은 settlement/net 컬럼이 있으면 slot은 1건만 만든다.
- 같은 펀드와 같은 버킷에 `설정금액/해지금액`, `입금액/출금액`, `투입금액/인출금액`, `Buy/Sell`만 있고 settlement/net 컬럼이 없으면 `SUB`와 `RED` slot을 분리해서 만든다.
- evidence_label에는 실제 근거 컬럼명 또는 cue를 그대로 넣는다.
- 익영업일, 익익영업일, 제3영업일은 왼쪽에서 오른쪽 순서대로 T+1, T+2, T+3으로 본다.
- 행이나 표의 주 금액 체계를 먼저 판단한 뒤 slot을 만든다.
- explicit order 컬럼과 execution 컬럼 중 하나를 무조건 우선한다고 가정하지 않는다.
- 실제 주문 금액을 뜻하는 컬럼만 선택하고 보조 금액 컬럼은 무시한다.
- 같은 버킷에 결제금액, 순유입금액, 순투입금액, 당일이체금액, 실행금액, 이체예정금액이 있으면 그 버킷은 slot 1건만 가진다.
- 같은 펀드가 여러 section에 나타나도 숫자 금액이 있는 occurrence만 유지한다.
- Subscription / Redemption 같은 sectioned document에서는 실제 숫자 금액이 있는 section에서 evidence_label을 가져온다.
- T일과 T+N grouped header가 있으면 각 금액을 바로 위 버킷에만 연결하고 옆 버킷으로 밀지 않는다.
- 좌수 관련 컬럼은 절대 사용하지 않는다.
- 합계/총계/summary/subtotal은 무시한다.
- 당일/확정/실행/당일이체/당일투입/당일인출은 t_day=0으로 본다.
- 예정/청구/예상/T+N은 시각적 컬럼 순서만 보고 t_day=1..N으로 본다.
- 문서에 미래 버킷이 없고 문서/base/NAV 날짜만 있으면 t_day=0으로 본다.
- slot_id는 T0_NET, T1_NET처럼 안정적으로 만든다.
- 문서가 분명 schedule 버킷을 뜻하는데도 매핑하지 못했을 때만 T_DAY_MISSING을 추가한다.
```

핵심 해석:

- 이 stage는 단순히 `t_day`만 정하는 단계가 아니다.
- 실제로는 **slot 정의**를 만드는 단계이며, `evidence_label`을 함께 고정하는 것이 핵심이다.

#### Stage 4. `transfer_amount`

역할:
- 각 slot의 실제 금액을 결정한다

이 stage가 특히 중요한 이유:
- 실제 실문서 이슈의 많은 비율이 금액 오추출이기 때문이다

프롬프트 핵심 규칙:
- `결제금액`, `순유입금액`, `순투입금액`, `정산액`, `당일이체금액`, `실행금액`, `이체예정금액`이 있으면 그것을 우선 사용
- slot이 explicit inflow 컬럼(`설정금액`, `입금액`, `투입금액`, `Buy`, `Subscription`)에서 왔으면 그 컬럼 금액을 양수로 사용
- slot이 explicit outflow 컬럼(`해지금액`, `출금액`, `인출금액`, `Sell`, `Redemption`)에서 왔으면 그 컬럼 금액을 음수로 사용
- settlement/net 컬럼과 explicit 컬럼이 함께 있으면 settlement/net 컬럼을 우선
- `좌수`는 금액이 아니다

즉 이 stage는 단순히 "숫자를 읽는" 단계가 아니라,
**어떤 숫자가 실제 주문금액인가를 선택하는 단계**다.

실무 메모:
- `동양생명_20260318.html`의 `정산내역`처럼 `설정액`, `해지액`, `정산액`이 함께 있는 문서는 `정산액`을 transfer_amount로 사용한다.
- 반대로 settlement/net 컬럼이 없고 explicit inflow/outflow만 있으면 같은 펀드/같은 버킷에서도 `SUB`와 `RED`가 별도 slot로 유지된다.

원문 프롬프트 전문:

```text
Stage name: transfer_amount
Goal: Resolve transfer_amount for each previously identified transaction slot.

Instructions:
- For each input slot, use evidence_label to locate the correct monetary cells for that settlement bucket.
- If the bucket has 결제금액, 순유입금액, 순투입금액, 정산액, 당일이체금액, 실행금액, or 이체예정금액, use that settlement amount as transfer_amount.
- If the slot comes from an explicit inflow column such as 설정금액, 입금액, 투입금액, Buy, or Subscription, use that exact column amount as a positive transfer_amount.
- If the slot comes from an explicit outflow column such as 해지금액, 출금액, 인출금액, Sell, or Redemption, use that exact column amount as a negative transfer_amount.
- For a single-direction bucket with only redemption, withdrawal, or outflow evidence, return a negative transfer_amount.
- For a single-direction bucket with only subscription, deposit, or inflow evidence, return a positive transfer_amount.
- Do not use units, shares, 좌수, or cumulative totals.
- When both separate inflow/outflow columns and a settlement amount column exist in the same bucket, prefer the settlement amount column and ignore the explicit columns for that bucket.
- If the same fund appears in both Subscription and Redemption sections but only one section has a numeric amount and the other shows '-', use only the numeric amount section.
- If evidence_label is a date or T+N label, use the amount under that exact scheduled column.
- Evidence labels such as 익영업일(T+1), 익익영업일(T+2), or 제3영업일(T+3) are scheduled buckets. Use the amount directly under that bucket.
- Do not replace missing values with 0.
- If a slot has no verifiable amount, set transfer_amount to null and add TRANSFER_AMOUNT_MISSING.

Output contract:
{"items":[{"fund_code":"string","fund_name":"string","base_date":"YYYY-MM-DD or null","t_day":0,"slot_id":"T0_1","evidence_label":"source column or cue","transfer_amount":"numeric string or null"}],"issues":["ISSUE_CODE"]}
```

한국어 번역:

```text
stage 이름: transfer_amount
목표: 앞 단계에서 찾은 각 거래 슬롯의 transfer_amount를 결정한다.

지시문:
- 각 slot에 대해 evidence_label을 사용해서 해당 결제 버킷의 올바른 금액 셀을 찾는다.
- 버킷에 결제금액, 순유입금액, 순투입금액, 정산액, 당일이체금액, 실행금액, 이체예정금액이 있으면 그 settlement amount를 transfer_amount로 사용한다.
- slot이 설정금액, 입금액, 투입금액, Buy, Subscription 같은 explicit inflow 컬럼에서 왔으면 그 컬럼 금액을 양수로 사용한다.
- slot이 해지금액, 출금액, 인출금액, Sell, Redemption 같은 explicit outflow 컬럼에서 왔으면 그 컬럼 금액을 음수로 사용한다.
- redemption/withdrawal/outflow만 있는 버킷은 음수로 반환한다.
- subscription/deposit/inflow만 있는 버킷은 양수로 반환한다.
- 좌수나 누적 합계는 사용하지 않는다.
- 같은 버킷에 분리 inflow/outflow 컬럼과 settlement amount 컬럼이 함께 있으면 settlement amount를 우선하고 explicit 컬럼은 무시한다.
- 같은 펀드가 Subscription/Redemption 두 section에 모두 있어도 숫자 금액이 있는 section만 사용한다.
- evidence_label이 날짜나 T+N 라벨이면 그 정확한 scheduled 컬럼 아래 금액을 사용한다.
- 익영업일(T+1), 익익영업일(T+2), 제3영업일(T+3) 같은 label은 scheduled bucket이므로 그 버킷 바로 아래 금액을 사용한다.
- 누락값을 0으로 바꾸지 않는다.
- 검증 가능한 금액이 없으면 transfer_amount를 null로 두고 TRANSFER_AMOUNT_MISSING을 추가한다.
```

핵심 해석:

- 이 stage의 본질은 **금액 선택 규칙**이다.
- 숫자를 읽는 것보다 “어느 숫자를 최종 주문 금액으로 써야 하는가”를 정하는 단계라고 보면 된다.

#### Stage 5. `settle_class`

역할:
- 각 slot이 `CONFIRMED`인지 `PENDING`인지 판단한다

핵심 규칙:
- `당일`, `확정`, `실행`, `당일이체`는 `CONFIRMED`
- `예정`, `청구`, `예상`, `T+N`은 `PENDING`
- `익영업일(T+1)` 같은 business-day schedule도 `PENDING`
- `설정금액`, `해지금액`, `입금액`, `출금액`, `Buy`, `Sell`은 보통 `CONFIRMED`
- 단, 문서가 명시적으로 예정/청구라고 하면 `PENDING`

중요:
- 여기서의 값은 아직 내부 enum 문자열이다
- 최종 출력 코드 `"1"`, `"2"`는 나중 단계에서 바뀐다

원문 프롬프트 전문:

```text
Stage name: settle_class
Goal: Resolve CONFIRMED or PENDING for each transaction slot.

Instructions:
- Use evidence_label and t_day together.
- Use only CONFIRMED or PENDING.
- 당일, 확정, 실행, 당일이체, 당일투입, 당일인출 map to CONFIRMED.
- 예정, 청구, 예상, T+N map to PENDING.
- Korean business-day headers such as 익영업일(T+1), 익익영업일(T+2), 익익익영업일(T+3), and 제N영업일(T+N) map to PENDING.
- Explicit order columns such as 설정금액, 해지금액, 입금액, 출금액, Buy, and Sell are normally CONFIRMED unless the document clearly marks them as 예정 or 청구.
- If the document has 결제일, 전좌수/후좌수, 실행금액, or other processed-result fields, classify as CONFIRMED.
- If the document has no 예정, 청구, 예상, T+N, 결제 예정일, 예정 금액, or 미래일자별 컬럼 and only document/base/NAV date exists, classify as CONFIRMED.
- Section labels such as Subscription and Redemption alone do not make a slot PENDING.
- If the same document contains both 당일 처리분 and separate 미래 예정분 columns, use CONFIRMED for the same-day slots and PENDING for the future slots.
- Use document labels first. If they are absent but t_day=0, you may set CONFIRMED. If t_day>=1, you may set PENDING.
- If still uncertain, set settle_class to null and add SETTLE_CLASS_MISSING.

Output contract:
{"items":[{"fund_code":"string","fund_name":"string","base_date":"YYYY-MM-DD or null","t_day":0,"slot_id":"T0_1","evidence_label":"source column or cue","transfer_amount":"numeric string or null","settle_class":"CONFIRMED|PENDING|null"}],"issues":["ISSUE_CODE"]}
```

한국어 번역:

```text
stage 이름: settle_class
목표: 각 거래 슬롯이 CONFIRMED인지 PENDING인지 판단한다.

지시문:
- evidence_label과 t_day를 함께 사용한다.
- 값은 반드시 CONFIRMED 또는 PENDING만 쓴다.
- 당일, 확정, 실행, 당일이체, 당일투입, 당일인출은 CONFIRMED다.
- 예정, 청구, 예상, T+N은 PENDING이다.
- 익영업일(T+1), 익익영업일(T+2), 익익익영업일(T+3), 제N영업일(T+N)은 PENDING이다.
- 설정금액, 해지금액, 입금액, 출금액, Buy, Sell 같은 explicit order 컬럼은 문서가 명시적으로 예정/청구라고 하지 않는 한 보통 CONFIRMED다.
- 문서에 결제일, 전좌수/후좌수, 실행금액 같은 처리 완료 필드가 있으면 CONFIRMED로 본다.
- 미래 버킷 관련 컬럼이 없고 문서/base/NAV 날짜만 있으면 CONFIRMED로 본다.
- Subscription/Redemption section 이름만으로는 PENDING이 되지 않는다.
- 같은 문서에 당일 처리분과 미래 예정분이 함께 있으면, 당일 slot은 CONFIRMED, 미래 slot은 PENDING으로 나눈다.
- 문서 라벨을 우선 사용하고, 라벨이 없으면 t_day=0은 CONFIRMED, t_day>=1은 PENDING으로 볼 수 있다.
- 그래도 불확실하면 settle_class를 null로 두고 SETTLE_CLASS_MISSING을 추가한다.
```

핵심 해석:

- 이 stage는 단순히 `t_day`를 다시 읽는 단계가 아니다.
- **문서 라벨과 처리 상태 힌트를 해석해서 거래 상태를 분류하는 단계**다.

#### Stage 6. `order_type`

역할:
- 최종 방향을 `SUB` 또는 `RED`로 확정한다

핵심 규칙:
- 금액에 부호가 이미 있으면 부호를 우선 사용
  - 음수 = `RED`
  - 양수 = `SUB`
- 그렇지 않으면 라벨을 본다
  - `투입`, `설정`, `입금`, `Buy`, `Subscription` = `SUB`
  - `인출`, `해지`, `출금`, `Sell`, `Redemption` = `RED`
- amount가 `-`, blank, zero인 section은 무시한다

왜 마지막에 두나:
- 방향은 금액과 section 문맥을 둘 다 봐야 안정적이기 때문이다
- 즉 `transfer_amount`가 먼저 확정되어 있어야 부호를 활용할 수 있다

원문 프롬프트 전문:

```text
Stage name: order_type
Goal: Resolve SUB or RED for each transaction slot.

Instructions:
- Use evidence_label, transfer_amount, and the document headers together.
- Use only SUB or RED.
- This stage resolves the raw direction of the source amount so the final aggregation can build one signed transfer_amount per settlement bucket.
- If transfer_amount already has a sign, use the sign first: negative=RED, positive=SUB.
- Ignore section rows whose amount is '-', blank, or zero. Determine SUB or RED from the section or column that contains the numeric amount.
- If the same fund appears in both Subscription and Redemption sections but only one section has a numeric amount, use that amount-bearing section and ignore the other section.
- Otherwise use source labels: 투입, 설정, 입금, 매입, Buy, Subscription map to SUB; 인출, 해지, 출금, 환매, Sell, Redemption map to RED.
- If a table has 구분 or Transaction Type and the value is Subscription, use SUB. If Redemption, use RED.
- If still uncertain, set order_type to null and add ORDER_TYPE_MISSING.

Output contract:
{"items":[{"fund_code":"string","fund_name":"string","base_date":"YYYY-MM-DD or null","t_day":0,"slot_id":"T0_1","evidence_label":"source column or cue","transfer_amount":"numeric string or null","settle_class":"CONFIRMED|PENDING|null","order_type":"SUB|RED|null"}],"issues":["ISSUE_CODE"]}
```

한국어 번역:

```text
stage 이름: order_type
목표: 각 거래 슬롯의 방향을 SUB 또는 RED로 결정한다.

지시문:
- evidence_label, transfer_amount, 문서 헤더를 함께 사용한다.
- 값은 반드시 SUB 또는 RED만 쓴다.
- 이 stage는 source amount의 원래 방향을 결정해서, 최종 집계가 settlement bucket별 signed transfer_amount를 만들 수 있게 한다.
- transfer_amount에 부호가 이미 있으면 부호를 우선 사용한다. 음수면 RED, 양수면 SUB다.
- 금액이 '-', blank, zero인 section row는 무시하고, 실제 숫자 금액이 있는 section 또는 컬럼에서 SUB/RED를 결정한다.
- 같은 펀드가 Subscription과 Redemption 두 section에 모두 나타나도 숫자 금액이 있는 section만 사용한다.
- 그 외에는 source label을 사용한다. 투입/설정/입금/매입/Buy/Subscription은 SUB, 인출/해지/출금/환매/Sell/Redemption은 RED다.
- 표에 구분 또는 Transaction Type이 있고 값이 Subscription이면 SUB, Redemption이면 RED다.
- 그래도 불확실하면 order_type을 null로 두고 ORDER_TYPE_MISSING을 추가한다.
```

핵심 해석:

- 이 stage는 금액 부호와 문서 문맥을 최종적으로 결합해 방향을 확정하는 단계다.
- 그래서 항상 마지막 stage로 두는 편이 안정적이다.

### 3-4. 프롬프트를 수정할 때 어디부터 손대야 하나

초보 개발자에게는 아래 순서를 권장한다.

#### 1. 가장 먼저 stage.instructions를 본다

대부분의 문서 해석 오류는 여기서 조정된다.

예:
- 특정 양식에서 `증감금액`을 더 강하게 우선해야 함
- `기준일`보다 `T일` aligned date를 더 명시해야 함
- `Buy/Sell Report`에서 특정 section 문구를 강조해야 함

#### 2. 그 다음 stage.output_contract를 본다

모델이 불필요한 필드를 계속 섞어 내거나,
이번 단계에서 받으면 안 되는 값을 같이 내면 여기서 막아야 한다.

예:
- stage 3인데 `order_type`까지 내고 있음
- stage 4인데 `settle_class`까지 같이 내려 함

#### 3. `system_prompt`는 마지막에 건드린다

`system_prompt`는 모든 stage에 동시에 영향을 준다.
그래서 여기 수정은 범위가 크고, 회귀 영향도 크다.

즉 보통은:

- stage별 문제 → stage prompt 수정
- 전역 형식 문제 → system prompt 수정

순서가 안전하다.

### 3-5. 프롬프트만으로 해결되지 않는 경우

아래는 코드 수정이 더 맞는 경우다.

- coverage mismatch
- explicit/net 집계 규칙 충돌
- `only_pending` 적용 위치 문제
- 최종 코드 치환 계약 문제
- invalid artifact 저장/예외 전달 문제

즉
- 문서 읽기/의미 해석의 문제면 프롬프트
- 집계/검증/출력 계약의 문제면 코드

로 나누어 보면 된다.

## 4. 다른 서버로 옮길 때 어떤 파일이 필요한가

`FundOrderExtractor`를 다른 서버에서 직접 쓰려면 아래 파일들이 필요하다.

- [app/extractor.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extractor.py)
- [app/schemas.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/schemas.py)
- [app/config.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/config.py)
- [app/prompts/extraction_prompts.yaml](/Users/bhkim/Documents/codex_prj_sam_asset/app/prompts/extraction_prompts.yaml)

그리고 보통은 아래 파일도 같이 필요하다.

- [app/document_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loader.py)
  - `DocumentLoadTaskPayload`
  - `TargetFundScope`
- [app/output_contract.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/output_contract.py)
  - 최종 외부 출력 직렬화

실무적으로는 extractor만 단독으로 떼기보다, 아래 묶음을 같이 가져가는 편이 훨씬 안전하다.

```text
app/
  config.py
  document_loader.py
  extractor.py
  output_contract.py
  schemas.py
  prompts/
    extraction_prompts.yaml
```

## 5. 필요한 Python 패키지

`FundOrderExtractor`가 직접 사용하는 주요 패키지는 아래와 같다.

- `langchain-openai`
- `pydantic`
- `PyYAML`

즉 최소 예시는 아래처럼 설치하면 된다.

```bash
pip install langchain-openai pydantic PyYAML
```

주의:

- extractor는 LLM endpoint에 직접 연결한다.
- 따라서 `DocumentLoader`와 달리, **LLM 서버와 API 설정이 반드시 필요하다.**

## 6. 필요한 환경변수

`FundOrderExtractor`는 보통 [app/config.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/config.py)의 `get_settings()`로 설정을 받는다.

최소로 중요한 값은 아래다.

- `LLM_BASE_URL`
- `LLM_API_KEY`
- `LLM_MODEL`

자주 같이 보는 값:

- `LLM_TIMEOUT_SECONDS`
- `LLM_RETRY_ATTEMPTS`
- `LLM_RETRY_BACKOFF_SECONDS`
- `LLM_STAGE_BATCH_SIZE`
- `LLM_CHUNK_SIZE_CHARS`

예:

```env
LLM_BASE_URL=http://localhost:3910/v1
LLM_API_KEY=your-key
LLM_MODEL=qwen3-next-80B-A3B-instruct
LLM_TIMEOUT_SECONDS=120
LLM_RETRY_ATTEMPTS=3
LLM_RETRY_BACKOFF_SECONDS=1.5
LLM_STAGE_BATCH_SIZE=12
LLM_CHUNK_SIZE_CHARS=12000
```

## 7. 가장 많이 쓰는 호출 방법 2가지

실제로는 아래 두 방식이 가장 많다.

### 7-1. `extract_from_task_payload()`

이 방식이 가장 추천된다.  
왜냐하면 `DocumentLoader`와 자연스럽게 이어지고, queue 기반 WAS 구조와도 가장 잘 맞기 때문이다.

```python
from pathlib import Path

from app.config import get_settings
from app.document_loader import DocumentLoader
from app.extractor import FundOrderExtractor

settings = get_settings()
loader = DocumentLoader()
extractor = FundOrderExtractor(settings)

task_payload = loader.build_task_payload(
    Path("/data/orders/sample.xlsx"),
    chunk_size_chars=settings.llm_chunk_size_chars,
)

outcome = extractor.extract_from_task_payload(task_payload)
print(outcome.result.orders)
```

이 메서드가 하는 일:

- 비지시서 문서면 즉시 예외
- empty 정상 문서면 0건 결과 반환
- 실제 staged extraction 수행
- coverage mismatch 반영
- blocking issue 검사

즉 **handler B의 표준 진입점**으로 보면 된다.

### 7-2. `extract()`

이미 다른 시스템에서 `chunks`, `raw_text`, `scope`를 준비해 둔 경우에는 `extract()`를 바로 쓸 수도 있다.

```python
from app.config import get_settings
from app.extractor import FundOrderExtractor

settings = get_settings()
extractor = FundOrderExtractor(settings)

outcome = extractor.extract(
    chunks=["chunk 1 text", "chunk 2 text"],
    raw_text="full raw text",
    target_fund_scope=None,
)
```

이 방식은 가능하지만, 초보 개발자에게는 보통 추천하지 않는다.

이유:

- `DocumentLoader`가 만든 chunk/scope 규칙을 같이 맞춰야 한다.
- coverage와 extractor의 전제가 어긋나면 검수하기가 어려워진다.

즉 특별한 이유가 없다면 `extract_from_task_payload()`를 먼저 쓰는 편이 안전하다.

## 8. 반환값은 어떤 구조인가

`FundOrderExtractor`는 `LLMExtractionOutcome`를 반환한다.

핵심 필드는 아래 두 개다.

- `outcome.result`
  - 최종 `ExtractionResult`
- `outcome.invalid_response_artifacts`
  - JSON 파싱에 실패한 stage raw response 목록

예:

```python
outcome = extractor.extract_from_task_payload(task_payload)

print(outcome.result.orders)
print(outcome.result.issues)
print(outcome.invalid_response_artifacts)
```

### 8-1. `ExtractionResult`

`ExtractionResult` 안에는 아래가 있다.

- `orders`
- `issues`

`orders`의 각 항목은 `OrderExtraction`이다.

중요:

이 시점의 값은 **내부 표현**이다.

- `settle_class`
  - `PENDING`
  - `CONFIRMED`
- `order_type`
  - `SUB`
  - `RED`

즉 아직 최종 외부 출력 코드 `"1"`, `"2"`, `"3"`가 아니다.

## 9. `only_pending`은 어디에서 적용해야 하나

이 부분은 처음 보는 개발자가 가장 자주 헷갈린다.

정답은:

- `only_pending`은 **extractor 내부 stage 로직에 넣지 않는다**
- **최종 결과 직전 후처리**로 적용한다

현재 프로젝트에서는 [app/extractor.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extractor.py)의
`apply_only_pending_filter()`를 사용한다.

예:

```python
from app.extractor import apply_only_pending_filter

outcome = extractor.extract_from_task_payload(task_payload)
result = apply_only_pending_filter(outcome.result, only_pending=True)
```

규칙은 아래와 같다.

- `only_pending=False`
  - 변경 없음
- `only_pending=True`
  1. 기존 `settle_class == PENDING` 주문 제거
  2. 남은 주문의 `settle_class`를 모두 `PENDING`으로 변경

즉 이름만 보면 "pending만 남긴다"처럼 보이지만,
실제 동작은 **기존 pending을 제거하고 남은 주문을 pending 상태로 바꾸는 계약용 후처리**다.

## 10. 최종 출력 코드 치환은 어디에서 하나

이것도 extractor 내부가 아니다.

현재 프로젝트 기준 최종 외부 출력 계약은 아래다.

- `settle_class`
  - `PENDING -> "1"`
  - `CONFIRMED -> "2"`
- `order_type`
  - `RED -> "1"`
  - `SUB -> "3"`

이 치환은 [app/output_contract.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/output_contract.py)에서 한다.

예:

```python
from app.output_contract import serialize_order_payload

serialized_orders = [serialize_order_payload(order) for order in result.orders]
```

즉 최종 순서는 항상 아래처럼 생각하면 된다.

1. `DocumentLoader`
2. `FundOrderExtractor`
3. `apply_only_pending_filter()`
4. `serialize_order_payload()`

## 11. queue 기반 WAS에서 쓰는 가장 현실적인 예제

이 구조가 실제 운영에 가장 가깝다.

```python
from pathlib import Path

from app.config import get_settings
from app.document_loader import DocumentLoadTaskPayload
from app.extractor import (
    ExtractionOutcomeError,
    FundOrderExtractor,
    apply_only_pending_filter,
    write_invalid_response_debug_files,
)
from app.output_contract import serialize_order_payload

settings = get_settings()
extractor = FundOrderExtractor(settings)

task_payload = DocumentLoadTaskPayload.read_json(
    Path("/jobs/20260320/loader_result.json")
)
# WAS에서는 로컬 YAML helper가 아니라 DB row의 prompt/use_counterparty_prompt를
# 기준으로 guidance 문자열을 준비해 넘긴다.
db_counterparty_prompt: str | None = None
counterparty_guidance = db_counterparty_prompt or None

try:
    outcome = extractor.extract_from_task_payload(
        task_payload,
        counterparty_guidance=counterparty_guidance,
    )
    result = apply_only_pending_filter(outcome.result, only_pending=False)
except ExtractionOutcomeError as exc:
    write_invalid_response_debug_files(
        debug_output_dir=settings.debug_output_dir,
        source_name=task_payload.file_name,
        artifacts=exc.invalid_response_artifacts,
    )
    raise

payload = {
    "file_name": task_payload.file_name,
    "source_path": task_payload.source_path,
    "model_name": settings.llm_model,
    "base_date": next((order.base_date for order in result.orders if order.base_date), None),
    "issues": result.issues,
    "orders": [serialize_order_payload(order) for order in result.orders],
}
```

이 예시가 중요한 이유:

- queue handler B에서 실제로 필요한 예외 처리까지 포함
- WAS DB source-of-truth guidance 주입 위치까지 포함
- invalid raw response artifact 저장 포함
- `only_pending` 위치와 출력 계약 위치가 분리되어 있음

## 12. 실패했을 때 무엇을 먼저 봐야 하나

보통 아래 순서로 보면 된다.

### 12-1. `issues`

가장 먼저 `outcome.result.issues`를 본다.

대표 예:

- `FUND_DISCOVERY_EMPTY`
- `TRANSFER_AMOUNT_EMPTY`
- `ORDER_COVERAGE_MISMATCH`
- `LLM_INVALID_RESPONSE_FORMAT`

### 12-2. invalid response artifact

`LLM_INVALID_RESPONSE_FORMAT`이 보이면,
`invalid_response_artifacts`를 파일로 저장해서 원문 응답을 본다.

현재 프로젝트에서는:

- [app/extractor.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extractor.py)
  - `write_invalid_response_debug_files()`

를 그대로 쓰면 된다.

### 12-3. prompt YAML

프롬프트 구조가 바뀌었으면 아래 파일을 본다.

- [app/prompts/extraction_prompts.yaml](/Users/bhkim/Documents/codex_prj_sam_asset/app/prompts/extraction_prompts.yaml)

이 extractor는 prompt를 코드에 하드코딩하지 않고 YAML에서 읽는다.
즉 stage 지시문 문제는 코드보다 YAML 수정으로 해결되는 경우가 많다.

### 12-4. `DocumentLoader` 출력

의외로 extractor 문제가 아니라 loader 구조화 문제인 경우도 많다.

특히 아래가 의심되면 먼저 loader를 본다.

- coverage mismatch
- 펀드 inventory 누락
- 표가 이상하게 분해됨
- `evidence_label`이 엉뚱한 컬럼으로 잡힘

이럴 때는 [readme/DocumentLoader_가이드.md](/Users/bhkim/Documents/codex_prj_sam_asset/readme/DocumentLoader_가이드.md)를 함께 보는 편이 좋다.

## 13. 초보 개발자가 자주 하는 실수

### 실수 1. extractor에 파일 경로를 바로 넘기려 한다

안 된다.

extractor는 파일을 직접 읽지 않는다.  
먼저 `DocumentLoader`가 문서를 읽어 `DocumentLoadTaskPayload`를 만들어야 한다.

### 실수 2. `only_pending`을 extractor 내부 옵션이라고 생각한다

아니다.

`only_pending`은 **최종 결과 후처리 규칙**이다.
즉 `apply_only_pending_filter()`를 별도로 적용해야 한다.

### 실수 3. extractor 결과가 바로 최종 출력 계약이라고 생각한다

아니다.

extractor 결과는 내부 enum 값이다.

- `PENDING / CONFIRMED`
- `SUB / RED`

최종 코드값 `"1"`, `"2"`, `"3"`은 직렬화 단계에서만 바꾼다.

### 실수 4. `extract()`와 `extract_from_task_payload()`를 같은 난이도로 본다

실제로는 `extract_from_task_payload()`가 훨씬 안전하다.

이유:

- loader 전제와 자연스럽게 맞고
- queue 구조와도 그대로 이어지며
- 문서 메타데이터와 coverage 정보도 함께 가진다

## 14. 다른 서버로 포팅할 때 체크리스트

아래 순서대로 보면 가장 안전하다.

1. 파일 복사
   - `extractor.py`
   - `schemas.py`
   - `config.py`
   - `document_loader.py`
   - `output_contract.py`
   - `prompts/extraction_prompts.yaml`
2. 패키지 설치
   - `langchain-openai`
   - `pydantic`
   - `PyYAML`
3. 환경변수 설정
   - `LLM_BASE_URL`
   - `LLM_API_KEY`
   - `LLM_MODEL`
4. 최소 예제 실행
   - `DocumentLoader.build_task_payload()`
   - `FundOrderExtractor.extract_from_task_payload()`
5. 실패 시 artifact 저장 경로 확인
   - `write_invalid_response_debug_files()`
6. 최종 출력 계약 확인
   - `apply_only_pending_filter()`
   - `serialize_order_payload()`

## 15. 한 줄 정리

`FundOrderExtractor`는  
**"DocumentLoader가 구조화한 문서를 받아, 7-stage LLM 추출과 결정론적 후처리로 최종 주문을 만드는 모듈"** 이다.

처음에는 아래 순서만 기억하면 충분하다.

1. `DocumentLoader.build_task_payload()`
2. `FundOrderExtractor.extract_from_task_payload()`
3. `apply_only_pending_filter()`
4. `serialize_order_payload()`

이 흐름만 지키면 초보 개발자도 extractor를 비교적 안전하게 붙일 수 있다.

## 2026-04-16 업데이트 메모

- `transfer_amount`는 extractor 내부에서도 artifact-aware canonical form으로 다룬다.
- spreadsheet float-tail artifact는 제거하고, true decimal은 소수 자릿수까지 보존한다.
  - 예: `70,000,000.00000001 -> 70,000,000`
  - 예: `50,572.49 -> 50,572.49`
  - 예: `23,213.40 -> 23,213.40`
  - 예: `23,213.4 -> 23,213.4`
- deterministic markdown helper의 fund code 우선순위는 아래로 고정한다.
  - 전용 `펀드코드/fund code`
  - explicit `운용사코드/managercode`
  - generic `펀드/fund`
- `_build_markdown_table_order_type_hints()`도 canonical amount를 key로 사용한다.
  - 따라서 float-tail table cell과 normalized target item이 같은 거래로 매칭된다.
- 이번 기준선에서 닫은 대표 회귀는 아래 2건이다.
  - KB true decimal `50,572.49` 보존
  - mixed-header `D32160` scope 누락 방지
- 테스트 결과 보고 기준은 [test_report/20260420_로컬_WAS_테스트_수행_방안.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260420_로컬_WAS_테스트_수행_방안.md)를 출처로 둔다.

## 2026-04-20 업데이트 메모

- 하나생명 deterministic 복구, false-warning cleanup, `only_pending=true` 검증 방식의 상세 근거는 [merge_report/merge_result_report_20260420_01.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/merge_result_report_20260420_01.md)를 출처로 둔다.
- 후속 금액 source-scale 보존 정책에서는 `format_source_transfer_amount()`가 최종 출력 금액을 정리한다.
  - 소수 2자리 절삭/패딩은 하지 않는다.
  - 금액 dedupe/signature 비교에는 숫자 canonical 값이 쓰인다.
  - 다른 펀드 정보가 같고 금액만 `23,213.40` / `23,213.4`처럼 소수 자릿수만 다르면 같은 주문으로 본다.
- 로컬/WAS 테스트 재수행 절차와 보고서 필수 항목은 [test_report/20260420_로컬_WAS_테스트_수행_방안.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260420_로컬_WAS_테스트_수행_방안.md)를 출처로 둔다.
