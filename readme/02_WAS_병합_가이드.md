# WAS 병합 가이드

이 문서는 **현재 구현된 데이터 추출 기능을 개발 WAS 서버로 포팅할 때**,  
`ExtractionService`를 그대로 붙이는 방식이 아니라 **queue + task handler 구조**로 옮기는 방법을 설명한다.

> 2026-04-28 기준 인계 보강.
> 최신 active handoff는 [handoff_26042802.md](/Users/bhkim/Documents/codex_prj_sam_asset/세션/handoff_26042802.md)다.
> 과거 active handoff 통합 archive는 [handoff_archive_260427.md](/Users/bhkim/Documents/codex_prj_sam_asset/세션/handoff_archive_260427.md)다.
> 공통 최신 상태와 테스트 보고 원칙은 [00_시작_안내.md](/Users/bhkim/Documents/codex_prj_sam_asset/readme/00_시작_안내.md)를 출처로 둔다.
> 이 문서는 WAS 포팅/병합 전용 규칙만 유지한다.
> 병합 계획서/결과보고서의 파일명, 저장 경로, canonical 정리 방식은 [06_WAS_병합_보고서_가이드.md](/Users/bhkim/Documents/codex_prj_sam_asset/readme/06_WAS_병합_보고서_가이드.md)를 따른다.
> canonical selective merge acceptance 기준은 [mr2024.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/20260424/mr2024.md)다.
> historical retained full review example은 [tt2050.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260424/tt2050.md)다.
> latest retained WAS full review는 [tt1430.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260428/tt1430.md)다.
> 최신 focused follow-up 예시는 [mp1925.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/20260427/mp1925.md), [mr1925.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/20260427/mr1925.md), [tt1925.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260427/tt1925.md)다.
> 본문의 `2026-04-14`, `2026-04-16` 표기는 과거 stable 기준선 설명이다.

먼저 범위를 분명히 하면:

- 이 문서는 **현재 구현 기준 포팅 가이드**다.
- 거래처 프로필, `document_family`, 형식별 profile/prompt override 같은 **장기 설계안**은
  [90_설계_거래처_프로필_프리징.md](/Users/bhkim/Documents/codex_prj_sam_asset/readme/90_설계_거래처_프로필_프리징.md)를 따른다.
- 즉 현재 WAS 반영은 이 문서를 기준으로 하고, 이후 구조 개편 일정이 잡히면 설계 문서를 기준으로 별도 구현한다.

특히 아래 전제를 기준으로 작성했다.

- WAS는 queue를 사용한다.
- 1차 task handler가 `DocumentLoader`를 호출한다.
- 1차 handler는 로딩 결과를 queue에 넣는다.
- 2차 task handler가 queue에서 결과를 꺼낸다.
- 2차 handler가 `FundOrderExtractor`에 전달해 최종 추출을 수행한다.

즉 이 문서의 목표는 아래 흐름을 초보 개발자도 그대로 구현할 수 있게 만드는 것이다.

```text
문서경로
  -> WAS handler A
  -> DocumentLoader
  -> 로딩 결과(queue payload)
  -> queue enqueue
  -> WAS handler B
  -> FundOrderExtractor
  -> 최종 추출 결과
```

추가로 최종 출력 계약을 바꿔야 할 때는 `only_pending` 값을 handler B까지 함께 전달할 수 있다.

- 기본값: `False`
- `True`일 때:
  1. 최종 결과에서 기존 `settle_class == PENDING` 주문 제거
  2. 남은 주문의 `settle_class`를 모두 `PENDING`으로 변경
  3. 최종 직렬화에서는 pending 코드 `"1"`로 출력

이 규칙은 추출 중간 단계가 아니라 **최종 결과 직전 후처리**로 적용된다.

중요:

- 2026-04-21 기준 WAS의 source of truth는 **DB의 거래처 설정값**이다.
- 즉 `prompt`, `use_counterparty_prompt`, `only_pending`, `designated_password`, `delivery_type`는 가능하면 WAS DB row에서 읽어 handler A/B에 그대로 전달한다.
- Local standalone helper인 [app/prompts/counterparty_prompt_map.yaml](/Users/bhkim/Documents/codex_prj_sam_asset/app/prompts/counterparty_prompt_map.yaml)은 포팅 참고용일 뿐, WAS의 정답 소스가 아니다.
- 파일명 기반 추정이나 로컬 YAML 재판정으로 `only_pending`을 다시 계산하지 않는 편이 현재 병합 기준과 맞다.

추가 중요 지침:

- `DocumentLoader`와 `FundOrderExtractor` 코어 런타임에는 거래처명, alias, 특정 문서군 이름, 특정 안내 문구에 직접 의존하는 하드코딩 분기를 새로 추가하지 않는다.
- 도메인 정책은 DB source of truth, 프롬프트/설정, `output_contract`, 상위 task/pipeline 계층에서만 다룬다.
- `DocumentLoader`의 삼성 대상 scope 계산처럼 남아 있는 레거시 업무 계약성 로직은 현재 구현 사실로만 취급하고, WAS 병합 시 신규 확대 기준으로 삼지 않는다.
- 현재 로컬 source-of-truth 실행 경로에서 duplicate/legacy copy는 service/component boundary의 guidance helper가 `counterparty_guidance + loader shape` 기준으로 먼저 `SKIPPED`를 surface한다.
- WAS 병합 시에는 이 동작을 extractor core가 아니라 handler/task/pipeline side에서 유지하는 것을 기본값으로 둔다.
- merge scope 밖에서 배포 blocker를 발견하면 임의 수정하지 않고 blocker로 문서화한다.
- 과거 문서에 적혀 있던 `cleanup_events` startup blocker(`system_tasks.py` vs `system_cleanup_tasks.yaml` 경로 mismatch)는 현재 repo 상태에서 재현 근거가 없다.
- 따라서 현재 WAS 병합 판단에서는 그 항목을 active known blocker로 유지하지 않는다.

추가로 최종 JSON/CSV 출력 계약에서는 상태값을 문자열 코드로 치환한다.
- `settle_class`
  - `PENDING -> "1"`
  - `CONFIRMED -> "2"`
- `order_type`
  - `RED -> "1"`
  - `SUB -> "3"`

현재 코드 기준으로는 [app/component.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/component.py)가
이 구조를 그대로 코드로 옮긴 facade 역할을 한다.  
즉 `ExtractionComponent`를 쓰면 아래 두 단계를 service 우회 없이 그대로 재사용할 수 있다.

- handler A: `DocumentLoader.build_task_payload()` -> JSON 저장
- handler B: `DocumentLoadTaskPayload.read_json()` -> `FundOrderExtractor.extract_from_task_payload()`
- 현재 로컬 기준 duplicate/legacy copy enforcement는 service/component 경계에서, WAS 병합 기준 enforcement는 handler B 직전 task/pipeline 경계에서 `counterparty_guidance`와 loader shape를 함께 읽는 helper로 처리하고, extractor core 안에는 다시 넣지 않는다.

## 최신 focused follow-up 상태 (2026-04-27)

- 범위는 `output_contract` pending `t_day` 후처리 계약 교체 1건이다.
  - 대상 거래처: `동양생명`, `한화생명`, `신한라이프`
  - 현재 계약: serialized pending row(`settle_class=="1"`)에서 `t_day=="03"`만 유지
  - confirmed row(`settle_class=="2"`)는 유지
- 반영 파일:
  - runtime: [/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend/src/app/services/variable_annuity/extract/output_contract.py](/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend/src/app/services/variable_annuity/extract/output_contract.py)
  - regression test: [/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend/tests/test_variable_annuity_output_contract.py](/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend/tests/test_variable_annuity_output_contract.py)
- 산출물:
  - 계획서: [mp1925.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/20260427/mp1925.md)
  - 결과보고서: [mr1925.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/20260427/mr1925.md)
  - focused validation: [tt1925.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260427/tt1925.md)
- 검증 결과:
  - `pytest tests/test_variable_annuity_output_contract.py -q`: `9 passed`
  - affected exact compare: `PASS 8 / FAIL 0 / BLOCKED 0`
  - answer update 없음
  - source review는 수행하지 않았고, retained baseline exact compare 중심의 축소 검증이다.
- validation summary에는 worker stderr의 async DB close cleanup noise가 남아 있지만, verdict에는 영향 없었다.

## 최신 WAS full authoritative review 상태 (2026-04-28)

- runtime은 `was`다.
- retained run root는 [141713](/Users/bhkim/Documents/codex_prj_sam_asset/output/test/20260428/141713)다.
- retained 보고서는 [tt1430.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260428/tt1430.md)다.
- 결과는 `PASS 40 / FAIL 0 / BLOCKED 0`이다.
- confidence는 `confirmed 1 / provisional 39`다.
- answer set update는 없었다.
- preflight DB sync는 `db_sync_action=noop`이다.
  - expected rows `17`
  - actual rows `17`
  - updated rows `0`
- full review runner는 아래 경로를 기준으로 본다.
  - local orchestrator: [was_full_document_authoritative_review.py](/Users/bhkim/Documents/codex_prj_sam_asset/scripts/was_full_document_authoritative_review.py)
  - WAS child runner: [/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend/scripts/va_extract_case_runner.py](/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend/scripts/va_extract_case_runner.py)
- 문서-only 전수 검수에서 WAS source of truth DB row는 `db_company` exact match로 조회한다.
  - 이메일 제목/도메인 재매칭보다 manifest의 `db_company`를 우선한다.
  - `answer_company`와 `db_company`는 `case_manifest.json`, `validation_summary.json`, canonical 보고서에 함께 남긴다.

## Canonical selective merge 상태 (2026-04-24)

- 이번 세션 병합 범위는 아래 네 묶음이다.
  - 거래처 prompt structured marker 기반 fixed-column contract
  - `동양생명`/`한화생명`/`신한라이프` final serialized `t_day=="02"` 제거
  - extractor LLM hardcode `temperature=0.0`, `max_tokens=16384`
  - `동양생명`, `IBK연금보험` DB prompt sync
- 반영된 핵심 동작:
  - `[[COUNTERPARTY_PROMPT_META]]` block 분리와 `fixed_stage_columns` runtime wiring
  - `동양생명` same-day `정산내역`은 `정산액`을 제외하고 `설정액`/`해지액`만 사용
  - `IBK연금보험` same-day `정산액`은 제외하고 `설정액`/`해지액`만 사용, future `예정 정산액 기준일+N`은 유지
  - pipeline 최종 저장도 shared output-contract dedupe 기준 사용
- acceptance 수치:
  - helper regression `26 passed`
  - affected exact compare `5 PASS / 0 FAIL / 0 BLOCKED`
  - full direct validation `38 PASS / 0 FAIL / 0 BLOCKED`
  - pipeline smoke `2 PASS / 0 FAIL / 0 BLOCKED`
- post-merge follow-up:
  - historical retained WAS full review는 [tt2050.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260424/tt2050.md)다.
  - WAS deployment review에서 plain DB company name `흥국생명`이 heungkuk canonical output normalization을 타지 못할 수 있는 issue를 [output_contract.py](/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend/src/app/services/variable_annuity/extract/output_contract.py)에서 수정했다.
  - post-merge targeted helper/output-contract regression은 `27 passed`다.
- canonical 보고서:
  - [mr2024.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/20260424/mr2024.md)
  - merge-time snapshot: [tt2024.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260424/tt2024.md)
  - historical retained full review: [tt2050.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260424/tt2050.md)
- 2026-04-20, 2026-04-21, 2026-04-16 병합 문서는 historical accepted baseline로 유지한다.

## 참고. 2026-04-14 stable 병합/검증 기준

이번 기준선에서 WAS는 "로컬 코드를 통째로 덮어쓴 것"이 아니라, 아래 동작을 보존하면서 필요한 차이만 선별 반영한 상태다.

- 유지해야 하는 WAS 계약
  - `DocumentLoadTaskPayload.write_json()` / `read_json()` handoff 계약
  - `Trace task_id` prompt prefix
  - prompt/response log
  - MHT parser-first + render hint 경로
  - legacy XLS direct BIFF + parser fallback
  - retry/state/reason normalization
- 병합 시 실제로 중요했던 로컬 규칙
  - loader의 decorated total-row 제외, `비고` 기반 pending bucket, 한국 금액 단위 인식, code-only row 유지
  - extractor의 base_date 이후 structure/markdown shortcut, 결정론적 settle 복구, row/section date 우선 규칙
- 2026-04-14 stable 검증 결과
  - Local direct extraction `rc=0`: `14/14`
  - WAS direct extraction `rc=0`: `14/14`
  - Local/WAS `orders + base_date` parity: `14/14`
  - Local/WAS issues parity: `14/14`
  - Local/WAS DB projection parity: `14/14`
  - WAS pipeline smoke: `3/3`

## 참고. 2026-04-20~2026-04-21 최신 병합/검증 기준

2026-04-20 하나생명 XLSX 전환 병합은 하나생명 XLSX 전환과 loader/extractor deterministic 보정을 선별 반영했다. 검증 수치와 케이스별 결과는 중복 기재하지 않고 [merge_report/20260420/mr0001.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/20260420/mr0001.md)를 출처로 둔다.

- WAS repo 기준 production 반영 파일:
  - `src/app/services/variable_annuity/extract/document_loader.py`
  - `src/app/services/variable_annuity/extract/extractor.py`
- 검증 support 반영 파일:
  - `tests/conftest.py`
- DB 반영 대상은 `tb_variable_counterparties.company_name='하나생명'` prompt row다.
- 이 하나생명 XLSX 전환 라운드에서는 `output_contract.py` 조건부 병합은 수행하지 않았다.
  - `11_카디프`, `14_흥국생명`, `17_메트라이프` final payload drift가 재현되지 않았기 때문이다.
- 하나생명 prompt는 XLSX workbook authoritative 계약으로 갱신했다.
- helper regression은 test source가 존재할 때 WAS venv로 실행한다.
  - 현재 재실행 절차는 [03_테스트_실행_가이드.md](/Users/bhkim/Documents/codex_prj_sam_asset/readme/03_테스트_실행_가이드.md)를 따른다.

후속 금액 source-scale 보존 라운드는 별도 변경이다.

- backend final payload 단계에서 소수 2자리 절삭/패딩을 하지 않는다.
- `18,711,858.00`처럼 소수부가 모두 0인 금액은 정수로 정리한다.
- `23,213.40`, `23,213.4`, `23,213.409`처럼 실제 소수부가 있는 금액은 추출 문자열의 소수 자릿수를 유지한다.
- `70,000,000.00000001` 같은 spreadsheet float-tail artifact는 정수로 정리한다.
- 이 라운드에서는 WAS `amount_normalization.py`, `output_contract.py`, `extractor.py`, `tasks/llm/pipeline.py`가 대상이다.

2026-04-21 메트라이프 selective merge는 위 accepted 기준선 위에 선별 반영한 추가 라운드다.

- WAS repo 기준 production 반영 파일:
  - `src/app/services/variable_annuity/extract/document_loader.py`
  - `src/app/services/variable_annuity/extract/extractor.py`
  - `src/app/services/variable_annuity/extract/prompts/extraction_prompts.yaml`
- DB 반영 대상:
  - `tb_variable_counterparties.company_name='메트라이프생명'`
- 병합 핵심:
  - 문서측 금액 토큰 정규화가 `₩`, `￦`, `KRW`, `원`, Unicode whitespace를 허용
  - deterministic amount parser가 loader와 같은 decorated amount 규칙을 사용
  - `instruction_document` 판정에서 explicit RED/outflow 컬럼의 non-zero 음수 printed amount도 거래 증거로 인정
- focused validation 결과:
  - 메트라이프 targeted regression `2/2 PASS`
  - 메트라이프 3건 포함 임의 9건 원본 대조 검수 `9 PASS / 0 FAIL`
  - 세부 보고서: [test_report/20260421/tt0002.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260421/tt0002.md), [test_report/20260421/tt0001.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260421/tt0001.md)

## 0-1. 주석 읽는 순서

WAS 포팅 전에 코드를 읽을 때는 아래 순서가 가장 빠르다.

1. [app/component.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/component.py)
   - handler A/B를 어떻게 연결하는지
2. [app/document_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loader.py)
   - 로더 결과 JSON이 어떤 기준으로 만들어지는지
3. [app/extractor.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extractor.py)
   - 최종 추출과 `only_pending`, 출력 계약 치환이 어느 단계에서 적용되는지

최근에는 helper 레벨까지 한국어 주석을 보강했기 때문에,
포팅 시에는 구현보다 주석의 "왜 이 로직이 필요한가"를 먼저 읽는 편이 안전하다.

추가로 [app/service.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/service.py)를 직접 쓰는 경우에도
이제는 내부적으로 같은 handoff 파일 경로를 한 번 거친다.  
즉 service는 "메모리 직통 경로"가 아니라, 호환용 단건 래퍼이면서도
실행 방식은 handler A/B 구조와 최대한 같게 유지하도록 맞춰져 있다.

## 0. 포팅 시 필요한 파일 목록

가장 먼저, WAS로 옮길 때 어떤 파일이 필요한지부터 정리한다.

### 0-1. 최소 필수 파일

queue 기반 WAS에서 `DocumentLoader -> FundOrderExtractor` 흐름만 구현하려면
최소한 아래 파일들은 같이 가져가야 한다.

- [app/document_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loader.py)
- [app/document_loading/__init__.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loading/__init__.py)
- [app/document_loading/common.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loading/common.py)
- [app/document_loading/dto.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loading/dto.py)
- [app/document_loading/loader_core.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loading/loader_core.py)
- [app/document_loading/markdown.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loading/markdown.py)
- [app/document_loading/coverage.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loading/coverage.py)
- [app/document_loading/scope.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loading/scope.py)
- [app/extractor.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extractor.py)
- [app/extraction/__init__.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extraction/__init__.py)
- [app/extraction/constants.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extraction/constants.py)
- [app/extraction/models.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extraction/models.py)
- [app/extraction/prompts.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extraction/prompts.py)
- [app/extraction/counterparty.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extraction/counterparty.py)
- [app/extraction/telemetry.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extraction/telemetry.py)
- [app/schemas.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/schemas.py)
- [app/config.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/config.py)
- [app/output_contract.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/output_contract.py)
- [app/document_loaders/__init__.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/__init__.py)
- [app/document_loaders/pdf_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/pdf_loader.py)
- [app/document_loaders/excel_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/excel_loader.py)
- [app/document_loaders/html_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/html_loader.py)
- [app/document_loaders/eml_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/eml_loader.py)
- [app/document_loaders/mht_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/mht_loader.py)
- [app/prompts/extraction_prompts.yaml](/Users/bhkim/Documents/codex_prj_sam_asset/app/prompts/extraction_prompts.yaml)

2026-04-23 기준 로컬 리팩토링 라운드에서는 위 구조를 "WAS mirror 가능 상태"로 정리했고, 실제 WAS repo 병합은 아직 수행하지 않았다.

### 0-2. 가져가지 않아도 되는 파일

아래 파일들은 현재 설명하는 queue 기반 포팅의 최소 범위에는 꼭 필요하지 않다.

- [app/service.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/service.py)
  - 단건 추출 호환용 오케스트레이터 참고 구현
- [app/component.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/component.py)
  - 단건/다건 payload와 병합 규칙용 상위 컴포넌트
  - 다만 "직접 handler를 구현하지 않고 현재 구조를 그대로 재사용"하려면 가져가는 편이 좋다.
- [main.py](/Users/bhkim/Documents/codex_prj_sam_asset/main.py)
  - CLI 진입점

즉 이번 문서 기준으로는:
- **필수**: `DocumentLoader`, `FundOrderExtractor`, 형식별 loader, schema, config, prompt
- **선택**: `ExtractionService`, `ExtractionComponent`, CLI

### 0-3. 왜 이 파일들이 필요한가

- `document_loader.py`
  - 문서 로딩과 구조화
- `document_loaders/*.py`
  - 형식별 실제 파싱 구현
- `extractor.py`
  - 7-stage LLM 추출
- `schemas.py`
  - 최종 결과 모델
- `config.py`
  - LLM 및 런타임 설정
- `output_contract.py`
  - 최종 JSON/CSV 출력 계약 직렬화
  - `settle_class`, `order_type` 문자열 코드 치환
  - `t_day`, `transfer_amount` 출력 포맷 정리
- `prompts/extraction_prompts.yaml`
  - 추출 프롬프트 정의

처음 포팅할 때는 이 파일 목록부터 확보한 뒤,
그 다음에 handler 구조를 구현하는 순서가 가장 안전하다.

## 1. 왜 `ExtractionService`를 그대로 쓰지 않는가

[app/service.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/service.py)의 `ExtractionService`는
현재 프로젝트에서는 편리한 오케스트레이터지만, queue 기반 WAS 구조에는 맞지 않는 부분이 있다.

이유는 간단하다.

- `ExtractionService`는 한 프로세스 안에서
  - 문서 로드
  - 비지시서 판정
  - empty 문서 판정
  - scope 계산
  - chunk 분할
  - LLM 추출
  - coverage 검증
  - blocking issue 검사
  를 한 번에 끝내도록 만들어져 있다.
- 그런데 WAS에서는 이 흐름을 두 단계 이상으로 나눠야 한다.
- 즉 WAS의 queue 경계와 현재 `ExtractionService`의 책임 경계가 다르다.

그래서 WAS에서는 `ExtractionService`를 직접 재사용하기보다,
그 안의 흐름을 참고해서 **handler A와 handler B의 얇은 orchestration**으로 나누는 편이 더 자연스럽다.

이 문서에서는 이 방식을 권장한다.

정리하면:
- service를 써도 내부 실행 경로는 같아졌다.
- 하지만 WAS 설계 관점에서는 여전히 handler A/B를 직접 드러내는 편이 더 명확하다.
- 운영 추적, queue handoff, 실패 재현 측면에서도 handler 분리 구조가 유리하다.

## 2. WAS에서 직접 써야 하는 핵심 모듈

queue 기반 구조에서 핵심은 아래 두 모듈이다.

- [app/document_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loader.py)
  - 문서를 읽고 `raw_text`, `markdown_text`, coverage 관련 메타데이터를 만든다.
- [app/extractor.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extractor.py)
  - `FundOrderExtractor`
  - `DocumentLoader`가 만든 결과를 바탕으로 7-stage LLM 추출을 수행한다.

이 구조에서는 `ExtractionService`를 "직접 쓰는 클래스"가 아니라,
**WAS handler에서 어떤 순서로 호출해야 하는지 참고하는 구현 예시**로 보는 것이 맞다.

## 3. 권장 아키텍처

queue 기반 WAS에서는 아래처럼 두 단계로 나누는 것이 가장 이해하기 쉽다.

### 3-1. Handler A: 문서 로딩 단계

역할:
- 파일 경로를 받는다.
- `DocumentLoader`로 문서를 읽는다.
- 추출 전에 필요한 문서 메타데이터를 계산한다.
- 그 결과를 queue payload로 만든다.

### 3-2. Handler B: 추출 단계

역할:
- queue payload를 받는다.
- `FundOrderExtractor`로 실제 주문 추출을 수행한다.
- coverage 검증과 blocking issue 검사를 한다.
- 최종 JSON/CSV 저장 또는 DB 저장 단계로 넘긴다.

## 4. Handler A가 해야 하는 일

Handler A는 아래 순서로 구현하면 된다.

1. 입력 문서 경로 검증
2. `DocumentLoader.build_task_payload(...)` 호출
3. `DocumentLoadTaskPayload.write_json(...)` 또는 queue enqueue
4. 다음 단계가 읽을 수 있는 위치를 전달

즉 Handler A는 "문서를 열고, 다음 단계가 추출할 수 있는 재료를 만드는 곳"이다.

## 5. Handler A 예시 코드

아래 코드는 이해를 돕기 위한 최소 예시다.

```python
from __future__ import annotations

from pathlib import Path

from app.config import get_settings
from app.document_loader import DocumentLoader


def run_loader_handler(
    source_path: str,
    result_json_path: str,
    pdf_password: str | None = None,
) -> str:
    settings = get_settings()
    loader = DocumentLoader()
    path = Path(source_path).expanduser()
    result_path = Path(result_json_path).expanduser()

    task_payload = loader.build_task_payload(
        path,
        chunk_size_chars=settings.llm_chunk_size_chars,
        pdf_password=pdf_password,
    )
    task_payload.write_json(result_path)
    return str(result_path)
```

위 코드에서 `task_payload.write_json(...)`는 부모 디렉터리까지 자동 생성한다.  
즉 job별 하위 디렉터리나 날짜별 폴더를 결과 경로로 넘겨도 Handler A 쪽에서 별도 `mkdir`를 하지 않아도 된다.

`only_pending`과 거래처 guidance 식별자는 Handler A가 아니라 Handler B에서 적용/조회하는 값이므로,
queue envelope에는 `task_payload_json_path`와 함께 `only_pending`, `counterparty_id`
(또는 이미 렌더링된 `counterparty_guidance`)를 별도 메타데이터로 전달해 두는 편이 가장 단순하다.

예:

```json
{
  "task_payload_json_path": "/jobs/20260319/loader_result.json",
  "counterparty_id": 123,
  "only_pending": true
}
```

즉 `DocumentLoadTaskPayload`는 로더 산출물 자체에 집중하고,
DB source-of-truth인 거래처 설정값은 queue/DB 메타데이터에서 다시 합쳐 Handler B로 넘기는 방식이 가장 안전하다.

Handler B에서 최종 payload를 저장할 때는 아래 코드 문자열 계약을 적용해야 한다.
- `settle_class`: `"1"` 또는 `"2"`
- `order_type`: `"1"` 또는 `"3"`

즉 Handler A는 "문서를 읽고, 다음 단계가 다시 읽을 수 있는 재료 JSON을 남기는 것"까지만 책임진다.
최종 출력 계약과 비즈니스 옵션(`only_pending`)은 Handler B에서 적용한다.

## 6. queue payload에는 무엇을 넣어야 하나

이제는 dict를 손으로 조립하지 않고, `DocumentLoadTaskPayload`를 그대로 저장하는 방식을 권장한다.
초보 개발자 기준으로는 아래 항목이 이 DTO 안에 들어 있다고 이해하면 된다.

- 문서 식별 정보
  - `source_path`
  - `file_name`
- 로딩 결과
  - `raw_text`
  - `markdown_text`
  - `content_type`
  - `chunks`
- 추출 판단용 메타데이터
  - `non_instruction_reason`
  - `allow_empty_result`
  - `expected_order_count`
  - `target_fund_scope`

저장되는 JSON의 개념적 예시는 아래와 같다.

```json
{
  "source_path": "/data/orders/sample.pdf",
  "file_name": "sample.pdf",
  "content_type": "application/pdf",
  "raw_text": "...",
  "markdown_text": "...",
  "chunks": ["...chunk1...", "...chunk2..."],
  "non_instruction_reason": null,
  "allow_empty_result": false,
  "expected_order_count": 8,
  "target_fund_scope": {
    "manager_column_present": true,
    "include_all_funds": false,
    "fund_codes": ["492007", "004434"],
    "fund_names": [],
    "canonical_fund_names": []
  }
}
```

핵심은 **Handler B가 다시 원문 파일을 읽지 않아도 되게 만드는 것**이다.
즉 Handler A 산출물 파일 하나만 있으면 Handler B는 바로 추출을 시작할 수 있어야 한다.

## 7. Handler B가 해야 하는 일

Handler B는 아래 순서로 구현하면 된다.

1. Handler A 산출물 파일 경로 dequeue
2. `DocumentLoadTaskPayload.read_json(...)` 호출
3. `FundOrderExtractor.extract_from_task_payload(...)` 호출
4. 실패하면 `ExtractionOutcomeError`에서 `outcome`과 invalid artifact를 읽어 디버그 파일 저장
5. 최종 `ExtractionResult` 저장 또는 다음 단계로 전달

즉 Handler B는 "이미 로드된 문서를 실제 주문 데이터로 바꾸는 곳"이다.

## 8. Handler B 예시 코드

```python
from __future__ import annotations

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


def run_extract_handler(
    task_payload_json_path: str,
    only_pending: bool = False,
    counterparty_guidance: str | None = None,
):
    settings = get_settings()
    extractor = FundOrderExtractor(settings)
    task_payload = DocumentLoadTaskPayload.read_json(Path(task_payload_json_path))
    try:
        # WAS에서는 이 guidance를 로컬 YAML이 아니라 DB row의
        # prompt/use_counterparty_prompt 설정에서 resolve해서 넘긴다.
        outcome = extractor.extract_from_task_payload(
            task_payload,
            counterparty_guidance=counterparty_guidance,
        )
    except ExtractionOutcomeError as exc:
        # service를 거치지 않고 extractor를 직접 쓰는 경우에는
        # 실패 예외에서 outcome을 읽어 invalid artifact를 직접 저장하면 된다.
        write_invalid_response_debug_files(
            debug_output_dir=settings.debug_output_dir,
            source_name=task_payload.file_name,
            artifacts=exc.invalid_response_artifacts,
        )
        raise
    filtered_result = apply_only_pending_filter(outcome.result, only_pending=only_pending)
    base_date = next((order.base_date for order in filtered_result.orders if order.base_date), None)
    return {
        "file_name": task_payload.file_name,
        "source_path": task_payload.source_path,
        "model_name": settings.llm_model,
        "base_date": base_date,
        "issues": filtered_result.issues,
        "orders": [serialize_order_payload(order) for order in filtered_result.orders],
    }
```

위 예시에서 중요한 점은 `ExtractionOutcomeError`가 단순 실패 메시지뿐 아니라
실패 시점의 `outcome`도 함께 들고 올라온다는 것이다.

- `exc.outcome.result`
  - 실패 시점까지 조립된 `ExtractionResult`
- `exc.invalid_response_artifacts`
  - JSON 파싱에 실패한 LLM 원문 응답 목록

즉 WAS의 handler B가 `FundOrderExtractor`를 직접 사용하더라도,
`ExtractionService` 없이 동일한 디버깅 artifact를 남길 수 있다.

현재 코드에서는 아래 접근도 모두 가능하다.

- `exc.outcome`
- `exc.result`
- `exc.invalid_response_artifacts`

즉 handler B 구현부에서
"실패했지만 partial result는 DB에 남길지",
"invalid raw response를 파일로 저장할지"
를 더 세밀하게 결정할 수 있다.

또한 `only_pending`이 필요하면 여기서 함께 적용하면 된다.
즉 handler A는 문서를 읽고 저장만 하고,
handler B가 DB 거래처 설정값을 다시 합쳐 prompt guidance/최종 결과 표현 계약까지 책임진다.

정리하면 Handler B의 순서는 항상 아래처럼 보는 것이 안전하다.
1. handoff JSON 읽기
2. `FundOrderExtractor`로 내부 추출
3. `ExtractionOutcomeError`가 있으면 invalid artifact 저장
4. `only_pending` 후처리
5. `serialize_order_payload()`로 최종 문자열 코드 계약 직렬화
6. JSON/CSV/DB 저장

## 9. queue 기반 구조에서 service는 무엇만 해야 하나

사용자가 요청한 구조대로라면, WAS 쪽 service 또는 task handler는 아래 역할만 가져야 한다.

- 입력 path를 받는다
- `DocumentLoader`를 호출한다
- 그 결과를 queue에 넣는다
- queue에서 결과를 꺼낸다
- `FundOrderExtractor`를 호출한다
- 최종 결과를 저장하거나 응답용 DTO로 바꾼다

즉 service의 책임은 **연결과 전달**이다.

현재 저장소 안에서는 [app/component.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/component.py)가
이 철학을 반영한 상위 facade다.  
service를 직접 많이 호출하기보다, WAS 쪽에서는

- `DocumentLoader`
- `FundOrderExtractor`
- 필요하면 `ExtractionComponent`

중 하나를 선택해서 쓰는 편이 더 자연스럽다.

아래는 좋지 않은 구조다.
- service가 문서 파싱 규칙을 직접 해석
- service가 금액 컬럼 의미를 직접 판단
- service가 stage별 추출 로직을 직접 구현

아래는 좋은 구조다.
- 파싱은 `DocumentLoader`
- 추출은 `FundOrderExtractor`
- service는 순서와 입출력만 연결

## 10. 추천 구현 형태

초보 개발자 기준으로는 아래처럼 3개 파일로 나누면 이해하기 쉽다.

### 10-1. `loader_task_handler.py`

역할:
- 파일 경로 입력
- `DocumentLoader` 호출
- queue payload enqueue

### 10-2. `extract_task_handler.py`

역할:
- queue payload dequeue
- `FundOrderExtractor` 호출
- 결과 검증
- DB 저장 / 파일 저장 / 응답 전환

### 10-3. `queue_dto.py`

역할:
- queue로 넘기는 payload 구조 정의
- 직렬화/역직렬화 규약 관리

현재 코드 기준으로는 이 역할을 [app/document_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loader.py)의
`DocumentLoadTaskPayload`가 담당한다.

## 10-1. 지금 저장소 코드와 매핑하면 어떻게 보이나

현재 구현은 아래처럼 대응된다.

- handler A 핵심
  - [app/component.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/component.py)의 `run_handler_a()`
  - [app/document_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loader.py)의 `build_task_payload()`
  - [app/document_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loader.py)의 `DocumentLoadTaskPayload.write_json()`
- handler B 핵심
  - [app/component.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/component.py)의 `run_handler_b()`
  - [app/document_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loader.py)의 `DocumentLoadTaskPayload.read_json()`
  - [app/extractor.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extractor.py)의 `FundOrderExtractor.extract_from_task_payload()`

즉 초보 개발자 입장에서는
"WAS에서 직접 구현할 코드"와 "현재 프로젝트에서 이미 구현된 대응 메서드"를
1:1로 대응해서 보는 것이 가장 이해하기 쉽다.

추가로 단건/다건을 모두 현재 코드 그대로 재사용하고 싶다면
- [app/component.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/component.py)의 `extract_document_payload()`
- [app/component.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/component.py)의 `extract_document_payloads()`
를 그대로 참고하면 된다.

다만 WAS에 queue가 이미 있다면, 문서 경계를 더 명확히 드러내기 위해
직접 `run_handler_a` / `run_handler_b` 개념으로 구현하는 편이 운영 추적에 더 유리하다.

## 11. 초보 개발자 기준으로 꼭 기억할 3가지

### 11-1. `DocumentLoader`는 파일 경로를 직접 받는다

다른 서버에서 문서 저장 위치가 달라도 상관없다.

```python
loaded = loader.load(Path("/new/storage/path/file.pdf"))
```

### 11-2. queue에는 "다음 단계가 다시 파일을 읽지 않게 할 정보"를 넣는다

적어도 아래는 넣는 것이 좋다.

- `raw_text`
- `markdown_text`
- `chunks`
- `expected_order_count`
- `target_fund_scope`

현재는 이 정보가 모두 `DocumentLoadTaskPayload` 안에 들어가므로,
WAS 쪽에서는 DTO를 그대로 JSON으로 저장하고 다시 읽는 방식이 가장 단순하다.

### 11-3. service는 얇게 유지한다

Service/handler는 아래만 하면 된다.

- 순서 제어
- queue 입출력
- 예외 처리
- 최종 저장

문서 해석과 추출 판단은 가능한 한 아래 모듈에 남겨 둔다.

- [app/document_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loader.py)
- [app/extractor.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extractor.py)

## 12. 포팅 시 필요한 설정

queue 기반 WAS에서도 최소한 아래 설정은 맞아야 한다.

- `LLM_BASE_URL`
- `LLM_API_KEY`
- `LLM_MODEL`
- `LLM_TIMEOUT_SECONDS`
- `LLM_RETRY_ATTEMPTS`
- `LLM_RETRY_BACKOFF_SECONDS`
- `LLM_CHUNK_SIZE_CHARS`
- `LLM_STAGE_BATCH_SIZE`
- `TASK_PAYLOAD_OUTPUT_DIR`
- `DELETE_TASK_PAYLOAD_FILES`

주의:
- `DocumentLoader`만 단독으로 쓰는 단계에서는 `.env` 의존이 크지 않다.
- 하지만 `FundOrderExtractor` 단계는 [app/config.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/config.py)의 설정을 사용한다.

### 12-1. handoff 결과물 저장/삭제 정책

`DocumentLoader`의 결과물은 `DocumentLoadTaskPayload` JSON 파일로 저장된다.  
이 결과물은 아래 env로 제어한다.

- `TASK_PAYLOAD_OUTPUT_DIR`
  - handler A 산출물 JSON을 저장할 기본 디렉터리
- `DELETE_TASK_PAYLOAD_FILES`
  - `true`면 추출 완료 후 내부적으로 생성한 handoff 파일을 삭제
  - `false`면 handoff 파일을 남겨 두고 운영/검수에서 다시 확인 가능

현재 **저장소의 `.env` 예시 값**은 아래와 같다.

```env
TASK_PAYLOAD_OUTPUT_DIR=./output/handoff
DELETE_TASK_PAYLOAD_FILES=false
```

하지만 **코드 기본값**은 다르다.

- `TASK_PAYLOAD_OUTPUT_DIR`
  - 기본값: `./output/handoff`
- `DELETE_TASK_PAYLOAD_FILES`
  - 코드 기본값: `true`

즉 정리하면:
- `.env`를 현재 저장소처럼 설정하면 handoff 파일을 남긴다.
- 운영 서버에서 env를 따로 주지 않으면 코드 기본값에 따라 handoff 파일을 삭제한다.

이 차이를 문서에 분리해서 적는 이유는,
운영 포팅 시 `.env` 누락 여부에 따라 handoff 파일 보존 정책이 달라질 수 있기 때문이다.

참고:
- `ExtractionComponent`가 내부적으로 handoff 경로를 관리할 때는 이 env를 따른다.
- `ExtractionService`를 직접 쓰는 경우에도 동일하게 이 env를 따른다.
- 호출자가 `run_handler_a(..., task_payload_json_path=...)`처럼 저장 경로를 직접 넘긴 경우에는
  그 파일은 호출자가 소유한다고 보고 component가 임의로 삭제하지 않는다.

## 12-2. 최종 출력 계약은 어디서 적용되나

최종 JSON/CSV 계약은 [app/output_contract.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/output_contract.py)의
`serialize_order_payload(...)`에서 적용한다.

이 helper가 하는 일은 아래와 같다.

1. 내부 모델(`OrderExtraction`)을 dict로 변환
2. `transfer_amount`를 외부 출력용 문자열로 정리
3. `t_day`를 외부 계약 문자열(`01`, `02`...)로 정리
4. `settle_class`를 문자열 코드로 치환
   - `PENDING -> "1"`
   - `CONFIRMED -> "2"`
5. `order_type`를 문자열 코드로 치환
   - `RED -> "1"`
   - `SUB -> "3"`

즉 WAS에서 DB 저장이나 응답 생성도 이 helper를 통과한 결과를 기준으로 보면 된다.

중요:
- 내부 extractor 결과를 바로 DB에 넣으면 `PENDING`, `CONFIRMED`, `SUB`, `RED`가 그대로 남는다.
- 최종 외부 계약과 맞추려면 **반드시 직렬화 helper 이후 결과**를 사용해야 한다.
- `transfer_amount`는 여기서 소수 2자리로 고정하지 않는다.
  - `23,213.40 -> 23,213.40`
  - `23,213.4 -> 23,213.4`
  - `23,213.409 -> 23,213.409`
  - `18,711,858.00 -> 18,711,858`
  - `70,000,000.00000001 -> 70,000,000`
- WAS pipeline dedupe key는 금액을 숫자 canonical 값으로 비교한다. 따라서 다른 펀드 정보가 같고 금액만 `23,213.40` / `23,213.4`처럼 소수 자릿수만 다르면 중복으로 처리한다.

## 12-3. invalid artifact는 어디서 저장하나

handler B에서 추출이 실패했을 때는 `ExtractionOutcomeError`가 올라올 수 있다.
이 예외는 단순 실패 메시지만 담는 것이 아니라,
실패 시점의 추출 결과와 invalid LLM 응답 artifact도 같이 들고 있다.

실전에서는 아래 두 값이 특히 중요하다.

- `exc.result`
  - 실패 시점까지 조립된 `ExtractionResult`
- `exc.invalid_response_artifacts`
  - JSON 파싱에 실패한 LLM 원문 응답 목록

artifact를 파일로 남길 때는 [app/extractor.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extractor.py)의
`write_invalid_response_debug_files(...)`를 그대로 쓰면 된다.

## 13. 문제 생기면 어디를 먼저 봐야 하나

### 13-1. Handler A에서 실패

먼저 볼 곳:
- [app/document_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loader.py)
- 각 형식별 loader
  - [app/document_loaders/pdf_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/pdf_loader.py)
  - [app/document_loaders/excel_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/excel_loader.py)
  - [app/document_loaders/eml_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/eml_loader.py)

### 13-2. Handler B에서 실패

먼저 볼 곳:
- [app/extractor.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extractor.py)
- prompt 설정
  - [app/prompts/extraction_prompts.yaml](/Users/bhkim/Documents/codex_prj_sam_asset/app/prompts/extraction_prompts.yaml)

### 13-3. coverage mismatch

먼저 볼 곳:
- handler A가 계산한 `expected_order_count`
- `raw_text`
- `markdown_text`
- handler B의 최종 `orders`

### 13-4. `only_pending` 결과가 예상과 다를 때

먼저 확인할 곳:
- Handler B가 `only_pending` 값을 실제로 받고 있는지
- `apply_only_pending_filter(...)`가 직렬화 전에 적용되는지
- 최종 JSON/CSV를 만들 때 `serialize_order_payload(...)`를 거쳤는지

가장 흔한 실수는 아래 두 가지다.
- Handler B에서 `only_pending`을 적용하지 않고 내부 결과를 그대로 저장
- `serialize_order_payload(...)`를 생략해서 문자열 코드 치환이 빠짐

## 14. 가장 현실적인 시작 방법

처음부터 queue 전체를 다 붙이기보다 아래 순서가 좋다.

1. 로컬에서 `DocumentLoader` 단독 실행 성공
2. 로컬에서 `FundOrderExtractor` 단독 실행 성공
3. queue payload 직렬화/역직렬화 확인
4. handler A -> handler B 연결
5. 실제 WAS job/task 등록

이 순서대로 가면 문제를 단계별로 분리해서 볼 수 있다.

## 15. 한 줄 요약

queue 기반 WAS에서는 `ExtractionService`를 그대로 쓰지 말고,

- **Handler A = DocumentLoader 중심**
- **Handler B = FundOrderExtractor 중심**
- **service/task handler = 연결만 담당**
- **최종 출력 계약(`only_pending`, 문자열 코드 치환) = Handler B에서 적용**

으로 구현하는 것이 가장 이해하기 쉽고 유지보수도 쉽다.

## 2026-04-16 업데이트 메모

- 현재 accepted WAS 병합 기준 보고서는 아래다.
  - `/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/20260416/mr0001.md`
- 2026-04-16 1차 병합 범위는 아래 6개로 고정했다.
  - `extract/amount_normalization.py`
  - `extract/document_loaders/excel_loader.py`
  - `extract/extractor.py`
  - `extract/output_contract.py`
  - `extract/prompts/extraction_prompts.yaml`
  - `tests/test_variable_annuity_extract_helpers.py`
- 이번 병합에서도 아래는 수정하지 않는다.
  - `tasks/llm/pipeline.py`
  - `parser.py`
  - `settings.py`
  - `.env`
- WAS 운영 기준 prompt source of truth는 계속 DB `tb_variable_counterparties` row다.
  - `prompt`
  - `use_counterparty_prompt`
  - `only_pending`
  - `designated_password`
- 당시 DB prompt parity와 acceptance 상세는 [merge_report/20260416/mr0001.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/20260416/mr0001.md)를 출처로 둔다.
- 테스트/검증 결과 보고 기준은 [03_테스트_실행_가이드.md](/Users/bhkim/Documents/codex_prj_sam_asset/readme/03_테스트_실행_가이드.md)를 출처로 둔다.

## 2026-04-20 업데이트 메모

- 2026-04-20 당시 accepted WAS 병합 기준 보고서는 [merge_report/20260420/mr0001.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/20260420/mr0001.md)다.
- 백업 manifest는 [manifest.json](/Users/bhkim/Documents/codex_prj_sam_asset/output/was_backup/was_merge_20260420_01/manifest.json)이다.
- 이번 병합에서도 아래 운영 코드는 수정하지 않았다.
  - `src/app/common/document/parser.py`
  - `src/app/config/settings.py`
  - `.env`
- 후속 금액 source-scale 보존 라운드에서는 `tasks/llm/pipeline.py`의 dedupe key가 숫자 canonical 금액을 쓰도록 갱신됐다.
- 하나생명 DB prompt는 갱신했지만 `use_counterparty_prompt=true`, `only_pending=true`, `designated_password=null`은 보존했다.
- LLM 연결 확인과 데이터 추출 테스트 보고서 작성 규칙은 [03_테스트_실행_가이드.md](/Users/bhkim/Documents/codex_prj_sam_asset/readme/03_테스트_실행_가이드.md)를 출처로 둔다.

## 2026-04-21 업데이트 메모

- 메트라이프 selective merge에서는 WAS production 코드 3개와 DB prompt row 1개만 선별 반영했다.
- `parser.py`, `settings.py`, `.env`, `output_contract.py`, 파일 기반 거래처 prompt runtime wiring은 이번 라운드에서 수정하지 않았다.
- 메트라이프 DB prompt는 로컬 [메트라이프생명.txt](/Users/bhkim/Documents/codex_prj_sam_asset/app/prompts/메트라이프생명.txt)와 parity를 맞췄다.
- 당시 생성된 legacy debug root:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/output/debug/was_merge_20260421_01`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/output/debug/was_validation_20260421_01`
- 새 테스트 실행의 최종 산출물 저장 규칙은 [03_테스트_실행_가이드.md](/Users/bhkim/Documents/codex_prj_sam_asset/readme/03_테스트_실행_가이드.md)를 따른다.

## 2026-04-24 최신 WAS merge 메모

- 이번 병합은 pending 상태가 아니라 완료 상태다.
- 병합 대상 코드는 아래로 닫혔다.
  - `src/app/services/variable_annuity/extract/extractor.py`
  - `src/app/services/variable_annuity/extract/output_contract.py`
  - `src/app/services/variable_annuity/extract/extraction/*`
  - `src/app/services/variable_annuity/extract/prompts/extraction_prompts.yaml`
  - `src/app/services/variable_annuity/tasks/llm/pipeline.py`
  - `tests/test_variable_annuity_extract_helpers.py`
  - `tests/test_variable_annuity_output_contract.py`
- DB row update는 `tb_variable_counterparties`의 아래 2건만 수행했다.
  - `company_name='동양생명'`
  - `company_name='IBK연금보험'`
- preserved fields는 모두 유지했다.
  - `use_counterparty_prompt`
  - `only_pending`
  - `designated_password`
  - `delivery_type`
- `parser.py`, `settings.py`, `.env`는 이번 라운드에서도 수정하지 않았다.
- post-merge acceptance는 아래 5개 축으로 닫는다.
  - helper regression `26 passed`
  - prompt parity `2/2 match`
  - affected exact compare `5 PASS / 0 FAIL / 0 BLOCKED`
  - direct full validation `38 PASS / 0 FAIL / 0 BLOCKED`
  - pipeline smoke `2 PASS / 0 FAIL / 0 BLOCKED`
- 최신 근거 문서:
  - [mr2024.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/20260424/mr2024.md)
  - merge-time snapshot: [tt2024.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260424/tt2024.md)
  - historical retained review: [tt2050.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260424/tt2050.md)
