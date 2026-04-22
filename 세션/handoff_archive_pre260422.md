# Handoff Archive Pre260422

- last updated: 2026-04-22

## 목적
- `/Users/bhkim/Documents/codex_prj_sam_asset/세션` 폴더의 `handoff_26042201.md` 이전 handoff들을 하나로 통합한 archive 문서다.
- 통합 대상 원본 문서(현재 세션 폴더에서는 삭제됨):
  - `handoff_26041301.md`
  - `handoff_26041401.md`
  - `handoff_26041501.md`
  - `handoff_26041601.md`
  - `handoff_26042001.md`
- 기존 `handoff_consolidated_26042001.md`의 역할을 이 archive가 대체한다.
- `handoff_26042201.md`는 최신 실행 인계본이며, 다음 세션에서는 그 문서를 먼저 읽는다.
- 이 문서는 과거 handoff를 보관용으로 압축한 archive이면서, 새 세션에서 다시 확인해야 할 source of truth와 운영 제약도 함께 정리한다.

## 기준 경로
- 로컬 워크스페이스:
  - `/Users/bhkim/Documents/codex_prj_sam_asset`
- WAS repo:
  - `/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend`
- 로컬 venv:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/.venv/bin/python`
- WAS venv:
  - `/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend/.venv/bin/python`

## 현재 최종 상태
- 로컬은 2026-04-22 기준 아래 변경 묶음까지 반영된 상태다.
  - extractor 내부 `reason_code + reason_summary`
  - stage별 `reason_code` parser whitelist
  - `transfer_amount` post-validation reason 보강
  - deterministic duplicate-copy precheck
  - sequential LLM execution
  - `*_llm_metrics.json` metrics sidecar
  - 동양생명 active-family shortcut
- WAS는 2026-04-21 메트라이프 selective merge와 메트라이프 DB prompt parity 업데이트까지 반영된 상태다.
- 최신 공식 WAS direct validation 기준선은 2026-04-20 하나생명 XLSX 전환 라운드의 `17 PASS / 0 FAIL`이다.
- 최신 focused WAS validation은 2026-04-21 9건 검수 라운드의 `9 PASS / 0 FAIL`이다.
- 후속 금액 source-scale 보존 정책 변경 이후에는 focused output-contract 검증을 수행했고, 17-case direct validation은 아직 재실행하지 않았다.
- 메트라이프 추가 targeted 케이스는 현재 2건이다.
  - `18_메트라이프_추가설정`
  - `19_메트라이프_추가설정해지`
- 하나생명은 PDF 기준 계약에서 XLSX 기준 계약으로 전환됐다.
- 운영 기준 prompt 실제 변경 거래처는 현재 하나생명과 메트라이프 2건이다.
- 현재 기준 known actionable finding은 1건이다.
  - 동양생명 후반 stage shortcut은 적용 완료됐고, 잔여 지연은 `instruction_document`/`fund_inventory` transport retry 쪽이다.
- 2026-04-20 후속 작업에서 금액 최종 저장 포맷 정책을 다시 변경했다.
  - 기준은 “지시서에 표기된 소수 자릿수 유지”다.
  - 소수점 2자리 절삭/패딩은 backend에서 제거했고, UI에서 처리할 예정으로 본다.
  - `.00` 정수형 소수와 spreadsheet float-tail artifact 정리는 유지한다.
- 2026-04-21 code review에서 document-side amount parser가 탭/NBSP를 놓치는 결함 1건을 찾았고 수정했다.
  - 예: `KRW\t100`, `₩ 123`
  - 로컬 unittest와 WAS 메트라이프 3건 재검수까지 통과했다.
- 2026-04-22 로컬 live exact 기준:
  - `동양생명_20260318` -> `52건`, `issues=[]`, exact same
  - `동양생명_20260413` -> `48건`, `issues=[]`, exact same
  - `카디프_251127` -> `base_date=2025-11-27`, `46건`, exact same
- 2026-04-22 targeted live 5건 기준:
  - guard 3건 `OK`
  - 동양생명 2건 `OK`
- 동양생명 live elapsed:
  - `20260318` -> `85.335s`
  - `20260413` -> `85.267s`
- sidecar 기준 동양생명 2건의 `llm_batches_started`는 `instruction_document=1`, `fund_inventory=1`, `t_day=0`, `transfer_amount=0`이다.
- 최신 로컬 deterministic/unit/component 기준은 `303 tests OK`다.
- 현재 WAS repo는 clean이며, 금액 정책 변경분은 HEAD에 반영된 상태다.
  - 확인 기준 HEAD: `d7d82cc` (`병합된 PR 558: feat:변액일임 CSV 금액표시 수정`)
  - 직전 관련 HEAD: `51068e8` (`병합된 PR 557: feat:변액일임 지시서 금액 소수점 그대로 추출하도록 수정`)

## 절대 규칙
- 아래 WAS 파일은 특별한 명시 지시 없이는 수정하지 않는다.
  - `/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend/src/app/common/document/parser.py`
  - `/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend/src/app/config/settings.py`
  - `/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend/.env`
- WAS 거래처 prompt source of truth는 파일이 아니라 DB다.
  - `tb_variable_counterparties.prompt`
  - `use_counterparty_prompt`
  - `only_pending`
  - `designated_password`
  - `delivery_type`
- 로컬 source of truth는 repo 파일이다.
  - `/Users/bhkim/Documents/codex_prj_sam_asset/app`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/tests`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/app/prompts`
- `only_pending`은 extraction 의미가 아니라 최종 출력 직전 downstream post-processing 계약이다.
- prompt에는 내부 추출 규칙을 적고, `only_pending=true/false`를 내부 `settle_class`나 `order_type` 강제 변환 규칙으로 섞지 않는다.
- 데이터 추출 테스트 결과 보고서는 항상 아래 폴더에 저장한다.
  - `/Users/bhkim/Documents/codex_prj_sam_asset/test_report`
- 데이터 추출 테스트 보고서에는 항상 `retry 횟수`와 `추출 시간`을 포함한다.
- WAS 작업은 반드시 WAS venv로 수행한다.
- 로컬 ad-hoc 작업은 반드시 로컬 venv로 수행한다.

## 핵심 계약
- external API, payload schema, `DocumentLoadTaskPayload` shape는 변경하지 않는다.
- `reason_code + reason_summary`는 내부 metadata다.
- `*_llm_metrics.json`은 내부 debug artifact다.
- 내부 추출 exactness와 final payload exactness를 분리해서 본다.
- `only_pending=true` 거래처에서 loader `expected_order_count`와 final output row count의 단순 동일성은 보조 지표다.
- 대표 baseline은 extraction 가능 여부를 결정하지 않는다.
  - representative baseline은 acceptance comparator/source-of-truth sample이다.
  - baseline에 없는 문서도 추출 가능하다.
- accepted warning이 있다고 해서 DB 저장이 항상 불가한 것은 아니지만, 결과가 정확한데 오진단성 warning이 남으면 기능 품질 문제로 보고 cleanup한다.
- 거래처별 attachment precedence는 모델이 볼 수 있는 loader-visible marker에 기반해야 한다.
  - PDF cue 예: `## Page`, `[PAGE]`
  - XLSX cue 예: `## Sheet`, `[SHEET]`
  - 파일명이나 source label처럼 모델이 보지 못하는 cue에 의존하지 않는다.

## 테스트 수행 방법
- 로컬/WAS 테스트 수행 명령과 보고서 작성 기준은 별도 문서로 분리했다.
  - `/Users/bhkim/Documents/codex_prj_sam_asset/readme/03_테스트_실행_가이드.md`
- 핵심 원칙:
  - 로컬 테스트는 로컬 venv를 사용한다.
  - WAS 테스트는 WAS venv를 사용한다.
  - WAS 테스트 전 LLM `/models` 연결을 확인한다.
  - 데이터 추출 테스트 결과 보고서는 반드시 `test_report`에 저장한다.
  - 데이터 추출 테스트 결과 보고서에는 `retry_counts`와 `elapsed_seconds`를 포함한다.
  - `retry_counts`와 `elapsed_seconds`는 `*_llm_metrics.json`을 우선 읽고, sidecar가 없을 때만 `_llm_pipeline.log` fallback을 사용한다.

## 변경/주의 거래처별 현재 규칙
- 카디프:
  - authoritative 문서는 PDF다.
  - XLSX는 duplicate copy로 skip한다.
  - 내부 추출은 `CONFIRMED`다.
  - `only_pending=true`는 final output 후처리에서만 적용한다.
  - `output_contract.py`에서 카디프 `settle_class`를 강제 rewrite하지 않는다.
- IBK:
  - `base_date`는 메일 `Date`가 아니라 문서 본문 `기준일자`다.
  - `운용사`에 `삼성` 포함 row만 추출한다.
  - 주문 근거는 `정산액` 및 `예정 정산액 기준일+N` net column이다.
  - `설정액`/`해지액`은 주문 근거로 split하지 않는다.
  - 내부 expected order는 3건이고, `only_pending=true` final output은 1건이다.
- 흥국생명-hanais:
  - 이메일에 동일 내용의 XLSX와 PDF가 함께 올 수 있다.
  - XLSX만 추출하고 PDF는 duplicate copy로 skip한다.
  - `운용사명`에 `삼성` 포함 row만 추출한다.
  - `운용사 펀드코드`를 우선 사용하고 없으면 `펀드코드`를 사용한다.
  - `결제일` 우선, 없으면 `확정일`을 사용한다.
  - `only_pending=false`이므로 confirmed order를 confirmed로 유지한다.
  - 실제 PDF fixture는 고객사 사정으로 워크스페이스에 없으며, regression은 runtime-generated minimal PDF로 duplicate skip만 검증한다.
- 흥국생명-heungkuklife:
  - `fund_name`이 없는 표도 유지한다.
  - `추가설정금액`, `당일인출금액`, `해지신청`, `설정신청` amount label을 인식한다.
  - `비고` 기반 pending bucket을 계산한다.
  - `0.5억`, `0.8억` 같은 억 단위 금액을 파싱한다.
  - `7개 펀드` summary row는 제외한다.
  - 로컬 fast-path로 stage 4~7 retry를 피하도록 안정화했다.
- 하나생명:
  - authoritative instruction은 XLSX다.
  - 기준 문서:
    - `/Users/bhkim/Documents/codex_prj_sam_asset/document/하나생명-0415-지시서.xlsx`
  - legacy PDF는 `duplicate PDF copy; use XLSX attachment` 성격으로 reject한다.
  - 방향은 page-level이 아니라 row-level `거래유형명` 또는 normalized `구분`에서 판정한다.
  - 금액은 `설정해지금액` 계열 컬럼을 authoritative source로 사용한다.
  - `펀드납입출금액`, `판매회사분결제금액`은 보조/중복 evidence다.
  - `운용사회사명`에 `삼성` 포함 row만 추출한다.
  - expected internal orders는 7건이다.
  - `BBC180 SUB` 금액은 `22,684,941`이다.
- 메트라이프:
  - `17_메트라이프`는 official 17-case에 포함된다.
  - official 17-case 밖 targeted validation은 현재 2건이다.
    - `18_메트라이프_추가설정`: 1건, `base_date=2026-04-09`, `SUB`, final amount `12,082,790`, `issues=[]`
    - `19_메트라이프_추가설정해지`: 1건, `base_date=2026-04-08`, internal signed amount `-23,182,592`, final amount `23,182,592`, `issues=[]`
  - accepted comparator 파일명은 기존 산출물 이름을 유지한다.
    - 추가설정: `/Users/bhkim/Documents/codex_prj_sam_asset/output/debug/metlife_additional_sub_20260410_084740.json`
    - 추가설정해지: `/Users/bhkim/Documents/codex_prj_sam_asset/output/correct_result/18_메트라이프_추가설정해지.json`
  - 2026-04-21 이후 document-side amount 인식은 `₩`, `￦`, `KRW`, `원`, Unicode whitespace를 포함한 pure amount token까지 지원한다.
  - explicit redemption/outflow column에 printed minus amount가 있어도 actionable instruction evidence로 본다.
- KB:
  - true decimal 금액과 지시서 표기 소수 자릿수를 보존한다.
  - `50,572.49` 같은 실제 소수 금액을 `50,572`로 절삭하면 regression이다.
  - `23,213.40`을 `23,213.4`로 줄이면 regression이다.
  - `23,213.4`를 `23,213.40`으로 padding하면 regression이다.
  - `23,213.409`를 `23,213.40`으로 절삭하면 regression이다.
  - spreadsheet float-tail artifact만 정리한다.

## 금액 canonicalization
- “소수점 이하 제거”를 blanket truncation으로 해석하면 안 된다.
- 정책:
  - `70,000,000.00000001` 같은 spreadsheet float-tail은 integer artifact로 정리한다.
  - `18,711,858.00`처럼 소수부가 모두 0이면 정수 `18,711,858`로 정리한다.
  - `50,572.49`, `23,213.40`, `23,213.4`, `23,213.409` 같은 true decimal은 지시서 표기 소수 자릿수를 보존한다.
  - backend final payload 단계에서 소수 2자리 고정 출력, padding, truncation을 하지 않는다.
- canonical amount는 dedupe, signature, table hint key 같은 숫자 동등성 비교에 사용한다.
- final output amount는 source-scale preserving formatter를 사용한다.
  - 로컬: `app.amount_normalization.format_source_transfer_amount`
  - WAS: `app.services.variable_annuity.extract.amount_normalization.format_source_transfer_amount`
- `format_final_transfer_amount`는 현재 compatibility wrapper로 남아 있지만, 동작은 source-scale preserving formatter와 같다.
- 중복 제거를 단순 `fund_code` 기준으로 하면 정상 주문을 잃는다.
- 다른 펀드 정보가 동일하고 금액만 `23,213.40` / `23,213.4`처럼 소수 자릿수만 다른 경우는 숫자 기준 중복으로 처리한다.
- 중복 제거 후 남는 row의 금액 표기는 먼저 채택된 payload 표현을 유지한다.
- duplicate collapse identity는 일반적으로 아래 기준을 사용한다.
  - `date + fund_code + order_type + amount`

## Loader / Extractor 핵심 구현 상태
- loader:
  - decorated count-summary row를 제외한다.
  - mixed/multi-line header를 segment 단위로 normalize한다.
  - `거래유형`, `거래유형명`을 order-context 및 grouping context로 인식한다.
  - row-context가 있는 표에서는 authoritative mixed amount column만 order bucket으로 유지한다.
  - 하나생명형 표에서 `설정해지금액`이 있으면 sibling amount column 때문에 중복 bucket이 생기지 않게 suppress한다.
  - legacy XLS direct BIFF와 parser fallback을 보존한다.
  - `_normalize_pipe_cell`, `_is_amount_string`, `_parse_numeric_amount_text`는 공통 document-side amount token 정규화를 사용한다.
  - document-side amount token 정규화는 `₩`, `￦`, `KRW`, `원`, Unicode whitespace를 제거한 뒤 pure numeric token만 허용한다.
- extractor:
  - base_date는 문서 단일 fan-out 구조를 우선 사용한다.
  - deterministic 우선순위는 문서별 구조에 맞춰 조정했다.
  - section NAV Date가 Document Date에 덮이지 않도록 보정했다.
  - AIA BUY & SELL REPORT에서는 transaction-row date가 `Date (Asia/Seoul)`보다 우선할 수 있다.
  - generic `펀드` fallback은 explicit `운용사코드`/manager code보다 뒤에 와야 한다.
  - row-context direction deterministic recovery를 지원한다.
  - `ORDER_COVERAGE_ESTIMATE_MISMATCH`, fund ambiguity, manager warning 등 오진단성 warning cleanup을 보수적으로 수행한다.
  - prompt/response log, `Trace task_id`, retry 정책, 상태/사유 정규화는 WAS에서 보존한다.
  - deterministic amount parser도 loader와 동일한 currency-decorated amount token 정규화를 공유한다.
  - `instruction_document` stage prompt는 explicit negative redemption/outflow printed amount를 actionable evidence로 본다.
- output contract:
  - 메트라이프 최종 저장 정규화를 유지한다.
  - 흥국생명-heungkuklife 정렬/정규화를 유지한다.
  - 카디프는 sort만 하고 `settle_class`를 강제 rewrite하지 않는다.

## 로컬 주요 변경 파일
- extractor / loader / output:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/app/extractor.py`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loader.py`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/excel_loader.py`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/app/output_contract.py`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/app/amount_normalization.py`
- prompts / mapping:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/app/prompts/extraction_prompts.yaml`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/app/prompts/counterparty_prompt_map.yaml`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/app/prompts/IBK.txt`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/app/prompts/흥국생명-hanais.txt`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/app/prompts/카디프.txt`
  - 하나생명 prompt는 운영 배포용 prompt 변경 목록 섹션에 별도 명시한다.
- tests:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/tests/test_service_guard.py`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/tests/test_counterparty_live_regression.py`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/tests/test_output_contract.py`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/tests/test_document_loader_markdown.py`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/tests/test_extractor_logic.py`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/tests/test_component.py`
- 이번 미병합 로컬 변경 묶음은 아래 다섯 축으로 보면 된다.
  - extractor 내부 reason metadata
  - duplicate-copy precheck
  - sequential LLM execution
  - metrics sidecar
  - 동양생명 active-family shortcut

## WAS 최신 병합 상태
- 2026-04-20 후속 금액 source-scale 보존 정책 반영 파일:
  - `/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend/src/app/services/variable_annuity/extract/amount_normalization.py`
  - `/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend/src/app/services/variable_annuity/extract/output_contract.py`
  - `/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend/src/app/services/variable_annuity/extract/extractor.py`
  - `/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend/src/app/services/variable_annuity/tasks/llm/pipeline.py`
- 위 라운드에서는 `parser.py`, `settings.py`, `.env`, DB prompt row를 변경하지 않았다.
- WAS pipeline dedupe key는 `canonicalize_transfer_amount()`를 사용해 `23,213.40`과 `23,213.4`를 같은 금액으로 본다.
- 2026-04-20 하나생명 XLSX 전환 라운드의 WAS 제품 코드 추가 반영 파일:
  - `/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend/src/app/services/variable_annuity/extract/document_loader.py`
  - `/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend/src/app/services/variable_annuity/extract/extractor.py`
- 2026-04-21 메트라이프 selective merge 반영 파일:
  - `/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend/src/app/services/variable_annuity/extract/document_loader.py`
  - `/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend/src/app/services/variable_annuity/extract/extractor.py`
  - `/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend/src/app/services/variable_annuity/extract/prompts/extraction_prompts.yaml`
- 2026-04-21 메트라이프 selective merge에서는 `parser.py`, `settings.py`, `.env`, `output_contract.py`를 건드리지 않았다.
- 2026-04-22 기준 최신 로컬 변경 묶음은 아직 WAS에 selective merge하지 않았다.
  - 우선 병합 후보는 extractor의 내부 reason metadata, duplicate-copy precheck, sequential execution, metrics sidecar, 동양생명 active-family shortcut이다.
  - duplicate-copy guard의 source of truth는 prompt가 아니라 extractor 코드 helper다.
  - metrics sidecar는 로컬 debug artifact이며, WAS 이식은 선택적으로 판단한다.
- WAS 테스트 bootstrap 관련 변경:
  - `/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend/tests/conftest.py`
- `tests/conftest.py`는 기존 fixture/function logic을 수정하거나 새 함수를 추가한 것이 아니다.
  - stale import path만 `app.shared...`에서 `app.common...`으로 보정했다.
- `tests/test_variable_annuity_extract_helpers.py`는 2026-04-20 검증 당시 재생성했던 helper regression source다.
  - 현재 WAS repo 재확인 기준으로는 source file이 남아 있지 않다.
  - 다음 세션에서 helper regression을 다시 실행해야 하면 manifest의 `created_files` 기록을 기준으로 재생성 또는 복구가 필요하다.
- 2026-04-20 하나생명 병합 라운드에서는 `output_contract.py`가 조건부 대상이었지만 수정하지 않았다.
  - `11_카디프`, `14_흥국생명`, `17_메트라이프` final payload drift가 재현되지 않았기 때문이다.
  - 이후 금액 source-scale 보존 정책 라운드에서는 `output_contract.py`를 수정했다.
- 2026-04-16 라운드에서 반영한 금액 canonicalization 및 output contract 계열 WAS 병합 내역은 `merge_result_report_26041601.md`를 기준으로 확인한다.
- 2026-04-20 백업과 보고서 경로는 보고서와 산출물 섹션에 모아 둔다.

## WAS DB Prompt 상태
- 2026-04-16에는 WAS DB prompt 17건 parity 업데이트를 수행했다.
- 2026-04-20에는 하나생명 prompt만 실제 변경됐다.
- 2026-04-21에는 메트라이프 prompt 1건을 실제 변경했다.
- 하나생명 prompt SHA:
  - before: `3eafcccb24c46bf86ebd2bc462ad870a0e1684c57f238a7ec81319986f8d426f`
  - after/local: `a58c63b49c8c96231e7dab49673a895ebc63957c41e67bb7af4978651ddf7299`
- 보존해야 할 하나생명 DB fields:
  - `use_counterparty_prompt=true`
  - `only_pending=true`
  - `designated_password=null`
  - `delivery_type=null`
- 메트라이프 prompt SHA:
  - before: `bda7b2b55fefb8ac892fa13519d89a5e3bab714075c462892512b5ac6e9ad31b`
  - after/local: `b14b5b77220342b2735fd75396ce387957683297122b60abcccff483515e8d5f`
  - after/local length: `2775`
- 보존해야 할 메트라이프 DB fields:
  - `use_counterparty_prompt=true`
  - `only_pending=true`
  - `designated_password=null`
  - `delivery_type=null`
- `흥국생명-hanis` DB company name과 `흥국생명-hanais` file/case naming 차이는 실제 운영 데이터 차이다.
  - 무심코 rename하지 않는다.

## 운영 배포용 Prompt 변경 목록
- 운영 배포 시 실제 내용이 변경된 거래처 prompt source file과 DB row는 현재 아래 2건이다.
  - `/Users/bhkim/Documents/codex_prj_sam_asset/app/prompts/하나생명.txt`
  - `tb_variable_counterparties.company_name='하나생명'`
- `/Users/bhkim/Documents/codex_prj_sam_asset/app/prompts/메트라이프생명.txt`
- `tb_variable_counterparties.company_name='메트라이프생명'`
- `counterparty_prompt_map.yaml`은 이번 운영 prompt 변경 대상이 아니다.
- report에 prompt parity가 더 넓게 기록되어 있어도 실제 runtime 변경 row는 하나생명과 메트라이프 2건이다.

## 검증 이력 요약
- 2026-04-13:
  - 로컬 대표 회귀 14건 모두 정확 추출
  - 흥국생명 exact same
  - 카디프/동양생명 단독 exact same
- 2026-04-14:
  - WAS 14건 direct extraction `14/14 COMPLETED`
  - baseline accuracy `14/14`
  - 원문 검수 `14/14`
  - 최종 `PASS`
- 2026-04-15:
  - 로컬 deterministic suite `Ran 62 tests`, `OK (skipped=7)`
  - targeted live regression에서 카디프, IBK, 흥국생명-hanais 통과
  - full gated live suite 시도는 LLM endpoint connection error로 product regression 근거에서 제외
- 2026-04-16:
  - 로컬 17거래처 검수 최종 `17 PASS / 0 FAIL`
  - total retry count `4`
  - longest extraction `17_메트라이프 / 51.877s`
  - WAS 병합 acceptance `PASS`
- 2026-04-20 하나생명 XLSX 전환 라운드:
  - LLM `/models` 200
  - LLM `/chat/completions` 200
  - WAS helper regression `7 passed`
  - WAS direct validation `17 PASS / 0 FAIL`
  - WAS pipeline smoke `3/3`, `all_pass=true`
  - legacy case id `18_메트라이프_추가설정해지` PASS
    - 현재 targeted case id 기준으로는 `19_메트라이프_추가설정해지`에 해당한다.
  - 하나생명 legacy PDF reject PASS
  - DB prompt parity `17/17`
- 2026-04-20 후속 금액 source-scale 보존 정책 검증:
  - 로컬 py_compile 통과
  - 로컬 전체 unittest: `425 tests OK`, `skipped=10`
  - WAS `git diff --check` 통과
  - WAS py_compile 통과
  - WAS focused pytest: `tests/test_variable_annuity_output_contract.py`, `4 passed`
    - 이 테스트 파일은 검증 당시 사용된 focused test source이며, 현재 WAS repo에는 source file로 남아 있지 않다.
    - 재검증이 필요하면 동일 케이스를 새 focused test나 ad-hoc harness로 재생성해야 한다.
  - WAS pytest의 Pydantic/deprecation warning 12건은 기존 import 경로 warning이며 이번 변경 failure가 아니다.
- 2026-04-21 메트라이프 selective merge:
  - WAS `/v1/models` `200`
  - WAS py_compile 통과
  - YAML parse 통과
  - 메트라이프 DB prompt parity 업데이트 완료
  - `17_메트라이프`, `18_메트라이프_추가설정`, `19_메트라이프_추가설정해지` exact same PASS
- 2026-04-21 WAS 9건 검수:
  - 최종 `9 PASS / 0 FAIL`
  - 메트라이프 3건 포함 9건 모두 `COMPLETED`
  - `04_IM` baseline drift는 `file_name/source_path`의 Unicode NFC/NFD 차이뿐이며 주문 데이터는 exact same
  - `13_한화생명`은 compact date evidence `기준일 : 20250826`를 원문 재검수로 확인했다
- 2026-04-21 code review 후속 수정:
  - document-side amount parser가 탭/NBSP를 놓치던 결함 1건 수정
  - 로컬 `tests.test_document_loader_markdown`, `tests.test_extractor_logic` `321 tests OK`
  - WAS 메트라이프 3건 재검수 PASS
- 2026-04-22 로컬 동양생명/카디프 재검증:
  - `동양생명_20260318` exact same, `52건`, `issues=[]`
  - `동양생명_20260413` exact same, `48건`, `issues=[]`
  - `카디프_251127` exact same, `46건`, `base_date=2025-11-27`
  - 결론: `base_date` 문서 단일 fan-out은 정상 동작, 남은 병목은 동양생명 `t_day` retry 반복

## 최신 17-case
- official local/WAS acceptance 대표 케이스:
  - `01_ABL`
  - `02_AIA`
  - `03_DB`
  - `04_IM`
  - `05_KB`
  - `06_KDB`
  - `07_교보생명`
  - `08_동양생명`
  - `09_라이나`
  - `10_신한라이프`
  - `11_카디프`
  - `12_하나생명`
  - `13_한화생명`
  - `14_흥국생명`
  - `15_IBK`
  - `16_흥국생명-hanais`
  - `17_메트라이프`
- 아래 메트라이프 targeted case 2건은 official 17-case에 포함하지 않는다.
  - `18_메트라이프_추가설정`
  - `19_메트라이프_추가설정해지`

## 최신 2026-04-20 Direct Validation 주요 케이스 결과
| Case | Orders | Issues | Retry | Elapsed | Comparator |
| --- | ---: | --- | --- | ---: | --- |
| `12_하나생명` | 7 | `[]` | `{}` | `6.97s` | baseline exact same |
| `03_DB` | 3 | `[]` | `{}` | `6.66s` | baseline exact same |
| `09_라이나` | 10 | `[]` | `{}` | `37.48s` | baseline exact same |
| `13_한화생명` | 6 | `[]` | `{}` | `5.7s` | baseline exact same |
| `15_IBK` | 1 | `[]` | `{}` | `12.99s` | baseline exact same |
| `17_메트라이프` | 14 | `[]` | `{"t_day": 4}` | `70.6s` | baseline exact same |
| `18_메트라이프_추가설정해지` | 1 | `[]` | `{}` | `5.67s` | exact same, 당시 legacy case id이며 현재 targeted case id 기준으로는 `19_메트라이프_추가설정해지` |

## 최신 2026-04-21 WAS 9건 검수 결과
| Case | Orders | Issues | Retry | Elapsed | Verdict |
| --- | ---: | --- | --- | ---: | --- |
| `17_메트라이프` | 14 | `[]` | `{"t_day": 2}` | `47.687s` | `PASS` |
| `18_메트라이프_추가설정` | 1 | `[]` | `{}` | `4.951s` | `PASS` |
| `19_메트라이프_추가설정해지` | 1 | `[]` | `{}` | `5.132s` | `PASS` |
| `01_ABL` | 4 | `[]` | `{}` | `3.017s` | `PASS` |
| `03_DB` | 3 | `[]` | `{}` | `2.941s` | `PASS` |
| `04_IM` | 8 | `[]` | `{}` | `16.662s` | `PASS` |
| `07_교보생명` | 2 | `[]` | `{}` | `8.958s` | `PASS` |
| `12_하나생명` | 7 | `[]` | `{}` | `3.204s` | `PASS` |
| `13_한화생명` | 6 | `[]` | `{}` | `1.983s` | `PASS` |
- `04_IM`은 baseline exact compare에서 `file_name/source_path`의 Unicode normalization 차이만 있었다.
- `13_한화생명`은 자동 evidence 탐지 초기 결과와 달리 원문 `기준일 : 20250826` 확인 후 최종 `PASS`로 확정했다.

## 보고서와 산출물
- 2026-04-14 WAS direct extraction:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/was_counterparty_direct_report_260414140106.md`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/was_counterparty_direct_report_260414140106_detailed.md`
- 2026-04-16 로컬 17거래처 review:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/output/debug/local_counterparty_review_20260416_101947`
- 2026-04-16 WAS 병합 보고서:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/merge_result_report_26041601.md`
- 2026-04-20 WAS 병합 계획서:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/merge_plan_20260420_01.md`
- 2026-04-20 WAS 병합 보고서:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/merge_result_report_20260420_01.md`
- 2026-04-20 데이터 추출 테스트 보고서:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260420_WAS_병합_데이터추출_테스트_보고서.md`
- 2026-04-21 메트라이프 WAS 병합 테스트 보고서:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260421_WAS_메트라이프_병합_데이터추출_테스트_보고서.md`
- 2026-04-21 WAS 9건 데이터 추출 검수 보고서:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260421_WAS_9건_데이터추출_검수_보고서.md`
- 거래처별 최대 2건 데이터 추출 테스트 보고서:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260416_거래처별_최대2건_추출테스트_보고서.md`
- 2026-04-20 accepted 병합 debug root:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/output/debug/was_merge_20260420_01`
- 2026-04-20 accepted 병합 backup root:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/output/was_backup/was_merge_20260420_01`
- 2026-04-20 prompt update report:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/output/debug/was_counterparty_prompt_update_20260420_01/update_report.json`
- 2026-04-21 메트라이프 WAS merge debug root:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/output/debug/was_merge_20260421_01`
- 2026-04-21 메트라이프 WAS backup root:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/output/was_backup/was_merge_20260421_01`
- 2026-04-21 prompt update report:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/output/debug/was_counterparty_prompt_update_20260421_01/update_report.json`
- 2026-04-21 WAS 9건 validation root:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/output/debug/was_validation_20260421_01`

## 현재 WAS Repo 상태
- 2026-04-21 기준 WAS repo working tree는 clean이다.
- `git status --short --untracked-files=all` 출력은 비어 있다.
- 금액 source-scale 보존 정책 변경분은 현재 HEAD에 반영되어 있다.
- 2026-04-21 메트라이프 selective merge 반영분과 9건 검수 산출물은 repo 밖 debug/report 경로에 저장돼 있다.
- `tests/test_variable_annuity_output_contract.py`는 검증 당시 사용된 focused test였지만 현재 WAS repo에는 존재하지 않는다.
- 이전 handoff 작성 당시 보였던 `scripts/git-no-askpass.sh` untracked 항목도 현재 존재하지 않는다.

## 현재 로컬 Repo 상태
- 2026-04-22 기준 로컬 repo working tree는 dirty다.
- 현재 수정/추가 파일:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/README.md`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loader.py`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/eml_loader.py`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/html_loader.py`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loaders/mht_loader.py`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/app/extractor.py`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/app/prompts/extraction_prompts.yaml`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/app/prompts/메트라이프생명.txt`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/readme/00_시작_안내.md`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/readme/01_시스템_구성_가이드.md`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/readme/02_WAS_병합_가이드.md`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/readme/03_테스트_실행_가이드.md`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/tests/test_counterparty_live_regression.py`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/tests/test_document_loader_markdown.py`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/tests/test_extractor_logic.py`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/tests/test_service_guard.py`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/세션/handoff_archive_pre260422.md`
  - `/Users/bhkim/Documents/codex_prj_sam_asset/세션/handoff_26042201.md`
- 이 변경들은 메트라이프 추가설정/추가설정해지 오판단 수정, 동양생명 `t_day` retry 정합성/성능 보강, `Settings.llm_*` runtime wiring 보강, 회귀 테스트 추가, 운영 문서 최신화에 해당한다.
- 새 세션에서 로컬 작업을 이어갈 때는 이 변경을 임의로 revert하지 말고 현재 diff와 2026-04-22 handoff를 함께 본다.

## 다음 세션 시작 순서
1. `/Users/bhkim/Documents/codex_prj_sam_asset/세션/handoff_26042201.md`를 먼저 읽는다.
2. 이 통합 handoff를 읽는다.
3. 필요한 경우 원본 handoff는 세부 이력 확인용으로만 연다.
4. 로컬 repo에서 `git status --short --untracked-files=all`을 확인하고, 최신 로컬 변경 묶음을 merge-first 기준으로 정리한다.
5. WAS repo에서 `git status --short --untracked-files=all`을 확인하고 clean 상태를 재확인한다.
6. LLM 터널을 확인한다.
7. 로컬 변경을 WAS에 selective merge한다.
8. guard 3건 + 동양생명 2건 focused validation을 수행한다.
9. 배포 전 운영 DB row를 백업한다.
10. 가능하면 official 17-case 또는 운영 가능한 subset smoke를 수행한다.
11. 모든 데이터 추출 결과 보고서에는 retry count와 elapsed time을 포함해 `test_report`에 저장한다.
12. merge가 끝난 뒤 refactor backlog를 정리한다.

## 현재 결론
- 공통 `base_date` fan-out 성능 개선은 현재 로컬에서 정상 동작 중이다.
- 동양생명/카디프 정확도는 현재 exact same 기준으로 통과 상태다.
- 동양생명 후반 stage shortcut은 적용 완료됐고, sidecar 기준 `t_day`/`transfer_amount`는 현재 targeted live에서 실행되지 않는다.
- 2026-04-21 메트라이프 selective merge, 메트라이프 DB prompt update, WAS 9건 검수까지는 완료됐다.
- 다음 큰 작업은 최신 로컬 변경 묶음의 WAS selective merge와 focused validation이다.
- 운영 prompt 변경 row는 현재 하나생명과 메트라이프 2건이다.
- WAS 코드 배포 scope는 현재 repo 상태를 먼저 확인한 뒤 금액 source-scale 보존 변경분, 하나생명 XLSX 전환 관련 변경분, 메트라이프 selective merge 변경분, 최신 로컬 변경 묶음, DB prompt 2건을 분리해서 판단한다.
