# `extractor.py` / `document_loader.py` 리팩토링 계획

## 목적

- [app/document_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loader.py)와 [app/extractor.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extractor.py)의 공개 import 경로를 유지한다.
- 큰 파일의 책임을 내부 패키지로 분리해 읽기/수정 단위를 줄인다.
- 동작 변경 없이 WAS mirror가 가능한 구조를 만든다.
- 이번 라운드에서는 로컬 구조 정리까지만 수행하고, 실제 WAS 병합은 하지 않는다.
- 최신 active handoff는 [handoff_26042701.md](/Users/bhkim/Documents/codex_prj_sam_asset/세션/handoff_26042701.md)다.
- 2026-04-24 기준으로 `document_loading/` 분리 구조 위에 HTML/MHT markdown 안정화가 추가 반영됐다.
  - `html_loader.py`, `mht_loader.py`, `document_loading/common.py`, `loader_core.py`, `markdown.py`가 이 구조 위에서 함께 움직인다.

## 가드레일

- 중요 지침
  - `DocumentLoader`와 `FundOrderExtractor` 코어 런타임에는 거래처명, alias, 특정 문서군 이름, 특정 안내 문구에 직접 의존하는 하드코딩 분기를 새로 추가하지 않는다.
  - 도메인 정책은 DB source of truth, 프롬프트/설정, `output_contract`, 상위 task/pipeline 계층에서만 다룬다.
  - `DocumentLoader`의 삼성 대상 scope 계산처럼 남아 있는 레거시 업무 계약성 로직은 현재 구현 사실로만 취급하고, 신규 리팩토링에서 확대하지 않는다.

- 유지 대상 public symbol
  - `app.document_loader`: `DocumentLoader`, `DocumentLoadTaskPayload`, `TargetFundScope`, `ExtractedDocumentText`, `normalize_fund_name_key`
  - `app.extractor`: `FundOrderExtractor`, `LLMExtractionOutcome`, `ExtractionOutcomeError`, `apply_only_pending_filter`, `write_invalid_response_debug_files`, `build_extract_llm_log_path`, `BLOCKING_EXTRACTION_ISSUES`
  - `app.extraction`: `load_counterparty_guidance`, `resolve_counterparty_prompt_name`, `detect_counterparty_guidance_non_instruction_reason`
- 유지 대상 계약
  - `DocumentLoadTaskPayload` JSON shape
  - `FundOrderExtractor.extract_from_task_payload()` 시그니처/의미
  - handler A/B 흐름
  - `only_pending`, output contract, sequential LLM execution
  - duplicate/legacy copy는 extractor core 하드코딩 pre-skip 계약으로 승격하지 않고, 최종 `SKIPPED`는 상위 정책/입력/판정 결과로 surface되게 유지

## 구조 계획

### 1. DocumentLoader

- 파사드 유지
  - [app/document_loader.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loader.py)는 진입점과 public re-export 역할만 유지한다.
- 내부 패키지 분리
  - [app/document_loading/dto.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loading/dto.py)
  - [app/document_loading/loader_core.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loading/loader_core.py)
  - [app/document_loading/markdown.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loading/markdown.py)
  - [app/document_loading/coverage.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loading/coverage.py)
  - [app/document_loading/scope.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/document_loading/scope.py)
- standalone 복사 기준
  - 현재 기준: `document_loader.py + document_loading/ + document_loaders/`

### 2. FundOrderExtractor

- 파사드 유지
  - [app/extractor.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extractor.py)는 `FundOrderExtractor` public entry를 유지한다.
- 내부 패키지 분리
  - [app/extraction/constants.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extraction/constants.py)
  - [app/extraction/models.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extraction/models.py)
  - [app/extraction/prompts.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extraction/prompts.py)
  - [app/extraction/counterparty.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extraction/counterparty.py)
  - [app/extraction/guidance.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extraction/guidance.py)
  - [app/extraction/telemetry.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/extraction/telemetry.py)
- 후속 분리 예정
  - `pipeline.py`
  - `deterministic.py`
  - `reconcile.py`

## 2026-04-23 적용 상태

- 완료
  - `document_loading/`, `extraction/` 내부 패키지 생성
- `DocumentLoader` DTO/public symbol 분리
- `DocumentLoader` markdown/coverage 로직을 내부 mixin으로 이동
- `DocumentLoaderCommonMixin` 추가
- `document_loader.py`를 facade-only class body로 정렬
- `FundOrderExtractor` 상수/model/prompt/counterparty/telemetry 로직 분리
- `detect_counterparty_guidance_non_instruction_reason()` helper 추가
- duplicate/legacy copy의 prompt-directed `SKIPPED`는 service/component 경계로 이동
- extractor core의 거래처 하드코딩 duplicate pre-skip 계약은 유지 대상에서 제외
- standalone 테스트를 새 복사 계약 기준으로 갱신
- AST 기반 standalone/facade 구조 테스트 추가
- 미완료
  - `FundOrderExtractor` method-level 세부 분해는 1차 분리만 적용
  - `pipeline.py`, `deterministic.py`, `reconcile.py`는 후속 라운드에서 class method 이관 예정

## 2026-04-24 추가 메모

- 이번 세션에서 WAS selective merge는 구조 정렬 묶음 전체가 아니라 HTML/MHT markdown 안정화 범위만 수행했다.
- 즉 현재 구조 상태는 “facade/내부 패키지 분리 완료 + HTML/MHT audit/fail-safe 보강 완료 + extractor method-level 추가 분해는 아직 계획 상태”로 본다.

## 검증 기준

- 로컬 회귀
  - `tests.test_document_loader_markdown`
  - `tests.test_document_loader_standalone`
  - `tests.test_extractor_logic`
  - `tests.test_service_guard`
  - `tests.test_component`
- 체크 포인트
  - public import 경로 유지
  - `build_task_payload()` / `extract_from_task_payload()` 동작 유지
  - standalone 복사본에서 `DocumentLoader` import 및 `build_task_payload()` 호출 가능

## WAS 메모

- mirror 기준은 [readme/02_WAS_병합_가이드.md](/Users/bhkim/Documents/codex_prj_sam_asset/readme/02_WAS_병합_가이드.md)에 반영한다.
- 이번 라운드에서는 사용자 지시에 따라 WAS repo 수정은 수행하지 않는다.
