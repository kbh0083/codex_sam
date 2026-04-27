# Handoff Archive 260427

- last updated: 2026-04-27

## 목적
- 이 문서는 `handoff_26042201.md`, `handoff_26042401.md`, `handoff_26042701.md` 세 문서를 한곳에 모아 보는 historical archive다.
- 현재 active handoff는 [handoff_26042702.md](/Users/bhkim/Documents/codex_prj_sam_asset/세션/handoff_26042702.md)다.
- 이 archive는 **현재 상태를 다시 쓰는 문서가 아니라**, 과거 active handoff들의 역할과 당시 판단을 보관하는 문서다.
- `26042201` 이전 장기 history는 계속 [handoff_archive_pre260422.md](/Users/bhkim/Documents/codex_prj_sam_asset/세션/handoff_archive_pre260422.md)를 본다.

## 통합 대상
- [handoff_26042201.md](/Users/bhkim/Documents/codex_prj_sam_asset/세션/handoff_26042201.md)
- [handoff_26042401.md](/Users/bhkim/Documents/codex_prj_sam_asset/세션/handoff_26042401.md)
- [handoff_26042701.md](/Users/bhkim/Documents/codex_prj_sam_asset/세션/handoff_26042701.md)

## Handoff 26042201

### 시점과 역할
- `2026-04-24` 직전 active handoff snapshot이다.
- HTML/MHT markdown 안정화 병합 직후, 로컬 full rerun과 canonical CSV 정리 결과를 다음 세션에 넘기기 위한 handoff였다.

### 당시 핵심 변경/검증/판단
- 가드레일을 명시했다.
  - `DocumentLoader` / `FundOrderExtractor` 코어에 거래처 하드코딩 분기 신규 추가 금지
  - 도메인 정책은 DB source of truth, prompt/config, `output_contract`, 상위 task/pipeline 계층에서 처리
- 당시 최신 로컬 검증 수치:
  - deterministic/unit suite `461 tests OK`
  - HTML/MHT focused `7 PASS / 0 FAIL / 0 BLOCKED` ([tt1230.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260424/tt1230.md))
  - `document` 전체 재검수 `38 PASS / 0 FAIL / 0 BLOCKED` ([tt1520.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260424/tt1520.md))
  - retained live regression `22 tests OK`
- full rerun 과정에서 legacy metadata-heavy answer CSV 13건을 canonical 7컬럼 형식으로 정리했다.

### 현재도 참고할 historical decision
- 정답지 JSON/CSV는 DB 저장용 최종 payload 기준으로 맞춘다.
- `거래처별_문서_정답`, `output/test`, `test_report`는 gitignored artifact라 새 clone에서 자동 복원되지 않는다.

## Handoff 26042401

### 시점과 역할
- `2026-04-24` 종료 시점 active handoff snapshot이다.
- canonical selective merge 직후의 WAS 반영 범위와 acceptance 결과를 전달하기 위한 handoff였다.

### 당시 핵심 변경/검증/판단
- 로컬 source of truth에는 두 규칙 묶음이 있었다.
  - 거래처 prompt structured marker 기반 fixed-column contract
  - 당시 기준 `동양생명`, `한화생명`, `신한라이프`의 final serialized `t_day=="02"` 제거 계약
- WAS selective merge 범위:
  - `extractor.py`
  - `output_contract.py`
  - `tasks/llm/pipeline.py`
  - `extract/extraction/*`
  - `extract/prompts/extraction_prompts.yaml`
  - `tests/test_variable_annuity_extract_helpers.py`
  - `tests/test_variable_annuity_output_contract.py`
- extractor LLM hardcode:
  - `llm_temperature=0.0`
  - `llm_max_tokens=16384`
- DB prompt sync:
  - `company_name='동양생명'`
  - `company_name='IBK연금보험'`
- 당시 acceptance:
  - helper regression `26 passed`
  - affected exact compare `5 PASS / 0 FAIL / 0 BLOCKED`
  - direct full review `38 PASS / 0 FAIL / 0 BLOCKED`
  - pipeline smoke `2 PASS / 0 FAIL / 0 BLOCKED`
  - canonical result: [mr2024.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/20260424/mr2024.md)

### 현재도 참고할 historical decision
- WAS source of truth는 DB row다.
- `only_pending`은 추출 중간 규칙이 아니라 최종 직렬화 직전 후처리다.
- canonical selective merge history는 지금도 [mr2024.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/20260424/mr2024.md)와 [tt2050.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260424/tt2050.md)가 기준이다.

## Handoff 26042701

### 시점과 역할
- `2026-04-27`에 작성된 첫 통합 active handoff였다.
- `2026-04-24` canonical baseline과 그 직후 로컬/WAS 후속 작업을 한 문서에 함께 실어 둔 snapshot이다.
- 현재는 [handoff_26042702.md](/Users/bhkim/Documents/codex_prj_sam_asset/세션/handoff_26042702.md)로 active 역할이 넘어갔다.

### 당시 핵심 변경/검증/판단
- 로컬 source of truth 측 변경:
  - structured marker 기반 `fixed_stage_columns` contract
  - `동양생명` / `IBK` same-day `정산액` 제외 contract
  - 당시 기준 final serialized `t_day=="02"` 제거 계약
  - prompt marker stripping bug fix와 regression test 추가
- 검증:
  - latest retained local full review [tt1921.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260424/tt1921.md) `38 PASS / 0 FAIL / 0 BLOCKED`
  - latest retained WAS full review [tt2050.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260424/tt2050.md) `38 PASS / 0 FAIL / 0 BLOCKED`
  - post-merge targeted regression `27 passed`
  - focused regression `7 passed`
- WAS 후속 수정:
  - pipeline filtered-empty reason을 `추출된 주문 데이터 없음`으로 정렬
  - plain DB company name `흥국생명`도 heungkuk canonical output normalization을 타도록 수정

### 현재도 참고할 historical decision
- `DocumentLoader -> DocumentLoadTaskPayload JSON -> FundOrderExtractor -> output_contract` 흐름이 authoritative runtime path다.
- local prompt source of truth와 WAS DB source of truth를 분리해서 봐야 한다.
- tunnel `3910`, `3900`, `5432`는 세션 간 지속되지 않으므로 매 세션 재확인이 필요하다.

## 현재 기준으로 superseded 되었지만 맥락상 중요한 운영 제약
- 거래처별 신규 정책은 extractor core가 아니라 prompt/DB config, `output_contract`, 상위 task/pipeline 계층에 둔다.
- WAS prompt/설정 값은 DB row가 authoritative source다.
- historical report의 과거 verdict와 수치는 현재 기준과 다르게 보여도, history 자체를 덮어쓰지 않는다.
- gitignored 산출물은 report/history의 일부이지만, 새 clone이나 다른 머신에서는 직접 복사하거나 재생성해야 한다.

## 현재 문서 사용 순서
1. 현재 세션/최신 상태는 [handoff_26042702.md](/Users/bhkim/Documents/codex_prj_sam_asset/세션/handoff_26042702.md)를 먼저 읽는다.
2. 이 archive는 “왜 이런 기준선이 생겼는지”가 필요할 때만 참고한다.
3. `26042201` 이전 장기 history가 필요하면 [handoff_archive_pre260422.md](/Users/bhkim/Documents/codex_prj_sam_asset/세션/handoff_archive_pre260422.md)를 이어서 본다.
