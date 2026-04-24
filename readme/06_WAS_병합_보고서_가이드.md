# WAS 병합 보고서 가이드

## 목적
- 이 문서는 WAS 병합 계획서와 결과보고서의 단일 작성 기준이다.
- `03_테스트_실행_가이드.md`가 테스트 산출물/검수 보고서를 다루고, 이 문서는 병합 자체의 계획/결과 기록을 다룬다.
- 새 병합 문서와 기존 legacy 병합 문서 정리는 반드시 이 규칙을 따른다.
- 최신 active handoff는 [handoff_26042401.md](/Users/bhkim/Documents/codex_prj_sam_asset/세션/handoff_26042401.md)다.
- 2026-04-24 현재 canonical migration은 적용 완료 상태다.
  - 최신 병합 결과 예시: [mr0001.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/20260424/mr0001.md)
  - 최신 full test 예시: [tt1520.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260424/tt1520.md)

## 핵심 규칙
- 병합당 Markdown 문서는 `계획서 1건 + 결과보고서 1건`만 유지한다.
- 병합 계획서는 `merge_report/YYYYMMDD/mpHHMM.md`에 저장한다.
- 병합 결과보고서는 `merge_report/YYYYMMDD/mrHHMM.md`에 저장한다.
- 파일명 길이 제한은 `.md` 확장자 제외 `20자 이내`다.
- 실제 시각이 없는 historical/legacy 병합은 `HHMM=00NN`을 사용한다.
  - 예: 1회차 `mp0001.md`, `mr0001.md`
- 병합당 상세 부록은 새로 만들지 않는다.
- 같은 병합의 `merge_plan_*`, `merge_report_*`, `merge_result_report_*`, `*_detailed.md`는 canonical 계획서 또는 결과보고서로 흡수한다.
- 병합 검증 성격 문서는 `merge_report`에 두지 않는다.
  - `counterparty_local_was_*`, `was_counterparty_direct_*` 같은 문서는 `test_report/YYYYMMDD/ttHHMM.md`로 이관한다.
- 과거 문서를 정리할 때는 기존 verdict, 수치, 날짜, 세션 ID를 바꾸지 않는다.
- 과거 문서에 원래 없던 계획서는 새로 만들지 않는다.

## 경로 규칙
- 병합 계획서:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/YYYYMMDD/mpHHMM.md`
- 병합 결과보고서:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/YYYYMMDD/mrHHMM.md`
- 병합 검증 테스트 보고서:
  - `/Users/bhkim/Documents/codex_prj_sam_asset/test_report/YYYYMMDD/ttHHMM.md`
- 새 canonical 문서를 만들면 관련 `readme`, `세션`, `merge_report`, `test_report` 내부 링크도 함께 갱신한다.

## 계획서 필수 섹션
- `개요`
- `병합 범위`
- `반영 대상`
- `제외/보존`
- `검증 계획`
- `산출물 링크`
- 신규 canonical 계획서는 위 제목을 그대로 사용한다.
- historical retrofit 문서는 기존 legacy heading을 유지할 수 있다.
  - 단, 위 6개 정보는 본문 어디에 있는지 명확히 남아 있어야 한다.

## 결과보고서 필수 섹션
- `개요`
- `병합 범위`
- `변경 파일`
- `Preflight`
- `검증 결과`
- `이슈/대응`
- `최종 판정`
- `산출물 링크`
- 신규 canonical 결과보고서는 위 제목을 그대로 사용한다.
- historical retrofit 문서는 기존 legacy heading을 유지할 수 있다.
  - 단, 위 8개 정보는 본문 어디에 있는지 명확히 남아 있어야 한다.

## legacy 정리 규칙
- canonical 계획서는 해당 병합의 기존 계획 관련 문서를 하나로 합친 결과물이다.
- canonical 결과보고서는 해당 병합의 기존 결과/상세/보강 문서를 하나로 합친 결과물이다.
- 흡수된 기존 파일명은 canonical 문서 본문에 남겨 추적 가능하게 한다.
- 과거 판단이 현재 기준과 달라 보여도, 이번 정리 작업에서는 history를 덮어쓰지 않는다.
- 단, 경로/링크/파일명은 최신 canonical 경로로 정리한다.
- historical 보고서의 산출물 링크는 `output/`, `output/debug/`, DB dump 같은 로컬 비추적 artifact를 가리킬 수 있다.
  - 다른 머신이나 새 clone에서 해당 파일이 없더라도, 보고서 history 자체를 잘못된 것으로 재판정하지 않는다.

## 테스트 보고서와의 경계
- 병합 계획/결과는 이 문서를 따른다.
- 검수/전수대조/smoke/focused rerun 같은 테스트 보고서는 `03_테스트_실행_가이드.md`를 따른다.
- 같은 검수 목적의 반복 실행은 테스트 쪽 canonical 보고서 1건으로 합치고, retained 실행 기준과 재시도 이력을 본문에 남긴다.

## 금지 규칙
- 병합 1건에 결과보고서를 여러 건 남기지 않는다.
- 같은 병합의 legacy 보고서를 별도 파일로 계속 유지하지 않는다.
- `merge_report` 루트에 날짜 디렉터리 없이 새 파일을 직접 만들지 않는다.
- 길이가 긴 descriptive slug 파일명을 새로 만들지 않는다.
- 병합 검증 테스트 결과를 `merge_report`에 계속 쌓지 않는다.
