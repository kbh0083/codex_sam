# WAS 이관 대상 목록

- last updated: 2026-04-28

## 목적
- 현재 워크스페이스 `/Users/bhkim/Documents/codex_prj_sam_asset`는 아카이빙 예정이고, 후속 작업은 WAS 워크스페이스 `/Users/bhkim/10_project/01_samsung_asset`에서 이어간다.
- 이 문서는 **실제 이관 작업을 수행하는 문서가 아니라**, WAS에서 연속 작업이 가능하도록 **이관해야 할 문서/산출물 범위만 확정**하는 문서다.
- 기준 범위는 `운영 번들`이다. 전체 역사 보관이 아니라, active 운영/검수/병합/정답지 체인을 끊지 않기 위한 최소 충분 묶음을 다룬다.

## 이관 원칙
- 민감 문서도 포함한다.
- 실제 이관 루트는 `/Users/bhkim/10_project/01_samsung_asset/92_codex` 미러 구조를 전제로 한다.
- 실제 이관 단계에서는 현재 문서 안의 절대경로 `/Users/bhkim/Documents/codex_prj_sam_asset/...`를 `/Users/bhkim/10_project/01_samsung_asset/92_codex/...`로 함께 바꿔야 한다.
- WAS 쪽 기존 [91_Extractor_readme](/Users/bhkim/10_project/01_samsung_asset/91_Extractor_readme)는 historical 참고로 두고, active 기준 문서는 `92_codex`로 전환하는 것을 기본값으로 둔다.

## 필수 이관 대상

### 1. 소스 지시서 corpus
- [document](/Users/bhkim/Documents/codex_prj_sam_asset/document)
- 포함 범위: `.DS_Store`를 제외한 실제 지시서 `40건`

### 2. 정답지 corpus
- [거래처별_문서_정답](/Users/bhkim/Documents/codex_prj_sam_asset/거래처별_문서_정답)
- 포함 범위: source copy + canonical JSON + canonical CSV 전체
- 현재 범위: `16개` 거래처 디렉터리, `122개` 파일

### 3. 참고자료 corpus
- [참고자료](/Users/bhkim/Documents/codex_prj_sam_asset/참고자료)
- 포함 범위: `.DS_Store`를 제외한 전체 파일 `7건`
- 현재 포함 파일은 아래와 같다.
- [authoritative_answer_registry.json](/Users/bhkim/Documents/codex_prj_sam_asset/참고자료/authoritative_answer_registry.json)
- [photo_2026-04-27_20-16-17.jpg](/Users/bhkim/Documents/codex_prj_sam_asset/참고자료/photo_2026-04-27_20-16-17.jpg)
- [photo_2026-04-27_20-17-16.jpg](/Users/bhkim/Documents/codex_prj_sam_asset/참고자료/photo_2026-04-27_20-17-16.jpg)
- [메트라이프_펀드코드.xlsx](/Users/bhkim/Documents/codex_prj_sam_asset/참고자료/메트라이프_펀드코드.xlsx)
- [메트라이프생명_0427.csv](/Users/bhkim/Documents/codex_prj_sam_asset/참고자료/메트라이프생명_0427.csv)
- [운용지시서_KDB_20260427.csv](/Users/bhkim/Documents/codex_prj_sam_asset/참고자료/운용지시서_KDB_20260427.csv)
- [흥국생명_HKlife_0413_정답지.md](/Users/bhkim/Documents/codex_prj_sam_asset/참고자료/흥국생명_HKlife_0413_정답지.md)

### 4. 핵심 운영 readme 6종
- [00_시작_안내.md](/Users/bhkim/Documents/codex_prj_sam_asset/readme/00_시작_안내.md)
- [01_시스템_구성_가이드.md](/Users/bhkim/Documents/codex_prj_sam_asset/readme/01_시스템_구성_가이드.md)
- [02_WAS_병합_가이드.md](/Users/bhkim/Documents/codex_prj_sam_asset/readme/02_WAS_병합_가이드.md)
- [03_테스트_실행_가이드.md](/Users/bhkim/Documents/codex_prj_sam_asset/readme/03_테스트_실행_가이드.md)
- [04_SSH_접속_정보.md](/Users/bhkim/Documents/codex_prj_sam_asset/readme/04_SSH_접속_정보.md)
- [06_WAS_병합_보고서_가이드.md](/Users/bhkim/Documents/codex_prj_sam_asset/readme/06_WAS_병합_보고서_가이드.md)

### 5. 세션 handoff 8종
- [handoff_26042201.md](/Users/bhkim/Documents/codex_prj_sam_asset/세션/handoff_26042201.md)
- [handoff_26042401.md](/Users/bhkim/Documents/codex_prj_sam_asset/세션/handoff_26042401.md)
- [handoff_26042701.md](/Users/bhkim/Documents/codex_prj_sam_asset/세션/handoff_26042701.md)
- [handoff_26042702.md](/Users/bhkim/Documents/codex_prj_sam_asset/세션/handoff_26042702.md)
- [handoff_26042801.md](/Users/bhkim/Documents/codex_prj_sam_asset/세션/handoff_26042801.md)
- [handoff_26042802.md](/Users/bhkim/Documents/codex_prj_sam_asset/세션/handoff_26042802.md)
- [handoff_archive_260427.md](/Users/bhkim/Documents/codex_prj_sam_asset/세션/handoff_archive_260427.md)
- [handoff_archive_pre260422.md](/Users/bhkim/Documents/codex_prj_sam_asset/세션/handoff_archive_pre260422.md)

### 6. retained / canonical 보고서
- 병합 보고서
- [mp2024.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/20260424/mp2024.md)
- [mr2024.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/20260424/mr2024.md)
- [mp1925.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/20260427/mp1925.md)
- [mr1925.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/20260427/mr1925.md)
- [mp1310.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/20260428/mp1310.md)
- [mr1310.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/20260428/mr1310.md)
- 테스트 보고서
- [tt1230.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260424/tt1230.md)
- [tt1432.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260424/tt1432.md)
- [tt1520.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260424/tt1520.md)
- [tt1921.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260424/tt1921.md)
- [tt2024.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260424/tt2024.md)
- [tt2050.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260424/tt2050.md)
- [tt1358.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260427/tt1358.md)
- [tt1925.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260427/tt1925.md)
- [tt1013.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260428/tt1013.md)
- [tt1310.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260428/tt1310.md)
- [tt1413.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260428/tt1413.md)
- [tt1430.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260428/tt1430.md)

### 7. retained machine-readable 요약 산출물
- [preflight_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/output/test/20260428/141713/preflight/preflight_summary.json)
- [case_manifest.json](/Users/bhkim/Documents/codex_prj_sam_asset/output/test/20260428/141713/review/case_manifest.json)
- [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/output/test/20260428/141713/review/validation_summary.json)
- [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/output/test/20260428/130950/review/validation_summary.json)

## 권장 추가 대상
- [05_리팩토링_계획_extractor_document_loader.md](/Users/bhkim/Documents/codex_prj_sam_asset/readme/05_리팩토링_계획_extractor_document_loader.md)
- [90_설계_거래처_프로필_프리징.md](/Users/bhkim/Documents/codex_prj_sam_asset/readme/90_설계_거래처_프로필_프리징.md)

## 이번 목록에서 제외
- `/Users/bhkim/Documents/codex_prj_sam_asset/document/.DS_Store`
- `/Users/bhkim/Documents/codex_prj_sam_asset/참고자료/.DS_Store`
- non-retained exploratory 보고서
  - `tt0911.md`
  - `tt0923.md`
  - `tt2023.md`
- full case artifact tree
  - `output/test/**/cases/**`
  - `output/test/**/debug/**`
  - `output/test/**/handoff/**`

## 실제 이관 시 배치 기본값
- `/Users/bhkim/10_project/01_samsung_asset/92_codex/readme/`
- `/Users/bhkim/10_project/01_samsung_asset/92_codex/세션/`
- `/Users/bhkim/10_project/01_samsung_asset/92_codex/merge_report/`
- `/Users/bhkim/10_project/01_samsung_asset/92_codex/test_report/`
- `/Users/bhkim/10_project/01_samsung_asset/92_codex/참고자료/`
- `/Users/bhkim/10_project/01_samsung_asset/92_codex/document/`
- `/Users/bhkim/10_project/01_samsung_asset/92_codex/거래처별_문서_정답/`
- `/Users/bhkim/10_project/01_samsung_asset/92_codex/output/test/20260428/141713/`
- `/Users/bhkim/10_project/01_samsung_asset/92_codex/output/test/20260428/130950/`
