# Novita Qwen3.6 지시서 추출 파라미터 테스트 보고서

- 실행 일시: `2026-05-14 16:23:01 KST`
- run id: `novita_qwen36_variable_data_162015`
- overall verdict: `REVIEW`
- model: `qwen/qwen3.6-27b`
- endpoint: `https://api.novita.ai/openai/v1/chat/completions`
- cycles: `2`
- artifact root: [novita_qwen36_variable_data_162015_artifacts](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_162015_artifacts>)
- deterministic 보강 사용 여부: `false`
- API key 저장 여부: `false`

## 테스트 대상

| Case | 거래처 | Source | Expected | Base date | Strategy |
| --- | --- | --- | ---: | --- | --- |
| `33_운용지시서_KDB생명_20260427_eml` | KDB생명 | [운용지시서(KDB생명)_20260427.eml](</Users/bhkim/Documents/codex_prj_sam_asset/document/운용지시서(KDB생명)_20260427.eml>) | `12` | `2026-04-27` | `single_prompt` |
| `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | 한화생명 | [삼성자산_한화생명_설정_해지_내역서_20251128.eml](</Users/bhkim/Documents/codex_prj_sam_asset/document/삼성자산_한화생명_설정_해지_내역서_20251128.eml>) | `3` | `2025-11-28` | `single_prompt` |
| `12_동양생명_20260318_html` | 동양생명 | [동양생명_20260318.html](</Users/bhkim/Documents/codex_prj_sam_asset/document/동양생명_20260318.html>) | `40` | `2026-03-18` | `dongyang_two_stage` |
| `37_카디프_251127_pdf` | 카디프생명 | [카디프_251127.pdf](</Users/bhkim/Documents/codex_prj_sam_asset/document/카디프_251127.pdf>) | `46` | `2025-11-27` | `single_prompt` |

## 파라미터 프로필

| Profile | enable_thinking | temperature | top_p | top_k | min_p | presence_penalty | repetition_penalty | max_tokens |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `thinking` | `true` | `0` | `1.0` | `20` | `0.0` | `0.0` | `1.0` | `32768` |
| `non_thinking` | `false` | `0` | `1.0` | `20` | `0.0` | `0.0` | `1.0` | `16384` |

## Verdict Summary

- PASS: `2`
- FAIL: `1`
- BLOCKED: `0`

| Profile | Total | PASS | FAIL | BLOCKED | Avg elapsed(s) | Avg total tokens |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `thinking` | `3` | `2` | `1` | `0` | `54.891` | `11733.7` |

## 실행 결과

| Cycle | Profile | Case | Verdict | Orders | Expected | JSON exact | CSV exact | Calls | Elapsed(s) | Tokens | Reasoning | Artifact |
| ---: | --- | --- | --- | ---: | ---: | --- | --- | ---: | ---: | ---: | --- | --- |
| `1` | `thinking` | `33_운용지시서_KDB생명_20260427_eml` | `PASS` | `12` | `12` | `True` | `True` | `1` | `42.405` | `8829` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_162015_artifacts/executions/cycle_01/thinking/33_운용지시서_KDB생명_20260427_eml>) |
| `1` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `PASS` | `3` | `3` | `True` | `True` | `2` | `60.568` | `13926` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_162015_artifacts/executions/cycle_01/thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml>) |
| `1` | `thinking` | `12_동양생명_20260318_html` | `FAIL` | `0` | `40` | `False` | `False` | `2` | `61.7` | `12446` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_162015_artifacts/executions/cycle_01/thinking/12_동양생명_20260318_html>) |

## LLM 처리 시간 및 Token Usage

| Cycle | Profile | Case | Calls | LLM elapsed(s) | Prompt tokens | Completion tokens | Total tokens | Finish reasons |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `1` | `thinking` | `33_운용지시서_KDB생명_20260427_eml` | `1` | `42.405` | `3361` | `5468` | `8829` | `{"extract_attempt_1": "stop"}` |
| `1` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `2` | `60.568` | `7392` | `6534` | `13926` | `{"extract_attempt_1": null, "extract_attempt_2": "stop"}` |
| `1` | `thinking` | `12_동양생명_20260318_html` | `2` | `61.7` | `5798` | `6648` | `12446` | `{"settlement_attempt_1": null, "settlement_attempt_2": null}` |

### 호출별 Usage

#### `1` / `thinking` / `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `extract_attempt_1` | `3696` | `3828` | `7524` |
| `extract_attempt_2` | `3696` | `2706` | `6402` |

#### `1` / `thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement_attempt_1` | `2899` | `3188` | `6087` |
| `settlement_attempt_2` | `2899` | `3460` | `6359` |


## 검토 필요 항목

### `1` / `thinking` / `12_동양생명_20260318_html`
- verdict: `FAIL`
- reason: `settlement JSON parse failed`
- analysis basis: `prompt-only result; no deterministic repair was applied`
- validation: [validation_summary.json](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_162015_artifacts/executions/cycle_01/thinking/12_동양생명_20260318_html/validation_summary.json>)
- missing orders: `0`
- extra orders: `0`

## Notes

- `response_format={"type":"json_object"}`와 Novita top-level `enable_thinking`/`separate_reasoning`을 요청마다 명시했다.
- HTTP 400 계열 파라미터 거부, 인증 오류, image input 미지원, timeout은 `BLOCKED`로 기록하며 파라미터를 제거해 재시도하지 않았다.
- JSON/CSV exact 비교는 status, base_date, issues, 주문 canonical field 기준으로 수행했다.
