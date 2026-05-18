# Novita Qwen3.6 지시서 추출 파라미터 테스트 보고서

- 실행 일시: `2026-05-14 16:18:17 KST`
- run id: `novita_qwen36_variable_data_161214`
- overall verdict: `REVIEW`
- model: `qwen/qwen3.6-27b`
- endpoint: `https://api.novita.ai/openai/v1/chat/completions`
- cycles: `2`
- artifact root: [novita_qwen36_variable_data_161214_artifacts](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_161214_artifacts>)
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

- PASS: `5`
- FAIL: `1`
- BLOCKED: `0`

| Profile | Total | PASS | FAIL | BLOCKED | Avg elapsed(s) | Avg total tokens |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `non_thinking` | `2` | `1` | `1` | `0` | `19.86` | `4350.0` |
| `thinking` | `4` | `4` | `0` | `0` | `80.681` | `15011.5` |

## 실행 결과

| Cycle | Profile | Case | Verdict | Orders | Expected | JSON exact | CSV exact | Calls | Elapsed(s) | Tokens | Reasoning | Artifact |
| ---: | --- | --- | --- | ---: | ---: | --- | --- | ---: | ---: | ---: | --- | --- |
| `1` | `thinking` | `33_운용지시서_KDB생명_20260427_eml` | `PASS` | `12` | `12` | `True` | `True` | `1` | `44.164` | `7888` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_161214_artifacts/executions/cycle_01/thinking/33_운용지시서_KDB생명_20260427_eml>) |
| `1` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `PASS` | `3` | `3` | `True` | `True` | `1` | `33.767` | `6552` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_161214_artifacts/executions/cycle_01/thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml>) |
| `1` | `thinking` | `12_동양생명_20260318_html` | `PASS` | `40` | `40` | `True` | `True` | `3` | `132.084` | `26674` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_161214_artifacts/executions/cycle_01/thinking/12_동양생명_20260318_html>) |
| `1` | `thinking` | `37_카디프_251127_pdf` | `PASS` | `46` | `46` | `True` | `True` | `1` | `112.71` | `18932` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_161214_artifacts/executions/cycle_01/thinking/37_카디프_251127_pdf>) |
| `1` | `non_thinking` | `33_운용지시서_KDB생명_20260427_eml` | `FAIL` | `11` | `12` | `False` | `False` | `1` | `33.75` | `4521` | `False` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_161214_artifacts/executions/cycle_01/non_thinking/33_운용지시서_KDB생명_20260427_eml>) |
| `1` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `PASS` | `3` | `3` | `True` | `True` | `1` | `5.969` | `4179` | `False` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_161214_artifacts/executions/cycle_01/non_thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml>) |

## LLM 처리 시간 및 Token Usage

| Cycle | Profile | Case | Calls | LLM elapsed(s) | Prompt tokens | Completion tokens | Total tokens | Finish reasons |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `1` | `thinking` | `33_운용지시서_KDB생명_20260427_eml` | `1` | `44.164` | `3248` | `4640` | `7888` | `{"extract_attempt_1": "stop"}` |
| `1` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `33.767` | `3696` | `2856` | `6552` | `{"extract_attempt_1": "stop"}` |
| `1` | `thinking` | `12_동양생명_20260318_html` | `3` | `132.084` | `11177` | `15497` | `26674` | `{"settlement_attempt_1": "stop", "forecast_attempt_1": null, "forecast_attempt_2": "stop"}` |
| `1` | `thinking` | `37_카디프_251127_pdf` | `1` | `112.71` | `5592` | `13340` | `18932` | `{"extract_attempt_1": "stop"}` |
| `1` | `non_thinking` | `33_운용지시서_KDB생명_20260427_eml` | `1` | `33.75` | `3252` | `1269` | `4521` | `{"extract_attempt_1": "stop"}` |
| `1` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `5.969` | `3700` | `479` | `4179` | `{"extract_attempt_1": "stop"}` |

### 호출별 Usage

#### `1` / `thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement_attempt_1` | `2899` | `4325` | `7224` |
| `forecast_attempt_1` | `4139` | `4046` | `8185` |
| `forecast_attempt_2` | `4139` | `7126` | `11265` |


## 검토 필요 항목

### `1` / `non_thinking` / `33_운용지시서_KDB생명_20260427_eml`
- verdict: `FAIL`
- reason: `validation mismatch`
- analysis basis: `prompt-only result; no deterministic repair was applied`
- validation: [validation_summary.json](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_161214_artifacts/executions/cycle_01/non_thinking/33_운용지시서_KDB생명_20260427_eml/validation_summary.json>)
- missing orders: `1`
- extra orders: `0`
- missing sample: `[{"base_date": "2026-04-27", "fund_code": "492007", "fund_name": "액티브배당성장70혼합형", "order_type": "3", "settle_class": "1", "t_day": "02", "transfer_amount": "6587078"}]`

## Notes

- `response_format={"type":"json_object"}`와 Novita top-level `enable_thinking`/`separate_reasoning`을 요청마다 명시했다.
- HTTP 400 계열 파라미터 거부, 인증 오류, image input 미지원, timeout은 `BLOCKED`로 기록하며 파라미터를 제거해 재시도하지 않았다.
- JSON/CSV exact 비교는 status, base_date, issues, 주문 canonical field 기준으로 수행했다.
