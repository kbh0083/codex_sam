# Novita Qwen3.5 지시서 추출 파라미터 테스트 보고서

- 실행 일시: `2026-05-14 15:05:47 KST`
- run id: `novita_qwen35_variable_data_145515`
- overall verdict: `PASS`
- model: `qwen/qwen3.5-397b-a17b`
- endpoint: `https://api.novita.ai/openai/v1/chat/completions`
- cycles: `2`
- artifact root: [novita_qwen35_variable_data_145515_artifacts](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_145515_artifacts)
- deterministic 보강 사용 여부: `false`
- API key 저장 여부: `false`

## 테스트 대상

| Case | 거래처 | Source | Expected | Base date | Strategy |
| --- | --- | --- | ---: | --- | --- |
| `33_운용지시서_KDB생명_20260427_eml` | KDB생명 | [운용지시서(KDB생명)_20260427.eml](</Users/bhkim/Documents/codex_prj_sam_asset/document/운용지시서(KDB생명)_20260427.eml>) | `12` | `2026-04-27` | `single_prompt` |
| `14_라이나_250826_xlsx` | 라이나생명 | [라이나_250826.xlsx](/Users/bhkim/Documents/codex_prj_sam_asset/document/라이나_250826.xlsx) | `10` | `2025-08-26` | `single_prompt` |
| `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | 한화생명 | [삼성자산_한화생명_설정_해지_내역서_20251128.eml](/Users/bhkim/Documents/codex_prj_sam_asset/document/삼성자산_한화생명_설정_해지_내역서_20251128.eml) | `3` | `2025-11-28` | `single_prompt` |
| `12_동양생명_20260318_html` | 동양생명 | [동양생명_20260318.html](/Users/bhkim/Documents/codex_prj_sam_asset/document/동양생명_20260318.html) | `40` | `2026-03-18` | `dongyang_two_stage` |
| `37_카디프_251127_pdf` | 카디프생명 | [카디프_251127.pdf](/Users/bhkim/Documents/codex_prj_sam_asset/document/카디프_251127.pdf) | `46` | `2025-11-27` | `single_prompt` |

## 파라미터 프로필

| Profile | enable_thinking | temperature | top_p | top_k | min_p | presence_penalty | repetition_penalty | max_tokens |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `thinking` | `true` | `0.6` | `0.95` | `20` | `0.0` | `0.0` | `1.0` | `32768` |
| `non_thinking` | `false` | `0` | `1.0` | `20` | `0.0` | `0.0` | `1.0` | `16384` |

## Verdict Summary

- PASS: `20`
- FAIL: `0`
- BLOCKED: `0`

| Profile | Total | PASS | FAIL | BLOCKED | Avg elapsed(s) | Avg total tokens |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `non_thinking` | `10` | `10` | `0` | `0` | `31.311` | `6790.1` |
| `thinking` | `10` | `10` | `0` | `0` | `31.759` | `6374.1` |

## 실행 결과

| Cycle | Profile | Case | Verdict | Orders | Expected | JSON exact | CSV exact | Calls | Elapsed(s) | Tokens | Reasoning | Artifact |
| ---: | --- | --- | --- | ---: | ---: | --- | --- | ---: | ---: | ---: | --- | --- |
| `1` | `thinking` | `33_운용지시서_KDB생명_20260427_eml` | `PASS` | `12` | `12` | `True` | `True` | `1` | `20.279` | `4462` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_145515_artifacts/executions/cycle_01/thinking/33_운용지시서_KDB생명_20260427_eml) |
| `1` | `thinking` | `14_라이나_250826_xlsx` | `PASS` | `10` | `10` | `True` | `True` | `1` | `12.99` | `4132` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_145515_artifacts/executions/cycle_01/thinking/14_라이나_250826_xlsx) |
| `1` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `PASS` | `3` | `3` | `True` | `True` | `1` | `8.757` | `4050` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_145515_artifacts/executions/cycle_01/thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml) |
| `1` | `thinking` | `12_동양생명_20260318_html` | `PASS` | `40` | `40` | `True` | `True` | `2` | `37.897` | `9263` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_145515_artifacts/executions/cycle_01/thinking/12_동양생명_20260318_html) |
| `1` | `thinking` | `37_카디프_251127_pdf` | `PASS` | `46` | `46` | `True` | `True` | `1` | `71.071` | `9965` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_145515_artifacts/executions/cycle_01/thinking/37_카디프_251127_pdf) |
| `1` | `non_thinking` | `33_운용지시서_KDB생명_20260427_eml` | `PASS` | `12` | `12` | `True` | `True` | `1` | `24.73` | `4465` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_145515_artifacts/executions/cycle_01/non_thinking/33_운용지시서_KDB생명_20260427_eml) |
| `1` | `non_thinking` | `14_라이나_250826_xlsx` | `PASS` | `10` | `10` | `True` | `True` | `1` | `15.783` | `4135` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_145515_artifacts/executions/cycle_01/non_thinking/14_라이나_250826_xlsx) |
| `1` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `PASS` | `3` | `3` | `True` | `True` | `1` | `9.75` | `4053` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_145515_artifacts/executions/cycle_01/non_thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml) |
| `1` | `non_thinking` | `12_동양생명_20260318_html` | `PASS` | `40` | `40` | `True` | `True` | `2` | `45.754` | `9756` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_145515_artifacts/executions/cycle_01/non_thinking/12_동양생명_20260318_html) |
| `1` | `non_thinking` | `37_카디프_251127_pdf` | `PASS` | `46` | `46` | `True` | `True` | `1` | `64.506` | `9968` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_145515_artifacts/executions/cycle_01/non_thinking/37_카디프_251127_pdf) |
| `2` | `thinking` | `33_운용지시서_KDB생명_20260427_eml` | `PASS` | `12` | `12` | `True` | `True` | `1` | `23.682` | `4462` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_145515_artifacts/executions/cycle_02/thinking/33_운용지시서_KDB생명_20260427_eml) |
| `2` | `thinking` | `14_라이나_250826_xlsx` | `PASS` | `10` | `10` | `True` | `True` | `1` | `15.619` | `4132` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_145515_artifacts/executions/cycle_02/thinking/14_라이나_250826_xlsx) |
| `2` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `PASS` | `3` | `3` | `True` | `True` | `1` | `10.087` | `4050` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_145515_artifacts/executions/cycle_02/thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml) |
| `2` | `thinking` | `12_동양생명_20260318_html` | `PASS` | `40` | `40` | `True` | `True` | `2` | `44.04` | `9260` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_145515_artifacts/executions/cycle_02/thinking/12_동양생명_20260318_html) |
| `2` | `thinking` | `37_카디프_251127_pdf` | `PASS` | `46` | `46` | `True` | `True` | `1` | `73.173` | `9965` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_145515_artifacts/executions/cycle_02/thinking/37_카디프_251127_pdf) |
| `2` | `non_thinking` | `33_운용지시서_KDB생명_20260427_eml` | `PASS` | `12` | `12` | `True` | `True` | `1` | `21.568` | `4465` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_145515_artifacts/executions/cycle_02/non_thinking/33_운용지시서_KDB생명_20260427_eml) |
| `2` | `non_thinking` | `14_라이나_250826_xlsx` | `PASS` | `10` | `10` | `True` | `True` | `1` | `12.086` | `7282` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_145515_artifacts/executions/cycle_02/non_thinking/14_라이나_250826_xlsx) |
| `2` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `PASS` | `3` | `3` | `True` | `True` | `1` | `9.304` | `4053` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_145515_artifacts/executions/cycle_02/non_thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml) |
| `2` | `non_thinking` | `12_동양생명_20260318_html` | `PASS` | `40` | `40` | `True` | `True` | `2` | `46.42` | `9756` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_145515_artifacts/executions/cycle_02/non_thinking/12_동양생명_20260318_html) |
| `2` | `non_thinking` | `37_카디프_251127_pdf` | `PASS` | `46` | `46` | `True` | `True` | `1` | `63.211` | `9968` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_145515_artifacts/executions/cycle_02/non_thinking/37_카디프_251127_pdf) |

## LLM 처리 시간 및 Token Usage

| Cycle | Profile | Case | Calls | LLM elapsed(s) | Prompt tokens | Completion tokens | Total tokens | Finish reasons |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `1` | `thinking` | `33_운용지시서_KDB생명_20260427_eml` | `1` | `20.279` | `3086` | `1376` | `4462` | `{"extract": "stop"}` |
| `1` | `thinking` | `14_라이나_250826_xlsx` | `1` | `12.99` | `3069` | `1063` | `4132` | `{"extract": "stop"}` |
| `1` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `8.757` | `3566` | `484` | `4050` | `{"extract": "stop"}` |
| `1` | `thinking` | `12_동양생명_20260318_html` | `2` | `37.897` | `6723` | `2540` | `9263` | `{"settlement": "stop", "forecast": "stop"}` |
| `1` | `thinking` | `37_카디프_251127_pdf` | `1` | `71.071` | `5496` | `4469` | `9965` | `{"extract": "stop"}` |
| `1` | `non_thinking` | `33_운용지시서_KDB생명_20260427_eml` | `1` | `24.73` | `3089` | `1376` | `4465` | `{"extract": "stop"}` |
| `1` | `non_thinking` | `14_라이나_250826_xlsx` | `1` | `15.783` | `3072` | `1063` | `4135` | `{"extract": "stop"}` |
| `1` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `9.75` | `3569` | `484` | `4053` | `{"extract": "stop"}` |
| `1` | `non_thinking` | `12_동양생명_20260318_html` | `2` | `45.754` | `6729` | `3027` | `9756` | `{"settlement": "stop", "forecast": "stop"}` |
| `1` | `non_thinking` | `37_카디프_251127_pdf` | `1` | `64.506` | `5499` | `4469` | `9968` | `{"extract": "stop"}` |
| `2` | `thinking` | `33_운용지시서_KDB생명_20260427_eml` | `1` | `23.682` | `3086` | `1376` | `4462` | `{"extract": "stop"}` |
| `2` | `thinking` | `14_라이나_250826_xlsx` | `1` | `15.619` | `3069` | `1063` | `4132` | `{"extract": "stop"}` |
| `2` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `10.087` | `3566` | `484` | `4050` | `{"extract": "stop"}` |
| `2` | `thinking` | `12_동양생명_20260318_html` | `2` | `44.04` | `6722` | `2538` | `9260` | `{"settlement": "stop", "forecast": "stop"}` |
| `2` | `thinking` | `37_카디프_251127_pdf` | `1` | `73.173` | `5496` | `4469` | `9965` | `{"extract": "stop"}` |
| `2` | `non_thinking` | `33_운용지시서_KDB생명_20260427_eml` | `1` | `21.568` | `3089` | `1376` | `4465` | `{"extract": "stop"}` |
| `2` | `non_thinking` | `14_라이나_250826_xlsx` | `1` | `12.086` | `6219` | `1063` | `7282` | `{"extract": "stop"}` |
| `2` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `9.304` | `3569` | `484` | `4053` | `{"extract": "stop"}` |
| `2` | `non_thinking` | `12_동양생명_20260318_html` | `2` | `46.42` | `6729` | `3027` | `9756` | `{"settlement": "stop", "forecast": "stop"}` |
| `2` | `non_thinking` | `37_카디프_251127_pdf` | `1` | `63.211` | `5499` | `4469` | `9968` | `{"extract": "stop"}` |

### 호출별 Usage

#### `1` / `thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement` | `2747` | `1435` | `4182` |
| `forecast` | `3976` | `1105` | `5081` |

#### `1` / `non_thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement` | `2750` | `1435` | `4185` |
| `forecast` | `3979` | `1592` | `5571` |

#### `2` / `thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement` | `2747` | `1434` | `4181` |
| `forecast` | `3975` | `1104` | `5079` |

#### `2` / `non_thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement` | `2750` | `1435` | `4185` |
| `forecast` | `3979` | `1592` | `5571` |

## Notes

- `response_format={"type":"json_object"}`와 Novita top-level `enable_thinking`/`separate_reasoning`을 요청마다 명시했다.
- HTTP 400 계열 파라미터 거부, 인증 오류, image input 미지원, timeout은 `BLOCKED`로 기록하며 파라미터를 제거해 재시도하지 않았다.
- JSON/CSV exact 비교는 status, base_date, issues, 주문 canonical field 기준으로 수행했다.
