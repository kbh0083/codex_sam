# Novita Qwen3.6 지시서 추출 파라미터 테스트 보고서

- 실행 일시: `2026-05-14 15:38:54 KST`
- run id: `novita_qwen36_variable_data_152808`
- overall verdict: `REVIEW`
- model: `qwen/qwen3.6-27b`
- endpoint: `https://api.novita.ai/openai/v1/chat/completions`
- cycles: `2`
- artifact root: [novita_qwen36_variable_data_152808_artifacts](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts)
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

- PASS: `11`
- FAIL: `9`
- BLOCKED: `0`

| Profile | Total | PASS | FAIL | BLOCKED | Avg elapsed(s) | Avg total tokens |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `non_thinking` | `10` | `6` | `4` | `0` | `21.043` | `6363.0` |
| `thinking` | `10` | `5` | `5` | `0` | `43.399` | `8481.4` |

## 실행 결과

| Cycle | Profile | Case | Verdict | Orders | Expected | JSON exact | CSV exact | Calls | Elapsed(s) | Tokens | Reasoning | Artifact |
| ---: | --- | --- | --- | ---: | ---: | --- | --- | ---: | ---: | ---: | --- | --- |
| `1` | `thinking` | `33_운용지시서_KDB생명_20260427_eml` | `PASS` | `12` | `12` | `True` | `True` | `1` | `38.843` | `7284` | `True` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_01/thinking/33_운용지시서_KDB생명_20260427_eml) |
| `1` | `thinking` | `14_라이나_250826_xlsx` | `PASS` | `10` | `10` | `True` | `True` | `1` | `24.823` | `5873` | `True` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_01/thinking/14_라이나_250826_xlsx) |
| `1` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `PASS` | `3` | `3` | `True` | `True` | `1` | `38.029` | `7746` | `True` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_01/thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml) |
| `1` | `thinking` | `12_동양생명_20260318_html` | `FAIL` | `40` | `40` | `False` | `False` | `2` | `77.023` | `15855` | `True` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_01/thinking/12_동양생명_20260318_html) |
| `1` | `thinking` | `37_카디프_251127_pdf` | `FAIL` | `0` | `46` | `False` | `False` | `1` | `78.573` | `13512` | `True` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_01/thinking/37_카디프_251127_pdf) |
| `1` | `non_thinking` | `33_운용지시서_KDB생명_20260427_eml` | `FAIL` | `11` | `12` | `False` | `False` | `1` | `14.67` | `4352` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_01/non_thinking/33_운용지시서_KDB생명_20260427_eml) |
| `1` | `non_thinking` | `14_라이나_250826_xlsx` | `PASS` | `10` | `10` | `True` | `True` | `1` | `10.125` | `4124` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_01/non_thinking/14_라이나_250826_xlsx) |
| `1` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `PASS` | `3` | `3` | `True` | `True` | `1` | `9.518` | `4042` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_01/non_thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml) |
| `1` | `non_thinking` | `12_동양생명_20260318_html` | `FAIL` | `40` | `40` | `False` | `False` | `2` | `24.956` | `9339` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_01/non_thinking/12_동양생명_20260318_html) |
| `1` | `non_thinking` | `37_카디프_251127_pdf` | `PASS` | `46` | `46` | `True` | `True` | `1` | `43.719` | `9957` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_01/non_thinking/37_카디프_251127_pdf) |
| `2` | `thinking` | `33_운용지시서_KDB생명_20260427_eml` | `PASS` | `12` | `12` | `True` | `True` | `1` | `49.353` | `7679` | `True` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_02/thinking/33_운용지시서_KDB생명_20260427_eml) |
| `2` | `thinking` | `14_라이나_250826_xlsx` | `PASS` | `10` | `10` | `True` | `True` | `1` | `35.822` | `5818` | `True` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_02/thinking/14_라이나_250826_xlsx) |
| `2` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `FAIL` | `0` | `3` | `False` | `False` | `1` | `24.743` | `5464` | `True` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_02/thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml) |
| `2` | `thinking` | `12_동양생명_20260318_html` | `FAIL` | `0` | `40` | `False` | `False` | `1` | `29.751` | `6400` | `True` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_02/thinking/12_동양생명_20260318_html) |
| `2` | `thinking` | `37_카디프_251127_pdf` | `FAIL` | `0` | `46` | `False` | `False` | `1` | `37.031` | `9183` | `True` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_02/thinking/37_카디프_251127_pdf) |
| `2` | `non_thinking` | `33_운용지시서_KDB생명_20260427_eml` | `FAIL` | `11` | `12` | `False` | `False` | `1` | `12.369` | `4352` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_02/non_thinking/33_운용지시서_KDB생명_20260427_eml) |
| `2` | `non_thinking` | `14_라이나_250826_xlsx` | `PASS` | `10` | `10` | `True` | `True` | `1` | `17.178` | `4124` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_02/non_thinking/14_라이나_250826_xlsx) |
| `2` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `PASS` | `3` | `3` | `True` | `True` | `1` | `8.455` | `4042` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_02/non_thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml) |
| `2` | `non_thinking` | `12_동양생명_20260318_html` | `FAIL` | `40` | `40` | `False` | `False` | `2` | `22.911` | `9341` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_02/non_thinking/12_동양생명_20260318_html) |
| `2` | `non_thinking` | `37_카디프_251127_pdf` | `PASS` | `46` | `46` | `True` | `True` | `1` | `46.529` | `9957` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_02/non_thinking/37_카디프_251127_pdf) |

## LLM 처리 시간 및 Token Usage

| Cycle | Profile | Case | Calls | LLM elapsed(s) | Prompt tokens | Completion tokens | Total tokens | Finish reasons |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `1` | `thinking` | `33_운용지시서_KDB생명_20260427_eml` | `1` | `38.843` | `3079` | `4205` | `7284` | `{"extract": "stop"}` |
| `1` | `thinking` | `14_라이나_250826_xlsx` | `1` | `24.823` | `3062` | `2811` | `5873` | `{"extract": "stop"}` |
| `1` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `38.029` | `3559` | `4187` | `7746` | `{"extract": "stop"}` |
| `1` | `thinking` | `12_동양생명_20260318_html` | `2` | `77.023` | `6719` | `9136` | `15855` | `{"settlement": "stop", "forecast": "stop"}` |
| `1` | `thinking` | `37_카디프_251127_pdf` | `1` | `78.573` | `5489` | `8023` | `13512` | `{"extract": "stop"}` |
| `1` | `non_thinking` | `33_운용지시서_KDB생명_20260427_eml` | `1` | `14.67` | `3083` | `1269` | `4352` | `{"extract": "stop"}` |
| `1` | `non_thinking` | `14_라이나_250826_xlsx` | `1` | `10.125` | `3066` | `1058` | `4124` | `{"extract": "stop"}` |
| `1` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `9.518` | `3563` | `479` | `4042` | `{"extract": "stop"}` |
| `1` | `non_thinking` | `12_동양생명_20260318_html` | `2` | `24.956` | `6742` | `2597` | `9339` | `{"settlement": "stop", "forecast": "stop"}` |
| `1` | `non_thinking` | `37_카디프_251127_pdf` | `1` | `43.719` | `5493` | `4464` | `9957` | `{"extract": "stop"}` |
| `2` | `thinking` | `33_운용지시서_KDB생명_20260427_eml` | `1` | `49.353` | `3079` | `4600` | `7679` | `{"extract": "stop"}` |
| `2` | `thinking` | `14_라이나_250826_xlsx` | `1` | `35.822` | `3062` | `2756` | `5818` | `{"extract": "stop"}` |
| `2` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `24.743` | `3559` | `1905` | `5464` | `{"extract": "stop"}` |
| `2` | `thinking` | `12_동양생명_20260318_html` | `1` | `29.751` | `2745` | `3655` | `6400` | `{"settlement": null}` |
| `2` | `thinking` | `37_카디프_251127_pdf` | `1` | `37.031` | `5489` | `3694` | `9183` | `{"extract": "stop"}` |
| `2` | `non_thinking` | `33_운용지시서_KDB생명_20260427_eml` | `1` | `12.369` | `3083` | `1269` | `4352` | `{"extract": "stop"}` |
| `2` | `non_thinking` | `14_라이나_250826_xlsx` | `1` | `17.178` | `3066` | `1058` | `4124` | `{"extract": "stop"}` |
| `2` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `8.455` | `3563` | `479` | `4042` | `{"extract": "stop"}` |
| `2` | `non_thinking` | `12_동양생명_20260318_html` | `2` | `22.911` | `6742` | `2599` | `9341` | `{"settlement": "stop", "forecast": "stop"}` |
| `2` | `non_thinking` | `37_카디프_251127_pdf` | `1` | `46.529` | `5493` | `4464` | `9957` | `{"extract": "stop"}` |

### 호출별 Usage

#### `1` / `thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement` | `2745` | `4412` | `7157` |
| `forecast` | `3974` | `4724` | `8698` |

#### `1` / `non_thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement` | `2749` | `1470` | `4219` |
| `forecast` | `3993` | `1127` | `5120` |

#### `2` / `non_thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement` | `2749` | `1470` | `4219` |
| `forecast` | `3993` | `1129` | `5122` |


## 검토 필요 항목

### `1` / `thinking` / `12_동양생명_20260318_html`
- verdict: `FAIL`
- reason: `validation mismatch`
- analysis basis: `prompt-only result; no deterministic repair was applied`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_01/thinking/12_동양생명_20260318_html/validation_summary.json)
- missing orders: `2`
- extra orders: `2`
- missing sample: `[{"base_date": "2026-03-18", "fund_code": "BALX00", "fund_name": "(디폴트옵션전용)글로벌에셋밸런스형", "order_type": "3", "settle_class": "2", "t_day": "01", "transfer_amount": "11537247"}, {"base_date": "2026-03-18", "fund_code": "BALX00", "fund_name": "(디폴트옵션전용)글로벌에셋밸런스형", "order_type": "1", "settle_class": "2", "t_day": "01", "transfer_amount": "8554421"}]`
- extra sample: `[{"base_date": "2026-03-18", "fund_code": "BALX00", "fund_name": "(디프트옵션전용)글로벌에셋밸런스형", "order_type": "3", "settle_class": "2", "t_day": "01", "transfer_amount": "11537247"}, {"base_date": "2026-03-18", "fund_code": "BALX00", "fund_name": "(디프트옵션전용)글로벌에셋밸런스형", "order_type": "1", "settle_class": "2", "t_day": "01", "transfer_amount": "8554421"}]`

### `1` / `thinking` / `37_카디프_251127_pdf`
- verdict: `FAIL`
- reason: `JSON parse failed`
- analysis basis: `prompt-only result; no deterministic repair was applied`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_01/thinking/37_카디프_251127_pdf/validation_summary.json)
- missing orders: `0`
- extra orders: `0`

### `1` / `non_thinking` / `33_운용지시서_KDB생명_20260427_eml`
- verdict: `FAIL`
- reason: `validation mismatch`
- analysis basis: `prompt-only result; no deterministic repair was applied`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_01/non_thinking/33_운용지시서_KDB생명_20260427_eml/validation_summary.json)
- missing orders: `1`
- extra orders: `0`
- missing sample: `[{"base_date": "2026-04-27", "fund_code": "492007", "fund_name": "액티브배당성장70혼합형", "order_type": "3", "settle_class": "1", "t_day": "02", "transfer_amount": "6587078"}]`

### `1` / `non_thinking` / `12_동양생명_20260318_html`
- verdict: `FAIL`
- reason: `validation mismatch`
- analysis basis: `prompt-only result; no deterministic repair was applied`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_01/non_thinking/12_동양생명_20260318_html/validation_summary.json)
- missing orders: `2`
- extra orders: `2`
- missing sample: `[{"base_date": "2026-03-18", "fund_code": "BALX00", "fund_name": "(디폴트옵션전용)글로벌에셋밸런스형", "order_type": "3", "settle_class": "2", "t_day": "01", "transfer_amount": "11537247"}, {"base_date": "2026-03-18", "fund_code": "BALX00", "fund_name": "(디폴트옵션전용)글로벌에셋밸런스형", "order_type": "1", "settle_class": "2", "t_day": "01", "transfer_amount": "8554421"}]`
- extra sample: `[{"base_date": "2026-03-18", "fund_code": "BALX00", "fund_name": "(디프트옵션전용)글로벌에셋밸런스형", "order_type": "3", "settle_class": "2", "t_day": "01", "transfer_amount": "11537247"}, {"base_date": "2026-03-18", "fund_code": "BALX00", "fund_name": "(디프트옵션전용)글로벌에셋밸런스형", "order_type": "1", "settle_class": "2", "t_day": "01", "transfer_amount": "8554421"}]`

### `2` / `thinking` / `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml`
- verdict: `FAIL`
- reason: `JSON parse failed`
- analysis basis: `prompt-only result; no deterministic repair was applied`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_02/thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml/validation_summary.json)
- missing orders: `0`
- extra orders: `0`

### `2` / `thinking` / `12_동양생명_20260318_html`
- verdict: `FAIL`
- reason: `settlement JSON parse failed`
- analysis basis: `prompt-only result; no deterministic repair was applied`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_02/thinking/12_동양생명_20260318_html/validation_summary.json)
- missing orders: `0`
- extra orders: `0`

### `2` / `thinking` / `37_카디프_251127_pdf`
- verdict: `FAIL`
- reason: `JSON parse failed`
- analysis basis: `prompt-only result; no deterministic repair was applied`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_02/thinking/37_카디프_251127_pdf/validation_summary.json)
- missing orders: `0`
- extra orders: `0`

### `2` / `non_thinking` / `33_운용지시서_KDB생명_20260427_eml`
- verdict: `FAIL`
- reason: `validation mismatch`
- analysis basis: `prompt-only result; no deterministic repair was applied`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_02/non_thinking/33_운용지시서_KDB생명_20260427_eml/validation_summary.json)
- missing orders: `1`
- extra orders: `0`
- missing sample: `[{"base_date": "2026-04-27", "fund_code": "492007", "fund_name": "액티브배당성장70혼합형", "order_type": "3", "settle_class": "1", "t_day": "02", "transfer_amount": "6587078"}]`

### `2` / `non_thinking` / `12_동양생명_20260318_html`
- verdict: `FAIL`
- reason: `validation mismatch`
- analysis basis: `prompt-only result; no deterministic repair was applied`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_152808_artifacts/executions/cycle_02/non_thinking/12_동양생명_20260318_html/validation_summary.json)
- missing orders: `2`
- extra orders: `2`
- missing sample: `[{"base_date": "2026-03-18", "fund_code": "BALX00", "fund_name": "(디폴트옵션전용)글로벌에셋밸런스형", "order_type": "3", "settle_class": "2", "t_day": "01", "transfer_amount": "11537247"}, {"base_date": "2026-03-18", "fund_code": "BALX00", "fund_name": "(디폴트옵션전용)글로벌에셋밸런스형", "order_type": "1", "settle_class": "2", "t_day": "01", "transfer_amount": "8554421"}]`
- extra sample: `[{"base_date": "2026-03-18", "fund_code": "BALX00", "fund_name": "(디프트옵션전용)글로벌에셋밸런스형", "order_type": "3", "settle_class": "2", "t_day": "01", "transfer_amount": "11537247"}, {"base_date": "2026-03-18", "fund_code": "BALX00", "fund_name": "(디프트옵션전용)글로벌에셋밸런스형", "order_type": "1", "settle_class": "2", "t_day": "01", "transfer_amount": "8554421"}]`

## Notes

- `response_format={"type":"json_object"}`와 Novita top-level `enable_thinking`/`separate_reasoning`을 요청마다 명시했다.
- HTTP 400 계열 파라미터 거부, 인증 오류, image input 미지원, timeout은 `BLOCKED`로 기록하며 파라미터를 제거해 재시도하지 않았다.
- JSON/CSV exact 비교는 status, base_date, issues, 주문 canonical field 기준으로 수행했다.
