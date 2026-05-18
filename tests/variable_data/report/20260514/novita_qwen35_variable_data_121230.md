# Novita Qwen3.5 지시서 추출 파라미터 테스트 보고서

- 실행 일시: `2026-05-14 12:25:16 KST`
- run id: `novita_qwen35_variable_data_121230`
- overall verdict: `REVIEW`
- model: `qwen/qwen3.5-397b-a17b`
- endpoint: `https://api.novita.ai/openai/v1/chat/completions`
- cycles: `2`
- artifact root: [novita_qwen35_variable_data_121230_artifacts](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts)
- API key 저장 여부: `false`

## 테스트 대상

| Case | 거래처 | Source | Expected | Base date | Strategy |
| --- | --- | --- | ---: | --- | --- |
| `33_운용지시서_KDB생명_20260427_eml` | KDB생명 | [운용지시서(KDB생명)_20260427.eml](/Users/bhkim/Documents/codex_prj_sam_asset/document/운용지시서(KDB생명)_20260427.eml) | `12` | `2026-04-27` | `single_prompt` |
| `14_라이나_250826_xlsx` | 라이나생명 | [라이나_250826.xlsx](/Users/bhkim/Documents/codex_prj_sam_asset/document/라이나_250826.xlsx) | `10` | `2025-08-26` | `single_prompt` |
| `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | 한화생명 | [삼성자산_한화생명_설정_해지_내역서_20251128.eml](/Users/bhkim/Documents/codex_prj_sam_asset/document/삼성자산_한화생명_설정_해지_내역서_20251128.eml) | `3` | `2025-11-28` | `single_prompt` |
| `12_동양생명_20260318_html` | 동양생명 | [동양생명_20260318.html](/Users/bhkim/Documents/codex_prj_sam_asset/document/동양생명_20260318.html) | `40` | `2026-03-18` | `dongyang_two_stage` |
| `37_카디프_251127_pdf` | 카디프생명 | [카디프_251127.pdf](/Users/bhkim/Documents/codex_prj_sam_asset/document/카디프_251127.pdf) | `46` | `2025-11-27` | `single_prompt` |

## 파라미터 프로필

| Profile | enable_thinking | temperature | top_p | top_k | min_p | presence_penalty | repetition_penalty | max_tokens |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `non_thinking` | `false` | `0` | `1.0` | `20` | `0.0` | `0.0` | `1.0` | `16384` |
| `thinking` | `true` | `0.6` | `0.95` | `20` | `0.0` | `0.0` | `1.0` | `32768` |

## Verdict Summary

- PASS: `10`
- FAIL: `10`
- BLOCKED: `0`

| Profile | Total | PASS | FAIL | BLOCKED | Avg elapsed(s) | Avg total tokens |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `non_thinking` | `10` | `4` | `6` | `0` | `35.219` | `6593.8` |
| `thinking` | `10` | `6` | `4` | `0` | `37.632` | `6536.7` |

## 실행 결과

| Cycle | Profile | Case | Verdict | Orders | Expected | JSON exact | CSV exact | Calls | Elapsed(s) | Tokens | Reasoning | Artifact |
| ---: | --- | --- | --- | ---: | ---: | --- | --- | ---: | ---: | ---: | --- | --- |
| `1` | `non_thinking` | `33_운용지시서_KDB생명_20260427_eml` | `PASS` | `12` | `12` | `True` | `True` | `1` | `18.641` | `4465` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_01/non_thinking/33_운용지시서_KDB생명_20260427_eml) |
| `1` | `non_thinking` | `14_라이나_250826_xlsx` | `FAIL` | `30` | `10` | `False` | `False` | `1` | `36.633` | `5961` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_01/non_thinking/14_라이나_250826_xlsx) |
| `1` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `FAIL` | `1` | `3` | `False` | `False` | `1` | `5.562` | `3318` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_01/non_thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml) |
| `1` | `non_thinking` | `12_동양생명_20260318_html` | `FAIL` | `40` | `40` | `False` | `False` | `2` | `36.976` | `9501` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_01/non_thinking/12_동양생명_20260318_html) |
| `1` | `non_thinking` | `37_카디프_251127_pdf` | `PASS` | `46` | `46` | `True` | `True` | `1` | `66.645` | `9968` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_01/non_thinking/37_카디프_251127_pdf) |
| `1` | `thinking` | `33_운용지시서_KDB생명_20260427_eml` | `PASS` | `12` | `12` | `True` | `True` | `1` | `22.087` | `4462` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_01/thinking/33_운용지시서_KDB생명_20260427_eml) |
| `1` | `thinking` | `14_라이나_250826_xlsx` | `FAIL` | `30` | `10` | `False` | `False` | `1` | `40.964` | `5958` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_01/thinking/14_라이나_250826_xlsx) |
| `1` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `FAIL` | `1` | `3` | `False` | `False` | `1` | `6.07` | `3315` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_01/thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml) |
| `1` | `thinking` | `12_동양생명_20260318_html` | `PASS` | `40` | `40` | `True` | `True` | `2` | `41.456` | `8994` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_01/thinking/12_동양생명_20260318_html) |
| `1` | `thinking` | `37_카디프_251127_pdf` | `PASS` | `46` | `46` | `True` | `True` | `1` | `63.636` | `9965` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_01/thinking/37_카디프_251127_pdf) |
| `2` | `non_thinking` | `33_운용지시서_KDB생명_20260427_eml` | `PASS` | `12` | `12` | `True` | `True` | `1` | `22.335` | `4465` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_02/non_thinking/33_운용지시서_KDB생명_20260427_eml) |
| `2` | `non_thinking` | `14_라이나_250826_xlsx` | `FAIL` | `30` | `10` | `False` | `False` | `1` | `47.719` | `5961` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_02/non_thinking/14_라이나_250826_xlsx) |
| `2` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `FAIL` | `1` | `3` | `False` | `False` | `1` | `6.575` | `3318` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_02/non_thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml) |
| `2` | `non_thinking` | `12_동양생명_20260318_html` | `FAIL` | `40` | `40` | `False` | `False` | `2` | `41.343` | `9013` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_02/non_thinking/12_동양생명_20260318_html) |
| `2` | `non_thinking` | `37_카디프_251127_pdf` | `PASS` | `46` | `46` | `True` | `True` | `1` | `69.757` | `9968` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_02/non_thinking/37_카디프_251127_pdf) |
| `2` | `thinking` | `33_운용지시서_KDB생명_20260427_eml` | `PASS` | `12` | `12` | `True` | `True` | `1` | `27.439` | `4462` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_02/thinking/33_운용지시서_KDB생명_20260427_eml) |
| `2` | `thinking` | `14_라이나_250826_xlsx` | `FAIL` | `30` | `10` | `False` | `False` | `1` | `49.17` | `5958` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_02/thinking/14_라이나_250826_xlsx) |
| `2` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `FAIL` | `1` | `3` | `False` | `False` | `1` | `7.353` | `3315` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_02/thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml) |
| `2` | `thinking` | `12_동양생명_20260318_html` | `PASS` | `40` | `40` | `True` | `True` | `2` | `45.719` | `8973` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_02/thinking/12_동양생명_20260318_html) |
| `2` | `thinking` | `37_카디프_251127_pdf` | `PASS` | `46` | `46` | `True` | `True` | `1` | `72.424` | `9965` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_02/thinking/37_카디프_251127_pdf) |

## LLM 처리 시간 및 Token Usage

| Cycle | Profile | Case | Calls | LLM elapsed(s) | Prompt tokens | Completion tokens | Total tokens | Finish reasons |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `1` | `non_thinking` | `33_운용지시서_KDB생명_20260427_eml` | `1` | `18.641` | `3089` | `1376` | `4465` | `{"extract": "stop"}` |
| `1` | `non_thinking` | `14_라이나_250826_xlsx` | `1` | `36.633` | `3010` | `2951` | `5961` | `{"extract": "stop"}` |
| `1` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `5.562` | `3061` | `257` | `3318` | `{"extract": "stop"}` |
| `1` | `non_thinking` | `12_동양생명_20260318_html` | `2` | `36.976` | `6467` | `3034` | `9501` | `{"settlement": "stop", "forecast": "stop"}` |
| `1` | `non_thinking` | `37_카디프_251127_pdf` | `1` | `66.645` | `5499` | `4469` | `9968` | `{"extract": "stop"}` |
| `1` | `thinking` | `33_운용지시서_KDB생명_20260427_eml` | `1` | `22.087` | `3086` | `1376` | `4462` | `{"extract": "stop"}` |
| `1` | `thinking` | `14_라이나_250826_xlsx` | `1` | `40.964` | `3007` | `2951` | `5958` | `{"extract": "stop"}` |
| `1` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `6.07` | `3058` | `257` | `3315` | `{"extract": "stop"}` |
| `1` | `thinking` | `12_동양생명_20260318_html` | `2` | `41.456` | `6456` | `2538` | `8994` | `{"settlement": "stop", "forecast": "stop"}` |
| `1` | `thinking` | `37_카디프_251127_pdf` | `1` | `63.636` | `5496` | `4469` | `9965` | `{"extract": "stop"}` |
| `2` | `non_thinking` | `33_운용지시서_KDB생명_20260427_eml` | `1` | `22.335` | `3089` | `1376` | `4465` | `{"extract": "stop"}` |
| `2` | `non_thinking` | `14_라이나_250826_xlsx` | `1` | `47.719` | `3010` | `2951` | `5961` | `{"extract": "stop"}` |
| `2` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `6.575` | `3061` | `257` | `3318` | `{"extract": "stop"}` |
| `2` | `non_thinking` | `12_동양생명_20260318_html` | `2` | `41.343` | `6455` | `2558` | `9013` | `{"settlement": "stop", "forecast": "stop"}` |
| `2` | `non_thinking` | `37_카디프_251127_pdf` | `1` | `69.757` | `5499` | `4469` | `9968` | `{"extract": "stop"}` |
| `2` | `thinking` | `33_운용지시서_KDB생명_20260427_eml` | `1` | `27.439` | `3086` | `1376` | `4462` | `{"extract": "stop"}` |
| `2` | `thinking` | `14_라이나_250826_xlsx` | `1` | `49.17` | `3007` | `2951` | `5958` | `{"extract": "stop"}` |
| `2` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `7.353` | `3058` | `257` | `3315` | `{"extract": "stop"}` |
| `2` | `thinking` | `12_동양생명_20260318_html` | `2` | `45.719` | `6457` | `2516` | `8973` | `{"settlement": "stop", "forecast": "stop"}` |
| `2` | `thinking` | `37_카디프_251127_pdf` | `1` | `72.424` | `5496` | `4469` | `9965` | `{"extract": "stop"}` |

### 호출별 Usage

#### `1` / `non_thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement` | `2617` | `1439` | `4056` |
| `forecast` | `3850` | `1595` | `5445` |

#### `1` / `thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement` | `2614` | `1434` | `4048` |
| `forecast` | `3842` | `1104` | `4946` |

#### `2` / `non_thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement` | `2614` | `970` | `3584` |
| `forecast` | `3841` | `1588` | `5429` |

#### `2` / `thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement` | `2614` | `1435` | `4049` |
| `forecast` | `3843` | `1081` | `4924` |


## 검토 필요 항목

### `1` / `non_thinking` / `14_라이나_250826_xlsx`
- verdict: `FAIL`
- reason: `validation mismatch`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_01/non_thinking/14_라이나_250826_xlsx/validation_summary.json)
- missing orders: `0`
- extra orders: `20`
- extra sample: `[{"base_date": "2025-08-26", "fund_code": "6102", "fund_name": "혼합안정형", "order_type": "1", "settle_class": "1", "t_day": "01", "transfer_amount": "335969"}, {"base_date": "2025-08-26", "fund_code": "6102", "fund_name": "혼합안정형", "order_type": "3", "settle_class": "1", "t_day": "01", "transfer_amount": "3826639"}, {"base_date": "2025-08-26", "fund_code": "6103", "fund_name": "브이캅그로스형", "order_type": "1", "settle_class": "1", "t_day": "01", "transfer_amount": "9639"}]`

### `1` / `non_thinking` / `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml`
- verdict: `FAIL`
- reason: `validation mismatch`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_01/non_thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml/validation_summary.json)
- missing orders: `2`
- extra orders: `0`
- missing sample: `[{"base_date": "2025-11-28", "fund_code": "V63053", "fund_name": "보장성변액 글로벌멀티에셋안정배분형III(삼성혼합)", "order_type": "1", "settle_class": "2", "t_day": "01", "transfer_amount": "85848214"}, {"base_date": "2025-11-28", "fund_code": "V63053", "fund_name": "보장성변액 글로벌멀티에셋안정배분형III(삼성혼합)", "order_type": "1", "settle_class": "1", "t_day": "03", "transfer_amount": "508615208"}]`

### `1` / `non_thinking` / `12_동양생명_20260318_html`
- verdict: `FAIL`
- reason: `validation mismatch`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_01/non_thinking/12_동양생명_20260318_html/validation_summary.json)
- missing orders: `8`
- extra orders: `8`
- missing sample: `[{"base_date": "2026-03-18", "fund_code": "BBI100", "fund_name": "변액유니버셜종신-채권형", "order_type": "3", "settle_class": "2", "t_day": "01", "transfer_amount": "15579386"}, {"base_date": "2026-03-18", "fund_code": "BBI100", "fund_name": "변액유니버셜종신-채권형", "order_type": "1", "settle_class": "2", "t_day": "01", "transfer_amount": "36487222"}, {"base_date": "2026-03-18", "fund_code": "BBI100", "fund_name": "변액유니버셜종신-채권형", "order_type": "1", "settle_class": "1", "t_day": "03", "transfer_amount": "17716295"}]`
- extra sample: `[{"base_date": "2026-03-18", "fund_code": "BBI100", "fund_name": "변액유니버설종신-채권형", "order_type": "3", "settle_class": "2", "t_day": "01", "transfer_amount": "15579386"}, {"base_date": "2026-03-18", "fund_code": "BBI100", "fund_name": "변액유니버설종신-채권형", "order_type": "1", "settle_class": "2", "t_day": "01", "transfer_amount": "36487222"}, {"base_date": "2026-03-18", "fund_code": "BBI100", "fund_name": "변액유니버설종신-채권형", "order_type": "1", "settle_class": "1", "t_day": "03", "transfer_amount": "17716295"}]`

### `1` / `thinking` / `14_라이나_250826_xlsx`
- verdict: `FAIL`
- reason: `validation mismatch`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_01/thinking/14_라이나_250826_xlsx/validation_summary.json)
- missing orders: `0`
- extra orders: `20`
- extra sample: `[{"base_date": "2025-08-26", "fund_code": "6102", "fund_name": "혼합안정형", "order_type": "1", "settle_class": "1", "t_day": "01", "transfer_amount": "335969"}, {"base_date": "2025-08-26", "fund_code": "6102", "fund_name": "혼합안정형", "order_type": "3", "settle_class": "1", "t_day": "01", "transfer_amount": "3826639"}, {"base_date": "2025-08-26", "fund_code": "6103", "fund_name": "브이캅그로스형", "order_type": "1", "settle_class": "1", "t_day": "01", "transfer_amount": "9639"}]`

### `1` / `thinking` / `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml`
- verdict: `FAIL`
- reason: `validation mismatch`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_01/thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml/validation_summary.json)
- missing orders: `2`
- extra orders: `0`
- missing sample: `[{"base_date": "2025-11-28", "fund_code": "V63053", "fund_name": "보장성변액 글로벌멀티에셋안정배분형III(삼성혼합)", "order_type": "1", "settle_class": "2", "t_day": "01", "transfer_amount": "85848214"}, {"base_date": "2025-11-28", "fund_code": "V63053", "fund_name": "보장성변액 글로벌멀티에셋안정배분형III(삼성혼합)", "order_type": "1", "settle_class": "1", "t_day": "03", "transfer_amount": "508615208"}]`

### `2` / `non_thinking` / `14_라이나_250826_xlsx`
- verdict: `FAIL`
- reason: `validation mismatch`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_02/non_thinking/14_라이나_250826_xlsx/validation_summary.json)
- missing orders: `0`
- extra orders: `20`
- extra sample: `[{"base_date": "2025-08-26", "fund_code": "6102", "fund_name": "혼합안정형", "order_type": "1", "settle_class": "1", "t_day": "01", "transfer_amount": "335969"}, {"base_date": "2025-08-26", "fund_code": "6102", "fund_name": "혼합안정형", "order_type": "3", "settle_class": "1", "t_day": "01", "transfer_amount": "3826639"}, {"base_date": "2025-08-26", "fund_code": "6103", "fund_name": "브이캅그로스형", "order_type": "1", "settle_class": "1", "t_day": "01", "transfer_amount": "9639"}]`

### `2` / `non_thinking` / `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml`
- verdict: `FAIL`
- reason: `validation mismatch`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_02/non_thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml/validation_summary.json)
- missing orders: `2`
- extra orders: `0`
- missing sample: `[{"base_date": "2025-11-28", "fund_code": "V63053", "fund_name": "보장성변액 글로벌멀티에셋안정배분형III(삼성혼합)", "order_type": "1", "settle_class": "2", "t_day": "01", "transfer_amount": "85848214"}, {"base_date": "2025-11-28", "fund_code": "V63053", "fund_name": "보장성변액 글로벌멀티에셋안정배분형III(삼성혼합)", "order_type": "1", "settle_class": "1", "t_day": "03", "transfer_amount": "508615208"}]`

### `2` / `non_thinking` / `12_동양생명_20260318_html`
- verdict: `FAIL`
- reason: `validation mismatch`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_02/non_thinking/12_동양생명_20260318_html/validation_summary.json)
- missing orders: `16`
- extra orders: `16`
- missing sample: `[{"base_date": "2026-03-18", "fund_code": "BBI100", "fund_name": "변액유니버셜종신-채권형", "order_type": "3", "settle_class": "2", "t_day": "01", "transfer_amount": "15579386"}, {"base_date": "2026-03-18", "fund_code": "BBI100", "fund_name": "변액유니버셜종신-채권형", "order_type": "1", "settle_class": "2", "t_day": "01", "transfer_amount": "36487222"}, {"base_date": "2026-03-18", "fund_code": "BBI100", "fund_name": "변액유니버셜종신-채권형", "order_type": "1", "settle_class": "1", "t_day": "03", "transfer_amount": "17716295"}]`
- extra sample: `[{"base_date": "2026-03-18", "fund_code": "BBI100", "fund_name": "변액유니버셜증권-채권형", "order_type": "3", "settle_class": "2", "t_day": "01", "transfer_amount": "15579386"}, {"base_date": "2026-03-18", "fund_code": "BBI100", "fund_name": "변액유니버셜증권-채권형", "order_type": "1", "settle_class": "2", "t_day": "01", "transfer_amount": "36487222"}, {"base_date": "2026-03-18", "fund_code": "BBI100", "fund_name": "변액유니버셜증권-채권형", "order_type": "1", "settle_class": "1", "t_day": "03", "transfer_amount": "17716295"}]`

### `2` / `thinking` / `14_라이나_250826_xlsx`
- verdict: `FAIL`
- reason: `validation mismatch`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_02/thinking/14_라이나_250826_xlsx/validation_summary.json)
- missing orders: `0`
- extra orders: `20`
- extra sample: `[{"base_date": "2025-08-26", "fund_code": "6102", "fund_name": "혼합안정형", "order_type": "1", "settle_class": "1", "t_day": "01", "transfer_amount": "335969"}, {"base_date": "2025-08-26", "fund_code": "6102", "fund_name": "혼합안정형", "order_type": "3", "settle_class": "1", "t_day": "01", "transfer_amount": "3826639"}, {"base_date": "2025-08-26", "fund_code": "6103", "fund_name": "브이캅그로스형", "order_type": "1", "settle_class": "1", "t_day": "01", "transfer_amount": "9639"}]`

### `2` / `thinking` / `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml`
- verdict: `FAIL`
- reason: `validation mismatch`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_121230_artifacts/executions/cycle_02/thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml/validation_summary.json)
- missing orders: `2`
- extra orders: `0`
- missing sample: `[{"base_date": "2025-11-28", "fund_code": "V63053", "fund_name": "보장성변액 글로벌멀티에셋안정배분형III(삼성혼합)", "order_type": "1", "settle_class": "2", "t_day": "01", "transfer_amount": "85848214"}, {"base_date": "2025-11-28", "fund_code": "V63053", "fund_name": "보장성변액 글로벌멀티에셋안정배분형III(삼성혼합)", "order_type": "1", "settle_class": "1", "t_day": "03", "transfer_amount": "508615208"}]`

## Notes

- `response_format={"type":"json_object"}`와 Novita top-level `enable_thinking`/`separate_reasoning`을 요청마다 명시했다.
- HTTP 400 계열 파라미터 거부, 인증 오류, image input 미지원, timeout은 `BLOCKED`로 기록하며 파라미터를 제거해 재시도하지 않았다.
- JSON/CSV exact 비교는 status, base_date, issues, 주문 canonical field 기준으로 수행했다.
