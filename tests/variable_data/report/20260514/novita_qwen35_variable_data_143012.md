# Novita Qwen3.5 지시서 추출 파라미터 테스트 보고서

- 실행 일시: `2026-05-14 14:32:38 KST`
- run id: `novita_qwen35_variable_data_143012`
- overall verdict: `REVIEW`
- model: `qwen/qwen3.5-397b-a17b`
- endpoint: `https://api.novita.ai/openai/v1/chat/completions`
- cycles: `1`
- artifact root: [novita_qwen35_variable_data_143012_artifacts](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_143012_artifacts)
- deterministic 보강 사용 여부: `false`
- API key 저장 여부: `false`

## 테스트 대상

| Case | 거래처 | Source | Expected | Base date | Strategy |
| --- | --- | --- | ---: | --- | --- |
| `14_라이나_250826_xlsx` | 라이나생명 | [라이나_250826.xlsx](/Users/bhkim/Documents/codex_prj_sam_asset/document/라이나_250826.xlsx) | `10` | `2025-08-26` | `single_prompt` |
| `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | 한화생명 | [삼성자산_한화생명_설정_해지_내역서_20251128.eml](/Users/bhkim/Documents/codex_prj_sam_asset/document/삼성자산_한화생명_설정_해지_내역서_20251128.eml) | `3` | `2025-11-28` | `single_prompt` |
| `12_동양생명_20260318_html` | 동양생명 | [동양생명_20260318.html](/Users/bhkim/Documents/codex_prj_sam_asset/document/동양생명_20260318.html) | `40` | `2026-03-18` | `dongyang_two_stage` |

## 파라미터 프로필

| Profile | enable_thinking | temperature | top_p | top_k | min_p | presence_penalty | repetition_penalty | max_tokens |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `non_thinking` | `false` | `0` | `1.0` | `20` | `0.0` | `0.0` | `1.0` | `16384` |
| `thinking` | `true` | `0.6` | `0.95` | `20` | `0.0` | `0.0` | `1.0` | `32768` |

## Verdict Summary

- PASS: `2`
- FAIL: `4`
- BLOCKED: `0`

| Profile | Total | PASS | FAIL | BLOCKED | Avg elapsed(s) | Avg total tokens |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `non_thinking` | `3` | `1` | `2` | `0` | `24.338` | `5843.7` |
| `thinking` | `3` | `1` | `2` | `0` | `23.911` | `5673.7` |

## 실행 결과

| Cycle | Profile | Case | Verdict | Orders | Expected | JSON exact | CSV exact | Calls | Elapsed(s) | Tokens | Reasoning | Artifact |
| ---: | --- | --- | --- | ---: | ---: | --- | --- | ---: | ---: | ---: | --- | --- |
| `1` | `non_thinking` | `14_라이나_250826_xlsx` | `PASS` | `10` | `10` | `True` | `True` | `1` | `16.268` | `4135` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_143012_artifacts/executions/cycle_01/non_thinking/14_라이나_250826_xlsx) |
| `1` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `FAIL` | `4` | `3` | `False` | `False` | `1` | `11.288` | `3895` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_143012_artifacts/executions/cycle_01/non_thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml) |
| `1` | `non_thinking` | `12_동양생명_20260318_html` | `FAIL` | `40` | `40` | `False` | `False` | `2` | `45.458` | `9501` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_143012_artifacts/executions/cycle_01/non_thinking/12_동양생명_20260318_html) |
| `1` | `thinking` | `14_라이나_250826_xlsx` | `PASS` | `10` | `10` | `True` | `True` | `1` | `18.734` | `4132` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_143012_artifacts/executions/cycle_01/thinking/14_라이나_250826_xlsx) |
| `1` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `FAIL` | `4` | `3` | `False` | `False` | `1` | `11.171` | `3892` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_143012_artifacts/executions/cycle_01/thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml) |
| `1` | `thinking` | `12_동양생명_20260318_html` | `FAIL` | `40` | `40` | `False` | `False` | `2` | `41.828` | `8997` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_143012_artifacts/executions/cycle_01/thinking/12_동양생명_20260318_html) |

## LLM 처리 시간 및 Token Usage

| Cycle | Profile | Case | Calls | LLM elapsed(s) | Prompt tokens | Completion tokens | Total tokens | Finish reasons |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `1` | `non_thinking` | `14_라이나_250826_xlsx` | `1` | `16.268` | `3072` | `1063` | `4135` | `{"extract": "stop"}` |
| `1` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `11.288` | `3311` | `584` | `3895` | `{"extract": "stop"}` |
| `1` | `non_thinking` | `12_동양생명_20260318_html` | `2` | `45.458` | `6467` | `3034` | `9501` | `{"settlement": "stop", "forecast": "stop"}` |
| `1` | `thinking` | `14_라이나_250826_xlsx` | `1` | `18.734` | `3069` | `1063` | `4132` | `{"extract": "stop"}` |
| `1` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `11.171` | `3308` | `584` | `3892` | `{"extract": "stop"}` |
| `1` | `thinking` | `12_동양생명_20260318_html` | `2` | `41.828` | `6457` | `2540` | `8997` | `{"settlement": "stop", "forecast": "stop"}` |

### 호출별 Usage

#### `1` / `non_thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement` | `2617` | `1439` | `4056` |
| `forecast` | `3850` | `1595` | `5445` |

#### `1` / `thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement` | `2614` | `1435` | `4049` |
| `forecast` | `3843` | `1105` | `4948` |


## 검토 필요 항목

### `1` / `non_thinking` / `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml`
- verdict: `FAIL`
- reason: `validation mismatch`
- analysis basis: `prompt-only result; no deterministic repair was applied`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_143012_artifacts/executions/cycle_01/non_thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml/validation_summary.json)
- missing orders: `1`
- extra orders: `2`
- missing sample: `[{"base_date": "2025-11-28", "fund_code": "V63053", "fund_name": "보장성변액 글로벌멀티에셋안정배분형III(삼성혼합)", "order_type": "1", "settle_class": "1", "t_day": "03", "transfer_amount": "508615208"}]`
- extra sample: `[{"base_date": "2025-11-28", "fund_code": "32451", "fund_name": "퇴직연금연금자산배분형", "order_type": "3", "settle_class": "1", "t_day": "03", "transfer_amount": "16600000"}, {"base_date": "2025-11-28", "fund_code": "V63053", "fund_name": "보장성변액 글로벌멀티에셋안정배분형III(삼성혼합)", "order_type": "1", "settle_class": "1", "t_day": "03", "transfer_amount": "458400000"}]`

### `1` / `non_thinking` / `12_동양생명_20260318_html`
- verdict: `FAIL`
- reason: `validation mismatch`
- analysis basis: `prompt-only result; no deterministic repair was applied`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_143012_artifacts/executions/cycle_01/non_thinking/12_동양생명_20260318_html/validation_summary.json)
- missing orders: `8`
- extra orders: `8`
- missing sample: `[{"base_date": "2026-03-18", "fund_code": "BBI100", "fund_name": "변액유니버셜종신-채권형", "order_type": "3", "settle_class": "2", "t_day": "01", "transfer_amount": "15579386"}, {"base_date": "2026-03-18", "fund_code": "BBI100", "fund_name": "변액유니버셜종신-채권형", "order_type": "1", "settle_class": "2", "t_day": "01", "transfer_amount": "36487222"}, {"base_date": "2026-03-18", "fund_code": "BBI100", "fund_name": "변액유니버셜종신-채권형", "order_type": "1", "settle_class": "1", "t_day": "03", "transfer_amount": "17716295"}]`
- extra sample: `[{"base_date": "2026-03-18", "fund_code": "BBI100", "fund_name": "변액유니버설종신-채권형", "order_type": "3", "settle_class": "2", "t_day": "01", "transfer_amount": "15579386"}, {"base_date": "2026-03-18", "fund_code": "BBI100", "fund_name": "변액유니버설종신-채권형", "order_type": "1", "settle_class": "2", "t_day": "01", "transfer_amount": "36487222"}, {"base_date": "2026-03-18", "fund_code": "BBI100", "fund_name": "변액유니버설종신-채권형", "order_type": "1", "settle_class": "1", "t_day": "03", "transfer_amount": "17716295"}]`

### `1` / `thinking` / `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml`
- verdict: `FAIL`
- reason: `validation mismatch`
- analysis basis: `prompt-only result; no deterministic repair was applied`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_143012_artifacts/executions/cycle_01/thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml/validation_summary.json)
- missing orders: `1`
- extra orders: `2`
- missing sample: `[{"base_date": "2025-11-28", "fund_code": "V63053", "fund_name": "보장성변액 글로벌멀티에셋안정배분형III(삼성혼합)", "order_type": "1", "settle_class": "1", "t_day": "03", "transfer_amount": "508615208"}]`
- extra sample: `[{"base_date": "2025-11-28", "fund_code": "32451", "fund_name": "퇴직연금연금자산배분형", "order_type": "3", "settle_class": "1", "t_day": "03", "transfer_amount": "16600000"}, {"base_date": "2025-11-28", "fund_code": "V63053", "fund_name": "보장성변액 글로벌멀티에셋안정배분형III(삼성혼합)", "order_type": "1", "settle_class": "1", "t_day": "03", "transfer_amount": "458400000"}]`

### `1` / `thinking` / `12_동양생명_20260318_html`
- verdict: `FAIL`
- reason: `validation mismatch`
- analysis basis: `prompt-only result; no deterministic repair was applied`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_143012_artifacts/executions/cycle_01/thinking/12_동양생명_20260318_html/validation_summary.json)
- missing orders: `8`
- extra orders: `8`
- missing sample: `[{"base_date": "2026-03-18", "fund_code": "BBI100", "fund_name": "변액유니버셜종신-채권형", "order_type": "3", "settle_class": "2", "t_day": "01", "transfer_amount": "15579386"}, {"base_date": "2026-03-18", "fund_code": "BBI100", "fund_name": "변액유니버셜종신-채권형", "order_type": "1", "settle_class": "2", "t_day": "01", "transfer_amount": "36487222"}, {"base_date": "2026-03-18", "fund_code": "BBI100", "fund_name": "변액유니버셜종신-채권형", "order_type": "1", "settle_class": "1", "t_day": "03", "transfer_amount": "17716295"}]`
- extra sample: `[{"base_date": "2026-03-18", "fund_code": "BBI100", "fund_name": "변액유니버설종신-채권형", "order_type": "3", "settle_class": "2", "t_day": "01", "transfer_amount": "15579386"}, {"base_date": "2026-03-18", "fund_code": "BBI100", "fund_name": "변액유니버설종신-채권형", "order_type": "1", "settle_class": "2", "t_day": "01", "transfer_amount": "36487222"}, {"base_date": "2026-03-18", "fund_code": "BBI100", "fund_name": "변액유니버설종신-채권형", "order_type": "1", "settle_class": "1", "t_day": "03", "transfer_amount": "17716295"}]`

## Notes

- `response_format={"type":"json_object"}`와 Novita top-level `enable_thinking`/`separate_reasoning`을 요청마다 명시했다.
- HTTP 400 계열 파라미터 거부, 인증 오류, image input 미지원, timeout은 `BLOCKED`로 기록하며 파라미터를 제거해 재시도하지 않았다.
- JSON/CSV exact 비교는 status, base_date, issues, 주문 canonical field 기준으로 수행했다.
