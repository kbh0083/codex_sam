# Novita Qwen3.5 지시서 추출 파라미터 테스트 보고서

- 실행 일시: `2026-05-14 14:36:13 KST`
- run id: `novita_qwen35_variable_data_143336`
- overall verdict: `REVIEW`
- model: `qwen/qwen3.5-397b-a17b`
- endpoint: `https://api.novita.ai/openai/v1/chat/completions`
- cycles: `1`
- artifact root: [novita_qwen35_variable_data_143336_artifacts](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_143336_artifacts)
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

- PASS: `4`
- FAIL: `2`
- BLOCKED: `0`

| Profile | Total | PASS | FAIL | BLOCKED | Avg elapsed(s) | Avg total tokens |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `non_thinking` | `3` | `3` | `0` | `0` | `28.397` | `5927.0` |
| `thinking` | `3` | `1` | `2` | `0` | `23.565` | `5721.7` |

## 실행 결과

| Cycle | Profile | Case | Verdict | Orders | Expected | JSON exact | CSV exact | Calls | Elapsed(s) | Tokens | Reasoning | Artifact |
| ---: | --- | --- | --- | ---: | ---: | --- | --- | ---: | ---: | ---: | --- | --- |
| `1` | `non_thinking` | `14_라이나_250826_xlsx` | `PASS` | `10` | `10` | `True` | `True` | `1` | `17.537` | `4135` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_143336_artifacts/executions/cycle_01/non_thinking/14_라이나_250826_xlsx) |
| `1` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `PASS` | `3` | `3` | `True` | `True` | `1` | `10.224` | `3920` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_143336_artifacts/executions/cycle_01/non_thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml) |
| `1` | `non_thinking` | `12_동양생명_20260318_html` | `PASS` | `40` | `40` | `True` | `True` | `2` | `57.431` | `9726` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_143336_artifacts/executions/cycle_01/non_thinking/12_동양생명_20260318_html) |
| `1` | `thinking` | `14_라이나_250826_xlsx` | `PASS` | `10` | `10` | `True` | `True` | `1` | `18.902` | `4132` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_143336_artifacts/executions/cycle_01/thinking/14_라이나_250826_xlsx) |
| `1` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `FAIL` | `2` | `3` | `False` | `False` | `1` | `8.322` | `3803` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_143336_artifacts/executions/cycle_01/thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml) |
| `1` | `thinking` | `12_동양생명_20260318_html` | `FAIL` | `40` | `40` | `False` | `False` | `2` | `43.472` | `9230` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_143336_artifacts/executions/cycle_01/thinking/12_동양생명_20260318_html) |

## LLM 처리 시간 및 Token Usage

| Cycle | Profile | Case | Calls | LLM elapsed(s) | Prompt tokens | Completion tokens | Total tokens | Finish reasons |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `1` | `non_thinking` | `14_라이나_250826_xlsx` | `1` | `17.537` | `3072` | `1063` | `4135` | `{"extract": "stop"}` |
| `1` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `10.224` | `3436` | `484` | `3920` | `{"extract": "stop"}` |
| `1` | `non_thinking` | `12_동양생명_20260318_html` | `2` | `57.431` | `6699` | `3027` | `9726` | `{"settlement": "stop", "forecast": "stop"}` |
| `1` | `thinking` | `14_라이나_250826_xlsx` | `1` | `18.902` | `3069` | `1063` | `4132` | `{"extract": "stop"}` |
| `1` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `8.322` | `3433` | `370` | `3803` | `{"extract": "stop"}` |
| `1` | `thinking` | `12_동양생명_20260318_html` | `2` | `43.472` | `6692` | `2538` | `9230` | `{"settlement": "stop", "forecast": "stop"}` |

### 호출별 Usage

#### `1` / `non_thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement` | `2735` | `1435` | `4170` |
| `forecast` | `3964` | `1592` | `5556` |

#### `1` / `thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement` | `2732` | `1434` | `4166` |
| `forecast` | `3960` | `1104` | `5064` |


## 검토 필요 항목

### `1` / `thinking` / `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml`
- verdict: `FAIL`
- reason: `validation mismatch`
- analysis basis: `prompt-only result; no deterministic repair was applied`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_143336_artifacts/executions/cycle_01/thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml/validation_summary.json)
- missing orders: `1`
- extra orders: `0`
- missing sample: `[{"base_date": "2025-11-28", "fund_code": "V63053", "fund_name": "보장성변액 글로벌멀티에셋안정배분형III(삼성혼합)", "order_type": "1", "settle_class": "1", "t_day": "03", "transfer_amount": "508615208"}]`

### `1` / `thinking` / `12_동양생명_20260318_html`
- verdict: `FAIL`
- reason: `validation mismatch`
- analysis basis: `prompt-only result; no deterministic repair was applied`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_143336_artifacts/executions/cycle_01/thinking/12_동양생명_20260318_html/validation_summary.json)
- missing orders: `2`
- extra orders: `2`
- missing sample: `[{"base_date": "2026-03-18", "fund_code": "BALI00", "fund_name": "퇴연주식형", "order_type": "3", "settle_class": "2", "t_day": "01", "transfer_amount": "8592243"}, {"base_date": "2026-03-18", "fund_code": "BALI00", "fund_name": "퇴연주식형", "order_type": "1", "settle_class": "1", "t_day": "03", "transfer_amount": "1656769"}]`
- extra sample: `[{"base_date": "2026-03-18", "fund_code": "BALI00", "fund_name": "퇴연금식형", "order_type": "3", "settle_class": "2", "t_day": "01", "transfer_amount": "8592243"}, {"base_date": "2026-03-18", "fund_code": "BALI00", "fund_name": "퇴연금식형", "order_type": "1", "settle_class": "1", "t_day": "03", "transfer_amount": "1656769"}]`

## Notes

- `response_format={"type":"json_object"}`와 Novita top-level `enable_thinking`/`separate_reasoning`을 요청마다 명시했다.
- HTTP 400 계열 파라미터 거부, 인증 오류, image input 미지원, timeout은 `BLOCKED`로 기록하며 파라미터를 제거해 재시도하지 않았다.
- JSON/CSV exact 비교는 status, base_date, issues, 주문 canonical field 기준으로 수행했다.
