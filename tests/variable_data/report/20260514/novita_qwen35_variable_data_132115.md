# Novita Qwen3.5 지시서 추출 파라미터 테스트 보고서

- 실행 일시: `2026-05-14 13:23:24 KST`
- run id: `novita_qwen35_variable_data_132115`
- overall verdict: `REVIEW`
- model: `qwen/qwen3.5-397b-a17b`
- endpoint: `https://api.novita.ai/openai/v1/chat/completions`
- cycles: `1`
- artifact root: [novita_qwen35_variable_data_132115_artifacts](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_132115_artifacts)
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
| `non_thinking` | `3` | `2` | `1` | `0` | `22.452` | `6773.7` |
| `thinking` | `3` | `2` | `1` | `0` | `20.279` | `5546.7` |

## 실행 결과

| Cycle | Profile | Case | Verdict | Orders | Expected | JSON exact | CSV exact | Calls | Elapsed(s) | Tokens | Reasoning | Artifact |
| ---: | --- | --- | --- | ---: | ---: | --- | --- | ---: | ---: | ---: | --- | --- |
| `1` | `non_thinking` | `14_라이나_250826_xlsx` | `PASS` | `10` | `10` | `True` | `True` | `1` | `12.857` | `7282` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_132115_artifacts/executions/cycle_01/non_thinking/14_라이나_250826_xlsx) |
| `1` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `FAIL` | `2` | `3` | `False` | `False` | `1` | `8.632` | `3538` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_132115_artifacts/executions/cycle_01/non_thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml) |
| `1` | `non_thinking` | `12_동양생명_20260318_html` | `PASS` | `40` | `40` | `True` | `True` | `2` | `45.866` | `9501` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_132115_artifacts/executions/cycle_01/non_thinking/12_동양생명_20260318_html) |
| `1` | `thinking` | `14_라이나_250826_xlsx` | `PASS` | `10` | `10` | `True` | `True` | `1` | `15.817` | `4132` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_132115_artifacts/executions/cycle_01/thinking/14_라이나_250826_xlsx) |
| `1` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `FAIL` | `2` | `3` | `False` | `False` | `1` | `8.58` | `3535` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_132115_artifacts/executions/cycle_01/thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml) |
| `1` | `thinking` | `12_동양생명_20260318_html` | `PASS` | `40` | `40` | `True` | `True` | `2` | `36.441` | `8973` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_132115_artifacts/executions/cycle_01/thinking/12_동양생명_20260318_html) |

## LLM 처리 시간 및 Token Usage

| Cycle | Profile | Case | Calls | LLM elapsed(s) | Prompt tokens | Completion tokens | Total tokens | Finish reasons |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `1` | `non_thinking` | `14_라이나_250826_xlsx` | `1` | `12.857` | `6219` | `1063` | `7282` | `{"extract": "stop"}` |
| `1` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `8.632` | `3168` | `370` | `3538` | `{"extract": "stop"}` |
| `1` | `non_thinking` | `12_동양생명_20260318_html` | `2` | `45.866` | `6467` | `3034` | `9501` | `{"settlement": "stop", "forecast": "stop"}` |
| `1` | `thinking` | `14_라이나_250826_xlsx` | `1` | `15.817` | `3069` | `1063` | `4132` | `{"extract": "stop"}` |
| `1` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `8.58` | `3165` | `370` | `3535` | `{"extract": "stop"}` |
| `1` | `thinking` | `12_동양생명_20260318_html` | `2` | `36.441` | `6457` | `2516` | `8973` | `{"settlement": "stop", "forecast": "stop"}` |

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
| `forecast` | `3843` | `1081` | `4924` |


## Repair 적용 내역

### `1` / `non_thinking` / `12_동양생명_20260318_html`
- repair count: `16`
- repair sample: `[{"fund_code": "BBI100", "from": "변액유니버설종신-채권형", "to": "변액유니버셜종신-채권형"}, {"fund_code": "BBI100", "from": "변액유니버설종신-채권형", "to": "변액유니버셜종신-채권형"}, {"fund_code": "BBIM20", "from": "변액유니버설종신-채권형 Ⅱ (채권_삼성)", "to": "변액유니버셜종신-채권형Ⅱ(채권_삼성)"}, {"fund_code": "BBIM20", "from": "변액유니버설종신-채권형 Ⅱ (채권_삼성)", "to": "변액유니버셜종신-채권형Ⅱ(채권_삼성)"}, {"fund_code": "BBJ650", "from": "변액연금 Ⅱ 주식혼합1형(ETF_삼성)", "to": "변액연금Ⅱ 주식혼합1형(ETF_삼성)"}, {"fund_code": "BBJ650", "from": "변액연금 Ⅱ 주식혼합1형(ETF_삼성)", "to": "변액연금Ⅱ 주식혼합1형(ETF_삼성)"}, {"fund_code": "BBJ770", "from": "변액연금 Ⅱ 주식혼합2형(ETF_삼성)", "to": "변액연금Ⅱ 주식혼합2형(ETF_삼성)"}, {"fund_code": "BBJ770", "from": "변액연금 Ⅱ 주식혼합2형(ETF_삼성)", "to": "변액연금Ⅱ 주식혼합2형(ETF_삼성)"}]`


## 검토 필요 항목

### `1` / `non_thinking` / `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml`
- verdict: `FAIL`
- reason: `validation mismatch`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_132115_artifacts/executions/cycle_01/non_thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml/validation_summary.json)
- missing orders: `1`
- extra orders: `0`
- missing sample: `[{"base_date": "2025-11-28", "fund_code": "V63053", "fund_name": "보장성변액 글로벌멀티에셋안정배분형III(삼성혼합)", "order_type": "1", "settle_class": "1", "t_day": "03", "transfer_amount": "508615208"}]`

### `1` / `thinking` / `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml`
- verdict: `FAIL`
- reason: `validation mismatch`
- validation: [validation_summary.json](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_132115_artifacts/executions/cycle_01/thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml/validation_summary.json)
- missing orders: `1`
- extra orders: `0`
- missing sample: `[{"base_date": "2025-11-28", "fund_code": "V63053", "fund_name": "보장성변액 글로벌멀티에셋안정배분형III(삼성혼합)", "order_type": "1", "settle_class": "1", "t_day": "03", "transfer_amount": "508615208"}]`

## Notes

- `response_format={"type":"json_object"}`와 Novita top-level `enable_thinking`/`separate_reasoning`을 요청마다 명시했다.
- HTTP 400 계열 파라미터 거부, 인증 오류, image input 미지원, timeout은 `BLOCKED`로 기록하며 파라미터를 제거해 재시도하지 않았다.
- JSON/CSV exact 비교는 status, base_date, issues, 주문 canonical field 기준으로 수행했다.
