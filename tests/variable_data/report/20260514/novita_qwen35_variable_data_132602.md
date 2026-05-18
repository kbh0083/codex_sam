# Novita Qwen3.5 지시서 추출 파라미터 테스트 보고서

- 실행 일시: `2026-05-14 13:28:21 KST`
- run id: `novita_qwen35_variable_data_132602`
- overall verdict: `PASS`
- model: `qwen/qwen3.5-397b-a17b`
- endpoint: `https://api.novita.ai/openai/v1/chat/completions`
- cycles: `1`
- artifact root: [novita_qwen35_variable_data_132602_artifacts](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_132602_artifacts)
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

- PASS: `6`
- FAIL: `0`
- BLOCKED: `0`

| Profile | Total | PASS | FAIL | BLOCKED | Avg elapsed(s) | Avg total tokens |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `non_thinking` | `3` | `3` | `0` | `0` | `20.25` | `5724.7` |
| `thinking` | `3` | `3` | `0` | `0` | `25.766` | `7127.7` |

## 실행 결과

| Cycle | Profile | Case | Verdict | Orders | Expected | JSON exact | CSV exact | Calls | Elapsed(s) | Tokens | Reasoning | Artifact |
| ---: | --- | --- | --- | ---: | ---: | --- | --- | ---: | ---: | ---: | --- | --- |
| `1` | `non_thinking` | `14_라이나_250826_xlsx` | `PASS` | `10` | `10` | `True` | `True` | `1` | `14.729` | `4135` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_132602_artifacts/executions/cycle_01/non_thinking/14_라이나_250826_xlsx) |
| `1` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `PASS` | `3` | `3` | `True` | `True` | `1` | `7.375` | `3538` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_132602_artifacts/executions/cycle_01/non_thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml) |
| `1` | `non_thinking` | `12_동양생명_20260318_html` | `PASS` | `40` | `40` | `True` | `True` | `2` | `38.646` | `9501` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_132602_artifacts/executions/cycle_01/non_thinking/12_동양생명_20260318_html) |
| `1` | `thinking` | `14_라이나_250826_xlsx` | `PASS` | `10` | `10` | `True` | `True` | `1` | `32.032` | `8875` | `True` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_132602_artifacts/executions/cycle_01/thinking/14_라이나_250826_xlsx) |
| `1` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `PASS` | `3` | `3` | `True` | `True` | `1` | `7.555` | `3535` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_132602_artifacts/executions/cycle_01/thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml) |
| `1` | `thinking` | `12_동양생명_20260318_html` | `PASS` | `40` | `40` | `True` | `True` | `2` | `37.711` | `8973` | `False` | [artifact](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen35_variable_data_132602_artifacts/executions/cycle_01/thinking/12_동양생명_20260318_html) |

## LLM 처리 시간 및 Token Usage

| Cycle | Profile | Case | Calls | LLM elapsed(s) | Prompt tokens | Completion tokens | Total tokens | Finish reasons |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `1` | `non_thinking` | `14_라이나_250826_xlsx` | `1` | `14.729` | `3072` | `1063` | `4135` | `{"extract": "stop"}` |
| `1` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `7.375` | `3168` | `370` | `3538` | `{"extract": "stop"}` |
| `1` | `non_thinking` | `12_동양생명_20260318_html` | `2` | `38.646` | `6467` | `3034` | `9501` | `{"settlement": "stop", "forecast": "stop"}` |
| `1` | `thinking` | `14_라이나_250826_xlsx` | `1` | `32.032` | `6217` | `2658` | `8875` | `{"extract": "stop"}` |
| `1` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `7.555` | `3165` | `370` | `3535` | `{"extract": "stop"}` |
| `1` | `thinking` | `12_동양생명_20260318_html` | `2` | `37.711` | `6457` | `2516` | `8973` | `{"settlement": "stop", "forecast": "stop"}` |

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

### `1` / `non_thinking` / `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml`
- repair count: `1`
- repair sample: `[{"type": "source_table_order_added", "order": {"fund_code": "V63053", "fund_name": "보장성변액 글로벌멀티에셋안정배분형III(삼성혼합)", "settle_class": "1", "order_type": "1", "base_date": "2025-11-28", "t_day": "03", "transfer_amount": "508,615,208"}, "evidence": {"row_index": 17, "row_type": "환매 신청", "column": "익익영업일 이체예상금액", "raw_value": "-508,615,208", "rule": "future_t03_request"}}]`

### `1` / `non_thinking` / `12_동양생명_20260318_html`
- repair count: `16`
- repair sample: `[{"fund_code": "BBI100", "from": "변액유니버설종신-채권형", "to": "변액유니버셜종신-채권형"}, {"fund_code": "BBI100", "from": "변액유니버설종신-채권형", "to": "변액유니버셜종신-채권형"}, {"fund_code": "BBIM20", "from": "변액유니버설종신-채권형 Ⅱ (채권_삼성)", "to": "변액유니버셜종신-채권형Ⅱ(채권_삼성)"}, {"fund_code": "BBIM20", "from": "변액유니버설종신-채권형 Ⅱ (채권_삼성)", "to": "변액유니버셜종신-채권형Ⅱ(채권_삼성)"}, {"fund_code": "BBJ650", "from": "변액연금 Ⅱ 주식혼합1형(ETF_삼성)", "to": "변액연금Ⅱ 주식혼합1형(ETF_삼성)"}, {"fund_code": "BBJ650", "from": "변액연금 Ⅱ 주식혼합1형(ETF_삼성)", "to": "변액연금Ⅱ 주식혼합1형(ETF_삼성)"}, {"fund_code": "BBJ770", "from": "변액연금 Ⅱ 주식혼합2형(ETF_삼성)", "to": "변액연금Ⅱ 주식혼합2형(ETF_삼성)"}, {"fund_code": "BBJ770", "from": "변액연금 Ⅱ 주식혼합2형(ETF_삼성)", "to": "변액연금Ⅱ 주식혼합2형(ETF_삼성)"}]`

### `1` / `thinking` / `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml`
- repair count: `1`
- repair sample: `[{"type": "source_table_order_added", "order": {"fund_code": "V63053", "fund_name": "보장성변액 글로벌멀티에셋안정배분형III(삼성혼합)", "settle_class": "1", "order_type": "1", "base_date": "2025-11-28", "t_day": "03", "transfer_amount": "508,615,208"}, "evidence": {"row_index": 17, "row_type": "환매 신청", "column": "익익영업일 이체예상금액", "raw_value": "-508,615,208", "rule": "future_t03_request"}}]`

## Notes

- `response_format={"type":"json_object"}`와 Novita top-level `enable_thinking`/`separate_reasoning`을 요청마다 명시했다.
- HTTP 400 계열 파라미터 거부, 인증 오류, image input 미지원, timeout은 `BLOCKED`로 기록하며 파라미터를 제거해 재시도하지 않았다.
- JSON/CSV exact 비교는 status, base_date, issues, 주문 canonical field 기준으로 수행했다.
