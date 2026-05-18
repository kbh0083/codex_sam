# Novita Qwen3.6 지시서 추출 파라미터 테스트 보고서

- 실행 일시: `2026-05-14 17:06:45 KST`
- run id: `novita_qwen36_variable_data_165218`
- overall verdict: `PASS`
- model: `qwen/qwen3.6-27b`
- endpoint: `https://api.novita.ai/openai/v1/chat/completions`
- cycles: `2`
- artifact root: [novita_qwen36_variable_data_165218_artifacts](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_165218_artifacts>)
- deterministic 보강 사용 여부: `false`
- API key 저장 여부: `false`

## 테스트 대상

| Case | 거래처 | Source | Expected | Base date | Strategy |
| --- | --- | --- | ---: | --- | --- |
| `33_운용지시서_KDB생명_20260427_eml` | KDB생명 | [운용지시서(KDB생명)_20260427.eml](</Users/bhkim/Documents/codex_prj_sam_asset/document/운용지시서(KDB생명)_20260427.eml>) | `12` | `2026-04-27` | `single_prompt` |
| `14_라이나_250826_xlsx` | 라이나생명 | [라이나_250826.xlsx](</Users/bhkim/Documents/codex_prj_sam_asset/document/라이나_250826.xlsx>) | `10` | `2025-08-26` | `single_prompt` |
| `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | 한화생명 | [삼성자산_한화생명_설정_해지_내역서_20251128.eml](</Users/bhkim/Documents/codex_prj_sam_asset/document/삼성자산_한화생명_설정_해지_내역서_20251128.eml>) | `3` | `2025-11-28` | `single_prompt` |
| `12_동양생명_20260318_html` | 동양생명 | [동양생명_20260318.html](</Users/bhkim/Documents/codex_prj_sam_asset/document/동양생명_20260318.html>) | `40` | `2026-03-18` | `dongyang_two_stage` |
| `37_카디프_251127_pdf` | 카디프생명 | [카디프_251127.pdf](</Users/bhkim/Documents/codex_prj_sam_asset/document/카디프_251127.pdf>) | `46` | `2025-11-27` | `single_prompt` |

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
| `non_thinking` | `10` | `10` | `0` | `0` | `23.946` | `6536.4` |
| `thinking` | `10` | `10` | `0` | `0` | `62.556` | `10876.0` |

## 실행 결과

| Cycle | Profile | Case | Verdict | Orders | Expected | JSON exact | CSV exact | Calls | Elapsed(s) | Tokens | Reasoning | Artifact |
| ---: | --- | --- | --- | ---: | ---: | --- | --- | ---: | ---: | ---: | --- | --- |
| `1` | `thinking` | `33_운용지시서_KDB생명_20260427_eml` | `PASS` | `12` | `12` | `True` | `True` | `1` | `44.809` | `8726` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_165218_artifacts/executions/cycle_01/thinking/33_운용지시서_KDB생명_20260427_eml>) |
| `1` | `thinking` | `14_라이나_250826_xlsx` | `PASS` | `10` | `10` | `True` | `True` | `1` | `23.678` | `5705` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_165218_artifacts/executions/cycle_01/thinking/14_라이나_250826_xlsx>) |
| `1` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `PASS` | `3` | `3` | `True` | `True` | `1` | `33.966` | `6666` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_165218_artifacts/executions/cycle_01/thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml>) |
| `1` | `thinking` | `12_동양생명_20260318_html` | `PASS` | `40` | `40` | `True` | `True` | `2` | `72.46` | `13097` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_165218_artifacts/executions/cycle_01/thinking/12_동양생명_20260318_html>) |
| `1` | `thinking` | `37_카디프_251127_pdf` | `PASS` | `46` | `46` | `True` | `True` | `1` | `83.612` | `14289` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_165218_artifacts/executions/cycle_01/thinking/37_카디프_251127_pdf>) |
| `1` | `non_thinking` | `33_운용지시서_KDB생명_20260427_eml` | `PASS` | `12` | `12` | `True` | `True` | `1` | `12.484` | `4736` | `False` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_165218_artifacts/executions/cycle_01/non_thinking/33_운용지시서_KDB생명_20260427_eml>) |
| `1` | `non_thinking` | `14_라이나_250826_xlsx` | `PASS` | `10` | `10` | `True` | `True` | `1` | `9.533` | `4124` | `False` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_165218_artifacts/executions/cycle_01/non_thinking/14_라이나_250826_xlsx>) |
| `1` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `PASS` | `3` | `3` | `True` | `True` | `1` | `6.297` | `4179` | `False` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_165218_artifacts/executions/cycle_01/non_thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml>) |
| `1` | `non_thinking` | `12_동양생명_20260318_html` | `PASS` | `40` | `40` | `True` | `True` | `2` | `29.699` | `9583` | `False` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_165218_artifacts/executions/cycle_01/non_thinking/12_동양생명_20260318_html>) |
| `1` | `non_thinking` | `37_카디프_251127_pdf` | `PASS` | `46` | `46` | `True` | `True` | `1` | `39.785` | `10060` | `False` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_165218_artifacts/executions/cycle_01/non_thinking/37_카디프_251127_pdf>) |
| `2` | `thinking` | `33_운용지시서_KDB생명_20260427_eml` | `PASS` | `12` | `12` | `True` | `True` | `1` | `48.15` | `8393` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_165218_artifacts/executions/cycle_02/thinking/33_운용지시서_KDB생명_20260427_eml>) |
| `2` | `thinking` | `14_라이나_250826_xlsx` | `PASS` | `10` | `10` | `True` | `True` | `1` | `37.759` | `6364` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_165218_artifacts/executions/cycle_02/thinking/14_라이나_250826_xlsx>) |
| `2` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `PASS` | `3` | `3` | `True` | `True` | `1` | `47.478` | `7618` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_165218_artifacts/executions/cycle_02/thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml>) |
| `2` | `thinking` | `12_동양생명_20260318_html` | `PASS` | `40` | `40` | `True` | `True` | `2` | `163.798` | `24621` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_165218_artifacts/executions/cycle_02/thinking/12_동양생명_20260318_html>) |
| `2` | `thinking` | `37_카디프_251127_pdf` | `PASS` | `46` | `46` | `True` | `True` | `1` | `69.848` | `13281` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_165218_artifacts/executions/cycle_02/thinking/37_카디프_251127_pdf>) |
| `2` | `non_thinking` | `33_운용지시서_KDB생명_20260427_eml` | `PASS` | `12` | `12` | `True` | `True` | `1` | `14.702` | `4736` | `False` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_165218_artifacts/executions/cycle_02/non_thinking/33_운용지시서_KDB생명_20260427_eml>) |
| `2` | `non_thinking` | `14_라이나_250826_xlsx` | `PASS` | `10` | `10` | `True` | `True` | `1` | `13.546` | `4124` | `False` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_165218_artifacts/executions/cycle_02/non_thinking/14_라이나_250826_xlsx>) |
| `2` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `PASS` | `3` | `3` | `True` | `True` | `1` | `8.054` | `4179` | `False` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_165218_artifacts/executions/cycle_02/non_thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml>) |
| `2` | `non_thinking` | `12_동양생명_20260318_html` | `PASS` | `40` | `40` | `True` | `True` | `2` | `61.693` | `9583` | `False` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_165218_artifacts/executions/cycle_02/non_thinking/12_동양생명_20260318_html>) |
| `2` | `non_thinking` | `37_카디프_251127_pdf` | `PASS` | `46` | `46` | `True` | `True` | `1` | `43.662` | `10060` | `False` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_165218_artifacts/executions/cycle_02/non_thinking/37_카디프_251127_pdf>) |

## LLM 처리 시간 및 Token Usage

| Cycle | Profile | Case | Calls | LLM elapsed(s) | Prompt tokens | Completion tokens | Total tokens | Finish reasons |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `1` | `thinking` | `33_운용지시서_KDB생명_20260427_eml` | `1` | `44.809` | `3361` | `5365` | `8726` | `{"extract_attempt_1": "stop"}` |
| `1` | `thinking` | `14_라이나_250826_xlsx` | `1` | `23.678` | `3062` | `2643` | `5705` | `{"extract_attempt_1": "stop"}` |
| `1` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `33.966` | `3696` | `2970` | `6666` | `{"extract_attempt_1": "stop"}` |
| `1` | `thinking` | `12_동양생명_20260318_html` | `2` | `72.46` | `7038` | `6059` | `13097` | `{"settlement_attempt_1": "stop", "forecast_attempt_1": "stop"}` |
| `1` | `thinking` | `37_카디프_251127_pdf` | `1` | `83.612` | `5592` | `8697` | `14289` | `{"extract_attempt_1": "stop"}` |
| `1` | `non_thinking` | `33_운용지시서_KDB생명_20260427_eml` | `1` | `12.484` | `3365` | `1371` | `4736` | `{"extract_attempt_1": "stop"}` |
| `1` | `non_thinking` | `14_라이나_250826_xlsx` | `1` | `9.533` | `3066` | `1058` | `4124` | `{"extract_attempt_1": "stop"}` |
| `1` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `6.297` | `3700` | `479` | `4179` | `{"extract_attempt_1": "stop"}` |
| `1` | `non_thinking` | `12_동양생명_20260318_html` | `2` | `29.699` | `7045` | `2538` | `9583` | `{"settlement_attempt_1": "stop", "forecast_attempt_1": "stop"}` |
| `1` | `non_thinking` | `37_카디프_251127_pdf` | `1` | `39.785` | `5596` | `4464` | `10060` | `{"extract_attempt_1": "stop"}` |
| `2` | `thinking` | `33_운용지시서_KDB생명_20260427_eml` | `1` | `48.15` | `3361` | `5032` | `8393` | `{"extract_attempt_1": "stop"}` |
| `2` | `thinking` | `14_라이나_250826_xlsx` | `1` | `37.759` | `3062` | `3302` | `6364` | `{"extract_attempt_1": "stop"}` |
| `2` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `47.478` | `3696` | `3922` | `7618` | `{"extract_attempt_1": "stop"}` |
| `2` | `thinking` | `12_동양생명_20260318_html` | `2` | `163.798` | `7038` | `17583` | `24621` | `{"settlement_attempt_1": "stop", "forecast_attempt_1": "stop"}` |
| `2` | `thinking` | `37_카디프_251127_pdf` | `1` | `69.848` | `5592` | `7689` | `13281` | `{"extract_attempt_1": "stop"}` |
| `2` | `non_thinking` | `33_운용지시서_KDB생명_20260427_eml` | `1` | `14.702` | `3365` | `1371` | `4736` | `{"extract_attempt_1": "stop"}` |
| `2` | `non_thinking` | `14_라이나_250826_xlsx` | `1` | `13.546` | `3066` | `1058` | `4124` | `{"extract_attempt_1": "stop"}` |
| `2` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `8.054` | `3700` | `479` | `4179` | `{"extract_attempt_1": "stop"}` |
| `2` | `non_thinking` | `12_동양생명_20260318_html` | `2` | `61.693` | `7045` | `2538` | `9583` | `{"settlement_attempt_1": "stop", "forecast_attempt_1": "stop"}` |
| `2` | `non_thinking` | `37_카디프_251127_pdf` | `1` | `43.662` | `5596` | `4464` | `10060` | `{"extract_attempt_1": "stop"}` |

### 호출별 Usage

#### `1` / `thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement_attempt_1` | `2899` | `2696` | `5595` |
| `forecast_attempt_1` | `4139` | `3363` | `7502` |

#### `1` / `non_thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement_attempt_1` | `2903` | `1434` | `4337` |
| `forecast_attempt_1` | `4142` | `1104` | `5246` |

#### `2` / `thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement_attempt_1` | `2899` | `6288` | `9187` |
| `forecast_attempt_1` | `4139` | `11295` | `15434` |

#### `2` / `non_thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement_attempt_1` | `2903` | `1434` | `4337` |
| `forecast_attempt_1` | `4142` | `1104` | `5246` |

## Notes

- `response_format={"type":"json_object"}`와 Novita top-level `enable_thinking`/`separate_reasoning`을 요청마다 명시했다.
- HTTP 400 계열 파라미터 거부, 인증 오류, image input 미지원, timeout은 `BLOCKED`로 기록하며 파라미터를 제거해 재시도하지 않았다.
- JSON/CSV exact 비교는 status, base_date, issues, 주문 canonical field 기준으로 수행했다.
