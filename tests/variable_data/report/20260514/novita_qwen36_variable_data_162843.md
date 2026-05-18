# Novita Qwen3.6 지시서 추출 파라미터 테스트 보고서

- 실행 일시: `2026-05-14 16:41:12 KST`
- run id: `novita_qwen36_variable_data_162843`
- overall verdict: `PASS`
- model: `qwen/qwen3.6-27b`
- endpoint: `https://api.novita.ai/openai/v1/chat/completions`
- cycles: `2`
- artifact root: [novita_qwen36_variable_data_162843_artifacts](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_162843_artifacts>)
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
| `thinking` | `true` | `0.6` | `0.95` | `20` | `0.0` | `0.0` | `1.0` | `32768` |
| `non_thinking` | `false` | `0` | `1.0` | `20` | `0.0` | `0.0` | `1.0` | `16384` |

## Verdict Summary

- PASS: `16`
- FAIL: `0`
- BLOCKED: `0`

| Profile | Total | PASS | FAIL | BLOCKED | Avg elapsed(s) | Avg total tokens |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `non_thinking` | `8` | `8` | `0` | `0` | `20.981` | `7139.5` |
| `thinking` | `8` | `8` | `0` | `0` | `72.385` | `13475.6` |

## 실행 결과

| Cycle | Profile | Case | Verdict | Orders | Expected | JSON exact | CSV exact | Calls | Elapsed(s) | Tokens | Reasoning | Artifact |
| ---: | --- | --- | --- | ---: | ---: | --- | --- | ---: | ---: | ---: | --- | --- |
| `1` | `thinking` | `33_운용지시서_KDB생명_20260427_eml` | `PASS` | `12` | `12` | `True` | `True` | `1` | `56.745` | `10112` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_162843_artifacts/executions/cycle_01/thinking/33_운용지시서_KDB생명_20260427_eml>) |
| `1` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `PASS` | `3` | `3` | `True` | `True` | `1` | `23.413` | `6091` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_162843_artifacts/executions/cycle_01/thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml>) |
| `1` | `thinking` | `12_동양생명_20260318_html` | `PASS` | `40` | `40` | `True` | `True` | `4` | `160.957` | `30687` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_162843_artifacts/executions/cycle_01/thinking/12_동양생명_20260318_html>) |
| `1` | `thinking` | `37_카디프_251127_pdf` | `PASS` | `46` | `46` | `True` | `True` | `1` | `95.304` | `16826` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_162843_artifacts/executions/cycle_01/thinking/37_카디프_251127_pdf>) |
| `1` | `non_thinking` | `33_운용지시서_KDB생명_20260427_eml` | `PASS` | `12` | `12` | `True` | `True` | `1` | `14.749` | `4736` | `False` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_162843_artifacts/executions/cycle_01/non_thinking/33_운용지시서_KDB생명_20260427_eml>) |
| `1` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `PASS` | `3` | `3` | `True` | `True` | `1` | `10.919` | `4179` | `False` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_162843_artifacts/executions/cycle_01/non_thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml>) |
| `1` | `non_thinking` | `12_동양생명_20260318_html` | `PASS` | `40` | `40` | `True` | `True` | `2` | `25.277` | `9583` | `False` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_162843_artifacts/executions/cycle_01/non_thinking/12_동양생명_20260318_html>) |
| `1` | `non_thinking` | `37_카디프_251127_pdf` | `PASS` | `46` | `46` | `True` | `True` | `1` | `35.505` | `10060` | `False` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_162843_artifacts/executions/cycle_01/non_thinking/37_카디프_251127_pdf>) |
| `2` | `thinking` | `33_운용지시서_KDB생명_20260427_eml` | `PASS` | `12` | `12` | `True` | `True` | `1` | `61.022` | `8303` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_162843_artifacts/executions/cycle_02/thinking/33_운용지시서_KDB생명_20260427_eml>) |
| `2` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `PASS` | `3` | `3` | `True` | `True` | `1` | `30.207` | `6931` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_162843_artifacts/executions/cycle_02/thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml>) |
| `2` | `thinking` | `12_동양생명_20260318_html` | `PASS` | `40` | `40` | `True` | `True` | `2` | `66.743` | `14322` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_162843_artifacts/executions/cycle_02/thinking/12_동양생명_20260318_html>) |
| `2` | `thinking` | `37_카디프_251127_pdf` | `PASS` | `46` | `46` | `True` | `True` | `1` | `84.692` | `14533` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_162843_artifacts/executions/cycle_02/thinking/37_카디프_251127_pdf>) |
| `2` | `non_thinking` | `33_운용지시서_KDB생명_20260427_eml` | `PASS` | `12` | `12` | `True` | `True` | `1` | `13.0` | `4736` | `False` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_162843_artifacts/executions/cycle_02/non_thinking/33_운용지시서_KDB생명_20260427_eml>) |
| `2` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `PASS` | `3` | `3` | `True` | `True` | `1` | `6.601` | `4179` | `False` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_162843_artifacts/executions/cycle_02/non_thinking/22_삼성자산_한화생명_설정_해지_내역서_20251128_eml>) |
| `2` | `non_thinking` | `12_동양생명_20260318_html` | `PASS` | `40` | `40` | `True` | `True` | `2` | `25.322` | `9583` | `False` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_162843_artifacts/executions/cycle_02/non_thinking/12_동양생명_20260318_html>) |
| `2` | `non_thinking` | `37_카디프_251127_pdf` | `PASS` | `46` | `46` | `True` | `True` | `1` | `36.477` | `10060` | `False` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_162843_artifacts/executions/cycle_02/non_thinking/37_카디프_251127_pdf>) |

## LLM 처리 시간 및 Token Usage

| Cycle | Profile | Case | Calls | LLM elapsed(s) | Prompt tokens | Completion tokens | Total tokens | Finish reasons |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `1` | `thinking` | `33_운용지시서_KDB생명_20260427_eml` | `1` | `56.745` | `3361` | `6751` | `10112` | `{"extract_attempt_1": "stop"}` |
| `1` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `23.413` | `3696` | `2395` | `6091` | `{"extract_attempt_1": "stop"}` |
| `1` | `thinking` | `12_동양생명_20260318_html` | `4` | `160.957` | `14076` | `16611` | `30687` | `{"settlement_attempt_1": "stop", "settlement_attempt_2": "stop", "forecast_attempt_1": "stop", "forecast_attempt_2": "stop"}` |
| `1` | `thinking` | `37_카디프_251127_pdf` | `1` | `95.304` | `5592` | `11234` | `16826` | `{"extract_attempt_1": "stop"}` |
| `1` | `non_thinking` | `33_운용지시서_KDB생명_20260427_eml` | `1` | `14.749` | `3365` | `1371` | `4736` | `{"extract_attempt_1": "stop"}` |
| `1` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `10.919` | `3700` | `479` | `4179` | `{"extract_attempt_1": "stop"}` |
| `1` | `non_thinking` | `12_동양생명_20260318_html` | `2` | `25.277` | `7045` | `2538` | `9583` | `{"settlement_attempt_1": "stop", "forecast_attempt_1": "stop"}` |
| `1` | `non_thinking` | `37_카디프_251127_pdf` | `1` | `35.505` | `5596` | `4464` | `10060` | `{"extract_attempt_1": "stop"}` |
| `2` | `thinking` | `33_운용지시서_KDB생명_20260427_eml` | `1` | `61.022` | `3361` | `4942` | `8303` | `{"extract_attempt_1": "stop"}` |
| `2` | `thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `30.207` | `3696` | `3235` | `6931` | `{"extract_attempt_1": "stop"}` |
| `2` | `thinking` | `12_동양생명_20260318_html` | `2` | `66.743` | `7037` | `7285` | `14322` | `{"settlement_attempt_1": "stop", "forecast_attempt_1": "stop"}` |
| `2` | `thinking` | `37_카디프_251127_pdf` | `1` | `84.692` | `5592` | `8941` | `14533` | `{"extract_attempt_1": "stop"}` |
| `2` | `non_thinking` | `33_운용지시서_KDB생명_20260427_eml` | `1` | `13.0` | `3365` | `1371` | `4736` | `{"extract_attempt_1": "stop"}` |
| `2` | `non_thinking` | `22_삼성자산_한화생명_설정_해지_내역서_20251128_eml` | `1` | `6.601` | `3700` | `479` | `4179` | `{"extract_attempt_1": "stop"}` |
| `2` | `non_thinking` | `12_동양생명_20260318_html` | `2` | `25.322` | `7045` | `2538` | `9583` | `{"settlement_attempt_1": "stop", "forecast_attempt_1": "stop"}` |
| `2` | `non_thinking` | `37_카디프_251127_pdf` | `1` | `36.477` | `5596` | `4464` | `10060` | `{"extract_attempt_1": "stop"}` |

### 호출별 Usage

#### `1` / `thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement_attempt_1` | `2899` | `3160` | `6059` |
| `settlement_attempt_2` | `2899` | `3311` | `6210` |
| `forecast_attempt_1` | `4139` | `3817` | `7956` |
| `forecast_attempt_2` | `4139` | `6323` | `10462` |

#### `1` / `non_thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement_attempt_1` | `2903` | `1434` | `4337` |
| `forecast_attempt_1` | `4142` | `1104` | `5246` |

#### `2` / `thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement_attempt_1` | `2899` | `2800` | `5699` |
| `forecast_attempt_1` | `4138` | `4485` | `8623` |

#### `2` / `non_thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement_attempt_1` | `2903` | `1434` | `4337` |
| `forecast_attempt_1` | `4142` | `1104` | `5246` |

## Notes

- `response_format={"type":"json_object"}`와 Novita top-level `enable_thinking`/`separate_reasoning`을 요청마다 명시했다.
- HTTP 400 계열 파라미터 거부, 인증 오류, image input 미지원, timeout은 `BLOCKED`로 기록하며 파라미터를 제거해 재시도하지 않았다.
- JSON/CSV exact 비교는 status, base_date, issues, 주문 canonical field 기준으로 수행했다.
