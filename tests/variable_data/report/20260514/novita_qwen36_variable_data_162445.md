# Novita Qwen3.6 지시서 추출 파라미터 테스트 보고서

- 실행 일시: `2026-05-14 16:28:34 KST`
- run id: `novita_qwen36_variable_data_162445`
- overall verdict: `PASS`
- model: `qwen/qwen3.6-27b`
- endpoint: `https://api.novita.ai/openai/v1/chat/completions`
- cycles: `1`
- artifact root: [novita_qwen36_variable_data_162445_artifacts](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_162445_artifacts>)
- deterministic 보강 사용 여부: `false`
- API key 저장 여부: `false`

## 테스트 대상

| Case | 거래처 | Source | Expected | Base date | Strategy |
| --- | --- | --- | ---: | --- | --- |
| `12_동양생명_20260318_html` | 동양생명 | [동양생명_20260318.html](</Users/bhkim/Documents/codex_prj_sam_asset/document/동양생명_20260318.html>) | `40` | `2026-03-18` | `dongyang_two_stage` |

## 파라미터 프로필

| Profile | enable_thinking | temperature | top_p | top_k | min_p | presence_penalty | repetition_penalty | max_tokens |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `thinking` | `true` | `0.6` | `0.95` | `20` | `0.0` | `0.0` | `1.0` | `32768` |

## Verdict Summary

- PASS: `1`
- FAIL: `0`
- BLOCKED: `0`

| Profile | Total | PASS | FAIL | BLOCKED | Avg elapsed(s) | Avg total tokens |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `thinking` | `1` | `1` | `0` | `0` | `228.304` | `41436.0` |

## 실행 결과

| Cycle | Profile | Case | Verdict | Orders | Expected | JSON exact | CSV exact | Calls | Elapsed(s) | Tokens | Reasoning | Artifact |
| ---: | --- | --- | --- | ---: | ---: | --- | --- | ---: | ---: | ---: | --- | --- |
| `1` | `thinking` | `12_동양생명_20260318_html` | `PASS` | `40` | `40` | `True` | `True` | `5` | `228.304` | `41436` | `True` | [artifact](</Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/report/20260514/novita_qwen36_variable_data_162445_artifacts/executions/cycle_01/thinking/12_동양생명_20260318_html>) |

## LLM 처리 시간 및 Token Usage

| Cycle | Profile | Case | Calls | LLM elapsed(s) | Prompt tokens | Completion tokens | Total tokens | Finish reasons |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `1` | `thinking` | `12_동양생명_20260318_html` | `5` | `228.304` | `15734` | `25702` | `41436` | `{"settlement_attempt_1": "stop", "settlement_attempt_2": "stop", "settlement_attempt_3": "stop", "settlement_attempt_4": "stop", "forecast_attempt_1": "stop"}` |

### 호출별 Usage

#### `1` / `thinking` / `12_동양생명_20260318_html`

| Call | Prompt tokens | Completion tokens | Total tokens |
| --- | ---: | ---: | ---: |
| `settlement_attempt_1` | `2899` | `3826` | `6725` |
| `settlement_attempt_2` | `2899` | `3186` | `6085` |
| `settlement_attempt_3` | `2899` | `4627` | `7526` |
| `settlement_attempt_4` | `2899` | `8354` | `11253` |
| `forecast_attempt_1` | `4138` | `5709` | `9847` |

## Notes

- `response_format={"type":"json_object"}`와 Novita top-level `enable_thinking`/`separate_reasoning`을 요청마다 명시했다.
- HTTP 400 계열 파라미터 거부, 인증 오류, image input 미지원, timeout은 `BLOCKED`로 기록하며 파라미터를 제거해 재시도하지 않았다.
- JSON/CSV exact 비교는 status, base_date, issues, 주문 canonical field 기준으로 수행했다.
