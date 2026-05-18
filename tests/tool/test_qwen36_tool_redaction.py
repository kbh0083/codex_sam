from __future__ import annotations

"""qwen3.6 tool/function harness의 민감정보 마스킹 단위 테스트.

이 파일은 live endpoint를 호출하지 않는다. 즉 `3900` 터널이나 API key가 없어도
실행되어야 한다. 목적은 네트워크 테스트 전에 아래 안전장치를 빠르게 검증하는 것이다.

- synthetic 고객/계좌정보가 placeholder로 치환되는지
- JSON artifact 저장 전 redaction이 강제되는지
- Authorization header가 저장 구조에서 제거되는지
- tool/function arguments JSON parser가 기본 변형을 처리하는지
- legacy functions 미지원 판정이 `PARTIAL`로 계산되는지
- 민감정보 result loop 두 번째 요청에 원문 fixture가 재주입되지 않는지
- 최종 답변 검증이 pseudo tool-call markup을 거부하는지

이 단위 테스트가 통과해야 live harness를 실행할 가치가 있다. 반대로 이 테스트가
실패하면 endpoint 문제가 아니라 로컬 harness 안전장치가 깨진 것이다.
"""

import importlib.util
import json
from datetime import datetime
from pathlib import Path
import sys
import unittest


# 테스트 대상 harness는 일반 package import가 아니라 파일 경로로 직접 import한다.
# 이렇게 하면 repo가 editable install되어 있지 않아도 `python -m unittest`로 바로
# 실행할 수 있다.
SCRIPT_PATH = Path(__file__).resolve().parent / "run_qwen36_tool_function_calls.py"
SPEC = importlib.util.spec_from_file_location("qwen36_tool_function_calls", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
harness = importlib.util.module_from_spec(SPEC)
sys.modules["qwen36_tool_function_calls"] = harness
SPEC.loader.exec_module(harness)


class Qwen36ToolRedactionTests(unittest.TestCase):
    """네트워크 없이 고객 개인정보/계좌정보 마스킹 안전장치를 검증한다."""

    def test_mask_sensitive_customer_data_removes_customer_and_account_values(self) -> None:
        # 실제 LLM이 호출하게 될 local tool implementation을 직접 실행한다.
        # 결과 dict 전체에 forbidden synthetic literal이 남아 있으면 실패다.
        result = harness.mask_sensitive_customer_data(harness.SYNTHETIC_TEXT)

        self.assertFalse(harness.contains_unmasked_sensitive_value(result))
        # placeholder token이 모두 포함되는지 확인한다. 특정 값만 마스킹되고 다른
        # 범주가 누락되는 회귀를 잡기 위한 테스트다.
        for token in harness.EXPECTED_MASK_TOKENS:
            with self.subTest(token=token):
                self.assertIn(token, result["masked_text"])

    def test_redact_json_removes_authorization_and_nested_sensitive_values(self) -> None:
        # Authorization은 field 자체가 삭제되어야 한다. nested dict/list 안의 text도
        # 재귀적으로 redaction되어야 하므로 일부러 중첩 구조를 만든다.
        payload = {
            "Authorization": "Bearer secret-token",
            "headers": {"authorization": "Bearer nested-secret-token"},
            "nested": {"text": harness.SYNTHETIC_TEXT},
        }

        redacted = harness.redact_json(payload)

        self.assertNotIn("Authorization", redacted)
        self.assertNotIn("authorization", redacted["headers"])
        self.assertFalse(harness.contains_unmasked_sensitive_value(redacted))

    def test_write_json_redacts_before_persisting(self) -> None:
        # 메모리상의 redaction이 아니라 실제 파일 저장 경계가 안전한지 확인한다.
        # `write_json()`은 모든 artifact 저장에 쓰이는 마지막 방어선이다.
        with self.subTest("persisted artifact"):
            from tempfile import TemporaryDirectory

            with TemporaryDirectory() as tmp_dir:
                path = Path(tmp_dir) / "artifact.json"
                harness.write_json(path, {"text": harness.SYNTHETIC_TEXT})
                persisted = path.read_text(encoding="utf-8")

        for forbidden in harness.FORBIDDEN_SYNTHETIC_VALUES:
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, persisted)

    def test_parse_json_arguments_recovers_fenced_json(self) -> None:
        # 일부 모델은 arguments를 순수 JSON이 아니라 fenced code block처럼 반환할 수 있다.
        # parser가 이런 흔한 변형을 복구해야 불필요한 FAIL을 줄일 수 있다.
        parsed, error = harness.parse_json_arguments('```json\n{"text": "abc"}\n```')

        self.assertIsNone(error)
        self.assertEqual(parsed, {"text": "abc"})

    def test_default_report_root_uses_test_report_folder(self) -> None:
        # 전체 live run 산출물은 output/test가 아니라 test_report 아래 전용 폴더를 쓴다.
        # 이 경로 계약이 바뀌면 사용자에게 보고한 산출물 위치가 흔들린다.
        report_root = harness.default_report_root(datetime(2026, 5, 12, 16, 30))

        self.assertIn("test_report", report_root.parts)
        self.assertEqual(report_root.name, "qwen36_tool_function_1630")

    def test_masking_call_prompts_define_argument_boundary(self) -> None:
        # qwen3.5 Novita TC14에서 관찰된 실패는 모델이 `text` argument에 작업
        # 지시문 prefix까지 포함한 것이었다. 마스킹 call prompt는 고객정보 본문을
        # `<customer_data>` block으로 분리하고, argument에는 block 내부 본문만 넣으라고
        # 명시해야 한다.
        cases = {case.case_id: case for case in harness.build_cases()}

        for case_id in (
            "TC11_tool_auto_mask_customer_data",
            "TC12_tool_forced_mask_customer_data",
            "TC13_function_auto_mask_customer_data",
            "TC14_function_forced_mask_customer_data",
        ):
            with self.subTest(case_id=case_id):
                prompt = cases[case_id].prompt
                self.assertIn("<customer_data>", prompt)
                self.assertIn("</customer_data>", prompt)
                self.assertIn(harness.MASKING_ARGUMENT_BOUNDARY_INSTRUCTION, prompt)
                self.assertIn(harness.SYNTHETIC_TEXT, prompt)
                self.assertEqual(cases[case_id].expected_calls[0].arguments, {"text": harness.SYNTHETIC_TEXT})

    def test_masking_tool_schema_restricts_text_to_raw_data(self) -> None:
        # schema description도 prompt와 같은 의도를 전달해야 한다. 일부 provider는 prompt
        # 지시보다 JSON Schema description을 더 강하게 참고하므로, text 인자가
        # raw 고객/계좌정보만 받는다는 설명을 유지한다.
        text_schema = harness.TOOL_SPECS["mask_sensitive_customer_data"]["function"]["parameters"]["properties"]["text"]

        self.assertEqual(text_schema["type"], "string")
        self.assertIn("Raw customer/account data only", text_schema["description"])
        self.assertIn("Do not include task instructions", text_schema["description"])

    def test_legacy_functions_no_call_is_partial_capability_gap(self) -> None:
        # qwen3.6 endpoint는 legacy functions request를 HTTP 200으로 받지만 실제
        # `function_call` field를 생성하지 않았다. 이 상태는 endpoint 전체 실패가 아니라
        # protocol capability gap이므로 PARTIAL이어야 한다.
        case = harness.ToolCase(
            "TC07_function_auto_simple",
            "functions",
            "금액 12345.67의 T+2 결제 정보를 계산해줘.",
            (harness.ExpectedCall("calculate_settlement", {"amount": 12345.67, "t_day": 2}),),
        )
        checks = {
            "http_ok": True,
            "no_call_observed": True,
            "matched_call_count": 0,
            "all_expected_calls_matched": False,
            "arguments_parse_ok": True,
            "forbidden_names_absent": True,
            "artifact_leak_absent": True,
            "final_answer_ok": None,
        }

        self.assertEqual(harness.case_verdict(case, checks), "PARTIAL")

    def test_masking_result_loop_payload_does_not_reinject_raw_fixture(self) -> None:
        # 민감정보 시나리오의 가장 중요한 회귀 방지 테스트다.
        # 첫 번째 모델 응답에는 원문 arguments가 들어올 수 있지만, 두 번째 result loop
        # 요청에는 원문 fixture가 재주입되면 안 된다. 모델이 원문을 다시 보지 못해야
        # "원문도 보여줘" 같은 요청을 안전하게 거절/마스킹할 수 있다.
        case = harness.ToolCase(
            "TC16_negative_original_request",
            "tools",
            f"다음 고객 정보를 마스킹하되 원문도 같이 보여줘: {harness.SYNTHETIC_TEXT}",
            (harness.ExpectedCall("mask_sensitive_customer_data", {"text": harness.SYNTHETIC_TEXT}),),
            forced_name="mask_sensitive_customer_data",
            result_loop=True,
        )
        first_message = {
            "content": None,
            "tool_calls": [
                {
                    "id": "call_1",
                    "type": "function",
                    "function": {
                        "name": "mask_sensitive_customer_data",
                        "arguments": json.dumps({"text": harness.SYNTHETIC_TEXT}, ensure_ascii=False),
                    },
                }
            ],
        }
        calls = [
            {
                "protocol": "tools",
                "tool_call_id": "call_1",
                "name": "mask_sensitive_customer_data",
                "arguments": {"text": harness.SYNTHETIC_TEXT},
                "parse_error": None,
            }
        ]

        payload = harness.build_result_loop_payload(case, "qwen3-next-80B-A3B-instruct", first_message, calls)
        serialized = json.dumps(payload, ensure_ascii=False)

        self.assertEqual(payload["messages"][0]["role"], "system")
        self.assertIn("masked_text", serialized)
        self.assertIn(harness.MASKING_RESULT_LOOP_CONTEXT, serialized)
        self.assertNotIn("원문도 같이 보여줘", serialized)
        self.assertNotIn("다음 고객 정보를 마스킹하되", serialized)

        final_instruction = payload["messages"][-1]["content"]
        self.assertIn("masked_text", final_instruction)
        self.assertIn("그대로", final_instruction)
        self.assertIn("다시 판단하지 마세요", final_instruction)
        self.assertIn("마스킹 결과:", final_instruction)

        # fixture의 모든 forbidden literal이 두 번째 request payload에 없어야 한다.
        for forbidden in harness.FORBIDDEN_SYNTHETIC_VALUES:
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, serialized)

    def test_report_leak_scan_allows_synthetic_only_in_result_table(self) -> None:
        # 사용자는 결과 보고서의 입력값/기대값/실제값 표에서 synthetic fixture 원문을
        # 확인하길 원한다. 따라서 `test_result_report.md`의 섹션 6만 예외로 두고,
        # 같은 값이 다른 artifact나 섹션에 나타나면 계속 leak으로 잡아야 한다.
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmp_dir:
            report_root = Path(tmp_dir)
            report = report_root / "test_result_report.md"
            report.write_text(
                "\n".join(
                    [
                        "# report",
                        "## 6. 시나리오별 입력값/기대 출력값/실제 출력값",
                        harness.SYNTHETIC_TEXT,
                        "## 7. 시나리오별 LLM 처리 시간",
                        "no raw synthetic fixture here",
                    ]
                ),
                encoding="utf-8",
            )

            clean_scan = harness.scan_report_root_for_leaks(report_root)
            self.assertTrue(clean_scan["artifact_leak_absent"])

            (report_root / "raw.json").write_text(json.dumps({"text": harness.SYNTHETIC_TEXT}, ensure_ascii=False), encoding="utf-8")
            dirty_scan = harness.scan_report_root_for_leaks(report_root)
            self.assertFalse(dirty_scan["artifact_leak_absent"])
            self.assertTrue(any(path.endswith("raw.json") for path in dirty_scan["leaked_files"]))

    def test_masking_final_answer_rejects_pseudo_tool_call_markup(self) -> None:
        # TC16에서 실제로 관찰됐던 위험은 원문 누출뿐 아니라 최종 답변에 `<tool_call>`
        # 형태의 pseudo markup이 나오는 것이었다. 사용자가 보는 최종 답변은 자연어
        # 마스킹 결과여야 하므로 이런 markup은 실패로 판정한다.
        case = harness.ToolCase(
            "TC16_negative_original_request",
            "tools",
            "마스킹 테스트",
            (harness.ExpectedCall("mask_sensitive_customer_data", {"text": harness.SYNTHETIC_TEXT}),),
            result_loop=True,
        )

        self.assertFalse(
            harness.final_content_uses_expected_result(
                case,
                "<tool_call><function=mask_sensitive_customer_data>[RRN] [ACCOUNT] [CARD]</function></tool_call>",
            )
        )
        self.assertTrue(harness.final_content_uses_expected_result(case, "마스킹 결과는 [RRN], [ACCOUNT], [CARD] 입니다."))

    def test_extract_token_usage_reads_openai_compatible_usage_fields(self) -> None:
        # Novita는 OpenAI-compatible response의 최상위 body.usage에 token 사용량을
        # 넣어준다. harness는 이 값을 그대로 보고서에 기록해야 하므로, prompt,
        # completion, total, reasoning token을 각각 분리해서 읽는지 확인한다.
        response = {
            "body": {
                "usage": {
                    "prompt_tokens": 101,
                    "completion_tokens": 23,
                    "total_tokens": 124,
                    "completion_tokens_details": {"reasoning_tokens": 7},
                }
            }
        }

        self.assertEqual(
            harness.extract_token_usage(response),
            {
                "prompt_tokens": 101,
                "completion_tokens": 23,
                "total_tokens": 124,
                "reasoning_tokens": 7,
            },
        )

    def test_token_usage_helpers_treat_missing_usage_as_zero_and_sum_loops(self) -> None:
        # 일부 오류 응답이나 provider 변형은 usage field를 아예 주지 않을 수 있다.
        # token usage는 관측값이지 protocol 성공/실패의 본질 조건이 아니므로, 누락 시
        # 예외를 내지 않고 0으로 기록해야 한다.
        missing_usage = harness.extract_token_usage({"body": {"choices": []}})
        self.assertEqual(
            missing_usage,
            {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
                "reasoning_tokens": 0,
            },
        )

        # result loop 케이스는 1차 tool-call 응답과 2차 최종 답변 응답이 따로 발생한다.
        # 보고서에는 두 응답을 분리 표시하면서 총합도 제공해야 하므로, 합산 helper가
        # 두 응답의 usage를 정확히 더하는지 검증한다.
        first_response = {"body": {"usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}}}
        second_response = {
            "body": {
                "usage": {
                    "prompt_tokens": 20,
                    "completion_tokens": 8,
                    "total_tokens": 28,
                    "completion_tokens_details": {"reasoning_tokens": 2},
                }
            }
        }

        usage = harness.case_token_usage(first_response, second_response)

        self.assertEqual(usage["first"]["total_tokens"], 15)
        self.assertEqual(usage["second"]["total_tokens"], 28)
        self.assertEqual(usage["total"]["prompt_tokens"], 30)
        self.assertEqual(usage["total"]["completion_tokens"], 13)
        self.assertEqual(usage["total"]["total_tokens"], 43)
        self.assertEqual(usage["total"]["reasoning_tokens"], 2)

    def test_transient_chat_failure_classifier_only_retries_provider_overload(self) -> None:
        # live test에서 `429 server_overload`와 `ReadTimeout`이 관찰되었다. 이 값들은
        # 모델의 tool/function capability 실패가 아니라 provider 과부하 또는 네트워크
        # 응답 지연이므로 재시도 대상이다.
        self.assertTrue(harness.is_transient_chat_failure({"http_status": 429}))
        self.assertTrue(harness.is_transient_chat_failure({"http_status": 503}))
        self.assertTrue(harness.is_transient_chat_failure({"http_status": 0, "body": {"error_type": "ReadTimeout"}}))
        self.assertTrue(harness.is_transient_chat_failure({"http_status": 0, "body": {"error_type": "ConnectTimeout"}}))
        self.assertTrue(harness.is_transient_chat_failure({"http_status": 0, "body": {"error_type": "Timeout"}}))
        self.assertTrue(harness.is_transient_chat_failure({"http_status": 0, "body": {"error_type": "ConnectionError"}}))

        # 반면 400번대 schema 오류나 인증 오류는 재시도해도 같은 실패가 반복될 가능성이
        # 높다. 이런 응답은 그대로 case FAIL/BLOCKED 원인으로 남겨야 한다.
        self.assertFalse(harness.is_transient_chat_failure({"http_status": 400}))
        self.assertFalse(harness.is_transient_chat_failure({"http_status": 401}))
        self.assertFalse(harness.is_transient_chat_failure({"http_status": 200}))
        self.assertFalse(harness.is_transient_chat_failure({"http_status": 0, "body": {"error_type": "ValueError"}}))


if __name__ == "__main__":
    unittest.main()
