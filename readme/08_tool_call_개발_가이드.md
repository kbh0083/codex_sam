# OpenAI-compatible Tool Call 개발 가이드

> last updated: 2026-05-14 KST

이 문서는 OpenAI-compatible Chat Completions API에서 tool call을 개발하고 검증할 때 따르는 실무 기준이다. 특정 endpoint, 테스트 환경, 보고서 경로에 의존하지 않고 단독으로 사용할 수 있도록 작성했다.

핵심 결론은 아래와 같다.

- 신규 개발은 `tools/tool_calls` protocol을 기본으로 사용한다.
- legacy `functions/function_call` protocol은 신규 개발에 사용하지 않는다.
- 모델은 tool을 직접 실행하지 않는다. 애플리케이션이 tool call을 검증하고 실행한 뒤 결과를 다시 모델에 전달한다.
- 민감정보를 다루는 tool은 prompt, tool arguments, artifact, 최종 답변 전 구간에서 redaction과 재노출 방지 기준을 적용한다.

## 1. 기본 생성 파라미터

Tool call 개발에서는 창의적 생성보다 구조 안정성과 재현성이 중요하다. 기본 호출값은 아래처럼 보수적으로 둔다.

| 항목 | 권장값 |
| --- | --- |
| `temperature` | `0` |
| `top_p` | `1.0` 또는 생략 |
| `stream` | `false` |
| `max_tokens` | tool schema와 결과 길이에 맞게 설정 |
| `tool_choice` | 기본 `auto`, 검증 목적이면 forced 또는 `none` |
| thinking/reasoning mode | 기본 비활성, provider가 지원하면 명시적으로 끔 |

예시:

```json
{
  "model": "your-model",
  "messages": [
    {"role": "user", "content": "서울 날씨를 조회해줘."}
  ],
  "temperature": 0,
  "stream": false,
  "tools": [],
  "tool_choice": "auto"
}
```

Provider나 serving framework마다 thinking/reasoning option 이름이 다르다. 구조화된 tool call이 필요한 경로에서는 중간 reasoning 출력이 응답 구조를 오염시키지 않도록, 해당 옵션을 비활성화하는 preflight를 먼저 수행한다.

## 2. Tool Call과 Function Call

`tool call`은 현재 권장되는 방식이다. 요청에는 `tools` 배열을 넣고, 응답에서는 `choices[0].message.tool_calls[]`를 읽는다.

```json
{
  "tools": [
    {
      "type": "function",
      "function": {
        "name": "get_weather",
        "description": "Get deterministic weather for a city.",
        "parameters": {
          "type": "object",
          "additionalProperties": false,
          "required": ["city"],
          "properties": {
            "city": {"type": "string"}
          }
        }
      }
    }
  ],
  "tool_choice": "auto"
}
```

응답의 핵심 필드는 아래 형태다.

```json
{
  "message": {
    "role": "assistant",
    "tool_calls": [
      {
        "id": "call_123",
        "type": "function",
        "function": {
          "name": "get_weather",
          "arguments": "{\"city\":\"서울\"}"
        }
      }
    ]
  }
}
```

`function call`은 구형 호환 방식이다. 요청에는 `functions`와 `function_call`을 넣고, 응답의 `message.function_call`을 읽는다. 신규 개발에서는 provider별 호환성 차이를 줄이기 위해 `tools/tool_calls`를 사용한다.

## 3. Endpoint Compatibility Checklist

새 endpoint나 모델을 붙일 때는 본 개발에 들어가기 전에 아래 항목을 확인한다.

- `/chat/completions` 또는 이에 준하는 chat endpoint가 HTTP 200으로 응답한다.
- `tools` 배열과 `tool_choice: "auto"` 요청이 수락된다.
- 응답에서 `choices[0].message.tool_calls[]`가 생성된다.
- `tool_calls[].function.name`이 요청 schema의 function name과 일치한다.
- `tool_calls[].function.arguments`가 JSON 문자열이며 object로 parse된다.
- forced `tool_choice`가 특정 tool 호출을 유도할 수 있다.
- `tool_choice: "none"`에서 tool call이 생성되지 않는다.
- tool result message를 포함한 두 번째 호출이 정상 처리된다.
- thinking/reasoning mode를 켰을 때 빈 tool call, pseudo markup, 비JSON arguments가 생기지 않는지 확인한다.
- timeout, 429, 5xx 같은 일시 실패에 대한 retry 정책이 애플리케이션에 정의돼 있다.

Endpoint가 legacy `functions/function_call`을 지원하더라도 신규 기능의 성공 기준으로 삼지 않는다. 호환성 검증이 필요할 때만 별도 capability test로 분리한다.

## 4. Schema 작성 규칙

- tool은 `{"type": "function", "function": {...}}` 형태로 정의한다.
- `function.name`은 실제 구현체 이름과 정확히 맞춘다.
- `description`은 모델이 호출 여부를 판단할 수 있을 만큼 구체적으로 작성한다.
- `parameters`는 JSON Schema object로 작성한다.
- 필수 인자는 `required`에 명시한다.
- 모델이 임의 인자를 만들지 않도록 가능한 경우 `additionalProperties: false`를 사용한다.
- 선택지가 제한된 값은 `enum`으로 고정한다.
- 날짜, 통화, 식별자처럼 형식이 중요한 값은 `description`에 허용 형식을 명시한다.

Schema 예시:

```json
{
  "type": "function",
  "function": {
    "name": "lookup_order_status",
    "description": "Look up a single order status by order_id.",
    "parameters": {
      "type": "object",
      "additionalProperties": false,
      "required": ["order_id"],
      "properties": {
        "order_id": {
          "type": "string",
          "description": "Stable order identifier, for example ORD-20260514-001."
        }
      }
    }
  }
}
```

## 5. 호출 제어

Tool 호출 제어는 `auto`, forced, `none` 세 가지로 나눈다.

### auto

`tool_choice: "auto"`는 사용 가능한 tool 목록을 모델에 제공하되, 실제 호출 여부와 호출할 tool을 모델이 판단하게 하는 기본 방식이다.

```json
{
  "model": "your-model",
  "messages": [
    {"role": "user", "content": "주문 ORD-20260514-001 상태를 확인해줘."}
  ],
  "tools": [
    {
      "type": "function",
      "function": {
        "name": "lookup_order_status",
        "description": "Look up a single order status by order_id.",
        "parameters": {
          "type": "object",
          "additionalProperties": false,
          "required": ["order_id"],
          "properties": {
            "order_id": {"type": "string"}
          }
        }
      }
    }
  ],
  "tool_choice": "auto"
}
```

auto 모드에서 모델은 아래 중 하나를 반환할 수 있다.

- tool이 필요하다고 판단하면 `message.tool_calls[]`를 반환한다.
- tool이 필요 없다고 판단하면 `tool_calls` 없이 일반 assistant 답변을 반환한다.
- 여러 tool이 필요하다고 판단하면 `tool_calls[]`에 2개 이상의 호출을 반환할 수 있다.

### forced

특정 tool 호출을 반드시 검증해야 하면 forced `tool_choice`를 사용한다.

```json
{
  "tool_choice": {
    "type": "function",
    "function": {
      "name": "lookup_order_status"
    }
  }
}
```

### none

tool 호출이 발생하지 않아야 하는 경로를 검증할 때는 `tool_choice: "none"`을 사용한다.

```json
{
  "tool_choice": "none"
}
```

## 6. Arguments 처리

`message.tool_calls[].function.arguments`는 문자열 JSON으로 온다. 애플리케이션은 이 값을 즉시 실행하지 말고 아래 순서로 검증한다.

1. 문자열 JSON parse
2. object 여부 확인
3. function name allowlist 확인
4. JSON Schema validation
5. 추가 인자 차단 또는 명시적 무시 정책 적용
6. 업무 규칙 validation
7. tool 실행

실패 처리 기준:

- JSON parse 실패는 실패로 처리한다.
- object가 아닌 arguments는 실패로 처리한다.
- 모르는 function name은 실패로 처리한다.
- 필수 인자가 없거나 enum 밖 값이면 실패로 처리한다.
- 민감정보가 포함된 arguments는 실행 전 redaction 또는 차단한다.

## 7. Tool Result Loop

모델은 tool을 직접 실행하지 않는다. 애플리케이션이 tool을 실행하고, 실행 결과를 `role: "tool"` 메시지로 다시 모델에 전달해 최종 답변을 만든다.

기본 흐름:

1. user message를 보낸다.
2. assistant response의 `tool_calls[]`를 읽는다.
3. 애플리케이션이 arguments를 parse하고 검증한다.
4. 애플리케이션이 로컬 또는 외부 tool을 실행한다.
5. `role: "tool"` 메시지에 실행 결과를 넣어 다시 호출한다.
6. 최종 assistant response를 사용자 답변으로 사용한다.

두 번째 요청의 `messages` 배열 예시:

```json
[
  {"role": "user", "content": "주문 ORD-20260514-001 상태를 확인해줘."},
  {
    "role": "assistant",
    "tool_calls": [
      {
        "id": "call_123",
        "type": "function",
        "function": {
          "name": "lookup_order_status",
          "arguments": "{\"order_id\":\"ORD-20260514-001\"}"
        }
      }
    ]
  },
  {
    "role": "tool",
    "tool_call_id": "call_123",
    "content": "{\"order_id\":\"ORD-20260514-001\",\"status\":\"processing\"}"
  }
]
```

Tool result는 필요한 최소 정보만 포함한다. 원문 사용자 입력 전체, 비밀값, 실행 로그, 불필요한 개인정보를 그대로 재주입하지 않는다.

## 8. 민감정보 마스킹 규칙

민감정보 tool 개발과 검증에는 실제 고객정보, 실제 계좌정보, 실제 비밀값을 사용하지 않는다. 테스트 입력은 synthetic fixture를 사용한다.

마스킹 대상:

- 고객명
- 주민/외국인등록번호 형식
- 생년월일
- 전화번호
- 이메일
- 주소
- 고객번호
- 은행명
- 예금주
- 계좌번호
- 카드번호

Artifact 보안용으로 아래 값도 저장 전 제거한다.

- `Authorization`
- API key 형태의 secret
- `Bearer ...` token
- session id, refresh token, access token

민감정보 tool result loop에서는 일반 tool loop보다 더 강한 기준을 적용한다.

- 두 번째 요청에 원문 prompt를 그대로 재주입하지 않는다.
- assistant `tool_calls[].function.arguments`도 redaction을 통과한 값만 사용한다.
- system 또는 developer guard로 원문 재노출 금지를 명시한다.
- 최종 답변에 원문 synthetic 값, `<tool_call>`, `<function>` 형태가 있으면 실패로 처리한다.
- 보고서와 JSON artifact는 저장 직전 redaction을 통과해야 한다.

## 9. 금지 사항

- 실제 고객정보, 실제 계좌정보, 실제 비밀값을 prompt나 artifact에 넣지 않는다.
- 신규 개발에서 legacy `functions/function_call`을 기본 protocol로 사용하지 않는다.
- tool result loop에서 민감정보 원문 prompt를 다시 넣지 않는다.
- 모델 응답을 검증 없이 그대로 실행하거나 저장하지 않는다.
- tool name을 prompt 문자열에서 ad hoc으로 파싱해 실행하지 않는다.
- endpoint별 compatibility gap을 애플리케이션 업무 실패와 섞어 판정하지 않는다.

## 10. 검증 절차

배포 전 최소 검증은 아래 순서로 수행한다.

1. 기본 chat completion preflight
2. `tools/tool_calls` auto 호출
3. forced `tool_choice` 호출
4. `tool_choice: "none"` 호출
5. arguments JSON parse 실패 케이스
6. schema validation 실패 케이스
7. tool result loop 2차 호출
8. 다중 tool call 응답 처리
9. timeout, 429, 5xx retry 처리
10. 민감정보 redaction 및 artifact scan

판정 기준:

- `PASS`: protocol field가 기대 위치에 있고, arguments parse와 schema validation이 통과하며, tool result loop와 artifact redaction이 정상이다.
- `PARTIAL`: endpoint는 동작하지만 일부 protocol capability가 빠져 있다.
- `FAIL`: 잘못된 tool name, JSON parse 실패, schema mismatch, tool result loop 실패, 민감정보 원문 누출, artifact leak이 발생한 경우다.
- `BLOCKED`: 인증, 모델 조회, 최소 chat completion preflight가 실패해 케이스 실행을 시작할 수 없는 경우다.

## 11. 운영 권장

운영 코드에서는 tool call을 일반 텍스트 응답보다 더 엄격하게 다룬다.

- tool 호출 가능 function은 allowlist로 제한한다.
- arguments는 schema와 업무 규칙을 모두 통과해야 실행한다.
- tool 실행 결과는 최소 정보만 모델에 되돌려준다.
- 민감정보가 포함될 수 있는 경로는 저장 전 redaction을 강제한다.
- endpoint capability test와 업무 정확도 test를 분리한다.
- model/provider 변경 시 compatibility checklist를 재실행한다.
