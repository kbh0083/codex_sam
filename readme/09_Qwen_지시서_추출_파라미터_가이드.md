# Qwen 문서 데이터 추출 파라미터 가이드 (vLLM 기준)

> last updated: 2026-05-18 KST
> scope: `Qwen/Qwen3.5-397B-A17B`, `Qwen/Qwen3.6-27B`

이 문서는 vLLM OpenAI-compatible Chat Completions API에서 Qwen 모델로 계약서, 보고서, 신청서, 지시서, 이메일, PDF, Excel, HTML 같은 일반 문서의 정형 데이터를 추출할 때 사용하는 운영 파라미터 기준을 정리한다.

핵심 결론은 아래와 같다.

- Hugging Face 공식 권장값은 일반 생성 품질 기준이다.
- 문서 데이터 추출은 창의적 답변보다 `JSON 안정성`, `원문 보존`, `golden dataset exact match`, `재현성`이 중요하다.
- vLLM 운영 기본값은 두 모델 모두 `non-thinking + temperature=0 + JSON response_format`으로 둔다.
- vLLM의 OpenAI Python SDK 호출에서는 `top_k`, `min_p`, `repetition_penalty`, `chat_template_kwargs`를 `extra_body`로 전달한다.
- Qwen thinking mode는 `chat_template_kwargs.enable_thinking=false`로 끄고, thinking fallback은 별도 실험 후보로만 사용한다.

최근 Novita 기반 난해 지시서 5건 비교 테스트에서도 운영 기본값은 `enable_thinking=false`가 가장 실용적인 후보로 확인됐다. 최종 안정화 기준에서 qwen3.5와 qwen3.6은 모두 `20/20 PASS`였고, qwen3.6 테스트는 qwen3.5 안정화 이후의 실행 로직과 프롬프트 개선 경험을 기반으로 구성됐다. qwen3.6 non-thinking은 평균 처리 시간이 짧았지만, qwen3.6 thinking은 token과 지연 비용이 크게 증가했다. 상세 비교는 [qwen35_qwen36_지시서_추출_비교_분석_보고서.md](/Users/bhkim/Documents/codex_prj_sam_asset/tests/variable_data/doc/qwen35_qwen36_지시서_추출_비교_분석_보고서.md)를 기준으로 본다.

## 1. 파라미터별 쉬운 설명

아래 설명은 일반 문서에서 원문 필드를 정확히 보존해야 하는 정형 데이터 추출 작업을 기준으로 한다.

| 파라미터 | 쉬운 의미 | 값을 높이거나 켰을 때 | 값을 낮추거나 껐을 때 | 문서 추출 권장 |
| --- | --- | --- | --- | --- |
| `enable_thinking` | Qwen이 답변 전에 추론 과정을 만들지 여부 | 복잡한 추론에는 도움이 될 수 있지만 `<think>` 블록, `reasoning` 필드, 추가 markup이 섞일 수 있다. | 바로 최종 답변을 생성해 JSON 구조가 단순해진다. | 기본 `false` |
| `temperature` | 답변의 무작위성 정도 | 표현과 후보 선택이 다양해지지만 같은 입력에서도 결과가 흔들릴 수 있다. | 가장 가능성 높은 답을 고정적으로 선택해 재현성이 좋아진다. | 기본 `0` |
| `top_p` | 누적 확률 상위 후보만 남기는 sampling 범위 | `1.0`에 가까울수록 후보를 넓게 보므로 원문 특수 표현을 보존하기 쉽다. | 너무 낮으면 후보 풀이 좁아져 희귀한 고유명사, 특수문자, 긴 식별자가 누락되거나 바뀔 수 있다. | `1.0` |
| `top_k` | 다음 token 후보를 확률 상위 k개로 제한 | 크게 잡으면 후보 풀이 넓어진다. | 작게 잡으면 선택지가 강하게 제한되어 원문 특수 표현이 누락될 수 있다. | `20` |
| `min_p` | 최고 확률 token 대비 너무 낮은 후보를 제거하는 기준 | 높이면 낮은 확률 후보가 더 많이 제거된다. | `0.0`이면 추가 제거를 거의 하지 않는다. | `0.0` |
| `presence_penalty` | 이미 나온 표현을 다시 쓰지 않게 하는 압력 | 반복을 줄이지만 원문에 반복 등장하는 필드명, 상품명, 기관명, 코드 보존을 방해할 수 있다. | 원문 반복을 더 잘 보존한다. | `0.0` |
| `repetition_penalty` | 같은 token 반복을 억제하는 압력 | 반복 출력은 줄지만 이름, 번호, 코드, 숫자 패턴이 변형될 수 있다. | `1.0`이면 반복 억제를 추가로 걸지 않는다. | `1.0` |
| `max_tokens` | 모델이 생성할 수 있는 최대 출력 길이 | 크게 잡으면 긴 JSON이나 많은 추출 row/레코드를 끝까지 쓸 여지가 생긴다. | 너무 작으면 JSON이 중간에 끊기거나 일부 레코드가 누락될 수 있다. | 기본 `16384`, fallback `32768` |
| `stream` | 응답을 조각으로 받을지 여부 | 사용자 화면에는 빨리 보이지만 JSON 완성 전 처리와 오류 복구가 복잡해진다. | 완성된 응답을 한 번에 받아 JSON parse가 단순해진다. | `false` |
| `response_format` | JSON 형태 출력을 강제하는 옵션 | `json_object`를 쓰면 모델이 JSON으로 답하도록 강하게 유도된다. | 생략하면 설명문, markdown, reasoning text가 섞일 가능성이 커진다. | `{"type": "json_object"}` |

핵심은 sampling 관련 파라미터를 "더 창의적으로" 쓰는 것이 아니라 "덜 흔들리게" 잠그는 것이다. 문서 데이터 추출에서는 모델이 새로운 표현을 잘 만드는 능력보다 원문에 있는 금액, 날짜, 코드, 이름, 주소, 계좌번호, 문서번호 같은 값을 그대로 옮기는 능력이 더 중요하다.

vLLM에서 필드 위치는 아래처럼 구분한다.

- OpenAI-compatible 표준 인자: `temperature`, `top_p`, `max_tokens`, `stream`, `response_format`, `presence_penalty`
- vLLM 확장 인자: `top_k`, `min_p`, `repetition_penalty`, `chat_template_kwargs`
- OpenAI Python SDK 사용 시 vLLM 확장 인자는 `extra_body` 안에 넣는다.
- Raw HTTP 사용 시 vLLM 확장 인자는 request body top-level에 직접 넣는다.
- Qwen thinking 제어는 `chat_template_kwargs: {"enable_thinking": false}`로 한다.

## 2. vLLM 운영 권장값

### 2.1 기본 추출 프로필

두 모델 모두 vLLM 기반 일반 문서 데이터 추출 기본값은 아래를 권장한다.

| 항목 | 권장값 | vLLM 전달 위치 |
| --- | --- | --- |
| `enable_thinking` | `false` | `extra_body.chat_template_kwargs` 또는 raw body `chat_template_kwargs` |
| `temperature` | `0` | 표준 인자 |
| `top_p` | `1.0` | 표준 인자 |
| `top_k` | `20` | `extra_body` 또는 raw body top-level |
| `min_p` | `0.0` | `extra_body` 또는 raw body top-level |
| `presence_penalty` | `0.0` | 표준 인자 |
| `repetition_penalty` | `1.0` | `extra_body` 또는 raw body top-level |
| `max_tokens` | `16384` | 표준 인자 |
| `stream` | `false` | 표준 인자 |
| `response_format` | `{"type": "json_object"}` | 표준 인자 |

### 2.2 OpenAI Python SDK 호출 예시

vLLM OpenAI-compatible endpoint를 OpenAI Python SDK로 호출할 때는 표준 인자는 top-level에 두고, vLLM 확장 인자는 `extra_body` 안에 넣는다.

```python
from openai import OpenAI

client = OpenAI(
    api_key="EMPTY",
    base_url="http://localhost:8000/v1",
)

response = client.chat.completions.create(
    model="Qwen/Qwen3.6-27B",
    messages=[
        {
            "role": "system",
            "content": "Extract structured data from the document. Return only a valid JSON object.",
        },
        {
            "role": "user",
            "content": "Document text or markdown goes here.",
        },
    ],
    temperature=0,
    top_p=1.0,
    presence_penalty=0.0,
    max_tokens=16384,
    stream=False,
    response_format={"type": "json_object"},
    extra_body={
        "top_k": 20,
        "min_p": 0.0,
        "repetition_penalty": 1.0,
        "chat_template_kwargs": {"enable_thinking": False},
    },
)

data = response.choices[0].message.content
```

### 2.3 Raw HTTP 호출 예시

Raw HTTP 요청에서는 `extra_body` wrapper를 쓰지 않는다. vLLM 확장 인자를 request body top-level에 직접 넣는다.

```json
{
  "model": "Qwen/Qwen3.6-27B",
  "messages": [
    {
      "role": "system",
      "content": "Extract structured data from the document. Return only a valid JSON object."
    },
    {
      "role": "user",
      "content": "Document text or markdown goes here."
    }
  ],
  "temperature": 0,
  "top_p": 1.0,
  "top_k": 20,
  "min_p": 0.0,
  "presence_penalty": 0.0,
  "repetition_penalty": 1.0,
  "max_tokens": 16384,
  "stream": false,
  "response_format": {"type": "json_object"},
  "chat_template_kwargs": {
    "enable_thinking": false
  }
}
```

### 2.4 모델별 기본값

| 모델 | 용도 | 권장값 |
| --- | --- | --- |
| `Qwen/Qwen3.5-397B-A17B` | 문서 데이터 추출 기본 | `enable_thinking=false`, `temperature=0`, `top_p=1.0`, `top_k=20`, `min_p=0.0`, `presence_penalty=0.0`, `repetition_penalty=1.0`, `max_tokens=16384`, `response_format=json_object` |
| `Qwen/Qwen3.6-27B` | 문서 데이터 추출 기본 | `enable_thinking=false`, `temperature=0`, `top_p=1.0`, `top_k=20`, `min_p=0.0`, `presence_penalty=0.0`, `repetition_penalty=1.0`, `max_tokens=16384`, `response_format=json_object` |

### 2.5 운영 기본값을 공식 non-thinking 권장값과 다르게 두는 이유

Hugging Face 모델 카드의 non-thinking 권장값은 일반 instruct 응답 품질을 위한 값이다. 반면 문서 데이터 추출은 정해진 원문에서 필드를 보존해 JSON으로 직렬화하는 작업이다.

따라서 운영 추출 기본값에서는 아래 위험을 줄이기 위해 sampling을 닫는다.

- `temperature=0.7`: 동일 입력에 대한 출력 변동 가능성 증가
- `presence_penalty=1.5`: 원문 반복 회피가 필드명, 상품명, 기관명, 식별자 보존을 흔들 가능성
- `top_p=0.8`: 후보 분포 절단에 따른 rare token 또는 특수 표기 누락 가능성

## 3. vLLM 서버 기동 기준

vLLM은 기본적으로 모델 저장소의 `generation_config.json`이 있으면 해당 값을 적용할 수 있다. 문서 데이터 추출 운영에서는 요청마다 sampling 값을 명시해 기본값 변화의 영향을 줄인다.

필요하면 vLLM 서버를 시작할 때 vLLM 기본 generation config를 명시한다.

```bash
vllm serve <MODEL_ID> \
  --generation-config vllm
```

Qwen thinking mode를 서버 기본값으로 끄려면 `--default-chat-template-kwargs`를 사용한다.

```bash
vllm serve <MODEL_ID> \
  --generation-config vllm \
  --default-chat-template-kwargs '{"enable_thinking": false}'
```

운영에서는 서버 기본값과 별도로 요청마다 아래 request-level 값을 넣는 방식을 권장한다. vLLM에서는 request-level `chat_template_kwargs`가 server default보다 우선한다.

```json
{
  "chat_template_kwargs": {
    "enable_thinking": false
  }
}
```

## 4. Thinking Mode 기준

문서 데이터 추출에서는 thinking mode를 기본으로 끈다. vLLM에서 Qwen thinking을 끄는 기본 방법은 `chat_template_kwargs.enable_thinking=false`다.

이유:

- thinking content는 `<think>...</think>` 형태 중간 출력이나 vLLM의 `reasoning`/`reasoning_content` 계열 필드로 분리될 수 있어 strict JSON 추출과 충돌할 수 있다.
- reasoning 출력이 응답 본문에 섞이면 JSON parse, schema validation, downstream 저장 단계가 실패할 수 있다.
- 일부 모델에서는 thinking mode와 tool call 또는 structured output이 함께 사용될 때 빈 call, 추가 markup, 비표준 필드가 생길 수 있다.
- 문서 데이터 추출은 다양한 후보를 창의적으로 생성하는 문제가 아니라 원문 표, 문단, 레이아웃, 메타데이터에서 정해진 필드를 안정적으로 직렬화하는 문제다.
- 운영 재현성과 audit 가능성이 중요하므로 같은 입력에서 같은 출력이 나오는 설정이 우선이다.

thinking mode는 아래 경우에만 별도 실험으로 사용한다.

| 상황 | 권장 |
| --- | --- |
| 일반 운영 문서 데이터 추출 | 사용하지 않음 |
| JSON strict mode 또는 schema-constrained output | 사용하지 않음 |
| tool/function call 검증 | 사용하지 않음 |
| 민감정보 마스킹 / 고객정보 처리 | 사용하지 않음 |
| HTML, Excel, 텍스트 PDF처럼 parser text가 안정적인 문서 | 사용하지 않음 |
| 표 구조가 깨진 PDF, 스캔 이미지, 문맥 해석이 어려운 문서 | fallback 후보로만 실험 |
| thinking fallback 결과 채택 | JSON parse, schema validation, coverage, golden dataset 또는 검수 기준 exact match 통과 시에만 가능 |

thinking fallback 후보값은 두 모델 모두 아래를 사용한다.

```json
{
  "enable_thinking": true,
  "temperature": 0.6,
  "top_p": 0.95,
  "top_k": 20,
  "min_p": 0.0,
  "presence_penalty": 0.0,
  "repetition_penalty": 1.0,
  "max_tokens": 32768
}
```

OpenAI Python SDK로 thinking fallback을 실험할 때는 `enable_thinking=true`를 `extra_body.chat_template_kwargs`에 넣는다.

```python
response = client.chat.completions.create(
    model="Qwen/Qwen3.6-27B",
    messages=messages,
    temperature=0.6,
    top_p=0.95,
    presence_penalty=0.0,
    max_tokens=32768,
    response_format={"type": "json_object"},
    extra_body={
        "top_k": 20,
        "min_p": 0.0,
        "repetition_penalty": 1.0,
        "chat_template_kwargs": {"enable_thinking": True},
    },
)
```

`Qwen3.6-27B`의 Hugging Face 모델 카드는 thinking general task에 `temperature=1.0`도 권장하지만, 문서 데이터 추출은 precise extraction에 가깝기 때문에 fallback 후보도 `0.6`을 우선한다.

## 5. Hugging Face 공식 권장값

아래 값은 Hugging Face 모델 카드 기준 공식 sampling parameter다. 운영 추출 기본값이 아니라 reference로 본다.

### 5.1 Qwen/Qwen3.5-397B-A17B

| 모드 | temperature | top_p | top_k | min_p | presence_penalty | repetition_penalty |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Thinking | `0.6` | `0.95` | `20` | `0.0` | `0.0` | `1.0` |
| Instruct / non-thinking | `0.7` | `0.8` | `20` | `0.0` | `1.5` | `1.0` |

### 5.2 Qwen/Qwen3.6-27B

| 모드 | temperature | top_p | top_k | min_p | presence_penalty | repetition_penalty |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Thinking general | `1.0` | `0.95` | `20` | `0.0` | `0.0` | `1.0` |
| Thinking precise | `0.6` | `0.95` | `20` | `0.0` | `0.0` | `1.0` |
| Instruct / non-thinking | `0.7` | `0.8` | `20` | `0.0` | `1.5` | `1.0` |

HF card의 non-thinking 권장값은 일반 instruct 응답 품질 기준이다. 문서 데이터 추출에서는 `temperature=0.7`과 `presence_penalty=1.5`가 JSON exactness와 field 보존을 흔들 수 있으므로 운영 기본값으로 쓰지 않는다.

## 6. A/B 검증 후보

새 vLLM endpoint나 새 모델 조건을 붙일 때는 후보를 많이 늘리지 말고 아래 3개만 비교한다.

| Candidate | 목적 | 파라미터 |
| --- | --- | --- |
| A | 운영 기본값 | `enable_thinking=false`, `temperature=0`, `top_p=1.0`, `top_k=20`, `min_p=0.0`, `presence_penalty=0.0`, `repetition_penalty=1.0` |
| B | 약한 sampling 허용 | `enable_thinking=false`, `temperature=0.2`, `top_p=0.95`, `top_k=20`, `min_p=0.0`, `presence_penalty=0.0`, `repetition_penalty=1.0` |
| C | thinking fallback | `enable_thinking=true`, `temperature=0.6`, `top_p=0.95`, `top_k=20`, `min_p=0.0`, `presence_penalty=0.0`, `repetition_penalty=1.0` |

채택 기준:

- golden dataset 또는 검수 기준 JSON exact match
- CSV, table, database row 등 downstream canonical compare 통과
- 원본 대조 기준 금액, 날짜, 방향, 식별자, 상품명, 기관명, 고유명사 오류 없음
- JSON parse 실패 0건
- schema validation 실패 0건
- coverage mismatch가 있으면 원본 대조로 설명 가능해야 함
- retry count와 elapsed가 운영 허용 범위여야 함

## 7. vLLM 적용 체크리스트

운영 적용 전 아래 항목을 확인한다.

- vLLM server가 OpenAI-compatible `/v1/chat/completions`를 제공하는지 확인한다.
- 모델 chat template이 정상 적용되는지 확인한다.
- `extra_body.top_k`, `extra_body.min_p`, `extra_body.repetition_penalty`가 요청에서 수락되는지 확인한다.
- `extra_body.chat_template_kwargs.enable_thinking=false`가 실제로 reasoning 출력 또는 `<think>` 블록을 제거하는지 확인한다.
- `response_format={"type": "json_object"}` 또는 동등한 JSON 강제 옵션이 동작하는지 확인한다.
- `temperature=0`에서 동일 입력 반복 호출 결과가 안정적인지 확인한다.
- `max_tokens=16384`가 모델 context, vLLM 설정, GPU memory, 과금 정책에 맞는지 확인한다.
- `--generation-config vllm` 사용 여부를 운영 정책으로 확정한다.
- `--default-chat-template-kwargs '{"enable_thinking": false}'` 사용 여부를 운영 정책으로 확정한다.
- streaming 응답을 쓰지 않는 경로에서 complete JSON object가 한 번에 수신되는지 확인한다.
- timeout, 429, 5xx에 대한 retry 정책을 별도로 정의한다.

## 8. 출처

- [Qwen/Qwen3.5-397B-A17B Hugging Face](https://huggingface.co/Qwen/Qwen3.5-397B-A17B)
- [Qwen/Qwen3.6-27B Hugging Face](https://huggingface.co/Qwen/Qwen3.6-27B)
- [vLLM OpenAI-Compatible Server](https://docs.vllm.ai/en/latest/serving/openai_compatible_server/)
- [vLLM Reasoning Outputs](https://docs.vllm.ai/en/v0.20.1/features/reasoning_outputs/)
- [Qwen vLLM Deployment Guide](https://qwen.readthedocs.io/en/stable/deployment/vllm.html)
