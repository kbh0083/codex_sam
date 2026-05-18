# qwen3.5-397B-A17B vs qwen3.6-27B 지시서 추출 비교 분석 보고서

- 작성일: `2026-05-14`
- 비교 목적: 동일한 5개 거래처 지시서에서 qwen3.5-397B-A17B와 qwen3.6-27B의 정형 데이터 추출 정확도, 처리 시간, token usage, 안정화 요인을 비교한다.
- 결론 요약: 최종 안정화 기준에서는 두 모델 모두 `20/20 PASS`를 달성했다. 다만 qwen3.6 테스트는 qwen3.5 테스트에서 안정화된 실행 로직과 프롬프트 개선 경험을 기반으로 구성됐으므로, 두 모델의 결과는 순수 cold-start 비교가 아니라 안정화 이력이 반영된 운영 관점 비교로 해석해야 한다. 운영 기본값은 `enable_thinking=false`가 적절하며, qwen3.6은 non-thinking에서 더 빠르지만 thinking에서는 시간과 token 비용이 크게 증가한다.

## 1. 비교 기준

| 구분 | 실험 식별자 | 용도 | 결과 요약 |
| --- | --- | --- | --- |
| qwen3.5 최종 | `novita_qwen35_variable_data_145515` | 최종 성능 비교 기준 | `20/20 PASS` |
| qwen3.6 최종 | `novita_qwen36_variable_data_165218` | 최종 성능 비교 기준 | `20/20 PASS` |
| qwen3.5 초기 안정성 참고 | `novita_qwen35_variable_data_121230` | 안정화 전 실패 패턴 분석 | `10 PASS / 10 FAIL` |
| qwen3.6 초기 안정성 참고 | `novita_qwen36_variable_data_152808` | 안정화 전 실패 패턴 분석 | `11 PASS / 9 FAIL` |

비교 대상 문서는 KDB생명, 라이나생명, 한화생명, 동양생명, 카디프생명 5건이다. 각 모델은 `thinking`, `non_thinking` profile을 각각 2cycle 실행해 총 20개 결과를 생성했다.

## 2. 실험 구성과 프롬프트 기준

| 항목 | qwen3.5 | qwen3.6 |
| --- | --- | --- |
| 테스트 실행 환경 | qwen3.5 전용 실행 구성 | qwen3.6 전용 실행 구성 |
| 입력 프롬프트 | qwen3.5 전용 프롬프트 세트 | qwen3.6 전용 프롬프트 세트 |
| 안정화 이력 | 최초 안정화 기준 | qwen3.5 안정화 결과를 기반으로 별도 구성 |
| 기본 모델 | `qwen/qwen3.5-397b-a17b` | `qwen/qwen3.6-27b` |
| non-thinking profile | `enable_thinking=false`, `temperature=0`, `top_p=1.0`, `max_tokens=16384` | 동일 |
| thinking profile | `enable_thinking=true`, `temperature=0.6`, `top_p=0.95`, `max_tokens=32768` | 동일 |
| JSON parse retry | 없음 | `PARSE_RETRIES=3` |
| 출력 형식 안정화 | 기본 JSON 객체 검증 | JSON 객체 wrapper guard와 parse retry 적용 |

`JSON parse retry`는 모델 응답이 유효한 JSON 객체로 파싱되지 않을 때 같은 조건으로 다시 호출하는 실행 안정화 정책이다. 예를 들어 최상위가 객체가 아닌 배열로 나오거나, 설명문이 섞이거나, reasoning만 출력되고 실제 JSON 본문이 비어 있으면 parser가 실패할 수 있다. qwen3.6 실험에서는 이런 형식 실패에 한해 최대 3회 재시도하도록 설정했다. 이 재시도는 누락 주문을 추가하거나 펀드명, 금액, 날짜를 수정하는 deterministic 후처리가 아니며, 추출 결과의 업무적 오류는 그대로 FAIL로 판정한다.

qwen3.6 테스트는 qwen3.5 테스트가 먼저 수행되고 실패 보정과 검증 기준이 안정화된 뒤 진행됐다. 따라서 qwen3.6 전용 실행 구성과 프롬프트는 qwen3.5에서 확인된 입력 렌더링, 출력 schema, 거래처별 규칙, JSON 형식 안정화 경험을 출발점으로 삼았다. 이 점 때문에 qwen3.6 최종 결과는 모델 자체 성능뿐 아니라 qwen3.5 안정화 과정에서 축적된 운영 지식의 이식 효과도 함께 포함한다.

qwen3.6 프롬프트는 qwen3.5 대비 출력 안정성 guard가 추가됐다. 라이나 프롬프트는 동일하고, 나머지는 아래 차이가 있다.

| Case | qwen3.6 추가/강화 내용 | 의미 |
| --- | --- | --- |
| KDB생명 | `D+1(예상금액)` 독립 주문 원천, `492007` D+1 누락 방지, top-level JSON object guard | non-thinking에서 특정 D+1 주문 누락을 방지 |
| 동양생명 | `BALX00` OCR 민감 펀드명, `issues=[]` 규칙, top-level object guard, 빈 content/배열 금지 | OCR exact mismatch와 stage JSON parse 실패를 방지 |
| 한화생명 | 빈 content 금지, top-level JSON object guard, `orders` wrapper 생략 금지 | thinking 응답이 reasoning만 출력하거나 배열을 반환하는 문제 방지 |
| 카디프생명 | top-level JSON object guard, 전체 wrapper 생략 금지 | thinking에서 주문 배열만 반환하는 문제 방지 |

## 3. 최종 정확도 비교

| Model | Overall | PASS | FAIL | BLOCKED | 대상 결과 수 |
| --- | --- | ---: | ---: | ---: | ---: |
| qwen3.5-397B-A17B | `PASS` | `20` | `0` | `0` | `20` |
| qwen3.6-27B | `PASS` | `20` | `0` | `0` | `20` |

최종 안정화 기준에서는 두 모델 모두 모든 검증 기준을 통과했다. 즉 `status=COMPLETED`, `base_date`, 주문 수, `issues=[]`, JSON core exact, CSV canonical exact가 모두 일치한다.

## 4. Profile별 성능 비교

| Model | Profile | Total | PASS | FAIL | BLOCKED | Avg elapsed(s) | Avg total tokens |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| qwen3.5 | `non_thinking` | `10` | `10` | `0` | `0` | `31.311` | `6,790.1` |
| qwen3.5 | `thinking` | `10` | `10` | `0` | `0` | `31.759` | `6,374.1` |
| qwen3.6 | `non_thinking` | `10` | `10` | `0` | `0` | `23.946` | `6,536.4` |
| qwen3.6 | `thinking` | `10` | `10` | `0` | `0` | `62.556` | `10,876.0` |

요약하면 qwen3.6 non-thinking은 qwen3.5 non-thinking 대비 평균 처리시간이 약 `23.5%` 짧고 token도 약 `3.7%` 적다. 반면 qwen3.6 thinking은 qwen3.5 thinking 대비 평균 처리시간이 약 `97.0%` 길고 token은 약 `70.6%` 많다.

## 5. Case/Profile별 상세 비교

Delta는 qwen3.6을 qwen3.5와 비교한 값이다. 음수는 qwen3.6이 더 작거나 빠르다는 의미다.

| Profile | Case | qwen3.5 elapsed | qwen3.6 elapsed | Elapsed delta | qwen3.5 tokens | qwen3.6 tokens | Token delta |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `non_thinking` | 동양생명 | `46.087` | `45.696` | `-0.8%` | `9,756.0` | `9,583.0` | `-1.8%` |
| `non_thinking` | 라이나생명 | `13.934` | `11.540` | `-17.2%` | `5,708.5` | `4,124.0` | `-27.8%` |
| `non_thinking` | 한화생명 | `9.527` | `7.175` | `-24.7%` | `4,053.0` | `4,179.0` | `+3.1%` |
| `non_thinking` | KDB생명 | `23.149` | `13.593` | `-41.3%` | `4,465.0` | `4,736.0` | `+6.1%` |
| `non_thinking` | 카디프생명 | `63.858` | `41.724` | `-34.7%` | `9,968.0` | `10,060.0` | `+0.9%` |
| `thinking` | 동양생명 | `40.968` | `118.129` | `+188.3%` | `9,261.5` | `18,859.0` | `+103.6%` |
| `thinking` | 라이나생명 | `14.305` | `30.718` | `+114.7%` | `4,132.0` | `6,034.5` | `+46.0%` |
| `thinking` | 한화생명 | `9.422` | `40.722` | `+332.2%` | `4,050.0` | `7,142.0` | `+76.3%` |
| `thinking` | KDB생명 | `21.980` | `46.480` | `+111.5%` | `4,462.0` | `8,559.5` | `+91.8%` |
| `thinking` | 카디프생명 | `72.122` | `76.730` | `+6.4%` | `9,965.0` | `13,785.0` | `+38.3%` |

non-thinking에서는 qwen3.6이 모든 case에서 시간상 우세했다. 특히 KDB생명과 카디프생명은 각각 `41.3%`, `34.7%` 빠르다. thinking에서는 qwen3.6이 모든 case에서 token을 더 많이 사용했고, 한화생명과 동양생명에서 처리시간 증가가 가장 컸다.

## 6. 초기 안정성 대비 최종 안정화

| Model | 실험 | PASS | FAIL | BLOCKED | 비고 |
| --- | --- | ---: | ---: | ---: | --- |
| qwen3.5 | 초기 `121230` | `10` | `10` | `0` | 라이나/한화는 thinking과 non-thinking 모두 실패, 동양은 non-thinking 실패 |
| qwen3.5 | 최종 `145515` | `20` | `0` | `0` | 프롬프트 조정 후 전체 PASS |
| qwen3.6 | 초기 `152808` | `11` | `9` | `0` | KDB non-thinking, 동양, 카디프 thinking, 한화 thinking 실패 |
| qwen3.6 | 최종 `165218` | `20` | `0` | `0` | qwen3.6 전용 프롬프트 guard와 parse retry 적용 후 전체 PASS |

초기 결과만 보면 두 모델 모두 난해 지시서에서 실패가 있었다. 다만 qwen3.6은 qwen3.5 테스트 후 안정화된 실행 로직을 기반으로 별도 구성됐으므로, qwen3.6의 초기 실패는 완전한 최초 시도라기보다 모델 교체 후 남은 출력 형식, OCR, 누락 주문 리스크를 확인한 성격이 강하다. 최종 결과의 차이는 모델 자체 능력만이 아니라 프롬프트의 업무 규칙 명시, strict JSON wrapper, OCR 민감 용어 고정, API 응답 재시도 정책까지 포함한 테스트 실행 환경의 안정화 효과를 반영한다.

## 7. OCR 정확도 분석

이 테스트에는 별도의 문자 단위 OCR ground truth 로그가 없다. 따라서 OCR 정확도는 최종 JSON/CSV exact 검증과 초기 실패 diff에서 드러난 `fund_name`, `fund_code`, 금액, 날짜 header의 exact mismatch를 기준으로 분석했다. 이 기준에서 숫자 금액 OCR은 최종 안정화 후 두 모델 모두 통과했고, 실제 OCR 민감도는 주로 동양생명의 긴 한글 펀드명과 특수문자에서 드러났다.

초기 실패 근거는 다음과 같다.

- `qwen3.5`: `121230` 초기 실험에서 동양생명 non-thinking 2회가 모두 OCR 표기 차이로 FAIL 처리됐다.
- `qwen3.6`: `152808` 초기 실험에서 동양생명 `BALX00` 펀드명이 `(디프트옵션전용)`으로 인식되어 FAIL 처리됐다.
- `qwen3.6 thinking`은 OCR보다 JSON wrapper/empty content 안정성이 더 큰 리스크였다. 카디프와 한화의 초기 thinking 실패는 top-level 배열, wrapper 누락, content empty 성격이 강했다.

| OCR 관찰 항목 | qwen3.5 초기 | qwen3.6 초기 | 최종 안정화 결과 |
| --- | --- | --- | --- |
| 동양생명 `유니버셜/유니버설` | non-thinking에서 `변액유니버셜종신-채권형`을 `변액유니버설종신-채권형`으로 출력한 mismatch 발생 | 해당 오류는 두드러지지 않음 | 두 모델 모두 `변액유니버셜종신-채권형` exact 통과 |
| 동양생명 `종신/증권` | non-thinking cycle 2에서 `변액유니버셜종신-채권형`을 `변액유니버셜증권-채권형`으로 출력 | 해당 오류는 두드러지지 않음 | qwen3.5 최종 프롬프트 보강 후 통과 |
| 동양생명 로마숫자 `Ⅱ/II` | non-thinking에서 `Ⅱ`를 `II` 또는 공백 포함 표기로 출력해 exact mismatch에 기여 | 해당 오류는 두드러지지 않음 | 두 모델 모두 OCR 민감 표기 규칙으로 통과 |
| 동양생명 `BALX00` 펀드명 | `디폴트옵션전용`은 비교적 안정적이었으나 공백 표기 차이가 있음 | `디폴트옵션전용`을 `디프트옵션전용`으로 읽어 2개 주문 mismatch 발생 | qwen3.6 프롬프트에 `BALX00: (디폴트옵션전용)글로벌에셋밸런스형` 고정 후 통과 |
| KDB생명 D+1 금액 cell | 최종 기준 통과 | 초기 non-thinking에서 `492007` D+1 입금 주문을 누락 | qwen3.6 프롬프트에 D+1 독립 주문 원천과 `492007` 재검수 규칙 추가 후 통과 |
| 카디프생명 영문 펀드명/금액 | 최종 기준 통과 | 초기 thinking 실패는 OCR 오독보다 top-level array 출력 문제 | JSON wrapper guard 후 통과 |

OCR 관점에서 qwen3.5와 qwen3.6의 성향은 다르다. qwen3.5는 동양생명 non-thinking에서 한글 음절 단위의 혼동(`유니버셜/유니버설`, `종신/증권`)과 로마숫자 표기 흔들림이 관찰됐다. qwen3.6은 한글 일반 펀드명은 상대적으로 안정적이었지만 `BALX00`의 `디폴트`를 `디프트`로 오인하는 특정 고유명사 오류가 반복됐다.

최종 결과에서는 두 모델 모두 OCR-sensitive field exact match를 달성했다. 다만 이는 모델의 순수 OCR 성능만으로 해석하면 안 된다. 동양생명은 프롬프트에 OCR 민감 펀드명을 명시했고, qwen3.6은 `BALX00` mapping과 top-level JSON object guard를 추가했다. 즉 OCR 정확도는 모델 능력, 이미지 렌더링 품질, 프롬프트의 OCR 민감 어휘 사전, 검증 결과 기반 보정이 결합된 결과다.

운영 관점의 OCR 권고는 다음과 같다.

| 항목 | 권고 |
| --- | --- |
| 고유명사/펀드명 | 거래처별 OCR 민감 사전을 프롬프트에 명시한다. 특히 `유니버셜`, `종신`, `Ⅱ`, `디폴트`처럼 유사 글자나 특수문자가 있는 항목은 코드와 함께 고정한다. |
| 금액 cell | 금액 OCR 자체보다 누락 방지가 중요하다. KDB D+1처럼 같은 row의 복수 금액 컬럼은 독립 주문 원천으로 명시한다. |
| OCR 불확실성 표시 | `issues`에는 실제로 읽을 수 없는 cell만 넣게 한다. 필드명을 issue로 넣으면 추출 결과가 맞아도 검증에서 실패한다. |
| 비교 방식 | 업무상 exact match가 필요하면 공백 정규화만으로 해결할 수 없는 음절 오류를 별도 사전으로 관리한다. |

## 8. 주요 인사이트

1. 운영 기본값은 `enable_thinking=false`가 적절하다.

최종 기준에서 두 모델 모두 non-thinking으로 100% 통과했다. qwen3.6 non-thinking은 qwen3.5보다 평균 처리시간과 평균 token이 모두 낮아, 동일 Novita endpoint 조건에서는 가장 실용적인 운영 후보이다.

2. qwen3.6 thinking은 비용과 지연이 크다.

qwen3.6 thinking은 정확도는 확보했지만 평균 token이 `10,876.0`으로 qwen3.5 thinking의 `6,374.1`보다 높다. 특히 동양생명처럼 2-stage visual OCR을 쓰는 케이스에서는 qwen3.6 thinking의 reasoning token 증가가 전체 비용과 지연을 크게 키운다.

3. qwen3.6은 출력 형식 guard가 더 중요하다.

초기 qwen3.6 실패에는 top-level 배열 출력, 빈 content, reasoning-only 응답, strict JSON object 오염이 포함됐다. 최종 qwen3.6 프롬프트는 JSON wrapper, 빈 content 금지, 배열 금지, `issues=[]` 규칙을 강하게 넣어 안정화했다.

4. 난해 지시서에서는 모델보다 입력/규칙 설계 영향이 크다.

KDB생명의 D+1 주문, 동양생명의 BALX00 OCR, 한화생명의 컬럼 선택, 카디프의 다중 페이지 추출은 모두 모델 크기보다 문서별 규칙과 검증 가능한 출력 schema가 성패를 좌우했다.

5. 최종 PASS는 순수 모델 비교가 아니라 안정화 이력이 반영된 추출 시스템 비교다.

qwen3.5와 qwen3.6은 최종 프롬프트와 테스트 실행 환경이 다르다. 특히 qwen3.6 테스트는 qwen3.5에서 안정화된 실행 로직과 거래처별 실패 보정 경험을 기반으로 구성됐고, 여기에 `PARSE_RETRIES=3`와 더 강한 JSON guard가 추가됐다. 따라서 최종 `20/20 PASS`는 모델 단독 성능이라기보다 모델별 운영 보정과 선행 안정화 지식까지 포함한 시스템 성능으로 해석해야 한다.

## 9. 운영 권고

| 목적 | 권고 |
| --- | --- |
| 기본 운영 추출 | qwen3.6 non-thinking 우선 검토. 최종 기준 평균 `23.946s`, `6,536.4 tokens`로 가장 효율적이다. |
| 보수적 운영 또는 retry 최소화 | qwen3.5 non-thinking도 유효하다. 평균 시간은 더 길지만 최종 20/20 PASS를 달성했다. |
| thinking mode 상시 운영 | 권장하지 않는다. 특히 qwen3.6 thinking은 시간/token 비용이 크다. |
| 복잡 문서 fallback | thinking보다 문서별 프롬프트 guard, 입력 범위 제한, 2-stage 분할, JSON parse retry를 먼저 적용한다. |
| 신규 거래처 온보딩 | 초기 실험에서 바로 모델을 교체하기보다 실패 diff를 보고 case-specific rule과 strict schema guard를 추가한다. |

## 10. 결론

최종 안정화된 테스트 기준에서는 qwen3.5-397B-A17B와 qwen3.6-27B 모두 5개 난해 지시서 20개 결과를 전부 통과했다. 다만 운영 관점의 선택 기준은 다르다.

- qwen3.6 non-thinking은 평균 속도와 token 효율이 가장 좋다.
- qwen3.6 thinking은 정확도 확보는 가능하지만 비용과 지연이 커서 기본값으로 부적합하다.
- qwen3.5는 thinking/non-thinking 간 비용 차이가 작고 안정화 후 전체 PASS를 달성해 보수적 대안으로 유지할 가치가 있다.
- qwen3.6 결과는 qwen3.5 안정화 이후의 실행 로직과 프롬프트 개선 경험을 기반으로 얻은 결과이므로, 모델 교체 시에는 선행 모델에서 축적한 규칙과 검증 체계를 이식하는 것이 중요하다.
- 두 모델 모두 성공의 핵심은 모델명보다 문서별 OCR/표 규칙, strict JSON 출력 계약, 검증 결과 기반 프롬프트 개선이었다.
