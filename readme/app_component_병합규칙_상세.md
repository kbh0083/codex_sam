# `app/component.py` 병합 규칙 상세 문서

이 문서는 [app/component.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/component.py)에 구현된
현재 병합 규칙을 처음 읽는 개발자가 빠르게 이해할 수 있도록 정리한 문서다.

중요:
- 이 문서는 **현재 코드 기준**이다.
- 예전처럼 같은 버킷의 `SUB`와 `RED`를 순액 1건으로 합치는 규칙은 더 이상 쓰지 않는다.
- 지금 병합의 핵심은 **완전 중복 제거**와 **문서 메타데이터 보존**이다.
- 2026-04-20 WAS 병합에서도 `app/component.py` 병합 규칙 자체는 바뀌지 않았다.
- 최신 공통 상태와 테스트 보고 원칙은 [처음읽는_개발자용_흐름.md](/Users/bhkim/Documents/codex_prj_sam_asset/readme/처음읽는_개발자용_흐름.md)를 출처로 둔다.

---

## 1. 병합의 목표

현재 병합 목표는 두 가지다.

- 여러 문서를 각각 추출한 뒤, **완전히 같은 주문만 1건으로 줄인다.**
- 문서별 payload는 유지하면서, 최종 `orders`와 CSV를 만들 수 있게 한다.

즉 현재 병합은
- `SUB/RED` 순액 합산이 아니라
- **동일 주문 dedupe**
에 초점이 있다.

---

## 2. 병합이 적용되는 위치

외부 진입점은 [ExtractionComponent](/Users/bhkim/Documents/codex_prj_sam_asset/app/component.py)다.

흐름:

```text
DocumentExtractionRequest[]
  -> ExtractionComponent.extract_document_payloads()
    -> 문서별 payload 생성
  -> ExtractionComponent.extract_merged_payload()
    -> build_merged_payload()
      -> merge_document_payload_orders()
```

즉 병합은 **문서를 합쳐서 한 번에 추출하는 방식이 아니라**,
문서별 추출이 모두 끝난 뒤 마지막 단계에서만 적용된다.

이렇게 하는 이유:
- 어떤 문서가 문제인지 분리해서 보기 쉽다.
- 문서별 실패 원인을 그대로 유지할 수 있다.
- 최종 merged 결과와 문서별 상세 결과를 동시에 보관할 수 있다.

---

## 3. 병합 관련 핵심 함수

현재 병합 관련 핵심 함수는 아래 순서로 읽는 것이 가장 빠르다.

1. [flatten_document_payload_orders](/Users/bhkim/Documents/codex_prj_sam_asset/app/component.py#L613)
   - 단건 payload를 CSV/merge용 flat row로 바꾼다.
2. [_order_identity](/Users/bhkim/Documents/codex_prj_sam_asset/app/component.py#L640)
   - 완전히 같은 주문인지 판정하는 key를 만든다.
3. [_is_preferred_order_representation](/Users/bhkim/Documents/codex_prj_sam_asset/app/component.py#L657)
   - 동일 identity 안에서 대표 표현을 고르는 hook이다.
4. [merge_document_payload_orders](/Users/bhkim/Documents/codex_prj_sam_asset/app/component.py#L670)
   - 실제 dedupe와 merged row 생성을 수행한다.
5. [build_merged_payload](/Users/bhkim/Documents/codex_prj_sam_asset/app/component.py#L749)
   - 병합 결과를 최종 JSON 계약으로 감싼다.
6. [payload_to_csv_rows](/Users/bhkim/Documents/codex_prj_sam_asset/app/component.py#L786)
   - 단건/다건 payload를 같은 CSV 열 구조로 정규화한다.

---

## 4. 펀드 식별 규칙

### 4-1. 현재 identity의 펀드 필드

현재 병합 identity는 별도 펀드키 fallback helper를 쓰지 않는다.
최종 직렬화 payload의 아래 두 펀드 필드를 모두 identity에 포함한다.

1. `fund_code`
2. `fund_name`

즉 `fund_code`가 같아도 `fund_name` 표현이 다르면 현재 dedupe key는 다른 주문으로 본다.

이유:
- 현재 외부 payload comparator는 최종 직렬화 결과의 정확성을 우선한다.
- 펀드명 표현 차이를 임의로 합치면 원본 문서와 baseline exact compare가 흔들릴 수 있다.

---

## 5. “완전히 동일한 주문” 제거 규칙

### 5-1. `_order_identity()`

아래 7개 값이 모두 같으면 동일 주문으로 본다.

- `fund_code`
- `fund_name`
- `settle_class`
- `order_type`
- `base_date`
- `t_day`
- `transfer_amount`

즉 다음처럼 모든 핵심 필드가 같으면 중복 표기로 판단한다.

```text
6101 / 펀드A / 2 / 3 / 2025-08-26 / 01 / 20,262,383
6101 / 펀드A / 2 / 3 / 2025-08-26 / 01 / 20,262,383
```

현재 외부 payload는 코드값을 쓰므로 위 예시는 실제 payload 표현에 가깝다.

- `settle_class`
  - `"1"` = PENDING
  - `"2"` = CONFIRMED
- `order_type`
  - `"1"` = RED
  - `"3"` = SUB

### 5-2. 어떤 표현을 남기나

동일 주문이 여러 문서에서 중복될 때는
[_is_preferred_order_representation](/Users/bhkim/Documents/codex_prj_sam_asset/app/component.py#L657)
규칙으로 대표 표현을 고른다.

현재 정책 자체는 아래와 같지만, identity에 `fund_name`이 포함되므로 서로 다른 펀드명 표현을 하나로 합치는 용도로 해석하면 안 된다.

- `fund_name`이 더 긴 쪽
- 길이가 같으면 문자열 비교상 더 큰 쪽

실제 의미:
- 같은 identity 안에서만 동작하는 safety hook이다.
- `fund_name`만 다른 두 주문은 현재 identity가 다르므로 둘 다 남는다.

---

## 6. 현재 병합은 무엇을 하지 않나

이 부분이 가장 중요하다.

현재 병합은 아래 일을 **하지 않는다**.

- 같은 펀드/같은 버킷의 `SUB`와 `RED`를 순액으로 합치지 않는다.
- `RED`를 음수로 복원해서 signed amount 합산하지 않는다.
- 합산 결과 0이면 제거하는 로직도 없다.

즉 아래 두 주문은 현재 병합에서 **별도 주문 2건**으로 유지된다.

```text
6114 / 2 / 3 / 2025-08-26 / 01 / 1,106,000
6114 / 2 / 1 / 2025-08-26 / 01 / 52,671
```

이 두 줄은 `order_type`과 `transfer_amount`가 다르므로 `_order_identity()`가 달라진다.
따라서 병합 단계에서는 서로 상쇄하지 않고 그대로 남는다.

---

## 7. 실제 병합 로직 순서

### 7-1. `merge_document_payload_orders()`

이 함수가 병합의 중심이다.

실행 순서:

1. 각 문서 payload를 순회한다.
2. 각 주문에 대해 `identity = _order_identity(order)`를 만든다.
3. 동일 identity가 이미 있으면
   - `_is_preferred_order_representation()`로 더 좋은 표현만 남긴다.
4. 동일 identity가 없으면 새 주문으로 추가한다.
5. 최종 `merged_orders`와 `merged_rows`를 함께 만든다.

즉 현재 우선순위는 아래와 같다.

```text
완전 중복 제거
  -> 대표 표현 선택
    -> merged orders / merged rows 생성
```

예전 문서처럼

```text
같은 버킷 grouping
  -> SUB/RED 순액 합산
```

단계는 더 이상 없다.

---

## 8. 병합 JSON 구조

### 8-1. `build_merged_payload()`

병합 결과 JSON은 아래 구조를 가진다.

```json
{
  "file_count": 2,
  "file_names": ["a.xlsx", "b.pdf"],
  "source_paths": ["...", "..."],
  "model_name": "...",
  "issues": [],
  "documents": [
    { "문서별 payload": "..." },
    { "문서별 payload": "..." }
  ],
  "orders": [
    { "최종 병합 주문": "..." }
  ]
}
```

중요한 점:
- `documents`
  - 문서별 상세 결과를 유지한다.
- `orders`
  - dedupe가 끝난 최종 병합 주문 목록이다.
  - 이 목록은 이미 최종 출력 계약을 따라 `settle_class`, `order_type`이 문자열 코드(`"1"`, `"2"`, `"3"`) 상태다.

즉 소비자는
- 문서별 디버깅은 `documents`
- 후속 처리 입력은 `orders`
로 나눠 보면 된다.

---

## 9. CSV 규칙

### 9-1. 왜 JSON과 CSV가 다르게 보이나

JSON은 구조를 보존해야 하므로 `documents`와 `orders`를 분리한다.
반면 CSV는 사람이 빠르게 검수하기 쉬워야 하므로 문서 메타데이터와 주문 필드를 한 줄에 같이 놓는다.

### 9-2. `payload_to_csv_rows()`

이 함수는 단건/다건 차이를 흡수한다.

- 단건 payload
  - `flatten_document_payload_orders()` 사용
- 다건 merged payload
  - `merge_document_payload_orders()`에서 만든 row 사용

그래서 CSV는 항상 같은 열 계약을 유지한다.

열 목록은 [CSV_FIELDNAMES](/Users/bhkim/Documents/codex_prj_sam_asset/app/component.py#L32)에 정의돼 있다.

---

## 10. 병합 규칙 예시

### 10-1. 완전 중복 제거

입력:

```text
문서 A: 6101 / 펀드A / 2 / 3 / 2025-08-26 / 01 / 20,262,383
문서 B: 6101 / 펀드A / 2 / 3 / 2025-08-26 / 01 / 20,262,383
```

출력:

```text
1건만 유지
```

### 10-2. 같은 펀드/같은 버킷이지만 방향이 다르면 그대로 유지

입력:

```text
문서 A: 6114 / 펀드B / 2 / 3 / 2025-08-26 / 01 / 1,106,000
문서 B: 6114 / 펀드B / 2 / 1 / 2025-08-26 / 01 / 52,671
```

출력:

```text
두 건 모두 유지
```

이유:
- `order_type`이 다르고
- `transfer_amount`도 다르기 때문에
- 현재 dedupe key가 달라진다.

### 10-3. 펀드명 표현만 다르면 둘 다 유지

입력:

```text
문서 A: 9999 / 펀드A / 2 / 3 / 2025-08-26 / 01 / 100,000
문서 B: 9999 / 펀드A(장기투자형) / 2 / 3 / 2025-08-26 / 01 / 100,000
```

출력:

```text
두 건 모두 유지
```

이유:
- 현재 identity에 `fund_name`이 포함된다.
- 펀드명 표현 차이를 병합 단계에서 임의로 흡수하지 않는다.

---

## 11. 수정 포인트를 찾는 법

병합 버그를 볼 때는 아래 순서로 보면 된다.

1. 중복이 안 지워진다
   - `_order_identity()`
2. 같은 identity 안에서 대표 표현이 예상과 다르다
   - `_is_preferred_order_representation()`
3. JSON은 맞는데 CSV가 이상하다
   - `payload_to_csv_rows()`
   - `write_orders_csv()`

반대로 아래 이슈는 현재 병합 코드 문제가 아닐 가능성이 크다.

- `SUB/RED`가 순액 1건으로 합쳐지지 않는다
  - 현재는 의도적으로 합치지 않는다.
- RED를 음수로 복원하지 않는다
  - 현재 병합 단계는 signed amount 계산을 하지 않는다.

---

## 12. 한 줄 요약

현재 [app/component.py](/Users/bhkim/Documents/codex_prj_sam_asset/app/component.py)의 병합 규칙은

1. **완전히 같은 주문만 제거하고**
2. **펀드명 표현만 다른 주문은 합치지 않으며**
3. **SUB/RED를 순액으로 합치지 않는다**

로 이해하면 된다.

## 2026-04-16 업데이트 메모

- `transfer_amount` identity는 raw spreadsheet 문자열이 아니라 canonicalized 값 기준으로 봐야 한다.
- 즉 아래 둘은 같은 주문으로 취급해야 한다.
  - `70,000,000.00000001`
  - `70,000,000`
- 아래 둘도 같은 숫자 금액이므로 다른 핵심 필드가 모두 같으면 중복으로 취급해야 한다.
  - `23,213.40`
  - `23,213.4`
- 반대로 아래 둘은 다른 주문으로 남겨야 한다.
  - `50,572.49`
  - `50,572`
- 병합/중복 제거는 금액 canonicalization 이후에 수행해야 하고, true decimal을 integer로 뭉개면 안 된다.
- dedupe 후 남는 row의 `transfer_amount` 표기는 먼저 채택된 payload 표현을 유지한다.
- 문서 또는 보고서에 남길 검수 기준은 [test_report/20260420_로컬_WAS_테스트_수행_방안.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260420_로컬_WAS_테스트_수행_방안.md)를 출처로 둔다.

## 2026-04-20 업데이트 메모

- 하나생명 XLSX 전환과 WAS 병합에서도 다건 병합 identity는 변경하지 않았다.
- 하나생명 XLSX의 `수탁은행용/판매사용` 중복 collapse는 loader/extractor 내부 logical order 복구 문제이지, `app/component.py` 다건 병합 규칙으로 해결할 문제가 아니다.
- 후속 금액 source-scale 보존 정책에서는 final payload 금액의 소수 2자리 절삭/패딩을 제거했다.
  - `23,213.40`은 `23,213.40`으로 유지한다.
  - `23,213.4`는 `23,213.4`로 유지한다.
  - `23,213.409`는 `23,213.409`로 유지한다.
- 2026-04-20 병합 범위와 검증 결과는 [merge_report/merge_result_report_20260420_01.md](/Users/bhkim/Documents/codex_prj_sam_asset/merge_report/merge_result_report_20260420_01.md)를 출처로 둔다.
- `SMOKE_PASS`와 데이터 추출 보고서 작성 규칙은 [test_report/20260420_로컬_WAS_테스트_수행_방안.md](/Users/bhkim/Documents/codex_prj_sam_asset/test_report/20260420_로컬_WAS_테스트_수행_방안.md)를 출처로 둔다.
