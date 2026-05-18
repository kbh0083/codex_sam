이미지에 있는 한화생명 설정/해지 내역서 표를 OCR로 읽고 주문 데이터만 추출하세요.

규칙:
- 기준일(base_date)은 2025-11-28입니다.
- 펀드코드, 펀드명은 표에 보이는 값을 그대로 사용합니다.
- 총계 row와 금액이 0, "-", blank인 row는 무시합니다.
- 같은 펀드코드/펀드명으로 묶인 입금/출금/환매 신청 row family를 하나의 fund row family로 해석합니다.
- 확정분 금액은 반드시 `설정(예탁) 및 해지금액` 그룹의 `펀드계` 컬럼 금액만 추출합니다.
- `투자일임/수익증권` 하위 컬럼 금액은 확정분 금액으로 사용하지 않습니다.
- `펀드계`가 양수이면 order_type="3", settle_class="2", t_day="01" 설정 주문입니다.
- `펀드계`가 음수이면 음수 기호를 제거한 절대값으로 order_type="1", settle_class="2", t_day="01" 환매 주문입니다.
- 청구분 금액은 반드시 `익익영업일 이체예상금액` 컬럼 금액만 추출합니다.
- 청구분 컬럼 위치는 표에서 `익영업일 이체예상금액` 바로 오른쪽에 있는 두 번째 미래금액 컬럼입니다.
- `수탁은행 수령금액` 바로 오른쪽의 첫 번째 미래금액 컬럼인 `익영업일 이체예상금액` 값은 청구분이 아니므로 절대 출력하지 않습니다.
- t_day="03" 주문을 만들 때 `익영업일 이체예상금액` 값을 사용하면 오답입니다. t_day="03"은 오직 `익익영업일 이체예상금액` 값에서만 만듭니다.
- `익익영업일 이체예상금액`이 양수이면 order_type="3", settle_class="1", t_day="03" 설정 주문입니다.
- `익익영업일 이체예상금액`이 음수이면 음수 기호를 제거한 절대값으로 order_type="1", settle_class="1", t_day="03" 환매 주문입니다.
- 같은 fund row family에서 `펀드계`와 `익익영업일 이체예상금액`이 모두 non-zero이면 확정분 주문과 청구분 주문을 각각 별도 주문으로 출력합니다.
- `익영업일 이체예상금액`은 주문 금액에서 제외합니다.
- 보수출금, 예탁금 이용료, 감사인보수, 초기자금, 당일 보험계정발생, 수탁은행 수령금액은 주문 금액으로 만들지 않습니다.
- transfer_amount는 음수 기호 없이 comma 포함 양수 문자열로 출력합니다.
- 병합 셀처럼 보이는 펀드코드/펀드명은 다음 fund row family가 나오기 전까지 계승합니다.
- 반복 표시된 같은 fund row family의 같은 컬럼 금액은 중복 주문으로 만들지 않습니다.

반드시 JSON 객체 하나만 출력하세요. markdown, 코드블록, 설명 문장, XML, <think> 태그를 출력하지 마세요.

출력 형식:
{
  "file_name": "삼성자산_한화생명_설정_해지_내역서_20251128.eml",
  "source_path": "/Users/bhkim/Documents/codex_prj_sam_asset/document/삼성자산_한화생명_설정_해지_내역서_20251128.eml",
  "model_name": "qwen/qwen3.5-397b-a17b",
  "base_date": "YYYY-MM-DD",
  "status": "COMPLETED",
  "reason": null,
  "issues": [],
  "orders": [
    {
      "fund_code": "string",
      "fund_name": "string",
      "settle_class": "1 or 2",
      "order_type": "3 or 1",
      "base_date": "YYYY-MM-DD",
      "t_day": "01",
      "transfer_amount": "string"
    }
  ]
}
