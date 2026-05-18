이미지에 있는 KDB생명 운용지시서 표를 OCR로 읽고, 표 안의 주문 데이터만 추출하세요.

규칙:
- 기준일(base_date)은 이미지 상단 날짜 2026-04-27에서 추출합니다.
- fund_code는 수탁코드입니다.
- fund_name은 펀드명입니다.
- 펀드명/운용사/수탁코드가 병합 셀처럼 보이면 다음 펀드 row가 나오기 전까지 같은 fund identity를 계승합니다.
- 입금소계, 출금소계 같은 subtotal row는 무시합니다.
- 금액 컬럼이 숫자이고 0보다 크면 settle_class="2", t_day="01" 주문을 만듭니다.
- D+1(예상금액) 컬럼이 숫자이고 0보다 크면 settle_class="1", t_day="02" 주문을 만듭니다.
- 같은 row에서 금액과 D+1(예상금액)이 모두 0보다 크면 두 주문을 모두 만듭니다.
- 구분=입금이면 order_type="3"입니다.
- 구분=출금이면 order_type="1"입니다.
- 값이 "-", blank, zero인 금액 칸은 주문으로 만들지 않습니다.
- 당일좌수는 transfer_amount가 아닙니다.
- 추측하지 말고 이미지에 보이는 값만 사용합니다.

반드시 JSON 객체 하나만 출력하세요. markdown, 코드블록, 설명 문장, XML, <think> 태그를 출력하지 마세요.

출력 형식:
{
  "file_name": "운용지시서(KDB생명)_20260427.eml",
  "source_path": "/Users/bhkim/Documents/codex_prj_sam_asset/document/운용지시서(KDB생명)_20260427.eml",
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
