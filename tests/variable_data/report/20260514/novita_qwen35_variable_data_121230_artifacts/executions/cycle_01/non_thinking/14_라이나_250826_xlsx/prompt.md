이미지에 있는 라이나생명 `당일` 시트 표에서 설정/해지 운용지시 주문만 추출하세요.

규칙:
- `당일 (2)` 같은 다른 시트는 사용하지 않습니다. 현재 이미지의 `당일` 시트만 사용합니다.
- 기준일(base_date)은 결제일 2025-08-26입니다.
- 펀드코드가 `합계`를 포함하는 subtotal/total row는 무시합니다.
- 설정금액이 0보다 크면 order_type="3" 주문을 만듭니다.
- 해지금액이 0보다 크면 order_type="1" 주문을 만듭니다.
- 모든 주문의 settle_class는 "1", t_day는 "01"입니다.
- transfer_amount는 표의 설정금액 또는 해지금액을 comma 포함 문자열로 출력합니다.
- 전일좌수, 설정신청좌수, 해지좌수, 기준가격, 잔여좌수, 미처리좌수/금액은 주문 금액이 아닙니다.
- 추측하지 말고 이미지에 보이는 값만 사용합니다.

반드시 JSON 객체 하나만 출력하세요. markdown, 코드블록, 설명 문장, XML, <think> 태그를 출력하지 마세요.

출력 형식:
{
  "file_name": "라이나_250826.xlsx",
  "source_path": "/Users/bhkim/Documents/codex_prj_sam_asset/document/라이나_250826.xlsx",
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
