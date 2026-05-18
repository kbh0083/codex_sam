이미지에 있는 한화생명 설정/해지 내역서 표를 OCR로 읽고 주문 데이터만 추출하세요.

규칙:
- 기준일(base_date)은 2025-11-28입니다.
- 펀드코드, 펀드명은 표에 보이는 값을 그대로 사용합니다.
- 총계 row와 금액이 0인 row는 무시합니다.
- 입금 row의 설정(예탁) 및 해지금액이 0보다 크면 order_type="3", settle_class="2", t_day="01" 주문입니다.
- 출금 row의 설정(예탁) 및 해지금액 절대값이 0보다 크면 order_type="1", settle_class="2", t_day="01" 주문입니다.
- 환매 신청 row의 익익영업일 이체예상금액이 0보다 크면 order_type="1", settle_class="1", t_day="03" 주문입니다.
- transfer_amount는 음수 기호 없이 comma 포함 양수 문자열로 출력합니다.
- 보수출금, 예탁금 이용료, 감사인보수, 초기자금은 주문 금액으로 만들지 않습니다.
- 병합 셀처럼 보이는 펀드코드/펀드명은 다음 펀드 row가 나오기 전까지 계승합니다.

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
