동양생명 지시서 이미지에서 `정산내역` 섹션만 읽어 JSON 객체 하나로 추출하세요.

중요 규칙:
- HTML 원문이나 hidden text가 아니라 첨부 이미지에 보이는 표만 OCR합니다.
- `예상내역`, 연락처, 합계 row는 추출하지 않습니다.
- 합계 row 제외 data row는 정확히 15개입니다.
- fund_code와 fund_name은 이미지에 보이는 문자열 그대로 복사합니다.
- 절대 교정/정규화하지 마세요: 숫자 0과 알파벳 O, `유니버셜`과 `유니버설`, 로마숫자 `Ⅱ`와 알파벳 `II`를 구분하세요.
- 이 문서의 OCR 민감 펀드명은 아래 표기를 우선합니다:
  - BBI100: `변액유니버셜종신-채권형`
  - BBIM20: `변액유니버셜종신-채권형Ⅱ(채권_삼성)`
  - BBJ650: `변액연금Ⅱ 주식혼합1형(ETF_삼성)`
  - BBJ770: `변액연금Ⅱ 주식혼합2형(ETF_삼성)`
- 금액은 comma와 음수 기호가 보이면 그대로 적고, blank 또는 dash도 보이는 대로 적습니다.
- 첫 글자는 `{`, 마지막 글자는 `}`여야 합니다. markdown/code fence/설명문/<think>는 출력하지 마세요.

반환 필드:
section=`settlement`, base_date=`2026-03-18`, row_count, issues, rows.
rows item: row_index, fund_code, fund_name, setting_amount, redemption_amount, settlement_amount.
