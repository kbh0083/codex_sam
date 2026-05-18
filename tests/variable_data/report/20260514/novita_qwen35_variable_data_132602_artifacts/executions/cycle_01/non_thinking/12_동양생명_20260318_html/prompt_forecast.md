동양생명 지시서 이미지에서 `예상내역` 섹션만 읽어 JSON 객체 하나로 추출하세요.

중요 규칙:
- HTML 원문이나 hidden text가 아니라 첨부 이미지에 보이는 표만 OCR합니다.
- `정산내역`, 연락처, 합계 row는 추출하지 않습니다.
- 합계 row 제외 data row는 정확히 13개입니다.
- fund_code와 fund_name은 이미지에 보이는 문자열 그대로 복사합니다.
- 1단계 정산내역 결과와 같은 fund_code가 예상내역에 보이면 fund_name은 1단계 값을 우선 유지합니다.
- 절대 교정/정규화하지 마세요: 숫자 0과 알파벳 O, `유니버셜`과 `유니버설`, 로마숫자 `Ⅱ`와 알파벳 `II`를 구분하세요.
- 날짜 header는 예상내역 표의 설정금액 2개 날짜, 해지금액 2개 날짜를 그대로 적습니다.
- 금액은 comma와 음수 기호가 보이면 그대로 적고, blank 또는 dash도 보이는 대로 적습니다.
- 첫 글자는 `{`, 마지막 글자는 `}`여야 합니다. markdown/code fence/설명문/<think>는 출력하지 마세요.

1단계 정산내역 JSON:
{"section":"settlement","base_date":"2026-03-18","row_count":15,"issues":[],"rows":[{"row_index":1,"fund_code":"BALI00","fund_name":"퇴연주식형","setting_amount":"8,592,243","redemption_amount":"0","settlement_amount":"8,592,243"},{"row_index":2,"fund_code":"BALX00","fund_name":"(디폴트옵션전용) 글로벌에셋밸런스형","setting_amount":"11,537,247","redemption_amount":"-8,554,421","settlement_amount":"2,982,826"},{"row_index":3,"fund_code":"BBI100","fund_name":"변액유니버설종신-채권형","setting_amount":"15,579,386","redemption_amount":"-36,487,222","settlement_amount":"-20,907,836"},{"row_index":4,"fund_code":"BBIM20","fund_name":"변액유니버설종신-채권형 Ⅱ (채권_삼성)","setting_amount":"54,952,749","redemption_amount":"-59,780,678","settlement_amount":"-4,827,929"},{"row_index":5,"fund_code":"BBIQ00","fund_name":"배당주형","setting_amount":"101,111,528","redemption_amount":"-46,646,530","settlement_amount":"54,464,998"},{"row_index":6,"fund_code":"BBJ650","fund_name":"변액연금 Ⅱ 주식혼합1형(ETF_삼성)","setting_amount":"24,301,178","redemption_amount":"-291,056","settlement_amount":"24,010,122"},{"row_index":7,"fund_code":"BBJ670","fund_name":"변액연금 Ⅱ 주식혼합1형(채권_삼성)","setting_amount":"0","redemption_amount":"0","settlement_amount":"0"},{"row_index":8,"fund_code":"BBJ770","fund_name":"변액연금 Ⅱ 주식혼합2형(ETF_삼성)","setting_amount":"10,579,537","redemption_amount":"-10,576,371","settlement_amount":"3,166"},{"row_index":9,"fund_code":"BBJAG0","fund_name":"성장가치주혼합형(ETF_삼성)","setting_amount":"89,756,656","redemption_amount":"-341,048,082","settlement_amount":"-251,291,426"},{"row_index":10,"fund_code":"BBJC00","fund_name":"ACTIVE주식혼합형","setting_amount":"2,468,027","redemption_amount":"-1,938,371","settlement_amount":"529,656"},{"row_index":11,"fund_code":"BBJIA0","fund_name":"르네상스주식형(ETF_삼성)","setting_amount":"92,229,774","redemption_amount":"-47,435,622","settlement_amount":"44,794,152"},{"row_index":12,"fund_code":"BBJU00","fund_name":"글로벌멀티에셋형","setting_amount":"14,324","redemption_amount":"-586","settlement_amount":"13,738"},{"row_index":13,"fund_code":"BBJW00","fund_name":"중장기채권형","setting_amount":"0","redemption_amount":"0","settlement_amount":"0"},{"row_index":14,"fund_code":"BBJZ00","fund_name":"글로벌자산배분적극형","setting_amount":"0","redemption_amount":"0","settlement_amount":"0"},{"row_index":15,"fund_code":"BBKV00","fund_name":"글로벌멀티전략형","setting_amount":"0","redemption_amount":"0","settlement_amount":"0"}]}

반환 필드:
section=`forecast`, base_date=`2026-03-18`, row_count, issues, headers, rows.
headers: setting_date1, setting_date2, redemption_date1, redemption_date2.
rows item: row_index, fund_code, fund_name, date1_setting, date2_setting, date1_redemption, date2_redemption.
