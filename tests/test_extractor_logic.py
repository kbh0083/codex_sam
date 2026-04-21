from __future__ import annotations

from pathlib import Path
import tempfile
import time
import types
import unittest
from unittest.mock import Mock, patch

import yaml

import app.extractor as extractor_module
from app.document_loader import DocumentLoadTaskPayload, TargetFundScope
from app.extractor import (
    FundAmountItem,
    FundAmountResult,
    FundBaseDateItem,
    FundBaseDateResult,
    FundOrderExtractor,
    FundResolvedItem,
    FundSeedResult,
    FundSeedItem,
    FundSettleItem,
    FundSettleResult,
    FundSlotItem,
    FundSlotResult,
    _load_prompt_bundle,
)
from app.schemas import ExtractionResult
from app.schemas import OrderExtraction
from app.schemas import OrderType, SettleClass


class ExtractorLogicTests(unittest.TestCase):
    """`FundOrderExtractor`의 순수 로직 helper를 집중 검증한다."""

    def setUp(self) -> None:
        """LLM 의존 없이 helper 메서드만 호출할 수 있는 extractor 골격을 만든다."""
        self.extractor = object.__new__(FundOrderExtractor)

    def _write_prompt_yaml(
        self,
        path: Path,
        user_prompt_template: str,
        system_prompt: str = "system prompt",
        retry_user_prompt_template: str | None = None,
    ) -> None:
        """prompt reload 관련 테스트용 최소 YAML 파일을 생성한다."""
        retry_template = retry_user_prompt_template or (
            "Retry {retry_attempt_number}/{retry_max_attempts}\n"
            "Stage {stage_number}/{total_stage_count}\n"
            "Stage goal: {stage_goal}\n"
            "Stage name: {stage_name}\n"
            "Stage instructions: {stage_instructions}\n"
            "Retry instructions: {retry_instructions}\n"
            "Output contract: {output_contract}\n"
            "Retry target issues JSON:\n{retry_target_issues_json}\n"
            "Previous output items JSON:\n{previous_output_items_json}\n"
            "Retry focus items JSON:\n{retry_focus_items_json}\n"
            "Input items JSON:\n{input_items_json}\n"
            "Document text:\n{document_text}"
        )
        payload = {
            "system_prompt": system_prompt,
            "user_prompt_template": user_prompt_template,
            "retry_user_prompt_template": retry_template,
            "stages": [
                {"number": 1, "name": "instruction_document", "goal": "goal 0", "instructions": "instruction 0", "retry_instructions": "retry instruction 0", "output_contract": '{"items":[],"issues":[]}'},
                {"number": 2, "name": "fund_inventory", "goal": "goal 1", "instructions": "instruction 1", "retry_instructions": "retry instruction 1", "output_contract": '{"items":[],"issues":[]}'},
                {"number": 3, "name": "base_date", "goal": "goal 2", "instructions": "instruction 2", "retry_instructions": "retry instruction 2", "output_contract": '{"items":[],"issues":[]}'},
                {"number": 4, "name": "t_day", "goal": "goal 3", "instructions": "instruction 3", "retry_instructions": "retry instruction 3", "output_contract": '{"items":[],"issues":[]}'},
                {"number": 5, "name": "transfer_amount", "goal": "goal 4", "instructions": "instruction 4", "retry_instructions": "retry instruction 4", "output_contract": '{"items":[],"issues":[]}'},
                {"number": 6, "name": "settle_class", "goal": "goal 5", "instructions": "instruction 5", "retry_instructions": "retry instruction 5", "output_contract": '{"items":[],"issues":[]}'},
                {"number": 7, "name": "order_type", "goal": "goal 6", "instructions": "instruction 6", "retry_instructions": "retry instruction 6", "output_contract": '{"items":[],"issues":[]}'},
            ],
        }
        path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8")

    def test_build_result_keeps_explicit_inflow_and_outflow_as_separate_orders(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="F001",
                fund_name="Alpha",
                base_date="2025-11-27",
                t_day=0,
                slot_id="a1",
                evidence_label="입금액",
                transfer_amount="100",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="F001",
                fund_name="Alpha",
                base_date="2025-11-27",
                t_day=0,
                slot_id="a2",
                evidence_label="출금액",
                transfer_amount="40",
                settle_class="CONFIRMED",
                order_type="RED",
            ),
            FundResolvedItem(
                fund_code="F002",
                fund_name="Beta",
                base_date="2025-11-27",
                t_day=0,
                slot_id="b1",
                evidence_label="출금액",
                transfer_amount="200",
                settle_class="CONFIRMED",
                order_type="RED",
            ),
        ]

        result = self.extractor._build_result(items, [])

        self.assertEqual(len(result.orders), 3)
        alpha_sub = next(
            order for order in result.orders if order.fund_code == "F001" and order.order_type.value == "SUB"
        )
        alpha_red = next(
            order for order in result.orders if order.fund_code == "F001" and order.order_type.value == "RED"
        )
        beta = next(order for order in result.orders if order.fund_code == "F002")
        self.assertEqual(alpha_sub.transfer_amount, "100")
        self.assertEqual(alpha_red.transfer_amount, "-40")
        self.assertEqual(beta.transfer_amount, "-200")
        self.assertEqual(beta.order_type.value, "RED")

    def test_build_result_preserves_document_decimal_scale(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="F001",
                fund_name="Scale Two",
                base_date="2025-11-27",
                t_day=0,
                slot_id="a1",
                evidence_label="당일이체금액",
                transfer_amount="23,213.40",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="F002",
                fund_name="Scale One",
                base_date="2025-11-27",
                t_day=0,
                slot_id="b1",
                evidence_label="당일이체금액",
                transfer_amount="23,213.4",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="F003",
                fund_name="Scale Three",
                base_date="2025-11-27",
                t_day=0,
                slot_id="c1",
                evidence_label="당일이체금액",
                transfer_amount="23,213.409",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="F004",
                fund_name="Artifact",
                base_date="2025-11-27",
                t_day=0,
                slot_id="d1",
                evidence_label="당일이체금액",
                transfer_amount="70,000,000.00000001",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
        ]

        result = self.extractor._build_result(items, [])
        amounts = {order.fund_code: order.transfer_amount for order in result.orders}

        self.assertEqual(amounts["F001"], "23,213.40")
        self.assertEqual(amounts["F002"], "23,213.4")
        self.assertEqual(amounts["F003"], "23,213.409")
        self.assertEqual(amounts["F004"], "70,000,000")

    def test_build_result_dedupes_same_evidence_with_decimal_scale_difference(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="F001",
                fund_name="Alpha",
                base_date="2025-11-27",
                t_day=0,
                slot_id="a1",
                evidence_label="당일이체금액",
                transfer_amount="23,213.40",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="F001",
                fund_name="Alpha",
                base_date="2025-11-27",
                t_day=0,
                slot_id="a1",
                evidence_label="당일이체금액",
                transfer_amount="23,213.4",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
        ]

        result = self.extractor._build_result(items, [])

        self.assertEqual(len(result.orders), 1)
        self.assertEqual(result.orders[0].transfer_amount, "23,213.40")

    def test_filter_fund_seeds_by_scope_keeps_only_samsung_targets(self) -> None:
        scope = TargetFundScope(
            manager_column_present=True,
            include_all_funds=False,
            fund_codes=frozenset({"6104", "6101"}),
            fund_names=frozenset({"채권형", "혼합성장형"}),
            canonical_fund_names=frozenset({"채권형", "혼합성장형"}),
        )
        seeds = [
            FundSeedItem(fund_code="6104", fund_name="채권형"),
            FundSeedItem(fund_code="6105", fund_name="해외혼합형"),
            FundSeedItem(fund_code="6101", fund_name="혼합성장형"),
        ]

        filtered = self.extractor._filter_fund_seeds_by_scope(seeds, scope)

        self.assertEqual([(item.fund_code, item.fund_name) for item in filtered], [("6104", "채권형"), ("6101", "혼합성장형")])

    def test_issue_has_code_matches_for_variant_suffix(self) -> None:
        self.assertTrue(FundOrderExtractor._issue_has_code("T_DAY_MISSING_FOR_6114", "T_DAY_MISSING"))

    def test_filter_fund_scope_matches_canonicalized_name(self) -> None:
        scope = TargetFundScope(
            manager_column_present=True,
            include_all_funds=False,
            fund_codes=frozenset(),
            fund_names=frozenset({"AIA VUL Alpha (삼성)"}),
            canonical_fund_names=frozenset({"aiavulalpha삼성"}),
        )

        self.assertTrue(self.extractor._is_target_fund("", "AIA VUL Alpha(삼성)", scope))
        self.assertTrue(self.extractor._is_target_fund("", "AIA  VUL Alpha / 삼성", scope))

    def test_build_result_prefers_execution_net_amount_when_present(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="F010",
                fund_name="Gamma",
                base_date="2025-11-27",
                t_day=0,
                slot_id="g1",
                evidence_label="입금액",
                transfer_amount="58,812",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="F010",
                fund_name="Gamma",
                base_date="2025-11-27",
                t_day=0,
                slot_id="g2",
                evidence_label="당일이체금액",
                transfer_amount="50,712",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="F011",
                fund_name="Delta",
                base_date="2025-11-27",
                t_day=0,
                slot_id="d1",
                evidence_label="당일이체금액",
                transfer_amount="-12,443,099",
                settle_class="CONFIRMED",
                order_type="RED",
            ),
            FundResolvedItem(
                fund_code="F012",
                fund_name="Epsilon",
                base_date="2025-11-27",
                t_day=0,
                slot_id="e1",
                evidence_label="당일이체금액",
                transfer_amount="35,450,749",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
        ]

        result = self.extractor._build_result(items, [])

        self.assertEqual(len(result.orders), 3)
        gamma = next(order for order in result.orders if order.fund_code == "F010")
        delta = next(order for order in result.orders if order.fund_code == "F011")
        epsilon = next(order for order in result.orders if order.fund_code == "F012")
        self.assertEqual(gamma.transfer_amount, "50,712")
        self.assertEqual(gamma.order_type.value, "SUB")
        self.assertEqual(delta.transfer_amount, "-12,443,099")
        self.assertEqual(delta.order_type.value, "RED")
        self.assertEqual(epsilon.transfer_amount, "35,450,749")
        self.assertEqual(epsilon.order_type.value, "SUB")

    def test_build_result_merges_same_fund_code_candidates_with_display_name_spacing_difference(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="BALX00",
                fund_name="(디폴트옵션전용) 글로벌에셋밸런스형",
                base_date="2026-03-18",
                t_day=0,
                slot_id="balx-confirmed-a",
                evidence_label="정산액",
                transfer_amount="2,982,826",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="BALX00",
                fund_name="(디폴트옵션전용)글로벌에셋밸런스형",
                base_date="2026-03-18",
                t_day=0,
                slot_id="balx-confirmed-b",
                evidence_label="정산액",
                transfer_amount="2,982,826",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="BALX00",
                fund_name="(디폴트옵션전용)글로벌에셋밸런스형",
                base_date="2026-03-18",
                t_day=1,
                slot_id="balx-pending-a",
                evidence_label="해지금액 / 3월19일",
                transfer_amount="44,300,158",
                settle_class="PENDING",
                order_type="RED",
            ),
            FundResolvedItem(
                fund_code="BALX00",
                fund_name="(디폴트옵션전용) 글로벌에셋밸런스형",
                base_date="2026-03-18",
                t_day=1,
                slot_id="balx-pending-b",
                evidence_label="해지금액 / 3월19일",
                transfer_amount="44,300,158",
                settle_class="PENDING",
                order_type="RED",
            ),
        ]

        result = self.extractor._build_result(items, [])

        self.assertEqual(len(result.orders), 2)
        confirmed = next(order for order in result.orders if order.settle_class.value == "CONFIRMED")
        pending = next(order for order in result.orders if order.settle_class.value == "PENDING")
        self.assertEqual(confirmed.fund_name, "(디폴트옵션전용) 글로벌에셋밸런스형")
        self.assertEqual(confirmed.transfer_amount, "2,982,826")
        self.assertEqual(pending.fund_name, "(디폴트옵션전용)글로벌에셋밸런스형")
        self.assertEqual(pending.transfer_amount, "-44,300,158")

    def test_build_result_uses_buy_sell_report_separate_buy_and_sell_amounts(self) -> None:
        document_text = (
            "Structured markdown view:\n"
            "## EML sample\n\n"
            "Raw text backup:\n"
            "```text\n"
            "BUY & SELL REPORT |  |  |  |  |  |  |  |  |  |\n"
            "Date | Buy&Sell | External Fund Manager | Fund Code | Fund Name | Fund Price | Buy |  | Sell |  | Custodian Bank\n"
            " |  |  |  |  |  | Amount | Unit | Amount | Unit |\n"
            "11-28-2025 | Buy&Sell | 삼성액티브 | 151128 | AIA VUL 주식형(1형)(삼성) | 1785.89 | 1,311,285 |  | 55,901,379 |  | \n"
            " |  | 삼성액티브 | 151161 | AIA VUL 장기성장주식형 (1형) - 1호펀드 (삼성) | 2588.67 | 3,597,188 |  | 9,770,019 |  | \n"
            "Total |  |  |  |  |  | 4,908,473 |  | 65,671,398 |  |\n"
            "```\n"
        )

        result = self.extractor._build_result(
            [],
            [
                "MULTIPLE_SLOTS_PER_FUND_WITH_SAME_TDAY",
                "ORDER_TYPE_AMBIGUOUS",
                "EVIDENCE_LABEL_AMBIGUOUS_FOR_NET",
                "EVIDENCE_LABEL_AMBIGUOUS_BUT_BOTH_USED",
            ],
            document_text=document_text,
        )

        self.assertEqual(len(result.orders), 4)
        first_sub = next(order for order in result.orders if order.fund_code == "151128" and order.order_type.value == "SUB")
        first_red = next(order for order in result.orders if order.fund_code == "151128" and order.order_type.value == "RED")
        second_sub = next(order for order in result.orders if order.fund_code == "151161" and order.order_type.value == "SUB")
        second_red = next(order for order in result.orders if order.fund_code == "151161" and order.order_type.value == "RED")
        self.assertEqual(first_sub.settle_class.value, "CONFIRMED")
        self.assertEqual(first_sub.base_date, "2025-11-28")
        self.assertEqual(first_sub.t_day, 0)
        self.assertEqual(first_sub.transfer_amount, "1,311,285")
        self.assertEqual(first_red.transfer_amount, "-55,901,379")
        self.assertEqual(second_sub.transfer_amount, "3,597,188")
        self.assertEqual(second_red.transfer_amount, "-9,770,019")
        self.assertEqual(result.issues, [])

    def test_build_result_buy_sell_report_drops_stale_blocking_stage_issues(self) -> None:
        document_text = (
            "Structured markdown view:\n"
            "## EML sample\n\n"
            "Raw text backup:\n"
            "```text\n"
            "BUY & SELL REPORT |  |  |  |  |  |  |  |  |  |\n"
            "Date | Buy&Sell | External Fund Manager | Fund Code | Fund Name | Fund Price | Buy |  | Sell |  | Custodian Bank\n"
            " |  |  |  |  |  | Amount | Unit | Amount | Unit |\n"
            "11-28-2025 | Buy&Sell | 삼성액티브 | 151128 | AIA VUL 주식형(1형)(삼성) | 1785.89 | 1,311,285 |  | 55,901,379 |  | \n"
            "Total |  |  |  |  |  | 1,311,285 |  | 55,901,379 |  |\n"
            "```\n"
        )

        result = self.extractor._build_result(
            [],
            [
                "FUND_DISCOVERY_EMPTY",
                "TRANSACTION_SLOT_EMPTY",
                "TRANSFER_AMOUNT_EMPTY",
                "SETTLE_CLASS_EMPTY",
                "LLM_INVALID_RESPONSE_FORMAT",
                "MANAGER_FILTER_APPLIED",
            ],
            document_text=document_text,
        )

        self.assertEqual(len(result.orders), 2)
        self.assertEqual(result.issues, ["MANAGER_FILTER_APPLIED"])

    def test_build_result_prefers_settlement_amount_label_when_present(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="F020",
                fund_name="Theta",
                base_date="2025-11-27",
                t_day=0,
                slot_id="t1",
                evidence_label="설정금액",
                transfer_amount="100",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="F020",
                fund_name="Theta",
                base_date="2025-11-27",
                t_day=0,
                slot_id="t2",
                evidence_label="해지금액",
                transfer_amount="40",
                settle_class="CONFIRMED",
                order_type="RED",
            ),
            FundResolvedItem(
                fund_code="F020",
                fund_name="Theta",
                base_date="2025-11-27",
                t_day=0,
                slot_id="t3",
                evidence_label="결제금액",
                transfer_amount="60",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
        ]

        result = self.extractor._build_result(items, [])

        self.assertEqual(len(result.orders), 1)
        self.assertEqual(result.orders[0].order_type.value, "SUB")
        self.assertEqual(result.orders[0].transfer_amount, "60")

    def test_build_result_prefers_net_settlement_amount_label_when_present(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="F021",
                fund_name="Iota",
                base_date="2026-03-18",
                t_day=0,
                slot_id="i1",
                evidence_label="설정액",
                transfer_amount="100",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="F021",
                fund_name="Iota",
                base_date="2026-03-18",
                t_day=0,
                slot_id="i2",
                evidence_label="해지액",
                transfer_amount="40",
                settle_class="CONFIRMED",
                order_type="RED",
            ),
            FundResolvedItem(
                fund_code="F021",
                fund_name="Iota",
                base_date="2026-03-18",
                t_day=0,
                slot_id="i3",
                evidence_label="정산액",
                transfer_amount="-60",
                settle_class="CONFIRMED",
                order_type="RED",
            ),
        ]

        result = self.extractor._build_result(items, [])

        self.assertEqual(len(result.orders), 1)
        self.assertEqual(result.orders[0].order_type.value, "RED")
        self.assertEqual(result.orders[0].transfer_amount, "-60")

    def test_build_deterministic_markdown_orders_reads_settlement_and_schedule_tables(self) -> None:
        markdown_text = (
            "## HTML 동양생명_20260318.html\n\n"
            "```text\n"
            "2026년 3월 18일\n"
            "정산내역\n"
            "예상내역\n"
            "```"
            "\n\n| 펀드코드 | 펀드명 | 설정액 | 해지액 | 정산액 |\n"
            "| --- | --- | --- | --- | --- |\n"
            "| BALX00 | Alpha | 100 | -40 | 60 |\n"
            "\n\n| 펀드코드 | 펀드명 | 설정금액 / 3월19일 | 설정금액 / 3월20일 | 해지금액 / 3월19일 | 해지금액 / 3월20일 |\n"
            "| --- | --- | --- | --- | --- | --- |\n"
            "| BALX00 | Alpha | 10 | 20 | 30 | 40 |\n"
        )
        raw_text = (
            "[HTML 동양생명_20260318.html]\n"
            "2026년 3월 18일\n"
            "정산내역\n"
            "예상내역\n"
        )

        orders = self.extractor._build_deterministic_markdown_orders(
            markdown_text=markdown_text,
            raw_text=raw_text,
            target_fund_scope=None,
        )

        self.assertEqual(len(orders), 5)
        confirmed = [order for order in orders if order.t_day == 0]
        pending = [order for order in orders if order.t_day > 0]
        self.assertEqual(len(confirmed), 1)
        self.assertEqual(len(pending), 4)
        self.assertEqual(confirmed[0].order_type.value, "SUB")
        self.assertEqual(confirmed[0].transfer_amount, "60")
        self.assertEqual({order.t_day for order in pending}, {1, 2})
        self.assertEqual(
            {(order.order_type.value, order.transfer_amount) for order in pending},
            {
                ("SUB", "10"),
                ("SUB", "20"),
                ("RED", "-30"),
                ("RED", "-40"),
            },
        )

    def test_build_deterministic_markdown_orders_allows_missing_fund_name_for_heungkuk(self) -> None:
        markdown_text = (
            "## EML [흥국생명] 설정해지 내역 운용지시건-삼성-0413\n\n"
            "| 펀드코드 | 추가설정금액 | 당일인출금액 | 해지신청 | 비고 |\n"
            "| --- | --- | --- | --- | --- |\n"
            "| 450038 | 0.4억 | / | / | / |\n"
            "| 450036 | / | 0.8억 | 0.5억 | 0.8억 > 10일 신청건 0.5억 > 15일 인출예정 |\n"
            "| 7개 펀드 | 0.4억 | 8.4억 | / | / |\n"
        )
        raw_text = "Date (Asia/Seoul): 2026-04-13\n"

        orders = self.extractor._build_deterministic_markdown_orders(
            markdown_text=markdown_text,
            raw_text=raw_text,
            target_fund_scope=None,
        )

        self.assertEqual(len(orders), 3)
        self.assertTrue(all(order.fund_name == "-" for order in orders))
        confirmed_sub = next(order for order in orders if order.fund_code == "450038")
        self.assertEqual(confirmed_sub.order_type, OrderType.SUB)
        self.assertEqual(confirmed_sub.settle_class, SettleClass.CONFIRMED)
        self.assertEqual(confirmed_sub.t_day, 0)
        self.assertEqual(confirmed_sub.transfer_amount, "40,000,000")
        pending_red = next(
            order
            for order in orders
            if order.fund_code == "450036"
            and order.settle_class is SettleClass.PENDING
        )
        self.assertEqual(pending_red.order_type, OrderType.RED)
        self.assertEqual(pending_red.t_day, 2)
        self.assertEqual(pending_red.transfer_amount, "-50,000,000")

    def test_build_deterministic_markdown_orders_prefers_uniform_transaction_row_date_over_asia_seoul_header(self) -> None:
        markdown_text = (
            "## EML 삼성액티브자산운용_설정_해지_20251128.eml\n\n"
            "| Date | Buy&Sell | External Fund Manager | Fund Code | Fund Name | Buy Amount | Sell Amount |\n"
            "| --- | --- | --- | --- | --- | --- | --- |\n"
            "| 11-28-2025 | Buy&Sell | 삼성액티브 | 151128 | AIA VUL 주식형(1형)(삼성) | 1311285 | 55901379 |\n"
            "|  |  | 삼성액티브 | 151161 | AIA VUL 장기성장주식형 (1형) - 1호펀드 (삼성) | 3597188 | 9770019 |\n"
        )
        raw_text = (
            "[EML 삼성액티브자산운용_설정_해지_20251128.eml]\n"
            "Subject: [2025/11/18] 삼성액티브자산운용 설정 해지-2\n"
            "Date: Mon, 16 Mar 2026 06:43:45 +0000\n"
            "Date (Asia/Seoul): 2026-03-16\n"
            "BUY & SELL REPORT |  |  |  |  |  |  |  |  |  |\n"
            "Date | Buy&Sell | External Fund Manager | Fund Code | Fund Name | Fund Price | Buy |  | Sell |  | Custodian Bank\n"
            " |  |  |  |  |  | Amount | Unit | Amount | Unit |\n"
            "11-28-2025 | Buy&Sell | 삼성액티브 | 151128 | AIA VUL 주식형(1형)(삼성) | 1785.89 | 1,311,285 |  | 55,901,379 |  | \n"
            " |  | 삼성액티브 | 151161 | AIA VUL 장기성장주식형 (1형) - 1호펀드 (삼성) | 2588.67 | 3,597,188 |  | 9,770,019 |  | \n"
            "Total |  |  |  |  |  | 4,908,473 |  | 65,671,398 |  |\n"
        )

        orders = self.extractor._build_deterministic_markdown_orders(
            markdown_text=markdown_text,
            raw_text=raw_text,
            target_fund_scope=None,
        )

        self.assertEqual(len(orders), 4)
        self.assertTrue(all(order.base_date == "2025-11-28" for order in orders))
        self.assertEqual(
            sorted((order.fund_code, order.order_type.value, order.transfer_amount) for order in orders),
            [
                ("151128", "RED", "-55,901,379"),
                ("151128", "SUB", "1,311,285"),
                ("151161", "RED", "-9,770,019"),
                ("151161", "SUB", "3,597,188"),
            ],
        )

    def test_build_deterministic_markdown_orders_returns_empty_when_transaction_row_dates_conflict(self) -> None:
        markdown_text = (
            "## EML sample.eml\n\n"
            "| Date | Buy&Sell | External Fund Manager | Fund Code | Fund Name | Buy Amount | Sell Amount |\n"
            "| --- | --- | --- | --- | --- | --- | --- |\n"
            "| 11-28-2025 | Buy&Sell | 삼성액티브 | 151128 | AIA VUL 주식형(1형)(삼성) | 1311285 | 55901379 |\n"
            "| 11-29-2025 | Buy&Sell | 삼성액티브 | 151161 | AIA VUL 장기성장주식형 (1형) - 1호펀드 (삼성) | 3597188 | 9770019 |\n"
        )
        raw_text = (
            "[EML sample.eml]\n"
            "Date (Asia/Seoul): 2026-03-16\n"
            "BUY & SELL REPORT |  |  |  |  |  |  |  |  |  |\n"
            "Date | Buy&Sell | External Fund Manager | Fund Code | Fund Name | Fund Price | Buy |  | Sell |  | Custodian Bank\n"
            "11-28-2025 | Buy&Sell | 삼성액티브 | 151128 | AIA VUL 주식형(1형)(삼성) | 1785.89 | 1,311,285 |  | 55,901,379 |  | \n"
            "11-29-2025 | Buy&Sell | 삼성액티브 | 151161 | AIA VUL 장기성장주식형 (1형) - 1호펀드 (삼성) | 2588.67 | 3,597,188 |  | 9,770,019 |  | \n"
        )

        orders = self.extractor._build_deterministic_markdown_orders(
            markdown_text=markdown_text,
            raw_text=raw_text,
            target_fund_scope=None,
        )

        self.assertEqual(orders, [])

    def test_derive_expected_t_day_family_slots_supports_code_only_identity_for_heungkuk(self) -> None:
        document_text = (
            "Structured markdown view:\n"
            "## EML [흥국생명] 설정해지 내역 운용지시건-삼성-0413\n\n"
            "| 펀드코드 | 추가설정금액 | 당일인출금액 | 해지신청 | 비고 |\n"
            "| --- | --- | --- | --- | --- |\n"
            "| 450038 | 0.4억 | / | / | / |\n"
            "| 450036 | / | 0.8억 | 0.5억 | 0.8억 > 10일 신청건 0.5억 > 15일 인출예정 |\n"
            "| 7개 펀드 | 0.4억 | 8.4억 | / | / |\n"
        )
        input_items = [
            {"fund_code": "450038", "fund_name": "", "base_date": "2026-04-13"},
            {"fund_code": "450036", "fund_name": "", "base_date": "2026-04-13"},
        ]

        expected = self.extractor._derive_expected_t_day_family_slots(
            document_text=document_text,
            input_items=input_items,
        )

        self.assertEqual(expected[("450038", "", "2026-04-13")], {(0, "SUB")})
        self.assertEqual(expected[("450036", "", "2026-04-13")], {(0, "RED"), (2, "RED")})

    def test_looks_like_total_row_handles_count_fund_summary(self) -> None:
        self.assertTrue(self.extractor._looks_like_total_row("7개 펀드", "-"))
        self.assertFalse(self.extractor._looks_like_total_row("450038", "-"))

    def test_build_markdown_shortcut_orders_after_base_date_supports_heungkuk(self) -> None:
        markdown_text = (
            "## EML [흥국생명] 설정해지 내역 운용지시건-삼성-0413\n\n"
            "| 펀드코드 | 추가설정금액 | 당일인출금액 | 해지신청 | 비고 |\n"
            "| --- | --- | --- | --- | --- |\n"
            "| 450038 | 0.4억 | / | / | / |\n"
            "| 450036 | / | 0.8억 | 0.5억 | 0.8억 > 10일 신청건 0.5억 > 15일 인출예정 |\n"
            "| 450033 | / | 0.3억 | / | 10일 신청건 |\n"
        )
        raw_text = "Date (Asia/Seoul): 2026-04-13\n"
        base_dates = [
            FundBaseDateItem(fund_code="450038", fund_name="", base_date="2026-04-13"),
            FundBaseDateItem(fund_code="450036", fund_name="", base_date="2026-04-13"),
            FundBaseDateItem(fund_code="450033", fund_name="", base_date="2026-04-13"),
        ]

        orders = self.extractor._build_markdown_shortcut_orders_after_base_date(
            base_dates=base_dates,
            markdown_text=markdown_text,
            raw_text=raw_text,
            target_fund_scope=None,
            expected_order_count=4,
        )

        self.assertEqual(len(orders), 4)
        self.assertTrue(all(order.fund_name == "-" for order in orders))
        self.assertEqual(
            sorted((order.fund_code, order.t_day, order.settle_class.value, order.order_type.value, order.transfer_amount) for order in orders),
            [
                ("450033", 0, "CONFIRMED", "RED", "-30,000,000"),
                ("450036", 0, "CONFIRMED", "RED", "-80,000,000"),
                ("450036", 2, "PENDING", "RED", "-50,000,000"),
                ("450038", 0, "CONFIRMED", "SUB", "40,000,000"),
            ],
        )

    def test_build_markdown_shortcut_orders_after_base_date_requires_expected_count(self) -> None:
        markdown_text = (
            "## EML [흥국생명] 설정해지 내역 운용지시건-삼성-0413\n\n"
            "| 펀드코드 | 추가설정금액 | 당일인출금액 | 해지신청 | 비고 |\n"
            "| --- | --- | --- | --- | --- |\n"
            "| 450038 | 0.4억 | / | / | / |\n"
            "| 450036 | / | 0.8억 | 0.5억 | 0.8억 > 10일 신청건 0.5억 > 15일 인출예정 |\n"
        )
        raw_text = "Date (Asia/Seoul): 2026-04-13\n"
        base_dates = [
            FundBaseDateItem(fund_code="450038", fund_name="", base_date="2026-04-13"),
            FundBaseDateItem(fund_code="450036", fund_name="", base_date="2026-04-13"),
        ]

        orders = self.extractor._build_markdown_shortcut_orders_after_base_date(
            base_dates=base_dates,
            markdown_text=markdown_text,
            raw_text=raw_text,
            target_fund_scope=None,
            expected_order_count=None,
        )

        self.assertEqual(orders, [])

    def test_build_markdown_shortcut_orders_after_base_date_requires_expected_count_match(self) -> None:
        markdown_text = (
            "## EML [흥국생명] 설정해지 내역 운용지시건-삼성-0413\n\n"
            "| 펀드코드 | 추가설정금액 | 당일인출금액 | 해지신청 | 비고 |\n"
            "| --- | --- | --- | --- | --- |\n"
            "| 450038 | 0.4억 | / | / | / |\n"
        )
        raw_text = "Date (Asia/Seoul): 2026-04-13\n"
        base_dates = [FundBaseDateItem(fund_code="450038", fund_name="", base_date="2026-04-13")]

        orders = self.extractor._build_markdown_shortcut_orders_after_base_date(
            base_dates=base_dates,
            markdown_text=markdown_text,
            raw_text=raw_text,
            target_fund_scope=None,
            expected_order_count=2,
        )

        self.assertEqual(orders, [])

    def test_build_markdown_shortcut_orders_after_base_date_requires_family_match(self) -> None:
        markdown_text = (
            "## EML [흥국생명] 설정해지 내역 운용지시건-삼성-0413\n\n"
            "| 펀드코드 | 추가설정금액 | 당일인출금액 | 해지신청 | 비고 |\n"
            "| --- | --- | --- | --- | --- |\n"
            "| 450038 | 0.4억 | / | / | / |\n"
        )
        raw_text = "Date (Asia/Seoul): 2026-04-13\n"
        base_dates = [FundBaseDateItem(fund_code="999999", fund_name="", base_date="2026-04-13")]

        orders = self.extractor._build_markdown_shortcut_orders_after_base_date(
            base_dates=base_dates,
            markdown_text=markdown_text,
            raw_text=raw_text,
            target_fund_scope=None,
            expected_order_count=1,
        )

        self.assertEqual(orders, [])

    def test_build_markdown_shortcut_orders_after_base_date_skips_when_pending_note_is_incomplete(self) -> None:
        markdown_text = (
            "## EML [흥국생명] 설정해지 내역 운용지시건-삼성-0413\n\n"
            "| 펀드코드 | 추가설정금액 | 당일인출금액 | 해지신청 | 비고 |\n"
            "| --- | --- | --- | --- | --- |\n"
            "| 450036 | / | 0.8억 | 0.5억 | 0.8억 > 10일 신청건 |\n"
        )
        raw_text = "Date (Asia/Seoul): 2026-04-13\n"
        base_dates = [FundBaseDateItem(fund_code="450036", fund_name="", base_date="2026-04-13")]

        orders = self.extractor._build_markdown_shortcut_orders_after_base_date(
            base_dates=base_dates,
            markdown_text=markdown_text,
            raw_text=raw_text,
            target_fund_scope=None,
            expected_order_count=2,
        )

        self.assertEqual(orders, [])

    def test_build_markdown_shortcut_orders_after_base_date_ignores_section_only_raw_text(self) -> None:
        markdown_text = "The order of Subscription and Redemption"
        raw_text = (
            "[PAGE 1]\n"
            "The order of Subscription and Redemption\n"
            "1. Subscription\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Future Mobility Active ETF FoF FME 20,751,062.2603 27-Nov-25 1,032.61 21,427,754 HANA\n"
            "2. Redemption\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Bond ETF FoF BEE 185,714,727.6942 28-Nov-25 1,120.91 208,169,495 SCFB\n"
        )
        base_dates = [
            FundBaseDateItem(fund_code="FME", fund_name="Future Mobility Active ETF FoF", base_date="2025-11-27"),
            FundBaseDateItem(fund_code="BEE", fund_name="Bond ETF FoF", base_date="2025-11-28"),
        ]

        orders = self.extractor._build_markdown_shortcut_orders_after_base_date(
            base_dates=base_dates,
            markdown_text=markdown_text,
            raw_text=raw_text,
            target_fund_scope=None,
            expected_order_count=2,
        )

        self.assertEqual(orders, [])

    def test_build_markdown_shortcut_orders_after_base_date_prefers_hanwha_fund_total_column(self) -> None:
        markdown_text = (
            "## HTML hanhwa_20250826.html\n\n"
            "| 펀드코드 | 펀드명 | 구분 | 설정(예탁) 및 해지금액 / 투자일임/수익증권 | 설정(예탁) 및 해지금액 / 펀드계 | "
            "익영업일(T+1) / 이체예상금액 | 익익영업일(T+2) / 이체예상금액 | 수탁은행 / 환매사 |\n"
            "| --- | --- | --- | --- | --- | --- | --- | --- |\n"
            "| V2201S | VUL적립 성장주식형(삼성액티브주식) | 입금 | 21,187,951 | 21,187,951 | -36,400,000 | -66,478,486 | 하나은행 |\n"
            "| V2201S | VUL적립 성장주식형(삼성액티브주식) | 출금 | 0 |  |  |  | 하나은행 |\n"
            "| V2201S | VUL적립 성장주식형(삼성액티브주식) | 환매 / 신청 | 0 |  |  |  | 하나은행 |\n"
            "| V5203F | 변액연금Ⅱ 가치주식형Ⅱ(삼성액티브주식) | 입금 | 0 | -47,580,112 | -55,500,000 | -9,739,437 | 하나은행 |\n"
            "| V5203F | 변액연금Ⅱ 가치주식형Ⅱ(삼성액티브주식) | 출금 | -47,580,112 |  |  |  | 하나은행 |\n"
            "| V5203F | 변액연금Ⅱ 가치주식형Ⅱ(삼성액티브주식) | 환매 / 신청 | 0 |  |  |  | 하나은행 |\n"
        )
        raw_text = "[HTML hanhwa_20250826.html]\n기준일 : 20250826\n"
        base_dates = [
            FundBaseDateItem(
                fund_code="V2201S",
                fund_name="VUL적립 성장주식형(삼성액티브주식)",
                base_date="2025-08-26",
            ),
            FundBaseDateItem(
                fund_code="V5203F",
                fund_name="변액연금Ⅱ 가치주식형Ⅱ(삼성액티브주식)",
                base_date="2025-08-26",
            ),
        ]

        orders = self.extractor._build_markdown_shortcut_orders_after_base_date(
            base_dates=base_dates,
            markdown_text=markdown_text,
            raw_text=raw_text,
            target_fund_scope=None,
            expected_order_count=6,
        )

        self.assertEqual(
            sorted(
                (
                    order.fund_code,
                    order.t_day,
                    order.settle_class.value,
                    order.order_type.value,
                    order.transfer_amount,
                )
                for order in orders
            ),
            [
                ("V2201S", 0, "CONFIRMED", "SUB", "21,187,951"),
                ("V2201S", 1, "PENDING", "RED", "-36,400,000"),
                ("V2201S", 2, "PENDING", "RED", "-66,478,486"),
                ("V5203F", 0, "CONFIRMED", "RED", "-47,580,112"),
                ("V5203F", 1, "PENDING", "RED", "-55,500,000"),
                ("V5203F", 2, "PENDING", "RED", "-9,739,437"),
            ],
        )

    def test_build_deterministic_markdown_orders_assigns_future_buckets_by_column_order_not_calendar_diff(self) -> None:
        markdown_text = (
            "## HTML sample.html\n\n"
            "```text\n"
            "2026년 3월 20일\n"
            "예상내역\n"
            "```"
            "\n\n| 펀드코드 | 펀드명 | 설정금액 / 3월23일 | 해지금액 / 3월24일 |\n"
            "| --- | --- | --- | --- |\n"
            "| F001 | Alpha | 100 | 40 |\n"
        )
        raw_text = (
            "[HTML sample.html]\n"
            "2026년 3월 20일\n"
            "예상내역\n"
        )

        orders = self.extractor._build_deterministic_markdown_orders(
            markdown_text=markdown_text,
            raw_text=raw_text,
            target_fund_scope=None,
        )

        self.assertEqual(len(orders), 2)
        self.assertEqual(
            [(order.t_day, order.order_type.value, order.transfer_amount) for order in orders],
            [
                (1, "SUB", "100"),
                (2, "RED", "-40"),
            ],
        )

    def test_build_deterministic_markdown_orders_reads_signed_future_date_columns_without_direction_labels(self) -> None:
        markdown_text = (
            "## PDF 신한라이프\n\n"
            "```text\n"
            "기준일자 : 2025-11-27\n"
            "```"
            "\n\n| 통합펀드코드 | 서브펀드코드 | 펀드명 | 운용사 | 당일이체금액 | 2025-11-28 예정금액 | 2025-12-01 예정금액 |\n"
            "| --- | --- | --- | --- | --- | --- | --- |\n"
            "| V0043 |  | 글로벌멀티에셋자산배분형 | 삼성자산운용 | -8,375,740 | 451,845 | -1,858,420 |\n"
        )
        raw_text = (
            "[PAGE 1]\n"
            "기준일자 : 2025-11-27\n"
            "이체 예정금액\n"
        )

        orders = self.extractor._build_deterministic_markdown_orders(
            markdown_text=markdown_text,
            raw_text=raw_text,
            target_fund_scope=None,
        )

        self.assertEqual(
            [(order.t_day, order.order_type.value, order.transfer_amount) for order in orders],
            [
                (0, "RED", "-8,375,740"),
                (1, "SUB", "451,845"),
                (2, "RED", "-1,858,420"),
            ],
        )

    def test_build_deterministic_markdown_orders_prioritizes_net_columns_over_gross_columns(self) -> None:
        markdown_text = (
            "## SHEET ABL\n\n"
            "```text\n"
            "특별계정_VL & VUL 자금 운용 현황\n"
            "2025-08-26\n"
            "```\n\n"
            "| 운용지시펀드코드 | 펀드명 | T일 투입금액 | T일 인출금액 | T일 순유입금액 (확정) | "
            "T+3일 펀드변경 투입 | T+3일 펀드변경 인출 | T+3일 펀드변경 순투입 |\n"
            "| --- | --- | --- | --- | --- | --- | --- | --- |\n"
            "| C1005 | 글로벌리츠(VUL) | 920315 | 0 | 920315 | 23224219 | 1688 | 23222531 |\n"
        )
        raw_text = (
            "[SHEET ABL]\n"
            "특별계정_VL & VUL 자금 운용 현황\n"
            "2025-08-26\n"
        )

        orders = self.extractor._build_deterministic_markdown_orders(
            markdown_text=markdown_text,
            raw_text=raw_text,
            target_fund_scope=None,
        )

        self.assertEqual(
            [(order.t_day, order.order_type.value, order.transfer_amount) for order in orders],
            [
                (0, "SUB", "920,315"),
                (3, "SUB", "23,222,531"),
            ],
        )

    def test_build_deterministic_markdown_orders_supports_generic_fund_header_as_code(self) -> None:
        markdown_text = (
            "## SHEET KB\n\n"
            "```text\n"
            "Date (Asia/Seoul): 2025-08-26\n"
            "```\n\n"
            "| 펀드 | 펀드명 | 설정금액 | 해지금액 | 순투입금액 |\n"
            "| --- | --- | --- | --- | --- |\n"
            "| 6570452 | USPDI미국장기회사채권형_채권2 | 6620.93 | -57193.42 | -50572.49 |\n"
        )
        raw_text = "[SHEET KB]\nDate (Asia/Seoul): 2025-08-26\n"

        orders = self.extractor._build_deterministic_markdown_orders(
            markdown_text=markdown_text,
            raw_text=raw_text,
            target_fund_scope=None,
        )

        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0].fund_code, "6570452")
        self.assertEqual(orders[0].fund_name, "USPDI미국장기회사채권형_채권2")
        self.assertEqual(orders[0].base_date, "2025-08-26")
        self.assertEqual(orders[0].t_day, 0)
        self.assertEqual(orders[0].order_type.value, "RED")
        self.assertEqual(orders[0].transfer_amount, "-50,572.49")

    def test_build_deterministic_markdown_orders_supports_db_manager_code_header_as_fund_code(self) -> None:
        markdown_text = (
            "## SHEET DB\n\n"
            "```text\n"
            "2025-08-26\n"
            "```\n\n"
            "| 이체일자 | 상품명 | 펀드명 | 운용사코드 | 운용사명 | 유입금액 | 유출금액 | 증감금액 |\n"
            "| --- | --- | --- | --- | --- | --- | --- | --- |\n"
            "| 2025-08-26 | 변액종신 | 변액종신아시아혼합형 | D32160 | 삼성자산운용 | 112135 | 1324595 | -1212460 |\n"
        )
        raw_text = (
            "[SHEET DB]\n"
            "2025-08-26\n"
            "이체일자 | 상품명 | 펀드명 | 운용사코드 | 운용사명 | 유입금액 | 유출금액 | 증감금액\n"
        )

        orders = self.extractor._build_deterministic_markdown_orders(
            markdown_text=markdown_text,
            raw_text=raw_text,
            target_fund_scope=TargetFundScope(
                manager_column_present=True,
                include_all_funds=False,
                fund_codes=frozenset({"D32160"}),
                fund_names=frozenset({"변액종신아시아혼합형"}),
                canonical_fund_names=frozenset({"변액종신아시아혼합형"}),
            ),
        )

        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0].fund_code, "D32160")
        self.assertEqual(orders[0].order_type.value, "RED")
        self.assertEqual(orders[0].transfer_amount, "-1,212,460")

    def test_build_deterministic_markdown_orders_prefers_manager_code_over_generic_fund_header(self) -> None:
        markdown_text = (
            "## SHEET sample\n\n"
            "```text\n"
            "2025-08-26\n"
            "```\n\n"
            "| 상품 | 펀드 | 펀드명 | 운용사코드 | 유입금액 | 유출금액 | 증감금액 |\n"
            "| --- | --- | --- | --- | --- | --- | --- |\n"
            "| 변액종신 | ABC123 | 변액종신아시아혼합형 | D32160 | 112135 | 1324595 | -1212460 |\n"
        )
        raw_text = "[SHEET sample]\n2025-08-26\n"

        self.assertEqual(
            self.extractor._markdown_fund_code_index(
                ["상품", "펀드", "펀드명", "운용사코드", "유입금액", "유출금액", "증감금액"],
                body_rows=[
                    ["변액종신", "ABC123", "변액종신아시아혼합형", "D32160", "112135", "1324595", "-1212460"]
                ],
            ),
            3,
        )

        orders = self.extractor._build_deterministic_markdown_orders(
            markdown_text=markdown_text,
            raw_text=raw_text,
            target_fund_scope=TargetFundScope(
                manager_column_present=True,
                include_all_funds=False,
                fund_codes=frozenset({"D32160"}),
                fund_names=frozenset({"변액종신아시아혼합형"}),
                canonical_fund_names=frozenset({"변액종신아시아혼합형"}),
            ),
        )

        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0].fund_code, "D32160")
        self.assertEqual(orders[0].fund_name, "변액종신아시아혼합형")
        self.assertEqual(orders[0].order_type.value, "RED")
        self.assertEqual(orders[0].transfer_amount, "-1,212,460")

    def test_build_deterministic_markdown_orders_does_not_treat_non_code_manager_column_as_fund_code(self) -> None:
        markdown_text = (
            "## SHEET sample\n\n"
            "```text\n"
            "2025-08-26\n"
            "```\n\n"
            "| 이체일자 | 펀드명 | 운용사코드 | 운용사명 | 증감금액 |\n"
            "| --- | --- | --- | --- | --- |\n"
            "| 2025-08-26 | 변액종신아시아혼합형 | 삼성자산운용 | 삼성자산운용 | -1212460 |\n"
        )
        raw_text = "[SHEET sample]\n2025-08-26\n"

        orders = self.extractor._build_deterministic_markdown_orders(
            markdown_text=markdown_text,
            raw_text=raw_text,
            target_fund_scope=None,
        )

        self.assertEqual(orders, [])

    def test_build_deterministic_t_day_items_supports_signed_future_date_columns_without_direction_labels(self) -> None:
        document_text = "\n".join(
            [
                "## Page 1",
                "",
                "| 통합펀드코드 | 서브펀드코드 | 펀드명 | 운용사 | 당일이체금액 | 2025-11-28 예정금액 | 2025-12-01 예정금액 |",
                "| --- | --- | --- | --- | --- | --- | --- |",
                "| V0043 |  | 글로벌멀티에셋자산배분형 | 삼성자산운용 | -8,375,740 | 451,845 | -1,858,420 |",
            ]
        )
        input_items = [
            {
                "fund_code": "V0043",
                "fund_name": "글로벌멀티에셋자산배분형",
                "base_date": "2025-11-27",
            }
        ]

        items = self.extractor._build_deterministic_t_day_items(
            document_text=document_text,
            input_items=input_items,
        )

        self.assertEqual(
            [(item.t_day, item.slot_id, item.evidence_label) for item in items],
            [
                (0, "T0_NET", "당일이체금액"),
                (1, "T1_NET", "2025-11-28 예정금액"),
                (2, "T2_NET", "2025-12-01 예정금액"),
            ],
        )

    def test_augment_fund_seeds_from_document_keeps_lina_red_only_fund(self) -> None:
        scope = TargetFundScope(
            manager_column_present=True,
            include_all_funds=False,
            fund_codes=frozenset({"6114", "6115", "6118"}),
            fund_names=frozenset({"인덱스주식형", "삼성그룹주형", "동남아시아형"}),
            canonical_fund_names=frozenset({"인덱스주식형", "삼성그룹주형", "동남아시아형"}),
        )
        seeds = [FundSeedItem(fund_code="6114", fund_name="인덱스주식형")]
        document_text = "\n".join(
            [
                "| 운용사명 | 펀드코드 | 펀드명 | 설정금액 | 해지금액 |",
                "| --- | --- | --- | --- | --- |",
                "| 삼성자산 | 6114 | 인덱스주식형 | 1,106,000 | 52,671 |",
                "| 삼성자산 | 6115 | 삼성그룹주형 | 0 | 4,485 |",
                "| 삼성자산 | 6118 | 동남아시아형 | 0 | 0 |",
            ]
        )

        augmented = self.extractor._augment_fund_seeds_from_document(
            document_text=document_text,
            seeds=seeds,
            target_fund_scope=scope,
        )

        self.assertEqual(
            [(item.fund_code, item.fund_name) for item in augmented],
            [("6114", "인덱스주식형"), ("6115", "삼성그룹주형")],
        )

    def test_fund_inventory_retry_findings_respect_target_scope(self) -> None:
        scope = TargetFundScope(
            manager_column_present=True,
            include_all_funds=False,
            fund_codes=frozenset({"6114"}),
            fund_names=frozenset({"인덱스주식형"}),
            canonical_fund_names=frozenset({"인덱스주식형"}),
        )
        document_text = "\n".join(
            [
                "| 운용사명 | 펀드코드 | 펀드명 | 설정금액 | 해지금액 |",
                "| --- | --- | --- | --- | --- |",
                "| 삼성자산 | 6114 | 인덱스주식형 | 1,106,000 | 52,671 |",
                "| 타사운용 | 6999 | 타사혼합형 | 5,000 | 0 |",
            ]
        )
        parsed = FundSeedResult(
            items=[FundSeedItem(fund_code="6114", fund_name="인덱스주식형")],
            issues=[],
        )

        findings = self.extractor._derive_retry_findings_from_stage_items(
            stage_name="fund_inventory",
            document_text=document_text,
            parsed=parsed,
            target_fund_scope=scope,
        )

        self.assertEqual(findings, [])

    def test_derive_retry_focus_items_for_fund_inventory_uses_missing_document_seeds_in_scope(self) -> None:
        scope = TargetFundScope(
            manager_column_present=True,
            include_all_funds=False,
            fund_codes=frozenset({"6114", "6115"}),
            fund_names=frozenset({"인덱스주식형", "삼성그룹주형"}),
            canonical_fund_names=frozenset({"인덱스주식형", "삼성그룹주형"}),
        )
        document_text = "\n".join(
            [
                "| 운용사명 | 펀드코드 | 펀드명 | 설정금액 | 해지금액 |",
                "| --- | --- | --- | --- | --- |",
                "| 삼성자산 | 6114 | 인덱스주식형 | 1,106,000 | 52,671 |",
                "| 삼성자산 | 6115 | 삼성그룹주형 | 146,250 | 4,485 |",
                "| 타사운용 | 6999 | 타사혼합형 | 5,000 | 0 |",
            ]
        )

        focus_items = self.extractor._derive_retry_focus_items(
            stage_name="fund_inventory",
            document_text=document_text,
            input_items=None,
            previous_output_items=[{"fund_code": "6114", "fund_name": "인덱스주식형"}],
            retry_target_issues=[
                {
                    "raw_issue": "_RETRY_FUND_DISCOVERY_PARTIAL",
                    "issue_code": "_RETRY_FUND_DISCOVERY_PARTIAL",
                    "fund_code": None,
                    "fund_name": None,
                    "slot_id": None,
                    "evidence_label": None,
                    "stage_name": "fund_inventory",
                }
            ],
            target_fund_scope=scope,
        )

        self.assertEqual(
            focus_items,
            [{"fund_code": "6115", "fund_name": "삼성그룹주형"}],
        )

    def test_recover_result_from_structured_markdown_preserves_non_blocking_issues(self) -> None:
        task_payload = DocumentLoadTaskPayload(
            source_path="/tmp/sample.html",
            file_name="sample.html",
            pdf_password=None,
            content_type="text/html",
            raw_text="[HTML sample.html]\n2026년 3월 18일\n정산내역\n",
            markdown_text=(
                "## HTML sample.html\n\n"
                "```text\n2026년 3월 18일\n정산내역\n```\n\n"
                "| 펀드코드 | 펀드명 | 설정액 | 해지액 | 정산액 |\n"
                "| --- | --- | --- | --- | --- |\n"
                "| F001 | Alpha | 100 | -40 | 60 |\n"
            ),
            chunks=("dummy",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=1,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )
        extraction_result = ExtractionResult(
            orders=[],
            issues=["MANAGER_FILTER_APPLIED", "ORDER_COVERAGE_MISMATCH", "ORDER_TYPE_MISSING"],
        )

        recovered = self.extractor._recover_result_from_structured_markdown(
            task_payload=task_payload,
            extraction_result=extraction_result,
        )

        self.assertIsNotNone(recovered)
        assert recovered is not None
        self.assertEqual(len(recovered.orders), 1)
        self.assertEqual(recovered.issues, ["MANAGER_FILTER_APPLIED"])

    def test_recover_result_from_structured_markdown_removes_stale_base_date_missing_issue(self) -> None:
        task_payload = DocumentLoadTaskPayload(
            source_path="/tmp/sample.html",
            file_name="sample.html",
            pdf_password=None,
            content_type="text/html",
            raw_text="[HTML sample.html]\n2026년 3월 18일\n정산내역\n",
            markdown_text=(
                "## HTML sample.html\n\n"
                "```text\n2026년 3월 18일\n정산내역\n```\n\n"
                "| 펀드코드 | 펀드명 | 설정액 | 해지액 | 정산액 |\n"
                "| --- | --- | --- | --- | --- |\n"
                "| F001 | Alpha | 100 | -40 | 60 |\n"
            ),
            chunks=("dummy",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=1,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )
        extraction_result = ExtractionResult(
            orders=[],
            issues=["BASE_DATE_MISSING", "MANAGER_FILTER_APPLIED", "ORDER_COVERAGE_MISMATCH"],
        )

        recovered = self.extractor._recover_result_from_structured_markdown(
            task_payload=task_payload,
            extraction_result=extraction_result,
        )

        self.assertIsNotNone(recovered)
        assert recovered is not None
        self.assertEqual(len(recovered.orders), 1)
        self.assertEqual(recovered.orders[0].base_date, "2026-03-18")
        self.assertEqual(recovered.issues, ["MANAGER_FILTER_APPLIED"])

    def test_recover_result_from_structured_markdown_applies_same_count_content_fix(self) -> None:
        task_payload = DocumentLoadTaskPayload(
            source_path="/tmp/sample.pdf",
            file_name="sample.pdf",
            pdf_password=None,
            content_type="application/pdf",
            raw_text="raw text",
            markdown_text="markdown text",
            chunks=("dummy",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=2,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )
        extraction_result = ExtractionResult(
            orders=[
                OrderExtraction(
                    fund_code="ABC",
                    fund_name="Alpha",
                    settle_class="CONFIRMED",
                    order_type="SUB",
                    base_date="2025-11-27",
                    t_day=0,
                    transfer_amount="100",
                ),
                OrderExtraction(
                    fund_code="DEF",
                    fund_name="Beta",
                    settle_class="CONFIRMED",
                    order_type="SUB",
                    base_date="2025-11-27",
                    t_day=0,
                    transfer_amount="200",
                ),
            ],
            issues=["MANAGER_FILTER_APPLIED", "ORDER_COVERAGE_MISMATCH"],
        )

        self.extractor._build_deterministic_markdown_orders = lambda **_: [
            OrderExtraction(
                fund_code="ABC",
                fund_name="Alpha",
                settle_class="CONFIRMED",
                order_type="RED",
                base_date="2025-11-27",
                t_day=0,
                transfer_amount="-100",
            ),
            OrderExtraction(
                fund_code="DEF",
                fund_name="Beta",
                settle_class="CONFIRMED",
                order_type="SUB",
                base_date="2025-11-27",
                t_day=0,
                transfer_amount="200",
            ),
        ]

        recovered = self.extractor._recover_result_from_structured_markdown(
            task_payload=task_payload,
            extraction_result=extraction_result,
        )

        self.assertIsNotNone(recovered)
        assert recovered is not None
        self.assertEqual(recovered.orders[0].order_type.value, "RED")
        self.assertEqual(recovered.orders[0].transfer_amount, "-100")
        self.assertEqual(recovered.issues, ["MANAGER_FILTER_APPLIED"])

    def test_recover_result_from_structured_markdown_accepts_expected_count_fix_even_when_recovered_is_smaller(self) -> None:
        task_payload = DocumentLoadTaskPayload(
            source_path="/tmp/sample.xlsx",
            file_name="sample.xlsx",
            pdf_password=None,
            content_type="application/vnd.ms-excel",
            raw_text="raw text",
            markdown_text="markdown text",
            chunks=("dummy",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=1,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )
        extraction_result = ExtractionResult(
            orders=[
                OrderExtraction(
                    fund_code="D32160",
                    fund_name="변액종신아시아혼합형",
                    settle_class="CONFIRMED",
                    order_type="SUB",
                    base_date="2025-08-26",
                    t_day=0,
                    transfer_amount="112135",
                ),
                OrderExtraction(
                    fund_code="D32160",
                    fund_name="변액종신아시아혼합형",
                    settle_class="CONFIRMED",
                    order_type="RED",
                    base_date="2025-08-26",
                    t_day=0,
                    transfer_amount="-1324595",
                ),
            ],
            issues=["ORDER_COVERAGE_MISMATCH"],
        )

        self.extractor._build_deterministic_markdown_orders = lambda **_: [
            OrderExtraction(
                fund_code="D32160",
                fund_name="변액종신아시아혼합형",
                settle_class="CONFIRMED",
                order_type="RED",
                base_date="2025-08-26",
                t_day=0,
                transfer_amount="-1212460",
            )
        ]

        recovered = self.extractor._recover_result_from_structured_markdown(
            task_payload=task_payload,
            extraction_result=extraction_result,
        )

        self.assertIsNotNone(recovered)
        assert recovered is not None
        self.assertEqual(len(recovered.orders), 1)
        self.assertEqual(recovered.orders[0].transfer_amount, "-1212460")
        self.assertEqual(recovered.issues, [])

    def test_recover_result_from_structured_markdown_rejects_expected_count_fix_when_families_do_not_match(self) -> None:
        task_payload = DocumentLoadTaskPayload(
            source_path="/tmp/sample.xlsx",
            file_name="sample.xlsx",
            pdf_password=None,
            content_type="application/vnd.ms-excel",
            raw_text="raw text",
            markdown_text="markdown text",
            chunks=("dummy",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=1,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )
        extraction_result = ExtractionResult(
            orders=[
                OrderExtraction(
                    fund_code="AAA",
                    fund_name="Alpha",
                    settle_class="CONFIRMED",
                    order_type="SUB",
                    base_date="2025-08-26",
                    t_day=0,
                    transfer_amount="100",
                ),
                OrderExtraction(
                    fund_code="AAA",
                    fund_name="Alpha",
                    settle_class="CONFIRMED",
                    order_type="RED",
                    base_date="2025-08-26",
                    t_day=0,
                    transfer_amount="-40",
                ),
            ],
            issues=["ORDER_COVERAGE_MISMATCH"],
        )

        self.extractor._build_deterministic_markdown_orders = lambda **_: [
            OrderExtraction(
                fund_code="BBB",
                fund_name="Beta",
                settle_class="CONFIRMED",
                order_type="SUB",
                base_date="2025-08-26",
                t_day=0,
                transfer_amount="60",
            )
        ]

        recovered = self.extractor._recover_result_from_structured_markdown(
            task_payload=task_payload,
            extraction_result=extraction_result,
        )

        self.assertIsNone(recovered)

    def test_recover_result_from_structured_markdown_accepts_document_backed_single_family_replacement(self) -> None:
        task_payload = DocumentLoadTaskPayload(
            source_path="/tmp/sample.xlsx",
            file_name="sample.xlsx",
            pdf_password=None,
            content_type="application/vnd.ms-excel",
            raw_text=(
                "[SHEET sample]\n"
                "펀드코드 | 펀드명 | 증감금액\n"
                "AAA | Alpha | 100\n"
                "BBB | Beta | 200\n"
            ),
            markdown_text="markdown text",
            chunks=("dummy",),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=2,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )
        extraction_result = ExtractionResult(
            orders=[
                OrderExtraction(
                    fund_code="AAA",
                    fund_name="Alpha",
                    settle_class="CONFIRMED",
                    order_type="SUB",
                    base_date="2025-08-26",
                    t_day=0,
                    transfer_amount="100",
                ),
                OrderExtraction(
                    fund_code="CCC",
                    fund_name="Gamma",
                    settle_class="CONFIRMED",
                    order_type="SUB",
                    base_date="2025-08-26",
                    t_day=0,
                    transfer_amount="200",
                ),
            ],
            issues=["ORDER_COVERAGE_MISMATCH"],
        )

        self.extractor._build_deterministic_markdown_orders = lambda **_: [
            OrderExtraction(
                fund_code="AAA",
                fund_name="Alpha",
                settle_class="CONFIRMED",
                order_type="SUB",
                base_date="2025-08-26",
                t_day=0,
                transfer_amount="100",
            ),
            OrderExtraction(
                fund_code="BBB",
                fund_name="Beta",
                settle_class="CONFIRMED",
                order_type="SUB",
                base_date="2025-08-26",
                t_day=0,
                transfer_amount="200",
            ),
        ]

        recovered = self.extractor._recover_result_from_structured_markdown(
            task_payload=task_payload,
            extraction_result=extraction_result,
        )

        self.assertIsNotNone(recovered)
        assert recovered is not None
        self.assertEqual([order.fund_code for order in recovered.orders], ["AAA", "BBB"])

    def test_recover_result_from_structured_markdown_replaces_kb_truncated_decimal_with_document_amount(self) -> None:
        markdown_text = (
            "## SHEET KB\n\n"
            "```text\n"
            "Date (Asia/Seoul): 2025-08-26\n"
            "```\n\n"
            "| 펀드 | 펀드명 | 설정금액 | 해지금액 | 순투입금액 |\n"
            "| --- | --- | --- | --- | --- |\n"
            "| 6570452 | USPDI미국장기회사채권형_채권2 | 6620.93 | -57193.42 | -50572.49 |\n"
        )
        raw_text = "[SHEET KB]\nDate (Asia/Seoul): 2025-08-26\n"
        task_payload = DocumentLoadTaskPayload(
            source_path="/tmp/KB라이프_250826.XLS",
            file_name="KB라이프_250826.XLS",
            pdf_password=None,
            content_type="application/vnd.ms-excel",
            raw_text=raw_text,
            markdown_text=markdown_text,
            chunks=(markdown_text,),
            non_instruction_reason=None,
            allow_empty_result=False,
            scope_excludes_all_funds=False,
            expected_order_count=1,
            target_fund_scope=TargetFundScope(manager_column_present=False),
        )
        extraction_result = ExtractionResult(
            orders=[
                OrderExtraction(
                    fund_code="6570452",
                    fund_name="USPDI미국장기회사채권형_채권2",
                    settle_class="CONFIRMED",
                    order_type="SUB",
                    base_date="2025-08-26",
                    t_day=0,
                    transfer_amount="50,572",
                )
            ],
            issues=[],
        )

        recovered = self.extractor._recover_result_from_structured_markdown(
            task_payload=task_payload,
            extraction_result=extraction_result,
        )

        self.assertIsNotNone(recovered)
        assert recovered is not None
        self.assertEqual(len(recovered.orders), 1)
        self.assertEqual(recovered.orders[0].order_type.value, "RED")
        self.assertEqual(recovered.orders[0].transfer_amount, "-50,572.49")

    def test_detect_base_date_from_text_supports_compact_labeled_date(self) -> None:
        detected = self.extractor._detect_base_date_from_text("[HTML sample.html]\n기준일 : 20251128\n정산내역\n")

        self.assertEqual(detected, "2025-11-28")

    def test_detect_base_date_from_text_supports_eml_asia_seoul_header(self) -> None:
        detected = self.extractor._detect_base_date_from_text(
            "Date (Asia/Seoul): 2026-04-13\nSubject: 흥국생명 설정해지 내역 운용지시건\n"
        )

        self.assertEqual(detected, "2026-04-13")

    def test_detect_title_adjacent_base_date_from_text_prefers_document_header_date(self) -> None:
        raw_text = (
            "[SHEET 호스트]\n"
            "특별계정_VL & VUL 자금 운용 현황 |  |  |\n"
            "2025-08-26 |  |  |\n"
            "운용지시펀드코드 | 펀드코드_NG&S | 펀드명 | 수탁사 | 2025-08-25 | T일 (영업일 기준)\n"
            "펀드코드_KASS | 펀드코드_NG&S | 펀드명 | 수탁사 | T-1일(영업일기준) NAV | T일 투입금액\n"
        )

        detected = self.extractor._detect_title_adjacent_base_date_from_text(raw_text)

        self.assertEqual(detected, "2025-08-26")

    def test_detect_base_date_from_text_supports_cardif_short_english_header_context(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "The order of Subscription and Redemption\n"
            "To : Samsung AMC Date :\n"
            "Fax : 3788-8989 27-Nov-25\n"
            "From : Jung Min WOO\n"
            "1. Subscription\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Future Mobility Active ETF FoF FME 20,751,062.2603 27-Nov-25 1,032.61 21,427,754 HANA\n"
        )

        detected = self.extractor._detect_base_date_from_text(raw_text)

        self.assertEqual(detected, "2025-11-27")

    def test_detect_base_date_from_text_ignores_short_english_nav_row_without_header_context(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "1. Subscription\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Future Mobility Active ETF FoF FME 20,751,062.2603 27-Nov-25 1,032.61 21,427,754 HANA\n"
        )

        detected = self.extractor._detect_base_date_from_text(raw_text)

        self.assertIsNone(detected)

    def test_detect_document_base_date_from_text_prefers_settlement_date_over_closing_date(self) -> None:
        raw_text = (
            "[SHEET 라이나]\n"
            "Closing Date : 2025-08-25\n"
            "Prior Date : 2025-08-25\n"
            "결제일 : 2025-08-26\n"
            "펀드코드 | 펀드명 | 설정금액\n"
        )

        detected = self.extractor._detect_document_base_date_from_text(raw_text)

        self.assertEqual(detected, "2025-08-26")

    def test_detect_document_base_date_from_text_prefers_uniform_section_nav_date_over_document_date(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "Document Date: 2025-11-28\n"
            "The order of Subscription and Redemption\n"
            "1. Subscription\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Future Mobility Active ETF FoF FME 20,751,062.2603 27-Nov-25 1,032.61 21,427,754 HANA\n"
            "Global Bond FoF II GBE 1,937,022.7838 27-Nov-25 1,249.43 2,420,174 HANA\n"
            "2. Redemption\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Bond ETF FoF BEE 185,714,727.6942 27-Nov-25 1,120.91 208,169,495 SCFB\n"
        )

        detected = self.extractor._detect_document_base_date_from_text(raw_text)

        self.assertEqual(detected, "2025-11-27")

    def test_detect_document_base_date_from_text_returns_none_when_section_nav_dates_conflict_with_document_date(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "Document Date: 2025-11-29\n"
            "The order of Subscription and Redemption\n"
            "1. Subscription\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Future Mobility Active ETF FoF FME 20,751,062.2603 27-Nov-25 1,032.61 21,427,754 HANA\n"
            "2. Redemption\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Bond ETF FoF BEE 185,714,727.6942 28-Nov-25 1,120.91 208,169,495 SCFB\n"
        )

        detected = self.extractor._detect_document_base_date_from_text(raw_text)

        self.assertIsNone(detected)

    def test_detect_document_base_date_from_text_prefers_uniform_transaction_row_date_over_asia_seoul_header(self) -> None:
        raw_text = (
            "[EML 삼성액티브자산운용_설정_해지_20251128.eml]\n"
            "Subject: [2025/11/18] 삼성액티브자산운용 설정 해지-2\n"
            "Date: Mon, 16 Mar 2026 06:43:45 +0000\n"
            "Date (Asia/Seoul): 2026-03-16\n"
            "BUY & SELL REPORT |  |  |  |  |  |  |  |  |  |\n"
            "Date | Buy&Sell | External Fund Manager | Fund Code | Fund Name | Fund Price | Buy |  | Sell |  | Custodian Bank\n"
            " |  |  |  |  |  | Amount | Unit | Amount | Unit |\n"
            "11-28-2025 | Buy&Sell | 삼성액티브 | 151128 | AIA VUL 주식형(1형)(삼성) | 1785.89 | 1,311,285 |  | 55,901,379 |  | \n"
            " |  | 삼성액티브 | 151161 | AIA VUL 장기성장주식형 (1형) - 1호펀드 (삼성) | 2588.67 | 3,597,188 |  | 9,770,019 |  | \n"
            "Total |  |  |  |  |  | 4,908,473 |  | 65,671,398 |  |\n"
        )

        detected = self.extractor._detect_document_base_date_from_text(raw_text)

        self.assertEqual(detected, "2025-11-28")

    def test_detect_document_base_date_from_text_returns_none_when_transaction_row_dates_conflict_with_asia_seoul(self) -> None:
        raw_text = (
            "[EML sample.eml]\n"
            "Date (Asia/Seoul): 2026-03-16\n"
            "BUY & SELL REPORT |  |  |  |  |  |  |  |  |  |\n"
            "Date | Buy&Sell | External Fund Manager | Fund Code | Fund Name | Fund Price | Buy |  | Sell |  | Custodian Bank\n"
            "11-28-2025 | Buy&Sell | 삼성액티브 | 151128 | AIA VUL 주식형(1형)(삼성) | 1785.89 | 1,311,285 |  | 55,901,379 |  | \n"
            "11-29-2025 | Buy&Sell | 삼성액티브 | 151161 | AIA VUL 장기성장주식형 (1형) - 1호펀드 (삼성) | 2588.67 | 3,597,188 |  | 9,770,019 |  | \n"
        )

        detected = self.extractor._detect_document_base_date_from_text(raw_text)

        self.assertIsNone(detected)

    def test_drop_zero_amount_stage_items_prunes_zero_slots_before_later_stage(self) -> None:
        items = [
            FundAmountItem(
                fund_code="32451",
                fund_name="삼성베스트혼합형",
                base_date="2025-11-28",
                t_day=2,
                slot_id="32451-2",
                evidence_label="익익영업일(T+2) / 이체예상금액",
                transfer_amount="0",
            ),
            FundAmountItem(
                fund_code="32451",
                fund_name="삼성베스트혼합형",
                base_date="2025-11-28",
                t_day=1,
                slot_id="32451-1",
                evidence_label="익영업일(T+1) / 이체예상금액",
                transfer_amount="500,063",
            ),
        ]

        filtered = self.extractor._drop_zero_amount_stage_items(items, stage_name="settle_class")

        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0].slot_id, "32451-1")

    def test_build_deterministic_markdown_orders_supports_hanwha_hybrid_headers(self) -> None:
        raw_text = "[HTML hanhwa.html]\n기준일 : 20251128\n"
        markdown_text = (
            "| 펀드코드 | 펀드명 | 설정(예탁) 및 해지금액 / 투자일임/수익증권 | 설정(예탁) 및 해지금액 / 펀드계 | "
            "익영업일(T+1) / 이체예상금액 | 익익영업일(T+2) / 이체예상금액 | 수탁은행 / 환매사 |\n"
            "| --- | --- | --- | --- | --- | --- | --- |\n"
            "| 32451 | 삼성베스트혼합형 | 입금 | 500,063 | 500,063 | 0 | 하나은행 |\n"
            "| V63053 | 삼성중소형FOCUS40증권투자신탁K-1호(주식혼합)C-F | 출금 | -85,848,214 | -85,848,214 | 0 | 하나은행 |\n"
        )

        orders = self.extractor._build_deterministic_markdown_orders(
            markdown_text=markdown_text,
            raw_text=raw_text,
            target_fund_scope=None,
        )

        self.assertEqual(len(orders), 4)
        observed = {
            (order.fund_code, order.t_day, order.order_type.value, order.transfer_amount)
            for order in orders
        }
        self.assertEqual(
            observed,
            {
                ("32451", 0, "SUB", "500,063"),
                ("32451", 1, "SUB", "500,063"),
                ("V63053", 0, "RED", "-85,848,214"),
                ("V63053", 1, "RED", "-85,848,214"),
            },
        )
        self.assertTrue(all(order.base_date == "2025-11-28" for order in orders))

    def test_build_deterministic_markdown_orders_supports_cardif_sectioned_raw_text(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "The order of Subscription and Redemption\n"
            "Date : 27-Nov-25\n"
            "1. Subscription\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Future Mobility Active ETF FoF FME 20,751,062.2603 27-Nov-25 1,032.61 21,427,754 HANA\n"
            "Global Bond FoF II GBE 1,937,022.7838 27-Nov-25 1,249.43 2,420,174 HANA\n"
            "2. Redemption\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Bond ETF FoF BEE 185,714,727.6942 27-Nov-25 1,120.91 208,169,495 SCFB\n"
            "Future Mobility Active ETF FoF FME - 27-Nov-25 1,032.61 - SCFB\n"
            "Global Bond FoF II GBE - 27-Nov-25 1,249.43 - SCFB\n"
        )

        orders = self.extractor._build_deterministic_markdown_orders(
            markdown_text="## PDF cardif.pdf\n\n```text\nno pipe table\n```",
            raw_text=raw_text,
            target_fund_scope=None,
        )

        self.assertEqual(len(orders), 3)
        observed = {
            (order.fund_code, order.order_type.value, order.settle_class.value, order.t_day, order.transfer_amount)
            for order in orders
        }
        self.assertEqual(
            observed,
            {
                ("FME", "SUB", "CONFIRMED", 0, "21,427,754"),
                ("GBE", "SUB", "CONFIRMED", 0, "2,420,174"),
                ("BEE", "RED", "CONFIRMED", 0, "-208,169,495"),
            },
        )
        self.assertTrue(all(order.base_date == "2025-11-27" for order in orders))

    def test_build_deterministic_markdown_orders_prefers_more_complete_section_recovery(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "Document Date: 2025-11-28\n"
            "The order of Subscription and Redemption\n"
            "1. Subscription\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Future Mobility Active ETF FoF FME 20,751,062.2603 27-Nov-25 1,032.61 21,427,754 HANA\n"
            "Global Bond FoF II GBE 1,937,022.7838 27-Nov-25 1,249.43 2,420,174 HANA\n"
            "2. Redemption\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Bond ETF FoF BEE 185,714,727.6942 27-Nov-25 1,120.91 208,169,495 SCFB\n"
        )
        markdown_text = (
            "| Fund Code | Fund Name | Subscription |\n"
            "| --- | --- | --- |\n"
            "| FME | Future Mobility Active ETF FoF | 21,427,754 |\n"
        )

        orders = self.extractor._build_deterministic_markdown_orders(
            markdown_text=markdown_text,
            raw_text=raw_text,
            target_fund_scope=None,
        )

        self.assertEqual(len(orders), 3)
        observed = {
            (order.fund_code, order.order_type.value, order.settle_class.value, order.t_day, order.transfer_amount)
            for order in orders
        }
        self.assertEqual(
            observed,
            {
                ("FME", "SUB", "CONFIRMED", 0, "21,427,754"),
                ("GBE", "SUB", "CONFIRMED", 0, "2,420,174"),
                ("BEE", "RED", "CONFIRMED", 0, "-208,169,495"),
            },
        )
        # 문서 상단 Document Date가 있더라도 section row의 NAV Date를 우선 사용해야 한다.
        self.assertTrue(all(order.base_date == "2025-11-27" for order in orders))

    def test_build_structure_shortcut_orders_after_base_date_supports_sectioned_same_day_document(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "The order of Subscription and Redemption\n"
            "Date : 27-Nov-25\n"
            "1. Subscription\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Future Mobility Active ETF FoF FME 20,751,062.2603 27-Nov-25 1,032.61 21,427,754 HANA\n"
            "Global Bond FoF II GBE 1,937,022.7838 27-Nov-25 1,249.43 2,420,174 HANA\n"
            "2. Redemption\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Bond ETF FoF BEE 185,714,727.6942 27-Nov-25 1,120.91 208,169,495 SCFB\n"
        )
        base_dates = [
            FundBaseDateItem(fund_code="FME", fund_name="Future Mobility Active ETF FoF", base_date="2025-11-27"),
            FundBaseDateItem(fund_code="GBE", fund_name="Global Bond FoF II", base_date="2025-11-27"),
            FundBaseDateItem(fund_code="BEE", fund_name="Bond ETF FoF", base_date="2025-11-27"),
        ]

        orders = self.extractor._build_structure_shortcut_orders_after_base_date(
            base_dates=base_dates,
            raw_text=raw_text,
            target_fund_scope=None,
            expected_order_count=3,
        )

        observed = {
            (order.fund_code, order.order_type.value, order.settle_class.value, order.t_day, order.transfer_amount)
            for order in orders
        }
        self.assertEqual(
            observed,
            {
                ("FME", "SUB", "CONFIRMED", 0, "21,427,754"),
                ("GBE", "SUB", "CONFIRMED", 0, "2,420,174"),
                ("BEE", "RED", "CONFIRMED", 0, "-208,169,495"),
            },
        )

    def test_build_structure_shortcut_orders_after_base_date_requires_expected_count(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "The order of Subscription and Redemption\n"
            "Date : 27-Nov-25\n"
            "1. Subscription\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Future Mobility Active ETF FoF FME 20,751,062.2603 27-Nov-25 1,032.61 21,427,754 HANA\n"
        )
        base_dates = [
            FundBaseDateItem(fund_code="FME", fund_name="Future Mobility Active ETF FoF", base_date="2025-11-27"),
        ]

        orders = self.extractor._build_structure_shortcut_orders_after_base_date(
            base_dates=base_dates,
            raw_text=raw_text,
            target_fund_scope=None,
            expected_order_count=None,
        )

        self.assertEqual(orders, [])

    def test_build_structure_shortcut_orders_after_base_date_skips_count_mismatch(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "The order of Subscription and Redemption\n"
            "Date : 27-Nov-25\n"
            "1. Subscription\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Future Mobility Active ETF FoF FME 20,751,062.2603 27-Nov-25 1,032.61 21,427,754 HANA\n"
        )
        base_dates = [
            FundBaseDateItem(fund_code="FME", fund_name="Future Mobility Active ETF FoF", base_date="2025-11-27"),
            FundBaseDateItem(fund_code="GBE", fund_name="Global Bond FoF II", base_date="2025-11-27"),
        ]

        orders = self.extractor._build_structure_shortcut_orders_after_base_date(
            base_dates=base_dates,
            raw_text=raw_text,
            target_fund_scope=None,
            expected_order_count=2,
        )

        self.assertEqual(orders, [])

    def test_resolve_document_base_date_for_seeds_fans_out_deterministic_date(self) -> None:
        prompt_bundle = _load_prompt_bundle()
        base_date_stage = prompt_bundle.stages["base_date"]
        seeds = [
            FundSeedItem(fund_code="450038", fund_name="-"),
            FundSeedItem(fund_code="450036", fund_name="-"),
        ]

        base_dates = self.extractor._resolve_document_base_date_for_seeds(
            prompt_bundle=prompt_bundle,
            base_date_stage=base_date_stage,
            seeds=seeds,
            document_text="sample doc",
            raw_text="Date (Asia/Seoul): 2026-04-13\nSubject: 흥국생명 설정해지 내역 운용지시건\n",
            artifacts=[],
            counterparty_guidance=None,
        )

        self.assertIsNotNone(base_dates)
        assert base_dates is not None
        self.assertEqual(
            [(item.fund_code, item.base_date) for item in base_dates],
            [("450038", "2026-04-13"), ("450036", "2026-04-13")],
        )

    def test_resolve_document_base_date_for_seeds_uses_one_shot_llm_when_deterministic_is_unavailable(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        prompt_bundle = _load_prompt_bundle()
        base_date_stage = prompt_bundle.stages["base_date"]
        seeds = [
            FundSeedItem(fund_code="F001", fund_name="Alpha"),
            FundSeedItem(fund_code="F002", fund_name="Beta"),
        ]
        test_case = self

        def fake_invoke_stage_with_issue_retry(
            self,
            *,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            batch_index,
            response_model,
            counterparty_guidance=None,
        ):
            test_case.assertEqual(stage.name, "base_date")
            test_case.assertEqual(len(input_items), 1)
            return (
                extractor_module._StageInvocation(
                    parsed=FundBaseDateResult(
                        items=[FundBaseDateItem(fund_code="F001", fund_name="Alpha", base_date="2025-11-27")],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27"}],"issues":[]}',
                ),
                [],
                None,
            )

        extractor._invoke_stage_with_issue_retry = types.MethodType(fake_invoke_stage_with_issue_retry, extractor)

        base_dates = extractor._resolve_document_base_date_for_seeds(
            prompt_bundle=prompt_bundle,
            base_date_stage=base_date_stage,
            seeds=seeds,
            document_text="document without explicit date header",
            raw_text="document without explicit date header",
            artifacts=[],
            counterparty_guidance=None,
        )

        self.assertIsNotNone(base_dates)
        assert base_dates is not None
        self.assertEqual(
            [(item.fund_code, item.base_date) for item in base_dates],
            [("F001", "2025-11-27"), ("F002", "2025-11-27")],
        )

    def test_resolve_document_base_date_for_seeds_returns_none_when_one_shot_llm_conflicts(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        prompt_bundle = _load_prompt_bundle()
        base_date_stage = prompt_bundle.stages["base_date"]
        seeds = [
            FundSeedItem(fund_code="F001", fund_name="Alpha"),
            FundSeedItem(fund_code="F002", fund_name="Beta"),
        ]

        def fake_invoke_stage_with_issue_retry(
            self,
            *,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            batch_index,
            response_model,
            counterparty_guidance=None,
        ):
            return (
                extractor_module._StageInvocation(
                    parsed=FundBaseDateResult(
                        items=[
                            FundBaseDateItem(fund_code="F001", fund_name="Alpha", base_date="2025-11-27"),
                            FundBaseDateItem(fund_code="F001", fund_name="Alpha", base_date="2025-11-28"),
                        ],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27"},{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-28"}],"issues":[]}',
                ),
                [],
                None,
            )

        extractor._invoke_stage_with_issue_retry = types.MethodType(fake_invoke_stage_with_issue_retry, extractor)

        base_dates = extractor._resolve_document_base_date_for_seeds(
            prompt_bundle=prompt_bundle,
            base_date_stage=base_date_stage,
            seeds=seeds,
            document_text="document without explicit date header",
            raw_text="document without explicit date header",
            artifacts=[],
            counterparty_guidance=None,
        )

        self.assertIsNone(base_dates)

    def test_resolve_document_base_date_for_seeds_prefers_uniform_section_nav_date_over_document_date(self) -> None:
        prompt_bundle = _load_prompt_bundle()
        base_date_stage = prompt_bundle.stages["base_date"]
        raw_text = (
            "[PAGE 1]\n"
            "Document Date: 2025-11-28\n"
            "The order of Subscription and Redemption\n"
            "1. Subscription\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Future Mobility Active ETF FoF FME 20,751,062.2603 27-Nov-25 1,032.61 21,427,754 HANA\n"
            "Global Bond FoF II GBE 1,937,022.7838 27-Nov-25 1,249.43 2,420,174 HANA\n"
            "2. Redemption\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Bond ETF FoF BEE 185,714,727.6942 27-Nov-25 1,120.91 208,169,495 SCFB\n"
        )
        seeds = [
            FundSeedItem(fund_code="FME", fund_name="Future Mobility Active ETF FoF"),
            FundSeedItem(fund_code="GBE", fund_name="Global Bond FoF II"),
            FundSeedItem(fund_code="BEE", fund_name="Bond ETF FoF"),
        ]

        base_dates = self.extractor._resolve_document_base_date_for_seeds(
            prompt_bundle=prompt_bundle,
            base_date_stage=base_date_stage,
            seeds=seeds,
            document_text="sample doc",
            raw_text=raw_text,
            artifacts=[],
            counterparty_guidance=None,
        )

        self.assertIsNotNone(base_dates)
        assert base_dates is not None
        self.assertEqual(
            [(item.fund_code, item.base_date) for item in base_dates],
            [("FME", "2025-11-27"), ("GBE", "2025-11-27"), ("BEE", "2025-11-27")],
        )

    def test_resolve_document_base_date_for_seeds_prefers_uniform_transaction_row_date_over_asia_seoul(self) -> None:
        prompt_bundle = _load_prompt_bundle()
        base_date_stage = prompt_bundle.stages["base_date"]
        raw_text = (
            "[EML 삼성액티브자산운용_설정_해지_20251128.eml]\n"
            "Subject: [2025/11/18] 삼성액티브자산운용 설정 해지-2\n"
            "Date: Mon, 16 Mar 2026 06:43:45 +0000\n"
            "Date (Asia/Seoul): 2026-03-16\n"
            "BUY & SELL REPORT |  |  |  |  |  |  |  |  |  |\n"
            "Date | Buy&Sell | External Fund Manager | Fund Code | Fund Name | Fund Price | Buy |  | Sell |  | Custodian Bank\n"
            " |  |  |  |  |  | Amount | Unit | Amount | Unit |\n"
            "11-28-2025 | Buy&Sell | 삼성액티브 | 151128 | AIA VUL 주식형(1형)(삼성) | 1785.89 | 1,311,285 |  | 55,901,379 |  | \n"
            " |  | 삼성액티브 | 151161 | AIA VUL 장기성장주식형 (1형) - 1호펀드 (삼성) | 2588.67 | 3,597,188 |  | 9,770,019 |  | \n"
        )
        seeds = [
            FundSeedItem(fund_code="151128", fund_name="AIA VUL 주식형(1형)(삼성)"),
            FundSeedItem(fund_code="151161", fund_name="AIA VUL 장기성장주식형 (1형) - 1호펀드 (삼성)"),
        ]
        extractor = object.__new__(FundOrderExtractor)
        extractor._resolve_document_base_date_via_one_shot_llm = Mock(side_effect=AssertionError("one-shot llm should not run"))

        base_dates = extractor._resolve_document_base_date_for_seeds(
            prompt_bundle=prompt_bundle,
            base_date_stage=base_date_stage,
            seeds=seeds,
            document_text="sample doc",
            raw_text=raw_text,
            artifacts=[],
            counterparty_guidance=None,
        )

        self.assertIsNotNone(base_dates)
        assert base_dates is not None
        self.assertEqual(
            [(item.fund_code, item.base_date) for item in base_dates],
            [("151128", "2025-11-28"), ("151161", "2025-11-28")],
        )

    def test_resolve_document_base_date_for_seeds_uses_one_shot_llm_when_section_nav_dates_conflict(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        prompt_bundle = _load_prompt_bundle()
        base_date_stage = prompt_bundle.stages["base_date"]
        raw_text = (
            "[PAGE 1]\n"
            "Document Date: 2025-11-29\n"
            "The order of Subscription and Redemption\n"
            "1. Subscription\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Future Mobility Active ETF FoF FME 20,751,062.2603 27-Nov-25 1,032.61 21,427,754 HANA\n"
            "2. Redemption\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Bond ETF FoF BEE 185,714,727.6942 28-Nov-25 1,120.91 208,169,495 SCFB\n"
        )
        seeds = [
            FundSeedItem(fund_code="FME", fund_name="Future Mobility Active ETF FoF"),
            FundSeedItem(fund_code="BEE", fund_name="Bond ETF FoF"),
        ]

        def fake_invoke_stage_with_issue_retry(
            self,
            *,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            batch_index,
            response_model,
            counterparty_guidance=None,
        ):
            return (
                extractor_module._StageInvocation(
                    parsed=FundBaseDateResult(
                        items=[FundBaseDateItem(fund_code="FME", fund_name="Future Mobility Active ETF FoF", base_date="2025-11-27")],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"FME","fund_name":"Future Mobility Active ETF FoF","base_date":"2025-11-27"}],"issues":[]}',
                ),
                [],
                None,
            )

        extractor._invoke_stage_with_issue_retry = types.MethodType(fake_invoke_stage_with_issue_retry, extractor)

        base_dates = extractor._resolve_document_base_date_for_seeds(
            prompt_bundle=prompt_bundle,
            base_date_stage=base_date_stage,
            seeds=seeds,
            document_text="document without explicit deterministic date",
            raw_text=raw_text,
            artifacts=[],
            counterparty_guidance=None,
        )

        self.assertIsNotNone(base_dates)
        assert base_dates is not None
        self.assertEqual(
            [(item.fund_code, item.base_date) for item in base_dates],
            [("FME", "2025-11-27"), ("BEE", "2025-11-27")],
        )

    def test_extract_without_expected_order_count_does_not_use_structure_shortcut(self) -> None:
        document_text = (
            "The order of Subscription and Redemption\n"
            "1. Subscription\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Future Mobility Active ETF FoF FME 20,751,062.2603 27-Nov-25 1,032.61 21,427,754 HANA\n"
        )
        raw_text = (
            "[PAGE 1]\n"
            "The order of Subscription and Redemption\n"
            "Date : 27-Nov-25\n"
            "1. Subscription\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Future Mobility Active ETF FoF FME 20,751,062.2603 27-Nov-25 1,032.61 21,427,754 HANA\n"
        )

        extractor = object.__new__(FundOrderExtractor)
        extractor.prompt_bundle = _load_prompt_bundle()
        extractor.system_prompt = extractor.prompt_bundle.system_prompt
        extractor.stage_batch_size = 10
        extractor.llm_chunk_size_chars = 1200
        extractor._refresh_prompt_bundle = types.MethodType(lambda self, force=False: self.prompt_bundle, extractor)
        extractor._classify_instruction_document = types.MethodType(
            lambda self, prompt_bundle, document_text, artifacts, *, counterparty_guidance=None: (True, None),
            extractor,
        )
        extractor._extract_fund_seeds = types.MethodType(
            lambda self, prompt_bundle, chunks, raw_text, issues, artifacts, *, target_fund_scope=None, counterparty_guidance=None: [
                FundSeedItem(fund_code="FME", fund_name="Future Mobility Active ETF FoF"),
            ],
            extractor,
        )
        extractor._resolve_document_base_date_for_seeds = types.MethodType(
            lambda self, **kwargs: [
                FundBaseDateItem(fund_code="FME", fund_name="Future Mobility Active ETF FoF", base_date="2025-11-27")
            ],
            extractor,
        )
        seen_stages: list[str] = []

        def fake_run_batched_stage(
            self,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            response_model,
            issues,
            artifacts,
            *,
            counterparty_guidance=None,
        ):
            seen_stages.append(stage.name)
            if stage.name == "t_day":
                return [FundSlotItem(fund_code="FME", fund_name="Future Mobility Active ETF FoF", base_date="2025-11-27", t_day=0, slot_id="T0_SUB", evidence_label="Subscription")]
            if stage.name == "transfer_amount":
                return [FundAmountItem(fund_code="FME", fund_name="Future Mobility Active ETF FoF", base_date="2025-11-27", t_day=0, slot_id="T0_SUB", evidence_label="Subscription", transfer_amount="21,427,754")]
            raise AssertionError(f"Unexpected stage after transfer_amount: {stage.name}")

        extractor._run_batched_stage = types.MethodType(fake_run_batched_stage, extractor)

        outcome = extractor.extract(
            [document_text],
            raw_text=raw_text,
        )

        self.assertEqual(seen_stages, ["t_day", "transfer_amount"])
        self.assertEqual(len(outcome.result.orders), 1)
        self.assertEqual(outcome.result.orders[0].fund_code, "FME")

    def test_same_count_markdown_vs_section_prefers_markdown_when_markdown_is_richer(self) -> None:
        markdown_orders = [
            OrderExtraction(
                fund_code="FME",
                fund_name="Future Mobility Active ETF FoF",
                settle_class="PENDING",
                order_type="SUB",
                base_date="2025-11-27",
                t_day=1,
                transfer_amount="21,427,754",
            ),
            OrderExtraction(
                fund_code="GBE",
                fund_name="Global Bond FoF II",
                settle_class="PENDING",
                order_type="SUB",
                base_date="2025-11-27",
                t_day=1,
                transfer_amount="2,420,174",
            ),
            OrderExtraction(
                fund_code="BEE",
                fund_name="Bond ETF FoF",
                settle_class="PENDING",
                order_type="RED",
                base_date="2025-11-27",
                t_day=1,
                transfer_amount="-208,169,495",
            ),
        ]
        section_orders = [
            OrderExtraction(
                fund_code="FME",
                fund_name="Future Mobility Active ETF FoF",
                settle_class="CONFIRMED",
                order_type="SUB",
                base_date="2025-11-27",
                t_day=0,
                transfer_amount="21,427,754",
            ),
            OrderExtraction(
                fund_code="GBE",
                fund_name="Global Bond FoF II",
                settle_class="CONFIRMED",
                order_type="SUB",
                base_date="2025-11-27",
                t_day=0,
                transfer_amount="2,420,174",
            ),
            OrderExtraction(
                fund_code="BEE",
                fund_name="Bond ETF FoF",
                settle_class="CONFIRMED",
                order_type="RED",
                base_date="2025-11-27",
                t_day=0,
                transfer_amount="-208,169,495",
            ),
        ]

        preferred = self.extractor._select_preferred_deterministic_orders(
            markdown_orders=markdown_orders,
            section_orders=section_orders,
        )

        self.assertIs(preferred, markdown_orders)
        self.assertTrue(all(order.t_day == 1 for order in preferred))
        self.assertTrue(all(order.settle_class.value == "PENDING" for order in preferred))

    def test_section_recovery_wins_when_it_strictly_recovers_more_orders(self) -> None:
        markdown_orders = [
            OrderExtraction(
                fund_code="FME",
                fund_name="Future Mobility Active ETF FoF",
                settle_class="CONFIRMED",
                order_type="SUB",
                base_date="2025-11-27",
                t_day=0,
                transfer_amount="21,427,754",
            ),
        ]
        section_orders = [
            OrderExtraction(
                fund_code="FME",
                fund_name="Future Mobility Active ETF FoF",
                settle_class="CONFIRMED",
                order_type="SUB",
                base_date="2025-11-27",
                t_day=0,
                transfer_amount="21,427,754",
            ),
            OrderExtraction(
                fund_code="GBE",
                fund_name="Global Bond FoF II",
                settle_class="CONFIRMED",
                order_type="SUB",
                base_date="2025-11-27",
                t_day=0,
                transfer_amount="2,420,174",
            ),
        ]

        preferred = self.extractor._select_preferred_deterministic_orders(
            markdown_orders=markdown_orders,
            section_orders=section_orders,
        )

        self.assertIs(preferred, section_orders)

    def test_same_count_section_recovery_can_win_when_only_direction_differs(self) -> None:
        markdown_orders = [
            OrderExtraction(
                fund_code="AEE",
                fund_name="Asia(ex Japan) Index FoF",
                settle_class="CONFIRMED",
                order_type="SUB",
                base_date="2025-11-27",
                t_day=0,
                transfer_amount="668,326",
            ),
        ]
        section_orders = [
            OrderExtraction(
                fund_code="AEE",
                fund_name="Asia(ex Japan) Index FoF",
                settle_class="CONFIRMED",
                order_type="RED",
                base_date="2025-11-27",
                t_day=0,
                transfer_amount="-668,326",
            ),
        ]

        preferred = self.extractor._select_preferred_deterministic_orders(
            markdown_orders=markdown_orders,
            section_orders=section_orders,
        )

        self.assertIs(preferred, section_orders)

    def test_build_result_ignores_summary_and_ratio_evidence(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="F030",
                fund_name="Lambda",
                base_date="2025-08-25",
                t_day=1,
                slot_id="l1",
                evidence_label="T+1일 순유입예상금액",
                transfer_amount="278625",
                settle_class="PENDING",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="F030",
                fund_name="Lambda",
                base_date="2025-08-25",
                t_day=1,
                slot_id="l2",
                evidence_label="5영업일 순유입 합산 (예상)",
                transfer_amount="6801078",
                settle_class="PENDING",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="F030",
                fund_name="Lambda",
                base_date="2025-08-25",
                t_day=1,
                slot_id="l3",
                evidence_label="NAV대비",
                transfer_amount="0.0019",
                settle_class="PENDING",
                order_type="SUB",
            ),
        ]

        result = self.extractor._build_result(items, [])

        self.assertEqual(len(result.orders), 1)
        self.assertEqual(result.orders[0].transfer_amount, "278,625")
        self.assertNotIn("ORDER_TYPE_MISSING", result.issues)

    def test_invalid_explicit_candidate_does_not_drop_valid_execution_candidate(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="F100",
                fund_name="Zeta",
                base_date="2025-11-27",
                t_day=0,
                slot_id="z1",
                evidence_label="입금액",
                transfer_amount=None,
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="F100",
                fund_name="Zeta",
                base_date="2025-11-27",
                t_day=0,
                slot_id="z2",
                evidence_label="당일이체금액",
                transfer_amount="36,189,748",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
        ]

        result = self.extractor._build_result(items, [])

        self.assertEqual(len(result.orders), 1)
        self.assertEqual(result.orders[0].transfer_amount, "36,189,748")

    def test_build_result_prefers_net_amount_when_explicit_and_execution_both_exist(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="F201",
                fund_name="Omega",
                base_date="2025-11-27",
                t_day=0,
                slot_id="o1",
                evidence_label="입금액",
                transfer_amount="273,670,191",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="F201",
                fund_name="Omega",
                base_date="2025-11-27",
                t_day=0,
                slot_id="o2",
                evidence_label="출금액",
                transfer_amount="346,581,036",
                settle_class="CONFIRMED",
                order_type="RED",
            ),
            FundResolvedItem(
                fund_code="F201",
                fund_name="Omega",
                base_date="2025-11-27",
                t_day=0,
                slot_id="o3",
                evidence_label="당일이체금액",
                transfer_amount="-72,910,845",
                settle_class="CONFIRMED",
                order_type="RED",
            ),
            FundResolvedItem(
                fund_code="F202",
                fund_name="Sigma",
                base_date="2025-11-27",
                t_day=0,
                slot_id="s1",
                evidence_label="입금액",
                transfer_amount="136,320,930",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="F202",
                fund_name="Sigma",
                base_date="2025-11-27",
                t_day=0,
                slot_id="s2",
                evidence_label="출금액",
                transfer_amount="77,690,019",
                settle_class="CONFIRMED",
                order_type="RED",
            ),
            FundResolvedItem(
                fund_code="F202",
                fund_name="Sigma",
                base_date="2025-11-27",
                t_day=0,
                slot_id="s3",
                evidence_label="당일이체금액",
                transfer_amount="58,630,911",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
        ]

        result = self.extractor._build_result(items, [])

        self.assertEqual(len(result.orders), 2)
        self.assertEqual(
            {
                (order.fund_code, order.order_type.value, order.transfer_amount)
                for order in result.orders
            },
            {
                ("F201", "RED", "-72,910,845"),
                ("F202", "SUB", "58,630,911"),
            },
        )

    def test_build_result_keeps_explicit_sub_and_red_separate_when_net_evidence_absent(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="F301",
                fund_name="Nu",
                base_date="2025-11-27",
                t_day=0,
                slot_id="n1",
                evidence_label="입금액",
                transfer_amount="120,000",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="F301",
                fund_name="Nu",
                base_date="2025-11-27",
                t_day=0,
                slot_id="n2",
                evidence_label="출금액",
                transfer_amount="50,000",
                settle_class="CONFIRMED",
                order_type="RED",
            ),
        ]

        result = self.extractor._build_result(items, [])

        self.assertEqual(len(result.orders), 2)
        self.assertEqual(
            {
                (order.fund_code, order.order_type.value, order.transfer_amount)
                for order in result.orders
            },
            {
                ("F301", "SUB", "120,000"),
                ("F301", "RED", "-50,000"),
            },
        )

    def test_instruction_document_without_future_columns_is_confirmed(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="AEE",
                fund_name="Asia(ex Japan) Index FoF",
                base_date="2025-11-27",
                t_day=None,
                slot_id="i1",
                evidence_label="Subscription",
                transfer_amount="668,326",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="ATE",
                fund_name="AI innovative Theme selection",
                base_date="2025-11-27",
                t_day=None,
                slot_id="i2",
                evidence_label="Redemption",
                transfer_amount="42,346",
                settle_class="CONFIRMED",
                order_type="RED",
            ),
        ]

        result = self.extractor._build_result(items, [])

        self.assertEqual(len(result.orders), 2)
        self.assertEqual(result.issues, [])
        self.assertTrue(all(order.settle_class.value == "CONFIRMED" for order in result.orders))
        self.assertTrue(all(order.t_day == 0 for order in result.orders))
        redemption = next(order for order in result.orders if order.fund_code == "ATE")
        self.assertEqual(redemption.transfer_amount, "-42,346")
        self.assertEqual(redemption.order_type.value, "RED")

    def test_instruction_evidence_keeps_confirmed_even_if_model_marks_pending(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="AEE",
                fund_name="Asia(ex Japan) Index FoF",
                base_date="2025-11-27",
                t_day=None,
                slot_id="i1",
                evidence_label="Subscription",
                transfer_amount="668,326",
                settle_class="PENDING",
                order_type="SUB",
            ),
        ]

        result = self.extractor._build_result(items, [])

        self.assertEqual(len(result.orders), 1)
        self.assertEqual(result.orders[0].settle_class.value, "CONFIRMED")
        self.assertEqual(result.orders[0].t_day, 0)
        self.assertEqual(result.issues, [])

    def test_normalize_order_type_prefers_explicit_section_label_before_model_value(self) -> None:
        order_type = self.extractor._normalize_order_type(
            value="SUB",
            amount="21,427,754",
            evidence_label="Redemption",
        )

        self.assertEqual(order_type.value, "RED")

    def test_build_result_uses_document_section_hint_to_correct_wrong_model_direction(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="KUB",
                fund_name="KOSPI200 USTN Balanced ETF FoF",
                base_date="2025-11-27",
                t_day=0,
                slot_id="k1",
                evidence_label="Subscription",
                transfer_amount="825,985",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="MAF",
                fund_name="New global asset allocation FoF",
                base_date="2025-11-27",
                t_day=0,
                slot_id="m1",
                evidence_label="Redemption",
                transfer_amount="-814,151",
                settle_class="CONFIRMED",
                order_type="RED",
            ),
        ]
        document_text = (
            "1. Subscription\n"
            "New global asset allocation FoF MAF 259,553.9429 27-Nov-25 3,136.73 814,151 HANA\n"
            "2. Redemption\n"
            "KOSPI200 USTN Balanced ETF FoF KUB 568,801.3803 27-Nov-25 1,452.15 825,985 SCFB\n"
        )

        result = self.extractor._build_result(items, [], document_text=document_text)

        kub = next(order for order in result.orders if order.fund_code == "KUB")
        maf = next(order for order in result.orders if order.fund_code == "MAF")
        self.assertEqual(kub.order_type.value, "RED")
        self.assertEqual(kub.transfer_amount, "-825,985")
        self.assertEqual(maf.order_type.value, "SUB")
        self.assertEqual(maf.transfer_amount, "814,151")

    def test_build_result_uses_korean_transaction_header_hint_for_split_row_pdf(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="BBC13F",
                fund_name="VUL 주식성장형(1형)_SamsungActive",
                base_date="2025-11-27",
                t_day=0,
                slot_id="h1",
                evidence_label="펀드납입(인출)금액 / 판매회사분결제액",
                transfer_amount="7,108,103",
                settle_class="CONFIRMED",
                order_type="RED",
            ),
            FundResolvedItem(
                fund_code="BBA72G",
                fund_name="VA 안정성장형 I_SamsungActive",
                base_date="2025-11-27",
                t_day=0,
                slot_id="h2",
                evidence_label="펀드납입(인출)금액 / 판매회사분결제액",
                transfer_amount="1,288,090",
                settle_class="CONFIRMED",
                order_type="RED",
            ),
        ]
        document_text = (
            "거래종류 : | 펀드설정(수탁은행용) |  |  |  | ( 단위 : 원 )\n"
            "| 설정 | BBC13F | 하나펀드서비스 | 2025-11-27 | 7,108,103 |\n"
            "거래종류 : | 펀드해지(수탁은행용) |  |  |  | ( 단위 : 원 )\n"
            "| 해지 | BBA72G | 하나펀드서비스 | 2025-11-27 | 1,288,090 |\n"
        )

        result = self.extractor._build_result(items, [], document_text=document_text)

        setting = next(order for order in result.orders if order.fund_code == "BBC13F")
        redemption = next(order for order in result.orders if order.fund_code == "BBA72G")
        self.assertEqual(setting.order_type.value, "SUB")
        self.assertEqual(setting.transfer_amount, "7,108,103")
        self.assertEqual(redemption.order_type.value, "RED")
        self.assertEqual(redemption.transfer_amount, "-1,288,090")

    def test_normalize_order_type_maps_buy_and_sell_labels(self) -> None:
        self.assertEqual(
            self.extractor._normalize_order_type(value=None, amount="100", evidence_label="Buy").value,
            "SUB",
        )
        self.assertEqual(
            self.extractor._normalize_order_type(value=None, amount="100", evidence_label="Sell").value,
            "RED",
        )

    def test_normalize_order_type_keeps_hybrid_inflow_outflow_header_neutral(self) -> None:
        order_type = self.extractor._normalize_order_type(
            value=None,
            amount="100",
            evidence_label="펀드납입(인출)금액 / 판매회사분결제액",
        )

        self.assertIsNone(order_type)

    def test_stage_items_are_complete_accepts_inferable_order_type_without_raw_value(self) -> None:
        items = [
            {
                "fund_code": "BBI100",
                "fund_name": "변액유니버셜종신-채권형",
                "base_date": "2026-03-18",
                "t_day": 2,
                "slot_id": "T2_RED",
                "evidence_label": "해지금액 / 3월20일",
                "transfer_amount": "17,716,295",
                "settle_class": "PENDING",
                "order_type": "",
            },
            {
                "fund_code": "BBIM20",
                "fund_name": "변액유니버셜종신-채권형Ⅱ(채권_삼성)",
                "base_date": "2026-03-18",
                "t_day": 0,
                "slot_id": "T0_1",
                "evidence_label": "정산액",
                "transfer_amount": "-20,907,836",
                "settle_class": "CONFIRMED",
                "order_type": "",
            },
        ]

        self.assertTrue(self.extractor._stage_items_are_complete("order_type", items))

    def test_reconcile_final_issues_removes_t_day_missing_for_non_schedule_items(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="AEE",
                fund_name="Asia(ex Japan) Index FoF",
                base_date="2025-11-27",
                t_day=None,
                slot_id="i1",
                evidence_label="Redemption",
                transfer_amount="42,346",
                settle_class="CONFIRMED",
                order_type="RED",
            ),
        ]

        result = self.extractor._build_result(items, ["T_DAY_MISSING"])
        self.extractor._reconcile_final_issues(items, result)

        self.assertEqual(result.issues, [])

    def test_reconcile_final_issues_removes_t_day_missing_for_zero_only_fund(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="492006",
                fund_name="혼합성장형",
                base_date="2025-11-28",
                t_day=None,
                slot_id="i0",
                evidence_label="D+1(예상금액)",
                transfer_amount=None,
                settle_class="PENDING",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="492007",
                fund_name="액티브배당성장70혼합형",
                base_date="2025-11-28",
                t_day=0,
                slot_id="i1",
                evidence_label="금액",
                transfer_amount="1,605,698",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="492007",
                fund_name="액티브배당성장70혼합형",
                base_date="2025-11-28",
                t_day=1,
                slot_id="i2",
                evidence_label="D+1(예상금액)",
                transfer_amount="41,258,553",
                settle_class="PENDING",
                order_type="SUB",
            ),
        ]
        document_text = (
            "펀드명 | 운용사 | 수탁코드 | 구분 | 내용 | 금액 | D+1(예상금액)\n"
            "혼합성장형 | 삼성액티브 자산운용 | 492006 | 입금 | 보험료입금 | - | -\n"
            "액티브배당성장70혼합형 | 삼성액티브 자산운용 | 492007 | 입금 | 보험료입금 | 1,605,698 | 41,258,553\n"
        )

        result = self.extractor._build_result(items, ["T_DAY_MISSING"], document_text=document_text)
        self.extractor._reconcile_final_issues(items, result, document_text=document_text)

        self.assertEqual(result.issues, [])

    def test_reconcile_final_issues_removes_detailed_t_day_missing_for_zero_only_fund(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="492006",
                fund_name="혼합성장형",
                base_date="2025-11-28",
                t_day=None,
                slot_id="i0",
                evidence_label="D+1(예상금액)",
                transfer_amount=None,
                settle_class="PENDING",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="492007",
                fund_name="액티브배당성장70혼합형",
                base_date="2025-11-28",
                t_day=0,
                slot_id="i1",
                evidence_label="금액",
                transfer_amount="1,605,698",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
        ]
        document_text = (
            "펀드명 | 운용사 | 수탁코드 | 구분 | 내용 | 금액 | D+1(예상금액)\n"
            "혼합성장형 | 삼성액티브 자산운용 | 492006 | 입금 | 보험료입금 | - | -\n"
            "액티브배당성장70혼합형 | 삼성액티브 자산운용 | 492007 | 입금 | 보험료입금 | 1,605,698 | 41,258,553\n"
        )

        result = self.extractor._build_result(
            items,
            ["T_DAY_MISSING: 492006 has no non-zero monetary value in any column"],
            document_text=document_text,
        )
        self.extractor._reconcile_final_issues(items, result, document_text=document_text)

        self.assertEqual(result.issues, [])

    def test_reconcile_final_issues_removes_t_day_missing_with_complete_schedule_sibling(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="492007",
                fund_name="액티브배당성장70혼합형",
                base_date="2025-11-28",
                t_day=None,
                slot_id="ghost",
                evidence_label="D+1(예상금액)",
                transfer_amount="41,258,553",
                settle_class="PENDING",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="492007",
                fund_name="액티브배당성장70혼합형",
                base_date="2025-11-28",
                t_day=1,
                slot_id="live",
                evidence_label="D+1(예상금액)",
                transfer_amount="41,258,553",
                settle_class="PENDING",
                order_type="SUB",
            ),
        ]
        document_text = "펀드명 | 수탁코드 | D+1(예상금액)\n액티브배당성장70혼합형 | 492007 | 41,258,553\n"

        result = ExtractionResult(
            orders=[
                OrderExtraction(
                    fund_code="492007",
                    fund_name="액티브배당성장70혼합형",
                    settle_class=SettleClass.PENDING,
                    order_type=OrderType.SUB,
                    base_date="2025-11-28",
                    t_day=1,
                    transfer_amount="41,258,553",
                )
            ],
            issues=["T_DAY_MISSING"],
        )
        self.extractor._reconcile_final_issues(items, result, document_text=document_text, raw_text=document_text)

        self.assertEqual(result.issues, [])

    def test_reconcile_final_issues_removes_transfer_amount_missing_when_final_orders_are_complete(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="AEE",
                fund_name="Asia(ex Japan) Index FoF",
                base_date="2025-11-27",
                t_day=0,
                slot_id="i1",
                evidence_label="Subscription",
                transfer_amount="668,326",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="ATE",
                fund_name="AI innovative Theme selection",
                base_date="2025-11-27",
                t_day=0,
                slot_id="i2",
                evidence_label="Redemption",
                transfer_amount="42,346",
                settle_class="CONFIRMED",
                order_type="RED",
            ),
        ]

        result = self.extractor._build_result(items, ["TRANSFER_AMOUNT_MISSING"])
        self.extractor._reconcile_final_issues(items, result)

        self.assertEqual(result.issues, [])

    def test_reconcile_final_issues_removes_transfer_amount_stage_partial_when_final_orders_are_complete(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="AEE",
                fund_name="Asia(ex Japan) Index FoF",
                base_date="2025-11-27",
                t_day=0,
                slot_id="i1",
                evidence_label="Subscription",
                transfer_amount="668,326",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
        ]

        result = self.extractor._build_result(items, ["TRANSFER_AMOUNT_STAGE_PARTIAL"])
        self.extractor._reconcile_final_issues(items, result)

        self.assertEqual(result.issues, [])

    def test_reconcile_final_issues_removes_transfer_amount_missing_for_zero_only_fund(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="492006",
                fund_name="혼합성장형",
                base_date="2025-11-28",
                t_day=0,
                slot_id="i0",
                evidence_label="금액",
                transfer_amount=None,
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="492007",
                fund_name="액티브배당성장70혼합형",
                base_date="2025-11-28",
                t_day=0,
                slot_id="i1",
                evidence_label="금액",
                transfer_amount="1,605,698",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="492007",
                fund_name="액티브배당성장70혼합형",
                base_date="2025-11-28",
                t_day=1,
                slot_id="i2",
                evidence_label="D+1(예상금액)",
                transfer_amount="41,258,553",
                settle_class="PENDING",
                order_type="SUB",
            ),
        ]
        document_text = (
            "펀드명 | 운용사 | 수탁코드 | 구분 | 내용 | 금액 | D+1(예상금액)\n"
            "혼합성장형 | 삼성액티브 자산운용 | 492006 | 입금 | 보험료입금 | - | -\n"
            "액티브배당성장70혼합형 | 삼성액티브 자산운용 | 492007 | 입금 | 보험료입금 | 1,605,698 | 41,258,553\n"
        )

        result = self.extractor._build_result(items, ["TRANSFER_AMOUNT_MISSING"], document_text=document_text)
        self.extractor._reconcile_final_issues(items, result, document_text=document_text)

        self.assertEqual(result.issues, [])

    def test_reconcile_final_issues_removes_t_day_stage_partial_when_final_orders_are_complete(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="AEE",
                fund_name="Asia(ex Japan) Index FoF",
                base_date="2025-11-27",
                t_day=1,
                slot_id="i1",
                evidence_label="D+1(예상금액)",
                transfer_amount="42,346",
                settle_class="PENDING",
                order_type="RED",
            ),
        ]

        result = self.extractor._build_result(items, ["T_DAY_STAGE_PARTIAL"])
        self.extractor._reconcile_final_issues(items, result)

        self.assertEqual(result.issues, [])

    def test_reconcile_final_issues_removes_order_type_missing_for_zero_only_fund(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="492006",
                fund_name="혼합성장형",
                base_date="2025-11-28",
                t_day=0,
                slot_id="i0",
                evidence_label="금액",
                transfer_amount="-",
                settle_class="CONFIRMED",
                order_type=None,
            ),
            FundResolvedItem(
                fund_code="492007",
                fund_name="액티브배당성장70혼합형",
                base_date="2025-11-28",
                t_day=0,
                slot_id="i1",
                evidence_label="금액",
                transfer_amount="1,605,698",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
        ]
        document_text = (
            "펀드명 | 운용사 | 수탁코드 | 구분 | 내용 | 금액 | D+1(예상금액)\n"
            "혼합성장형 | 삼성액티브 자산운용 | 492006 | 입금 | 보험료입금 | - | -\n"
            "액티브배당성장70혼합형 | 삼성액티브 자산운용 | 492007 | 입금 | 보험료입금 | 1,605,698 | 41,258,553\n"
        )

        result = self.extractor._build_result(items, ["ORDER_TYPE_MISSING"], document_text=document_text)
        self.extractor._reconcile_final_issues(items, result, document_text=document_text)

        self.assertEqual(result.issues, [])

    def test_reconcile_final_issues_removes_stale_missing_amount_and_order_type_with_complete_sibling(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="V2201S",
                fund_name="한화대표펀드",
                base_date="2025-08-26",
                t_day=1,
                slot_id="ghost",
                evidence_label="설정(예탁) 및 해지금액",
                transfer_amount=None,
                settle_class="CONFIRMED",
                order_type=None,
            ),
            FundResolvedItem(
                fund_code="V2201S",
                fund_name="한화대표펀드",
                base_date="2025-08-26",
                t_day=1,
                slot_id="live",
                evidence_label="설정(예탁) 및 해지금액",
                transfer_amount="21,187,951",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
        ]
        document_text = (
            "펀드코드 | 펀드명 | 설정(예탁) 및 해지금액\n"
            "V2201S | 한화대표펀드 | 21,187,951\n"
        )

        result = self.extractor._build_result(
            items,
            ["TRANSFER_AMOUNT_MISSING", "ORDER_TYPE_MISSING"],
            document_text=document_text,
        )
        self.extractor._reconcile_final_issues(
            items,
            result,
            document_text=document_text,
        )

        self.assertEqual(len(result.orders), 1)
        self.assertEqual(result.issues, [])

    def test_reconcile_final_issues_keeps_missing_amount_and_order_type_without_complete_sibling(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="V2201S",
                fund_name="한화대표펀드",
                base_date="2025-08-26",
                t_day=0,
                slot_id="ghost",
                evidence_label="설정(예탁) 및 해지금액",
                transfer_amount=None,
                settle_class="CONFIRMED",
                order_type=None,
            ),
            FundResolvedItem(
                fund_code="V2201S",
                fund_name="한화대표펀드",
                base_date="2025-08-26",
                t_day=1,
                slot_id="live",
                evidence_label="설정(예탁) 및 해지금액",
                transfer_amount="21,187,951",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
        ]
        document_text = (
            "펀드코드 | 펀드명 | 설정(예탁) 및 해지금액\n"
            "V2201S | 한화대표펀드 | 21,187,951\n"
        )

        result = self.extractor._build_result(
            items,
            ["TRANSFER_AMOUNT_MISSING", "ORDER_TYPE_MISSING"],
            document_text=document_text,
        )
        self.extractor._reconcile_final_issues(
            items,
            result,
            document_text=document_text,
        )

        self.assertIn("TRANSFER_AMOUNT_MISSING", result.issues)
        self.assertIn("ORDER_TYPE_MISSING", result.issues)

    def test_reconcile_final_issues_removes_settle_class_missing_for_non_actionable_items(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="C1005",
                fund_name="글로벌리츠(VUL)",
                base_date="2025-08-25",
                t_day=0,
                slot_id="t0",
                evidence_label="T일 순유입 확정금액",
                transfer_amount="920315",
                settle_class=None,
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="C1005",
                fund_name="글로벌리츠(VUL)",
                base_date="2025-08-25",
                t_day=1,
                slot_id="t1",
                evidence_label="T+1일 순유입예상금액",
                transfer_amount="278625",
                settle_class="PENDING",
                order_type="SUB",
            ),
        ]

        result = self.extractor._build_result(items, ["SETTLE_CLASS_MISSING"])
        self.extractor._reconcile_final_issues(items, result)

        self.assertEqual(result.issues, [])

    def test_reconcile_final_issues_removes_settle_class_missing_with_complete_sibling(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="C1005",
                fund_name="글로벌리츠(VUL)",
                base_date="2025-08-25",
                t_day=0,
                slot_id="ghost",
                evidence_label="설정(예탁) 및 해지금액",
                transfer_amount="920315",
                settle_class=None,
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="C1005",
                fund_name="글로벌리츠(VUL)",
                base_date="2025-08-25",
                t_day=0,
                slot_id="live",
                evidence_label="설정(예탁) 및 해지금액",
                transfer_amount="920315",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
        ]

        result = self.extractor._build_result(items, ["SETTLE_CLASS_MISSING"])
        self.extractor._reconcile_final_issues(items, result)

        self.assertEqual(len(result.orders), 1)
        self.assertEqual(result.issues, [])

    def test_reconcile_final_issues_removes_stale_base_date_missing_when_final_orders_are_complete(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="KDB01",
                fund_name="KDB Bond",
                base_date=None,
                t_day=0,
                slot_id="t0",
                evidence_label="정산금액",
                transfer_amount="100",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="KDB01",
                fund_name="KDB Bond",
                base_date="2025-11-28",
                t_day=0,
                slot_id="t1",
                evidence_label="정산금액",
                transfer_amount="100",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
        ]

        result = ExtractionResult(
            orders=[
                OrderExtraction(
                    fund_code="KDB01",
                    fund_name="KDB Bond",
                    base_date="2025-11-28",
                    t_day=0,
                    settle_class="CONFIRMED",
                    order_type="SUB",
                    transfer_amount="100",
                )
            ],
            issues=["BASE_DATE_MISSING"],
        )

        self.extractor._reconcile_final_issues(items, result)

        self.assertEqual(result.issues, [])

    def test_reconcile_final_issues_removes_llm_invalid_response_format_when_final_orders_are_complete(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="KDB01",
                fund_name="KDB Bond",
                base_date="2025-11-28",
                t_day=0,
                slot_id="t0",
                evidence_label="정산금액",
                transfer_amount="100",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
        ]

        result = ExtractionResult(
            orders=[
                OrderExtraction(
                    fund_code="KDB01",
                    fund_name="KDB Bond",
                    base_date="2025-11-28",
                    t_day=0,
                    settle_class="CONFIRMED",
                    order_type="SUB",
                    transfer_amount="100",
                )
            ],
            issues=["LLM_INVALID_RESPONSE_FORMAT"],
        )

        self.extractor._reconcile_final_issues(items, result)

        self.assertEqual(result.issues, [])

    def test_reconcile_final_issues_keeps_llm_invalid_response_format_when_other_blocking_issue_remains(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="KDB01",
                fund_name="KDB Bond",
                base_date="2025-11-28",
                t_day=0,
                slot_id="t0",
                evidence_label="정산금액",
                transfer_amount="100",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
        ]

        result = ExtractionResult(
            orders=[
                OrderExtraction(
                    fund_code="KDB01",
                    fund_name="KDB Bond",
                    base_date="2025-11-28",
                    t_day=0,
                    settle_class="CONFIRMED",
                    order_type="SUB",
                    transfer_amount="100",
                )
            ],
            issues=["LLM_INVALID_RESPONSE_FORMAT", "TRANSACTION_SLOT_EMPTY"],
        )

        self.extractor._reconcile_final_issues(items, result)

        self.assertIn("LLM_INVALID_RESPONSE_FORMAT", result.issues)

    def test_reconcile_final_issues_removes_fund_metadata_incomplete_with_complete_sibling(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="",
                fund_name="한화대표펀드",
                base_date="2025-08-26",
                t_day=1,
                slot_id="ghost",
                evidence_label="설정(예탁) 및 해지금액",
                transfer_amount="21,187,951",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="V2201S",
                fund_name="한화대표펀드",
                base_date="2025-08-26",
                t_day=1,
                slot_id="live",
                evidence_label="설정(예탁) 및 해지금액",
                transfer_amount="21,187,951",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
        ]
        document_text = "V2201S | 한화대표펀드 | 설정(예탁) 및 해지금액 | 21,187,951"

        result = self.extractor._build_result(items, [], document_text=document_text)
        self.extractor._reconcile_final_issues(items, result, document_text=document_text, raw_text=document_text)

        self.assertEqual(len(result.orders), 1)
        self.assertEqual(result.issues, [])

    def test_reconcile_final_issues_keeps_fund_metadata_incomplete_without_complete_sibling(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="",
                fund_name="한화대표펀드",
                base_date="2025-08-26",
                t_day=1,
                slot_id="ghost",
                evidence_label="설정(예탁) 및 해지금액",
                transfer_amount="21,187,951",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
        ]
        document_text = "한화대표펀드 | 설정(예탁) 및 해지금액 | 21,187,951"

        result = self.extractor._build_result(items, [], document_text=document_text)
        self.extractor._reconcile_final_issues(items, result, document_text=document_text, raw_text=document_text)

        self.assertIn("FUND_METADATA_INCOMPLETE", result.issues)

    def test_reconcile_final_issues_removes_fund_metadata_incomplete_with_unique_complete_sibling_even_when_both_metadata_fields_are_missing(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="",
                fund_name="",
                base_date="2025-08-26",
                t_day=1,
                slot_id="ghost",
                evidence_label="설정(예탁) 및 해지금액",
                transfer_amount="21,187,951",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="V2201S",
                fund_name="한화대표펀드",
                base_date="2025-08-26",
                t_day=1,
                slot_id="live",
                evidence_label="설정(예탁) 및 해지금액",
                transfer_amount="21,187,951",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
        ]
        document_text = "V2201S | 한화대표펀드 | 설정(예탁) 및 해지금액 | 21,187,951"

        result = self.extractor._build_result(items, [], document_text=document_text)
        self.extractor._reconcile_final_issues(items, result, document_text=document_text, raw_text=document_text)

        self.assertEqual(len(result.orders), 1)
        self.assertEqual(result.issues, [])

    def test_reconcile_final_issues_keeps_fund_metadata_incomplete_when_both_metadata_fields_are_missing_and_sibling_is_ambiguous(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="",
                fund_name="",
                base_date="2025-08-26",
                t_day=1,
                slot_id="ghost",
                evidence_label="설정(예탁) 및 해지금액",
                transfer_amount="21,187,951",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="V2201S",
                fund_name="한화대표펀드",
                base_date="2025-08-26",
                t_day=1,
                slot_id="live1",
                evidence_label="설정(예탁) 및 해지금액",
                transfer_amount="21,187,951",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="V2202S",
                fund_name="한화대표2펀드",
                base_date="2025-08-26",
                t_day=1,
                slot_id="live2",
                evidence_label="설정(예탁) 및 해지금액",
                transfer_amount="21,187,951",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
        ]
        document_text = (
            "V2201S | 한화대표펀드 | 설정(예탁) 및 해지금액 | 21,187,951\n"
            "V2202S | 한화대표2펀드 | 설정(예탁) 및 해지금액 | 21,187,951"
        )

        result = self.extractor._build_result(items, [], document_text=document_text)
        self.extractor._reconcile_final_issues(items, result, document_text=document_text, raw_text=document_text)

        self.assertIn("FUND_METADATA_INCOMPLETE", result.issues)

    def test_reconcile_final_issues_removes_transfer_amount_zero_when_final_orders_are_non_zero(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="V2201S",
                fund_name="성장주식형",
                base_date="2025-08-26",
                t_day=0,
                slot_id="t0",
                evidence_label="설정(예탁) 및 해지금액 / 투자일임/수익증권",
                transfer_amount="21,187,951",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="V5203F",
                fund_name="가치주식형",
                base_date="2025-08-26",
                t_day=0,
                slot_id="t1",
                evidence_label="설정(예탁) 및 해지금액 / 투자일임/수익증권",
                transfer_amount="0",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="V5203F",
                fund_name="가치주식형",
                base_date="2025-08-26",
                t_day=0,
                slot_id="t2",
                evidence_label="설정(예탁) 및 해지금액 / 펀드계",
                transfer_amount="-47,580,112",
                settle_class="CONFIRMED",
                order_type="RED",
            ),
        ]

        result = self.extractor._build_result(items, ["TRANSFER_AMOUNT_ZERO"])
        self.extractor._reconcile_final_issues(items, result)

        self.assertEqual(result.issues, [])

    def test_reconcile_final_issues_overrides_uniform_base_date_with_title_adjacent_hint(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="C1005",
                fund_name="글로벌리츠(VUL)",
                base_date="2025-08-25",
                t_day=1,
                slot_id="t1",
                evidence_label="T+1일 순유입예상금액",
                transfer_amount="278625",
                settle_class="PENDING",
                order_type="SUB",
            )
        ]
        result = ExtractionResult(
            orders=[
                OrderExtraction(
                    fund_code="C1005",
                    fund_name="글로벌리츠(VUL)",
                    base_date="2025-08-25",
                    t_day=1,
                    settle_class="PENDING",
                    order_type="SUB",
                    transfer_amount="278625",
                )
            ],
            issues=[],
        )
        raw_text = (
            "[SHEET 호스트]\n"
            "특별계정_VL & VUL 자금 운용 현황 |  |  |\n"
            "2025-08-26 |  |  |\n"
            "운용지시펀드코드 | 펀드코드_NG&S | 펀드명 | 수탁사 | 2025-08-25 | T일 (영업일 기준)\n"
            "펀드코드_KASS | 펀드코드_NG&S | 펀드명 | 수탁사 | T-1일(영업일기준) NAV | T일 투입금액\n"
        )

        self.extractor._reconcile_final_issues(items, result, raw_text=raw_text)

        self.assertEqual(result.orders[0].base_date, "2025-08-26")

    def test_reconcile_final_issues_does_not_override_base_date_without_prior_reference_signal(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="F001",
                fund_name="Alpha",
                base_date="2025-08-25",
                t_day=0,
                slot_id="t0",
                evidence_label="결제금액",
                transfer_amount="100",
                settle_class="CONFIRMED",
                order_type="SUB",
            )
        ]
        result = ExtractionResult(
            orders=[
                OrderExtraction(
                    fund_code="F001",
                    fund_name="Alpha",
                    base_date="2025-08-25",
                    t_day=0,
                    settle_class="CONFIRMED",
                    order_type="SUB",
                    transfer_amount="100",
                )
            ],
            issues=[],
        )
        raw_text = (
            "[SHEET 호스트]\n"
            "Alpha 운용 보고서 |  |  |\n"
            "2025-08-26 |  |  |\n"
            "펀드코드 | 펀드명 | 결제일 | 결제금액\n"
            "F001 | Alpha | 2025-08-25 | 100\n"
        )

        self.extractor._reconcile_final_issues(items, result, raw_text=raw_text)

        self.assertEqual(result.orders[0].base_date, "2025-08-25")

    def test_reconcile_final_issues_removes_stale_manager_scope_and_duplicate_pair_warnings(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="D32160",
                fund_name="변액종신아시아혼합형",
                base_date="2025-08-26",
                t_day=1,
                slot_id="t1",
                evidence_label="증감금액",
                transfer_amount="-1,212,460",
                settle_class="PENDING",
                order_type="RED",
            ),
            FundResolvedItem(
                fund_code="D32100",
                fund_name="차이나혼합형",
                base_date="2025-08-26",
                t_day=1,
                slot_id="t2",
                evidence_label="증감금액",
                transfer_amount="-4,006,841",
                settle_class="PENDING",
                order_type="RED",
            ),
            FundResolvedItem(
                fund_code="D32140",
                fund_name="브릭스혼합형",
                base_date="2025-08-26",
                t_day=1,
                slot_id="t3",
                evidence_label="증감금액",
                transfer_amount="111,278",
                settle_class="PENDING",
                order_type="SUB",
            ),
        ]
        result = ExtractionResult(
            orders=[
                OrderExtraction(
                    fund_code="D32160",
                    fund_name="변액종신아시아혼합형",
                    base_date="2025-08-26",
                    t_day=1,
                    settle_class="PENDING",
                    order_type="RED",
                    transfer_amount="-1,212,460",
                ),
                OrderExtraction(
                    fund_code="D32100",
                    fund_name="차이나혼합형",
                    base_date="2025-08-26",
                    t_day=1,
                    settle_class="PENDING",
                    order_type="RED",
                    transfer_amount="-4,006,841",
                ),
                OrderExtraction(
                    fund_code="D32140",
                    fund_name="브릭스혼합형",
                    base_date="2025-08-26",
                    t_day=1,
                    settle_class="PENDING",
                    order_type="SUB",
                    transfer_amount="111,278",
                ),
            ],
            issues=["NO_MANAGER_FILTER_APPLIED", "DUPLICATE_FUND_CODE_NAME_PAIRS"],
        )
        target_fund_scope = TargetFundScope(
            manager_column_present=True,
            include_all_funds=False,
            fund_codes=frozenset({"D32160", "D32100", "D32140"}),
            fund_names=frozenset({"변액종신아시아혼합형", "차이나혼합형", "브릭스혼합형"}),
            canonical_fund_names=frozenset({"변액종신아시아혼합형", "차이나혼합형", "브릭스혼합형"}),
        )

        self.extractor._reconcile_final_issues(items, result, target_fund_scope=target_fund_scope)

        self.assertEqual(result.issues, [])

    def test_reconcile_final_issues_removes_stale_manager_missing_warnings_when_target_orders_are_complete(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="D32160",
                fund_name="변액종신아시아혼합형",
                base_date="2025-08-26",
                t_day=1,
                slot_id="t1",
                evidence_label="증감금액",
                transfer_amount="-1,212,460",
                settle_class="PENDING",
                order_type="RED",
            ),
        ]
        result = ExtractionResult(
            orders=[
                OrderExtraction(
                    fund_code="D32160",
                    fund_name="변액종신아시아혼합형",
                    base_date="2025-08-26",
                    t_day=1,
                    settle_class="PENDING",
                    order_type="RED",
                    transfer_amount="-1,212,460",
                )
            ],
            issues=[
                "NO_MANAGER_INFO_FOR_FUND_D32160",
                "MANAGER_MISSING",
                "MANAGER_MISSING_IN_RAW_TEXT_FOR_D34040",
            ],
        )
        target_fund_scope = TargetFundScope(
            manager_column_present=True,
            include_all_funds=False,
            fund_codes=frozenset({"D32160"}),
            fund_names=frozenset({"변액종신아시아혼합형"}),
            canonical_fund_names=frozenset({"변액종신아시아혼합형"}),
        )

        self.extractor._reconcile_final_issues(items, result, target_fund_scope=target_fund_scope)

        self.assertEqual(result.issues, [])

    def test_reconcile_final_issues_removes_fund_name_ambiguity_when_final_orders_are_corroborated(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="D32160",
                fund_name="변액종신아시아혼합형",
                base_date="2025-08-26",
                t_day=1,
                slot_id="t1",
                evidence_label="증감금액",
                transfer_amount="-1,212,460",
                settle_class="PENDING",
                order_type="RED",
            ),
        ]
        result = ExtractionResult(
            orders=[
                OrderExtraction(
                    fund_code="D32160",
                    fund_name="변액종신아시아혼합형",
                    base_date="2025-08-26",
                    t_day=1,
                    settle_class="PENDING",
                    order_type="RED",
                    transfer_amount="-1,212,460",
                )
            ],
            issues=["FUND_NAME_AMBIGUOUS_IN_RAW_TEXT"],
        )
        markdown_text = (
            "## SHEET DB\n\n"
            "```text\n2025-08-26\n```\n\n"
            "| 이체일자 | 상품명 | 펀드명 | 운용사코드 | 운용사명 | 유입금액 | 유출금액 | 증감금액 |\n"
            "| --- | --- | --- | --- | --- | --- | --- | --- |\n"
            "| 2025-08-26 | 변액종신 | 변액종신아시아혼합형 | D32160 | 삼성자산운용 | 112135 | 1324595 | -1212460 |\n"
        )
        raw_text = (
            "[SHEET DB]\n"
            "2025-08-26\n"
            "이체일자 | 상품명 | 펀드명 | 운용사코드 | 운용사명 | 유입금액 | 유출금액 | 증감금액\n"
            "2025-08-26 | 변액종신 | 변액종신아시아혼합형 | D32160 | 삼성자산운용 | 112135 | 1324595 | -1212460\n"
        )
        target_fund_scope = TargetFundScope(
            manager_column_present=True,
            include_all_funds=False,
            fund_codes=frozenset({"D32160"}),
            fund_names=frozenset({"변액종신아시아혼합형"}),
            canonical_fund_names=frozenset({"변액종신아시아혼합형"}),
        )

        self.extractor._reconcile_final_issues(
            items,
            result,
            markdown_text=markdown_text,
            document_text=markdown_text,
            raw_text=raw_text,
            target_fund_scope=target_fund_scope,
        )

        self.assertEqual(result.issues, [])

    def test_reconcile_final_issues_removes_markdown_fund_name_ambiguity_when_final_orders_are_corroborated(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="D32160",
                fund_name="변액종신아시아혼합형",
                base_date="2025-08-26",
                t_day=1,
                settle_class="PENDING",
                order_type="RED",
                transfer_amount="-1,212,460",
            )
        ]
        result = ExtractionResult(
            orders=[
                OrderExtraction(
                    fund_code="D32160",
                    fund_name="변액종신아시아혼합형",
                    base_date="2025-08-26",
                    t_day=1,
                    settle_class="PENDING",
                    order_type="RED",
                    transfer_amount="-1,212,460",
                )
            ],
            issues=["FUND_NAME_AMBIGUOUS_IN_MARKDOWN"],
        )
        markdown_text = (
            "## SHEET DB\n\n"
            "```text\n2025-08-26\n```\n\n"
            "| 이체일자 | 상품명 | 펀드명 | 운용사코드 | 운용사명 | 유입금액 | 유출금액 | 증감금액 |\n"
            "| --- | --- | --- | --- | --- | --- | --- | --- |\n"
            "| 2025-08-26 | 변액종신 | 변액종신아시아혼합형 | D32160 | 삼성자산운용 | 112135 | 1324595 | -1212460 |\n"
        )
        raw_text = (
            "[SHEET DB]\n"
            "2025-08-26\n"
            "이체일자 | 상품명 | 펀드명 | 운용사코드 | 운용사명 | 유입금액 | 유출금액 | 증감금액\n"
            "2025-08-26 | 변액종신 | 변액종신아시아혼합형 | D32160 | 삼성자산운용 | 112135 | 1324595 | -1212460\n"
        )
        target_fund_scope = TargetFundScope(
            manager_column_present=True,
            include_all_funds=False,
            fund_codes=frozenset({"D32160"}),
            fund_names=frozenset({"변액종신아시아혼합형"}),
            canonical_fund_names=frozenset({"변액종신아시아혼합형"}),
        )

        self.extractor._reconcile_final_issues(
            items,
            result,
            markdown_text=markdown_text,
            document_text=markdown_text,
            raw_text=raw_text,
            target_fund_scope=target_fund_scope,
        )

        self.assertEqual(result.issues, [])

    def test_build_result_computes_decimal_net_amount(self) -> None:
        items = [
            FundResolvedItem(
                fund_code="F301",
                fund_name="Decimal Fund",
                base_date="2025-08-26",
                t_day=0,
                slot_id="d1",
                evidence_label="설정금액",
                transfer_amount="6,620.93",
                settle_class="CONFIRMED",
                order_type="SUB",
            ),
            FundResolvedItem(
                fund_code="F301",
                fund_name="Decimal Fund",
                base_date="2025-08-26",
                t_day=0,
                slot_id="d2",
                evidence_label="해지금액",
                transfer_amount="57,193.42",
                settle_class="CONFIRMED",
                order_type="RED",
            ),
        ]

        result = self.extractor._build_result(items, [])

        self.assertEqual(len(result.orders), 2)
        self.assertEqual({order.order_type.value for order in result.orders}, {"SUB", "RED"})
        self.assertEqual({order.transfer_amount for order in result.orders}, {"6,620.93", "-57,193.42"})

    def test_dedupe_orders_by_signature_collapses_xlsx_float_tail_artifact(self) -> None:
        orders = [
            OrderExtraction(
                fund_code="450042",
                fund_name="변액유니버셜 주식",
                settle_class="CONFIRMED",
                order_type="SUB",
                base_date="2026-04-15",
                t_day=0,
                transfer_amount="70,000,000.00000001",
            ),
            OrderExtraction(
                fund_code="450042",
                fund_name="변액유니버셜 주식",
                settle_class="CONFIRMED",
                order_type="SUB",
                base_date="2026-04-15",
                t_day=0,
                transfer_amount="70,000,000",
            ),
        ]

        deduped = self.extractor._dedupe_orders_by_signature(orders)

        self.assertEqual(len(deduped), 1)

    def test_dedupe_orders_by_signature_keeps_real_decimal_amount_distinct(self) -> None:
        orders = [
            OrderExtraction(
                fund_code="KB001",
                fund_name="KB Decimal",
                settle_class="CONFIRMED",
                order_type="SUB",
                base_date="2026-04-15",
                t_day=0,
                transfer_amount="50,572.49",
            ),
            OrderExtraction(
                fund_code="KB001",
                fund_name="KB Decimal",
                settle_class="CONFIRMED",
                order_type="SUB",
                base_date="2026-04-15",
                t_day=0,
                transfer_amount="50,572",
            ),
        ]

        deduped = self.extractor._dedupe_orders_by_signature(orders)

        self.assertEqual(len(deduped), 2)

    def test_normalize_settle_class_treats_business_day_headers_as_pending(self) -> None:
        settle_class = self.extractor._normalize_settle_class(
            value=None,
            t_day=None,
            evidence_label="익익영업일(T+2) / 이체예상금액",
        )

        self.assertEqual(settle_class.value, "PENDING")

    def test_evidence_implies_schedule_for_business_day_headers(self) -> None:
        self.assertTrue(self.extractor._evidence_implies_schedule("익영업일(T+1) / 이체예상금액"))
        self.assertTrue(self.extractor._evidence_implies_schedule("제3영업일(T+3) / 이체예상금액"))
        self.assertTrue(self.extractor._evidence_implies_schedule("설정금액 / 3월19일"))
        self.assertTrue(self.extractor._evidence_implies_schedule("해지금액 / 03/20"))

    def test_evidence_implies_schedule_does_not_treat_t_day_as_future_bucket(self) -> None:
        self.assertFalse(self.extractor._evidence_implies_schedule("T일 순유입 확정금액"))

    def test_normalize_settle_class_treats_korean_date_labels_as_pending(self) -> None:
        settle_class = self.extractor._normalize_settle_class(
            value="CONFIRMED",
            t_day=1,
            evidence_label="해지금액 / 3월19일",
        )

        self.assertEqual(settle_class.value, "PENDING")

    def test_normalize_settle_class_treats_t_day_execution_as_confirmed(self) -> None:
        settle_class = self.extractor._normalize_settle_class(
            value=None,
            t_day=0,
            evidence_label="T일 순유입 확정금액",
        )

        self.assertEqual(settle_class.value, "CONFIRMED")

    def test_prompt_bundle_loads_all_required_stages_from_yaml(self) -> None:
        prompt_bundle = _load_prompt_bundle()

        self.assertIn("You extract variable-annuity fund order data", prompt_bundle.system_prompt)
        self.assertIn("Stage {stage_number}/{total_stage_count}", prompt_bundle.user_prompt_template)
        self.assertEqual(prompt_bundle.stages["instruction_document"].number, 1)
        self.assertEqual(prompt_bundle.stages["fund_inventory"].number, 2)
        self.assertEqual(prompt_bundle.stages["base_date"].number, 3)
        self.assertEqual(prompt_bundle.stages["t_day"].number, 4)
        self.assertEqual(prompt_bundle.stages["transfer_amount"].number, 5)
        self.assertEqual(prompt_bundle.stages["settle_class"].number, 6)
        self.assertEqual(prompt_bundle.stages["order_type"].number, 7)

    def test_build_user_prompt_uses_yaml_template(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.prompt_bundle = _load_prompt_bundle()
        stage = extractor.prompt_bundle.stages["fund_inventory"]

        prompt = extractor._build_user_prompt(
            prompt_bundle=extractor.prompt_bundle,
            stage=stage,
            document_text="sample document",
            input_items=None,
            counterparty_guidance=None,
        )

        self.assertIn("Stage 2/7: Extract all distinct fund_code and fund_name pairs from the document.", prompt)
        self.assertIn("Stage name: fund_inventory", prompt)
        self.assertIn("Input items JSON:\nnull", prompt)
        self.assertIn("Document text:\nsample document", prompt)

    def test_build_user_prompt_prefixes_local_trace_task_id(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.prompt_bundle = _load_prompt_bundle()
        stage = extractor.prompt_bundle.stages["fund_inventory"]
        token = extractor_module._ACTIVE_EXTRACT_LOG_PATH.set(
            Path("/tmp/example_20260408_120000_llm_pipeline.log")
        )
        try:
            prompt = extractor._build_user_prompt(
                prompt_bundle=extractor.prompt_bundle,
                stage=stage,
                document_text="sample document",
                input_items=None,
                counterparty_guidance=None,
            )
        finally:
            extractor_module._ACTIVE_EXTRACT_LOG_PATH.reset(token)

        self.assertTrue(prompt.startswith("Trace task_id: example_20260408_120000\n\n"))

    def test_build_user_prompt_emphasizes_counterparty_guidance_for_sensitive_stage(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.prompt_bundle = _load_prompt_bundle()
        stage = extractor.prompt_bundle.stages["transfer_amount"]

        prompt = extractor._build_user_prompt(
            prompt_bundle=extractor.prompt_bundle,
            stage=stage,
            document_text="sample document",
            input_items=None,
            counterparty_guidance="Use 정산액 as the final net amount.",
        )

        self.assertIn("This counterparty-specific guidance is especially important for this stage.", prompt)
        self.assertIn("Prefer these counterparty-specific rules when multiple interpretations are plausible.", prompt)
        self.assertIn("Use 정산액 as the final net amount.", prompt)

    def test_build_user_prompt_keeps_plain_counterparty_guidance_for_non_sensitive_stage(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.prompt_bundle = _load_prompt_bundle()
        stage = extractor.prompt_bundle.stages["fund_inventory"]

        prompt = extractor._build_user_prompt(
            prompt_bundle=extractor.prompt_bundle,
            stage=stage,
            document_text="sample document",
            input_items=None,
            counterparty_guidance="Recognize BUY & SELL REPORT tables.",
        )

        self.assertIn("Recognize BUY & SELL REPORT tables.", prompt)
        self.assertNotIn("This counterparty-specific guidance is especially important for this stage.", prompt)

    def test_build_retry_user_prompt_includes_previous_output_and_target_issues(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.prompt_bundle = _load_prompt_bundle()
        extractor.llm_stage_issue_retry_attempts = 2
        stage = extractor.prompt_bundle.stages["t_day"]
        token = extractor_module._ACTIVE_EXTRACT_LOG_PATH.set(Path("/tmp/retry_trace_llm_pipeline.log"))
        try:
            prompt = extractor._build_retry_user_prompt(
                prompt_bundle=extractor.prompt_bundle,
                stage=stage,
                document_text="sample document",
                input_items=[{"fund_code": "6114", "fund_name": "인덱스주식형", "base_date": "2025-08-26"}],
                retry_context={
                    "attempt_number": 1,
                    "max_attempts": 2,
                    "target_issues": [
                        {
                            "raw_issue": "T_DAY_MISSING_FOR_6114",
                            "issue_code": "T_DAY_MISSING",
                            "fund_code": "6114",
                            "fund_name": "인덱스주식형",
                            "slot_id": None,
                            "evidence_label": None,
                            "stage_name": "t_day",
                        }
                    ],
                    "previous_output_items": [
                        {
                            "fund_code": "6114",
                            "fund_name": "인덱스주식형",
                            "base_date": "2025-08-26",
                            "t_day": None,
                            "slot_id": "",
                            "evidence_label": "설정금액",
                        }
                    ],
                    "focus_items": [
                        {
                            "fund_code": "6114",
                            "fund_name": "인덱스주식형",
                            "base_date": "2025-08-26",
                            "t_day": None,
                            "slot_id": "",
                            "evidence_label": "설정금액",
                        }
                    ],
                },
                counterparty_guidance=None,
            )
        finally:
            extractor_module._ACTIVE_EXTRACT_LOG_PATH.reset(token)
        self.assertTrue(prompt.startswith("Trace task_id: retry_trace\n\n"))
        self.assertIn("Retry 1/2", prompt)
        self.assertIn("T_DAY_MISSING_FOR_6114", prompt)
        self.assertIn('"issue_code": "T_DAY_MISSING"', prompt)
        self.assertIn("Previous output items JSON:", prompt)
        self.assertIn('"fund_code": "6114"', prompt)

    def test_normalize_stage_retry_issue_extracts_fund_code_from_detailed_issue(self) -> None:
        normalized = self.extractor._normalize_stage_retry_issue(
            stage_name="t_day",
            issue="T_DAY_MISSING_FOR_6114",
            input_items=[{"fund_code": "6114", "fund_name": "인덱스주식형", "base_date": "2025-08-26"}],
            previous_output_items=[
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "base_date": "2025-08-26",
                    "t_day": None,
                    "slot_id": "",
                    "evidence_label": "설정금액",
                }
            ],
        )

        self.assertEqual(normalized["issue_code"], "T_DAY_MISSING")
        self.assertEqual(normalized["fund_code"], "6114")
        self.assertEqual(normalized["fund_name"], "인덱스주식형")
        self.assertEqual(normalized["evidence_label"], "설정금액")

    def test_public_stage_findings_hide_internal_retry_findings(self) -> None:
        self.assertEqual(
            FundOrderExtractor._public_stage_findings(["_RETRY_FUND_DISCOVERY_PARTIAL", "BASE_DATE_MISSING"]),
            ["BASE_DATE_MISSING"],
        )

    def test_invoke_stage_with_issue_retry_builds_retry_context_after_first_issue(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.llm_stage_issue_retry_attempts = 2
        prompt_bundle = _load_prompt_bundle()
        stage = prompt_bundle.stages["base_date"]

        responses = iter(
            [
                types.SimpleNamespace(
                    parsed=FundBaseDateResult(
                        items=[FundBaseDateItem(fund_code="6114", fund_name="인덱스주식형", base_date=None)],
                        issues=["BASE_DATE_MISSING"],
                    ),
                    raw_response="first",
                ),
                types.SimpleNamespace(
                    parsed=FundBaseDateResult(
                        items=[FundBaseDateItem(fund_code="6114", fund_name="인덱스주식형", base_date="2025-08-26")],
                        issues=[],
                    ),
                    raw_response="second",
                ),
            ]
        )
        seen_retry_contexts: list[dict[str, object] | None] = []

        def fake_invoke_stage(
            self,
            *,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            batch_index,
            response_model,
            counterparty_guidance=None,
        ):
            active_context = getattr(self, "_active_stage_retry_context", None)
            seen_retry_contexts.append(None if active_context is None else dict(active_context))
            return next(responses)

        extractor._invoke_stage = types.MethodType(fake_invoke_stage, extractor)

        invocation, stage_issues, partial_issue = extractor._invoke_stage_with_issue_retry(
            prompt_bundle=prompt_bundle,
            stage=stage,
            document_text="sample document",
            input_items=[{"fund_code": "6114", "fund_name": "인덱스주식형"}],
            batch_index=1,
            response_model=FundBaseDateResult,
        )

        self.assertEqual(len(seen_retry_contexts), 2)
        self.assertIsNone(seen_retry_contexts[0])
        self.assertIsNotNone(seen_retry_contexts[1])
        retry_context = seen_retry_contexts[1]
        assert retry_context is not None
        self.assertEqual(retry_context["attempt_number"], 1)
        self.assertEqual(retry_context["max_attempts"], 2)
        self.assertEqual(len(retry_context["target_issues"]), 2)
        self.assertEqual(retry_context["target_issues"][0]["raw_issue"], "BASE_DATE_MISSING")
        self.assertEqual(retry_context["target_issues"][0]["issue_code"], "BASE_DATE_MISSING")
        self.assertEqual(retry_context["target_issues"][0]["fund_code"], "6114")
        self.assertEqual(retry_context["target_issues"][0]["fund_name"], "인덱스주식형")
        self.assertEqual(retry_context["target_issues"][1]["raw_issue"], "BASE_DATE_STAGE_PARTIAL")
        self.assertEqual(
            retry_context["previous_output_items"],
            [{"fund_code": "6114", "fund_name": "인덱스주식형", "base_date": None}],
        )
        self.assertEqual(
            retry_context["focus_items"],
            [{"fund_code": "6114", "fund_name": "인덱스주식형", "base_date": None}],
        )
        self.assertEqual(
            [(item.fund_code, item.base_date) for item in invocation.parsed.items],
            [("6114", "2025-08-26")],
        )
        self.assertEqual(stage_issues, [])
        self.assertIsNone(partial_issue)

    def test_derive_retry_focus_items_for_transfer_amount_partial_uses_missing_input_slots(self) -> None:
        focus_items = self.extractor._derive_retry_focus_items(
            stage_name="transfer_amount",
            document_text="irrelevant",
            input_items=[
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "base_date": "2025-08-26",
                    "t_day": 0,
                    "slot_id": "T0_SUB",
                    "evidence_label": "설정금액",
                },
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "base_date": "2025-08-26",
                    "t_day": 0,
                    "slot_id": "T0_RED",
                    "evidence_label": "해지금액",
                },
            ],
            previous_output_items=[
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "base_date": "2025-08-26",
                    "t_day": 0,
                    "slot_id": "T0_RED",
                    "evidence_label": "해지금액",
                    "transfer_amount": "52,671",
                }
            ],
            retry_target_issues=[
                {
                    "raw_issue": "TRANSFER_AMOUNT_STAGE_PARTIAL",
                    "issue_code": "TRANSFER_AMOUNT_STAGE_PARTIAL",
                    "fund_code": None,
                    "fund_name": None,
                    "slot_id": None,
                    "evidence_label": None,
                    "stage_name": "transfer_amount",
                }
            ],
        )

        self.assertEqual(
            focus_items,
            [
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "base_date": "2025-08-26",
                    "t_day": 0,
                    "slot_id": "T0_SUB",
                    "evidence_label": "설정금액",
                }
            ],
        )

    def test_derive_retry_focus_items_for_t_day_partial_uses_missing_deterministic_slots(self) -> None:
        document_text = "\n".join(
            [
                "| 펀드코드 | 펀드명 | 설정금액 | 해지금액 |",
                "| --- | --- | --- | --- |",
                "| 6114 | 인덱스주식형 | 1,106,000 | 52,671 |",
            ]
        )

        focus_items = self.extractor._derive_retry_focus_items(
            stage_name="t_day",
            document_text=document_text,
            input_items=[
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "base_date": "2025-08-26",
                }
            ],
            previous_output_items=[
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "base_date": "2025-08-26",
                    "t_day": 0,
                    "slot_id": "T0_RED",
                    "evidence_label": "해지금액",
                }
            ],
            retry_target_issues=[
                {
                    "raw_issue": "T_DAY_STAGE_PARTIAL",
                    "issue_code": "T_DAY_STAGE_PARTIAL",
                    "fund_code": None,
                    "fund_name": None,
                    "slot_id": None,
                    "evidence_label": None,
                    "stage_name": "t_day",
                }
            ],
        )

    def test_replace_incomplete_t_day_items_with_deterministic_document_slots(self) -> None:
        document_text = "\n".join(
            [
                "| 펀드코드 | 펀드명 | 설정금액 | 해지금액 |",
                "| --- | --- | --- | --- |",
                "| 6114 | 인덱스주식형 | 1,106,000 | 52,671 |",
            ]
        )
        replaced = self.extractor._replace_incomplete_t_day_items_with_deterministic_document_slots(
            document_text=document_text,
            input_items=[
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "base_date": "2025-08-26",
                }
            ],
            output_items=[
                FundSlotItem(
                    fund_code="6114",
                    fund_name="인덱스주식형",
                    base_date="2025-08-26",
                    t_day=0,
                    slot_id="T0_NET",
                    evidence_label="정산액",
                ),
                FundSlotItem(
                    fund_code="6114",
                    fund_name="인덱스주식형",
                    base_date="2025-08-26",
                    t_day=0,
                    slot_id="T0_RED_ALT",
                    evidence_label="출금금액",
                ),
            ],
        )

        self.assertEqual(
            [(item.slot_id, item.evidence_label) for item in replaced],
            [
                ("T0_SUB", "설정금액"),
                ("T0_RED", "해지금액"),
            ],
        )

    def test_replace_incomplete_order_type_items_with_deterministic_values(self) -> None:
        replaced = self.extractor._replace_incomplete_order_type_items_with_deterministic_values(
            input_items=[
                FundSettleItem(
                    fund_code="6114",
                    fund_name="인덱스주식형",
                    base_date="2025-08-26",
                    t_day=0,
                    slot_id="T0_NET",
                    evidence_label="정산액",
                    transfer_amount="-52,671",
                    settle_class="CONFIRMED",
                ),
                FundSettleItem(
                    fund_code="6114",
                    fund_name="인덱스주식형",
                    base_date="2025-08-26",
                    t_day=1,
                    slot_id="T1_SUB",
                    evidence_label="설정금액 / 익영업일",
                    transfer_amount="1,106,000",
                    settle_class="PENDING",
                ),
            ],
            output_items=[],
        )

        self.assertEqual(
            [(item.slot_id, item.order_type) for item in replaced],
            [("T0_NET", "RED"), ("T1_SUB", "SUB")],
        )

    def test_build_deterministic_resolved_items_from_settle_items(self) -> None:
        resolved = self.extractor._build_deterministic_resolved_items_from_settle_items(
            [
                FundSettleItem(
                    fund_code="6114",
                    fund_name="인덱스주식형",
                    base_date="2025-08-26",
                    t_day=0,
                    slot_id="T0_NET",
                    evidence_label="정산액",
                    transfer_amount="-52,671",
                    settle_class="CONFIRMED",
                ),
                FundSettleItem(
                    fund_code="6114",
                    fund_name="인덱스주식형",
                    base_date="2025-08-26",
                    t_day=1,
                    slot_id="T1_SUB",
                    evidence_label="설정금액 / 익영업일",
                    transfer_amount="1,106,000",
                    settle_class="PENDING",
                ),
            ]
        )

        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertEqual(
            [(item.slot_id, item.order_type) for item in resolved],
            [("T0_NET", "RED"), ("T1_SUB", "SUB")],
        )

    def test_build_deterministic_settle_items_from_amount_items(self) -> None:
        settle_items = self.extractor._build_deterministic_settle_items_from_amount_items(
            [
                FundAmountItem(
                    fund_code="6114",
                    fund_name="인덱스주식형",
                    base_date="2025-08-26",
                    t_day=0,
                    slot_id="T0_NET",
                    evidence_label="정산액",
                    transfer_amount="-52,671",
                ),
                FundAmountItem(
                    fund_code="6114",
                    fund_name="인덱스주식형",
                    base_date="2025-08-26",
                    t_day=2,
                    slot_id="T2_RED",
                    evidence_label="해지금액 / 8월 28일",
                    transfer_amount="1,106,000",
                ),
            ]
        )

        self.assertIsNotNone(settle_items)
        assert settle_items is not None
        self.assertEqual(
            [(item.slot_id, item.settle_class) for item in settle_items],
            [("T0_NET", "CONFIRMED"), ("T2_RED", "PENDING")],
        )

    def test_build_deterministic_resolved_items_from_settle_items_uses_document_backed_hint_for_unsigned_settlement(self) -> None:
        document_text = "\n".join(
            [
                "| 펀드코드 | 펀드명 | 설정액 | 해지액 | 정산액 |",
                "| --- | --- | --- | --- | --- |",
                "| BBI100 | 변액유니버셜종신-채권형 | 15,579,386 | -36,487,222 | -20,907,836 |",
            ]
        )
        raw_text = "\n".join(
            [
                "2026년 3월 18일",
                "정산내역",
                "펀드코드 | 펀드명 | 설정액 | 해지액 | 정산액",
                "BBI100 | 변액유니버셜종신-채권형 | 15,579,386 | -36,487,222 | -20,907,836",
            ]
        )

        resolved = self.extractor._build_deterministic_resolved_items_from_settle_items(
            [
                FundSettleItem(
                    fund_code="BBI100",
                    fund_name="변액유니버셜종신-채권형",
                    base_date="2026-03-18",
                    t_day=0,
                    slot_id="T0_NET",
                    evidence_label="정산액",
                    transfer_amount="20,907,836",
                    settle_class="CONFIRMED",
                ),
            ],
            document_text=document_text,
            raw_text=raw_text,
            target_fund_scope=None,
        )

        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertEqual([(item.slot_id, item.order_type) for item in resolved], [("T0_NET", "RED")])

    def test_replace_incomplete_order_type_items_with_deterministic_values_keeps_output_when_direction_is_ambiguous(self) -> None:
        existing = [
            FundResolvedItem(
                fund_code="H001",
                fund_name="Hybrid",
                base_date="2025-08-26",
                t_day=0,
                slot_id="T0_NET",
                evidence_label="펀드납입(인출)금액 / 판매회사분결제액",
                transfer_amount="100",
                settle_class="CONFIRMED",
                order_type="SUB",
            )
        ]

        replaced = self.extractor._replace_incomplete_order_type_items_with_deterministic_values(
            input_items=[
                FundSettleItem(
                    fund_code="H001",
                    fund_name="Hybrid",
                    base_date="2025-08-26",
                    t_day=0,
                    slot_id="T0_NET",
                    evidence_label="펀드납입(인출)금액 / 판매회사분결제액",
                    transfer_amount="100",
                    settle_class="CONFIRMED",
                ),
            ],
            output_items=existing,
        )

        self.assertIs(replaced, existing)

    def test_derive_retry_focus_items_for_t_day_partial_uses_expected_slots_when_current_has_ghost_slot(self) -> None:
        document_text = "\n".join(
            [
                "| 펀드코드 | 펀드명 | 설정금액 | 해지금액 |",
                "| --- | --- | --- | --- |",
                "| 6114 | 인덱스주식형 | 1,106,000 | 52,671 |",
            ]
        )

        focus_items = self.extractor._derive_retry_focus_items(
            stage_name="t_day",
            document_text=document_text,
            input_items=[
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "base_date": "2025-08-26",
                }
            ],
            previous_output_items=[
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "base_date": "2025-08-26",
                    "t_day": 0,
                    "slot_id": "T0_NET",
                    "evidence_label": "정산액",
                },
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "base_date": "2025-08-26",
                    "t_day": 0,
                    "slot_id": "T0_RED_ALT",
                    "evidence_label": "출금금액",
                },
            ],
            retry_target_issues=[
                {
                    "raw_issue": "T_DAY_STAGE_PARTIAL",
                    "issue_code": "T_DAY_STAGE_PARTIAL",
                    "fund_code": None,
                    "fund_name": None,
                    "slot_id": None,
                    "evidence_label": None,
                    "stage_name": "t_day",
                }
            ],
        )

        self.assertEqual(
            focus_items,
            [
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "base_date": "2025-08-26",
                    "t_day": 0,
                    "slot_id": "T0_SUB",
                    "evidence_label": "설정금액",
                    "retry_expected_family_slots": ["T0_RED", "T0_SUB"],
                },
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "base_date": "2025-08-26",
                    "t_day": 0,
                    "slot_id": "T0_RED",
                    "evidence_label": "해지금액",
                    "retry_expected_family_slots": ["T0_RED", "T0_SUB"],
                },
            ],
        )

    def test_build_stage_retry_context_refines_generic_t_day_issue_to_exact_focus_slots(self) -> None:
        document_text = "\n".join(
            [
                "| 펀드코드 | 펀드명 | 설정금액 | 해지금액 |",
                "| --- | --- | --- | --- |",
                "| 6114 | 인덱스주식형 | 1,106,000 | 52,671 |",
            ]
        )
        prompt_bundle = _load_prompt_bundle()
        stage = prompt_bundle.stages["t_day"]
        self.extractor.llm_stage_issue_retry_attempts = 3

        retry_context = self.extractor._build_stage_retry_context(
            stage=stage,
            document_text=document_text,
            input_items=[
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "base_date": "2025-08-26",
                }
            ],
            previous_parsed=FundSlotResult(
                items=[
                    FundSlotItem(
                        fund_code="6114",
                        fund_name="인덱스주식형",
                        base_date="2025-08-26",
                        t_day=0,
                        slot_id="T0_RED",
                        evidence_label="해지금액",
                    )
                ],
                issues=[],
            ),
            stage_issues=[],
            partial_issue="T_DAY_STAGE_PARTIAL",
            attempt_number=1,
        )

        assert retry_context is not None
        self.assertEqual(
            retry_context["target_issues"],
            [
                {
                    "raw_issue": "T_DAY_STAGE_PARTIAL",
                    "issue_code": "T_DAY_STAGE_PARTIAL",
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "slot_id": "T0_SUB",
                    "evidence_label": "설정금액",
                    "stage_name": "t_day",
                }
            ],
        )
        self.assertEqual(
            retry_context["focus_items"],
            [
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "base_date": "2025-08-26",
                    "t_day": 0,
                    "slot_id": "T0_SUB",
                    "evidence_label": "설정금액",
                    "retry_expected_family_slots": ["T0_RED", "T0_SUB"],
                }
            ],
        )

    def test_normalize_stage_retry_issue_keeps_generic_t_day_partial_without_slot_scope(self) -> None:
        normalized_issue = self.extractor._normalize_stage_retry_issue(
            stage_name="t_day",
            issue="T_DAY_STAGE_PARTIAL",
            input_items=[
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "base_date": "2025-08-26",
                }
            ],
            previous_output_items=[
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "base_date": "2025-08-26",
                    "t_day": 0,
                    "slot_id": "T0_RED",
                    "evidence_label": "해지금액",
                }
            ],
        )

        self.assertEqual(
            normalized_issue,
            {
                "raw_issue": "T_DAY_STAGE_PARTIAL",
                "issue_code": "T_DAY_STAGE_PARTIAL",
                "fund_code": "6114",
                "fund_name": "인덱스주식형",
                "slot_id": None,
                "evidence_label": None,
                "stage_name": "t_day",
            },
        )

    def test_build_stage_retry_context_refines_generic_transfer_amount_issue_to_exact_missing_slot(self) -> None:
        prompt_bundle = _load_prompt_bundle()
        stage = prompt_bundle.stages["transfer_amount"]
        self.extractor.llm_stage_issue_retry_attempts = 3

        retry_context = self.extractor._build_stage_retry_context(
            stage=stage,
            document_text="irrelevant",
            input_items=[
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "base_date": "2025-08-26",
                    "t_day": 0,
                    "slot_id": "T0_SUB",
                    "evidence_label": "설정금액",
                },
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "base_date": "2025-08-26",
                    "t_day": 0,
                    "slot_id": "T0_RED",
                    "evidence_label": "해지금액",
                },
            ],
            previous_parsed=FundAmountResult(
                items=[
                    FundAmountItem(
                        fund_code="6114",
                        fund_name="인덱스주식형",
                        base_date="2025-08-26",
                        t_day=0,
                        slot_id="T0_RED",
                        evidence_label="해지금액",
                        transfer_amount="52,671",
                    )
                ],
                issues=[],
            ),
            stage_issues=[],
            partial_issue="TRANSFER_AMOUNT_STAGE_PARTIAL",
            attempt_number=1,
        )

        assert retry_context is not None
        self.assertEqual(
            retry_context["target_issues"],
            [
                {
                    "raw_issue": "TRANSFER_AMOUNT_STAGE_PARTIAL",
                    "issue_code": "TRANSFER_AMOUNT_STAGE_PARTIAL",
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "slot_id": "T0_SUB",
                    "evidence_label": "설정금액",
                    "stage_name": "transfer_amount",
                }
            ],
        )

        self.assertEqual(
            retry_context["focus_items"],
            [
                {
                    "fund_code": "6114",
                    "fund_name": "인덱스주식형",
                    "base_date": "2025-08-26",
                    "t_day": 0,
                    "slot_id": "T0_SUB",
                    "evidence_label": "설정금액",
                }
            ],
        )

    def test_load_prompt_bundle_rejects_unknown_user_prompt_placeholder(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            prompt_path = Path(temp_dir) / "invalid_prompts.yaml"
            self._write_prompt_yaml(
                prompt_path,
                user_prompt_template="Stage {stage_num}/{total_stage_count}\nDocument text:\n{document_text}",
            )

            with self.assertRaisesRegex(ValueError, "unknown user prompt placeholders"):
                _load_prompt_bundle(prompt_path)

    def test_load_prompt_bundle_rejects_missing_user_prompt_placeholder(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            prompt_path = Path(temp_dir) / "invalid_prompts.yaml"
            self._write_prompt_yaml(
                prompt_path,
                user_prompt_template=(
                    "Stage {stage_number}/{total_stage_count}\n"
                    "Stage goal: {stage_goal}\n"
                    "Stage name: {stage_name}\n"
                    "Stage instructions: {stage_instructions}\n"
                    "Output contract: {output_contract}\n"
                    "Document text:\n{document_text}"
                ),
            )

            with self.assertRaisesRegex(ValueError, "missing user prompt placeholders"):
                _load_prompt_bundle(prompt_path)

    def test_refresh_prompt_bundle_reloads_when_yaml_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            prompt_path = Path(temp_dir) / "reload_prompts.yaml"
            user_prompt_template = (
                "Stage {stage_number}/{total_stage_count}\n"
                "Stage goal: {stage_goal}\n"
                "Stage name: {stage_name}\n"
                "Stage instructions: {stage_instructions}\n"
                "Output contract: {output_contract}\n"
                "Input items JSON:\n{input_items_json}\n"
                "Document text:\n{document_text}"
            )
            self._write_prompt_yaml(prompt_path, user_prompt_template=user_prompt_template, system_prompt="prompt v1")

            extractor = object.__new__(FundOrderExtractor)
            extractor.prompt_path = prompt_path
            extractor._prompt_mtime_ns = -1
            extractor.prompt_bundle = None
            extractor.system_prompt = ""

            extractor._refresh_prompt_bundle(force=True)
            self.assertEqual(extractor.system_prompt, "prompt v1")

            time.sleep(0.02)
            self._write_prompt_yaml(prompt_path, user_prompt_template=user_prompt_template, system_prompt="prompt v2")

            extractor._refresh_prompt_bundle()
            self.assertEqual(extractor.system_prompt, "prompt v2")

    def test_refresh_prompt_bundle_keeps_last_good_bundle_when_yaml_is_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            prompt_path = Path(temp_dir) / "reload_prompts.yaml"
            valid_template = (
                "Stage {stage_number}/{total_stage_count}\n"
                "Stage goal: {stage_goal}\n"
                "Stage name: {stage_name}\n"
                "Stage instructions: {stage_instructions}\n"
                "Output contract: {output_contract}\n"
                "Input items JSON:\n{input_items_json}\n"
                "Document text:\n{document_text}"
            )
            self._write_prompt_yaml(prompt_path, user_prompt_template=valid_template, system_prompt="prompt v1")

            extractor = object.__new__(FundOrderExtractor)
            extractor.prompt_path = prompt_path
            extractor._prompt_mtime_ns = -1
            extractor.prompt_bundle = None
            extractor.system_prompt = ""

            extractor._refresh_prompt_bundle(force=True)
            original_bundle = extractor.prompt_bundle

            time.sleep(0.02)
            self._write_prompt_yaml(
                prompt_path,
                user_prompt_template="Stage {stage_num}/{total_stage_count}\nDocument text:\n{document_text}",
                system_prompt="prompt broken",
            )

            reloaded_bundle = extractor._refresh_prompt_bundle()

            self.assertIs(reloaded_bundle, original_bundle)
            self.assertIs(extractor.prompt_bundle, original_bundle)
            self.assertEqual(extractor.system_prompt, "prompt v1")

    def test_extract_uses_one_prompt_snapshot_for_all_stages(self) -> None:
        prompt_bundle_v1 = _load_prompt_bundle()
        prompt_bundle_v2 = _load_prompt_bundle()
        prompt_bundle_v2 = type(prompt_bundle_v2)(
            system_prompt="changed system prompt",
            user_prompt_template=prompt_bundle_v2.user_prompt_template,
            retry_user_prompt_template=prompt_bundle_v2.retry_user_prompt_template,
            stages={
                name: type(stage)(
                    number=stage.number,
                    name=stage.name,
                    goal=f"changed {stage.goal}",
                    instructions=stage.instructions,
                    output_contract=stage.output_contract,
                    retry_instructions=stage.retry_instructions,
                )
                for name, stage in prompt_bundle_v2.stages.items()
            },
        )

        extractor = object.__new__(FundOrderExtractor)
        extractor.prompt_bundle = prompt_bundle_v2
        extractor.system_prompt = prompt_bundle_v2.system_prompt
        extractor.stage_batch_size = 10

        def fake_refresh(self, force: bool = False):
            """요청 시작 시점에는 v1 prompt snapshot만 보이도록 고정한다."""
            return prompt_bundle_v1

        seen_stage_goals: list[str] = []

        def fake_extract_fund_seeds(
            self,
            prompt_bundle,
            chunks,
            raw_text,
            issues,
            artifacts,
            *,
            target_fund_scope=None,
            counterparty_guidance=None,
        ):
            """Stage 1 이후 prompt 파일이 바뀌어도 이번 요청은 기존 snapshot을 쓰게 만든다."""
            self.prompt_bundle = prompt_bundle_v2
            self.system_prompt = prompt_bundle_v2.system_prompt
            self._prompt_mtime_ns = 999
            return [FundSeedItem(fund_code="F001", fund_name="Alpha")]

        def fake_run_batched_stage(
            self,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            response_model,
            issues,
            artifacts,
            *,
            counterparty_guidance=None,
        ):
            """각 stage가 v1 snapshot goal을 계속 사용했는지 기록한다."""
            seen_stage_goals.append(stage.goal)
            if stage.name == "base_date":
                return [FundBaseDateItem(fund_code="F001", fund_name="Alpha", base_date="2025-11-27")]
            if stage.name == "t_day":
                return [FundSlotItem(fund_code="F001", fund_name="Alpha", base_date="2025-11-27", t_day=0, slot_id="T0_NET", evidence_label="당일이체금액")]
            if stage.name == "transfer_amount":
                return [FundAmountItem(fund_code="F001", fund_name="Alpha", base_date="2025-11-27", t_day=0, slot_id="T0_NET", evidence_label="당일이체금액", transfer_amount="100")]
            if stage.name == "settle_class":
                return [FundSettleItem(fund_code="F001", fund_name="Alpha", base_date="2025-11-27", t_day=0, slot_id="T0_NET", evidence_label="당일이체금액", transfer_amount="100", settle_class="CONFIRMED")]
            if stage.name == "order_type":
                return [FundResolvedItem(fund_code="F001", fund_name="Alpha", base_date="2025-11-27", t_day=0, slot_id="T0_NET", evidence_label="당일이체금액", transfer_amount="100", settle_class="CONFIRMED", order_type="SUB")]
            raise AssertionError(f"Unexpected stage: {stage.name}")

        extractor._refresh_prompt_bundle = types.MethodType(fake_refresh, extractor)
        extractor._extract_fund_seeds = types.MethodType(fake_extract_fund_seeds, extractor)
        extractor._run_batched_stage = types.MethodType(fake_run_batched_stage, extractor)
        extractor._classify_instruction_document = types.MethodType(
            lambda self, prompt_bundle, document_text, artifacts, *, counterparty_guidance=None: (True, None),
            extractor,
        )
        extractor._resolve_document_base_date_for_seeds = types.MethodType(
            lambda self, **kwargs: [FundBaseDateItem(fund_code="F001", fund_name="Alpha", base_date="2025-11-27")],
            extractor,
        )

        outcome = extractor.extract(["sample doc"])

        self.assertEqual(len(outcome.result.orders), 1)
        self.assertTrue(all(goal.startswith(("Resolve", "Enumerate")) or "Extract all distinct fund_code" in goal for goal in seen_stage_goals))
        self.assertTrue(all(not goal.startswith("changed ") for goal in seen_stage_goals))

    def test_extract_recovers_direct_orders_when_fund_inventory_is_empty(self) -> None:
        document_text = (
            "Structured markdown view:\n"
            "## EML sample\n\n"
            "Raw text backup:\n"
            "```text\n"
            "BUY & SELL REPORT |  |  |  |  |  |  |  |  |  |\n"
            "Date | Buy&Sell | External Fund Manager | Fund Code | Fund Name | Fund Price | Buy |  | Sell |  | Custodian Bank\n"
            " |  |  |  |  |  | Amount | Unit | Amount | Unit |\n"
            "11-28-2025 | Buy&Sell | 삼성액티브 | 151128 | AIA VUL 주식형(1형)(삼성) | 1785.89 | 1,311,285 |  | 55,901,379 |  | \n"
            "Total |  |  |  |  |  | 1,311,285 |  | 55,901,379 |  |\n"
            "```\n"
        )

        extractor = object.__new__(FundOrderExtractor)
        extractor.prompt_bundle = _load_prompt_bundle()
        extractor.system_prompt = extractor.prompt_bundle.system_prompt
        extractor.stage_batch_size = 10
        extractor.llm_chunk_size_chars = 1200
        extractor._refresh_prompt_bundle = types.MethodType(lambda self, force=False: self.prompt_bundle, extractor)
        extractor._extract_fund_seeds = types.MethodType(
            lambda self, prompt_bundle, chunks, raw_text, issues, artifacts, *, target_fund_scope=None, counterparty_guidance=None: [],
            extractor,
        )
        extractor._classify_instruction_document = types.MethodType(
            lambda self, prompt_bundle, document_text, artifacts, *, counterparty_guidance=None: (True, None),
            extractor,
        )

        outcome = extractor.extract([document_text])

        self.assertEqual(len(outcome.result.orders), 2)
        self.assertEqual(outcome.result.issues, [])

    def test_extract_recovers_direct_orders_when_t_day_stage_is_empty(self) -> None:
        document_text = (
            "Structured markdown view:\n"
            "## EML sample\n\n"
            "Raw text backup:\n"
            "```text\n"
            "BUY & SELL REPORT |  |  |  |  |  |  |  |  |  |\n"
            "Date | Buy&Sell | External Fund Manager | Fund Code | Fund Name | Fund Price | Buy |  | Sell |  | Custodian Bank\n"
            " |  |  |  |  |  | Amount | Unit | Amount | Unit |\n"
            "11-28-2025 | Buy&Sell | 삼성액티브 | 151128 | AIA VUL 주식형(1형)(삼성) | 1785.89 | 1,311,285 |  | 55,901,379 |  | \n"
            "Total |  |  |  |  |  | 1,311,285 |  | 55,901,379 |  |\n"
            "```\n"
        )

        extractor = object.__new__(FundOrderExtractor)
        extractor.prompt_bundle = _load_prompt_bundle()
        extractor.system_prompt = extractor.prompt_bundle.system_prompt
        extractor.stage_batch_size = 10
        extractor.llm_chunk_size_chars = 1200
        extractor._refresh_prompt_bundle = types.MethodType(lambda self, force=False: self.prompt_bundle, extractor)
        extractor._extract_fund_seeds = types.MethodType(
            lambda self, prompt_bundle, chunks, raw_text, issues, artifacts, *, target_fund_scope=None, counterparty_guidance=None: [
                FundSeedItem(fund_code="151128", fund_name="AIA VUL 주식형(1형)(삼성)")
            ],
            extractor,
        )
        extractor._classify_instruction_document = types.MethodType(
            lambda self, prompt_bundle, document_text, artifacts, *, counterparty_guidance=None: (True, None),
            extractor,
        )
        extractor._resolve_document_base_date_for_seeds = types.MethodType(
            lambda self, **kwargs: [
                FundBaseDateItem(fund_code="151128", fund_name="AIA VUL 주식형(1형)(삼성)", base_date="2025-11-28")
            ],
            extractor,
        )

        def fake_run_batched_stage(
            self,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            response_model,
            issues,
            artifacts,
            *,
            counterparty_guidance=None,
        ):
            if stage.name == "t_day":
                return []
            raise AssertionError(f"Unexpected stage: {stage.name}")

        extractor._run_batched_stage = types.MethodType(fake_run_batched_stage, extractor)

        outcome = extractor.extract([document_text])

        self.assertEqual(len(outcome.result.orders), 2)
        self.assertEqual(outcome.result.issues, [])

    def test_extract_stops_when_instruction_document_stage_marks_non_instruction(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.prompt_bundle = _load_prompt_bundle()
        extractor.system_prompt = extractor.prompt_bundle.system_prompt
        extractor.stage_batch_size = 10
        extractor.llm_chunk_size_chars = 1200
        extractor._refresh_prompt_bundle = types.MethodType(lambda self, force=False: self.prompt_bundle, extractor)
        extractor._classify_instruction_document = types.MethodType(
            lambda self, prompt_bundle, document_text, artifacts, *, counterparty_guidance=None: (False, "cover email only"),
            extractor,
        )
        extractor._extract_fund_seeds = types.MethodType(
            lambda self, prompt_bundle, chunks, raw_text, issues, artifacts, *, target_fund_scope=None, counterparty_guidance=None: [FundSeedItem(fund_code="F001", fund_name="Alpha")],
            extractor,
        )

        outcome = extractor.extract(["wrapper email body"])

        self.assertEqual(outcome.result.orders, [])
        self.assertEqual(outcome.result.issues, ["DOCUMENT_NOT_INSTRUCTION: cover email only"])

    def test_extract_uses_markdown_shortcut_after_base_date(self) -> None:
        document_text = (
            "## EML [흥국생명] 설정해지 내역 운용지시건-삼성-0413\n\n"
            "| 펀드코드 | 추가설정금액 | 당일인출금액 | 해지신청 | 비고 |\n"
            "| --- | --- | --- | --- | --- |\n"
            "| 450038 | 0.4억 | / | / | / |\n"
            "| 450036 | / | 0.8억 | 0.5억 | 0.8억 > 10일 신청건 0.5억 > 15일 인출예정 |\n"
            "| 450033 | / | 0.3억 | / | 10일 신청건 |\n"
        )
        raw_text = "Date (Asia/Seoul): 2026-04-13\n"

        extractor = object.__new__(FundOrderExtractor)
        extractor.prompt_bundle = _load_prompt_bundle()
        extractor.system_prompt = extractor.prompt_bundle.system_prompt
        extractor.stage_batch_size = 10
        extractor.llm_chunk_size_chars = 1200
        extractor._refresh_prompt_bundle = types.MethodType(lambda self, force=False: self.prompt_bundle, extractor)
        extractor._classify_instruction_document = types.MethodType(
            lambda self, prompt_bundle, document_text, artifacts, *, counterparty_guidance=None: (True, None),
            extractor,
        )
        extractor._extract_fund_seeds = types.MethodType(
            lambda self, prompt_bundle, chunks, raw_text, issues, artifacts, *, target_fund_scope=None, counterparty_guidance=None: [
                FundSeedItem(fund_code="450038", fund_name="-"),
                FundSeedItem(fund_code="450036", fund_name="-"),
                FundSeedItem(fund_code="450033", fund_name="-"),
            ],
            extractor,
        )

        def fake_run_batched_stage(
            self,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            response_model,
            issues,
            artifacts,
            *,
            counterparty_guidance=None,
        ):
            raise AssertionError(f"Unexpected stage during markdown shortcut path: {stage.name}")

        extractor._run_batched_stage = types.MethodType(fake_run_batched_stage, extractor)

        outcome = extractor.extract(
            [document_text],
            raw_text=raw_text,
            markdown_text=document_text,
            expected_order_count=4,
        )

        self.assertEqual(len(outcome.result.orders), 4)
        self.assertEqual(outcome.result.issues, [])
        self.assertEqual(
            sorted((order.fund_code, order.t_day, order.settle_class.value) for order in outcome.result.orders),
            [
                ("450033", 0, "CONFIRMED"),
                ("450036", 0, "CONFIRMED"),
                ("450036", 2, "PENDING"),
                ("450038", 0, "CONFIRMED"),
            ],
        )

    def test_extract_without_expected_order_count_does_not_use_markdown_shortcut(self) -> None:
        document_text = (
            "## EML [흥국생명] 설정해지 내역 운용지시건-삼성-0413\n\n"
            "| 펀드코드 | 추가설정금액 |\n"
            "| --- | --- |\n"
            "| 450038 | 0.4억 |\n"
        )
        raw_text = "Date (Asia/Seoul): 2026-04-13\n"

        extractor = object.__new__(FundOrderExtractor)
        extractor.prompt_bundle = _load_prompt_bundle()
        extractor.system_prompt = extractor.prompt_bundle.system_prompt
        extractor.stage_batch_size = 10
        extractor.llm_chunk_size_chars = 1200
        extractor._refresh_prompt_bundle = types.MethodType(lambda self, force=False: self.prompt_bundle, extractor)
        extractor._classify_instruction_document = types.MethodType(
            lambda self, prompt_bundle, document_text, artifacts, *, counterparty_guidance=None: (True, None),
            extractor,
        )
        extractor._extract_fund_seeds = types.MethodType(
            lambda self, prompt_bundle, chunks, raw_text, issues, artifacts, *, target_fund_scope=None, counterparty_guidance=None: [
                FundSeedItem(fund_code="450038", fund_name="-"),
            ],
            extractor,
        )
        seen_stages: list[str] = []

        def fake_run_batched_stage(
            self,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            response_model,
            issues,
            artifacts,
            *,
            counterparty_guidance=None,
        ):
            seen_stages.append(stage.name)
            if stage.name == "t_day":
                return [FundSlotItem(fund_code="450038", fund_name="-", base_date="2026-04-13", t_day=0, slot_id="T0_SUB", evidence_label="추가설정금액")]
            if stage.name == "transfer_amount":
                return [FundAmountItem(fund_code="450038", fund_name="-", base_date="2026-04-13", t_day=0, slot_id="T0_SUB", evidence_label="추가설정금액", transfer_amount="40,000,000")]
            raise AssertionError(f"Unexpected stage after transfer_amount: {stage.name}")

        extractor._run_batched_stage = types.MethodType(fake_run_batched_stage, extractor)

        outcome = extractor.extract(
            [document_text],
            raw_text=raw_text,
            markdown_text=document_text,
        )

        self.assertEqual(seen_stages, ["t_day", "transfer_amount"])
        self.assertEqual(len(outcome.result.orders), 1)
        self.assertEqual(outcome.result.orders[0].fund_code, "450038")

    def test_extract_falls_back_to_staged_path_when_markdown_shortcut_is_not_safe(self) -> None:
        document_text = (
            "## EML [흥국생명] 설정해지 내역 운용지시건-삼성-0413\n\n"
            "| 펀드코드 | 추가설정금액 | 당일인출금액 | 해지신청 | 비고 |\n"
            "| --- | --- | --- | --- | --- |\n"
            "| 450036 | / | 0.8억 | 0.5억 | 0.8억 > 10일 신청건 |\n"
        )
        raw_text = "Date (Asia/Seoul): 2026-04-13\n"

        extractor = object.__new__(FundOrderExtractor)
        extractor.prompt_bundle = _load_prompt_bundle()
        extractor.system_prompt = extractor.prompt_bundle.system_prompt
        extractor.stage_batch_size = 10
        extractor.llm_chunk_size_chars = 1200
        extractor._refresh_prompt_bundle = types.MethodType(lambda self, force=False: self.prompt_bundle, extractor)
        extractor._classify_instruction_document = types.MethodType(
            lambda self, prompt_bundle, document_text, artifacts, *, counterparty_guidance=None: (True, None),
            extractor,
        )
        extractor._extract_fund_seeds = types.MethodType(
            lambda self, prompt_bundle, chunks, raw_text, issues, artifacts, *, target_fund_scope=None, counterparty_guidance=None: [
                FundSeedItem(fund_code="450036", fund_name="-"),
            ],
            extractor,
        )
        seen_stages: list[str] = []

        def fake_run_batched_stage(
            self,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            response_model,
            issues,
            artifacts,
            *,
            counterparty_guidance=None,
        ):
            seen_stages.append(stage.name)
            if stage.name == "t_day":
                return [FundSlotItem(fund_code="450036", fund_name="-", base_date="2026-04-13", t_day=0, slot_id="T0_RED", evidence_label="당일인출금액")]
            if stage.name == "transfer_amount":
                return [FundAmountItem(fund_code="450036", fund_name="-", base_date="2026-04-13", t_day=0, slot_id="T0_RED", evidence_label="당일인출금액", transfer_amount="80,000,000")]
            if stage.name == "settle_class":
                return [FundSettleItem(fund_code="450036", fund_name="-", base_date="2026-04-13", t_day=0, slot_id="T0_RED", evidence_label="당일인출금액", transfer_amount="80,000,000", settle_class="CONFIRMED")]
            if stage.name == "order_type":
                return [FundResolvedItem(fund_code="450036", fund_name="-", base_date="2026-04-13", t_day=0, slot_id="T0_RED", evidence_label="당일인출금액", transfer_amount="80,000,000", settle_class="CONFIRMED", order_type="RED")]
            raise AssertionError(f"Unexpected stage: {stage.name}")

        extractor._run_batched_stage = types.MethodType(fake_run_batched_stage, extractor)

        outcome = extractor.extract(
            [document_text],
            raw_text=raw_text,
            markdown_text=document_text,
            expected_order_count=2,
        )

        self.assertEqual(seen_stages, ["t_day", "transfer_amount"])
        self.assertEqual(len(outcome.result.orders), 1)

    def test_extract_falls_back_to_legacy_base_date_batches_when_document_level_path_returns_none(self) -> None:
        document_text = "sample doc without explicit base date"
        raw_text = "sample doc without explicit base date"

        extractor = object.__new__(FundOrderExtractor)
        extractor.prompt_bundle = _load_prompt_bundle()
        extractor.system_prompt = extractor.prompt_bundle.system_prompt
        extractor.stage_batch_size = 10
        extractor.llm_chunk_size_chars = 1200
        extractor._refresh_prompt_bundle = types.MethodType(lambda self, force=False: self.prompt_bundle, extractor)
        extractor._classify_instruction_document = types.MethodType(
            lambda self, prompt_bundle, document_text, artifacts, *, counterparty_guidance=None: (True, None),
            extractor,
        )
        extractor._extract_fund_seeds = types.MethodType(
            lambda self, prompt_bundle, chunks, raw_text, issues, artifacts, *, target_fund_scope=None, counterparty_guidance=None: [
                FundSeedItem(fund_code="F001", fund_name="Alpha"),
            ],
            extractor,
        )
        extractor._resolve_document_base_date_for_seeds = types.MethodType(
            lambda self, **kwargs: None,
            extractor,
        )
        seen_stages: list[str] = []

        def fake_run_batched_stage(
            self,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            response_model,
            issues,
            artifacts,
            *,
            counterparty_guidance=None,
        ):
            seen_stages.append(stage.name)
            if stage.name == "base_date":
                return [FundBaseDateItem(fund_code="F001", fund_name="Alpha", base_date="2025-11-27")]
            if stage.name == "t_day":
                return [FundSlotItem(fund_code="F001", fund_name="Alpha", base_date="2025-11-27", t_day=0, slot_id="T0_NET", evidence_label="당일이체금액")]
            if stage.name == "transfer_amount":
                return [FundAmountItem(fund_code="F001", fund_name="Alpha", base_date="2025-11-27", t_day=0, slot_id="T0_NET", evidence_label="당일이체금액", transfer_amount="100")]
            if stage.name == "order_type":
                return [FundResolvedItem(fund_code="F001", fund_name="Alpha", base_date="2025-11-27", t_day=0, slot_id="T0_NET", evidence_label="당일이체금액", transfer_amount="100", settle_class="CONFIRMED", order_type="SUB")]
            raise AssertionError(f"Unexpected stage after order_type: {stage.name}")

        extractor._run_batched_stage = types.MethodType(fake_run_batched_stage, extractor)

        outcome = extractor.extract(
            [document_text],
            raw_text=raw_text,
        )

        self.assertEqual(seen_stages, ["base_date", "t_day", "transfer_amount", "order_type"])
        self.assertEqual(len(outcome.result.orders), 1)

    def test_extract_uses_structure_shortcut_with_section_nav_date_over_document_date(self) -> None:
        raw_text = (
            "[PAGE 1]\n"
            "Document Date: 2025-11-28\n"
            "The order of Subscription and Redemption\n"
            "1. Subscription\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Future Mobility Active ETF FoF FME 20,751,062.2603 27-Nov-25 1,032.61 21,427,754 HANA\n"
            "Global Bond FoF II GBE 1,937,022.7838 27-Nov-25 1,249.43 2,420,174 HANA\n"
            "2. Redemption\n"
            "Fund Name Code No. of Unit NAV Date NAV Amount(KRW) Bank\n"
            "Bond ETF FoF BEE 185,714,727.6942 27-Nov-25 1,120.91 208,169,495 SCFB\n"
        )

        extractor = object.__new__(FundOrderExtractor)
        extractor.prompt_bundle = _load_prompt_bundle()
        extractor.system_prompt = extractor.prompt_bundle.system_prompt
        extractor.stage_batch_size = 10
        extractor.llm_chunk_size_chars = 1200
        extractor._refresh_prompt_bundle = types.MethodType(lambda self, force=False: self.prompt_bundle, extractor)
        extractor._classify_instruction_document = types.MethodType(
            lambda self, prompt_bundle, document_text, artifacts, *, counterparty_guidance=None: (True, None),
            extractor,
        )
        extractor._extract_fund_seeds = types.MethodType(
            lambda self, prompt_bundle, chunks, raw_text, issues, artifacts, *, target_fund_scope=None, counterparty_guidance=None: [
                FundSeedItem(fund_code="FME", fund_name="Future Mobility Active ETF FoF"),
                FundSeedItem(fund_code="GBE", fund_name="Global Bond FoF II"),
                FundSeedItem(fund_code="BEE", fund_name="Bond ETF FoF"),
            ],
            extractor,
        )

        def fake_run_batched_stage(
            self,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            response_model,
            issues,
            artifacts,
            *,
            counterparty_guidance=None,
        ):
            raise AssertionError(f"Unexpected stage during structure shortcut path: {stage.name}")

        extractor._run_batched_stage = types.MethodType(fake_run_batched_stage, extractor)

        outcome = extractor.extract(
            ["section ledger body"],
            raw_text=raw_text,
            expected_order_count=3,
        )

        self.assertEqual(
            sorted((order.fund_code, order.base_date, order.order_type.value) for order in outcome.result.orders),
            [
                ("BEE", "2025-11-27", "RED"),
                ("FME", "2025-11-27", "SUB"),
                ("GBE", "2025-11-27", "SUB"),
            ],
        )
        self.assertEqual(outcome.result.issues, [])

    def test_invoke_stage_retries_json_mode_before_falling_back(self) -> None:
        class FakeBoundLLM:
            """JSON mode 재시도 횟수를 기록하는 최소 bound client다."""

            def __init__(self, responses):
                """미리 준비한 응답 목록을 순서대로 소비하도록 초기화한다."""
                self.responses = list(responses)
                self.calls = 0

            def invoke(self, messages):
                """현재 호출 차수에 해당하는 응답 또는 예외를 반환한다."""
                response = self.responses[self.calls]
                self.calls += 1
                if isinstance(response, Exception):
                    raise response
                return response

        class FakeLLM:
            """JSON mode와 prompt-only mode를 분리해 재현하는 최소 fake LLM이다."""

            def __init__(self, json_responses, prompt_responses):
                """두 호출 모드별 응답 시퀀스를 저장한다."""
                self.bound = FakeBoundLLM(json_responses)
                self.prompt_responses = list(prompt_responses)
                self.prompt_calls = 0

            def bind(self, **kwargs):
                """JSON mode 호출용 bound client를 돌려준다."""
                return self.bound

            def invoke(self, messages):
                """prompt-only fallback 호출용 응답을 순서대로 반환한다."""
                response = self.prompt_responses[self.prompt_calls]
                self.prompt_calls += 1
                if isinstance(response, Exception):
                    raise response
                return response

        extractor = object.__new__(FundOrderExtractor)
        extractor.llm_retry_attempts = 3
        extractor.llm_retry_backoff_seconds = 0
        extractor.llm = FakeLLM(
            json_responses=[
                RuntimeError("Connection error"),
                types.SimpleNamespace(content='{"items":[{"fund_code":"F001","fund_name":"Alpha"}],"issues":[]}'),
            ],
            prompt_responses=[],
        )
        prompt_bundle = _load_prompt_bundle()
        stage = prompt_bundle.stages["fund_inventory"]

        with patch("app.extractor.time.sleep", return_value=None):
            invocation = extractor._invoke_stage(
                prompt_bundle=prompt_bundle,
                stage=stage,
                document_text="sample",
                input_items=None,
                batch_index=1,
                response_model=FundSeedResult,
                counterparty_guidance=None,
            )

        self.assertIsNotNone(invocation.parsed)
        self.assertEqual(len(invocation.parsed.items), 1)
        self.assertEqual(invocation.parsed.items[0].fund_code, "F001")
        self.assertEqual(extractor.llm.bound.calls, 2)
        self.assertEqual(extractor.llm.prompt_calls, 0)

    def test_invoke_stage_retries_prompt_only_mode_after_json_failures(self) -> None:
        class FakeBoundLLM:
            """JSON mode 실패를 반복 재현하는 최소 bound client다."""

            def __init__(self, responses):
                """응답 시퀀스를 저장하고 호출 횟수를 기록한다."""
                self.responses = list(responses)
                self.calls = 0

            def invoke(self, messages):
                """현재 차수의 JSON mode 응답 또는 예외를 반환한다."""
                response = self.responses[self.calls]
                self.calls += 1
                if isinstance(response, Exception):
                    raise response
                return response

            def bind(self, **kwargs):
                """bound client 위에 추가 bind가 와도 그대로 자신을 돌려준다."""
                return self

        class FakeLLM:
            """JSON mode 실패 후 prompt-only fallback 성공을 재현하는 fake LLM이다."""

            def __init__(self, json_responses, prompt_responses):
                """두 모드의 응답 시퀀스를 각각 저장한다."""
                self.bound = FakeBoundLLM(json_responses)
                self.prompt_responses = list(prompt_responses)
                self.prompt_calls = 0

            def bind(self, **kwargs):
                """prompt client와 JSON mode bound client를 분리해 돌려준다."""
                if "response_format" in kwargs:
                    return self.bound
                return self

            def invoke(self, messages):
                """prompt-only fallback 응답을 순서대로 반환한다."""
                response = self.prompt_responses[self.prompt_calls]
                self.prompt_calls += 1
                if isinstance(response, Exception):
                    raise response
                return response

        extractor = object.__new__(FundOrderExtractor)
        extractor.llm_retry_attempts = 2
        extractor.llm_retry_backoff_seconds = 0
        extractor.llm = FakeLLM(
            json_responses=[
                RuntimeError("Connection error"),
                RuntimeError("Connection error"),
            ],
            prompt_responses=[
                RuntimeError("Connection error"),
                types.SimpleNamespace(content='{"items":[{"fund_code":"F002","fund_name":"Beta"}],"issues":[]}'),
            ],
        )
        prompt_bundle = _load_prompt_bundle()
        stage = prompt_bundle.stages["fund_inventory"]

        with patch("app.extractor.time.sleep", return_value=None):
            invocation = extractor._invoke_stage(
                prompt_bundle=prompt_bundle,
                stage=stage,
                document_text="sample",
                input_items=None,
                batch_index=1,
                response_model=FundSeedResult,
                counterparty_guidance=None,
            )

        self.assertIsNotNone(invocation.parsed)
        self.assertEqual(len(invocation.parsed.items), 1)
        self.assertEqual(invocation.parsed.items[0].fund_code, "F002")
        self.assertEqual(extractor.llm.bound.calls, 2)
        self.assertEqual(extractor.llm.prompt_calls, 2)

    def test_invoke_stage_writes_prompt_and_response_logs_when_log_path_is_set(self) -> None:
        class FakeBoundLLM:
            def invoke(self, messages):
                return types.SimpleNamespace(content='{"items":[{"fund_code":"F001","fund_name":"Alpha"}],"issues":[]}')

            def bind(self, **kwargs):
                return self

        class FakeLLM:
            def __init__(self) -> None:
                self.bound = FakeBoundLLM()

            def bind(self, **kwargs):
                return self.bound

            def invoke(self, messages):
                raise AssertionError("prompt-only mode should not be used")

        extractor = object.__new__(FundOrderExtractor)
        extractor.llm_retry_attempts = 1
        extractor.llm_retry_backoff_seconds = 0
        extractor.llm = FakeLLM()
        prompt_bundle = _load_prompt_bundle()
        stage = prompt_bundle.stages["fund_inventory"]

        with tempfile.TemporaryDirectory() as tmp_dir:
            log_path = Path(tmp_dir) / "sample_llm_pipeline.log"
            token = extractor_module._ACTIVE_EXTRACT_LOG_PATH.set(log_path)
            try:
                invocation = extractor._invoke_stage(
                    prompt_bundle=prompt_bundle,
                    stage=stage,
                    document_text="sample",
                    input_items=None,
                    batch_index=1,
                    response_model=FundSeedResult,
                    counterparty_guidance=None,
                )
            finally:
                extractor_module._ACTIVE_EXTRACT_LOG_PATH.reset(token)

            self.assertIsNotNone(invocation.parsed)
            logged_text = log_path.read_text(encoding="utf-8")

        self.assertIn("LLM prompt stage=fund_inventory batch=1 mode=json_object attempt=1", logged_text)
        self.assertIn("[System]", logged_text)
        self.assertIn("[User]", logged_text)
        self.assertIn("LLM response stage=fund_inventory batch=1 mode=json_object attempt=1", logged_text)
        self.assertIn("[Response]", logged_text)

    def test_extract_fund_seeds_reinvokes_same_stage_when_stage_issues_exist(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.settings = types.SimpleNamespace(llm_chunk_size_chars=12000)
        extractor.llm_stage_issue_retry_attempts = 2

        prompt_bundle = _load_prompt_bundle()
        responses = iter(
            [
                types.SimpleNamespace(
                    parsed=FundSeedResult(
                        items=[FundSeedItem(fund_code="F001", fund_name="Alpha")],
                        issues=["NO_FUND_DATA_FOUND"],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha"}],"issues":["NO_FUND_DATA_FOUND"]}',
                ),
                types.SimpleNamespace(
                    parsed=FundSeedResult(
                        items=[FundSeedItem(fund_code="F001", fund_name="Alpha")],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha"}],"issues":[]}',
                ),
            ]
        )
        seen_stage_names: list[str] = []

        def fake_invoke_stage(
            self,
            *,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            batch_index,
            response_model,
            counterparty_guidance=None,
        ):
            seen_stage_names.append(stage.name)
            return next(responses)

        extractor._invoke_stage = types.MethodType(fake_invoke_stage, extractor)

        issues: list[str] = []
        artifacts = []
        seeds = extractor._extract_fund_seeds(
            prompt_bundle,
            ["sample chunk"],
            None,
            issues,
            artifacts,
            counterparty_guidance=None,
        )

        self.assertEqual(seen_stage_names, ["fund_inventory", "fund_inventory"])
        self.assertEqual([(item.fund_code, item.fund_name) for item in seeds], [("F001", "Alpha")])
        self.assertEqual(issues, [])
        self.assertEqual(artifacts, [])

    def test_extract_fund_seeds_rejects_retry_with_extra_seed_rows(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.settings = types.SimpleNamespace(llm_chunk_size_chars=12000)
        extractor.llm_stage_issue_retry_attempts = 1

        prompt_bundle = _load_prompt_bundle()
        responses = iter(
            [
                types.SimpleNamespace(
                    parsed=FundSeedResult(
                        items=[FundSeedItem(fund_code="F001", fund_name="Alpha")],
                        issues=["NO_FUND_DATA_FOUND"],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha"}],"issues":["NO_FUND_DATA_FOUND"]}',
                ),
                types.SimpleNamespace(
                    parsed=FundSeedResult(
                        items=[
                            FundSeedItem(fund_code="F001", fund_name="Alpha"),
                            FundSeedItem(fund_code="F999", fund_name="Ghost"),
                        ],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha"},{"fund_code":"F999","fund_name":"Ghost"}],"issues":[]}',
                ),
            ]
        )

        def fake_invoke_stage(
            self,
            *,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            batch_index,
            response_model,
            counterparty_guidance=None,
        ):
            return next(responses)

        extractor._invoke_stage = types.MethodType(fake_invoke_stage, extractor)

        issues: list[str] = []
        artifacts = []
        seeds = extractor._extract_fund_seeds(
            prompt_bundle,
            ["sample chunk"],
            None,
            issues,
            artifacts,
            counterparty_guidance=None,
        )

        self.assertEqual([(item.fund_code, item.fund_name) for item in seeds], [("F001", "Alpha")])
        self.assertEqual(issues, ["NO_FUND_DATA_FOUND"])
        self.assertEqual(artifacts, [])

    def test_extract_fund_seeds_accepts_retry_with_document_evidenced_extra_seed(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.settings = types.SimpleNamespace(llm_chunk_size_chars=12000)
        extractor.llm_stage_issue_retry_attempts = 1

        prompt_bundle = _load_prompt_bundle()
        responses = iter(
            [
                types.SimpleNamespace(
                    parsed=FundSeedResult(
                        items=[FundSeedItem(fund_code="F001", fund_name="Alpha")],
                        issues=["NO_FUND_DATA_FOUND"],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha"}],"issues":["NO_FUND_DATA_FOUND"]}',
                ),
                types.SimpleNamespace(
                    parsed=FundSeedResult(
                        items=[
                            FundSeedItem(fund_code="F001", fund_name="Alpha"),
                            FundSeedItem(fund_code="F002", fund_name="Beta"),
                        ],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha"},{"fund_code":"F002","fund_name":"Beta"}],"issues":[]}',
                ),
            ]
        )

        def fake_invoke_stage(
            self,
            *,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            batch_index,
            response_model,
            counterparty_guidance=None,
        ):
            return next(responses)

        extractor._invoke_stage = types.MethodType(fake_invoke_stage, extractor)

        issues: list[str] = []
        artifacts = []
        seeds = extractor._extract_fund_seeds(
            prompt_bundle,
            ["sample chunk includes F002 and Beta"],
            None,
            issues,
            artifacts,
            counterparty_guidance=None,
        )

        self.assertEqual(
            [(item.fund_code, item.fund_name) for item in seeds],
            [("F001", "Alpha"), ("F002", "Beta")],
        )
        self.assertEqual(issues, [])
        self.assertEqual(artifacts, [])

    def test_extract_fund_seeds_rejects_empty_to_ghost_seed_retry(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.settings = types.SimpleNamespace(llm_chunk_size_chars=12000)
        extractor.llm_stage_issue_retry_attempts = 1

        prompt_bundle = _load_prompt_bundle()
        responses = iter(
            [
                types.SimpleNamespace(
                    parsed=FundSeedResult(items=[], issues=["NO_FUND_DATA_FOUND"]),
                    raw_response='{"items":[],"issues":["NO_FUND_DATA_FOUND"]}',
                ),
                types.SimpleNamespace(
                    parsed=FundSeedResult(
                        items=[FundSeedItem(fund_code="F999", fund_name="Ghost")],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"F999","fund_name":"Ghost"}],"issues":[]}',
                ),
            ]
        )

        def fake_invoke_stage(
            self,
            *,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            batch_index,
            response_model,
            counterparty_guidance=None,
        ):
            return next(responses)

        extractor._invoke_stage = types.MethodType(fake_invoke_stage, extractor)

        issues: list[str] = []
        artifacts = []
        seeds = extractor._extract_fund_seeds(
            prompt_bundle,
            ["sample chunk without supporting evidence"],
            None,
            issues,
            artifacts,
            counterparty_guidance=None,
        )

        self.assertEqual([(item.fund_code, item.fund_name) for item in seeds], [])
        self.assertEqual(issues, ["NO_FUND_DATA_FOUND"])
        self.assertEqual(artifacts, [])

    def test_extract_fund_seeds_reinvokes_when_document_table_has_missing_sibling_seed_without_explicit_issue(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.settings = types.SimpleNamespace(llm_chunk_size_chars=12000)
        extractor.llm_stage_issue_retry_attempts = 1

        prompt_bundle = _load_prompt_bundle()
        responses = iter(
            [
                types.SimpleNamespace(
                    parsed=FundSeedResult(
                        items=[FundSeedItem(fund_code="F001", fund_name="Alpha")],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha"}],"issues":[]}',
                ),
                types.SimpleNamespace(
                    parsed=FundSeedResult(
                        items=[
                            FundSeedItem(fund_code="F001", fund_name="Alpha"),
                            FundSeedItem(fund_code="F002", fund_name="Beta"),
                        ],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha"},{"fund_code":"F002","fund_name":"Beta"}],"issues":[]}',
                ),
            ]
        )
        call_count = 0

        def fake_invoke_stage(
            self,
            *,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            batch_index,
            response_model,
            counterparty_guidance=None,
        ):
            nonlocal call_count
            call_count += 1
            return next(responses)

        extractor._invoke_stage = types.MethodType(fake_invoke_stage, extractor)

        issues: list[str] = []
        artifacts = []
        seeds = extractor._extract_fund_seeds(
            prompt_bundle,
            [
                "\n".join(
                    [
                        "| 펀드코드 | 펀드명 |",
                        "| --- | --- |",
                        "| F001 | Alpha |",
                        "| F002 | Beta |",
                    ]
                )
            ],
            None,
            issues,
            artifacts,
            counterparty_guidance=None,
        )

        self.assertEqual(call_count, 2)
        self.assertEqual([(item.fund_code, item.fund_name) for item in seeds], [("F001", "Alpha"), ("F002", "Beta")])
        self.assertEqual(issues, [])
        self.assertEqual(artifacts, [])

    def test_augment_t_day_items_from_document_keeps_lina_surviving_red_slot(self) -> None:
        input_items = [
            {
                "fund_code": "6115",
                "fund_name": "삼성그룹주형",
                "base_date": "2026-04-03",
            }
        ]
        document_text = "\n".join(
            [
                "| 펀드코드 | 펀드명 | 설정금액 | 해지금액 |",
                "| --- | --- | --- | --- |",
                "| 6115 | 삼성그룹주형 | 0 | 4,485 |",
            ]
        )

        augmented = self.extractor._augment_t_day_items_from_document(
            document_text=document_text,
            input_items=input_items,
            output_items=[],
        )

        self.assertEqual(
            [(item.fund_code, item.t_day, item.slot_id, item.evidence_label) for item in augmented],
            [("6115", 0, "T0_RED", "해지금액")],
        )
        self.assertTrue(
            self.extractor._t_day_document_slots_are_complete(
                document_text=document_text,
                input_items=input_items,
                output_items=augmented,
            )
        )

    def test_t_day_document_slots_are_not_complete_when_extra_slot_remains(self) -> None:
        input_items = [
            {
                "fund_code": "6115",
                "fund_name": "삼성그룹주형",
                "base_date": "2026-04-03",
            }
        ]
        document_text = "\n".join(
            [
                "| 펀드코드 | 펀드명 | 설정금액 | 해지금액 |",
                "| --- | --- | --- | --- |",
                "| 6115 | 삼성그룹주형 | 0 | 4,485 |",
            ]
        )
        output_items = [
            FundSlotItem(
                fund_code="6115",
                fund_name="삼성그룹주형",
                base_date="2026-04-03",
                t_day=0,
                slot_id="T0_RED",
                evidence_label="해지금액",
            ),
            FundSlotItem(
                fund_code="6115",
                fund_name="삼성그룹주형",
                base_date="2026-04-03",
                t_day=0,
                slot_id="T0_RED_ALT",
                evidence_label="출금금액",
            ),
        ]

        self.assertFalse(
            self.extractor._t_day_document_slots_are_complete(
                document_text=document_text,
                input_items=input_items,
                output_items=output_items,
            )
        )

    def test_fund_inventory_seed_signatures_ignore_name_only_continuation_rows_in_combined_code_name_column(self) -> None:
        document_text = "\n".join(
            [
                "| 거래유형 / 판매사 | 펀드코드 / 펀드명 | 결제일 | 펀드납입(인출)금액 |",
                "| --- | --- | --- | --- |",
                "| 설정 | BBC13F | 2025-11-27 | 7,108,103 |",
                "| 하나생명 | VUL 주식성장형(1형)_SamsungActive |  | 7,108,103 |",
                "| 해지 | BBA72G | 2025-11-27 | 1,288,090 |",
                "| 하나생명 | VA 안정성장형 I_SamsungActive |  | 1,288,090 |",
            ]
        )

        signatures = self.extractor._derive_document_fund_seed_signatures(document_text)

        self.assertEqual(
            signatures,
            {
                ("BBC13F", ""),
                ("BBA72G", ""),
            },
        )

    def test_build_deterministic_markdown_table_orders_prefers_hanalife_setting_redemption_amount(self) -> None:
        raw_text = "결제일 : 2026-04-15\n"
        markdown_text = "\n".join(
            [
                "| 거래일자 | 거래유형명 | 펀드코드 | 펀드명 | 설정해지금액 / 최저연금보증해징 | 펀드납입출금액 / 최저적립보증비용 | 판매회사분결제금액 / 최저보장보증비용 |",
                "| --- | --- | --- | --- | --- | --- | --- |",
                "| 2026-04-15 | 해지 | BBA933 | VA 이머징프릭스주식성장형 | 5,351 | 5,351 | 5,351 |",
                "| 2026-04-15 | 해지 | BBA933 | VA 이머징프릭스주식성장형 | 5,351 | 5,351 | 5,351 |",
                "| 2026-04-15 | 설정 | BBC143 | 글로벌혼합형(1형) | 2,906,740 | 2,906,740 | 2,906,740 |",
                "| 2026-04-15 | 설정 | BBC143 | 글로벌혼합형(1형) | 2,906,740 | 2,906,740 | 2,906,740 |",
                "| 2026-04-15 | 설정 | BBC152 | 아태브릭스주식성장형 | 4,128,002 | 4,128,002 | 4,128,002 |",
                "| 2026-04-15 | 설정 | BBC152 | 아태브릭스주식성장형 | 4,128,002 | 4,128,002 | 4,128,002 |",
                "| 2026-04-15 | 해지 | BBC170 | 차이나주식성장형 | 689 | 689 | 689 |",
                "| 2026-04-15 | 해지 | BBC170 | 차이나주식성장형 | 689 | 689 | 689 |",
                "| 2026-04-15 | 설정 | BBC170 | 차이나주식성장형 | 100,000 | 100,000 | 100,000 |",
                "| 2026-04-15 | 설정 | BBC170 | 차이나주식성장형 | 100,000 | 100,000 | 100,000 |",
                "| 2026-04-15 | 해지 | BBC180 | 유럽주식성장형 | 21,514,177 | 21,514,177 | 21,514,177 |",
                "| 2026-04-15 | 해지 | BBC180 | 유럽주식성장형 | 21,514,177 | 21,514,177 | 21,514,177 |",
                "| 2026-04-15 | 설정 | BBC180 | 유럽주식성장형 | 22,684,941 | 22,684,941 | 22,684,941 |",
                "| 2026-04-15 | 설정 | BBC180 | 유럽주식성장형 | 22,684,941 | 22,684,941 | 22,684,941 |",
            ]
        )

        orders = self.extractor._dedupe_orders_by_signature(
            self.extractor._build_deterministic_markdown_table_orders(
                markdown_text=markdown_text,
                raw_text=raw_text,
                target_fund_scope=None,
            )
        )

        self.assertEqual(
            [
                (
                    order.fund_code,
                    order.fund_name,
                    order.settle_class,
                    order.order_type,
                    order.base_date,
                    order.t_day,
                    order.transfer_amount,
                )
                for order in orders
            ],
            [
                (
                    "BBA933",
                    "VA 이머징프릭스주식성장형",
                    SettleClass.CONFIRMED,
                    OrderType.RED,
                    "2026-04-15",
                    0,
                    "-5,351",
                ),
                (
                    "BBC143",
                    "글로벌혼합형(1형)",
                    SettleClass.CONFIRMED,
                    OrderType.SUB,
                    "2026-04-15",
                    0,
                    "2,906,740",
                ),
                (
                    "BBC152",
                    "아태브릭스주식성장형",
                    SettleClass.CONFIRMED,
                    OrderType.SUB,
                    "2026-04-15",
                    0,
                    "4,128,002",
                ),
                (
                    "BBC170",
                    "차이나주식성장형",
                    SettleClass.CONFIRMED,
                    OrderType.RED,
                    "2026-04-15",
                    0,
                    "-689",
                ),
                (
                    "BBC170",
                    "차이나주식성장형",
                    SettleClass.CONFIRMED,
                    OrderType.SUB,
                    "2026-04-15",
                    0,
                    "100,000",
                ),
                (
                    "BBC180",
                    "유럽주식성장형",
                    SettleClass.CONFIRMED,
                    OrderType.RED,
                    "2026-04-15",
                    0,
                    "-21,514,177",
                ),
                (
                    "BBC180",
                    "유럽주식성장형",
                    SettleClass.CONFIRMED,
                    OrderType.SUB,
                    "2026-04-15",
                    0,
                    "22,684,941",
                ),
            ],
        )

    def test_stage_signatures_collapse_same_fund_code_with_spacing_only_name_differences(self) -> None:
        left = {
            "fund_code": "BALX00",
            "fund_name": "(디폴트옵션전용) 글로벌에셋밸런스형",
            "base_date": "2026-03-18",
            "t_day": 2,
            "slot_id": "T2_RED",
        }
        right = {
            "fund_code": "BALX00",
            "fund_name": "(디폴트옵션전용)글로벌에셋밸런스형",
            "base_date": "2026-03-18",
            "t_day": 2,
            "slot_id": "T2_RED",
        }

        self.assertEqual(
            self.extractor._fund_seed_signature(left),
            self.extractor._fund_seed_signature(right),
        )
        self.assertEqual(
            self.extractor._stage_partial_item_signature("t_day", left),
            self.extractor._stage_partial_item_signature("t_day", right),
        )
        self.assertEqual(
            self.extractor._stage_item_signature("t_day", left),
            self.extractor._stage_item_signature("t_day", right),
        )

    def test_fund_seed_document_evidence_does_not_accept_substring_matches(self) -> None:
        extractor = object.__new__(FundOrderExtractor)

        self.assertFalse(
            extractor._fund_seed_has_document_evidence(
                "document has F100 only",
                {"fund_code": "F1", "fund_name": "Ghost"},
            )
        )
        self.assertFalse(
            extractor._fund_seed_has_document_evidence(
                "document has AlphaBeta fund",
                {"fund_code": "", "fund_name": "Beta"},
            )
        )
        self.assertTrue(
            extractor._fund_seed_has_document_evidence(
                "document mentions 미국주식인덱스 ( 환오픈형 )",
                {"fund_code": "", "fund_name": "미국주식인덱스(환오픈형)"},
            )
        )

    def test_run_batched_stage_reinvokes_same_stage_when_parsed_issues_exist(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.llm_stage_issue_retry_attempts = 2
        extractor.stage_batch_size = 12

        prompt_bundle = _load_prompt_bundle()
        stage = prompt_bundle.stages["base_date"]
        responses = iter(
            [
                types.SimpleNamespace(
                    parsed=FundBaseDateResult(
                        items=[FundBaseDateItem(fund_code="F001", fund_name="Alpha", base_date="2025-11-27")],
                        issues=["BASE_DATE_MISSING"],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27"}],"issues":["BASE_DATE_MISSING"]}',
                ),
                types.SimpleNamespace(
                    parsed=FundBaseDateResult(
                        items=[FundBaseDateItem(fund_code="F001", fund_name="Alpha", base_date="2025-11-27")],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27"}],"issues":[]}',
                ),
            ]
        )
        call_count = 0

        def fake_invoke_stage(
            self,
            *,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            batch_index,
            response_model,
            counterparty_guidance=None,
        ):
            nonlocal call_count
            call_count += 1
            return next(responses)

        extractor._invoke_stage = types.MethodType(fake_invoke_stage, extractor)

        issues: list[str] = []
        artifacts = []
        result = extractor._run_batched_stage(
            prompt_bundle=prompt_bundle,
            stage=stage,
            document_text="sample",
            input_items=[{"fund_code": "F001", "fund_name": "Alpha"}],
            response_model=FundBaseDateResult,
            issues=issues,
            artifacts=artifacts,
            counterparty_guidance=None,
        )

        self.assertEqual(call_count, 2)
        self.assertEqual([(item.fund_code, item.base_date) for item in result], [("F001", "2025-11-27")])
        self.assertEqual(issues, [])
        self.assertEqual(artifacts, [])

    def test_run_batched_stage_accepts_same_slot_missing_value_recovery(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.llm_stage_issue_retry_attempts = 2
        extractor.stage_batch_size = 12

        prompt_bundle = _load_prompt_bundle()
        stage = prompt_bundle.stages["transfer_amount"]
        responses = iter(
            [
                types.SimpleNamespace(
                    parsed=FundAmountResult(
                        items=[
                            FundAmountItem(
                                fund_code="F001",
                                fund_name="Alpha",
                                base_date="2025-11-27",
                                t_day=0,
                                slot_id="T0_NET",
                                evidence_label="당일이체금액",
                                transfer_amount=None,
                            )
                        ],
                        issues=["TRANSFER_AMOUNT_MISSING"],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27","t_day":0,"slot_id":"T0_NET","evidence_label":"당일이체금액","transfer_amount":null}],"issues":["TRANSFER_AMOUNT_MISSING"]}',
                ),
                types.SimpleNamespace(
                    parsed=FundAmountResult(
                        items=[
                            FundAmountItem(
                                fund_code="F001",
                                fund_name="Alpha",
                                base_date="2025-11-27",
                                t_day=0,
                                slot_id="T0_NET",
                                evidence_label="당일이체금액",
                                transfer_amount="100",
                            )
                        ],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27","t_day":0,"slot_id":"T0_NET","evidence_label":"당일이체금액","transfer_amount":"100"}],"issues":[]}',
                ),
            ]
        )
        call_count = 0

        def fake_invoke_stage(
            self,
            *,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            batch_index,
            response_model,
            counterparty_guidance=None,
        ):
            nonlocal call_count
            call_count += 1
            return next(responses)

        extractor._invoke_stage = types.MethodType(fake_invoke_stage, extractor)

        issues: list[str] = []
        artifacts = []
        result = extractor._run_batched_stage(
            prompt_bundle=prompt_bundle,
            stage=stage,
            document_text="sample",
            input_items=[
                {
                    "fund_code": "F001",
                    "fund_name": "Alpha",
                    "base_date": "2025-11-27",
                    "t_day": 0,
                    "slot_id": "T0_NET",
                    "evidence_label": "당일이체금액",
                }
            ],
            response_model=FundAmountResult,
            issues=issues,
            artifacts=artifacts,
            counterparty_guidance=None,
        )

        self.assertEqual(call_count, 2)
        self.assertEqual([(item.fund_code, item.transfer_amount) for item in result], [("F001", "100")])
        self.assertEqual(issues, [])
        self.assertEqual(artifacts, [])

    def test_run_batched_stage_rejects_retry_that_changes_existing_non_empty_value(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.llm_stage_issue_retry_attempts = 1
        extractor.stage_batch_size = 12

        prompt_bundle = _load_prompt_bundle()
        stage = prompt_bundle.stages["transfer_amount"]
        responses = iter(
            [
                types.SimpleNamespace(
                    parsed=FundAmountResult(
                        items=[
                            FundAmountItem(
                                fund_code="F001",
                                fund_name="Alpha",
                                base_date="2025-11-27",
                                t_day=0,
                                slot_id="T0_NET",
                                evidence_label="당일이체금액",
                                transfer_amount="100",
                            )
                        ],
                        issues=["TRANSFER_AMOUNT_MISSING"],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27","t_day":0,"slot_id":"T0_NET","evidence_label":"당일이체금액","transfer_amount":"100"}],"issues":["TRANSFER_AMOUNT_MISSING"]}',
                ),
                types.SimpleNamespace(
                    parsed=FundAmountResult(
                        items=[
                            FundAmountItem(
                                fund_code="F001",
                                fund_name="Alpha",
                                base_date="2025-11-27",
                                t_day=0,
                                slot_id="T0_NET",
                                evidence_label="당일이체금액",
                                transfer_amount="200",
                            )
                        ],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27","t_day":0,"slot_id":"T0_NET","evidence_label":"당일이체금액","transfer_amount":"200"}],"issues":[]}',
                ),
            ]
        )
        call_count = 0

        def fake_invoke_stage(
            self,
            *,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            batch_index,
            response_model,
            counterparty_guidance=None,
        ):
            nonlocal call_count
            call_count += 1
            return next(responses)

        extractor._invoke_stage = types.MethodType(fake_invoke_stage, extractor)

        issues: list[str] = []
        artifacts = []
        result = extractor._run_batched_stage(
            prompt_bundle=prompt_bundle,
            stage=stage,
            document_text="sample",
            input_items=[
                {
                    "fund_code": "F001",
                    "fund_name": "Alpha",
                    "base_date": "2025-11-27",
                    "t_day": 0,
                    "slot_id": "T0_NET",
                    "evidence_label": "당일이체금액",
                }
            ],
            response_model=FundAmountResult,
            issues=issues,
            artifacts=artifacts,
            counterparty_guidance=None,
        )

        self.assertEqual(call_count, 2)
        self.assertEqual([(item.fund_code, item.transfer_amount) for item in result], [("F001", "100")])
        self.assertEqual(issues, ["TRANSFER_AMOUNT_MISSING"])
        self.assertEqual(artifacts, [])

    def test_run_batched_stage_rejects_retry_with_extra_row_outside_requested_batch(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.llm_stage_issue_retry_attempts = 1
        extractor.stage_batch_size = 12

        prompt_bundle = _load_prompt_bundle()
        stage = prompt_bundle.stages["transfer_amount"]
        responses = iter(
            [
                types.SimpleNamespace(
                    parsed=FundAmountResult(
                        items=[
                            FundAmountItem(
                                fund_code="F001",
                                fund_name="Alpha",
                                base_date="2025-11-27",
                                t_day=0,
                                slot_id="T0_NET",
                                evidence_label="당일이체금액",
                                transfer_amount="100",
                            )
                        ],
                        issues=["TRANSFER_AMOUNT_MISSING"],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27","t_day":0,"slot_id":"T0_NET","evidence_label":"당일이체금액","transfer_amount":"100"}],"issues":["TRANSFER_AMOUNT_MISSING"]}',
                ),
                types.SimpleNamespace(
                    parsed=FundAmountResult(
                        items=[
                            FundAmountItem(
                                fund_code="F001",
                                fund_name="Alpha",
                                base_date="2025-11-27",
                                t_day=0,
                                slot_id="T0_NET",
                                evidence_label="당일이체금액",
                                transfer_amount="100",
                            ),
                            FundAmountItem(
                                fund_code="F999",
                                fund_name="Ghost",
                                base_date="2025-11-27",
                                t_day=0,
                                slot_id="T0_NET",
                                evidence_label="당일이체금액",
                                transfer_amount="999",
                            ),
                        ],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27","t_day":0,"slot_id":"T0_NET","evidence_label":"당일이체금액","transfer_amount":"100"},{"fund_code":"F999","fund_name":"Ghost","base_date":"2025-11-27","t_day":0,"slot_id":"T0_NET","evidence_label":"당일이체금액","transfer_amount":"999"}],"issues":[]}',
                ),
            ]
        )
        call_count = 0

        def fake_invoke_stage(
            self,
            *,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            batch_index,
            response_model,
            counterparty_guidance=None,
        ):
            nonlocal call_count
            call_count += 1
            return next(responses)

        extractor._invoke_stage = types.MethodType(fake_invoke_stage, extractor)

        issues: list[str] = []
        artifacts = []
        result = extractor._run_batched_stage(
            prompt_bundle=prompt_bundle,
            stage=stage,
            document_text="sample",
            input_items=[
                {
                    "fund_code": "F001",
                    "fund_name": "Alpha",
                    "base_date": "2025-11-27",
                    "t_day": 0,
                    "slot_id": "T0_NET",
                    "evidence_label": "당일이체금액",
                }
            ],
            response_model=FundAmountResult,
            issues=issues,
            artifacts=artifacts,
            counterparty_guidance=None,
        )

        self.assertEqual(call_count, 2)
        self.assertEqual([(item.fund_code, item.transfer_amount) for item in result], [("F001", "100")])
        self.assertEqual(issues, ["TRANSFER_AMOUNT_MISSING"])
        self.assertEqual(artifacts, [])

    def test_run_batched_stage_accepts_same_score_retry_with_partial_same_slot_improvement(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.llm_stage_issue_retry_attempts = 1
        extractor.stage_batch_size = 12

        prompt_bundle = _load_prompt_bundle()
        stage = prompt_bundle.stages["transfer_amount"]
        responses = iter(
            [
                types.SimpleNamespace(
                    parsed=FundAmountResult(
                        items=[
                            FundAmountItem(
                                fund_code="F001",
                                fund_name="Alpha",
                                base_date="2025-11-27",
                                t_day=0,
                                slot_id="T0_NET",
                                evidence_label="당일이체금액",
                                transfer_amount=None,
                            ),
                            FundAmountItem(
                                fund_code="F002",
                                fund_name="Beta",
                                base_date="2025-11-27",
                                t_day=1,
                                slot_id="T1_NET",
                                evidence_label="익영업일이체금액",
                                transfer_amount=None,
                            ),
                        ],
                        issues=["TRANSFER_AMOUNT_MISSING"],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27","t_day":0,"slot_id":"T0_NET","evidence_label":"당일이체금액","transfer_amount":null},{"fund_code":"F002","fund_name":"Beta","base_date":"2025-11-27","t_day":1,"slot_id":"T1_NET","evidence_label":"익영업일체금액","transfer_amount":null}],"issues":["TRANSFER_AMOUNT_MISSING"]}',
                ),
                types.SimpleNamespace(
                    parsed=FundAmountResult(
                        items=[
                            FundAmountItem(
                                fund_code="F001",
                                fund_name="Alpha",
                                base_date="2025-11-27",
                                t_day=0,
                                slot_id="T0_NET",
                                evidence_label="당일이체금액",
                                transfer_amount="100",
                            ),
                            FundAmountItem(
                                fund_code="F002",
                                fund_name="Beta",
                                base_date="2025-11-27",
                                t_day=1,
                                slot_id="T1_NET",
                                evidence_label="익영업일이체금액",
                                transfer_amount=None,
                            ),
                        ],
                        issues=["TRANSFER_AMOUNT_MISSING"],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27","t_day":0,"slot_id":"T0_NET","evidence_label":"당일이체금액","transfer_amount":"100"},{"fund_code":"F002","fund_name":"Beta","base_date":"2025-11-27","t_day":1,"slot_id":"T1_NET","evidence_label":"익영업일이체금액","transfer_amount":null}],"issues":["TRANSFER_AMOUNT_MISSING"]}',
                ),
            ]
        )

        def fake_invoke_stage(
            self,
            *,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            batch_index,
            response_model,
            counterparty_guidance=None,
        ):
            return next(responses)

        extractor._invoke_stage = types.MethodType(fake_invoke_stage, extractor)

        issues: list[str] = []
        artifacts = []
        result = extractor._run_batched_stage(
            prompt_bundle=prompt_bundle,
            stage=stage,
            document_text="sample",
            input_items=[
                {
                    "fund_code": "F001",
                    "fund_name": "Alpha",
                    "base_date": "2025-11-27",
                    "t_day": 0,
                    "slot_id": "T0_NET",
                    "evidence_label": "당일이체금액",
                },
                {
                    "fund_code": "F002",
                    "fund_name": "Beta",
                    "base_date": "2025-11-27",
                    "t_day": 1,
                    "slot_id": "T1_NET",
                    "evidence_label": "익영업일이체금액",
                },
            ],
            response_model=FundAmountResult,
            issues=issues,
            artifacts=artifacts,
            counterparty_guidance=None,
        )

        self.assertEqual(
            [(item.fund_code, item.transfer_amount) for item in result],
            [("F001", "100"), ("F002", None)],
        )
        self.assertEqual(issues, ["TRANSFER_AMOUNT_MISSING", "TRANSFER_AMOUNT_STAGE_PARTIAL"])
        self.assertEqual(artifacts, [])

    def test_run_batched_stage_reinvokes_same_stage_when_stage_is_partial(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.llm_stage_issue_retry_attempts = 2
        extractor.stage_batch_size = 12

        prompt_bundle = _load_prompt_bundle()
        stage = prompt_bundle.stages["base_date"]
        responses = iter(
            [
                types.SimpleNamespace(
                    parsed=FundBaseDateResult(
                        items=[FundBaseDateItem(fund_code="F001", fund_name="Alpha", base_date="2025-11-27")],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27"}],"issues":[]}',
                ),
                types.SimpleNamespace(
                    parsed=FundBaseDateResult(
                        items=[
                            FundBaseDateItem(fund_code="F001", fund_name="Alpha", base_date="2025-11-27"),
                            FundBaseDateItem(fund_code="F002", fund_name="Beta", base_date="2025-11-27"),
                        ],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27"},{"fund_code":"F002","fund_name":"Beta","base_date":"2025-11-27"}],"issues":[]}',
                ),
            ]
        )
        call_count = 0

        def fake_invoke_stage(
            self,
            *,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            batch_index,
            response_model,
            counterparty_guidance=None,
        ):
            nonlocal call_count
            call_count += 1
            return next(responses)

        extractor._invoke_stage = types.MethodType(fake_invoke_stage, extractor)

        issues: list[str] = []
        artifacts = []
        result = extractor._run_batched_stage(
            prompt_bundle=prompt_bundle,
            stage=stage,
            document_text="sample",
            input_items=[
                {"fund_code": "F001", "fund_name": "Alpha"},
                {"fund_code": "F002", "fund_name": "Beta"},
            ],
            response_model=FundBaseDateResult,
            issues=issues,
            artifacts=artifacts,
            counterparty_guidance=None,
        )

        self.assertEqual(call_count, 2)
        self.assertEqual(
            [(item.fund_code, item.base_date) for item in result],
            [("F001", "2025-11-27"), ("F002", "2025-11-27")],
        )
        self.assertEqual(issues, [])
        self.assertEqual(artifacts, [])

    def test_run_batched_stage_reinvokes_same_stage_when_t_day_stage_is_partial(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.llm_stage_issue_retry_attempts = 2
        extractor.stage_batch_size = 12

        prompt_bundle = _load_prompt_bundle()
        stage = prompt_bundle.stages["t_day"]
        responses = iter(
            [
                types.SimpleNamespace(
                    parsed=FundSlotResult(
                        items=[
                            FundSlotItem(
                                fund_code="F001",
                                fund_name="Alpha",
                                base_date="2025-11-27",
                                t_day=0,
                                slot_id="T0_NET",
                                evidence_label="당일이체금액",
                            )
                        ],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27","t_day":0,"slot_id":"T0_NET","evidence_label":"당일이체금액"}],"issues":[]}',
                ),
                types.SimpleNamespace(
                    parsed=FundSlotResult(
                        items=[
                            FundSlotItem(
                                fund_code="F001",
                                fund_name="Alpha",
                                base_date="2025-11-27",
                                t_day=0,
                                slot_id="T0_NET",
                                evidence_label="당일이체금액",
                            ),
                            FundSlotItem(
                                fund_code="F002",
                                fund_name="Beta",
                                base_date="2025-11-27",
                                t_day=1,
                                slot_id="T1_NET",
                                evidence_label="익영업일이체금액",
                            ),
                        ],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27","t_day":0,"slot_id":"T0_NET","evidence_label":"당일이체금액"},{"fund_code":"F002","fund_name":"Beta","base_date":"2025-11-27","t_day":1,"slot_id":"T1_NET","evidence_label":"익영업일이체금액"}],"issues":[]}',
                ),
            ]
        )
        call_count = 0

        def fake_invoke_stage(
            self,
            *,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            batch_index,
            response_model,
            counterparty_guidance=None,
        ):
            nonlocal call_count
            call_count += 1
            return next(responses)

        extractor._invoke_stage = types.MethodType(fake_invoke_stage, extractor)

        issues: list[str] = []
        artifacts = []
        result = extractor._run_batched_stage(
            prompt_bundle=prompt_bundle,
            stage=stage,
            document_text="sample",
            input_items=[
                {"fund_code": "F001", "fund_name": "Alpha", "base_date": "2025-11-27"},
                {"fund_code": "F002", "fund_name": "Beta", "base_date": "2025-11-27"},
            ],
            response_model=FundSlotResult,
            issues=issues,
            artifacts=artifacts,
            counterparty_guidance=None,
        )

        self.assertEqual(call_count, 2)
        self.assertEqual([(item.fund_code, item.t_day) for item in result], [("F001", 0), ("F002", 1)])
        self.assertEqual(issues, [])
        self.assertEqual(artifacts, [])

    def test_run_batched_stage_reinvokes_t_day_stage_when_required_field_is_missing_without_explicit_issue(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.llm_stage_issue_retry_attempts = 1
        extractor.stage_batch_size = 12

        prompt_bundle = _load_prompt_bundle()
        stage = prompt_bundle.stages["t_day"]
        responses = iter(
            [
                types.SimpleNamespace(
                    parsed=FundSlotResult(
                        items=[
                            FundSlotItem(
                                fund_code="F6114",
                                fund_name="Index",
                                base_date="2025-08-26",
                                t_day=None,
                                slot_id="",
                                evidence_label="설정금액",
                            )
                        ],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"F6114","fund_name":"Index","base_date":"2025-08-26","t_day":null,"slot_id":"","evidence_label":"설정금액"}],"issues":[]}',
                ),
                types.SimpleNamespace(
                    parsed=FundSlotResult(
                        items=[
                            FundSlotItem(
                                fund_code="F6114",
                                fund_name="Index",
                                base_date="2025-08-26",
                                t_day=0,
                                slot_id="T0_SUB",
                                evidence_label="설정금액",
                            )
                        ],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"F6114","fund_name":"Index","base_date":"2025-08-26","t_day":0,"slot_id":"T0_SUB","evidence_label":"설정금액"}],"issues":[]}',
                ),
            ]
        )
        call_count = 0

        def fake_invoke_stage(
            self,
            *,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            batch_index,
            response_model,
            counterparty_guidance=None,
        ):
            nonlocal call_count
            call_count += 1
            return next(responses)

        extractor._invoke_stage = types.MethodType(fake_invoke_stage, extractor)

        issues: list[str] = []
        artifacts = []
        result = extractor._run_batched_stage(
            prompt_bundle=prompt_bundle,
            stage=stage,
            document_text="sample with 설정금액",
            input_items=[{"fund_code": "F6114", "fund_name": "Index", "base_date": "2025-08-26"}],
            response_model=FundSlotResult,
            issues=issues,
            artifacts=artifacts,
            counterparty_guidance=None,
        )

        self.assertEqual(call_count, 2)
        self.assertEqual([(item.fund_code, item.t_day, item.slot_id) for item in result], [("F6114", 0, "T0_SUB")])
        self.assertEqual(issues, [])
        self.assertEqual(artifacts, [])

    def test_run_batched_stage_reinvokes_t_day_stage_when_document_has_missing_sibling_slot_without_explicit_issue(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.llm_stage_issue_retry_attempts = 1
        extractor.stage_batch_size = 12

        prompt_bundle = _load_prompt_bundle()
        stage = prompt_bundle.stages["t_day"]
        responses = iter(
            [
                types.SimpleNamespace(
                    parsed=FundSlotResult(
                        items=[
                            FundSlotItem(
                                fund_code="6114",
                                fund_name="혼합성장형",
                                base_date="2026-04-03",
                                t_day=0,
                                slot_id="T0_SUB",
                                evidence_label="설정금액",
                            )
                        ],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"6114","fund_name":"혼합성장형","base_date":"2026-04-03","t_day":0,"slot_id":"T0_SUB","evidence_label":"설정금액"}],"issues":[]}',
                ),
                types.SimpleNamespace(
                    parsed=FundSlotResult(
                        items=[
                            FundSlotItem(
                                fund_code="6114",
                                fund_name="혼합성장형",
                                base_date="2026-04-03",
                                t_day=0,
                                slot_id="T0_SUB",
                                evidence_label="설정금액",
                            ),
                            FundSlotItem(
                                fund_code="6114",
                                fund_name="혼합성장형",
                                base_date="2026-04-03",
                                t_day=0,
                                slot_id="T0_RED",
                                evidence_label="해지금액",
                            ),
                        ],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"6114","fund_name":"혼합성장형","base_date":"2026-04-03","t_day":0,"slot_id":"T0_SUB","evidence_label":"설정금액"},{"fund_code":"6114","fund_name":"혼합성장형","base_date":"2026-04-03","t_day":0,"slot_id":"T0_RED","evidence_label":"해지금액"}],"issues":[]}',
                ),
            ]
        )
        call_count = 0

        def fake_invoke_stage(
            self,
            *,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            batch_index,
            response_model,
            counterparty_guidance=None,
        ):
            nonlocal call_count
            call_count += 1
            return next(responses)

        extractor._invoke_stage = types.MethodType(fake_invoke_stage, extractor)

        issues: list[str] = []
        artifacts = []
        result = extractor._run_batched_stage(
            prompt_bundle=prompt_bundle,
            stage=stage,
            document_text="\n".join(
                [
                    "| 펀드코드 | 펀드명 | 설정금액 | 해지금액 |",
                    "| --- | --- | --- | --- |",
                    "| 6114 | 혼합성장형 | 24,502 | -396,168 |",
                ]
            ),
            input_items=[{"fund_code": "6114", "fund_name": "혼합성장형", "base_date": "2026-04-03"}],
            response_model=FundSlotResult,
            issues=issues,
            artifacts=artifacts,
            counterparty_guidance=None,
        )

        self.assertEqual(call_count, 2)
        self.assertEqual(
            [(item.fund_code, item.t_day, item.slot_id) for item in result],
            [("6114", 0, "T0_SUB"), ("6114", 0, "T0_RED")],
        )
        self.assertEqual(issues, [])
        self.assertEqual(artifacts, [])

    def test_run_batched_stage_accepts_t_day_same_family_recovery(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.llm_stage_issue_retry_attempts = 1
        extractor.stage_batch_size = 12

        prompt_bundle = _load_prompt_bundle()
        stage = prompt_bundle.stages["t_day"]
        responses = iter(
            [
                types.SimpleNamespace(
                    parsed=FundSlotResult(
                        items=[
                            FundSlotItem(
                                fund_code="F001",
                                fund_name="Alpha",
                                base_date="2025-11-27",
                                t_day=None,
                                slot_id="",
                                evidence_label=None,
                            )
                        ],
                        issues=["T_DAY_MISSING"],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27","t_day":null,"slot_id":"","evidence_label":null}],"issues":["T_DAY_MISSING"]}',
                ),
                types.SimpleNamespace(
                    parsed=FundSlotResult(
                        items=[
                            FundSlotItem(
                                fund_code="F001",
                                fund_name="Alpha",
                                base_date="2025-11-27",
                                t_day=0,
                                slot_id="T0_NET",
                                evidence_label="당일이체금액",
                            )
                        ],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27","t_day":0,"slot_id":"T0_NET","evidence_label":"당일이체금액"}],"issues":[]}',
                ),
            ]
        )

        def fake_invoke_stage(
            self,
            *,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            batch_index,
            response_model,
            counterparty_guidance=None,
        ):
            return next(responses)

        extractor._invoke_stage = types.MethodType(fake_invoke_stage, extractor)

        issues: list[str] = []
        artifacts = []
        result = extractor._run_batched_stage(
            prompt_bundle=prompt_bundle,
            stage=stage,
            document_text="sample with 당일이체금액",
            input_items=[{"fund_code": "F001", "fund_name": "Alpha", "base_date": "2025-11-27"}],
            response_model=FundSlotResult,
            issues=issues,
            artifacts=artifacts,
            counterparty_guidance=None,
        )

        self.assertEqual([(item.fund_code, item.t_day, item.slot_id) for item in result], [("F001", 0, "T0_NET")])
        self.assertEqual(issues, [])
        self.assertEqual(artifacts, [])

    def test_run_batched_stage_rejects_t_day_same_family_ghost_slot(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.llm_stage_issue_retry_attempts = 1
        extractor.stage_batch_size = 12

        prompt_bundle = _load_prompt_bundle()
        stage = prompt_bundle.stages["t_day"]
        responses = iter(
            [
                types.SimpleNamespace(
                    parsed=FundSlotResult(
                        items=[
                            FundSlotItem(
                                fund_code="F001",
                                fund_name="Alpha",
                                base_date="2025-11-27",
                                t_day=None,
                                slot_id="",
                                evidence_label=None,
                            )
                        ],
                        issues=["T_DAY_MISSING"],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27","t_day":null,"slot_id":"","evidence_label":null}],"issues":["T_DAY_MISSING"]}',
                ),
                types.SimpleNamespace(
                    parsed=FundSlotResult(
                        items=[
                            FundSlotItem(
                                fund_code="F001",
                                fund_name="Alpha",
                                base_date="2025-11-27",
                                t_day=0,
                                slot_id="T0_NET",
                                evidence_label="당일이체금액",
                            ),
                            FundSlotItem(
                                fund_code="F001",
                                fund_name="Alpha",
                                base_date="2025-11-27",
                                t_day=99,
                                slot_id="T99_GHOST",
                                evidence_label="유령슬롯",
                            ),
                        ],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27","t_day":0,"slot_id":"T0_NET","evidence_label":"당일이체금액"},{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27","t_day":99,"slot_id":"T99_GHOST","evidence_label":"유령슬롯"}],"issues":[]}',
                ),
            ]
        )

        def fake_invoke_stage(
            self,
            *,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            batch_index,
            response_model,
            counterparty_guidance=None,
        ):
            return next(responses)

        extractor._invoke_stage = types.MethodType(fake_invoke_stage, extractor)

        issues: list[str] = []
        artifacts = []
        result = extractor._run_batched_stage(
            prompt_bundle=prompt_bundle,
            stage=stage,
            document_text="sample with 당일이체금액 only",
            input_items=[{"fund_code": "F001", "fund_name": "Alpha", "base_date": "2025-11-27"}],
            response_model=FundSlotResult,
            issues=issues,
            artifacts=artifacts,
            counterparty_guidance=None,
        )

        self.assertEqual([(item.fund_code, item.t_day, item.slot_id) for item in result], [("F001", None, "")])
        self.assertEqual(issues, ["T_DAY_MISSING", "T_DAY_STAGE_PARTIAL"])
        self.assertEqual(artifacts, [])

    def test_run_batched_stage_rejects_t_day_same_family_duplicate_evidence_slot(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.llm_stage_issue_retry_attempts = 1
        extractor.stage_batch_size = 12

        prompt_bundle = _load_prompt_bundle()
        stage = prompt_bundle.stages["t_day"]
        responses = iter(
            [
                types.SimpleNamespace(
                    parsed=FundSlotResult(
                        items=[
                            FundSlotItem(
                                fund_code="F001",
                                fund_name="Alpha",
                                base_date="2025-11-27",
                                t_day=None,
                                slot_id="",
                                evidence_label=None,
                            )
                        ],
                        issues=["T_DAY_MISSING"],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27","t_day":null,"slot_id":"","evidence_label":null}],"issues":["T_DAY_MISSING"]}',
                ),
                types.SimpleNamespace(
                    parsed=FundSlotResult(
                        items=[
                            FundSlotItem(
                                fund_code="F001",
                                fund_name="Alpha",
                                base_date="2025-11-27",
                                t_day=0,
                                slot_id="T0_NET",
                                evidence_label="당일이체금액",
                            ),
                            FundSlotItem(
                                fund_code="F001",
                                fund_name="Alpha",
                                base_date="2025-11-27",
                                t_day=0,
                                slot_id="T0_GHOST",
                                evidence_label="당일이체금액",
                            ),
                        ],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27","t_day":0,"slot_id":"T0_NET","evidence_label":"당일이체금액"},{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27","t_day":0,"slot_id":"T0_GHOST","evidence_label":"당일이체금액"}],"issues":[]}',
                ),
            ]
        )

        def fake_invoke_stage(
            self,
            *,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            batch_index,
            response_model,
            counterparty_guidance=None,
        ):
            return next(responses)

        extractor._invoke_stage = types.MethodType(fake_invoke_stage, extractor)

        issues: list[str] = []
        artifacts = []
        result = extractor._run_batched_stage(
            prompt_bundle=prompt_bundle,
            stage=stage,
            document_text="sample with 당일이체금액 only",
            input_items=[{"fund_code": "F001", "fund_name": "Alpha", "base_date": "2025-11-27"}],
            response_model=FundSlotResult,
            issues=issues,
            artifacts=artifacts,
            counterparty_guidance=None,
        )

        self.assertEqual([(item.fund_code, item.t_day, item.slot_id) for item in result], [("F001", None, "")])
        self.assertEqual(issues, ["T_DAY_MISSING", "T_DAY_STAGE_PARTIAL"])
        self.assertEqual(artifacts, [])

    def test_run_batched_stage_rejects_t_day_complete_family_conflicting_evidence_label(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.llm_stage_issue_retry_attempts = 1
        extractor.stage_batch_size = 12

        prompt_bundle = _load_prompt_bundle()
        stage = prompt_bundle.stages["t_day"]
        responses = iter(
            [
                types.SimpleNamespace(
                    parsed=FundSlotResult(
                        items=[
                            FundSlotItem(
                                fund_code="F001",
                                fund_name="Alpha",
                                base_date="2025-11-27",
                                t_day=0,
                                slot_id="T0_NET",
                                evidence_label="당일이체금액",
                            )
                        ],
                        issues=["T_DAY_MISSING"],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27","t_day":0,"slot_id":"T0_NET","evidence_label":"당일이체금액"}],"issues":["T_DAY_MISSING"]}',
                ),
                types.SimpleNamespace(
                    parsed=FundSlotResult(
                        items=[
                            FundSlotItem(
                                fund_code="F001",
                                fund_name="Alpha",
                                base_date="2025-11-27",
                                t_day=0,
                                slot_id="T0_NET",
                                evidence_label="익영업일이체금액",
                            )
                        ],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27","t_day":0,"slot_id":"T0_NET","evidence_label":"익영업일이체금액"}],"issues":[]}',
                ),
            ]
        )

        def fake_invoke_stage(
            self,
            *,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            batch_index,
            response_model,
            counterparty_guidance=None,
        ):
            return next(responses)

        extractor._invoke_stage = types.MethodType(fake_invoke_stage, extractor)

        issues: list[str] = []
        artifacts = []
        result = extractor._run_batched_stage(
            prompt_bundle=prompt_bundle,
            stage=stage,
            document_text="sample with 당일이체금액 and 익영업일이체금액",
            input_items=[{"fund_code": "F001", "fund_name": "Alpha", "base_date": "2025-11-27"}],
            response_model=FundSlotResult,
            issues=issues,
            artifacts=artifacts,
            counterparty_guidance=None,
        )

        self.assertEqual(
            [(item.fund_code, item.t_day, item.slot_id, item.evidence_label) for item in result],
            [("F001", 0, "T0_NET", "익영업일이체금액")],
        )
        self.assertEqual(issues, [])
        self.assertEqual(artifacts, [])

    def test_stage_partial_issue_code_flags_unexpected_extra_output(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        issue_code = extractor._stage_partial_issue_code(
            stage=types.SimpleNamespace(name="transfer_amount"),
            input_items=[
                {
                    "fund_code": "F001",
                    "fund_name": "Alpha",
                    "base_date": "2025-11-27",
                    "t_day": 0,
                    "slot_id": "T0_NET",
                    "evidence_label": "당일이체금액",
                }
            ],
            output_items=[
                FundAmountItem(
                    fund_code="F001",
                    fund_name="Alpha",
                    base_date="2025-11-27",
                    t_day=0,
                    slot_id="T0_NET",
                    evidence_label="당일이체금액",
                    transfer_amount="100",
                ),
                FundAmountItem(
                    fund_code="F999",
                    fund_name="Ghost",
                    base_date="2025-11-27",
                    t_day=0,
                    slot_id="T0_NET",
                    evidence_label="당일이체금액",
                    transfer_amount="999",
                ),
            ],
        )

        self.assertEqual(issue_code, "TRANSFER_AMOUNT_STAGE_PARTIAL")

    def test_stage_partial_issue_code_flags_duplicate_t_day_evidence_output(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        issue_code = extractor._stage_partial_issue_code(
            stage=types.SimpleNamespace(name="t_day"),
            input_items=[
                {
                    "fund_code": "F001",
                    "fund_name": "Alpha",
                    "base_date": "2025-11-27",
                }
            ],
            output_items=[
                FundSlotItem(
                    fund_code="F001",
                    fund_name="Alpha",
                    base_date="2025-11-27",
                    t_day=0,
                    slot_id="T0_NET",
                    evidence_label="당일이체금액",
                ),
                FundSlotItem(
                    fund_code="F001",
                    fund_name="Alpha",
                    base_date="2025-11-27",
                    t_day=0,
                    slot_id="T0_GHOST",
                    evidence_label="당일이체금액",
                ),
            ],
        )

        self.assertEqual(issue_code, "T_DAY_STAGE_PARTIAL")

    def test_stage_partial_issue_code_flags_missing_required_t_day_field_even_without_stage_issue(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        issue_code = extractor._stage_partial_issue_code(
            stage=types.SimpleNamespace(name="t_day"),
            input_items=[
                {
                    "fund_code": "F6114",
                    "fund_name": "Index",
                    "base_date": "2025-08-26",
                }
            ],
            output_items=[
                {
                    "fund_code": "F6114",
                    "fund_name": "Index",
                    "base_date": "2025-08-26",
                    "t_day": None,
                    "slot_id": "",
                    "evidence_label": "설정금액",
                }
            ],
        )

        self.assertEqual(issue_code, "T_DAY_STAGE_PARTIAL")

    def test_collect_stage_findings_flags_invalid_transfer_amount_text_even_without_explicit_issue(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        stage_issues, partial_issue = extractor._collect_stage_findings(
            stage=types.SimpleNamespace(name="transfer_amount"),
            document_text="sample",
            input_items=[
                {
                    "fund_code": "F001",
                    "fund_name": "Alpha",
                    "base_date": "2025-11-27",
                    "t_day": 0,
                    "slot_id": "T0_NET",
                    "evidence_label": "당일이체금액",
                }
            ],
            parsed=FundAmountResult(
                items=[
                    FundAmountItem(
                        fund_code="F001",
                        fund_name="Alpha",
                        base_date="2025-11-27",
                        t_day=0,
                        slot_id="T0_NET",
                        evidence_label="당일이체금액",
                        transfer_amount="KRW100",
                    )
                ],
                issues=[],
            ),
        )

        self.assertEqual(stage_issues, ["TRANSFER_AMOUNT_MISSING"])
        self.assertEqual(partial_issue, "TRANSFER_AMOUNT_STAGE_PARTIAL")

    def test_collect_stage_findings_flags_invalid_settle_class_even_without_explicit_issue(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        stage_issues, partial_issue = extractor._collect_stage_findings(
            stage=types.SimpleNamespace(name="settle_class"),
            document_text="sample",
            input_items=[
                {
                    "fund_code": "F001",
                    "fund_name": "Alpha",
                    "base_date": "2025-11-27",
                    "t_day": 0,
                    "slot_id": "T0_NET",
                    "evidence_label": "정산액",
                    "transfer_amount": "100",
                }
            ],
            parsed=FundSettleResult(
                items=[
                    FundSettleItem(
                        fund_code="F001",
                        fund_name="Alpha",
                        base_date="2025-11-27",
                        t_day=None,
                        slot_id="T0_NET",
                        evidence_label="정산액",
                        transfer_amount="100",
                        settle_class="UNKNOWN",
                    )
                ],
                issues=[],
            ),
        )

        self.assertEqual(stage_issues, ["SETTLE_CLASS_MISSING"])
        self.assertEqual(partial_issue, "SETTLE_CLASS_STAGE_PARTIAL")

    def test_stage_partial_issue_code_flags_duplicate_transfer_amount_signature_output(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        issue_code = extractor._stage_partial_issue_code(
            stage=types.SimpleNamespace(name="transfer_amount"),
            input_items=[
                {
                    "fund_code": "F001",
                    "fund_name": "Alpha",
                    "base_date": "2025-11-27",
                    "t_day": 0,
                    "slot_id": "T0_NET",
                    "evidence_label": "당일이체금액",
                }
            ],
            output_items=[
                FundAmountItem(
                    fund_code="F001",
                    fund_name="Alpha",
                    base_date="2025-11-27",
                    t_day=0,
                    slot_id="T0_NET",
                    evidence_label="당일이체금액",
                    transfer_amount="100",
                ),
                FundAmountItem(
                    fund_code="F001",
                    fund_name="Alpha",
                    base_date="2025-11-27",
                    t_day=0,
                    slot_id="T0_NET",
                    evidence_label="당일이체금액",
                    transfer_amount="999",
                ),
            ],
        )

        self.assertEqual(issue_code, "TRANSFER_AMOUNT_STAGE_PARTIAL")

    def test_stage_partial_issue_code_allows_identical_duplicate_transfer_amount_output(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        issue_code = extractor._stage_partial_issue_code(
            stage=types.SimpleNamespace(name="transfer_amount"),
            input_items=[
                {
                    "fund_code": "F001",
                    "fund_name": "Alpha",
                    "base_date": "2025-11-27",
                    "t_day": 0,
                    "slot_id": "T0_NET",
                    "evidence_label": "당일이체금액",
                }
            ],
            output_items=[
                FundAmountItem(
                    fund_code="F001",
                    fund_name="Alpha",
                    base_date="2025-11-27",
                    t_day=0,
                    slot_id="T0_NET",
                    evidence_label="당일이체금액",
                    transfer_amount="100",
                ),
                FundAmountItem(
                    fund_code="F001",
                    fund_name="Alpha",
                    base_date="2025-11-27",
                    t_day=0,
                    slot_id="T0_NET",
                    evidence_label="당일이체금액",
                    transfer_amount="100",
                ),
            ],
        )

        self.assertIsNone(issue_code)

    def test_stage_partial_issue_code_allows_identical_duplicate_t_day_output(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        issue_code = extractor._stage_partial_issue_code(
            stage=types.SimpleNamespace(name="t_day"),
            input_items=[
                {
                    "fund_code": "F001",
                    "fund_name": "Alpha",
                    "base_date": "2025-11-27",
                }
            ],
            output_items=[
                FundSlotItem(
                    fund_code="F001",
                    fund_name="Alpha",
                    base_date="2025-11-27",
                    t_day=0,
                    slot_id="T0_NET",
                    evidence_label="당일이체금액",
                ),
                FundSlotItem(
                    fund_code="F001",
                    fund_name="Alpha",
                    base_date="2025-11-27",
                    t_day=0,
                    slot_id="T0_NET",
                    evidence_label="당일이체금액",
                ),
            ],
        )

        self.assertIsNone(issue_code)

    def test_run_batched_stage_retries_up_to_three_times_after_initial_issue(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.llm_stage_issue_retry_attempts = 3
        extractor.stage_batch_size = 12

        prompt_bundle = _load_prompt_bundle()
        stage = prompt_bundle.stages["base_date"]
        responses = iter(
            [
                types.SimpleNamespace(
                    parsed=FundBaseDateResult(
                        items=[FundBaseDateItem(fund_code="F001", fund_name="Alpha", base_date=None)],
                        issues=["BASE_DATE_MISSING"],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":null}],"issues":["BASE_DATE_MISSING"]}',
                ),
                types.SimpleNamespace(
                    parsed=FundBaseDateResult(
                        items=[FundBaseDateItem(fund_code="F001", fund_name="Alpha", base_date=None)],
                        issues=["BASE_DATE_MISSING"],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":null}],"issues":["BASE_DATE_MISSING"]}',
                ),
                types.SimpleNamespace(
                    parsed=FundBaseDateResult(
                        items=[FundBaseDateItem(fund_code="F001", fund_name="Alpha", base_date=None)],
                        issues=["BASE_DATE_MISSING"],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":null}],"issues":["BASE_DATE_MISSING"]}',
                ),
                types.SimpleNamespace(
                    parsed=FundBaseDateResult(
                        items=[FundBaseDateItem(fund_code="F001", fund_name="Alpha", base_date=None)],
                        issues=["BASE_DATE_MISSING"],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":null}],"issues":["BASE_DATE_MISSING"]}',
                ),
            ]
        )
        call_count = 0

        def fake_invoke_stage(
            self,
            *,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            batch_index,
            response_model,
            counterparty_guidance=None,
        ):
            nonlocal call_count
            call_count += 1
            return next(responses)

        extractor._invoke_stage = types.MethodType(fake_invoke_stage, extractor)

        issues: list[str] = []
        artifacts = []
        result = extractor._run_batched_stage(
            prompt_bundle=prompt_bundle,
            stage=stage,
            document_text="sample",
            input_items=[{"fund_code": "F001", "fund_name": "Alpha"}],
            response_model=FundBaseDateResult,
            issues=issues,
            artifacts=artifacts,
            counterparty_guidance=None,
        )

        self.assertEqual(call_count, 4)
        self.assertEqual([(item.fund_code, item.base_date) for item in result], [("F001", None)])
        self.assertEqual(issues, ["BASE_DATE_MISSING", "BASE_DATE_STAGE_PARTIAL"])
        self.assertEqual(artifacts, [])

    def test_run_batched_stage_continues_retry_after_intermediate_parsed_none(self) -> None:
        extractor = object.__new__(FundOrderExtractor)
        extractor.llm_stage_issue_retry_attempts = 3
        extractor.stage_batch_size = 12

        prompt_bundle = _load_prompt_bundle()
        stage = prompt_bundle.stages["base_date"]
        responses = iter(
            [
                types.SimpleNamespace(
                    parsed=FundBaseDateResult(
                        items=[FundBaseDateItem(fund_code="F001", fund_name="Alpha", base_date=None)],
                        issues=["BASE_DATE_MISSING"],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":null}],"issues":["BASE_DATE_MISSING"]}',
                ),
                types.SimpleNamespace(parsed=None, raw_response="invalid-json"),
                types.SimpleNamespace(
                    parsed=FundBaseDateResult(
                        items=[FundBaseDateItem(fund_code="F001", fund_name="Alpha", base_date="2025-11-27")],
                        issues=[],
                    ),
                    raw_response='{"items":[{"fund_code":"F001","fund_name":"Alpha","base_date":"2025-11-27"}],"issues":[]}',
                ),
            ]
        )
        call_count = 0

        def fake_invoke_stage(
            self,
            *,
            prompt_bundle,
            stage,
            document_text,
            input_items,
            batch_index,
            response_model,
            counterparty_guidance=None,
        ):
            nonlocal call_count
            call_count += 1
            return next(responses)

        extractor._invoke_stage = types.MethodType(fake_invoke_stage, extractor)

        issues: list[str] = []
        artifacts = []
        result = extractor._run_batched_stage(
            prompt_bundle=prompt_bundle,
            stage=stage,
            document_text="sample",
            input_items=[{"fund_code": "F001", "fund_name": "Alpha"}],
            response_model=FundBaseDateResult,
            issues=issues,
            artifacts=artifacts,
            counterparty_guidance=None,
        )

        self.assertEqual(call_count, 3)
        self.assertEqual([(item.fund_code, item.base_date) for item in result], [("F001", "2025-11-27")])
        self.assertEqual(issues, [])
        self.assertEqual(artifacts, [])


if __name__ == "__main__":
    unittest.main()
