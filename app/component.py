from __future__ import annotations

import csv
import json
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.config import Settings, get_settings
from app.document_loader import DocumentLoadTaskPayload, DocumentLoader
from app.extraction import (
    detect_counterparty_guidance_non_instruction_reason,
    load_counterparty_guidance,
    resolve_counterparty_prompt_name,
)
from app.extractor import (
    ExtractionOutcomeError,
    FundOrderExtractor,
    apply_only_pending_filter,
    build_extract_llm_log_path,
    write_invalid_response_debug_files,
)
from app.output_contract import (
    dedupe_serialized_order_payloads,
    format_output_t_day,
    format_output_transfer_amount,
    normalize_counterparty_output_order_payloads,
    serialize_order_payload,
    serialized_order_payload_identity,
)

# CSV는 사람이 검수하기 쉽도록 문서 메타데이터 + 주문 필드를 한 줄에 같이 둔다.
# JSON은 구조적 표현에, CSV는 빠른 눈검사와 엑셀 연동에 각각 맞춘 형태다.
CSV_FIELDNAMES = [
    "file_name",
    "source_path",
    "model_name",
    "document_base_date",
    "issues",
    "fund_code",
    "fund_name",
    "settle_class",
    "order_type",
    "base_date",
    "t_day",
    "transfer_amount",
]

STATUS_COMPLETED = "COMPLETED"
STATUS_SKIPPED = "SKIPPED"
STATUS_FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class DocumentExtractionRequest:
    """WAS/배치 코드가 추출 요청 1건을 전달할 때 쓰는 최소 단위 객체.

    지금까지는 CLI가 `Path`와 `pdf_password`를 직접 들고 service를 호출했다.
    하지만 다른 서버 코드로 포팅할 때는 "입력 한 건"을 명시적으로 표현하는 타입이
    있는 편이 더 읽기 쉽고, 추후 필드가 늘어도 호출 계약을 안정적으로 유지할 수 있다.
    """

    source_path: str | Path
    pdf_password: str | None = None
    # 외부 계약에서 "확정분만 남기고, 내부적으로는 settle_class를 PENDING으로 맞춘 뒤
    # 최종 출력에서는 pending 코드('1')로 보이게" 해야 할 때 쓴다.
    # 기본값은 False이며, 값이 True일 때만 최종 결과 직전에 후처리를 적용한다.
    only_pending: bool = False
    # 거래처별 문서 특징 프롬프트를 선택적으로 사용할지 제어한다.
    # 기본값은 False이며, True일 때만 source_path를 기준으로 대응하는 프롬프트 파일을 찾아
    # stage user prompt 안의 counterparty guidance 블록에 주입한다.
    use_counterparty_prompt: bool = False

    def as_path(self) -> Path:
        """문자열/Path 입력을 항상 `Path` 객체로 정규화한다.

        queue/WAS 코드에서는 요청 DTO를 만들 때 문자열을 넘기는 경우가 많고,
        테스트에서는 `Path` 객체를 직접 쓰는 경우도 있다.
        호출부마다 타입을 신경 쓰지 않게 하려면, 가장 바깥 경계에서 한 번만
        `Path`로 정규화해 두는 편이 이해하기 쉽고 실수도 줄어든다.
        """
        return Path(self.source_path).expanduser()


class ExtractionComponent:
    """CLI와 WAS가 공통으로 재사용하는 최상위 추출 컴포넌트.

    이 클래스의 목적은 두 가지다.
    1. 현재 로컬 CLI가 쓰는 추출/병합/CSV 평탄화 규칙을 한 곳으로 모은다.
    2. WAS 쪽에서는 `main.py`를 몰라도 이 컴포넌트만 import 해서 동일한 결과를 얻도록 한다.

    중요한 점은 이 컴포넌트가 더 이상 `ExtractionService`를 감싸지 않는다는 것이다.
    현재 구현은 사용자가 요구한 queue/WAS 아키텍처와 동일한 경계를 그대로 따른다.

    - handler A 역할: `DocumentLoader.build_task_payload()` + JSON 파일 저장
    - handler B 역할: 저장된 JSON 재로딩 + `FundOrderExtractor.extract_from_task_payload()`

    즉 이 클래스는 "service 우회용 얇은 래퍼"가 아니라,
    handler A/B 실행 순서를 재사용 가능한 코드로 묶은 facade라고 보면 된다.
    """

    def __init__(
        self,
        *,
        settings: Settings | None = None,
        document_loader: DocumentLoader | None = None,
        extractor: FundOrderExtractor | None = None,
    ) -> None:
        """외부 진입점에서 재사용할 공통 의존성을 한 번만 묶어 둔다.

        `ExtractionComponent`는 단순 편의 래퍼가 아니라,
        CLI와 WAS가 같은 handler A/B 경로를 타도록 만드는 파사드다.
        그래서 생성 시점에 loader/extractor/settings를 고정해 두면
        호출부는 문서마다 같은 조립 코드를 반복하지 않아도 된다.
        """
        # WAS/CLI가 같은 경로를 타도록 component가 직접 loader/extractor를 가진다.
        # 이렇게 해야 외부 통합 코드가 service 내부 구현에 다시 묶이지 않고,
        # handler A/B 계약을 component 레벨에서 그대로 재사용할 수 있다.
        # 외부 호출자는 보통 component 하나만 생성해서 여러 문서를 순차 처리한다.
        # 그래서 settings/loader/extractor를 생성자에서 고정해 두면
        # 호출부가 매번 같은 객체를 다시 만들 필요가 없다.
        self.settings = settings or get_settings()
        self.loader = document_loader or DocumentLoader()
        self.extractor = extractor or FundOrderExtractor(self.settings)
        # component도 service와 동일하게 debug artifact를 저장할 수 있어야 하므로
        # 생성 시점에 디렉터리를 보장한다.
        self.settings.debug_output_dir.mkdir(parents=True, exist_ok=True)
        # handler A 결과물을 파일로 남길 기본 루트다.
        # env에서 보존/삭제 정책을 바꿔도 이 경로를 기준으로 동작한다.
        self.settings.task_payload_output_dir.mkdir(parents=True, exist_ok=True)

    def _candidate_source_paths(self, file_path: Path) -> list[Path]:
        """사용자 입력 경로를 실제 파일 경로 후보들로 확장한다.

        CLI/WAS 모두 파일명만 받아 실행할 수 있으므로,
        bare filename은 `DOCUMENT_INPUT_DIR`를 먼저 보고 현재 작업 디렉터리를 나중에 본다.
        사용자가 디렉터리를 포함한 상대경로를 준 경우에는 그 의도를 존중해 원래 경로를 먼저 본다.
        """
        # WAS/CLI 모두 사용자 입력은 문자열일 가능성이 높으므로,
        # 가장 먼저 홈 디렉터리 확장을 적용한다.
        normalized_path = file_path.expanduser()
        if normalized_path.is_absolute():
            candidates = [normalized_path]
        elif normalized_path.parent == Path("."):
            candidates = [
                (self.settings.document_input_dir / normalized_path).expanduser(),
                normalized_path,
            ]
        else:
            candidates = [
                normalized_path,
                (self.settings.document_input_dir / normalized_path).expanduser(),
            ]

        # 경로 후보가 중복되면 같은 파일을 여러 번 검사하게 되므로 순서 보존 dedupe를 적용한다.
        unique_candidates: list[Path] = []
        seen: set[Path] = set()
        for candidate in candidates:
            if candidate in seen:
                continue
            seen.add(candidate)
            unique_candidates.append(candidate)
        return unique_candidates

    def _validate_source_path(self, file_path: Path) -> Path:
        """호출자가 준 경로 또는 파일명을 실제 문서 경로로 해석한다.

        이 검증을 component 안에 두는 이유는, handler A/B 경로로 실행하는 모든 호출자가
        같은 경로 해석 규칙을 공유하게 하려는 것이다.
        즉 CLI, WAS, 배치가 각각 따로 경로 규칙을 구현하지 않도록 여기서 흡수한다.
        """
        # component는 외부 진입점이므로, 여기서 경로 해석 규칙을 통일해 두면
        # CLI/WAS/배치가 각자 다른 방식으로 파일을 찾는 문제를 줄일 수 있다.
        candidates = self._candidate_source_paths(file_path)
        for candidate in candidates:
            if candidate.exists():
                if not candidate.is_file():
                    raise ValueError(f"Not a file: {candidate}")
                return candidate

        candidate_text = ", ".join(str(candidate) for candidate in candidates)
        raise ValueError(f"File not found. checked=[{candidate_text}]")

    @staticmethod
    def _format_output_transfer_amount(order_type: str | None, transfer_amount: str | None) -> str | None:
        """공통 출력 계약 helper로 위임한다.

        component와 service가 서로 다른 규칙으로 직렬화되면
        같은 문서를 어느 진입점으로 실행했는지에 따라 JSON/CSV 계약이 달라진다.
        그래서 실제 구현은 공통 helper에 두고, 이 메서드는 호환용 wrapper로만 남긴다.
        """
        return format_output_transfer_amount(order_type=order_type, transfer_amount=transfer_amount)

    @staticmethod
    def _format_output_t_day(settle_class: str | None, t_day: Any) -> str | None:
        """공통 출력 계약 helper로 위임한다."""
        return format_output_t_day(settle_class=settle_class, t_day=t_day)

    @staticmethod
    def _serialize_order_payload(order: Any) -> dict[str, Any]:
        """공통 출력 계약 helper를 사용해 최종 payload를 직렬화한다.

        최신 요구사항에서는 최종 출력 단계에서만 아래 계약을 적용한다.
        - settle_class: PENDING -> "1", CONFIRMED -> "2"
        - order_type: RED -> "1", SUB -> "3"

        이 규칙은 내부 모델이 아니라 외부 JSON/CSV에만 적용해야 하므로,
        실제 직렬화는 공통 helper 한 곳에서 처리한다.
        """
        return serialize_order_payload(order)

    @staticmethod
    def _non_instruction_error_message(task_payload: DocumentLoadTaskPayload, reason: str) -> str:
        """비지시서 skip reason을 WAS와 같은 문장 형태로 만든다."""
        return (
            "Document is not a variable-annuity order instruction. "
            f"path={task_payload.source_path} reason={reason}"
        )

    @staticmethod
    def _classify_document_status(
        *,
        task_payload: DocumentLoadTaskPayload,
        has_orders: bool,
    ) -> tuple[str, str | None]:
        """문서 1건의 최종 상태를 COMPLETED/SKIPPED/FAILED로 정규화한다."""
        if has_orders:
            return STATUS_COMPLETED, None
        if task_payload.non_instruction_reason:
            return (
                STATUS_SKIPPED,
                ExtractionComponent._non_instruction_error_message(
                    task_payload, task_payload.non_instruction_reason
                ),
            )
        if task_payload.allow_empty_result:
            return STATUS_SKIPPED, "거래가 없는 문서"
        if task_payload.scope_excludes_all_funds:
            return STATUS_SKIPPED, "대상 거래처 scope에 해당하는 주문 없음"
        return STATUS_SKIPPED, "추출된 주문 데이터 없음"

    @staticmethod
    def _classify_merged_status(document_payloads: list[dict[str, Any]], merged_orders: list[dict[str, Any]]) -> tuple[str, str | None]:
        """문서별 payload를 합친 뒤 최종 상태를 정규화한다."""
        if merged_orders:
            return STATUS_COMPLETED, None
        document_statuses = [str(payload.get("status", "")) for payload in document_payloads]
        if document_statuses and all(status == STATUS_SKIPPED for status in document_statuses):
            reasons = [
                str(payload.get("reason", "")).strip()
                for payload in document_payloads
                if str(payload.get("reason", "")).strip()
            ]
            if not reasons:
                return STATUS_SKIPPED, "모든 문서가 제외됨"
            merged_reason = "; ".join(dict.fromkeys(reasons))
            return STATUS_SKIPPED, merged_reason
        return STATUS_FAILED, "병합 결과 저장할 주문 데이터 없음"

    def _build_skipped_payload(
        self,
        *,
        task_payload: DocumentLoadTaskPayload,
        reason: str,
    ) -> dict[str, Any]:
        """지시서 아님/정상 0건 문서를 local payload 계약으로 정규화한다."""
        return {
            "file_name": task_payload.file_name,
            "source_path": task_payload.source_path,
            "model_name": self.settings.llm_model,
            "base_date": None,
            "status": STATUS_SKIPPED,
            "reason": reason,
            "issues": [],
            "orders": [],
        }

    def _build_failed_payload(
        self,
        *,
        task_payload: DocumentLoadTaskPayload,
        reason: str,
        issues: list[str] | None = None,
    ) -> dict[str, Any]:
        """실패한 문서도 외부 payload 계약으로 정규화한다.

        로컬 단건 추출 경로도 WAS와 같은 상태 체계를 보이려면,
        예외를 그대로 호출자에게 넘기기보다 최소 메타데이터를 갖춘 FAILED payload로
        반환할 수 있어야 한다. 실패 payload는 후속 merge/CSV 경로에서 오해를 줄이기 위해
        주문을 비우고, 실패 사유와 issue만 싣는다.
        """
        return {
            "file_name": task_payload.file_name,
            "source_path": task_payload.source_path,
            "model_name": self.settings.llm_model,
            "base_date": None,
            "status": STATUS_FAILED,
            "reason": reason,
            "issues": list(issues or []),
            "orders": [],
        }

    def _build_extract_log_path(self, *, source_name: str) -> Path:
        """로컬 extractor가 prompt/response 로그를 남길 파일 경로를 만든다."""
        return build_extract_llm_log_path(
            debug_output_dir=self.settings.debug_output_dir,
            source_name=source_name,
        )

    def _build_result_payload(
        self,
        *,
        task_payload: DocumentLoadTaskPayload,
        extraction_result: Any,
    ) -> dict[str, Any]:
        """handler B 완료 후 외부 표준 payload를 만든다.

        handler B의 내부 결과는 `ExtractionResult`와 `OrderExtraction` 모델이지만,
        외부 계약(JSON/CSV/다건 merge)은 dict 기반 payload를 기준으로 움직인다.
        그래서 "추출 완료"와 "외부 계약 직렬화"의 경계를 이 메서드 하나에 모아 둔다.
        """
        # 주문 목록이 비어 있지 않다면 첫 유효 기준일을 문서 메타데이터로 끌어올린다.
        # 다건 merge에서도 문서별 base_date를 따로 추적할 수 있게 하기 위함이다.
        prompt_name = resolve_counterparty_prompt_name(
            task_payload.source_path,
            document_text=task_payload.markdown_text,
        )
        order_payloads = normalize_counterparty_output_order_payloads(
            [self._serialize_order_payload(order) for order in extraction_result.orders],
            prompt_name=prompt_name,
        )
        order_payloads = dedupe_serialized_order_payloads(order_payloads)
        base_date = next((order.get("base_date") for order in order_payloads if order.get("base_date")), None)
        status, reason = self._classify_document_status(
            task_payload=task_payload,
            has_orders=bool(order_payloads),
        )
        return {
            "file_name": task_payload.file_name,
            "source_path": task_payload.source_path,
            "model_name": self.settings.llm_model,
            "base_date": base_date,
            "status": status,
            "reason": reason,
            "issues": extraction_result.issues,
            "orders": order_payloads,
        }

    @staticmethod
    def _default_handoff_file_name(index: int, request: DocumentExtractionRequest) -> str:
        """handler A 중간 파일명을 입력 순서 기준으로 안정적으로 만든다.

        왜 입력 순서를 파일명에 넣는가:
        - 같은 이름의 문서가 다른 폴더에서 들어와도 충돌을 줄이고
        - 다건 실행에서 어떤 입력이 어떤 handoff 파일로 저장됐는지 추적하기 쉽게 만들기 위해서다.
        """
        # request DTO는 문자열/Path 모두 허용하므로 먼저 Path로 정규화한다.
        source_path = request.as_path()
        stem = source_path.stem or f"document_{index:02d}"
        return f"{index:02d}_{stem}_task_payload.json"

    def _build_internal_handoff_dir(self) -> Path:
        """내부적으로 사용할 handoff 디렉터리를 만든다.

        호출자가 handoff 경로를 직접 주지 않는 경우에도 로더 결과는 반드시 파일로 한 번 저장한다.
        삭제 옵션이 꺼져 있으면 이 디렉터리가 그대로 보존되어 운영/검수에서 재확인할 수 있다.
        """
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        handoff_dir = self.settings.task_payload_output_dir / run_id
        handoff_dir.mkdir(parents=True, exist_ok=True)
        return handoff_dir

    def _cleanup_handoff_dir(self, handoff_dir: Path) -> None:
        """내부적으로 관리한 handoff 디렉터리를 정리한다.

        env에서 `DELETE_TASK_PAYLOAD_FILES=true`일 때만 호출한다.
        외부에서 handoff_dir를 명시적으로 넘긴 경우에는 호출자가 소유권을 갖는다고 보고
        component가 임의로 삭제하지 않는다.
        """
        for path in sorted(handoff_dir.glob("**/*"), reverse=True):
            if path.is_file():
                path.unlink(missing_ok=True)
            elif path.is_dir():
                try:
                    path.rmdir()
                except OSError:
                    pass
        try:
            handoff_dir.rmdir()
        except OSError:
            pass

    def run_handler_a(self, request: DocumentExtractionRequest, task_payload_json_path: Path) -> Path:
        """문서 로딩 단계(handler A)를 실행하고 결과를 파일로 저장한다.

        여기서는 "문서를 읽어 다음 단계 재료를 만든다"까지만 수행한다.
        최종 추출은 하지 않는다. 즉 이 메서드의 출력은 `최종 주문`이 아니라
        `Handler B가 다시 읽어 사용할 로딩 산출물 파일 경로`다.
        """
        # handler A는 "문서를 읽는다"까지만 책임진다.
        # 그래서 이 메서드는 최종 주문이 아니라 handoff 파일 경로를 반환한다.
        normalized_path = self._validate_source_path(request.as_path())
        task_payload = self.loader.build_task_payload(
            normalized_path,
            chunk_size_chars=self.settings.llm_chunk_size_chars,
            pdf_password=request.pdf_password,
        )
        # DocumentLoadTaskPayload는 queue 메시지로도, 파일 handoff로도 사용할 수 있는
        # 로더 단계 DTO다. 현재 구현은 파일 기반 handoff를 기본 계약으로 쓴다.
        task_payload.write_json(task_payload_json_path)
        return task_payload_json_path

    def run_handler_b(
        self,
        task_payload_json_path: Path,
        *,
        only_pending: bool = False,
        use_counterparty_prompt: bool = False,
    ) -> dict[str, Any]:
        """handler A 산출물 파일을 읽어 실제 추출(handler B)을 수행한다.

        이 메서드는 queue consumer가 해야 하는 일을 그대로 구현한 것이다.
        - handoff 파일 읽기
        - extractor 호출
        - 실패 시 invalid artifact 저장
        - 성공 시 외부 payload 직렬화

        따라서 WAS에서는 이 메서드의 흐름을 거의 그대로 옮기면 된다.
        """
        # handler B는 원문 파일을 다시 열지 않고, handler A 산출물을 그대로 재사용한다.
        # 이 구조가 있어야 queue/WAS에서 프로세스가 분리돼도 같은 결과를 재현할 수 있다.
        task_payload = DocumentLoadTaskPayload.read_json(task_payload_json_path)
        counterparty_guidance = load_counterparty_guidance(
            task_payload.source_path,
            use_counterparty_prompt=use_counterparty_prompt,
            document_text=task_payload.markdown_text,
        )
        if task_payload.non_instruction_reason:
            return self._build_skipped_payload(
                task_payload=task_payload,
                reason=self._non_instruction_error_message(
                    task_payload,
                    task_payload.non_instruction_reason,
                ),
            )
        if task_payload.allow_empty_result:
            return self._build_skipped_payload(
                task_payload=task_payload,
                reason="거래가 없는 문서",
            )
        if task_payload.scope_excludes_all_funds:
            return self._build_skipped_payload(
                task_payload=task_payload,
                reason="대상 거래처 scope에 해당하는 주문 없음",
            )
        prompt_directed_reason = detect_counterparty_guidance_non_instruction_reason(
            counterparty_guidance=counterparty_guidance,
            markdown_text=task_payload.markdown_text,
            raw_text=task_payload.raw_text,
        )
        if prompt_directed_reason:
            return self._build_skipped_payload(
                task_payload=task_payload,
                reason=self._non_instruction_error_message(task_payload, prompt_directed_reason),
            )
        try:
            outcome = self.extractor.extract_from_task_payload(
                task_payload,
                counterparty_guidance=counterparty_guidance,
                extract_log_path=self._build_extract_log_path(source_name=task_payload.file_name),
            )
        except ExtractionOutcomeError as exc:
            # handler B는 실패하더라도 원문 응답 artifact를 남겨야 운영 디버깅이 가능하다.
            # 그래서 service 없이 component를 직접 쓰는 경로도 같은 helper를 사용한다.
            write_invalid_response_debug_files(
                debug_output_dir=self.settings.debug_output_dir,
                source_name=task_payload.file_name,
                artifacts=exc.invalid_response_artifacts,
            )
            return self._build_failed_payload(
                task_payload=task_payload,
                reason=str(exc),
                issues=exc.result.issues or [str(exc)],
            )
        except ValueError as exc:
            if "not a variable-annuity order instruction" in str(exc):
                return self._build_skipped_payload(task_payload=task_payload, reason=str(exc))
            return self._build_failed_payload(
                task_payload=task_payload,
                reason=str(exc),
                issues=[str(exc)],
            )
        write_invalid_response_debug_files(
            debug_output_dir=self.settings.debug_output_dir,
            source_name=task_payload.file_name,
            artifacts=outcome.invalid_response_artifacts,
        )
        filtered_result = apply_only_pending_filter(outcome.result, only_pending=only_pending)
        # `only_pending`은 추출 중간 단계가 아니라 "최종 외부 출력 계약"을 바꾸는 옵션이다.
        # 그래서 extractor의 검증/coverage 판단이 모두 끝난 뒤 마지막 결과에만 적용한다.
        #
        # 요구사항은 이름과 달리 아래 순서를 강제한다.
        # 1. 기존 PENDING 주문 제거
        # 2. 남은 주문의 settle_class를 모두 PENDING 으로 변경
        # 3. 최종 직렬화 시 PENDING -> "1", CONFIRMED -> "2" 코드로 치환
        #
        # 이 규칙을 run_handler_b에서 적용하면 CLI/WAS/service 직접 호출이 모두 같은
        # 최종 출력 계약을 공유할 수 있다.
        # extractor의 내부 모델 결과를 외부 계약 payload로 바꿔 반환한다.
        return self._build_result_payload(task_payload=task_payload, extraction_result=filtered_result)

    def extract_document_payload(
        self,
        request: DocumentExtractionRequest,
        *,
        handoff_dir: Path | None = None,
        handoff_index: int = 1,
    ) -> dict[str, Any]:
        """문서 1건을 현재 외부 JSON 계약(payload)으로 추출한다.

        구현상으로는 반드시 handler A -> 파일 저장 -> handler B 순서를 따른다.
        즉 단건 추출도 queue handoff 경로와 같은 코드를 타므로,
        CLI와 WAS가 서로 다른 추출 경로로 어긋나는 문제를 줄일 수 있다.
        `only_pending` 같은 최종 출력 옵션도 request DTO를 통해 이 경로로 함께 전달된다.
        """
        managed_handoff_dir: Path | None = None
        if handoff_dir is None:
            # 기본 호출 경로도 반드시 파일 handoff를 거치되,
            # 삭제 여부는 env 설정을 따른다.
            managed_handoff_dir = self._build_internal_handoff_dir()
            handoff_dir = managed_handoff_dir
        else:
            # 호출자가 handoff 디렉터리를 명시적으로 넘긴 경우에는
            # 그 경로를 그대로 사용하고 자동 삭제도 하지 않는다.
            handoff_dir.mkdir(parents=True, exist_ok=True)

        handoff_path = handoff_dir / self._default_handoff_file_name(handoff_index, request)
        try:
            self.run_handler_a(request, handoff_path)
            return self.run_handler_b(
                handoff_path,
                only_pending=request.only_pending,
                use_counterparty_prompt=request.use_counterparty_prompt,
            )
        finally:
            if managed_handoff_dir is not None and self.settings.delete_task_payload_files:
                self._cleanup_handoff_dir(managed_handoff_dir)

    def extract_document_payloads(
        self,
        requests: list[DocumentExtractionRequest],
        *,
        handoff_dir: Path | None = None,
    ) -> list[dict[str, Any]]:
        """문서 배열을 받아 각 문서를 독립적으로 추출한다.

        사용자 요구사항대로 다건 입력이어도 내부 추출은 문서별로 완전히 분리된다.
        하나가 실패하면 호출자 쪽에서 전체 실패로 볼지, 부분 성공으로 볼지는 상위 계층이 결정한다.
        각 문서는 반드시 handler A/B 파일 handoff 경로를 한 번 거친다.
        """
        managed_handoff_dir: Path | None = None
        if handoff_dir is None:
            # 다건 실행에서는 문서마다 제각각 임시 디렉터리를 만들기보다
            # 같은 실행 단위를 하나의 handoff 디렉터리로 묶어 두는 편이 추적하기 쉽다.
            managed_handoff_dir = self._build_internal_handoff_dir()
            handoff_dir = managed_handoff_dir
        else:
            handoff_dir.mkdir(parents=True, exist_ok=True)

        try:
            # 입력 순서를 그대로 유지하는 이유는 merge 이전 단계에서
            # "어느 문서가 몇 번째로 처리됐는지"를 추적하기 쉽게 만들기 위해서다.
            return [
                self.extract_document_payload(request, handoff_dir=handoff_dir, handoff_index=index)
                for index, request in enumerate(requests, start=1)
            ]
        finally:
            if managed_handoff_dir is not None and self.settings.delete_task_payload_files:
                self._cleanup_handoff_dir(managed_handoff_dir)

    def extract_merged_payload(
        self,
        requests: list[DocumentExtractionRequest],
        *,
        handoff_dir: Path | None = None,
    ) -> dict[str, Any]:
        """여러 문서를 개별 추출한 뒤 merge 규칙까지 적용한 최종 payload를 반환한다.

        여기서 중요한 점은 "문서를 한 번에 합쳐서 추출"하지 않는다는 점이다.
        각 문서는 먼저 독립적으로 검증된 payload를 만든 뒤, 마지막 단계에서만
        병합 규칙을 적용한다. 이렇게 해야 문서별 실패 원인 추적이 쉽고,
        일부 문서가 이상할 때 어느 입력이 문제인지 분리해서 볼 수 있다.
        """
        document_payloads = self.extract_document_payloads(requests, handoff_dir=handoff_dir)
        return build_merged_payload(document_payloads)

    def payload_to_csv_rows(self, payload: dict[str, Any]) -> list[dict[str, str]]:
        """payload를 CSV 행 목록으로 변환한다.

        호출자는 단건/다건 payload 구조 차이를 몰라도 된다.
        이 메서드가 내부에서 두 경우를 흡수해 항상 "주문 1건 = CSV 1행" 형태로 돌려준다.
        """
        # module-level helper에 위임하되, 외부 호출자는 component 하나만 알면 되게 facade를 유지한다.
        return payload_to_csv_rows(payload)

    def write_payload_files(
        self,
        *,
        payload: dict[str, Any],
        json_path: Path,
        csv_path: Path,
    ) -> None:
        """payload를 JSON/CSV 파일로 저장한다.

        WAS에서는 파일 저장 대신 DB/HTTP 응답으로 바로 넘길 수도 있지만,
        배치나 검수 스크립트에서는 여전히 파일 저장이 편하다.
        그래서 저장 기능도 이 컴포넌트에 같이 두어 CLI와 같은 규칙을 재사용한다.
        """
        # JSON과 CSV는 둘 다 검수/대외 연계에 쓰이므로 같은 payload에서 함께 만든다.
        # 한쪽만 저장하면 같은 실행의 결과를 서로 다른 타이밍에 비교하게 되어 혼란이 생길 수 있다.
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        write_orders_csv(csv_path, payload)


def flatten_document_payload_orders(payload: dict[str, Any]) -> list[dict[str, str]]:
    """문서 1건 payload를 CSV/merge용 flat row 목록으로 바꾼다.

    JSON은 구조적으로 문서 메타데이터와 주문 목록이 분리되어 있지만,
    CSV는 사람이 엑셀에서 바로 검수하기 쉽도록 둘을 한 줄에 같이 실어 보낸다.
    """
    issues = "|".join(payload.get("issues", []))
    rows: list[dict[str, str]] = []
    for order in payload.get("orders", []):
        rows.append(
            {
                "file_name": payload.get("file_name", ""),
                "source_path": payload.get("source_path", ""),
                "model_name": payload.get("model_name", ""),
                "document_base_date": payload.get("base_date", ""),
                "issues": issues,
                "fund_code": order.get("fund_code", ""),
                "fund_name": order.get("fund_name", ""),
                "settle_class": order.get("settle_class", ""),
                "order_type": order.get("order_type", ""),
                "base_date": order.get("base_date", ""),
                "t_day": order.get("t_day", ""),
                "transfer_amount": order.get("transfer_amount", ""),
            }
        )
    return rows

def _order_identity(order: dict[str, Any]) -> tuple[str, str, str, str, str, str, str]:
    """완전히 동일한 주문인지 비교하기 위한 identity key를 만든다.

    이 key가 같으면 문서가 달라도 "동일 주문의 중복 표기"로 보고 하나만 남긴다.
    현재 dedupe 기준은 아래 7개 필드를 모두 같이 본다.

    - fund_code
    - fund_name
    - settle_class
    - order_type
    - base_date
    - t_day
    - transfer_amount
    """
    return serialized_order_payload_identity(order)


def _is_preferred_order_representation(candidate: dict[str, Any], current: dict[str, Any]) -> bool:
    """완전히 같은 주문이 중복될 때 어느 표현을 남길지 결정한다.

    현재 정책은 "펀드명이 더 풍부한 표현"을 남기는 것이다.
    예를 들어 축약명과 완전한 이름이 동시에 있으면, 사람이 검수하기 쉬운 완전한 이름을 남긴다.
    """
    candidate_name = str(candidate.get("fund_name", "")).strip()
    current_name = str(current.get("fund_name", "")).strip()
    if len(candidate_name) != len(current_name):
        return len(candidate_name) > len(current_name)
    return candidate_name > current_name


def merge_document_payload_orders(document_payloads: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """여러 문서의 orders를 합쳐 merged orders와 CSV row를 동시에 만든다.

    규칙:
    1. 완전히 동일한 주문은 1건만 남긴다.
    2. 같은 펀드/같은 버킷이어도 SUB/RED는 서로 합치지 않고 그대로 유지한다.

    이 규칙을 CLI 내부에만 두지 않고 별도 컴포넌트 함수로 분리해 두면,
    WAS에서도 같은 merge 계약을 그대로 재사용할 수 있다.
    """
    grouped: dict[tuple[str, str, str, str, str, str, str], dict[str, object]] = {}

    for document_payload in document_payloads:
        # merge 단계에서도 문서 메타데이터를 같이 잡아 두는 이유는
        # 최종 CSV가 "어느 문서에서 온 주문인지"를 역추적 가능해야 하기 때문이다.
        source_file_name = str(document_payload.get("file_name", ""))
        source_path = str(document_payload.get("source_path", ""))
        model_name = str(document_payload.get("model_name", ""))
        document_base_date = str(document_payload.get("base_date", ""))
        issues = "|".join(document_payload.get("issues", []))

        for order in document_payload.get("orders", []):
            normalized_orders = dedupe_serialized_order_payloads([dict(order)])
            if not normalized_orders:
                continue
            order_dict = normalized_orders[0]
            identity = _order_identity(order_dict)
            group = grouped.setdefault(identity, {"entries": [], "seen": {}})
            seen: dict[tuple[str, str, str, str, str, str], int] = group["seen"]  # type: ignore[assignment]
            if identity in seen:
                entry_index = seen[identity]
                existing_entry = group["entries"][entry_index]  # type: ignore[index]
                if _is_preferred_order_representation(order_dict, existing_entry["order"]):
                    existing_entry["order"] = order_dict
                continue

            seen[identity] = len(group["entries"])
            group["entries"].append(  # type: ignore[operator]
                {
                    "order": order_dict,
                    "file_name": source_file_name,
                    "source_path": source_path,
                    "model_name": model_name,
                    "document_base_date": document_base_date,
                    "issues": issues,
                }
            )

    merged_orders: list[dict[str, Any]] = []
    merged_rows: list[dict[str, str]] = []

    for group in grouped.values():
        # 그룹 키 자체가 완전 동일 주문(identity)이므로,
        # 여기서는 "대표 표현 하나"만 남기고 금액을 다시 계산하지 않는다.
        # 즉 문서 간에 SUB/RED가 동시에 있어도 서로 순액으로 합치지 않는다.
        entries: list[dict[str, object]] = group["entries"]  # type: ignore[assignment]
        entry = entries[0]
        order_dict = dict(entry["order"])
        merged_orders.append(order_dict)
        merged_rows.append(
            {
                "file_name": str(entry["file_name"]),
                "source_path": str(entry["source_path"]),
                "model_name": str(entry["model_name"]),
                "document_base_date": str(entry["document_base_date"]),
                "issues": str(entry["issues"]),
                "fund_code": str(order_dict.get("fund_code", "")),
                "fund_name": str(order_dict.get("fund_name", "")),
                "settle_class": str(order_dict.get("settle_class", "")),
                "order_type": str(order_dict.get("order_type", "")),
                "base_date": str(order_dict.get("base_date", "")),
                "t_day": str(order_dict.get("t_day", "")),
                "transfer_amount": str(order_dict.get("transfer_amount", "")),
            }
        )

    return merged_orders, merged_rows


def build_merged_payload(document_payloads: list[dict[str, Any]]) -> dict[str, Any]:
    """여러 문서 payload를 하나의 merged JSON 계약으로 합친다.

    top-level에는 병합 결과와 문서 목록을 같이 둔다.
    즉 소비자는
    - `documents`로 문서별 상세 결과를 보고
    - `orders`로 전체 병합 결과를 바로 사용할 수 있다.
    """
    merged_issues: list[str] = []
    seen_issues: set[str] = set()
    merged_orders, _ = merge_document_payload_orders(document_payloads)
    model_names = {payload.get("model_name", "") for payload in document_payloads if payload.get("model_name")}

    for payload in document_payloads:
        for issue in payload.get("issues", []):
            if issue not in seen_issues:
                seen_issues.add(issue)
                merged_issues.append(issue)

    merged_status, merged_reason = ExtractionComponent._classify_merged_status(
        document_payloads=document_payloads,
        merged_orders=merged_orders,
    )

    return {
        "file_count": len(document_payloads),
        "file_names": [payload.get("file_name", "") for payload in document_payloads],
        "source_paths": [payload.get("source_path", "") for payload in document_payloads],
        "model_name": next(iter(model_names), "") if len(model_names) == 1 else "",
        "status": merged_status,
        "reason": merged_reason,
        "issues": merged_issues,
        "documents": document_payloads,
        "orders": merged_orders,
    }


def payload_to_csv_rows(payload: dict[str, Any]) -> list[dict[str, str]]:
    """single/merged payload를 모두 CSV 행 목록으로 정규화한다.

    JSON은 단건/다건 구조가 조금 다르지만, CSV는 항상 같은 열 계약을 유지해야 한다.
    이 함수가 그 차이를 흡수한다.
    """
    if "documents" in payload:
        _, rows = merge_document_payload_orders(list(payload.get("documents", [])))
        return rows
    return flatten_document_payload_orders(payload)


def write_orders_csv(csv_path: Path, payload: dict[str, Any]) -> None:
    """JSON payload를 CSV로 저장한다.

    이 함수를 component 모듈에 둔 이유는, CLI와 WAS 배치/아카이빙 코드가
    같은 CSV 평탄화 규칙을 공유하게 하기 위해서다.
    """
    rows = payload_to_csv_rows(payload)
    with csv_path.open("w", encoding="utf-8-sig", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
