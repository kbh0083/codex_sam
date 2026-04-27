from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from app.config import get_settings
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
)
from app.schemas import ExtractionResult

logger = logging.getLogger(__name__)

STATUS_COMPLETED = "COMPLETED"
STATUS_SKIPPED = "SKIPPED"
STATUS_FAILED = "FAILED"


class NonInstructionDocumentError(ValueError):
    """처리 대상 문서가 지시서가 아닐 때 발생시키는 예외.

    일반적인 추출 실패와 구분해 두면 WAS/배치 쪽에서
    - 입력 파일이 잘못 섞였는지
    - 추출기가 문서를 못 읽은 것인지
    를 다르게 처리할 수 있다.

    `ValueError`를 상속하는 이유는 기존 CLI/WAS 샘플이 이미 ValueError를 400 계열로
    매핑하고 있기 때문이다. 즉 새 예외를 추가해도 외부 계약을 깨지 않는다.
    """


class ExtractionService:
    """웹 프레임워크와 무관한 추출 오케스트레이터.

    이 클래스는 "문서를 읽고 -> LLM 추출을 수행하고 -> 검증 후 반환"까지만 책임진다.
    FastAPI, DB 저장, 업로드 API 같은 외부 입출력 관심사는 의도적으로 배제했다.
    그래서 현재는 CLI에서 쓰고, 나중에는 다른 개발 서버나 WAS에서 그대로 import 해서
    재사용할 수 있다.
    """

    def __init__(self) -> None:
        """서비스 실행에 필요한 설정/로더/추출기를 준비한다.

        이 초기화가 끝나면 이후 메서드는 "환경설정이 이미 유효하다"는 가정 아래
        순수한 추출 흐름만 다루면 된다.

        현재 구조에서 이 클래스는 "중심 엔진"이라기보다
        - 기존 단건 호출 코드와의 호환을 유지하고
        - 내부적으로는 최신 handler handoff 경로를 그대로 재사용하는
        얇은 오케스트레이터에 가깝다.
        """
        # 설정 객체는 extractor/model/debug 경로 같은 런타임 정책을 들고 있다.
        # loader는 문서 해석 전담, extractor는 LLM 추출 전담이므로
        # service는 두 객체를 연결만 하도록 최소 책임으로 유지한다.
        self.settings = get_settings()
        self.loader = DocumentLoader()
        self.extractor = FundOrderExtractor(self.settings)
        # invalid response artifact, 운영 디버그 파일은 항상 쓸 수 있어야 하므로
        # 서비스 시작 시점에 디렉터리를 만들어 둔다.
        self.settings.debug_output_dir.mkdir(parents=True, exist_ok=True)
        # service를 직접 쓰는 경로도 DocumentLoader 결과를 파일로 저장했다가 다시 읽는다.
        # 따라서 handoff 결과물을 남길 기본 디렉터리도 함께 보장해 둔다.
        self.settings.task_payload_output_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _non_instruction_error_message(task_payload: DocumentLoadTaskPayload, reason: str) -> str:
        return (
            "Document is not a variable-annuity order instruction. "
            f"path={task_payload.source_path} reason={reason}"
        )

    def extract_file_path(
        self,
        file_path: Path,
        pdf_password: str | None = None,
        only_pending: bool = False,
        use_counterparty_prompt: bool = False,
    ) -> ExtractionResult:
        """문서 1건을 읽어 검증까지 끝난 추출 결과를 반환한다.

        여기서 중요한 점은 "LLM 응답이 왔다"가 아니라 "저장 가능한 결과인가"까지
        확인한다는 것이다. coverage 검증과 blocking issue 검사를 통과하지 못하면
        예외를 발생시켜 파일 저장/후속 처리로 넘어가지 않는다.

        `only_pending`은 저장 가능성 판단에는 영향을 주지 않고,
        검증을 통과한 최종 결과 표현만 바꾼다.
        """
        extraction_result, _, normalized_path = self._extract_file_path_internal(
            file_path,
            pdf_password=pdf_password,
            only_pending=only_pending,
            use_counterparty_prompt=use_counterparty_prompt,
        )
        logger.info(
            "Extraction completed for %s: orders=%s issues=%s",
            normalized_path,
            len(extraction_result.orders),
            extraction_result.issues,
        )
        return extraction_result

    def extract_file_path_to_payload(
        self,
        file_path: Path,
        pdf_password: str | None = None,
        only_pending: bool = False,
        use_counterparty_prompt: bool = False,
    ) -> dict[str, Any]:
        """검증 완료된 결과를 JSON 직렬화 가능한 dict 로 변환한다.

        CLI는 이 payload 를 그대로 JSON 파일에 저장하고, CSV는 여기서 나온 orders 를
        평탄화해서 만든다. 즉 이 함수의 출력이 현재 서비스의 외부 계약(contract)이다.
        `only_pending`도 이 payload 계약에 그대로 반영된다.
        """
        normalized_path = Path(file_path).expanduser()
        logger.info("Preparing payload extraction for local file: %s", normalized_path)
        try:
            extraction_result, task_payload, normalized_path = self._extract_file_path_internal(
                normalized_path,
                pdf_password=pdf_password,
                only_pending=only_pending,
                use_counterparty_prompt=use_counterparty_prompt,
            )
        except NonInstructionDocumentError as exc:
            return self._build_status_payload(
                source_path=normalized_path,
                status=STATUS_SKIPPED,
                reason=str(exc),
            )
        except ExtractionOutcomeError as exc:
            return self._build_status_payload(
                source_path=normalized_path,
                status=STATUS_FAILED,
                reason=str(exc),
                issues=exc.result.issues,
            )
        except ValueError as exc:
            return self._build_status_payload(
                source_path=normalized_path,
                status=STATUS_FAILED,
                reason=str(exc),
                issues=[str(exc)],
            )
        except Exception as exc:
            return self._build_status_payload(
                source_path=normalized_path,
                status=STATUS_FAILED,
                reason=str(exc),
                issues=[str(exc)],
            )

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
        status = STATUS_COMPLETED if order_payloads else STATUS_SKIPPED
        if order_payloads:
            reason = None
        elif task_payload.allow_empty_result:
            reason = "거래가 없는 문서"
        elif task_payload.scope_excludes_all_funds:
            reason = "대상 거래처 scope에 해당하는 주문 없음"
        else:
            reason = "추출된 주문 데이터 없음"
        logger.info(
            "Payload extraction completed: orders=%s base_date=%s issues=%s status=%s reason=%s",
            len(order_payloads),
            base_date,
            extraction_result.issues,
            status,
            reason,
        )
        return {
            "file_name": normalized_path.name,
            "source_path": str(normalized_path),
            "model_name": self.settings.llm_model,
            "base_date": base_date,
            "status": status,
            "reason": reason,
            "issues": extraction_result.issues,
            "orders": order_payloads,
        }

    def _build_status_payload(
        self,
        *,
        source_path: Path,
        status: str,
        reason: str,
        issues: list[str] | None = None,
    ) -> dict[str, Any]:
        """service 단건 경로도 외부 payload 계약에서 상태/사유를 명시한다."""
        return {
            "file_name": source_path.name,
            "source_path": str(source_path),
            "model_name": self.settings.llm_model,
            "base_date": None,
            "status": status,
            "reason": reason,
            "issues": list(issues or []),
            "orders": [],
        }

    def _extract_file_path_internal(
        self,
        file_path: Path,
        *,
        pdf_password: str | None = None,
        only_pending: bool = False,
        use_counterparty_prompt: bool = False,
    ) -> tuple[ExtractionResult, DocumentLoadTaskPayload, Path]:
        """추출 결과와 payload 문맥을 함께 반환한다."""
        task_payload, normalized_path = self._build_task_payload(file_path, pdf_password=pdf_password)
        counterparty_guidance = load_counterparty_guidance(
            normalized_path,
            use_counterparty_prompt=use_counterparty_prompt,
            document_text=task_payload.markdown_text,
        )
        task_payload = self._reload_task_payload_via_handoff_file(task_payload, normalized_path)
        prompt_directed_reason = detect_counterparty_guidance_non_instruction_reason(
            counterparty_guidance=counterparty_guidance,
            markdown_text=task_payload.markdown_text,
            raw_text=task_payload.raw_text,
        )
        if prompt_directed_reason:
            raise NonInstructionDocumentError(
                self._non_instruction_error_message(task_payload, prompt_directed_reason)
            )
        try:
            extraction_outcome = self.extractor.extract_from_task_payload(
                task_payload,
                counterparty_guidance=counterparty_guidance,
                extract_log_path=build_extract_llm_log_path(
                    debug_output_dir=self.settings.debug_output_dir,
                    source_name=task_payload.file_name,
                ),
            )
        except ExtractionOutcomeError as exc:
            write_invalid_response_debug_files(
                debug_output_dir=self.settings.debug_output_dir,
                source_name=task_payload.file_name,
                artifacts=exc.invalid_response_artifacts,
            )
            raise
        except ValueError as exc:
            if task_payload.non_instruction_reason or "not a variable-annuity order instruction" in str(exc).lower():
                raise NonInstructionDocumentError(str(exc)) from exc
            raise
        write_invalid_response_debug_files(
            debug_output_dir=self.settings.debug_output_dir,
            source_name=task_payload.file_name,
            artifacts=extraction_outcome.invalid_response_artifacts,
        )
        extraction_result = apply_only_pending_filter(extraction_outcome.result, only_pending=only_pending)
        return extraction_result, task_payload, normalized_path

    @staticmethod
    def _serialize_order_payload(order: Any) -> dict[str, Any]:
        """공통 출력 계약 helper를 사용해 최종 payload를 직렬화한다.

        service/component가 서로 다른 규칙으로 직렬화하면
        같은 문서를 어느 진입점으로 실행했는지에 따라 결과 계약이 달라진다.
        그래서 최종 출력 직렬화는 공통 helper 한 곳에 두고,
        service는 그 helper를 그대로 재사용한다.
        """
        return serialize_order_payload(order)

    @staticmethod
    def _format_output_transfer_amount(order_type: str | None, transfer_amount: str | None) -> str | None:
        """공통 출력 계약 helper로 위임한다."""
        return format_output_transfer_amount(order_type=order_type, transfer_amount=transfer_amount)

    @staticmethod
    def _format_output_t_day(settle_class: str | None, t_day: Any) -> str | None:
        """공통 출력 계약 helper로 위임한다."""
        return format_output_t_day(settle_class=settle_class, t_day=t_day)

    def _build_task_payload(
        self,
        file_path: Path,
        pdf_password: str | None = None,
    ) -> tuple[DocumentLoadTaskPayload, Path]:
        """경로 검증과 `DocumentLoader` 단계 산출물 생성을 한 단계로 묶는다.

        이 함수 이후부터는 항상
        - 실제 존재하는 파일 경로
        - raw/markdown/chunk/scope/coverage 가 포함된 task payload
        가 확보된 상태로 다음 단계가 진행된다.
        """
        # 경로 검증과 loader 호출을 한 메서드에 모아 두면
        # service/component/CLI 모두 같은 진입 규칙을 공유하기 쉽다.
        normalized_path = self._validate_source_path(file_path)
        task_payload = self.loader.build_task_payload(
            normalized_path,
            chunk_size_chars=self.settings.llm_chunk_size_chars,
            pdf_password=pdf_password,
        )
        return task_payload, normalized_path

    def _reload_task_payload_via_handoff_file(
        self,
        task_payload: DocumentLoadTaskPayload,
        normalized_path: Path,
    ) -> DocumentLoadTaskPayload:
        """로더 산출물을 임시 handoff 파일로 저장한 뒤 다시 읽어 온다.

        service가 직접 호출되더라도 실행 경로는 handler A/B 계약을 그대로 따른다.
        따라서 이 메서드는 service 내부의 "가상 handler handoff"라고 보면 된다.

        왜 굳이 다시 읽는가:
        - queue/WAS 운영 환경에서는 handler A 결과가 실제 파일/메시지로 전달된다.
        - 메모리 객체를 바로 넘기면 service 경로만 별도 구현이 되어 운영 경로와 어긋난다.
        - 파일 직렬화/역직렬화가 깨졌을 때도 service 경로에서 바로 드러나게 할 수 있다.
        """
        # service도 더 이상 메모리 직통 경로를 사용하지 않는다.
        # env에서 삭제 옵션을 끄면 이 handoff 파일이 그대로 남아
        # "로더 단계 결과가 실제로 무엇이었는지"를 운영 중 다시 확인할 수 있다.
        handoff_dir = self.settings.task_payload_output_dir / "service"
        handoff_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        handoff_path = handoff_dir / f"{normalized_path.stem}_{timestamp}_task_payload.json"
        task_payload.write_json(handoff_path)
        try:
            return DocumentLoadTaskPayload.read_json(handoff_path)
        finally:
            if self.settings.delete_task_payload_files:
                handoff_path.unlink(missing_ok=True)

    def _candidate_source_paths(self, file_path: Path) -> list[Path]:
        """사용자 입력 경로를 실제 파일 경로 후보들로 확장한다.

        새 요구사항에서는 CLI가 파일명만 받아도 동작해야 한다.
        그래서 "파일명만 입력"된 경우에는 ENV의 문서 루트(`DOCUMENT_INPUT_DIR`)를
        먼저 확인하고, 그 다음에 현재 작업 디렉터리 기준 후보를 본다.
        반대로 사용자가 디렉터리를 포함한 상대경로를 준 경우에는 그 의도를 존중해
        원래 경로를 먼저 확인한다.
        """
        # 사용자가 `~/...`를 넘길 수 있으므로 먼저 expanduser를 적용한다.
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

        # 같은 후보가 중복으로 들어갈 수 있어 순서를 유지한 채 dedupe 한다.
        # 같은 후보가 여러 규칙에서 중복 생성될 수 있어 순서를 유지한 채 dedupe 한다.
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

        파일명만 들어온 경우에는 `DOCUMENT_INPUT_DIR`를 포함한 후보들을 순서대로 검사한다.
        예외 메시지에 검사한 후보를 모두 남기는 이유는 운영 중 경로 문제를
        빠르게 진단하기 위해서다.
        """
        # 여기서 확정된 경로가 이후 로그/출력/디버깅의 기준이 되므로
        # "어디를 찾아봤는지"를 예외 메시지까지 남긴다.
        candidates = self._candidate_source_paths(file_path)
        for candidate in candidates:
            if candidate.exists():
                if not candidate.is_file():
                    raise ValueError(f"Not a file: {candidate}")
                return candidate

        candidate_text = ", ".join(str(candidate) for candidate in candidates)
        raise ValueError(f"File not found. checked=[{candidate_text}]")
