from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

from app.component import (
    CSV_FIELDNAMES,
    DocumentExtractionRequest,
    ExtractionComponent,
    build_merged_payload,
    flatten_document_payload_orders,
    merge_document_payload_orders,
    payload_to_csv_rows,
    write_orders_csv,
)
from app.logging_utils import setup_logging

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """현재 서비스가 외부에 노출하는 최소 CLI 계약을 정의한다.

    CLI는 단순해 보여도 실제로는 WAS와 같은 추출 경로를 검증하는 용도로도 쓰인다.
    그래서 옵션은 많지 않게 유지하되,
    - 입력 문서 목록
    - 암호화 PDF 비밀번호
    - 최종 산출물 저장 위치
    세 가지는 명시적으로 받는다.
    """
    parser = argparse.ArgumentParser(
        description="Extract fund orders from local documents and save the result as JSON and CSV.",
    )
    parser.add_argument(
        "-f",
        "--file",
        required=True,
        nargs="+",
        help="Path or file name of a PDF/HTML/EML/MHT/XLS/XLSX/XLSM document.",
    )
    parser.add_argument(
        "-p",
        "--pdf-password",
        default=None,
        help="Password for an encrypted PDF.",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="Base path to save the extracted files. Timestamped .json/.csv files will be created.",
    )
    parser.add_argument(
        "--only-pending",
        action="store_true",
        # 이름은 only_pending이지만, 현재 업무 규칙은
        # "기존 PENDING 주문 제거 -> 남은 주문의 settle_class를 내부적으로 PENDING 으로 변경
        # -> 최종 출력에서는 settle_class 코드 '1'로 직렬화"이다.
        # CLI도 이 계약을 그대로 외부에 노출한다.
        help=(
            "Drop orders whose settle_class is already PENDING, then rewrite the remaining "
            "orders to the pending output code."
        ),
    )
    parser.add_argument(
        "--use-counterparty-prompt",
        action="store_true",
        help=(
            "Inject a file-based counterparty-specific guidance block into the stage prompt "
            "when a matching prompt file exists for the input document."
        ),
    )
    return parser


def resolve_output_base(file_paths: list[Path], output_arg: str | None) -> Path:
    """타임스탬프가 붙기 전의 출력 base path를 계산한다.

    다건 실행일 때 기본 파일명을 `merged_orders`로 고정하는 이유는,
    handler A/B handoff 파일과 최종 산출물 파일의 역할을 구분해서 보기 쉽게 만들기 위해서다.
    """
    if output_arg:
        candidate = Path(output_arg).expanduser()
        return candidate.with_suffix("") if candidate.suffix else candidate
    if len(file_paths) == 1:
        # 단건은 원본 파일명을 기본으로 쓰면 결과 파일을 찾기 쉽다.
        return Path.cwd() / "output" / file_paths[0].stem
    # 다건은 문서명 하나로 대표하기 어렵기 때문에 merged_orders를 기본 이름으로 사용한다.
    return Path.cwd() / "output" / "merged_orders"


def build_output_paths(file_paths: list[Path], output_arg: str | None) -> tuple[Path, Path]:
    """한 번의 CLI 실행에 사용할 JSON/CSV 경로를 만든다.

    최종 결과 파일에는 타임스탬프를 붙여서,
    같은 문서를 반복 검수하거나 추출 로직을 비교할 때 이전 결과를 덮어쓰지 않게 한다.
    """
    base_path = resolve_output_base(file_paths, output_arg)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = base_path.parent / f"{base_path.name}_{timestamp}.json"
    csv_path = base_path.parent / f"{base_path.name}_{timestamp}.csv"
    return json_path, csv_path


def main() -> int:
    """로컬 추출/검수용 CLI 진입점.

    실제 실행 경로도 WAS에서 권장하는 handler A -> handler B 흐름과 동일하게 맞춘다.
    즉 CLI라도 내부적으로는

    - handler A: `DocumentLoader.build_task_payload()` -> JSON 파일 저장
    - handler B: 저장된 JSON 재로딩 -> `FundOrderExtractor.extract_from_task_payload()`

    순서를 그대로 타게 해서, 로컬 검수와 WAS 운영 경로가 최대한 어긋나지 않도록 한다.
    """
    setup_logging()
    args = build_parser().parse_args()
    # CLI 인자는 문자열로 들어오므로 여기서 한 번 Path로 정규화해 둔다.
    # 이후 단계는 Path만 다루게 하면 경로 처리 코드가 단순해진다.
    file_paths = [Path(value).expanduser() for value in args.file]
    logger.info("[1/7] Input file(s) resolved: count=%s paths=%s", len(file_paths), file_paths)

    component = ExtractionComponent()
    json_path, csv_path = build_output_paths(file_paths, args.output)
    # 최종 산출물은 실행 성공 시점에 항상 저장 가능해야 하므로 부모 디렉터리를 먼저 보장한다.
    json_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("[2/7] Output paths prepared: json=%s csv=%s", json_path, csv_path)

    try:
        logger.info("[3/7] Starting extraction pipeline")
        # CLI는 외부 사용자가 가장 많이 만나는 경계이므로,
        # 입력값을 request DTO로 감싸 component와 같은 계약으로 전달한다.
        requests = [
            DocumentExtractionRequest(
                source_path=file_path,
                pdf_password=args.pdf_password,
                only_pending=args.only_pending,
                use_counterparty_prompt=args.use_counterparty_prompt,
            )
            for file_path in file_paths
        ]
        logger.info(
            "[4/7] Handler handoff configured: dir=%s delete_after_extract=%s",
            component.settings.task_payload_output_dir,
            component.settings.delete_task_payload_files,
        )
        # CLI도 queue/WAS와 같은 handler A -> handler B 경로를 실제로 검증한다.
        # handoff 디렉터리를 명시적으로 넘기지 않으면 component가 env 설정을 기준으로
        # 기본 저장 경로와 삭제 정책을 적용한다.
        document_payloads = component.extract_document_payloads(requests)
        # 단건은 문서 1건 payload를 그대로 쓰고,
        # 다건은 개별 문서 payload를 마지막에만 병합한다.
        payload = document_payloads[0] if len(document_payloads) == 1 else build_merged_payload(document_payloads)
    except ValueError as exc:
        logger.exception("Extraction failed with validation error")
        print(str(exc), file=sys.stderr)
        return 1
    except Exception as exc:  # pragma: no cover
        logger.exception("Extraction failed with unexpected error")
        print(f"Unexpected error: {exc}", file=sys.stderr)
        return 1

    logger.info(
        "[5/7] Extraction completed: orders=%s issues=%s",
        len(payload.get("orders", [])),
        payload.get("issues", []),
    )
    # JSON을 먼저 저장하고 CSV를 이어서 저장한다.
    # 사람이 눈으로 확인할 때는 JSON이 기준 원본이므로, 저장 순서를 고정해 두는 편이 추적하기 쉽다.
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("[6/7] JSON saved")
    write_orders_csv(csv_path, payload)
    logger.info("[7/7] CSV saved")
    print(f"Saved JSON to {json_path}", file=sys.stderr)
    print(f"Saved CSV to {csv_path}", file=sys.stderr)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
