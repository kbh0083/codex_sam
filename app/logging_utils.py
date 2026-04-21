from __future__ import annotations

import logging


def setup_logging(level: int = logging.INFO) -> None:
    """프로젝트 전체에서 공통으로 쓰는 기본 로깅 포맷을 설정한다.

    이미 핸들러가 붙어 있다면 새로 `basicConfig()`를 호출하지 않고
    레벨만 조정한다. 이렇게 하는 이유는 테스트나 외부 런처가
    자체 로깅 핸들러를 먼저 구성한 경우 그 설정을 망가뜨리지 않기 위해서다.
    """
    root_logger = logging.getLogger()
    if root_logger.handlers:
        root_logger.setLevel(level)
        return

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
