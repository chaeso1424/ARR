# utils/logging.py
from __future__ import annotations
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, Optional

BASE_DIR = Path(__file__).resolve().parents[1]
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_LOG = BASE_DIR / "logs.txt"

# ---- 내부 상태: 핸들러/로거 캐시 ----
_bot_file_handlers: Dict[str, RotatingFileHandler] = {}
_root_logger: Optional[logging.Logger] = None

def _make_file_handler(path: Path) -> RotatingFileHandler:
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(
        filename=str(path),
        maxBytes=5 * 1024 * 1024,  # 5MB
        backupCount=5,             # 최근 5개 보존
        encoding="utf-8",
        delay=True,                # 실제 쓰기 전까지 파일 오픈 지연
    )
    return handler

def _ensure_root_logger() -> logging.Logger:
    global _root_logger
    if _root_logger:
        return _root_logger

    logger = logging.getLogger("arr")
    logger.setLevel(logging.INFO)
    logger.propagate = False  # root 로거로 전파 방지

    # 기본 파일 핸들러 (logs.txt)
    base_handler = _make_file_handler(DEFAULT_LOG)
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s%(bot_suffix)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    base_handler.setFormatter(formatter)
    logger.addHandler(base_handler)

    _root_logger = logger
    return logger

def _get_or_create_bot_handler(bot_id: str) -> RotatingFileHandler:
    if bot_id in _bot_file_handlers:
        return _bot_file_handlers[bot_id]
    path = LOGS_DIR / f"{bot_id}.log"
    handler = _make_file_handler(path)
    # 봇별 파일에는 동일 포맷
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    _bot_file_handlers[bot_id] = handler
    return handler

class BotLoggerAdapter(logging.LoggerAdapter):
    """extra={'bot_id': ...}를 받아 메시지에 bot_id suffix를 붙임"""
    def process(self, msg, kwargs):
        bot_id = self.extra.get("bot_id")
        suffix = f"  (bot={bot_id})" if bot_id else ""
        # 포맷터에서 쓸 수 있도록 record에 임시 필드 추가
        kwargs.setdefault("extra", {})
        kwargs["extra"]["bot_suffix"] = suffix
        return msg, kwargs

def get_logger(bot_id: str | None = None) -> logging.Logger:
    """
    - bot_id가 None: 기본 로거(arr) 반환 (logs.txt에 기록)
    - bot_id가 존재: 기본 로거 + 봇별 파일 핸들러를 장착한 Adapter 반환
    """
    root = _ensure_root_logger()

    if not bot_id:
        # 기본 로거 그대로
        return root

    # 봇별 핸들러를 root logger에(한 번만) 추가
    handler = _get_or_create_bot_handler(bot_id)
    if handler not in root.handlers:
        # 같은 핸들러를 여러 번 붙이지 않게 주의
        root.addHandler(handler)

    return BotLoggerAdapter(root, {"bot_id": bot_id})

# ---- 레거시 호환 함수 ----
def log(msg: str, bot_id: str | None = None, level: int = logging.INFO):
    logger = get_logger(bot_id)
    logger.log(level, msg)
