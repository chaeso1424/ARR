# utils/logging.py
from __future__ import annotations
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, Optional
import os
from contextvars import ContextVar
from contextlib import contextmanager

# ─────────────────────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parents[1]

LOGS_DIR = Path(os.getenv("LOGS_DIR", str(BASE_DIR / "logs")))
DEFAULT_LOG_BASENAME = os.getenv("DEFAULT_LOG_BASENAME", "logs.txt")
BOT_LOG_PATTERN = os.getenv("BOT_LOG_PATTERN", "{bot_id}.log")

MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", str(5 * 1024 * 1024)))
BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "5"))

# logs.txt에도 모든 로그를 남기고 싶다면 1 (기본 1: 복제 기록)
MIRROR_TO_APP = os.getenv("MIRROR_TO_APP", "1") in ("1","true","TRUE","True")

LOGS_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────────────────────
# 컨텍스트: 현재 스레드/태스크의 bot_id
# ─────────────────────────────────────────────────────────────
_current_bot_id: ContextVar[Optional[str]] = ContextVar("current_bot_id", default=None)

def set_bot_context(bot_id: Optional[str]) -> None:
    _current_bot_id.set(bot_id)

def clear_bot_context() -> None:
    _current_bot_id.set(None)

@contextmanager
def bot_context(bot_id: Optional[str]):
    token = _current_bot_id.set(bot_id)
    try:
        yield
    finally:
        _current_bot_id.reset(token)

def resolve_log_path(bot_id: str | None = None) -> Path:
    if bot_id:
        return LOGS_DIR / BOT_LOG_PATTERN.format(bot_id=bot_id)
    return BASE_DIR / DEFAULT_LOG_BASENAME

def _make_file_handler(path: Path) -> RotatingFileHandler:
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(
        filename=str(path),
        maxBytes=MAX_BYTES,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
        delay=True,
    )
    return handler

# ─────────────────────────────────────────────────────────────
# 필터들
# ─────────────────────────────────────────────────────────────
class _EnsureFields(logging.Filter):
    """record에 bot_suffix, bot_id 기본값 주입."""
    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "bot_id"):
            record.bot_id = None
        if not hasattr(record, "bot_suffix"):
            record.bot_suffix = ""
        return True

class _InjectBotFromContext(logging.Filter):
    """record.bot_id가 비어있으면 contextvars에서 채운다."""
    def filter(self, record: logging.LogRecord) -> bool:
        if getattr(record, "bot_id", None) is None:
            c = _current_bot_id.get()
            record.bot_id = c
        # 보기 좋게 suffix도 동기화
        if getattr(record, "bot_id", None):
            record.bot_suffix = f"  (bot={record.bot_id})"
        return True

class _AllowOnlyBot(logging.Filter):
    """봇 핸들러: 해당 bot_id만 통과."""
    def __init__(self, bot_id: str) -> None:
        super().__init__()
        self._bot_id = bot_id
    def filter(self, record: logging.LogRecord) -> bool:
        return getattr(record, "bot_id", None) == self._bot_id

class _AllowOnlyApp(logging.Filter):
    """루트 핸들러: bot_id=None 만 통과. (미러링 켜면 모두 통과)"""
    def filter(self, record: logging.LogRecord) -> bool:
        if MIRROR_TO_APP:
            return True
        return getattr(record, "bot_id", None) is None

# ─────────────────────────────────────────────────────────────
# 로거 구성
# ─────────────────────────────────────────────────────────────
_bot_file_handlers: Dict[str, RotatingFileHandler] = {}
_root_logger: Optional[logging.Logger] = None

def _ensure_root_logger() -> logging.Logger:
    global _root_logger
    if _root_logger:
        return _root_logger

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.propagate = False

    # 기본 파일 핸들러 (logs.txt)
    base_handler = _make_file_handler(resolve_log_path(None))
    base_handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s%(bot_suffix)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    # 필수 필터: 필드 보강 + 컨텍스트 주입 + 루트용 라우팅
    base_handler.addFilter(_EnsureFields())
    base_handler.addFilter(_InjectBotFromContext())
    base_handler.addFilter(_AllowOnlyApp())
    logger.addHandler(base_handler)

    _root_logger = logger
    return logger

def _get_or_create_bot_handler(bot_id: str) -> RotatingFileHandler:
    if bot_id in _bot_file_handlers:
        return _bot_file_handlers[bot_id]
    path = resolve_log_path(bot_id)
    handler = _make_file_handler(path)
    handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    handler.addFilter(_EnsureFields())
    handler.addFilter(_InjectBotFromContext())
    handler.addFilter(_AllowOnlyBot(bot_id))
    _bot_file_handlers[bot_id] = handler
    return handler

class BotLoggerAdapter(logging.LoggerAdapter):
    """extra={'bot_id': ...} → record에 bot_id 심고 suffix도 추가."""
    def process(self, msg, kwargs):
        bot_id = self.extra.get("bot_id")
        kwargs.setdefault("extra", {})
        kwargs["extra"]["bot_id"] = bot_id
        kwargs["extra"]["bot_suffix"] = f"  (bot={bot_id})" if bot_id else ""
        return msg, kwargs

def get_logger(bot_id: str | None = None) -> logging.LoggerAdapter:
    """
    - bot_id=None: 루트 핸들러(logs.txt) 기록
    - bot_id=... : 해당 봇 핸들러(logs/<bot>.log) + 옵션에 따라 logs.txt에도 복제
    """
    root = _ensure_root_logger()
    if bot_id:
        h = _get_or_create_bot_handler(bot_id)
        if h not in root.handlers:
            root.addHandler(h)
    return BotLoggerAdapter(root, {"bot_id": bot_id})

# 레거시 편의 함수
def log(msg: str, bot_id: str | None = None, level: int = logging.INFO):
    logger = get_logger(bot_id)
    logger.log(level, msg)
