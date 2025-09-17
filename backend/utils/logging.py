# utils/logging.py
from __future__ import annotations
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, Optional
import os

# ─────────────────────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parents[1]

LOGS_DIR = Path(os.getenv("LOGS_DIR", str(BASE_DIR / "logs")))
DEFAULT_LOG_BASENAME = os.getenv("DEFAULT_LOG_BASENAME", "logs.txt")
BOT_LOG_PATTERN = os.getenv("BOT_LOG_PATTERN", "{bot_id}.log")

MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", str(5 * 1024 * 1024)))
BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "5"))

# 봇 로그를 루트 로그(기본 파일)에도 "추가로" 남기고 싶다면 1
MIRROR_TO_APP = os.getenv("MIRROR_TO_APP", "0") in ("1", "true", "TRUE", "True")

LOGS_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────────────────────
# 내부 상태
# ─────────────────────────────────────────────────────────────
_bot_file_handlers: Dict[str, RotatingFileHandler] = {}
_root_logger: Optional[logging.Logger] = None


def resolve_log_path(bot_id: str | None = None) -> Path:
    if bot_id:
        fname = BOT_LOG_PATTERN.format(bot_id=bot_id)
        return LOGS_DIR / fname
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
# 필터: 핸들러 라우팅
# ─────────────────────────────────────────────────────────────
class _EnsureFields(logging.Filter):
    """record에 bot_suffix, bot_id 기본값 주입."""
    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "bot_suffix"):
            record.bot_suffix = ""
        if not hasattr(record, "bot_id"):
            record.bot_id = None
        return True


class _AllowOnlyApp(logging.Filter):
    """기본 핸들러: bot_id가 None 인 레코드만 통과 (MIRROR_TO_APP이면 모두 허용)."""
    def filter(self, record: logging.LogRecord) -> bool:
        if MIRROR_TO_APP:
            return True
        return getattr(record, "bot_id", None) is None


class _AllowOnlyBot(logging.Filter):
    """봇 핸들러: 특정 bot_id만 통과."""
    def __init__(self, bot_id: str) -> None:
        super().__init__()
        self._bot_id = bot_id
    def filter(self, record: logging.LogRecord) -> bool:
        return getattr(record, "bot_id", None) == self._bot_id


# ─────────────────────────────────────────────────────────────
# 로거 구성
# ─────────────────────────────────────────────────────────────
def _ensure_root_logger() -> logging.Logger:
    global _root_logger
    if _root_logger:
        return _root_logger

    logger = logging.getLogger("arr")
    logger.setLevel(logging.INFO)
    logger.propagate = False  # 루트로 전파 방지

    # 기본 파일 핸들러 (레거시 경로/파일명 유지)
    base_handler = _make_file_handler(resolve_log_path(None))
    base_handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s%(bot_suffix)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    base_handler.addFilter(_EnsureFields())
    base_handler.addFilter(_AllowOnlyApp())  # ★ bot_id=None 만 통과(미러링 끄면)

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
    handler.addFilter(_AllowOnlyBot(bot_id))  # ★ 해당 bot_id만 통과

    _bot_file_handlers[bot_id] = handler
    return handler


class BotLoggerAdapter(logging.LoggerAdapter):
    """extra={'bot_id': ...} → 메시지에 bot_id suffix도 추가, record에도 심음."""
    def process(self, msg, kwargs):
        bot_id = self.extra.get("bot_id")
        suffix = f"  (bot={bot_id})" if bot_id else ""
        kwargs.setdefault("extra", {})
        # 핸들러 라우팅을 위해 record에 bot_id를 심는다
        kwargs["extra"]["bot_id"] = bot_id
        # 보기 좋은 suffix도 추가
        kwargs["extra"]["bot_suffix"] = suffix
        return msg, kwargs


def get_logger(bot_id: str | None = None) -> logging.LoggerAdapter:
    """
    - bot_id=None: 루트 로그로만 기록 (BASE_DIR/DEFAULT_LOG_BASENAME)
    - bot_id 있음: 해당 봇 파일(LOGS_DIR/BOT_LOG_PATTERN)로만 기록
                   (MIRROR_TO_APP=1이면 루트에도 복제 기록)
    """
    root = _ensure_root_logger()

    if bot_id:
        handler = _get_or_create_bot_handler(bot_id)
        # 같은 핸들러를 여러 번 붙이지 않도록 체크
        if handler not in root.handlers:
            root.addHandler(handler)

    return BotLoggerAdapter(root, {"bot_id": bot_id})


# ─────────────────────────────────────────────────────────────
# 레거시 호환 함수
# ─────────────────────────────────────────────────────────────
def log(msg: str, bot_id: str | None = None, level: int = logging.INFO):
    logger = get_logger(bot_id)
    logger.log(level, msg)
