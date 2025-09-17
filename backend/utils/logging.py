# utils/logging.py
from __future__ import annotations
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, Optional
import os

# ─────────────────────────────────────────────────────────────
# 설정 (환경변수로 프론트 요구 파일명/경로를 제어 가능)
# ─────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parents[1]

# logs 디렉토리 (기본: ./logs)
LOGS_DIR = Path(os.getenv("LOGS_DIR", str(BASE_DIR / "logs")))

# 루트 로그 파일명 (기본: logs.txt) — 기존 코드와 동일 기본값
DEFAULT_LOG_BASENAME = os.getenv("DEFAULT_LOG_BASENAME", "logs.txt")

# 봇별 로그 파일명 패턴 (기본: {bot_id}.log) — 기존 코드와 동일 기본값
# 예시로 프론트가 'bot_<id>.log'를 원하면 BOT_LOG_PATTERN="bot_{bot_id}.log"
BOT_LOG_PATTERN = os.getenv("BOT_LOG_PATTERN", "{bot_id}.log")

# 로테이팅 설정
MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", str(5 * 1024 * 1024)))  # 5MB
BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "5"))

LOGS_DIR.mkdir(parents=True, exist_ok=True)

# ---- 내부 상태: 핸들러/로거 캐시 ----
_bot_file_handlers: Dict[str, RotatingFileHandler] = {}
_root_logger: Optional[logging.Logger] = None


def _make_file_handler(path: Path) -> RotatingFileHandler:
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(
        filename=str(path),
        maxBytes=MAX_BYTES,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
        delay=True,  # 실제 쓰기 전까지 파일 오픈 지연
    )
    return handler


class _EnsureBotSuffix(logging.Filter):
    """record.bot_suffix 가 없으면 기본값 ''를 주입해서 포맷 에러를 방지."""
    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "bot_suffix"):
            record.bot_suffix = ""
        return True


def resolve_log_path(bot_id: str | None = None) -> Path:
    """프론트엔드가 파일 경로를 계산할 때도 동일 규칙을 쓰도록 공개 함수 제공."""
    if bot_id:
        fname = BOT_LOG_PATTERN.format(bot_id=bot_id)
        return LOGS_DIR / fname
    # 루트 로그는 기본적으로 BASE_DIR 옆의 파일명을 유지(레거시 호환)
    # 필요 시 DEFAULT_LOG_BASENAME을 'app.log' 등으로 바꾸면 됨
    return BASE_DIR / DEFAULT_LOG_BASENAME


def _ensure_root_logger() -> logging.Logger:
    global _root_logger
    if _root_logger:
        return _root_logger

    logger = logging.getLogger("arr")
    logger.setLevel(logging.INFO)
    logger.propagate = False  # root 로거로 전파 방지

    # 기본 파일 핸들러 (레거시: BASE_DIR/logs.txt)
    base_handler = _make_file_handler(resolve_log_path(None))
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s%(bot_suffix)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    base_handler.setFormatter(formatter)
    base_handler.addFilter(_EnsureBotSuffix())   # ★ 필수: bot_suffix 기본값 주입
    logger.addHandler(base_handler)

    _root_logger = logger
    return logger


def _get_or_create_bot_handler(bot_id: str) -> RotatingFileHandler:
    if bot_id in _bot_file_handlers:
        return _bot_file_handlers[bot_id]

    path = resolve_log_path(bot_id)
    handler = _make_file_handler(path)
    # 봇별 파일에는 bot_suffix 없어도 됨(이미 파일명이 봇별이므로)
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    # 안전을 위해 동일 필터 주입(없어도 무방)
    handler.addFilter(_EnsureBotSuffix())
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


def get_logger(bot_id: str | None = None) -> logging.LoggerAdapter:
    """
    - bot_id가 None: 기본 로거에 대한 Adapter 반환 (BASE_DIR/DEFAULT_LOG_BASENAME에 기록)
    - bot_id가 존재: 기본 로거 + 봇별 파일 핸들러 장착한 Adapter 반환 (LOGS_DIR/BOT_LOG_PATTERN)
    """
    root = _ensure_root_logger()

    if bot_id:
        # 봇별 핸들러를 root logger에(한 번만) 추가
        handler = _get_or_create_bot_handler(bot_id)
        if handler not in root.handlers:
            root.addHandler(handler)

    # 항상 Adapter로 감싸서 bot_suffix를 보장
    return BotLoggerAdapter(root, {"bot_id": bot_id})


# ---- 레거시 호환 함수 ----
def log(msg: str, bot_id: str | None = None, level: int = logging.INFO):
    logger = get_logger(bot_id)  # 항상 Adapter
    logger.log(level, msg)
