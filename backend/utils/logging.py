# utils/logging.py
import logging
import logging.handlers
import os
import threading
from typing import Optional

# ─────────────────────────────────────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────────────────────────────────────
LOG_DIR = os.getenv("LOG_DIR", "./logs")
APP_LOG_NAME = os.getenv("APP_LOG_NAME", "app.log")
MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", str(5 * 1024 * 1024)))  # 5MB
BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "5"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

_os_lock = threading.Lock()
_loggers_cache = {}  # bot_id -> logger
_app_logger: Optional[logging.Logger] = None

FMT = "%(asctime)s [%(levelname)s] %(message)s"
DATEFMT = "%Y-%m-%d %H:%M:%S"


def _ensure_dir(path: str):
    if not os.path.isdir(path):
        with _os_lock:
            os.makedirs(path, exist_ok=True)


def _mk_rotating_file_handler(path: str) -> logging.Handler:
    handler = logging.handlers.RotatingFileHandler(
        path, maxBytes=MAX_BYTES, backupCount=BACKUP_COUNT, encoding="utf-8"
    )
    handler.setFormatter(logging.Formatter(FMT, DATEFMT))
    return handler


def _get_app_logger() -> logging.Logger:
    """
    앱 전역 로그 (콘솔 + app.log). 봇별 로그와 별개.
    """
    global _app_logger
    if _app_logger is not None:
        return _app_logger

    with _os_lock:
        if _app_logger is not None:
            return _app_logger

        _ensure_dir(LOG_DIR)

        logger = logging.getLogger("app")
        logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
        logger.propagate = False  # 루트로 전파 방지

        # 중복 생성 방지
        if not logger.handlers:
            # 콘솔
            ch = logging.StreamHandler()
            ch.setFormatter(logging.Formatter(FMT, DATEFMT))
            logger.addHandler(ch)

            # 파일
            app_file = os.path.join(LOG_DIR, APP_LOG_NAME)
            fh = _mk_rotating_file_handler(app_file)
            logger.addHandler(fh)

        _app_logger = logger
        return logger


def _get_bot_logger(bot_id: str) -> logging.Logger:
    """
    봇별 전용 로거. 각 bot_id마다 logs/bot_{bot_id}.log 로 분리 기록.
    """
    if not bot_id:
        return _get_app_logger()

    if bot_id in _loggers_cache:
        return _loggers_cache[bot_id]

    with _os_lock:
        if bot_id in _loggers_cache:
            return _loggers_cache[bot_id]

        _ensure_dir(LOG_DIR)

        name = f"bot.{bot_id}"
        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
        logger.propagate = False  # 상위 로거로 전파 금지 (중복 출력 방지)

        # 중복 핸들러 방지
        if not logger.handlers:
            bot_file = os.path.join(LOG_DIR, f"bot_{bot_id}.log")
            fh = _mk_rotating_file_handler(bot_file)
            logger.addHandler(fh)

        _loggers_cache[bot_id] = logger
        return logger


def log(msg: str, *, bot_id: Optional[str] = None, level: int = None):
    """
    사용 예:
        log("hello", bot_id="abc123")
    BotRunner._log에서 호출:
        def _log(self, msg: str):
            log(msg, bot_id=self.bot_id)
    """
    if level is None:
        level = getattr(logging, LOG_LEVEL, logging.INFO)

    # 봇별 파일로 기록
    bl = _get_bot_logger(bot_id or "")
    bl.log(level, msg)

    # 전역(app)에도 한 줄 남기고 싶다면 아래 주석 해제
    # (bot_id 표시를 붙여 구분)
    app = _get_app_logger()
    if bot_id:
        app.log(level, f"[{bot_id}] {msg}")
    else:
        app.log(level, msg)
