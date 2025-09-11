# utils/logging.py
from pathlib import Path
from datetime import datetime
import re

BASE_DIR = Path(__file__).resolve().parents[1]  # 프로젝트 루트
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_LOG = BASE_DIR / "logs.txt"
BOT_ID_SAFE_RE = re.compile(r"[^A-Za-z0-9._-]")

def _safe_bot_id(s: str) -> str:
    return BOT_ID_SAFE_RE.sub("_", str(s))[:64]

def log(msg: str, bot_id: str | None = None):
    """기본 logs.txt + (옵션) 봇별 파일에 동시 기록"""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"

    # 1) 기본 파일
    with DEFAULT_LOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

    # 2) 봇별 파일
    if bot_id:
        path = LOGS_DIR / f"{_safe_bot_id(bot_id)}.log"
        with path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")