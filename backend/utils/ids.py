# utils/ids.py
import re

_ID_SAFE_RE = re.compile(r"[^A-Za-z0-9._-]")

def safe_id(s: str, max_len: int = 64) -> str:
    """파일명/키 등에 안전한 ID로 정규화"""
    return _ID_SAFE_RE.sub("_", str(s))[:max_len]