# utils/balance_store.py
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone
import threading

# ── 파일 경로
WEEKLY_STORE = Path("./data/balance_weekly.json")
DAILY_STORE  = Path("./data/balance_daily.json")
for p in (WEEKLY_STORE, DAILY_STORE):
    p.parent.mkdir(parents=True, exist_ok=True)

_file_lock = threading.Lock()

# ── 공통 유틸
def _now_utc():
    return datetime.now(timezone.utc)

def _to_day_start_utc(dt: datetime):
    dt = dt.astimezone(timezone.utc)
    return dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

def _to_week_start_utc(dt: datetime):
    dt = dt.astimezone(timezone.utc)
    week_start = dt - timedelta(days=dt.weekday())
    return week_start.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

def _iso(dt: datetime):
    return dt.astimezone(timezone.utc).isoformat()

def _load(path: Path) -> list[dict]:
    if path.exists():
        with _file_lock:
            return json.loads(path.read_text(encoding="utf-8"))
    return []

def _save(path: Path, rows: list[dict]) -> None:
    with _file_lock:
        path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

def _sort_and_clip(rows: list[dict], key_name: str, last_n: int) -> list[dict]:
    """UTC 키로 정렬 후 '현재 시각 이전'만 남기고 뒤에서 last_n개 자르기"""
    now_iso = _iso(_now_utc())
    safe = [r for r in rows if r.get(key_name) and r[key_name] <= now_iso]
    safe.sort(key=lambda r: r[key_name])
    if last_n is not None and last_n > 0:
        safe = safe[-last_n:]
    return safe

# ── 주간 스냅샷
def upsert_weekly_snapshot(balance: float):
    now = _now_utc()
    wk = _to_week_start_utc(now)
    week_key = _iso(wk)
    rows = _load(WEEKLY_STORE)
    snap = {"week": week_key, "ts": _iso(now), "balance": float(balance)}
    idx = next((i for i, r in enumerate(rows) if r.get("week") == week_key), None)
    rows.append(snap) if idx is None else rows.__setitem__(idx, snap)
    _save(WEEKLY_STORE, rows[-260:])  # 보관 한도(최대 5년치) - 필요시 조정

def get_weekly_series(last_weeks: int = 12) -> list[dict]:
    """
    주간 시리즈: 스냅샷 '있는 주'만 반환.
    -> 마지막 스냅샷 주가 맨 오른쪽 끝이 됨
    """
    rows = _load(WEEKLY_STORE)
    rows = _sort_and_clip(rows, key_name="week", last_n=last_weeks)

    out = []
    for r in rows:
        out.append({"date": r["week"][:10], "balance": float(r.get("balance", 0.0))})
    return out

# ── 일간 스냅샷
def upsert_daily_snapshot(balance: float, at: datetime | None = None):
    now = at or _now_utc()
    day = _to_day_start_utc(now)
    day_key = _iso(day)
    rows = _load(DAILY_STORE)
    snap = {"day": day_key, "ts": _iso(now), "balance": float(balance)}
    idx = next((i for i, r in enumerate(rows) if r.get("day") == day_key), None)
    rows.append(snap) if idx is None else rows.__setitem__(idx, snap)
    _save(DAILY_STORE, rows[-1095:])  # 보관 한도(약 3년치)

def get_daily_series(last_days: int = 30) -> list[dict]:
    """
    일간 시리즈: 스냅샷 '있는 날'만 반환.
    -> 마지막 스냅샷 날짜가 맨 오른쪽 끝이 됨 (미래/빈 날짜 채우지 않음)
    """
    rows = _load(DAILY_STORE)
    rows = _sort_and_clip(rows, key_name="day", last_n=last_days)

    out = []
    for r in rows:
        # "YYYY-MM-DD"만 넘김 (ApexCharts가 불규칙 간격도 지원)
        out.append({"date": r["day"][:10], "balance": float(r.get("balance", 0.0))})
    return out
