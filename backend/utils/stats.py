# utils/stats.py
from __future__ import annotations
import json, time, threading
from pathlib import Path
from typing import Literal, Optional, Dict, Any, List
from datetime import datetime, timezone, timedelta

_STATS_FILE = Path("./data/stats.json")
_LOCK = threading.Lock()

# 이벤트 보존 개수 (기간 조회 대비 여유롭게)
_MAX_EVENTS = 20000
_MAX_DEDUPE = 5000

# 기본 구조
_DEFAULT = {
    "totals": {                   # 합계
        "volume": 0.0,           # 체결 금액 합(∑ |price * qty|)
        "entries_new": 0,        # 1차 시장가 진입 횟수
        "entries_dca": 0,        # DCA 추가 체결 횟수
        "tp_fills": 0            # TP 체결 횟수
    },
    "by_symbol": {},             # 심볼별 {volume, entries_new, entries_dca, tp_fills}
    "events": [],                # 최근 이벤트 로그 (최대 N 개 유지)
    "dedupe": {                  # 중복 방지용 키 저장
        "fills": []              # order_key 목록 (예: f"{symbol}:{orderId}:{time}")
    },
    "last_updated": 0
}

def _load() -> Dict[str, Any]:
    if _STATS_FILE.exists():
        try:
            with _STATS_FILE.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    # deep copy of DEFAULT
    return json.loads(json.dumps(_DEFAULT))

def _save(data: Dict[str, Any]) -> None:
    _STATS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with _STATS_FILE.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

def _buckets_for(sym: str, data: Dict[str, Any]) -> Dict[str, Any]:
    bys = data.setdefault("by_symbol", {})
    if sym not in bys:
        bys[sym] = {"volume": 0.0, "entries_new": 0, "entries_dca": 0, "tp_fills": 0}
    return bys[sym]

def _append_event(data: Dict[str, Any], event: Dict[str, Any]) -> None:
    data["events"].append(event)
    if len(data["events"]) > _MAX_EVENTS:
        data["events"] = data["events"][-_MAX_EVENTS:]

def _mark_dedupe(data: Dict[str, Any], key: str) -> None:
    dedupe = data.setdefault("dedupe", {}).setdefault("fills", [])
    dedupe.append(key)
    if len(dedupe) > _MAX_DEDUPE:
        data["dedupe"] = {"fills": dedupe[-_MAX_DEDUPE:]}

def _seen(data: Dict[str, Any], key: str) -> bool:
    return key in data.get("dedupe", {}).get("fills", [])

def _add_volume(data: Dict[str, Any], sym: str, notional: float) -> None:
    data["totals"]["volume"] = round(float(data["totals"].get("volume", 0.0)) + abs(notional), 8)
    bs = _buckets_for(sym, data)
    bs["volume"] = round(float(bs.get("volume", 0.0)) + abs(notional), 8)

def record_event(
    *,
    kind: Literal["ENTRY","DCA","TP"],
    symbol: str,
    price: float,
    qty: float,
    order_id: Optional[int] = None,
    client_order_id: Optional[str] = None,
    ts_ms: Optional[int] = None,
    # ⬇️ 추가: TP 실현손익 집계를 위해
    pnl: Optional[float] = None,
    side: Optional[str] = None,          # "BUY"|"SELL" (TP일 때 방향 참고)
    entry_price: Optional[float] = None  # TP 계산 참고용(디버그)
) -> None:
    """
    runner.py에서 체결 '확정' 시점마다 호출.
    - 중복 방지: (symbol, order_id/clientOrderId, ts_ms) 조합으로 1회만 반영
    - notional = |price * qty|
    - ENTRY/DCA/TP 카운트를 즉시 증가
    """
    if ts_ms is None:
        ts_ms = int(time.time() * 1000)
    key = f"{symbol}:{order_id or client_order_id or ''}:{ts_ms}"

    with _LOCK:
        data = _load()
        if _seen(data, key):
            return  # 이미 집계됨

        notional = float(abs(price) * abs(qty))
        _add_volume(data, symbol, notional)

        if kind == "ENTRY":
            data["totals"]["entries_new"] = int(data["totals"].get("entries_new", 0)) + 1
            _buckets_for(symbol, data)["entries_new"] += 1
        elif kind == "DCA":
            data["totals"]["entries_dca"] = int(data["totals"].get("entries_dca", 0)) + 1
            _buckets_for(symbol, data)["entries_dca"] += 1
        elif kind == "TP":
            data["totals"]["tp_fills"] = int(data["totals"].get("tp_fills", 0)) + 1
            _buckets_for(symbol, data)["tp_fills"] += 1

        _append_event(data, {
            "t": ts_ms,
            "kind": kind,
            "symbol": symbol,
            "price": float(price),
            "qty": float(qty),
            "orderId": order_id,
            "clientOrderId": client_order_id,
            "notional": notional,
            # ⬇️ 선택 필드
            **({"pnl": float(pnl)} if pnl is not None else {}),
            **({"side": str(side)} if side else {}),
            **({"entryPrice": float(entry_price)} if entry_price is not None else {}),
        })
        _mark_dedupe(data, key)
        data["last_updated"] = int(time.time())

        _save(data)

def get_stats() -> Dict[str, Any]:
    with _LOCK:
        return _load()

def reset_stats() -> None:
    with _LOCK:
        _save(json.loads(json.dumps(_DEFAULT)))

# ===== 기간 합산 =====

def _since_ms_from_window(window: str, default_days: int = 30) -> int:
    """
    "7d", "30d", "4w" 같은 윈도우를 ms 타임스탬프로 변환
    """
    s = (window or "").strip().lower()
    now = datetime.now(timezone.utc)
    try:
        if s.endswith("d"):
            days = int(s[:-1] or default_days)
            dt = now - timedelta(days=days)
        elif s.endswith("w"):
            weeks = int(s[:-1] or max(default_days // 7, 1))
            dt = now - timedelta(weeks=weeks)
        else:
            dt = now - timedelta(days=default_days)
    except Exception:
        dt = now - timedelta(days=default_days)
    return int(dt.timestamp() * 1000)

def _aggregate(events: List[Dict[str, Any]], symbols: Optional[List[str]] = None) -> Dict[str, Any]:
    totals = {"volume": 0.0, "entries_new": 0, "entries_dca": 0, "tp_fills": 0}
    by_symbol: Dict[str, Dict[str, Any]] = {}
    fills_count = 0

    def bucket(sym: str) -> Dict[str, Any]:
        if sym not in by_symbol:
            by_symbol[sym] = {"volume": 0.0, "entries_new": 0, "entries_dca": 0, "tp_fills": 0}
        return by_symbol[sym]

    for ev in events:
        sym = ev.get("symbol")
        if symbols and sym not in symbols:
            continue
        kind = (ev.get("kind") or "").upper()
        notional = float(ev.get("notional") or (abs(float(ev.get("price", 0.0)) * float(ev.get("qty", 0.0)))))
        totals["volume"] += abs(notional)
        b = bucket(sym)
        b["volume"] = b.get("volume", 0.0) + abs(notional)

        if kind == "ENTRY":
            totals["entries_new"] += 1
            b["entries_new"] = b.get("entries_new", 0) + 1
            fills_count += 1
        elif kind == "DCA":
            totals["entries_dca"] += 1
            b["entries_dca"] = b.get("entries_dca", 0) + 1
            fills_count += 1
        elif kind == "TP":
            totals["tp_fills"] += 1
            b["tp_fills"] = b.get("tp_fills", 0) + 1

    totals["volume"] = round(totals["volume"], 8)
    for sym in by_symbol:
        by_symbol[sym]["volume"] = round(by_symbol[sym]["volume"], 8)

    return {
        "totals": totals,
        "by_symbol": by_symbol,
        "fills_count": fills_count,
    }

def get_stats_window(*, window: str = "30d", symbols: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    윈도우(예: "7d","30d","4w") 기준으로 events만 필터링하여 합산.
    반환 스키마는 /api/stats 와 거의 동일.
    """
    since_ms = _since_ms_from_window(window, 30)
    with _LOCK:
        data = _load()
        events = [ev for ev in (data.get("events") or []) if int(ev.get("t", 0)) >= since_ms]
        agg = _aggregate(events, symbols)
        return {
            "window": window,
            "since_ms": since_ms,
            "last_updated": data.get("last_updated") or 0,
            **agg,
        }

def get_profit_window(*, window: str = "30d", symbols: Optional[List[str]] = None, baseline_usdt: Optional[float] = None) -> Dict[str, Any]:
    """
    TP 이벤트의 pnl을 윈도우 기준으로 합산.
    baseline_usdt가 주어지면 수익률 = pnl_sum / baseline_usdt * 100
    없으면 수익률 0 (또는 필요시 다른 기준으로 바꿔도 됨)
    """
    since_ms = _since_ms_from_window(window, 30)
    with _LOCK:
        data = _load()
        evs = [ev for ev in (data.get("events") or []) if int(ev.get("t", 0)) >= since_ms]
        if symbols:
            evs = [ev for ev in evs if ev.get("symbol") in symbols]

        pnl_sum = 0.0
        win = 0
        loss = 0
        breakeven = 0

        for ev in evs:
            if (ev.get("kind") or "").upper() != "TP":
                continue
            pnl = float(ev.get("pnl") or 0.0)
            pnl_sum += pnl
            if pnl > 0:
                win += 1
            elif pnl < 0:
                loss += 1
            else:
                breakeven += 1

        pct = 0.0
        if baseline_usdt:
            try:
                pct = (pnl_sum / float(baseline_usdt)) * 100.0
            except Exception:
                pct = 0.0

        return {
            "window": window,
            "since_ms": since_ms,
            "pnl_usdt": round(pnl_sum, 2),
            "pnl_pct": round(pct, 2),
            "wins": win,
            "losses": loss,
            "breakeven": breakeven,
            "last_updated": data.get("last_updated") or 0,
        }

def _month_start_utc_ms() -> int:
    now = datetime.now(timezone.utc)
    dt = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return int(dt.timestamp() * 1000)

def _sum_tp(events: List[dict], symbols: Optional[List[str]] = None):
    pnl_sum = 0.0
    win = loss = breakeven = 0
    for ev in events:
        if (ev.get("kind") or "").upper() != "TP":
            continue
        if symbols and ev.get("symbol") not in symbols:
            continue
        pnl = float(ev.get("pnl") or 0.0)
        pnl_sum += pnl
        if pnl > 0: win += 1
        elif pnl < 0: loss += 1
        else: breakeven += 1
    return pnl_sum, win, loss, breakeven

def get_profit_kpi(*, baseline_usdt: Optional[float] = None, symbols: Optional[List[str]] = None) -> dict:
    """
    전체 누적(ALL)과 이번 달 MTD(UTC) 둘 다 반환
    """
    with _LOCK:
        data = _load()
        all_events = data.get("events") or []

        # ALL
        all_pnl, all_win, all_loss, all_be = _sum_tp(all_events, symbols)

        # MTD (UTC 현재 달 1일 00:00:00 ~ 현재)
        since_ms = _month_start_utc_ms()
        mtd_events = [ev for ev in all_events if int(ev.get("t", 0)) >= since_ms]
        mtd_pnl, mtd_win, mtd_loss, mtd_be = _sum_tp(mtd_events, symbols)

        mtd_pct = 0.0
        if baseline_usdt and float(baseline_usdt) != 0.0:
            try:
                mtd_pct = (mtd_pnl / float(baseline_usdt)) * 100.0
            except Exception:
                mtd_pct = 0.0

        month_label = datetime.now(timezone.utc).strftime("%Y-%m")  # 예: 2025-09

        return {
            "all": {
                "pnl_usdt": round(all_pnl, 2),
                "wins": all_win,
                "losses": all_loss,
                "breakeven": all_be,
            },
            "mtd": {
                "pnl_usdt": round(mtd_pnl, 2),
                "pnl_pct": round(mtd_pct, 2),
                "wins": mtd_win,
                "losses": mtd_loss,
                "breakeven": mtd_be,
                "since_ms": since_ms,
                "month_utc": month_label,
            },
            "last_updated": data.get("last_updated") or 0,
        }