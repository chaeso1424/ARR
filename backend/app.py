# app.py — A안(봇별 로그) 정리본 (optimized & instrumented)
import os
import json, math
import re
import jwt
import datetime as dt
import threading
import time as _time, time
from dotenv import load_dotenv
from pathlib import Path
from collections import deque
from functools import wraps
from flask import Flask, request, jsonify, Response, stream_with_context, g
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from time import perf_counter

# ───────────────────────────────────────────────────────────────────────────────
# 0) 스레드풀 / 전역 캐시
# ───────────────────────────────────────────────────────────────────────────────
_EXEC = ThreadPoolExecutor(max_workers=5)  # 2 vCPU 기준 4~6 권장

# 주별 일별 시리즈
LAST_WEEKLY = {"series": [], "ts": 0.0}
LAST_DAILY  = {"series": [], "ts": 0.0}

# 상태 캐시(봇 status용)
STATUS_CACHE = {}  # { bot_id: {"ts": float, "data": dict} }
STATUS_TTL = 2.0   # 초

LAST_SUMMARY = None
LAST_SUMMARY_TS = 0.0
LAST_SUMMARY_LOCK = threading.Lock()

# ───────────────────────────────────────────────────────────────────────────────
# 1) 환경 로드
# ───────────────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
load_dotenv()  # 현재 작업 디렉토리 기준

from collections import defaultdict
from utils.balance_store import (
    upsert_weekly_snapshot, get_weekly_series,
    upsert_daily_snapshot, get_daily_series
)
from utils.stats import get_stats, reset_stats, get_stats_window, get_profit_window, get_profit_kpi

from utils.logging import log
from models.config import BotConfig
from models.state import BotState
from services.bingx_client import BingXClient, BASE, _req_get, _ts
from bot.runner import BotRunner
from flask_cors import CORS

# ───────────────────────────────────────────────────────────────────────────────
# 2) 경로/상수
# ───────────────────────────────────────────────────────────────────────────────
BOTS_DIR = BASE_DIR / "data" / "bots"
BOTS_DIR.mkdir(parents=True, exist_ok=True)

LOGS_DIR = BASE_DIR /"logs"              # backend/logs/{bot}.log
LOGS_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_LOG_FILE = BASE_DIR / "logs.txt"  # 공용 로그(옵션)

ANSI_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")      # ANSI 컬러 제거

APPLY_ON_SAVE = os.getenv("APPLY_ON_SAVE", "true").lower() == "true"
SKIP_SETUP   = os.getenv("SKIP_SETUP", "false").lower() == "false" and False  # 안전 기본값 False
DEFAULT_BOT_ID = "default"

ENABLE_SNAPSHOT_DAEMON = os.getenv("ENABLE_SNAPSHOT_DAEMON", "true").lower() == "true"

NO_CACHE_HEADERS = {
    "Cache-Control": "no-cache, no-store, must-revalidate",
    "Pragma": "no-cache",
    "Expires": "0",
}

DEFAULT_SUMMARY = {
    "asset": "USDT",
    "balance": 0.0,
    "equity": 0.0,
    "available_margin": 0.0,
}

# ───────────────────────────────────────────────────────────────────────────────
# 3) Flask 앱/전역
# ───────────────────────────────────────────────────────────────────────────────
app = Flask(__name__)
# ⚠️ jwt decode/encode 모두 app.config['SECRET_KEY'] 사용 → 확실히 세팅
app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET', 'dev')
app.secret_key = app.config['SECRET_KEY']

USERS = {
    "artcokr12345@naver.com": "16430878a!!",
}

client = BingXClient()
BOTS: dict[str, dict] = {}  # { bot_id: {"cfg": BotConfig, "state": BotState, "runner": BotRunner} }

# ───────────────────────────────────────────────────────────────────────────────
# 3-1) 경량 타이밍 유틸
# ───────────────────────────────────────────────────────────────────────────────
class Span:
    def __init__(self, name):
        self.name = name
        self.t0 = perf_counter()
        self.dt = None
    def end(self):
        self.dt = (perf_counter() - self.t0) * 1000.0
        return self.dt

def tmark(key: str):
    if not hasattr(g, '_marks'): g._marks = {}
    g._marks[key] = perf_counter()

def tdelta(key: str):
    if not hasattr(g, '_marks'): return None
    t0 = g._marks.get(key)
    if t0 is None: return None
    return (perf_counter() - t0) * 1000.0

@app.before_request
def _tic():
    g._t0 = time.time()
    g._marks = {}
    tmark('req')

@app.after_request
def _toc(resp):
    try:
        dt = (time.time() - getattr(g, "_t0", time.time())) * 1000
        resp.headers["X-Elapsed-ms"] = f"{dt:.1f}"
        # 가장 중요한 엔드포인트 일부는 세부 마크도 함께 로깅
        detail = []
        for k in getattr(g, '_marks', {}):
            if k != 'req':
                d = tdelta(k)
                if d is not None:
                    detail.append(f"{k}={d:.1f}ms")
        detail_s = (" "+" ".join(detail)) if detail else ""
        print(f"[{request.method}] {request.path} -> {resp.status_code} in {dt:.1f}ms{detail_s}")
    except Exception:
        pass
    return resp

# ✅ CORS 설정: 프론트 dev 도메인 허용 + Authorization 헤더 허용
CORS(
    app,
    resources={
        r"/api/*": {"origins": ["http://localhost:3000", "http://127.0.0.1:3000", "*"]},
        r"/logs/*": {"origins": ["http://localhost:3000", "http://127.0.0.1:3000", "*"]},
    },
    supports_credentials=False,
    allow_headers=["Content-Type", "Authorization"],
    expose_headers=["Content-Type", "X-Elapsed-ms"],
    methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
)

# ───────────────────────────────────────────────────────────────────────────────
# 4) 인증(토큰)
# ───────────────────────────────────────────────────────────────────────────────
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        if "Authorization" in request.headers:
            parts = request.headers["Authorization"].split(" ")
            if len(parts) == 2 and parts[0].lower() == "bearer":
                token = parts[1]
        if not token:
            return jsonify({"error": "Token missing"}), 401
        try:
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=["HS256"])
            request.user = data["sub"]
        except jwt.ExpiredSignatureError:
            return jsonify({"error": "Token expired"}), 401
        except Exception:
            return jsonify({"error": "Token invalid"}), 401
        return f(*args, **kwargs)
    return decorated

# ───────────────────────────────────────────────────────────────────────────────
# 5) 봇 설정 저장/로드 유틸
# ───────────────────────────────────────────────────────────────────────────────
def safe_bot_id(s: str) -> str:
    """파일 경로 안전화"""
    return re.sub(r"[^A-Za-z0-9._-]", "_", s)[:64]

def bot_file(bot_id: str) -> Path:
    return BOTS_DIR / f"{bot_id}.json"

def default_config(bot_id: str = "bot-1", name: str | None = None) -> dict:
    return {
        "id": bot_id,
        "name": name or bot_id,
        "symbol": "ETH-USDT",
        "side": "BUY",
        "margin_mode": "CROSS",
        "leverage": 10,
        "tp_percent": 0.5,
        "repeat_mode": True,
        "dca_config": [[0, 50], [0.4, 50]],
    }

def read_bot_config(bot_id: str) -> dict | None:
    f = bot_file(bot_id)
    if f.exists():
        with f.open("r", encoding="utf-8") as fp:
            return json.load(fp)
    return None

def write_bot_config(bot_id: str, data: dict):
    # 기본 보정
    data = {**default_config(bot_id), **data}
    f = bot_file(bot_id)
    f.parent.mkdir(parents=True, exist_ok=True)
    with f.open("w", encoding="utf-8") as fp:
        json.dump(data, fp, ensure_ascii=False, indent=2)

def load_cfg_into_obj(cfg_obj: BotConfig, data: dict):
    cfg_obj.symbol       = data.get("symbol", getattr(cfg_obj, "symbol", "ETH-USDT"))
    cfg_obj.side         = data.get("side",   getattr(cfg_obj, "side", "BUY"))
    cfg_obj.margin_mode  = data.get("margin_mode", getattr(cfg_obj, "margin_mode", "CROSS"))
    try:    cfg_obj.leverage   = int(data.get("leverage", getattr(cfg_obj, "leverage", 10)))
    except: pass
    try:    cfg_obj.tp_percent = float(data.get("tp_percent", getattr(cfg_obj, "tp_percent", 0.5)))
    except: pass
    dca = data.get("dca_config")
    if isinstance(dca, list):
        cfg_obj.dca_config = dca

def get_or_create_bot(bot_id: str) -> dict:
    if bot_id in BOTS:
        return BOTS[bot_id]
    data = read_bot_config(bot_id) or default_config(bot_id)
    cfg = BotConfig(
        symbol=data["symbol"],
        side=data["side"],
        margin_mode=data["margin_mode"],
        leverage=int(data["leverage"]),
        tp_percent=float(data["tp_percent"]),
        dca_config=data["dca_config"],
    )
    state = BotState()
    state.repeat_mode = bool(data.get("repeat_mode", False))
    runner = BotRunner(cfg, state, client, bot_id=bot_id)  # ← runner가 bot_id 받아서 각자 로그 파일 쓰게 구현 권장
    BOTS[bot_id] = {"cfg": cfg, "state": state, "runner": runner}
    return BOTS[bot_id]

# 서버 부팅 시 기본 봇 확보
get_or_create_bot(DEFAULT_BOT_ID)

# ───────────────────────────────────────────────────────────────────────────────
# 6) 로그 tail 유틸 (A안: 파일 분리)
# ───────────────────────────────────────────────────────────────────────────────
def _tail_log_lines(path: Path, tail: int, grep: str | None = None, level: str | None = None, strip_ansi: bool = True):
    tail = max(1, min(int(tail or 500), 5000))
    lines = deque(maxlen=tail)
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if strip_ansi:
                    line = ANSI_RE.sub("", line)
                if grep and grep.lower() not in line.lower():
                    continue
                if level and (level.lower() not in line.lower()):
                    continue
                lines.append(line.rstrip("\n"))
    except FileNotFoundError:
        return []
    return list(lines)

# ───────────────────────────────────────────────────────────────────────────────
# 7) 주별 잔고 시리즈 (캐시 즉시 응답 + 백그라운드 갱신 + 프리워밍)
# ───────────────────────────────────────────────────────────────────────────────
LAST_DAILY  = {"series": [], "ts": 0.0}
LAST_WEEKLY = {"series": [], "ts": 0.0}
MAX_AGE_S   = 60  # 신선 기준(초)

def _refresh_daily(days):
    s = Span("refresh_daily")
    try:
        res = get_daily_series(days)
        LAST_DAILY["series"] = res or []
        LAST_DAILY["ts"] = _time.time()
    except Exception as e:
        print("[daily refresh] error:", e)
    finally:
        print(f"[series] daily refreshed in {s.end():.1f}ms, len={len(LAST_DAILY['series'])}")

def _refresh_weekly(weeks):
    s = Span("refresh_weekly")
    try:
        res = get_weekly_series(weeks)
        LAST_WEEKLY["series"] = res or []
        LAST_WEEKLY["ts"] = _time.time()
    except Exception as e:
        print("[weekly refresh] error:", e)
    finally:
        print(f"[series] weekly refreshed in {s.end():.1f}ms, len={len(LAST_WEEKLY['series'])}")

# 앱 시작 시 캐시 프리워밍(비차단)
_EXEC.submit(_refresh_daily, 30)
_EXEC.submit(_refresh_weekly, 12)

@app.get("/api/balance/series")
def api_balance_series():
    tmark('series_entry')
    gran = (request.args.get("granularity") or "w").lower()
    now  = _time.time()
    if gran == "d":
        days = int(request.args.get("days", "30") or 30)
        series = LAST_DAILY["series"] or []
        stale  = (now - LAST_DAILY["ts"] > MAX_AGE_S)
        if stale:
            print("[series] daily stale → refresh schedule")
            _EXEC.submit(_refresh_daily, days)
        return jsonify({"granularity": "d", "series": series, "stale": stale})
    else:
        weeks = int(request.args.get("weeks", "12") or 12)
        series = LAST_WEEKLY["series"] or []
        stale  = (now - LAST_WEEKLY["ts"] > MAX_AGE_S)
        if stale:
            print("[series] weekly stale → refresh schedule")
            _EXEC.submit(_refresh_weekly, weeks)
        return jsonify({"granularity": "w", "series": series, "stale": stale})

# ───────────────────────────────────────────────────────────────────────────────
# 8) 거래 요약/통계/수익 KPI
# ───────────────────────────────────────────────────────────────────────────────
@app.get("/api/trades/summary")
def api_trades_summary():
    window = (request.args.get("window") or "30d").lower()
    symbols_param = request.args.get("symbols")
    symbols = [s.strip() for s in symbols_param.split(",")] if symbols_param else None
    agg = get_stats_window(window=window, symbols=symbols)
    return jsonify({
        "window": window,
        "total_volume": agg["totals"]["volume"],
        "fills_count": agg["fills_count"],
        "entries": {
            "new": agg["totals"]["entries_new"],
            "dca": agg["totals"]["entries_dca"]
        }
    })

@app.get("/api/stats")
def api_stats():
    window = (request.args.get("window") or "30d").lower()
    symbols_param = request.args.get("symbols")
    symbols = [s.strip() for s in symbols_param.split(",")] if symbols_param else None
    res = get_stats_window(window=window, symbols=symbols)
    return jsonify(res)

@app.post("/api/stats/reset")
def api_stats_reset():
    reset_stats()
    return jsonify({"ok": True})

@app.get("/api/profit/summary")
def api_profit_summary():
    window = request.args.get("window", "30d")
    baseline = request.args.get("baseline")
    baseline_usdt = float(baseline) if baseline is not None else None

    symbols_param = request.args.get("symbols")
    symbols = [s.strip() for s in symbols_param.split(",")] if symbols_param else None

    agg = get_stats_window(window=window, symbols=symbols)
    pnl_usdt = 0.0
    wins = losses = 0

    from utils.stats import _since_ms_from_window, _load, _LOCK
    since_ms = _since_ms_from_window(window, 30)
    with _LOCK:
        raw = _load()
        window_events = [ev for ev in (raw.get("events") or []) if int(ev.get("t", 0)) >= since_ms]
        if symbols:
            window_events = [ev for ev in window_events if ev.get("symbol") in symbols]
        for ev in window_events:
            if (ev.get("kind") or "").upper() == "TP":
                pnl = float(ev.get("pnl") or 0.0)
                pnl_usdt += pnl
                if pnl > 0: wins += 1
                elif pnl < 0: losses += 1

    pnl_pct = None
    if baseline_usdt and baseline_usdt != 0:
        pnl_pct = round((pnl_usdt / baseline_usdt) * 100.0, 2)

    return jsonify({
        "window": window,
        "pnl_usdt": round(pnl_usdt, 2),
        "pnl_pct": pnl_pct,
        "wins": wins,
        "losses": losses
    })

@app.get("/api/profit/kpi")
def api_profit_kpi():
    baseline = request.args.get("baseline")
    baseline_usdt = float(baseline) if baseline is not None else None

    symbols_param = request.args.get("symbols")
    symbols = [s.strip() for s in symbols_param.split(",")] if symbols_param else None

    data = get_profit_kpi(baseline_usdt=baseline_usdt, symbols=symbols)
    return jsonify(data)

# ───────────────────────────────────────────────────────────────────────────────
# 9) 계정 요약(SSE/스냅샷)
# ───────────────────────────────────────────────────────────────────────────────
_LAST_WS_SAVE = 0.0

def _summary_from_snap(snap):
    d = snap if isinstance(snap, dict) else {}
    return {
        "currency": d.get("asset", "USDT"),
        "balance": float(d.get("balance", 0) or 0),
        "equity": float(d.get("equity", 0) or 0),
        "available_margin": float(d.get("available_margin", 0) or 0),
    }

def _seconds_until_next_utc_midnight():
    now = datetime.now(timezone.utc)
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(1, int((tomorrow - now).total_seconds()))

def _daily_snapshot_worker():
    while True:
        try:
            snap = client._get_snapshot_cached(min_ttl=0.0)
            summ = _summary_from_snap(snap)
            bal = summ["balance"]
            upsert_daily_snapshot(bal)
            upsert_weekly_snapshot(bal)
            print("[snapshot] daily+weekly saved", bal)
        except Exception as e:
            print("[snapshot] failed:", e)
        time.sleep(_seconds_until_next_utc_midnight())

def start_snapshot_daemon_once():
    if ENABLE_SNAPSHOT_DAEMON and not getattr(start_snapshot_daemon_once, "_started", False):
        t = threading.Thread(target=_daily_snapshot_worker, daemon=True)
        t.start()
        start_snapshot_daemon_once._started = True

# Flask 앱 생성 직후 어딘가에서 호출
start_snapshot_daemon_once()

@app.get("/api/account/summary")
def account_summary():
    try:
        tmark('snap')
        snap = client._get_snapshot_cached(min_ttl=5.0)
        res = _summary_from_snap(snap)
        # 주간 스냅샷 저장 (여기서도 None 안전)
        try:
            _maybe_upsert_weekly_async(res["balance"])
        except Exception as e:
            print("weekly snapshot save failed:", e)
        # 성공값은 캐시에 보관
        global LAST_SUMMARY
        LAST_SUMMARY = res
        return jsonify(res), 200
    except Exception as e:
        global LAST_SUMMARY
        if LAST_SUMMARY:
            return jsonify(LAST_SUMMARY), 200
        return jsonify(_summary_from_snap(None)), 200

def _maybe_upsert_weekly_async(balance, min_interval=300):
    global _LAST_WS_SAVE
    now = _time.time()
    if now - _LAST_WS_SAVE < min_interval:
        return
    _LAST_WS_SAVE = now
    threading.Thread(target=lambda: upsert_weekly_snapshot(balance), daemon=True).start()

@app.get("/api/account/summary/stream")
def account_summary_stream():
    @stream_with_context
    def gen():
        yield "retry: 10000\n\n"
        last = None
        while True:
            try:
                snap = client._get_snapshot_cached(min_ttl=5.0)
                res = _summary_from_snap(snap)
                if res != last:
                    yield f"data: {json.dumps(res)}\n\n"
                    last = res
                else:
                    yield ": keep-alive\n\n"
            except Exception as e:
                # 에러 시에도 마지막 값 또는 기본값으로 유지
                yield f": err {str(e)}\n\n"
                fallback = LAST_SUMMARY or _summary_from_snap(None)
                yield f"data: {json.dumps(fallback)}\n\n"
            time.sleep(10)

    return Response(
        gen(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )

# ───────────────────────────────────────────────────────────────────────────────
# 10) 로그 API (A안: 봇별 파일 + 파일 화이트리스트)
# ───────────────────────────────────────────────────────────────────────────────
# 허용 파일 화이트리스트 (필요하면 더 추가)
FILE_MAP = {
    "logs.txt": DEFAULT_LOG_FILE,   # 예: /opt/arr/backend/logs.txt
}

def _resolve_log_path(bot: str | None, file_key: str | None):
    """
    우선순위:
    1) file_key가 있으면 화이트리스트에서만 허용
    2) bot이 있으면 backend/{bot}.log
    3) 없으면 DEFAULT_LOG_FILE
    """
    if file_key:
        p = FILE_MAP.get(str(file_key))
        return p if p else None
    if bot:
        return LOGS_DIR / f"{safe_bot_id(bot)}.log"
    return DEFAULT_LOG_FILE

@app.get("/api/logs")
def logs_json():
    tmark('logs')
    tail   = request.args.get("tail", "500")
    bot    = request.args.get("bot")
    file_k = request.args.get("file")
    grep   = request.args.get("grep")
    level  = request.args.get("level")
    noansi = request.args.get("noansi", "1") != "0"

    path = _resolve_log_path(bot, file_k)
    if file_k and path is None:
        return jsonify({"error": "unsupported file"}), 400, NO_CACHE_HEADERS
    if not path or not path.exists():
        return jsonify([]), 200, NO_CACHE_HEADERS

    lines = _tail_log_lines(path, tail, grep=grep, level=level, strip_ansi=noansi)
    return jsonify(lines), 200, NO_CACHE_HEADERS

@app.get("/logs/text")
def logs_text():
    tail   = request.args.get("tail", "500")
    bot    = request.args.get("bot")
    file_k = request.args.get("file")
    grep   = request.args.get("grep")
    level  = request.args.get("level")
    noansi = request.args.get("noansi", "1") != "0"

    path = _resolve_log_path(bot, file_k)
    if file_k and path is None:
        return Response("unsupported file", status=400, mimetype="text/plain", headers=NO_CACHE_HEADERS)
    if not path or not path.exists():
        return Response("", mimetype="text/plain", headers=NO_CACHE_HEADERS)

    lines = _tail_log_lines(path, tail, grep=grep, level=level, strip_ansi=noansi)
    body = "\n".join(lines)
    return Response(body, mimetype="text/plain", headers=NO_CACHE_HEADERS)

# ───────────────────────────────────────────────────────────────────────────────
# 11) 다중 봇 API (병렬 수집 + 캐시 + 타이밍)
# ───────────────────────────────────────────────────────────────────────────────
def _apply_exchange_settings_async(cfg):
    try:
        client.set_margin_mode(cfg.symbol, cfg.margin_mode)
        client.set_leverage(cfg.symbol, cfg.leverage)
    except Exception as e:
        log(f"⚠️ config 적용 실패({getattr(cfg,'symbol',None)}): {e}")

def _get_status_live(cfg, state, timeout_each=0.7):
    s_all = Span("status_live")
    # 병렬 제출
    fut_ppqp = _EXEC.submit(lambda: client.get_symbol_filters(cfg.symbol))
    fut_mark = _EXEC.submit(lambda: float(client.get_mark_price(cfg.symbol)))
    fut_pos  = _EXEC.submit(lambda: client.position_info(cfg.symbol, cfg.side))
    fut_lev  = _EXEC.submit(lambda: client.get_current_leverage(cfg.symbol, cfg.side))

    # 개별 타임아웃 수집(타임아웃/에러는 None)
    def _get(f, name):
        s = Span(name)
        try:
            val = f.result(timeout=timeout_each)
            return val
        except Exception as e:
            print(f"[status] {name} timeout/error: {e}")
            return None
        finally:
            d = s.end()
            # 50ms 넘는 것만 찍어서 노이즈 감소
            if d >= 50:
                print(f"[status] {name} {d:.1f}ms")

    ppqp = _get(fut_ppqp, 'ppqp') or (4, 0)
    try:
        pp, qp = int(ppqp[0]), int(ppqp[1])
    except:
        pp, qp = 4, 0

    mark = _get(fut_mark, 'mark')
    if mark is not None:
        try: mark = float(mark)
        except: mark = None

    pos = _get(fut_pos, 'pos') or (0.0, 0.0)
    try:
        avg, qty = float(pos[0] or 0.0), float(pos[1] or 0.0)
    except:
        avg, qty = 0.0, 0.0

    exch_lev = _get(fut_lev, 'lev')

    d_all = s_all.end()
    if d_all >= 100:
        print(f"[status] aggregate {d_all:.1f}ms")

    return {
        "running": state.running,
        "repeat_mode": getattr(state, "repeat_mode", False),
        "tp_order_id": getattr(state, "tp_order_id", None),
        "symbol": cfg.symbol,
        "side": cfg.side,
        "price_precision": pp,
        "position_avg_price": avg,
        "position_qty": qty,
        "mark_price": mark,
        "exchange_leverage": exch_lev,
        "cfg_leverage": cfg.leverage,
        "dca_config": cfg.dca_config,
    }

@app.get("/api/bots")
def list_bots():
    s = Span("bots_list")
    out = []
    for f in BOTS_DIR.glob("*.json"):
        bot_id = f.stem
        cfg_data = read_bot_config(bot_id) or default_config(bot_id)
        running = bool(bot_id in BOTS and BOTS[bot_id]["state"].running)
        out.append({
            "id": bot_id,
            "name": cfg_data.get("name", bot_id),
            "symbol": cfg_data.get("symbol"),
            "side": cfg_data.get("side"),
            "margin_mode": cfg_data.get("margin_mode"),
            "leverage": cfg_data.get("leverage"),
            "status": "running" if running else "stopped",
            "running": running
        })
    d = s.end()
    if d >= 50:
        print(f"[/api/bots] built in {d:.1f}ms (n={len(out)})")
    # 파일이 하나도 없으면 "파일만" 생성하고 리턴 (Runner 생성 X)
    if not out:
        cfg = default_config(DEFAULT_BOT_ID)
        write_bot_config(DEFAULT_BOT_ID, cfg)
        out = [{
            "id": DEFAULT_BOT_ID, "name": DEFAULT_BOT_ID, "symbol": cfg["symbol"], "side": cfg["side"],
            "margin_mode": cfg["margin_mode"], "leverage": cfg["leverage"],
            "status": "stopped", "running": False
        }]
    return jsonify(out)

@app.post("/api/bots")
def create_bot():
    data = request.get_json(force=True, silent=True) or {}
    bot_id = data.get("id") or f"bot_{int(time.time())}"
    if bot_file(bot_id).exists():
        return jsonify({"ok": False, "error": "bot_id already exists"}), 400

    name = str(data.get("name", "")).strip() or bot_id
    cfg = default_config(bot_id, name)
    cfg.update({
        "symbol": data.get("symbol", cfg["symbol"]),
        "side": data.get("side", cfg["side"]),
        "margin_mode": str(data.get("margin_mode", cfg["margin_mode"])).upper(),
        "leverage": int(data.get("leverage", cfg["leverage"])),
        "tp_percent": float(data.get("tp_percent", cfg["tp_percent"])),
        "repeat_mode": bool(data.get("repeat_mode", cfg["repeat_mode"])),
        "dca_config": data.get("dca_config", cfg["dca_config"]),
    })
    write_bot_config(bot_id, cfg)
    get_or_create_bot(bot_id)
    return jsonify({"ok": True, "id": bot_id})

@app.delete("/api/bots/<bot_id>")
def delete_bot(bot_id):
    bot = BOTS.get(bot_id)
    if bot:
        try: bot["runner"].stop()
        except Exception: pass
        BOTS.pop(bot_id, None)
    f = bot_file(bot_id)
    if f.exists():
        f.unlink()
    # 로그 파일도 같이 제거하려면:
    lf = LOGS_DIR / f"{safe_bot_id(bot_id)}.log"
    if lf.exists():
        lf.unlink()
    return jsonify({"ok": True})

@app.route("/api/bots/<bot_id>/config", methods=["GET", "PUT"])  # ← CORS 프리플라이트 용 OPTIONS는 Flask가 자동 처리
def bot_config(bot_id):
    if request.method == "GET":
        data = read_bot_config(bot_id)
        if not data:
            write_bot_config(bot_id, default_config(bot_id))
            data = read_bot_config(bot_id)
        return jsonify(data)

    raw = request.get_json(force=True, silent=True) or {}
    name = str(raw.get("name", "")).strip() or bot_id
    data = {
        "id": bot_id,
        "name": name,
        "symbol": raw.get("symbol", "ETH-USDT"),
        "side": "SELL" if str(raw.get("side", "BUY")).upper() == "SELL" else "BUY",
        "margin_mode": "ISOLATED" if str(raw.get("margin_mode","CROSS")).upper() == "ISOLATED" else "CROSS",
        "leverage": int(raw.get("leverage", 10)),
        "tp_percent": float(raw.get("tp_percent", 0.5)),
        "repeat_mode": bool(raw.get("repeat_mode", False)),
        "dca_config": [],
    }
    for item in raw.get("dca_config", []):
        try:
            gap, usdt = float(item[0]), float(item[1])
            data["dca_config"].append([gap, usdt])
        except Exception:
            continue

    # 파일만 저장
    write_bot_config(bot_id, data)

    # 메모리에 로드(Runner가 이미 있으면 설정 반영만)
    bot = get_or_create_bot(bot_id)
    cfg = bot["cfg"]; state = bot["state"]
    load_cfg_into_obj(cfg, data)
    state.repeat_mode = data["repeat_mode"]

    # 거래소 적용은 비동기로 예약(응답 지연 방지)
    if APPLY_ON_SAVE and not SKIP_SETUP:
        _EXEC.submit(_apply_exchange_settings_async, cfg)

    return jsonify({"ok": True, "cfg": data, "apply_scheduled": bool(APPLY_ON_SAVE and not SKIP_SETUP)})

@app.post("/api/bots/<bot_id>/start")
def start_bot(bot_id):
    bot = get_or_create_bot(bot_id)
    state = bot["state"]
    if state.running:
        return jsonify({"ok": False, "msg": "already running"}), 400
    bot["runner"].start()
    return jsonify({"ok": True})

@app.post("/api/bots/<bot_id>/stop")
def stop_bot(bot_id):
    bot = get_or_create_bot(bot_id)
    bot["runner"].stop()
    return jsonify({"ok": True})

@app.get("/api/bots/<bot_id>/status")
def status_bot(bot_id):
    bot = get_or_create_bot(bot_id)
    cfg = bot["cfg"]; state = bot["state"]

    now = _time.time()
    cache = STATUS_CACHE.get(bot_id)
    # 캐시가 아주 신선하면 바로 리턴
    if cache and (now - cache["ts"] <= STATUS_TTL):
        return jsonify(cache["data"])

    # 최신값 시도(병렬 + 타임아웃)
    data = _get_status_live(cfg, state)
    # 캐시 업데이트
    STATUS_CACHE[bot_id] = {"ts": now, "data": data}
    return jsonify(data)

# ───────────────────────────────────────────────────────────────────────────────
# 12) 디버그 & 로그인
# ───────────────────────────────────────────────────────────────────────────────
@app.get("/debug/balance")
def debug_balance():
    try:
        val = client.get_available_usdt()
        return jsonify({"ok": True, "available_usdt": val})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/debug/balance/raw")
def debug_balance_raw():
    try:
        url = f"{BASE}/openApi/swap/v2/user/balance"
        j = _req_get(url, {"recvWindow": 60000, "timestamp": _ts()}, signed=True)
        return jsonify({"ok": True, "json": j})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.post("/api/auth/login")
def login():
    data = request.get_json() or {}
    email = data.get("id") or data.get("email")
    password = data.get("password")
    if not email or not password or USERS.get(email) != password:
        return jsonify({"error": "Invalid credentials"}), 401

    token = jwt.encode({
        "sub": email,
        "exp": datetime.utcnow() + timedelta(hours=1)
    }, app.config['SECRET_KEY'], algorithm="HS256")
    return jsonify({"token": token})

# ───────────────────────────────────────────────────────────────────────────────
# 13) 엔트리포인트
# ───────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(debug=True)
