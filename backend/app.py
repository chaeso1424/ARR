# app.py — A안(봇별 로그) 정리본
import os
from pathlib import Path
from dotenv import load_dotenv



# ───────────────────────────────────────────────────────────────────────────────
# 0) 환경 로드
# ───────────────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
load_dotenv()  # 현재 작업 디렉토리 기준

import json, math
import re
import jwt
import datetime as dt
import threading
import time
from collections import deque
from functools import wraps
from flask import Flask, request, jsonify, Response, stream_with_context
from datetime import datetime, timedelta, timezone
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
# 1) 경로/상수
# ───────────────────────────────────────────────────────────────────────────────
BOTS_DIR = BASE_DIR / "data" / "bots"
BOTS_DIR.mkdir(parents=True, exist_ok=True)

LOGS_DIR = BASE_DIR /"logs"              # backend/logs/{bot}.log
LOGS_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_LOG_FILE = BASE_DIR / "logs.txt"  # 공용 로그(옵션)

ANSI_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")      # ANSI 컬러 제거

APPLY_ON_SAVE = os.getenv("APPLY_ON_SAVE", "true").lower() == "true"
SKIP_SETUP   = os.getenv("SKIP_SETUP", "false").lower() == "true"
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
# 파일 상단에 추가 (DEFAULT_SUMMARY 아래쯤)
LAST_SUMMARY = None

def _summary_from_snap(snap):
    """snap(dict|None)을 안전하게 요약 dict로 변환하고, 숫자는 float로 정규화"""
    d = snap if isinstance(snap, dict) else {}
    return {
        "currency": d.get("asset", "USDT"),
        "balance": float(d.get("balance", 0) or 0),
        "equity": float(d.get("equity", 0) or 0),
        "available_margin": float(d.get("available_margin", 0) or 0),
    }


# ───────────────────────────────────────────────────────────────────────────────
# 2) Flask 앱/전역
# ───────────────────────────────────────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET", "dev")

USERS = {
    "artcokr12345@naver.com": "16430878a!!",
}

client = BingXClient()
BOTS: dict[str, dict] = {}  # { bot_id: {"cfg": BotConfig, "state": BotState, "runner": BotRunner} }

# ───────────────────────────────────────────────────────────────────────────────
# 3) 인증(토큰)
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

# ✅ CORS 설정: 프론트 dev 도메인 허용 + Authorization 헤더 허용
CORS(
    app,
    resources={
        r"/api/*": {"origins": ["http://localhost:3000", "http://127.0.0.1:3000"]},
        r"/logs/*": {"origins": ["http://localhost:3000", "http://127.0.0.1:3000"]},
    },
    supports_credentials=False,  # 토큰은 Authorization 헤더로 보내므로 보통 False
    allow_headers=["Content-Type", "Authorization"],
    expose_headers=["Content-Type"],
    methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
)


# ───────────────────────────────────────────────────────────────────────────────
# 4) 봇 설정 저장/로드 유틸
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
# 5) 로그 tail 유틸 (A안: 파일 분리)
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
# 7) 주별 잔고 시리즈
# ───────────────────────────────────────────────────────────────────────────────

def _seconds_until_next_utc_midnight():
    now = datetime.now(timezone.utc)
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(1, int((tomorrow - now).total_seconds()))

def _daily_snapshot_worker():
    # 주의: gunicorn workers를 1개로 유지 (네가 이미 -w 1 사용)
    while True:
        try:
            snap = client._get_snapshot_cached(min_ttl=0.0)
            summ = _summary_from_snap(snap)  # None이어도 안전
            bal = summ["balance"]
            upsert_daily_snapshot(bal)
            upsert_weekly_snapshot(bal)
            print("[snapshot] daily+weekly saved", bal)
        except Exception as e:
            print("[snapshot] failed:", e)
        time.sleep(_seconds_until_next_utc_midnight())

def start_snapshot_daemon_once():
    # 중복 실행 방지 (단일 프로세스 가정: -w 1)
    if ENABLE_SNAPSHOT_DAEMON and not getattr(start_snapshot_daemon_once, "_started", False):
        t = threading.Thread(target=_daily_snapshot_worker, daemon=True)
        t.start()
        start_snapshot_daemon_once._started = True

# Flask 앱 생성 직후 어딘가에서 호출
start_snapshot_daemon_once()

@app.get("/api/balance/series")
def api_balance_series():
    gran = request.args.get("granularity", "w")  # 'd' or 'w'
    if gran == "d":
        days = request.args.get("days", "30")
        try: days = max(1, int(days))
        except: days = 30
        return jsonify({"granularity": "d", "series": get_daily_series(days)})
    else:
        weeks = request.args.get("weeks", "12")
        try: weeks = max(1, int(weeks))
        except: weeks = 12
        return jsonify({"granularity": "w", "series": get_weekly_series(weeks)})
    
# ───────────────────────────────────────────────────────────────────────────────
# 6) “총 거래량 + 체결건수 + (신규 진입 vs DCA) 도넛” / profit chart
# ───────────────────────────────────────────────────────────────────────────────

@app.get("/api/trades/summary")
def api_trades_summary():
    """
    /api/trades/summary?window=7d
    프론트 호환 스키마로 반환:
    {
      "total_volume": 0,
      "fills_count": 0,
      "entries": {"new": 0, "dca": 0}
    }
    """
    window = (request.args.get("window") or "30d").lower()  # '7d' | '30d' | '4w'
    # 필요하면 symbols 필터도 지원 가능: symbols=BTC-USDT,ETH-USDT
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
    """
    /api/stats?window=7d&symbols=BTC-USDT,ETH-USDT
    - window: 7d / 30d / 4w (기본 30d)
    - symbols: 콤마로 구분된 심볼 목록(옵션)
    """
    window = (request.args.get("window") or "30d").lower()
    symbols_param = request.args.get("symbols")
    symbols = [s.strip() for s in symbols_param.split(",")] if symbols_param else None

    # 기간 지정 시 윈도우 합산 사용
    res = get_stats_window(window=window, symbols=symbols)
    return jsonify(res)

@app.post("/api/stats/reset")
def api_stats_reset():
    reset_stats()
    return jsonify({"ok": True})

@app.get("/api/profit/summary")
def api_profit_summary():
    # /api/profit/summary?window=30d&baseline=10000&symbols=BTC-USDT,ETH-USDT
    window = request.args.get("window", "30d")
    baseline = request.args.get("baseline")
    baseline_usdt = float(baseline) if baseline is not None else None

    symbols_param = request.args.get("symbols")
    symbols = [s.strip() for s in symbols_param.split(",")] if symbols_param else None

    agg = get_stats_window(window=window, symbols=symbols)  # { totals/by_symbol/fills_count/ ... }
    pnl_usdt = 0.0
    wins = losses = 0

    # TP 이벤트 합계(pnl), 승/패 카운트
    # utils/stats.py의 events 내부에 pnl 저장해두는 설계여서, window 필터링된 집합으로 합산
    data_full = get_stats_window(window=window, symbols=symbols)  # 동일 윈도우 이벤트 가져오기
    # 위 함수가 events를 안 내려주면, 별도로 utils에서 window events를 돌려주는 helper 하나 두어도 좋음

    # 간단 예: utils에 window events getter가 없다면 get_stats() 후 수동 필터
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
        "pnl_pct": pnl_pct,           # 프론트가 쓰면 %로, 안 쓰면 무시
        "wins": wins,
        "losses": losses
    })

@app.get("/api/profit/kpi")
def api_profit_kpi():
    # /api/profit/kpi?baseline=10000&symbols=BTC-USDT,ETH-USDT
    baseline = request.args.get("baseline")
    baseline_usdt = float(baseline) if baseline is not None else None

    symbols_param = request.args.get("symbols")
    symbols = [s.strip() for s in symbols_param.split(",")] if symbols_param else None

    data = get_profit_kpi(baseline_usdt=baseline_usdt, symbols=symbols)
    return jsonify(data)

# ───────────────────────────────────────────────────────────────────────────────
# 6) 계정 요약(SSE/스냅샷)
# ───────────────────────────────────────────────────────────────────────────────
@app.get("/api/account/summary")
def account_summary():
    global LAST_SUMMARY
    try:
        snap = client._get_snapshot_cached(min_ttl=5.0)
        res = _summary_from_snap(snap)

        # 주간 스냅샷 저장 (여기서도 None 안전)
        try:
            upsert_weekly_snapshot(res["balance"])
        except Exception as e:
            print("weekly snapshot save failed:", e)

        # 성공값은 캐시에 보관
        LAST_SUMMARY = res
        return jsonify(res), 200

    except Exception as e:
        # 예외가 나도 마지막 성공값 또는 기본값으로 200 응답
        if LAST_SUMMARY:
            return jsonify(LAST_SUMMARY), 200
        return jsonify(_summary_from_snap(None)), 200


@app.get("/api/account/summary/stream")
def account_summary_stream():
    @stream_with_context
    def gen():
        global LAST_SUMMARY
        yield "retry: 10000\n\n"
        last = None
        while True:
            try:
                snap = client._get_snapshot_cached(min_ttl=5.0)
                res = _summary_from_snap(snap)
                if res != last:
                    LAST_SUMMARY = res
                    yield f"data: {json.dumps(res)}\n\n"
                    last = res
                else:
                    yield ": keep-alive\n\n"
            except Exception as e:
                # 에러 시에도 끊지 말고 마지막 값 또는 기본값으로 유지
                yield f": err {str(e)}\n\n"
                fallback = LAST_SUMMARY or _summary_from_snap(None)
                yield f"data: {json.dumps(fallback)}\n\n"
            time.sleep(10)

    return Response(gen(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"})


# ───────────────────────────────────────────────────────────────────────────────
# 7) 로그 API (A안: 봇별 파일)
# ───────────────────────────────────────────────────────────────────────────────
@app.get("/api/logs")
def logs_json():
    """
    ?tail=500&bot=BOT_ID&noansi=1&grep=...&level=...
    - bot이 있으면 backend/{bot}.log 읽기
    - bot이 없으면 backend/logs.txt
    - 대상 파일이 아예 없으면 [] 반환 (⚠️ 기본 파일로 대체하지 않음)
    """
    tail   = request.args.get("tail", "500")
    bot    = request.args.get("bot")
    grep   = request.args.get("grep")
    level  = request.args.get("level")
    noansi = request.args.get("noansi", "1") != "0"

    # 파일 경로 결정
    if bot:
        path = LOGS_DIR / f"{safe_bot_id(bot)}.log"
    else:
        path = DEFAULT_LOG_FILE

    # 파일 없으면 빈 배열 리턴
    if not path.exists():
        return jsonify([]), 200, NO_CACHE_HEADERS

    lines = _tail_log_lines(path, tail, grep=grep, level=level, strip_ansi=noansi)
    return jsonify(lines), 200, NO_CACHE_HEADERS


@app.get("/logs/text")
def logs_text():
    tail   = request.args.get("tail", "500")
    bot    = request.args.get("bot")
    grep   = request.args.get("grep")
    level  = request.args.get("level")
    noansi = request.args.get("noansi", "1") != "0"

    path = (LOGS_DIR / f"{safe_bot_id(bot)}.log") if bot else DEFAULT_LOG_FILE
    if not path.exists():
        # 텍스트는 빈 본문 반환
        return Response("", mimetype="text/plain", headers=NO_CACHE_HEADERS)

    lines = _tail_log_lines(path, tail, grep=grep, level=level, strip_ansi=noansi)
    body = "\n".join(lines)
    return Response(body, mimetype="text/plain", headers=NO_CACHE_HEADERS)

# ───────────────────────────────────────────────────────────────────────────────
# 8) 다중 봇 API
# ───────────────────────────────────────────────────────────────────────────────
@app.get("/api/bots")
def list_bots():
    out = []
    for f in BOTS_DIR.glob("*.json"):
        bot_id = f.stem
        cfg_data = read_bot_config(bot_id) or default_config(bot_id)
        running = False
        if bot_id in BOTS:
            running = BOTS[bot_id]["state"].running
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
    if not out:
        write_bot_config(DEFAULT_BOT_ID, default_config(DEFAULT_BOT_ID))
        get_or_create_bot(DEFAULT_BOT_ID)
        out = [{
            "id": DEFAULT_BOT_ID, "name": DEFAULT_BOT_ID, "symbol": "ETH-USDT", "side": "BUY",
            "margin_mode": "CROSS", "leverage": 10, "status": "stopped", "running": False
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

@app.route("/api/bots/<bot_id>/config", methods=["GET", "PUT"])
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
        "margin_mode": "ISOLATED" if str(raw.get("margin_mode", "CROSS")).upper() == "ISOLATED" else "CROSS",
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

    write_bot_config(bot_id, data)
    bot = get_or_create_bot(bot_id)
    cfg = bot["cfg"]; state = bot["state"]
    load_cfg_into_obj(cfg, data)
    state.repeat_mode = data["repeat_mode"]

    if APPLY_ON_SAVE and not SKIP_SETUP:
        try:
            client.set_margin_mode(cfg.symbol, cfg.margin_mode)
            client.set_leverage(cfg.symbol, cfg.leverage)
        except Exception as e:
            log(f"⚠️ config 적용 실패({bot_id}): {e}")

    return jsonify({"ok": True, "cfg": data})

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

    try:    pp, qp = client.get_symbol_filters(cfg.symbol)
    except: pp, qp = 4, 0
    try:    mark = float(client.get_mark_price(cfg.symbol))
    except: mark = None

    avg, qty = client.position_info(cfg.symbol, cfg.side)
    exch_lev = client.get_current_leverage(cfg.symbol, cfg.side)

    out = {
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
    return jsonify(out)

# ───────────────────────────────────────────────────────────────────────────────
# 9) 디버그 & 로그인
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
# 10) 엔트리포인트
# ───────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(debug=True)

