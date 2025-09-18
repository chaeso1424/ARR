# services/bingx_client.py
import os, random
import time, re, threading, logging
import hmac
import hashlib
import requests
import uuid, json
import datetime
from typing import Optional
import urllib.parse
from urllib.parse import urlencode
from utils.logging import log
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from decimal import Decimal, InvalidOperation


import os
# --- force IPv4 only for urllib3/requests ---
import socket
try:
    import urllib3.util.connection as urllib3_cn
    urllib3_cn.allowed_gai_family = lambda: socket.AF_INET
except Exception:
    pass

SKIP_SETUP = os.getenv("SKIP_SETUP", "false").lower() == "true"

API_KEY = os.getenv("BINGX_API_KEY", "")
API_SECRET = os.getenv("BINGX_API_SECRET", "")
BASE = os.getenv("BINGX_BASE", "https://open-api.bingx.com")
POSITION_MODE = os.getenv("BINGX_POSITION_MODE", "HEDGE").upper()  # HEDGE or ONEWAY


# ---- 타임아웃/세션/재시도 기본값 ----
CONNECT_TIMEOUT = int(os.getenv("BINGX_CONNECT_TIMEOUT", "10"))   # 5 -> 10
READ_TIMEOUT    = int(os.getenv("BINGX_READ_TIMEOUT", "15"))      # 5 -> 15
TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

# 전역 세션 (커넥션 풀/keep-alive)
_session = requests.Session()
# HTTPAdapter의 내장 Retry는 status 코드 재시도만 다루므로,
# 우리는 네트워크 계열은 수동 지수백오프로 처리하되, 429/5xx만 어댑터 레벨에서 한번 더 안전망.
_retry = Retry(
    total=0,  # 여기선 0. (아래 수동 재시도 사용)
    backoff_factor=0,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=False,
)
_adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=_retry)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)

# ---- 서버 시간 오프셋(ms) ----
_SERVER_OFFSET_MS = 0
# 선물/스왑/스팟 후보 엔드포인트 (그대로 유지 가능)
_SERVER_TIME_PATHS = [
    "/openApi/swap/v2/server/time",
    "/openApi/common/server/time",
    "/openApi/spot/v1/common/time",
]

# 동기화 스레드 관리용 전역
_SERVER_OFFSET_MS = 0
_TIME_SYNC_LOCK = threading.Lock()
_TIME_SYNC_THREAD: Optional[threading.Thread] = None
_TIME_SYNC_STOP = threading.Event()

# === Resilience settings ===
# 재시도 횟수 / 간격은 환경변수로 조절 가능
MAX_RETRIES = int(os.getenv("BINGX_MAX_RETRIES", "3"))
RETRY_DELAY = float(os.getenv("BINGX_RETRY_DELAY", "30"))

def _now_ms():
    import time as _t
    return int(_t.time() * 1000)

def _should_retry(err: Exception) -> bool:
    s = str(err).lower()
    hints = (
        "timed out",
        "timeout",
        "temporarily unavailable",
        "service temporarily unavailable",
        "bad gateway",
        "gateway time-out",
        "connection aborted",
        "connection reset",
        "read timed out",
        "max retries",
        "eof occurred in violation",
        "remote end closed connection",
        "tlsv1 alert",
        "name or service not known",
        "failed to establish a new connection",
    )
    return any(h in s for h in hints)

# 서버 시간 동기화 로직
def _sync_server_time_once():
    """여러 경로를 순차 시도해서 서버 타임(ms)과 로컬 차이를 보정."""
    global _SERVER_OFFSET_MS
    last_err = None
    for path in _SERVER_TIME_PATHS:
        url = f"{BASE}{path}"
        try:
            r = _session.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            j = r.json()
            # 응답 포맷이 다를 수 있어 보편적으로 탐색
            server_ms = None
            for k in ("serverTime", "timestamp", "time", "server_ts"):
                v = j.get("data", {}).get(k) if isinstance(j.get("data"), dict) else j.get(k)
                if v is not None:
                    server_ms = int(v)
                    break
            if server_ms is None:
                # 혹시 문자열로 들어오는 경우 대비
                txt = "".join(ch for ch in r.text if ch.isdigit())
                if len(txt) >= 10:
                    server_ms = int(txt[:13]) if len(txt) >= 13 else int(txt) * 1000
            if server_ms:
                local_ms = int(time.time() * 1000)
                _SERVER_OFFSET_MS = server_ms - local_ms
                return True
        except Exception as e:
            last_err = e
            continue
    # 실패해도 치명적이진 않음(로컬 시간 사용). 로그만 남김.
    try:
        log(f"⚠️ server time sync failed (fallback to local): {last_err}")
    except Exception:
        pass
    return False

def server_time_ms() -> int:
    return int(time.time() * 1000 + _SERVER_OFFSET_MS)

def server_offset_ms() -> int:
    return _SERVER_OFFSET_MS

def _sync_server_time_once() -> bool:
    """
    기존에 쓰던 구현 유지. 실패 시 예외 던지지 말고 False 반환 + log만.
    - 내부에서 BASE, _session, TIMEOUT, _SERVER_OFFSET_MS 사용
    """
    # ← 당신이 기존에 쓰던 안전한 버전(예외 삼킴) 그대로 두세요.
    ...

def start_server_time_sync(interval_sec: int = 600, jitter_sec: int = 0) -> bool:
    """이미 실행 중이면 False, 새로 시작하면 True 반환."""
    global _TIME_SYNC_THREAD
    with _TIME_SYNC_LOCK:
        if _TIME_SYNC_THREAD and _TIME_SYNC_THREAD.is_alive():
            return False

        _TIME_SYNC_STOP.clear()

        def _loop():
            try:
                if jitter_sec > 0:
                    time.sleep(random.uniform(0, float(jitter_sec)))
                _sync_server_time_once()
                while not _TIME_SYNC_STOP.wait(interval_sec):
                    try:
                        _sync_server_time_once()
                    except Exception as e:
                        try:
                            log(f"⚠️ server time sync error: {e}")
                        except Exception:
                            pass
            finally:
                # 스레드 종료 시 포인터 정리
                with _TIME_SYNC_LOCK:
                    if threading.current_thread() is _TIME_SYNC_THREAD:
                        _TIME_SYNC_THREAD = None

        t = threading.Thread(target=_loop, name="bingx-time-sync", daemon=True)
        t.start()
        _TIME_SYNC_THREAD = t
        return True

def stop_server_time_sync(join_timeout: float | None = 1.0):
    _TIME_SYNC_STOP.set()
    with _TIME_SYNC_LOCK:
        t = _TIME_SYNC_THREAD
    if t and t.is_alive():
        try:
            t.join(join_timeout)
        except Exception:
            pass

# ---------- low-level utils ----------
def _ts():
    """서버 오프셋 보정된 epoch(ms)"""
    return int(time.time() * 1000) + _SERVER_OFFSET_MS

def _headers(form: bool = False):
    if not API_KEY:
        raise RuntimeError("BINGX_API_KEY is empty. Check your .env and app load order.")
    h = {"X-BX-APIKEY": API_KEY}
    h["Content-Type"] = "application/x-www-form-urlencoded" if form else "application/json"
    return h

def _sign(params: dict) -> str:
    """
    파라미터 키를 정렬한 쿼리스트링 + HMAC-SHA256 서명 문자열을 반환.
    """
    # 키 정렬로 라이브러리별 인코딩/순서 차이 방지
    qs = "&".join(f"{k}={params[k]}" for k in sorted(params))
    sig = hmac.new(API_SECRET.encode("utf-8"), qs.encode("utf-8"), hashlib.sha256).hexdigest()
    return qs + "&signature=" + sig

# --- normalize payload for signed requests (bool -> "true"/"false") ---
def _coerce_params(d: dict | None) -> dict:
    out = {}
    for k, v in (d or {}).items():
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif v is None:
            continue
        else:
            out[k] = v  
    return out

def _exp_backoff_sleep(attempt: int, base: float = 0.5, cap: float = 15.0):
    # 0.5, 1.0, 2.0, 4.0 ... + 지터
    dur = min(cap, base * (2 ** max(attempt - 1, 0))) + random.random() * 0.4
    time.sleep(dur)


def _req_get(url: str, params: dict | None = None, signed: bool = False) -> dict:
    params = params or {}
    # 서명 시 recvWindow/timestamp 채움
    if signed:
        p = _coerce_params(params)
        p["recvWindow"] = p.get("recvWindow", 60000)
        p["timestamp"]  = _ts()
        payload = _sign(p)            # 정렬된 쿼리 + signature
        send_url = f"{url}?{payload}" # 전송 문자열 == 서명 문자열
        data = None
        headers = _headers(form=False)
    else:
        send_url = url
        data = None
        headers = _headers(form=False)

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if signed:
                r = _session.get(send_url, timeout=TIMEOUT, headers=headers)
            else:
                r = _session.get(send_url, params=params, timeout=TIMEOUT, headers=headers)
            # 일부 엔드포인트는 200이지만 code != 0일 수 있음
            j = r.json()
            code = str(j.get("code", "0"))
            if code != "0":
                # 레이트리밋 메시지에 해제시각이 들어올 수 있으므로 그대로 띄움
                raise RuntimeError(f"BingX error @GET {url}: {code} {j.get('msg') or j}")
            return j
        except Exception as e:
            last_err = e
            if attempt < MAX_RETRIES and _should_retry(e):
                try:
                    log(f"⚠️ _req_get retry {attempt}/{MAX_RETRIES-1}: {e}")
                except Exception:
                    pass
                _exp_backoff_sleep(attempt)
                continue
            break
    raise last_err

def _req_post(url: str, body: dict | None = None, signed: bool = False) -> dict:
    body = body or {}
    if signed:
        b = _coerce_params(body)
        b["recvWindow"] = b.get("recvWindow", 60000)
        b["timestamp"]  = _ts()
        payload = _sign(b)   # form-urlencoded
        headers = _headers(form=True)
    else:
        payload = None
        headers = _headers(form=False)

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if signed:
                r = _session.post(url, data=payload, timeout=TIMEOUT, headers=headers)
            else:
                r = _session.post(url, json=body, timeout=TIMEOUT, headers=headers)
            j = r.json()
            code = str(j.get("code", "0"))
            if code != "0":
                raise RuntimeError(f"BingX error @POST {url}: {code} {j.get('msg')}")
            return j
        except Exception as e:
            last_err = e
            if attempt < MAX_RETRIES and _should_retry(e):
                try:
                    log(f"⚠️ _req_post retry {attempt}/{MAX_RETRIES-1}: {e}")
                except Exception:
                    pass
                _exp_backoff_sleep(attempt)
                continue
            break
    raise last_err

def _req_delete(url: str, params: dict | None = None, signed: bool = False) -> dict:
    params = params or {}
    if signed:
        p = _coerce_params(params)
        p["recvWindow"] = p.get("recvWindow", 60000)
        p["timestamp"]  = _ts()
        payload = _sign(p)
        send_url = f"{url}?{payload}"
        headers = _headers(form=True)
    else:
        send_url = url
        headers = _headers(form=False)

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if signed:
                r = _session.delete(send_url, timeout=TIMEOUT, headers=headers)
            else:
                r = _session.delete(send_url, params=params, timeout=TIMEOUT, headers=headers)
            j = r.json()
            code = str(j.get("code", "0"))
            if code != "0":
                raise RuntimeError(f"BingX error @DELETE {url}: {code} {j.get('msg')}")
            return j
        except Exception as e:
            last_err = e
            if attempt < MAX_RETRIES and _should_retry(e):
                try:
                    log(f"⚠️ _req_delete retry {attempt}/{MAX_RETRIES-1}: {e}")
                except Exception:
                    pass
                _exp_backoff_sleep(attempt)
                continue
            break
    raise last_err

def _make_cid(tag: str) -> str:
    """
    BingX는 clientOrderId를 모두 소문자로 처리하므로 소문자로만 생성
    tag: "entry", "dca", "tp", "sl" 등
    """
    return f"arr-{tag}-{uuid.uuid4().hex[:8]}"

# ---------- high-level client ----------
class BingXClient:
    def __init__(self):
        self._spec_cache: dict[str, dict] = {}
        self._last_position: dict[tuple[str, str], tuple[float, float]] = {}

        self._snap_cache = None            # 마지막 정상 스냅샷
        self._snap_ts = 0.0                # 스냅샷 시각(epoch sec)
        self._snap_block_until = 0.0       # 레이트리밋 해제 예정 시각(epoch sec)
        self.log = logging.getLogger("bingx.client")
        

        # ✅ 시작 직후 1회 서버타임 동기화(실패해도 무시)
        try:
            _sync_server_time_once()
        except Exception:
            pass
        
        self._start_initial_fetch()

    def get_current_leverage(self, symbol: str, side: str) -> float | None:
        """
        Return the current applied leverage for the given symbol/side.
        HEDGE mode will query positionSide separately.
        """
        url = f"{BASE}/openApi/swap/v2/user/positions"
        j = _req_get(url, {"symbol": symbol, "recvWindow": 60000, "timestamp": _ts()}, signed=True)
        arr = j.get("data", [])
        want = "LONG" if str(side).upper() == "BUY" else "SHORT"
        for p in arr if isinstance(arr, list) else []:
            if POSITION_MODE == "HEDGE":
                ps = (p.get("positionSide") or p.get("posSide") or p.get("side") or "").upper()
                if ps != want:
                    continue
            lev = p.get("leverage") or p.get("leverageLevel") or p.get("positionLeverage")
            if lev is not None:
                try:
                    return float(lev)
                except Exception:
                    pass
        return None

    #주문 응답에서 orderId 추출
    def _extract_order_id(self, resp: dict) -> str:
        """
        BingX 주문 응답에서 orderId를 최대한 유연하게 추출한다.
        지원 케이스:
        - resp["orderId"] / resp["id"]
        - resp["data"]["orderId"] / resp["data"]["id"]
        - resp["data"]["order"]["orderId"] / ["orderID"] / ["id"]
        """
        if not isinstance(resp, dict):
            return ""
        for k in ("orderId", "orderID", "id"):
            v = resp.get(k)
            if v:
                return str(v)
        d = resp.get("data")
        if isinstance(d, dict):
            for k in ("orderId", "orderID", "id"):
                v = d.get(k)
                if v:
                    return str(v)
            o = d.get("order")
            if isinstance(o, dict):
                for k in ("orderId", "orderID", "id", "order_id"):
                    v = o.get(k)
                    if v:
                        return str(v)
        return ""

    def get_symbol_filters(self, symbol: str) -> tuple[int, int]:
        """
        /quote/contracts에서 symbol의 pricePrecision, quantityPrecision을 얻어온다.
        실패 시 (4, 3) 반환.
        """
        try:
            url = f"{BASE}/openApi/swap/v2/quote/contracts"
            j = _req_get(url)
            data = j.get("data", [])
            for it in data if isinstance(data, list) else []:
                s = it.get("symbol") or it.get("contractCode") or it.get("symbolName")
                if s == symbol:
                    pp = int(it.get("pricePrecision", it.get("pricePrecisionNum", 4)))
                    qp = int(it.get("quantityPrecision", it.get("volPrecision", 3)))
                    return max(pp,0), max(qp,0)
        except Exception as e:
            log(f"⚠️ get_symbol_filters fallback: {e}")
        return 4, 3

    def _round_to_precision(self, value: float, digits: int) -> float:
        if digits < 0: digits = 0
        fmt = f"{{:.{digits}f}}"
        return float(fmt.format(value))

    # BingXClient 클래스 안에 추가
    def get_contract_spec(self, symbol: str) -> dict:
        """
        Read contract spec for a symbol. Returns a dict with contractSize, minQty,
        qtyStep, pricePrecision, quantityPrecision.
        """
        # If cached, return the cached spec
        if symbol in self._spec_cache:
            return self._spec_cache[symbol]
        spec = {
            "contractSize": 1.0,
            "minQty": 0.0,
            "qtyStep": 1.0,
            "pricePrecision": 4,
            "quantityPrecision": 0,
        }
        try:
            url = f"{BASE}/openApi/swap/v2/quote/contracts"
            j = _req_get(url)
            data = j.get("data", [])
            if isinstance(data, list):
                for it in data:
                    s = it.get("symbol") or it.get("contractCode") or it.get("symbolName")
                    if s == symbol:
                        pp = int(it.get("pricePrecision") or it.get("pricePrecisionNum") or 4)
                        qp = int(it.get("quantityPrecision") or it.get("volPrecision") or 0)
                        contract_size = float(it.get("contractSize") or it.get("multiplier") or 1.0)
                        min_qty = float(it.get("minQty") or it.get("minVol") or it.get("minTradeNum") or 0.0)
                        qty_step = it.get("volumeStep") or it.get("stepSize")
                        if qty_step is None:
                            qty_step = 1.0 if qp == 0 else 10 ** (-qp)
                        spec.update({
                            "contractSize": contract_size,
                            "minQty": float(qty_step) if min_qty == 0 else float(min_qty),
                            "qtyStep": float(qty_step),
                            "pricePrecision": pp,
                            "quantityPrecision": qp,
                        })
                        break
        except Exception as e:
            log(f"⚠️ get_contract_spec fallback: {e}")
        # Cache and return
        self._spec_cache[symbol] = spec
        return spec

    def _try_order(self, url: str, variants: list[dict]) -> str:
        """
        variants를 순차 시도. 성공하면 orderId 반환.
        - orderId를 찾았을 때는 어떤 로그도 남기지 않음
        - 못 찾았을 때만 오류 로그 남기고 다음 variant 시도
        """
        import json
        last_err = None

        for i, body in enumerate(variants):
            try:
                j = _req_post(url, body, signed=True)

                # 1) 주 추출 경로
                oid = None
                try:
                    oid = self._extract_order_id(j)
                except Exception:
                    oid = None

                # 2) 폴백(필요 시)
                if not oid:
                    try:
                        o = (j.get("data") or {}).get("order") or {}
                        oid = o.get("orderId") or o.get("orderID") or o.get("id")
                    except Exception:
                        oid = None

                # ✅ 찾았으면 즉시 반환(로그 없음)
                if oid:
                    return str(oid)

                # ❌ 못 찾았으면: code/msg 포함해서 간단 로그 후 다음 variant
                code = (j or {}).get("code")
                msg  = (j or {}).get("msg") or (j or {}).get("message")
                try:
                    preview = json.dumps(j, ensure_ascii=False)[:300]
                except Exception:
                    preview = str(j)[:300]

                log(f"⚠️ order variant#{i+1}: missing orderId (code={code}, msg={msg}) resp={preview}")
                last_err = RuntimeError(f"missing orderId in response (variant#{i+1})")

            except Exception as e:
                last_err = e
                s = str(e)
                if ("80001" in s) or ("insufficient" in s.lower()):
                    log(f"ℹ️ order variant#{i+1} skipped (insufficient): {e}")
                elif "80017" in s or "position not exist" in s.lower():
                    # 🔽 노이즈 줄이기: 실패(⚠️) 대신 정보 로그로
                    log(f"ℹ️ order variant#{i+1} skipped (no position): {e}")
                    # 바로 다음 variant를 시도할 필요도 거의 없음. 보통 호출부에서 정리하므로 break가 맞습니다.
                    break
                else:
                    log(f"⚠️ order variant#{i+1} failed: {e}")
                continue

        # 모든 시도 실패
        raise last_err or RuntimeError("all order variants failed")

    # ----- Market / Quote -----
    def list_symbols(self) -> list[str]:
        """
        거래가능 선물 심볼 목록. 실패시 안전 기본값 반환.
        """
        try:
            url = f"{BASE}/openApi/swap/v2/quote/contracts"
            j = _req_get(url)
            data = j.get("data", [])
            out = []
            for it in data if isinstance(data, list) else []:
                s = it.get("symbol") or it.get("contractCode") or it.get("symbolName")
                if s and s.endswith("USDT"):
                    out.append(s)
            if out:
                return sorted(set(out))
        except Exception as e:
            log(f"⚠️ list_symbols fallback: {e}")
        return ["BTC-USDT", "ETH-USDT", "DOGE-USDT"]

    def get_last_price(self, symbol: str) -> float:
        """최신 체결가"""
        url = f"{BASE}/openApi/swap/v2/quote/price"
        j = _req_get(url, {"symbol": symbol})
        d = j.get("data", {})
        return float(d.get("price"))

    def get_mark_price(self, symbol: str) -> float:
        """마크프라이스 (없을 경우 최신가로 폴백)"""
        try:
            url = f"{BASE}/openApi/swap/v2/quote/premiumIndex"
            j = _req_get(url, {"symbol": symbol})
            d = j.get("data", {})
            for k in ("markPrice", "indexPrice", "price"):
                if k in d:
                    return float(d[k])
        except Exception as e:
            log(f"⚠️ get_mark_price fallback to last price: {e}")
        return self.get_last_price(symbol)
    

    #
    

    # ─────────────────────────────────────────────
    # 1) USDT 항목을 안전하게 찾아주는 헬퍼(FrontEnd)
    # ─────────────────────────────────────────────

    def _start_initial_fetch(self):
        t = threading.Thread(target=self._initial_fetch_loop, daemon=True)
        t.start()

    def _initial_fetch_loop(self):
        """서버 기동 직후 최초 스냅샷을 얻을 때까지 백오프로 재시도."""
        attempt = 0
        while self._snap_cache is None:
            try:
                self.log.info("initial fetch: trying to create first snapshot...")
                self._fetch_user_balance_snapshot_usdt()  # 성공 시 캐시/타임스탬프 갱신
                self.log.info("initial fetch: snapshot obtained.")
                break
            except Exception as e:
                now = time.time()
                # 레이트리밋 해제 시각이 있으면 그때까지 대기
                if self._snap_block_until > now:
                    sleep_for = (self._snap_block_until - now) + 1.0
                else:
                    # 지수 백오프 (2,4,8,16,32,60...)
                    attempt = min(attempt + 1, 5)
                    sleep_for = min(60.0, 2.0 ** attempt)
                time.sleep(sleep_for)

    def _parse_unblock_until_secs(self, msg: str) -> float | None:
        m = re.search(r"unblocked after (\d{13})", str(msg))
        return int(m.group(1))/1000.0 if m else None
    
    def _extract_usdt_obj_from_balance_payload(self, data: dict) -> dict | None:
        if not isinstance(data, (dict, list)):
            self.log.debug("payload 'data' is not dict/list: %r", type(data))
            return None
        cand = data.get("balance", data) if isinstance(data, dict) else data
        if isinstance(cand, dict):
            asset = (cand.get("asset") or "USDT").upper()
            self.log.debug("balance cand=dict asset=%s keys=%s", asset, list(cand.keys()))
            if asset in ("USDT", ""):
                return cand
        elif isinstance(cand, list):
            self.log.debug("balance cand=list len=%d", len(cand))
            for it in cand:
                if isinstance(it, dict) and (it.get("asset") or "").upper() == "USDT":
                    return it
        self.log.warning("USDT object not found in payload")
        return None

    def _fetch_user_balance_snapshot_usdt(self) -> dict:
        url = f"{BASE}/openApi/swap/v2/user/balance"  
        self.log.debug("FETCH %s (block_until=%s now=%s)", url, self._snap_block_until, time.time())
        try:
            j = _req_get(url, {"recvWindow": 60000, "timestamp": _ts()}, signed=True)
            data = j.get("data", {})
            usdt = self._extract_usdt_obj_from_balance_payload(data)
            if not isinstance(usdt, dict):
                raise RuntimeError(f"unexpected /user/balance payload: {j}")

            def f(*keys, default=0.0):
                for k in keys:
                    v = usdt.get(k)
                    if v is not None:
                        try:
                            return float(v)
                        except Exception:
                            pass
                return float(default)

            snap = {
                "asset":             (usdt.get("asset") or "USDT").upper(),
                "balance":           f("balance", "walletBalance"),
                "equity":            f("equity", "totalEquity", "equityUSDT", "walletBalance", "balance"),
                "available_margin":  f("availableMargin", "availableBalance", "available"),
                "realised_profit":   f("realisedProfit", "realizedProfit"),
                "unrealized_profit": f("unrealizedProfit", "unrealizedPnl", "uPnl"),
            }
            self._snap_cache = snap
            self._snap_ts = time.time()
            self.log.debug(
                "SNAPSHOT OK asset=%s bal=%.4f eq=%.4f avail=%.4f",
                snap["asset"], snap["balance"], snap["equity"], snap["available_margin"]
            )
            return snap

        except Exception as e:
            unblock = self._parse_unblock_until_secs(str(e))
            if unblock:
                self._snap_block_until = max(self._snap_block_until, unblock)
                self.log.warning("RATE LIMITED (100410), unblock_at=%s",
                                 time.strftime("%H:%M:%S", time.localtime(self._snap_block_until)))
            else:
                self._snap_block_until = max(self._snap_block_until, time.time() + 30)
            raise

    def _get_snapshot_cached(self, min_ttl: float = 10.0) -> dict | None:
        now = time.time()
        if now < self._snap_block_until:
            self.log.debug("using cache (rate-limited until %s)",
                           time.strftime("%H:%M:%S", time.localtime(self._snap_block_until)))
            return self._snap_cache

        if self._snap_cache is not None and (now - self._snap_ts) < min_ttl:
            self.log.debug("using fresh cache (age=%.1fs < ttl=%.1fs)", now - self._snap_ts, min_ttl)
            return self._snap_cache

        try:
            self.log.debug("cache stale or empty -> refetch")
            return self._fetch_user_balance_snapshot_usdt()
        except Exception as e:
            self.log.warning("refetch failed: %s; keep last snapshot", e)
            return self._snap_cache

    
    def get_accountbalance_usdt(self) -> float:
        snap = self._get_snapshot_cached(min_ttl=5.0)
        return float(snap.get("balance", 0.0))

    def get_equity_usdt(self) -> float:
        snap = self._get_snapshot_cached(min_ttl=5.0)
        return float(snap.get("equity", 0.0))

    def get_available_usdt(self) -> float:
        snap = self._get_snapshot_cached(min_ttl=5.0)
        return float(snap.get("available_margin", 0.0))




    # ----- Settings (Margin mode / Leverage) -----
    def set_margin_mode(self, symbol: str, mode: str):
        """마진 모드 설정 - UI: cross/isolated - API: CROSSED/ISOLATED"""
        m_primary = "CROSSED" if mode.upper().startswith("CROSS") else "ISOLATED"
        url = f"{BASE}/openApi/swap/v2/trade/marginType"
        body = {
            "symbol": symbol,
            "marginType": m_primary,   # ✅ BingX API가 요구하는 필드
            "recvWindow": 60000,
            "timestamp": _ts(),
        }

        if SKIP_SETUP:
            log("ℹ️ SKIP_SETUP=TRUE → set_margin_mode 생략")
            return

        try:
            _req_post(url, body, signed=True)
            log(f"ℹ️ margin mode set: {symbol} → {m_primary}")
        except Exception as e:
            log(f"⚠️ set_margin_mode failed: {e}")


    def set_leverage(self, symbol: str, leverage: int):
        """레버리지 설정 - HEDGE 모드: LONG/SHORT 각각 호출 - ONEWAY 모드: LONG만 시도"""
        url = f"{BASE}/openApi/swap/v2/trade/leverage"
        sides = ["LONG", "SHORT"] if POSITION_MODE == "HEDGE" else ["LONG"]
        for s in sides:
            body1 = {
                "symbol": symbol,
                "side": s,
                "leverage": int(leverage),
                "recvWindow": 60000,
                "timestamp": _ts(),
            }
            try:
                _req_post(url, body1, signed=True)
                continue
            except Exception as e1:
                log(f"⚠️ set_leverage(primary,{s}) failed: {e1}")
            body2 = {
                "symbol": symbol,
                "positionSide": s,
                "leverage": int(leverage),
                "recvWindow": 60000,
                "timestamp": _ts(),
            }
            try:
                _req_post(url, body2, signed=True)
            except Exception as e2:
                log(f"⚠️ set_leverage(alt,{s}) failed: {e2}")
            if SKIP_SETUP:
                log("ℹ️ SKIP_SETUP=TRUE → set_leverage 생략")
                return
            url = f"{BASE}/openApi/swap/v2/trade/leverage"
            sides = ["LONG", "SHORT"] if POSITION_MODE == "HEDGE" else ["LONG"]
            for s in sides:
                body1 = {"symbol": symbol, "side": s, "leverage": int(leverage), "recvWindow": 60000, "timestamp": _ts()}
                try:
                    _req_post(url, body1, signed=True)
                    continue
                except Exception as e1:
                    log(f"⚠️ set_leverage(primary,{s},{url}) failed: {e1}")
                body2 = {"symbol": symbol, "positionSide": s, "leverage": int(leverage), "recvWindow": 60000, "timestamp": _ts()}
                try:
                    _req_post(url, body2, signed=True)
                except Exception as e2:
                    log(f"⚠️ set_leverage(alt,{s},{url}) failed: {e2}")

    # ----- Orders / Positions -----


    def place_market(self, symbol: str, side: str, qty: float,
                    reduce_only: bool=False, position_side: str|None=None,
                    close_position: bool=False) -> str:
        url = f"{BASE}/openApi/swap/v2/trade/order"

        # --- 수량 검증(그대로 사용) ---
        try:
            q = float(qty)
        except Exception:
            raise RuntimeError("quantity must be a number")
        if q <= 0:
            raise RuntimeError("quantity must be > 0")

        base = {
            "symbol": symbol,
            "type": "MARKET",
            "side": side.upper(),
            "quantity": q,                # ← 들어온 값 그대로 사용
            "recvWindow": 60000,
            "timestamp": _ts(),
            "clientOrderId": _make_cid("entry"),
        }

        if POSITION_MODE == "HEDGE":
            ps = (position_side or ("LONG" if side.upper() == "BUY" else "SHORT")).upper()
            base["positionSide"] = ps
        else:
            if reduce_only:
                base["reduceOnly"] = True

        # close 포지션용 변형(필요시)
        variants = []
        if close_position:
            v = dict(base)
            v["closePosition"] = "true"   # 문자열
            variants.append(v)
        variants.append(base)

        time.sleep(0.5)
        return self._try_order(url, variants)
    

    def place_limit(self, symbol: str, side: str, qty: float, price: float,
                    reduce_only: bool=False, position_side: str|None=None,
                    tif: str="GTC", close_position: bool=False) -> str:
        url = f"{BASE}/openApi/swap/v2/trade/order"

        # --- 간단 검증만 (보정 없음) ---
        try:
            q = float(qty)
        except Exception:
            raise RuntimeError("quantity must be a number")
        if q <= 0:
            raise RuntimeError("quantity must be > 0 (limit)")

        try:
            p = float(price)
        except Exception:
            raise RuntimeError("price must be a number")
        if p <= 0:
            raise RuntimeError("price must be > 0 (limit)")

        base = {
            "symbol": symbol,
            "type": "LIMIT",
            "side": side.upper(),
            "quantity": q,
            "price": p,
            "timeInForce": tif,
            "recvWindow": 60000,
            "timestamp": _ts(),
            "clientOrderId": _make_cid("dca"),
        }

        if POSITION_MODE == "HEDGE":
            ps = (position_side or ("LONG" if side.upper() == "BUY" else "SHORT")).upper()
            base["positionSide"] = ps
        else:
            if reduce_only:
                base["reduceOnly"] = True

        variants = []
        if close_position:
            v = dict(base)
            v["closePosition"] = "true"   # 문자열
            variants.append(v)
        variants.append(base)

        time.sleep(0.5)
        return self._try_order(url, variants)


    def place_tp_market(self, symbol: str,
                        side: str|None,                 # 포지션 반대 방향(없으면 position_side로 자동결정)
                        stop_price: float,              # 트리거 가격 (필수)
                        position_side: str|None=None,   # HEDGE 모드에서 LONG/SHORT 지정
                        ) -> str:
        """
        TAKE_PROFIT_MARKET 조건부 시장가 익절 주문.
        항상 전체 청산(closePosition=true)으로만 전송합니다.
        """
        import time

        # stop_price 간단 검증만 (정밀도/라운딩 없음)
        try:
            sp = float(stop_price)
        except Exception:
            raise RuntimeError("stop_price must be a number")
        if sp <= 0:
            raise RuntimeError("stop_price must be > 0 for TAKE_PROFIT_MARKET")

        url = f"{BASE}/openApi/swap/v2/trade/order"

        base = {
            "symbol": symbol,
            "type": "TAKE_PROFIT_MARKET",
            "stopPrice": sp,         # 들어온 값 그대로 사용
            "recvWindow": 60000,
            "timestamp": _ts(),
            "clientOrderId": _make_cid("tp"),
        }

        # side / positionSide 설정
        if POSITION_MODE == "HEDGE":
            ps = (position_side or "").upper()
            if not ps:
                raise RuntimeError("HEDGE mode requires position_side (LONG/SHORT)")
            base["positionSide"] = ps
            if side is None:
                side = "SELL" if ps == "LONG" else "BUY"
            base["side"] = side.upper()
        else:
            if side is None:
                raise RuntimeError("ONE-WAY mode requires explicit side (BUY/SELL)")
            base["side"] = side.upper()

        # 항상 전체 청산
        v = dict(base)
        v["closePosition"] = "true"   # quantity 미포함
        variants = [v]

        time.sleep(0.5)
        return self._try_order(url, variants)



    def cancel_order(self, symbol: str, order_id: str) -> bool:
        url = f"{BASE}/openApi/swap/v2/trade/order"
        try:
            _req_delete(
                url,
                {"symbol": symbol, "orderId": int(order_id), "recvWindow": 60000, "timestamp": _ts()},
                signed=True,
            )
            return True
        except Exception as e:
            msg = str(e)
            # 이미 취소된 주문 → 조용히 무시
            if any(code in msg for code in ("80018", "109414", "not exist", "does not exist", "unknown order")):
                return False
            # ❗ 레이트리밋/네트워크 오류도 무해하게 처리
            if "100410" in msg or "109501" in msg or "network issue" in msg.lower():
                self.log.warning(f"cancel_order deferred: {e}")
                return False
            # 진짜 심각한 오류만 남김
            log(f"⚠️ cancel_order: {e}")
            return False

        


    def open_orders(self, symbol: str) -> list[dict]:
        """열려있는 주문 목록"""
        import time
        url = f"{BASE}/openApi/swap/v2/trade/openOrders"
        try:
            j = _req_get(url, {"symbol": symbol, "recvWindow": 60000, "timestamp": _ts()}, signed=True)
            data = j.get("data", [])
            return data if isinstance(data, list) else data.get("orders", [])
        except Exception as e:
            s = str(e).lower()
            if "100421" in s or "timestamp" in s:
                # ⏱️ fresh timestamp로 1회 재시도 (대기 추가)
                log("⚠️ open_orders timestamp mismatch → 60초 대기 후 재시도")
                time.sleep(60)  # ✅ 대기 추가
                try:
                    j = _req_get(url, {"symbol": symbol, "recvWindow": 60000, "timestamp": _ts()}, signed=True)
                    data = j.get("data", [])
                    return data if isinstance(data, list) else data.get("orders", [])
                except Exception as e2:
                    log(f"⚠️ open_orders(ts retry): {e2}")
            else:
                log(f"⚠️ open_orders: {e}")
            return []

    
    def position_info(self, symbol: str, side: str) -> tuple[float, float]:
        """
        Return current position (entry/avg price, quantity). If no position exists,
        returns (0.0, 0.0). Uses a sticky cache to avoid returning zero erroneously
        on transient network/API failures.
        NOTE: positionId도 함께 파싱해 내부 캐시에 저장하지만, 반환값은 (entry, qty)로 유지합니다.
        """
        url = f"{BASE}/openApi/swap/v2/user/positions"
        cache_key = (str(symbol), str(side).upper())

        # 내부 캐시 초기화(최초 1회)
        if not hasattr(self, "_last_position"):
            self._last_position = {}
        if not hasattr(self, "_last_position_id"):
            self._last_position_id = {}
        if not hasattr(self, "_last_position_id_ts"):
            self._last_position_id_ts = {}

        # 네트워크 실패 시 기존 (entry, qty) 캐시 반환
        def _from_cache():
            return self._last_position.get(cache_key, (0.0, 0.0))

        try:
            j = _req_get(url, {"symbol": symbol, "recvWindow": 60000, "timestamp": _ts()}, signed=True)
        except Exception as e:
            try: log(f"⚠️ position_info failed: {e}")
            except Exception: pass
            return _from_cache()

        arr = j.get("data", [])
        want = "LONG" if str(side).upper() == "BUY" else "SHORT"
        pos = None
        for p in arr if isinstance(arr, list) else []:
            if POSITION_MODE == "HEDGE":
                ps = (p.get("positionSide") or p.get("posSide") or p.get("side") or "").upper()
                if ps == want:
                    pos = p
                    break
            else:
                pos = p
                break

        if not pos:
            # 포지션 없음: 두 캐시 갱신
            self._last_position[cache_key] = (0.0, 0.0)
            return 0.0, 0.0 

        # entry (avg price)
        entry = 0.0
        for k in ("entryPrice", "avgPrice", "avgEntryPrice", "openPrice", "positionOpenPrice"):
            v = pos.get(k)
            if v not in (None, ""):
                try:
                    entry = float(v)
                    if entry > 0:
                        break
                except:
                    pass

        # qty (absolute)
        qty = 0.0
        for k in ("positionAmt", "positionAmount", "quantity", "positionQty", "positionSize", "amount", "qty"):
            v = pos.get(k)
            if v not in (None, ""):
                try:
                    qty = abs(float(v))
                    if qty > 0:
                        break
                except:
                    pass

        pid = None
        for k in ("positionId", "id", "position_id"):
            v = pos.get(k)
            if v not in (None, ""):
                pid = str(v)
                break

        self._last_position[cache_key] = (entry, qty)
        if pid:
            self._last_position_id[cache_key] = pid
            self._last_position_id_ts[cache_key] = _now_ms()
        return entry, qty
    
    def get_recent_position_id(self, symbol: str, side: str, max_age_ms: int = 120_000) -> str | None:
        """
        포지션이 0이 된 뒤에도 '최근 max_age_ms 이내'에 본 positionId가 있으면 반환
        """
        if not hasattr(self, "_last_position_id") or not hasattr(self, "_last_position_id_ts"):
            return None
        key = (str(symbol), str(side).upper())
        pid = self._last_position_id.get(key)
        ts  = self._last_position_id_ts.get(key, 0)
        if pid and (_now_ms() - ts) <= max_age_ms:
            return pid
        return None