import time
import os
import requests
import hmac
import json
from hashlib import sha256
from dotenv import load_dotenv
load_dotenv()

APIKEY = os.getenv("BINGX_API_KEY", "")
SECRETKEY = os.getenv("BINGX_API_SECRET", "")
APIURL = os.getenv("BINGX_BASE", "https://open-api.bingx.com")

def _ts() -> int:
    return int(time.time() * 1000)

def _sign(secret: str, payload: str) -> str:
    return hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), digestmod=sha256).hexdigest()

def _query(params: dict) -> str:
    if "timestamp" not in params:
        params["timestamp"] = _ts()
    keys = sorted(params)
    return "&".join(f"{k}={params[k]}" for k in keys)

def _request(method: str, path: str, params: dict) -> dict:
    qs = _query(params)
    sig = _sign(SECRETKEY, qs)
    url = f"{APIURL}{path}?{qs}&signature={sig}"
    headers = {"X-BX-APIKEY": APIKEY}
    resp = requests.request(method, url, headers=headers)
    resp.raise_for_status()
    return resp.json()

def get_position_net_profit(symbol: str, position_id: str | int) -> float | None:
    """
    지정된 심볼/포지션ID의 positionHistory 호출 후 netProfit 반환
    """
    path = "/openApi/swap/v1/trade/positionHistory"
    method = "GET"

    end_ms = _ts()
    start_ms = end_ms - 60 * 60 * 1000  # 최근 1시간

    params = {
        "recvWindow": "60000",
        "symbol": symbol,
        "positionId": str(position_id),
        "pageId": 0,
        "pageSize": 1,   # 어차피 하나만
        "startTs": start_ms,
        "endTs": end_ms,
    }
    res = _request(method, path, params)

    # 응답 구조: {"code":0,"data":{"records":[{"netProfit": "..."}]}}
    try:
        return float(res["data"]["records"][0]["netProfit"])
    except Exception:
        return None

# 호출용
def demo(self, symbol: str, position_id: str | int) -> float | None:
    return get_position_net_profit(symbol, position_id)


if __name__ == "__main__":
    print(demo(None, "BTC-USDT", 123456789))
