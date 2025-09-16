import time
import requests
import hmac
from hashlib import sha256

APIURL = "https://open-api.bingx.com"
APIKEY = "0tMW6VUQuVeFFZkyEYnhqKXLvl4cNjVFQJZwS4Rzp81JuotZn89GpPx4QcYjgPcvkLgADEHZO65iYjRrdQ"
SECRETKEY = "PPB369pbczd1ZyI04sWd0OWxCz4hLn3UYLldwTlz4z0PWjPQfDzxRc9uB9Jtnas58REdDC1s7W8A9OYoQqg"

def get_sign(api_secret, payload: str) -> str:
    return hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), digestmod=sha256).hexdigest()

def parse_param(params: dict) -> str:
    # 키 정렬 + timestamp 자동 추가
    keys = sorted(params)
    qs = "&".join(f"{k}={params[k]}" for k in keys)
    ts = str(int(time.time() * 1000))
    return (qs + "&timestamp=" + ts) if qs else ("timestamp=" + ts)

def send_request(method: str, path: str, url_params: str):
    sig = get_sign(SECRETKEY, url_params)
    url = f"{APIURL}{path}?{url_params}&signature={sig}"
    headers = {"X-BX-APIKEY": APIKEY}
    print("➡️", url)  # 디버그용
    r = requests.request(method, url, headers=headers, timeout=10)
    return r.text

def demo():
    path = "/openApi/swap/v1/trade/positionHistory"
    method = "GET"

    # 최근 10분 (밀리초)
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - 12 * 60 * 60 * 1000   # ← 최근 12시간

    params = {
        "recvWindow": "5000",
        # 심볼: 엔드포인트에 따라 BTC-USDT 또는 BTCUSDT를 씁니다.
        # 우선 문서 샘플처럼 하이픈 포함 형태 사용:
        "positionId": "1967918231500050432",
        "symbol": "BTC-USDT",
        "pageId": 0,
        "pageSize": 50,
        "startTs": start_ms,   # ← 반드시 startTs
        "endTs": end_ms,       # ← 반드시 endTs
    }

    qs = parse_param(params)
    return send_request(method, path, qs)

if __name__ == "__main__":
    print("\n=== positionHistory (last 10m) ===")
    print(demo())
