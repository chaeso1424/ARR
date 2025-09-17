import json
from services.v1_api import _request, _ts

def get_position_net_profit(symbol: str, position_id: str | int) -> float | None:
    """
    지정된 심볼/포지션ID의 positionHistory 호출 후 netProfit 반환
    """
    path = "/openApi/swap/v1/trade/positionHistory"
    method = "GET"

    end_ms = _ts()
    start_ms = end_ms - 24 * 60 * 60 * 1000  # 최근 24시간

    params = {
        "recvWindow": "60000",
        "symbol": symbol,
        "positionId": str(position_id),
        "pageId": 0,
        "pageSize": 5,
        "startTs": start_ms,
        "endTs": end_ms,
    }
    res = _request(method, path, params)

    print("📩 API 응답 원본:", json.dumps(res, indent=2, ensure_ascii=False))

    try:
        records = res["data"].get("positionHistory", [])
        if not records:
            print("⚠️ positionHistory가 비어있음 → netProfit 없음")
            return None
        net_profit = float(records[0]["netProfit"])
        print(f"✅ netProfit 추출 성공: {net_profit}")
        return net_profit
    except Exception as e:
        print(f"❌ netProfit 추출 실패: {e}")
        return None


if __name__ == "__main__":
    result = get_position_net_profit("BTC-USDT", 1968121039583137792)
    print("최종 반환값:", result)
