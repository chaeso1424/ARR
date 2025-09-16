# debug_poshist.py (혹은 BingXClient가 있는 파일 근처)

import time
from utils.logging import log

def norm_symbol(s: str) -> str:
    return s.replace('-', '')

def fetch_position_history_10m(client, symbol: str, position_side: str | None = None):
    sym = norm_symbol(symbol)
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - 10 * 60 * 1000  # 10분

    page = 1
    got = []

    while True:
        # 네 클라이언트 시그니처에 맞춰 파라미터 이름 조정
        resp = client.get_positions_history(
            symbol=sym,
            startTime=start_ms,
            endTime=end_ms,
            page=page,
            limit=200,
            positionSide=position_side  # 필요 없는 거래소면 None 무시됨
        ) or []

        log.info("[poshist-10m] sym=%s side=%s page=%s count=%s",
                 sym, position_side, page, len(resp))
        if resp:
            got.extend(resp)
            # 다음 페이지 유무 판단: resp 개수가 limit 미만이면 종료
            if len(resp) < 200:
                break
            page += 1
        else:
            break

    # 샘플 2건만 찍기
    log.info("[poshist-10m] total=%s sample=%s", len(got), got[:2])
    return got