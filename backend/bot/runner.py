# runner.py
import threading
import time
from pathlib import Path
import json
import uuid

from utils.logging import log
from utils.mathx import tp_price_from_roi, floor_to_step, _safe_close_qty
from utils.stats import record_event
from models.config import BotConfig
from models.state import BotState
from services.bingx_client import BingXClient
from redis_helper import get_redis
import os

# ===== 운영 파라미터 =====
RESTART_DELAY_SEC = int(os.getenv("RESTART_DELAY_SEC", "60"))   # TP 후 다음 사이클 대기
CLOSE_ZERO_STREAK = int(os.getenv("CLOSE_ZERO_STREAK", "3"))    # 종료 판정에 필요한 연속 0회수
ZERO_EPS_FACTOR   = float(os.getenv("ZERO_EPS_FACTOR", "0.5"))  # 0 판정 여유(최소단위의 50%)
POLL_SEC          = 1.5
HB_TTL_SEC        = 180  # 하트비트 TTL
RUN_FLAG_TTL_SEC  = 180  # run-flag TTL


class BotRunner:
    def __init__(self, cfg: BotConfig, state: BotState, client: BingXClient, bot_id: str):
        self.cfg = cfg
        self.state = state
        self.client = client
        self._thread: threading.Thread | None = None
        self._stop = False
        self._hb_thread = None
        self._hb_stop = False
        self._lev_checked_this_cycle = False
        self._owner = f"{uuid.uuid4().hex}"


        self.bot_id = bot_id
        base = Path(__file__).resolve().parents[1]  # 프로젝트 루트 기준 조정

        # TP 기준값(모니터링 데드밴드용)
        self._last_tp_price: float | None = None
        self._last_tp_qty: float | None = None

        # 현재 attach 모드 여부를 기록한다. attach 모드에서는 기존 DCA 리밋을 삭제하지 않음.
        self._attach_mode: bool = False

        # ── Redis 핸들러 (멀티 워커 하트비트 공유용)
        try:
            self._r = get_redis()
        except Exception:
            self._r = None

    def _ts_ms(self) -> int:
        return int(time.time() * 1000)
    
    def _log(self, msg: str):
        log(msg, bot_id=self.bot_id)

    def self_logs(self, msg: str):
        return self._log(msg)

    # ---------- lifecycle ----------
    def _runkey(self) -> str:
        return f"bot:running:{self.bot_id}"

    def _hbkey(self) -> str:
        return f"bot:hb:{self.bot_id}"

    def _hb_loop(self):
        """메인 루프와 무관하게 1초마다 하트비트/러닝플래그를 Redis에 갱신."""
        try:
            r = get_redis()
        except Exception:
            r = None

        miss = 0
        while not self._hb_stop:
            ts = time.time()
            # 메모리 하트비트도 매초 갱신 (같은 프로세스에서 즉시 참조 가능)
            self.state.last_heartbeat = ts

            if r:
                try:
                    r.setex(self._runkey(), int(RUN_FLAG_TTL_SEC), self._owner)
                    r.setex(self._hbkey(), int(HB_TTL_SEC), json.dumps({"ts": ts, "owner": self._owner, "running": True}))
                    miss = 0
                except Exception as e:
                    miss += 1
                    if miss % 10 == 1:
                        self._log(f"HB: redis set fail x{miss}: {e}")

            time.sleep(1.0)

    def start(self):
        """봇 시작: 메모리/Redis 상태를 먼저 세팅한 뒤 쓰레드 기동."""
        if self.state.running:
            self._log("ℹ️ 이미 실행 중")
            return

        self._stop = False
        self._hb_stop = False

        # 시작 직후 1회 기록(메모리 + Redis)
        now = time.time()
        self.state.last_heartbeat = now
        try:
            r = get_redis()
            # run-flag에는 소유자 토큰을 값으로 저장
            r.setex(self._runkey(), int(RUN_FLAG_TTL_SEC), self._owner)
            # hb에는 JSON을 저장하여 소유자/타임스탬프/러닝여부를 한 번에 볼 수 있게
            r.setex(self._hbkey(), int(HB_TTL_SEC), json.dumps({"ts": now, "owner": self._owner, "running": True}))
            self._r = r
        except Exception as e:
            self._r = None
            self._log(f"HB init fail (non-fatal): {e}")

        # 하트비트 쓰레드 → 메인 쓰레드 순으로 기동
        self._hb_thread = threading.Thread(target=self._hb_loop, daemon=True)
        self._hb_thread.start()

        self._thread = threading.Thread(target=self._run, daemon=True)
        self.state.running = True
        self._thread.start()

    def stop(self):
        """봇 정지: 쓰레드 정리 후 Redis 키를 즉시 삭제."""
        self._stop = True
        self._hb_stop = True

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        if self._hb_thread and self._hb_thread.is_alive():
            self._hb_thread.join(timeout=2)

        self.state.running = False

        # ── ⬇️ 여기 추가: 소유권 확보 (CAS + stale 판정) ──
        try:
            r = get_redis()
            now  = int(time.time())
            stale_sec = max(int(HB_TTL_SEC) + 60, 240)  # HB TTL보다 여유를 두고 staleness 판단

            script = """
            local runkey = KEYS[1]
            local hbkey  = KEYS[2]
            local owner  = ARGV[1]
            local now    = tonumber(ARGV[2])
            local hbttl  = tonumber(ARGV[3])
            local rttl   = tonumber(ARGV[4])
            local stale  = tonumber(ARGV[5])

            local cur = redis.call("GET", runkey)
            if not cur then
            -- 아무도 없음 → 내가 선점
            redis.call("SETEX", runkey, rttl, owner)
            redis.call("SETEX", hbkey,  hbttl, cjson.encode({ts=now, owner=owner, running=true}))
            return "TAKEN"
            end

            -- runkey 존재 → HB 확인
            local hb = redis.call("GET", hbkey)
            if not hb then
            -- HB 없으면 죽었다고 보고 인수
            redis.call("SETEX", runkey, rttl, owner)
            redis.call("SETEX", hbkey,  hbttl, cjson.encode({ts=now, owner=owner, running=true}))
            return "TAKEN_NOHB"
            end

            local ok, obj = pcall(cjson.decode, hb)
            if not ok then
            -- HB 깨짐 → 인수
            redis.call("SETEX", runkey, rttl, owner)
            redis.call("SETEX", hbkey,  hbttl, cjson.encode({ts=now, owner=owner, running=true}))
            return "TAKEN_BADHB"
            end

            if obj.running == false then
            -- 종료마커 → 인수
            redis.call("SETEX", runkey, rttl, owner)
            redis.call("SETEX", hbkey,  hbttl, cjson.encode({ts=now, owner=owner, running=true}))
            return "TAKEN_ENDED"
            end

            local ts = tonumber(obj.ts or 0)
            if ts <= 0 or (now - ts) > stale then
            -- 오래된 HB → 인수
            redis.call("SETEX", runkey, rttl, owner)
            redis.call("SETEX", hbkey,  hbttl, cjson.encode({ts=now, owner=owner, running=true}))
            return "TAKEN_STALE"
            end

            -- 아직 살아있음
            return "BUSY"
            """

            res = r.eval(
                script, 2,
                self._runkey(), self._hbkey(),
                self._owner, str(now), str(HB_TTL_SEC), str(RUN_FLAG_TTL_SEC), str(stale_sec)
            )
            if str(res).startswith("BUSY"):
                self._log("⛔ 다른 인스턴스가 실행 중으로 판단(BUSY). 시작 중단.")
                return
            else:
                self._log(f"🔑 소유권 확보: {res}")
            self._r = r
        except Exception as e:
            self._r = None
            self._log(f"HB owner-takeover init fail (non-fatal): {e}")


    # ---------- helpers ----------
    def _now(self) -> float:
        return time.time()

    def _wait_cancel(self, order_id: str, timeout: float = 3.0) -> bool:
        """취소 요청 후 openOrders에서 사라질 때까지 잠깐 대기."""
        t0 = time.time()
        want = str(order_id)
        while time.time() - t0 < timeout:
            try:
                oo = self.client.open_orders(self.cfg.symbol)
                alive = any(
                    str(o.get("orderId") or o.get("orderID") or o.get("id") or "")
                    == want
                    for o in oo
                )
                if not alive:
                    return True
            except Exception:
                pass
            time.sleep(0.2)
        return False

    def _estimate_required_margin(
        self, side: str, mark: float, spec: dict, pp: int, step: float
    ) -> tuple[float, list]:
        """
        현재 설정(DCA, 레버리지)을 기준으로 '지금 가격'에서 모든 진입 주문(1차+리밋)에
        필요한 총 증거금(USDT)을 추정. (TP 제외)
        반환: (required_total_usdt, plan_list[{'type','price','qty','usdt'}...])
        """
        contract = float(spec.get("contractSize", 1.0)) or 1.0
        min_qty = float(spec.get("minQty", 0.0))
        lev = max(float(self.cfg.leverage), 1.0)

        def _plan_unit(price: float, usdt_amt: float) -> tuple[float, float, float]:
            target_notional = float(usdt_amt) * lev
            raw_qty = target_notional / max(price * contract, 1e-12)
            q = floor_to_step(raw_qty, step)
            if q < (min_qty or step):
                q = max(min_qty, step)
            need_margin = (q * price * contract) / lev
            return q, price, need_margin

        plan: list[dict] = []
        first_usdt = float(self.cfg.dca_config[0][1])
        q1, p1, m1 = _plan_unit(mark, first_usdt)
        plan.append({"type": "MARKET", "price": p1, "qty": q1, "usdt": m1})

        cum = 0.0
        for gap, usdt_amt in self.cfg.dca_config[1:]:
            cum += float(gap)
            price = mark * (1 - cum / 100.0) if side == "BUY" else mark * (1 + cum / 100.0)
            price = float(f"{price:.{pp}f}")
            q, p, m = _plan_unit(price, float(usdt_amt))
            plan.append({"type": "LIMIT", "price": p, "qty": q, "usdt": m})

        fee_buf = 1.002  # 수수료/슬리피지 버퍼(0.2%)
        required_total = sum(x["usdt"] for x in plan) * fee_buf
        return required_total, plan

    def _refresh_position(self) -> None:
        """스티키 평균가: qty>0인데 avg=0이면 이전 avg 유지."""
        old_avg = float(self.state.position_avg_price or 0.0)
        avg, qty = self.client.position_info(self.cfg.symbol, self.cfg.side)
        if qty > 0 and (avg is None or avg <= 0) and old_avg > 0:
            avg = old_avg
        self.state.position_avg_price = avg
        self.state.position_qty = qty

    def _cancel_tracked_limits(
        self,
        want_side: str | None = None,    # "BUY"/"SELL" (없으면 self.cfg.side)
        want_pos:  str | None = None,    # "LONG"/"SHORT" (헷지모드 권장)
        attempts:  int = 3,              # 재시도 횟수
        verify_sleep: float = 0.4        # 각 라운드 후 반영 대기
    ) -> bool:
        """
        BingX SWAP v2 기준: 새 사이클 진입 전에 '엔트리 유발 가능' 주문을 깨끗하게 제거.
        - reduceOnly=True 또는 closePosition=True 인 주문은 제외(청산용).
        - LIMIT / LIMIT_MAKER / STOP / TAKE_PROFIT / STOP_LOSS_LIMIT / TAKE_PROFIT_LIMIT /
        STOP_MARKET / TAKE_PROFIT_MARKET (+ 타입 누락 방어) 중 이번 사이클 방향과 일치하는 주문만 취소.
        - 헷지 모드면 positionSide까지 want_pos와 일치하는 경우만 위험으로 간주.
        - 추적 ID(self.state.open_limit_ids) 우선 취소 → 심볼 전체 스윕 → 검증 반복.
        반환: 모두 제거되면 True, 남아 있으면 False
        """
        import time

        sym = self.cfg.symbol
        cfg_side = (self.cfg.side or "BUY").upper()
        use_side = (want_side or cfg_side).upper()                           # "BUY"/"SELL"
        use_pos  = (want_pos or ("LONG" if use_side == "BUY" else "SHORT")).upper()
        hedge    = bool(getattr(self.cfg, "hedge_mode", False))
        order_tag = getattr(self.cfg, "order_tag", None)  # 내 주문만 취소하고 싶을 때 프리픽스

        def _truthy(x) -> bool:
            if x is None: return False
            if isinstance(x, (int, float)): return bool(x)
            return str(x).strip().lower() in ("true","1","yes","y","t")

        # 0) 우리가 트래킹하던 ID 우선 취소(한 번만)
        for oid in list(self.state.open_limit_ids):
            try:
                self.client.cancel_order(sym, oid)
            except Exception as e:
                msg = str(e).lower()
                if any(k in msg for k in ("80018","not exist","does not exist","unknown order","filled","canceled","cancelled")):
                    self._log(f"ℹ️ 건너뜀(이미 정리됨): {oid}")
                else:
                    self._log(f"⚠️ 리밋 취소 실패: {oid} {e}")

        # 라운드 반복
        for attempt in range(1, int(max(1, attempts)) + 1):
            time.sleep(verify_sleep)

            # 1) 현재 오픈오더 조회
            try:
                open_orders = self.client.open_orders(sym) or []
            except Exception as e:
                self._log(f"⚠️ 오픈오더 조회 실패: {e}")
                # 조회가 실패해도 재시도 라운드 진행
                continue

            # 2) 엔트리-위험 주문 선별
            danger = []
            seen = set(self.state.open_limit_ids)  # 직전 취소 재시도 방지용
            for o in open_orders:
                oid = o.get("orderId") or o.get("orderID") or o.get("id")
                if not oid or oid in seen:
                    continue

                o_side = str(o.get("side") or o.get("orderSide") or "").upper()                # BUY/SELL
                o_pos  = str(o.get("positionSide") or o.get("posSide") or o.get("position_side") or "").upper()  # LONG/SHORT/""
                o_typ  = str(o.get("type") or o.get("orderType") or "").upper()
                reduce_only = _truthy(o.get("reduceOnly") or o.get("reduce_only") or o.get("reduceOnlyFlag"))
                close_pos   = _truthy(o.get("closePosition"))

                # 내 주문만 취소하고 싶다면 clientOrderId 접두어 필터
                if order_tag:
                    cid = o.get("clientOrderId") or o.get("client_order_id") or ""
                    if not str(cid).startswith(str(order_tag)):
                        continue

                # 청산류는 제외
                if reduce_only or close_pos:
                    continue

                # 엔트리로 오인될 수 있는 타입을 넓게 차단 (+ 타입 누락 방어)
                is_entryish = (o_typ == "") or any(t in o_typ for t in (
                    "LIMIT","LIMIT_MAKER",
                    "STOP","TAKE_PROFIT",
                    "STOP_LOSS_LIMIT","TAKE_PROFIT_LIMIT",
                    "STOP_MARKET","TAKE_PROFIT_MARKET"
                ))
                if not is_entryish:
                    continue

                # 이번 사이클 방향 일치만 위험
                if o_side != use_side:
                    continue

                # 헷지면 positionSide 일치 필요 (비어있으면 스킵)
                if hedge:
                    if not o_pos or o_pos != use_pos:
                        continue

                danger.append(oid)

            # 3) 위험 주문이 없다 → 성공
            if not danger:
                self.state.open_limit_ids.clear()
                return True

            # 4) 취소 시도
            cancelled_any = False
            for oid in danger:
                try:
                    self.client.cancel_order(sym, oid)
                    cancelled_any = True
                except Exception as e:
                    msg = str(e).lower()
                    if any(k in msg for k in ("80018","not exist","does not exist","unknown order","filled","canceled","cancelled")):
                        self._log(f"ℹ️ 건너뜀(이미 정리됨): {oid}")
                    else:
                        self._log(f"⚠️ 오픈오더 취소 실패: {oid} {e}")

            # 5) 다음 라운드로 재검증. 취소가 전혀 안 먹으면 짧게 추가 대기
            if not cancelled_any:
                time.sleep(verify_sleep)

        # 재시도 모두 실패 → 남아있음
        self.state.open_limit_ids.clear()
        return False


    # ---------- main loop ----------
    def _run(self) -> None:
        """
        Main loop with loop-based self-healing. We do NOT recurse on error anymore.
        This prevents momentary `state.running = False` flickers.
        """
        try:
            while not self._stop:
                self._lev_checked_this_cycle = False
                try:
                    # 1) 정밀도/스펙 동기화
                    try:
                        pp, qp = self.client.get_symbol_filters(self.cfg.symbol)
                        self.cfg.price_precision = pp
                        self.cfg.qty_precision = qp
                        self._log(f"ℹ️ precision synced: price={pp}, qty={qp}")
                    except Exception as e:
                        self._log(f"⚠️ precision sync failed: {e}")
                        pp, qp = 4, 0

                    spec = self.client.get_contract_spec(self.cfg.symbol)
                    pp = int(spec.get("pricePrecision", pp))
                    qp = int(spec.get("quantityPrecision", qp))
                    contract = float(spec.get("contractSize", 1.0)) or 1.0
                    min_qty = float(spec.get("minQty", 0.0))
                    step = float(spec.get("qtyStep") or (1.0 if qp == 0 else 10 ** (-qp)))
                    if step <= 0:
                        step = 1.0 if qp == 0 else 10 ** (-qp)
                    self._log(f"ℹ️ spec: contractSize={contract}, minQty={min_qty}, qtyStep={step}, pp={pp}, qp={qp}")

                    side = self.cfg.side.upper()
                    mark = float(self.client.get_mark_price(self.cfg.symbol))

                    # ---- 현재 포지션 파악(attach 모드 여부 선결정) ----
                    try:
                        pre_avg, pre_qty = self.client.position_info(self.cfg.symbol, self.cfg.side)
                    except Exception:
                        pre_avg, pre_qty = 0.0, 0.0
                    min_live_qty = max(float(min_qty or 0.0), float(step or 0.0))
                    attach_mode = (float(pre_qty) >= (min_live_qty * ZERO_EPS_FACTOR))
                    self._attach_mode = attach_mode

                    # 0) 가용 USDT 체크 (attach 모드면 패스 가능)
                    try:
                        av = float(self.client.get_available_usdt())
                        if av < 0.99:  # 1차 조회 결과가 0에 가까움
                            self._log("⚠️ 가용 USDT 0 → 재측정 시도")
                            time.sleep(1)
                            av = float(self.client.get_available_usdt())
                    except Exception as e:
                        self._log(f"❌ 가용잔고 조회 실패: {e}")
                        av = 0.0

                    budget = sum(float(usdt) for _, usdt in self.cfg.dca_config)

                    if av < 0.99 and not attach_mode:
                        self._log("⛔ 가용 USDT 없음 → 종료")
                        break

                    # 1.6) === 사전 예산 점검 (attach 모드는 스킵) ===
                    if not attach_mode:
                        required, plan = self._estimate_required_margin(side, mark, spec, pp, step)
                        try:
                            av = float(self.client.get_available_usdt())
                        except Exception:
                            pass
                        self.state.budget_ok = av + 1e-9 >= required
                        self.state.budget_required = required
                        self.state.budget_available = av
                        if av + 1e-9 < required:
                            gap = required - av
                            self._log("⛔ 예산 부족: 모든 진입 주문에 필요한 증거금이 가용 USDT보다 큽니다.")
                            self._log(f"   필요≈{required:.4f} USDT, 가용≈{av:.4f} USDT, 부족≈{gap:.4f} USDT")
                            for idx, x in enumerate(plan, start=1):
                                self._log(
                                    f"   · {idx:02d} {x['type']}: price={x['price']} qty={x['qty']} → 증거금≈{x['usdt']:.4f} USDT"
                                )
                            break
                        else:
                            self._log(f" 예산 확인 OK: 필요≈{required:.4f} USDT ≤ 가용≈{av:.4f} USDT")

                    # === attach 모드: 시장가/DCA 스킵, TP만 확보 ===
                    if attach_mode:
                        self._log(f" 기존 포지션 연결 모드: qty={pre_qty}, avg={pre_avg} → DCA/시장가 스킵, TP 확보")
                        self.state.position_avg_price = pre_avg
                        self.state.position_qty = pre_qty
                        try:
                            self._prev_qty_snap = float(self.state.position_qty or 0.0)
                        except Exception:
                            self._prev_qty_snap = 0.0
                        last_entry = float(pre_avg or 0.0)
                        last_tp_price = self._last_tp_price
                        last_tp_qty = self._last_tp_qty
                    else:
                        entry_side = "BUY" if side == "BUY" else "SELL"
                        entry_pos  = "LONG" if side == "BUY" else "SHORT"

                        ok = self._cancel_tracked_limits(
                            want_side=entry_side,
                            want_pos=entry_pos,
                            attempts=3,
                            verify_sleep=0.4
                        )
                        if not ok:
                            self._log("⚠️ 엔트리 전 오류로 인한 잔여 리밋 정리 실패 ⚠️")
                            break

                        if not self._lev_checked_this_cycle:
                            try:
                                lev_now = self.client.get_current_leverage(self.cfg.symbol, self.cfg.side)
                                if lev_now is not None:
                                    want = float(self.cfg.leverage)
                                    diff = abs(lev_now - want) / max(want, 1.0)
                                    if diff > 0.02:
                                        self._log(f"⛔ 레버리지 불일치: 설정={want}x, 거래소={lev_now}x → 수량/증거금 오차 발생")
                                        self._log("   거래소 앱/웹에서 해당 심볼의 레버리지를 설정값과 동일하게 맞춘 뒤 다시 시작하세요.")
                                        break  # 이번 사이클 중단
                                else:
                                    self._log("ℹ️ 레버리지 조회값 없음(포지션 없음/일시 실패) → 진입 후 다시 확인 예정")
                            except Exception as e:
                                self._log(f"⚠️ 레버리지 확인 생략(일시 오류): {e}")
                            finally:
                                self._lev_checked_this_cycle = True

                        # 2) 1차 시장가 진입
                        self._cancel_tracked_limits()
                        first_usdt = float(self.cfg.dca_config[0][1])
                        target_notional = first_usdt * float(self.cfg.leverage)
                        raw_qty = target_notional / max(mark * contract, 1e-12)
                        qty = floor_to_step(raw_qty, step)
                        if qty < (min_qty or step):
                            self._log(f"⚠️ 1차 수량이 최소수량 미달(raw={raw_qty}) → {max(min_qty, step)}로 보정")
                            qty = max(min_qty, step)
                        try:
                            oid = self.client.place_market(self.cfg.symbol, side, qty)
                        except Exception as e:
                            msg = str(e)
                            if "80001" in msg:
                                self._log(f"❌ 시장가 진입 실패: {e}")
                                break
                            elif "timed out" in msg.lower():
                                self._log(f"⚠️ 시장가 주문 타임아웃: {e} → attach 모드로 재시도")
                                continue
                            else:
                                raise
                        if not oid:
                            raise RuntimeError("market order failed: no orderId")
                        self._log(f" 1차 시장가 진입 주문: {oid} (투입≈{first_usdt} USDT, qty={qty})")
                        time.sleep(1)
                        self._refresh_position()
                        try:
                            entry_filled_qty = max(float(self.state.position_qty or 0.0) - float(pre_qty or 0.0), 0.0)
                            entry_price = float(self.state.position_avg_price or 0.0) or float(mark)
                            if entry_filled_qty > 0:
                                record_event(
                                    kind="ENTRY",
                                    symbol=self.cfg.symbol,
                                    price=entry_price,
                                    qty=entry_filled_qty,
                                    order_id=int(oid) if str(oid).isdigit() else None,
                                    client_order_id=None,
                                    ts_ms=self._ts_ms(),
                                )
                                self._log(f"📈 ENTRY 집계: price={entry_price}, qty={entry_filled_qty}")
                        except Exception as _e:
                            self._log(f"⚠️ ENTRY 집계 실패(무시): {_e}")

                        try:
                            self._prev_qty_snap = float(self.state.position_qty or 0.0)
                        except Exception:
                            self._prev_qty_snap = 0.0

                        base_price = float(self.state.position_avg_price or 0.0)
                        if base_price <= 0:
                            try:
                                base_price = float(self.client.get_mark_price(self.cfg.symbol))
                            except Exception:
                                base_price = float(self.client.get_last_price(self.cfg.symbol))
                            self._log(f"⚠️ avg_price=0 → fallback base_price={base_price} (DCA initial only)")

                        entry_pos_side = "LONG" if side == "BUY" else "SHORT"
                        cumulative = 0.0
                        self.state.open_limit_ids.clear()

                        for i, (gap_pct, usdt_amt) in enumerate(self.cfg.dca_config[1:], start=2):
                            cumulative += float(gap_pct)
                            if side == "BUY":
                                price = base_price * (1 - cumulative / 100.0)
                            else:
                                price = base_price * (1 + cumulative / 100.0)
                            price = float(f"{price:.{pp}f}")
                            target_notional = float(usdt_amt) * float(self.cfg.leverage)
                            raw_qty = target_notional / max(price * contract, 1e-12)
                            q = floor_to_step(raw_qty, step)
                            if q < (min_qty or step):
                                self._log(f"⚠️ {i}차 수량이 최소수량 미달(raw={raw_qty}) → {max(min_qty, step)}로 보정")
                                q = max(min_qty, step)
                            try:
                                lid = self.client.place_limit(
                                    self.cfg.symbol,
                                    side,
                                    q,
                                    price,
                                    position_side=entry_pos_side,
                                )
                            except Exception as e:
                                msg = str(e)
                                if "80001" in msg:
                                    self._log(f"⚠️ {i}차 리밋 주문 실패: {e}")
                                    break
                                elif "timed out" in msg.lower():
                                    self._log(f"⚠️ {i}차 리밋 타임아웃: {e} → 남은 리밋 생략")
                                    break
                                else:
                                    raise
                            self.state.open_limit_ids.append(str(lid))
                            self._log(f" {i}차 리밋: id={lid}, price={price}, qty={q}, 투입≈{usdt_amt}USDT")

                        # 4) 초기 TP 세팅
                        self._refresh_position()
                        tick = 10 ** (-pp) if pp > 0 else 0.01
                        min_allowed = max(float(min_qty or 0.0), float(step or 0.0))
                        qty_now = float(self.state.position_qty or 0.0)

                        last_entry = None
                        last_tp_price = None
                        last_tp_qty = None

                        if qty_now >= min_allowed:
                            entry = float(self.state.position_avg_price or 0.0)
                            if entry <= 0:
                                try:
                                    entry = float(self.client.get_mark_price(self.cfg.symbol))
                                except Exception:
                                    entry = float(self.client.get_last_price(self.cfg.symbol))
                                self._log(f"⚠️ avg_price=0 → fallback entry={entry} (initial only)")

                            tp_stop = tp_price_from_roi(entry, side, float(self.cfg.tp_percent), int(self.cfg.leverage), pp)
                            tp_qty = _safe_close_qty(qty_now, float(step or 1.0), min_allowed)
                            if tp_qty < min_allowed:
                                tp_qty = min_allowed
                            if tp_stop <= 0 or tp_qty <= 0:
                                raise RuntimeError(f"TP invalid: stop={tp_stop}, qty={tp_qty}")

                            tp_side = "SELL" if side == "BUY" else "BUY"
                            tp_pos  = "LONG" if side == "BUY" else "SHORT"
                            new_tp_id: str | None = None

                            try:
                                new_tp_id = self.client.place_tp_market(
                                    self.cfg.symbol,
                                    side=tp_side,
                                    stop_price=tp_stop,
                                    qty=tp_qty,
                                    position_side=tp_pos,
                                )
                            except Exception as e:
                                if "80001" in str(e):
                                    self._log(f"⚠️ 초기 TP 주문 실패: {e}")
                                    new_tp_id = None
                                else:
                                    raise

                            if new_tp_id:
                                self.state.tp_order_id = str(new_tp_id)
                                last_entry = entry
                                last_tp_price = tp_stop
                                last_tp_qty = tp_qty
                                self._last_tp_price = tp_stop
                                self._last_tp_qty = tp_qty
                                self._log(f"✅ TP(MKT) 배치 완료: id={new_tp_id}, stop={tp_stop}, qty={tp_qty}, side={tp_side}/{tp_pos}")
                            else:
                                self._log("ℹ️ 초기 TP 주문 생략")
                                last_entry = entry
                                last_tp_price = tp_stop
                                last_tp_qty = tp_qty
                        else:
                            self._log("ℹ️ 포지션 없음 또는 최소단위 미만 → TP 생략")
                            last_entry = None
                            last_tp_price = None
                            last_tp_qty = None

                    # ===== 5) 모니터링 루프 =====
                    tp_reset_cooldown = 3.0
                    last_tp_reset_ts = 0.0
                    zero_streak = 0
                    did_cleanup = False

                    while not self._stop:
                        time.sleep(POLL_SEC)
                        self._refresh_position()

                        if not hasattr(self, "_prev_qty_snap"):
                            self._prev_qty_snap = float(self.state.position_qty or 0.0)
                        prev_qty_snap = float(self._prev_qty_snap or 0.0)

                        qty_now_for_dca = float(self.state.position_qty or 0.0)
                        min_allowed = max(float(min_qty or 0.0), float(step or 0.0))
                        zero_eps = min_allowed * ZERO_EPS_FACTOR
                        inc = qty_now_for_dca - prev_qty_snap

                        if qty_now_for_dca > 0:
                            self._last_nonzero_qty = qty_now_for_dca

                        has_tracked_limits = bool(self.state.open_limit_ids)
                        if has_tracked_limits and inc > zero_eps:
                            try:
                                dca_price = float(self.state.position_avg_price or 0.0) or float(mark)
                                dca_qty = inc
                                record_event(
                                    kind="DCA",
                                    symbol=self.cfg.symbol,
                                    price=dca_price,
                                    qty=dca_qty,
                                    order_id=None,
                                    client_order_id=None,
                                    ts_ms=self._ts_ms(),
                                )
                                self._log(f"📈 DCA 집계: price={dca_price}, qty={dca_qty}")
                            except Exception as _e:
                                self._log(f"⚠️ DCA 집계 실패(무시): {_e}")

                        self._prev_qty_snap = qty_now_for_dca

                        entry_now = float(self.state.position_avg_price or 0.0)
                        qty_now   = float(self.state.position_qty or 0.0)

                        try:
                            open_orders = self.client.open_orders(self.cfg.symbol)
                        except Exception as e:
                            self._log(f"⚠️ 오픈오더 조회 실패: {e}")
                            open_orders = []

                        tp_alive = False
                        if self.state.tp_order_id:
                            want = str(self.state.tp_order_id)
                            for o in open_orders:
                                oid = str(o.get("orderId") or o.get("orderID") or o.get("id") or "")
                                if oid == want:
                                    tp_alive = True
                                    break

                        tick = 10 ** (-pp) if pp > 0 else 0.01
                        min_allowed = max(float(min_qty or 0.0), float(step or 0.0))
                        zero_eps = min_allowed * ZERO_EPS_FACTOR
                        if qty_now < zero_eps:
                            zero_streak += 1
                        else:
                            zero_streak = 0
                        really_closed = (zero_streak >= CLOSE_ZERO_STREAK) and (not tp_alive)

                        if really_closed:
                            try:
                                chk_avg, chk_qty = self.client.position_info(self.cfg.symbol, self.cfg.side)
                            except Exception:
                                chk_avg, chk_qty = 0.0, 0.0

                            if float(chk_qty or 0.0) < zero_eps:
                                try:
                                    if hasattr(self, "_last_nonzero_qty") and float(self._last_nonzero_qty) > 0:
                                        closed_qty = float(self._last_nonzero_qty)
                                    elif hasattr(self, "_prev_qty_snap") and float(self._prev_qty_snap) > 0:
                                        closed_qty = float(self._prev_qty_snap)
                                    else:
                                        closed_qty = float(qty_now)

                                    tp_price = float(self._last_tp_price or 0.0) or float(mark)

                                    eff_entry = float(entry_now or 0.0) if (entry_now and entry_now > 0) else float(last_entry or 0.0)
                                    if eff_entry <= 0:
                                        eff_entry = float(self.state.position_avg_price or 0.0) or float(mark)

                                    side_dir = self.cfg.side.upper()
                                    contract_size = float(contract or 1.0)
                                    if side_dir == "BUY":
                                        pnl = (tp_price - eff_entry) * closed_qty * contract_size
                                    else:
                                        pnl = (eff_entry - tp_price) * closed_qty * contract_size
                                    if abs(pnl) < 1e-10:
                                        pnl = 0.0

                                    record_event(
                                        kind="TP",
                                        symbol=self.cfg.symbol,
                                        price=tp_price,
                                        qty=closed_qty,
                                        ts_ms=self._ts_ms(),
                                        pnl=pnl,
                                        side=side_dir,
                                        entry_price=eff_entry
                                    )
                                    self._log(f"📈 TP 집계: price={tp_price}, qty={closed_qty}, pnl={pnl:.6f}")
                                    self._last_nonzero_qty = 0.0
                                except Exception as _e:
                                    self._log(f"⚠️ TP 집계 실패(무시): {_e}")

                                if not did_cleanup:
                                    self._cancel_tracked_limits()
                                    if self.state.tp_order_id:
                                        try:
                                            self.client.cancel_order(self.cfg.symbol, self.state.tp_order_id)
                                        except Exception:
                                            pass
                                        self.state.tp_order_id = None
                                    self.state.reset_orders()
                                    did_cleanup = True

                                self._log("✅ 포지션 종료 확정(연속검증+이중확인) → 대기")
                                break
                            else:
                                zero_streak = 0

                        want_side = "SELL" if side == "BUY" else "BUY"
                        want_pos  = "LONG" if side == "BUY" else "SHORT"
                        tp_equal_exists = False
                        tp_equal_id = None
                        tp_equal_price = None
                        for o in open_orders:
                            o_side = str(o.get("side") or o.get("orderSide") or "").upper()
                            o_pos  = str(o.get("positionSide") or o.get("posSide") or o.get("position_side") or "").upper()
                            if (o_side != want_side) or (o_pos != want_pos):
                                continue
                            q = o.get("origQty") or o.get("quantity") or o.get("qty") or o.get("orig_quantity")
                            try:
                                oq = float(q) if q is not None else 0.0
                            except Exception:
                                oq = 0.0
                            if abs(qty_now - oq) < float(step or 1.0):
                                tp_equal_exists = True
                                tp_equal_id = str(o.get("orderId") or o.get("orderID") or o.get("id") or "")
                                p = o.get("price") or o.get("origPrice") or o.get("limitPrice")
                                try:
                                    tp_equal_price = float(p) if p is not None else None
                                except Exception:
                                    tp_equal_price = None
                                break

                        if tp_equal_exists:
                            if not tp_alive:
                                self.state.tp_order_id = tp_equal_id
                                if tp_equal_price is not None:
                                    self._last_tp_price = tp_equal_price
                                self._last_tp_qty = qty_now
                                self._log(f"ℹ️ 기존 TP 채택: id={tp_equal_id}, qty≈{qty_now}")
                            continue

                        need_reset_tp = False
                        eff_entry = entry_now if entry_now > 0 else float(last_entry or 0.0)
                        if not tp_alive:
                            need_reset_tp = (qty_now >= min_allowed and eff_entry > 0)
                        else:
                            if qty_now >= min_allowed and eff_entry > 0:
                                ideal_stop = tp_price_from_roi(eff_entry, side, float(self.cfg.tp_percent), int(self.cfg.leverage), pp)
                                ideal_qty  = _safe_close_qty(qty_now, step, min_allowed)
                                if (last_entry is None) or (last_tp_price is None) or (last_tp_qty is None):
                                    need_reset_tp = True
                                elif (abs(eff_entry - last_entry) >= 2 * tick) or \
                                    (abs(ideal_stop - last_tp_price) >= 2 * tick) or \
                                    (abs(ideal_qty - last_tp_qty) >= float(step or 1.0)):
                                    need_reset_tp = True

                        if need_reset_tp:
                            now_ts = self._now()
                            if now_ts - last_tp_reset_ts < tp_reset_cooldown:
                                continue
                            if self.state.tp_order_id and tp_alive:
                                try:
                                    self.client.cancel_order(self.cfg.symbol, self.state.tp_order_id)
                                    self._wait_cancel(self.state.tp_order_id, timeout=2.5)
                                except Exception as e:
                                    self._log(f"⚠️ TP 취소 실패(무시): {e}")
                            if eff_entry <= 0 or qty_now < min_allowed:
                                continue

                            new_stop = tp_price_from_roi(eff_entry, side, float(self.cfg.tp_percent), int(self.cfg.leverage), pp)
                            new_qty  = _safe_close_qty(qty_now, step, min_allowed)
                            self._refresh_position()
                            qty_last = float(self.state.position_qty or 0.0)
                            if qty_last < min_allowed:
                                self._log("ⓘ TP skip: position vanished just before placement (qty=0)")
                                continue
                            new_qty  = _safe_close_qty(qty_last, step, min_allowed)
                            new_side = "SELL" if side == "BUY" else "BUY"
                            new_pos  = "LONG" if side == "BUY" else "SHORT"

                            try:
                                new_id = self.client.place_tp_market(
                                    self.cfg.symbol,
                                    side=new_side,
                                    stop_price=new_stop,
                                    qty=new_qty,
                                    position_side=new_pos,
                                )
                            except Exception as e:
                                msg = str(e)
                                if ("80001" in msg) or ("timed out" in msg.lower()):
                                    continue
                                else:
                                    raise

                            self.state.tp_order_id = str(new_id)
                            last_entry     = eff_entry
                            last_tp_price  = new_stop
                            last_tp_qty    = new_qty
                            self._last_tp_price = new_stop
                            self._last_tp_qty   = new_qty
                            last_tp_reset_ts = now_ts
                            self._log(f"♻️ TP 재설정(MKT): id={new_id}, stop={new_stop}, qty={new_qty}")

                    # 루프 탈출: repeat면 다시 반복
                    if self._stop:
                        pass
                    elif not self.state.repeat_mode:
                        pass
                    else:
                        if not did_cleanup:
                            self._cancel_tracked_limits()
                            if self.state.tp_order_id:
                                try:
                                    self.client.cancel_order(self.cfg.symbol, self.state.tp_order_id)
                                except Exception:
                                    pass
                                self.state.tp_order_id = None
                            self.state.reset_orders()
                            did_cleanup = True
                        delay = max(0, RESTART_DELAY_SEC)
                        if delay > 0:
                            self._log(f" 반복 모드 → {delay}초 대기 후 재시작")
                            for _ in range(delay):
                                if self._stop:
                                    break
                                time.sleep(1)
                        if not self._stop:
                            self._log(" 재시작")

                except Exception as e:
                    # 🔁 여기서만 자기치유. 재귀 호출 금지.
                    try:
                        self._log(f"⚠️ 런타임 오류(자동복구 유지): {e}")
                    except Exception:
                        pass
                    if self._stop:
                        break
                    time.sleep(300)
                    continue
        finally:
            self.state.running = False
            # ⬇️ Redis도 정리
            if getattr(self, "_r", None):
                try:
                    # 종료 직전, 5초짜리 "정상 종료" 마커 남기기 (판독측이 부드럽게 전환)
                    self._r.setex(self._hbkey(), 5, json.dumps({"ts": time.time(), "running": False}))
                except Exception:
                    pass
            self._log("⏹️ 봇 종료")
