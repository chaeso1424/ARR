// react-bootstrap
import { Row, Col, Card, Table, ListGroup, Button, Form, ButtonGroup, InputGroup, Badge } from 'react-bootstrap';

// react
import React, { useEffect, useRef, useState } from 'react';

// third party
import Chart from 'react-apexcharts';
import PerfectScrollbar from 'react-perfect-scrollbar';

// project import (그대로 유지)
import buildAccountBalanceChart from './chart/accountbal-chart';
import buildTradesDonut from './chart/trades-donut';
import OrderCard from '../../components/Widgets/Statistic/OrderCard';
import buildProfitDonut from './chart/profit-chart';
import { useBaseline } from '@/hooks/useBaseline';



// ==============================|| DASHBOARD ANALYTICS (Multi-Bot) ||============================== //

const DashAnalytics = () => {
  // ── 공용 상태 (요약 카드)
  const [currency, setCurrency] = useState('USDT');
  const [balance, setBalance] = useState(null);
  const [equity, setEquity] = useState(null);
  const [available, setAvailable] = useState(null);
  const [loading, setLoading] = useState(true);

  // ✅ 에러 표시 (흰 화면 방지용)
  const [error, setError] = useState('');

  // ── 봇 목록/선택
  const [bots, setBots] = useState([]);           // [{id, name?, status?}]
  const [currentBotId, setCurrentBotId] = useState(null);
  const esRef = useRef(null);                     // EventSource 핸들
  const [botName, setBotName] = useState('');
  const [botStatus, setBotStatus] = useState(null);
  const botsPollRef = useRef(null);

  // ── 설정 상태 (봇별)
  const [symbol, setSymbol] = useState('ETH-USDT');
  const [marginMode, setMarginMode] = useState('CROSS'); // CROSS/ISOLATED
  const [side, setSide] = useState('BUY');               // BUY/SELL
  const [leverage, setLeverage] = useState(10);
  const [tp_percent, setTp_percent] = useState(0.5);
  const [repeat_mode, setRepeat_mode] = useState(true);
  // dca_config는 [[gap, usdt], ...]
  const [tiers, setTiers] = useState([
    [0, 50],
    [0.4, 50],
  ]);

  //통계
  const [gran, setGran] = useState('w'); // 'w' | 'd'
  const [range, setRange] = useState(12); // weeks=12 or days=30 등
  const [tradesWindow, setTradesWindow] = useState('7d'); // 버튼 토글에서 사용 중
  const [tradesSummary, setTradesSummary] = useState(null);
  const [tradesDonut, setTradesDonut] = useState(null);
  const formatUSDT = (v) => `${(v ?? 0).toLocaleString(undefined, { maximumFractionDigits: 2 })} USDT`;
  const [profitWindow, setProfitWindow] = useState('7d');
  const [profit, setProfit] = useState({ pnl_usdt: 0, pnl_pct: 0, wins: 0, losses: 0 });
  const [profitDonut, setProfitDonut] = useState(null);

  //초기자산
  const [baseline, setBaseline] = useBaseline(10000);  // ✅ 기본값 10000 USDT
  

  // accunt balance chart
  const [balanceChart, setBalanceChart] = useState(null);
  const fetchSeries = async (g = gran, r = range) => {
    const q = g === 'd' ? `granularity=d&days=${r}` : `granularity=w&weeks=${r}`;
    const res = await fetch(`/api/balance/series?${q}`);
    if (!res.ok) throw new Error(await res.text());
    const { series } = await res.json();
    const labels = series.map(s => s.date);
    const values = series.map(s => s.balance);
    setBalanceChart(buildAccountBalanceChart({ labels, values }));
  };

  useEffect(() => { fetchSeries(gran, range); }, [gran, range]); // 토글/범위 변경 시

  useEffect(() => {
    let aborted = false;

    async function load() {
      try {
        const res = await fetch(`/api/trades/summary?window=${tradesWindow}`, {
          cache: 'no-store'
        });
        const data = await res.json();
        if (aborted) return;

        setTradesSummary(data);
        setTradesDonut(
          buildTradesDonut({
            newCount: data?.entries?.new ?? 0,
            dcaCount: data?.entries?.dca ?? 0
          })
        );
      } catch (e) {
        console.error('load trades summary failed', e);
      }
    }

    load();
    return () => { aborted = true; };
  }, [tradesWindow]);

  // ── 포맷터 (그대로 유지)
  const fmt = (n, { money = false } = {}) =>
    n == null ? '…' : n.toLocaleString(undefined, { maximumFractionDigits: 2 });

  // 공통 적용 함수 (요약값 세팅)
  const applySummary = (d) => {
    if (!d) return;
    setCurrency(d?.asset || d?.currency || 'USDT');
    setBalance(Number(d?.balance ?? 0));
    setEquity(Number(d?.equity ?? 0));
    setAvailable(Number(d?.available_margin ?? 0));
    setLoading(false);
  };

  // ─────────────────────────────────────────────────────────────────────────────
  // ✅ 토큰 자동 첨부 fetch 래퍼
  // ─────────────────────────────────────────────────────────────────────────────
  // ✅ 백엔드 기본 URL (환경변수로 오버라이드 가능)
  const API_BASE =
    (typeof import.meta !== 'undefined' && import.meta.env && import.meta.env.VITE_API_BASE_URL) ||
    "https://artcokr.org";

  // ✅ 토큰 자동 첨부 + 절대경로 보정
  const authedFetch = async (url, options = {}) => {
    const token = localStorage.getItem('auth_token');
    const isAbsolute = /^https?:\/\//i.test(url);
    const fullUrl = isAbsolute ? url : new URL(url, API_BASE.endsWith('/') ? API_BASE : API_BASE + '/').toString();

    const mergedHeaders = {
      ...(options.headers || {}),
      ...(token ? { Authorization: `Bearer ${token}` } : {})
    };

    return fetch(fullUrl, { ...options, headers: mergedHeaders });
  };
  // ─────────────────────────────────────────────────────────────────────────────
  // 봇 목록 불러오기 & 기본 선택
  // ─────────────────────────────────────────────────────────────────────────────
  const loadBots = async () => {
    try {
      const res = await authedFetch('/api/bots'); // ✅ 토큰 추가
      if (!res.ok) throw new Error(await res.text());
      const list = await res.json();
      setBots(list || []);
      if (!currentBotId && list?.length) {
        setCurrentBotId(list[0].id);
      }
    } catch (e) {
      console.warn('loadBots failed', e);
      setError(e.message || 'loadBots failed');
      setBots([]);
      setCurrentBotId(null);
    }
  };

  useEffect(() => {
    loadBots();
    // 글로벌 스냅샷 1회 (초기 로딩 체감 개선)
    (async () => {
      try {
        const r = await authedFetch('/api/account/summary'); // ✅ 토큰 추가
        if (r.ok) applySummary(await r.json());
        else setError(`summary HTTP ${r.status}`);
      } catch (e) {
        setError(e.message || 'summary fetch failed');
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ─────────────────────────────────────────────────────────────────────────────
  // 선택된 봇의 설정 로드
  // ─────────────────────────────────────────────────────────────────────────────
  const loadConfig = async (botId) => {
    if (!botId) return;
    try {
      const res = await authedFetch(`/api/bots/${encodeURIComponent(botId)}/config`); // ✅ 토큰 추가
      if (!res.ok) throw new Error(await res.text());
      const cfg = await res.json();

      setBotName(cfg.name || botId);

      if (cfg.symbol) setSymbol(cfg.symbol);
      if (cfg.side) setSide(cfg.side);
      if (cfg.margin_mode) setMarginMode(String(cfg.margin_mode).toUpperCase());
      if (cfg.leverage != null) setLeverage(Number(cfg.leverage) || 1);
      if (cfg.tp_percent != null) setTp_percent(Number(cfg.tp_percent) || 0);
      if (cfg.repeat_mode != null) setRepeat_mode(!!cfg.repeat_mode);
      if (Array.isArray(cfg.dca_config)) setTiers(cfg.dca_config);
    } catch (e) {
      console.warn('loadConfig failed', e);
      setError(e.message || 'loadConfig failed');
    }
  };

  // ─────────────────────────────────────────────────────────────────────────────
  // 선택된 봇의 요약(EventSource) 구독 + 폴백
  // ─────────────────────────────────────────────────────────────────────────────
  const pollRef = useRef(null);

  const stopPolling = () => {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  };

  const startPolling = (url) => {
    stopPolling();
    const fetchOnce = async () => {
      try {
        const r = await authedFetch(url); // ✅ 토큰 추가
        if (!r.ok) return;
        const d = await r.json();
        applySummary(d);
      } catch (e) {
        // 조용히 폴백 유지
      }
    };
    fetchOnce();
    pollRef.current = setInterval(fetchOnce, 10000); // 10초 간격
  };

  const openGlobalES = () => {
    closeES();
    try {
      // ⚠️ SSE는 헤더에 토큰을 못 실음 → 쿼리스트링으로 전달(백엔드가 지원해야 함)
      const token = localStorage.getItem('auth_token');
      const url = token ? `/api/account/summary/stream?token=${encodeURIComponent(token)}` : '/api/account/summary/stream';
      const es = new EventSource(url);
      esRef.current = es;
      setLoading(true);

      es.onmessage = (e) => {
        try {
          applySummary(JSON.parse(e.data));
        } catch {}
      };
      es.onerror = () => {
        closeES();
        startPolling('/api/account/summary'); // 글로벌 SSE 실패 → 글로벌 폴링
      };
    } catch {
      startPolling('/api/account/summary'); // ES 생성 실패 → 글로벌 폴링
    }
  };

  const openES = () => {
    closeES();
    stopPolling();
    try {
      const token = localStorage.getItem('auth_token');
      const urlBase = `/api/account/summary/stream`;
      const url = token ? `${urlBase}?token=${encodeURIComponent(token)}` : urlBase;

      const es = new EventSource(url);
      esRef.current = es;
      setLoading(true);

      es.onmessage = (e) => {
        try {
          applySummary(JSON.parse(e.data));
        } catch {}
      };

      es.onerror = async () => {
        closeES();
        try {
          const r = await authedFetch(`/api/account/summary`);
          if (r.ok) {
            applySummary(await r.json());
          }
        } catch {}
        // 실패해도 조용히 유지
      };
    } catch {
      // ES 생성 실패시 폴링
      startPolling('/api/account/summary');
    }
  };

  const closeES = () => {
    try {
      if (esRef.current) {
        esRef.current.close();
        esRef.current = null;
      }
    } catch {}
  };

  // 봇 변경 시 설정/요약 모두 갱신 + 정리
  useEffect(() => {
    // 봇 바뀌면 설정만 봇별로 로딩하고, 요약은 글로벌로 유지
    if (!currentBotId) return;
    loadConfig(currentBotId);
    openES(); // ← botId 제거 (글로벌)
    return () => {
      closeES();
      stopPolling();
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentBotId]);

  // 컴포넌트 언마운트 시 정리
  useEffect(() => {
    return () => {
      closeES();
      stopPolling();
    };
  }, []);

  // 선택된 봇의 상태를 3초마다 폴링
  useEffect(() => {
    if (!currentBotId) {
      setBotStatus(null);
      return;
    }

    let stop = false;

    const tick = async () => {
      try {
        const r = await authedFetch(`/api/bots/${encodeURIComponent(currentBotId)}/status`); // ✅ 토큰 추가
        if (r.ok) {
          const j = await r.json();
          if (!stop) setBotStatus(j);
        }
      } catch (e) {
        // 네트워크 오류는 조용히 무시
      }
    };

    // 즉시 1회 + 인터벌
    tick();
    const id = setInterval(tick, 3000);

    return () => {
      stop = true;
      clearInterval(id);
    };
  }, [currentBotId]);

  // 봇 목록 주기 폴링 (5초)
  useEffect(() => {
    // 이미 한 번 loadBots()는 componentDidMount에서 호출됨
    botsPollRef.current = setInterval(() => {
      loadBots();
    }, 5000);
    return () => {
      if (botsPollRef.current) clearInterval(botsPollRef.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ─────────────────────────────────────────────────────────────────────────────
  // 티어 조작 핸들러 (배열-의-배열: [gap, usdt])
  // ─────────────────────────────────────────────────────────────────────────────
  const addTier = () => setTiers(prev => [...prev, [1.0, 50]]);
  const removeTier = (i) => setTiers(prev => prev.filter((_, idx) => idx !== i));
  const updateTierGap = (i, gap) =>
    setTiers(prev => prev.map((t, idx) => (idx === i ? [Number(gap) || 0, t[1]] : t)));
  const updateTierUsdt = (i, usdt) =>
    setTiers(prev => prev.map((t, idx) => (idx === i ? [t[0], Number(usdt) || 0] : t)));

  // ─────────────────────────────────────────────────────────────────────────────
  // 로컬 리셋 & 저장/실행 제어
  // ─────────────────────────────────────────────────────────────────────────────
  const handleResetLocal = () => {
    setSymbol('ETH-USDT');
    setSide('BUY');
    setMarginMode('CROSS');
    setLeverage(10);
    setTp_percent(4.5);
    setRepeat_mode(false);
    setTiers([
      [0, 50],
      [0.4, 50],
    ]);
  };

  const handleSave = async () => {
    if (!currentBotId) return alert('Select a bot first.');
    const payload = {
      name: botName,
      symbol,
      side,                    // BUY / SELL
      margin_mode: marginMode, // CROSS / ISOLATED
      leverage,
      tp_percent,
      repeat_mode,
      dca_config: tiers        // [[gap, usdt], ...]
    };
    try {
      const res = await authedFetch(`/api/bots/${encodeURIComponent(currentBotId)}/config`, { // ✅ 토큰 추가
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (!res.ok) throw new Error(await res.text());
      alert('Saved!');
      loadBots();
    } catch (e) {
      console.error(e);
      setError(e.message || 'save failed');
      alert('Save failed');
    }
  };

  const startBot = async () => {
    if (!currentBotId) return;
    try {
      const r = await authedFetch(`/api/bots/${encodeURIComponent(currentBotId)}/start`, { method: 'POST' }); // ✅
      if (!r.ok) throw new Error(await r.text());
      loadBots();
      alert('Bot started.');
    } catch (e) {
      setError(e.message || 'start failed');
      alert('Start failed');
    }
  };

  const stopBot = async () => {
    if (!currentBotId) return;
    try {
      const r = await authedFetch(`/api/bots/${encodeURIComponent(currentBotId)}/stop`, { method: 'POST' }); // ✅
      if (!r.ok) throw new Error(await res.text());
      loadBots();
      alert('Bot stopped.');
    } catch (e) {
      setError(e.message || 'stop failed');
      alert('Stop failed');
    }
  };

  const createBot = async () => {
    try {
      const r = await authedFetch('/api/bots', { method: 'POST' }); // ✅
      if (!r.ok) throw new Error(await r.text());
      const j = await r.json();
      await loadBots();
      if (j?.id) setCurrentBotId(j.id);
    } catch (e) {
      setError(e.message || 'create failed');
      alert('Create failed');
    }
  };

  // ─────────────────────────────────────────────────────────────────────────────
  // 통계 제어
  // ─────────────────────────────────────────────────────────────────────────────

  useEffect(() => {
    (async () => {
      try {
        await fetch('/api/account/summary').catch(() => {});
        const qs = gran === 'd'
          ? `granularity=d&days=${range}`
          : `granularity=w&weeks=${range}`;

        const res = await fetch(`/api/balance/series?${qs}`);
        if (!res.ok) throw new Error(await res.text());

        const { series } = await res.json();
        const labels = series.map(s => s.date);
        const values = series.map(s => s.balance);
        setBalanceChart(buildAccountBalanceChart({ labels, values }));
      } catch (e) {
        console.warn('failed to load balance series', e);
      }
    })();
  }, [gran, range]);

  useEffect(() => {
    let aborted = false;
    (async () => {
      try {
        const res = await fetch(
          `/api/profit/summary?window=${profitWindow}&baseline=${baseline}`,
          { cache: 'no-store' }
        );
        const data = await res.json();
        if (aborted) return;
        setProfit(data);
        setProfitDonut(buildProfitDonut({ wins: data.wins, losses: data.losses }));
      } catch (e) {
        console.error('profit load failed', e);
      }
    })();
    return () => { aborted = true; };
  }, [profitWindow, baseline]); // ← baseline 의존성 추가

  function useProfitKpi(baseline) {
    const [kpi, setKpi] = useState({ all: { pnl_usdt: 0 }, mtd: { pnl_usdt: 0, pnl_pct: 0, month_utc: '' } });
    useEffect(() => {
      let aborted = false;
      (async () => {
        try {
          const res = await fetch(`/api/profit/kpi?baseline=${baseline}`, { cache: 'no-store' });
          const json = await res.json();
          if (!aborted) setKpi(json);
        } catch (e) {
          console.error('profit kpi load failed', e);
        }
      })();
      return () => { aborted = true; };
    }, [baseline]);
    return kpi;
  }
  const kpi = useProfitKpi(baseline);

  // ─────────────────────────────────────────────────────────────────────────────
  // UI
  // ─────────────────────────────────────────────────────────────────────────────
  return (
    <React.Fragment>
      {/* 🔴 에러 배너 (있으면 맨 위에 표시) */}
      {error && (
        <div style={{ color: '#900', background: '#fee', padding: 10, marginBottom: 10, border: '1px solid #f88', borderRadius: 6 }}>
          에러: {String(error)}
        </div>
      )}
      
      <Row>
        {/* 우측 상단: 요약 카드/차트 (기존 유지) */}
        <Col lg={12} md={12}>
          <Row>
            {/* Account Balance */}
            <Col md={6} xl={3}>
              <OrderCard
                params={{
                  title: 'Account Balance',
                  class: 'bg-c-blue',
                  icon: 'feather icon-shopping-cart',
                  primaryText: loading ? '…' : fmt(balance, { money: true }),
                  extraText: currency
                }}
              />
            </Col>

            {/* Equity */}
            <Col md={6} xl={3}>
              <OrderCard
                params={{
                  title: 'Equity',
                  class: 'bg-c-green',
                  icon: 'feather icon-tag',
                  primaryText: loading ? '…' : fmt(equity, { money: true }),
                  extraText: currency
                }}
              />
            </Col>

            {/* Available Margin */}
            <Col md={6} xl={3}>
              <OrderCard
                params={{
                  title: 'Available Margin',
                  class: 'bg-c-yellow',
                  icon: 'feather icon-repeat',
                  primaryText: loading ? '…' : fmt(available, { money: true }),
                  extraText: currency
                }}
              />
            </Col>
            <Col md={6} xl={3}>
              <OrderCard
                params={{
                  title: 'Total Profit',
                  class: 'bg-c-red',
                  icon: 'feather icon-award',
                  // 전체 누적 수익 (전 기간)
                  primaryText: `${kpi.all?.pnl_usdt >= 0 ? '+' : ''}${(kpi.all?.pnl_usdt || 0).toLocaleString()} USDT`,
                  // 보조 라벨
                  secondaryText: 'This Month',
                  // 이번 달(UTC) 수익 USDT 표시
                  extraText: `${kpi.mtd?.pnl_usdt >= 0 ? '+' : ''}${(kpi.mtd?.pnl_usdt || 0).toLocaleString()} USDT`
                }}
              />
            </Col>

            {/* 차트 영역 (그대로) */}
            <Col md={12} xl={6}>
              <Card>
                <Card.Header className="d-flex justify-content-between align-items-center">
                  <h5 className="mb-0">
                    Account Balance ({gran === 'd' ? 'Daily' : 'Weekly'})
                  </h5>

                  <div className="d-flex gap-2 align-items-center">
                    {/* Baseline 입력칸 */}
                    <div className="d-flex align-items-center">
                      <small className="me-2 text-muted">Baseline</small>
                      <Form.Control
                        type="number"
                        min={1}
                        step={1}
                        value={baseline}
                        onChange={(e) =>
                          setBaseline(Math.max(1, Number(e.target.value || 0)))
                        }
                        size="sm"
                        className="w-auto"
                        style={{ width: 110 }}
                        placeholder=""  // 필요시 "USDT" 표시 가능
                      />
                    </div>

                    {/* 기간 선택 버튼 */}
                    <ButtonGroup>
                      <Button
                        variant={gran === 'd' ? 'primary' : 'outline-primary'}
                        onClick={() => {
                          setGran('d');
                          setRange(30);
                        }} // 기본 30일
                      >
                        Daily
                      </Button>
                      <Button
                        variant={gran === 'w' ? 'primary' : 'outline-primary'}
                        onClick={() => {
                          setGran('w');
                          setRange(12);
                        }} // 기본 12주
                      >
                        Weekly
                      </Button>
                    </ButtonGroup>
                  </div>
                </Card.Header>

                <Card.Body className="ps-4 pt-4 pb-0">
                  {balanceChart && <Chart {...balanceChart} />}
                </Card.Body>
              </Card>
            </Col>

            <Col md={12} xl={6}>
              <Row>
                <Col sm={6}>
                  <Card>
                    <Card.Body>
                      <Row className="align-items-start card-title-row">
                        <Col sm="auto"><span>Trades</span></Col>
                        <Col className="text-end">
                          {/* 기간 토글 (선택) */}
                          <div className="mb-2">
                            <ButtonGroup size="sm">
                              <Button
                                variant={tradesWindow === '7d' ? 'primary' : 'outline-primary'}
                                onClick={() => setTradesWindow('7d')}
                              >
                                7d
                              </Button>
                              <Button
                                variant={tradesWindow === '30d' ? 'primary' : 'outline-primary'}
                                onClick={() => setTradesWindow('30d')}
                              >
                                30d
                              </Button>
                            </ButtonGroup>
                          </div>

                          <h2 className="mb-0">{formatUSDT(tradesSummary?.total_volume)}</h2>
                          <span className="text-muted">
                            Fills&nbsp;{tradesSummary?.fills_count ?? 0}
                          </span>
                        </Col>
                      </Row>

                      {/* 도넛 차트: 신규 vs DCA */}
                      {tradesDonut && <Chart {...tradesDonut} />}

                      <Row className="mt-3 text-center">
                        <Col>
                          <h3 className="m-0">
                            <i className="fas fa-circle f-10 mx-2 text-success" />
                            {tradesSummary?.entries?.new ?? 0}
                          </h3>
                          <span className="ms-3">New</span>
                        </Col>
                        <Col>
                          <h3 className="m-0">
                            <i className="fas fa-circle text-primary f-10 mx-2" />
                            {tradesSummary?.entries?.dca ?? 0}
                          </h3>
                          <span className="ms-3">DCA</span>
                        </Col>
                      </Row>
                    </Card.Body>
                  </Card>
                </Col>
                {/* Profit */}
                <Col sm={6}>
                  <Card className="bg-primary text-white">
                    <Card.Body>
                      {/* 상단 타이틀 + 기간 토글 */}
                      <Row className="align-items-center justify-content-between">
                        <Col sm="auto">
                          <span>Profit</span>
                          {/* ⬇️ Profit 밑에 Baseline 입력 추가 */}
                          <div className="mt-1 d-flex align-items-center">
                          </div>
                        </Col>
                        <Col sm="auto">
                          <ButtonGroup size="sm">
                            <Button
                              variant={profitWindow === '7d' ? 'light' : 'outline-light'}
                              onClick={() => setProfitWindow('7d')}
                            >
                              7d
                            </Button>
                            <Button
                              variant={profitWindow === '30d' ? 'light' : 'outline-light'}
                              onClick={() => setProfitWindow('30d')}
                            >
                              30d
                            </Button>
                          </ButtonGroup>
                        </Col>
                      </Row>

                      {/* 금액 + 수익률 (오른쪽 정렬) */}
                      <Row className="mt-2">
                        <Col className="text-end">
                          <h2 className="mb-0 text-white">
                            {profit.pnl_usdt >= 0
                              ? `+${profit.pnl_usdt.toLocaleString()} USDT`
                              : `${profit.pnl_usdt.toLocaleString()} USDT`}
                          </h2>
                          <span className="text-white">
                            {profit.pnl_pct >= 0 ? `+${profit.pnl_pct}%` : `${profit.pnl_pct}%`}
                            <i
                              className={`feather ${
                                profit.pnl_pct >= 0 ? 'icon-trending-up' : 'icon-trending-down'
                              } ms-1`}
                            />
                          </span>
                        </Col>
                      </Row>

                      {/* 도넛 */}
                      {profitDonut && <Chart {...profitDonut} />}

                      {/* 레이블/카운트 */}
                      <Row className="mt-3 text-center">
                        <Col>
                          <h3 className="m-0 text-white">
                            <i className="fas fa-circle f-10 mx-2 text-success" />
                            {profit.wins ?? 0}
                          </h3>
                          <span className="ms-3 text-white-50">Winning</span>
                        </Col>
                        <Col>
                          <h3 className="m-0 text-white">
                            <i className="fas fa-circle f-10 mx-2 text-white" />
                            {profit.losses ?? 0}
                          </h3>
                          <span className="ms-3 text-white-50">Losing</span>
                        </Col>
                      </Row>
                    </Card.Body>
                  </Card>
                </Col>


              </Row>
            </Col>

            {/* 좌측: 봇 리스트 패널 */}
            <Col lg={4} md={12}>
              <Card style={{ height: 575, overflow: 'hidden' }}>
                <Card.Header className="d-flex justify-content-between align-items-center">
                  <h5 className="mb-0">Bots</h5>
                  <Button size="sm" variant="primary" onClick={createBot}>+ New</Button>
                </Card.Header>
                <PerfectScrollbar options={{ suppressScrollX: true }}>
                  <ListGroup variant="flush" className="p-2">
                    {bots.length === 0 && (
                      <div className="text-muted p-3">No bots. Click “New”.</div>
                    )}
                    {bots.map((b) => {
                      const active = b.id === currentBotId;
                      return (
                        <ListGroup.Item
                          key={b.id}
                          action
                          active={active}
                          onClick={() => setCurrentBotId(b.id)}
                          className="d-flex justify-content-between align-items-center"
                        >
                          <div className="text-truncate" style={{ maxWidth: 180 }}>
                            {b.name || b.id}
                          </div>
                          {b.status && (
                            <Badge bg={b.status === 'running' ? 'success' : 'secondary'}>
                              {b.status}
                            </Badge>
                          )}
                        </ListGroup.Item>
                      );
                    })}
                  </ListGroup>
                </PerfectScrollbar>
                <div className="d-flex gap-2 p-2 border-top">
                  <Button variant="success" size="sm" className="w-50" onClick={startBot} disabled={!currentBotId}>
                    Start
                  </Button>
                  <Button variant="outline-danger" size="sm" className="w-50" onClick={stopBot} disabled={!currentBotId}>
                    Stop
                  </Button>
                </div>
              </Card>
            </Col>


            {/* 우측: 선택된 봇 설정 (전체 스크롤) */}
            <Col lg={8} md={12} className="d-flex align-items-stretch">
              <Card className="w-100" style={{ height: 575, overflow: 'hidden' }}>

                <Card.Header className="d-flex justify-content-between align-items-center">
                  <div className="d-flex align-items-center gap-2">
                    <h5 className="mb-0">
                      Bot Settings {currentBotId ? `· ${currentBotId}` : ''}
                    </h5>
                    {botStatus && (
                      <Badge bg={botStatus.running ? 'success' : 'secondary'}>
                        {botStatus.running ? 'running' : 'stopped'}
                      </Badge>
                    )}
                  </div>
                  <div className="d-flex gap-2">
                    <Button variant="outline-secondary" size="sm" onClick={handleResetLocal}>Reset</Button>
                    <Button variant="primary" size="sm" onClick={handleSave} disabled={!currentBotId}>Save</Button>
                  </div>
                </Card.Header>

                {/* 🔴 여기부터 전부 스크롤 */}
                <PerfectScrollbar options={{ suppressScrollX: true }}>
                  <Card.Body style={{ padding: 16 }}>
                    {/* 상단 옵션 */}
                    
                    <Row className="g-3">
                      <Col sm={12} md={6}>
                        <Form.Label>Bot Name</Form.Label>
                        <Form.Control
                          type="text"
                          placeholder="e.g. ETH Cross Long"
                          value={botName}
                          onChange={e => setBotName(e.target.value)}
                          disabled={!currentBotId}
                        />
                      </Col>

                      <Col sm={6} md={4}>
                        <Form.Label>Symbol</Form.Label>
                        <Form.Select value={symbol} onChange={e => setSymbol(e.target.value)} disabled={!currentBotId}>
                          <option value="ETH-USDT">ETH-USDT</option>
                          <option value="BTC-USDT">BTC-USDT</option>
                        </Form.Select>
                      </Col>

                      <Col sm={6} md={4}>
                        <Form.Label>Margin Mode</Form.Label>
                        <Form.Select value={marginMode} onChange={e => setMarginMode(e.target.value)} disabled={!currentBotId}>
                          <option value="CROSS">Cross</option>
                          <option value="ISOLATED">Isolated</option>
                        </Form.Select>
                      </Col>

                      <Col sm={6} md={4}>
                        <Form.Label>Side</Form.Label>
                        <Form.Select value={side} onChange={e => setSide(e.target.value)} disabled={!currentBotId}>
                          <option value="BUY">Long</option>
                          <option value="SELL">Short</option>
                        </Form.Select>
                      </Col>

                      <Col sm={6} md={4}>
                        <Form.Label>Leverage</Form.Label>
                        <InputGroup>
                          <Form.Control
                            type="number"
                            min={1}
                            max={125}
                            value={leverage}
                            onChange={e => setLeverage(Number(e.target.value) || 1)}
                            disabled={!currentBotId}
                          />
                          <InputGroup.Text>x</InputGroup.Text>
                        </InputGroup>
                      </Col>

                      <Col sm={6} md={4}>
                        <Form.Label>TP % (from avg entry)</Form.Label>
                        <InputGroup>
                          <Form.Control
                            type="number"
                            step="0.01"
                            value={tp_percent}
                            onChange={e => setTp_percent(Number(e.target.value) || 0)}
                            disabled={!currentBotId}
                          />
                          <InputGroup.Text>%</InputGroup.Text>
                        </InputGroup>
                      </Col>

                      <Col sm={6} md={4} className="d-flex align-items-end">
                        <Form.Check
                          type="switch"
                          id="repeat-switch"
                          label="Repeat after TP"
                          checked={repeat_mode}
                          onChange={e => setRepeat_mode(e.target.checked)}
                          disabled={!currentBotId}
                        />
                      </Col>
                    </Row>

                    {/* 티어 리스트 */}
                    <div className="p-2 mt-3">
                      {tiers.map((t, idx) => (
                        <Row key={idx} className="g-2 align-items-end mb-2">
                          <Col xs={12} md={4}>
                            <Form.Label className="mb-1">Tier {idx + 1} — USDT</Form.Label>
                            <Form.Control
                              type="number"
                              min={0}
                              step="0.01"
                              value={t[1]}
                              onChange={e => updateTierUsdt(idx, e.target.value)}
                              disabled={!currentBotId}
                            />
                          </Col>

                          <Col xs={12} md={4}>
                            <Form.Label className="mb-1">Gap % (cumulative)</Form.Label>
                            <InputGroup>
                              <Form.Control
                                type="number"
                                step="0.01"
                                value={t[0]}
                                onChange={e => updateTierGap(idx, e.target.value)}
                                disabled={!currentBotId}
                              />
                              <InputGroup.Text>%</InputGroup.Text>
                            </InputGroup>
                          </Col>

                          <Col xs="auto" className="mt-1 mt-md-0">
                            <Button variant="outline-danger" onClick={() => removeTier(idx)} disabled={!currentBotId}>
                              Remove
                            </Button>
                          </Col>
                        </Row>
                      ))}

                      <div className="mt-2">
                        <Button variant="outline-primary" onClick={addTier} disabled={!currentBotId}>
                          + Add Tier
                        </Button>
                      </div>
                    </div>

                    {/* 하단 버튼 */}
                    <div className="d-flex justify-content-end gap-2 mt-3">
                      <Button variant="secondary" onClick={handleResetLocal}>Reset</Button>
                      <Button variant="primary" onClick={handleSave} disabled={!currentBotId}>Save</Button>
                    </div>
                  </Card.Body>
                </PerfectScrollbar>
              </Card>
            </Col>

          </Row>
        </Col>
      </Row>
    </React.Fragment>
  );
};

export default DashAnalytics;
