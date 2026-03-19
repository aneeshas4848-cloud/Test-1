import json, uuid, ssl, math, pathlib, time as time_mod
from datetime import datetime, timezone, date, time, timedelta
from typing import Dict, Optional, Tuple, List

import requests, certifi, websocket

from market import MarketDataFeed_pb2
from market.candle_builder import CandleBuilder
from strategy.ema_crossover import EMACrossoverStrategy
from risk.risk_manager import RiskManager, RiskConfig
from execution.paper_broker import PaperBroker
from reporting.trade_reporter import TradeReporter

WATCHLIST_JSON = pathlib.Path("data/nifty200_watchlist.json")

CAPITAL = 100000
RISK_PER_TRADE_PCT = 0.01
MAX_DAILY_LOSS_PCT = 0.03
MAX_TRADES_PER_DAY = 3

TIMEFRAME_MINUTES = 5
STOP_LOSS_PCT = 0.008

PROFIT_LADDER_STEP = 0.01
PROFIT_LADDER_MAX = 0.10
PROFIT_LADDER_BUFFER = 0.002
PROFIT_LADDER_CONFIRM_CANDLES = 1

BATCH_SIZE = 150
TOP_K_MONITOR = 5

MIN_ABS_MOVE_PCT = 0.4
MAX_ABS_MOVE_PCT = 5.0
MIN_LTP = 20.0
MAX_LTP = 3000.0
MIN_VOLUME = 100000
MIN_AFFORDABLE_SHARES = 1
EMA_GAP_PCT = 0.0005

SCAN_INTERVAL_SEC = 60
MAX_FOCUS_CANDLES = 5
RESCAN_INTERVAL_MIN = 20
SYMBOL_COOLDOWN_MIN = 45
CHOPPY_STREAK_THRESHOLD = 2
CHOPPY_PAUSE_MIN = 5

IST_TZ = timezone(timedelta(hours=5, minutes=30))
SKIP_FIRST_MINUTES = 10
NO_NEW_ENTRIES_AFTER_IST = time(15, 15)
FORCE_EXIT_FROM_IST = time(15, 25)
STOP_BOT_AFTER_IST = time(15, 30)

WS_PING_INTERVAL = 30
WS_PING_TIMEOUT = 20
WS_RECONNECT_DELAY_SEC = 5
WS_MAX_RETRIES_PER_SESSION = 5


def now_ist() -> datetime:
    return datetime.now(timezone.utc).astimezone(IST_TZ)


def ist_time_now() -> time:
    return now_ist().time()


def in_early_skip_window_ist() -> bool:
    ist = now_ist()
    market_open = ist.replace(hour=9, minute=15, second=0, microsecond=0)
    return ist < market_open + timedelta(minutes=SKIP_FIRST_MINUTES)


def market_stopped() -> bool:
    return ist_time_now() >= STOP_BOT_AFTER_IST


def no_new_entries_now() -> bool:
    return ist_time_now() >= NO_NEW_ENTRIES_AFTER_IST


def load_token() -> str:
    return pathlib.Path("access_token.txt").read_text(encoding="utf-8").strip()


def load_watchlist_symbol_to_key() -> Dict[str, str]:
    if not WATCHLIST_JSON.exists():
        raise SystemExit("Missing watchlist json. Build it first.")
    return json.loads(WATCHLIST_JSON.read_text(encoding="utf-8"))


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def fetch_ltp_v3(token: str, keys: list[str]) -> dict:
    url = "https://api.upstox.com/v3/market-quote/ltp"
    r = requests.get(
        url,
        headers={"Accept": "application/json", "Authorization": f"Bearer {token}"},
        params={"instrument_key": ",".join(keys)},
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get("data", {})


def token_to_symbol(instrument_token: str) -> Optional[str]:
    return instrument_token.split(":", 1)[1].strip() if ":" in instrument_token else None


def ms_to_dt_local(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone()


def symbol_in_cooldown(symbol: str, cooldown_until: Dict[str, datetime]) -> bool:
    until = cooldown_until.get(symbol)
    return bool(until and now_ist() < until)


def breakout_confirmed(prev_candle, candle, bias: str) -> bool:
    if prev_candle is None:
        return False
    return candle.close > prev_candle.high if bias == "LONG" else candle.close < prev_candle.low


def pick_top_candidates_bi_directional(
    token: str,
    sym_to_key: Dict[str, str],
    cooldown_until: Dict[str, datetime],
    top_k: int = TOP_K_MONITOR,
) -> List[Tuple[float, str, str, str, float, int, float, float]]:
    candidates = []

    for batch in chunks(list(sym_to_key.values()), BATCH_SIZE):
        data = fetch_ltp_v3(token, batch)
        for inst_token, v in data.items():
            sym = token_to_symbol(inst_token)
            if not sym or symbol_in_cooldown(sym, cooldown_until):
                continue

            inst_key = sym_to_key.get(sym)
            if not inst_key:
                continue

            ltp = float(v.get("last_price") or 0.0)
            cp = float(v.get("cp") or 0.0)
            vol = float(v.get("volume") or 0.0)

            if ltp <= 0 or cp <= 0 or ltp < MIN_LTP or ltp > MAX_LTP or vol < MIN_VOLUME:
                continue

            pct = (ltp - cp) / cp * 100.0
            ap = abs(pct)
            if ap < MIN_ABS_MOVE_PCT or ap > MAX_ABS_MOVE_PCT:
                continue

            max_shares = int(CAPITAL // ltp)
            if max_shares < MIN_AFFORDABLE_SHARES:
                continue

            bias = "LONG" if pct > 0 else "SHORT"
            score = ap * 6.0 + math.log1p(vol)
            candidates.append((score, sym, inst_key, bias, ltp, max_shares, pct, vol))

    candidates.sort(reverse=True, key=lambda x: x[0])
    return candidates[:top_k]


def get_authorized_ws_url(token: str) -> str:
    r = requests.get(
        "https://api.upstox.com/v3/feed/market-data-feed/authorize",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["data"]["authorized_redirect_uri"]


def make_sub_msg(instrument_keys: List[str]) -> bytes:
    return json.dumps(
        {
            "guid": uuid.uuid4().hex,
            "method": "sub",
            "data": {"mode": "ltpc", "instrumentKeys": instrument_keys},
        }
    ).encode("utf-8")


def record_exit_and_print(closed_pos, symbol: str, risk, reporter):
    pnl = closed_pos.pnl()
    risk.record_trade_closed(date.today(), pnl)
    reporter.record_exit(
        symbol,
        closed_pos.qty,
        float(closed_pos.exit_price),
        float(pnl),
        reason=str(closed_pos.exit_reason or "RULE_EXIT"),
    )


def run_topk_session(token: str, candidates, risk, broker, reporter) -> dict:
    session_result = {
        "trade_opened": False,
        "trade_closed": False,
        "stop_requested": False,
        "session_reason": "",
    }

    by_key = {}
    for score, sym, inst_key, bias, ltp, max_shares, pct, vol in candidates:
        by_key[inst_key] = {
            "symbol": sym,
            "bias": bias,
            "score": score,
            "ltp": ltp,
            "max_shares": max_shares,
            "pct": pct,
            "vol": vol,
            "builder": CandleBuilder(timeframe_minutes=TIMEFRAME_MINUTES),
            "strat": EMACrossoverStrategy(fast=9, slow=21),
            "focus_candle_count": 0,
            "disabled": False,
            "prev_candle": None,
        }

    active_trade_symbol = None
    active_trade_key = None
    session_start_ist = now_ist()
    retries = 0
    instrument_keys = list(by_key.keys())

    while retries < WS_MAX_RETRIES_PER_SESSION:
        if market_stopped():
            session_result["stop_requested"] = True
            session_result["session_reason"] = "MARKET_CLOSE"
            return session_result

        if no_new_entries_now() and not (broker.position and broker.position.is_open):
            session_result["session_reason"] = "ENTRY_CUTOFF"
            return session_result

        ws_url = get_authorized_ws_url(token)

        def on_open(ws):
            ws.send(make_sub_msg(instrument_keys), opcode=websocket.ABNF.OPCODE_BINARY)
            reporter.info("subscribed_topk", {"symbols": [by_key[k]["symbol"] for k in instrument_keys]})

        def on_message(ws, message):
            nonlocal active_trade_symbol, active_trade_key

            if not isinstance(message, (bytes, bytearray)):
                return

            resp = MarketDataFeed_pb2.FeedResponse()
            resp.ParseFromString(message)
            if resp.type not in (MarketDataFeed_pb2.initial_feed, MarketDataFeed_pb2.live_feed):
                return

            if not (broker.position and broker.position.is_open):
                if (now_ist() - session_start_ist).total_seconds() >= RESCAN_INTERVAL_MIN * 60:
                    session_result["session_reason"] = "PERIODIC_RESCAN"
                    ws.close()
                    return

            if broker.position and broker.position.is_open and ist_time_now() >= FORCE_EXIT_FROM_IST:
                last_ltp = None
                if active_trade_key and active_trade_key in resp.feeds and resp.feeds[active_trade_key].HasField("ltpc"):
                    last_ltp = float(resp.feeds[active_trade_key].ltpc.ltp)
                if last_ltp is None:
                    return
                closed_pos = broker.close(now_ist(), last_ltp, reason="FORCED_EOD")
                if closed_pos:
                    record_exit_and_print(closed_pos, active_trade_symbol or "UNKNOWN", risk, reporter)
                session_result["trade_closed"] = True
                session_result["session_reason"] = "FORCED_EOD"
                ws.close()
                return

            if broker.position and broker.position.is_open and active_trade_key in resp.feeds and resp.feeds[active_trade_key].HasField("ltpc"):
                ltpc = resp.feeds[active_trade_key].ltpc
                closed_tick = broker.check_exits(ms_to_dt_local(int(ltpc.ltt)), float(ltpc.ltp), is_candle_close=False)
                if closed_tick:
                    record_exit_and_print(closed_tick, active_trade_symbol, risk, reporter)
                    session_result["trade_closed"] = True
                    session_result["session_reason"] = str(closed_tick.exit_reason or "RULE_EXIT")
                    ws.close()
                    return

            triggered_candidates = []

            for inst_key, feed in resp.feeds.items():
                if inst_key not in by_key or not feed.HasField("ltpc"):
                    continue

                info = by_key[inst_key]
                if info["disabled"]:
                    continue

                ltpc = feed.ltpc
                candle = info["builder"].update(ms_to_dt_local(int(ltpc.ltt)), float(ltpc.ltp))
                if not candle:
                    continue

                if broker.position and broker.position.is_open and inst_key == active_trade_key:
                    closed_candle = broker.check_exits(candle.ts, candle.close, is_candle_close=True)
                    if closed_candle:
                        record_exit_and_print(closed_candle, active_trade_symbol, risk, reporter)
                        session_result["trade_closed"] = True
                        session_result["session_reason"] = str(closed_candle.exit_reason or "RULE_EXIT")
                        ws.close()
                        return

                if broker.position and broker.position.is_open:
                    info["prev_candle"] = candle
                    continue

                info["focus_candle_count"] += 1
                if info["focus_candle_count"] >= MAX_FOCUS_CANDLES:
                    info["disabled"] = True
                    info["prev_candle"] = candle
                    continue

                if in_early_skip_window_ist():
                    info["prev_candle"] = candle
                    continue

                if no_new_entries_now():
                    session_result["session_reason"] = "ENTRY_CUTOFF"
                    ws.close()
                    return

                sig = info["strat"].on_candle_close(candle)
                fast = info["strat"].fast.value or 0.0
                slow = info["strat"].slow.value or 0.0

                ok, reason = risk.can_trade(date.today())
                if not ok:
                    reporter.info("blocked_by_risk", {"reason": reason})
                    session_result["session_reason"] = f"RISK_BLOCK_{reason}"
                    ws.close()
                    return

                if abs(fast - slow) < candle.close * EMA_GAP_PCT:
                    info["prev_candle"] = candle
                    continue

                if info["bias"] == "LONG" and sig.action != "BUY":
                    info["prev_candle"] = candle
                    continue

                if info["bias"] == "SHORT" and sig.action != "SELL":
                    info["prev_candle"] = candle
                    continue

                if not breakout_confirmed(info["prev_candle"], candle, info["bias"]):
                    info["prev_candle"] = candle
                    continue

                triggered_candidates.append(
                    {
                        "inst_key": inst_key,
                        "symbol": info["symbol"],
                        "bias": info["bias"],
                        "score": info["score"],
                        "candle": candle,
                        "max_shares": info["max_shares"],
                        "pct": info["pct"],
                        "vol": info["vol"],
                    }
                )
                info["prev_candle"] = candle

            if broker.position and broker.position.is_open:
                return

            if all(info["disabled"] for info in by_key.values()):
                session_result["session_reason"] = "TOP5_EXHAUSTED"
                ws.close()
                return

            if not triggered_candidates:
                return

            triggered_candidates.sort(key=lambda x: (x["score"], abs(x["pct"]), x["vol"]), reverse=True)
            chosen = triggered_candidates[0]
            candle = chosen["candle"]

            stop_rupees = candle.close * STOP_LOSS_PCT
            if stop_rupees <= 0:
                return

            qty = max(
                1,
                min(
                    int(risk.position_risk_amount() // stop_rupees) or 1,
                    int(CAPITAL // candle.close),
                    chosen["max_shares"],
                ),
            )

            active_trade_symbol = chosen["symbol"]
            active_trade_key = chosen["inst_key"]
            sl = candle.close - stop_rupees if chosen["bias"] == "LONG" else candle.close + stop_rupees

            risk.record_trade_opened(date.today())

            if chosen["bias"] == "LONG":
                broker.open_long(
                    candle.ts,
                    candle.close,
                    qty,
                    sl,
                    take_profit=None,
                    profit_lock_mode="ladder",
                    profit_ladder_step=PROFIT_LADDER_STEP,
                    profit_ladder_max=PROFIT_LADDER_MAX,
                    profit_ladder_buffer=PROFIT_LADDER_BUFFER,
                    profit_ladder_confirm_candles=PROFIT_LADDER_CONFIRM_CANDLES,
                )
                reporter.info(
                    "entry_details",
                    {
                        "side": "LONG",
                        "exit_model": "LADDER",
                        "selected_from_topk": [by_key[k]["symbol"] for k in instrument_keys],
                        "breakout_confirmation": True,
                    },
                )
            else:
                broker.open_short(
                    candle.ts,
                    candle.close,
                    qty,
                    sl,
                    take_profit=None,
                    profit_lock_mode="ladder",
                    profit_ladder_step=PROFIT_LADDER_STEP,
                    profit_ladder_max=PROFIT_LADDER_MAX,
                    profit_ladder_buffer=PROFIT_LADDER_BUFFER,
                    profit_ladder_confirm_candles=PROFIT_LADDER_CONFIRM_CANDLES,
                )
                reporter.info(
                    "entry_details",
                    {
                        "side": "SHORT",
                        "exit_model": "LADDER",
                        "selected_from_topk": [by_key[k]["symbol"] for k in instrument_keys],
                        "breakout_confirmation": True,
                    },
                )

            reporter.record_entry(active_trade_symbol, qty, float(candle.close), float(sl), float(PROFIT_LADDER_STEP))
            session_result["trade_opened"] = True

        def on_error(ws, error):
            reporter.error("websocket_error", {"error": str(error)})

        def on_close(ws, code, reason):
            reporter.info("websocket_closed", {"code": code, "reason": str(reason)})

        try:
            ws = websocket.WebSocketApp(
                ws_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT,
                sslopt={"cert_reqs": ssl.CERT_REQUIRED, "ca_certs": certifi.where()},
            )

            if session_result["trade_closed"] or session_result["session_reason"] in {
                "TOP5_EXHAUSTED",
                "ENTRY_CUTOFF",
                "FORCED_EOD",
                "MARKET_CLOSE",
                "PERIODIC_RESCAN",
            }:
                return session_result

            if market_stopped():
                session_result["stop_requested"] = True
                session_result["session_reason"] = "MARKET_CLOSE"
                return session_result

            retries += 1
            reporter.info("reconnecting_topk_session", {"retry": retries})
            time_mod.sleep(WS_RECONNECT_DELAY_SEC)

        except KeyboardInterrupt:
            session_result["stop_requested"] = True
            session_result["session_reason"] = "STOPPED_BY_USER"
            return session_result
        except Exception as e:
            retries += 1
            reporter.error("topk_session_error", {"error": str(e), "retry": retries})
            time_mod.sleep(WS_RECONNECT_DELAY_SEC)

    session_result["session_reason"] = "TOP5_RETRY_EXHAUSTED"
    return session_result


def main():
    token = load_token()
    sym_to_key = load_watchlist_symbol_to_key()

    reporter = TradeReporter(
        json_path="web/data/dashboard_n200_top5.json",
        csv_dir="logs_n200_top5",
        capital=CAPITAL,
        mode="PAPER_TOP5",
    )
    risk = RiskManager(
        RiskConfig(
            capital=CAPITAL,
            risk_per_trade_pct=RISK_PER_TRADE_PCT,
            max_daily_loss_pct=MAX_DAILY_LOSS_PCT,
            max_trades_per_day=MAX_TRADES_PER_DAY,
        )
    )
    broker = PaperBroker()

    cooldown_until: Dict[str, datetime] = {}
    choppy_streak = 0

    while True:
        if market_stopped():
            reporter.info("stop_bot_market_close", {"time_ist": str(ist_time_now())})
            break

        if no_new_entries_now():
            reporter.info("entry_cutoff_waiting", {"time_ist": str(ist_time_now())})
            while not market_stopped():
                time_mod.sleep(30)
            reporter.info("stop_bot_market_close", {"time_ist": str(ist_time_now())})
            break

        if broker.position and broker.position.is_open:
            time_mod.sleep(5)
            continue

        token = load_token()
        try:
            top_candidates = pick_top_candidates_bi_directional(
                token,
                sym_to_key,
                cooldown_until=cooldown_until,
                top_k=TOP_K_MONITOR,
            )
        except Exception as e:
            reporter.error("scan_failed", {"error": str(e)})
            time_mod.sleep(SCAN_INTERVAL_SEC)
            continue

        if not top_candidates:
            choppy_streak += 1
            reporter.info("no_candidates_found", {"reason": "filters_not_matched", "choppy_streak": choppy_streak})
            time_mod.sleep((CHOPPY_PAUSE_MIN * 60) if choppy_streak >= CHOPPY_STREAK_THRESHOLD else SCAN_INTERVAL_SEC)
            continue

        first = top_candidates[0]
        shortlist = [
            {
                "symbol": sym,
                "bias": bias,
                "ltp": round(ltp, 2),
                "pct": round(pct, 2),
                "vol": int(vol),
                "score": round(float(score), 2),
                "max_shares": int(max_shares),
            }
            for score, sym, inst_key, bias, ltp, max_shares, pct, vol in top_candidates
        ]

        reporter.set_selected(
            first[1],
            first[2],
            extras={
                "bias": first[3],
                "ltp": round(first[4], 2),
                "pct": round(first[6], 2),
                "vol": int(first[7]),
                "max_shares": int(first[5]),
                "score": round(float(first[0]), 2),
                "watchlist": "nifty200_watchlist",
                "mode": "SCAN_THEN_TRADE_TOP5",
                "scan_interval_sec": SCAN_INTERVAL_SEC,
                "max_focus_candles": MAX_FOCUS_CANDLES,
                "rescan_interval_min": RESCAN_INTERVAL_MIN,
                "ema_gap_pct": EMA_GAP_PCT,
                "stop_loss_pct": STOP_LOSS_PCT,
                "top_k_monitor": TOP_K_MONITOR,
                "symbol_cooldown_min": SYMBOL_COOLDOWN_MIN,
                "breakout_confirmation": True,
                "shortlist": shortlist,
            },
        )

        result = run_topk_session(token=token, candidates=top_candidates, risk=risk, broker=broker, reporter=reporter)
        if result["stop_requested"]:
            break

        if result["trade_opened"] or result["trade_closed"]:
            for sym in [x[1] for x in top_candidates]:
                cooldown_until[sym] = now_ist() + timedelta(minutes=SYMBOL_COOLDOWN_MIN)

        reporter.info("return_to_scan_mode", {"reason": result["session_reason"]})

        if result["session_reason"] in {"TOP5_EXHAUSTED", "TOP5_RETRY_EXHAUSTED", "PERIODIC_RESCAN"}:
            choppy_streak += 1
        else:
            choppy_streak = 0

        time_mod.sleep((CHOPPY_PAUSE_MIN * 60) if choppy_streak >= CHOPPY_STREAK_THRESHOLD else SCAN_INTERVAL_SEC)


if __name__ == "__main__":
    main()
