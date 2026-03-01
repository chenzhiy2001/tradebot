#!/usr/bin/env python3
"""
Theta Bot — Late-entry volatility-filtered BTC 5-minute binary scalper.

Strategy:
  1. Stream real-time BTC/USD from Chainlink via Polymarket RTDS websocket
     (same oracle that determines market resolution) for distance checks.
  2. Stream real-time BTC/USDT from Binance websocket (1s kline) for
     high-resolut   ion volatility computation.
  3. Compute rolling 5-min realized volatility (stdev of 1s returns via Binance).
  3. For each Polymarket BTC 5-min window:
     - Wait until ENTRY_DELAY seconds into the window (let uncertainty fade).
     - Check: is realized volatility low? (adaptive percentile + absolute ceiling)
     - Check: is z-score high enough? z = |dist| / (vol × √t_remaining)
       z > 2.0 ≈ 97.7% — unifies distance, vol, and time into one metric
     - If both → buy the winning side at ask, hold to resolution ($1.00).
     - If vol high, z-score low, or price near threshold → skip the window.
  4. No selling needed — just hold to resolution. Winner gets $1/share.
     Only crash_held as safety net (refuses to sell into crashed book).

Edge:
  Polymarket prices reflect AVERAGE volatility. By entering LATE and
  only when vol is LOW, we buy at 0.87-0.92 what's actually ~95%+ to win.

Usage:
  python theta.py              # live trading
  python theta.py --dry-run    # simulate trades using WS prices
"""

import os
import sys
import time
import json
import math
import threading
import asyncio
import requests
from collections import deque
from datetime import datetime, timezone, timedelta
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    OrderArgs, OrderType,
    BalanceAllowanceParams, AssetType,
)
from py_clob_client.order_builder.constants import BUY, SELL
from dotenv import load_dotenv

load_dotenv()
private_key = os.getenv("PRIVATE_KEY")
founder_address = os.getenv("FUNDER_ADDRESS")

# =========================================================================
# CLOB CONFIG
# =========================================================================
HOST = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
RTDS_WS_URL = "wss://ws-live-data.polymarket.com"
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@kline_1s"
CHAIN_ID = 137

# Chainlink symbol (Polymarket resolution source)
CHAINLINK_SYMBOL = "btc/usd"
FUNDER_ADDRESS = founder_address

# =========================================================================
# STRATEGY PARAMETERS
# =========================================================================
INITIAL_BALANCE = 100.0         # Starting tracked balance

# ── Flat bet sizing ──
# Flat % of tracked balance. The edge is thin (~5% above breakeven).
# Aggressive sizing (tiers, Kelly) amplified losses. Keep it simple.
BET_PCT = 0.15                 # 15% of tracked balance per trade
MIN_BET = 5                    # Polymarket minimum bet
MAX_BET = 2000                 # Safety cap

# ── Entry conditions ──
ENTRY_DELAY = 10               # Seconds into window before considering entry
                               # Earlier entry = cheaper prices, more time for fill
MIN_Z_SCORE = 1.3              # Unified entry threshold — z = |dist| / (vol × √secs_left)
                               # z<1.5 trades had 88.2% WR over 76 trades — same as z≥1.5
                               # More trades = more compounding. z=1.3 is profitable.
MAX_VOL = 0.0030               # ABSOLUTE ceiling — never trade above this no matter what
                               # (extreme events: flash crash, CPI, etc.)
VOL_PERCENTILE = 70            # Enter only when vol is in the bottom N% of recent history
                               # 50 = "calmer than median" — p30 was too strict (blocked 100% of ticks)
MAX_ENTRY_PRICE = 0.93         # Hard cutoff — the 0.85-0.93 zone is where the real edge lives
                               # Above 0.93, payoff is too thin: even 95% WR barely breaks even

# ── Safety ──
MIN_STOP_SELL = 0.50           # Don't sell below this — hold for resolution
MIN_ENTRY_SECS_LEFT = 15       # Don't enter with fewer than this many seconds left
                               # (not enough time for order to fill)

# ── Polymarket fee formula ──
CRYPTO_FEE_RATE = 0.25
CRYPTO_FEE_EXPONENT = 2

# ── Timing ──
TICK_INTERVAL = 0.25           # 250ms main loop tick
ORDER_POLL_SECS = 1.0          # How often to poll share balance
BUY_FILL_TIMEOUT = 15          # Max seconds waiting for GTC buy fill
RESOLUTION_GRACE = 15          # Seconds after window end to check resolution
NO_MATCH_BACKOFF = 10          # Seconds backoff after "no match" error

# ── Volatility computation ──
VOL_WINDOW = 300               # Seconds of price history for vol computation
VOL_MIN_SAMPLES = 30           # Minimum 1s candles needed to compute vol
VOL_HISTORY_SIZE = 360         # Rolling buffer of vol measurements for adaptive threshold
                               # At 5s sample interval = ~30 min of history

# ── Data files ──
LOG_FILE = "theta_log.txt"
TRADE_LOG = "theta_trades.json"
TICK_LOG = "theta_ticks.csv"          # Every evaluation tick — for backtesting z-score model

DRY_RUN = "--dry-run" in sys.argv


# =========================================================================
# LOGGING
# =========================================================================
def log(msg):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    line = f"[{ts}] {msg}"
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")
    print(line)


# =========================================================================
# TICK CSV LOGGER
# =========================================================================
_TICK_CSV_FIELDS = [
    "timestamp", "btc_price", "threshold", "distance", "vol", "vol_thresh",
    "secs_left", "z_score", "up_mid", "down_mid", "action", "reason",
]

def _init_tick_log():
    """Write CSV header if file doesn't exist or is empty."""
    if not os.path.exists(TICK_LOG) or os.path.getsize(TICK_LOG) == 0:
        with open(TICK_LOG, "w") as f:
            f.write(",".join(_TICK_CSV_FIELDS) + "\n")

def _log_tick(row: dict):
    """Append one row to tick CSV. Missing fields become empty."""
    vals = [str(row.get(k, "")) for k in _TICK_CSV_FIELDS]
    with open(TICK_LOG, "a") as f:
        f.write(",".join(vals) + "\n")


# =========================================================================
# CLOB CLIENT
# =========================================================================
client = ClobClient(
    host=HOST,
    key=private_key,
    chain_id=CHAIN_ID,
    signature_type=1,
    funder=FUNDER_ADDRESS,
)
client.set_api_creds(client.create_or_derive_api_creds())


def get_usdc_balance():
    try:
        ba = client.get_balance_allowance(
            params=BalanceAllowanceParams(
                asset_type=AssetType.COLLATERAL, token_id="", signature_type=1
            )
        )
        return float(ba.get("balance", 0)) / 1e6
    except Exception as e:
        log(f"  ⚠ Balance error: {e}")
        return None


def get_share_balance(token_id):
    """Get on-chain share balance for a conditional token."""
    try:
        client.update_balance_allowance(
            params=BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
                signature_type=1,
            )
        )
        ba = client.get_balance_allowance(
            params=BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
                signature_type=1,
            )
        )
        return float(ba.get("balance", 0)) / 1e6
    except Exception:
        return None


def compute_taker_fee(shares, price):
    if price <= 0 or price >= 1:
        return 0.0
    fee_shares = shares * CRYPTO_FEE_RATE * (price * (1 - price)) ** CRYPTO_FEE_EXPONENT
    return round(fee_shares * price, 4)


# =========================================================================
# CHAINLINK PRICE FEED (via Polymarket RTDS — matches resolution source)
# =========================================================================
class ChainlinkFeed:
    """Real-time BTC/USD price from Chainlink via Polymarket RTDS websocket.

    This is the SAME data source Polymarket uses to resolve crypto markets.
    Used for distance-from-threshold checks (resolution-accurate price).
    Volatility is computed from Binance's faster stream instead.

    Stores prices for rolling price history.
    """

    def __init__(self):
        self._price = None              # Latest Chainlink BTC/USD price
        self._prices = deque(maxlen=3600)   # (timestamp, price) pairs — ~1hr at 1/s
        self._lock = threading.Lock()
        self._connected = False

    def start(self):
        t = threading.Thread(target=self._run, daemon=True)
        t.start()

    def _run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._ws_loop())

    async def _ws_loop(self):
        import websockets
        while True:
            try:
                async with websockets.connect(
                    RTDS_WS_URL, close_timeout=5, open_timeout=10
                ) as ws:
                    # Subscribe to Chainlink crypto prices
                    sub_msg = json.dumps({
                        "action": "subscribe",
                        "subscriptions": [{
                            "topic": "crypto_prices_chainlink",
                            "type": "*",
                            "filters": "",
                        }]
                    })
                    await ws.send(sub_msg)
                    self._connected = True
                    log("  🔗 Chainlink RTDS connected (Polymarket resolution source)")

                    last_ping = time.time()
                    last_data = time.time()

                    while True:
                        # Keep alive — RTDS needs pings every ~5s
                        if time.time() - last_ping > 4:
                            try:
                                await ws.send("PING")
                                last_ping = time.time()
                            except Exception:
                                break

                        # Staleness check — if no price data for 30s, force reconnect
                        if time.time() - last_data > 30:
                            log("  ⚠ Chainlink RTDS stale (no data 30s) — reconnecting")
                            break

                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1)
                        except asyncio.TimeoutError:
                            continue

                        try:
                            data = json.loads(msg)
                        except (json.JSONDecodeError, TypeError):
                            continue

                        if data.get("topic") == "crypto_prices_chainlink":
                            payload = data.get("payload", {})
                            symbol = payload.get("symbol", "").lower()
                            value = payload.get("value")
                            ts_ms = payload.get("timestamp")
                            if symbol == CHAINLINK_SYMBOL and value is not None:
                                try:
                                    price_val = float(value)
                                    ts = (ts_ms / 1000.0) if ts_ms else time.time()
                                    with self._lock:
                                        self._price = price_val
                                        self._prices.append((ts, price_val))
                                    last_data = time.time()
                                except (ValueError, TypeError):
                                    pass

            except Exception as e:
                self._connected = False
                log(f"  ⚠ Chainlink RTDS error: {e}, reconnecting in 2s...")
                await asyncio.sleep(2)

    @property
    def price(self):
        """Current Chainlink BTC/USD price."""
        with self._lock:
            return self._price

    @property
    def connected(self):
        return self._connected

    def price_at_time(self, target_ts):
        """Return the stored price closest to target_ts (epoch seconds).

        Searches the rolling (timestamp, price) buffer for the entry
        nearest to the requested time.  Returns (price, delta_secs)
        where delta_secs = |entry_ts - target_ts|, or (None, None).
        """
        with self._lock:
            prices = list(self._prices)
        if not prices:
            return None, None
        best = min(prices, key=lambda tp: abs(tp[0] - target_ts))
        return best[1], abs(best[0] - target_ts)

    def distance_from_round(self, threshold_price):
        """How far BTC is from a threshold price, as a fraction.

        Returns (distance_fraction, btc_price) or (None, None).
        distance_fraction > 0 means BTC is ABOVE threshold.
        distance_fraction < 0 means BTC is BELOW threshold.
        """
        p = self.price
        if p is None or threshold_price is None or threshold_price <= 0:
            return None, None
        return (p - threshold_price) / threshold_price, p


# =========================================================================
# BINANCE PRICE FEED (high-frequency volatility source)
# =========================================================================
class BinanceFeed:
    """Real-time BTC/USDT price via Binance websocket (1s kline).

    Used ONLY for volatility computation — Binance updates every ~1s vs
    Chainlink's slower cadence, giving much better vol estimates.
    """

    def __init__(self):
        self._price = None
        self._prices = deque(maxlen=VOL_WINDOW)
        self._vol_history = deque(maxlen=VOL_HISTORY_SIZE)  # rolling vol measurements
        self._last_vol_sample = 0  # timestamp of last vol sample
        self._lock = threading.Lock()
        self._connected = False

    def start(self):
        t = threading.Thread(target=self._run, daemon=True)
        t.start()

    def _run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._ws_loop())

    async def _ws_loop(self):
        import websockets
        while True:
            try:
                async with websockets.connect(
                    BINANCE_WS_URL, close_timeout=5, open_timeout=10
                ) as ws:
                    self._connected = True
                    log("  🔌 Binance WS connected (vol source)")

                    while True:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=5)
                        except asyncio.TimeoutError:
                            await ws.ping()
                            continue

                        try:
                            data = json.loads(msg)
                        except (json.JSONDecodeError, TypeError):
                            continue

                        k = data.get("k")
                        if k:
                            try:
                                close = float(k["c"])
                                ts = time.time()
                                with self._lock:
                                    self._price = close
                                    self._prices.append((ts, close))
                            except (ValueError, KeyError):
                                pass

            except Exception as e:
                self._connected = False
                log(f"  ⚠ Binance WS error: {e}, reconnecting in 2s...")
                await asyncio.sleep(2)

    @property
    def price(self):
        with self._lock:
            return self._price

    @property
    def connected(self):
        return self._connected

    def realized_vol(self):
        """Compute realized volatility as stdev of 1-second log returns.

        Returns (vol, n_samples) or (None, 0) if not enough data.
        """
        with self._lock:
            prices = list(self._prices)

        if len(prices) < VOL_MIN_SAMPLES:
            return None, len(prices)

        log_returns = []
        for i in range(1, len(prices)):
            dt = prices[i][0] - prices[i - 1][0]
            if dt > 5:  # Skip gaps > 5s (reconnection)
                continue
            if prices[i - 1][1] > 0:
                lr = math.log(prices[i][1] / prices[i - 1][1])
                log_returns.append(lr)

        if len(log_returns) < VOL_MIN_SAMPLES:
            return None, len(log_returns)

        mean = sum(log_returns) / len(log_returns)
        variance = sum((r - mean) ** 2 for r in log_returns) / len(log_returns)
        vol = math.sqrt(variance)

        return vol, len(log_returns)

    def record_vol_sample(self):
        """Sample current vol into history buffer (call every ~5s from main loop)."""
        vol, n = self.realized_vol()
        if vol is not None:
            with self._lock:
                self._vol_history.append(vol)

    def vol_threshold(self):
        """Compute adaptive vol threshold = Nth percentile of recent vol history.

        Returns (threshold, n_history) or (None, 0) if not enough history.
        When vol < threshold, market is calmer than usual → good to enter.
        """
        with self._lock:
            history = sorted(self._vol_history)

        if len(history) < 20:  # Need at least ~100s of history
            return None, len(history)

        # Compute the VOL_PERCENTILE-th percentile
        idx = int(len(history) * VOL_PERCENTILE / 100.0)
        idx = max(0, min(idx, len(history) - 1))
        return history[idx], len(history)


# =========================================================================
# POLYMARKET WEBSOCKET FEED (reused from btc80)
# =========================================================================
class PolymarketFeed:
    """Real-time Polymarket book data via WebSocket."""

    def __init__(self):
        self._prices = {}
        self._lock = threading.Lock()
        self._wanted = set()
        self._active = set()
        self._connected = False
        self._force_reconnect = False

    def start(self):
        t = threading.Thread(target=self._run, daemon=True)
        t.start()

    def _run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._ws_loop())

    async def _ws_loop(self):
        import websockets
        while True:
            try:
                async with websockets.connect(
                    WS_MARKET_URL, close_timeout=5, open_timeout=10
                ) as ws:
                    self._connected = True
                    self._active = set()
                    log("  🔌 Polymarket WS connected")
                    last_ping = time.time()
                    last_sub_time = 0.0

                    while True:
                        with self._lock:
                            wanted = set(self._wanted)

                        if self._force_reconnect:
                            self._force_reconnect = False
                            break

                        changed = wanted != self._active
                        stale = (time.time() - last_sub_time) > 5

                        if (changed or stale) and wanted:
                            if changed and self._active:
                                self._active = set()
                                self._force_reconnect = True
                                break

                            await ws.send(json.dumps({
                                "type": "market",
                                "assets_ids": list(wanted),
                            }))
                            last_sub_time = time.time()
                            if changed:
                                log(f"  📡 Subscribed to {len(wanted)} tokens")
                            self._active = wanted

                        if time.time() - last_ping > 10:
                            try:
                                await ws.send("ping")
                                last_ping = time.time()
                            except Exception:
                                break

                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=0.05)
                        except asyncio.TimeoutError:
                            continue

                        try:
                            data = json.loads(msg)
                        except (json.JSONDecodeError, TypeError):
                            continue
                        self._handle(data)

            except Exception as e:
                self._connected = False
                log(f"  ⚠ Polymarket WS error: {e}, reconnecting in 2s...")
                await asyncio.sleep(2)

    def _handle(self, data):
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    self._handle(item)
            return
        if not isinstance(data, dict):
            return
        etype = data.get("event_type", "")
        if etype == "best_bid_ask":
            self._on_bba(data)
        elif etype == "book":
            self._on_book(data)
        elif etype == "price_change":
            self._on_price_change(data)

    def _on_bba(self, data):
        asset_id = data.get("asset_id", "")
        if not asset_id:
            return
        try:
            bid = float(data.get("best_bid", 0))
            ask = float(data.get("best_ask", 0))
        except (ValueError, TypeError):
            return
        with self._lock:
            self._prices[asset_id] = {"best_bid": bid, "best_ask": ask, "ts": time.time()}

    def _on_book(self, data):
        asset_id = data.get("asset_id", "")
        if not asset_id:
            return
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        best_bid = max((float(b["price"]) for b in bids), default=0)
        best_ask = min((float(a["price"]) for a in asks), default=0)
        with self._lock:
            self._prices[asset_id] = {"best_bid": best_bid, "best_ask": best_ask, "ts": time.time()}

    def _on_price_change(self, data):
        for pc in data.get("price_changes", []):
            asset_id = pc.get("asset_id", "")
            if not asset_id:
                continue
            try:
                best_bid = float(pc.get("best_bid", 0))
                best_ask = float(pc.get("best_ask", 0))
            except (ValueError, TypeError):
                continue
            if best_bid > 0 or best_ask > 0:
                with self._lock:
                    existing = self._prices.get(asset_id, {})
                    self._prices[asset_id] = {
                        "best_bid": best_bid if best_bid > 0 else existing.get("best_bid", 0),
                        "best_ask": best_ask if best_ask > 0 else existing.get("best_ask", 0),
                        "ts": time.time(),
                    }

    def subscribe(self, token_ids):
        with self._lock:
            self._wanted = set(token_ids)

    def get_price(self, token_id):
        """Returns (best_bid, best_ask, age_seconds) or (None, None, None)."""
        with self._lock:
            data = self._prices.get(token_id)
        if not data:
            return None, None, None
        return data["best_bid"], data["best_ask"], time.time() - data["ts"]

    def mid_price(self, token_id):
        """Returns mid-price or None."""
        bid, ask, age = self.get_price(token_id)
        if bid and ask and bid > 0 and ask > 0 and (age is None or age < 10):
            return (bid + ask) / 2.0
        return None

    @property
    def connected(self):
        return self._connected


# =========================================================================
# MARKET DISCOVERY — BTC 5-minute
# =========================================================================
def discover_btc_market():
    """Fetch current active BTC 5m up/down market from Polymarket.
    Returns dict with epoch, start, end, up_token, down_token, threshold or None."""
    now = datetime.now(timezone.utc)
    interval = 5
    aligned = (now.minute // interval) * interval
    window_start = now.replace(minute=aligned, second=0, microsecond=0)
    window_end = window_start + timedelta(minutes=interval)

    if now >= window_end:
        return None

    epoch = int(window_start.timestamp())
    slug = f"btc-updown-{interval}m-{epoch}"

    try:
        resp = requests.get(f"{GAMMA_API}/events/slug/{slug}", timeout=10)
        if resp.status_code != 200:
            return None
        event = resp.json()
        for m in event.get("markets", []):
            if m.get("closed"):
                continue
            tokens = json.loads(m.get("clobTokenIds", "[]"))
            outcomes = json.loads(m.get("outcomes", "[]"))
            if len(tokens) != 2 or len(outcomes) != 2:
                continue

            up_idx = next((i for i, o in enumerate(outcomes) if "up" in o.lower()), 0)
            down_idx = 1 - up_idx

            return {
                "epoch": epoch,
                "start": window_start,
                "end": window_end,
                "up_token": tokens[up_idx],
                "down_token": tokens[down_idx],
                "threshold": None,  # Set later from Chainlink snapshot
            }
    except Exception as e:
        log(f"  ⚠ Discovery error: {e}")
    return None


# =========================================================================
# THETA BOT
# =========================================================================
class ThetaBot:
    """Late-entry volatility-filtered BTC 5-min binary scalper.

    Core logic:
      - Wait 2.5 min into each window
      - Check Binance vol + Chainlink distance from threshold
      - If low vol + comfortable distance → buy winning side on Polymarket
      - Hold to resolution → collect $1/share on win
    """

    def __init__(self, poly, chainlink, binance):
        self.poly = poly
        self.chainlink = chainlink
        self.binance = binance
        self.tracked_balance = INITIAL_BALANCE
        self.position = None
        self.resolving_positions = []   # positions waiting for share redemption
        self.pending_buy = None
        self.current_window = None
        self.trade_count = 0
        self.win_count = 0
        self.total_pnl = 0.0
        self.trade_history = []         # list of {"win_frac": float} for Kelly
        self._load_trade_history()
        self._no_match_until = 0
        self._entered_this_window = False  # Only one entry per window
        self._skip_reason = ""             # Why we skipped (for dashboard)

    # ─── TRADE HISTORY ──────────────────────────────────────────────

    def _load_trade_history(self):
        """Bootstrap trade history from saved log (theta_trades.json)."""
        if not os.path.exists(TRADE_LOG):
            return
        try:
            with open(TRADE_LOG) as f:
                trades = json.load(f)
            for t in trades:
                cost = t.get("cost", 0)
                pnl = t.get("pnl", 0)
                entry_price = t.get("entry_price", 0)
                if cost > 0:
                    self.trade_history.append({
                        "win_frac": pnl / cost,
                        "entry_price": entry_price,
                    })
            if self.trade_history:
                wins = sum(1 for h in self.trade_history if h["win_frac"] > 0)
                log(f"  📈 Loaded {len(self.trade_history)} trades "
                    f"({wins}W/{len(self.trade_history)-wins}L, "
                    f"{wins/len(self.trade_history):.0%} WR)")
        except Exception as e:
            log(f"  ⚠ Could not load trade history: {e}")

    def update_market(self, window):
        """Update current window and subscribe to tokens."""
        old_epoch = self.current_window["epoch"] if self.current_window else None
        self.current_window = window
        if window and window["epoch"] != old_epoch:
            # Cancel pending buy from old window
            if self.pending_buy and old_epoch is not None:
                log("  ⚠ Window changing — canceling pending buy")
                self._cancel_pending_buy_and_handle_partial("window change")
            # If holding from old window, DON'T sell — let it resolve naturally.
            # _manage_position() will detect shares disappearing (resolution).
            if self.position and old_epoch is not None:
                log(f"  ⏳ Window changed while holding — waiting for resolution")
            tokens = [window["up_token"], window["down_token"]]
            self.poly.subscribe(tokens)
            self._entered_this_window = False
            self._skip_reason = ""
            self._low_bal_logged = False

            # Snapshot Chainlink price as threshold ("Up or Down" markets
            # resolve based on price at end vs price at beginning of window).
            # Look up the stored price closest to the exact window start time
            # to match Polymarket's "Price to beat".
            win_start_ts = window["start"].timestamp()
            cl_historical, delta_secs = self.chainlink.price_at_time(win_start_ts)
            if cl_historical is not None and delta_secs < 10:
                # Good historical match — within 10s of window start
                window["threshold"] = round(cl_historical, 2)
                log(f"  🔄 New window (threshold=${cl_historical:,.2f} "
                    f"from Chainlink @{delta_secs:.1f}s of start)")
            else:
                # No reliable historical price near window start.
                # Leave threshold=None — tick() will retry price_at_time()
                # as Chainlink buffer fills.  NEVER use current price as
                # threshold — it can be minutes stale and cause phantom wins.
                lag_info = f" (nearest={delta_secs:.0f}s away)" if delta_secs is not None else ""
                log(f"  🔄 New window (threshold=pending — "
                    f"no Chainlink data near start{lag_info})")

    # ─── MAIN TICK ────────────────────────────────────────────────────

    def tick(self):
        """Main tick: manage resolving, then position/pending, or evaluate entry."""
        self._manage_resolving()

        if self.position:
            self._manage_position()
            return

        if self.pending_buy:
            self._manage_pending_buy()
            return

        # No position — evaluate entry
        if not self.current_window:
            return
        if self._entered_this_window:
            return

        now_utc = datetime.now(timezone.utc)
        if now_utc >= self.current_window["end"]:
            return

        secs_into_window = (now_utc - self.current_window["start"]).total_seconds()
        secs_left = (self.current_window["end"] - now_utc).total_seconds()

        # Too early — haven't waited long enough
        if secs_into_window < ENTRY_DELAY:
            self._skip_reason = f"waiting ({secs_into_window:.0f}/{ENTRY_DELAY}s)"
            return

        # Too late — not enough time for order to fill
        if secs_left <= MIN_ENTRY_SECS_LEFT:
            self._skip_reason = "too late for entry"
            return

        # No-match backoff
        if time.time() < self._no_match_until:
            self._skip_reason = "no-match backoff"
            return

        # ── Check volatility (from Binance — faster updates) ──
        vol, n_samples = self.binance.realized_vol()
        if vol is None:
            self._skip_reason = f"vol: need {VOL_MIN_SAMPLES} samples (have {n_samples})"
            return

        # Absolute ceiling — never trade in extreme vol
        if vol > MAX_VOL:
            self._skip_reason = f"vol extreme: {vol:.6f} > {MAX_VOL} (abs ceiling)"
            self._log_eval(secs_left=secs_left, vol=vol, action="skip",
                           reason="vol_extreme")
            return

        # Adaptive threshold — vol must be in the bottom N% of recent history
        vol_thresh, n_hist = self.binance.vol_threshold()
        if vol_thresh is None:
            self._skip_reason = f"vol history: need 20+ samples (have {n_hist})"
            return
        if vol > vol_thresh:
            self._skip_reason = (f"vol not calm: {vol:.6f} > p{VOL_PERCENTILE}={vol_thresh:.6f} "
                                 f"({n_hist} samples)")
            self._log_eval(secs_left=secs_left, vol=vol, vol_thresh=vol_thresh,
                           action="skip", reason="vol_percentile")
            return

        # ── Check BTC distance from threshold (from Chainlink — resolution source) ──
        threshold = self.current_window.get("threshold")
        if threshold is None:
            # Try to get historical price at window start now that Chainlink
            # buffer may have filled.  NEVER use current price as threshold —
            # that is fundamentally wrong and caused a $35 phantom-win bug.
            win_start_ts = self.current_window["start"].timestamp()
            cl_historical, delta_secs = self.chainlink.price_at_time(win_start_ts)
            if cl_historical is not None and delta_secs is not None and delta_secs < 10:
                threshold = round(cl_historical, 2)
                self.current_window["threshold"] = threshold
                log(f"  📌 Late threshold from history: ${threshold:,.2f} "
                    f"({delta_secs:.1f}s from window start)")
            else:
                self._skip_reason = "no threshold (Chainlink has no data near window start)"
                return

        dist, btc_price = self.chainlink.distance_from_round(threshold)
        if dist is None:
            self._skip_reason = "no BTC price"
            return

        abs_dist = abs(dist)

        # ── Z-score: unified distance + vol + time metric ──
        # z = |distance| / (vol × √secs_left)
        # Higher z → price is further away in vol-adjusted terms → safer entry
        # z > 2.0 ≈ 97.7% confidence price stays on the winning side
        z = abs_dist / (vol * math.sqrt(secs_left)) if secs_left > 0 else 0.0

        # Get both Polymarket mids for logging (always, not just on entry)
        up_mid = self.poly.mid_price(self.current_window["up_token"])
        down_mid = self.poly.mid_price(self.current_window["down_token"])

        if z < MIN_Z_SCORE:
            self._skip_reason = (f"z-score low: {z:.2f} < {MIN_Z_SCORE} "
                                 f"(dist={dist:+.4f} vol={vol:.6f} t={secs_left:.0f}s)")
            self._log_eval(secs_left=secs_left, vol=vol, vol_thresh=vol_thresh,
                           btc_price=btc_price, threshold=threshold, dist=dist,
                           z=z, up_mid=up_mid, down_mid=down_mid,
                           action="skip", reason="z_score_low")
            return

        # ── Determine which side to buy ──
        # dist > 0 → BTC above threshold → UP wins
        # dist < 0 → BTC below threshold → DOWN wins
        if dist > 0:
            side = "UP"
            token = self.current_window["up_token"]
        else:
            side = "DOWN"
            token = self.current_window["down_token"]

        # ── Check Polymarket mid price ──
        mid = up_mid if side == "UP" else down_mid
        if mid is None:
            self._skip_reason = "no Polymarket price"
            return
        if mid > MAX_ENTRY_PRICE:
            self._skip_reason = f"mid too high: {mid:.3f} > {MAX_ENTRY_PRICE} (not enough upside)"
            self._log_eval(secs_left=secs_left, vol=vol, vol_thresh=vol_thresh,
                           btc_price=btc_price, threshold=threshold, dist=dist,
                           z=z, up_mid=up_mid, down_mid=down_mid,
                           action="skip", reason="mid_too_high")
            return

        # ── All conditions met — enter! ──
        self._log_eval(secs_left=secs_left, vol=vol, vol_thresh=vol_thresh,
                       btc_price=btc_price, threshold=threshold, dist=dist,
                       z=z, up_mid=up_mid, down_mid=down_mid,
                       action="entry", reason=side)
        log(f"  ✅ ENTRY SIGNAL: BTC {side}")
        log(f"     BTC=${btc_price:,.2f} thresh=${threshold:,.0f} dist={dist:+.4f}")
        log(f"     z-score={z:.2f} (min={MIN_Z_SCORE}) | vol={vol:.6f} | t={secs_left:.0f}s")
        log(f"     mid={mid:.3f} | {secs_into_window:.0f}s in, {secs_left:.0f}s left")
        self._enter(token, side, mid,
                    entry_z=z, entry_vol=vol, entry_dist=dist, entry_secs_left=secs_left)

    # ─── TICK CSV LOGGING ─────────────────────────────────────────────

    def _log_eval(self, *, secs_left=None, vol=None, vol_thresh=None,
                  btc_price=None, threshold=None, dist=None, z=None,
                  up_mid=None, down_mid=None, action="", reason=""):
        """Write one evaluation row to the tick CSV."""
        _log_tick({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "btc_price": f"{btc_price:.2f}" if btc_price else "",
            "threshold": f"{threshold:.0f}" if threshold else "",
            "distance": f"{dist:+.6f}" if dist is not None else "",
            "vol": f"{vol:.8f}" if vol is not None else "",
            "vol_thresh": f"{vol_thresh:.8f}" if vol_thresh is not None else "",
            "secs_left": f"{secs_left:.1f}" if secs_left is not None else "",
            "z_score": f"{z:.4f}" if z is not None else "",
            "up_mid": f"{up_mid:.4f}" if up_mid is not None else "",
            "down_mid": f"{down_mid:.4f}" if down_mid is not None else "",
            "action": action,
            "reason": reason,
        })

    # ─── ENTRY ────────────────────────────────────────────────────────

    def _enter(self, token_id, side, mid, *,
               entry_z=None, entry_vol=None, entry_dist=None, entry_secs_left=None):
        """Place a GTC limit buy at the current ask price (fill instantly)."""
        self._entered_this_window = True
        self._entry_metrics = {
            "z": entry_z, "vol": entry_vol,
            "dist": entry_dist, "secs_left": entry_secs_left,
        }

        real_balance = get_usdc_balance()
        if real_balance is None:
            return

        # Reality checks (same as btc80)
        if real_balance < MIN_BET and self.tracked_balance > INITIAL_BALANCE:
            log(f"  ⚠ Reality check: real=${real_balance:.2f} — resetting tracked")
            self.tracked_balance = INITIAL_BALANCE
        if self.tracked_balance < MIN_BET and real_balance >= MIN_BET:
            log(f"  ⚠ Reverse reality check: tracked=${self.tracked_balance:.2f} — resetting")
            self.tracked_balance = INITIAL_BALANCE

        effective = min(self.tracked_balance, real_balance)

        # ── Flat bet sizing: fixed % of tracked balance ──
        _, ask, _ = self.poly.get_price(token_id)
        if ask is None or ask <= 0:
            buy_price = round(mid, 2)
        else:
            buy_price = round(ask, 2)

        bet = round(effective * BET_PCT)
        bet = min(bet, MAX_BET, int(real_balance))
        if bet < MIN_BET:
            if not hasattr(self, '_low_bal_logged') or not self._low_bal_logged:
                log(f"  ⚠ Balance too low (tracked=${self.tracked_balance:.2f}, "
                    f"real=${real_balance:.2f}) — skipping")
                self._low_bal_logged = True
            return

        shares_to_buy = round(bet / buy_price, 2)

        log(f"  ⚡ BTC {side}: {shares_to_buy:.1f}sh @ {buy_price} "
            f"(${bet} | {BET_PCT:.0%} of ${self.tracked_balance:.2f})")

        if DRY_RUN:
            log(f"  🧪 DRY RUN — buy {shares_to_buy:.1f}sh @ {buy_price}")
            self.position = {
                "token_id": token_id,
                "side": side,
                "entry_price": buy_price,
                "shares": shares_to_buy,
                "cost": bet,
                "entry_time": time.time(),
                "window_end": self.current_window["end"] if self.current_window else None,
                "threshold": self.current_window.get("threshold") if self.current_window else None,
                "limit_order_id": None,
                "usdc_snapshot": real_balance - bet,
                "last_poll": 0,
                "dry_run": True,
                "crash_held": False,
                "entry_metrics": self._entry_metrics,
            }
            return

        # ── Place GTC limit buy at ask (fills instantly) ──
        pre_buy_bal = get_share_balance(token_id) or 0
        try:
            order_args = OrderArgs(
                price=buy_price,
                size=shares_to_buy,
                side=BUY,
                token_id=token_id,
            )
            signed_order = client.create_order(order_args)
            resp = client.post_order(signed_order, OrderType.GTC)

            order_id = resp.get("orderID", "") if isinstance(resp, dict) else ""
            raw_taking = resp.get("takingAmount", 0) if isinstance(resp, dict) else 0
            try:
                taking = float(raw_taking) if raw_taking != "" else 0
            except (ValueError, TypeError):
                taking = 0

            if not order_id and taking <= 0:
                log(f"  ⚠ GTC buy rejected: {resp}")
                return

            self.pending_buy = {
                "order_id": order_id,
                "token_id": token_id,
                "side": side,
                "bet": bet,
                "buy_price": buy_price,
                "shares_requested": shares_to_buy,
                "placed_at": time.time(),
                "pre_buy_bal": pre_buy_bal,
                "last_poll": 0,
            }

            if order_id:
                log(f"  📋 GTC buy placed (order={order_id[:12]}...)")

            if taking > 0 and not order_id:
                log(f"  ✅ GTC buy filled instantly! {taking:.1f}sh")
                self._on_buy_filled(taking)

        except Exception as e:
            err_str = str(e)
            if 'no match' in err_str.lower():
                self._no_match_until = time.time() + NO_MATCH_BACKOFF
                log(f"  ⚠ Buy error: no match — backing off {NO_MATCH_BACKOFF}s")
                return

            log(f"  ⚠ Buy error: {e}")
            self._no_match_until = time.time() + NO_MATCH_BACKOFF
            time.sleep(2)
            new_shares = 0
            for attempt in range(5):
                share_bal = get_share_balance(token_id) or 0
                new_shares = share_bal - pre_buy_bal
                if new_shares >= 1.0:
                    break
                log(f"  ⚠ Share check {attempt+1}/5: bal={share_bal}, new={new_shares:.1f}")
                time.sleep(2)

            if new_shares >= 1.0:
                log(f"  ⚡ Shares detected ({new_shares:.1f}sh) despite error — treating as fill")
                cost = round(new_shares * buy_price, 2)
                self.pending_buy = {
                    "order_id": "",
                    "token_id": token_id,
                    "side": side,
                    "bet": cost,
                    "buy_price": buy_price,
                    "shares_requested": new_shares,
                    "placed_at": time.time(),
                    "pre_buy_bal": pre_buy_bal,
                    "last_poll": 0,
                }
                self._on_buy_filled(new_shares)

    # ─── PENDING BUY MANAGEMENT ───────────────────────────────────────

    def _cancel_pending_buy_and_handle_partial(self, reason):
        """Cancel pending GTC buy and handle any partial fill."""
        pb = self.pending_buy
        if not pb:
            return
        token_id = pb["token_id"]

        try:
            client.cancel(pb["order_id"])
            log(f"  ❌ Canceled GTC buy ({reason})")
        except Exception:
            pass

        time.sleep(2)
        new_shares = 0
        share_bal = None
        for attempt in range(5):
            share_bal = get_share_balance(token_id)
            if share_bal is not None:
                new_shares = share_bal - pb["pre_buy_bal"]
                if new_shares >= 1.0:
                    break
            log(f"  ⚠ Share balance check {attempt+1}/5: bal={share_bal}, new={new_shares:.1f}")
            time.sleep(2)

        if share_bal is None:
            log("  ⚠ Could not read share balance after cancel — assuming filled")
            new_shares = pb["shares_requested"]

        partial_cost = round(new_shares * pb["buy_price"], 2)

        if new_shares >= 1.0 and partial_cost >= MIN_BET:
            log(f"  ⚡ Partial fill: {new_shares:.1f}sh (${partial_cost:.2f}) — treating as entry")
            self._on_buy_filled(new_shares)
        else:
            if new_shares > 0:
                log(f"  ⚠ Tiny partial fill ({new_shares:.1f}sh) — ignoring")
            self.pending_buy = None

    def _manage_pending_buy(self):
        """Poll for GTC buy fill."""
        pb = self.pending_buy
        if not pb:
            return

        now = time.time()
        token_id = pb["token_id"]

        if now - pb["placed_at"] > BUY_FILL_TIMEOUT:
            log(f"  ⚠ GTC buy timed out after {BUY_FILL_TIMEOUT}s")
            self._cancel_pending_buy_and_handle_partial("timeout")
            return

        if now - pb.get("last_poll", 0) < ORDER_POLL_SECS:
            return
        pb["last_poll"] = now

        share_bal = get_share_balance(token_id)
        if share_bal is None:
            return

        new_shares = share_bal - pb["pre_buy_bal"]
        if new_shares >= pb["shares_requested"] - 1.0:
            log(f"  ✅ GTC buy filled! {new_shares:.1f}sh @ {pb['buy_price']}")
            self._on_buy_filled(new_shares)

    def _on_buy_filled(self, actual_shares):
        """Called when GTC buy is filled — record position, hold to resolution."""
        pb = self.pending_buy
        if not pb:
            return

        if actual_shares > pb["shares_requested"] * 1.05:
            log(f"  ⚠ Share balance inflated: {actual_shares:.1f}sh — capping")
            actual_shares = pb["shares_requested"]

        token_id = pb["token_id"]
        side = pb["side"]
        buy_price = pb["buy_price"]
        actual_cost = round(actual_shares * buy_price, 2)
        self.pending_buy = None

        usdc_snap = get_usdc_balance() or 0

        self.position = {
            "token_id": token_id,
            "side": side,
            "entry_price": buy_price,
            "shares": actual_shares,
            "cost": actual_cost,
            "entry_time": time.time(),
            "window_end": self.current_window["end"] if self.current_window else None,
            "threshold": self.current_window.get("threshold") if self.current_window else None,
            "limit_order_id": None,  # No GTC sell — hold to resolution
            "usdc_snapshot": usdc_snap,
            "last_poll": 0,
            "dry_run": False,
            "crash_held": False,
            "entry_metrics": getattr(self, '_entry_metrics', {}),
        }
        log(f"  📊 Position opened: {actual_shares:.1f}sh BTC {side} @ {buy_price}")
        log(f"     Holding to resolution (target: $1.00/share)")

    # ─── POSITION MANAGEMENT ─────────────────────────────────────────

    def _manage_position(self):
        """Monitor active position. Once window ends, move to resolving queue."""
        pos = self.position
        if not pos:
            return

        # ── Check if window has ended → move to resolving queue ──
        win_end = pos.get("window_end")
        if win_end:
            secs_past_end = (datetime.now(timezone.utc) - win_end).total_seconds()
            if secs_past_end > 0:
                log(f"  ♻ Window ended — moving position to resolving queue")
                self.resolving_positions.append(pos)
                self.position = None
                return

        # Window still active — nothing to do, just hold

    # ─── RESOLVING POSITIONS ─────────────────────────────────────────

    def _resolve_by_chainlink(self, pos):
        """Determine win/loss using Chainlink price vs threshold."""
        threshold = pos.get("threshold")
        side = pos["side"]

        # Get Chainlink price at window end time
        cl_price = None
        win_end = pos.get("window_end")
        if win_end:
            cl_price, delta = self.chainlink.price_at_time(win_end.timestamp())
            if cl_price is None or delta > 30:
                cl_price = self.chainlink.price  # fallback to current
        else:
            cl_price = self.chainlink.price

        # Determine outcome
        if threshold and cl_price:
            if side == "UP":
                won = cl_price > threshold
            else:
                won = cl_price < threshold
            log(f"  📊 Resolution: BTC ${cl_price:,.2f} vs beat=${threshold:,.2f} "
                f"→ {side} {'WIN' if won else 'LOSS'}")
        else:
            # Fallback: USDC comparison (only reliable if no other positions in flight)
            usdc_now = get_usdc_balance()
            usdc_before = pos.get("usdc_snapshot", 0)
            won = usdc_now is not None and usdc_now > usdc_before + pos["cost"] * 0.5
            log(f"  📊 Resolution (USDC fallback): {'WIN' if won else 'LOSS'}")

        if won:
            proceeds = pos["shares"] * 1.0
            pnl = proceeds - pos["cost"]
            log(f"  🎉 RESOLVED WIN: BTC {side}, proceeds=${proceeds:.2f}, PnL ${pnl:+.2f}")
        else:
            proceeds = 0.0
            pnl = -pos["cost"]
            log(f"  ❌ RESOLVED LOSS: BTC {side}, PnL ${pnl:+.2f}")

        self._handle_exit(proceeds, pnl, 1.0 if won else 0.0, "resolved", pos=pos)

    def _manage_resolving(self):
        """Poll resolving positions for share redemption. Non-blocking."""
        for pos in list(self.resolving_positions):  # copy so we can remove
            token_id = pos["token_id"]
            now = time.time()

            # ── DRY RUN ──
            if pos.get("dry_run"):
                win_end = pos.get("window_end")
                if win_end and (datetime.now(timezone.utc) - win_end).total_seconds() > 5:
                    mid = self.poly.mid_price(token_id)
                    if mid is not None:
                        resolved_price = 1.0 if mid > 0.50 else 0.0
                        revenue = pos["shares"] * resolved_price
                        pnl = revenue - pos["cost"]
                        log(f"  📊 RESOLVED: BTC {pos['side']} → "
                            f"{'WIN' if resolved_price > 0 else 'LOSS'}")
                        log(f"  🧪 DRY RUN — PnL ${pnl:+.2f}")
                        self._handle_exit(revenue, pnl, resolved_price, "resolved", pos=pos)
                continue

            # ── Crash-held: just poll for share disappearance ──
            if pos.get("crash_held"):
                if now - pos.get("last_poll", 0) >= ORDER_POLL_SECS:
                    pos["last_poll"] = now
                    share_bal = get_share_balance(token_id)
                    if share_bal is not None and share_bal < 1.0:
                        self._resolve_by_chainlink(pos)
                continue

            # ── Poll for resolution (shares disappear) ──
            if now - pos.get("last_poll", 0) >= ORDER_POLL_SECS:
                pos["last_poll"] = now
                share_bal = get_share_balance(token_id)
                if share_bal is not None and share_bal < 1.0:
                    self._resolve_by_chainlink(pos)
                    continue

            # ── Hard timeout: force sell after 120s past window end ──
            win_end = pos.get("window_end")
            if win_end:
                secs_past_end = (datetime.now(timezone.utc) - win_end).total_seconds()
                if secs_past_end > 120:
                    log(f"  ⚠ Resolving position {secs_past_end:.0f}s past window end "
                        f"— forcing sell")
                    self._execute_sell(pos=pos)
                    continue
                if secs_past_end > RESOLUTION_GRACE:
                    # Log every ~5s
                    last_res_log = pos.get("_last_res_log", 0)
                    if now - last_res_log >= 5:
                        log(f"  📊 Resolving: window ended {secs_past_end:.0f}s ago "
                            f"— BTC {pos['side']}")
                        pos["_last_res_log"] = now
                    # Check more frequently after grace period
                    share_bal = get_share_balance(token_id)
                    if share_bal is not None and share_bal < 1.0:
                        self._resolve_by_chainlink(pos)
                        continue

    # ─── SELL EXECUTION ───────────────────────────────────────────────

    def _execute_sell(self, pos=None):
        """Sell all shares at bid-1¢. Used only for emergency close."""
        if pos is None:
            pos = self.position
        if not pos:
            return

        token_id = pos["token_id"]

        # Crash check
        bid, _, _ = self.poly.get_price(token_id)
        if bid is not None and bid < MIN_STOP_SELL:
            log(f"  ⚠ Bid={bid:.2f} < {MIN_STOP_SELL} — holding for resolution")
            pos["crash_held"] = True
            return

        share_bal = get_share_balance(token_id)
        if share_bal is not None and share_bal < 1.0:
            log(f"  ℹ No shares to sell — already resolved")
            self._resolve_by_chainlink(pos)
            return

        sell_amount = share_bal if share_bal is not None else pos["shares"]

        sell_price = round(bid - 0.01, 2) if bid and bid > 0.03 else 0.02
        log(f"  📤 Selling {sell_amount:.1f}sh BTC {pos['side']} @ {sell_price} (bid={bid})")

        try:
            order_args = OrderArgs(
                price=sell_price,
                size=sell_amount,
                side=SELL,
                token_id=token_id,
            )
            signed_order = client.create_order(order_args)
            resp = client.post_order(signed_order, OrderType.GTC)
        except Exception as e:
            err_str = str(e).lower()
            if 'price' in err_str and 'max' in err_str:
                log(f"  ✅ Market resolved in our favor")
                pnl = pos["shares"] * 1.0 - pos["cost"]
                self._handle_exit(pos["shares"], pnl, 1.0, "resolved", pos=pos)
                return
            if 'no match' in err_str or 'orderbook' in err_str:
                log(f"  ⚠ Market closed — determining outcome via Chainlink")
                self._resolve_by_chainlink(pos)
                return
            if 'not enough balance' in err_str:
                time.sleep(1)
                remaining = get_share_balance(token_id)
                if remaining is not None and remaining < 1.0:
                    log(f"  ✅ Shares already sold/resolved")
                    proceeds = sell_amount * sell_price
                    fee = compute_taker_fee(sell_amount, sell_price)
                    proceeds -= fee
                    pnl = proceeds - pos["cost"]
                    self._handle_exit(proceeds, pnl, sell_price, "sell", pos=pos)
                    return
            log(f"  ⚠ Sell error: {e}")
            log(f"  ❌ Sell failed — holding for resolution")
            return

        # Parse response
        order_id = resp.get("orderID", "") if isinstance(resp, dict) else ""
        raw_taking = resp.get("takingAmount", 0) if isinstance(resp, dict) else 0
        try:
            taking = float(raw_taking) if raw_taking != "" else 0
        except (ValueError, TypeError):
            taking = 0
        raw_making = resp.get("makingAmount", 0) if isinstance(resp, dict) else 0
        try:
            making = float(raw_making) if raw_making != "" else 0
        except (ValueError, TypeError):
            making = 0

        # Instant fill
        if taking > 0 and not order_id:
            actual_shares = making if making > 0 else sell_amount
            actual_price = round(taking / actual_shares, 4) if actual_shares > 0 else 0
            pnl = taking - pos["cost"]
            log(f"  💰 Sold {actual_shares:.1f}sh @ ~{actual_price:.3f} → PnL ${pnl:+.2f}")
            self._handle_exit(taking, pnl, actual_price, "sell", pos=pos)
            return

        # GTC resting — wait up to 60s
        if order_id:
            log(f"  📋 Sell placed (order={order_id[:12]}...)")
            deadline = time.time() + 60
            while time.time() < deadline:
                remaining = get_share_balance(token_id)
                if remaining is not None and remaining < 1.0:
                    proceeds = sell_amount * sell_price
                    fee = compute_taker_fee(sell_amount, sell_price)
                    proceeds -= fee
                    pnl = proceeds - pos["cost"]
                    log(f"  💰 Sell filled → PnL ${pnl:+.2f}")
                    self._handle_exit(proceeds, pnl, sell_price, "sell", pos=pos)
                    return
                time.sleep(2)
            # Not filled — leave resting
            log(f"  ⚠ Not filled in 60s — leaving sell resting")
            proceeds = sell_amount * sell_price
            fee = compute_taker_fee(sell_amount, sell_price)
            proceeds -= fee
            pnl = proceeds - pos["cost"]
            self._handle_exit(proceeds, pnl, sell_price, "sell_resting", pos=pos)
            return

        # Ambiguous
        log(f"  ⚠ Ambiguous sell response: {resp}")
        time.sleep(2)
        remaining = get_share_balance(token_id)
        if remaining is not None and remaining < 1.0:
            proceeds = sell_amount * sell_price
            fee = compute_taker_fee(sell_amount, sell_price)
            proceeds -= fee
            pnl = proceeds - pos["cost"]
            self._handle_exit(proceeds, pnl, sell_price, "sell", pos=pos)
            return

        log(f"  ❌ Sell failed — holding for resolution")

    # ─── EXIT HANDLING ────────────────────────────────────────────────

    def _handle_exit(self, proceeds, pnl, sell_price, reason, pos=None):
        """Record trade, update balance, clear position."""
        if pos is None:
            pos = self.position
        if not pos:
            return

        self.trade_count += 1
        if pnl > 0:
            self.win_count += 1
        self.total_pnl += pnl

        # Update trade history
        cost = pos.get("cost", 0)
        if cost > 0:
            self.trade_history.append({
                "win_frac": pnl / cost,
                "entry_price": pos["entry_price"],
            })

        # Update tracked balance
        self.tracked_balance += pnl
        if self.tracked_balance < MIN_BET:
            self.tracked_balance = INITIAL_BALANCE
            log(f"  💼 Tracked balance reset to ${INITIAL_BALANCE:.2f}")
        else:
            log(f"  💼 Tracked balance: ${self.tracked_balance:.2f}")

        # Clear position from appropriate location
        if pos is self.position:
            self.position = None
        elif pos in self.resolving_positions:
            self.resolving_positions.remove(pos)

        # Log trade
        metrics = pos.get("entry_metrics", {})
        entry = {
            "time": datetime.now(timezone.utc).isoformat(),
            "side": pos["side"],
            "entry_price": pos["entry_price"],
            "exit_price": sell_price,
            "shares": pos["shares"],
            "cost": pos["cost"],
            "pnl": round(pnl, 4),
            "hold_secs": round(time.time() - pos["entry_time"], 1),
            "reason": reason,
            "tracked_balance": round(self.tracked_balance, 2),
            "dry_run": pos.get("dry_run", False),
            "entry_z": metrics.get("z"),
            "entry_vol": metrics.get("vol"),
            "entry_dist": metrics.get("dist"),
            "entry_secs_left": metrics.get("secs_left"),
        }

        trades = []
        if os.path.exists(TRADE_LOG):
            try:
                with open(TRADE_LOG) as f:
                    trades = json.load(f)
            except Exception:
                pass
        trades.append(entry)
        with open(TRADE_LOG, "w") as f:
            json.dump(trades, f, indent=2)

        result = "W" if pnl > 0 else "L"
        log(f"  📊 Trade #{self.trade_count}: {result} ${pnl:+.2f} | "
            f"session {self.win_count}/{self.trade_count} | "
            f"cumulative ${self.total_pnl:+.2f}")

    def get_status(self):
        """One-line status for dashboard."""
        resolving_tag = ""
        if self.resolving_positions:
            n = len(self.resolving_positions)
            resolving_tag = f" | {n} resolving"
        if self.position:
            pos = self.position
            hold = time.time() - pos["entry_time"]
            mid = self.poly.mid_price(pos["token_id"]) or 0
            return (f"HOLDING BTC {pos['side']} | entry={pos['entry_price']:.3f} "
                    f"now={mid:.3f} | hold={hold:.0f}s{resolving_tag}")
        if self.pending_buy:
            pb = self.pending_buy
            wait = time.time() - pb["placed_at"]
            return (f"PENDING BUY BTC {pb['side']} | {pb['shares_requested']:.1f}sh @ "
                    f"{pb['buy_price']} | waiting {wait:.0f}s{resolving_tag}")
        if self._skip_reason:
            return f"SCANNING | skip: {self._skip_reason}{resolving_tag}"
        return f"SCANNING...{resolving_tag}"


# =========================================================================
# MAIN
# =========================================================================
def main():
    mode = "DRY" if DRY_RUN else "LIVE"
    log(f"═══ Theta Bot ═══ [{mode}]")
    log(f"Strategy: Late-entry vol-filtered BTC binary scalper")
    log(f"  Entry: {ENTRY_DELAY}s into window | z≥{MIN_Z_SCORE} | max_vol={MAX_VOL}")
    log(f"  Bet: flat {BET_PCT:.0%} of balance | max_entry={MAX_ENTRY_PRICE}")
    log(f"  Safety: min_stop_sell={MIN_STOP_SELL} | min_entry_secs={MIN_ENTRY_SECS_LEFT}")
    log(f"  Balance: ${INITIAL_BALANCE:.2f} start")
    log("")

    # Init tick CSV
    _init_tick_log()

    # Start feeds
    chainlink = ChainlinkFeed()
    chainlink.start()

    binance = BinanceFeed()
    binance.start()

    poly = PolymarketFeed()
    poly.start()

    bot = ThetaBot(poly, chainlink, binance)

    log("Waiting for WS connections...")
    for _ in range(30):
        if poly.connected and chainlink.connected and binance.connected:
            break
        time.sleep(1)

    if not poly.connected:
        log("⚠ Polymarket WS not connected after 30s")
    if not chainlink.connected:
        log("⚠ Chainlink RTDS not connected after 30s")
    if not binance.connected:
        log("⚠ Binance WS not connected after 30s")

    balance = get_usdc_balance()
    if balance:
        log(f"Real USDC balance: ${balance:.2f}")
    log(f"Tracked balance: ${bot.tracked_balance:.2f}")

    # Wait for vol data to accumulate
    log(f"Accumulating {VOL_MIN_SAMPLES} Binance ticks for volatility...")
    log("")

    last_discovery = 0
    discovery_interval = 15

    while True:
        try:
            now = time.time()

            if now - last_discovery > discovery_interval:
                window = discover_btc_market()
                if window:
                    old_epoch = bot.current_window["epoch"] if bot.current_window else None
                    if window["epoch"] != old_epoch:
                        bot.update_market(window)
                last_discovery = now

            bot.tick()

            # Dashboard
            now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
            poly_ws = "🟢" if poly.connected else "🔴"
            cl_ws = "🟢" if chainlink.connected else "🔴"
            bn_ws = "🟢" if binance.connected else "🔴"
            status = bot.get_status()

            btc_price = chainlink.price
            vol, n_samples = binance.realized_vol()
            vol_thresh, n_hist = binance.vol_threshold()

            # Sample vol into history every ~5s
            if now - getattr(main, '_last_vol_sample', 0) >= 5:
                binance.record_vol_sample()
                main._last_vol_sample = now

            up_mid = down_mid = None
            threshold = None
            if bot.current_window:
                up_mid = poly.mid_price(bot.current_window["up_token"])
                down_mid = poly.mid_price(bot.current_window["down_token"])
                threshold = bot.current_window.get("threshold")

            secs_in = secs_left = 0
            if bot.current_window:
                now_utc = datetime.now(timezone.utc)
                secs_in = (now_utc - bot.current_window["start"]).total_seconds()
                secs_left = (bot.current_window["end"] - now_utc).total_seconds()

            print(f"\033[2J\033[H", end="")
            print(f"═══ Theta Bot [{mode}] ═══  {now_str} UTC")
            print(f"{'─' * 70}")
            print(f"  Polymarket {poly_ws}  |  Chainlink {cl_ws}  |  Binance {bn_ws}")
            print(f"  BTC: ${btc_price:,.2f}" if btc_price else "  BTC: --")
            if threshold:
                dist = (btc_price - threshold) / threshold if btc_price else 0
                diff_dollar = (btc_price - threshold) if btc_price else 0
                diff_arrow = "▲" if diff_dollar >= 0 else "▼"
                # Compute live z-score for dashboard
                z_str = "--"
                if vol and secs_left > 0:
                    z_live = abs(dist) / (vol * math.sqrt(secs_left))
                    z_ok = "✅" if z_live >= MIN_Z_SCORE else "❌"
                    z_str = f"{z_ok} {z_live:.2f} (min={MIN_Z_SCORE})"
                print(f"  Price to beat: ${threshold:,.2f}  |  "
                      f"{diff_arrow} ${abs(diff_dollar):,.2f}  |  Z: {z_str}")
            print(f"  Vol: {vol:.6f} ({n_samples} samples)" if vol else
                  f"  Vol: accumulating ({n_samples}/{VOL_MIN_SAMPLES} samples)")
            if vol and vol_thresh:
                vol_ok = "✅" if vol <= vol_thresh and vol <= MAX_VOL else "❌"
                print(f"  Vol filter: {vol_ok} curr={vol:.6f} "
                      f"p{VOL_PERCENTILE}={vol_thresh:.6f} ceil={MAX_VOL} "
                      f"({n_hist} hist)")
            elif vol:
                print(f"  Vol filter: ⏳ building history ({n_hist}/20)")
            print(f"{'─' * 70}")
            if up_mid or down_mid:
                print(f"  UP={up_mid:.3f}" if up_mid else "  UP=--", end="  |  ")
                print(f"DOWN={down_mid:.3f}" if down_mid else "DOWN=--")
            print(f"  Window: {secs_in:.0f}s in / {secs_left:.0f}s left"
                  f"  (entry after {ENTRY_DELAY}s)")
            print(f"  {status}")
            print(f"{'─' * 70}")
            print(f"  Balance: ${bot.tracked_balance:.2f}")
            print(f"  Trades: {bot.trade_count} | Wins: {bot.win_count} | "
                  f"PnL: ${bot.total_pnl:+.2f}")
            print(f"  Log: {LOG_FILE} | Trades: {TRADE_LOG}")
            print(f"  Press Ctrl+C to stop")

            time.sleep(TICK_INTERVAL)

        except KeyboardInterrupt:
            log(f"\nStopping...")
            if bot.pending_buy:
                bot._cancel_pending_buy_and_handle_partial("shutdown")
            if bot.position and not bot.position.get("dry_run"):
                log("  Closing open position...")
                bot._execute_sell()
            for rpos in list(bot.resolving_positions):
                if not rpos.get("dry_run"):
                    log(f"  Closing resolving position: BTC {rpos['side']}...")
                    bot._execute_sell(pos=rpos)
            bal = get_usdc_balance()
            if bal:
                log(f"Final USDC balance: ${bal:.2f}")
            log(f"Tracked balance: ${bot.tracked_balance:.2f}")
            log(f"Trades: {bot.trade_count} | Wins: {bot.win_count} | "
                f"PnL: ${bot.total_pnl:+.2f}")
            break
        except Exception as e:
            log(f"⚠ Main loop error: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5)


if __name__ == "__main__":
    main()
