#!/usr/bin/env python3
"""
Sniper Bot — Bayesian prediction-based trading on Polymarket crypto markets.

Strategy:
  Use real-time Binance prices to compute the TRUE probability that a
  crypto "Up or Down" market will resolve UP, using the Bayesian formula:

    P(UP) = Φ( log(1 + R) / (σ √t_remaining) )

  where R = current return from window open, σ = realized volatility
  per √second (estimated from completed windows), t_remaining = seconds
  left in the window, Φ = standard normal CDF.

  Compare this to Polymarket's implied probability (token mid-price).
  When the edge exceeds MIN_EDGE, buy the underpriced token.

  Markets resolve automatically — buy-and-hold to resolution.
  If our token wins, shares → $1.00.  If it loses, shares → $0.00.

Entry:
  - GTC limit buy at best_ask (crosses spread, fills immediately as taker)
  - Taker fee ~0.5-1.5% (negligible vs 15-25% edge)
  - Only one trade per window, only when elapsed > MIN_ELAPSED_PCT

Data:
  - Records every window (predictions, trades, outcomes) to sniper_data.jsonl
  - After each completed window, recalibrates volatility estimate from ALL
    historical data (predict_data.jsonl + sniper_data.jsonl)
  - Continuously learns and adapts σ

Fees (5m/15m crypto markets):
  - Entry at ask → taker: fee = C × 0.25 × (p(1-p))^2 ≈ 0.5-1.5%
  - No exit needed — market resolves automatically
"""

import os
import sys
import time
import json
import math
import asyncio
import threading
import re
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    OrderArgs, OrderType, OpenOrderParams,
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
BINANCE_WS_URL = "wss://stream.binance.com:9443/stream"
CHAIN_ID = 137
FUNDER_ADDRESS = founder_address

CRYPTOS = {
    "btc": "btcusdt",
    "eth": "ethusdt",
    "sol": "solusdt",
    "xrp": "xrpusdt",
}

# =========================================================================
# STRATEGY PARAMETERS
# =========================================================================
BET_AMOUNT = 10               # Fixed bet size in USDC per trade
MAX_POSITIONS = 8             # Max concurrent positions (windows we're active in)
MIN_EDGE = 0.15               # Minimum |edge| to trade (Bayesian prob - Poly implied)
MIN_RETURN_ABS = 0.0003       # Minimum |crypto return| (0.03%) — filters noise/flat
MIN_ELAPSED_PCT = 0.70        # Don't trade before 70% of window elapsed
MAX_ELAPSED_PCT = 0.95        # Don't trade after 95% (might not fill before resolution)
ENTRY_PRICE_MIN = 0.10        # Don't buy tokens cheaper than this
ENTRY_PRICE_MAX = 0.90        # Don't buy tokens more expensive than this
FILL_WAIT = 5                 # Seconds to wait for GTC limit buy fill

# Bayesian model
DEFAULT_VOL_PER_SEC = 5.8e-5  # Default σ per √second (~0.1% per 5min window)
MIN_WINDOWS_FOR_VOL = 10      # Need this many completed windows before using empirical vol

# Polymarket fee formula (5m/15m crypto)
CRYPTO_FEE_RATE = 0.25
CRYPTO_FEE_EXPONENT = 2

# Data files
DATA_FILE = "sniper_data.jsonl"
PREDICT_DATA_FILE = "predict_data.jsonl"
TRADE_LOG = "sniper_trades.json"
DECISION_LOG = "sniper_log.txt"

MARKET_POLL_INTERVAL = 30     # Seconds between market discovery polls
TICK_INTERVAL = 1.0           # Main loop tick interval

DRY_RUN = "--dry-run" in sys.argv


# =========================================================================
# LOGGING
# =========================================================================
def log(msg):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    with open(DECISION_LOG, "a") as f:
        f.write(line + "\n")
    print(msg)


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
    """Fee in USDC: C × feeRate × (p(1-p))^exponent × p"""
    if price <= 0 or price >= 1:
        return 0.0
    fee_shares = shares * CRYPTO_FEE_RATE * (price * (1 - price)) ** CRYPTO_FEE_EXPONENT
    return round(fee_shares * price, 4)


# =========================================================================
# BAYESIAN ENGINE
# =========================================================================
class BayesianEngine:
    """Computes Bayesian probability of UP outcome using Binance price data."""

    def __init__(self):
        self._vol_per_sec = DEFAULT_VOL_PER_SEC
        self._window_returns = []  # (return, duration_sec) from completed windows
        self._lock = threading.Lock()
        self._load_historical()

    def _load_historical(self):
        """Load completed window data from existing JSONL files."""
        loaded = 0
        for fname in [PREDICT_DATA_FILE, DATA_FILE]:
            if not os.path.exists(fname):
                continue
            try:
                with open(fname) as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        rec = json.loads(line)
                        op = rec.get("open_price")
                        cp = rec.get("close_price")
                        interval = rec.get("interval", 5)
                        if op and cp and op > 0:
                            ret = (cp - op) / op
                            duration = interval * 60
                            self._window_returns.append((ret, duration))
                            loaded += 1
            except Exception as e:
                log(f"  ⚠ Error loading {fname}: {e}")

        if loaded > 0:
            self._recompute_vol()
            log(f"  📊 Loaded {loaded} historical windows → σ/√s = {self._vol_per_sec:.6f}")
        else:
            log(f"  📊 No historical data — using default σ/√s = {self._vol_per_sec:.6f}")

    def _recompute_vol(self):
        """Recompute volatility per √second from all window returns."""
        if len(self._window_returns) < MIN_WINDOWS_FOR_VOL:
            return

        # Normalize returns to per-√second: z_i = R_i / √D_i
        # Under the model, z_i ~ N(0, σ²), so std(z_i) = σ
        normalized = [r / math.sqrt(d) for r, d in self._window_returns if d > 0]
        if len(normalized) < MIN_WINDOWS_FOR_VOL:
            return

        mean_z = sum(normalized) / len(normalized)
        var_z = sum((z - mean_z) ** 2 for z in normalized) / (len(normalized) - 1)
        new_vol = math.sqrt(var_z)

        if new_vol > 0:
            self._vol_per_sec = new_vol
            log(f"  📊 Vol recalibrated: σ/√s = {self._vol_per_sec:.6f} "
                f"(from {len(normalized)} windows, ~{self._vol_per_sec * math.sqrt(300) * 100:.3f}% per 5min)")

    def add_completed_window(self, ret, duration_sec):
        """Add a completed window's return to the dataset and recalibrate."""
        with self._lock:
            self._window_returns.append((ret, duration_sec))
            self._recompute_vol()

    def prob_up(self, current_return, seconds_remaining):
        """
        P(UP) = Φ( log(1+R) / (σ √t_rem) )

        current_return: (S(t) - S(0)) / S(0)  (e.g. 0.001 = 0.1%)
        seconds_remaining: seconds until window closes
        """
        if seconds_remaining <= 0:
            # Window is over — return is final
            return 1.0 if current_return > 0 else 0.0

        with self._lock:
            vol = self._vol_per_sec

        # Avoid log of non-positive
        if current_return <= -1:
            return 0.0

        log_return = math.log(1 + current_return)
        denom = vol * math.sqrt(seconds_remaining)

        if denom <= 0:
            return 0.5

        z = log_return / denom
        # Φ(z) = 0.5 * (1 + erf(z / √2))
        p = 0.5 * (1 + math.erf(z / math.sqrt(2)))
        return max(0.001, min(0.999, p))

    @property
    def vol_per_sec(self):
        with self._lock:
            return self._vol_per_sec

    @property
    def n_windows(self):
        with self._lock:
            return len(self._window_returns)


# =========================================================================
# BINANCE PRICE FEED
# =========================================================================
class BinanceFeed:
    """Real-time crypto prices from Binance WebSocket."""

    def __init__(self):
        self._prices = {}
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

        streams = "/".join(f"{sym}@miniTicker" for sym in CRYPTOS.values())
        url = f"{BINANCE_WS_URL}?streams={streams}"

        while True:
            try:
                async with websockets.connect(url, close_timeout=5, open_timeout=10) as ws:
                    self._connected = True
                    log("  🔌 Binance WS connected")

                    while True:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            await ws.ping()
                            continue

                        try:
                            data = json.loads(msg)
                        except (json.JSONDecodeError, TypeError):
                            continue

                        payload = data.get("data", data)
                        symbol = payload.get("s", "").lower()
                        close_price = payload.get("c")

                        if symbol and close_price:
                            with self._lock:
                                self._prices[symbol] = {
                                    "price": float(close_price),
                                    "ts": time.time(),
                                }

            except Exception as e:
                self._connected = False
                log(f"  ⚠ Binance WS error: {e}, reconnecting in 2s...")
                await asyncio.sleep(2)

    def get_price(self, crypto):
        """Get latest price for a crypto (e.g. 'btc'). Returns (price, age_secs) or (None, None)."""
        symbol = CRYPTOS.get(crypto.lower())
        if not symbol:
            return None, None
        with self._lock:
            data = self._prices.get(symbol)
        if not data:
            return None, None
        return data["price"], time.time() - data["ts"]

    @property
    def connected(self):
        return self._connected


# =========================================================================
# POLYMARKET PRICE FEED
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
                                "custom_feature_enabled": True,
                            }))
                            last_sub_time = time.time()
                            if changed:
                                log(f"  📡 Polymarket subscribed to {len(wanted)} tokens")
                            self._active = wanted

                        if time.time() - last_ping > 10:
                            try:
                                await ws.send("ping")
                                last_ping = time.time()
                            except Exception:
                                break

                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=0.1)
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

    @property
    def connected(self):
        return self._connected


# =========================================================================
# MARKET DISCOVERY
# =========================================================================
def discover_markets():
    """Fetch current Polymarket crypto updown markets."""
    markets = []
    now = datetime.now(timezone.utc)

    for interval in [5, 15]:
        aligned = (now.minute // interval) * interval
        window_start = now.replace(minute=aligned, second=0, microsecond=0)
        epoch = int(window_start.timestamp())

        for crypto in CRYPTOS:
            slug = f"{crypto}-updown-{interval}m-{epoch}"
            try:
                resp = requests.get(f"{GAMMA_API}/events/slug/{slug}", timeout=10)
                if resp.status_code == 200:
                    event = resp.json()
                    for m in event.get("markets", []):
                        if not m.get("closed"):
                            m["_crypto"] = crypto
                            m["_interval"] = interval
                            m["_epoch"] = epoch
                            markets.append(m)
            except Exception:
                pass
    return markets


# =========================================================================
# SNIPER — window manager + trade execution
# =========================================================================
class Sniper:
    """Tracks active windows, computes Bayesian edge, executes trades."""

    def __init__(self, binance, poly, engine):
        self.binance = binance
        self.poly = poly
        self.engine = engine
        self._windows = {}        # key -> window_state
        self._trades = []         # completed trade records
        self._lock = threading.Lock()
        self._session_start = time.time()
        self._session_cost = 0.0  # Total USDC spent on buys
        self._session_won = 0.0   # Total USDC from winning trades
        self._trade_count = 0
        self._win_count = 0
        self._start_balance = get_usdc_balance()

    def update_markets(self, markets):
        """Add new market windows, subscribe tokens."""
        all_tokens = []
        now = datetime.now(timezone.utc)

        for m in markets:
            question = m.get("question", "")
            tokens = json.loads(m.get("clobTokenIds", "[]"))
            if len(tokens) != 2:
                continue

            crypto = m["_crypto"]
            interval = m["_interval"]
            epoch = m["_epoch"]

            # Compute window times from epoch (reliable, no timezone parsing needed)
            start_utc = datetime.fromtimestamp(epoch, tz=timezone.utc)
            end_utc = start_utc + timedelta(minutes=interval)

            if now >= end_utc:
                continue

            key = f"{crypto}_{interval}m_{epoch}"
            all_tokens.extend(tokens)

            with self._lock:
                if key not in self._windows:
                    self._windows[key] = {
                        "key": key,
                        "question": question,
                        "crypto": crypto,
                        "interval": interval,
                        "start_utc": start_utc,
                        "end_utc": end_utc,
                        "duration": interval * 60,
                        "up_token": tokens[0],
                        "down_token": tokens[1],
                        "open_price": None,
                        "close_price": None,
                        "traded": False,
                        "trade_info": None,   # filled in if we trade
                        "completed": False,
                        "signals": [],        # log of signals seen
                    }

        self.poly.subscribe(all_tokens)

    def tick(self):
        """Main tick: compute signals, execute trades, finalize windows."""
        now = datetime.now(timezone.utc)
        to_remove = []

        with self._lock:
            windows = list(self._windows.items())

        for key, w in windows:
            start = w["start_utc"]
            end = w["end_utc"]
            duration = w["duration"]
            elapsed = (now - start).total_seconds()
            pct = elapsed / duration if duration > 0 else 0
            remaining = max(0, (end - now).total_seconds())

            crypto = w["crypto"]
            binance_price, binance_age = self.binance.get_price(crypto)

            # Record open price at window start
            if w["open_price"] is None and binance_price is not None:
                if pct <= 0.05:
                    w["open_price"] = binance_price
                elif pct <= 0.15:
                    w["open_price"] = binance_price  # Late join, best we can do

            # ─── Compute Bayesian signal ───
            if (w["open_price"] is not None
                    and binance_price is not None
                    and not w["traded"]
                    and not w["completed"]):

                current_return = (binance_price - w["open_price"]) / w["open_price"]
                p_up = self.engine.prob_up(current_return, remaining)

                # Get Polymarket implied probability
                up_bid, up_ask, poly_age = self.poly.get_price(w["up_token"])
                down_bid, down_ask, _ = self.poly.get_price(w["down_token"])

                implied_up = None
                if up_bid and up_ask and up_bid > 0 and up_ask > 0:
                    implied_up = (up_bid + up_ask) / 2.0

                if implied_up is not None:
                    edge_up = p_up - implied_up           # + means UP token is cheap
                    edge_down = (1.0 - p_up) - (1.0 - implied_up)  # = -edge_up

                    # Log signal for dashboard
                    signal = {
                        "pct": round(pct, 3),
                        "return": round(current_return * 100, 4),
                        "p_up": round(p_up, 4),
                        "implied_up": round(implied_up, 4),
                        "edge_up": round(edge_up, 4),
                        "remaining": round(remaining, 1),
                    }
                    # Keep last 5 signals per window
                    w["signals"] = w["signals"][-4:] + [signal]

                    # ─── Trade decision ───
                    if (MIN_ELAPSED_PCT <= pct <= MAX_ELAPSED_PCT
                            and abs(current_return) >= MIN_RETURN_ABS):

                        # Determine which side to buy
                        if edge_up >= MIN_EDGE:
                            # UP token is underpriced → buy UP
                            token_id = w["up_token"]
                            buy_side = "UP"
                            buy_price = up_ask  # Cross spread for instant fill
                            edge = edge_up
                            our_prob = p_up
                        elif edge_down >= MIN_EDGE:
                            # DOWN token is underpriced → buy DOWN
                            token_id = w["down_token"]
                            buy_side = "DOWN"
                            buy_price = down_ask if down_ask and down_ask > 0 else (1.0 - up_bid if up_bid else None)
                            edge = edge_down
                            our_prob = 1.0 - p_up
                        else:
                            continue  # Not enough edge

                        if buy_price is None or buy_price <= 0:
                            continue

                        # Price range check
                        if not (ENTRY_PRICE_MIN <= buy_price <= ENTRY_PRICE_MAX):
                            continue

                        # Balance check
                        balance = get_usdc_balance()
                        if balance is None or balance < BET_AMOUNT + 2:
                            continue

                        # Count active positions
                        active_count = sum(
                            1 for ww in self._windows.values()
                            if ww.get("traded") and not ww.get("completed")
                        )
                        if active_count >= MAX_POSITIONS:
                            continue

                        # ─── EXECUTE TRADE ───
                        log(f"  🎯 SIGNAL {key}: {buy_side} | P(UP)={p_up:.3f} impl={implied_up:.3f} "
                            f"edge={edge:.3f} | ret={current_return*100:+.3f}% | {remaining:.0f}s left")

                        success = self._execute_buy(
                            w, token_id, buy_side, buy_price, edge, our_prob, current_return
                        )
                        if success:
                            w["traded"] = True

            # ─── Finalize completed windows ───
            if now >= end and not w["completed"]:
                if binance_price is not None:
                    w["close_price"] = binance_price

                if w["open_price"] and w["close_price"]:
                    outcome = "UP" if w["close_price"] > w["open_price"] else "DOWN"
                    ret = (w["close_price"] - w["open_price"]) / w["open_price"]

                    # Update volatility estimate
                    self.engine.add_completed_window(ret, duration)

                    # Resolve trade PnL
                    if w["traded"] and w["trade_info"]:
                        ti = w["trade_info"]
                        won = (ti["side"] == outcome)
                        pnl = ti["shares"] * (1.0 - ti["entry_price"]) if won else -ti["cost"]
                        ti["outcome"] = outcome
                        ti["won"] = won
                        ti["pnl"] = round(pnl, 4)

                        self._trade_count += 1
                        if won:
                            self._win_count += 1
                            self._session_won += ti["shares"]  # get $1 per share
                        self._session_cost += ti["cost"]

                        emoji = "✅" if won else "❌"
                        log(f"  {emoji} {key}: {outcome} | trade={ti['side']} @ {ti['entry_price']:.2f} "
                            f"| {'WON' if won else 'LOST'} ${abs(pnl):.2f} | "
                            f"session {self._win_count}/{self._trade_count} wins")
                    else:
                        log(f"  📊 {key}: {outcome} (no trade) | ret={ret*100:+.3f}%")

                    # Save completed window data
                    self._save_window(w, outcome, ret)
                else:
                    log(f"  ⚠ {key}: ended but missing price data")

                w["completed"] = True
                to_remove.append(key)

        # Clean up
        with self._lock:
            for key in to_remove:
                self._windows.pop(key, None)

    def _execute_buy(self, w, token_id, side, buy_price, edge, our_prob, current_return):
        """Place a GTC limit buy and verify fill. Returns True if filled."""
        buy_price = round(buy_price, 2)
        est_shares = BET_AMOUNT / buy_price
        fee_est = compute_taker_fee(est_shares, buy_price)

        log(f"  💰 Buying {side}: ~{est_shares:.0f}sh @ {buy_price:.2f} "
            f"(${BET_AMOUNT:.0f} + ~${fee_est:.2f} fee)")

        if DRY_RUN:
            log(f"  🧪 DRY RUN — skipping execution")
            w["trade_info"] = {
                "side": side,
                "token_id": token_id,
                "entry_price": buy_price,
                "shares": est_shares,
                "cost": BET_AMOUNT + fee_est,
                "edge": edge,
                "our_prob": our_prob,
                "return_at_entry": current_return,
                "dry_run": True,
                "outcome": None,
                "won": None,
                "pnl": None,
            }
            return True

        try:
            # Pre-trade balance
            bal_before = get_usdc_balance()

            # Place GTC limit buy at ask price (will cross spread → taker fill)
            buy_order = OrderArgs(
                token_id=token_id,
                price=buy_price,
                size=est_shares,
                side=BUY,
            )
            signed_buy = client.create_order(buy_order)
            resp = client.post_order(signed_buy, OrderType.GTC)

            order_id = resp.get("orderID", "") if isinstance(resp, dict) else ""
            resp_status = resp.get("status", "") if isinstance(resp, dict) else ""

            if not resp_status or resp_status == "error":
                log(f"  ⚠ Buy rejected: {resp}")
                return False

            log(f"  📝 Order placed (id: {order_id[:8] if order_id else '?'})")

            # Wait for fill
            time.sleep(FILL_WAIT)

            # Check if we got shares
            actual = get_share_balance(token_id)
            if actual is not None and actual >= 1:
                # Compute actual cost
                bal_after = get_usdc_balance()
                actual_cost = (bal_before - bal_after) if bal_before and bal_after else BET_AMOUNT + fee_est

                log(f"  ✅ Filled! {actual:.1f} shares, cost ${actual_cost:.2f}")

                w["trade_info"] = {
                    "side": side,
                    "token_id": token_id,
                    "entry_price": buy_price,
                    "shares": actual,
                    "cost": actual_cost,
                    "edge": edge,
                    "our_prob": our_prob,
                    "return_at_entry": current_return,
                    "dry_run": False,
                    "outcome": None,
                    "won": None,
                    "pnl": None,
                }
                return True
            else:
                # Not filled — cancel and move on
                log(f"  ⏳ Not filled after {FILL_WAIT}s — cancelling")
                try:
                    if order_id:
                        client.cancel(order_id)
                except Exception:
                    pass
                return False

        except Exception as e:
            log(f"  ⚠ Buy error: {e}")
            return False

    def _save_window(self, w, outcome, ret):
        """Write completed window data to JSONL."""
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "market": w["question"],
            "crypto": w["crypto"],
            "interval": w["interval"],
            "window_start": w["start_utc"].isoformat(),
            "window_end": w["end_utc"].isoformat(),
            "open_price": w["open_price"],
            "close_price": w["close_price"],
            "outcome": outcome,
            "return_pct": round(ret * 100, 4),
            "signals": w["signals"],
            "traded": w["traded"],
            "trade": w["trade_info"],
        }
        with open(DATA_FILE, "a") as f:
            f.write(json.dumps(record) + "\n")

        # Also save trade to trade log if we traded
        if w["traded"] and w["trade_info"]:
            ti = w["trade_info"]
            trade_entry = {
                "time": datetime.now(timezone.utc).isoformat(),
                "window": w["key"],
                "crypto": w["crypto"],
                "interval": w["interval"],
                "side": ti["side"],
                "entry_price": ti["entry_price"],
                "shares": ti["shares"],
                "cost": ti["cost"],
                "edge": ti["edge"],
                "our_prob": ti["our_prob"],
                "return_at_entry": ti["return_at_entry"],
                "outcome": ti.get("outcome"),
                "won": ti.get("won"),
                "pnl": ti.get("pnl"),
                "dry_run": ti.get("dry_run", False),
            }

            # Append to trades JSON
            trades = []
            if os.path.exists(TRADE_LOG):
                try:
                    with open(TRADE_LOG) as f:
                        trades = json.load(f)
                except Exception:
                    pass
            trades.append(trade_entry)
            with open(TRADE_LOG, "w") as f:
                json.dump(trades, f, indent=2)

    def get_dashboard(self):
        """Formatted dashboard string."""
        lines = []
        now = datetime.now(timezone.utc)

        with self._lock:
            windows = sorted(self._windows.values(), key=lambda w: w["key"])

        if not windows:
            lines.append("  (no active windows)")
        else:
            for w in windows:
                start = w["start_utc"]
                end = w["end_utc"]
                duration = w["duration"]
                elapsed = (now - start).total_seconds()
                pct = min(elapsed / duration, 1.0) if duration > 0 else 0
                remaining = max(0, (end - now).total_seconds())

                crypto = w["crypto"].upper()
                binance_price, _ = self.binance.get_price(w["crypto"])
                up_bid, up_ask, _ = self.poly.get_price(w["up_token"])

                # Price & return
                price_str = f"${binance_price:,.2f}" if binance_price else "?"
                open_str = f"${w['open_price']:,.2f}" if w["open_price"] else "?"

                ret_str = "?"
                direction = "⚪"
                p_up_str = ""
                edge_str = ""

                if w["open_price"] and binance_price:
                    ret = (binance_price - w["open_price"]) / w["open_price"]
                    direction = "🟢" if ret > 0 else ("🔴" if ret < 0 else "⚪")
                    ret_str = f"{ret*100:+.3f}%"

                    # Bayesian prob
                    p_up = self.engine.prob_up(ret, remaining)
                    p_up_str = f"P(UP)={p_up:.2f}"

                    # Edge vs Polymarket
                    if up_bid and up_ask and up_bid > 0 and up_ask > 0:
                        implied = (up_bid + up_ask) / 2.0
                        edge = p_up - implied
                        edge_str = f"edge={edge:+.2f}"

                traded_str = " 🎯TRADED" if w["traded"] else ""

                bar_len = 20
                filled = int(pct * bar_len)
                bar = "█" * filled + "░" * (bar_len - filled)

                lines.append(
                    f"  {crypto:4s} {w['interval']:2d}m │{bar}│ {pct:5.1%} {remaining:3.0f}s │ "
                    f"{direction} {ret_str:>8s} │ {p_up_str:>10s} {edge_str:>10s} │ "
                    f"now={price_str} open={open_str}{traded_str}"
                )

                # Show last signal
                if w["signals"]:
                    s = w["signals"][-1]
                    lines.append(
                        f"         └ @{s['pct']:.0%}: ret={s['return']:+.3f}% "
                        f"P(UP)={s['p_up']:.3f} impl={s['implied_up']:.3f} "
                        f"edge={s['edge_up']:+.3f}"
                    )

        # ─── Session summary ───
        elapsed_min = (time.time() - self._session_start) / 60
        lines.append(f"\n{'─' * 90}")

        # Balance
        bal = get_usdc_balance()
        bal_str = f"${bal:.2f}" if bal else "?"
        start_str = f"${self._start_balance:.2f}" if self._start_balance else "?"
        if bal and self._start_balance:
            delta = bal - self._start_balance
            lines.append(f"  💵 Balance: {bal_str} (start: {start_str}, Δ{delta:+.2f})")
        else:
            lines.append(f"  💵 Balance: {bal_str}")

        # Trade stats
        wr = self._win_count / self._trade_count * 100 if self._trade_count > 0 else 0
        net_pnl = self._session_won - self._session_cost
        lines.append(
            f"  📈 Trades: {self._trade_count} | Wins: {self._win_count} ({wr:.0f}%) | "
            f"Net PnL: ${net_pnl:+.2f} | Runtime: {elapsed_min:.1f}min"
        )

        # Model info
        lines.append(
            f"  🧠 Model: σ/√s={self.engine.vol_per_sec:.6f} "
            f"(~{self.engine.vol_per_sec * math.sqrt(300) * 100:.3f}%/5min) | "
            f"Data: {self.engine.n_windows} windows | "
            f"MIN_EDGE={MIN_EDGE} MIN_ELAPSED={MIN_ELAPSED_PCT:.0%}"
        )

        return "\n".join(lines)


# =========================================================================
# MAIN
# =========================================================================
def main():
    mode = "DRY RUN" if DRY_RUN else "LIVE"
    log(f"═══ Sniper Bot ═══ [{mode}]")
    log(f"Strategy: Bayesian prediction → buy underpriced token → hold to resolution")
    log(f"Params: BET=${BET_AMOUNT} MIN_EDGE={MIN_EDGE} ELAPSED=[{MIN_ELAPSED_PCT:.0%},{MAX_ELAPSED_PCT:.0%}]")
    log(f"Tracking: {', '.join(c.upper() for c in CRYPTOS)}")
    log("")

    # Start feeds
    binance = BinanceFeed()
    binance.start()

    poly = PolymarketFeed()
    poly.start()

    engine = BayesianEngine()
    sniper = Sniper(binance, poly, engine)

    # Wait for WS connections
    log("Waiting for WebSocket connections...")
    for _ in range(30):
        if binance.connected and poly.connected:
            break
        time.sleep(1)

    if not binance.connected:
        log("⚠ Binance WS not connected after 30s")
    if not poly.connected:
        log("⚠ Polymarket WS not connected after 30s")

    log("")
    last_discovery = 0

    while True:
        try:
            now = time.time()

            # Discover markets periodically
            if now - last_discovery > MARKET_POLL_INTERVAL:
                markets = discover_markets()
                if markets:
                    sniper.update_markets(markets)
                last_discovery = now

            # Main tick: signals + trades + finalization
            sniper.tick()

            # Dashboard
            now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
            bn_status = "🟢" if binance.connected else "🔴"
            pm_status = "🟢" if poly.connected else "🔴"
            dashboard = sniper.get_dashboard()

            print(f"\033[2J\033[H", end="")
            print(f"═══ Sniper Bot [{mode}] ═══  {now_str} UTC  │  Binance {bn_status}  Polymarket {pm_status}")
            print(f"{'─' * 90}")
            print(dashboard)
            print(f"\n{'─' * 90}")
            print(f"Data: {DATA_FILE} | Trades: {TRADE_LOG} | Log: {DECISION_LOG}")
            print(f"Press Ctrl+C to stop")

            time.sleep(TICK_INTERVAL)

        except KeyboardInterrupt:
            log(f"\nStopped.")
            bal = get_usdc_balance()
            if bal and sniper._start_balance:
                log(f"Final balance: ${bal:.2f} (Δ{bal - sniper._start_balance:+.2f})")
            log(f"Trades: {sniper._trade_count} | Wins: {sniper._win_count}")
            break
        except Exception as e:
            log(f"⚠ Main loop error: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5)


if __name__ == "__main__":
    main()
