#!/usr/bin/env python3
"""
Cross-Exchange Arbitrage Bot — Spot vs Polymarket

Replicates the behavior of top profitable wallets on Polymarket 5-min
crypto "Up or Down" markets.

Strategy (reverse-engineered from wallet analysis):
  1. Stream real-time Binance spot prices for BTC/ETH/SOL/XRP
  2. For each 5-min window, determine open price at window start
  3. Compute "fair probability" of Up based on current spot vs open:
       P(up) = Φ( log(1 + R) / (σ√t_remaining) )
  4. Compare to Polymarket book prices (Up bid/ask, Down bid/ask)
  5. When Polymarket underprices a side by ≥ MIN_EDGE, aggressively buy
  6. When Polymarket overprices a side we hold, sell for profit
  7. Trade FREQUENTLY — every few seconds if edge exists
  8. Accumulate both sides over the window, bought at different moments
  9. At resolution: paired shares pay $1 guaranteed

Key difference from arb.py:
  - arb.py: passive limit orders on both sides simultaneously → never fills
  - This bot: aggressive taker (FAK) on whichever side is mispriced RIGHT NOW
  - Uses Binance spot as ground truth for "fair value"
  - Trades many times per window, not once
"""

import os
import sys
import time
import json
import math
import asyncio
import threading
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque
from scipy.stats import norm
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    OrderArgs, OrderType, OpenOrderParams,
    BalanceAllowanceParams, AssetType,
    MarketOrderArgs,
)
from py_clob_client.order_builder.constants import BUY, SELL
from dotenv import load_dotenv

load_dotenv()

# =========================================================================
# CONFIG
# =========================================================================
HOST            = "https://clob.polymarket.com"
GAMMA_API       = "https://gamma-api.polymarket.com"
WS_MARKET_URL   = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BINANCE_WS_URL  = "wss://stream.binance.com:9443/stream"
RTDS_WS_URL     = "wss://ws-live-data.polymarket.com"
CHAIN_ID        = 137

# Chainlink symbols (Polymarket resolution source)
CHAINLINK_SYMBOLS = {
    "btc": "btc/usd",
    "eth": "eth/usd",
    "sol": "sol/usd",
    "xrp": "xrp/usd",
}
PRICE_BUFFER_SECONDS = 900  # Keep 15 min of price history

# -- Strategy --
MIN_EDGE        = 0.06       # Minimum edge (fair_prob - market_price) to trade
BET_AMOUNT      = 1          # $ per trade (small, frequent)
MAX_BET         = 2          # $ cap per single trade
MIN_BET_SHARES  = 5          # Polymarket min order size
MAX_EXPOSURE    = 4          # Max total $ deployed per window (both sides combined)
MAX_UNPAIRED    = 3          # Max $ of unpaired directional exposure per window
TRADE_COOLDOWN  = 5          # Seconds between trades on the same side
MIN_ELAPSED_S   = 210        # Trade in last 90s only (wallets: 240-300s into 300s window)
STOP_BEFORE_END = 5          # Stop trading 5s before window ends (wallets trade until last second)

# -- Fair value model (Bayesian / Black-Scholes) --
DEFAULT_VOL     = 5e-5       # σ per √second (empirical: 3-8e-5 for BTC 5-min)
                             # Lower → more extreme probabilities → trades on strong moves

# -- Risk --
MIN_BALANCE     = 2          # Stop trading below this USDC
MAX_WINDOWS     = 2          # Max simultaneous windows ($11 budget)
BOOK_STALE_S    = 10         # Ignore book data older than 10s
PRICE_STALE_S   = 5          # Ignore Binance price older than 5s
MAX_TOTAL_COST  = 8          # Max total $ across all windows
MIN_ASK_PRICE   = 0.05       # Don't buy below 5c (extreme longshots lose almost always)
MAX_EDGE        = 0.30       # If model says edge > 30%, model is probably wrong — skip

# -- Markets --
CRYPTOS = {
    "btc": "btcusdt",
    "eth": "ethusdt",
    "sol": "solusdt",
    "xrp": "xrpusdt",
}
INTERVALS = [5]

TICK = 0.01
WS_WARMUP = 3

DRY_RUN     = "--dry-run" in sys.argv
DRY_BALANCE = 1000.0
LOG_FILE    = "xarb_dry_log.txt" if DRY_RUN else "xarb_log.txt"
TRADE_FILE  = "xarb_dry_trades.json" if DRY_RUN else "xarb_trades.json"


def log(msg):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


# =========================================================================
# CLOB CLIENT
# =========================================================================
private_key = os.getenv("PRIVATE_KEY")
funder_address = os.getenv("FUNDER_ADDRESS")

client = ClobClient(
    host=HOST, key=private_key, chain_id=CHAIN_ID,
    signature_type=1, funder=funder_address,
)
client.set_api_creds(client.create_or_derive_api_creds())


def get_usdc_balance():
    try:
        ba = client.get_balance_allowance(
            params=BalanceAllowanceParams(
                asset_type=AssetType.COLLATERAL, token_id="", signature_type=1))
        return float(ba.get("balance", 0)) / 1e6
    except Exception as e:
        log(f"  balance error: {e}")
        return None


def get_share_balance(token_id):
    try:
        client.update_balance_allowance(
            params=BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL, token_id=token_id, signature_type=1))
        ba = client.get_balance_allowance(
            params=BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL, token_id=token_id, signature_type=1))
        return float(ba.get("balance", 0)) / 1e6
    except Exception:
        return None


def cancel_all_orders(token_id=None):
    try:
        if token_id:
            client.cancel_market_orders(asset_id=token_id)
        else:
            client.cancel_all()
        return True
    except Exception as e:
        log(f"  cancel error: {e}")
        return False


def post_fak_buy(token_id, amount):
    """Place a FAK (fill-and-kill) market buy. Returns (shares, cost) or (0, 0)."""
    try:
        mo = MarketOrderArgs(
            token_id=token_id,
            amount=amount,
            side=BUY,
            order_type=OrderType.FAK,
        )
        signed = client.create_market_order(mo)
        resp = client.post_order(signed, OrderType.FAK)

        shares = 0
        cost = amount  # fallback
        if isinstance(resp, dict):
            taking = resp.get("takingAmount", "0")
            making = resp.get("makingAmount", "0")
            try:
                shares = float(taking) if taking else 0
            except (ValueError, TypeError):
                pass
            try:
                filled_cost = float(making) if making else 0
                if filled_cost > 0:
                    cost = filled_cost
            except (ValueError, TypeError):
                pass
        return shares, cost
    except Exception as e:
        log(f"    FAK buy error: {e}")
        return 0, 0


def post_fak_sell(token_id, amount):
    """Place a FAK market sell. amount = shares to sell. Returns (shares_sold, proceeds)."""
    try:
        mo = MarketOrderArgs(
            token_id=token_id,
            amount=amount,
            side=SELL,
            order_type=OrderType.FAK,
        )
        signed = client.create_market_order(mo)
        resp = client.post_order(signed, OrderType.FAK)

        shares_sold = 0
        proceeds = 0
        if isinstance(resp, dict):
            taking = resp.get("takingAmount", "0")
            making = resp.get("makingAmount", "0")
            try:
                shares_sold = float(taking) if taking else 0
            except (ValueError, TypeError):
                pass
            try:
                proceeds = float(making) if making else 0
            except (ValueError, TypeError):
                pass
        return shares_sold, proceeds
    except Exception as e:
        log(f"    FAK sell error: {e}")
        return 0, 0


# =========================================================================
# MARKET DISCOVERY
# =========================================================================
def discover_markets():
    """Find active 5-min crypto updown markets."""
    markets = []
    now = datetime.now(timezone.utc)
    for interval in INTERVALS:
        aligned = (now.minute // interval) * interval
        window_start = now.replace(minute=aligned, second=0, microsecond=0)
        window_end = window_start + timedelta(minutes=interval)
        epoch = int(window_start.timestamp())
        secs_left = (window_end - now).total_seconds()

        for crypto in CRYPTOS:
            slug = f"{crypto}-updown-{interval}m-{epoch}"
            try:
                resp = requests.get(f"{GAMMA_API}/events/slug/{slug}", timeout=10)
                if resp.status_code != 200:
                    continue
                event = resp.json()
                for m in event.get("markets", []):
                    if m.get("closed"):
                        continue
                    raw_tokens = m.get("clobTokenIds", [])
                    if isinstance(raw_tokens, str):
                        try:
                            raw_tokens = json.loads(raw_tokens)
                        except Exception:
                            continue
                    if len(raw_tokens) != 2:
                        continue

                    raw_outcomes = m.get("outcomes", [])
                    if isinstance(raw_outcomes, str):
                        try:
                            outcomes = json.loads(raw_outcomes)
                        except Exception:
                            outcomes = raw_outcomes
                    else:
                        outcomes = raw_outcomes

                    up_idx = 0
                    for i, o in enumerate(outcomes):
                        if "up" in o.lower():
                            up_idx = i
                            break
                    down_idx = 1 - up_idx

                    markets.append({
                        "crypto": crypto.upper(),
                        "interval": interval,
                        "slug": slug,
                        "condition_id": m.get("conditionId", ""),
                        "up_token": raw_tokens[up_idx],
                        "down_token": raw_tokens[down_idx],
                        "window_start": window_start,
                        "window_end": window_end,
                        "secs_left": secs_left,
                    })
            except Exception:
                continue
    return markets


# =========================================================================
# FAIR VALUE MODEL
# =========================================================================
def fair_prob_up(current_return, secs_remaining, vol=DEFAULT_VOL):
    """
    Bayesian probability that the crypto finishes UP from open.

    P(up) = Φ( log(1 + R) / (σ√t) )

    - R = (price_now - open) / open
    - σ = realized volatility per √second
    - t = seconds remaining
    - Φ = standard normal CDF

    When R > 0 (price above open), P(up) > 0.5.
    As t → 0, P(up) → 1 if R > 0 (certainty increases).
    """
    if secs_remaining <= 1:
        return 1.0 if current_return >= 0 else 0.0
    if abs(current_return) < 1e-10:
        return 0.5

    try:
        z = math.log(1 + current_return) / (vol * math.sqrt(secs_remaining))
        return float(norm.cdf(z))
    except (ValueError, ZeroDivisionError):
        return 0.5


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
                    log("  Binance WS connected")

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
                log(f"  Binance WS error: {e}, reconnecting in 2s...")
                await asyncio.sleep(2)

    def get_price(self, crypto):
        """Returns (price, age_secs) or (None, None)."""
        symbol = CRYPTOS.get(crypto.lower())
        if not symbol:
            return None, None
        with self._lock:
            data = self._prices.get(symbol)
        if not data:
            return None, None
        return data["price"], time.time() - data["ts"]


# =========================================================================
# CHAINLINK PRICE FEED  (Polymarket resolution source)
# =========================================================================
class ChainlinkFeed:
    """Real-time Chainlink prices from Polymarket RTDS WebSocket.
    This is the SAME data source Polymarket uses to resolve crypto markets.
    Keeps a rolling buffer so we can look up the EXACT price at any timestamp."""

    def __init__(self):
        self._prices = {}     # symbol -> {price, ts}
        self._buffers = {}    # symbol -> deque of (unix_ts_secs, price)
        self._lock = threading.Lock()
        self._connected = False

    def start(self):
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._ws_loop())

    async def _ws_loop(self):
        import websockets

        while True:
            try:
                async with websockets.connect(RTDS_WS_URL, close_timeout=5, open_timeout=10) as ws:
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
                    log("  Chainlink RTDS connected (resolution source)")

                    last_ping = time.time()
                    while True:
                        if time.time() - last_ping > 4:
                            try:
                                await ws.send("PING")
                                last_ping = time.time()
                            except Exception:
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
                            ts = payload.get("timestamp")
                            if symbol and value is not None:
                                price_val = float(value)
                                ts_secs = (ts / 1000.0) if ts else time.time()
                                with self._lock:
                                    self._prices[symbol] = {"price": price_val, "ts": ts_secs}
                                    if symbol not in self._buffers:
                                        self._buffers[symbol] = deque()
                                    buf = self._buffers[symbol]
                                    buf.append((ts_secs, price_val))
                                    cutoff = time.time() - PRICE_BUFFER_SECONDS
                                    while buf and buf[0][0] < cutoff:
                                        buf.popleft()

            except Exception as e:
                self._connected = False
                log(f"  Chainlink RTDS error: {e}, reconnecting in 2s...")
                await asyncio.sleep(2)

    def get_price(self, crypto):
        """Get latest Chainlink price. Returns (price, age_secs) or (None, None)."""
        symbol = CHAINLINK_SYMBOLS.get(crypto.lower())
        if not symbol:
            return None, None
        with self._lock:
            data = self._prices.get(symbol)
        if not data:
            return None, None
        return data["price"], time.time() - data["ts"]

    def get_price_at(self, crypto, target_ts):
        """Get Chainlink price at a specific timestamp (for open price lookup).
        Returns (price, delta_secs) or (None, None)."""
        symbol = CHAINLINK_SYMBOLS.get(crypto.lower())
        if not symbol:
            return None, None
        with self._lock:
            buf = self._buffers.get(symbol)
            if not buf or len(buf) == 0:
                return None, None
            lo, hi = 0, len(buf) - 1
            best_idx = None
            while lo <= hi:
                mid = (lo + hi) // 2
                if buf[mid][0] <= target_ts:
                    best_idx = mid
                    lo = mid + 1
                else:
                    hi = mid - 1
            if best_idx is not None:
                ts, price = buf[best_idx]
                return price, abs(target_ts - ts)
            ts, price = buf[0]
            return price, abs(target_ts - ts)

    @property
    def connected(self):
        return self._connected


# =========================================================================
# BOOK TRACKER (Polymarket order book via WebSocket)
# =========================================================================
class BookTracker:
    def __init__(self):
        self._lock = threading.Lock()
        self._books = {}
        self._last_update = {}
        self.ws_start_time = time.time()

    def on_snapshot(self, token_id, bids, asks):
        with self._lock:
            self._books[token_id] = {
                "bids": {float(b["price"]): float(b["size"]) for b in bids},
                "asks": {float(a["price"]): float(a["size"]) for a in asks},
                "bb": max((float(b["price"]) for b in bids), default=0),
                "ba": min((float(a["price"]) for a in asks), default=1),
            }
            self._last_update[token_id] = time.time()

    def on_delta(self, token_id, side, price, size, bb, ba):
        price, size = float(price), float(size)
        bb, ba = float(bb), float(ba)
        with self._lock:
            if token_id not in self._books:
                self._books[token_id] = {"bids": {}, "asks": {}, "bb": 0, "ba": 1}
            book = self._books[token_id]
            bucket = "bids" if side.upper() == "BUY" else "asks"
            if size == 0:
                book[bucket].pop(price, None)
            else:
                book[bucket][price] = size
            book["bb"] = bb
            book["ba"] = ba
            self._last_update[token_id] = time.time()

    def get_book(self, token_id):
        with self._lock:
            if time.time() - self.ws_start_time < WS_WARMUP:
                return None
            book = self._books.get(token_id)
            if not book:
                return None
            if time.time() - self._last_update.get(token_id, 0) > BOOK_STALE_S:
                return None
            bb, ba = book["bb"], book["ba"]
            if bb <= 0 or ba <= 0:
                return None
            mid = (bb + ba) / 2
            return {"bb": bb, "ba": ba, "mid": mid}


# =========================================================================
# MARKET WEBSOCKET
# =========================================================================
class MarketWS:
    def __init__(self, tracker: BookTracker):
        self.tracker = tracker
        self._tokens = set()
        self._force_reconnect = False

    def start(self):
        threading.Thread(target=self._run, daemon=True).start()

    def subscribe(self, token_ids):
        new = set(token_ids)
        if new != self._tokens:
            self._tokens = new
            self._force_reconnect = True

    def _run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._ws_loop())

    async def _ws_loop(self):
        import websockets

        while True:
            if not self._tokens:
                await asyncio.sleep(1)
                continue
            try:
                async with websockets.connect(
                    WS_MARKET_URL, close_timeout=5, open_timeout=10,
                    max_size=10 * 1024 * 1024,
                ) as ws:
                    self._force_reconnect = False
                    self.tracker.ws_start_time = time.time()
                    sub = {"type": "market", "assets_ids": list(self._tokens)}
                    await ws.send(json.dumps(sub))
                    log(f"  WS: subscribed to {len(self._tokens)} tokens")

                    while not self._force_reconnect:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            continue
                        msgs = json.loads(raw)
                        if not isinstance(msgs, list):
                            msgs = [msgs]
                        for msg in msgs:
                            etype = msg.get("event_type", "")
                            if etype == "book":
                                for aid, bd in msg.items():
                                    if aid == "event_type":
                                        continue
                                    if isinstance(bd, dict):
                                        bids = bd.get("bids", [])
                                        asks = bd.get("asks", [])
                                        if bids or asks:
                                            self.tracker.on_snapshot(aid, bids, asks)
                            elif etype == "price_change":
                                for ch in msg.get("price_changes", []):
                                    self.tracker.on_delta(
                                        ch["asset_id"], ch["side"],
                                        ch["price"], ch["size"],
                                        ch.get("best_bid", 0), ch.get("best_ask", 0))
                    log("  WS: reconnecting (token change)")
            except Exception as e:
                log(f"  WS error: {e}")
                await asyncio.sleep(2)


# =========================================================================
# TRADE RECORDER
# =========================================================================
def record_trade(data):
    try:
        existing = []
        if os.path.exists(TRADE_FILE):
            with open(TRADE_FILE) as f:
                existing = json.load(f)
        existing.append(data)
        with open(TRADE_FILE, "w") as f:
            json.dump(existing, f, indent=2)
    except Exception:
        pass


# =========================================================================
# CROSS-ARB BOT
# =========================================================================
class CrossArbBot:
    """
    Cross-exchange arbitrage: Binance spot vs Polymarket binary options.

    Per 5-min window:
      - Track Binance spot price and Polymarket book
      - Compute fair P(up) from spot vs open
      - Aggressively buy underpriced side (FAK market order)
      - Sell overpriced holdings
      - Repeat every few seconds
      - At resolution: paired shares → guaranteed $1
    """

    def __init__(self):
        self.tracker = BookTracker()
        self.ws = MarketWS(self.tracker)
        self.binance = BinanceFeed()
        self.chainlink = ChainlinkFeed()
        self.markets = []
        self.last_discovery = 0
        self.last_status = 0
        self.balance = 0
        self._shutting_down = False

        # Per-window state: condition_id → {...}
        self._windows = {}
        self._windows_lock = threading.Lock()
        self._known_tokens = set()

        # Session stats
        self.session_start = time.time()
        self.start_balance = 0
        self.total_buys = 0
        self.total_sells = 0
        self.total_cost = 0.0
        self.total_proceeds = 0.0
        self.windows_completed = 0

    def run(self):
        log("=" * 60)
        log(f"  XARB BOT — Cross-Exchange Arbitrage {'(DRY RUN)' if DRY_RUN else '(LIVE)'}")
        log("=" * 60)

        self.balance = DRY_BALANCE if DRY_RUN else (get_usdc_balance() or 0)
        self.start_balance = self.balance
        log(f"  Balance: ${self.balance:.2f}{' (simulated)' if DRY_RUN else ''}")
        log(f"  Config: min_edge={MIN_EDGE} bet=${BET_AMOUNT} max_exposure=${MAX_EXPOSURE}")
        log(f"  Vol: {DEFAULT_VOL:.1e}  cooldown={TRADE_COOLDOWN}s  ask_floor={MIN_ASK_PRICE}")
        log(f"  Price source: Chainlink RTDS (resolution-accurate)")
        log(f"  Markets: {', '.join(CRYPTOS)} x {INTERVALS}min")

        self.binance.start()
        self.chainlink.start()
        self.ws.start()

        # Wait for Chainlink (primary) and Binance (fallback)
        for _ in range(30):
            if self.chainlink.connected and self.binance._connected:
                break
            time.sleep(0.5)
        if not self.chainlink.connected:
            log("  WARNING: Chainlink RTDS not connected after 15s")
        if not self.binance._connected:
            log("  WARNING: Binance not connected after 15s")

        try:
            while True:
                self._tick()
                time.sleep(1)
        except KeyboardInterrupt:
            log("")
            log("  Shutting down...")
            self._shutting_down = True
            self._final_report()

    # -- Discovery --

    def _discover(self):
        now = time.time()
        if now - self.last_discovery < 5:
            return
        self.last_discovery = now

        markets = discover_markets()
        self.markets = [m for m in markets if m["secs_left"] > STOP_BEFORE_END + 10]
        if not self.markets:
            return

        all_tokens = set()
        for m in self.markets:
            all_tokens.add(m["up_token"])
            all_tokens.add(m["down_token"])
            self._known_tokens.add(m["up_token"])
            self._known_tokens.add(m["down_token"])

        self.ws.subscribe(all_tokens)

        cryptos = sorted(set(m["crypto"] for m in self.markets))
        if len(self.markets) != getattr(self, "_prev_count", 0):
            secs = self.markets[0]["secs_left"] if self.markets else 0
            log(f"  Tracking {len(self.markets)} markets: "
                f"{', '.join(cryptos)} ({secs:.0f}s left)")
            self._prev_count = len(self.markets)

    # -- Main tick (runs every second) --

    def _tick(self):
        self._discover()

        now = time.time()
        now_utc = datetime.now(timezone.utc)

        # Status every 30s
        if now - self.last_status > 30:
            self.last_status = now
            mins = (now - self.session_start) / 60
            with self._windows_lock:
                n_active = sum(1 for w in self._windows.values() if not w["done"])
            log(f"  [{mins:.0f}m] buys={self.total_buys} sells={self.total_sells} "
                f"cost=${self.total_cost:.2f} proceeds=${self.total_proceeds:.2f} "
                f"windows={self.windows_completed} active={n_active}")

        # Process each market
        for m in self.markets:
            cid = m["condition_id"]
            crypto = m["crypto"]
            secs_into = (now_utc - m["window_start"]).total_seconds()
            secs_left = (m["window_end"] - now_utc).total_seconds()

            # Initialize window state
            with self._windows_lock:
                if cid not in self._windows:
                    self._windows[cid] = {
                        "crypto": crypto,
                        "up_token": m["up_token"],
                        "down_token": m["down_token"],
                        "window_start": m["window_start"],
                        "window_end": m["window_end"],
                        "open_price": None,
                        "up_shares": 0,
                        "up_cost": 0.0,
                        "dn_shares": 0,
                        "dn_cost": 0.0,
                        "last_buy_up": 0,
                        "last_buy_dn": 0,
                        "last_sell_up": 0,
                        "last_sell_dn": 0,
                        "trades": [],
                        "done": False,
                        "last_log": 0,
                    }
                w = self._windows[cid]

            if w["done"]:
                continue

            # Window ended — mark done, report
            if secs_left <= 0:
                self._close_window(cid, w)
                continue

            # Get current price: prefer Chainlink (resolution source), fallback Binance
            spot, spot_age = self.chainlink.get_price(crypto)
            if spot is None or spot_age > PRICE_STALE_S:
                spot, spot_age = self.binance.get_price(crypto)
            if spot is None or spot_age > PRICE_STALE_S:
                continue

            # Set open price from Chainlink buffer at EXACT window start timestamp
            # (this is the "price to beat" that Polymarket resolves against)
            if w["open_price"] is None:
                start_ts = m["window_start"].timestamp()
                cl_open, delta = self.chainlink.get_price_at(crypto, start_ts)
                if cl_open is not None and delta <= 10.0:
                    w["open_price"] = cl_open
                    log(f"  {crypto} open price: ${cl_open:,.2f} (Chainlink, delta={delta:.1f}s)")
                elif secs_into <= 5 and spot is not None:
                    # Only use live price as fallback in the first 5s of window
                    w["open_price"] = spot
                    log(f"  {crypto} open price: ${spot:,.2f} (live fallback, {secs_into:.0f}s in)")
                else:
                    # Don't have the open price and too late to estimate — skip this window
                    if not w.get("_open_warned"):
                        w["_open_warned"] = True
                        log(f"  {crypto} no Chainlink open price (buffer too short, {secs_into:.0f}s in) — skipping window")
                    continue
                continue

            # Too early or too late
            if secs_into < MIN_ELAPSED_S:
                continue
            if secs_left < STOP_BEFORE_END:
                continue

            # Compute fair value
            current_return = (spot - w["open_price"]) / w["open_price"]
            secs_remaining = secs_left
            p_up = fair_prob_up(current_return, secs_remaining)
            p_dn = 1.0 - p_up

            # Get Polymarket books
            up_book = self.tracker.get_book(w["up_token"])
            dn_book = self.tracker.get_book(w["down_token"])
            if not up_book or not dn_book:
                continue

            # Diagnostic logging every 15s
            if now - w["last_log"] >= 15:
                w["last_log"] = now
                edge_up = p_up - up_book["ba"]
                edge_dn = p_dn - dn_book["ba"]
                ret_pct = current_return * 100
                log(f"    {crypto} spot=${spot:,.2f} R={ret_pct:+.3f}% "
                    f"P(up)={p_up:.3f} | "
                    f"Up ba={up_book['ba']:.2f} edge={edge_up:+.3f} | "
                    f"Dn ba={dn_book['ba']:.2f} edge={edge_dn:+.3f}")

            # Current exposure in this window
            total_exposure = w["up_cost"] + w["dn_cost"]
            paired = min(w["up_shares"], w["dn_shares"])
            unpaired_up = w["up_shares"] - paired
            unpaired_dn = w["dn_shares"] - paired
            unpaired_cost_up = unpaired_up * (w["up_cost"] / w["up_shares"]) if w["up_shares"] > 0 else 0
            unpaired_cost_dn = unpaired_dn * (w["dn_cost"] / w["dn_shares"]) if w["dn_shares"] > 0 else 0
            max_unpaired_cost = max(unpaired_cost_up, unpaired_cost_dn)

            # ── BUY LOGIC: buy underpriced side ──
            # Edge = our fair probability - Polymarket ask price
            # If edge > MIN_EDGE, Polymarket is underpricing this side

            for side, token, book, fair_p, last_key, shares_key, cost_key in [
                ("Up", w["up_token"], up_book, p_up, "last_buy_up", "up_shares", "up_cost"),
                ("Dn", w["down_token"], dn_book, p_dn, "last_buy_dn", "dn_shares", "dn_cost"),
            ]:
                ask = book["ba"]
                edge = fair_p - ask

                if edge < MIN_EDGE:
                    continue

                # Skip extreme longshots (market prices < 5c almost always lose)
                if ask < MIN_ASK_PRICE:
                    continue

                # Skip if edge is absurdly large (model probably wrong)
                if edge > MAX_EDGE:
                    continue

                # Cooldown
                if now - w[last_key] < TRADE_COOLDOWN:
                    continue

                # Exposure limits
                if total_exposure >= MAX_EXPOSURE:
                    continue
                if max_unpaired_cost >= MAX_UNPAIRED:
                    # Only allow buying the OPPOSITE side to reduce unpaired
                    if side == "Up" and unpaired_cost_up >= unpaired_cost_dn:
                        continue
                    if side == "Dn" and unpaired_cost_dn >= unpaired_cost_up:
                        continue

                # Balance check
                if not DRY_RUN:
                    bal = get_usdc_balance()
                    if bal is not None:
                        self.balance = bal
                    if self.balance < MIN_BALANCE:
                        continue

                # Size: scale with edge, cap at MAX_BET
                amt = min(BET_AMOUNT * (edge / MIN_EDGE), MAX_BET)
                amt = max(amt, 0.5)  # At least $0.50

                # Ensure we get MIN_BET_SHARES
                if ask > 0 and amt / ask < MIN_BET_SHARES:
                    amt = MIN_BET_SHARES * ask

                # Global cost cap (include this pending trade)
                remaining_budget = MAX_TOTAL_COST - (self.total_cost - self.total_proceeds)
                if remaining_budget <= 0.5:
                    continue
                amt = min(amt, remaining_budget)

                if amt > self.balance - MIN_BALANCE and not DRY_RUN:
                    amt = self.balance - MIN_BALANCE
                    if amt < 0.5:
                        continue

                log(f"    {crypto} BUY {side} ${amt:.2f} @ ~{ask:.2f} "
                    f"[edge={edge:.3f} P={fair_p:.3f}]")

                if DRY_RUN:
                    # Simulate fill at ask
                    sim_shares = amt / ask
                    w[shares_key] += sim_shares
                    w[cost_key] += amt
                    w[last_key] = now
                    self.total_buys += 1
                    self.total_cost += amt
                    log(f"    [SIM] +{sim_shares:.1f}sh @ {ask:.3f}")
                else:
                    shares, cost = post_fak_buy(token, amt)
                    if shares > 0:
                        w[shares_key] += shares
                        w[cost_key] += cost
                        w[last_key] = now
                        self.total_buys += 1
                        self.total_cost += cost
                        self.balance -= cost
                        avg = cost / shares if shares > 0 else 0
                        log(f"    FILLED +{shares:.1f}sh @ {avg:.3f} (${cost:.2f})")

                        w["trades"].append({
                            "time": datetime.now(timezone.utc).isoformat(),
                            "action": "BUY",
                            "side": side,
                            "shares": round(shares, 2),
                            "cost": round(cost, 4),
                            "avg_price": round(avg, 4),
                            "edge": round(edge, 4),
                            "fair_p": round(fair_p, 4),
                            "spot": round(spot, 2),
                        })
                    else:
                        log(f"    {crypto} BUY {side} FAILED (no fill)")

                # Recalculate after trade
                total_exposure = w["up_cost"] + w["dn_cost"]

            # ── SELL LOGIC ──
            # Two modes:
            #   1. Profit take: bid > entry AND model says overpriced → lock in gain
            #   2. Stop loss: model flipped against us → cut loss before resolution
            for side, token, book, fair_p, last_key, shares_key, cost_key in [
                ("Up", w["up_token"], up_book, p_up, "last_sell_up", "up_shares", "up_cost"),
                ("Dn", w["down_token"], dn_book, p_dn, "last_sell_dn", "dn_shares", "dn_cost"),
            ]:
                shares_held = w[shares_key]
                cost_held = w[cost_key]
                if shares_held < MIN_BET_SHARES:
                    continue

                avg_entry = cost_held / shares_held
                bid = book["bb"]

                should_sell = False
                reason = ""

                # Profit take: bid above entry and model says overpriced
                if bid > avg_entry + 0.02 and fair_p < bid - MIN_EDGE:
                    should_sell = True
                    reason = "PROFIT"

                # Stop loss: model flipped hard against us — fair value dropped
                # well below our entry, meaning we're likely holding a loser.
                # Sell if we can recover anything (bid > 0.02) and model says
                # fair prob is at least 10c below our entry price.
                elif fair_p < avg_entry - 0.10 and bid >= 0.03:
                    should_sell = True
                    reason = "STOPLOSS"

                if not should_sell:
                    continue

                # Cooldown
                if now - w[last_key] < TRADE_COOLDOWN:
                    continue

                sell_shares = math.floor(shares_held * 100) / 100
                if sell_shares < MIN_BET_SHARES:
                    continue

                log(f"    {crypto} {reason} SELL {side} {sell_shares:.1f}sh @ ~{bid:.2f} "
                    f"[entry={avg_entry:.3f} fair={fair_p:.3f}]")

                if DRY_RUN:
                    proceeds = sell_shares * bid
                    w[shares_key] = 0
                    w[cost_key] = 0
                    w[last_key] = now
                    self.total_sells += 1
                    self.total_proceeds += proceeds
                    log(f"    [SIM] sold {sell_shares:.1f}sh @ {bid:.3f} (${proceeds:.2f})")
                else:
                    sold, proceeds = post_fak_sell(token, sell_shares)
                    if sold > 0:
                        remaining = shares_held - sold
                        if remaining < 0.5:
                            w[shares_key] = 0
                            w[cost_key] = 0
                        else:
                            w[shares_key] = remaining
                            w[cost_key] = avg_entry * remaining
                        w[last_key] = now
                        self.total_sells += 1
                        self.total_proceeds += proceeds
                        self.balance += proceeds
                        log(f"    {reason} SOLD {sold:.1f}sh (${proceeds:.2f})")

                        w["trades"].append({
                            "time": datetime.now(timezone.utc).isoformat(),
                            "action": "SELL",
                            "reason": reason,
                            "side": side,
                            "shares": round(sold, 2),
                            "proceeds": round(proceeds, 4),
                            "edge": round(fair_p - bid, 4),
                            "spot": round(spot, 2),
                        })

    def _close_window(self, cid, w):
        """Window ended — report positions (hold to resolution)."""
        crypto = w["crypto"]
        w["done"] = True
        self.windows_completed += 1

        up_sh = w["up_shares"]
        dn_sh = w["dn_shares"]
        up_cost = w["up_cost"]
        dn_cost = w["dn_cost"]
        total_cost = up_cost + dn_cost

        paired = min(up_sh, dn_sh)
        if paired > 0:
            up_avg = up_cost / up_sh if up_sh > 0 else 0
            dn_avg = dn_cost / dn_sh if dn_sh > 0 else 0
            pair_profit = paired * (1.0 - up_avg - dn_avg)
            log(f"  {crypto} PAIRED: {paired:.0f}sh × (1 - {up_avg:.3f} - {dn_avg:.3f}) "
                f"= ${pair_profit:+.2f} guaranteed")
        else:
            pair_profit = 0

        unpaired_up = up_sh - paired
        unpaired_dn = dn_sh - paired
        if unpaired_up > 0:
            log(f"  {crypto} UNPAIRED Up: {unpaired_up:.0f}sh (resolves $1 or $0)")
        if unpaired_dn > 0:
            log(f"  {crypto} UNPAIRED Dn: {unpaired_dn:.0f}sh (resolves $1 or $0)")

        if up_sh == 0 and dn_sh == 0:
            log(f"  {crypto} window: no position")
        else:
            n_trades = len(w["trades"])
            log(f"  {crypto} window: {n_trades} trades, cost=${total_cost:.2f}, "
                f"Up={up_sh:.1f}sh Dn={dn_sh:.1f}sh paired={paired:.0f}")

        if w["trades"]:
            record_trade({
                "time": datetime.now(timezone.utc).isoformat(),
                "crypto": crypto,
                "up_token": w["up_token"][:20] + "...",
                "down_token": w["down_token"][:20] + "...",
                "up_shares": round(up_sh, 2),
                "down_shares": round(dn_sh, 2),
                "up_cost": round(up_cost, 4),
                "dn_cost": round(dn_cost, 4),
                "paired_shares": round(paired, 2),
                "paired_profit": round(pair_profit, 4),
                "n_trades": len(w["trades"]),
                "trades": w["trades"],
                "dry_run": DRY_RUN,
            })

    def _final_report(self):
        """Shutdown — cancel orders, report session."""
        if not DRY_RUN:
            log("  Cancelling all orders...")
            cancel_all_orders()
            time.sleep(2)

            # Check remaining positions
            positions = []
            for tok in self._known_tokens:
                try:
                    bal = get_share_balance(tok) or 0
                    if bal >= MIN_BET_SHARES:
                        positions.append((tok[:20] + "...", bal))
                except Exception:
                    pass
            if positions:
                log(f"  Holding {len(positions)} positions to resolution:")
                for tok, bal in positions:
                    log(f"    {tok} : {bal:.1f}sh")

        end_balance = self.balance if DRY_RUN else (get_usdc_balance() or 0)
        actual_pnl = end_balance - self.start_balance

        log(f"  Session: buys={self.total_buys} sells={self.total_sells} "
            f"windows={self.windows_completed}")
        log(f"  Total cost: ${self.total_cost:.2f}  proceeds: ${self.total_proceeds:.2f}")
        log(f"  USDC: ${self.start_balance:.2f} -> ${end_balance:.2f} (${actual_pnl:+.2f})")


# =========================================================================
# ENTRY POINT
# =========================================================================
if __name__ == "__main__":
    bot = CrossArbBot()
    bot.run()
