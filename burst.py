#!/usr/bin/env python3
"""
Book Imbalance Bot — order-book-based strategy on Polymarket crypto markets.

Strategy:
  Monitor real-time WebSocket order book for depth imbalances.
  When bid-side depth significantly exceeds ask-side depth, buy that token
  using a GTC limit order at the bid (maker, 0% fee).

  Rationale: heavy bids signal accumulation / informed positioning;
  the price is likely to move up toward the stacked bids.
  This is a LEADING indicator — the imbalance appears before the move.

Signal:
  - Compute near-money bid/ask depth (within IMBALANCE_DEPTH_RANGE of best price)
  - If bid_depth / ask_depth > IMBALANCE_RATIO → buy this token
  - Min bid depth filter prevents false signals on thin books
  - Max spread filter ensures the market is liquid

Entry:
  - GTC limit buy at best_bid + LIMIT_BUY_OFFSET (maker = 0% fee)
  - Wait up to FILL_TIMEOUT seconds for fill (WS User Channel + REST fallback)
  - No settlement blind spot — limit fills are atomic on-book
  - Better entry price (buying near bid, not crossing to ask)

Exit (quick flip):
  - Place GTC limit sell at entry + PROFIT_TARGET immediately (maker = 0% fee)
  - If price drops below entry - STOP_LOSS → cancel limit, market sell (cut loss)
  - If FLIP_TIMEOUT seconds pass → cancel limit, market sell (take whatever)
  - TP/SL dynamically adjusted from order book depth analysis

Fees (5m/15m crypto markets):
  - BOTH entry and exit are maker (GTC limit) = 0% fee
  - Only SL/timeout exits use taker (FAK market) = formula-based fee
"""

import os
import sys
import time
import math
import re
import requests
import json
import asyncio
import threading
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    OrderArgs, OrderType, OpenOrderParams,
    BalanceAllowanceParams, AssetType,
)
from py_clob_client.order_builder.constants import BUY, SELL
from py_clob_client import MarketOrderArgs
from dotenv import load_dotenv

load_dotenv()
private_key = os.getenv("PRIVATE_KEY")
founder_address = os.getenv("FUNDER_ADDRESS")

# Configuration
HOST = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
WS_USER_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
CHAIN_ID = 137
FUNDER_ADDRESS = founder_address

# =========================================================================
# STRATEGY PARAMETERS
# =========================================================================
BUY_AMOUNT = 10              # Base bet in $
MAX_BUY_AMOUNT = 30          # Cap per trade
MIN_ENTRY_PRICE = 0.20       # Don't buy token below this
MAX_ENTRY_PRICE = 0.90       # Don't buy token above this
MIN_RR_RATIO = 1.0           # Skip trade if profit_target / stop_loss < this (risk/reward gate)

# Book imbalance signal — LEADING indicator (detects pressure before the move)
IMBALANCE_RATIO = 3.0        # Bid/ask depth ratio to trigger entry (bid_depth / ask_depth)
                             # 2.0x was noise (3/3 trades lost); 3.0x requires strong imbalance
IMBALANCE_DEPTH_RANGE = 0.10 # Look at orders within this range of best price
IMBALANCE_MIN_DEPTH = 500    # Min bid depth ($) in range to consider (filters thin books)
IMBALANCE_MAX_SPREAD = 0.03  # Skip if spread > this (too illiquid)
WS_WARMUP_SECONDS = 15       # Don't fire signals for this many seconds after WS connects
                             # (first book snapshots are unreliable — stale state, not fresh signal)

# Limit buy entry — GTC maker order (0% fee, fills atomically, no settlement blind spot)
LIMIT_BUY_OFFSET = 0.01      # Place buy at best_bid + this (near top of book)
FILL_TIMEOUT = 10             # Seconds to wait for limit buy fill before cancelling
                              # (was 15 — fills after 6s tend to be adverse selection;
                              # good fills happen in 1-3s, book monitor cancels if signal fades)
SCAN_COOLDOWN = 10.0          # Don't re-signal same token within this many seconds

# Burst detection — still used for trade-window tracking (background, does not trigger entries)
BURST_WINDOW = 5.0
BURST_MULTIPLIER = 3.0
BURST_MIN_ABSOLUTE = 100
BURST_BASELINE_WINDOW = 60

# Fees — real Polymarket formula: fee = C * feeRate * (p * (1-p))^exponent
# For 5m/15m crypto: feeRate=0.25, exponent=2. Max ~1.56% at p=0.50.
# Maker (GTC limit) = 0% fee. Taker (FAK market) = formula-based fee.
CRYPTO_FEE_RATE = 0.25       # feeRate for 5m/15m crypto markets
CRYPTO_FEE_EXPONENT = 2      # exponent for 5m/15m crypto markets

# Quick flip exit — defaults (overridden by book analysis when available)
PROFIT_TARGET = 0.05         # Default limit sell at entry + this (maker, 0% fee)
STOP_LOSS = 0.03             # Default cancel limit + market sell if price drops this much
BOOK_TP_ENABLED = True       # Use order book depth to set dynamic take-profit
BOOK_SL_ENABLED = True       # Use order book depth to set dynamic stop-loss
BOOK_WALL_MIN_SIZE = 200     # Min $ at a price level to count as a "wall"
BOOK_TP_WALL_MARGIN = 0.01   # Place TP this much below the ask wall
BOOK_TP_MIN = 0.02           # Minimum TP even if wall is close
BOOK_TP_MAX = 0.10           # Maximum TP even if no wall found
BOOK_SL_MIN = 0.02           # Minimum SL even if support is close
BOOK_SL_MAX = 0.05           # Maximum SL even if no support found
FLIP_TIMEOUT = 20            # Max seconds to hold before force-selling
                             # Session 3: 5 timeouts were net +$1.74 at 15s — strategy captures momentum
                             # that needs more time. Extending to 20s to let winners run.
LIMIT_SELL_RETRIES = 5       # Retry limit sell placement this many times (was 3 — too few
                             # when cancel_all_orders_for_token locks balance temporarily)
LIMIT_SELL_RETRY_DELAY = 2   # Seconds between retries (was 1 — need longer for on-chain settlement)

# Risk management
MAX_CONCURRENT_POSITIONS = 3
MIN_BALANCE_BUFFER = 5
COOLDOWN_AFTER_ENTRY = 30.0  # Don't re-enter same TOKEN for N seconds (was 5)
MARKET_COOLDOWN = 30.0       # Don't re-enter same MARKET (either side) for N seconds (was 60)
LOSS_LOCKOUT = 120.0         # After stop-loss/timeout on a market, lock it out for this long
MIN_TIME_REMAINING = 35      # Don't enter markets with less than this many seconds until resolution
                             # FLIP_TIMEOUT=20 + 2s confirm + 5s settle = ~27s needed

CRYPTOS = ["btc", "eth", "sol", "xrp"]

DRY_RUN = "--dry-run" in sys.argv
DECISION_LOG = "burst_dry_log.txt" if DRY_RUN else "burst_log.txt"
TRADE_LOG = "burst_dry_trades.json" if DRY_RUN else "burst_trades.json"


def log(message):
    timestamp = datetime.now(timezone.utc).isoformat()
    line = f"[{timestamp}] {message}"
    with open(DECISION_LOG, "a") as f:
        f.write(line + "\n")
    print(message)


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


def get_price(token_id, side=BUY):
    """Get price — tries WS cache first, falls back to REST API."""
    # Try WebSocket-cached price first (0 latency, no API call)
    if _detector_ref:
        ws_price = _detector_ref.get_ws_price(token_id, side)
        if ws_price > 0:
            return ws_price
    # Fallback to REST
    try:
        data = client.get_price(token_id, side=side)
        return float(data.get("price", 0))
    except Exception:
        return 0.0


# Global references (set in run_burst) for WS access from quick_flip
_detector_ref = None
_user_ws_ref = None


def get_actual_share_balance(token_id):
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


def cancel_all_orders_for_token(token_id):
    """Cancel any open orders for a token."""
    try:
        orders = client.get_orders(OpenOrderParams())
        for order in orders:
            if order.get("asset_id") == token_id:
                client.cancel(order.get("id"))
    except Exception:
        pass


def compute_taker_fee(shares, price, fee_rate=CRYPTO_FEE_RATE, exponent=CRYPTO_FEE_EXPONENT):
    """Compute taker fee in USDC using Polymarket formula:
       fee_shares = C × feeRate × (p × (1 - p))^exponent
       fee_usdc = fee_shares × p
    where C = shares, p = price.
    The formula yields fee in shares (collected on buys); multiply by price for USDC.
    Rounded to 4 decimal places (Polymarket precision)."""
    if price <= 0 or price >= 1:
        return 0.0
    fee_shares = shares * fee_rate * (price * (1 - price)) ** exponent
    fee_usdc = fee_shares * price
    return round(fee_usdc, 4)


# =========================================================================
# MARKET DISCOVERY
# =========================================================================
def get_current_crypto_markets():
    """Fetch current 5-min and 15-min crypto updown markets."""
    markets = []
    now = datetime.now(timezone.utc)

    for interval in [5, 15]:
        aligned = (now.minute // interval) * interval
        window_start = now.replace(minute=aligned, second=0, microsecond=0)
        elapsed = (now - window_start).total_seconds() / 60
        epoch = int(window_start.timestamp())

        for crypto in CRYPTOS:
            slug = f"{crypto}-updown-{interval}m-{epoch}"
            try:
                resp = requests.get(
                    f"{GAMMA_API}/events/slug/{slug}", timeout=10
                )
                if resp.status_code == 200:
                    event = resp.json()
                    for m in event.get("markets", []):
                        if not m.get("closed"):
                            m["_elapsed_min"] = elapsed
                            m["_crypto"] = crypto.upper()
                            m["_slug"] = slug
                            m["_interval"] = interval
                            m["_epoch"] = epoch
                            markets.append(m)
            except Exception:
                pass
    return markets


# =========================================================================
# BURST DETECTOR — real-time sliding window over WebSocket trades
# =========================================================================
class BurstDetector:
    """
    Maintains a per-token sliding window of trades.
    On each new trade, checks if a burst has occurred:
      net $ in last BURST_WINDOW seconds >= dynamic threshold
    Threshold is proportional to recent market activity:
      threshold = max(BURST_MIN_ABSOLUTE, baseline_avg_per_window * BURST_MULTIPLIER)
    This adapts to each token's volume — quiet markets trigger on smaller absolute
    bursts, busy markets require proportionally larger ones.
    """

    def __init__(self, on_burst):
        self._lock = threading.Lock()
        # token -> deque of (timestamp, side, cost) — short 5s burst window
        self._windows = defaultdict(deque)
        # token -> deque of (timestamp, cost) — longer baseline window for avg volume
        self._baselines = defaultdict(deque)
        self._on_burst = on_burst          # callback(token, ratio, direction, info)
        self._cooldowns = {}               # token -> last_burst_time
        self._imbalance_cooldowns = {}     # token -> last_imbalance_signal_time
        self._ws_connected = False
        self._event_count = 0
        self._msg_count = 0
        self._wanted = set()
        self._active = set()
        self._last_sub_time = 0.0
        self._force_reconnect = False

        # Map token -> market info (set by main thread)
        self._token_info = {}              # token -> {market, tokens, condition_id, ...}
        self._market_cooldowns = {}        # market_question -> last_entry_time
        self._loss_lockouts = {}           # market_question -> lockout_until_time

        # WebSocket-cached prices: token -> {"best_bid": float, "best_ask": float, "spread": float, "ts": float}
        self._prices = {}
        # WebSocket-cached order books: token -> {"bids": [(price, size), ...], "asks": [(price, size), ...], "ts": float}
        self._books = {}
        # Price-change callbacks: token_id -> list of callables
        # Called on any price update (best_bid_ask, book, price_change)
        self._price_callbacks = defaultdict(list)

    def start(self):
        t = threading.Thread(target=self._run_loop, daemon=True)
        t.start()

    def _run_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._ws_loop())

    async def _ws_loop(self):
        try:
            import websockets
        except ImportError:
            import subprocess
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "websockets"]
            )
            import websockets

        while True:
            try:
                async with websockets.connect(
                    WS_MARKET_URL, close_timeout=5, open_timeout=10
                ) as ws:
                    self._ws_connected = True
                    self._ws_connect_time = time.time()
                    self._active = set()
                    log("  🔌 Market WebSocket connected")

                    last_ping = time.time()
                    while True:
                        await self._sync_subscriptions(ws)
                        if self._force_reconnect:
                            self._force_reconnect = False
                            break

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

                        self._msg_count += 1
                        self._dispatch_event(data)

            except Exception as e:
                self._ws_connected = False
                log(f"  ⚠ Market WS disconnected: {e}, reconnecting in 2s...")
                await asyncio.sleep(2)

    def _dispatch_event(self, data):
        """Route incoming WS events to the appropriate handler."""
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    self._dispatch_event(item)
            return
        if not isinstance(data, dict):
            return
        etype = data.get("event_type", "")
        if etype == "last_trade_price":
            self._on_trade(data)
        elif etype == "best_bid_ask":
            self._on_best_bid_ask(data)
        elif etype == "book":
            self._on_book(data)
        elif etype == "price_change":
            self._on_price_change(data)
        elif etype == "market_resolved":
            pass  # Markets auto-removed when get_current_crypto_markets() stops returning them

    def _on_best_bid_ask(self, data):
        """Cache real-time best bid/ask from WebSocket."""
        asset_id = data.get("asset_id", "")
        if not asset_id:
            return
        try:
            best_bid = float(data.get("best_bid", 0))
            best_ask = float(data.get("best_ask", 0))
            spread = float(data.get("spread", 0))
        except (ValueError, TypeError):
            return
        with self._lock:
            self._prices[asset_id] = {
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread": spread,
                "ts": time.time(),
            }
        self._fire_price_callbacks(asset_id)

    def _on_book(self, data):
        """Cache full order book snapshot from WebSocket."""
        asset_id = data.get("asset_id", "")
        if not asset_id:
            return
        bids = []
        asks = []
        for b in data.get("bids", []):
            try:
                bids.append((float(b["price"]), float(b["size"])))
            except (KeyError, ValueError, TypeError):
                pass
        for a in data.get("asks", []):
            try:
                asks.append((float(a["price"]), float(a["size"])))
            except (KeyError, ValueError, TypeError):
                pass
        # Sort bids descending (best bid first), asks ascending (best ask first)
        bids.sort(key=lambda x: -x[0])
        asks.sort(key=lambda x: x[0])
        with self._lock:
            self._books[asset_id] = {"bids": bids, "asks": asks, "ts": time.time()}
            # Also update best bid/ask from the book snapshot
            if bids and asks:
                self._prices[asset_id] = {
                    "best_bid": bids[0][0],
                    "best_ask": asks[0][0],
                    "spread": round(asks[0][0] - bids[0][0], 4),
                    "ts": time.time(),
                }
        self._fire_price_callbacks(asset_id)
        self._check_imbalance(asset_id)

    def _on_price_change(self, data):
        """Incrementally update order book from price_change events."""
        for pc in data.get("price_changes", []):
            asset_id = pc.get("asset_id", "")
            if not asset_id:
                continue
            try:
                price = float(pc["price"])
                size = float(pc["size"])
                side = pc["side"]  # "BUY" or "SELL"
                best_bid = float(pc.get("best_bid", 0))
                best_ask = float(pc.get("best_ask", 0))
            except (KeyError, ValueError, TypeError):
                continue

            with self._lock:
                book = self._books.get(asset_id)
                if book:
                    if side == "BUY":
                        # Update bid level
                        book["bids"] = [(p, s) for p, s in book["bids"] if p != price]
                        if size > 0:
                            book["bids"].append((price, size))
                            book["bids"].sort(key=lambda x: -x[0])
                    else:
                        # Update ask level
                        book["asks"] = [(p, s) for p, s in book["asks"] if p != price]
                        if size > 0:
                            book["asks"].append((price, size))
                            book["asks"].sort(key=lambda x: x[0])
                    book["ts"] = time.time()

                # Always update cached best bid/ask
                if best_bid > 0 or best_ask > 0:
                    existing = self._prices.get(asset_id, {})
                    self._prices[asset_id] = {
                        "best_bid": best_bid if best_bid > 0 else existing.get("best_bid", 0),
                        "best_ask": best_ask if best_ask > 0 else existing.get("best_ask", 0),
                        "spread": round(best_ask - best_bid, 4) if best_bid > 0 and best_ask > 0 else existing.get("spread", 0),
                        "ts": time.time(),
                    }

            self._fire_price_callbacks(asset_id)
            self._check_imbalance(asset_id)

    async def _sync_subscriptions(self, ws):
        with self._lock:
            wanted = set(self._wanted)

        now = time.time()
        changed = wanted != self._active
        stale = (now - self._last_sub_time) > 5

        if not changed and not stale:
            return

        removed = self._active - wanted
        if removed:
            with self._lock:
                for t in removed:
                    self._windows.pop(t, None)
                    self._baselines.pop(t, None)
                    self._prices.pop(t, None)
                    self._books.pop(t, None)

        if changed and self._active:
            self._active = set()
            self._force_reconnect = True
            return

        if wanted:
            await ws.send(json.dumps({
                "type": "market",
                "assets_ids": list(wanted),
                "custom_feature_enabled": True,
            }))
        self._last_sub_time = now

        if changed:
            added = wanted - self._active
            log(f"  📡 Subscribed to {len(wanted)} tokens (+{len(added)}, -{len(removed)})")
        self._active = wanted

    def _on_trade(self, data):
        """Process a trade and check for burst."""
        asset_id = data.get("asset_id", "")
        if not asset_id:
            return

        self._event_count += 1
        now = time.time()
        side = data.get("side", "")
        size = float(data.get("size", 0))
        price = float(data.get("price", 0))
        cost = size * price

        with self._lock:
            # Update burst window (short, 5s)
            window = self._windows[asset_id]
            window.append((now, side, cost))

            # Update baseline window (longer, 60s)
            baseline = self._baselines[asset_id]
            baseline.append((now, cost))

            # Prune old trades outside the burst window
            cutoff = now - BURST_WINDOW
            while window and window[0][0] < cutoff:
                window.popleft()

            # Prune old trades outside the baseline window
            baseline_cutoff = now - BURST_BASELINE_WINDOW
            while baseline and baseline[0][0] < baseline_cutoff:
                baseline.popleft()

            # Compute baseline: average $ volume per BURST_WINDOW-sized chunk
            total_baseline_vol = sum(c for _, c in baseline)
            baseline_duration = max(now - baseline[0][0], BURST_WINDOW) if baseline else BURST_WINDOW
            num_windows = baseline_duration / BURST_WINDOW
            avg_vol_per_window = total_baseline_vol / num_windows if num_windows > 0 else 0

            # Dynamic threshold: proportional to recent activity
            dynamic_threshold = max(BURST_MIN_ABSOLUTE, avg_vol_per_window * BURST_MULTIPLIER)

            # Compute net buy in window
            net_buy = 0.0
            net_sell = 0.0
            for _, s, c in window:
                if s == "BUY":
                    net_buy += c
                else:
                    net_sell += c

            net = net_buy - net_sell

            # Check per-token cooldown
            last_burst = self._cooldowns.get(asset_id, 0)
            if now - last_burst < COOLDOWN_AFTER_ENTRY:
                return

            # Burst detection disabled — entry signals come from book imbalance scanner.
            # Window/baseline tracking kept for potential future use.

    def set_loss_lockout(self, market_question, duration=None):
        """Lock out a market for LOSS_LOCKOUT seconds after a stop-loss/timeout."""
        if not market_question:
            return
        lockout = duration or LOSS_LOCKOUT
        with self._lock:
            self._loss_lockouts[market_question] = time.time() + lockout

    def update_subscriptions(self, wanted_tokens):
        with self._lock:
            self._wanted = set(wanted_tokens)

    def set_token_info(self, token_id, info):
        with self._lock:
            self._token_info[token_id] = info

    @property
    def connected(self):
        return self._ws_connected

    @property
    def event_count(self):
        return self._event_count

    @property
    def msg_count(self):
        return self._msg_count

    @property
    def subscribed_count(self):
        return len(self._active)

    # --- Price callback system (event-driven flip monitor) ---

    def _fire_price_callbacks(self, asset_id):
        """Fire registered price callbacks for an asset (called from WS handlers)."""
        with self._lock:
            callbacks = list(self._price_callbacks.get(asset_id, []))
        for cb in callbacks:
            try:
                cb(asset_id)
            except Exception:
                pass

    # --- Book imbalance scanner ---

    def _check_imbalance(self, asset_id):
        """Check if order book shows significant buy-side imbalance.
        Called on every book/price update. Fires callback if imbalance
        exceeds threshold and cooldowns are clear."""
        now = time.time()

        # Warmup guard: don't fire signals right after WS connects
        # (initial book snapshots are stale state, not fresh signals)
        if hasattr(self, '_ws_connect_time') and now - self._ws_connect_time < WS_WARMUP_SECONDS:
            return

        with self._lock:
            book = self._books.get(asset_id)
            if not book or now - book.get("ts", 0) > 30:
                return
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            if not bids or not asks:
                return

            best_bid = bids[0][0]
            best_ask = asks[0][0]
            spread = best_ask - best_bid
            if spread <= 0 or spread > IMBALANCE_MAX_SPREAD:
                return

            # Compute near-money depth (orders within IMBALANCE_DEPTH_RANGE of best price)
            bid_depth = sum(p * s for p, s in bids if p >= best_bid - IMBALANCE_DEPTH_RANGE)
            ask_depth = sum(p * s for p, s in asks if p <= best_ask + IMBALANCE_DEPTH_RANGE)

            if bid_depth < IMBALANCE_MIN_DEPTH:
                return
            if ask_depth <= 0:
                return

            ratio = bid_depth / ask_depth
            if ratio < IMBALANCE_RATIO:
                return

            # --- Cooldown checks (under lock) ---
            # Per-token scan cooldown
            last_scan = self._imbalance_cooldowns.get(asset_id, 0)
            if now - last_scan < SCAN_COOLDOWN:
                return

            # Per-token entry cooldown
            last_entry = self._cooldowns.get(asset_id, 0)
            if now - last_entry < COOLDOWN_AFTER_ENTRY:
                return

            # Per-market cooldown + loss lockout
            info = self._token_info.get(asset_id, {})
            market_q = info.get("market", "")
            if market_q:
                lockout_until = self._loss_lockouts.get(market_q, 0)
                if now < lockout_until:
                    return
                last_market = self._market_cooldowns.get(market_q, 0)
                if now - last_market < MARKET_COOLDOWN:
                    return

            # All checks passed — set cooldowns
            self._imbalance_cooldowns[asset_id] = now
            self._cooldowns[asset_id] = now
            if market_q:
                self._market_cooldowns[market_q] = now

            # Copy info for callback (outside lock)
            info = dict(self._token_info.get(asset_id, {}))
            info['_bid_depth'] = round(bid_depth, 0)
            info['_ask_depth'] = round(ask_depth, 0)
            info['_ratio'] = round(ratio, 2)
            info['_spread'] = round(spread, 4)
            info['_best_bid'] = best_bid
            info['_best_ask'] = best_ask

        # Fire callback outside lock
        self._on_burst(asset_id, ratio, "BUY_IMBALANCE", info)

    def on_price_update(self, token_id, callback):
        """Register a callback to be called on every price update for token_id.
        callback(asset_id) is called from the WS thread."""
        with self._lock:
            self._price_callbacks[token_id].append(callback)

    def remove_price_callback(self, token_id, callback):
        """Unregister a previously registered price callback."""
        with self._lock:
            cbs = self._price_callbacks.get(token_id, [])
            try:
                cbs.remove(callback)
            except ValueError:
                pass

    # --- Public accessors for WS-cached data ---

    def get_ws_price(self, token_id, side=BUY):
        """Get price from WS cache. Returns best_ask for BUY, best_bid for SELL.
        Returns 0.0 if not cached (caller should fall back to REST)."""
        with self._lock:
            p = self._prices.get(token_id)
        if not p:
            return 0.0
        # Stale check: if older than 30s, don't trust it
        if time.time() - p.get("ts", 0) > 30:
            return 0.0
        if side == BUY:
            return p.get("best_ask", 0.0)
        else:
            return p.get("best_bid", 0.0)

    def get_book(self, token_id):
        """Get cached order book for a token. Returns {"bids": [...], "asks": [...]} or None."""
        with self._lock:
            book = self._books.get(token_id)
        if not book:
            return None
        # Stale check
        if time.time() - book.get("ts", 0) > 30:
            return None
        return book

    def analyze_book(self, token_id, entry_price=None):
        """Analyze order book depth to suggest dynamic TP/SL.
        Returns dict with:
          - spread: best ask - best bid
          - total_bid_depth: total $ on bid side
          - total_ask_depth: total $ on ask side
          - first_ask_wall: (price, size_$) of first significant ask wall above entry
          - first_bid_wall: (price, size_$) of first significant bid wall below entry
          - suggested_tp: suggested take-profit offset
          - suggested_sl: suggested stop-loss offset
        Returns None if book unavailable.
        """
        book = self.get_book(token_id)
        if not book:
            return None

        bids = book.get("bids", [])
        asks = book.get("asks", [])
        if not bids or not asks:
            return None

        best_bid = bids[0][0]
        best_ask = asks[0][0]
        spread = round(best_ask - best_bid, 4)

        # Total depth (in $, i.e. size × price)
        total_bid_depth = sum(p * s for p, s in bids)
        total_ask_depth = sum(p * s for p, s in asks)

        ref_price = entry_price if entry_price else best_ask

        # Find first significant ask wall above entry (resistance)
        first_ask_wall = None
        if BOOK_TP_ENABLED:
            for price, size in asks:
                dollar_val = price * size
                if price > ref_price and dollar_val >= BOOK_WALL_MIN_SIZE:
                    first_ask_wall = (price, round(dollar_val, 0))
                    break

        # Find first significant bid wall below entry (support)
        first_bid_wall = None
        if BOOK_SL_ENABLED:
            for price, size in bids:
                dollar_val = price * size
                if price < ref_price and dollar_val >= BOOK_WALL_MIN_SIZE:
                    first_bid_wall = (price, round(dollar_val, 0))
                    break

        # Compute suggested TP from ask wall
        if first_ask_wall and BOOK_TP_ENABLED:
            wall_dist = first_ask_wall[0] - ref_price - BOOK_TP_WALL_MARGIN
            suggested_tp = max(BOOK_TP_MIN, min(BOOK_TP_MAX, round(wall_dist, 2)))
        else:
            suggested_tp = PROFIT_TARGET  # Default

        # Compute suggested SL from bid wall / spread
        if first_bid_wall and BOOK_SL_ENABLED:
            support_dist = ref_price - first_bid_wall[0]
            # Stop loss just below the support wall
            suggested_sl = max(BOOK_SL_MIN, min(BOOK_SL_MAX, round(support_dist + 0.01, 2)))
        else:
            suggested_sl = STOP_LOSS  # Default

        return {
            "spread": spread,
            "total_bid_depth": round(total_bid_depth, 0),
            "total_ask_depth": round(total_ask_depth, 0),
            "first_ask_wall": first_ask_wall,
            "first_bid_wall": first_bid_wall,
            "suggested_tp": suggested_tp,
            "suggested_sl": suggested_sl,
        }


# =========================================================================
# USER CHANNEL — real-time order/trade fill notifications
# =========================================================================
class UserChannelWS:
    """
    Subscribes to the authenticated User WebSocket channel to receive
    real-time trade fill and order status updates. Replaces polling
    get_actual_share_balance() for detecting limit fill.

    Events:
      - trade CONFIRMED → our buy/sell filled on-chain
      - order CANCELLATION → our order was cancelled
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._connected = False
        # token_id -> list of callbacks to fire on trade CONFIRMED
        self._fill_callbacks = defaultdict(list)
        # token_id -> latest confirmed trade info
        self._confirmed_trades = {}
        # condition_ids to subscribe to
        self._wanted_markets = set()

    def start(self):
        t = threading.Thread(target=self._run_loop, daemon=True)
        t.start()

    def _run_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._ws_loop())

    async def _ws_loop(self):
        try:
            import websockets
        except ImportError:
            import subprocess
            subprocess.check_call([sys.executable, "-m", "pip", "install", "websockets"])
            import websockets

        api_creds = client.creds
        if not api_creds:
            log("  ⚠ No API creds for User Channel — fill detection via REST only")
            return

        auth = {
            "apiKey": api_creds.api_key,
            "secret": api_creds.api_secret,
            "passphrase": api_creds.api_passphrase,
        }

        while True:
            try:
                async with websockets.connect(
                    WS_USER_URL, close_timeout=5, open_timeout=10
                ) as ws:
                    self._connected = True
                    log("  🔌 User WebSocket connected")

                    last_ping = time.time()
                    last_sub = set()

                    while True:
                        # Sync subscriptions
                        with self._lock:
                            wanted = set(self._wanted_markets)
                        if wanted != last_sub:
                            if wanted:
                                await ws.send(json.dumps({
                                    "auth": auth,
                                    "markets": list(wanted),
                                    "type": "user",
                                }))
                            last_sub = wanted

                        # Heartbeat
                        if time.time() - last_ping > 10:
                            try:
                                await ws.send("PING")
                                last_ping = time.time()
                            except Exception:
                                break

                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=0.1)
                        except asyncio.TimeoutError:
                            continue

                        if msg == "PONG":
                            continue

                        try:
                            data = json.loads(msg)
                        except (json.JSONDecodeError, TypeError):
                            continue

                        if isinstance(data, list):
                            for item in data:
                                if isinstance(item, dict):
                                    self._handle_event(item)
                        elif isinstance(data, dict):
                            self._handle_event(data)

            except Exception as e:
                self._connected = False
                log(f"  ⚠ User WS disconnected: {e}, reconnecting in 3s...")
                await asyncio.sleep(3)

    def _handle_event(self, data):
        etype = data.get("event_type") or data.get("type", "")
        if etype == "trade":
            self._on_trade_event(data)

    def _on_trade_event(self, data):
        """Handle trade lifecycle event: MATCHED → MINED → CONFIRMED."""
        status = data.get("status", "")
        asset_id = data.get("asset_id", "")
        if not asset_id:
            return

        # Fire on MATCHED (immediate) and CONFIRMED (on-chain) — catch fills ASAP
        if status in ("MATCHED", "CONFIRMED"):
            trade_info = {
                "asset_id": asset_id,
                "side": data.get("side", ""),
                "size": float(data.get("size", 0)),
                "price": float(data.get("price", 0)),
                "status": status,
                "timestamp": data.get("timestamp", ""),
            }
            with self._lock:
                self._confirmed_trades[asset_id] = trade_info
                callbacks = list(self._fill_callbacks.get(asset_id, []))
            # Fire callbacks outside lock
            for cb in callbacks:
                try:
                    cb(trade_info)
                except Exception:
                    pass

    def subscribe_market(self, condition_id):
        """Add a market (by condition_id) to the user channel subscription."""
        with self._lock:
            self._wanted_markets.add(condition_id)

    def on_fill(self, token_id, callback):
        """Register a callback for when a trade on this token is CONFIRMED.
        callback(trade_info) called with {"asset_id", "side", "size", "price", "status"}."""
        with self._lock:
            self._fill_callbacks[token_id].append(callback)

    def remove_fill_callback(self, token_id, callback=None):
        """Remove fill callback(s) for a token."""
        with self._lock:
            if callback:
                cbs = self._fill_callbacks.get(token_id, [])
                self._fill_callbacks[token_id] = [cb for cb in cbs if cb != callback]
            else:
                self._fill_callbacks.pop(token_id, None)

    def get_last_confirmed(self, token_id):
        """Get the last confirmed trade info for a token, or None."""
        with self._lock:
            return self._confirmed_trades.get(token_id)

    @property
    def connected(self):
        return self._connected


# =========================================================================
# TIME REMAINING — parse market end time to avoid entering near resolution
# =========================================================================
def parse_market_end_time(market_name):
    """Parse end time from market name like 'Bitcoin Up or Down - February 20, 7:30PM-7:35PM ET'.
    Returns datetime in UTC, or None if parsing fails.
    """
    match = re.search(r'(\w+)\s+(\d+),\s+\d+:\d+[AP]M-(\d+:\d+[AP]M)\s+ET', market_name)
    if not match:
        return None
    month_str = match.group(1)   # "February"
    day_str = match.group(2)     # "20"
    end_time_str = match.group(3) # "7:35PM"

    year = datetime.now().year
    dt_str = f"{month_str} {day_str} {year} {end_time_str}"
    try:
        dt = datetime.strptime(dt_str, "%B %d %Y %I:%M%p")
    except ValueError:
        return None

    # ET in winter (Nov-Mar) = EST = UTC-5; in summer = EDT = UTC-4
    # Simple approach: February is always EST
    est = timezone(timedelta(hours=-5))
    dt_utc = dt.replace(tzinfo=est).astimezone(timezone.utc)
    return dt_utc


# =========================================================================
# QUICK FLIP — monitor price and exit
# =========================================================================
def quick_flip(token_id, entry_price, shares, pos_info):
    """
    Exit strategy after burst entry:
    1. Analyze order book for dynamic TP/SL (or use defaults)
    2. Place GTC limit sell at entry + TP (maker = 0% fee)
    3. Monitor: if stop loss or timeout hit → cancel limit, market sell
    4. If limit fills (shares go to 0) → profit taken at 0% exit fee
    """
    start = time.time()
    side_label = pos_info.get("side", "?")
    market = pos_info.get("market", "?")

    # --- Dynamic TP/SL from order book analysis ---
    profit_target = PROFIT_TARGET
    stop_loss = STOP_LOSS
    book_info_str = "book N/A, using defaults"
    entry_spread = 0.0
    if _detector_ref and (BOOK_TP_ENABLED or BOOK_SL_ENABLED):
        analysis = _detector_ref.analyze_book(token_id, entry_price=entry_price)
        if analysis:
            profit_target = analysis["suggested_tp"]
            stop_loss = analysis["suggested_sl"]
            entry_spread = analysis["spread"]
            wall_ask = analysis["first_ask_wall"]
            wall_bid = analysis["first_bid_wall"]
            ask_str = f"ask wall ${wall_ask[1]:.0f}@{wall_ask[0]:.2f}" if wall_ask else "no ask wall"
            bid_str = f"bid wall ${wall_bid[1]:.0f}@{wall_bid[0]:.2f}" if wall_bid else "no bid wall"
            book_info_str = (f"spread={entry_spread:.2f}, depth=${analysis['total_bid_depth']:.0f}b/${analysis['total_ask_depth']:.0f}a, "
                           f"{ask_str}, {bid_str}")

    target_price = round(entry_price + profit_target, 2)
    # SL is measured from the BID at entry (not the ask we paid).
    # We buy at ask, but monitor the bid for exit. The spread is NOT a loss —
    # it's the natural cost of crossing. Without this, a spread of 0.04-0.07
    # instantly triggers a SL of 0.03.
    entry_bid = entry_price - entry_spread
    stop_price = round(entry_bid - stop_loss, 2)

    rr_ratio = profit_target / stop_loss if stop_loss > 0 else 999

    # Both entry and exit are maker (0% fee)
    entry_is_maker = pos_info.get("entry_is_maker", False)
    target_gross_pnl = (target_price - entry_price) * shares

    log(f"  ⏱ Flip monitor started: {side_label} {shares:.0f}sh @ {entry_price:.2f} (bid {entry_bid:.2f}, spread {entry_spread:.2f})")
    log(f"    TP: {target_price:.2f} (+{profit_target:.2f}) | SL: {stop_price:.2f} (-{stop_loss:.2f} from bid) | R:R {rr_ratio:.1f}x | Timeout: {FLIP_TIMEOUT}s")
    log(f"    Book: {book_info_str}")
    if entry_is_maker:
        log(f"    Entry: maker (0% fee) | Net P&L if target hit: ~${target_gross_pnl:+.2f}")
    else:
        entry_fee_est = compute_taker_fee(shares, entry_price)
        log(f"    Entry fee: ~${entry_fee_est:.2f} | Net P&L if target hit: ~${target_gross_pnl - entry_fee_est:+.2f}")

    # Step 1: Place GTC limit sell at profit target in BACKGROUND THREAD
    # This way monitoring starts immediately — no 3s blind spot for stop-loss
    limit_state = {"placed": False, "sell_shares": shares, "abort": False}

    def _place_limit_sell():
        for attempt in range(LIMIT_SELL_RETRIES):
            if limit_state["abort"]:
                return
            try:
                time.sleep(LIMIT_SELL_RETRY_DELAY)  # Wait for shares to settle
                if limit_state["abort"]:
                    return
                actual = get_actual_share_balance(token_id)
                if actual is None or actual < 1:
                    log(f"  ⏳ Waiting for shares (attempt {attempt+1}/{LIMIT_SELL_RETRIES}, balance: {actual})")
                    continue
                sell_shares = math.floor(actual * 100) / 100
                if sell_shares < 5:
                    log(f"  ⚠ Shares {sell_shares:.1f} below min order size 5 — will use market sell")
                    break
                if limit_state["abort"]:
                    return
                sell_order = OrderArgs(
                    token_id=token_id,
                    price=target_price,
                    size=sell_shares,
                    side=SELL,
                )
                signed_sell = client.create_order(sell_order)
                resp = client.post_order(signed_sell, OrderType.GTC)
                limit_state["placed"] = True
                limit_state["sell_shares"] = sell_shares
                log(f"  ✓ Limit sell placed: {sell_shares:.0f}sh at {target_price:.2f} (GTC, 0% fee)")
                return
            except Exception as e:
                log(f"  ⚠ Limit sell attempt {attempt+1}/{LIMIT_SELL_RETRIES} failed: {e}")
        if not limit_state["abort"]:
            log(f"  ⚠ All limit sell attempts failed — will use market sell on exit")

    if not DRY_RUN:
        threading.Thread(target=_place_limit_sell, daemon=True).start()

    # Step 2: Event-driven monitor for stop loss / timeout / limit fill
    # Instead of polling every 0.2s, we wake on WS price updates + fill events
    exit_reason = "timeout"
    exit_price = entry_price
    exit_is_maker = False
    limit_fill_event = threading.Event()
    monitor_cond = threading.Condition()

    def _on_price_tick(asset_id):
        """WS callback: new price available for our token — wake monitor."""
        with monitor_cond:
            monitor_cond.notify_all()

    def _on_limit_fill(trade_info):
        """User Channel callback: our limit sell was confirmed on-chain."""
        if trade_info.get("side") == "SELL":
            limit_fill_event.set()
            with monitor_cond:
                monitor_cond.notify_all()

    # Register callbacks
    if _detector_ref:
        _detector_ref.on_price_update(token_id, _on_price_tick)
    if not DRY_RUN and _user_ws_ref:
        _user_ws_ref.on_fill(token_id, _on_limit_fill)

    last_rest_check = start
    while True:
        elapsed = time.time() - start
        remaining = FLIP_TIMEOUT - elapsed

        if remaining <= 0:
            # Before declaring timeout, check if limit order silently filled
            # (WS event missed, or filled between REST checks)
            if limit_state["placed"] and not DRY_RUN:
                remaining_shares = get_actual_share_balance(token_id)
                if remaining_shares is not None and remaining_shares < 0.5:
                    exit_reason = "profit"
                    exit_price = target_price
                    exit_is_maker = True
                    log(f"  ✓ Limit fill detected at timeout (shares={remaining_shares:.3f})")
                    break
            exit_reason = "timeout"
            exit_price = get_price(token_id, side=SELL) or entry_price
            break

        # Wait for next WS event or 2s safety-net timeout
        with monitor_cond:
            monitor_cond.wait(timeout=min(remaining, 2.0))

        current_price = get_price(token_id, side=SELL)

        if current_price <= 0:
            continue

        # Check if limit order filled via User Channel event (instant, no REST call)
        if limit_state["placed"] and not DRY_RUN:
            if limit_fill_event.is_set():
                exit_reason = "profit"
                exit_price = target_price
                exit_is_maker = True
                break
            # REST fallback every 5s in case WS missed it
            now = time.time()
            if now - last_rest_check >= 5.0:
                last_rest_check = now
                remaining_shares = get_actual_share_balance(token_id)
                if remaining_shares is not None and remaining_shares < 0.5:
                    exit_reason = "profit"
                    exit_price = target_price
                    exit_is_maker = True
                    break

        # Dry run: simulate limit fill when price reaches target
        if DRY_RUN and current_price >= target_price:
            exit_reason = "profit"
            exit_price = target_price
            exit_is_maker = True
            break

        if current_price <= stop_price:
            exit_reason = "stop_loss"
            exit_price = current_price
            break

    # Clean up callbacks
    if _detector_ref:
        _detector_ref.remove_price_callback(token_id, _on_price_tick)
    if _user_ws_ref:
        _user_ws_ref.remove_fill_callback(token_id, _on_limit_fill)

    # Signal background limit sell thread to stop
    limit_state["abort"] = True
    limit_placed = limit_state["placed"]

    # Step 3: Execute exit
    pnl = 0.0
    gross_pnl = (exit_price - entry_price) * shares
    # Compute fees: entry is maker (0%) if limit buy filled, else taker
    entry_fee = 0.0 if entry_is_maker else compute_taker_fee(shares, entry_price)
    exit_fee = 0.0 if exit_is_maker else compute_taker_fee(shares, exit_price)
    net_pnl = gross_pnl - entry_fee - exit_fee

    if DRY_RUN:
        pnl = net_pnl
        fee_str = f"entry {'0%' if entry_is_maker else f'${entry_fee:.2f}'}" + f" + exit {'0%' if exit_is_maker else f'${exit_fee:.2f}'}"
        log(f"  🧪 DRY-RUN flip: {exit_reason} at {exit_price:.2f} | Gross ${gross_pnl:+.2f} | Fees: {fee_str} | Net: ${pnl:+.2f}")
    else:
        if exit_reason == "profit" and exit_is_maker:
            # Limit order already filled — no action needed
            pnl = net_pnl
            fee_note = "0% both sides" if entry_is_maker else f"entry ${entry_fee:.2f}"
            log(f"  ✓ Limit filled (profit): {target_price:.2f} | Net P&L: ${pnl:+.2f} ({fee_note})")
        else:
            # ALWAYS cancel all orders for this token before attempting exit sell.
            # Even if limit_placed=False, the _place_limit_sell thread may have
            # placed an order between when we read the flag and now (race condition).
            cancel_all_orders_for_token(token_id)
            if limit_placed:
                log(f"  ↩ Cancelled limit sell (exit: {exit_reason})")
            # Wait for on-chain allowance release after cancel.
            # Without this, sell attempts fail with "not enough balance / allowance".
            time.sleep(2.0)

            sell_succeeded = False
            # Recalculate fees assuming maker exit (0% fee)
            exit_fee = 0.0
            net_pnl = gross_pnl - entry_fee - exit_fee
            try:
                actual = get_actual_share_balance(token_id)
                sell_shares = math.floor((actual or shares) * 100) / 100

                if sell_shares > 0:
                    # GTC limit sell at low price → fills instantly at best bid (maker = 0% fee)
                    # This avoids taker fees on stop-loss/timeout exits
                    sell_limit_price = round(max(exit_price - 0.05, 0.01), 2)
                    last_err = None
                    for attempt in range(3):
                        try:
                            # Refresh allowance on each retry (cancel may not have settled yet)
                            if attempt > 0:
                                time.sleep(1.0)
                                actual = get_actual_share_balance(token_id)
                                sell_shares = math.floor((actual or shares) * 100) / 100
                                if sell_shares <= 0:
                                    break
                            sell_order = OrderArgs(
                                token_id=token_id,
                                price=sell_limit_price,
                                size=sell_shares,
                                side=SELL,
                            )
                            signed_sell = client.create_order(sell_order)
                            resp = client.post_order(signed_sell, OrderType.GTC)
                            sell_succeeded = True
                            break
                        except Exception as gtc_err:
                            last_err = gtc_err
                            # Fallback to FAK if GTC fails on final attempt
                            if attempt == 2:
                                try:
                                    mo = MarketOrderArgs(
                                        token_id=token_id,
                                        amount=sell_shares,
                                        side=SELL,
                                        order_type=OrderType.FAK,
                                    )
                                    signed = client.create_market_order(mo)
                                    resp = client.post_order(signed, OrderType.FAK)
                                    sell_succeeded = True
                                    exit_fee = compute_taker_fee(sell_shares, exit_price)
                                    net_pnl = gross_pnl - entry_fee - exit_fee
                                    exit_is_maker = False
                                    log(f"  ⚠ GTC sell failed, used FAK fallback (taker fee)")
                                except Exception as fak_err:
                                    last_err = fak_err

                    if sell_succeeded:
                        pnl = net_pnl
                        exit_is_maker = exit_fee == 0.0
                        fee_note = f"maker 0% exit" if exit_is_maker else f"fees: ${entry_fee+exit_fee:.2f}"
                        log(f"  ✓ {'Limit' if exit_is_maker else 'Market'} sell ({exit_reason}): {sell_shares:.0f}sh at ~{exit_price:.2f} | Net P&L: ${pnl:+.2f} ({fee_note})")
                    else:
                        # All attempts failed — log unrealized loss
                        pnl = net_pnl
                        log(f"  ✗ Sell failed after 3 attempts: {last_err}")
                        log(f"  ⚠ {sell_shares:.0f}sh ORPHANED — unrealized P&L: ${pnl:+.2f}")
                else:
                    # Shares already gone — if limit was placed, it filled
                    if limit_placed:
                        exit_reason = "profit"
                        exit_price = target_price
                        exit_is_maker = True
                        exit_fee = 0.0
                        gross_pnl = (target_price - entry_price) * shares
                        net_pnl = gross_pnl - entry_fee
                        pnl = net_pnl
                        log(f"  ✓ Limit already filled (detected on exit): {target_price:.2f} | Net P&L: ${pnl:+.2f}")
                    else:
                        log(f"  ⚠ No shares to sell and no limit placed (balance: {actual})")
            except Exception as e:
                pnl = net_pnl  # Record the loss even if sell fails
                log(f"  ✗ Flip sell failed: {e}")
                log(f"  ⚠ Shares may be orphaned — unrealized P&L: ${pnl:+.2f}")

        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": f"FLIP_{exit_reason.upper()}",
            "market": market,
            "side": side_label,
            "token": token_id,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "shares": shares,
            "gross_pnl": round(gross_pnl, 4),
            "entry_fee": round(entry_fee, 4),
            "exit_fee": round(exit_fee, 4),
            "net_pnl": round(pnl, 4),
            "entry_is_maker": entry_is_maker,
            "exit_is_maker": exit_is_maker,
            "elapsed": round(time.time() - start, 1),
            "profit_target": profit_target,
            "stop_loss": stop_loss,
            "book_tp_enabled": BOOK_TP_ENABLED,
            "book_sl_enabled": BOOK_SL_ENABLED,
        }
        with open(TRADE_LOG, "a") as f:
            f.write(json.dumps(record) + "\n")

    return exit_reason, pnl


# =========================================================================
# MAIN BOT
# =========================================================================
def _cleanup_orphaned_positions():
    """On startup, find and sell any shares left from previous runs.
    Scans current crypto market tokens for non-zero balances and dumps them."""
    log("🔍 Checking for orphaned positions from previous runs...")
    orphans_found = 0
    orphans_sold = 0
    try:
        markets = get_current_crypto_markets()
        checked = set()
        for market in markets:
            tokens = json.loads(market.get("clobTokenIds", "[]"))
            question = market.get("question", "?")
            for idx, token_id in enumerate(tokens):
                if token_id in checked:
                    continue
                checked.add(token_id)
                balance = get_actual_share_balance(token_id)
                if balance is None or balance < 1:
                    continue
                side_name = "UP" if idx == 0 else "DOWN"
                orphans_found += 1
                sell_shares = math.floor(balance * 100) / 100
                log(f"  ⚠ Found orphan: {question} {side_name} — {sell_shares:.0f}sh")
                # Cancel any resting orders first
                cancel_all_orders_for_token(token_id)
                time.sleep(1.0)
                # Sell via GTC at low price (maker, 0% fee)
                try:
                    current_price = get_price(token_id, side=SELL)
                    sell_limit_price = round(max(current_price - 0.05, 0.01), 2) if current_price > 0 else 0.01
                    sell_order = OrderArgs(
                        token_id=token_id,
                        price=sell_limit_price,
                        size=sell_shares,
                        side=SELL,
                    )
                    signed_sell = client.create_order(sell_order)
                    resp = client.post_order(signed_sell, OrderType.GTC)
                    orphans_sold += 1
                    log(f"  ✓ Sold orphan: {sell_shares:.0f}sh at ~{current_price:.2f}")
                except Exception as e:
                    log(f"  ✗ Failed to sell orphan: {e}")
                    # Try FAK as fallback
                    try:
                        mo = MarketOrderArgs(
                            token_id=token_id,
                            amount=sell_shares,
                            side=SELL,
                            order_type=OrderType.FAK,
                        )
                        signed = client.create_market_order(mo)
                        client.post_order(signed, OrderType.FAK)
                        orphans_sold += 1
                        log(f"  ✓ Sold orphan via FAK: {sell_shares:.0f}sh")
                    except Exception as e2:
                        log(f"  ✗ FAK fallback also failed: {e2}")
    except Exception as e:
        log(f"  ⚠ Orphan cleanup error: {e}")
    if orphans_found == 0:
        log("  ✓ No orphans found")
    else:
        log(f"  Cleanup: {orphans_sold}/{orphans_found} orphans sold")


def run_burst():
    # Clean up any orphaned positions from previous runs BEFORE starting
    _cleanup_orphaned_positions()
    time.sleep(1)  # Let balance settle after cleanup

    starting_balance = get_usdc_balance() or 100
    log(f"\n💵 Starting balance: ${starting_balance:.2f}")

    positions = {}        # token_id -> pos_info
    positions_lock = threading.Lock()
    session_pnl = [0.0]   # mutable container for thread access

    def on_imbalance(token_id, ratio, direction, info):
        """Called from WS thread when order book imbalance is detected.
        direction='BUY_IMBALANCE' → bid depth >> ask depth on this token → buy it.
        Limit buy at best_bid + offset (maker, 0% fee).
        """
        if not info:
            return

        tokens = info.get("tokens", [])
        if len(tokens) != 2:
            return

        with positions_lock:
            if len(positions) >= MAX_CONCURRENT_POSITIONS:
                return
            # Don't double-enter same market
            for t in tokens:
                if t in positions:
                    return

        market = info.get("market", "?")
        buy_side = info.get("side", "UP")  # which side this token represents
        best_bid = info.get('_best_bid', 0)
        best_ask = info.get('_best_ask', 0)
        bid_depth = info.get('_bid_depth', 0)
        ask_depth = info.get('_ask_depth', 0)
        spread = info.get('_spread', 0)

        # Check time remaining — don't enter markets near resolution
        market_end = parse_market_end_time(market)
        if market_end:
            now_utc = datetime.now(timezone.utc)
            remaining = (market_end - now_utc).total_seconds()
            if remaining < MIN_TIME_REMAINING:
                log(f"  📊 Imbalance on {market} {buy_side}: ratio {ratio:.1f}x — only {remaining:.0f}s left")
                return

        # Price range check
        if best_ask <= 0 or best_ask >= 1:
            return
        if best_ask < MIN_ENTRY_PRICE:
            log(f"  📊 Imbalance on {market} {buy_side}: ratio {ratio:.1f}x — price {best_ask:.2f} too cheap")
            return
        if best_ask > MAX_ENTRY_PRICE:
            log(f"  📊 Imbalance on {market} {buy_side}: ratio {ratio:.1f}x — price {best_ask:.2f} too expensive")
            return

        # Compute limit buy price: best_bid + offset (sits near top of book)
        limit_price = round(best_bid + LIMIT_BUY_OFFSET, 2)
        if limit_price >= best_ask:
            # Our limit is at or above the ask — would cross the spread.
            # Place at best_bid instead (pure maker, slightly lower fill chance).
            limit_price = best_bid

        # Pre-buy R:R gate — check book TP/SL before committing
        if _detector_ref and (BOOK_TP_ENABLED or BOOK_SL_ENABLED):
            pre_analysis = _detector_ref.analyze_book(token_id, entry_price=limit_price)
            if pre_analysis:
                pre_tp = pre_analysis["suggested_tp"]
                pre_sl = pre_analysis["suggested_sl"]
                pre_rr = pre_tp / pre_sl if pre_sl > 0 else 999
                if pre_rr < MIN_RR_RATIO:
                    log(f"  📊 Imbalance on {market} {buy_side}: ratio {ratio:.1f}x — bad R:R ({pre_rr:.1f}x < {MIN_RR_RATIO}x)")
                    return

        # Check balance
        bal = get_usdc_balance()
        if bal is None or bal < MIN_BALANCE_BUFFER + BUY_AMOUNT:
            log(f"  📊 Imbalance on {market} {buy_side}: ratio {ratio:.1f}x — insufficient balance (${bal or 0:.2f})")
            return

        amount = min(BUY_AMOUNT, MAX_BUY_AMOUNT, bal - MIN_BALANCE_BUFFER)
        est_shares = math.floor(amount / limit_price * 100) / 100
        if est_shares < 5:
            log(f"  📊 Imbalance on {market} {buy_side}: shares {est_shares:.1f} below min 5")
            return

        log(f"\n📊 BOOK IMBALANCE: {market} — buy {buy_side}")
        log(f"  Bid ${bid_depth:.0f} / Ask ${ask_depth:.0f} = {ratio:.1f}x ratio | spread {spread:.2f}")
        log(f"  Limit buy {est_shares:.0f}sh at {limit_price:.2f} (best_bid {best_bid:.2f}, ask {best_ask:.2f}), ${amount:.0f}")

        if DRY_RUN:
            shares = amount / limit_price
            log(f"  🧪 DRY-RUN: limit buy {shares:.0f}sh of {buy_side} at {limit_price:.2f} (maker, 0% fee)")
            record = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "action": "DRY_IMBALANCE_BUY",
                "market": market,
                "buy_side": buy_side,
                "token": token_id,
                "limit_price": limit_price,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "bid_depth": bid_depth,
                "ask_depth": ask_depth,
                "ratio": ratio,
                "amount": amount,
                "entry_is_maker": True,
            }
            with open(TRADE_LOG, "a") as f:
                f.write(json.dumps(record) + "\n")

            pos_info = {"market": market, "side": buy_side, "entry_price": limit_price, "shares": shares,
                        "fade_reason": f"imbalance {ratio:.1f}x", "burst_side": buy_side, "entry_is_maker": True}
            with positions_lock:
                positions[token_id] = pos_info

            reason, pnl = quick_flip(token_id, limit_price, shares, pos_info)
            session_pnl[0] += pnl
            with positions_lock:
                positions.pop(token_id, None)
            return

        # LIVE: reserve position slot and spawn buy thread
        with positions_lock:
            positions[token_id] = {"market": market, "side": buy_side, "entry_price": limit_price,
                                   "shares": 0, "fade_reason": f"imbalance {ratio:.1f}x",
                                   "burst_side": buy_side, "entry_is_maker": True, "status": "buying"}

        def do_limit_buy_and_flip():
            try:
                entry_start = time.time()

                # === GTC LIMIT BUY (maker, 0% fee) ===
                # Sits on the order book at our price. Fills atomically when someone
                # sells into it — no settlement blind spot, no taker fee.
                buy_order = OrderArgs(
                    token_id=token_id,
                    price=limit_price,
                    size=est_shares,
                    side=BUY,
                )
                signed_buy = client.create_order(buy_order)
                resp = client.post_order(signed_buy, OrderType.GTC)

                order_id = resp.get("orderID", "") if isinstance(resp, dict) else ""
                resp_status = resp.get("status", "") if isinstance(resp, dict) else ""
                log(f"  📝 Limit buy placed: {est_shares:.0f}sh at {limit_price:.2f} (order {order_id[:8] if order_id else '?'})")

                if not resp_status or resp_status == "error":
                    log(f"  ⚠ Limit buy rejected: {resp}")
                    with positions_lock:
                        positions.pop(token_id, None)
                    return

                # === WAIT FOR FILL ===
                # Primary: User Channel WS fires MATCHED/CONFIRMED event
                # Fallback: REST share balance check every 3s
                # Safety: Monitor book health — cancel if imbalance disappears (adverse selection guard)
                fill_event = threading.Event()
                def _on_buy_fill(trade_info):
                    if trade_info.get("side") == "BUY":
                        fill_event.set()
                user_ws.on_fill(token_id, _on_buy_fill)

                filled = False
                cancelled_for_book = False
                fill_deadline = entry_start + FILL_TIMEOUT
                actual = None

                while time.time() < fill_deadline:
                    remaining = fill_deadline - time.time()
                    if remaining <= 0:
                        break
                    # Wait for WS event or poll interval (whichever comes first)
                    if fill_event.wait(timeout=min(3.0, remaining)):
                        # WS confirmed fill — check share balance
                        time.sleep(0.3)  # brief pause for chain
                        actual = get_actual_share_balance(token_id)
                        if actual is not None and actual >= 5:
                            filled = True
                            break
                    else:
                        # REST fallback check
                        actual = get_actual_share_balance(token_id)
                        if actual is not None and actual >= 5:
                            filled = True
                            break

                        # === BOOK HEALTH CHECK ===
                        # If imbalance has disappeared while we wait, the signal is invalid.
                        # Cancel early to avoid adverse fill (sellers dumping through our level).
                        wait_elapsed = time.time() - entry_start
                        if wait_elapsed >= 3.0 and _detector_ref:
                            cur_book = _detector_ref._books.get(token_id)
                            if cur_book and time.time() - cur_book.get("ts", 0) < 10:
                                cur_bids = cur_book.get("bids", [])
                                cur_asks = cur_book.get("asks", [])
                                if cur_bids and cur_asks:
                                    cur_best_bid = cur_bids[0][0]
                                    cur_bid_depth = sum(p * s for p, s in cur_bids if p >= cur_best_bid - IMBALANCE_DEPTH_RANGE)
                                    cur_ask_depth = sum(p * s for p, s in cur_asks if p <= cur_asks[0][0] + IMBALANCE_DEPTH_RANGE)
                                    cur_ratio = cur_bid_depth / cur_ask_depth if cur_ask_depth > 0 else 0
                                    # If ratio dropped below 1.5x, imbalance is gone — cancel
                                    if cur_ratio < 1.5:
                                        log(f"  📉 Book imbalance gone ({cur_ratio:.1f}x < 1.5x) after {wait_elapsed:.1f}s — cancelling buy")
                                        cancelled_for_book = True
                                        break

                user_ws.remove_fill_callback(token_id, _on_buy_fill)

                if not filled and not cancelled_for_book:
                    # Final check before giving up
                    actual = get_actual_share_balance(token_id)
                    if actual is not None and actual >= 5:
                        filled = True

                if not filled:
                    # No fill — cancel the limit order and release position
                    elapsed = time.time() - entry_start
                    reason_str = "book deteriorated" if cancelled_for_book else f"not filled after {elapsed:.1f}s"
                    log(f"  ⏰ Limit buy {reason_str} — cancelling")
                    cancel_all_orders_for_token(token_id)
                    # Dump any partial fill dust
                    if actual and actual > 0:
                        try:
                            dump_mo = MarketOrderArgs(token_id=token_id, amount=actual, side=SELL, order_type=OrderType.FAK)
                            signed_dump = client.create_market_order(dump_mo)
                            client.post_order(signed_dump, OrderType.FAK)
                            log(f"  🧹 Dumped {actual:.1f} dust shares")
                        except Exception:
                            pass
                    with positions_lock:
                        positions.pop(token_id, None)
                    return

                # === FILLED — start exit ===
                buy_shares = math.floor(actual * 100) / 100
                fill_price = limit_price  # Maker fills at limit price or better
                elapsed = time.time() - entry_start

                log(f"  ✓ Limit buy filled: {buy_shares:.0f}sh of {buy_side} at {fill_price:.2f} (maker, 0% fee) in {elapsed:.1f}s")

                # Cancel any remaining open orders for this token (partial fills leave resting qty)
                cancel_all_orders_for_token(token_id)
                # Wait for on-chain settlement after cancel — without this,
                # balance/allowance temporarily shows 0 and limit sell placement fails
                time.sleep(1.5)

                # === POST-FILL SANITY CHECK ===
                # Our limit buy fills when someone SELLS into it — meaning price
                # may already be crashing through our level. Check current bid:
                # if it's already at or below our stop-loss level, dump immediately
                # instead of entering a doomed position.
                current_bid_now = get_price(token_id, side=SELL)
                if current_bid_now > 0:
                    # Quick SL estimate: fill_price - STOP_LOSS (or book SL)
                    quick_sl = STOP_LOSS
                    if _detector_ref and BOOK_SL_ENABLED:
                        post_analysis = _detector_ref.analyze_book(token_id, entry_price=fill_price)
                        if post_analysis:
                            quick_sl = post_analysis["suggested_sl"]
                    abort_price = fill_price - quick_sl
                    if current_bid_now <= abort_price:
                        log(f"  ⚠ Post-fill abort: bid {current_bid_now:.2f} already at/below SL {abort_price:.2f} — dumping")
                        try:
                            dump_mo = MarketOrderArgs(token_id=token_id, amount=buy_shares, side=SELL, order_type=OrderType.FAK)
                            signed_dump = client.create_market_order(dump_mo)
                            client.post_order(signed_dump, OrderType.FAK)
                            dump_price = current_bid_now
                            dump_pnl = (dump_price - fill_price) * buy_shares
                            log(f"  🚫 Dumped {buy_shares:.0f}sh at ~{dump_price:.2f} | PnL: ${dump_pnl:+.2f}")
                            session_pnl[0] += dump_pnl
                            record = {
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "action": "IMBALANCE_BUY",
                                "market": market, "buy_side": buy_side, "token": token_id,
                                "limit_price": limit_price, "fill_price": fill_price,
                                "best_bid": best_bid, "best_ask": best_ask,
                                "bid_depth": bid_depth, "ask_depth": ask_depth,
                                "ratio": ratio, "amount": amount, "shares": buy_shares,
                                "entry_is_maker": True, "fill_time": round(elapsed, 2),
                                "aborted": True, "abort_bid": current_bid_now,
                            }
                            with open(TRADE_LOG, "a") as f:
                                f.write(json.dumps(record) + "\n")
                        except Exception as e:
                            log(f"  ⚠ Abort dump failed: {e}")
                        with positions_lock:
                            positions.pop(token_id, None)
                        if True:  # always lock out after adverse fill
                            detector.set_loss_lockout(market)
                        return

                record = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "action": "IMBALANCE_BUY",
                    "market": market,
                    "buy_side": buy_side,
                    "token": token_id,
                    "limit_price": limit_price,
                    "fill_price": fill_price,
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "bid_depth": bid_depth,
                    "ask_depth": ask_depth,
                    "ratio": ratio,
                    "amount": amount,
                    "shares": buy_shares,
                    "entry_is_maker": True,
                    "fill_time": round(elapsed, 2),
                    "response": str(resp),
                }
                with open(TRADE_LOG, "a") as f:
                    f.write(json.dumps(record) + "\n")

                # Update position with real shares and start flip
                pos_info = {"market": market, "side": buy_side, "entry_price": fill_price, "shares": buy_shares,
                            "fade_reason": f"imbalance {ratio:.1f}x", "burst_side": buy_side, "entry_is_maker": True}
                with positions_lock:
                    positions[token_id] = pos_info

                reason, pnl = quick_flip(token_id, fill_price, buy_shares, pos_info)
                session_pnl[0] += pnl
                with positions_lock:
                    positions.pop(token_id, None)

                # After a losing exit, lock out this market
                if reason in ("stop_loss", "timeout"):
                    detector.set_loss_lockout(market)

            except Exception as e:
                log(f"  ✗ Imbalance buy failed: {e}")
                cancel_all_orders_for_token(token_id)
                with positions_lock:
                    positions.pop(token_id, None)

        threading.Thread(target=do_limit_buy_and_flip, daemon=True).start()

    # Create burst detector (now used for book imbalance scanning)
    global _detector_ref, _user_ws_ref
    detector = BurstDetector(on_burst=on_imbalance)
    _detector_ref = detector
    detector.start()

    # Create user channel for real-time fill detection
    user_ws = UserChannelWS()
    _user_ws_ref = user_ws
    if not DRY_RUN:
        user_ws.start()

    log(f"\n{'='*60}")
    if DRY_RUN:
        log(f"🧪 DRY-RUN MODE — no orders will be placed")
    log(f"📊 BOOK IMBALANCE bot — order book depth strategy")
    log(f"Signal: bid/ask depth ratio > {IMBALANCE_RATIO}x (range ±{IMBALANCE_DEPTH_RANGE}, min bid ${IMBALANCE_MIN_DEPTH}, max spread {IMBALANCE_MAX_SPREAD})")
    log(f"Entry: GTC limit buy at best_bid + {LIMIT_BUY_OFFSET} (maker 0%) | fill timeout {FILL_TIMEOUT}s | scan cooldown {SCAN_COOLDOWN}s")
    log(f"Exit: dynamic TP/SL from book (defaults +{PROFIT_TARGET}/-{STOP_LOSS}) | wall min ${BOOK_WALL_MIN_SIZE}")
    log(f"  Book TP: {'ON' if BOOK_TP_ENABLED else 'OFF'} (range {BOOK_TP_MIN}-{BOOK_TP_MAX}), Book SL: {'ON' if BOOK_SL_ENABLED else 'OFF'} (range {BOOK_SL_MIN}-{BOOK_SL_MAX})")
    log(f"Timeout: {FLIP_TIMEOUT}s | Monitor: event-driven (WS price callbacks), REST fallback 5s")
    log(f"Entry price: {MIN_ENTRY_PRICE}-{MAX_ENTRY_PRICE}, R:R gate >= {MIN_RR_RATIO}x")
    log(f"Cooldowns: {COOLDOWN_AFTER_ENTRY}s/token, {MARKET_COOLDOWN}s/market, {LOSS_LOCKOUT}s/loss-lockout")
    log(f"Time filter: skip markets with <{MIN_TIME_REMAINING}s remaining")
    log(f"Max positions: {MAX_CONCURRENT_POSITIONS}")
    log(f"Buy amount: ${BUY_AMOUNT}, Cryptos: {', '.join(CRYPTOS)}")
    log(f"{'='*60}\n")

    # Main loop: discover markets, update subscriptions, show status
    while True:
        try:
            markets = get_current_crypto_markets()

            all_tokens = []
            for m in markets:
                tokens = json.loads(m.get("clobTokenIds", "[]"))
                if len(tokens) == 2:
                    all_tokens.extend(tokens)
                    question = m.get("question", "N/A")
                    # Register which side each token represents
                    detector.set_token_info(tokens[0], {
                        "market": question,
                        "side": "UP",
                        "tokens": tokens,
                        "condition_id": m.get("conditionId", ""),
                        "slug": m.get("_slug", ""),
                        "interval": m.get("_interval", 5),
                        "crypto": m.get("_crypto", ""),
                    })
                    detector.set_token_info(tokens[1], {
                        "market": question,
                        "side": "DOWN",
                        "tokens": tokens,
                        "condition_id": m.get("conditionId", ""),
                        "slug": m.get("_slug", ""),
                        "interval": m.get("_interval", 5),
                        "crypto": m.get("_crypto", ""),
                    })

            detector.update_subscriptions(all_tokens)

            # Subscribe markets to user channel for fill detection
            for m in markets:
                cid = m.get("conditionId", "")
                if cid:
                    user_ws.subscribe_market(cid)

            # Status
            bal = get_usdc_balance()
            now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
            ws_mkt = "🟢" if detector.connected else "🔴"
            ws_usr = "🟢" if user_ws.connected else "🔴"
            bal_str = f"${bal:.2f}" if bal else "?"
            dry_str = " 🧪 DRY" if DRY_RUN else ""

            with positions_lock:
                n_pos = len(positions)

            print(f"\r⏰ {now_str} | {n_pos} pos | bal {bal_str} | "
                  f"session ${session_pnl[0]:+.2f} | "
                  f"mkt{ws_mkt} usr{ws_usr} ({detector.event_count} events, {detector.subscribed_count} subs)"
                  f"{dry_str}    ", end="", flush=True)

            time.sleep(5)

        except KeyboardInterrupt:
            log(f"\nBot stopped. Session P&L: ${session_pnl[0]:+.2f}")
            break
        except Exception as e:
            log(f"\n⚠ Error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    run_burst()
