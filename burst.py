#!/usr/bin/env python3
"""
Burst Bot — CONTRARIAN (fade-the-burst) strategy on Polymarket crypto markets.

Strategy:
  Monitor real-time WebSocket trade feed for sudden bursts of buying or selling.
  When a burst is detected, buy the OPPOSITE side — fading the herd.
  Rationale: bursts are typically late retail piling in after the move;
  the price is more likely to revert than continue.

Signals:
  1. Buy burst on token X → buy the opposite token (fade the buy)
  2. Sell burst on token X → buy token X itself (fade the sell / oversold bounce)

Detection:
  - Sliding 5-second window of trades per token
  - Buy burst = net buy $ exceeds BURST_THRESHOLD → fade it
  - Sell burst = net sell $ exceeds BURST_THRESHOLD → fade it
  - Only fade when burst side is elevated (>=50¢) — ensures meaningful signal

Exit (quick flip):
  - Place GTC limit sell at entry + PROFIT_TARGET immediately (maker = 0% fee)
  - If price drops below entry - STOP_LOSS → cancel limit, market sell (cut loss)
  - If FLIP_TIMEOUT seconds pass → cancel limit, market sell (take whatever)

Fees (5m/15m crypto markets):
  - Formula: fee = shares × feeRate × (p × (1 - p))^exponent
  - feeRate=0.25, exponent=2 → max ~1.56% at p=0.50, near-zero at extremes
  - Taker orders (FAK market) pay this fee; maker orders (GTC limit) pay 0%
  - Fee rate fetched per-token from API: GET /fee-rate?token_id={id}
  - SDK auto-includes feeRateBps in signed orders
"""

import os
import sys
import time
import math
import requests
import json
import asyncio
import threading
from collections import defaultdict, deque
from datetime import datetime, timezone
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
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CHAIN_ID = 137
FUNDER_ADDRESS = founder_address

# =========================================================================
# STRATEGY PARAMETERS
# =========================================================================
BUY_AMOUNT = 10              # Base bet in $
BURST_WINDOW = 5.0           # Seconds — detect bursts within this window
BURST_THRESHOLD = 500        # $ net volume in the window to trigger (was 200, too low)
MIN_ENTRY_PRICE = 0.08       # Don't buy fade token below this (dead side)
MAX_ENTRY_PRICE = 0.55       # Don't buy fade token above this (too expensive for contrarian)
MIN_BURST_PRICE = 0.50       # Burst side must be >= this to fade (ensures one-sided market)

# Fees — real Polymarket formula: fee = C * feeRate * (p * (1-p))^exponent
# For 5m/15m crypto: feeRate=0.25, exponent=2. Max ~1.56% at p=0.50.
# Maker (GTC limit) = 0% fee. Taker (FAK market) = formula-based fee.
CRYPTO_FEE_RATE = 0.25       # feeRate for 5m/15m crypto markets
CRYPTO_FEE_EXPONENT = 2      # exponent for 5m/15m crypto markets

# Quick flip exit
PROFIT_TARGET = 0.03         # Limit sell at entry + this (maker, 0% fee) (was 0.05, unreachable)
STOP_LOSS = 0.05             # Cancel limit + market sell if price drops this much (was 0.10)
FLIP_TIMEOUT = 30            # Max seconds to hold before force-selling (was 15)
PRICE_CHECK_INTERVAL = 1.0   # How often to check price during flip
LIMIT_SELL_RETRIES = 3       # Retry limit sell placement this many times
LIMIT_SELL_RETRY_DELAY = 3   # Seconds between retries

# Risk management
MAX_CONCURRENT_POSITIONS = 2
MIN_BALANCE_BUFFER = 5
SESSION_STOP_LOSS_PCT = 0.30
COOLDOWN_AFTER_ENTRY = 30.0  # Don't re-enter same TOKEN for N seconds (was 5)
MARKET_COOLDOWN = 60.0       # Don't re-enter same MARKET (either side) for N seconds

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
    try:
        data = client.get_price(token_id, side=side)
        return float(data.get("price", 0))
    except Exception:
        return 0.0


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


# Cache fee rates fetched from API to avoid repeated calls
_fee_rate_cache = {}  # token_id -> fee_rate_bps (int)


def get_fee_rate_bps(token_id):
    """Fetch fee rate in basis points from Polymarket API for a token.
    Returns the fee_rate_bps (e.g. 2500 for 0.25 feeRate on crypto markets).
    Caches results per token."""
    if token_id in _fee_rate_cache:
        return _fee_rate_cache[token_id]
    try:
        resp = requests.get(
            f"{HOST}/fee-rate", params={"token_id": token_id}, timeout=5
        )
        if resp.status_code == 200:
            data = resp.json()
            # API returns fee_rate_bps as string or int
            bps = int(data) if isinstance(data, (int, float)) else int(data.get("fee_rate_bps", 0))
            _fee_rate_cache[token_id] = bps
            return bps
    except Exception as e:
        log(f"  ⚠ Fee rate fetch failed for {token_id[:12]}...: {e}")
    # Fallback: assume crypto fee rate
    return 2500  # 0.25 as bps


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


def compute_pnl_with_fees(entry_price, exit_price, shares, exit_is_maker=False):
    """Compute P&L with real Polymarket fee formula.
    Entry is always taker (market buy). Exit is maker (0%) or taker."""
    entry_cost = shares * entry_price
    exit_revenue = shares * exit_price
    entry_fee = compute_taker_fee(shares, entry_price)
    exit_fee = 0.0 if exit_is_maker else compute_taker_fee(shares, exit_price)
    gross_pnl = exit_revenue - entry_cost
    net_pnl = gross_pnl - entry_fee - exit_fee
    return net_pnl, entry_fee, exit_fee


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
      net buy $ in last BURST_WINDOW seconds >= BURST_THRESHOLD
    If so, fires a callback.
    """

    def __init__(self, on_burst):
        self._lock = threading.Lock()
        # token -> deque of (timestamp, side, cost)
        self._windows = defaultdict(deque)
        self._on_burst = on_burst          # callback(token, net_buy, direction)
        self._cooldowns = {}               # token -> last_burst_time
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
                    WS_URL, close_timeout=5, open_timeout=10
                ) as ws:
                    self._ws_connected = True
                    self._active = set()
                    log("  🔌 WebSocket connected")

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

                        if isinstance(data, dict):
                            if data.get("event_type") == "last_trade_price":
                                self._on_trade(data)
                        elif isinstance(data, list):
                            for item in data:
                                if isinstance(item, dict) and item.get("event_type") == "last_trade_price":
                                    self._on_trade(item)

            except Exception as e:
                self._ws_connected = False
                log(f"  ⚠ WS disconnected: {e}, reconnecting in 2s...")
                await asyncio.sleep(2)

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

        if changed and self._active:
            self._active = set()
            self._force_reconnect = True
            return

        if wanted:
            await ws.send(json.dumps({"type": "market", "assets_ids": list(wanted)}))
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
            window = self._windows[asset_id]
            window.append((now, side, cost))

            # Prune old trades outside the burst window
            cutoff = now - BURST_WINDOW
            while window and window[0][0] < cutoff:
                window.popleft()

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

            # Check per-market cooldown (both UP and DOWN sides)
            info = self._token_info.get(asset_id, {})
            market_q = info.get("market", "")
            if market_q:
                last_market = self._market_cooldowns.get(market_q, 0)
                if now - last_market < MARKET_COOLDOWN:
                    return

            # Check burst threshold — BOTH buying and selling bursts
            burst_direction = None
            burst_amount = 0
            if net >= BURST_THRESHOLD:
                burst_direction = "BUY"   # heavy buying → fade by buying opposite
                burst_amount = net
            elif (-net) >= BURST_THRESHOLD:
                burst_direction = "SELL"  # heavy selling → fade by buying this token
                burst_amount = -net

            if burst_direction:
                self._cooldowns[asset_id] = now
                if market_q:
                    self._market_cooldowns[market_q] = now
                # Clear window after firing (don't re-trigger on same trades)
                window.clear()

        # Fire callback outside lock
        if burst_direction:
            info = self._token_info.get(asset_id, {})
            self._on_burst(asset_id, burst_amount, burst_direction, info)

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


# =========================================================================
# QUICK FLIP — monitor price and exit
# =========================================================================
def quick_flip(token_id, entry_price, shares, pos_info):
    """
    Exit strategy after burst entry:
    1. Immediately place GTC limit sell at entry + PROFIT_TARGET (maker = 0% fee)
    2. Monitor: if stop loss or timeout hit → cancel limit, market sell
    3. If limit fills (shares go to 0) → profit taken at 0% exit fee
    """
    start = time.time()
    side_label = pos_info.get("side", "?")
    market = pos_info.get("market", "?")
    target_price = round(entry_price + PROFIT_TARGET, 2)
    stop_price = entry_price - STOP_LOSS

    # Estimate entry fee for logging (using real fee formula)
    entry_fee_est = compute_taker_fee(shares, entry_price)
    target_net_pnl, _, _ = compute_pnl_with_fees(entry_price, target_price, shares, exit_is_maker=True)

    log(f"  ⏱ Flip monitor started: {side_label} {shares:.0f}sh @ {entry_price:.2f}")
    log(f"    Limit sell: {target_price:.2f} (maker 0% fee) | Stop: {stop_price:.2f} | Timeout: {FLIP_TIMEOUT}s")
    log(f"    Entry fee: ~${entry_fee_est:.2f} | Net P&L if target hit: ~${target_net_pnl:+.2f}")

    # Step 1: Place GTC limit sell at profit target (maker = 0% fee)
    # Retry multiple times since shares take time to settle on-chain
    limit_placed = False
    sell_shares = shares
    if not DRY_RUN:
        for attempt in range(LIMIT_SELL_RETRIES):
            try:
                time.sleep(LIMIT_SELL_RETRY_DELAY)  # Wait for shares to settle
                # Approve allowance for this token before selling
                actual = get_actual_share_balance(token_id)
                if actual is None or actual < 1:
                    log(f"  ⏳ Waiting for shares (attempt {attempt+1}/{LIMIT_SELL_RETRIES}, balance: {actual})")
                    continue
                sell_shares = math.floor(actual * 100) / 100
                if sell_shares < 5:  # Polymarket min order size is 5 shares
                    log(f"  ⚠ Shares {sell_shares:.1f} below min order size 5 — will use market sell")
                    break
                sell_order = OrderArgs(
                    token_id=token_id,
                    price=target_price,
                    size=sell_shares,
                    side=SELL,
                )
                signed_sell = client.create_order(sell_order)
                resp = client.post_order(signed_sell, OrderType.GTC)
                limit_placed = True
                log(f"  ✓ Limit sell placed: {sell_shares:.0f}sh at {target_price:.2f} (GTC, 0% fee)")
                break
            except Exception as e:
                log(f"  ⚠ Limit sell attempt {attempt+1}/{LIMIT_SELL_RETRIES} failed: {e}")
        if not limit_placed:
            log(f"  ⚠ All limit sell attempts failed — will use market sell on exit")
    else:
        limit_placed = True  # Pretend for dry run
        sell_shares = shares

    # Step 2: Monitor for stop loss / timeout / limit fill
    exit_reason = "timeout"
    exit_price = entry_price
    exit_is_maker = False

    while True:
        elapsed = time.time() - start
        current_price = get_price(token_id, side=SELL)

        if current_price <= 0:
            time.sleep(PRICE_CHECK_INTERVAL)
            continue

        # Check if limit order filled (shares dropped to ~0)
        if limit_placed and not DRY_RUN:
            remaining = get_actual_share_balance(token_id)
            if remaining is not None and remaining < 0.5:
                exit_reason = "profit"
                exit_price = target_price  # Filled at our limit price
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
        elif elapsed >= FLIP_TIMEOUT:
            exit_reason = "timeout"
            exit_price = current_price
            break

        time.sleep(PRICE_CHECK_INTERVAL)

    # Step 3: Execute exit
    pnl = 0.0
    net_pnl, entry_fee, exit_fee = compute_pnl_with_fees(
        entry_price, exit_price, shares, exit_is_maker=exit_is_maker
    )

    if DRY_RUN:
        pnl = net_pnl
        fee_str = f"entry fee ${entry_fee:.2f}" + (f" + exit fee ${exit_fee:.2f}" if exit_fee > 0 else " + exit fee $0")
        log(f"  🧪 DRY-RUN flip: {exit_reason} at {exit_price:.2f} | Gross ${(exit_price-entry_price)*shares:+.2f} | Fees: {fee_str} | Net: ${pnl:+.2f}")
    else:
        if exit_reason == "profit" and exit_is_maker:
            # Limit order already filled — no action needed
            pnl = net_pnl
            log(f"  ✓ Limit filled (profit): {target_price:.2f} | Net P&L: ${pnl:+.2f} (0% exit fee)")
        else:
            # Cancel limit order, then market sell
            if limit_placed:
                cancel_all_orders_for_token(token_id)
                log(f"  ↩ Cancelled limit sell (exit: {exit_reason})")

            try:
                actual = get_actual_share_balance(token_id)
                sell_shares = math.floor((actual or shares) * 100) / 100

                if sell_shares > 0:
                    mo = MarketOrderArgs(
                        token_id=token_id,
                        amount=sell_shares,
                        side=SELL,
                        order_type=OrderType.FAK,
                    )
                    signed = client.create_market_order(mo)
                    resp = client.post_order(signed, OrderType.FAK)

                    pnl = net_pnl
                    log(f"  ✓ Market sell ({exit_reason}): {sell_shares:.0f}sh at ~{exit_price:.2f} | Net P&L: ${pnl:+.2f} (fees: ${entry_fee+exit_fee:.2f})")
                else:
                    log(f"  ⚠ No shares to sell (balance: {actual})")
            except Exception as e:
                log(f"  ✗ Flip sell failed: {e}")

        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": f"FLIP_{exit_reason.upper()}",
            "market": market,
            "side": side_label,
            "token": token_id,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "shares": shares,
            "gross_pnl": round((exit_price - entry_price) * shares, 4),
            "entry_fee": round(entry_fee, 4),
            "exit_fee": round(exit_fee, 4),
            "net_pnl": round(pnl, 4),
            "exit_is_maker": exit_is_maker,
            "elapsed": round(time.time() - start, 1),
        }
        with open(TRADE_LOG, "a") as f:
            f.write(json.dumps(record) + "\n")

    return exit_reason, pnl


# =========================================================================
# MAIN BOT
# =========================================================================
def run_burst():
    starting_balance = get_usdc_balance() or 100
    log(f"\n💵 Starting balance: ${starting_balance:.2f}")

    positions = {}        # token_id -> pos_info
    positions_lock = threading.Lock()
    session_pnl = [0.0]   # mutable container for thread access

    def on_burst(token_id, net_volume, direction, info):
        """Called from WS thread when a burst is detected — CONTRARIAN: fade the burst.
        direction='BUY'  → heavy buying on this token → buy OPPOSITE token
        direction='SELL' → heavy selling on this token → buy THIS token (oversold)
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
        burst_side = info.get("side", "UP")  # which side this token represents

        # CONTRARIAN LOGIC: determine which token to buy (the fade token)
        if direction == "BUY":
            # Heavy buying on this token → buy the OPPOSITE token
            if burst_side == "UP":
                fade_token = tokens[1]   # buy DOWN
                fade_side = "DOWN"
            else:
                fade_token = tokens[0]   # buy UP
                fade_side = "UP"
            fade_reason = f"fade {burst_side} buying"
        else:
            # Heavy selling on this token → buy THIS token (oversold bounce)
            fade_token = token_id
            fade_side = burst_side
            fade_reason = f"fade {burst_side} selling"

        # Check burst side price — must be elevated enough to be meaningful
        burst_price = get_price(token_id, side=BUY)
        if direction == "BUY" and burst_price < MIN_BURST_PRICE:
            log(f"  ⚡ Burst on {market} {burst_side}: ${net_volume:.0f} {direction} — burst price {burst_price:.2f} too low to fade")
            return
        if direction == "SELL" and burst_price > (1 - MIN_BURST_PRICE):
            log(f"  ⚡ Burst on {market} {burst_side}: ${net_volume:.0f} {direction} — burst price {burst_price:.2f} too high to fade")
            return

        # Get the FADE token's price (the one we're buying)
        price = get_price(fade_token, side=BUY)
        if price <= 0 or price >= 1:
            return
        if price < MIN_ENTRY_PRICE:
            log(f"  ⚡ Burst on {market} {burst_side}: ${net_volume:.0f} {direction} — fade {fade_side} at {price:.2f} too cheap")
            return
        if price > MAX_ENTRY_PRICE:
            log(f"  ⚡ Burst on {market} {burst_side}: ${net_volume:.0f} {direction} — fade {fade_side} at {price:.2f} too expensive")
            return

        # Check balance
        bal = get_usdc_balance()
        if bal is None or bal < MIN_BALANCE_BUFFER + BUY_AMOUNT:
            return

        amount = min(BUY_AMOUNT, bal - MIN_BALANCE_BUFFER)

        log(f"\n🔄 FADE BURST: {market} — {fade_reason}")
        log(f"  Burst: ${net_volume:.0f} {direction} on {burst_side} (price {burst_price:.2f})")
        est_shares = amount / price
        entry_fee = compute_taker_fee(est_shares, price)
        eff_rate = (entry_fee / amount * 100) if amount > 0 else 0
        log(f"  Buying {fade_side} at {price:.2f}, Bet: ${amount:.0f} (fee ~${entry_fee:.2f}, {eff_rate:.2f}%)")

        if DRY_RUN:
            shares = amount / price
            log(f"  🧪 DRY-RUN: would buy {shares:.0f} shares of {fade_side}")
            record = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "action": "DRY_FADE_BUY",
                "market": market,
                "burst_side": burst_side,
                "fade_side": fade_side,
                "burst_direction": direction,
                "fade_reason": fade_reason,
                "token": fade_token,
                "burst_token": token_id,
                "price": price,
                "burst_price": burst_price,
                "amount": amount,
                "burst_net": net_volume,
            }
            with open(TRADE_LOG, "a") as f:
                f.write(json.dumps(record) + "\n")

            # Track position and start flip monitor
            pos_info = {"market": market, "side": fade_side, "entry_price": price, "shares": shares,
                        "fade_reason": fade_reason, "burst_side": burst_side}
            with positions_lock:
                positions[fade_token] = pos_info

            reason, pnl = quick_flip(fade_token, price, shares, pos_info)
            session_pnl[0] += pnl
            with positions_lock:
                positions.pop(fade_token, None)
            return

        # LIVE: place market buy on the FADE token (contrarian)
        try:
            mo = MarketOrderArgs(
                token_id=fade_token,
                amount=amount,
                side=BUY,
                order_type=OrderType.FAK,
            )
            signed = client.create_market_order(mo)
            resp = client.post_order(signed, OrderType.FAK)

            # Determine actual shares received and real fill price
            actual_shares = 0
            if isinstance(resp, dict):
                taking = resp.get("takingAmount", "0")
                actual_shares = float(taking) if taking else 0
            # Fallback: check on-chain balance
            if actual_shares == 0:
                time.sleep(1)
                chain_bal = get_actual_share_balance(fade_token)
                if chain_bal and chain_bal > 0.5:
                    actual_shares = chain_bal
                else:
                    actual_shares = amount / price  # last resort estimate

            # Compute REAL entry price from amount spent / shares received
            # This accounts for slippage — the actual cost basis
            real_entry_price = amount / actual_shares if actual_shares > 0 else price
            real_entry_price = round(real_entry_price, 4)

            if abs(real_entry_price - price) > 0.02:
                log(f"  ⚠ Slippage: asked {price:.2f}, got {real_entry_price:.2f} ({actual_shares:.1f}sh for ${amount})")

            log(f"  ✓ Bought {actual_shares:.0f} shares of {fade_side} at {real_entry_price:.2f} (ask was {price:.2f}) [{fade_reason}]")

            record = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "action": "FADE_BUY",
                "market": market,
                "burst_side": burst_side,
                "fade_side": fade_side,
                "burst_direction": direction,
                "fade_reason": fade_reason,
                "token": fade_token,
                "burst_token": token_id,
                "ask_price": price,
                "fill_price": real_entry_price,
                "burst_price": burst_price,
                "amount": amount,
                "shares": actual_shares,
                "burst_net": net_volume,
                "response": str(resp),
            }
            with open(TRADE_LOG, "a") as f:
                f.write(json.dumps(record) + "\n")

            # Track and start flip in this thread (WS thread)
            # Use a separate thread so WS keeps receiving
            # IMPORTANT: use real_entry_price not the ask price for P&L
            pos_info = {"market": market, "side": fade_side, "entry_price": real_entry_price, "shares": actual_shares,
                        "fade_reason": fade_reason, "burst_side": burst_side}
            with positions_lock:
                positions[fade_token] = pos_info

            def do_flip():
                reason, pnl = quick_flip(fade_token, real_entry_price, actual_shares, pos_info)
                session_pnl[0] += pnl
                with positions_lock:
                    positions.pop(fade_token, None)

            threading.Thread(target=do_flip, daemon=True).start()

        except Exception as e:
            log(f"  ✗ Fade buy failed: {e}")

    # Create burst detector
    detector = BurstDetector(on_burst=on_burst)
    detector.start()

    log(f"\n{'='*60}")
    if DRY_RUN:
        log(f"🧪 DRY-RUN MODE — no orders will be placed")
    log(f"🔄 CONTRARIAN burst bot (fade-the-burst)")
    log(f"Detection: {BURST_WINDOW}s window, ${BURST_THRESHOLD} threshold (buy + sell bursts)")
    log(f"Fade logic: buy bursts → buy opposite side | sell bursts → buy same side")
    log(f"Quick flip: +{PROFIT_TARGET} profit (limit GTC, 0% fee) / -{STOP_LOSS} stop / {FLIP_TIMEOUT}s timeout")
    log(f"Fade price: {MIN_ENTRY_PRICE}-{MAX_ENTRY_PRICE}, Burst min: {MIN_BURST_PRICE}")
    log(f"Cooldowns: {COOLDOWN_AFTER_ENTRY}s/token, {MARKET_COOLDOWN}s/market, Limit retries: {LIMIT_SELL_RETRIES}")
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

            # Status
            bal = get_usdc_balance()
            now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
            ws_str = "🟢" if detector.connected else "🔴"
            bal_str = f"${bal:.2f}" if bal else "?"
            dry_str = " 🧪 DRY" if DRY_RUN else ""

            with positions_lock:
                n_pos = len(positions)

            print(f"\r⏰ {now_str} | {n_pos} pos | bal {bal_str} | "
                  f"session ${session_pnl[0]:+.2f} | "
                  f"WS {ws_str} ({detector.event_count} events, {detector.subscribed_count} subs)"
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
