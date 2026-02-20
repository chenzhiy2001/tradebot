#!/usr/bin/env python3
"""
Burst Bot: Ultra-fast momentum scalping on Polymarket crypto markets.

Strategy:
  Monitor real-time WebSocket trade feed for sudden bursts of buying.
  When a burst is detected (heavy volume in 5 seconds), buy immediately
  and flip within 5-10 seconds for a quick profit as price adjusts.

Detection:
  - Sliding 5-second window of trades per token
  - Burst = net buy volume exceeds BURST_THRESHOLD in the window
  - Must be one-sided (ratio check optional at this speed)

Exit (quick flip):
  - If price rises above entry + PROFIT_TARGET → market sell (profit)
  - If price drops below entry - STOP_LOSS → market sell (cut loss)
  - If FLIP_TIMEOUT seconds pass → market sell (take whatever)
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
BURST_THRESHOLD = 200        # $ net buy in the window to trigger
MAX_ENTRY_PRICE = 0.60       # Don't enter if price already above this

# Quick flip exit
PROFIT_TARGET = 0.05         # Sell if price rises this much above entry
STOP_LOSS = 0.10             # Sell if price drops this much below entry
FLIP_TIMEOUT = 15            # Max seconds to hold before force-selling
PRICE_CHECK_INTERVAL = 1.0   # How often to check price during flip

# Risk management
MAX_CONCURRENT_POSITIONS = 2
MIN_BALANCE_BUFFER = 5
SESSION_STOP_LOSS_PCT = 0.30
COOLDOWN_AFTER_ENTRY = 5.0   # Don't re-enter same market for N seconds

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

            # Check cooldown
            last_burst = self._cooldowns.get(asset_id, 0)
            if now - last_burst < COOLDOWN_AFTER_ENTRY:
                return

            # Check burst threshold
            if net >= BURST_THRESHOLD:
                self._cooldowns[asset_id] = now
                # Clear window after firing (don't re-trigger on same trades)
                window.clear()

        # Fire callback outside lock
        if net >= BURST_THRESHOLD:
            info = self._token_info.get(asset_id, {})
            self._on_burst(asset_id, net, "BUY", info)

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
    Monitor price after burst entry. Exit when:
    - Price >= entry + PROFIT_TARGET  → take profit
    - Price <= entry - STOP_LOSS      → cut loss
    - Elapsed >= FLIP_TIMEOUT         → force exit
    """
    start = time.time()
    side_label = pos_info.get("side", "?")
    market = pos_info.get("market", "?")

    log(f"  ⏱ Flip monitor started: {side_label} {shares:.0f}sh @ {entry_price:.2f}")
    log(f"    Target: {entry_price + PROFIT_TARGET:.2f} | Stop: {entry_price - STOP_LOSS:.2f} | Timeout: {FLIP_TIMEOUT}s")

    exit_reason = "timeout"
    exit_price = entry_price

    while True:
        elapsed = time.time() - start
        current_price = get_price(token_id, side=SELL)

        if current_price <= 0:
            time.sleep(PRICE_CHECK_INTERVAL)
            continue

        if current_price >= entry_price + PROFIT_TARGET:
            exit_reason = "profit"
            exit_price = current_price
            break
        elif current_price <= entry_price - STOP_LOSS:
            exit_reason = "stop_loss"
            exit_price = current_price
            break
        elif elapsed >= FLIP_TIMEOUT:
            exit_reason = "timeout"
            exit_price = current_price
            break

        time.sleep(PRICE_CHECK_INTERVAL)

    # Execute the sell
    pnl = 0.0
    if DRY_RUN:
        pnl = (exit_price - entry_price) * shares
        log(f"  🧪 DRY-RUN flip: {exit_reason} at {exit_price:.2f} (P&L: ${pnl:+.2f})")
    else:
        try:
            # Get actual share balance
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

                pnl = (exit_price - entry_price) * sell_shares
                log(f"  ✓ Flip {exit_reason}: sold {sell_shares:.0f}sh at ~{exit_price:.2f} (P&L: ${pnl:+.2f})")

                record = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "action": f"FLIP_{exit_reason.upper()}",
                    "market": market,
                    "side": side_label,
                    "token": token_id,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "shares": sell_shares,
                    "pnl": pnl,
                    "elapsed": round(time.time() - start, 1),
                    "response": str(resp),
                }
                with open(TRADE_LOG, "a") as f:
                    f.write(json.dumps(record) + "\n")
            else:
                log(f"  ⚠ No shares to sell (balance: {actual})")
        except Exception as e:
            log(f"  ✗ Flip sell failed: {e}")

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

    def on_burst(token_id, net_buy, direction, info):
        """Called from WS thread when a burst is detected."""
        if not info:
            return

        with positions_lock:
            if len(positions) >= MAX_CONCURRENT_POSITIONS:
                return
            # Don't double-enter same market
            market_tokens = info.get("tokens", [])
            for t in market_tokens:
                if t in positions:
                    return

        market = info.get("market", "?")
        side = info.get("side", "UP")  # which side this token represents

        # Get current price
        price = get_price(token_id, side=BUY)
        if price <= 0 or price >= 1 or price > MAX_ENTRY_PRICE:
            log(f"  ⚡ Burst on {market} {side}: ${net_buy:.0f} in {BURST_WINDOW}s — price {price:.2f} too high")
            return

        # Check balance
        bal = get_usdc_balance()
        if bal is None or bal < MIN_BALANCE_BUFFER + BUY_AMOUNT:
            return

        amount = min(BUY_AMOUNT, bal - MIN_BALANCE_BUFFER)

        log(f"\n⚡ BURST DETECTED: {market} {side}")
        log(f"  Net buy: ${net_buy:.0f} in {BURST_WINDOW}s, Price: {price:.2f}, Bet: ${amount:.0f}")

        if DRY_RUN:
            shares = amount / price
            log(f"  🧪 DRY-RUN: would buy {shares:.0f} shares")
            record = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "action": "DRY_BURST_BUY",
                "market": market,
                "side": side,
                "token": token_id,
                "price": price,
                "amount": amount,
                "burst_net": net_buy,
            }
            with open(TRADE_LOG, "a") as f:
                f.write(json.dumps(record) + "\n")

            # Track position and start flip monitor
            pos_info = {"market": market, "side": side, "entry_price": price, "shares": shares}
            with positions_lock:
                positions[token_id] = pos_info

            reason, pnl = quick_flip(token_id, price, shares, pos_info)
            session_pnl[0] += pnl
            with positions_lock:
                positions.pop(token_id, None)
            return

        # LIVE: place market buy
        try:
            mo = MarketOrderArgs(
                token_id=token_id,
                amount=amount,
                side=BUY,
                order_type=OrderType.FAK,
            )
            signed = client.create_market_order(mo)
            resp = client.post_order(signed, OrderType.FAK)

            actual_shares = 0
            if isinstance(resp, dict):
                taking = resp.get("takingAmount", "0")
                actual_shares = float(taking) if taking else 0
            if actual_shares == 0:
                actual_shares = amount / price

            log(f"  ✓ Bought {actual_shares:.0f} shares of {side} at {price:.2f}")

            record = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "action": "BURST_BUY",
                "market": market,
                "side": side,
                "token": token_id,
                "price": price,
                "amount": amount,
                "shares": actual_shares,
                "burst_net": net_buy,
                "response": str(resp),
            }
            with open(TRADE_LOG, "a") as f:
                f.write(json.dumps(record) + "\n")

            # Track and start flip in this thread (WS thread)
            # Use a separate thread so WS keeps receiving
            pos_info = {"market": market, "side": side, "entry_price": price, "shares": actual_shares}
            with positions_lock:
                positions[token_id] = pos_info

            def do_flip():
                reason, pnl = quick_flip(token_id, price, actual_shares, pos_info)
                session_pnl[0] += pnl
                with positions_lock:
                    positions.pop(token_id, None)

            threading.Thread(target=do_flip, daemon=True).start()

        except Exception as e:
            log(f"  ✗ Burst buy failed: {e}")

    # Create burst detector
    detector = BurstDetector(on_burst=on_burst)
    detector.start()

    log(f"\n{'='*60}")
    if DRY_RUN:
        log(f"🧪 DRY-RUN MODE — no orders will be placed")
    log(f"Burst bot started (5s window, ${BURST_THRESHOLD} threshold)")
    log(f"Quick flip: +{PROFIT_TARGET} profit / -{STOP_LOSS} stop / {FLIP_TIMEOUT}s timeout")
    log(f"Max entry: {MAX_ENTRY_PRICE}, Max positions: {MAX_CONCURRENT_POSITIONS}")
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
