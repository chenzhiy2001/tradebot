#!/usr/bin/env python3
"""
Follower Bot — BTC-leads-ETH momentum trading on Polymarket.

Strategy:
  BTC and ETH crypto up/down markets share the same 5m/15m windows.
  BTC prices move first; ETH follows with a lag.

  1. Stream real-time Polymarket order-book data for both BTC and ETH tokens.
  2. Track the BTC token mid-price in a rolling window.
  3. When BTC's UP (or DOWN) token surges by ≥ SPIKE_THRESHOLD in ≤ SPIKE_WINDOW
     seconds, immediately buy the SAME side of ETH at the ask.
  4. After entry, monitor BTC momentum:
     - If BTC token price stalls or reverses (drops from peak by ≥ EXIT_REVERT),
       sell the ETH position at the bid (take profit / cut loss).
     - Hard stop-loss if ETH price drops by ≥ STOP_LOSS from entry.
     - Hard take-profit if ETH price rises by ≥ TAKE_PROFIT from entry.
     - Time stop: sell after MAX_HOLD_SECS regardless.
  5. Only trade within the first MAX_ENTRY_PCT of each window (need room to exit).

Exits:
  Sell at best_bid (market sell) — NOT at $0.99 like sniper.py.
  This is intra-window momentum, not hold-to-resolution.

Pairs: BTC leads → ETH follows (same window, same side).
"""

import os
import sys
import time
import json
import threading
import asyncio
import requests
from datetime import datetime, timezone, timedelta
from collections import deque
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
CHAIN_ID = 137
FUNDER_ADDRESS = founder_address

# =========================================================================
# STRATEGY PARAMETERS
# =========================================================================
BET_AMOUNT = 5                # USDC per trade
MIN_BET = 5                   # Polymarket minimum
MAX_BET = 20                  # Cap per trade
MAX_EXPOSURE_PCT = 0.30       # Max 30% of balance at risk
FILL_WAIT = 3                 # Seconds to wait for buy fill
MIN_ORDER_SIZE = 5            # Polymarket minimum order size in shares

# Spike detection — BTC token price must rise this much this fast
SPIKE_THRESHOLD = 0.10        # BTC token mid-price jump ≥ 4¢ (e.g. 0.50 → 0.54)
SPIKE_WINDOW = 15             # … within 15 seconds
SPIKE_COOLDOWN = 30           # Don't re-enter same side for 30s after a trade

# Exit conditions
EXIT_REVERT = 0.02            # Sell ETH when BTC token drops 2¢ from its peak post-entry
STOP_LOSS = 0.04              # Sell ETH if its price drops 4¢ from entry
TAKE_PROFIT = 0.06            # Sell ETH if its price rises 6¢ from entry
MAX_HOLD_SECS = 120           # Hard time stop: sell after 2 minutes regardless

# Window timing
MAX_ENTRY_PCT = 0.60          # Only enter in first 60% of window (need exit room)
INTERVALS = [5]               # Only 5-minute windows (faster signal)

# Polymarket fee formula (5m/15m crypto)
CRYPTO_FEE_RATE = 0.25
CRYPTO_FEE_EXPONENT = 2

# Data files
LOG_FILE = "follower_log.txt"
TRADE_LOG = "follower_trades.json"

DRY_RUN = "--dry-run" in sys.argv
TICK_INTERVAL = 0.25          # 250ms ticks for fast reaction


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
    if price <= 0 or price >= 1:
        return 0.0
    fee_shares = shares * CRYPTO_FEE_RATE * (price * (1 - price)) ** CRYPTO_FEE_EXPONENT
    return round(fee_shares * price, 4)


# =========================================================================
# POLYMARKET WEBSOCKET FEED
# =========================================================================
class PolymarketFeed:
    """Real-time Polymarket book data via WebSocket."""

    def __init__(self):
        self._prices = {}     # token_id -> {best_bid, best_ask, ts}
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
# MARKET DISCOVERY
# =========================================================================
def discover_markets():
    """Fetch current active BTC and ETH 5m markets from Polymarket."""
    result = {}  # epoch -> {btc_up, btc_down, eth_up, eth_down, start, end, interval}
    now = datetime.now(timezone.utc)

    for interval in INTERVALS:
        aligned = (now.minute // interval) * interval
        window_start = now.replace(minute=aligned, second=0, microsecond=0)
        epoch = int(window_start.timestamp())

        for crypto in ["btc", "eth"]:
            slug = f"{crypto}-updown-{interval}m-{epoch}"
            try:
                resp = requests.get(f"{GAMMA_API}/events/slug/{slug}", timeout=10)
                if resp.status_code != 200:
                    continue
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

                    if epoch not in result:
                        result[epoch] = {
                            "start": window_start,
                            "end": window_start + timedelta(minutes=interval),
                            "interval": interval,
                        }
                    result[epoch][f"{crypto}_up"] = tokens[up_idx]
                    result[epoch][f"{crypto}_down"] = tokens[down_idx]
            except Exception:
                pass

    # Only return windows where we have BOTH btc and eth tokens
    complete = {}
    for epoch, info in result.items():
        if all(k in info for k in ["btc_up", "btc_down", "eth_up", "eth_down"]):
            if now < info["end"]:  # not expired
                complete[epoch] = info
    return complete


# =========================================================================
# SPIKE DETECTOR — watches BTC token mid-prices for sudden moves
# =========================================================================
class SpikeDetector:
    """Track BTC UP and DOWN token mid-prices, detect spikes."""

    def __init__(self):
        # Rolling price history: deque of (timestamp, mid_price)
        self._history_up = deque(maxlen=200)
        self._history_down = deque(maxlen=200)

    def update(self, side, mid_price):
        """Record a new mid-price tick for BTC UP or DOWN token."""
        now = time.time()
        if side == "UP":
            self._history_up.append((now, mid_price))
        else:
            self._history_down.append((now, mid_price))

    def check_spike(self, side):
        """Check if the given BTC side spiked recently.

        Returns (is_spike, current_price, price_change) or (False, None, 0).
        A spike = price increased by ≥ SPIKE_THRESHOLD in the last SPIKE_WINDOW seconds.
        """
        history = self._history_up if side == "UP" else self._history_down
        if len(history) < 2:
            return False, None, 0

        now = time.time()
        current_ts, current_price = history[-1]

        # Only consider fresh data
        if now - current_ts > 2.0:
            return False, current_price, 0

        # Find the oldest price within SPIKE_WINDOW
        oldest_price = None
        for ts, price in history:
            if now - ts <= SPIKE_WINDOW:
                oldest_price = price
                break

        if oldest_price is None:
            return False, current_price, 0

        change = current_price - oldest_price
        is_spike = change >= SPIKE_THRESHOLD

        return is_spike, current_price, change

    def get_current(self, side):
        """Get latest BTC mid-price for a side."""
        history = self._history_up if side == "UP" else self._history_down
        if not history:
            return None
        ts, price = history[-1]
        if time.time() - ts > 5.0:
            return None
        return price

    def get_peak_since(self, side, since_ts):
        """Get peak BTC mid-price for a side since a given timestamp."""
        history = self._history_up if side == "UP" else self._history_down
        peak = None
        for ts, price in history:
            if ts >= since_ts:
                if peak is None or price > peak:
                    peak = price
        return peak


# =========================================================================
# FOLLOWER — main trading engine
# =========================================================================
class Follower:
    """Monitor BTC spikes, trade ETH tokens."""

    def __init__(self, poly):
        self.poly = poly
        self.detector = SpikeDetector()
        self._windows = {}     # epoch -> window info from discover_markets
        self._position = None  # current open position (only 1 at a time)
        self._cooldowns = {}   # side -> timestamp of last trade close
        self._start_balance = get_usdc_balance()
        self._trade_count = 0
        self._win_count = 0
        self._total_pnl = 0.0
        self._lock = threading.Lock()

    def update_markets(self, markets):
        """Update tracked windows and subscribe to all tokens."""
        self._windows = markets
        all_tokens = []
        for epoch, info in markets.items():
            for k in ["btc_up", "btc_down", "eth_up", "eth_down"]:
                if k in info:
                    all_tokens.append(info[k])
        if all_tokens:
            self.poly.subscribe(all_tokens)

    def tick(self):
        """Main tick: update BTC prices, detect spikes, manage positions."""
        now = time.time()
        now_utc = datetime.now(timezone.utc)

        # ─── Update BTC price tracking ───
        for epoch, info in list(self._windows.items()):
            # Skip expired windows
            if now_utc >= info["end"]:
                continue

            for side in ["UP", "DOWN"]:
                btc_token = info.get(f"btc_{side.lower()}")
                if btc_token:
                    mid = self.poly.mid_price(btc_token)
                    if mid is not None:
                        self.detector.update(side, mid)

        # ─── Manage open position ───
        if self._position is not None:
            self._manage_position()
            return  # Don't open new positions while one is active

        # ─── Scan for BTC spikes → enter ETH ───
        for epoch, info in list(self._windows.items()):
            if now_utc >= info["end"]:
                continue

            elapsed = (now_utc - info["start"]).total_seconds()
            duration = (info["end"] - info["start"]).total_seconds()
            pct = elapsed / duration if duration > 0 else 1.0

            if pct > MAX_ENTRY_PCT:
                continue  # Too late in window

            for side in ["UP", "DOWN"]:
                # Check cooldown
                cd = self._cooldowns.get(side, 0)
                if now - cd < SPIKE_COOLDOWN:
                    continue

                is_spike, btc_price, change = self.detector.check_spike(side)
                if not is_spike:
                    continue

                # BTC spike detected! Check ETH token
                eth_token = info.get(f"eth_{side.lower()}")
                if not eth_token:
                    continue

                eth_bid, eth_ask, eth_age = self.poly.get_price(eth_token)
                if not eth_ask or eth_ask <= 0 or (eth_age and eth_age > 5):
                    continue

                # Check balance
                balance = get_usdc_balance()
                if balance is None:
                    continue
                bet = min(BET_AMOUNT, balance * MAX_EXPOSURE_PCT)
                if bet < MIN_BET:
                    continue

                log(f"  ⚡ BTC {side} SPIKE: +{change:.3f} in {SPIKE_WINDOW}s "
                    f"(btc_mid={btc_price:.3f})")
                log(f"  🎯 Buying ETH {side} @ {eth_ask:.2f} | bet=${bet:.0f} | "
                    f"window {pct:.0%} elapsed")

                success = self._buy_eth(
                    eth_token, side, eth_ask, bet, epoch, info
                )
                if success:
                    return  # Only one position at a time

    def _buy_eth(self, token_id, side, buy_price, bet, epoch, window_info):
        """Place a GTC limit buy on ETH token at the ask. Returns True if filled."""
        buy_price = round(buy_price, 2)
        est_shares = bet / buy_price

        if est_shares < MIN_ORDER_SIZE:
            est_shares = MIN_ORDER_SIZE

        fee_est = compute_taker_fee(est_shares, buy_price)

        log(f"  💰 Buying ETH {side}: ~{est_shares:.0f}sh @ {buy_price:.2f} "
            f"(${bet:.0f} + ~${fee_est:.2f} fee)")

        if DRY_RUN:
            log(f"  🧪 DRY RUN — skipping execution")
            self._position = {
                "token_id": token_id,
                "side": side,
                "entry_price": buy_price,
                "shares": est_shares,
                "cost": bet + fee_est,
                "entry_time": time.time(),
                "epoch": epoch,
                "btc_token": window_info.get(f"btc_{side.lower()}"),
                "btc_peak": self.detector.get_current(side) or 0,
                "dry_run": True,
            }
            return True

        try:
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
            time.sleep(FILL_WAIT)

            actual = get_share_balance(token_id)

            # Always cancel buy to prevent additional fills
            try:
                if order_id:
                    client.cancel(order_id)
            except Exception:
                pass

            if actual is not None and actual >= MIN_ORDER_SIZE:
                fee = compute_taker_fee(actual, buy_price)
                actual_cost = round(actual * buy_price + fee, 4)
                log(f"  ✅ Filled! {actual:.1f}sh, cost ${actual_cost:.2f} (fee ${fee:.2f})")

                btc_current = self.detector.get_current(side)
                self._position = {
                    "token_id": token_id,
                    "side": side,
                    "entry_price": buy_price,
                    "shares": actual,
                    "cost": actual_cost,
                    "entry_time": time.time(),
                    "epoch": epoch,
                    "btc_token": window_info.get(f"btc_{side.lower()}"),
                    "btc_peak": btc_current or buy_price,
                    "order_id": order_id,
                    "dry_run": False,
                }
                return True
            else:
                log(f"  ⏳ Not filled after {FILL_WAIT}s — cancelled")
                return False

        except Exception as e:
            log(f"  ⚠ Buy error: {e}")
            return False

    def _manage_position(self):
        """Check exit conditions for the open ETH position."""
        pos = self._position
        if pos is None:
            return

        now = time.time()
        hold_secs = now - pos["entry_time"]
        side = pos["side"]

        # Get current ETH price
        eth_bid, eth_ask, eth_age = self.poly.get_price(pos["token_id"])
        if not eth_bid or eth_bid <= 0:
            # No price data — keep holding unless time expired
            if hold_secs > MAX_HOLD_SECS:
                log(f"  ⏰ TIME STOP: held {hold_secs:.0f}s, no ETH price — forcing exit")
                self._sell_eth("time_stop_no_price")
            return

        eth_mid = (eth_bid + (eth_ask or eth_bid)) / 2.0
        price_change = eth_mid - pos["entry_price"]

        # Update BTC peak
        btc_current = self.detector.get_current(side)
        if btc_current is not None and btc_current > pos["btc_peak"]:
            pos["btc_peak"] = btc_current

        # ─── Exit checks (in priority order) ───

        # 1. Stop loss
        if price_change <= -STOP_LOSS:
            log(f"  🛑 STOP LOSS: ETH {side} dropped {price_change:+.3f} from entry "
                f"(mid={eth_mid:.3f}, entry={pos['entry_price']:.2f})")
            self._sell_eth("stop_loss")
            return

        # 2. Take profit
        if price_change >= TAKE_PROFIT:
            log(f"  🎉 TAKE PROFIT: ETH {side} gained {price_change:+.3f} "
                f"(mid={eth_mid:.3f}, entry={pos['entry_price']:.2f})")
            self._sell_eth("take_profit")
            return

        # 3. BTC momentum reversal — BTC token dropped from peak
        if btc_current is not None:
            btc_drop = pos["btc_peak"] - btc_current
            if btc_drop >= EXIT_REVERT:
                log(f"  📉 BTC REVERT: {side} dropped {btc_drop:.3f} from peak "
                    f"(peak={pos['btc_peak']:.3f}, now={btc_current:.3f})")
                self._sell_eth("btc_revert")
                return

        # 4. Time stop
        if hold_secs > MAX_HOLD_SECS:
            log(f"  ⏰ TIME STOP: held {hold_secs:.0f}s (ETH change={price_change:+.3f})")
            self._sell_eth("time_stop")
            return

    def _sell_eth(self, reason):
        """Sell ETH position at best bid (market sell)."""
        pos = self._position
        if pos is None:
            return

        token_id = pos["token_id"]
        shares = pos["shares"]
        side = pos["side"]

        # Get current best bid for market sell
        eth_bid, _, _ = self.poly.get_price(token_id)
        sell_price = round(eth_bid, 2) if eth_bid and eth_bid > 0 else 0.01

        log(f"  📤 Selling ETH {side}: {shares:.1f}sh @ {sell_price:.2f} ({reason})")

        if pos.get("dry_run"):
            fee = compute_taker_fee(shares, sell_price)
            revenue = shares * sell_price - fee
            pnl = revenue - pos["cost"]
            log(f"  🧪 DRY RUN — PnL ${pnl:+.2f}")
            self._record_trade(pos, sell_price, pnl, reason)
            self._position = None
            self._cooldowns[side] = time.time()
            return

        try:
            sell_order = OrderArgs(
                token_id=token_id,
                price=sell_price,
                size=shares,
                side=SELL,
            )
            signed = client.create_order(sell_order)
            resp = client.post_order(signed, OrderType.GTC)

            order_id = resp.get("orderID", "") if isinstance(resp, dict) else ""
            status = resp.get("status", "") if isinstance(resp, dict) else ""

            if order_id and status != "error":
                log(f"  📝 Sell order placed (id: {order_id[:8]})")
            else:
                log(f"  ⚠ Sell order failed: {resp}")

            # Wait briefly for fill
            time.sleep(2)

            # Check remaining shares
            remaining = get_share_balance(token_id)
            sold = shares - (remaining or 0)

            if sold > 0:
                fee = compute_taker_fee(sold, sell_price)
                revenue = sold * sell_price - fee
                pnl = revenue - pos["cost"]
                log(f"  💰 Sold {sold:.1f}sh @ {sell_price:.2f} "
                    f"→ PnL ${pnl:+.2f} ({reason})")
                self._record_trade(pos, sell_price, pnl, reason)
            else:
                log(f"  ⚠ Sell didn't fill — trying lower price")
                # Retry at a lower price (1¢ below bid)
                retry_price = round(max(0.01, sell_price - 0.01), 2)
                try:
                    sell_order2 = OrderArgs(
                        token_id=token_id,
                        price=retry_price,
                        size=shares,
                        side=SELL,
                    )
                    signed2 = client.create_order(sell_order2)
                    resp2 = client.post_order(signed2, OrderType.GTC)
                    order_id2 = resp2.get("orderID", "") if isinstance(resp2, dict) else ""
                    log(f"  📝 Retry sell @ {retry_price} (id: {order_id2[:8] if order_id2 else '?'})")

                    time.sleep(3)
                    remaining2 = get_share_balance(token_id)
                    sold2 = shares - (remaining2 or 0)
                    fee2 = compute_taker_fee(sold2, retry_price) if sold2 > 0 else 0
                    revenue2 = sold2 * retry_price - fee2 if sold2 > 0 else 0
                    pnl2 = revenue2 - pos["cost"]
                    if sold2 > 0:
                        log(f"  💰 Retry sold {sold2:.1f}sh @ {retry_price} → PnL ${pnl2:+.2f}")
                    else:
                        pnl2 = -pos["cost"]
                        log(f"  ⚠ Still not sold — shares stuck. PnL ≈ ${pnl2:+.2f}")
                    self._record_trade(pos, retry_price, pnl2, reason)
                except Exception as e:
                    log(f"  ⚠ Retry sell error: {e}")
                    self._record_trade(pos, sell_price, -pos["cost"], reason + "_stuck")

            # Cancel any remaining open orders for this token
            try:
                open_orders = client.get_orders(
                    OpenOrderParams(market=token_id)
                )
                if open_orders:
                    for order in open_orders:
                        oid = order.get("id", "")
                        if oid:
                            try:
                                client.cancel(oid)
                            except Exception:
                                pass
            except Exception:
                pass

        except Exception as e:
            log(f"  ⚠ Sell error: {e}")
            self._record_trade(pos, 0, -pos["cost"], reason + "_error")

        self._position = None
        self._cooldowns[side] = time.time()

    def _record_trade(self, pos, sell_price, pnl, reason):
        """Record completed trade to log file."""
        self._trade_count += 1
        self._total_pnl += pnl
        if pnl > 0:
            self._win_count += 1

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
            "epoch": pos["epoch"],
            "dry_run": pos.get("dry_run", False),
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
        log(f"  📊 Trade #{self._trade_count}: {result} ${pnl:+.2f} | "
            f"session {self._win_count}/{self._trade_count} | "
            f"cumulative ${self._total_pnl:+.2f}")

    def get_status(self):
        """One-line status for dashboard."""
        if self._position:
            pos = self._position
            hold = time.time() - pos["entry_time"]
            eth_bid, _, _ = self.poly.get_price(pos["token_id"])
            eth_mid = eth_bid or 0
            change = eth_mid - pos["entry_price"]
            btc_now = self.detector.get_current(pos["side"])
            btc_drop = (pos["btc_peak"] - btc_now) if btc_now else 0
            return (f"HOLDING ETH {pos['side']} | entry={pos['entry_price']:.2f} "
                    f"now={eth_mid:.3f} Δ{change:+.3f} | "
                    f"BTC peak={pos['btc_peak']:.3f} drop={btc_drop:.3f} | "
                    f"hold={hold:.0f}s/{MAX_HOLD_SECS}s")
        return "SCANNING for BTC spikes..."


# =========================================================================
# MAIN
# =========================================================================
def main():
    mode = "DRY" if DRY_RUN else "LIVE"
    log(f"═══ Follower Bot ═══ [{mode}]")
    log(f"Strategy: BTC spike → buy ETH same side → sell on BTC revert")
    log(f"Params: BET=${BET_AMOUNT} SPIKE={SPIKE_THRESHOLD:.2f}/{SPIKE_WINDOW}s "
        f"EXIT_REVERT={EXIT_REVERT} SL={STOP_LOSS} TP={TAKE_PROFIT}")
    log(f"Windows: {INTERVALS}m | Max hold: {MAX_HOLD_SECS}s")
    log("")

    poly = PolymarketFeed()
    poly.start()

    follower = Follower(poly)

    # Wait for WS
    log("Waiting for Polymarket WS...")
    for _ in range(30):
        if poly.connected:
            break
        time.sleep(1)

    if not poly.connected:
        log("⚠ Polymarket WS not connected after 30s")

    balance = get_usdc_balance()
    if balance:
        log(f"Starting balance: ${balance:.2f}")
    log("")

    last_discovery = 0
    discovery_interval = 15  # Check for new windows every 15s

    while True:
        try:
            now = time.time()

            # Discover markets
            if now - last_discovery > discovery_interval:
                markets = discover_markets()
                if markets:
                    follower.update_markets(markets)
                last_discovery = now

            # Main tick
            follower.tick()

            # Dashboard
            now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
            pm_status = "🟢" if poly.connected else "🔴"
            status = follower.get_status()

            # Show BTC prices
            btc_up = follower.detector.get_current("UP")
            btc_down = follower.detector.get_current("DOWN")
            btc_str = ""
            if btc_up:
                btc_str += f"BTC_UP={btc_up:.3f} "
            if btc_down:
                btc_str += f"BTC_DOWN={btc_down:.3f}"

            print(f"\033[2J\033[H", end="")
            print(f"═══ Follower Bot [{mode}] ═══  {now_str} UTC  │  Polymarket {pm_status}")
            print(f"{'─' * 80}")
            print(f"  {btc_str}")
            print(f"  {status}")
            print(f"{'─' * 80}")
            print(f"  Trades: {follower._trade_count} | "
                  f"Wins: {follower._win_count} | "
                  f"PnL: ${follower._total_pnl:+.2f}")
            print(f"  Log: {LOG_FILE} | Trades: {TRADE_LOG}")
            print(f"  Press Ctrl+C to stop")

            time.sleep(TICK_INTERVAL)

        except KeyboardInterrupt:
            log(f"\nStopping...")
            # If we have an open position, try to close it
            if follower._position and not follower._position.get("dry_run"):
                log("  Closing open position...")
                follower._sell_eth("shutdown")
            # Cancel all open orders
            try:
                open_orders = client.get_orders()
                if open_orders:
                    for order in open_orders:
                        oid = order.get("id", "")
                        if oid:
                            try:
                                client.cancel(oid)
                            except Exception:
                                pass
            except Exception:
                pass
            bal = get_usdc_balance()
            if bal and follower._start_balance:
                log(f"Final balance: ${bal:.2f} (Δ{bal - follower._start_balance:+.2f})")
            log(f"Trades: {follower._trade_count} | Wins: {follower._win_count} | "
                f"PnL: ${follower._total_pnl:+.2f}")
            break
        except Exception as e:
            log(f"⚠ Main loop error: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5)


if __name__ == "__main__":
    main()
