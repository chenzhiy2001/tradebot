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
import queue
import threading
import asyncio
import requests
from datetime import datetime, timezone, timedelta
from collections import deque
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    MarketOrderArgs, OrderType,
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
BET_AMOUNT = 10               # USDC per trade
MIN_BET = 5                   # Polymarket minimum
MAX_BET = 30                  # Cap per trade
MAX_EXPOSURE_PCT = 0.30       # Max 30% of balance at risk
MIN_ORDER_SIZE = 5            # Polymarket minimum order size in shares

# Spike detection — BTC token price must rise this much this fast
SPIKE_THRESHOLD = 0.10        # BTC token mid-price jump ≥ 10¢
SPIKE_WINDOW = 5              # … within 5 seconds
SPIKE_COOLDOWN = 0            # Cooldown between same-side trades (0 = no limit)
MAX_SPIKES_PER_WINDOW = 1000  # Max spike entries per 5m window

# Exit conditions
EXIT_REVERT = 0.10            # Sell ETH when BTC token drops 10¢ from its peak post-entry
STOP_LOSS = 0.04              # Sell ETH if its price drops 4¢ from entry MID (not ask)
TAKE_PROFIT = 0.99            # Sell ETH if its price rises 99¢
MAX_HOLD_SECS = 120           # Hard time stop: sell after 2 minutes regardless
ENTRY_GRACE_SECS = 3          # Don't check stop-loss for first 3s (let position settle)

# Window timing
MAX_ENTRY_PCT = 0.80          # Only enter in first 80% of window
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


def compute_taker_fee(shares, price):
    if price <= 0 or price >= 1:
        return 0.0
    fee_shares = shares * CRYPTO_FEE_RATE * (price * (1 - price)) ** CRYPTO_FEE_EXPONENT
    return round(fee_shares * price, 4)


# =========================================================================
# BACKGROUND SELL WORKER
# =========================================================================
class SellWorker:
    """Background thread that retries failed sells until shares are gone."""

    RETRY_INTERVAL = 3       # seconds between retries
    MAX_RETRIES = 60         # give up after ~3 minutes

    def __init__(self, poly):
        self.poly = poly
        self._queue = queue.Queue()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        self._active_count = 0
        self._lock = threading.Lock()

    def enqueue(self, pos, reason):
        """Hand off a failed sell for background retry."""
        with self._lock:
            self._active_count += 1
        self._queue.put((pos, reason, 0))
        log(f"  🔄 Queued background sell: {pos['shares']:.1f}sh {pos['side']} "
            f"(token {pos['token_id'][:8]}...)")

    @property
    def pending(self):
        with self._lock:
            return self._active_count

    def drain(self, timeout=15):
        """Block until all pending sells finish or timeout."""
        deadline = time.time() + timeout
        while self.pending > 0 and time.time() < deadline:
            time.sleep(0.5)
        remaining = self.pending
        if remaining > 0:
            log(f"  ⚠ SellWorker drain timeout: {remaining} sells still pending")

    def _run(self):
        """Worker loop — process sell queue forever."""
        while True:
            try:
                pos, reason, attempts = self._queue.get()
                self._try_sell(pos, reason, attempts)
            except Exception as e:
                log(f"  ⚠ SellWorker error: {e}")
                time.sleep(1)

    def _try_sell(self, pos, reason, attempts):
        """Attempt FOK sell. Re-queue on failure."""
        token_id = pos["token_id"]
        shares = pos["shares"]
        side = pos["side"]

        if attempts >= self.MAX_RETRIES:
            log(f"  ❌ SellWorker gave up on {shares:.1f}sh {side} "
                f"after {attempts} attempts")
            with self._lock:
                self._active_count -= 1
            return

        try:
            bid, _, _ = self.poly.get_price(token_id)
            price = round(bid, 2) if bid and bid > 0 else 0.01

            market_order = MarketOrderArgs(
                token_id=token_id,
                amount=shares,
                side=SELL,
            )
            signed = client.create_market_order(market_order)
            resp = client.post_order(signed, OrderType.FOK)

            success = resp.get("success", False) if isinstance(resp, dict) else False
            taking = float(resp.get("takingAmount", 0)) if isinstance(resp, dict) else 0
            making = float(resp.get("makingAmount", 0)) if isinstance(resp, dict) else 0

            if success or taking > 0:
                actual_revenue = taking
                actual_shares_sold = making if making > 0 else shares
                actual_price = round(actual_revenue / actual_shares_sold, 4) if actual_shares_sold > 0 else price
                log(f"  🔄✅ Background sold {actual_shares_sold:.1f}sh {side} "
                    f"@ ~{actual_price:.3f} (${actual_revenue:.2f}) "
                    f"[attempt {attempts+1}]")
                with self._lock:
                    self._active_count -= 1
                return

        except Exception as e:
            err_str = str(e)
            if "not enough balance" in err_str or "allowance" in err_str:
                log(f"  🔄✅ Background sell: shares already gone ({side})")
                with self._lock:
                    self._active_count -= 1
                return
            log(f"  🔄⚠ Background sell error (attempt {attempts+1}): {e}")

        # Re-queue for retry
        time.sleep(self.RETRY_INTERVAL)
        self._queue.put((pos, reason, attempts + 1))


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

    def get_spike_start(self, side):
        """Get the oldest price within SPIKE_WINDOW, i.e. where the spike started from."""
        history = self._history_up if side == "UP" else self._history_down
        if len(history) < 2:
            return None
        now = time.time()
        for ts, price in history:
            if now - ts <= SPIKE_WINDOW:
                return price
        return None


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
        self._window_trade_count = {}  # epoch -> count of trades in this window
        self._sell_worker = SellWorker(poly)

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

                # BTC spike detected! Check window trade limit
                wnd_trades = self._window_trade_count.get(epoch, 0)
                if wnd_trades >= MAX_SPIKES_PER_WINDOW:
                    continue  # Already traded enough in this window

                # Check ETH token
                eth_token = info.get(f"eth_{side.lower()}")
                if not eth_token:
                    continue

                eth_bid, eth_ask, eth_age = self.poly.get_price(eth_token)
                if not eth_ask or eth_ask <= 0 or (eth_age and eth_age > 5):
                    continue
                if not eth_bid or eth_bid <= 0:
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
                log(f"  🎯 Buying ETH {side} @ market | "
                    f"bet=${bet:.0f} | window {pct:.0%} elapsed")

                success = self._buy_eth(
                    eth_token, side, bet, epoch, info
                )
                if success:
                    self._window_trade_count[epoch] = wnd_trades + 1
                    return  # Only one position at a time

    def _buy_eth(self, token_id, side, bet, epoch, window_info):
        """FOK market buy on ETH token. Returns True if filled."""

        log(f"  💰 Buying ETH {side}: ${bet:.0f} FOK market order")

        if DRY_RUN:
            eth_mid = self.poly.mid_price(token_id) or 0.50
            est_shares = bet / eth_mid if eth_mid > 0 else 0
            log(f"  🧪 DRY RUN — ~{est_shares:.0f}sh @ ~{eth_mid:.2f}")
            self._position = {
                "token_id": token_id,
                "side": side,
                "entry_price": eth_mid,
                "entry_mid": eth_mid,
                "shares": est_shares,
                "cost": bet,
                "entry_time": time.time(),
                "epoch": epoch,
                "btc_token": window_info.get(f"btc_{side.lower()}"),
                "btc_peak": self.detector.get_current(side) or 0,
                "dry_run": True,
            }
            return True

        try:
            market_order = MarketOrderArgs(
                token_id=token_id,
                amount=bet,
                side=BUY,
            )
            signed = client.create_market_order(market_order)
            resp = client.post_order(signed, OrderType.FOK)

            order_id = resp.get("orderID", "") if isinstance(resp, dict) else ""
            status = resp.get("status", "") if isinstance(resp, dict) else ""
            success = resp.get("success", False) if isinstance(resp, dict) else False
            taking = float(resp.get("takingAmount", 0)) if isinstance(resp, dict) else 0
            making = float(resp.get("makingAmount", 0)) if isinstance(resp, dict) else 0

            if not success and status == "error":
                log(f"  ⚠ Buy rejected: {resp}")
                return False

            if taking <= 0:
                log(f"  ⚠ Buy got 0 shares: {resp}")
                return False

            # FOK: makingAmount = USDC spent, takingAmount = shares received
            actual_shares = taking
            actual_cost = making
            fill_price = round(actual_cost / actual_shares, 4) if actual_shares > 0 else 0

            log(f"  ✅ FOK filled! {actual_shares:.1f}sh @ ~{fill_price:.3f} "
                f"(spent ${actual_cost:.2f})")

            eth_mid_now = self.poly.mid_price(token_id) or fill_price
            btc_current = self.detector.get_current(side)
            self._position = {
                "token_id": token_id,
                "side": side,
                "entry_price": fill_price,
                "entry_mid": eth_mid_now,
                "shares": actual_shares,
                "cost": actual_cost,
                "entry_time": time.time(),
                "epoch": epoch,
                "btc_token": window_info.get(f"btc_{side.lower()}"),
                "btc_peak": btc_current or fill_price,
                "order_id": order_id,
                "dry_run": False,
            }
            return True

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
        # Compare mid-to-mid (not mid vs ask) to avoid spread triggering stop-loss
        entry_mid = pos.get("entry_mid", pos["entry_price"])
        price_change = eth_mid - entry_mid

        # Update BTC peak
        btc_current = self.detector.get_current(side)
        if btc_current is not None and btc_current > pos["btc_peak"]:
            pos["btc_peak"] = btc_current

        # ─── Exit checks (in priority order) ───

        # 1. Stop loss (with grace period to let position settle)
        if price_change <= -STOP_LOSS and hold_secs >= ENTRY_GRACE_SECS:
            log(f"  🛑 STOP LOSS: ETH {side} dropped {price_change:+.3f} from entry_mid "
                f"(mid={eth_mid:.3f}, entry_mid={entry_mid:.3f})")
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
        """FOK market sell of ETH position."""
        pos = self._position
        if pos is None:
            return

        token_id = pos["token_id"]
        shares = pos["shares"]
        side = pos["side"]

        eth_bid, _, _ = self.poly.get_price(token_id)
        sell_price = round(eth_bid, 2) if eth_bid and eth_bid > 0 else 0.01

        log(f"  📤 Selling ETH {side}: {shares:.1f}sh @ ~{sell_price:.2f} FOK ({reason})")

        if pos.get("dry_run"):
            fee = compute_taker_fee(shares, sell_price)
            revenue = shares * sell_price - fee
            pnl = revenue - pos["cost"]
            log(f"  🧪 DRY RUN — PnL ${pnl:+.2f}")
            self._record_trade(pos, sell_price, pnl, reason)
            self._position = None
            self._cooldowns[side] = time.time()
            return

        # Use FOK market sell — amount = shares to sell
        sell_amount = shares
        max_retries = 3

        for attempt in range(max_retries):
            try:
                market_order = MarketOrderArgs(
                    token_id=token_id,
                    amount=sell_amount,
                    side=SELL,
                )
                signed = client.create_market_order(market_order)
                resp = client.post_order(signed, OrderType.FOK)

                success = resp.get("success", False) if isinstance(resp, dict) else False
                taking = float(resp.get("takingAmount", 0)) if isinstance(resp, dict) else 0
                making = float(resp.get("makingAmount", 0)) if isinstance(resp, dict) else 0

                if success or taking > 0:
                    # taking = USDC received (already net of fees from Polymarket)
                    # making = shares sold
                    actual_revenue = taking
                    actual_shares_sold = making if making > 0 else shares
                    actual_price = round(actual_revenue / actual_shares_sold, 4) if actual_shares_sold > 0 else sell_price
                    # No fee subtraction — takingAmount is already net of fees
                    pnl = actual_revenue - pos["cost"]
                    log(f"  💰 Sold {actual_shares_sold:.1f}sh @ ~{actual_price:.3f} "
                        f"→ PnL ${pnl:+.2f} ({reason})")
                    self._record_trade(pos, actual_price, pnl, reason)
                    self._position = None
                    self._cooldowns[side] = time.time()
                    return

                # FOK rejected — retry (book may have changed)
                log(f"  ⚠ Sell FOK rejected (attempt {attempt+1}): {resp}")
                time.sleep(0.5)

            except Exception as e:
                err_str = str(e)
                log(f"  ⚠ Sell error (attempt {attempt+1}): {e}")
                if "not enough balance" in err_str or "allowance" in err_str:
                    # Shares already gone — a previous sell likely went through
                    log(f"  💰 Shares already sold (balance error) — recording at {sell_price}")
                    fee = compute_taker_fee(shares, sell_price)
                    pnl = shares * sell_price - fee - pos["cost"]
                    self._record_trade(pos, sell_price, pnl, reason)
                    self._position = None
                    self._cooldowns[side] = time.time()
                    return
                time.sleep(0.5)

        # All inline retries failed — hand off to background sell worker
        log(f"  ⚠ Inline sell failed — handing to background worker")
        self._sell_worker.enqueue(pos, reason)

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
            entry_mid = pos.get("entry_mid", pos["entry_price"])
            change = eth_mid - entry_mid
            btc_now = self.detector.get_current(pos["side"])
            btc_drop = (pos["btc_peak"] - btc_now) if btc_now else 0
            return (f"HOLDING ETH {pos['side']} | entry={pos['entry_price']:.2f} "
                    f"now={eth_mid:.3f} Δ{change:+.3f} | "
                    f"BTC peak={pos['btc_peak']:.3f} drop={btc_drop:.3f} | "
                    f"hold={hold:.0f}s/{MAX_HOLD_SECS}s")
        pending = self._sell_worker.pending
        base = "SCANNING for BTC spikes..."
        if pending > 0:
            base += f" | ⚠ {pending} background sell(s)"
        return base


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
            # Wait for background sell worker to finish
            if follower._sell_worker.pending > 0:
                log(f"  Draining {follower._sell_worker.pending} background sell(s)...")
                follower._sell_worker.drain(timeout=20)
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
