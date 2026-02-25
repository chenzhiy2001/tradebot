#!/usr/bin/env python3
"""
GapBot — Gap-based lead-lag trading on Polymarket.

Core Insight:
  BTC reprices first on Polymarket; ETH/SOL/XRP lag by 5-15 seconds.
  Instead of entering on any BTC spike, we measure the DIVERGENCE:
  how much BTC moved vs how much the follower has already caught up.
  We only enter when there is still unexploited lag.

Entry Logic:
  Every tick:
    1. Compute btc_move = BTC price now - BTC price N seconds ago.
    2. Compute fol_move = follower price now - follower price N seconds ago.
    3. gap = btc_move - fol_move  (how much the follower HASN'T moved yet)
    4. Enter if:
       - btc_move ≥ MIN_BTC_MOVE  (BTC actually surged, not noise)
       - gap ≥ MIN_GAP            (follower still lagging — that's our edge)
       - BTC moved in same direction for ≥ MIN_CONSECUTIVE ticks (sustained)
       - follower mid in [MIN_MID, MAX_MID]  (favorable price zone)
       - spread ≤ MAX_SPREAD

Exit Logic:
  - Hard stop-loss: entry - STOP_LOSS (immediate, no grace period after settlement)
  - Trailing stop: once profit ≥ TRAIL_ACTIVATE, trail at TRAIL_PCT retracement
  - Time stop: MAX_HOLD_SECS
  - Take profit: TAKE_PROFIT
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
MIN_BET = 5
MAX_BET = 50
BET_PCT = 0.90                # Bet 90% of balance

# Gap detection — the core edge
MIN_BTC_MOVE = 0.06           # BTC must move ≥ 6¢ in the lookback window
MIN_GAP = 0.03                # Follower must still lag by ≥ 3¢ (unexploited edge)
LOOKBACK_SECS = 10            # Measure moves over this window

# Leader/follower config
LEADER = "btc"
FOLLOWERS = ["eth", "sol", "xrp"]

# Entry quality filters
MIN_MID = 0.25                # Only buy if follower mid ≥ 25¢
MAX_MID = 0.65                # Only buy if follower mid ≤ 65¢ (favorable zone)
MAX_SPREAD = 0.06             # Skip if spread > 6¢
MAX_TRADES_PER_WINDOW = 3

# Exit conditions
STOP_LOSS = 0.05              # Hard stop: exit if down 5¢ from entry (no grace)
TRAIL_ACTIVATE = 0.04         # Start trailing after 4¢ gain
TRAIL_PCT = 0.40              # Trail: allow 40% retracement (keep 60%)
TRAIL_MIN = 0.02              # Minimum trail distance: 2¢
TAKE_PROFIT = 0.95            # Hard TP
MAX_HOLD_SECS = 60            # Shorter hold — edge decays fast
SETTLEMENT_SECS = 5           # Wait this long after buy before checking exits

# Window timing
MAX_ENTRY_PCT = 0.70
INTERVALS = [5]

# Polymarket fee
CRYPTO_FEE_RATE = 0.25
CRYPTO_FEE_EXPONENT = 2

# Data files
LOG_FILE = "gapbot_log.txt"
TRADE_LOG = "gapbot_trades.json"

DRY_RUN = "--dry-run" in sys.argv
TICK_INTERVAL = 0.25          # 250ms ticks

TOKEN_COOLDOWN_SECS = 10


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


def wait_for_settlement(token_id, expected_shares, pre_buy_balance=0, timeout=30):
    deadline = time.time() + timeout
    last_balance = 0
    while time.time() < deadline:
        bal = get_share_balance(token_id)
        if bal is not None:
            last_balance = bal
            new_shares = bal - pre_buy_balance
            if new_shares >= expected_shares * 0.9:
                return True, bal
        time.sleep(2)
    return False, last_balance


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

    RETRY_INTERVAL = 3
    MAX_RETRIES = 20

    def __init__(self, poly):
        self.poly = poly
        self._queue = queue.Queue()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        self._active_count = 0
        self._lock = threading.Lock()

    def enqueue(self, pos, reason):
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
        deadline = time.time() + timeout
        while self.pending > 0 and time.time() < deadline:
            time.sleep(0.5)
        remaining = self.pending
        if remaining > 0:
            log(f"  ⚠ SellWorker drain timeout: {remaining} sells still pending")

    def _run(self):
        while True:
            try:
                pos, reason, attempts = self._queue.get()
                self._try_sell(pos, reason, attempts)
            except Exception as e:
                log(f"  ⚠ SellWorker error: {e}")
                time.sleep(1)

    def _try_sell(self, pos, reason, attempts):
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
            bal = get_share_balance(token_id)
            if bal is not None and bal < 1.0:
                log(f"  🔄✅ Background sell: shares already gone "
                    f"({side}, balance={bal:.1f})")
                with self._lock:
                    self._active_count -= 1
                return

            sell_amount = bal if (bal is not None and bal > 0) else shares
            if sell_amount != shares:
                log(f"  🔄 Adjusted sell amount: {shares:.1f} → {sell_amount:.1f} (on-chain)")

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
                actual_revenue = taking
                actual_shares_sold = making if making > 0 else shares
                actual_price = round(actual_revenue / actual_shares_sold, 4) if actual_shares_sold > 0 else 0
                log(f"  🔄✅ Background sold {actual_shares_sold:.1f}sh {side} "
                    f"@ ~{actual_price:.3f} (${actual_revenue:.2f}) "
                    f"[attempt {attempts+1}]")
                with self._lock:
                    self._active_count -= 1
                return

        except Exception as e:
            err_str = str(e)
            if 'No orderbook exists' in err_str or 'orderbook' in err_str.lower():
                log(f"  🔄✅ Market resolved, shares redeemed ({side})")
                with self._lock:
                    self._active_count -= 1
                return
            if 'price' in err_str.lower() and 'max' in err_str.lower():
                log(f"  🔄✅ Price at max — market resolved in our favor ({side})")
                with self._lock:
                    self._active_count -= 1
                return
            if 'no match' in err_str.lower():
                log(f"  🔄✅ Token expired / no match — stopping retries ({side})")
                with self._lock:
                    self._active_count -= 1
                return
            if 'not ready' in err_str.lower() or '425' in err_str:
                log(f"  🔄⚠ Service not ready, retrying (not counted as attempt)")
                time.sleep(self.RETRY_INTERVAL)
                self._queue.put((pos, reason, attempts))
                return
            log(f"  🔄⚠ Background sell error (attempt {attempts+1}): {e}")

        time.sleep(self.RETRY_INTERVAL)
        self._queue.put((pos, reason, attempts + 1))


# =========================================================================
# POLYMARKET WEBSOCKET FEED
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
    all_cryptos = [LEADER] + FOLLOWERS
    result = {}
    now = datetime.now(timezone.utc)

    for interval in INTERVALS:
        aligned = (now.minute // interval) * interval
        window_start = now.replace(minute=aligned, second=0, microsecond=0)
        epoch = int(window_start.timestamp())

        for crypto in all_cryptos:
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

    leader_keys = [f"{LEADER}_up", f"{LEADER}_down"]
    complete = {}
    for epoch, info in result.items():
        if not all(k in info for k in leader_keys):
            continue
        has_follower = any(
            f"{f}_up" in info and f"{f}_down" in info for f in FOLLOWERS
        )
        if has_follower and now < info["end"]:
            complete[epoch] = info
    return complete


# =========================================================================
# PRICE TRACKER — rolling price history for gap detection
# =========================================================================
class PriceTracker:
    """Track rolling mid-price history for any token, keyed by a label."""

    def __init__(self, maxlen=400):
        self._history = {}  # label -> deque of (timestamp, mid_price)
        self._maxlen = maxlen

    def update(self, label, mid_price):
        if label not in self._history:
            self._history[label] = deque(maxlen=self._maxlen)
        self._history[label].append((time.time(), mid_price))

    def clear(self):
        self._history.clear()

    def get_current(self, label):
        """Latest price for label, or None if stale (>5s)."""
        h = self._history.get(label)
        if not h:
            return None
        ts, price = h[-1]
        if time.time() - ts > 5.0:
            return None
        return price

    def get_price_at(self, label, secs_ago):
        """Price approximately secs_ago seconds in the past."""
        h = self._history.get(label)
        if not h:
            return None
        target = time.time() - secs_ago
        # Find the tick closest to target time
        best = None
        best_diff = float("inf")
        for ts, price in h:
            diff = abs(ts - target)
            if diff < best_diff:
                best_diff = diff
                best = price
        # Only valid if within 2s of the target time
        if best_diff > 2.0:
            return None
        return best

    def get_consecutive_direction(self, label, min_ticks=3):
        """Count how many recent ticks moved in the same direction.
        Returns (count, direction): direction is +1 (up) or -1 (down).
        Returns (0, 0) if fewer than 2 ticks."""
        h = self._history.get(label)
        if not h or len(h) < 2:
            return 0, 0

        # Only consider ticks within the last LOOKBACK_SECS + a margin
        now = time.time()
        recent = [(ts, p) for ts, p in h if now - ts <= LOOKBACK_SECS + 2]
        if len(recent) < 2:
            return 0, 0

        # Work backwards from most recent tick
        direction = 0
        count = 0
        for i in range(len(recent) - 1, 0, -1):
            delta = recent[i][1] - recent[i - 1][1]
            if delta == 0:
                continue  # skip flat ticks
            tick_dir = 1 if delta > 0 else -1
            if direction == 0:
                direction = tick_dir
                count = 1
            elif tick_dir == direction:
                count += 1
            else:
                break  # direction changed

        return count, direction

    def get_move(self, label, secs):
        """Price change over the last `secs` seconds. Returns (move, current)."""
        current = self.get_current(label)
        past = self.get_price_at(label, secs)
        if current is None or past is None:
            return None, current
        return current - past, current


# =========================================================================
# GAPBOT — main trading engine
# =========================================================================
class GapBot:
    """Detect BTC-follower divergences and trade the gap."""

    def __init__(self, poly):
        self.poly = poly
        self.tracker = PriceTracker()
        self._windows = {}
        self._position = None
        self._cooldowns = {}
        self._start_balance = get_usdc_balance()
        self._trade_count = 0
        self._win_count = 0
        self._total_pnl = 0.0
        self._window_trade_count = {}
        self._sell_worker = SellWorker(poly)

    def update_markets(self, markets):
        """Update tracked windows and subscribe to all tokens."""
        old_leader_tokens = set()
        for info in self._windows.values():
            for k in [f"{LEADER}_up", f"{LEADER}_down"]:
                if k in info:
                    old_leader_tokens.add(info[k])
        new_leader_tokens = set()
        for info in markets.values():
            for k in [f"{LEADER}_up", f"{LEADER}_down"]:
                if k in info:
                    new_leader_tokens.add(info[k])

        if new_leader_tokens != old_leader_tokens:
            self.tracker.clear()
            if old_leader_tokens:
                log("  🔄 New window tokens — price history cleared")

        self._windows = markets
        all_tokens = []
        for epoch, info in markets.items():
            for k in info:
                if k.endswith("_up") or k.endswith("_down"):
                    all_tokens.append(info[k])
        if all_tokens:
            self.poly.subscribe(all_tokens)

    def tick(self):
        """Main tick: update prices, detect gaps, manage positions."""
        now = time.time()
        now_utc = datetime.now(timezone.utc)

        # ─── Update price tracking for ALL tokens ───
        for epoch, info in list(self._windows.items()):
            if now_utc >= info["end"]:
                continue

            for crypto in [LEADER] + FOLLOWERS:
                for side_label in ["up", "down"]:
                    token = info.get(f"{crypto}_{side_label}")
                    if not token:
                        continue
                    mid = self.poly.mid_price(token)
                    if mid is not None:
                        label = f"{crypto}_{side_label}"
                        self.tracker.update(label, mid)

        # ─── Manage open position ───
        if self._position is not None:
            self._manage_position()
            return

        # ─── Scan for BTC-follower gaps ───
        for epoch, info in list(self._windows.items()):
            if now_utc >= info["end"]:
                continue

            elapsed = (now_utc - info["start"]).total_seconds()
            duration = (info["end"] - info["start"]).total_seconds()
            pct = elapsed / duration if duration > 0 else 1.0

            if pct > MAX_ENTRY_PCT:
                continue

            wnd_trades = self._window_trade_count.get(epoch, 0)
            if wnd_trades >= MAX_TRADES_PER_WINDOW:
                continue

            # Check both sides
            best_signal = None  # (gap, crypto, side, token, fol_mid, btc_move, fol_move)

            for side in ["UP", "DOWN"]:
                side_label = side.lower()

                # BTC move over lookback
                btc_move, btc_now = self.tracker.get_move(
                    f"{LEADER}_{side_label}", LOOKBACK_SECS
                )
                if btc_move is None:
                    continue

                # Diagnostic: log significant BTC moves below threshold
                if btc_move >= 0.04 and btc_move < MIN_BTC_MOVE:
                    self._log_diagnostic(
                        f"BTC {side} near-miss: move={btc_move:.3f}<{MIN_BTC_MOVE}"
                    )

                if btc_move < MIN_BTC_MOVE:
                    continue

                # Now find the best follower with the largest gap
                for crypto in FOLLOWERS:
                    token = info.get(f"{crypto}_{side_label}")
                    if not token:
                        continue

                    # Cooldown check
                    last_sold = self._cooldowns.get(token, 0)
                    if now - last_sold < TOKEN_COOLDOWN_SECS:
                        continue

                    # Follower move over same lookback
                    fol_move, fol_now = self.tracker.get_move(
                        f"{crypto}_{side_label}", LOOKBACK_SECS
                    )
                    if fol_move is None or fol_now is None:
                        continue

                    # The gap: how much follower HASN'T caught up
                    gap = btc_move - fol_move

                    # Diagnostic: log gaps that are close but not enough
                    if gap > 0.01 and gap < MIN_GAP:
                        self._log_diagnostic(
                            f"  {crypto.upper()} {side} gap={gap:.3f} < {MIN_GAP} "
                            f"(btc={btc_move:.3f}, fol={fol_move:.3f})"
                        )

                    if gap < MIN_GAP:
                        continue

                    # Price zone filter
                    bid, ask, age = self.poly.get_price(token)
                    if not bid or not ask or bid <= 0 or ask <= 0:
                        continue
                    if age and age > 5:
                        continue

                    fol_mid = (bid + ask) / 2
                    if fol_mid < MIN_MID or fol_mid > MAX_MID:
                        continue

                    spread = ask - bid
                    if spread > MAX_SPREAD:
                        continue

                    # Score by gap size — take the biggest gap
                    if best_signal is None or gap > best_signal[0]:
                        best_signal = (gap, crypto, side, token, fol_mid, btc_move, fol_move)

            if best_signal is None:
                continue

            gap, crypto, side, token, fol_mid, btc_move, fol_move = best_signal

            # Check balance
            balance = get_usdc_balance()
            if balance is None:
                continue
            bet = int(min(balance * BET_PCT, MAX_BET))
            if bet < MIN_BET:
                continue

            log(f"  🎯 GAP DETECTED: {LEADER.upper()} {side} moved +{btc_move:.3f}, "
                f"{crypto.upper()} only +{fol_move:.3f} → gap={gap:.3f}")
            log(f"  💰 Buying {crypto.upper()} {side} @ {fol_mid:.3f} | "
                f"bet=${bet} | window {pct:.0%}")

            success = self._buy_follower(token, crypto, side, bet, epoch, info)
            if success:
                self._window_trade_count[epoch] = wnd_trades + 1
                return

    def _buy_follower(self, token_id, crypto, side, bet, epoch, window_info):
        """FOK market buy. Returns True if filled."""
        crypto_label = crypto.upper()

        if DRY_RUN:
            mid = self.poly.mid_price(token_id) or 0.50
            est_shares = bet / mid if mid > 0 else 0
            log(f"  🧪 DRY RUN — ~{est_shares:.0f}sh @ ~{mid:.2f}")
            self._position = {
                "token_id": token_id,
                "crypto": crypto,
                "side": side,
                "entry_price": mid,
                "entry_mid": mid,
                "shares": est_shares,
                "cost": bet,
                "entry_time": time.time(),
                "settle_time": time.time(),  # instant in dry run
                "epoch": epoch,
                "peak": mid,
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

            actual_shares = taking
            actual_cost = making
            fill_price = round(actual_cost / actual_shares, 4) if actual_shares > 0 else 0

            log(f"  ✅ FOK filled! {actual_shares:.1f}sh @ ~{fill_price:.3f} "
                f"(spent ${actual_cost:.2f})")

            # Settlement
            pre_buy_bal = get_share_balance(token_id) or 0
            log(f"  ⏳ Waiting for settlement...")
            settled, on_chain_shares = wait_for_settlement(
                token_id, actual_shares, pre_buy_balance=pre_buy_bal, timeout=30
            )
            settle_time = time.time()
            if settled:
                new_shares = on_chain_shares - pre_buy_bal
                log(f"  ✅ Settled: {new_shares:.1f}sh")
                if new_shares > actual_shares * 1.5:
                    log(f"  ⚠ On-chain inflated — using FOK amount")
                else:
                    actual_shares = new_shares
            else:
                log(f"  ⚠ Settlement timeout — proceeding")

            self._position = {
                "token_id": token_id,
                "crypto": crypto,
                "side": side,
                "entry_price": fill_price,
                "entry_mid": fill_price,
                "shares": actual_shares,
                "cost": actual_cost,
                "entry_time": time.time(),
                "settle_time": settle_time,
                "epoch": epoch,
                "peak": fill_price,
                "order_id": order_id,
                "dry_run": False,
            }
            return True

        except Exception as e:
            log(f"  ⚠ Buy error: {e}")
            return False

    def _manage_position(self):
        """Check exit conditions."""
        pos = self._position
        if pos is None:
            return

        now = time.time()
        hold_secs = now - pos["entry_time"]
        since_settle = now - pos.get("settle_time", pos["entry_time"])
        side = pos["side"]
        crypto_label = pos.get("crypto", "eth").upper()

        bid, ask, age = self.poly.get_price(pos["token_id"])
        if not bid or bid <= 0:
            if hold_secs > MAX_HOLD_SECS:
                log(f"  ⏰ TIME STOP: no price after {hold_secs:.0f}s")
                self._sell_position("time_stop_no_price")
            return

        mid = (bid + (ask or bid)) / 2.0
        entry_mid = pos.get("entry_mid", pos["entry_price"])
        change = mid - entry_mid

        # Update peak after settlement
        if since_settle >= SETTLEMENT_SECS:
            if mid > pos.get("peak", 0):
                pos["peak"] = mid

        # ─── Exit checks ───

        # 1. Take profit
        if change >= TAKE_PROFIT:
            log(f"  🎉 TAKE PROFIT: {crypto_label} {side} +{change:.3f}")
            self._sell_position("take_profit")
            return

        # 2. HARD STOP-LOSS — fire immediately after settlement resolves
        #    This is the key improvement: no 8-second blind grace period
        if since_settle >= SETTLEMENT_SECS:
            if change <= -STOP_LOSS:
                log(f"  🛑 STOP LOSS: {crypto_label} {side} {change:+.3f} "
                    f"(entry={entry_mid:.3f}, now={mid:.3f}, limit=-{STOP_LOSS})")
                self._sell_position("stop_loss")
                return

        # 3. Trailing stop — once we've gained enough, protect profits
        if since_settle >= SETTLEMENT_SECS:
            peak = pos.get("peak", mid)
            gain = peak - entry_mid
            if gain >= TRAIL_ACTIVATE:
                trail_distance = max(TRAIL_MIN, TRAIL_PCT * gain)
                drop = peak - mid
                if drop >= trail_distance:
                    profit = pos["shares"] * (mid - entry_mid)
                    log(f"  📉 TRAIL: {crypto_label} {side} dropped {drop:.3f} from peak "
                        f"(peak={peak:.3f}, now={mid:.3f}, trail={trail_distance:.3f}, "
                        f"profit=${profit:.2f})")
                    self._sell_position("trail")
                    return

        # 4. Time stop
        if hold_secs > MAX_HOLD_SECS:
            log(f"  ⏰ TIME STOP: {crypto_label} {side} held {hold_secs:.0f}s "
                f"(change={change:+.3f})")
            self._sell_position("time_stop")
            return

    def _sell_position(self, reason):
        """FOK market sell of open position."""
        pos = self._position
        if pos is None:
            return

        token_id = pos["token_id"]
        shares = pos["shares"]
        side = pos["side"]
        crypto_label = pos.get("crypto", "eth").upper()

        bid, _, _ = self.poly.get_price(token_id)
        sell_price = round(bid, 2) if bid and bid > 0 else 0.01

        log(f"  📤 Selling {crypto_label} {side}: {shares:.1f}sh @ ~{sell_price:.2f} "
            f"FOK ({reason})")

        if pos.get("dry_run"):
            fee = compute_taker_fee(shares, sell_price)
            revenue = shares * sell_price - fee
            pnl = revenue - pos["cost"]
            log(f"  🧪 DRY RUN — PnL ${pnl:+.2f}")
            self._record_trade(pos, sell_price, pnl, reason)
            self._position = None
            self._cooldowns[side] = time.time()
            self._cooldowns[token_id] = time.time()
            return

        on_chain_bal = get_share_balance(token_id)
        if on_chain_bal is not None and on_chain_bal > 0:
            sell_amount = on_chain_bal
            if abs(sell_amount - shares) > 1.0:
                log(f"  🔄 Adjusted sell: {shares:.1f} → {sell_amount:.1f}sh (on-chain)")
        else:
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
                    actual_revenue = taking
                    actual_shares_sold = making if making > 0 else shares
                    actual_price = round(actual_revenue / actual_shares_sold, 4) if actual_shares_sold > 0 else sell_price
                    pnl = actual_revenue - pos["cost"]
                    log(f"  💰 Sold {actual_shares_sold:.1f}sh @ ~{actual_price:.3f} "
                        f"→ PnL ${pnl:+.2f} ({reason})")
                    self._record_trade(pos, actual_price, pnl, reason)
                    self._position = None
                    self._cooldowns[side] = time.time()
                    self._cooldowns[token_id] = time.time()
                    return

                log(f"  ⚠ Sell FOK rejected (attempt {attempt+1}): {resp}")
                time.sleep(0.5)

            except Exception as e:
                err_str = str(e)
                if 'price' in err_str.lower() and 'max' in err_str.lower():
                    log(f"  ✅ Market resolved in our favor")
                    pnl = shares * 1.0 - pos["cost"]
                    self._record_trade(pos, 1.0, pnl, reason + "_resolved")
                    self._position = None
                    self._cooldowns[side] = time.time()
                    self._cooldowns[token_id] = time.time()
                    return
                if 'no match' in err_str.lower():
                    log(f"  ⚠ Token expired / no match")
                    self._record_trade(pos, 0.0, -pos["cost"], reason + "_expired")
                    self._position = None
                    self._cooldowns[side] = time.time()
                    self._cooldowns[token_id] = time.time()
                    return
                log(f"  ⚠ Sell error (attempt {attempt+1}): {e}")
                time.sleep(0.5)

        log(f"  ⚠ Inline sell failed — handing to background worker")
        self._sell_worker.enqueue(pos, reason)
        self._position = None
        self._cooldowns[side] = time.time()
        self._cooldowns[token_id] = time.time()

    def _record_trade(self, pos, sell_price, pnl, reason):
        self._trade_count += 1
        self._total_pnl += pnl
        if pnl > 0:
            self._win_count += 1

        entry = {
            "time": datetime.now(timezone.utc).isoformat(),
            "crypto": pos.get("crypto", "eth"),
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

    def _log_diagnostic(self, msg):
        """Throttled diagnostic logging — max once per 5 seconds per msg prefix."""
        now = time.time()
        if not hasattr(self, '_diag_log_times'):
            self._diag_log_times = {}
        # Use first 30 chars as key to avoid flooding
        key = msg[:30]
        if now - self._diag_log_times.get(key, 0) >= 5.0:
            self._diag_log_times[key] = now
            log(f"  🔍 {msg}")

    def get_status(self):
        if self._position:
            pos = self._position
            hold = time.time() - pos["entry_time"]
            bid, _, _ = self.poly.get_price(pos["token_id"])
            mid = bid or 0
            entry_mid = pos.get("entry_mid", pos["entry_price"])
            change = mid - entry_mid
            crypto_label = pos.get("crypto", "ETH").upper()
            return (f"HOLDING {crypto_label} {pos['side']} | "
                    f"entry={pos['entry_price']:.3f} now={mid:.3f} Δ{change:+.3f} | "
                    f"hold={hold:.0f}s/{MAX_HOLD_SECS}s")
        pending = self._sell_worker.pending
        base = "SCANNING for gaps..."
        if pending > 0:
            base += f" | ⚠ {pending} background sell(s)"
        return base


# =========================================================================
# MAIN
# =========================================================================
def main():
    mode = "DRY" if DRY_RUN else "LIVE"
    followers_str = ",".join(f.upper() for f in FOLLOWERS)
    log(f"═══ GapBot ═══ [{mode}]")
    log(f"Strategy: {LEADER.upper()} → {followers_str} gap-based lag trading")
    log(f"Entry: BTC move ≥{MIN_BTC_MOVE}, gap ≥{MIN_GAP}, "
        f"lookback {LOOKBACK_SECS}s")
    log(f"Price zone: [{MIN_MID}, {MAX_MID}] | spread ≤{MAX_SPREAD}")
    log(f"Exit: stop={STOP_LOSS}, trail after +{TRAIL_ACTIVATE} "
        f"({TRAIL_PCT:.0%} retr, min {TRAIL_MIN}), TP={TAKE_PROFIT}, "
        f"time={MAX_HOLD_SECS}s")
    log(f"Bet: {BET_PCT:.0%} of balance, min ${MIN_BET}, max ${MAX_BET}")
    log("")

    poly = PolymarketFeed()
    poly.start()

    bot = GapBot(poly)

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
    discovery_interval = 15

    while True:
        try:
            now = time.time()

            if now - last_discovery > discovery_interval:
                markets = discover_markets()
                if markets:
                    bot.update_markets(markets)
                last_discovery = now

            bot.tick()

            # Dashboard
            now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
            pm_status = "🟢" if poly.connected else "🔴"
            status = bot.get_status()

            # Show prices + gap info
            price_parts = []
            gap_parts = []
            for epoch, info in bot._windows.items():
                for side_label in ["up", "down"]:
                    btc_tok = info.get(f"{LEADER}_{side_label}")
                    if btc_tok:
                        btc_mid = poly.mid_price(btc_tok)
                        if btc_mid:
                            price_parts.append(
                                f"{LEADER.upper()}_{side_label.upper()}={btc_mid:.3f}"
                            )
                    for crypto in FOLLOWERS:
                        tok = info.get(f"{crypto}_{side_label}")
                        if tok:
                            fol_mid = poly.mid_price(tok)
                            if fol_mid:
                                price_parts.append(
                                    f"{crypto.upper()}_{side_label.upper()}={fol_mid:.3f}"
                                )

                    # Show current gap
                    btc_move, _ = bot.tracker.get_move(
                        f"{LEADER}_{side_label}", LOOKBACK_SECS
                    )
                    if btc_move and btc_move > 0.03:
                        for crypto in FOLLOWERS:
                            fol_move, _ = bot.tracker.get_move(
                                f"{crypto}_{side_label}", LOOKBACK_SECS
                            )
                            if fol_move is not None:
                                gap = btc_move - fol_move
                                if gap > 0.02:
                                    gap_parts.append(
                                        f"{crypto.upper()}_{side_label.upper()} "
                                        f"gap={gap:.3f}"
                                    )
                break  # only current window

            print(f"\033[2J\033[H", end="")
            print(f"═══ GapBot [{mode}] ═══  {now_str} UTC  │  WS {pm_status}")
            print(f"{'─' * 80}")
            # prices on 2 rows
            half = len(price_parts) // 2
            if price_parts:
                print(f"  {' | '.join(price_parts[:half or len(price_parts)])}")
            if half:
                print(f"  {' | '.join(price_parts[half:])}")
            if gap_parts:
                print(f"  ⚡ {' | '.join(gap_parts)}")
            print(f"  {status}")
            print(f"{'─' * 80}")
            print(f"  Trades: {bot._trade_count} | "
                  f"Wins: {bot._win_count} | "
                  f"PnL: ${bot._total_pnl:+.2f}")
            print(f"  Log: {LOG_FILE} | Trades: {TRADE_LOG}")
            print(f"  Press Ctrl+C to stop")

            time.sleep(TICK_INTERVAL)

        except KeyboardInterrupt:
            log(f"\nStopping...")
            if bot._position and not bot._position.get("dry_run"):
                log("  Closing open position...")
                bot._sell_position("shutdown")
            if bot._sell_worker.pending > 0:
                log(f"  Draining {bot._sell_worker.pending} background sell(s)...")
                bot._sell_worker.drain(timeout=20)
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
            if bal and bot._start_balance:
                log(f"Final balance: ${bal:.2f} "
                    f"(Δ{bal - bot._start_balance:+.2f})")
            log(f"Trades: {bot._trade_count} | Wins: {bot._win_count} | "
                f"PnL: ${bot._total_pnl:+.2f}")
            break
        except Exception as e:
            log(f"⚠ Main loop error: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5)


if __name__ == "__main__":
    main()
