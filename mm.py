#!/usr/bin/env python3
"""
mm.py — Spread-Capture Market Maker for Polymarket 5-min crypto markets.

Strategy:
  1. Monitor order books for BTC, ETH, SOL, XRP 5-min markets via WS
  2. When spread >= MIN_SPREAD: post limit BUY at best_bid (maker, 0% fee)
  3. On fill: post limit SELL at entry + SPREAD_TARGET (maker, 0% fee)
  4. Exits:
     - Sell limit fills → "spread_captured" (pure profit, 0% fee both sides)
     - Hold > MAX_HOLD → cancel sell, market-sell ("timeout")
     - Window ending → cancel sell, market-sell ("window_end")

What's different from old maker.py (142 trades, -$34.74):
  - NO stop-loss (old 8¢ SL caused -$40 of losses)
  - NO adverse-fill panic-selling (old logic caused -$15 of losses)
  - NO directional signal needed (old imbalance/OFI added complexity, not edge)
  - Longer fill timeout (30s vs 8s → fewer wasted entries)
  - Longer hold time (120s vs 25s → more time for spread capture)
  - Position thread manages full lifecycle (cleaner than scattered checks)

Key edge:
  Maker orders = 0% fee. Buy at bid, sell at ask = earn the full spread.
  Old bot's timeout exits (no SL, just hold & sell) were 75% WR, +$1.74 avg.

Usage:
  python mm.py              # live trading
  python mm.py --dry-run    # simulate (no real orders)
"""

import os, sys, time, json, math, threading, asyncio, requests
from collections import deque
from datetime import datetime, timezone, timedelta
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    OrderArgs, OrderType, OpenOrderParams,
    BalanceAllowanceParams, AssetType,
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
CHAIN_ID        = 137

# ── Spread capture ──
SPREAD_TARGET   = 0.02     # post SELL at entry + this (2¢)
MIN_SPREAD      = 0.02     # only enter if book spread >= this
MIN_MID         = 0.20     # skip dead/resolved tokens
MAX_MID         = 0.80     # skip tokens near 1 (thin spread)
MIN_DEPTH       = 100      # minimum total depth ($) near BBA

# ── Sizing ──
BET_SIZE        = 25       # $ per trade
MAX_BET         = 40       # cap
MIN_SHARES      = 5        # Polymarket minimum order
TICK            = 0.01     # price tick

# ── Timing ──
FILL_WAIT       = 30       # seconds to wait for buy fill
MAX_HOLD        = 120      # seconds after fill before force-exit
EXIT_BUFFER     = 60       # force-exit this many seconds before window end
MIN_ENTRY_TIME  = 150      # only enter if >= this many seconds remain
WS_WARMUP       = 8        # WS warm-up period
COOLDOWN        = 15       # don't re-enter same token for N seconds

# ── Risk ──
MAX_POSITIONS   = 4        # max simultaneous positions
MIN_BALANCE     = 5        # stop trading below this USDC

# ── Markets ──
CRYPTOS         = ["btc", "eth", "sol", "xrp"]
INTERVALS       = [5]

DRY_RUN   = "--dry-run" in sys.argv
LOG_FILE  = "mm_dry_log.txt" if DRY_RUN else "mm_log.txt"
TRADE_FILE = "mm_dry_trades.json" if DRY_RUN else "mm_trades.json"


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
        log(f"  ⚠ Balance error: {e}")
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
        log(f"  ⚠ cancel error: {e}")
        return False


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
                        "window_end": window_end,
                        "secs_left": secs_left,
                    })
            except Exception:
                continue
    return markets


# =========================================================================
# BOOK TRACKER
# =========================================================================
class BookTracker:
    """Maintains order books from WS snapshots + deltas."""

    def __init__(self):
        self._lock = threading.Lock()
        self._books = {}
        self._last_update = {}
        self.ws_start_time = 0

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
        """Return {bb, ba, mid, spread, bid_depth, ask_depth} or None."""
        with self._lock:
            if time.time() - self.ws_start_time < WS_WARMUP:
                return None
            book = self._books.get(token_id)
            if not book:
                return None
            if time.time() - self._last_update.get(token_id, 0) > 30:
                return None
            bb, ba = book["bb"], book["ba"]
            if bb <= 0 or ba <= 0 or ba <= bb:
                return None
            mid = (bb + ba) / 2
            spread = ba - bb
            bid_depth = sum(s for p, s in book["bids"].items() if p >= bb - 0.05)
            ask_depth = sum(s for p, s in book["asks"].items() if p <= ba + 0.05)
            return {
                "bb": bb, "ba": ba, "mid": mid, "spread": spread,
                "bid_depth": bid_depth, "ask_depth": ask_depth,
            }


# =========================================================================
# MARKET WEBSOCKET
# =========================================================================
class MarketWS:
    """Connect to Polymarket WS for order book data."""

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
        try:
            import websockets
        except ImportError:
            import subprocess
            subprocess.check_call([sys.executable, "-m", "pip", "install", "websockets"])
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
                    log(f"  📡 WS: subscribed to {len(self._tokens)} tokens")

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
                    log("  🔄 WS: reconnecting (token change)")
            except Exception as e:
                log(f"  ⚠ WS error: {e}")
                await asyncio.sleep(2)


# =========================================================================
# POSITION MANAGER
# =========================================================================
class PositionManager:
    """Track open positions with simple lifecycle: pending → filled → closed."""

    def __init__(self):
        self._lock = threading.Lock()
        self._positions = {}     # token_id -> pos dict
        self._cooldowns = {}     # token_id -> last_close_time
        self.trade_count = 0
        self.total_pnl = 0.0
        self.session_start = time.time()

    @property
    def n_open(self):
        with self._lock:
            return len(self._positions)

    def can_enter(self, token_id):
        now = time.time()
        with self._lock:
            if len(self._positions) >= MAX_POSITIONS:
                return False
            if token_id in self._positions:
                return False
            if now - self._cooldowns.get(token_id, 0) < COOLDOWN:
                return False
        return True

    def open(self, token_id, entry_price, shares, cost, order_id, window_end, crypto):
        with self._lock:
            self._positions[token_id] = {
                "token_id": token_id,
                "entry_price": entry_price,
                "shares": shares,
                "cost": cost,
                "order_id": order_id,
                "window_end": window_end,
                "crypto": crypto,
                "entry_time": time.time(),
                "fill_time": None,
                "status": "pending",
            }

    def mark_filled(self, token_id, actual_shares):
        with self._lock:
            pos = self._positions.get(token_id)
            if pos:
                pos["shares"] = actual_shares
                pos["fill_time"] = time.time()
                pos["status"] = "filled"

    def close(self, token_id, exit_price, reason):
        with self._lock:
            pos = self._positions.pop(token_id, None)
            if not pos:
                return None
            self._cooldowns[token_id] = time.time()

        entry = pos["entry_price"]
        shares = pos["shares"]
        pnl = (exit_price - entry) * shares
        hold = time.time() - pos["entry_time"]

        self.trade_count += 1
        self.total_pnl += pnl

        record = {
            "time": datetime.now(timezone.utc).isoformat(),
            "crypto": pos["crypto"],
            "token_id": token_id[:20] + "...",
            "entry_price": entry,
            "exit_price": round(exit_price, 4),
            "shares": round(shares, 2),
            "cost": round(pos["cost"], 4),
            "pnl": round(pnl, 4),
            "hold_secs": round(hold, 1),
            "reason": reason,
            "dry_run": DRY_RUN,
        }
        try:
            existing = []
            if os.path.exists(TRADE_FILE):
                with open(TRADE_FILE) as f:
                    existing = json.load(f)
            existing.append(record)
            with open(TRADE_FILE, "w") as f:
                json.dump(existing, f, indent=2)
        except Exception:
            pass

        return record

    def get_all(self):
        with self._lock:
            return list(self._positions.items())

    def has_position(self, token_id):
        with self._lock:
            return token_id in self._positions


# =========================================================================
# MM BOT
# =========================================================================
class MMBot:
    """
    Main market-maker loop.
    - Discovers markets, subscribes to book data.
    - Scans for spread opportunities and enters via limit buy.
    - Each position runs in its own thread for fill + exit management.
    """

    def __init__(self):
        self.tracker = BookTracker()
        self.ws = MarketWS(self.tracker)
        self.pm = PositionManager()
        self.markets = []
        self.token_to_market = {}
        self.last_discovery = 0
        self.last_status = 0
        self.balance = 0
        self._known_tokens = set()
        self._shutting_down = False

    def run(self):
        log(f"{'=' * 60}")
        log(f"  MM BOT {'(DRY RUN)' if DRY_RUN else '(LIVE)'}")
        log(f"{'=' * 60}")

        self.balance = get_usdc_balance() or 0
        log(f"  Balance: ${self.balance:.2f}")
        log(f"  Config: bet=${BET_SIZE}, spread_target={SPREAD_TARGET}, "
            f"fill_wait={FILL_WAIT}s, max_hold={MAX_HOLD}s")
        log(f"  Markets: {', '.join(CRYPTOS)}")

        self.ws.start()
        self._discover()

        try:
            while True:
                self._tick()
                time.sleep(0.5)
        except KeyboardInterrupt:
            log("\n  🛑 Shutting down...")
            self._shutting_down = True
            time.sleep(1)
            self._cleanup()
            end_balance = get_usdc_balance() or 0
            actual_pnl = end_balance - self.balance
            log(f"  Session: {self.pm.trade_count} trades, "
                f"PnL: ${self.pm.total_pnl:+.2f} (book) | "
                f"${actual_pnl:+.2f} (USDC: ${self.balance:.2f} → ${end_balance:.2f})")

    # ── Discovery ──

    def _discover(self):
        now = time.time()
        if now - self.last_discovery < 30:
            return
        self.last_discovery = now

        markets = discover_markets()
        self.markets = [m for m in markets if m["secs_left"] > EXIT_BUFFER]
        if not self.markets:
            return

        all_tokens = set()
        self.token_to_market = {}
        for m in self.markets:
            for tok in [m["up_token"], m["down_token"]]:
                all_tokens.add(tok)
                self.token_to_market[tok] = m

        self.ws.subscribe(all_tokens)

        if len(self.markets) != getattr(self, "_prev_count", 0):
            cryptos = sorted(set(m["crypto"] for m in self.markets))
            log(f"  🔍 Tracking {len(self.markets)} markets: {', '.join(cryptos)}")
            self._prev_count = len(self.markets)

    # ── Main tick ──

    def _tick(self):
        self._discover()

        # Status every 30s
        now = time.time()
        if now - self.last_status > 30:
            self.last_status = now
            mins = (now - self.pm.session_start) / 60
            tph = self.pm.trade_count / max((now - self.pm.session_start) / 3600, 0.001)
            log(f"  📊 {mins:.0f}m | trades={self.pm.trade_count} ({tph:.0f}/hr) | "
                f"pnl=${self.pm.total_pnl:+.2f} | open={self.pm.n_open}/{MAX_POSITIONS} | "
                f"markets={len(self.markets)}")

        # Scan entries
        if self.pm.n_open < MAX_POSITIONS:
            self._scan_entries()

    # ── Entry scanning ──

    def _scan_entries(self):
        for token_id, market in self.token_to_market.items():
            secs_left = (market["window_end"] - datetime.now(timezone.utc)).total_seconds()
            if secs_left < MIN_ENTRY_TIME:
                continue
            if not self.pm.can_enter(token_id):
                continue

            book = self.tracker.get_book(token_id)
            if not book:
                continue
            if book["spread"] < MIN_SPREAD:
                continue
            if book["mid"] < MIN_MID or book["mid"] > MAX_MID:
                continue
            if book["bid_depth"] + book["ask_depth"] < MIN_DEPTH:
                continue

            self._enter(token_id, market, book)
            return  # one entry per tick

    def _enter(self, token_id, market, book):
        entry_price = round(math.floor(book["bb"] / TICK) * TICK, 2)
        if entry_price <= 0.01 or entry_price >= 0.99:
            return

        self.balance = get_usdc_balance() or self.balance
        if self.balance < MIN_BALANCE:
            return

        bet = min(BET_SIZE, MAX_BET)
        shares = round(bet / entry_price, 2)
        if shares < MIN_SHARES:
            return

        crypto = market["crypto"]
        secs = (market["window_end"] - datetime.now(timezone.utc)).total_seconds()
        log(f"  ⚡ {crypto} BUY @ {entry_price} | "
            f"shares={shares:.1f} bet=${bet:.2f} | "
            f"spread={book['spread']:.3f} mid={book['mid']:.3f} secs_left={secs:.0f}")

        if DRY_RUN:
            self.pm.open(token_id, entry_price, shares, bet, "dry",
                         market["window_end"], crypto)
            self.pm.mark_filled(token_id, shares)
            # Simulate: close at mid after a few seconds
            threading.Thread(
                target=self._dry_run_lifecycle,
                args=(token_id, entry_price, shares, market),
                daemon=True).start()
            return

        try:
            order_args = OrderArgs(
                price=entry_price, size=shares,
                side=BUY, token_id=token_id,
            )
            signed = client.create_order(order_args)
            resp = client.post_order(signed, OrderType.GTC)
            order_id = resp.get("orderID", "") if isinstance(resp, dict) else ""
            if not order_id:
                log(f"    ⚠ Order rejected: {resp}")
                return

            log(f"    📝 Limit BUY posted: {order_id[:16]}...")
            self._known_tokens.add(token_id)
            self.pm.open(token_id, entry_price, shares, bet, order_id,
                         market["window_end"], crypto)

            threading.Thread(
                target=self._position_lifecycle,
                args=(token_id, order_id, entry_price, shares, market),
                daemon=True).start()

        except Exception as e:
            log(f"    ⚠ Order error: {e}")
            self._known_tokens.add(token_id)

    # ── Position lifecycle (one thread per position) ──

    def _position_lifecycle(self, token_id, order_id, entry_price, ordered_shares, market):
        """
        Full lifecycle for one position:
          Phase 1: Wait for buy fill
          Phase 2: Post sell limit, wait for sell fill or force-exit
        """
        crypto = market["crypto"]
        pre_bal = get_share_balance(token_id) or 0
        start = time.time()

        # ─── PHASE 1: Wait for buy limit to fill ───
        actual_shares = 0
        while time.time() - start < FILL_WAIT:
            if self._shutting_down:
                cancel_all_orders(token_id)
                self.pm.close(token_id, entry_price, "shutdown")
                return
            time.sleep(1)

            # Abort if not enough time for a full round trip
            secs_left = (market["window_end"] - datetime.now(timezone.utc)).total_seconds()
            if secs_left < EXIT_BUFFER + 30:
                cancel_all_orders(token_id)
                self.pm.close(token_id, entry_price, "no_fill")
                log(f"    ❌ {crypto} cancelled (window ending)")
                return

            bal = get_share_balance(token_id) or 0
            filled = bal - pre_bal
            if filled >= MIN_SHARES:
                # Cancel remaining buy order
                try:
                    client.cancel(order_id)
                except Exception:
                    pass
                cancel_all_orders(token_id)
                time.sleep(1.5)

                # Re-check for late chunks
                final_bal = get_share_balance(token_id) or 0
                actual_shares = min(max(final_bal - pre_bal, filled), ordered_shares)
                break

        if actual_shares < MIN_SHARES:
            # No fill
            cancel_all_orders(token_id)
            time.sleep(1)
            # Check one last time
            bal = get_share_balance(token_id) or 0
            filled = bal - pre_bal
            if filled >= MIN_SHARES:
                actual_shares = min(filled, ordered_shares)
            else:
                self.pm.close(token_id, entry_price, "no_fill")
                log(f"    ❌ {crypto} no fill after {FILL_WAIT}s")
                return

        self.pm.mark_filled(token_id, actual_shares)
        fill_time = time.time()
        log(f"    ✅ {crypto} filled: {actual_shares:.1f}sh @ {entry_price}")

        # ─── PHASE 2: Post sell limit, monitor until exit ───
        sell_price = round(entry_price + SPREAD_TARGET, 2)
        if sell_price > 0.99:
            sell_price = 0.99

        sell_shares = math.floor(actual_shares * 100) / 100
        sell_posted = False
        if sell_shares >= MIN_SHARES and not self._shutting_down:
            try:
                order_args = OrderArgs(
                    price=sell_price, size=sell_shares,
                    side=SELL, token_id=token_id,
                )
                signed = client.create_order(order_args)
                resp = client.post_order(signed, OrderType.GTC)
                sell_oid = resp.get("orderID", "") if isinstance(resp, dict) else ""
                if sell_oid:
                    log(f"    🎯 {crypto} SELL limit @ {sell_price} ({sell_shares:.1f}sh)")
                    sell_posted = True
                else:
                    log(f"    ⚠ Sell order failed: {resp}")
            except Exception as e:
                log(f"    ⚠ Sell order error: {e}")

        # Monitor: check every 3s if sell filled or if we need to force-exit
        check_interval = 3
        while True:
            if self._shutting_down:
                log(f"    🛑 {crypto} shutdown — force selling")
                self._force_sell(token_id)
                book = self.tracker.get_book(token_id)
                exit_price = book["bb"] if book else entry_price
                trade = self.pm.close(token_id, exit_price, "shutdown")
                if trade:
                    log(f"    {'💰' if trade['pnl'] > 0 else '🔻'} {crypto}: "
                        f"pnl=${trade['pnl']:+.4f} ({trade['hold_secs']:.1f}s)")
                return

            time.sleep(check_interval)

            # Check if sell limit filled (no more shares)
            bal = get_share_balance(token_id) or 0
            if bal < MIN_SHARES:
                trade = self.pm.close(token_id, sell_price, "spread_captured")
                if trade:
                    log(f"    💰 {crypto} SPREAD CAPTURED: "
                        f"pnl=${trade['pnl']:+.4f} ({trade['hold_secs']:.1f}s)")
                return

            hold = time.time() - fill_time
            secs_left = (market["window_end"] - datetime.now(timezone.utc)).total_seconds()

            # Force exit: window ending
            if secs_left < EXIT_BUFFER:
                log(f"    ⏰ {crypto} window ending ({secs_left:.0f}s left)")
                self._force_sell(token_id)
                book = self.tracker.get_book(token_id)
                exit_price = book["bb"] if book else entry_price
                trade = self.pm.close(token_id, exit_price, "window_end")
                if trade:
                    log(f"    {'💰' if trade['pnl'] > 0 else '🔻'} {crypto}: "
                        f"pnl=${trade['pnl']:+.4f} ({trade['hold_secs']:.1f}s)")
                return

            # Force exit: max hold time
            if hold > MAX_HOLD:
                log(f"    ⏰ {crypto} max hold ({hold:.0f}s)")
                self._force_sell(token_id)
                book = self.tracker.get_book(token_id)
                exit_price = book["bb"] if book else entry_price
                trade = self.pm.close(token_id, exit_price, "timeout")
                if trade:
                    log(f"    {'💰' if trade['pnl'] > 0 else '🔻'} {crypto}: "
                        f"pnl=${trade['pnl']:+.4f} ({trade['hold_secs']:.1f}s)")
                return

    def _dry_run_lifecycle(self, token_id, entry_price, shares, market):
        """Simulate position lifecycle in dry run mode."""
        time.sleep(5)
        book = self.tracker.get_book(token_id)
        if book:
            mid = book["mid"]
            if mid >= entry_price + SPREAD_TARGET:
                exit_price = entry_price + SPREAD_TARGET
                reason = "spread_captured"
            else:
                exit_price = mid
                reason = "timeout"
        else:
            exit_price = entry_price
            reason = "timeout_no_data"
        trade = self.pm.close(token_id, exit_price, reason)
        if trade:
            log(f"    🧪 DRY {trade['crypto']}: pnl=${trade['pnl']:+.4f} ({reason})")

    # ── Sell helpers ──

    def _force_sell(self, token_id, max_retries=8):
        """Cancel all orders then sell all shares. Retries until clean."""
        if DRY_RUN:
            return

        for attempt in range(max_retries):
            cancel_all_orders(token_id)
            time.sleep(2 if attempt == 0 else 3)

            bal = get_share_balance(token_id) or 0
            if bal < MIN_SHARES:
                if attempt > 0:
                    log(f"    ✅ All shares sold ({attempt + 1} attempts)")
                return

            sell_size = math.floor(bal * 100) / 100
            if sell_size < MIN_SHARES:
                log(f"    ⚠ {bal:.2f}sh dust (< min {MIN_SHARES})")
                return

            try:
                order_args = OrderArgs(
                    price=0.01, size=sell_size,
                    side=SELL, token_id=token_id,
                )
                signed = client.create_order(order_args)
                client.post_order(signed, OrderType.GTC)
                log(f"    📤 Sell {sell_size:.1f}sh @ market (attempt {attempt + 1})")
            except Exception as e:
                log(f"    ⚠ Sell error (attempt {attempt + 1}): {e}")
                continue

            time.sleep(3)

        bal = get_share_balance(token_id) or 0
        if bal >= MIN_SHARES:
            log(f"    ❌ FAILED to sell {bal:.1f}sh after {max_retries} attempts!")

    # ── Cleanup ──

    def _cleanup(self):
        """Cancel everything and sell all held shares."""
        if DRY_RUN:
            return

        log("  🧹 Cleanup: cancelling all orders...")
        cancel_all_orders()
        time.sleep(3)

        all_tokens = set(self._known_tokens) | set(self.token_to_market.keys())
        for token_id in all_tokens:
            try:
                bal = get_share_balance(token_id) or 0
            except Exception:
                continue
            if bal >= MIN_SHARES:
                log(f"  🧹 Selling {bal:.1f}sh on {token_id[:20]}...")
                self._force_sell(token_id, max_retries=10)
            elif bal > 0.5:
                log(f"  ⚠ Dust: {bal:.1f}sh on {token_id[:20]}... (< min)")

        # Final verification
        time.sleep(3)
        for token_id in all_tokens:
            try:
                bal = get_share_balance(token_id) or 0
                if bal >= MIN_SHARES:
                    log(f"  ⚠ STILL {bal:.1f}sh on {token_id[:20]} — retrying")
                    self._force_sell(token_id, max_retries=5)
            except Exception:
                continue

        log("  ✅ Cleanup complete")


# =========================================================================
# ENTRY POINT
# =========================================================================
if __name__ == "__main__":
    bot = MMBot()
    bot.run()
