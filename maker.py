#!/usr/bin/env python3
"""
Maker Bot — Book-imbalance-driven market-making on Polymarket 5-min crypto markets.

Strategy:
  1. Monitor order books via Market WS for BTC, ETH, SOL, XRP 5-min markets
  2. Compute book imbalance = (bid_depth - ask_depth) / (bid_depth + ask_depth)
  3. When imbalance is strongly bullish AND trade flow (OFI) confirms:
     → Post GTC limit BUY at best_bid (maker = 0% fee)
  4. When imbalance is strongly bearish AND trade flow confirms:
     → Post GTC limit SELL at best_ask (or buy DOWN token)
  5. On fill, immediately post a limit exit on the other side for profit
  6. Risk: cancel + market exit on stop-loss or timeout

Key advantage over taker strategy:
  - Maker orders = 0% fee (vs formula-based taker fee)
  - Buy at bid, sell at ask = earn the spread instead of paying it
  - Book imbalance signal tells us WHICH side to quote on
  - OFI + intensity confirm the signal is real (not spoofed)

Usage:
  python maker.py              # live trading
  python maker.py --dry-run    # simulate (no real orders)
"""

import os, sys, time, json, math, threading, asyncio, requests
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta
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
# CONFIG
# =========================================================================
HOST            = "https://clob.polymarket.com"
GAMMA_API       = "https://gamma-api.polymarket.com"
WS_MARKET_URL   = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
WS_USER_URL     = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
CHAIN_ID        = 137
FUNDER_ADDRESS  = founder_address

# ── Signal parameters (from deep_analyze.py findings) ──
IMBALANCE_THRESHOLD  = 0.30   # |imbalance| must exceed this (top/bottom ~30%)
OFI_CONFIRM          = True   # require OFI to agree with imbalance direction
OFI_WINDOW           = 5      # seconds of net volume to compute OFI
INTENSITY_CONFIRM    = True   # require above-median trade intensity
INTENSITY_WINDOW     = 5      # seconds
MIN_MID_PRICE        = 0.20   # don't trade when mid < this (resolved/dead)
MAX_MID_PRICE        = 0.80   # don't trade when mid > this (signal weakens/reverses)
DEPTH_RANGE          = 0.05   # how far from BBA to measure depth (cents)
MIN_DEPTH            = 200    # minimum total depth ($) to trust signal
MAX_SPREAD           = 0.03   # skip if spread > 3c (too illiquid)

# ── Position sizing ──
BASE_BET             = 15     # $ per trade
MAX_BET              = 40     # cap per trade
BET_FRACTION         = 0.15   # fraction of balance per trade

# ── Execution ──
FILL_TIMEOUT         = 8      # seconds to wait for limit fill
EXIT_PROFIT_TARGET   = 0.02   # post limit sell at entry + this (2c)
EXIT_STOP_LOSS       = 0.03   # market exit if price drops this much (3c)
EXIT_TIMEOUT         = 30     # max hold seconds before forced exit
COOLDOWN_PER_TOKEN   = 10     # don't re-enter same token for N seconds
COOLDOWN_PER_MARKET  = 8      # don't re-enter same market for N seconds
WS_WARMUP           = 8       # seconds after WS connect before firing signals

# ── Risk ──
MAX_CONCURRENT       = 3      # max open positions at once
MIN_BALANCE          = 5      # don't trade below this USDC balance
MIN_TIME_REMAINING   = 40     # don't enter markets with < N seconds left

# ── Markets ──
CRYPTOS = ["btc", "eth", "sol", "xrp"]
INTERVALS = [5]               # 5-minute markets only for now

DRY_RUN = "--dry-run" in sys.argv
LOG_FILE = "maker_dry_log.txt" if DRY_RUN else "maker_log.txt"
TRADE_FILE = "maker_dry_trades.json" if DRY_RUN else "maker_trades.json"


def log(msg):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


# =========================================================================
# CLOB CLIENT
# =========================================================================
client = ClobClient(
    host=HOST, key=private_key, chain_id=CHAIN_ID,
    signature_type=1, funder=FUNDER_ADDRESS,
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
    """Cancel open orders, optionally filtered by token."""
    try:
        orders = client.get_orders(OpenOrderParams())
        for o in orders:
            if token_id is None or o.get("asset_id") == token_id:
                try:
                    client.cancel(o.get("id"))
                except Exception:
                    pass
    except Exception:
        pass


# =========================================================================
# MARKET DISCOVERY
# =========================================================================
def discover_markets():
    """Find all active 5-min crypto updown markets."""
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
                    tokens = raw_tokens
                    if len(tokens) != 2:
                        continue
                    condition_id = m.get("conditionId", "")
                    # Determine UP vs DOWN token
                    raw_outcomes = m.get("outcomes", [])
                    if isinstance(raw_outcomes, str):
                        try:
                            outcomes = json.loads(raw_outcomes)
                        except Exception:
                            outcomes = raw_outcomes
                    else:
                        outcomes = raw_outcomes
                    # outcomes = ["Up", "Down"] typically
                    up_idx = 0
                    if len(outcomes) >= 2:
                        for i, o in enumerate(outcomes):
                            if "up" in o.lower():
                                up_idx = i
                                break
                    down_idx = 1 - up_idx
                    markets.append({
                        "crypto": crypto.upper(),
                        "interval": interval,
                        "epoch": epoch,
                        "slug": slug,
                        "condition_id": condition_id,
                        "up_token": tokens[up_idx],
                        "down_token": tokens[down_idx],
                        "window_end": window_end,
                        "secs_left": secs_left,
                        "question": m.get("question", ""),
                    })
            except Exception:
                continue
    return markets


# =========================================================================
# ORDER BOOK TRACKER
# =========================================================================
class BookTracker:
    """
    Maintains full order books from WS snapshots + deltas.
    Computes imbalance, OFI, and trade intensity in real-time.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._books = {}          # token_id -> {"bids": {price: size}, "asks": {price: size}, "bb": float, "ba": float}
        self._trades = {}         # token_id -> deque of (ts, side, size)
        self._last_update = {}    # token_id -> timestamp
        self._ws_connect_time = 0

    def on_book_snapshot(self, token_id, bids, asks):
        """Process full book snapshot."""
        with self._lock:
            self._books[token_id] = {
                "bids": {float(b["price"]): float(b["size"]) for b in bids},
                "asks": {float(a["price"]): float(a["size"]) for a in asks},
                "bb": max((float(b["price"]) for b in bids), default=0),
                "ba": min((float(a["price"]) for a in asks), default=1),
            }
            self._last_update[token_id] = time.time()

    def on_book_delta(self, token_id, side, price, size, best_bid, best_ask):
        """Process incremental book update."""
        price = float(price)
        size = float(size)
        bb = float(best_bid)
        ba = float(best_ask)

        with self._lock:
            if token_id not in self._books:
                self._books[token_id] = {"bids": {}, "asks": {}, "bb": 0, "ba": 1}
            book = self._books[token_id]

            if side.upper() == "BUY":
                if size == 0:
                    book["bids"].pop(price, None)
                else:
                    book["bids"][price] = size
            else:
                if size == 0:
                    book["asks"].pop(price, None)
                else:
                    book["asks"][price] = size

            book["bb"] = bb
            book["ba"] = ba
            self._last_update[token_id] = time.time()

    def on_trade(self, token_id, side, size, timestamp):
        """Record a trade fill for OFI computation."""
        with self._lock:
            if token_id not in self._trades:
                self._trades[token_id] = deque(maxlen=500)
            self._trades[token_id].append((time.time(), side.upper(), float(size)))

    def get_signal(self, token_id):
        """
        Compute the full signal state for a token.
        Returns dict with imbalance, ofi, intensity, mid, spread, etc.
        Returns None if data insufficient.
        """
        now = time.time()

        # Warmup check
        if now - self._ws_connect_time < WS_WARMUP:
            return None

        with self._lock:
            book = self._books.get(token_id)
            if not book:
                return None
            if now - self._last_update.get(token_id, 0) > 30:
                return None

            bb = book["bb"]
            ba = book["ba"]
            if bb <= 0 or ba <= 0 or ba <= bb:
                return None

            mid = (bb + ba) / 2
            spread = ba - bb

            if spread > MAX_SPREAD:
                return None
            if mid < MIN_MID_PRICE or mid > MAX_MID_PRICE:
                return None

            # Compute near-money depth
            bid_depth = sum(s for p, s in book["bids"].items() if p >= bb - DEPTH_RANGE)
            ask_depth = sum(s for p, s in book["asks"].items() if p <= ba + DEPTH_RANGE)
            total = bid_depth + ask_depth

            if total < MIN_DEPTH:
                return None

            imbalance = (bid_depth - ask_depth) / total  # -1 to +1

            # OFI: net buy volume over window
            trades = self._trades.get(token_id, deque())
            cutoff = now - OFI_WINDOW
            buy_vol = 0
            sell_vol = 0
            trade_count = 0
            for ts, side, sz in trades:
                if ts >= cutoff:
                    trade_count += 1
                    if side == "BUY":
                        buy_vol += sz
                    else:
                        sell_vol += sz
            ofi = buy_vol - sell_vol

            # Intensity: trades per second over window
            intensity = trade_count / max(OFI_WINDOW, 1)

        return {
            "mid": mid,
            "spread": spread,
            "bb": bb,
            "ba": ba,
            "bid_depth": bid_depth,
            "ask_depth": ask_depth,
            "imbalance": imbalance,
            "ofi": ofi,
            "buy_vol": buy_vol,
            "sell_vol": sell_vol,
            "intensity": intensity,
            "trade_count": trade_count,
        }


# =========================================================================
# POSITION MANAGER
# =========================================================================
class PositionManager:
    """Tracks open positions and handles exits."""

    def __init__(self):
        self._lock = threading.Lock()
        self.positions = {}       # token_id -> position dict
        self.cooldowns = {}       # token_id -> last_entry_time
        self.market_cooldowns = {}  # condition_id -> last_entry_time
        self.trade_count = 0
        self.total_pnl = 0.0
        self.session_start = time.time()

    @property
    def n_open(self):
        with self._lock:
            return len(self.positions)

    def can_enter(self, token_id, condition_id):
        now = time.time()
        with self._lock:
            if len(self.positions) >= MAX_CONCURRENT:
                return False
            if token_id in self.positions:
                return False
            if now - self.cooldowns.get(token_id, 0) < COOLDOWN_PER_TOKEN:
                return False
            if now - self.market_cooldowns.get(condition_id, 0) < COOLDOWN_PER_MARKET:
                return False
        return True

    def open_position(self, token_id, condition_id, side, entry_price, shares, cost, order_id=None, dry_run=False):
        with self._lock:
            self.positions[token_id] = {
                "token_id": token_id,
                "condition_id": condition_id,
                "side": side,
                "entry_price": entry_price,
                "shares": shares,
                "cost": cost,
                "entry_time": time.time(),
                "order_id": order_id,
                "exit_order_id": None,
                "dry_run": dry_run,
                "status": "open",
            }
            self.cooldowns[token_id] = time.time()
            self.market_cooldowns[condition_id] = time.time()

    def close_position(self, token_id, exit_price, reason):
        with self._lock:
            pos = self.positions.pop(token_id, None)
            if not pos:
                return None

        pnl = (exit_price - pos["entry_price"]) * pos["shares"]
        if pos["side"] == "SELL":
            pnl = -pnl  # short position

        hold_time = time.time() - pos["entry_time"]

        self.trade_count += 1
        self.total_pnl += pnl

        trade_record = {
            "time": datetime.now(timezone.utc).isoformat(),
            "token_id": token_id[:20] + "...",
            "side": pos["side"],
            "entry_price": pos["entry_price"],
            "exit_price": exit_price,
            "shares": pos["shares"],
            "cost": pos["cost"],
            "pnl": round(pnl, 4),
            "hold_secs": round(hold_time, 1),
            "reason": reason,
            "dry_run": pos["dry_run"],
        }

        # Save trade
        try:
            existing = []
            if os.path.exists(TRADE_FILE):
                with open(TRADE_FILE) as f:
                    existing = json.load(f)
            existing.append(trade_record)
            with open(TRADE_FILE, "w") as f:
                json.dump(existing, f, indent=2)
        except Exception:
            pass

        return trade_record

    def get_positions(self):
        with self._lock:
            return dict(self.positions)


# =========================================================================
# MARKET WEBSOCKET
# =========================================================================
class MarketWS:
    """Connects to Polymarket Market WS and dispatches events to BookTracker."""

    def __init__(self, tracker: BookTracker):
        self.tracker = tracker
        self._tokens = set()
        self._connected = False
        self._ws = None
        self._force_reconnect = False

    def start(self):
        t = threading.Thread(target=self._run_loop, daemon=True)
        t.start()

    def subscribe(self, token_ids):
        new = set(token_ids)
        if new != self._tokens:
            self._tokens = new
            self._force_reconnect = True

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

        while True:
            if not self._tokens:
                await asyncio.sleep(1)
                continue

            try:
                async with websockets.connect(
                    WS_MARKET_URL, close_timeout=5, open_timeout=10,
                    max_size=10 * 1024 * 1024
                ) as ws:
                    self._ws = ws
                    self._connected = True
                    self._force_reconnect = False
                    self.tracker._ws_connect_time = time.time()

                    # Subscribe
                    sub = {"type": "market", "assets_ids": list(self._tokens)}
                    await ws.send(json.dumps(sub))
                    log(f"  📡 Market WS: subscribed to {len(self._tokens)} tokens")

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
                                # Full snapshot
                                for asset_id, book_data in msg.items():
                                    if asset_id == "event_type":
                                        continue
                                    if isinstance(book_data, dict):
                                        bids = book_data.get("bids", [])
                                        asks = book_data.get("asks", [])
                                        if bids or asks:
                                            self.tracker.on_book_snapshot(asset_id, bids, asks)

                            elif etype == "price_change":
                                changes = msg.get("price_changes", [])
                                for ch in changes:
                                    self.tracker.on_book_delta(
                                        ch["asset_id"], ch["side"],
                                        ch["price"], ch["size"],
                                        ch.get("best_bid", 0),
                                        ch.get("best_ask", 0),
                                    )

                            elif etype == "last_trade_price":
                                asset_id = msg.get("asset_id", "")
                                if asset_id:
                                    self.tracker.on_trade(
                                        asset_id,
                                        msg.get("side", ""),
                                        msg.get("size", 0),
                                        msg.get("timestamp", ""),
                                    )

                    log("  🔄 Market WS: reconnecting (token change)")

            except Exception as e:
                log(f"  ⚠ Market WS error: {e}")
                self._connected = False
                await asyncio.sleep(2)


# =========================================================================
# MAIN BOT
# =========================================================================
class MakerBot:
    """
    Main trading loop:
      1. Discover markets every 5 minutes
      2. Subscribe to order books
      3. Scan for signals every 100ms
      4. Enter positions via limit orders
      5. Manage exits (profit target, stop loss, timeout)
    """

    def __init__(self):
        self.tracker = BookTracker()
        self.ws = MarketWS(self.tracker)
        self.pm = PositionManager()
        self.markets = []
        self.token_to_market = {}   # token_id -> market dict
        self.last_discovery = 0
        self.last_status = 0
        self.balance = 0

    def run(self):
        log(f"{'=' * 60}")
        log(f"  MAKER BOT {'(DRY RUN)' if DRY_RUN else '(LIVE)'}")
        log(f"{'=' * 60}")

        self.balance = get_usdc_balance() or 0
        log(f"  Balance: ${self.balance:.2f}")
        log(f"  Config: bet=${BASE_BET}, max=${MAX_BET}, "
            f"imb_thresh={IMBALANCE_THRESHOLD}, spread≤{MAX_SPREAD}")
        log(f"  Markets: {', '.join(CRYPTOS)} × {INTERVALS}")

        self.ws.start()
        self._discover_and_subscribe()

        try:
            while True:
                self._tick()
                time.sleep(0.1)  # 100ms main loop
        except KeyboardInterrupt:
            log("\n  🛑 Shutting down...")
            self._cleanup()
            log(f"  Session: {self.pm.trade_count} trades, "
                f"P&L: ${self.pm.total_pnl:+.2f}")

    def _discover_and_subscribe(self):
        """Find markets and subscribe WS."""
        now = time.time()
        if now - self.last_discovery < 30:  # re-discover every 30s
            return

        self.last_discovery = now
        markets = discover_markets()
        self.markets = [m for m in markets if m["secs_left"] > MIN_TIME_REMAINING]

        if not self.markets:
            return

        # Build token -> market mapping
        all_tokens = set()
        self.token_to_market = {}
        for m in self.markets:
            for tok in [m["up_token"], m["down_token"]]:
                all_tokens.add(tok)
                self.token_to_market[tok] = m

        self.ws.subscribe(all_tokens)

        if len(self.markets) != getattr(self, '_last_market_count', 0):
            cryptos = set(m["crypto"] for m in self.markets)
            log(f"  🔍 Tracking {len(self.markets)} markets: {', '.join(sorted(cryptos))}")
            self._last_market_count = len(self.markets)

    def _tick(self):
        """Main loop tick — check signals, manage positions, periodic tasks."""
        now = time.time()

        # Periodic discovery
        self._discover_and_subscribe()

        # Periodic status
        if now - self.last_status > 30:
            self.last_status = now
            runtime = now - self.pm.session_start
            runtime_min = runtime / 60
            trades_per_hour = self.pm.trade_count / max(runtime / 3600, 0.001)
            log(f"  📊 {runtime_min:.0f}m | trades={self.pm.trade_count} "
                f"({trades_per_hour:.0f}/hr) | "
                f"pnl=${self.pm.total_pnl:+.2f} | "
                f"open={self.pm.n_open}/{MAX_CONCURRENT} | "
                f"markets={len(self.markets)}")

        # Check exits for open positions
        self._check_exits()

        # Scan for entry signals
        if self.pm.n_open < MAX_CONCURRENT:
            self._scan_entries()

    def _scan_entries(self):
        """Scan all tracked tokens for entry signals."""
        for token_id, market in self.token_to_market.items():
            # Quick check: enough time left?
            secs_left = (market["window_end"] - datetime.now(timezone.utc)).total_seconds()
            if secs_left < MIN_TIME_REMAINING:
                continue

            # Can we enter this token?
            if not self.pm.can_enter(token_id, market["condition_id"]):
                continue

            signal = self.tracker.get_signal(token_id)
            if signal is None:
                continue

            imb = signal["imbalance"]
            ofi = signal["ofi"]
            intensity = signal["intensity"]

            # ── ENTRY LOGIC ──
            # Strong bullish: imbalance positive + OFI positive
            if imb >= IMBALANCE_THRESHOLD:
                if OFI_CONFIRM and ofi <= 0:
                    continue
                if INTENSITY_CONFIRM and intensity < 1.0:  # at least 1 trade/sec
                    continue
                self._enter(token_id, market, "BUY", signal)
                return  # one entry per tick

            # Strong bearish: imbalance negative + OFI negative
            elif imb <= -IMBALANCE_THRESHOLD:
                if OFI_CONFIRM and ofi >= 0:
                    continue
                if INTENSITY_CONFIRM and intensity < 1.0:
                    continue
                # For bearish signal, buy the DOWN token instead
                if token_id == market["up_token"]:
                    down_token = market["down_token"]
                    down_signal = self.tracker.get_signal(down_token)
                    if down_signal and self.pm.can_enter(down_token, market["condition_id"]):
                        self._enter(down_token, market, "BUY", down_signal,
                                    reason_prefix="BEAR→DOWN")
                        return

    def _enter(self, token_id, market, side, signal, reason_prefix="BULL"):
        """Place a limit entry order."""
        bb = signal["bb"]
        ba = signal["ba"]
        mid = signal["mid"]
        spread = signal["spread"]
        imb = signal["imbalance"]
        ofi = signal["ofi"]

        # Determine entry price: post at best bid (maker)
        if side == "BUY":
            entry_price = bb  # post at best bid
        else:
            entry_price = ba  # post at best ask

        if entry_price <= 0 or entry_price >= 1:
            return

        # Position sizing
        self.balance = get_usdc_balance() or self.balance
        if self.balance < MIN_BALANCE:
            return

        bet = min(BASE_BET, self.balance * BET_FRACTION, MAX_BET)
        if bet < 2:
            return

        shares = round(bet / entry_price, 2)

        crypto = market["crypto"]
        secs = (market["window_end"] - datetime.now(timezone.utc)).total_seconds()

        log(f"  ⚡ {reason_prefix} {crypto} {side} | "
            f"imb={imb:+.3f} ofi={ofi:+.0f} int={signal['intensity']:.1f} | "
            f"entry={entry_price} shares={shares:.1f} bet=${bet:.2f} | "
            f"spread={spread:.3f} secs_left={secs:.0f}")

        if DRY_RUN:
            log(f"    🧪 DRY RUN — {shares:.1f}sh @ {entry_price}")
            self.pm.open_position(
                token_id, market["condition_id"], side,
                entry_price, shares, bet, dry_run=True)
            return

        # Place GTC limit order (maker, 0% fee)
        try:
            order_args = OrderArgs(
                price=entry_price,
                size=shares,
                side=BUY if side == "BUY" else SELL,
                token_id=token_id,
            )
            signed = client.create_order(order_args)
            resp = client.post_order(signed, OrderType.GTC)
            order_id = resp.get("orderID", "") if isinstance(resp, dict) else ""

            if not order_id:
                log(f"    ⚠ Order rejected: {resp}")
                return

            log(f"    📝 Limit {side} posted: {order_id[:16]}...")
            self.pm.open_position(
                token_id, market["condition_id"], side,
                entry_price, shares, bet, order_id=order_id)

            # Start fill monitor thread
            threading.Thread(
                target=self._monitor_fill,
                args=(token_id, order_id, entry_price, shares),
                daemon=True).start()

        except Exception as e:
            log(f"    ⚠ Order error: {e}")

    def _monitor_fill(self, token_id, order_id, entry_price, shares):
        """
        Monitor if our limit order gets filled.
        If filled → post limit exit.
        If timeout → cancel order.
        """
        start = time.time()
        pre_bal = get_share_balance(token_id) or 0

        while time.time() - start < FILL_TIMEOUT:
            time.sleep(0.5)

            # Check if we have shares now
            bal = get_share_balance(token_id) or 0
            if bal > pre_bal + 0.5:
                actual_shares = bal - pre_bal
                log(f"    ✅ Filled: {actual_shares:.1f}sh @ {entry_price}")

                # Update position with actual fills
                with self.pm._lock:
                    pos = self.pm.positions.get(token_id)
                    if pos:
                        pos["shares"] = actual_shares
                        pos["status"] = "filled"

                # Post exit order
                self._post_exit(token_id, entry_price, actual_shares)
                return

        # Timeout — cancel unfilled order
        log(f"    ⏰ Fill timeout, cancelling {order_id[:16]}...")
        try:
            client.cancel(order_id)
        except Exception:
            pass

        # Check if partially filled
        bal = get_share_balance(token_id) or 0
        filled = bal - pre_bal
        if filled > 0.5:
            log(f"    📦 Partial fill: {filled:.1f}sh")
            with self.pm._lock:
                pos = self.pm.positions.get(token_id)
                if pos:
                    pos["shares"] = filled
                    pos["status"] = "filled"
            self._post_exit(token_id, entry_price, filled)
        else:
            # No fill — remove position
            self.pm.close_position(token_id, entry_price, "no_fill")
            log(f"    ❌ No fill, position removed")

    def _post_exit(self, token_id, entry_price, shares):
        """Post a limit sell at entry + profit target."""
        exit_price = round(entry_price + EXIT_PROFIT_TARGET, 2)
        if exit_price > 0.99:
            exit_price = 0.99

        if DRY_RUN:
            log(f"    🎯 DRY: exit limit sell @ {exit_price}")
            return

        try:
            order_args = OrderArgs(
                price=exit_price,
                size=shares,
                side=SELL,
                token_id=token_id,
            )
            signed = client.create_order(order_args)
            resp = client.post_order(signed, OrderType.GTC)
            exit_oid = resp.get("orderID", "") if isinstance(resp, dict) else ""

            if exit_oid:
                log(f"    🎯 Exit limit sell @ {exit_price}: {exit_oid[:16]}...")
                with self.pm._lock:
                    pos = self.pm.positions.get(token_id)
                    if pos:
                        pos["exit_order_id"] = exit_oid
            else:
                log(f"    ⚠ Exit order failed: {resp}")
        except Exception as e:
            log(f"    ⚠ Exit order error: {e}")

    def _check_exits(self):
        """Check all open positions for exit conditions."""
        positions = self.pm.get_positions()
        now = time.time()

        for token_id, pos in positions.items():
            hold_time = now - pos["entry_time"]
            entry = pos["entry_price"]

            # Get current price
            signal = self.tracker.get_signal(token_id)
            if signal is None:
                # No data — check timeout only
                if hold_time > EXIT_TIMEOUT:
                    self._force_exit(token_id, pos, "timeout_no_data")
                continue

            current_mid = signal["mid"]

            if DRY_RUN:
                # Dry run exit logic
                profit = current_mid - entry
                if pos["side"] == "SELL":
                    profit = -profit

                if profit >= EXIT_PROFIT_TARGET:
                    exit_price = entry + EXIT_PROFIT_TARGET
                    trade = self.pm.close_position(token_id, exit_price, "take_profit")
                    if trade:
                        log(f"    💰 DRY TP: {trade['pnl']:+.4f} ({trade['hold_secs']:.1f}s)")
                elif profit <= -EXIT_STOP_LOSS:
                    trade = self.pm.close_position(token_id, current_mid, "stop_loss")
                    if trade:
                        log(f"    🛑 DRY SL: {trade['pnl']:+.4f} ({trade['hold_secs']:.1f}s)")
                elif hold_time > EXIT_TIMEOUT:
                    trade = self.pm.close_position(token_id, current_mid, "timeout")
                    if trade:
                        log(f"    ⏰ DRY TO: {trade['pnl']:+.4f} ({trade['hold_secs']:.1f}s)")
                continue

            # Live exit checks
            price_change = current_mid - entry
            if pos["side"] == "SELL":
                price_change = -price_change

            # Stop loss — market sell immediately
            if price_change <= -EXIT_STOP_LOSS:
                self._force_exit(token_id, pos, "stop_loss")

            # Timeout — market sell
            elif hold_time > EXIT_TIMEOUT:
                self._force_exit(token_id, pos, "timeout")

            # TP is handled by the limit exit order (passive)

    def _force_exit(self, token_id, pos, reason):
        """Force exit via cancel + market sell."""
        log(f"    🚨 Force exit ({reason}): {token_id[:20]}...")

        # Cancel any open exit order
        if pos.get("exit_order_id"):
            try:
                client.cancel(pos["exit_order_id"])
            except Exception:
                pass

        if not DRY_RUN:
            cancel_all_orders(token_id)

            # Market sell remaining shares
            bal = get_share_balance(token_id) or 0
            if bal > 0.5:
                try:
                    from py_clob_client import MarketOrderArgs
                    mo = MarketOrderArgs(
                        token_id=token_id,
                        amount=bal,
                        side=SELL,
                        order_type=OrderType.FAK,
                    )
                    signed = client.create_market_order(mo)
                    client.post_order(signed, OrderType.FAK)
                    log(f"    📤 Market sold {bal:.1f}sh")
                except Exception as e:
                    log(f"    ⚠ Market sell error: {e}")

        # Get approximate exit price from book
        signal = self.tracker.get_signal(token_id)
        exit_price = signal["bb"] if signal else pos["entry_price"]

        trade = self.pm.close_position(token_id, exit_price, reason)
        if trade:
            log(f"    {'🛑' if trade['pnl'] < 0 else '💰'} Exit: "
                f"pnl=${trade['pnl']:+.4f} hold={trade['hold_secs']:.1f}s ({reason})")

    def _cleanup(self):
        """Cancel all orders on shutdown."""
        if not DRY_RUN:
            log("  Cancelling all open orders...")
            cancel_all_orders()


# =========================================================================
# ENTRY POINT
# =========================================================================
if __name__ == "__main__":
    bot = MakerBot()
    bot.run()
