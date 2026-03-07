#!/usr/bin/env python3
"""
Maker2 — Spread-capture market-making on Polymarket 5-min crypto markets.

Strategy (NO directional prediction):
  1. Find 5-min crypto Up/Down markets
  2. Monitor order books for tokens priced near 0.50 with spread >= 2¢
  3. Post GTC limit BUY at best_bid (maker = 0% fee)
  4. On fill, post GTC limit SELL at entry + 2¢ (maker = 0% fee)
  5. If TP doesn't fill before resolution:
     - Token resolves $1.00 → profit = (1.00 - entry)
     - Token resolves $0.00 → loss = entry
     - At entry ~0.50 → EV ≈ 0 (breakeven), but TP adds +2¢ edge
  6. Cancel unfilled entry orders if mid moves 3¢ against us

Why this works (theory):
  - 0% maker fee on BOTH sides = pure spread capture
  - No directional prediction needed — works on 50/50 markets
  - Adverse selection is contained: worst case is holding to resolution
  - At near-0.50 prices, resolution EV is ~breakeven
  - Every TP fill is pure profit (2¢/share × shares)
  - Key: only enter when spread ≥ 2¢ so TP price is at or inside the ask

Risk:
  - Token moves far from 0.50 → resolution is no longer 50/50
  - Spread collapses → can't exit profitably  
  - Many fills without TP → stuck holding to resolution (variance)
  - Managed via: strict price range (0.35-0.65), cancel on adverse move, position limits

Usage:
  python maker2.py              # live trading
  python maker2.py --dry-run    # simulate (no real orders)
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
funder_address = os.getenv("FUNDER_ADDRESS")

# =========================================================================
# CONFIG
# =========================================================================
HOST = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CHAIN_ID = 137
FUNDER_ADDRESS = funder_address

CRYPTOS = ["btc", "eth", "sol", "xrp"]

# ── Entry conditions ──
MIN_SPREAD = 0.02             # Only enter when spread >= 2¢ (room for TP)
MAX_SPREAD = 0.06             # Skip if spread > 6¢ (too illiquid / suspicious)
MIN_PRICE = 0.35              # Only buy tokens priced 0.35-0.65 (near 50/50)
MAX_PRICE = 0.65              # Ensures resolution EV is close to breakeven
MIN_DEPTH = 50                # Minimum depth at BBA ($) to trust the book
CANCEL_ADVERSE = 0.03         # Cancel entry if mid moves 3¢ against us

# ── Position sizing ──
BET_AMOUNT = 15               # $ per trade
MAX_BET = 25                  # Cap
MIN_SHARES = 5                # Polymarket minimum
MAX_POSITIONS = 2             # Max concurrent positions
MIN_BALANCE = 8               # Stop trading below this

# ── Exit ──
PROFIT_TARGET = 0.02          # Sell at entry + 2¢ (maker, 0% fee)
FILL_WAIT = 8                 # Seconds to wait for entry fill
COOLDOWN = 20                 # Don't re-enter same token for N seconds
MIN_TIME_REMAINING = 90       # Don't enter with < 90s left in window
MAX_ELAPSED_PCT = 0.60        # Don't enter after 60% of window elapsed

# ── Timing ──
SCAN_INTERVAL = 0.5           # Main loop frequency (seconds)
STATUS_INTERVAL = 30          # Status log frequency
BOOK_STALE_SECS = 15          # Consider book stale after this
WS_WARMUP = 5                 # Wait for WS data before trading

# ── Fee model ──
CRYPTO_FEE_RATE = 0.25
CRYPTO_FEE_EXPONENT = 2

# ── Files ──
DRY_RUN = "--dry-run" in sys.argv
LOG_FILE = "maker2_log.txt"
TRADE_FILE = "maker2_trades.json"


# =========================================================================
# LOGGING
# =========================================================================
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


# =========================================================================
# MARKET DISCOVERY
# =========================================================================
def discover_markets():
    """Find active 5-min crypto up/down markets for both current and next window."""
    markets = []
    now = datetime.now(timezone.utc)

    for offset in range(2):  # current and next window
        aligned = (now.minute // 5) * 5
        window_start = now.replace(minute=aligned, second=0, microsecond=0) + timedelta(minutes=5 * offset)
        window_end = window_start + timedelta(minutes=5)
        epoch = int(window_start.timestamp())

        if window_end < now:
            continue

        for crypto in CRYPTOS:
            slug = f"{crypto}-updown-5m-{epoch}"
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
                        raw_tokens = json.loads(raw_tokens)
                    if len(raw_tokens) != 2:
                        continue
                    raw_outcomes = m.get("outcomes", [])
                    if isinstance(raw_outcomes, str):
                        raw_outcomes = json.loads(raw_outcomes)
                    up_idx = 0
                    for i, o in enumerate(raw_outcomes):
                        if "up" in o.lower():
                            up_idx = i
                            break
                    down_idx = 1 - up_idx

                    markets.append({
                        "crypto": crypto,
                        "epoch": epoch,
                        "condition_id": m.get("conditionId", ""),
                        "up_token": raw_tokens[up_idx],
                        "down_token": raw_tokens[down_idx],
                        "window_start": window_start,
                        "window_end": window_end,
                        "question": m.get("question", ""),
                    })
            except Exception:
                continue
    return markets


# =========================================================================
# BOOK TRACKER (via WebSocket)
# =========================================================================
class BookTracker:
    """Maintains live order books from Polymarket WS. Thread-safe."""

    def __init__(self):
        self._lock = threading.Lock()
        self._books = {}       # token_id -> {"bids": {price: size}, "asks": {price: size}}
        self._last_update = {} # token_id -> timestamp
        self._ws_time = 0      # when WS connected

    def on_snapshot(self, token_id, bids, asks):
        with self._lock:
            self._books[token_id] = {
                "bids": {float(b["price"]): float(b["size"]) for b in bids},
                "asks": {float(a["price"]): float(a["size"]) for a in asks},
            }
            self._last_update[token_id] = time.time()

    def on_delta(self, token_id, side, price, size):
        price, size = float(price), float(size)
        with self._lock:
            if token_id not in self._books:
                self._books[token_id] = {"bids": {}, "asks": {}}
            book = self._books[token_id]
            key = "bids" if side.upper() == "BUY" else "asks"
            if size == 0:
                book[key].pop(price, None)
            else:
                book[key][price] = size
            self._last_update[token_id] = time.time()

    def get_bba(self, token_id):
        """Get best bid/ask and depth. Returns None if stale or unavailable."""
        now = time.time()
        if now - self._ws_time < WS_WARMUP:
            return None

        with self._lock:
            if token_id not in self._books:
                return None
            if now - self._last_update.get(token_id, 0) > BOOK_STALE_SECS:
                return None

            book = self._books[token_id]
            bids = book["bids"]
            asks = book["asks"]
            if not bids or not asks:
                return None

            bb = max(bids.keys())
            ba = min(asks.keys())
            if ba <= bb:
                return None  # crossed book

            # Depth at BBA
            bid_depth = bids.get(bb, 0)
            ask_depth = asks.get(ba, 0)
            mid = (bb + ba) / 2

            return {
                "bb": bb, "ba": ba, "mid": mid,
                "spread": round(ba - bb, 2),
                "bid_depth": bid_depth, "ask_depth": ask_depth,
            }


# =========================================================================
# WEBSOCKET
# =========================================================================
class MarketWS:
    """Connects to Polymarket WS and feeds BookTracker."""

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
                    self.tracker._ws_time = time.time()

                    sub = {"type": "market", "assets_ids": list(self._tokens)}
                    await ws.send(json.dumps(sub))
                    log(f"  📡 WS subscribed to {len(self._tokens)} tokens")

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
                                for asset_id, book_data in msg.items():
                                    if asset_id == "event_type":
                                        continue
                                    if isinstance(book_data, dict):
                                        bids = book_data.get("bids", [])
                                        asks = book_data.get("asks", [])
                                        if bids or asks:
                                            self.tracker.on_snapshot(asset_id, bids, asks)

                            elif etype == "price_change":
                                for ch in msg.get("price_changes", []):
                                    self.tracker.on_delta(
                                        ch["asset_id"], ch["side"],
                                        ch["price"], ch["size"],
                                    )

                    log("  🔄 WS reconnecting (token change)")

            except Exception as e:
                log(f"  ⚠ WS error: {e}")
                await asyncio.sleep(2)


# =========================================================================
# MAIN BOT
# =========================================================================
class Maker2:
    def __init__(self):
        self.tracker = BookTracker()
        self.ws = MarketWS(self.tracker)
        self.markets = {}           # crypto -> market dict (prefer started windows)
        self.token_to_market = {}   # token_id -> market
        self.positions = []         # active positions
        self.completed = []         # finished trades
        self._cooldowns = {}        # token_id -> last_exit_time
        self._lock = threading.Lock()
        self._trade_count = 0
        self._win_count = 0
        self._pnl = 0.0
        self._start_balance = None
        self._last_discovery = 0
        self._last_status = 0
        self._known_tokens = set()
        self._running = True

    # ── Market discovery ──
    def _update_markets(self):
        now_ts = time.time()
        if now_ts - self._last_discovery < 30:
            return
        self._last_discovery = now_ts

        found = discover_markets()
        now = datetime.now(timezone.utc)

        # Prefer active (started) windows
        best = {}
        for m in found:
            crypto = m["crypto"]
            started = m["window_start"] <= now
            remaining = (m["window_end"] - now).total_seconds()
            if remaining < MIN_TIME_REMAINING:
                continue
            prev = best.get(crypto)
            if prev is None:
                best[crypto] = m
            elif started and not (prev["window_start"] <= now):
                best[crypto] = m  # prefer started window
            elif started == (prev["window_start"] <= now) and remaining > (prev["window_end"] - now).total_seconds():
                best[crypto] = m  # prefer more time remaining

        with self._lock:
            self.markets = best
            self.token_to_market = {}
            all_tokens = set()
            for m in best.values():
                for tok in [m["up_token"], m["down_token"]]:
                    all_tokens.add(tok)
                    self.token_to_market[tok] = m

        self.ws.subscribe(all_tokens)

    # ── Entry logic ──
    def _scan_entries(self):
        """Look for tokens with favorable spread to enter."""
        now = datetime.now(timezone.utc)
        now_ts = time.time()

        active = len([p for p in self.positions if not p.get("resolved")])
        if active >= MAX_POSITIONS:
            return

        balance = get_usdc_balance()
        if balance is None or balance < MIN_BALANCE:
            return

        with self._lock:
            candidates = list(self.token_to_market.items())

        for token_id, market in candidates:
            # Check window timing
            remaining = (market["window_end"] - now).total_seconds()
            if remaining < MIN_TIME_REMAINING:
                continue
            elapsed_pct = 1.0 - remaining / 300  # 5-min window
            if elapsed_pct > MAX_ELAPSED_PCT:
                continue

            # Cooldown check
            if now_ts - self._cooldowns.get(token_id, 0) < COOLDOWN:
                continue

            # Already have position on this token?
            has_pos = any(
                p["token_id"] == token_id and not p.get("resolved")
                for p in self.positions
            )
            if has_pos:
                continue

            # Position limit re-check (may have changed)
            active = len([p for p in self.positions if not p.get("resolved")])
            if active >= MAX_POSITIONS:
                return

            # Get book state
            bba = self.tracker.get_bba(token_id)
            if bba is None:
                continue

            bb, ba, mid, spread = bba["bb"], bba["ba"], bba["mid"], bba["spread"]

            # Entry conditions
            if spread < MIN_SPREAD:
                continue
            if spread > MAX_SPREAD:
                continue
            if mid < MIN_PRICE or mid > MAX_PRICE:
                continue
            if bba["bid_depth"] < MIN_DEPTH:
                continue

            # Entry price: at best bid (maker)
            entry_price = bb
            if entry_price <= 0.01 or entry_price >= 0.99:
                continue

            # TP price must be <= best ask (so it can fill)
            tp_price = round(entry_price + PROFIT_TARGET, 2)
            if tp_price > ba:
                continue  # TP would be outside the ask — won't fill easily

            # Sizing
            bet = min(BET_AMOUNT, MAX_BET, balance * 0.25)
            shares = max(MIN_SHARES, math.floor(bet / entry_price))

            crypto = market["crypto"].upper()
            is_up = token_id == market["up_token"]
            direction = "UP" if is_up else "DOWN"

            log(f"\n  📊 ENTRY: {crypto} {direction} | "
                f"bb={bb} ba={ba} spread={spread} mid={mid:.2f} | "
                f"→ BUY {shares}sh @ {entry_price} (TP @ {tp_price})")

            self._execute_entry(token_id, market, direction, entry_price,
                                tp_price, shares, bet)
            return  # one entry per scan

    def _execute_entry(self, token_id, market, direction, entry_price,
                       tp_price, shares, bet):
        """Place limit buy, wait for fill, then post TP sell."""
        if DRY_RUN:
            log(f"     🧪 DRY RUN — would buy {shares}sh @ {entry_price}")
            return

        try:
            # Record pre-buy balance to detect actual fills
            pre_bal = get_share_balance(token_id) or 0

            order = OrderArgs(
                token_id=token_id,
                price=entry_price,
                size=shares,
                side=BUY,
            )
            signed = client.create_order(order)
            resp = client.post_order(signed, OrderType.GTC)
            order_id = resp.get("orderID", "") if isinstance(resp, dict) else ""
            status = resp.get("status", "") if isinstance(resp, dict) else ""

            if not order_id or status == "error":
                log(f"     ⚠ Buy rejected: {resp}")
                return

            log(f"     📝 Limit BUY posted: {order_id[:10]}")
            self._known_tokens.add(token_id)

            # Monitor fill in a thread
            threading.Thread(
                target=self._fill_monitor,
                args=(token_id, market, direction, order_id, entry_price,
                      tp_price, shares, bet, pre_bal),
                daemon=True,
            ).start()

        except Exception as e:
            log(f"     ⚠ Buy error: {e}")

    def _fill_monitor(self, token_id, market, direction, order_id,
                      entry_price, tp_price, shares, bet, pre_bal):
        """Wait for fill, cancel if adverse move, post TP on fill."""
        start = time.time()

        while time.time() - start < FILL_WAIT:
            time.sleep(0.5)

            # Check for adverse move — cancel if mid drops too far
            bba = self.tracker.get_bba(token_id)
            if bba and bba["mid"] < entry_price - CANCEL_ADVERSE:
                log(f"     🛡 Cancel: mid={bba['mid']:.2f} < {entry_price} - {CANCEL_ADVERSE}")
                try:
                    client.cancel(order_id)
                except Exception:
                    pass
                # Check for partial fill
                time.sleep(1)
                bal = get_share_balance(token_id) or 0
                filled = max(0, bal - pre_bal)
                if filled >= MIN_SHARES:
                    log(f"     ⚠ Partial fill on cancel: {filled:.1f}sh")
                    self._create_position(token_id, market, direction,
                                          entry_price, tp_price, filled, bet)
                else:
                    log(f"     🛡 Cancelled before fill")
                return

            # Check for fill
            bal = get_share_balance(token_id) or 0
            filled = max(0, bal - pre_bal)
            if filled >= MIN_SHARES:
                # Cancel remaining order
                try:
                    client.cancel(order_id)
                except Exception:
                    pass
                # Wait for settlement then get final fill
                time.sleep(1)
                final_bal = get_share_balance(token_id) or 0
                actual = max(0, min(final_bal - pre_bal, shares))
                if actual < MIN_SHARES:
                    actual = filled

                log(f"     ✅ Filled: {actual:.1f}sh @ {entry_price}")
                self._create_position(token_id, market, direction,
                                      entry_price, tp_price, actual, bet)
                return

        # Timeout — cancel
        log(f"     ⏰ No fill after {FILL_WAIT}s, cancelling")
        try:
            client.cancel(order_id)
        except Exception:
            pass
        # Check for late fill
        time.sleep(1)
        bal = get_share_balance(token_id) or 0
        filled = max(0, bal - pre_bal)
        if filled >= MIN_SHARES:
            log(f"     ⚠ Late fill: {filled:.1f}sh")
            self._create_position(token_id, market, direction,
                                  entry_price, tp_price, filled, bet)
        else:
            self._cooldowns[token_id] = time.time()

    def _create_position(self, token_id, market, direction,
                         entry_price, tp_price, shares, bet):
        """Create tracked position and post TP sell order."""
        cost = round(shares * entry_price, 4)
        pos = {
            "token_id": token_id,
            "market": market,
            "direction": direction,
            "entry_price": entry_price,
            "tp_price": tp_price,
            "shares": shares,
            "cost": cost,
            "entry_time": time.time(),
            "tp_order_id": None,
            "resolved": False,
            "pnl": None,
        }

        self.positions.append(pos)
        self._trade_count += 1
        self._log_trade(pos, "OPEN")

        # Post TP sell order (maker, 0% fee)
        if not DRY_RUN:
            try:
                # Ensure allowance
                client.update_balance_allowance(
                    params=BalanceAllowanceParams(
                        asset_type=AssetType.CONDITIONAL,
                        token_id=token_id, signature_type=1,
                    ))

                sell_shares = math.floor(shares * 100) / 100
                if sell_shares < MIN_SHARES:
                    log(f"     ⚠ {shares:.2f}sh rounds to {sell_shares} < min, no TP order")
                    return

                sell_order = OrderArgs(
                    token_id=token_id,
                    price=tp_price,
                    size=sell_shares,
                    side=SELL,
                )
                signed = client.create_order(sell_order)
                resp = client.post_order(signed, OrderType.GTC)
                oid = resp.get("orderID", "") if isinstance(resp, dict) else ""

                if oid:
                    pos["tp_order_id"] = oid
                    log(f"     🎯 TP sell posted @ {tp_price} ({sell_shares:.1f}sh): {oid[:10]}")
                else:
                    log(f"     ⚠ TP order failed: {resp}")
            except Exception as e:
                log(f"     ⚠ TP order error: {e}")

    # ── Position monitoring ──
    def _monitor_positions(self):
        """Check if TP orders filled or window is ending."""
        now = datetime.now(timezone.utc)
        to_remove = []

        for pos in self.positions:
            if pos.get("resolved"):
                to_remove.append(pos)
                continue

            token_id = pos["token_id"]
            market = pos["market"]
            window_end = market["window_end"]
            remaining = (window_end - now).total_seconds()

            # Check if TP filled (shares gone)
            if pos.get("tp_order_id"):
                bal = get_share_balance(token_id)
                if bal is not None and bal < 1:
                    # TP filled!
                    payout = round(pos["shares"] * pos["tp_price"], 4)
                    pnl = round(payout - pos["cost"], 2)
                    pos["resolved"] = True
                    pos["pnl"] = pnl
                    pos["exit_price"] = pos["tp_price"]
                    pos["exit_reason"] = "take_profit"
                    if pnl > 0:
                        self._win_count += 1
                    self._pnl += pnl
                    log(f"  💰 TP FILLED: {market['crypto'].upper()} {pos['direction']} — "
                        f"+${pnl:.2f} ({pos['shares']:.1f}sh @ {pos['entry_price']} → {pos['tp_price']})")
                    self._log_trade(pos, "SOLD")
                    to_remove.append(pos)
                    self._cooldowns[token_id] = time.time()
                    continue

            # Window ending — let it resolve (don't panic sell)
            # The whole point is: at ~0.50, resolution EV ≈ 0
            # We only log a warning with < 30s left
            if remaining < 30 and not pos.get("_warned_window"):
                pos["_warned_window"] = True
                bba = self.tracker.get_bba(token_id)
                mid_str = f"mid={bba['mid']:.2f}" if bba else "no data"
                log(f"  ⏳ {market['crypto'].upper()} {pos['direction']}: "
                    f"window ending in {remaining:.0f}s, holding to resolution ({mid_str})")

            # Window ended — position will resolve via CLOB
            if remaining < -5:
                # Check if resolved (balance went to 0 or tokens redeemed)
                bal = get_share_balance(token_id)
                if bal is not None and bal < 1:
                    # Resolved — figure out if we won or lost
                    # If token resolved $1, we'd get shares redeemed
                    # We can't easily tell, so estimate from final balance change
                    pos["resolved"] = True
                    pos["exit_reason"] = "resolution"
                    # We'll compute actual PnL from USDC delta at session end
                    pos["pnl"] = 0  # placeholder
                    log(f"  🏁 RESOLVED: {market['crypto'].upper()} {pos['direction']} "
                        f"({pos['shares']:.1f}sh @ {pos['entry_price']})")
                    self._log_trade(pos, "RESOLVED")
                    to_remove.append(pos)
                    self._cooldowns[token_id] = time.time()
                elif remaining < -60:
                    # Stale — force remove
                    pos["resolved"] = True
                    pos["pnl"] = 0
                    pos["exit_reason"] = "stale"
                    to_remove.append(pos)

        for pos in to_remove:
            if pos in self.positions:
                self.positions.remove(pos)
            self.completed.append(pos)

    # ── Ghost sweep ──
    def _ghost_sweep(self):
        """Find shares we don't have tracked positions for and sell them."""
        if DRY_RUN:
            return

        tracked_tokens = {p["token_id"] for p in self.positions if not p.get("resolved")}

        for token_id in list(self._known_tokens):
            if token_id in tracked_tokens:
                continue
            try:
                bal = get_share_balance(token_id) or 0
                if bal >= MIN_SHARES:
                    log(f"  👻 Ghost: {bal:.1f}sh on {token_id[:16]} — selling")
                    self._sell_shares(token_id, bal)
            except Exception:
                continue

    def _sell_shares(self, token_id, shares):
        """Sell shares at market (0.01 price = matches best bids)."""
        try:
            client.update_balance_allowance(
                params=BalanceAllowanceParams(
                    asset_type=AssetType.CONDITIONAL,
                    token_id=token_id, signature_type=1,
                ))
            sell_size = math.floor(shares * 100) / 100
            if sell_size < MIN_SHARES:
                return
            order = OrderArgs(
                token_id=token_id,
                price=0.01,
                size=sell_size,
                side=SELL,
            )
            signed = client.create_order(order)
            client.post_order(signed, OrderType.GTC)
        except Exception as e:
            log(f"  ⚠ Sell error: {e}")

    # ── Status logging ──
    def _log_status(self):
        now = datetime.now(timezone.utc)
        active = [p for p in self.positions if not p.get("resolved")]
        bal = get_usdc_balance()
        bal_str = f"${bal:.2f}" if bal else "?"

        log(f"\n  ⏰ {now.strftime('%H:%M:%S')} | Pos: {len(active)} | "
            f"Trades: {self._trade_count} W:{self._win_count} | "
            f"PnL: ${self._pnl:+.2f} | Bal: {bal_str}")

        with self._lock:
            for crypto, m in self.markets.items():
                remaining = (m["window_end"] - now).total_seconds()
                if remaining > 0:
                    # Show spread for both tokens
                    for tok, label in [(m["up_token"], "UP"), (m["down_token"], "DOWN")]:
                        bba = self.tracker.get_bba(tok)
                        if bba:
                            log(f"     {crypto.upper()} {label}: "
                                f"bb={bba['bb']} ba={bba['ba']} "
                                f"spread={bba['spread']} mid={bba['mid']:.2f} "
                                f"({remaining:.0f}s)")

        # Show active positions
        for p in active:
            bba = self.tracker.get_bba(p["token_id"])
            mid_str = f"mid={bba['mid']:.2f}" if bba else "?"
            hold = time.time() - p["entry_time"]
            log(f"     📦 {p['market']['crypto'].upper()} {p['direction']}: "
                f"{p['shares']:.1f}sh @ {p['entry_price']} (TP @ {p['tp_price']}) "
                f"{mid_str} {hold:.0f}s held")

    def _log_trade(self, pos, action):
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": action,
            "crypto": pos["market"]["crypto"],
            "direction": pos["direction"],
            "question": pos["market"]["question"],
            "entry_price": pos["entry_price"],
            "tp_price": pos["tp_price"],
            "shares": pos["shares"],
            "cost": pos["cost"],
            "pnl": pos.get("pnl", 0),
            "exit_price": pos.get("exit_price"),
            "exit_reason": pos.get("exit_reason"),
        }
        with open(TRADE_FILE, "a") as f:
            f.write(json.dumps(record) + "\n")

    # ── Shutdown ──
    def _shutdown(self):
        self._running = False
        log("  Cancelling all orders...")
        try:
            client.cancel_all()
        except Exception:
            pass

        # Don't sell positions — let them resolve naturally
        active = [p for p in self.positions if not p.get("resolved")]
        if active:
            log(f"  📦 {len(active)} positions held to resolution")
            for p in active:
                log(f"     {p['market']['crypto'].upper()} {p['direction']}: "
                    f"{p['shares']:.1f}sh @ {p['entry_price']}")

        final = get_usdc_balance()
        log("")
        log("=" * 60)
        log("  MAKER2 STOPPED")
        log(f"  Trades: {self._trade_count} | TP wins: {self._win_count}")
        log(f"  TP P&L: ${self._pnl:+.2f}")
        if final:
            log(f"  Final USDC: ${final:.2f}")
        if self._start_balance and final:
            log(f"  Delta: ${final - self._start_balance:+.2f}")
        log("=" * 60)

    # ── Main loop ──
    def run(self):
        log("")
        log("=" * 60)
        log(f"  MAKER2 {'(DRY RUN)' if DRY_RUN else '(LIVE)'}")
        log(f"  Bet: ${BET_AMOUNT}, Max positions: {MAX_POSITIONS}")
        log(f"  Spread: {MIN_SPREAD}-{MAX_SPREAD}, Price: {MIN_PRICE}-{MAX_PRICE}")
        log(f"  TP: +{PROFIT_TARGET}, Cancel adverse: {CANCEL_ADVERSE}")
        log("=" * 60)

        self._start_balance = get_usdc_balance()
        log(f"\n  Starting USDC: ${self._start_balance:.2f}" if self._start_balance
            else "  ⚠ Could not get balance")

        self.ws.start()
        self._update_markets()

        # Wait for WS warmup
        log("  Waiting for order book data...")
        time.sleep(WS_WARMUP)

        try:
            last_ghost = 0
            last_status = 0

            while self._running:
                self._update_markets()
                self._scan_entries()
                self._monitor_positions()

                now_ts = time.time()

                # Periodic ghost sweep
                if now_ts - last_ghost > 30:
                    last_ghost = now_ts
                    self._ghost_sweep()

                # Periodic status
                if now_ts - last_status > STATUS_INTERVAL:
                    last_status = now_ts
                    self._log_status()

                time.sleep(SCAN_INTERVAL)

        except KeyboardInterrupt:
            log("\n  Shutting down...")
            self._shutdown()


if __name__ == "__main__":
    bot = Maker2()
    bot.run()
