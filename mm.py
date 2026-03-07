#!/usr/bin/env python3
"""
mm.py -- Two-Sided Market Maker for Polymarket 5-min crypto markets.

Strategy:
  For each 5-min window, for each crypto (BTC, ETH, SOL, XRP):
    1. Post BUY Up at best_bid_up with GTD expiry (maker, 0% fee)
    2. Post BUY Down at best_bid_down with GTD expiry (maker, 0% fee)
       Together = TWO-SIDED liquidity:
         BUY Up = bid on Up; BUY Down = bid on Down = ask on Up
    3. When either BUY fills -> post SELL at entry + SPREAD_TARGET (maker)
    4. If sell fills -> spread captured (both legs maker = 0% total fee)
    5. GTD auto-cancels all orders before window end -> no resolution risk
    6. Force-sell remaining tokens in cleanup window

  Paired position bonus:
    If BOTH Up and Down fill for the same market:
      cost = up_entry + down_entry (< $1.00 if spread > 0)
      At resolution one side = $1.00 -> guaranteed profit = $1.00 - cost

Revenue streams:
  1. Spread capture: buy at bid, sell at ask (2c+ per leg)
  2. Pair profit: buy both sides < $1.00, one resolves to $1.00
  3. Liquidity rewards: daily USDC for two-sided quoting (Q_min formula)
  4. Maker rebates: 20% of crypto taker fees redistributed daily

Usage:
  python mm.py              # live trading
  python mm.py --dry-run    # simulate (no real orders)
"""

import os, sys, time, json, math, threading, asyncio, requests
from datetime import datetime, timezone, timedelta
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    OrderArgs, OrderType, BalanceAllowanceParams, AssetType,
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

# -- Quoting --
QUOTE_SIZE      = 10       # $ per side per token (so $20 per market, $80 for 4)
SPREAD_TARGET   = 0.02     # post SELL at entry + 2c
MIN_SPREAD      = 0.02     # only quote if book spread >= this
MIN_MID         = 0.15     # skip tokens near 0
MAX_MID         = 0.85     # skip tokens near 1
MIN_DEPTH       = 50       # min total depth ($) to trust book
MIN_SHARES      = 5        # Polymarket minimum order
TICK            = 0.01     # price tick

# -- Timing --
BUY_CUTOFF      = 150      # stop posting buys N sec before window end
SELL_EXPIRY_BUF = 30       # GTD sell expires N sec before window end
CLEANUP_BUF     = 75       # force-sell remaining N sec before window end
CHECK_INTERVAL  = 2        # seconds between fill checks per market
REQUOTE_INTERVAL = 10      # seconds between requoting if book moved
WS_WARMUP       = 5        # WS warm-up delay

# -- Risk --
MAX_MARKETS     = 4        # max simultaneous market threads
MIN_BALANCE     = 5        # stop trading below this USDC

# -- Markets --
CRYPTOS         = ["btc", "eth", "sol", "xrp"]
INTERVALS       = [5]

DRY_RUN    = "--dry-run" in sys.argv
LOG_FILE   = "mm_dry_log.txt" if DRY_RUN else "mm_log.txt"
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


def post_limit(token_id, side, price, size, expiry_ts=None):
    """Post a limit order (GTD with expiry or GTC). Returns order_id or None."""
    try:
        kwargs = dict(price=price, size=size, side=side, token_id=token_id)
        if expiry_ts:
            kwargs["expiration"] = str(int(expiry_ts))
        order_args = OrderArgs(**kwargs)
        signed = client.create_order(order_args)
        order_type = OrderType.GTD if expiry_ts else OrderType.GTC
        resp = client.post_order(signed, order_type)
        oid = resp.get("orderID", "") if isinstance(resp, dict) else ""
        return oid if oid else None
    except Exception as e:
        log(f"    order error ({side} {size}@{price}): {e}")
        return None


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
def record_trade(crypto, token_side, token_id, entry_price, exit_price, shares,
                 cost, pnl, hold_secs, reason):
    record = {
        "time": datetime.now(timezone.utc).isoformat(),
        "crypto": crypto,
        "token_side": token_side,
        "token_id": token_id[:20] + "...",
        "entry_price": entry_price,
        "exit_price": round(exit_price, 4),
        "shares": round(shares, 2),
        "cost": round(cost, 4),
        "pnl": round(pnl, 4),
        "hold_secs": round(hold_secs, 1),
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


# =========================================================================
# FORCE SELL
# =========================================================================
def force_sell(token_id, tracker=None, max_retries=8):
    """Cancel all orders then sell all shares at best bid. Returns total sold."""
    if DRY_RUN:
        return 0
    total_sold = 0
    for attempt in range(max_retries):
        cancel_all_orders(token_id)
        time.sleep(2 if attempt == 0 else 3)
        bal = get_share_balance(token_id) or 0
        if bal < MIN_SHARES:
            return total_sold
        sell_size = math.floor(bal * 100) / 100
        if sell_size < MIN_SHARES:
            return total_sold
        # Use best bid from book if available, otherwise fall back to 0.01
        sell_price = 0.01
        if tracker:
            book = tracker.get_book(token_id)
            if book and book["bb"] > 0.01:
                sell_price = round(math.floor(book["bb"] / TICK) * TICK, 2)
        try:
            order_args = OrderArgs(
                price=sell_price, size=sell_size, side=SELL, token_id=token_id)
            signed = client.create_order(order_args)
            client.post_order(signed, OrderType.GTC)
            log(f"    force-sell {sell_size:.1f}sh @ {sell_price} (attempt {attempt + 1})")
            total_sold += sell_size
        except Exception as e:
            log(f"    sell error (attempt {attempt + 1}): {e}")
            continue
        time.sleep(3)
    return total_sold


# =========================================================================
# MM BOT
# =========================================================================
class MMBot:
    """
    Two-sided market maker.
    Per market: one thread manages both Up and Down tokens.
    Main thread handles discovery and status.
    """

    def __init__(self):
        self.tracker = BookTracker()
        self.ws = MarketWS(self.tracker)
        self.markets = []
        self.token_to_market = {}
        self.last_discovery = 0
        self.last_status = 0
        self.balance = 0
        self._shutting_down = False
        self._known_tokens = set()

        # Track active market threads by condition_id
        self._active_markets = {}
        self._active_lock = threading.Lock()

        # Track condition_ids that failed to post orders (avoid re-launch spam)
        self._failed_cids = set()

        # Aggregated stats
        self.trade_count = 0
        self.total_pnl = 0.0
        self.session_start = time.time()
        self.spread_captures = 0
        self.pairs_matched = 0

    def run(self):
        log("=" * 60)
        log(f"  MM BOT v2 -- TWO-SIDED {'(DRY RUN)' if DRY_RUN else '(LIVE)'}")
        log("=" * 60)

        self.balance = get_usdc_balance() or 0
        self.start_balance = self.balance
        log(f"  Balance: ${self.balance:.2f}")
        log(f"  Config: quote=${QUOTE_SIZE}/side  spread={SPREAD_TARGET}  "
            f"buy_cutoff={BUY_CUTOFF}s  sell_exp={SELL_EXPIRY_BUF}s  cleanup={CLEANUP_BUF}s")
        log(f"  Markets: {', '.join(CRYPTOS)}")

        self.ws.start()
        self._discover()

        try:
            while True:
                self._tick()
                time.sleep(1)
        except KeyboardInterrupt:
            log("")
            log("  Shutting down...")
            self._shutting_down = True
            time.sleep(2)
            self._cleanup()
            end_balance = get_usdc_balance() or 0
            actual_pnl = end_balance - self.start_balance
            log(f"  Session: {self.trade_count} trades ({self.spread_captures} captures, "
                f"{self.pairs_matched} pairs) | "
                f"PnL: ${self.total_pnl:+.2f} (book) | "
                f"${actual_pnl:+.2f} (USDC: ${self.start_balance:.2f} -> ${end_balance:.2f})")

    # -- Discovery --

    def _discover(self):
        now = time.time()
        if now - self.last_discovery < 5:
            return
        self.last_discovery = now

        markets = discover_markets()
        self.markets = [m for m in markets if m["secs_left"] > CLEANUP_BUF + 30]
        if not self.markets:
            return

        all_tokens = set()
        self.token_to_market = {}
        current_cids = set()
        for m in self.markets:
            current_cids.add(m["condition_id"])
            for tok in [m["up_token"], m["down_token"]]:
                all_tokens.add(tok)
                self.token_to_market[tok] = m

        # Clear failed cids from previous windows
        self._failed_cids -= self._failed_cids - current_cids

        self.ws.subscribe(all_tokens)

        cryptos = sorted(set(m["crypto"] for m in self.markets))
        if len(self.markets) != getattr(self, "_prev_count", 0):
            secs = self.markets[0]["secs_left"] if self.markets else 0
            log(f"  Tracking {len(self.markets)} markets: "
                f"{', '.join(cryptos)} ({secs:.0f}s left)")
            self._prev_count = len(self.markets)

    # -- Main tick --

    def _tick(self):
        self._discover()

        now = time.time()
        if now - self.last_status > 30:
            self.last_status = now
            mins = (now - self.session_start) / 60
            with self._active_lock:
                n_active = len(self._active_markets)
            log(f"  [{mins:.0f}m] trades={self.trade_count} "
                f"(cap={self.spread_captures} pair={self.pairs_matched}) | "
                f"pnl=${self.total_pnl:+.2f} | active={n_active}/{MAX_MARKETS}")

        # Launch market threads for new markets
        for m in self.markets:
            cid = m["condition_id"]
            secs_left = (m["window_end"] - datetime.now(timezone.utc)).total_seconds()
            if secs_left < BUY_CUTOFF:
                continue

            with self._active_lock:
                if cid in self._active_markets:
                    continue
                if cid in self._failed_cids:
                    continue
                if len(self._active_markets) >= MAX_MARKETS:
                    continue

            # Check balance
            bal = get_usdc_balance()
            if bal is not None:
                self.balance = bal
            if self.balance < MIN_BALANCE + QUOTE_SIZE * 2:
                continue

            t = threading.Thread(
                target=self._market_lifecycle, args=(m,), daemon=True)
            with self._active_lock:
                self._active_markets[cid] = t
            t.start()

    # -- Per-market lifecycle (one thread per crypto per window) --

    def _market_lifecycle(self, market):
        """
        Full lifecycle for one market (both Up and Down tokens).
        Phase 1: Post BUY Up + BUY Down (GTD)
        Phase 2: Monitor fills, post SELLs on fill
        Phase 3: Cleanup - force sell remaining
        """
        crypto = market["crypto"]
        up_token = market["up_token"]
        down_token = market["down_token"]
        window_end = market["window_end"]
        cid = market["condition_id"]

        # GTD expiry timestamps
        # Polymarket requires expiration > now + 60s security buffer
        # So set buy_expiry = now + (secs_to_window_end - BUY_CUTOFF)
        # and sell_expiry = now + (secs_to_window_end - SELL_EXPIRY_BUF)
        now_ts = int(time.time())
        secs_to_end = int(window_end.timestamp()) - now_ts
        buy_expiry = now_ts + max(secs_to_end - BUY_CUTOFF, 90)
        sell_expiry = now_ts + max(secs_to_end - SELL_EXPIRY_BUF, 90)
        cleanup_time = window_end - timedelta(seconds=CLEANUP_BUF)

        # Per-token state
        state = {
            up_token: {
                "status": "idle",
                "buy_oid": None,
                "sell_oid": None,
                "entry": 0,
                "shares": 0,
                "label": f"{crypto} Up",
                "token_side": "Up",
                "fill_time": 0,
                "pre_bal": 0,
                "filled_shares": 0,
            },
            down_token: {
                "status": "idle",
                "buy_oid": None,
                "sell_oid": None,
                "entry": 0,
                "shares": 0,
                "label": f"{crypto} Dn",
                "token_side": "Down",
                "fill_time": 0,
                "pre_bal": 0,
                "filled_shares": 0,
            },
        }

        self._known_tokens.add(up_token)
        self._known_tokens.add(down_token)

        try:
            secs_left = (window_end - datetime.now(timezone.utc)).total_seconds()
            log(f"  {crypto} starting two-sided quotes ({secs_left:.0f}s left)")

            # Wait for WS book data before proceeding
            deadline = time.time() + WS_WARMUP + 2
            while time.time() < deadline:
                if all(self.tracker.get_book(t) for t in [up_token, down_token]):
                    break
                time.sleep(0.5)

            # --- PHASE 1: Post BUY orders for both tokens ---
            for token_id, st in state.items():
                book = self.tracker.get_book(token_id)
                if not book:
                    log(f"    {st['label']}: no book data, skipping")
                    continue
                if book["spread"] < MIN_SPREAD - 0.001:
                    log(f"    {st['label']}: spread {book['spread']:.2f} < {MIN_SPREAD}, skip")
                    continue
                if book["mid"] < MIN_MID or book["mid"] > MAX_MID:
                    log(f"    {st['label']}: mid {book['mid']:.2f} out of range, skip")
                    continue
                if book["bid_depth"] + book["ask_depth"] < MIN_DEPTH:
                    continue

                entry_price = round(math.floor(book["bb"] / TICK) * TICK, 2)
                if entry_price <= 0.01 or entry_price >= 0.99:
                    continue

                shares = round(QUOTE_SIZE / entry_price, 2)
                if shares < MIN_SHARES:
                    continue

                # Record pre-fill share balance
                if not DRY_RUN:
                    st["pre_bal"] = get_share_balance(token_id) or 0

                exp_str = datetime.fromtimestamp(
                    buy_expiry, tz=timezone.utc).strftime("%H:%M:%S")
                log(f"    {st['label']} BUY @ {entry_price} "
                    f"({shares:.1f}sh, ${QUOTE_SIZE:.0f}) exp={exp_str}")

                if DRY_RUN:
                    st["status"] = "buying"
                    st["entry"] = entry_price
                    st["shares"] = shares
                    continue

                oid = post_limit(token_id, BUY, entry_price, shares, buy_expiry)
                if oid:
                    st["buy_oid"] = oid
                    st["entry"] = entry_price
                    st["shares"] = shares
                    st["status"] = "buying"
                else:
                    log(f"    {st['label']}: buy order failed")

            # Check we posted at least one order
            active = [t for t, s in state.items() if s["status"] == "buying"]
            if not active:
                log(f"    {crypto}: no orders posted, exiting")
                with self._active_lock:
                    self._failed_cids.add(cid)
                return

            # --- PHASE 2: Monitor fills and manage positions ---
            last_requote = time.time()

            while datetime.now(timezone.utc) < cleanup_time:
                if self._shutting_down:
                    break
                time.sleep(CHECK_INTERVAL)

                for token_id, st in state.items():
                    if st["status"] == "buying":
                        self._check_buy_fill(token_id, st, sell_expiry, crypto)
                    elif st["status"] == "selling":
                        self._check_sell_fill(token_id, st, crypto)

                # Requote stale buys if book moved
                now_t = time.time()
                if now_t - last_requote > REQUOTE_INTERVAL:
                    last_requote = now_t
                    secs_left = (window_end - datetime.now(timezone.utc)).total_seconds()
                    if secs_left > BUY_CUTOFF:
                        for token_id, st in state.items():
                            if st["status"] == "buying":
                                self._try_requote(token_id, st, buy_expiry)

            # --- PHASE 3: Cleanup ---
            log(f"  {crypto} cleanup phase")

            for token_id, st in state.items():
                # Cancel all pending orders for this token
                if st["status"] in ("buying", "selling"):
                    cancel_all_orders(token_id)

                if st["status"] == "buying":
                    # Check for late fill after cancel
                    if not DRY_RUN:
                        time.sleep(1)
                        bal = get_share_balance(token_id) or 0
                        filled = bal - st["pre_bal"]
                        if filled >= MIN_SHARES:
                            st["shares"] = min(filled, st["shares"])
                            st["status"] = "filled"
                            st["fill_time"] = time.time()
                            st["filled_shares"] = st["shares"]
                            log(f"    {st['label']}: late fill {filled:.1f}sh")
                        else:
                            st["status"] = "done"
                    else:
                        st["status"] = "done"

                # Force sell any tokens we still hold
                if st["status"] in ("filled", "selling"):
                    book = self.tracker.get_book(token_id)
                    exit_price = book["bb"] if book else st["entry"]

                    if not DRY_RUN:
                        force_sell(token_id, tracker=self.tracker)

                    hold = time.time() - st["fill_time"] if st["fill_time"] else 0
                    pnl = (exit_price - st["entry"]) * st["shares"]
                    self.trade_count += 1
                    self.total_pnl += pnl
                    record_trade(crypto, st["token_side"], token_id,
                                 st["entry"], exit_price, st["shares"],
                                 st["entry"] * st["shares"], pnl, hold, "cleanup")
                    icon = "+" if pnl >= 0 else "-"
                    log(f"    [{icon}] {st['label']} cleanup: "
                        f"pnl=${pnl:+.2f} ({hold:.0f}s hold)")
                    st["status"] = "done"

            # Check for matched pair
            up_st = state[up_token]
            down_st = state[down_token]
            up_filled = up_st.get("filled_shares", 0)
            down_filled = down_st.get("filled_shares", 0)
            if up_filled > 0 and down_filled > 0:
                pair_cost = up_st["entry"] + down_st["entry"]
                matched = min(up_filled, down_filled)
                if matched > 0 and pair_cost < 1.0:
                    pair_profit = (1.0 - pair_cost) * matched
                    log(f"  {crypto} PAIR: {matched:.0f}sh x "
                        f"${1.0 - pair_cost:.2f} = ${pair_profit:+.2f} implicit")
                    self.pairs_matched += 1

        except Exception as e:
            log(f"  {crypto} lifecycle error: {e}")
            for token_id in [up_token, down_token]:
                try:
                    cancel_all_orders(token_id)
                    if not DRY_RUN:
                        force_sell(token_id, tracker=self.tracker)
                except Exception:
                    pass
        finally:
            with self._active_lock:
                self._active_markets.pop(cid, None)

    def _check_buy_fill(self, token_id, st, sell_expiry, crypto):
        """Check if a BUY order filled. If so, post SELL."""
        if DRY_RUN:
            return

        bal = get_share_balance(token_id) or 0
        filled = bal - st["pre_bal"]
        if filled >= MIN_SHARES:
            actual_shares = min(filled, st["shares"])
            st["shares"] = actual_shares
            st["status"] = "filled"
            st["fill_time"] = time.time()
            st["filled_shares"] = actual_shares
            log(f"    [FILL] {st['label']}: {actual_shares:.1f}sh @ {st['entry']}")

            # Cancel remaining buy if any
            if st["buy_oid"]:
                try:
                    client.cancel(st["buy_oid"])
                except Exception:
                    pass

            # Post SELL at entry + spread target
            sell_price = round(st["entry"] + SPREAD_TARGET, 2)
            if sell_price > 0.99:
                sell_price = 0.99
            sell_shares = math.floor(actual_shares * 100) / 100
            if sell_shares < MIN_SHARES:
                log(f"    {st['label']}: {actual_shares:.1f}sh too small to sell")
                return

            oid = post_limit(token_id, SELL, sell_price, sell_shares, sell_expiry)
            if oid:
                st["sell_oid"] = oid
                st["status"] = "selling"
                exp_str = datetime.fromtimestamp(
                    sell_expiry, tz=timezone.utc).strftime("%H:%M:%S")
                log(f"    [SELL] {st['label']} limit @ {sell_price} "
                    f"({sell_shares:.1f}sh) exp={exp_str}")
            else:
                log(f"    {st['label']}: sell order failed")

    def _check_sell_fill(self, token_id, st, crypto):
        """Check if SELL order filled (shares gone = sold)."""
        if DRY_RUN:
            return

        bal = get_share_balance(token_id) or 0
        remaining = bal - st["pre_bal"]
        if remaining < MIN_SHARES:
            # Sell filled!
            exit_price = st["entry"] + SPREAD_TARGET
            hold = time.time() - st["fill_time"]
            pnl = SPREAD_TARGET * st["shares"]
            self.trade_count += 1
            self.total_pnl += pnl
            self.spread_captures += 1
            record_trade(crypto, st["token_side"], token_id,
                         st["entry"], exit_price, st["shares"],
                         st["entry"] * st["shares"], pnl, hold, "spread_captured")
            log(f"    [CAPTURED] {st['label']}: "
                f"pnl=${pnl:+.4f} ({hold:.1f}s hold)")
            st["status"] = "done"

    def _try_requote(self, token_id, st, buy_expiry):
        """Cancel stale buy and repost if book moved significantly."""
        book = self.tracker.get_book(token_id)
        if not book:
            return
        new_bb = round(math.floor(book["bb"] / TICK) * TICK, 2)
        if new_bb == st["entry"]:
            return
        if new_bb <= 0.01 or new_bb >= 0.99:
            return
        if book["mid"] < MIN_MID or book["mid"] > MAX_MID:
            return
        if book["spread"] < MIN_SPREAD - 0.001:
            return

        # Cancel old order
        if st["buy_oid"]:
            try:
                client.cancel(st["buy_oid"])
            except Exception:
                pass

        # Check for partial fill before reposting
        if not DRY_RUN:
            time.sleep(1)
            bal = get_share_balance(token_id) or 0
            filled = bal - st["pre_bal"]
            if filled >= MIN_SHARES:
                return  # Got filled, will handle in next check cycle

        new_shares = round(QUOTE_SIZE / new_bb, 2)
        if new_shares < MIN_SHARES:
            return

        if DRY_RUN:
            st["entry"] = new_bb
            st["shares"] = new_shares
            return

        oid = post_limit(token_id, BUY, new_bb, new_shares, buy_expiry)
        if oid:
            st["buy_oid"] = oid
            st["entry"] = new_bb
            st["shares"] = new_shares
            log(f"    [REQUOTE] {st['label']} BUY @ {new_bb} ({new_shares:.1f}sh)")

    # -- Global Cleanup --

    def _cleanup(self):
        """Cancel everything and sell all held shares."""
        if DRY_RUN:
            return

        log("  Global cleanup: cancelling all orders...")
        cancel_all_orders()
        time.sleep(3)

        all_tokens = set(self._known_tokens) | set(self.token_to_market.keys())
        for token_id in all_tokens:
            try:
                bal = get_share_balance(token_id) or 0
            except Exception:
                continue
            if bal >= MIN_SHARES:
                log(f"  Selling {bal:.1f}sh on {token_id[:20]}...")
                force_sell(token_id, tracker=self.tracker, max_retries=10)

        time.sleep(3)
        for token_id in all_tokens:
            try:
                bal = get_share_balance(token_id) or 0
                if bal >= MIN_SHARES:
                    log(f"  STILL {bal:.1f}sh -- retrying")
                    force_sell(token_id, tracker=self.tracker, max_retries=5)
            except Exception:
                continue

        log("  Cleanup complete")


# =========================================================================
# ENTRY POINT
# =========================================================================
if __name__ == "__main__":
    bot = MMBot()
    bot.run()
