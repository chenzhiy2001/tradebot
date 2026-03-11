#!/usr/bin/env python3
"""
arb.py — Conditional Token Arbitrage for Polymarket 5-min crypto markets.

Strategy (reverse-engineered from top-PnL wallet behavior):
  In binary markets, Up + Down = $1.00 at resolution (guaranteed).
  If we buy Up at bid_up and Down at bid_down via LIMIT orders (maker = 0% fee),
  and bid_up + bid_down < $1.00, we lock in guaranteed profit regardless of outcome.

  Profitable wallets show:
    - ~50% win rate BUT positive PnL (edge is SIZE not accuracy)
    - 40-75% straddle rate (buying both sides in most markets)
    - Frequent small bets (limit orders providing liquidity on both sides)
    - Occasional huge bets when spread is wide (large arb edge)
    - Hold to resolution (winner = $1/share)
    - Keep buying even with existing position (layering)

  This bot replicates that behavior:
    1. For each 5-min window, monitor Up+Down order books via WebSocket.
    2. Compute pair_edge = 1.00 - best_bid_up - best_bid_down.
    3. When pair_edge > MIN_EDGE, place GTC limit BUYs on both sides at best_bid.
    4. Continuously refresh orders as the book moves (always at best bid).
    5. Track fill status — paired shares = guaranteed profit.
    6. Dynamic sizing: more when edge is wider.
    7. Cancel unfilled orders before window ends to limit naked directional risk.

Revenue:
  pair_profit = paired_shares × (1.00 - avg_cost_up - avg_cost_down)
  - 0% maker fee on both entry legs
  - Resolution payout is automatic ($1 for winner, $0 for loser)

Risk:
  Unpaired fills (only one side filled) = directional exposure (coin flip).
  Managed via: MAX_UNPAIRED_COST cap, CANCEL_BUFFER before window end.

Usage:
  python arb.py              # live trading
  python arb.py --dry-run    # simulate (no real orders)
"""

import os, sys, time, json, math, threading, asyncio, requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict
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

# -- Arb parameters --
MIN_EDGE        = 0.02      # Minimum pair_edge (1.00 - bid_up - bid_dn) to quote
BASE_SIZE       = 3          # $ per side when edge = MIN_EDGE (min viable with 5-share min)
MAX_SIZE        = 5          # $ per side cap
EDGE_SCALE      = True       # Scale size proportionally with edge
MAX_PAIR_COST   = 0.98       # Absolute max we'll pay for a pair (cost_up + cost_dn)
MAX_UNPAIRED_COST = 5        # Max $ of unpaired directional exposure per market
TICK            = 0.01       # Price tick
MIN_SHARES      = 5          # Polymarket minimum order
MIN_MID         = 0.15       # Skip tokens near 0
MAX_MID         = 0.85       # Skip tokens near 1
MIN_DEPTH       = 30         # Min depth ($) to trust book

# -- Timing --
QUOTE_START     = 5          # Start quoting N seconds into window
CANCEL_BUFFER   = 30         # Cancel unfilled orders N seconds before window end
CHECK_INTERVAL  = 2          # Seconds between fill checks
REQUOTE_INTERVAL = 5         # Seconds between requoting on book changes
WS_WARMUP       = 3          # WS warm-up delay

# -- Risk --
MAX_MARKETS     = 1          # Max simultaneous market threads ($11 budget)
MIN_BALANCE     = 2          # Stop trading below this USDC

# -- Markets --
CRYPTOS         = ["btc", "eth", "sol", "xrp"]
INTERVALS       = [5]

DRY_RUN    = "--dry-run" in sys.argv
DRY_BALANCE = 1000.0         # Simulated balance for dry-run mode
LOG_FILE   = "arb_dry_log.txt" if DRY_RUN else "arb_log.txt"
TRADE_FILE = "arb_dry_trades.json" if DRY_RUN else "arb_trades.json"


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
                        "window_start": window_start,
                        "window_end": window_end,
                        "secs_left": secs_left,
                    })
            except Exception:
                continue
    return markets


# =========================================================================
# BOOK TRACKER (same as mm.py)
# =========================================================================
class BookTracker:
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
def record_trade(data):
    try:
        existing = []
        if os.path.exists(TRADE_FILE):
            with open(TRADE_FILE) as f:
                existing = json.load(f)
        existing.append(data)
        with open(TRADE_FILE, "w") as f:
            json.dump(existing, f, indent=2)
    except Exception:
        pass


# =========================================================================
# FORCE SELL (safety: sell leftover tokens after resolution timeout)
# =========================================================================
def force_sell(token_id, tracker=None, max_retries=5):
    if DRY_RUN:
        return 0
    total_sold = 0
    for attempt in range(max_retries):
        cancel_all_orders(token_id)
        time.sleep(2)
        bal = get_share_balance(token_id) or 0
        if bal < MIN_SHARES:
            return total_sold
        sell_size = math.floor(bal * 100) / 100
        if sell_size < MIN_SHARES:
            return total_sold
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
            log(f"    force-sell {sell_size:.1f}sh @ {sell_price}")
            total_sold += sell_size
        except Exception as e:
            log(f"    sell error: {e}")
        time.sleep(2)
    return total_sold


# =========================================================================
# ARB BOT
# =========================================================================
class ArbBot:
    """
    Conditional token arbitrage.

    Per market window: one thread manages both Up and Down tokens.
    Places limit BUYs on both sides at best_bid.
    Tracks paired fills (guaranteed profit) vs unpaired (directional risk).
    Holds to resolution.
    """

    def __init__(self):
        self.tracker = BookTracker()
        self.ws = MarketWS(self.tracker)
        self.markets = []
        self.last_discovery = 0
        self.last_status = 0
        self.balance = 0
        self._shutting_down = False
        self._known_tokens = set()

        self._active_markets = {}   # condition_id → thread
        self._active_lock = threading.Lock()
        self._failed_cids = set()

        # Session stats
        self.session_start = time.time()
        self.start_balance = 0
        self.total_paired_profit = 0.0
        self.total_unpaired_pnl = 0.0
        self.total_trades = 0
        self.pairs_completed = 0
        self.single_legs = 0

    def run(self):
        log("=" * 60)
        log(f"  ARB BOT — Conditional Token Arbitrage {'(DRY RUN)' if DRY_RUN else '(LIVE)'}")
        log("=" * 60)

        self.balance = DRY_BALANCE if DRY_RUN else (get_usdc_balance() or 0)
        self.start_balance = self.balance
        log(f"  Balance: ${self.balance:.2f}{' (simulated)' if DRY_RUN else ''}")
        log(f"  Config: min_edge={MIN_EDGE} base_size=${BASE_SIZE} max_size=${MAX_SIZE}")
        log(f"  Cancel buffer: {CANCEL_BUFFER}s before window end")
        log(f"  Markets: {', '.join(CRYPTOS)} × {INTERVALS}min")

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
            end_balance = self.balance if DRY_RUN else (get_usdc_balance() or 0)
            actual_pnl = end_balance - self.start_balance
            log(f"  Session: pairs={self.pairs_completed} singles={self.single_legs} "
                f"trades={self.total_trades}")
            log(f"  Paired profit: ${self.total_paired_profit:+.2f}")
            log(f"  Unpaired PnL: ${self.total_unpaired_pnl:+.2f}")
            log(f"  USDC: ${self.start_balance:.2f} -> ${end_balance:.2f} "
                f"(${actual_pnl:+.2f})")

    # -- Discovery --

    def _discover(self):
        now = time.time()
        if now - self.last_discovery < 5:
            return
        self.last_discovery = now

        markets = discover_markets()
        self.markets = [m for m in markets if m["secs_left"] > CANCEL_BUFFER + 30]
        if not self.markets:
            return

        all_tokens = set()
        current_cids = set()
        for m in self.markets:
            current_cids.add(m["condition_id"])
            all_tokens.add(m["up_token"])
            all_tokens.add(m["down_token"])

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
            log(f"  [{mins:.0f}m] pairs={self.pairs_completed} "
                f"singles={self.single_legs} "
                f"paired_profit=${self.total_paired_profit:+.2f} "
                f"unpaired=${self.total_unpaired_pnl:+.2f} "
                f"active={n_active}/{MAX_MARKETS}")

        for m in self.markets:
            cid = m["condition_id"]
            secs_into = (datetime.now(timezone.utc) - m["window_start"]).total_seconds()
            secs_left = (m["window_end"] - datetime.now(timezone.utc)).total_seconds()

            if secs_into < QUOTE_START:
                continue
            if secs_left < CANCEL_BUFFER + 15:
                continue

            with self._active_lock:
                if cid in self._active_markets:
                    continue
                if cid in self._failed_cids:
                    continue
                if len(self._active_markets) >= MAX_MARKETS:
                    continue

            if not DRY_RUN:
                bal = get_usdc_balance()
                if bal is not None:
                    self.balance = bal
            if self.balance < MIN_BALANCE + BASE_SIZE * 2:
                continue

            t = threading.Thread(
                target=self._market_lifecycle, args=(m,), daemon=True)
            with self._active_lock:
                self._active_markets[cid] = t
            t.start()

    # -- Per-market lifecycle --

    def _market_lifecycle(self, market):
        """
        Full lifecycle for one market (both Up and Down tokens).

        Phase 1: Continuously quote limit BUYs on both sides at best_bid
                  whenever pair_edge > MIN_EDGE.
        Phase 2: Track fills; when shares appear, update pair tracking.
        Phase 3: Cancel unfilled orders before window end.
        Phase 4: Let positions resolve (hold to end).
        """
        crypto = market["crypto"]
        up_token = market["up_token"]
        down_token = market["down_token"]
        window_end = market["window_end"]
        cid = market["condition_id"]
        cancel_time = window_end - timedelta(seconds=CANCEL_BUFFER)

        # Per-token state
        state = {
            up_token: {
                "label": f"{crypto} Up",
                "token_side": "Up",
                "buy_oid": None,
                "buy_price": 0,
                "buy_size": 0,
                "filled_shares": 0,
                "filled_cost": 0.0,
                "pre_bal": 0,
            },
            down_token: {
                "label": f"{crypto} Dn",
                "token_side": "Down",
                "buy_oid": None,
                "buy_price": 0,
                "buy_size": 0,
                "filled_shares": 0,
                "filled_cost": 0.0,
                "pre_bal": 0,
            },
        }

        self._known_tokens.add(up_token)
        self._known_tokens.add(down_token)

        try:
            secs_left = (window_end - datetime.now(timezone.utc)).total_seconds()
            log(f"  {crypto} arb starting ({secs_left:.0f}s left)")

            # Wait for book data
            deadline = time.time() + WS_WARMUP + 2
            while time.time() < deadline:
                if all(self.tracker.get_book(t) for t in [up_token, down_token]):
                    break
                time.sleep(0.5)

            # Get initial balances
            if not DRY_RUN:
                for tok in [up_token, down_token]:
                    state[tok]["pre_bal"] = get_share_balance(tok) or 0

            last_requote = 0
            last_book_log = 0

            # ── MAIN LOOP: quote + monitor fills ──
            while datetime.now(timezone.utc) < cancel_time:
                if self._shutting_down:
                    break

                now_t = time.time()

                # Check fills
                for tok in [up_token, down_token]:
                    self._check_fill(tok, state[tok])

                # Get books for both sides
                up_book = self.tracker.get_book(up_token)
                dn_book = self.tracker.get_book(down_token)

                # Diagnostic book logging every 15s
                if now_t - last_book_log >= 15:
                    last_book_log = now_t
                    if up_book and dn_book:
                        pe = 1.0 - up_book["bb"] - dn_book["bb"]
                        log(f"    {crypto} book: Up bb={up_book['bb']:.2f} ba={up_book['ba']:.2f} "
                            f"Dn bb={dn_book['bb']:.2f} ba={dn_book['ba']:.2f} "
                            f"| pair_edge={pe:.3f} {'OK' if pe >= MIN_EDGE else 'thin'}")
                    elif not up_book and not dn_book:
                        log(f"    {crypto} book: NO DATA for either token")
                    else:
                        log(f"    {crypto} book: partial (Up={'ok' if up_book else 'NONE'}, "
                            f"Dn={'ok' if dn_book else 'NONE'})")

                if up_book and dn_book:
                    pair_cost = up_book["bb"] + dn_book["bb"]
                    pair_edge = 1.0 - pair_cost

                    # Requote if edge is sufficient and book moved
                    if now_t - last_requote >= REQUOTE_INTERVAL:
                        last_requote = now_t

                        if pair_edge >= MIN_EDGE and pair_cost <= MAX_PAIR_COST:
                            # Dynamic sizing: scale with edge
                            if EDGE_SCALE:
                                scale = pair_edge / MIN_EDGE
                                size_per_side = min(BASE_SIZE * scale, MAX_SIZE)
                            else:
                                size_per_side = BASE_SIZE

                            # Cap based on unpaired exposure
                            for tok in [up_token, down_token]:
                                st = state[tok]
                                other_tok = down_token if tok == up_token else up_token
                                other_st = state[other_tok]

                                book = self.tracker.get_book(tok)
                                if not book:
                                    continue
                                bb = book["bb"]
                                if bb <= 0.01 or bb >= 0.99:
                                    continue
                                if book["mid"] < MIN_MID or book["mid"] > MAX_MID:
                                    continue
                                if book["bid_depth"] + book["ask_depth"] < MIN_DEPTH:
                                    continue

                                entry_price = round(math.floor(bb / TICK) * TICK, 2)
                                if entry_price <= 0.01:
                                    continue

                                # Check unpaired exposure limit
                                my_filled_cost = st["filled_cost"]
                                other_filled_cost = other_st["filled_cost"]
                                paired_cost = min(my_filled_cost, other_filled_cost)
                                my_unpaired_cost = my_filled_cost - paired_cost
                                remaining_unpaired = MAX_UNPAIRED_COST - my_unpaired_cost

                                # Only quote up to what keeps us within unpaired limit
                                # (any fills beyond the other side become directional risk)
                                # Allow full size_per_side if other side has room
                                effective_size = size_per_side
                                if my_filled_cost > other_filled_cost:
                                    # We already have more on this side — limit further buys
                                    effective_size = min(effective_size, remaining_unpaired)
                                if effective_size < MIN_BET:
                                    continue

                                shares = round(effective_size / entry_price, 2)
                                if shares < MIN_SHARES:
                                    continue

                                # Is re-quote needed? Skip if price unchanged
                                if st["buy_oid"] and st["buy_price"] == entry_price:
                                    continue

                                # Cancel stale order
                                if st["buy_oid"]:
                                    try:
                                        client.cancel(st["buy_oid"])
                                    except Exception:
                                        cancel_all_orders(tok)
                                    st["buy_oid"] = None
                                    time.sleep(0.3)

                                    # Check for partial fill from cancelled order
                                    self._check_fill(tok, st)

                                # Post new limit buy
                                log(f"    {st['label']} BUY @ {entry_price} "
                                    f"({shares:.1f}sh, ${effective_size:.0f}) "
                                    f"[edge={pair_edge:.3f}]")

                                if not DRY_RUN:
                                    oid = post_limit(tok, BUY, entry_price, shares)
                                    if oid:
                                        st["buy_oid"] = oid
                                        st["buy_price"] = entry_price
                                        st["buy_size"] = shares
                                    else:
                                        log(f"    {st['label']}: order failed")
                                else:
                                    st["buy_oid"] = "dry"
                                    st["buy_price"] = entry_price
                                    st["buy_size"] = shares

                        else:
                            # Edge too thin — cancel existing quotes
                            for tok in [up_token, down_token]:
                                st = state[tok]
                                if st["buy_oid"] and st["buy_oid"] != "dry":
                                    try:
                                        client.cancel(st["buy_oid"])
                                    except Exception:
                                        cancel_all_orders(tok)
                                    st["buy_oid"] = None
                                    # Check for fill before clearing
                                    self._check_fill(tok, st)

                time.sleep(CHECK_INTERVAL)

            # ── CANCEL PHASE: cancel unfilled orders ──
            log(f"  {crypto} cancel phase ({CANCEL_BUFFER}s to window end)")
            for tok in [up_token, down_token]:
                st = state[tok]
                if st["buy_oid"]:
                    cancel_all_orders(tok)
                    st["buy_oid"] = None
                    time.sleep(0.5)
                    self._check_fill(tok, st)

            # ── REPORT: calculate pair profit ──
            up_st = state[up_token]
            dn_st = state[down_token]
            up_shares = up_st["filled_shares"]
            dn_shares = dn_st["filled_shares"]
            up_cost = up_st["filled_cost"]
            dn_cost = dn_st["filled_cost"]

            if up_shares > 0 or dn_shares > 0:
                up_avg = up_cost / up_shares if up_shares > 0 else 0
                dn_avg = dn_cost / dn_shares if dn_shares > 0 else 0
                paired = min(up_shares, dn_shares)
                unpaired_up = up_shares - paired
                unpaired_dn = dn_shares - paired

                if paired > 0:
                    pair_profit = paired * (1.0 - up_avg - dn_avg)
                    self.total_paired_profit += pair_profit
                    self.pairs_completed += 1
                    log(f"  {crypto} PAIR: {paired:.0f}sh × "
                        f"(1.00 - {up_avg:.3f} - {dn_avg:.3f}) = "
                        f"${pair_profit:+.2f} guaranteed")

                if unpaired_up > 0:
                    self.single_legs += 1
                    log(f"  {crypto} UNPAIRED Up: {unpaired_up:.0f}sh @ {up_avg:.3f} "
                        f"(directional, resolves to $1 or $0)")
                if unpaired_dn > 0:
                    self.single_legs += 1
                    log(f"  {crypto} UNPAIRED Dn: {unpaired_dn:.0f}sh @ {dn_avg:.3f} "
                        f"(directional, resolves to $1 or $0)")

                self.total_trades += 1

                record_trade({
                    "time": datetime.now(timezone.utc).isoformat(),
                    "crypto": crypto,
                    "up_token": up_token[:20] + "...",
                    "down_token": down_token[:20] + "...",
                    "up_shares": round(up_shares, 2),
                    "down_shares": round(dn_shares, 2),
                    "up_avg_entry": round(up_avg, 4),
                    "down_avg_entry": round(dn_avg, 4),
                    "up_cost": round(up_cost, 4),
                    "down_cost": round(dn_cost, 4),
                    "paired_shares": round(paired, 2),
                    "paired_profit": round(paired * (1.0 - up_avg - dn_avg), 4) if paired > 0 else 0,
                    "unpaired_up": round(unpaired_up, 2),
                    "unpaired_dn": round(unpaired_dn, 2),
                    "dry_run": DRY_RUN,
                })
            else:
                log(f"  {crypto} no fills this window")

        except Exception as e:
            log(f"  {crypto} lifecycle error: {e}")
            import traceback
            traceback.print_exc()
            for tok in [up_token, down_token]:
                try:
                    cancel_all_orders(tok)
                except Exception:
                    pass
        finally:
            with self._active_lock:
                self._active_markets.pop(cid, None)

    def _check_fill(self, token_id, st):
        """Check if limit buy got (partially) filled by comparing share balance."""
        if DRY_RUN:
            # Simulate: assume fill happens after order is posted
            if st["buy_oid"] == "dry" and st["buy_price"] > 0:
                # Use book depth to estimate realistic fill fraction
                book = self.tracker.get_book(token_id)
                fill_frac = 0.5  # default
                if book:
                    # More depth at bid → higher fill probability
                    fill_frac = min(0.9, max(0.2, book["bid_depth"] / 200))
                sim_shares = st["buy_size"] * fill_frac
                sim_cost = sim_shares * st["buy_price"]
                st["filled_shares"] += sim_shares
                st["filled_cost"] += sim_cost
                st["buy_oid"] = None
                log(f"    [SIM FILL] {st['label']}: {sim_shares:.1f}sh @ {st['buy_price']} "
                    f"({fill_frac*100:.0f}% fill)")
            return

        bal = get_share_balance(token_id) or 0
        new_shares = bal - st["pre_bal"]

        if new_shares > st["filled_shares"] + 0.5:
            delta = new_shares - st["filled_shares"]
            cost_delta = delta * st["buy_price"] if st["buy_price"] > 0 else 0
            st["filled_shares"] = new_shares
            st["filled_cost"] += cost_delta
            log(f"    [FILL] {st['label']}: +{delta:.1f}sh @ {st['buy_price']} "
                f"(total: {new_shares:.1f}sh, ${st['filled_cost']:.2f})")

    # -- Cleanup --

    def _cleanup(self):
        if DRY_RUN:
            return
        log("  Global cleanup: cancelling all orders...")
        cancel_all_orders()
        time.sleep(2)

        # Don't force-sell — we WANT to hold to resolution
        # Only sell if something is stuck long after expiry
        all_tokens = set(self._known_tokens)
        stuck = []
        for tok in all_tokens:
            try:
                bal = get_share_balance(tok) or 0
                if bal >= MIN_SHARES:
                    stuck.append((tok, bal))
            except Exception:
                continue

        if stuck:
            log(f"  Holding {len(stuck)} positions to resolution:")
            for tok, bal in stuck:
                log(f"    {tok[:20]}... : {bal:.1f}sh")


# =========================================================================
# ENTRY POINT
# =========================================================================
if __name__ == "__main__":
    MIN_BET = 1  # repeated for clarity
    bot = ArbBot()
    bot.run()
