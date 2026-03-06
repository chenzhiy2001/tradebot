#!/usr/bin/env python3
"""
Whale Bot — Copy ANY large trades (≥$1000) on Polymarket 5-min crypto markets.

Strategy:
  1. Connect to RTDS WebSocket `activity` topic — streams ALL trades in real time.
  2. Accumulate trade fragments per (wallet, token). When accumulated value ≥ $1000,
     that's a "whale signal" — someone just committed serious capital.
  3. Anti-MM filter: if the same wallet trades BOTH sides of a market within 60s,
     they're market-making, not directional betting. Ignore them.
  4. Copy-buy with a FAK market order. Copy-sell if we hold and a whale sells.
  5. Hold to resolution — winner gets $1/share.
  6. Track live P&L per wallet; auto-mute wallets that lose us money.

Edge:
  $1000+ on a 5-min binary = serious conviction. No stale wallet lists needed —
  the size filter IS the quality filter.

Usage:
  python whale.py              # live trading
  python whale.py --dry-run    # simulate (no real orders)
"""

import os
import sys
import time
import json
import math
import asyncio
import threading
import requests
from collections import deque
from datetime import datetime, timezone, timedelta
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, BalanceAllowanceParams, AssetType
from py_clob_client.order_builder.constants import BUY, SELL
from py_clob_client import MarketOrderArgs
from dotenv import load_dotenv

load_dotenv()
private_key = os.getenv("PRIVATE_KEY")
founder_address = os.getenv("FUNDER_ADDRESS")

# =========================================================================
# CONFIG
# =========================================================================
HOST = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
RTDS_WS_URL = "wss://ws-live-data.polymarket.com"
CHAIN_ID = 137
FUNDER_ADDRESS = founder_address

DRY_RUN = "--dry-run" in sys.argv

# =========================================================================
# STRATEGY PARAMETERS
# =========================================================================
MIN_WHALE_SIZE = 1000         # Minimum accumulated $ to count as a whale trade
BUY_AMOUNT = 15               # Our bet size per copy-trade ($)
MIN_BET = 3                   # Minimum viable bet
ACCUM_WINDOW = 15             # Seconds to accumulate fragments from same wallet+token
COOLDOWN_SECS = 30            # Don't re-enter same market within this window
MAX_POSITIONS = 4             # Max concurrent positions
MAX_PER_WINDOW = 2            # Max new entries per 5-min window
MAX_RISK_PCT = 0.80           # Don't risk more than 80% of balance

# Anti market-maker: if wallet trades both sides within this window, ignore
MM_DETECT_WINDOW = 60         # Seconds

CRYPTOS = {"btc", "eth", "sol", "xrp"}
INTERVALS = [5]               # 5-min markets only

# Live performance — auto-mute wallets losing us money
MIN_LIVE_TRADES = 3           # Need this many resolved before muting
MUTE_THRESHOLD = 0            # Mute if PnL below this

# Polymarket fee
CRYPTO_FEE_RATE = 0.25
CRYPTO_FEE_EXPONENT = 2

# Files
LOG_FILE = "whale_log.txt"
TRADE_LOG = "whale_trades.json"
LIVE_PERF_FILE = "whale_perf.json"


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


def get_price(token_id, side=BUY):
    try:
        data = client.get_price(token_id, side=side)
        return float(data.get("price", 0))
    except Exception:
        return 0.0


def compute_taker_fee(shares, price):
    if price <= 0 or price >= 1:
        return 0.0
    fee_shares = shares * CRYPTO_FEE_RATE * (price * (1 - price)) ** CRYPTO_FEE_EXPONENT
    return round(fee_shares * price, 4)


# =========================================================================
# MARKET CACHE — maps condition_id → market info
# =========================================================================
class MarketCache:
    def __init__(self):
        self._cache = {}
        self._token_map = {}
        self._last_refresh = 0
        self._lock = threading.Lock()

    def refresh(self):
        markets = {}
        token_map = {}
        now = datetime.now(timezone.utc)

        for interval in INTERVALS:
            aligned = (now.minute // interval) * interval
            window_start = now.replace(minute=aligned, second=0, microsecond=0)
            window_end = window_start + timedelta(minutes=interval)
            epoch = int(window_start.timestamp())

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
                        condition_id = m.get("conditionId", "")
                        tokens = json.loads(m.get("clobTokenIds", "[]"))
                        outcomes = json.loads(m.get("outcomes", "[]"))
                        if len(tokens) != 2 or len(outcomes) != 2:
                            continue

                        up_idx = next((i for i, o in enumerate(outcomes) if "up" in o.lower()), 0)
                        down_idx = 1 - up_idx

                        market_info = {
                            "condition_id": condition_id,
                            "question": m.get("question", ""),
                            "crypto": crypto.upper(),
                            "interval": interval,
                            "slug": slug,
                            "epoch": epoch,
                            "window_start": window_start,
                            "window_end": window_end,
                            "tokens": tokens,
                            "up_token": tokens[up_idx],
                            "down_token": tokens[down_idx],
                            "up_idx": up_idx,
                            "down_idx": down_idx,
                        }
                        markets[condition_id] = market_info

                        for idx, tok in enumerate(tokens):
                            side_name = "Up" if idx == up_idx else "Down"
                            token_map[tok] = {
                                "condition_id": condition_id,
                                "side": side_name,
                                "token_id": tok,
                            }
                except Exception as e:
                    log(f"  ⚠ Market fetch {slug}: {e}")

        with self._lock:
            self._cache = markets
            self._token_map = token_map
            self._last_refresh = time.time()

        return markets

    def get_by_condition(self, condition_id):
        with self._lock:
            return self._cache.get(condition_id)

    def get_by_token(self, token_id):
        with self._lock:
            return self._token_map.get(token_id)

    def all_condition_ids(self):
        with self._lock:
            return set(self._cache.keys())

    def needs_refresh(self):
        if time.time() - self._last_refresh > 30:
            return True
        now = datetime.now(timezone.utc)
        aligned = (now.minute // 5) * 5
        window_start = now.replace(minute=aligned, second=0, microsecond=0)
        epoch = int(window_start.timestamp())
        with self._lock:
            for cid, info in self._cache.items():
                if info["epoch"] == epoch:
                    return False
        return True


# =========================================================================
# LIVE PERFORMANCE TRACKING
# =========================================================================
def load_live_perf():
    try:
        with open(LIVE_PERF_FILE, "r") as f:
            data = json.load(f)
        if data.get("_date") != datetime.now(timezone.utc).strftime("%Y-%m-%d"):
            return {"_date": datetime.now(timezone.utc).strftime("%Y-%m-%d")}
        return data
    except (FileNotFoundError, json.JSONDecodeError):
        return {"_date": datetime.now(timezone.utc).strftime("%Y-%m-%d")}


def save_live_perf(perf):
    with open(LIVE_PERF_FILE, "w") as f:
        json.dump(perf, f, indent=2)


def is_wallet_muted(wallet_addr, live_perf):
    if wallet_addr not in live_perf:
        return False
    stats = live_perf[wallet_addr]
    total = stats.get("wins", 0) + stats.get("losses", 0)
    if total < MIN_LIVE_TRADES:
        return False
    return stats.get("pnl", 0) < MUTE_THRESHOLD


# =========================================================================
# RTDS ACTIVITY FEED
# =========================================================================
class ActivityFeed:
    def __init__(self, on_trade_callback):
        self._on_trade = on_trade_callback
        self._connected = False
        self._msg_count = 0
        self._trade_count = 0

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
                    RTDS_WS_URL, close_timeout=5, open_timeout=10
                ) as ws:
                    sub_msg = json.dumps({
                        "action": "subscribe",
                        "subscriptions": [{
                            "topic": "activity",
                            "type": "*",
                            "filters": "",
                        }]
                    })
                    await ws.send(sub_msg)
                    self._connected = True
                    log("  🔗 RTDS activity feed connected")

                    last_ping = time.time()
                    last_data = time.time()

                    while True:
                        if time.time() - last_ping > 4:
                            try:
                                await ws.send("PING")
                                last_ping = time.time()
                            except Exception:
                                break

                        if time.time() - last_data > 60:
                            log("  ⚠ Activity feed stale (60s) — reconnecting")
                            break

                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1)
                        except asyncio.TimeoutError:
                            continue

                        self._msg_count += 1
                        last_data = time.time()

                        try:
                            data = json.loads(msg)
                        except (json.JSONDecodeError, TypeError):
                            continue

                        topic = data.get("topic")
                        evt_type = data.get("type")
                        if topic == "activity" and evt_type in ("trades", "orders_matched"):
                            payload = data.get("payload", {})
                            self._trade_count += 1
                            self._on_trade(payload)

            except Exception as e:
                self._connected = False
                log(f"  ⚠ Activity feed error: {e}, reconnecting in 2s...")
                await asyncio.sleep(2)

    @property
    def connected(self):
        return self._connected

    @property
    def stats(self):
        return f"msgs={self._msg_count} trades={self._trade_count}"


# =========================================================================
# WHALE BOT
# =========================================================================
class WhaleBot:
    def __init__(self):
        self.live_perf = load_live_perf()
        self.market_cache = MarketCache()
        self.positions = {}           # token_id → position info
        self.cooldowns = {}           # condition_id → expiry timestamp
        self._trade_queue = deque()
        self._lock = threading.Lock()
        self._start_balance = None
        self._session_pnl = 0.0
        self._session_trades = 0
        self._session_wins = 0

        # Accumulator: (wallet, token_id) → {shares, dollar_total, ...}
        self._accum = {}

        # Anti-MM tracker: wallet → {condition_id → {sides: set(), last_seen: float}}
        self._mm_tracker = {}

        self._window_entries = {}     # epoch_key → count

    # ─── MAIN LOOP ───────────────────────────────────────────────────

    def run(self):
        log(f"\n{'='*60}")
        log(f"Whale bot {'(DRY RUN) ' if DRY_RUN else ''}started")
        log(f"Whale threshold: ${MIN_WHALE_SIZE}")
        log(f"Buy amount: ${BUY_AMOUNT}, max positions: {MAX_POSITIONS}")
        log(f"Markets: {', '.join(sorted(CRYPTOS))} × {INTERVALS}min")
        log(f"{'='*60}\n")

        self._start_balance = get_usdc_balance()
        if self._start_balance is not None:
            log(f"  Starting USDC: ${self._start_balance:.2f}")

        feed = ActivityFeed(on_trade_callback=self._on_ws_trade)
        feed.start()

        for _ in range(30):
            if feed.connected:
                break
            time.sleep(0.5)
        if not feed.connected:
            log("  ⚠ Activity feed failed to connect after 15s")

        self.market_cache.refresh()
        log(f"  Discovered {len(self.market_cache.all_condition_ids())} active markets")

        last_status = 0
        while True:
            try:
                self._process_queue()
                self._flush_accumulators()

                if self.market_cache.needs_refresh():
                    self.market_cache.refresh()

                self._check_resolutions()
                self._clean_mm_tracker()

                if time.time() - last_status > 30:
                    self._print_status(feed)
                    last_status = time.time()

                time.sleep(0.1)

            except KeyboardInterrupt:
                self._shutdown()
                break
            except Exception as e:
                log(f"  ⚠ Main loop error: {e}")
                time.sleep(2)

    # ─── WS CALLBACK ─────────────────────────────────────────────────

    def _on_ws_trade(self, payload):
        self._trade_queue.append(payload)

    # ─── PROCESS QUEUE ───────────────────────────────────────────────

    def _process_queue(self):
        while self._trade_queue:
            try:
                payload = self._trade_queue.popleft()
            except IndexError:
                break
            self._handle_trade(payload)

    # ─── ANTI-MM DETECTION ───────────────────────────────────────────

    def _record_mm_activity(self, wallet, condition_id, side_name):
        """Track which sides a wallet has traded for MM detection."""
        now = time.time()
        if wallet not in self._mm_tracker:
            self._mm_tracker[wallet] = {}
        if condition_id not in self._mm_tracker[wallet]:
            self._mm_tracker[wallet][condition_id] = {"sides": set(), "last_seen": now}
        entry = self._mm_tracker[wallet][condition_id]
        entry["sides"].add(side_name)
        entry["last_seen"] = now

    def _is_market_maker(self, wallet, condition_id):
        """Returns True if wallet traded both sides of this market recently."""
        entry = self._mm_tracker.get(wallet, {}).get(condition_id)
        if not entry:
            return False
        if time.time() - entry["last_seen"] > MM_DETECT_WINDOW:
            return False
        return len(entry["sides"]) >= 2

    def _clean_mm_tracker(self):
        """Remove stale entries from MM tracker."""
        now = time.time()
        stale_wallets = []
        for wallet, markets in self._mm_tracker.items():
            stale_cids = [cid for cid, e in markets.items()
                          if now - e["last_seen"] > MM_DETECT_WINDOW * 2]
            for cid in stale_cids:
                del markets[cid]
            if not markets:
                stale_wallets.append(wallet)
        for w in stale_wallets:
            del self._mm_tracker[w]

    # ─── HANDLE INCOMING TRADE ───────────────────────────────────────

    def _handle_trade(self, payload):
        proxy_wallet = (payload.get("proxyWallet") or "").lower()
        if not proxy_wallet:
            return

        side = payload.get("side", "").upper()       # BUY or SELL
        size = float(payload.get("size", 0))
        price = float(payload.get("price", 0))
        condition_id = payload.get("conditionId", "")
        token_id = payload.get("asset", "")
        outcome = payload.get("outcome", "")
        event_slug = payload.get("eventSlug", "")
        name = payload.get("name", "") or payload.get("pseudonym", "") or proxy_wallet[:10]

        trade_value = size * price

        # Only care about crypto 5-min markets
        is_crypto = False
        for crypto in CRYPTOS:
            for interval in INTERVALS:
                if f"{crypto}-updown-{interval}m-" in (event_slug or "").lower():
                    is_crypto = True
                    break
            if is_crypto:
                break
        if not is_crypto:
            return

        # Track for MM detection
        token_info = self.market_cache.get_by_token(token_id)
        side_name = token_info["side"] if token_info else outcome
        self._record_mm_activity(proxy_wallet, condition_id, side_name)

        # Accumulate into (wallet, token_id) bucket
        key = (proxy_wallet, token_id)
        now = time.time()

        if key in self._accum:
            acc = self._accum[key]
            if acc["side"] != side or (now - acc["last_seen"]) > ACCUM_WINDOW:
                del self._accum[key]
            else:
                acc["shares"] += size
                acc["dollar_total"] += trade_value
                acc["last_seen"] = now
                acc["fragments"] += 1
                return

        self._accum[key] = {
            "shares": size,
            "dollar_total": trade_value,
            "first_seen": now,
            "last_seen": now,
            "side": side,
            "condition_id": condition_id,
            "outcome": outcome,
            "event_slug": event_slug,
            "name": name,
            "fragments": 1,
            "triggered": False,
        }

    # ─── FLUSH ACCUMULATORS ──────────────────────────────────────────

    def _flush_accumulators(self):
        now = time.time()
        expired = []
        for key, acc in list(self._accum.items()):
            stale = now - acc["last_seen"]

            if acc["triggered"]:
                if stale > ACCUM_WINDOW:
                    expired.append(key)
                continue

            if acc["dollar_total"] >= MIN_WHALE_SIZE:
                acc["triggered"] = True
                self._trigger_whale(key, acc)
            elif stale > ACCUM_WINDOW:
                expired.append(key)

        for key in expired:
            self._accum.pop(key, None)

    # ─── TRIGGER WHALE TRADE ─────────────────────────────────────────

    def _trigger_whale(self, key, acc):
        wallet_addr, token_id = key
        trade_side = acc["side"]
        total_shares = acc["shares"]
        total_dollar = acc["dollar_total"]
        avg_price = total_dollar / total_shares if total_shares > 0 else 0
        condition_id = acc["condition_id"]
        outcome = acc["outcome"]
        name = acc["name"]

        log(f"\n  🐋 WHALE: {name} ({wallet_addr[:12]}…) {trade_side} "
            f"{total_shares:.0f}sh @ {avg_price:.2f} (${total_dollar:.0f}) "
            f"[{acc['fragments']} frags] — {outcome}")

        # Anti-MM check
        if self._is_market_maker(wallet_addr, condition_id):
            log(f"     ⊘ Skip: market maker (traded both sides)")
            return

        # Muted wallet check
        if is_wallet_muted(wallet_addr, self.live_perf):
            stats = self.live_perf.get(wallet_addr, {})
            log(f"     ⊘ Skip: muted wallet (PnL=${stats.get('pnl', 0):+.1f} "
                f"over {stats.get('wins', 0)}W/{stats.get('losses', 0)}L)")
            return

        if trade_side == "SELL":
            self._handle_whale_sell(wallet_addr, name, token_id, condition_id,
                                    total_shares, avg_price)
            return

        # ── BUY path ──
        market_info = self.market_cache.get_by_condition(condition_id)
        if not market_info:
            self.market_cache.refresh()
            market_info = self.market_cache.get_by_condition(condition_id)
        if not market_info:
            log(f"     ⊘ Skip: market not found in cache")
            return

        now_utc = datetime.now(timezone.utc)
        secs_left = (market_info["window_end"] - now_utc).total_seconds()
        if secs_left < 30:
            log(f"     ⊘ Skip: only {secs_left:.0f}s left in window")
            return

        if condition_id in self.cooldowns and time.time() < self.cooldowns[condition_id]:
            remaining = self.cooldowns[condition_id] - time.time()
            log(f"     ⊘ Skip: cooldown ({remaining:.0f}s left)")
            return

        if len(self.positions) >= MAX_POSITIONS:
            log(f"     ⊘ Skip: max positions ({MAX_POSITIONS})")
            return

        # Budget check
        balance = get_usdc_balance()
        if balance is not None:
            total_at_risk = sum(p["cost"] for p in self.positions.values())
            total_capital = balance + total_at_risk
            if total_at_risk + BUY_AMOUNT > total_capital * MAX_RISK_PCT:
                log(f"     ⊘ Skip: risk cap (${total_at_risk + BUY_AMOUNT:.0f} > "
                    f"{MAX_RISK_PCT*100:.0f}% of ${total_capital:.0f})")
                return

        # Per-window limit
        wkey = market_info["window_end"].isoformat()
        entries = self._window_entries.get(wkey, 0)
        if entries >= MAX_PER_WINDOW:
            log(f"     ⊘ Skip: {entries} entries this window (max {MAX_PER_WINDOW})")
            return

        # Don't double-enter
        if token_id in self.positions:
            log(f"     ⊘ Skip: already holding this token")
            return
        for tok, pos in self.positions.items():
            if pos.get("condition_id") == condition_id:
                log(f"     ⊘ Skip: already in this market")
                return

        success = self._execute_buy(
            token_id=token_id,
            condition_id=condition_id,
            side=outcome,
            price=avg_price,
            wallet_addr=wallet_addr,
            wallet_name=name,
            market_info=market_info,
            whale_size=total_dollar,
        )
        if success:
            self._window_entries[wkey] = entries + 1

    # ─── WHALE SELL — exit if we hold ────────────────────────────────

    def _handle_whale_sell(self, wallet_addr, name, token_id, condition_id,
                           sell_shares, sell_price):
        our_token = None
        our_pos = None
        if token_id in self.positions:
            our_token = token_id
            our_pos = self.positions[token_id]
        else:
            for tok, pos in self.positions.items():
                if pos.get("condition_id") == condition_id:
                    our_token = tok
                    our_pos = pos
                    break

        if not our_pos:
            log(f"     (we don't hold this market)")
            return

        log(f"     🏃 WHALE SELL → exiting {our_pos['market_info']['crypto']} {our_pos['side']}")

        if DRY_RUN:
            cp = get_price(our_token, side=SELL)
            pnl = (cp - our_pos["entry_price"]) * our_pos["shares"] if cp > 0 else 0
            log(f"     🧪 DRY RUN — would sell {our_pos['shares']:.1f}sh @ ~{cp:.2f} (P&L: ${pnl:+.2f})")
            self._record_close(our_token, our_pos, cp, "WHALE_SELL")
            return

        try:
            actual_bal = get_share_balance(our_token)
            if not actual_bal or actual_bal < 0.5:
                log(f"     ⚠ No shares to sell (balance={actual_bal})")
                self._record_close(our_token, our_pos, our_pos["entry_price"], "WHALE_SELL_EMPTY")
                return

            sell_amt = math.floor(actual_bal * 100) / 100
            mo = MarketOrderArgs(
                token_id=our_token,
                amount=sell_amt,
                side=SELL,
                order_type=OrderType.FAK,
            )
            signed = client.create_market_order(mo)
            resp = client.post_order(signed, OrderType.FAK)

            cp = get_price(our_token, side=SELL)
            if cp <= 0:
                cp = sell_price
            pnl = (cp * sell_amt) - our_pos["cost"]
            log(f"     ✓ Sold {sell_amt:.1f}sh @ ~{cp:.2f} (P&L: ${pnl:+.2f})")
            self._record_close(our_token, our_pos, cp, "WHALE_SELL")

        except Exception as e:
            log(f"     ✗ Sell failed: {e}")

    # ─── EXECUTE BUY ─────────────────────────────────────────────────

    def _execute_buy(self, *, token_id, condition_id, side, price,
                     wallet_addr, wallet_name, market_info, whale_size):
        amount = BUY_AMOUNT

        balance = get_usdc_balance()
        if balance is None:
            log(f"     ⚠ Can't get balance — skipping")
            return False
        if balance < MIN_BET:
            log(f"     ⚠ Balance ${balance:.2f} < ${MIN_BET} — skipping")
            return False
        amount = min(amount, math.floor(balance * 100) / 100)

        current_price = get_price(token_id, side=BUY)
        if current_price <= 0:
            current_price = price

        log(f"     ⚡ COPY {side} — ${amount:.2f} @ {current_price:.2f} "
            f"(whale: {wallet_name}, ${whale_size:.0f})")

        if DRY_RUN:
            shares_est = amount / current_price if current_price > 0 else 0
            log(f"     🧪 DRY RUN — would buy ~{shares_est:.1f} shares")
            self.positions[token_id] = {
                "token_id": token_id,
                "condition_id": condition_id,
                "side": side,
                "entry_price": current_price,
                "shares": shares_est,
                "cost": amount,
                "entry_time": time.time(),
                "wallet_addr": wallet_addr,
                "wallet_name": wallet_name,
                "market_info": {
                    "question": market_info["question"],
                    "crypto": market_info["crypto"],
                    "slug": market_info["slug"],
                    "window_end": market_info["window_end"].isoformat(),
                },
                "dry_run": True,
            }
            self.cooldowns[condition_id] = time.time() + COOLDOWN_SECS
            return True

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
            actual_cost = amount
            if isinstance(resp, dict):
                taking = resp.get("takingAmount", "0")
                making = resp.get("makingAmount", "0")
                try:
                    actual_shares = float(taking) if taking else 0
                except (ValueError, TypeError):
                    actual_shares = 0
                try:
                    filled_cost = float(making) if making else 0
                    if filled_cost > 0:
                        actual_cost = filled_cost
                except (ValueError, TypeError):
                    pass

            if actual_shares <= 0:
                actual_shares = amount / current_price if current_price > 0 else 0
                actual_cost = amount

            time.sleep(1)
            on_chain = get_share_balance(token_id)
            if on_chain and on_chain > 0.5:
                actual_shares = on_chain

            self.positions[token_id] = {
                "token_id": token_id,
                "condition_id": condition_id,
                "side": side,
                "entry_price": current_price,
                "shares": actual_shares,
                "cost": actual_cost,
                "entry_time": time.time(),
                "wallet_addr": wallet_addr,
                "wallet_name": wallet_name,
                "market_info": {
                    "question": market_info["question"],
                    "crypto": market_info["crypto"],
                    "slug": market_info["slug"],
                    "window_end": market_info["window_end"].isoformat(),
                },
                "dry_run": False,
            }
            self.cooldowns[condition_id] = time.time() + COOLDOWN_SECS

            log(f"     ✓ Bought {actual_shares:.1f}sh @ {current_price:.2f} for ${actual_cost:.2f}")

            record = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "action": "WHALE_BUY",
                "market": market_info["question"],
                "crypto": market_info["crypto"],
                "side": side,
                "token": token_id,
                "price": current_price,
                "amount": amount,
                "actual_cost": actual_cost,
                "shares": actual_shares,
                "whale_addr": wallet_addr,
                "whale_name": wallet_name,
                "whale_size": whale_size,
                "response": str(resp),
            }
            with open(TRADE_LOG, "a") as f:
                f.write(json.dumps(record) + "\n")

            return True

        except Exception as e:
            log(f"     ✗ Buy failed: {e}")
            return False

    # ─── RECORD CLOSE ────────────────────────────────────────────────

    def _record_close(self, token_id, pos, exit_price, action):
        shares = pos["shares"]
        cost = pos["cost"]
        pnl = (exit_price * shares) - cost

        self._session_pnl += pnl
        self._session_trades += 1
        if pnl >= 0:
            self._session_wins += 1

        wallet_addr = pos.get("wallet_addr", "")
        wallet_name = pos.get("wallet_name", "")
        won = pnl >= 0
        if wallet_addr and wallet_addr != "_date":
            if wallet_addr not in self.live_perf or not isinstance(self.live_perf.get(wallet_addr), dict):
                self.live_perf[wallet_addr] = {
                    "name": wallet_name, "wins": 0, "losses": 0, "pnl": 0.0
                }
            if won:
                self.live_perf[wallet_addr]["wins"] += 1
            else:
                self.live_perf[wallet_addr]["losses"] += 1
            self.live_perf[wallet_addr]["pnl"] += pnl
            save_live_perf(self.live_perf)

        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": action,
            "market": pos["market_info"]["question"],
            "side": pos["side"],
            "token": token_id,
            "entry_price": pos["entry_price"],
            "exit_price": exit_price,
            "shares": shares,
            "cost": cost,
            "pnl": round(pnl, 4),
            "whale_addr": wallet_addr,
            "whale_name": wallet_name,
        }
        with open(TRADE_LOG, "a") as f:
            f.write(json.dumps(record) + "\n")

        if token_id in self.positions:
            del self.positions[token_id]

    # ─── RESOLUTION CHECK ────────────────────────────────────────────

    def _check_resolutions(self):
        now_utc = datetime.now(timezone.utc)
        tokens_to_remove = []

        for token_id, pos in self.positions.items():
            window_end_str = pos["market_info"].get("window_end", "")
            try:
                window_end = datetime.fromisoformat(window_end_str)
            except (ValueError, TypeError):
                continue

            secs_past = (now_utc - window_end).total_seconds()
            if secs_past < 90:
                continue

            slug = pos["market_info"].get("slug", "")
            if not slug:
                if secs_past > 600:
                    tokens_to_remove.append(token_id)
                continue

            won = None
            try:
                r = requests.get(f"{GAMMA_API}/events/slug/{slug}", timeout=10)
                if r.status_code == 200:
                    event_data = r.json()
                    mlist = event_data.get("markets", [])
                    if mlist:
                        mdata = mlist[0]
                        is_closed = mdata.get("closed", False)
                        op = json.loads(mdata.get("outcomePrices", "[]"))
                        if op and len(op) >= 2:
                            up_price = float(op[0])
                            if up_price > 0.90 and is_closed:
                                winner_side = "Up"
                            elif up_price < 0.10 and is_closed:
                                winner_side = "Down"
                            else:
                                if secs_past > 600:
                                    tokens_to_remove.append(token_id)
                                continue

                            pos_side = pos["side"].lower().strip()
                            winner_lower = winner_side.lower().strip()
                            won = (pos_side == winner_lower)
            except Exception:
                if secs_past > 600:
                    tokens_to_remove.append(token_id)
                continue

            if won is None:
                if secs_past > 600:
                    tokens_to_remove.append(token_id)
                continue

            cost = pos["cost"]
            shares = pos["shares"]
            entry_price = pos["entry_price"]

            if won:
                proceeds = shares * 1.0
                fee = compute_taker_fee(shares, entry_price)
                pnl = proceeds - cost - fee
                self._session_wins += 1
                log(f"\n  ✅ WON: {pos['market_info']['question']}")
            else:
                pnl = -cost
                log(f"\n  ❌ LOST: {pos['market_info']['question']}")

            self._session_pnl += pnl
            log(f"     {pos['side']} @ {entry_price:.2f} → P&L: ${pnl:+.2f} "
                f"(whale: {pos.get('wallet_name', '?')})")

            wallet_addr = pos.get("wallet_addr", "")
            wallet_name = pos.get("wallet_name", "")
            if wallet_addr and wallet_addr != "_date":
                if wallet_addr not in self.live_perf or not isinstance(self.live_perf.get(wallet_addr), dict):
                    self.live_perf[wallet_addr] = {
                        "name": wallet_name, "wins": 0, "losses": 0, "pnl": 0.0
                    }
                if won:
                    self.live_perf[wallet_addr]["wins"] += 1
                else:
                    self.live_perf[wallet_addr]["losses"] += 1
                self.live_perf[wallet_addr]["pnl"] += pnl

                perf = self.live_perf[wallet_addr]
                muted_str = " [MUTED]" if is_wallet_muted(wallet_addr, self.live_perf) else ""
                log(f"     {wallet_name}: {perf['wins']}W/{perf['losses']}L "
                    f"PnL=${perf['pnl']:+.1f}{muted_str}")
                save_live_perf(self.live_perf)

            record = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "action": "RESOLVED",
                "market": pos["market_info"]["question"],
                "side": pos["side"],
                "token": token_id,
                "entry_price": entry_price,
                "shares": shares,
                "cost": cost,
                "won": won,
                "pnl": round(pnl, 4),
                "whale_addr": wallet_addr,
                "whale_name": wallet_name,
            }
            with open(TRADE_LOG, "a") as f:
                f.write(json.dumps(record) + "\n")

            tokens_to_remove.append(token_id)

        for token_id in tokens_to_remove:
            if token_id in self.positions:
                del self.positions[token_id]

    # ─── STATUS ──────────────────────────────────────────────────────

    def _print_status(self, feed):
        now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
        n_markets = len(self.market_cache.all_condition_ids())
        n_pos = len(self.positions)
        n_muted = sum(1 for addr in self.live_perf
                      if addr != "_date" and is_wallet_muted(addr, self.live_perf))
        n_tracked = sum(1 for addr in self.live_perf if addr != "_date")

        log(f"\n  ⏰ {now_str} UTC | Feed: {'✓' if feed.connected else '✗'} ({feed.stats}) "
            f"| Markets: {n_markets} | Pos: {n_pos}/{MAX_POSITIONS} "
            f"| Whales seen: {n_tracked} ({n_muted} muted)")

        if self._session_trades > 0:
            wr = self._session_wins / self._session_trades * 100
            log(f"     Session: {self._session_trades} trades, {self._session_wins}W, "
                f"PnL=${self._session_pnl:+.2f}, WR={wr:.0f}%")

        if self.positions:
            for tok, pos in self.positions.items():
                try:
                    window_end = datetime.fromisoformat(pos["market_info"]["window_end"])
                    secs_left = max(0, (window_end - datetime.now(timezone.utc)).total_seconds())
                except (ValueError, TypeError):
                    secs_left = 0
                cp = get_price(tok, side=SELL)
                gain = cp - pos["entry_price"]
                emoji = "📈" if gain >= 0 else "📉"
                log(f"     {emoji} {pos['market_info']['crypto']} {pos['side']}: "
                    f"bought {pos['entry_price']:.2f} → now {cp:.2f} ({gain:+.2f}) "
                    f"[{secs_left:.0f}s left] whale: {pos.get('wallet_name', '?')}")

    # ─── SHUTDOWN ─────────────────────────────────────────────────────

    def _shutdown(self):
        save_live_perf(self.live_perf)

        if self.positions:
            log(f"\n  🛑 Shutting down — selling {len(self.positions)} open position(s)...")
            for tok in list(self.positions.keys()):
                pos = self.positions[tok]
                log(f"  Selling {pos['market_info']['crypto']} {pos['side']} "
                    f"({pos['shares']:.1f}sh @ {pos['entry_price']:.2f})...")
                if DRY_RUN:
                    cp = get_price(tok, side=SELL)
                    pnl = (cp - pos["entry_price"]) * pos["shares"] if cp > 0 else 0
                    log(f"     🧪 DRY RUN — would sell @ ~{cp:.2f} (P&L: ${pnl:+.2f})")
                    self._record_close(tok, pos, cp, "SHUTDOWN_SELL")
                    continue
                try:
                    actual_bal = get_share_balance(tok)
                    if not actual_bal or actual_bal < 0.5:
                        log(f"     ⚠ No shares to sell (balance={actual_bal})")
                        self._record_close(tok, pos, 0, "SHUTDOWN_EMPTY")
                        continue
                    sell_amt = math.floor(actual_bal * 100) / 100
                    mo = MarketOrderArgs(
                        token_id=tok,
                        amount=sell_amt,
                        side=SELL,
                        order_type=OrderType.FAK,
                    )
                    signed = client.create_market_order(mo)
                    resp = client.post_order(signed, OrderType.FAK)
                    cp = get_price(tok, side=SELL)
                    if cp <= 0:
                        cp = pos["entry_price"]
                    pnl = (cp * sell_amt) - pos["cost"]
                    log(f"     ✓ Sold {sell_amt:.1f}sh @ ~{cp:.2f} (P&L: ${pnl:+.2f})")
                    self._record_close(tok, pos, cp, "SHUTDOWN_SELL")
                except Exception as e:
                    log(f"     ✗ Shutdown sell failed: {e}")
            self.positions.clear()

        final_balance = get_usdc_balance()
        log(f"\n{'='*60}")
        log(f"Whale bot stopped")
        log(f"Session: {self._session_trades} trades, {self._session_wins} wins")
        log(f"Book P&L: ${self._session_pnl:+.2f}")
        if self._start_balance is not None and final_balance is not None:
            delta = final_balance - self._start_balance
            log(f"USDC delta: ${delta:+.2f} (${self._start_balance:.2f} → ${final_balance:.2f})")
        log(f"{'='*60}")


# =========================================================================
# MAIN
# =========================================================================
if __name__ == "__main__":
    bot = WhaleBot()
    bot.run()
