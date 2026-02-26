#!/usr/bin/env python3
"""
BTC80 Bot — Buy BTC side at 80¢, GTC limit sell at 99¢, stop loss at 60¢.

Strategy:
  For each BTC 5-minute crypto market on Polymarket:
  1. Monitor BTC UP and DOWN mid-prices via WebSocket.
  2. When either side's mid-price first reaches ≥ 0.80, buy with 99% of tracked balance.
  3. After on-chain settlement, place a GTC limit sell at 0.99.
  4. Monitor price — if mid drops to ≤ 0.60, cancel the limit and FOK sell (stop loss).
  5. After exit: check order-book ask depth. If balance > depth, reset to $10.
  6. Repeat.

Balance Management:
  A virtual tracked balance starts at $10 and compounds through wins.
  When the tracked balance exceeds what the order book can actually fill,
  reset to $10 (take profits off the table).

Usage:
  python btc80.py             # live trading
  python btc80.py --dry-run   # simulate trades using WS prices
"""

import os
import sys
import time
import json
import threading
import asyncio
import requests
from datetime import datetime, timezone, timedelta
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    MarketOrderArgs, OrderArgs, OrderType,
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
INITIAL_BALANCE = 10.0        # Starting tracked balance / profit-taking reset value
BET_PCT = 0.99                # Use 99% of tracked balance per trade
MIN_BET = 5                   # Polymarket minimum bet
MAX_BET = 50                  # Safety cap

ENTRY_MID = 0.80              # Buy when either BTC side mid-price reaches this
MAX_ENTRY_MID = 0.92          # Don't buy above this — risk/reward too bad
LIMIT_SELL_PRICE = 0.99       # GTC limit sell price (take profit)
STOP_LOSS_MID = 0.60          # FOK sell if mid drops to this (stop loss)

# Polymarket fee formula (5m crypto)
CRYPTO_FEE_RATE = 0.25
CRYPTO_FEE_EXPONENT = 2

# Timing
TICK_INTERVAL = 0.25          # 250ms main loop tick
ORDER_POLL_SECS = 3.0         # How often to poll share balance for GTC fill detection
SETTLEMENT_TIMEOUT = 30       # Max seconds to wait for on-chain settlement
RESOLUTION_GRACE = 15         # Seconds after window end to check for resolution
NO_MATCH_BACKOFF = 10         # Seconds to stop retrying after first "no match" error
STOP_LOSS_COOLDOWN = 60       # Seconds to wait after a stop loss before re-entering
SELL_MAX_RETRIES = 5          # Max FOK sell attempts for stop loss (was 3)
SELL_RETRY_SLEEP = 1.0        # Seconds between sell retries (was 0.5)

# Data files
LOG_FILE = "btc80_log.txt"
TRADE_LOG = "btc80_trades.json"

DRY_RUN = "--dry-run" in sys.argv


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
    """Get on-chain share balance for a conditional token."""
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
    """Poll until on-chain balance shows NEW shares, or timeout."""
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


def get_ask_depth_usdc(token_id):
    """Get total ask-side USDC depth from order book.
    This tells us the maximum we could spend on a single FOK buy."""
    try:
        book = client.get_order_book(token_id)
        asks = book.asks if hasattr(book, "asks") else []
        total = 0.0
        for ask in asks:
            if isinstance(ask, dict):
                price = float(ask.get("price", 0))
                size = float(ask.get("size", 0))
            else:
                price = float(getattr(ask, "price", 0))
                size = float(getattr(ask, "size", 0))
            total += size * price
        return round(total, 2)
    except Exception as e:
        log(f"  ⚠ Order book error: {e}")
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
                    log("  🔌 WS connected")
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
                log(f"  ⚠ WS error: {e}, reconnecting in 2s...")
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
# MARKET DISCOVERY — BTC 5-minute only
# =========================================================================
def discover_btc_market():
    """Fetch current active BTC 5m up/down market from Polymarket.
    Returns dict with epoch, start, end, up_token, down_token or None."""
    now = datetime.now(timezone.utc)
    interval = 5
    aligned = (now.minute // interval) * interval
    window_start = now.replace(minute=aligned, second=0, microsecond=0)
    window_end = window_start + timedelta(minutes=interval)

    # If current window already ended, skip
    if now >= window_end:
        return None

    epoch = int(window_start.timestamp())
    slug = f"btc-updown-{interval}m-{epoch}"

    try:
        resp = requests.get(f"{GAMMA_API}/events/slug/{slug}", timeout=10)
        if resp.status_code != 200:
            return None
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

            return {
                "epoch": epoch,
                "start": window_start,
                "end": window_end,
                "up_token": tokens[up_idx],
                "down_token": tokens[down_idx],
            }
    except Exception as e:
        log(f"  ⚠ Discovery error: {e}")
    return None


# =========================================================================
# BTC80 BOT
# =========================================================================
class BTC80Bot:
    """Buy BTC side at 80¢, limit sell at 99¢, stop loss at 60¢."""

    def __init__(self, poly):
        self.poly = poly
        self.tracked_balance = INITIAL_BALANCE
        self.position = None
        self.current_window = None
        self.trade_count = 0
        self.win_count = 0
        self.total_pnl = 0.0
        self._no_match_until = 0  # Backoff timestamp after "no match" errors
        self._stop_loss_until = 0  # Cooldown timestamp after stop loss

    def update_market(self, window):
        """Update current window and subscribe to tokens."""
        old_epoch = self.current_window["epoch"] if self.current_window else None
        self.current_window = window
        if window and window["epoch"] != old_epoch:
            tokens = [window["up_token"], window["down_token"]]
            self.poly.subscribe(tokens)
            self._low_bal_logged = False  # Reset balance-too-low log flag on new window
            if old_epoch is not None:
                log("  🔄 New window")

    def tick(self):
        """Main tick: check entry or manage position."""
        if self.position:
            self._manage_position()
            return

        # No position — look for entry
        if not self.current_window:
            return
        now_utc = datetime.now(timezone.utc)
        if now_utc >= self.current_window["end"]:
            return

        # No-match backoff: skip entry attempts for a while after market goes dead
        if time.time() < self._no_match_until:
            return

        # Post-stop-loss cooldown: don't re-enter the same crash
        if time.time() < self._stop_loss_until:
            return

        for side in ["UP", "DOWN"]:
            token = self.current_window[f"{side.lower()}_token"]
            mid = self.poly.mid_price(token)
            if mid is None:
                continue
            if mid >= ENTRY_MID:
                if mid > MAX_ENTRY_MID:
                    continue  # Risk/reward too bad above cap
                self._enter(token, side)
                return

    # ─── ENTRY ────────────────────────────────────────────────────────

    def _enter(self, token_id, side):
        """Buy the BTC side via FOK, then place GTC limit sell at 0.99."""
        # Determine bet size
        real_balance = get_usdc_balance()
        if real_balance is None:
            return

        # Reality check: if real USDC is too low but tracked is inflated
        # (e.g. funds locked in unclaimed positions), reset tracked balance
        if real_balance < MIN_BET and self.tracked_balance > INITIAL_BALANCE:
            log(f"  ⚠ Reality check: real=${real_balance:.2f} < ${MIN_BET} "
                f"but tracked=${self.tracked_balance:.2f} — resetting to ${INITIAL_BALANCE}")
            self.tracked_balance = INITIAL_BALANCE

        effective = min(self.tracked_balance, real_balance)
        bet = int(effective * BET_PCT)
        bet = min(bet, MAX_BET)
        if bet < MIN_BET:
            # Set backoff so we don't spam this message every 250ms
            if not hasattr(self, '_low_bal_logged') or not self._low_bal_logged:
                log(f"  ⚠ Balance too low (tracked=${self.tracked_balance:.2f}, "
                    f"real=${real_balance:.2f}) — pausing until next window")
                self._low_bal_logged = True
            return

        log(f"  ⚡ BTC {side} mid ≥ {ENTRY_MID} — BUYING")
        log(f"  💰 BTC {side}: ${bet} FOK market buy "
            f"(tracked=${self.tracked_balance:.2f})")

        if DRY_RUN:
            mid = self.poly.mid_price(token_id) or ENTRY_MID
            est_shares = bet / mid if mid > 0 else 0
            log(f"  🧪 DRY RUN — ~{est_shares:.1f}sh @ ~{mid:.3f}")
            self.position = {
                "token_id": token_id,
                "side": side,
                "entry_price": mid,
                "shares": est_shares,
                "cost": bet,
                "entry_time": time.time(),
                "limit_order_id": None,
                "usdc_snapshot": real_balance - bet,
                "last_poll": 0,
                "dry_run": True,
            }
            log(f"  📋 GTC limit sell at {LIMIT_SELL_PRICE} (simulated)")
            return

        # ── Live FOK buy ──
        try:
            market_order = MarketOrderArgs(
                token_id=token_id,
                amount=bet,
                side=BUY,
            )
            signed = client.create_market_order(market_order)
            resp = client.post_order(signed, OrderType.FOK)

            success = resp.get("success", False) if isinstance(resp, dict) else False
            taking = float(resp.get("takingAmount", 0)) if isinstance(resp, dict) else 0
            making = float(resp.get("makingAmount", 0)) if isinstance(resp, dict) else 0

            if not success and (resp.get("status") == "error" if isinstance(resp, dict) else True):
                log(f"  ⚠ Buy rejected: {resp}")
                return

            if taking <= 0:
                log(f"  ⚠ Buy got 0 shares: {resp}")
                return

            actual_shares = taking
            actual_cost = making
            fill_price = round(actual_cost / actual_shares, 4) if actual_shares > 0 else 0

            log(f"  ✅ FOK filled! {actual_shares:.1f}sh @ ~{fill_price:.3f} "
                f"(spent ${actual_cost:.2f})")

        except Exception as e:
            err_str = str(e)
            if 'no match' in err_str.lower():
                self._no_match_until = time.time() + NO_MATCH_BACKOFF
                log(f"  ⚠ Buy error: no match — backing off {NO_MATCH_BACKOFF}s")
            elif 'not enough balance' in err_str.lower():
                self._no_match_until = time.time() + NO_MATCH_BACKOFF
                log(f"  ⚠ Buy error: not enough balance — backing off {NO_MATCH_BACKOFF}s")
            elif 'fully filled' in err_str.lower() or 'fok' in err_str.lower():
                self._no_match_until = time.time() + NO_MATCH_BACKOFF
                log(f"  ⚠ Buy error: FOK fill failed — backing off {NO_MATCH_BACKOFF}s")
            else:
                log(f"  ⚠ Buy error: {e}")
            return

        # ── Wait for settlement ──
        pre_buy_bal = get_share_balance(token_id) or 0
        log(f"  ⏳ Waiting for settlement...")
        settled, on_chain = wait_for_settlement(
            token_id, actual_shares, pre_buy_balance=pre_buy_bal,
            timeout=SETTLEMENT_TIMEOUT,
        )
        if settled:
            new_shares = on_chain - pre_buy_bal
            log(f"  ✅ Settled: {new_shares:.1f}sh")
            if new_shares > actual_shares * 1.5:
                log(f"  ⚠ On-chain inflated — using FOK amount")
            else:
                actual_shares = new_shares
        else:
            log(f"  ⚠ Settlement timeout — proceeding anyway")

        # ── Check if price already crashed during settlement ──
        mid_now = self.poly.mid_price(token_id)
        if mid_now is not None and mid_now <= STOP_LOSS_MID:
            log(f"  🛑 Price crashed to {mid_now:.3f} during settlement — immediate stop loss")
            self.position = {
                "token_id": token_id,
                "side": side,
                "entry_price": fill_price,
                "shares": actual_shares,
                "cost": actual_cost,
                "entry_time": time.time(),
                "limit_order_id": None,
                "usdc_snapshot": 0,
                "last_poll": 0,
                "dry_run": False,
            }
            self._execute_stop_loss()
            return

        # ── Place GTC limit sell at 0.99 ──
        limit_order_id = None
        try:
            order_args = OrderArgs(
                price=LIMIT_SELL_PRICE,
                size=actual_shares,
                side=SELL,
                token_id=token_id,
            )
            signed_order = client.create_order(order_args)
            limit_resp = client.post_order(signed_order, OrderType.GTC)
            limit_order_id = limit_resp.get("orderID", "") if isinstance(limit_resp, dict) else ""
            if limit_order_id:
                log(f"  📋 GTC limit sell placed at {LIMIT_SELL_PRICE} "
                    f"(order={limit_order_id[:12]}...)")
            else:
                log(f"  ⚠ GTC limit response: {limit_resp}")
        except Exception as e:
            log(f"  ⚠ GTC limit error: {e} — will rely on stop-loss only")

        # ── Record USDC snapshot (for detecting limit fill proceeds) ──
        usdc_snap = get_usdc_balance() or 0

        self.position = {
            "token_id": token_id,
            "side": side,
            "entry_price": fill_price,
            "shares": actual_shares,
            "cost": actual_cost,
            "entry_time": time.time(),
            "limit_order_id": limit_order_id,
            "usdc_snapshot": usdc_snap,
            "last_poll": 0,
            "dry_run": False,
        }

    # ─── POSITION MANAGEMENT ─────────────────────────────────────────

    def _manage_position(self):
        """Monitor for stop loss, GTC fill, or window resolution."""
        pos = self.position
        if not pos:
            return

        token_id = pos["token_id"]
        side = pos["side"]
        now = time.time()

        # ── DRY RUN simulation ──
        if pos.get("dry_run"):
            mid = self.poly.mid_price(token_id)
            if mid is None:
                return
            if mid <= STOP_LOSS_MID:
                log(f"  🛑 STOP LOSS: BTC {side} mid={mid:.3f} ≤ {STOP_LOSS_MID}")
                fee = compute_taker_fee(pos["shares"], mid)
                revenue = pos["shares"] * mid - fee
                pnl = revenue - pos["cost"]
                log(f"  🧪 DRY RUN — Sold {pos['shares']:.1f}sh @ ~{mid:.3f} → PnL ${pnl:+.2f}")
                self._handle_exit(revenue, pnl, mid, "stop_loss")
                return
            if mid >= LIMIT_SELL_PRICE:
                log(f"  🎉 LIMIT FILL: BTC {side} mid={mid:.3f} ≥ {LIMIT_SELL_PRICE}")
                fee = compute_taker_fee(pos["shares"], LIMIT_SELL_PRICE)
                revenue = pos["shares"] * LIMIT_SELL_PRICE - fee
                pnl = revenue - pos["cost"]
                log(f"  🧪 DRY RUN — Sold {pos['shares']:.1f}sh @ ~{LIMIT_SELL_PRICE} → PnL ${pnl:+.2f}")
                self._handle_exit(revenue, pnl, LIMIT_SELL_PRICE, "limit_fill")
                return
            # Check window resolution in dry run
            if self.current_window and datetime.now(timezone.utc) > self.current_window["end"] + timedelta(seconds=5):
                # Simulate resolution: if mid was > 0.50, assume win
                resolved_price = 1.0 if mid > 0.50 else 0.0
                revenue = pos["shares"] * resolved_price
                pnl = revenue - pos["cost"]
                log(f"  📊 RESOLVED: BTC {side} → {'WIN' if resolved_price > 0 else 'LOSS'}")
                log(f"  🧪 DRY RUN — PnL ${pnl:+.2f}")
                self._handle_exit(revenue, pnl, resolved_price, "resolved")
                return
            return

        # ── LIVE: Check stop loss (fastest priority) ──
        mid = self.poly.mid_price(token_id)
        if mid is not None and mid <= STOP_LOSS_MID:
            log(f"  🛑 STOP LOSS: BTC {side} mid={mid:.3f} ≤ {STOP_LOSS_MID}")
            self._execute_stop_loss()
            return

        # ── Check if GTC limit sell filled (poll every N seconds) ──
        if now - pos.get("last_poll", 0) >= ORDER_POLL_SECS:
            pos["last_poll"] = now
            share_bal = get_share_balance(token_id)
            if share_bal is not None and share_bal < 1.0:
                # Shares gone → GTC filled or market resolved
                log(f"  🎉 LIMIT FILLED: BTC {side} shares={share_bal:.1f} (sold)")
                # Compute proceeds analytically (USDC snapshot is unreliable due to settlement lag)
                proceeds = pos["shares"] * LIMIT_SELL_PRICE
                fee = compute_taker_fee(pos["shares"], LIMIT_SELL_PRICE)
                proceeds -= fee
                pnl = proceeds - pos["cost"]
                sell_price = LIMIT_SELL_PRICE
                log(f"  💰 Proceeds: ${proceeds:.2f} → PnL ${pnl:+.2f}")
                self._handle_exit(proceeds, pnl, sell_price, "limit_fill")
                return

        # ── Check window resolution ──
        if self.current_window:
            secs_past_end = (datetime.now(timezone.utc) - self.current_window["end"]).total_seconds()
            if secs_past_end > RESOLUTION_GRACE:
                log(f"  📊 Window ended {secs_past_end:.0f}s ago — checking resolution")
                share_bal = get_share_balance(token_id)
                if share_bal is not None and share_bal < 1.0:
                    # Shares redeemed
                    time.sleep(3)
                    usdc_now = get_usdc_balance() or 0
                    proceeds = usdc_now - pos.get("usdc_snapshot", 0)
                    if proceeds > pos["cost"] * 0.5:
                        # Won (got back more than half)
                        pnl = proceeds - pos["cost"]
                        log(f"  🎉 RESOLVED WIN: proceeds=${proceeds:.2f}, PnL ${pnl:+.2f}")
                    else:
                        pnl = (proceeds if proceeds > 0 else 0) - pos["cost"]
                        log(f"  ❌ RESOLVED LOSS: proceeds=${proceeds:.2f}, PnL ${pnl:+.2f}")
                    self._handle_exit(max(proceeds, 0), pnl, 0, "resolved")
                else:
                    # Shares still there? Try canceling + selling
                    log(f"  ⚠ Window ended but still have {share_bal:.1f}sh — forcing sell")
                    self._execute_stop_loss()

    # ─── STOP LOSS EXECUTION ─────────────────────────────────────────

    def _execute_stop_loss(self):
        """Cancel GTC limit order, then sell all shares via GTC limit at 0.01.
        Uses GTC at floor price instead of FOK so partial fills work even in
        thin/crashing order books where FOK can't match."""
        pos = self.position
        if not pos:
            return

        token_id = pos["token_id"]
        side = pos["side"]

        # Cancel take-profit GTC limit order first
        if pos.get("limit_order_id"):
            try:
                client.cancel(pos["limit_order_id"])
                log(f"  ❌ Canceled GTC limit order")
            except Exception as e:
                log(f"  ⚠ Cancel error (may be already filled): {e}")

        # Check actual share balance
        share_bal = get_share_balance(token_id)
        if share_bal is not None and share_bal < 1.0:
            # Shares already gone (GTC filled while we were canceling)
            log(f"  ℹ Shares already gone — GTC likely filled")
            time.sleep(3)
            usdc_now = get_usdc_balance() or 0
            proceeds = usdc_now - pos.get("usdc_snapshot", 0)
            if proceeds <= 0:
                proceeds = pos["shares"] * LIMIT_SELL_PRICE
            pnl = proceeds - pos["cost"]
            self._handle_exit(proceeds, pnl, LIMIT_SELL_PRICE, "limit_fill")
            return

        sell_amount = share_bal if (share_bal and share_bal > 0) else pos["shares"]

        # Place aggressive GTC sell at floor price (0.01) — sweeps all bids
        # This works unlike FOK because it partially fills and rests the remainder
        stop_sell_order_id = None
        for attempt in range(SELL_MAX_RETRIES):
            try:
                log(f"  📤 GTC sell {sell_amount:.1f}sh BTC {side} @ 0.01 (attempt {attempt+1})")

                order_args = OrderArgs(
                    price=0.01,
                    size=sell_amount,
                    side=SELL,
                    token_id=token_id,
                )
                signed_order = client.create_order(order_args)
                resp = client.post_order(signed_order, OrderType.GTC)

                order_id = resp.get("orderID", "") if isinstance(resp, dict) else ""
                success = resp.get("success", False) if isinstance(resp, dict) else False
                taking = float(resp.get("takingAmount", 0)) if isinstance(resp, dict) else 0
                making = float(resp.get("makingAmount", 0)) if isinstance(resp, dict) else 0

                if order_id:
                    stop_sell_order_id = order_id
                    log(f"  📋 Stop-sell order placed (order={order_id[:12]}...)")
                    # Wait for fills — poll share balance
                    break

                if taking > 0:
                    # Immediately filled entirely
                    actual_revenue = taking
                    actual_shares_sold = making if making > 0 else sell_amount
                    actual_price = round(actual_revenue / actual_shares_sold, 4) if actual_shares_sold > 0 else 0
                    pnl = actual_revenue - pos["cost"]
                    log(f"  💰 Sold {actual_shares_sold:.1f}sh @ ~{actual_price:.3f} "
                        f"→ PnL ${pnl:+.2f} (stop_loss)")
                    self._handle_exit(actual_revenue, pnl, actual_price, "stop_loss")
                    return

                log(f"  ⚠ Sell order rejected (attempt {attempt+1}): {resp}")
                time.sleep(SELL_RETRY_SLEEP)

            except Exception as e:
                err_str = str(e)
                if 'price' in err_str.lower() and 'max' in err_str.lower():
                    log(f"  ✅ Market resolved in our favor")
                    pnl = pos["shares"] * 1.0 - pos["cost"]
                    self._handle_exit(pos["shares"], pnl, 1.0, "stop_loss_resolved")
                    return
                if 'no match' in err_str.lower() or 'orderbook' in err_str.lower():
                    log(f"  ⚠ Market closed — shares may auto-redeem")
                    self._handle_exit(0, -pos["cost"], 0, "stop_loss_expired")
                    return
                log(f"  ⚠ Sell error (attempt {attempt+1}): {e}")
                time.sleep(SELL_RETRY_SLEEP)

        # If GTC order was placed, wait for it to fill (poll share balance)
        if stop_sell_order_id:
            log(f"  ⏳ Waiting for stop-sell to fill...")
            deadline = time.time() + 30  # Wait up to 30s for fills
            while time.time() < deadline:
                remaining = get_share_balance(token_id)
                if remaining is not None and remaining < 1.0:
                    # All shares sold
                    time.sleep(2)  # Let USDC settle
                    usdc_now = get_usdc_balance() or 0
                    proceeds = usdc_now - pos.get("usdc_snapshot", 0)
                    if proceeds <= 0:
                        # Fallback estimate from current bid
                        bid, _, _ = self.poly.get_price(token_id)
                        proceeds = sell_amount * (bid or 0.01)
                    pnl = proceeds - pos["cost"]
                    log(f"  💰 Stop-sell filled — proceeds=${proceeds:.2f}, PnL ${pnl:+.2f}")
                    self._handle_exit(proceeds, pnl, 0, "stop_loss")
                    return
                time.sleep(3)
            # Timed out — cancel and accept whatever we got
            try:
                client.cancel(stop_sell_order_id)
                log(f"  ⚠ Canceled unfilled stop-sell order")
            except Exception:
                pass

        # All retries failed — shares may still exist and auto-resolve
        log(f"  ❌ Stop loss sell failed after {SELL_MAX_RETRIES} attempts")
        # Check real balance: if enough USDC remains, reset tracked balance
        real_bal = get_usdc_balance()
        if real_bal is not None and real_bal >= MIN_BET:
            log(f"  🔄 Real USDC=${real_bal:.2f} ≥ ${MIN_BET} — resetting tracked balance")
            self._handle_exit(INITIAL_BALANCE, INITIAL_BALANCE - pos["cost"], 0, "stop_loss_failed_reset")
        else:
            self._handle_exit(0, -pos["cost"], 0, "stop_loss_failed")

    # ─── EXIT HANDLING ────────────────────────────────────────────────

    def _handle_exit(self, proceeds, pnl, sell_price, reason):
        """Update tracked balance, check book depth, record trade."""
        pos = self.position
        self.position = None

        # Set cooldown after stop-loss exits to avoid re-entering the same crash
        if reason and 'stop_loss' in reason:
            self._stop_loss_until = time.time() + STOP_LOSS_COOLDOWN
            log(f"  ⏸ Stop-loss cooldown: {STOP_LOSS_COOLDOWN}s before next entry")

        if proceeds > 0:
            self.tracked_balance = proceeds
        else:
            # Total loss — reset to 0 (will need external deposit)
            self.tracked_balance = max(0, self.tracked_balance + pnl)

        # Check order-book depth: if balance > what book can fill, take profit
        depth_token = None
        if self.current_window:
            # Check the side we'd most likely buy next (use UP as proxy)
            depth_token = self.current_window.get("up_token")

        if depth_token:
            ask_depth = get_ask_depth_usdc(depth_token)
            if ask_depth is not None and ask_depth > 0:
                log(f"  📊 Order book ask depth: ${ask_depth:.2f}")
                if self.tracked_balance > ask_depth:
                    old = self.tracked_balance
                    self.tracked_balance = INITIAL_BALANCE
                    profit_taken = old - INITIAL_BALANCE
                    log(f"  📊 Balance ${old:.2f} > depth ${ask_depth:.2f} "
                        f"→ TAKE PROFIT ${profit_taken:.2f}, reset to ${INITIAL_BALANCE:.2f}")

        log(f"  💼 Tracked balance: ${self.tracked_balance:.2f}")

        # Record trade
        if pos:
            self._record_trade(pos, sell_price, pnl, reason)

    def _record_trade(self, pos, sell_price, pnl, reason):
        """Record completed trade to JSON file."""
        self.trade_count += 1
        self.total_pnl += pnl
        if pnl > 0:
            self.win_count += 1

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
            "tracked_balance": round(self.tracked_balance, 2),
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
        log(f"  📊 Trade #{self.trade_count}: {result} ${pnl:+.2f} | "
            f"session {self.win_count}/{self.trade_count} | "
            f"cumulative ${self.total_pnl:+.2f}")

    def get_status(self):
        """One-line status for dashboard."""
        if self.position:
            pos = self.position
            hold = time.time() - pos["entry_time"]
            mid = self.poly.mid_price(pos["token_id"]) or 0
            change = mid - pos["entry_price"] if mid else 0
            return (f"HOLDING BTC {pos['side']} | entry={pos['entry_price']:.3f} "
                    f"now={mid:.3f} Δ{change:+.3f} | hold={hold:.0f}s | "
                    f"stop≤{STOP_LOSS_MID} limit={LIMIT_SELL_PRICE}")
        return "SCANNING for BTC side ≥ 0.80..."


# =========================================================================
# MAIN
# =========================================================================
def main():
    mode = "DRY" if DRY_RUN else "LIVE"
    log(f"═══ BTC80 Bot ═══ [{mode}]")
    log(f"Strategy: Buy BTC side at {ENTRY_MID}-{MAX_ENTRY_MID} → limit sell {LIMIT_SELL_PRICE} / stop {STOP_LOSS_MID}")
    log(f"Balance: ${INITIAL_BALANCE:.2f} start | 99% per trade | reset when > book depth")
    log(f"Sell retries: {SELL_MAX_RETRIES} @ {SELL_RETRY_SLEEP}s | no-match backoff: {NO_MATCH_BACKOFF}s")
    log("")

    poly = PolymarketFeed()
    poly.start()

    bot = BTC80Bot(poly)

    log("Waiting for WS...")
    for _ in range(30):
        if poly.connected:
            break
        time.sleep(1)

    if not poly.connected:
        log("⚠ WS not connected after 30s")

    balance = get_usdc_balance()
    if balance:
        log(f"Real USDC balance: ${balance:.2f}")
    log(f"Tracked balance: ${bot.tracked_balance:.2f}")
    log("")

    last_discovery = 0
    discovery_interval = 15

    while True:
        try:
            now = time.time()

            # Discover/update market window
            if now - last_discovery > discovery_interval:
                window = discover_btc_market()
                if window:
                    old_epoch = bot.current_window["epoch"] if bot.current_window else None
                    if window["epoch"] != old_epoch:
                        bot.update_market(window)
                last_discovery = now

            # Main tick
            bot.tick()

            # Dashboard
            now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
            ws_status = "🟢" if poly.connected else "🔴"
            status = bot.get_status()

            up_mid = down_mid = None
            if bot.current_window:
                up_mid = poly.mid_price(bot.current_window["up_token"])
                down_mid = poly.mid_price(bot.current_window["down_token"])

            price_str = ""
            if up_mid:
                price_str += f"UP={up_mid:.3f} "
            if down_mid:
                price_str += f"DOWN={down_mid:.3f}"

            print(f"\033[2J\033[H", end="")
            print(f"═══ BTC80 Bot [{mode}] ═══  {now_str} UTC  │  WS {ws_status}")
            print(f"{'─' * 70}")
            print(f"  BTC: {price_str}")
            print(f"  {status}")
            print(f"  Tracked balance: ${bot.tracked_balance:.2f}")
            print(f"{'─' * 70}")
            print(f"  Trades: {bot.trade_count} | Wins: {bot.win_count} | "
                  f"PnL: ${bot.total_pnl:+.2f}")
            print(f"  Log: {LOG_FILE} | Trades: {TRADE_LOG}")
            print(f"  Press Ctrl+C to stop")

            time.sleep(TICK_INTERVAL)

        except KeyboardInterrupt:
            log(f"\nStopping...")
            # Cancel any open GTC orders
            if bot.position and bot.position.get("limit_order_id"):
                try:
                    client.cancel(bot.position["limit_order_id"])
                    log("  Canceled open GTC order")
                except Exception:
                    pass
            # Try to sell if holding
            if bot.position and not bot.position.get("dry_run"):
                log("  Closing open position...")
                bot._execute_stop_loss()
            bal = get_usdc_balance()
            if bal:
                log(f"Final USDC balance: ${bal:.2f}")
            log(f"Tracked balance: ${bot.tracked_balance:.2f}")
            log(f"Trades: {bot.trade_count} | Wins: {bot.win_count} | "
                f"PnL: ${bot.total_pnl:+.2f}")
            break
        except Exception as e:
            log(f"⚠ Main loop error: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5)


if __name__ == "__main__":
    main()
