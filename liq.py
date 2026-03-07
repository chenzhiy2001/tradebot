#!/usr/bin/env python3
"""
Liquidation Sniper — Trade Polymarket 5-min crypto markets using
Binance volume spike detection (liquidation cascades).

Strategy:
  1. Monitor Binance aggTrade stream for BTC, ETH, SOL, XRP
  2. Track rolling 5-second volume windows per symbol
  3. Detect volume spikes: when recent volume exceeds N× the baseline
  4. Determine spike direction from taker buy/sell ratio
  5. When a spike occurs during an active 5-min window:
     → Buy the token matching the spike direction on Polymarket
  6. Exit at $0.99 GTC sell (fills at resolution if correct)

Why this works (theory):
  - Large liquidation cascades cause forced market orders on Binance
  - These create detectable volume spikes with directional momentum
  - The cascade continues for 5-30 seconds → momentum alpha
  - Polymarket tokens reprice slowly (thin orderbook) → exploitable lag
  - Entry at taker (crossing spread) fills instantly

Risk:
  - Spikes may be noise (random volume bursts)
  - Polymarket may already reflect the move by the time we execute
  - False signals → lose entire bet amount
  - Managed via: high spike threshold, edge confirmation, small bets

Usage:
  python liq.py              # live trading
  python liq.py --dry-run    # simulate (no real orders)
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
BINANCE_WS_URL = "wss://stream.binance.com:9443/stream"
BINANCE_FUTURES_WS = "wss://fstream.binance.com/stream"
CHAIN_ID = 137
FUNDER_ADDRESS = funder_address

CRYPTOS = {
    "btc": "btcusdt",
    "eth": "ethusdt",
    "sol": "solusdt",
    "xrp": "xrpusdt",
}

# ── Volume spike detection ──
SPIKE_WINDOW = 5              # Seconds to aggregate volume
BASELINE_WINDOW = 120         # Seconds for baseline volume (rolling 2 min)
SPIKE_THRESHOLD = 5.0         # Volume must be N× baseline to trigger
MIN_SPIKE_USD = 50_000        # Minimum USD volume in spike window
MIN_DIRECTION_RATIO = 0.65    # At least 65% of spike volume must be same direction
SPIKE_COOLDOWN = 30           # Don't re-trigger same crypto for N seconds

# ── Trading params ──
BET_AMOUNT = 15               # USDC per trade
MAX_BET = 30                  # Cap
MIN_BET = 5                   # Floor
MAX_POSITIONS = 2             # Max concurrent positions (reduce correlated risk)
MIN_BALANCE = 10              # Stop trading below this
ENTRY_PRICE_MAX = 0.85        # Don't buy tokens above 85¢ (too little upside)
ENTRY_PRICE_MIN = 0.20        # Don't buy tokens below 20¢ (too speculative)
MIN_TIME_REMAINING = 60       # Don't enter with < 60s left
MAX_ELAPSED_PCT = 0.75        # Don't enter after 75% of window elapsed
FILL_WAIT = 4                 # Seconds to wait for limit buy fill
MIN_ORDER_SIZE = 5            # Polymarket minimum

# ── Exit params (sell during the spike, don't hold to resolution) ──
PROFIT_TARGET = 0.06          # Take profit when token moves +6¢ above entry
STOP_LOSS = 0.04              # Cut loss when token drops -4¢ below entry
MAX_HOLD_SECS = 45            # Force exit after 45 seconds regardless
EXIT_CHECK_INTERVAL = 2       # Check midpoint every 2 seconds
EXIT_SPREAD = 0.02            # Sell 2¢ below mid to cross spread and fill fast
FALLBACK_PRICE = 0.99         # Last-resort GTC sell if active exit fails

# ── Fee model (5m crypto markets) ──
CRYPTO_FEE_RATE = 0.25
CRYPTO_FEE_EXPONENT = 2

# ── Data files ──
LOG_FILE = "liq_log.txt"
TRADE_FILE = "liq_trades.json"

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
    try:
        client.update_balance_allowance(
            params=BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id, signature_type=1,
            )
        )
        ba = client.get_balance_allowance(
            params=BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id, signature_type=1,
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
# BINANCE VOLUME SPIKE DETECTOR
# =========================================================================
class BinanceSpikeFeed:
    """Monitor Binance aggTrade + forceOrder streams for volume spikes."""

    def __init__(self):
        self._lock = threading.Lock()
        # Per-symbol rolling trade volume buckets
        # Each entry: (timestamp, usd_volume, is_buyer_maker)
        self._trades = defaultdict(deque)      # symbol → deque of (ts, usd, is_buyer_maker)
        self._liquidations = defaultdict(deque) # symbol → deque of (ts, usd, side)
        self._prices = {}                       # symbol → latest price
        self._connected = False
        self._callbacks = []  # list of fn(crypto, direction, spike_usd, ratio)

    def on_spike(self, callback):
        """Register callback for volume spike events."""
        self._callbacks.append(callback)

    def start(self):
        t = threading.Thread(target=self._run, daemon=True)
        t.start()

    def _run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._ws_loop())

    async def _ws_loop(self):
        import websockets

        # Combined stream: aggTrade for all symbols + forceOrder for all symbols
        agg_streams = [f"{sym}@aggTrade" for sym in CRYPTOS.values()]
        liq_streams = [f"{sym}@forceOrder" for sym in CRYPTOS.values()]
        all_streams = agg_streams + liq_streams
        streams = "/".join(all_streams)
        url = f"{BINANCE_FUTURES_WS}?streams={streams}"

        while True:
            try:
                async with websockets.connect(url, close_timeout=5, open_timeout=10) as ws:
                    self._connected = True
                    log("  🔌 Binance spike feed connected")
                    while True:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            await ws.ping()
                            continue
                        try:
                            data = json.loads(msg)
                        except (json.JSONDecodeError, TypeError):
                            continue
                        stream = data.get("stream", "")
                        payload = data.get("data", {})

                        if "@aggTrade" in stream:
                            self._handle_agg_trade(payload)
                        elif "@forceOrder" in stream:
                            self._handle_liquidation(payload)

            except Exception as e:
                self._connected = False
                log(f"  ⚠ Binance spike WS error: {e}, reconnecting in 2s...")
                await asyncio.sleep(2)

    def _handle_agg_trade(self, data):
        """Process aggTrade event — accumulate volume."""
        symbol = data.get("s", "").lower()
        price = float(data.get("p", 0))
        qty = float(data.get("q", 0))
        is_buyer_maker = data.get("m", False)  # True = seller is taker (price pressure down)
        usd = price * qty
        now = time.time()

        with self._lock:
            self._prices[symbol] = price
            buf = self._trades[symbol]
            buf.append((now, usd, is_buyer_maker))
            # Trim old entries beyond baseline window
            cutoff = now - BASELINE_WINDOW - 10
            while buf and buf[0][0] < cutoff:
                buf.popleft()

        # Check for spike on every trade
        self._check_spike(symbol, now)

    def _handle_liquidation(self, data):
        """Process forceOrder (liquidation) event."""
        o = data.get("o", {})
        symbol = o.get("s", "").lower()
        side = o.get("S", "")  # SELL = long liquidated (bearish), BUY = short liquidated (bullish)
        qty = float(o.get("q", 0))
        price = float(o.get("p", 0))
        usd = qty * price
        now = time.time()

        with self._lock:
            self._liquidations[symbol].append((now, usd, side))
            # Trim
            cutoff = now - BASELINE_WINDOW
            while self._liquidations[symbol] and self._liquidations[symbol][0][0] < cutoff:
                self._liquidations[symbol].popleft()

        if usd >= 10_000:
            crypto = self._symbol_to_crypto(symbol)
            direction = "DOWN" if side == "SELL" else "UP"
            log(f"  ⚡ Liquidation: {crypto} {side} ${usd:,.0f} → expect {direction}")

    def _check_spike(self, symbol, now):
        """Check if recent volume is a spike vs baseline."""
        with self._lock:
            buf = list(self._trades[symbol])

        if len(buf) < 10:
            return

        # Recent volume (last SPIKE_WINDOW seconds)
        spike_cutoff = now - SPIKE_WINDOW
        recent = [(ts, usd, bm) for ts, usd, bm in buf if ts >= spike_cutoff]
        recent_total = sum(usd for _, usd, _ in recent)

        if recent_total < MIN_SPIKE_USD:
            return

        # Baseline volume per SPIKE_WINDOW seconds (from BASELINE_WINDOW)
        baseline_cutoff = now - BASELINE_WINDOW
        all_in_baseline = [(ts, usd, bm) for ts, usd, bm in buf if ts >= baseline_cutoff]
        baseline_total = sum(usd for _, usd, _ in all_in_baseline)
        baseline_duration = now - baseline_cutoff
        if baseline_duration <= SPIKE_WINDOW:
            return
        baseline_rate = baseline_total / baseline_duration * SPIKE_WINDOW

        if baseline_rate <= 0:
            return

        spike_ratio = recent_total / baseline_rate
        if spike_ratio < SPIKE_THRESHOLD:
            return

        # Determine direction: is_buyer_maker=True means seller taker (bearish)
        sell_vol = sum(usd for _, usd, bm in recent if bm)       # seller taker = bearish
        buy_vol = sum(usd for _, usd, bm in recent if not bm)    # buyer taker = bullish

        if recent_total <= 0:
            return

        if sell_vol > buy_vol:
            direction = "DOWN"
            dir_ratio = sell_vol / recent_total
        else:
            direction = "UP"
            dir_ratio = buy_vol / recent_total

        if dir_ratio < MIN_DIRECTION_RATIO:
            return

        crypto = self._symbol_to_crypto(symbol)
        if not crypto:
            return

        # Fire callbacks
        for cb in self._callbacks:
            try:
                cb(crypto, direction, recent_total, spike_ratio, dir_ratio)
            except Exception as e:
                log(f"  ⚠ Spike callback error: {e}")

    def _symbol_to_crypto(self, symbol):
        for crypto, sym in CRYPTOS.items():
            if sym == symbol:
                return crypto
        return None

    def get_price(self, crypto):
        """Latest Binance price for a crypto."""
        symbol = CRYPTOS.get(crypto.lower())
        if not symbol:
            return None
        with self._lock:
            return self._prices.get(symbol)

    def get_volume_stats(self, crypto):
        """Get current volume rate for a crypto. Returns (recent_5s, baseline_rate_5s)."""
        symbol = CRYPTOS.get(crypto.lower())
        if not symbol:
            return 0, 0
        now = time.time()
        with self._lock:
            buf = list(self._trades.get(symbol, []))
        if not buf:
            return 0, 0
        recent = sum(usd for ts, usd, _ in buf if ts >= now - SPIKE_WINDOW)
        baseline = sum(usd for ts, usd, _ in buf if ts >= now - BASELINE_WINDOW)
        duration = min(now - buf[0][0], BASELINE_WINDOW) if buf else BASELINE_WINDOW
        baseline_rate = baseline / duration * SPIKE_WINDOW if duration > 0 else 0
        return recent, baseline_rate


# =========================================================================
# MARKET DISCOVERY
# =========================================================================
def discover_markets():
    """Find current live 5-min crypto markets on Polymarket."""
    markets = []
    now = datetime.now(timezone.utc)

    for interval in [5]:
        aligned = (now.minute // interval) * interval
        window_start = now.replace(minute=aligned, second=0, microsecond=0)
        window_end = window_start + timedelta(minutes=interval)
        epoch = int(window_start.timestamp())

        # Also check next window (for pre-positioning)
        for offset in [0, 1]:
            target_start = window_start + timedelta(minutes=interval * offset)
            target_end = target_start + timedelta(minutes=interval)
            target_epoch = int(target_start.timestamp())

            if now >= target_end:
                continue

            for crypto in CRYPTOS:
                slug = f"{crypto}-updown-{interval}m-{target_epoch}"
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

                        up_idx = next((i for i, o in enumerate(outcomes)
                                       if "up" in o.lower()), 0)
                        down_idx = 1 - up_idx

                        outcome_prices = json.loads(m.get("outcomePrices", "[]"))
                        try:
                            up_mid = float(outcome_prices[up_idx])
                            down_mid = float(outcome_prices[down_idx])
                        except (ValueError, TypeError, IndexError):
                            up_mid = down_mid = 0.5

                        markets.append({
                            "condition_id": m.get("conditionId", ""),
                            "question": m.get("question", ""),
                            "crypto": crypto,
                            "interval": interval,
                            "slug": slug,
                            "epoch": target_epoch,
                            "window_start": target_start,
                            "window_end": target_end,
                            "up_token": tokens[up_idx],
                            "down_token": tokens[down_idx],
                            "up_mid": up_mid,
                            "down_mid": down_mid,
                        })
                except Exception as e:
                    log(f"  ⚠ Market fetch {slug}: {e}")

    return markets


# =========================================================================
# LIQUIDATION SNIPER BOT
# =========================================================================
class LiqSniper:
    """Core bot: reacts to volume spikes by buying in spike direction."""

    def __init__(self):
        self.feed = BinanceSpikeFeed()
        self.markets = {}           # crypto → market dict (current live markets)
        self.positions = []         # active positions
        self.completed = []         # completed trades
        self._spike_cooldowns = {}  # crypto → last spike time
        self._lock = threading.Lock()
        self._trade_count = 0
        self._win_count = 0
        self._pnl = 0.0
        self._start_balance = None
        self._running = True

    def run(self):
        log("")
        log("=" * 60)
        log("  LIQUIDATION SNIPER" + (" (DRY RUN)" if DRY_RUN else " (LIVE)"))
        log(f"  Bet: ${BET_AMOUNT}, Max positions: {MAX_POSITIONS}")
        log(f"  Spike threshold: {SPIKE_THRESHOLD}×, Min spike: ${MIN_SPIKE_USD:,}")
        log(f"  Direction ratio: {MIN_DIRECTION_RATIO:.0%}, Cooldown: {SPIKE_COOLDOWN}s")
        log(f"  Exit: TP +{PROFIT_TARGET:.0%} / SL -{STOP_LOSS:.0%} / timeout {MAX_HOLD_SECS}s")
        log("=" * 60)

        self._start_balance = get_usdc_balance()
        log(f"\n  Starting USDC: ${self._start_balance:.2f}" if self._start_balance else
            "  ⚠ Could not get balance")

        # Register spike callback
        self.feed.on_spike(self._on_spike)
        self.feed.start()

        # Wait for feed connection
        log("  Waiting for Binance feed...")
        for _ in range(30):
            if self.feed._connected:
                break
            time.sleep(1)
        if not self.feed._connected:
            log("  ⚠ Binance feed not connected after 30s")
            return

        # Main loop
        last_market_poll = 0
        last_status = 0
        try:
            while self._running:
                now = time.time()

                # Poll markets every 15 seconds
                if now - last_market_poll >= 15:
                    last_market_poll = now
                    self._update_markets()

                # Monitor positions
                self._monitor_positions()

                # Status every 30 seconds
                if now - last_status >= 30:
                    last_status = now
                    self._log_status()

                time.sleep(1.0)

        except KeyboardInterrupt:
            log("\n  Shutting down...")
        finally:
            self._shutdown()

    def _update_markets(self):
        """Refresh available markets. Prefer CURRENT (active) window over next."""
        try:
            found = discover_markets()
            now = datetime.now(timezone.utc)
            with self._lock:
                self.markets = {}
                for m in found:
                    if now >= m["window_end"]:
                        continue  # expired
                    crypto = m["crypto"]
                    existing = self.markets.get(crypto)
                    if existing is None:
                        self.markets[crypto] = m
                    else:
                        # Prefer the window that's already started
                        m_active = m["window_start"] <= now
                        ex_active = existing["window_start"] <= now
                        if m_active and not ex_active:
                            self.markets[crypto] = m
                        elif m_active == ex_active and m["window_start"] < existing["window_start"]:
                            self.markets[crypto] = m
        except Exception as e:
            log(f"  ⚠ Market update error: {e}")

    def _on_spike(self, crypto, direction, spike_usd, spike_ratio, dir_ratio):
        """Called by BinanceSpikeFeed when a volume spike is detected."""
        now = time.time()

        # Check if we have a live market for this crypto
        with self._lock:
            market = self.markets.get(crypto)

        if not market:
            return  # silent — no market, no log spam

        market_key = f"{crypto}_{market['epoch']}"

        # Check for existing position on this market
        existing_pos = None
        for pos in self.positions:
            if pos.get("market_key") == market_key:
                existing_pos = pos
                break

        # If we have a position in the SAME direction, cooldown applies
        if existing_pos and existing_pos["direction"] == direction:
            return  # already positioned correctly

        # If we have a position in the OPPOSITE direction → skip (don't flip)
        # Flipping has been net negative in every session due to race conditions
        # and the cost of selling at a loss + buying into an already-moved market
        if existing_pos is not None and existing_pos["direction"] != direction:
            return

        # Normal cooldown for fresh entries
        last_spike = self._spike_cooldowns.get(crypto, 0)
        if now - last_spike < SPIKE_COOLDOWN:
            return

        self._spike_cooldowns[crypto] = now

        log(f"\n  🚨 SPIKE: {crypto.upper()} {direction} — "
            f"${spike_usd:,.0f} vol ({spike_ratio:.1f}× baseline), "
            f"{dir_ratio:.0%} directional")

        # Check window timing
        utc_now = datetime.now(timezone.utc)
        window_start = market["window_start"]
        window_end = market["window_end"]

        if utc_now < window_start:
            log(f"     ⊘ Window not started yet")
            return

        remaining = (window_end - utc_now).total_seconds()
        if remaining < MIN_TIME_REMAINING:
            log(f"     ⊘ Too late: {remaining:.0f}s remaining < {MIN_TIME_REMAINING}s")
            return

        elapsed_pct = 1.0 - remaining / (window_end - window_start).total_seconds()
        if elapsed_pct > MAX_ELAPSED_PCT:
            log(f"     ⊘ Too late: {elapsed_pct:.0%} elapsed > {MAX_ELAPSED_PCT:.0%}")
            return

        # Check position limits
        if len(self.positions) >= MAX_POSITIONS:
            log(f"     ⊘ Max positions ({MAX_POSITIONS}) reached")
            return

        # Check balance
        balance = get_usdc_balance()
        if balance is None or balance < MIN_BALANCE:
            log(f"     ⊘ Low balance: ${balance}")
            return

        # Determine token to buy
        if direction == "UP":
            token_id = market["up_token"]
            token_price = market["up_mid"]
        else:
            token_id = market["down_token"]
            token_price = market["down_mid"]

        # Refresh token price from CLOB
        try:
            mid_data = client.get_midpoint(token_id)
            live_mid = float(mid_data.get("mid", 0))
            if live_mid > 0:
                token_price = live_mid
        except Exception:
            pass

        # Price checks
        if token_price > ENTRY_PRICE_MAX:
            log(f"     ⊘ Token too expensive: {token_price:.2f} > {ENTRY_PRICE_MAX}")
            return
        if token_price < ENTRY_PRICE_MIN:
            log(f"     ⊘ Token too cheap: {token_price:.2f} < {ENTRY_PRICE_MIN}")
            return

        # Calculate buy price (cross spread aggressively: mid + 2¢)
        buy_price = min(round(token_price + 0.02, 2), 0.95)
        bet = min(BET_AMOUNT, MAX_BET, balance * 0.25)  # Never more than 25% of balance
        est_shares = max(MIN_ORDER_SIZE, math.floor(bet / buy_price))
        fee_est = compute_taker_fee(est_shares, buy_price)

        log(f"     → BUY {direction} {crypto.upper()}: ~{est_shares}sh @ {buy_price:.2f} "
            f"(${bet:.0f}, fee ~${fee_est:.2f})")

        self._execute_buy(market, market_key, direction, token_id, buy_price,
                          est_shares, spike_usd, spike_ratio, dir_ratio)

    def _execute_buy(self, market, market_key, direction, token_id,
                     buy_price, est_shares, spike_usd, spike_ratio, dir_ratio):
        """Place GTC limit buy and check fill."""
        if DRY_RUN:
            log(f"     🧪 DRY RUN — would buy {est_shares}sh @ {buy_price}")
            self.positions.append({
                "market_key": market_key,
                "market": market,
                "direction": direction,
                "token_id": token_id,
                "buy_price": buy_price,
                "shares": est_shares,
                "cost": est_shares * buy_price,
                "spike_usd": spike_usd,
                "spike_ratio": spike_ratio,
                "dir_ratio": dir_ratio,
                "buy_time": time.time(),
                "sell_order_id": None,
                "resolved": False,
                "dry_run": True,
            })
            return

        try:
            # Record pre-buy balance to detect actual fill amount
            pre_balance = get_share_balance(token_id) or 0

            buy_order = OrderArgs(
                token_id=token_id,
                price=buy_price,
                size=est_shares,
                side=BUY,
            )
            signed = client.create_order(buy_order)
            resp = client.post_order(signed, OrderType.GTC)

            order_id = resp.get("orderID", "") if isinstance(resp, dict) else ""
            status = resp.get("status", "") if isinstance(resp, dict) else ""

            if not status or status == "error":
                log(f"     ⚠ Buy rejected: {resp}")
                return

            log(f"     📝 Order placed (id: {order_id[:10]})")

            # Wait for fill
            time.sleep(FILL_WAIT)

            total_balance = get_share_balance(token_id)
            actual = max(0, (total_balance or 0) - pre_balance)

            # Cancel remaining order
            try:
                if order_id:
                    client.cancel(order_id)
            except Exception:
                pass

            if actual is not None and actual >= MIN_ORDER_SIZE:
                fee = compute_taker_fee(actual, buy_price)
                cost = round(actual * buy_price + fee, 4)
                log(f"     ✅ Filled! {actual:.1f}sh, cost ${cost:.2f}")

                pos = {
                    "market_key": market_key,
                    "market": market,
                    "direction": direction,
                    "token_id": token_id,
                    "buy_price": buy_price,
                    "shares": actual,
                    "cost": cost,
                    "spike_usd": spike_usd,
                    "spike_ratio": spike_ratio,
                    "dir_ratio": dir_ratio,
                    "buy_time": time.time(),
                    "sell_order_id": None,
                    "resolved": False,
                    "dry_run": False,
                }
                self.positions.append(pos)
                self._trade_count += 1

                # Log trade
                self._log_trade(pos, "OPEN")

                # Post-fill sanity check: if mid already past SL, sell immediately
                try:
                    mid_data = client.get_midpoint(token_id)
                    post_mid = float(mid_data.get("mid", 0))
                    if post_mid > 0 and (post_mid - buy_price) <= -STOP_LOSS * 2:
                        sell_at = max(round(post_mid - EXIT_SPREAD, 2), 0.01)
                        log(f"     ⚠ Post-fill SL: mid={post_mid:.2f} already "
                            f"{post_mid - buy_price:+.2f}¢ from entry, emergency sell @ {sell_at}")
                        pos["_pending_sell_price"] = sell_at
                        self._sell_now(pos, "SL")
                except Exception:
                    pass
            else:
                log(f"     ⏳ Not filled after {FILL_WAIT}s")
                # Double-check: wait a beat and re-check in case of cancel/fill race
                time.sleep(1)
                late_total = get_share_balance(token_id)
                late_fill = max(0, (late_total or 0) - pre_balance) if late_total is not None else 0
                if late_fill >= MIN_ORDER_SIZE:
                    fee = compute_taker_fee(late_fill, buy_price)
                    cost = round(late_fill * buy_price + fee, 4)
                    log(f"     ⚠ Late fill detected! {late_fill:.1f}sh — adopting position")
                    pos = {
                        "market_key": market_key,
                        "market": market,
                        "direction": direction,
                        "token_id": token_id,
                        "buy_price": buy_price,
                        "shares": late_fill,
                        "cost": cost,
                        "spike_usd": spike_usd,
                        "spike_ratio": spike_ratio,
                        "dir_ratio": dir_ratio,
                        "buy_time": time.time(),
                        "sell_order_id": None,
                        "resolved": False,
                        "dry_run": False,
                    }
                    self.positions.append(pos)
                    self._trade_count += 1
                    self._log_trade(pos, "OPEN")

        except Exception as e:
            log(f"     ⚠ Buy error: {e}")

    def _sell_now(self, pos, reason, sell_price=None):
        """Execute an immediate sell at the given price (crossing spread)."""
        if pos.get("dry_run"):
            pos["resolved"] = True
            pos["pnl"] = 0
            return True

        # If a sell order already exists, DON'T place another one — tokens are locked
        if pos.get("sell_order_id"):
            # Just check if it filled
            bal = get_share_balance(pos["token_id"])
            if bal is not None and bal < 1:
                sell_price = pos.get("_pending_sell_price", pos["buy_price"])
                payout = round(pos["shares"] * sell_price, 4)
                pnl = round(payout - pos["cost"], 2)
                pos["resolved"] = True
                pos["pnl"] = pnl
                pos["won"] = pnl > 0
                if pnl > 0:
                    self._win_count += 1
                self._pnl += pnl
                sym = "+" if pnl >= 0 else ""
                log(f"  {'💰' if pnl > 0 else '❌'} {reason}: "
                    f"{pos['market']['crypto'].upper()} {pos['direction']} — "
                    f"{sym}${pnl:.2f}")
                self._log_trade(pos, reason)
                return True
            return False  # sell still pending, don't spam new orders

        # Get current mid if no price given
        if sell_price is None:
            try:
                mid_data = client.get_midpoint(pos["token_id"])
                mid = float(mid_data.get("mid", 0))
                sell_price = max(round(mid - EXIT_SPREAD, 2), 0.01)
            except Exception:
                sell_price = max(round(pos["buy_price"] - EXIT_SPREAD, 2), 0.01)

        try:
            # Ensure token allowance is set before selling
            client.update_balance_allowance(
                params=BalanceAllowanceParams(
                    asset_type=AssetType.CONDITIONAL,
                    token_id=pos["token_id"], signature_type=1,
                )
            )

            bal = get_share_balance(pos["token_id"])
            if bal is None or bal < 1:
                # No shares to sell (already resolved?)
                pos["resolved"] = True
                pos["pnl"] = round(-pos["cost"], 2)
                pos["won"] = False
                self._pnl += pos["pnl"]
                log(f"  ❌ {reason}: {pos['market']['crypto'].upper()} "
                    f"{pos['direction']} — no shares to sell")
                return True

            # Sell only what we bought, not stale shares from other sessions
            shares_to_sell = min(bal, pos["shares"])

            sell_order = OrderArgs(
                token_id=pos["token_id"],
                price=sell_price,
                size=shares_to_sell,
                side=SELL,
            )
            signed = client.create_order(sell_order)
            resp = client.post_order(signed, OrderType.GTC)
            oid = resp.get("orderID", "") if isinstance(resp, dict) else ""
            status = resp.get("status", "") if isinstance(resp, dict) else ""

            if not oid or status == "error":
                log(f"     ⚠ Sell rejected ({reason}): {resp}")
                return False

            # Wait briefly for fill
            time.sleep(2)
            remaining = get_share_balance(pos["token_id"])

            if remaining is not None and remaining < 1:
                # Fully sold
                payout = round(shares_to_sell * sell_price, 4)
                pnl = round(payout - pos["cost"], 2)
                pos["resolved"] = True
                pos["pnl"] = pnl
                pos["exit_price"] = sell_price
                pos["won"] = pnl > 0
                if pnl > 0:
                    self._win_count += 1
                self._pnl += pnl
                sym = "+" if pnl >= 0 else ""
                log(f"  {'💰' if pnl > 0 else '❌'} {reason}: "
                    f"{pos['market']['crypto'].upper()} {pos['direction']} — "
                    f"{sym}${pnl:.2f} (sold {shares_to_sell:.0f}sh @ {sell_price:.2f})")
                self._log_trade(pos, reason)
                # Cancel order just in case
                try:
                    if oid:
                        client.cancel(oid)
                except Exception:
                    pass
                return True
            else:
                # Sell order placed but not fully filled — leave it live
                pos["sell_order_id"] = oid
                pos["_pending_sell_price"] = sell_price
                log(f"     📤 {reason} sell placed @ {sell_price} "
                    f"({shares_to_sell:.0f}sh, waiting for fill)")
                return False

        except Exception as e:
            log(f"     ⚠ Sell error ({reason}): {e}")
            return False

    def _monitor_positions(self):
        """Active exit management: take profit, stop loss, time exit."""
        now_ts = time.time()
        now = datetime.now(timezone.utc)
        to_remove = []

        for pos in self.positions:
            market = pos["market"]
            window_end = market["window_end"]

            if pos["resolved"]:
                to_remove.append(pos)
                continue

            # If a sell order exists, just check if it filled — don't place more
            if not pos.get("dry_run") and pos.get("sell_order_id"):
                bal = get_share_balance(pos["token_id"])
                if bal is not None and bal < 1:
                    # Sell filled
                    sell_price = pos.get("_pending_sell_price", pos["buy_price"])
                    payout = round(pos["shares"] * sell_price, 4)
                    pnl = round(payout - pos["cost"], 2)
                    pos["resolved"] = True
                    pos["pnl"] = pnl
                    pos["won"] = pnl > 0
                    if pnl > 0:
                        self._win_count += 1
                    self._pnl += pnl
                    sym = "+" if pnl >= 0 else ""
                    log(f"  {'💰' if pnl > 0 else '❌'} SOLD: "
                        f"{market['crypto'].upper()} {pos['direction']} — "
                        f"{sym}${pnl:.2f}")
                    self._log_trade(pos, "SOLD")
                    to_remove.append(pos)
                # Either way, don't try to place new sell orders
                continue

            # Skip active monitoring for dry runs after window end
            if pos.get("dry_run") and now > window_end:
                pos["resolved"] = True
                pos["pnl"] = 0
                to_remove.append(pos)
                continue

            # ── Active exit monitoring (only when NO sell order exists) ──
            hold_secs = now_ts - pos["buy_time"]
            last_check = pos.get("_last_exit_check", 0)

            if now_ts - last_check < EXIT_CHECK_INTERVAL:
                continue
            pos["_last_exit_check"] = now_ts

            # Get current token midpoint
            try:
                mid_data = client.get_midpoint(pos["token_id"])
                current_mid = float(mid_data.get("mid", 0))
            except Exception:
                current_mid = 0

            if current_mid <= 0:
                continue

            entry = pos["buy_price"]
            move = current_mid - entry

            # Log price movement
            if not pos.get("_logged_first_check"):
                pos["_logged_first_check"] = True
                log(f"     📊 Monitoring {market['crypto'].upper()} {pos['direction']}: "
                    f"entry={entry:.2f}, current={current_mid:.2f}, "
                    f"TP@{entry + PROFIT_TARGET:.2f}, SL@{entry - STOP_LOSS:.2f}")

            # 1. TAKE PROFIT — token moved up enough
            if move >= PROFIT_TARGET:
                sell_at = max(round(current_mid - EXIT_SPREAD, 2), 0.01)
                log(f"     🎯 TAKE PROFIT: mid={current_mid:.2f} (+{move:.2f}¢), "
                    f"selling @ {sell_at}")
                pos["_pending_sell_price"] = sell_at
                if self._sell_now(pos, "TP", sell_at):
                    to_remove.append(pos)
                continue

            # 2. STOP LOSS — token dropped
            if move <= -STOP_LOSS:
                sell_at = max(round(current_mid - EXIT_SPREAD, 2), 0.01)
                log(f"     🛑 STOP LOSS: mid={current_mid:.2f} ({move:.2f}¢), "
                    f"selling @ {sell_at}")
                pos["_pending_sell_price"] = sell_at
                if self._sell_now(pos, "SL", sell_at):
                    to_remove.append(pos)
                continue

            # 3. TIME EXIT — held too long, spike momentum is gone
            if hold_secs >= MAX_HOLD_SECS:
                sell_at = max(round(current_mid - EXIT_SPREAD, 2), 0.01)
                log(f"     ⏱ TIME EXIT: {hold_secs:.0f}s held, mid={current_mid:.2f} "
                    f"({move:+.2f}¢), selling @ {sell_at}")
                pos["_pending_sell_price"] = sell_at
                if self._sell_now(pos, "TIMEOUT", sell_at):
                    to_remove.append(pos)
                continue

            # 4. WINDOW ENDING — must exit before resolution
            remaining_secs = (window_end - now).total_seconds()
            if remaining_secs < 20:
                sell_at = max(round(current_mid - EXIT_SPREAD, 2), 0.01)
                log(f"     ⏳ WINDOW EXIT: {remaining_secs:.0f}s left, "
                    f"mid={current_mid:.2f}, selling @ {sell_at}")
                pos["_pending_sell_price"] = sell_at
                if self._sell_now(pos, "WINDOW", sell_at):
                    to_remove.append(pos)
                continue

            # 5. Post-resolution cleanup (fallback)
            if now > window_end + timedelta(minutes=10):
                bal = get_share_balance(pos["token_id"]) if not pos.get("dry_run") else 0
                if bal is not None and bal < 1:
                    pos["resolved"] = True
                    pos["pnl"] = round(-pos["cost"], 2)
                    pos["won"] = False
                    self._pnl += pos["pnl"]
                    log(f"  ❌ EXPIRED: {market['crypto'].upper()} {pos['direction']} — "
                        f"${pos['pnl']:.2f}")
                    self._log_trade(pos, "EXPIRED")
                    to_remove.append(pos)

        for pos in to_remove:
            if pos in self.positions:
                self.positions.remove(pos)
            self.completed.append(pos)

    def _log_status(self):
        """Print periodic status."""
        now = datetime.now(timezone.utc)
        active = len(self.positions)
        balance = get_usdc_balance()
        bal_str = f"${balance:.2f}" if balance else "?"

        # Volume stats
        vol_parts = []
        for crypto in CRYPTOS:
            recent, baseline = self.feed.get_volume_stats(crypto)
            price = self.feed.get_price(crypto)
            if price and baseline > 0:
                ratio = recent / baseline if baseline > 0 else 0
                vol_parts.append(f"{crypto.upper()}(${price:,.0f}):{ratio:.1f}×")

        log(f"\n  ⏰ {now.strftime('%H:%M:%S')} | Pos: {active} | "
            f"Trades: {self._trade_count} W:{self._win_count} | "
            f"PnL: ${self._pnl:+.2f} | Bal: {bal_str}")
        if vol_parts:
            log(f"     Vol: {', '.join(vol_parts)}")

        # Show available markets
        with self._lock:
            for crypto, m in self.markets.items():
                remaining = (m["window_end"] - now).total_seconds()
                if remaining > 0:
                    log(f"     {crypto.upper()}: {m['question'][:50]} ({remaining:.0f}s left)")

    def _log_trade(self, pos, action):
        """Append trade record to JSON file."""
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": action,
            "crypto": pos["market"]["crypto"],
            "direction": pos["direction"],
            "question": pos["market"]["question"],
            "buy_price": pos["buy_price"],
            "shares": pos["shares"],
            "cost": pos["cost"],
            "spike_usd": pos["spike_usd"],
            "spike_ratio": pos["spike_ratio"],
            "dir_ratio": pos["dir_ratio"],
            "pnl": pos.get("pnl", 0),
            "won": pos.get("won"),
        }
        with open(TRADE_FILE, "a") as f:
            f.write(json.dumps(record) + "\n")

    def _shutdown(self):
        """Clean shutdown: cancel ALL open orders to prevent orphaned positions."""
        self._running = False
        log("  Cancelling open buy orders (keeping sells live)...")
        # Cancel all open orders for tracked markets to prevent orphaned positions
        try:
            for pos in self.positions:
                if not pos.get("resolved") and not pos.get("sell_order_id"):
                    # This position has no pending sell — safe to cancel any lingering buys
                    pass
            # Use CLOB cancel_all to nuke any lingering buy orders
            open_orders = client.get_orders(OpenOrderParams())
            if open_orders:
                for order in open_orders:
                    side = order.get("side", "")
                    oid = order.get("id", "")
                    if side == "BUY" and oid:
                        try:
                            client.cancel(oid)
                            log(f"  Cancelled lingering buy order {oid[:10]}")
                        except Exception:
                            pass
        except Exception as e:
            log(f"  ⚠ Error cancelling orders: {e}")

        final_balance = get_usdc_balance()
        log("")
        log("=" * 60)
        log("  LIQUIDATION SNIPER STOPPED")
        log(f"  Trades: {self._trade_count} | Wins: {self._win_count}")
        log(f"  P&L: ${self._pnl:+.2f}")
        if final_balance:
            log(f"  Final USDC: ${final_balance:.2f}")
        if self._start_balance and final_balance:
            log(f"  Delta: ${final_balance - self._start_balance:+.2f}")
        log("=" * 60)


# =========================================================================
# MAIN
# =========================================================================
if __name__ == "__main__":
    bot = LiqSniper()
    bot.run()
