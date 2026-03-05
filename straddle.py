#!/usr/bin/env python3
"""
Straddle Bot — Volatility-based dual limit order strategy for Polymarket crypto markets.

Strategy:
  Place GTC limit BUY orders on BOTH sides (Up + Down) of a 5-min crypto market.
  If both fill, guaranteed profit: $1.00 - price_up - price_down per share.
  
  Use real-time Binance volatility to set order prices:
    - High volatility → wider spread (cheaper orders, more profit per pair)
    - Low volatility  → tighter spread (orders closer to mid, higher fill prob)
    - Strong trend    → skip (one-sided risk too high)
  
  Order lifecycle:
    1. ~30s before window opens: discover market, compute vol, place dual orders
    2. Monitor fills throughout window
    3. Both fill → guaranteed profit, wait for resolution
    4. One fills, near expiry → cancel unfilled side, accept one-sided exposure
    5. Neither fills → cancel both, no cost
    6. After resolution → claim winning shares ($1 each)

Math:
  Buy Up at price $a$, Down at price $b$ (both limit buys, GTC).
  If both fill: cost = $a + b$ per share-pair, revenue = $1.00 → profit = $1 - a - b
  If only Up fills and Up wins: revenue = $1.00, cost = $a → profit = $1 - a
  If only Up fills and Up loses: revenue = $0, cost = $a → loss = $a
  
  Key insight: lower prices → more profit if both fill, but lower fill probability.
  Optimal price depends on expected volatility (= probability of price swinging to both extremes).
"""

import os
import sys
import time
import json
import math
import asyncio
import threading
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque
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
# CONFIGURATION
# =========================================================================
HOST = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
BINANCE_WS_URL = "wss://stream.binance.com:9443/stream"
CHAIN_ID = 137

BINANCE_SYMBOLS = {
    "btc": "btcusdt", "eth": "ethusdt", "sol": "solusdt", "xrp": "xrpusdt",
}
CRYPTOS = ["btc", "eth", "sol", "xrp"]
INTERVALS = [5]                # 5-min markets

# ── Strategy params ──
SHARES_PER_SIDE = 15           # Shares to order per side
MAX_PAIRS = 2                  # Max simultaneous market pairs
MAX_TOTAL_RISK = 25.0          # Max USDC locked across all open orders

# ── Volatility → price mapping ──
# At each vol regime, what limit price to use for both sides
# profit_per_pair = 1 - 2 * price (assumes symmetric pricing)
DEFAULT_PRICE = 0.35           # Fallback if vol estimation fails
MIN_LIMIT_PRICE = 0.15        # Never go below 15¢ (too unlikely to fill)
MAX_LIMIT_PRICE = 0.42        # Never go above 42¢ (too little profit)
MIN_EXPECTED_MOVE = 0.0008    # Skip if expected 5min move < 0.08% (too calm)

# How far into the window to keep trying to fill
CANCEL_UNFILLED_PCT = 0.80    # Cancel unfilled orders after 80% of window
MIN_WINDOW_SECS = 30          # Don't enter if less than 30s until window opens

# ── Volatility estimation ──
VOL_LOOKBACK_MINUTES = 30     # Rolling window for realized vol
VOL_SAMPLE_INTERVAL = 5       # Sample Binance prices every N seconds
MIN_VOL_SAMPLES = 30          # Need at least this many samples before trading

# ── Trend detection ──
TREND_LOOKBACK_MINUTES = 5    # Recent momentum window
TREND_THRESHOLD = 2.0         # Skip if |momentum| > TREND_THRESHOLD × σ

# Data files
LOG_FILE = "straddle_log.txt"
TRADE_LOG = "straddle_trades.json"

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
    funder=funder_address,
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


# =========================================================================
# BINANCE PRICE FEED (real-time volatility source)
# =========================================================================
class BinanceFeed:
    """Real-time crypto prices from Binance WebSocket with rolling history."""

    def __init__(self):
        self._prices = {}       # symbol → {price, ts}
        self._history = {}      # symbol → deque of (ts, price)
        self._lock = threading.Lock()
        self._connected = False

    def start(self):
        t = threading.Thread(target=self._run, daemon=True)
        t.start()

    def _run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._ws_loop())

    async def _ws_loop(self):
        import websockets
        streams = "/".join(f"{sym}@miniTicker" for sym in BINANCE_SYMBOLS.values())
        url = f"{BINANCE_WS_URL}?streams={streams}"

        while True:
            try:
                async with websockets.connect(url, close_timeout=5, open_timeout=10) as ws:
                    self._connected = True
                    log("  🔌 Binance WS connected")
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
                        payload = data.get("data", data)
                        symbol = payload.get("s", "").lower()
                        close_price = payload.get("c")
                        if symbol and close_price:
                            now = time.time()
                            price = float(close_price)
                            with self._lock:
                                self._prices[symbol] = {"price": price, "ts": now}
                                if symbol not in self._history:
                                    self._history[symbol] = deque()
                                buf = self._history[symbol]
                                # Only store one sample per VOL_SAMPLE_INTERVAL
                                if not buf or (now - buf[-1][0]) >= VOL_SAMPLE_INTERVAL:
                                    buf.append((now, price))
                                    # Trim old entries
                                    cutoff = now - VOL_LOOKBACK_MINUTES * 60
                                    while buf and buf[0][0] < cutoff:
                                        buf.popleft()
            except Exception as e:
                self._connected = False
                log(f"  ⚠ Binance WS error: {e}, reconnecting in 2s...")
                await asyncio.sleep(2)

    def get_price(self, crypto):
        """Latest price for a crypto. Returns (price, age_secs) or (None, None)."""
        symbol = BINANCE_SYMBOLS.get(crypto.lower())
        if not symbol:
            return None, None
        with self._lock:
            data = self._prices.get(symbol)
        if not data:
            return None, None
        return data["price"], time.time() - data["ts"]

    def get_volatility(self, crypto):
        """Compute annualized realized vol from rolling price history.
        Returns (vol_per_sqrt_sec, n_samples) or (None, 0).
        
        vol_per_sqrt_sec: σ such that expected 5-min move ≈ σ × √300
        """
        symbol = BINANCE_SYMBOLS.get(crypto.lower())
        if not symbol:
            return None, 0
        with self._lock:
            buf = self._history.get(symbol)
            if not buf or len(buf) < MIN_VOL_SAMPLES:
                return None, len(buf) if buf else 0
            samples = list(buf)

        # Compute log returns between consecutive samples
        log_returns = []
        for i in range(1, len(samples)):
            dt = samples[i][0] - samples[i-1][0]
            if dt <= 0:
                continue
            lr = math.log(samples[i][1] / samples[i-1][1])
            # Normalize to per-√second
            log_returns.append(lr / math.sqrt(dt))

        if len(log_returns) < 10:
            return None, len(log_returns)

        mean_lr = sum(log_returns) / len(log_returns)
        var_lr = sum((lr - mean_lr) ** 2 for lr in log_returns) / (len(log_returns) - 1)
        vol = math.sqrt(var_lr)
        return vol, len(log_returns)

    def get_momentum(self, crypto, lookback_secs=None):
        """Compute recent momentum (signed return) over lookback period.
        Returns (return_pct, n_samples) or (None, 0).
        """
        if lookback_secs is None:
            lookback_secs = TREND_LOOKBACK_MINUTES * 60
        symbol = BINANCE_SYMBOLS.get(crypto.lower())
        if not symbol:
            return None, 0
        with self._lock:
            buf = self._history.get(symbol)
            if not buf or len(buf) < 2:
                return None, 0
            samples = list(buf)

        cutoff = time.time() - lookback_secs
        recent = [(ts, p) for ts, p in samples if ts >= cutoff]
        if len(recent) < 2:
            return None, 0

        ret = (recent[-1][1] - recent[0][1]) / recent[0][1]
        return ret, len(recent)


# =========================================================================
# MARKET DISCOVERY
# =========================================================================
def discover_next_window():
    """Find the NEXT 5-min crypto market window (not yet started or just started).
    Returns list of market dicts or empty list."""
    now = datetime.now(timezone.utc)
    markets = []

    for interval in INTERVALS:
        # Current window
        aligned = (now.minute // interval) * interval
        current_start = now.replace(minute=aligned, second=0, microsecond=0)
        current_end = current_start + timedelta(minutes=interval)

        # How far into the current window?
        elapsed = (now - current_start).total_seconds()
        total = interval * 60

        # If early enough in current window, use it
        if elapsed < total * 0.15:  # First 15% of window
            target_start = current_start
            target_end = current_end
        else:
            # Next window
            target_start = current_end
            target_end = target_start + timedelta(minutes=interval)

        epoch = int(target_start.timestamp())

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

                    up_idx = next((i for i, o in enumerate(outcomes)
                                   if "up" in o.lower()), 0)
                    down_idx = 1 - up_idx

                    markets.append({
                        "condition_id": condition_id,
                        "question": m.get("question", ""),
                        "crypto": crypto.upper(),
                        "interval": interval,
                        "slug": slug,
                        "epoch": epoch,
                        "window_start": target_start,
                        "window_end": target_end,
                        "tokens": tokens,
                        "up_token": tokens[up_idx],
                        "down_token": tokens[down_idx],
                    })
            except Exception as e:
                log(f"  ⚠ Market fetch {slug}: {e}")

    return markets


# =========================================================================
# VOLATILITY → ORDER PRICE MAPPING
# =========================================================================
def compute_limit_price(vol_per_sqrt_sec, interval_secs=300):
    """Map realized volatility to optimal limit order price.
    
    The key relationship:
    - Expected 5-min move = σ × √300
    - Higher vol → price tokens swing wider → our low-price limits fill more often
    - We want to set prices where P(both fill) × profit > P(one fill) × loss
    
    Empirical calibration (from Polymarket 5m crypto markets):
    - vol 3e-5 (~0.05%/5min): very calm, tokens barely move → need tight prices (0.44)
    - vol 7e-5 (~0.12%/5min): normal, moderate swings → medium prices (0.38)
    - vol 1.5e-4 (~0.26%/5min): volatile, wide swings → wide prices (0.28)
    - vol 3e-4 (~0.52%/5min): extreme volatility → aggressive prices (0.18)
    """
    if vol_per_sqrt_sec is None:
        return DEFAULT_PRICE

    expected_move = vol_per_sqrt_sec * math.sqrt(interval_secs)

    # Linear interpolation between vol levels
    # Format: (expected_move_pct, limit_price)
    breakpoints = [
        (0.0003, 0.45),   # Nearly flat → tight spread, small profit
        (0.0008, 0.42),   # Calm
        (0.0015, 0.38),   # Normal
        (0.0025, 0.33),   # Active
        (0.0040, 0.28),   # Volatile
        (0.0060, 0.22),   # Very volatile
        (0.0100, 0.15),   # Extreme → widest spread
    ]

    # Clamp
    if expected_move <= breakpoints[0][0]:
        return min(MAX_LIMIT_PRICE, breakpoints[0][1])
    if expected_move >= breakpoints[-1][0]:
        return max(MIN_LIMIT_PRICE, breakpoints[-1][1])

    # Linear interpolation
    for i in range(len(breakpoints) - 1):
        m0, p0 = breakpoints[i]
        m1, p1 = breakpoints[i + 1]
        if m0 <= expected_move <= m1:
            t = (expected_move - m0) / (m1 - m0)
            price = p0 + t * (p1 - p0)
            return round(max(MIN_LIMIT_PRICE, min(MAX_LIMIT_PRICE, price)), 2)

    return DEFAULT_PRICE


def should_skip_trend(momentum, vol_per_sqrt_sec, interval_secs=300):
    """Return True if market is trending too strongly for both-sides to work.
    
    A strong trend means price moves in one direction without reverting,
    so only one side fills and it's likely the LOSING side.
    """
    if momentum is None or vol_per_sqrt_sec is None:
        return False

    expected_move = vol_per_sqrt_sec * math.sqrt(interval_secs)
    if expected_move <= 0:
        return False

    # Momentum / expected_move = how many σ the trend is
    trend_strength = abs(momentum) / expected_move
    return trend_strength > TREND_THRESHOLD


# =========================================================================
# ORDER MANAGEMENT
# =========================================================================
class StraddlePosition:
    """Tracks a dual-order position on one market."""

    def __init__(self, market, price, shares):
        self.market = market
        self.price = price
        self.shares = shares
        self.up_order_id = None
        self.down_order_id = None
        self.up_filled = False
        self.down_filled = False
        self.up_shares = 0.0
        self.down_shares = 0.0
        self.cancelled = False
        self.resolved = False
        self.outcome = None       # "Up" or "Down"
        self.pnl = 0.0
        self.created_at = time.time()

    @property
    def both_filled(self):
        return self.up_filled and self.down_filled

    @property
    def one_filled(self):
        return (self.up_filled or self.down_filled) and not self.both_filled

    @property
    def neither_filled(self):
        return not self.up_filled and not self.down_filled

    @property
    def cost(self):
        cost = 0
        if self.up_filled:
            cost += self.up_shares * self.price
        if self.down_filled:
            cost += self.down_shares * self.price
        return cost

    @property
    def guaranteed_profit(self):
        """Profit per share if both sides filled."""
        return 1.0 - 2 * self.price

    def status_str(self):
        up = f"Up={'✓' if self.up_filled else '○'}"
        dn = f"Dn={'✓' if self.down_filled else '○'}"
        return f"{self.market['crypto']} {up} {dn} @{self.price} ({self.market['question'][:40]})"


# =========================================================================
# STRADDLE BOT
# =========================================================================
class StraddleBot:

    def __init__(self):
        self.binance = BinanceFeed()
        self.positions = []      # active StraddlePositions
        self.completed = []      # resolved positions
        self.total_pnl = 0.0
        self.total_trades = 0
        self.total_wins = 0      # "win" = both sides filled (guaranteed profit)
        self.single_fills = 0    # one side filled only
        self.single_wins = 0     # one side filled and it won
        self._traded_epochs = set()  # epochs we already have orders on
        self._last_status = 0
        self._last_discovery = 0

    def run(self):
        log(f"\n{'='*60}")
        log(f"Straddle bot {'(DRY RUN) ' if DRY_RUN else ''}started")
        log(f"Shares/side: {SHARES_PER_SIDE}, Max pairs: {MAX_PAIRS}")
        log(f"Price range: {MIN_LIMIT_PRICE}-{MAX_LIMIT_PRICE}, Default: {DEFAULT_PRICE}")
        log(f"Vol lookback: {VOL_LOOKBACK_MINUTES}min, Trend threshold: {TREND_THRESHOLD}σ")
        log(f"{'='*60}\n")

        balance = get_usdc_balance()
        if balance is not None:
            log(f"  Starting USDC: ${balance:.2f}")

        # Start price feed
        self.binance.start()

        # Wait for price data
        log("  Waiting for Binance price data...")
        for _ in range(60):
            time.sleep(1)
            if self.binance._connected:
                prices_ready = sum(1 for c in CRYPTOS
                                   if self.binance.get_price(c)[0] is not None)
                if prices_ready >= 2:
                    break
        log(f"  Binance ready: {prices_ready}/{len(CRYPTOS)} cryptos")

        self._last_status = time.time()
        self._print_status()

        # Main loop
        try:
            while True:
                self._tick()
                time.sleep(2)
        except KeyboardInterrupt:
            pass
        finally:
            try:
                log("\n  Shutting down...")
                self._cancel_all_open()
                self._print_summary()
            except Exception:
                pass

    def _tick(self):
        now = datetime.now(timezone.utc)

        # 1. Check for new windows to trade
        if len(self.positions) < MAX_PAIRS:
            self._find_and_enter_markets()

        # 2. Monitor existing positions
        for pos in list(self.positions):
            self._monitor_position(pos)

        # 3. Status update every 30s
        t = time.time()
        if t - self._last_status >= 30:
            self._last_status = t
            self._print_status()

    def _find_and_enter_markets(self):
        """Discover upcoming markets and place dual orders."""
        # Only call the API every 15 seconds to avoid rate limiting
        t = time.time()
        if t - self._last_discovery < 15:
            return
        self._last_discovery = t

        markets = discover_next_window()
        now = datetime.now(timezone.utc)

        if not markets:
            return  # Gamma API returned nothing — will retry next tick

        # Sort by volatility (highest first) — more volatile = better for straddles
        def _vol_key(m):
            v, _ = self.binance.get_volatility(m["crypto"].lower())
            return v or 0
        markets.sort(key=_vol_key, reverse=True)

        for market in markets:
            if len(self.positions) >= MAX_PAIRS:
                break

            crypto = market["crypto"].lower()
            epoch = market["epoch"]

            # Don't double-trade same window
            key = f"{crypto}-{epoch}"
            if key in self._traded_epochs:
                continue

            window_start = market["window_start"]
            window_end = market["window_end"]
            secs_to_start = (window_start - now).total_seconds()
            secs_to_end = (window_end - now).total_seconds()

            # Only enter if window is about to start or just started
            if secs_to_end < MIN_WINDOW_SECS:
                continue
            if secs_to_start > 60:  # Don't place orders more than 60s early
                continue

            # Check volatility
            vol, n_samples = self.binance.get_volatility(crypto)
            if vol is None:
                continue

            expected_move = vol * math.sqrt(300)

            # Skip if volatility too low — strategy needs price to swing both ways
            if expected_move < MIN_EXPECTED_MOVE:
                self._traded_epochs.add(key)
                log(f"  ⊘ {crypto.upper()}: vol too low "
                    f"({expected_move*100:.3f}% < {MIN_EXPECTED_MOVE*100:.3f}%), skipping")
                continue

            # Check trend
            momentum, _ = self.binance.get_momentum(crypto)
            if should_skip_trend(momentum, vol):
                trend_σ = abs(momentum) / expected_move if expected_move > 0 else 0
                log(f"  ⊘ {crypto.upper()}: trend too strong "
                    f"({momentum*100:+.3f}% = {trend_σ:.1f}σ), skipping")
                self._traded_epochs.add(key)
                continue

            # Compute order price from vol
            limit_price = compute_limit_price(vol)

            # Check risk: both sides' capital will be locked when orders are placed
            pair_cost = 2 * SHARES_PER_SIDE * limit_price
            locked_capital = sum(2 * p.shares * p.price for p in self.positions)
            if locked_capital + pair_cost > MAX_TOTAL_RISK:
                log(f"  ⊘ {crypto.upper()}: capital limit "
                    f"(${locked_capital + pair_cost:.0f} > ${MAX_TOTAL_RISK:.0f})")
                continue

            self._traded_epochs.add(key)

            disp_mom = f"{momentum*100:+.3f}%" if momentum else "?"
            log(f"\n  📊 {crypto.upper()} window {window_start.strftime('%H:%M')}-{window_end.strftime('%H:%M')}")
            log(f"     Vol: σ/√s={vol:.2e} ({expected_move*100:.3f}%/5min, {n_samples} samples)")
            log(f"     Momentum: {disp_mom}")
            log(f"     → Limit price: {limit_price:.2f} (profit if both fill: ${(1-2*limit_price)*SHARES_PER_SIDE:.2f})")

            # Place orders
            pos = StraddlePosition(market, limit_price, SHARES_PER_SIDE)

            if DRY_RUN:
                log(f"     [DRY RUN] Would place {SHARES_PER_SIDE}sh @ {limit_price} on both sides")
                pos.up_order_id = "dry-up"
                pos.down_order_id = "dry-down"
                self.positions.append(pos)
                continue

            # Place Up limit buy
            try:
                up_order = OrderArgs(
                    token_id=market["up_token"],
                    price=limit_price,
                    size=SHARES_PER_SIDE,
                    side=BUY,
                )
                signed = client.create_order(up_order)
                resp = client.post_order(signed, OrderType.GTC)
                pos.up_order_id = resp.get("orderID", "") if isinstance(resp, dict) else ""
                log(f"     ✓ Up order placed (id: {pos.up_order_id[:12]})")
            except Exception as e:
                log(f"     ✗ Up order failed: {e}")
                continue

            # Place Down limit buy
            try:
                dn_order = OrderArgs(
                    token_id=market["down_token"],
                    price=limit_price,
                    size=SHARES_PER_SIDE,
                    side=BUY,
                )
                signed = client.create_order(dn_order)
                resp = client.post_order(signed, OrderType.GTC)
                pos.down_order_id = resp.get("orderID", "") if isinstance(resp, dict) else ""
                log(f"     ✓ Down order placed (id: {pos.down_order_id[:12]})")
            except Exception as e:
                log(f"     ✗ Down order failed: {e}")
                # Cancel the Up order since we couldn't complete the pair
                if pos.up_order_id:
                    try:
                        client.cancel(pos.up_order_id)
                        log(f"     ↩ Cancelled orphan Up order")
                    except Exception:
                        pass
                continue

            self.positions.append(pos)
            self._save_trade(pos, "PLACED")

    def _monitor_position(self, pos):
        """Check fill status and manage position lifecycle."""
        now = datetime.now(timezone.utc)
        market = pos.market
        window_end = market["window_end"]
        window_start = market["window_start"]
        total_secs = (window_end - window_start).total_seconds()
        elapsed = (now - window_start).total_seconds()
        pct_elapsed = elapsed / total_secs if total_secs > 0 else 1.0

        if pos.cancelled or pos.resolved:
            return

        if DRY_RUN:
            # Simulate: both fill near the middle of the window
            if pct_elapsed >= 0.3 and not pos.up_filled:
                pos.up_filled = True
                pos.up_shares = pos.shares
                log(f"     [DRY] Up filled: {pos.shares}sh @ {pos.price}")
            if pct_elapsed >= 0.5 and not pos.down_filled:
                pos.down_filled = True
                pos.down_shares = pos.shares
                log(f"     [DRY] Down filled: {pos.shares}sh @ {pos.price}")
            if pct_elapsed >= 1.0:
                # Resolve
                import random
                pos.outcome = random.choice(["Up", "Down"])
                self._resolve_position(pos)
            return

        # Check fill status by querying share balance
        if not pos.up_filled:
            bal = get_share_balance(market["up_token"])
            if bal is not None and bal >= 1.0:  # At least 1 share filled
                pos.up_filled = True
                pos.up_shares = bal
                log(f"     ✓ Up FILLED: {bal:.1f}sh @ {pos.price} "
                    f"({market['crypto']} {market['question'][:35]})")

        if not pos.down_filled:
            bal = get_share_balance(market["down_token"])
            if bal is not None and bal >= 1.0:
                pos.down_filled = True
                pos.down_shares = bal
                log(f"     ✓ Down FILLED: {bal:.1f}sh @ {pos.price} "
                    f"({market['crypto']} {market['question'][:35]})")

        if pos.both_filled:
            profit = pos.up_shares + pos.down_shares - pos.cost
            log(f"  🎯 BOTH FILLED! {market['crypto']} guaranteed profit: "
                f"${profit:.2f} (cost ${pos.cost:.2f})")
            # Cancel any remaining limit order fragments
            self._cancel_orders(pos)

        # Near expiry: cancel unfilled orders
        if pct_elapsed >= CANCEL_UNFILLED_PCT and not pos.both_filled:
            if pos.neither_filled:
                log(f"  ⏰ {market['crypto']} {pct_elapsed*100:.0f}% elapsed, "
                    f"neither filled → cancelling both")
                self._cancel_orders(pos)
                pos.cancelled = True
                self.positions.remove(pos)
                return
            # One filled — can't cancel, ride it out
            if pos.one_filled:
                self._cancel_unfilled_side(pos)

        # After window ends: resolve
        if now > window_end + timedelta(seconds=10):
            self._check_resolution(pos)

    def _cancel_orders(self, pos):
        """Cancel both open orders."""
        for oid_attr in ["up_order_id", "down_order_id"]:
            oid = getattr(pos, oid_attr)
            if oid and not DRY_RUN:
                try:
                    client.cancel(oid)
                except Exception:
                    pass

    def _cancel_unfilled_side(self, pos):
        """Cancel the unfilled side's order."""
        if not pos.up_filled and pos.up_order_id:
            try:
                client.cancel(pos.up_order_id)
                log(f"     ↩ Cancelled unfilled Up order ({pos.market['crypto']})")
            except Exception:
                pass
        if not pos.down_filled and pos.down_order_id:
            try:
                client.cancel(pos.down_order_id)
                log(f"     ↩ Cancelled unfilled Down order ({pos.market['crypto']})")
            except Exception:
                pass

    def _check_resolution(self, pos):
        """Check if market has resolved and compute P&L."""
        market = pos.market
        slug = market["slug"]

        try:
            resp = requests.get(f"{GAMMA_API}/events/slug/{slug}", timeout=10)
            if resp.status_code != 200:
                return
            event = resp.json()
            for m in event.get("markets", []):
                cid = m.get("conditionId", "")
                if cid != market["condition_id"]:
                    continue
                # Check if resolved
                outcomes = json.loads(m.get("outcomes", "[]"))
                prices = json.loads(m.get("outcomePrices", "[]"))
                if not prices:
                    return
                # A resolved market has one price at 1.0 and the other at 0.0
                try:
                    float_prices = [float(p) for p in prices]
                except (ValueError, TypeError):
                    return
                if max(float_prices) < 0.95:
                    return  # Not yet resolved

                winner_idx = float_prices.index(max(float_prices))
                winner = outcomes[winner_idx] if winner_idx < len(outcomes) else "?"
                pos.outcome = winner
                self._resolve_position(pos)
        except Exception as e:
            log(f"  ⚠ Resolution check error: {e}")

    def _resolve_position(self, pos):
        """Compute final P&L for a resolved position."""
        if pos.resolved:
            return
        pos.resolved = True

        market = pos.market
        won_up = pos.outcome and "up" in pos.outcome.lower()

        if pos.both_filled:
            # Guaranteed profit — winner pays $1/share
            winning_shares = pos.up_shares if won_up else pos.down_shares
            pnl = winning_shares - pos.cost
            self.total_wins += 1
            result = "BOTH"
        elif pos.up_filled:
            if won_up:
                pnl = pos.up_shares - pos.up_shares * pos.price  # $1/share - cost
                self.single_wins += 1
            else:
                pnl = -pos.up_shares * pos.price  # Total loss
            self.single_fills += 1
            result = f"UP-only ({'won' if won_up else 'lost'})"
        elif pos.down_filled:
            if not won_up:
                pnl = pos.down_shares - pos.down_shares * pos.price
                self.single_wins += 1
            else:
                pnl = -pos.down_shares * pos.price
            self.single_fills += 1
            result = f"DN-only ({'won' if not won_up else 'lost'})"
        else:
            pnl = 0
            result = "NEITHER"

        pos.pnl = pnl
        self.total_pnl += pnl
        self.total_trades += 1

        emoji = "✅" if pnl >= 0 else "❌"
        log(f"  {emoji} {market['crypto']} resolved {pos.outcome}: {result} "
            f"PnL ${pnl:+.2f} (total: ${self.total_pnl:+.2f})")

        self._save_trade(pos, "RESOLVED")

        # Remove from active
        if pos in self.positions:
            self.positions.remove(pos)
        self.completed.append(pos)

    def _save_trade(self, pos, action):
        """Log trade to JSONL file."""
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": action,
            "crypto": pos.market["crypto"],
            "question": pos.market["question"],
            "slug": pos.market["slug"],
            "price": pos.price,
            "shares": pos.shares,
            "up_filled": pos.up_filled,
            "down_filled": pos.down_filled,
            "up_shares": round(pos.up_shares, 2),
            "down_shares": round(pos.down_shares, 2),
            "outcome": pos.outcome,
            "pnl": round(pos.pnl, 2) if pos.pnl else 0,
        }
        with open(TRADE_LOG, "a") as f:
            f.write(json.dumps(record) + "\n")

    def _cancel_all_open(self):
        """Cancel all open orders on shutdown."""
        log("  Cancelling all open orders...")
        for pos in self.positions:
            self._cancel_orders(pos)

    def _print_status(self):
        """Print periodic status."""
        active = len(self.positions)
        up_filled = sum(1 for p in self.positions if p.up_filled)
        dn_filled = sum(1 for p in self.positions if p.down_filled)
        both = sum(1 for p in self.positions if p.both_filled)

        # Vol summary
        vol_parts = []
        for c in CRYPTOS:
            v, n = self.binance.get_volatility(c)
            if v:
                em = v * math.sqrt(300) * 100
                p = self.binance.get_price(c)[0]
                price_str = f"${p:,.0f}" if p else "?"
                vol_parts.append(f"{c.upper()}({price_str}):{em:.3f}%/{n}s")

        vol_str = ", ".join(vol_parts) if vol_parts else "warming up..."

        log(f"\n  ⏰ {datetime.now(timezone.utc).strftime('%H:%M:%S')} | "
            f"Active: {active} | Both: {both} | Up: {up_filled} Down: {dn_filled} | "
            f"PnL: ${self.total_pnl:+.2f} | Trades: {self.total_trades}")
        log(f"     Vol: {vol_str}")
        for pos in self.positions:
            log(f"     {pos.status_str()}")

    def _print_summary(self):
        """Print final session summary."""
        log(f"\n{'='*60}")
        log(f"Straddle bot stopped")
        log(f"  Trades: {self.total_trades}")
        log(f"  Both-fill wins: {self.total_wins}")
        log(f"  Single-fills: {self.single_fills} ({self.single_wins} won)")
        log(f"  Total P&L: ${self.total_pnl:+.2f}")

        balance = get_usdc_balance()
        if balance is not None:
            log(f"  Final USDC: ${balance:.2f}")
        log(f"{'='*60}")


# =========================================================================
# MAIN
# =========================================================================
if __name__ == "__main__":
    bot = StraddleBot()
    bot.run()
