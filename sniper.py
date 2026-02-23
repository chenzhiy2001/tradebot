#!/usr/bin/env python3
"""
Sniper Bot — Bayesian prediction-based trading on Polymarket crypto markets.

Strategy:
  Use real-time Binance prices to compute the TRUE probability that a
  crypto "Up or Down" market will resolve UP, using the Bayesian formula:

    P(UP) = Φ( log(1 + R) / (σ √t_remaining) )

  where R = current return from window open, σ = realized volatility
  per √second (estimated from completed windows), t_remaining = seconds
  left in the window, Φ = standard normal CDF.

  Compare this to Polymarket's implied probability (token mid-price).
  When the edge exceeds MIN_EDGE, buy the underpriced token.

  Exit via GTC limit sell at $0.99 — fills when winning token resolves to $1.
  Losing side: sell never fills, falls back to resolution ($0).

Entry:
  - GTC limit buy at best_ask (crosses spread, fills immediately as taker)
  - Taker fee ~0.5-1.5% (negligible vs 15-25% edge)
  - Only one trade per window

Exit:
  - After buy fills, place GTC limit sell at $0.99
  - Winning token → $1.00 at resolution, $0.99 sell fills automatically
  - If sell placement fails, retries every SELL_RETRY_INTERVAL seconds
  - Losing token → $0, sell never fills → resolution (lose cost)

Data:
  - Records every window (predictions, trades, outcomes) to sniper_data.jsonl
  - After each completed window, recalibrates volatility estimate from ALL
    historical data (predict_data.jsonl + sniper_data.jsonl)
  - Continuously learns and adapts σ

Fees (5m/15m crypto markets):
  - Entry at ask → taker: fee = C × 0.25 × (p(1-p))^2 ≈ 0.5-1.5%
  - Exit via limit sell → maker/taker fee on sell side
"""

import os
import sys
import time
import json
import math
import asyncio
import threading
import re
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict
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
# CLOB CONFIG
# =========================================================================
HOST = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
RTDS_WS_URL = "wss://ws-live-data.polymarket.com"
BINANCE_WS_URL = "wss://stream.binance.com:9443/stream"
CHAIN_ID = 137
FUNDER_ADDRESS = founder_address

# Chainlink symbols (Polymarket resolution source)
CHAINLINK_SYMBOLS = {
    "btc": "btc/usd",
    "eth": "eth/usd",
    "sol": "sol/usd",
    "xrp": "xrp/usd",
}

# Binance symbols (fallback only)
CRYPTOS = {
    "btc": "btcusdt",
    "eth": "ethusdt",
    "sol": "solusdt",
    "xrp": "xrpusdt",
}

# =========================================================================
# STRATEGY PARAMETERS
# =========================================================================
BET_AMOUNT = 10               # Bet size in USDC per trade (auto-tuned by Kelly)
MIN_BET = 5                   # Floor — never bet less than this
MAX_BET = 50                  # Ceiling — never bet more than this
KELLY_FRACTION = 0.25         # Use quarter-Kelly (conservative) to size bets
MAX_POSITIONS = 8             # Max concurrent positions (windows we're active in)
MIN_TRADES_FOR_FULL_KELLY = 50  # Need this many trades before Kelly confidence = 100%
MAX_EXPOSURE_PCT = 0.50       # Never more than 50% of balance deployed in open positions
BET_RAMP_FACTOR = 1.5         # Max bet size INCREASE per reanalysis cycle (1.5 = +50%)
DRAWDOWN_HALT_PCT = 0.30      # If balance < 30% of session start, lock to MIN_BET
MIN_EDGE = 0.15               # Minimum |edge| to trade (data: 0.15+ = 68% WR, profitable)
MIN_RETURN_ABS = 0.0001       # Minimum |crypto return| (0.01%) — data: profitable at this level
MAX_ELAPSED_PCT = 0.75        # Don't enter after 75% of window elapsed (data: 80%+ → 0% WR)
ENTRY_PRICE_MIN = 0.55        # Only buy tokens priced ≥55¢ (lower = 50/50 WR, fees eat profits)
FILL_WAIT = 5                 # Seconds to wait for GTC limit buy fill
EXIT_PRICE = 0.99             # GTC sell price — fills when winning token → $1.00
MIN_ORDER_SIZE = 5            # Polymarket minimum order size in shares
SELL_RETRY_INTERVAL = 10      # Seconds between sell placement retries if first attempt fails
SELL_WAIT_AFTER_END = 600     # Seconds to wait after window ends for sell to fill (resolution ~2-3min, buffer for delays)
REANALYSIS_INTERVAL = 300     # Reanalyze every 5 minutes (not per-window)

# Bayesian model
DEFAULT_VOL_PER_SEC = 5.8e-5  # Default σ per √second (~0.1% per 5min window)
MIN_WINDOWS_FOR_VOL = 10      # Need this many completed windows before using empirical vol
DATA_LOOKBACK_SECS = 7200     # Use data from the last 2 hours for both σ and signal reanalysis
                              # (vol regimes and market microstructure both shift hourly)

# Polymarket fee formula (5m/15m crypto)
CRYPTO_FEE_RATE = 0.25
CRYPTO_FEE_EXPONENT = 2

# Data files
DATA_FILE = "sniper_data.jsonl"
TRADE_LOG = "sniper_trades.json"
DECISION_LOG = "sniper_log.txt"

MARKET_POLL_INTERVAL = 30     # Seconds between market discovery polls
TICK_INTERVAL = 1.0           # Main loop tick interval

AUTO_TUNE_EDGE = True         # Auto-adjust MIN_EDGE based on observed calibration

DRY_RUN = "--dry-run" in sys.argv


# =========================================================================
# LOGGING
# =========================================================================
def log(msg):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    with open(DECISION_LOG, "a") as f:
        f.write(line + "\n")
    print(msg)


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


def compute_taker_fee(shares, price):
    """Fee in USDC: C × feeRate × (p(1-p))^exponent × p"""
    if price <= 0 or price >= 1:
        return 0.0
    fee_shares = shares * CRYPTO_FEE_RATE * (price * (1 - price)) ** CRYPTO_FEE_EXPONENT
    return round(fee_shares * price, 4)


# =========================================================================
# BAYESIAN ENGINE
# =========================================================================
class BayesianEngine:
    """Computes Bayesian probability of UP outcome using Binance price data."""

    def __init__(self):
        self._vol_per_sec = DEFAULT_VOL_PER_SEC
        self._drift_per_sec = 0.0  # Estimated drift (μ) per second from recent windows
        self._window_returns = []  # (return, duration_sec) from completed windows
        self._signal_history = []  # dicts of {p_up, implied_up, edge_up, return, actual_up}
        self._lock = threading.Lock()
        self._load_historical()

    def _load_historical(self):
        """Load completed window data from sniper's own data file."""
        loaded = 0
        if not os.path.exists(DATA_FILE):
            log(f"  📊 No historical data — using default σ/√s = {self._vol_per_sec:.6f}")
            return
        try:
            with open(DATA_FILE) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    rec = json.loads(line)
                    op = rec.get("open_price")
                    cp = rec.get("close_price")
                    interval = rec.get("interval", 5)
                    if op and cp and op > 0:
                        ret = math.log(cp / op)  # log return for consistency with P(UP) model
                        duration = interval * 60
                        # Use window_end timestamp if available, else 0 (will be filtered by recency)
                        ts_str = rec.get("window_end") or rec.get("timestamp", "")
                        try:
                            from datetime import datetime as _dt
                            ts = _dt.fromisoformat(ts_str).timestamp() if ts_str else 0
                        except Exception:
                            ts = 0
                        self._window_returns.append((ret, duration, ts))
                        loaded += 1
        except Exception as e:
            log(f"  ⚠ Error loading {DATA_FILE}: {e}")

        if loaded > 0:
            self._recompute_vol()
            self._recompute_drift()
            log(f"  📊 Loaded {loaded} historical windows → σ/√s = {self._vol_per_sec:.6f}")
        else:
            log(f"  📊 No historical data — using default σ/√s = {self._vol_per_sec:.6f}")

    def _recompute_vol(self):
        """Recompute volatility per √second from recent window returns only."""
        if len(self._window_returns) < MIN_WINDOWS_FOR_VOL:
            return

        # Only use recent windows — vol regime changes with market conditions
        cutoff = time.time() - DATA_LOOKBACK_SECS
        recent = [(r, d) for r, d, ts in self._window_returns if ts >= cutoff]

        # Normalize log returns to per-√second: z_i = log_ret_i / √D_i
        # Under GBM (zero drift), log_ret ~ N(0, σ²t), so z_i ~ N(0, σ²)
        normalized = [r / math.sqrt(d) for r, d in recent if d > 0]
        if len(normalized) < MIN_WINDOWS_FOR_VOL:
            return

        mean_z = sum(normalized) / len(normalized)
        var_z = sum((z - mean_z) ** 2 for z in normalized) / (len(normalized) - 1)
        new_vol = math.sqrt(var_z)

        if new_vol > 0:
            self._vol_per_sec = new_vol
            log(f"  📊 Vol recalibrated: σ/√s = {self._vol_per_sec:.6f} "
                f"(from {len(normalized)}/{len(self._window_returns)} recent windows, "
                f"~{self._vol_per_sec * math.sqrt(300) * 100:.3f}% per 5min)")

    def _recompute_drift(self):
        """Estimate drift (μ per second) from recent completed windows.
        Positive μ → UP bias, negative μ → DOWN bias.
        Used in P(UP) formula to account for market momentum."""
        if len(self._window_returns) < MIN_WINDOWS_FOR_VOL:
            return

        cutoff = time.time() - DATA_LOOKBACK_SECS
        recent = [(r, d) for r, d, ts in self._window_returns if ts >= cutoff]
        drift_estimates = [r / d for r, d in recent if d > 0]
        if len(drift_estimates) < MIN_WINDOWS_FOR_VOL:
            return

        self._drift_per_sec = sum(drift_estimates) / len(drift_estimates)
        drift_5m = self._drift_per_sec * 300 * 100  # % per 5min window
        direction = "↑" if self._drift_per_sec > 0 else "↓"
        log(f"  📊 Drift estimated: μ/s = {self._drift_per_sec:.2e} "
            f"({direction}{abs(drift_5m):.4f}%/5min from {len(drift_estimates)} windows)")

    def add_completed_window(self, ret, duration_sec):
        """Add a completed window's return to the dataset.
        ret: simple return (close-open)/open — converted to log return internally."""
        with self._lock:
            log_ret = math.log(1 + ret) if ret > -1 else -10  # log return
            self._window_returns.append((log_ret, duration_sec, time.time()))

    def add_signal_outcome(self, signal, outcome):
        """Record a signal and its actual outcome for reanalysis.
        signal: dict with keys p_up, implied_up, edge_up, return, pct
        outcome: 'UP' or 'DOWN'
        """
        with self._lock:
            self._signal_history.append({
                **signal,
                "actual_up": 1.0 if outcome == "UP" else 0.0,
                "_ts": time.time(),
            })

    @staticmethod
    def _sig_tradeable(s, threshold):
        """Check if signal would trigger a trade at given edge threshold."""
        return s["edge_up"] >= threshold or s["edge_down"] >= threshold

    @staticmethod
    def _sig_win(s, threshold):
        """Check if tradeable signal would have been a winning trade."""
        return ((s["edge_up"] >= threshold and s["actual_up"] == 1.0) or
                (s["edge_down"] >= threshold and s["actual_up"] == 0.0))

    @staticmethod
    def _sig_entry_price(s, threshold):
        """Estimate entry price for a tradeable signal."""
        if s["edge_up"] >= threshold:
            return s["implied_up"]
        if s["edge_down"] >= threshold:
            return 1.0 - s["implied_up"]
        return 0.55

    @staticmethod
    def _sig_best_edge(s):
        """Get the best (highest) edge from a signal."""
        return max(s["edge_up"], s["edge_down"])

    def reanalyze(self):
        """Full reanalysis of prediction accuracy and edge calibration.
        Returns a report dict with findings and recommended adjustments."""
        cutoff = time.time() - DATA_LOOKBACK_SECS
        with self._lock:
            # Only use recent signals — market microstructure changes hourly
            signals = [s for s in self._signal_history if s.get("_ts", 0) >= cutoff]
            # Keep cap at 500 in case of very busy periods
            signals = signals[-500:]
            n_windows = len(self._window_returns)

        if len(signals) < 20:
            return None  # Not enough data

        report = {"n_signals": len(signals), "n_windows": n_windows}

        # 1. Prediction accuracy — bin P(UP) predictions and check calibration
        #    If we predict P(UP)=0.80, do 80% of those actually resolve UP?
        bins = [(0, 0.2), (0.2, 0.4), (0.4, 0.6), (0.6, 0.8), (0.8, 1.01)]
        calibration = []
        for lo, hi in bins:
            in_bin = [s for s in signals if lo <= s["p_up"] < hi]
            if len(in_bin) >= 3:
                predicted_avg = sum(s["p_up"] for s in in_bin) / len(in_bin)
                actual_avg = sum(s["actual_up"] for s in in_bin) / len(in_bin)
                calibration.append({
                    "bin": f"{lo:.1f}-{hi:.1f}",
                    "n": len(in_bin),
                    "predicted": round(predicted_avg, 3),
                    "actual": round(actual_avg, 3),
                    "gap": round(actual_avg - predicted_avg, 3),
                })
        report["calibration"] = calibration

        # 2. Edge analysis — for signals where we'd have traded,
        #    what was the actual win rate vs predicted?
        for threshold in [0.10, 0.15, 0.20, 0.25, 0.30]:
            tradeable = [s for s in signals
                         if self._sig_tradeable(s, threshold)
                         and abs(s["return"]) >= MIN_RETURN_ABS * 100]  # return is in %
            if len(tradeable) >= 5:
                wins = sum(1 for s in tradeable if self._sig_win(s, threshold))
                total_edge = sum(self._sig_best_edge(s) for s in tradeable)

                wr = wins / len(tradeable) if tradeable else 0
                avg_edge = total_edge / len(tradeable) if tradeable else 0
                # Compute breakeven WR from actual payoff asymmetry:
                #   Win payout per share = EXIT_PRICE - avg_entry
                #   Loss per share = avg_entry
                #   Breakeven WR = avg_entry / EXIT_PRICE
                # At avg entry 0.55: breakeven = 55.6%, at 0.65: 65.7%
                entry_prices = [self._sig_entry_price(s, threshold) for s in tradeable]
                avg_entry = sum(entry_prices) / len(entry_prices) if entry_prices else 0.55
                breakeven_wr = avg_entry / EXIT_PRICE
                report[f"edge_{threshold:.2f}"] = {
                    "n": len(tradeable),
                    "win_rate": round(wr, 3),
                    "avg_edge": round(avg_edge, 3),
                    "avg_entry": round(avg_entry, 3),
                    "breakeven_wr": round(breakeven_wr, 3),
                    "profitable": wr > breakeven_wr + 0.03,  # Need WR > breakeven + 3% margin
                }

        # 3. Optimal edge threshold — find lowest threshold that beats breakeven WR
        optimal_edge = MIN_EDGE
        for threshold in [0.10, 0.12, 0.15, 0.18, 0.20, 0.25, 0.30]:
            key = f"edge_{threshold:.2f}"
            if key not in report:
                # Compute it if not already done
                tradeable = [s for s in signals
                             if self._sig_tradeable(s, threshold)
                             and abs(s["return"]) >= MIN_RETURN_ABS * 100]
                if len(tradeable) >= 5:
                    wins = sum(1 for s in tradeable if self._sig_win(s, threshold))
                    wr = wins / len(tradeable)
                    entry_prices = [self._sig_entry_price(s, threshold) for s in tradeable]
                    avg_entry = sum(entry_prices) / len(entry_prices) if entry_prices else 0.55
                    breakeven_wr = avg_entry / EXIT_PRICE
                    report[key] = {"n": len(tradeable), "win_rate": round(wr, 3),
                                   "breakeven_wr": round(breakeven_wr, 3)}
            data = report.get(key)
            if data and data.get("n", 0) >= 5:
                be_wr = data.get("breakeven_wr", 0.65)  # conservative default
                if data["win_rate"] >= be_wr + 0.03:  # WR beats breakeven + 3% margin
                    optimal_edge = threshold
                    break  # Use the lowest profitable threshold

        report["optimal_edge"] = optimal_edge
        report["current_edge"] = MIN_EDGE

        # 4. Timing analysis — informational only (model handles timing via t_rem)
        timing_buckets = [(0.0, 0.20), (0.20, 0.40), (0.40, 0.60), (0.60, 0.80), (0.80, 1.01)]
        timing_analysis = []
        for lo, hi in timing_buckets:
            eligible = [s for s in signals
                        if lo <= s["pct"] < hi
                        and self._sig_tradeable(s, optimal_edge)
                        and abs(s["return"]) >= MIN_RETURN_ABS * 100]
            if len(eligible) >= 3:
                wins = sum(1 for s in eligible if self._sig_win(s, optimal_edge))
                wr = wins / len(eligible)
                timing_analysis.append({
                    "bucket": f"{lo:.0%}-{hi:.0%}",
                    "n": len(eligible),
                    "win_rate": round(wr, 3),
                })
        report["timing"] = timing_analysis

        # 5. Kelly criterion bet sizing
        #    Kelly fraction f* = (p*b - q) / b
        #    where p = win rate, q = 1-p, b = net odds (win_payout / loss_cost)
        #    For Polymarket: buy at price P, win → $1/share, lose → $0
        #      b = (1-P)/P (e.g. buy at 0.60 → b = 0.667)
        #    Use average entry price from tradeable signals
        #
        #    KEY FIX: Use Bayesian WR (Beta posterior) instead of raw WR to
        #    prevent small-sample overconfidence (7/7 wins = 100% WR → ruin).
        #    Also apply confidence scaling: Kelly ramps up as sqrt(n),
        #    reaching full confidence only at MIN_TRADES_FOR_FULL_KELLY.
        tradeable_at_optimal = [s for s in signals
                                if self._sig_tradeable(s, optimal_edge)
                                and abs(s["return"]) >= MIN_RETURN_ABS * 100]
        if len(tradeable_at_optimal) >= 5:
            n_trades = len(tradeable_at_optimal)
            wins = sum(1 for s in tradeable_at_optimal if self._sig_win(s, optimal_edge))

            # Bayesian WR: Beta(1,1) prior → posterior mean = (wins+1)/(n+2)
            # This naturally penalizes small samples:
            #   7/7 wins → 8/9 = 0.889 (not 1.0)
            #  20/30 wins → 21/32 = 0.656 (not 0.667)
            p_raw = wins / n_trades
            p = (wins + 1) / (n_trades + 2)  # Bayesian posterior mean
            q = 1 - p

            # Confidence scaling: ramp up Kelly with sample size
            # Full confidence at MIN_TRADES_FOR_FULL_KELLY trades
            confidence = min(1.0, math.sqrt(n_trades) / math.sqrt(MIN_TRADES_FOR_FULL_KELLY))

            # Estimate average entry price from implied probs
            avg_prices = [self._sig_entry_price(s, optimal_edge) for s in tradeable_at_optimal]
            avg_price = sum(avg_prices) / len(avg_prices) if avg_prices else 0.50
            avg_price = max(0.10, min(0.90, avg_price))

            b = (EXIT_PRICE - avg_price) / avg_price  # net odds (sell at $0.99, not $1.00)
            kelly_full = (p * b - q) / b if b > 0 else 0
            kelly_full = max(0, kelly_full)  # never negative

            # Effective Kelly = full_kelly × KELLY_FRACTION × confidence
            kelly_bet_pct = kelly_full * KELLY_FRACTION * confidence

            report["kelly"] = {
                "win_rate_raw": round(p_raw, 3),
                "win_rate_bayes": round(p, 3),
                "n_trades": n_trades,
                "avg_entry_price": round(avg_price, 3),
                "net_odds": round(b, 3),
                "kelly_full": round(kelly_full, 4),
                "kelly_fraction": KELLY_FRACTION,
                "confidence": round(confidence, 3),
                "kelly_bet_pct": round(kelly_bet_pct, 4),
            }
        else:
            report["kelly"] = None

        return report

    @property
    def signal_count(self):
        with self._lock:
            return len(self._signal_history)

    def prob_up(self, current_return, seconds_remaining):
        """
        P(UP) = Φ( log(1+R) / (σ √t_rem) )

        Uses symmetric GBM (zero drift). Drift estimation is kept for
        monitoring only — using it in the formula caused wrong-side bets
        during market reversals (positive return → DOWN bet due to stale drift).

        current_return: (S(t) - S(0)) / S(0)  (e.g. 0.001 = 0.1%)
        seconds_remaining: seconds until window closes
        """
        if seconds_remaining <= 0:
            # Window is over — return is final
            return 1.0 if current_return > 0 else 0.0

        with self._lock:
            vol = self._vol_per_sec

        # Avoid log of non-positive
        if current_return <= -1:
            return 0.0

        log_return = math.log(1 + current_return)
        denom = vol * math.sqrt(seconds_remaining)

        if denom <= 0:
            return 0.5

        z = log_return / denom
        # Φ(z) = 0.5 * (1 + erf(z / √2))
        p = 0.5 * (1 + math.erf(z / math.sqrt(2)))
        return max(0.001, min(0.999, p))

    @property
    def vol_per_sec(self):
        with self._lock:
            return self._vol_per_sec

    @property
    def drift_per_sec(self):
        with self._lock:
            return self._drift_per_sec

    def recent_side_bias(self):
        """Returns (up_rate, n) — fraction of recent windows that resolved UP."""
        cutoff = time.time() - DATA_LOOKBACK_SECS
        with self._lock:
            recent = [s for s in self._signal_history if s.get("_ts", 0) >= cutoff]
        if len(recent) < 10:
            return 0.5, len(recent)  # Not enough data — assume neutral
        up_count = sum(1 for s in recent if s.get("actual_up", 0) == 1.0)
        return up_count / len(recent), len(recent)

    @property
    def n_windows(self):
        with self._lock:
            return len(self._window_returns)


# =========================================================================
# CHAINLINK PRICE FEED (via Polymarket RTDS — matches resolution source)
# =========================================================================
class ChainlinkFeed:
    """Real-time Chainlink prices from Polymarket RTDS WebSocket.
    This is the SAME data source Polymarket uses to resolve crypto markets."""

    def __init__(self):
        self._prices = {}  # symbol -> {price, ts}
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

        while True:
            try:
                async with websockets.connect(RTDS_WS_URL, close_timeout=5, open_timeout=10) as ws:
                    # Subscribe to Chainlink crypto prices
                    sub_msg = json.dumps({
                        "action": "subscribe",
                        "subscriptions": [{
                            "topic": "crypto_prices_chainlink",
                            "type": "*",
                            "filters": "",
                        }]
                    })
                    await ws.send(sub_msg)
                    self._connected = True
                    log("  \U0001f517 Chainlink RTDS connected (Polymarket resolution source)")

                    last_ping = time.time()

                    while True:
                        # Keep alive
                        if time.time() - last_ping > 4:
                            try:
                                await ws.send("PING")
                                last_ping = time.time()
                            except Exception:
                                break

                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1)
                        except asyncio.TimeoutError:
                            continue

                        try:
                            data = json.loads(msg)
                        except (json.JSONDecodeError, TypeError):
                            continue

                        # Debug: log first few messages to diagnose format
                        if not hasattr(self, '_debug_count'):
                            self._debug_count = 0
                        if self._debug_count < 3:
                            self._debug_count += 1
                            log(f"  🔍 RTDS msg #{self._debug_count}: {str(data)[:200]}")

                        if data.get("topic") == "crypto_prices_chainlink":
                            payload = data.get("payload", {})
                            symbol = payload.get("symbol", "").lower()
                            value = payload.get("value")
                            ts = payload.get("timestamp")  # unix ms
                            if symbol and value is not None:
                                with self._lock:
                                    self._prices[symbol] = {
                                        "price": float(value),
                                        "ts": (ts / 1000.0) if ts else time.time(),
                                    }

            except Exception as e:
                self._connected = False
                log(f"  \u26a0 Chainlink RTDS error: {e}, reconnecting in 2s...")
                await asyncio.sleep(2)

    def get_price(self, crypto):
        """Get latest Chainlink price for a crypto (e.g. 'btc'). Returns (price, age_secs) or (None, None)."""
        symbol = CHAINLINK_SYMBOLS.get(crypto.lower())
        if not symbol:
            return None, None
        with self._lock:
            data = self._prices.get(symbol)
        if not data:
            return None, None
        return data["price"], time.time() - data["ts"]

    @property
    def connected(self):
        return self._connected


# =========================================================================
# BINANCE PRICE FEED (fallback)
# =========================================================================
class BinanceFeed:
    """Real-time crypto prices from Binance WebSocket."""

    def __init__(self):
        self._prices = {}
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

        streams = "/".join(f"{sym}@miniTicker" for sym in CRYPTOS.values())
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
                            with self._lock:
                                self._prices[symbol] = {
                                    "price": float(close_price),
                                    "ts": time.time(),
                                }

            except Exception as e:
                self._connected = False
                log(f"  ⚠ Binance WS error: {e}, reconnecting in 2s...")
                await asyncio.sleep(2)

    def get_price(self, crypto):
        """Get latest price for a crypto (e.g. 'btc'). Returns (price, age_secs) or (None, None)."""
        symbol = CRYPTOS.get(crypto.lower())
        if not symbol:
            return None, None
        with self._lock:
            data = self._prices.get(symbol)
        if not data:
            return None, None
        return data["price"], time.time() - data["ts"]

    @property
    def connected(self):
        return self._connected


# =========================================================================
# POLYMARKET PRICE FEED
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
                                "custom_feature_enabled": True,
                            }))
                            last_sub_time = time.time()
                            if changed:
                                log(f"  📡 Polymarket subscribed to {len(wanted)} tokens")
                            self._active = wanted

                        if time.time() - last_ping > 10:
                            try:
                                await ws.send("ping")
                                last_ping = time.time()
                            except Exception:
                                break

                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=0.1)
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

    @property
    def connected(self):
        return self._connected


# =========================================================================
# MARKET DISCOVERY
# =========================================================================
def discover_markets():
    """Fetch current Polymarket crypto updown markets."""
    markets = []
    now = datetime.now(timezone.utc)

    for interval in [5, 15]:
        aligned = (now.minute // interval) * interval
        window_start = now.replace(minute=aligned, second=0, microsecond=0)
        epoch = int(window_start.timestamp())

        for crypto in CRYPTOS:
            slug = f"{crypto}-updown-{interval}m-{epoch}"
            try:
                resp = requests.get(f"{GAMMA_API}/events/slug/{slug}", timeout=10)
                if resp.status_code == 200:
                    event = resp.json()
                    for m in event.get("markets", []):
                        if not m.get("closed"):
                            m["_crypto"] = crypto
                            m["_interval"] = interval
                            m["_epoch"] = epoch
                            markets.append(m)
            except Exception:
                pass
    return markets


# =========================================================================
# SNIPER — window manager + trade execution
# =========================================================================
class Sniper:
    """Tracks active windows, computes Bayesian edge, executes trades."""

    def __init__(self, chainlink, binance, poly, engine):
        self.chainlink = chainlink
        self.binance = binance
        self.poly = poly
        self.engine = engine
        self._windows = {}        # key -> window_state
        self._trades = []         # completed trade records
        self._lock = threading.Lock()
        self._session_start = time.time()
        self._session_cost = 0.0  # Total USDC spent on buys
        self._session_won = 0.0   # Total USDC from winning trades
        self._trade_count = 0
        self._win_count = 0
        self._start_balance = get_usdc_balance()
        self._last_report = None
        self._last_reanalysis = 0  # epoch time of last reanalysis

    def update_markets(self, markets):
        """Add new market windows, subscribe tokens."""
        all_tokens = []
        now = datetime.now(timezone.utc)

        for m in markets:
            question = m.get("question", "")
            tokens = json.loads(m.get("clobTokenIds", "[]"))
            if len(tokens) != 2:
                continue

            crypto = m["_crypto"]
            interval = m["_interval"]
            epoch = m["_epoch"]

            # Verify token ordering from outcomes field
            outcomes = json.loads(m.get("outcomes", "[]"))
            if len(outcomes) == 2:
                # Match tokens to UP/DOWN outcomes
                up_idx = next((i for i, o in enumerate(outcomes) if "up" in o.lower()), 0)
                down_idx = 1 - up_idx
                up_token = tokens[up_idx]
                down_token = tokens[down_idx]
            else:
                # Fallback: assume standard ordering
                up_token = tokens[0]
                down_token = tokens[1]

            # Compute window times from epoch (reliable, no timezone parsing needed)
            start_utc = datetime.fromtimestamp(epoch, tz=timezone.utc)
            end_utc = start_utc + timedelta(minutes=interval)

            if now >= end_utc:
                continue

            key = f"{crypto}_{interval}m_{epoch}"
            all_tokens.extend(tokens)

            with self._lock:
                if key not in self._windows:
                    self._windows[key] = {
                        "key": key,
                        "question": question,
                        "crypto": crypto,
                        "interval": interval,
                        "start_utc": start_utc,
                        "end_utc": end_utc,
                        "duration": interval * 60,
                        "up_token": up_token,
                        "down_token": down_token,
                        "open_price": None,
                        "close_price": None,
                        "traded": False,
                        "trade_info": None,   # filled in if we trade
                        "sell_order_id": None,
                        "sell_placed": False,
                        "sell_filled": False,
                        "exit_price": None,
                        "_last_sell_attempt": 0,
                        "window_ended": False,
                        "window_ended_at": None,
                        "completed": False,
                        "signals": [],        # log of signals seen
                    }

        self.poly.subscribe(all_tokens)

    def tick(self):
        """Main tick: compute signals, execute trades, finalize windows."""
        now = datetime.now(timezone.utc)
        to_remove = []

        with self._lock:
            windows = list(self._windows.items())

        for key, w in windows:
            start = w["start_utc"]
            end = w["end_utc"]
            duration = w["duration"]
            elapsed = (now - start).total_seconds()
            pct = elapsed / duration if duration > 0 else 0
            remaining = max(0, (end - now).total_seconds())

            crypto = w["crypto"]
            # Use Chainlink (resolution source) as primary, Binance as fallback
            cl_price, cl_age = self.chainlink.get_price(crypto)
            bn_price, bn_age = self.binance.get_price(crypto)
            price = cl_price if cl_price is not None else bn_price

            # Record open price at window start
            if w["open_price"] is None and price is not None:
                if pct <= 0.05:
                    w["open_price"] = price
                elif pct <= 0.15:
                    w["open_price"] = price  # Late join, best we can do

            # ─── Check exit sell fills / retry placement ───
            # Keep checking even after window ends — sell fills at resolution
            if (w["traded"] and not w.get("sell_filled")
                    and not w["completed"]):
                if w.get("sell_placed"):
                    self._check_sell_fills(w, key)
                elif not w["trade_info"].get("dry_run"):
                    # Sell not yet placed — retry periodically (but only if we have enough shares)
                    ti = w["trade_info"]
                    if ti and ti.get("shares", 0) >= MIN_ORDER_SIZE:
                        if time.time() - w.get("_last_sell_attempt", 0) >= SELL_RETRY_INTERVAL:
                            self._place_exit_sell(w)

            # ─── Compute Bayesian signal ───
            if (w["open_price"] is not None
                    and price is not None
                    and not w["traded"]
                    and not w["completed"]):

                current_return = (price - w["open_price"]) / w["open_price"]
                p_up = self.engine.prob_up(current_return, remaining)

                # Get Polymarket implied probability
                up_bid, up_ask, poly_age = self.poly.get_price(w["up_token"])
                down_bid, down_ask, _ = self.poly.get_price(w["down_token"])

                implied_up = None
                if up_bid and up_ask and up_bid > 0 and up_ask > 0:
                    implied_up = (up_bid + up_ask) / 2.0

                # Compute ask-based edges: edge = our_prob - actual_buy_price
                # These reflect the REAL edge after crossing the spread.
                # Mid-price edges systematically inflate edge by half the spread.
                edge_up = (p_up - up_ask) if (up_ask and up_ask > 0) else None
                edge_down = ((1.0 - p_up) - down_ask) if (down_ask and down_ask > 0) else None

                if implied_up is not None:
                    # Log signal for dashboard and reanalysis
                    signal = {
                        "pct": round(pct, 3),
                        "return": round(current_return * 100, 4),
                        "p_up": round(p_up, 4),
                        "implied_up": round(implied_up, 4),
                        "edge_up": round(edge_up, 4) if edge_up is not None else 0,
                        "edge_down": round(edge_down, 4) if edge_down is not None else 0,
                        "remaining": round(remaining, 1),
                    }
                    # Keep last 5 signals per window
                    w["signals"] = w["signals"][-4:] + [signal]

                    # ─── Trade decision ───
                    if pct > MAX_ELAPSED_PCT:
                        continue  # Too late in window — 0% WR historically

                    # Skip if Polymarket prices are stale (>10s old)
                    if poly_age is not None and poly_age > 10:
                        continue

                    if abs(current_return) >= MIN_RETURN_ABS:

                        # Determine which side to buy (prefer higher edge)
                        buy_up = edge_up is not None and edge_up >= MIN_EDGE
                        buy_down = edge_down is not None and edge_down >= MIN_EDGE
                        if buy_up and buy_down:
                            # Both sides have edge — pick higher
                            if edge_down > edge_up:
                                buy_up = False
                            else:
                                buy_down = False

                        if buy_up:
                            token_id = w["up_token"]
                            buy_side = "UP"
                            buy_price = up_ask
                            edge = edge_up
                            our_prob = p_up
                        elif buy_down:
                            token_id = w["down_token"]
                            buy_side = "DOWN"
                            buy_price = down_ask
                            edge = edge_down
                            our_prob = 1.0 - p_up
                        else:
                            continue  # Not enough edge

                        if buy_price is None or buy_price <= 0:
                            continue

                        # Price range check
                        if buy_price < ENTRY_PRICE_MIN:
                            continue

                        # Balance check
                        balance = get_usdc_balance()
                        if balance is None or balance < BET_AMOUNT + 2:
                            continue

                        # Count active positions & total exposure
                        active_count = 0
                        open_exposure = 0.0
                        for ww in self._windows.values():
                            if ww.get("traded") and not ww.get("completed"):
                                active_count += 1
                                ti = ww.get("trade_info")
                                if ti:
                                    open_exposure += ti.get("cost", 0)
                        if active_count >= MAX_POSITIONS:
                            continue

                        # Exposure cap: don't exceed MAX_EXPOSURE_PCT of balance
                        if open_exposure + BET_AMOUNT > balance * MAX_EXPOSURE_PCT:
                            continue  # Skip — would exceed exposure limit

                        # ─── EXECUTE TRADE ───
                        log(f"  🎯 SIGNAL {key}: {buy_side} | P(UP)={p_up:.3f} impl={implied_up:.3f} "
                            f"edge={edge:.3f} | ret={current_return*100:+.3f}% | {remaining:.0f}s left "
                            f"| price={'CL' if cl_price is not None else 'BN'}")

                        success = self._execute_buy(
                            w, token_id, buy_side, buy_price, edge, our_prob, current_return
                        )
                        if success:
                            w["traded"] = True

            # ─── Window ended: record data once ───
            if now >= end and not w["window_ended"]:
                if price is not None:
                    w["close_price"] = price

                if w["open_price"] and w["close_price"]:
                    outcome = "UP" if w["close_price"] > w["open_price"] else "DOWN"
                    ret = (w["close_price"] - w["open_price"]) / w["open_price"]
                    w["_outcome"] = outcome
                    w["_return"] = ret

                    # Vol data (σ) — pure price, always safe at window end
                    self.engine.add_completed_window(ret, duration)

                    # Signal outcomes = price direction (UP/DOWN), always known
                    # at window end regardless of whether our sell has filled.
                    # Trade PnL is a separate question resolved later.
                    for sig in w["signals"]:
                        self.engine.add_signal_outcome(sig, outcome)

                    if not w["traded"]:
                        # No trade — finalize immediately
                        log(f"  📊 {key}: {outcome} (no trade) | ret={ret*100:+.3f}%")
                        self._save_window(w, outcome, ret)
                        w["completed"] = True
                        to_remove.append(key)
                    elif w.get("sell_filled"):
                        # Sell already filled before window ended — finalize
                        ti = w["trade_info"]
                        ti["outcome"] = outcome
                        emoji = "✅" if ti["pnl"] > 0 else "❌"
                        log(f"  {emoji} {key}: {outcome} | SOLD @ {w['exit_price']:.2f} "
                            f"| PnL ${ti['pnl']:+.2f} | "
                            f"session {self._win_count}/{self._trade_count} wins")
                        self._save_window(w, outcome, ret)
                        w["completed"] = True
                        to_remove.append(key)
                    else:
                        # Traded but sell not yet filled — keep alive for resolution
                        log(f"  ⏳ {key}: window ended ({outcome}) — waiting for sell to fill at resolution")
                else:
                    log(f"  ⚠ {key}: ended but missing price data")
                    w["completed"] = True
                    to_remove.append(key)

                w["window_ended"] = True
                w["window_ended_at"] = time.time()

            # ─── Post-window: sell filled after window end ───
            if (w["window_ended"] and w["traded"]
                    and not w["completed"] and w.get("sell_filled")):
                ti = w["trade_info"]
                outcome = w.get("_outcome", "?")
                ret = w.get("_return", 0)
                ti["outcome"] = outcome
                emoji = "✅" if ti["pnl"] > 0 else "❌"
                log(f"  {emoji} {key}: {outcome} | SOLD @ {w['exit_price']:.2f} "
                    f"| PnL ${ti['pnl']:+.2f} (post-window) | "
                    f"session {self._win_count}/{self._trade_count} wins")
                self._save_window(w, outcome, ret)
                w["completed"] = True
                to_remove.append(key)

            # ─── Post-window timeout: sell didn't fill → loss ───
            if (w["window_ended"] and w["traded"]
                    and not w["completed"] and not w.get("sell_filled")
                    and w.get("window_ended_at")
                    and time.time() - w["window_ended_at"] > SELL_WAIT_AFTER_END):
                ti = w["trade_info"]
                outcome = w.get("_outcome", "?")
                ret = w.get("_return", 0)
                ti["outcome"] = outcome

                # Cancel the sell order
                if w.get("sell_placed") and w.get("sell_order_id"):
                    try:
                        client.cancel(w["sell_order_id"])
                        log(f"  🚫 Cancelled unfilled sell for {key} (timeout {SELL_WAIT_AFTER_END}s)")
                    except Exception:
                        pass

                # Check if our direction was actually correct
                direction_correct = (ti["side"] == outcome) if outcome in ("UP", "DOWN") else False

                if direction_correct:
                    # Direction correct but sell didn't fill at $0.99 — shares are
                    # still in wallet and redeemable via claimer.py at $1.00/share.
                    # Don't count as loss — mark as pending claim.
                    pnl_est = ti["shares"] * EXIT_PRICE - ti["cost"]  # estimated profit after claim
                    ti["exit_type"] = "pending_claim"
                    ti["won"] = True
                    ti["pnl"] = round(pnl_est, 4)

                    self._trade_count += 1
                    self._win_count += 1
                    self._session_won += ti["shares"] * EXIT_PRICE
                    self._session_cost += ti["cost"]

                    log(f"  ⏳ {key}: {outcome} | trade={ti['side']} @ {ti['entry_price']:.2f} "
                        f"| direction CORRECT, sell didn't fill → pending claim "
                        f"({ti['shares']:.1f}sh × $1.00 ≈ ${pnl_est:+.2f}) | "
                        f"session {self._win_count}/{self._trade_count} wins")
                else:
                    # Direction wrong — genuine loss
                    pnl = -ti["cost"]
                    ti["exit_type"] = "resolution"
                    ti["won"] = False
                    ti["pnl"] = round(pnl, 4)

                    self._trade_count += 1
                    self._session_cost += ti["cost"]

                    log(f"  ❌ {key}: {outcome} | trade={ti['side']} @ {ti['entry_price']:.2f} "
                        f"| LOST ${abs(pnl):.2f} (resolution timeout) | "
                        f"session {self._win_count}/{self._trade_count} wins")
                self._save_window(w, outcome, ret)
                w["completed"] = True
                to_remove.append(key)

        # Periodic reanalysis — run every REANALYSIS_INTERVAL seconds
        # instead of per-window, so we see a complete batch of signal data
        if time.time() - self._last_reanalysis >= REANALYSIS_INTERVAL:
            self._run_reanalysis()
            self._last_reanalysis = time.time()

        # Clean up
        with self._lock:
            for key in to_remove:
                self._windows.pop(key, None)

    def _execute_buy(self, w, token_id, side, buy_price, edge, our_prob, current_return):
        """Place a GTC limit buy and verify fill. Returns True if filled."""
        buy_price = round(buy_price, 2)
        est_shares = BET_AMOUNT / buy_price

        # Ensure we buy enough shares to meet Polymarket's minimum order size
        # (needed for both the buy and the subsequent exit sell)
        if est_shares < MIN_ORDER_SIZE:
            est_shares = MIN_ORDER_SIZE
            log(f"  📐 Bumped to {MIN_ORDER_SIZE}sh minimum (${est_shares * buy_price:.2f} cost)")

        fee_est = compute_taker_fee(est_shares, buy_price)

        log(f"  💰 Buying {side}: ~{est_shares:.0f}sh @ {buy_price:.2f} "
            f"(${BET_AMOUNT:.0f} + ~${fee_est:.2f} fee)")

        if DRY_RUN:
            log(f"  🧪 DRY RUN — skipping execution")
            w["trade_info"] = {
                "side": side,
                "token_id": token_id,
                "entry_price": buy_price,
                "shares": est_shares,
                "cost": BET_AMOUNT + fee_est,
                "edge": edge,
                "our_prob": our_prob,
                "return_at_entry": current_return,
                "dry_run": True,
                "exit_type": None,
                "outcome": None,
                "won": None,
                "pnl": None,
            }
            return True

        try:
            # Place GTC limit buy at ask price (will cross spread → taker fill)
            buy_order = OrderArgs(
                token_id=token_id,
                price=buy_price,
                size=est_shares,
                side=BUY,
            )
            signed_buy = client.create_order(buy_order)
            resp = client.post_order(signed_buy, OrderType.GTC)

            order_id = resp.get("orderID", "") if isinstance(resp, dict) else ""
            resp_status = resp.get("status", "") if isinstance(resp, dict) else ""

            if not resp_status or resp_status == "error":
                log(f"  ⚠ Buy rejected: {resp}")
                return False

            log(f"  📝 Order placed (id: {order_id[:8] if order_id else '?'})")

            # Wait for fill
            time.sleep(FILL_WAIT)

            # Check if we got shares
            actual = get_share_balance(token_id)

            # Always cancel the buy order to prevent additional partial fills
            try:
                if order_id:
                    client.cancel(order_id)
            except Exception:
                pass

            if actual is not None and actual >= MIN_ORDER_SIZE:
                # Compute cost analytically (balance diff is unreliable with concurrent positions)
                fee = compute_taker_fee(actual, buy_price)
                actual_cost = round(actual * buy_price + fee, 4)

                log(f"  ✅ Filled! {actual:.1f} shares, cost ${actual_cost:.2f} (fee ${fee:.2f})")

                w["trade_info"] = {
                    "side": side,
                    "token_id": token_id,
                    "entry_price": buy_price,
                    "shares": actual,
                    "cost": actual_cost,
                    "edge": edge,
                    "our_prob": our_prob,
                    "return_at_entry": current_return,
                    "dry_run": False,
                    "exit_type": None,   # 'sell' or 'resolution'
                    "outcome": None,
                    "won": None,
                    "pnl": None,
                }

                self._place_exit_sell(w)
                return True
            elif actual is not None and actual >= 1:
                # Partial fill too small to sell — log and skip (shares will resolve with market)
                log(f"  ⚠ Partial fill {actual:.1f}sh < {MIN_ORDER_SIZE} min — cannot trade, skipping")
                return False
            else:
                # Not filled at all
                log(f"  ⏳ Not filled after {FILL_WAIT}s — cancelled")
                return False

        except Exception as e:
            log(f"  ⚠ Buy error: {e}")
            return False

    def _place_exit_sell(self, w):
        """Place a GTC limit sell at EXIT_PRICE ($0.99). Fills when winning token → $1.00."""
        ti = w["trade_info"]
        if not ti or ti.get("dry_run"):
            return

        token_id = ti["token_id"]
        shares = ti["shares"]
        w["_last_sell_attempt"] = time.time()

        try:
            sell_order = OrderArgs(
                token_id=token_id,
                price=EXIT_PRICE,
                size=shares,
                side=SELL,
            )
            signed = client.create_order(sell_order)
            resp = client.post_order(signed, OrderType.GTC)

            order_id = resp.get("orderID", "") if isinstance(resp, dict) else ""
            status = resp.get("status", "") if isinstance(resp, dict) else ""

            if order_id and status != "error":
                w["sell_order_id"] = order_id
                w["sell_placed"] = True
                w["exit_price"] = EXIT_PRICE
                log(f"  📤 EXIT SELL: {shares:.1f}sh @ {EXIT_PRICE} "
                    f"(entry={ti['entry_price']:.2f}) "
                    f"order={order_id[:8]}")
            else:
                log(f"  ⚠ Exit sell failed (will retry): {resp}")
        except Exception as e:
            log(f"  ⚠ Exit sell error (will retry): {e}")

    def _check_sell_fills(self, w, key):
        """Check if exit sell has filled by looking at share balance. Returns True if filled."""
        ti = w["trade_info"]
        if not ti:
            return False

        shares = get_share_balance(ti["token_id"])
        if shares is not None and shares < 1:
            # Sell filled!
            w["sell_filled"] = True
            sell_price = w.get("exit_price", ti["entry_price"])
            sell_revenue = ti["shares"] * sell_price
            sell_fee = compute_taker_fee(ti["shares"], sell_price)
            pnl = sell_revenue - sell_fee - ti["cost"]

            ti["exit_type"] = "sell"
            ti["exit_price"] = sell_price
            ti["pnl"] = round(pnl, 4)
            ti["won"] = pnl > 0

            self._trade_count += 1
            if pnl > 0:
                self._win_count += 1
            self._session_won += sell_revenue - sell_fee
            self._session_cost += ti["cost"]

            log(f"  💰 SELL FILLED {key}: {ti['shares']:.1f}sh @ {sell_price:.2f} "
                f"→ PnL ${pnl:+.2f} (bought @ {ti['entry_price']:.2f})")
            return True
        return False

    def _save_window(self, w, outcome, ret):
        """Write completed window data to JSONL."""
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "market": w["question"],
            "crypto": w["crypto"],
            "interval": w["interval"],
            "window_start": w["start_utc"].isoformat(),
            "window_end": w["end_utc"].isoformat(),
            "open_price": w["open_price"],
            "close_price": w["close_price"],
            "outcome": outcome,
            "return_pct": round(ret * 100, 4),
            "signals": w["signals"],
            "traded": w["traded"],
            "trade": w["trade_info"],
        }
        with open(DATA_FILE, "a") as f:
            f.write(json.dumps(record) + "\n")

        # Also save trade to trade log if we traded
        if w["traded"] and w["trade_info"]:
            ti = w["trade_info"]
            trade_entry = {
                "time": datetime.now(timezone.utc).isoformat(),
                "window": w["key"],
                "crypto": w["crypto"],
                "interval": w["interval"],
                "side": ti["side"],
                "entry_price": ti["entry_price"],
                "shares": ti["shares"],
                "cost": ti["cost"],
                "edge": ti["edge"],
                "our_prob": ti["our_prob"],
                "return_at_entry": ti["return_at_entry"],
                "outcome": ti.get("outcome"),
                "won": ti.get("won"),
                "pnl": ti.get("pnl"),
                "exit_type": ti.get("exit_type"),
                "dry_run": ti.get("dry_run", False),
            }

            # Append to trades JSON
            trades = []
            if os.path.exists(TRADE_LOG):
                try:
                    with open(TRADE_LOG) as f:
                        trades = json.load(f)
                except Exception:
                    pass
            trades.append(trade_entry)
            with open(TRADE_LOG, "w") as f:
                json.dump(trades, f, indent=2)

    def _run_reanalysis(self):
        """Run full reanalysis: σ, edge, timing, Kelly. Called every window."""
        global MIN_EDGE, BET_AMOUNT

        # Recalibrate volatility and drift
        self.engine._recompute_vol()
        self.engine._recompute_drift()

        report = self.engine.reanalyze()
        if not report:
            return

        self._last_report = report
        log(f"  \n{'═' * 60}")
        log(f"  🔬 REANALYSIS ({report['n_signals']} signals, {report['n_windows']} windows)")

        # Print calibration
        if report.get("calibration"):
            log(f"  Calibration (predicted vs actual P(UP)):")
            for c in report["calibration"]:
                marker = "✅" if abs(c["gap"]) < 0.10 else "⚠️"
                log(f"    {marker} bin {c['bin']}: pred={c['predicted']:.2f} actual={c['actual']:.2f} "
                    f"gap={c['gap']:+.2f} (n={c['n']})")

        # Print edge analysis
        for threshold in [0.10, 0.15, 0.20, 0.25, 0.30]:
            key = f"edge_{threshold:.2f}"
            if key in report:
                d = report[key]
                be = d.get("breakeven_wr", 0.65)
                marker = "✅" if d.get("profitable") or d["win_rate"] >= be + 0.03 else "❌"
                log(f"    {marker} edge≥{threshold:.2f}: WR={d['win_rate']:.0%} vs BE={be:.0%} (n={d['n']})")

        # Print timing analysis
        if report.get("timing"):
            log(f"  Entry timing analysis:")
            for t in report["timing"]:
                marker = "✅" if t["win_rate"] >= 0.60 else "❌"
                log(f"    {marker} {t['bucket']}: WR={t['win_rate']:.0%} (n={t['n']})")

        # Auto-tune MIN_EDGE
        optimal = report.get("optimal_edge", MIN_EDGE)
        if AUTO_TUNE_EDGE and optimal != MIN_EDGE:
            old_edge = MIN_EDGE
            MIN_EDGE = optimal
            log(f"  🔧 AUTO-TUNE: MIN_EDGE {old_edge:.2f} → {MIN_EDGE:.2f}")
        else:
            log(f"  🔧 MIN_EDGE={MIN_EDGE:.2f} (optimal={optimal:.2f}, no change)")

        # Auto-tune BET_AMOUNT via Kelly criterion
        #   Bayesian WR + confidence scaling + bet smoothing + drawdown halt
        kelly = report.get("kelly")
        if kelly and AUTO_TUNE_EDGE:
            kelly_pct = kelly["kelly_bet_pct"]
            if kelly_pct > 0:
                balance = get_usdc_balance()
                if balance and balance > 0:
                    # Use session starting balance for drawdown detection
                    start_bal = self._start_balance or balance

                    # Drawdown halt: if balance < DRAWDOWN_HALT_PCT of start, lock to MIN_BET
                    drawdown_active = balance < start_bal * DRAWDOWN_HALT_PCT
                    if drawdown_active:
                        old_bet = BET_AMOUNT
                        BET_AMOUNT = MIN_BET
                        log(f"  🔧 AUTO-TUNE: BET_AMOUNT ${old_bet:.1f} → ${BET_AMOUNT:.1f} "
                            f"(DRAWDOWN HALT: ${balance:.0f} < {DRAWDOWN_HALT_PCT:.0%} of ${start_bal:.0f})")
                    else:
                        raw_bet = balance * kelly_pct
                        new_bet = round(max(MIN_BET, min(MAX_BET, raw_bet)), 1)

                        # Bet smoothing: cap INCREASES to BET_RAMP_FACTOR per cycle
                        # (decreases are uncapped — protect capital fast)
                        old_bet = BET_AMOUNT
                        if new_bet > old_bet * BET_RAMP_FACTOR:
                            new_bet = round(old_bet * BET_RAMP_FACTOR, 1)
                            new_bet = max(MIN_BET, min(MAX_BET, new_bet))

                        BET_AMOUNT = new_bet
                        conf = kelly.get('confidence', 1.0)
                        log(f"  🔧 AUTO-TUNE: BET_AMOUNT ${old_bet:.1f} → ${BET_AMOUNT:.1f} "
                            f"(Kelly={kelly['kelly_full']:.1%} × {KELLY_FRACTION} × conf={conf:.2f} "
                            f"= {kelly_pct:.1%} of ${balance:.0f})"
                            f"{' [ramped]' if new_bet < raw_bet else ''}")
                else:
                    log(f"  🔧 BET_AMOUNT=${BET_AMOUNT:.1f} (Kelly={kelly['kelly_full']:.1%}, balance unknown)")
            else:
                log(f"  🔧 BET_AMOUNT=${BET_AMOUNT:.1f} (Kelly=0 — edge not profitable enough to bet)")
        else:
            log(f"  🔧 BET_AMOUNT=${BET_AMOUNT:.1f} (not enough data for Kelly)")

        log(f"  {'═' * 60}\n")

    def get_dashboard(self):
        """Formatted dashboard string."""
        lines = []
        now = datetime.now(timezone.utc)

        with self._lock:
            windows = sorted(self._windows.values(), key=lambda w: w["key"])

        if not windows:
            lines.append("  (no active windows)")
        else:
            for w in windows:
                start = w["start_utc"]
                end = w["end_utc"]
                duration = w["duration"]
                elapsed = (now - start).total_seconds()
                pct = min(elapsed / duration, 1.0) if duration > 0 else 0
                remaining = max(0, (end - now).total_seconds())

                crypto = w["crypto"].upper()
                cl_price, _ = self.chainlink.get_price(w["crypto"])
                bn_price, _ = self.binance.get_price(w["crypto"])
                dash_price = cl_price if cl_price is not None else bn_price
                up_bid, up_ask, _ = self.poly.get_price(w["up_token"])

                # Price & return
                src_tag = "CL" if cl_price is not None else "BN"
                price_str = f"${dash_price:,.2f}({src_tag})" if dash_price else "?"
                open_str = f"${w['open_price']:,.2f}" if w["open_price"] else "?"

                ret_str = "?"
                direction = "⚪"
                p_up_str = ""
                edge_str = ""

                if w["open_price"] and dash_price:
                    ret = (dash_price - w["open_price"]) / w["open_price"]
                    direction = "🟢" if ret > 0 else ("🔴" if ret < 0 else "⚪")
                    ret_str = f"{ret*100:+.3f}%"

                    # Bayesian prob
                    p_up = self.engine.prob_up(ret, remaining)
                    p_up_str = f"P(UP)={p_up:.2f}"

                    # Edge vs Polymarket
                    if up_bid and up_ask and up_bid > 0 and up_ask > 0:
                        implied = (up_bid + up_ask) / 2.0
                        edge = p_up - implied
                        edge_str = f"edge={edge:+.2f}"

                traded_str = ""
                if w.get("sell_filled"):
                    traded_str = " 💰SOLD"
                elif w.get("window_ended") and w.get("sell_placed"):
                    wait = int(time.time() - w.get("window_ended_at", time.time()))
                    traded_str = f" ⏳RESOLVING({wait}s)"
                elif w.get("sell_placed"):
                    traded_str = f" 📤SELL@{w.get('exit_price', 0):.2f}"
                elif w["traded"]:
                    traded_str = " 🎯TRADED(no bid)"

                bar_len = 20
                filled = int(pct * bar_len)
                bar = "█" * filled + "░" * (bar_len - filled)

                lines.append(
                    f"  {crypto:4s} {w['interval']:2d}m │{bar}│ {pct:5.1%} {remaining:3.0f}s │ "
                    f"{direction} {ret_str:>8s} │ {p_up_str:>10s} {edge_str:>10s} │ "
                    f"now={price_str} open={open_str}{traded_str}"
                )

                # Show last signal
                if w["signals"]:
                    s = w["signals"][-1]
                    eu = s.get('edge_up', 0)
                    ed = s.get('edge_down', 0)
                    lines.append(
                        f"         └ @{s['pct']:.0%}: ret={s['return']:+.3f}% "
                        f"P(UP)={s['p_up']:.3f} impl={s['implied_up']:.3f} "
                        f"eUP={eu:+.3f} eDN={ed:+.3f}"
                    )

        # ─── Session summary ───
        elapsed_min = (time.time() - self._session_start) / 60
        lines.append(f"\n{'─' * 90}")

        # Balance
        bal = get_usdc_balance()
        bal_str = f"${bal:.2f}" if bal else "?"
        start_str = f"${self._start_balance:.2f}" if self._start_balance else "?"
        if bal and self._start_balance:
            delta = bal - self._start_balance
            lines.append(f"  💵 Balance: {bal_str} (start: {start_str}, Δ{delta:+.2f})")
        else:
            lines.append(f"  💵 Balance: {bal_str}")

        # Trade stats
        wr = self._win_count / self._trade_count * 100 if self._trade_count > 0 else 0
        net_pnl = self._session_won - self._session_cost
        lines.append(
            f"  📈 Trades: {self._trade_count} | Wins: {self._win_count} ({wr:.0f}%) | "
            f"Net PnL: ${net_pnl:+.2f} | Runtime: {elapsed_min:.1f}min"
        )

        # Model info
        drift = self.engine.drift_per_sec
        drift_dir = "↑" if drift > 0 else ("↓" if drift < 0 else "→")
        drift_5m = drift * 300 * 100  # % per 5min
        up_rate, bias_n = self.engine.recent_side_bias()
        lines.append(
            f"  🧠 Model: σ/√s={self.engine.vol_per_sec:.6f} "
            f"(~{self.engine.vol_per_sec * math.sqrt(300) * 100:.3f}%/5min) | "
            f"μ={drift_dir}{abs(drift_5m):.4f}%/5min | "
            f"Bias: {up_rate:.0%}UP/{1-up_rate:.0%}DN(n={bias_n}) | "
            f"Data: {self.engine.n_windows}w {self.engine.signal_count}s | "
            f"MIN_EDGE={MIN_EDGE}"
        )

        # Last reanalysis summary
        if self._last_report:
            r = self._last_report
            opt = r.get('optimal_edge', MIN_EDGE)
            # Find entry for current MIN_EDGE threshold
            key = f"edge_{MIN_EDGE:.2f}"
            wr_str = ""
            if key in r:
                wr_str = f"WR={r[key]['win_rate']:.0%}(n={r[key]['n']})"
            lines.append(
                f"  🔬 Last reanalysis: optimal_edge={opt:.2f} {wr_str}"
            )

        return "\n".join(lines)


# =========================================================================
# MAIN
# =========================================================================
def main():
    mode = "DRY RUN" if DRY_RUN else "LIVE"
    log(f"═══ Sniper Bot ═══ [{mode}]")
    log(f"Strategy: Bayesian prediction → buy underpriced token → sell at $0.99")
    log(f"Price source: Chainlink (via Polymarket RTDS) — matches resolution source")
    log(f"Params: BET=${BET_AMOUNT} MIN_EDGE={MIN_EDGE} PRICE_MIN={ENTRY_PRICE_MIN} MIN_ORDER={MIN_ORDER_SIZE}sh")
    log(f"Tracking: {', '.join(c.upper() for c in CRYPTOS)}")
    log("")

    # Start feeds
    chainlink = ChainlinkFeed()
    chainlink.start()

    binance = BinanceFeed()
    binance.start()

    poly = PolymarketFeed()
    poly.start()

    engine = BayesianEngine()
    sniper = Sniper(chainlink, binance, poly, engine)

    # Wait for WS connections
    log("Waiting for WebSocket connections...")
    for _ in range(30):
        if (chainlink.connected or binance.connected) and poly.connected:
            break
        time.sleep(1)

    if not chainlink.connected:
        log("⚠ Chainlink RTDS not connected after 30s (using Binance fallback)")
    if not binance.connected:
        log("⚠ Binance WS not connected after 30s")
    if not poly.connected:
        log("⚠ Polymarket WS not connected after 30s")

    log("")
    last_discovery = 0

    while True:
        try:
            now = time.time()

            # Discover markets periodically
            if now - last_discovery > MARKET_POLL_INTERVAL:
                markets = discover_markets()
                if markets:
                    sniper.update_markets(markets)
                last_discovery = now

            # Main tick: signals + trades + finalization
            sniper.tick()

            # Dashboard
            now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
            cl_status = "🟢" if chainlink.connected else "🔴"
            bn_status = "🟢" if binance.connected else "🔴"
            pm_status = "🟢" if poly.connected else "🔴"
            dashboard = sniper.get_dashboard()

            print(f"\033[2J\033[H", end="")
            print(f"═══ Sniper Bot [{mode}] ═══  {now_str} UTC  │  Chainlink {cl_status}  Binance {bn_status}  Polymarket {pm_status}")
            print(f"{'─' * 90}")
            print(dashboard)
            print(f"\n{'─' * 90}")
            print(f"Data: {DATA_FILE} | Trades: {TRADE_LOG} | Log: {DECISION_LOG}")
            print(f"Press Ctrl+C to stop")

            time.sleep(TICK_INTERVAL)

        except KeyboardInterrupt:
            log(f"\nStopping — cancelling all open orders...")
            # Cancel all open sell orders so shares can resolve naturally
            with sniper._lock:
                for key, w in sniper._windows.items():
                    if w.get("sell_placed") and w.get("sell_order_id") and not w.get("sell_filled"):
                        try:
                            client.cancel(w["sell_order_id"])
                            log(f"  🚫 Cancelled sell for {key}")
                        except Exception:
                            pass
            # Also cancel any other open orders on the book
            try:
                client.cancel_all()
                log(f"  🚫 Cancelled all remaining open orders")
            except Exception:
                pass
            bal = get_usdc_balance()
            if bal and sniper._start_balance:
                log(f"Final balance: ${bal:.2f} (Δ{bal - sniper._start_balance:+.2f})")
            log(f"Trades: {sniper._trade_count} | Wins: {sniper._win_count}")
            break
        except Exception as e:
            log(f"⚠ Main loop error: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5)


if __name__ == "__main__":
    main()
