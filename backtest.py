#!/usr/bin/env python3
"""
Backtest: Evaluate flow signal accuracy against resolved markets.

Critical: filters trades by timestamp to simulate what the bot would
ACTUALLY see at decision time — not the full post-resolution flow.

Usage:
    python backtest.py [HOURS] [--cutoff-pct 50]

    HOURS        — look back N hours (default: 6)
    --cutoff-pct — only show results for this specific cutoff %
                   (default: grid at 30, 40, 50, 60, 70, 80%)
"""

import sys
import json
import time
import requests
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"

CRYPTOS = ["btc", "eth", "sol", "xrp"]
INTERVALS = [5, 15]
MAX_WORKERS = 12

# Parse args
HOURS = 6
CUTOFF_PCT = None  # None = show grid

for i, arg in enumerate(sys.argv[1:], 1):
    if arg.isdigit():
        HOURS = int(arg)
    elif arg == "--cutoff-pct" and i < len(sys.argv) - 1:
        CUTOFF_PCT = int(sys.argv[i + 1])
    elif arg == "--cryptos" and i < len(sys.argv) - 1:
        CRYPTOS = sys.argv[i + 1].lower().split(",")


# ---------------------------------------------------------------------------
# Step 1: Discover resolved markets
# ---------------------------------------------------------------------------
def discover_slugs():
    """Generate slugs for fully-elapsed market windows."""
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=HOURS)
    slugs = []

    for interval in INTERVALS:
        aligned_min = (start.minute // interval) * interval
        cursor = start.replace(minute=aligned_min, second=0, microsecond=0)

        while cursor < now - timedelta(minutes=interval):
            epoch = int(cursor.timestamp())
            for crypto in CRYPTOS:
                slug = f"{crypto}-updown-{interval}m-{epoch}"
                slugs.append({
                    "slug": slug,
                    "crypto": crypto.upper(),
                    "interval": interval,
                    "epoch": epoch,
                })
            cursor += timedelta(minutes=interval)

    return slugs


def fetch_market_info(slug_info):
    """Fetch market data from GAMMA API. Returns market dict or None."""
    slug = slug_info["slug"]
    try:
        r = requests.get(f"{GAMMA_API}/events/slug/{slug}", timeout=15)
        if r.status_code != 200:
            return None
        event = r.json()
        markets = event.get("markets", [])
        if not markets:
            return None

        market = markets[0]
        tokens = json.loads(market.get("clobTokenIds", "[]"))
        if len(tokens) != 2:
            return None

        # Determine outcome
        winner = None
        outcome_prices = market.get("outcomePrices", "")
        if isinstance(outcome_prices, str) and outcome_prices:
            try:
                prices = json.loads(outcome_prices)
                up_price = float(prices[0])
                down_price = float(prices[1])
                if up_price > 0.9:
                    winner = "UP"
                elif down_price > 0.9:
                    winner = "DOWN"
            except (json.JSONDecodeError, IndexError, ValueError):
                pass

        if not winner:
            outcome = market.get("outcome")
            if outcome in ("Yes", "yes"):
                winner = "UP"
            elif outcome in ("No", "no"):
                winner = "DOWN"

        if not winner:
            return None

        return {
            "slug": slug,
            "crypto": slug_info["crypto"],
            "interval": slug_info["interval"],
            "epoch": slug_info["epoch"],
            "condition_id": market.get("conditionId", ""),
            "question": market.get("question", ""),
            "tokens": tokens,
            "outcome": winner,
        }
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Step 2: Fetch ALL trades with pagination — store raw
# ---------------------------------------------------------------------------
def fetch_all_trades(condition_id):
    """Fetch all trades for a market using ?market=conditionId.
    This is the only endpoint that correctly filters trades to the market.
    Resolved markets return full data (3000+ for BTC 5m)."""
    all_trades = []
    page_size = 200
    max_pages = 20  # 20 * 200 = 4000 max

    offset = 0
    for _ in range(max_pages):
        try:
            r = requests.get(
                f"{DATA_API}/trades",
                params={"market": condition_id, "limit": page_size, "offset": offset},
                timeout=15,
            )
            if r.status_code != 200:
                break
            batch = r.json()
            if not batch:
                break
            all_trades.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
        except Exception:
            break

    return all_trades


def fetch_market_with_trades(market_info):
    """Fetch raw trades for a resolved market."""
    trades = fetch_all_trades(market_info["condition_id"])
    market_info["raw_trades"] = trades
    market_info["total_trades"] = len(trades)
    return market_info


# ---------------------------------------------------------------------------
# Step 3: Compute flow with time filtering
# ---------------------------------------------------------------------------
def compute_flow_at_cutoff(raw_trades, up_token, epoch, interval_min, cutoff_pct):
    """
    Compute net flow using only trades from the first cutoff_pct% of the window.

    A 5-min market at cutoff 50% → only trades from first 2.5 minutes.
    This simulates what the bot sees when it scans mid-way through.
    """
    window_seconds = interval_min * 60
    cutoff_ts = epoch + (window_seconds * cutoff_pct / 100)

    result = {
        "up_buys": 0.0, "up_sells": 0.0,
        "down_buys": 0.0, "down_sells": 0.0,
        "up_net": 0.0, "down_net": 0.0,
        "trade_count": 0,
    }

    for t in raw_trades:
        ts = t.get("timestamp")
        if ts is None:
            continue
        ts = int(ts)

        # Only count trades within market window AND before cutoff
        if ts < epoch or ts > cutoff_ts:
            continue

        asset = t.get("asset", "")
        side = t.get("side", "")
        size = float(t.get("size", 0))
        price = float(t.get("price", 0))
        cost = size * price
        is_up = (asset == up_token)

        result["trade_count"] += 1

        if is_up:
            if side == "BUY":
                result["up_buys"] += cost
            else:
                result["up_sells"] += cost
        else:
            if side == "BUY":
                result["down_buys"] += cost
            else:
                result["down_sells"] += cost

    result["up_net"] = result["up_buys"] - result["up_sells"]
    result["down_net"] = result["down_buys"] - result["down_sells"]
    return result


# ---------------------------------------------------------------------------
# Step 4: Signal evaluation
# ---------------------------------------------------------------------------
def evaluate_signal(flow, min_flow, min_ratio):
    """Returns "UP", "DOWN", or None."""
    up_net = flow["up_net"]
    down_net = flow["down_net"]

    if up_net >= min_flow:
        if up_net / max(abs(down_net), 1) >= min_ratio:
            return "UP"

    if down_net >= min_flow:
        if down_net / max(abs(up_net), 1) >= min_ratio:
            return "DOWN"

    return None


def run_threshold_analysis(data, cutoff_pct, min_flow_values, min_ratio_values):
    """Compute flow at given cutoff, then grid-search thresholds."""
    # Pre-compute flows
    flows = {}
    for m in data:
        flows[m["slug"]] = compute_flow_at_cutoff(
            m["raw_trades"], m["tokens"][0], m["epoch"], m["interval"], cutoff_pct
        )

    has_trades = sum(1 for f in flows.values() if f["trade_count"] > 0)
    total = sum(f["trade_count"] for f in flows.values())

    print(f"\n{'='*90}")
    print(f"  CUTOFF: first {cutoff_pct}% of window  "
          f"(5m → {cutoff_pct * 5 / 100:.1f}min, 15m → {cutoff_pct * 15 / 100:.1f}min)")
    print(f"  {has_trades}/{len(data)} markets have trades, {total:,} trades included")
    print(f"{'='*90}")

    print(f"\n  {'MIN_FLOW':>10} {'MIN_RATIO':>10} {'Signals':>8} {'Wins':>6} {'Losses':>7} "
          f"{'WR%':>6} {'UP_sig':>7} {'DN_sig':>7} {'UP_W':>5} {'DN_W':>5}")
    print(f"  {'-'*87}")

    all_results = []
    for mf in min_flow_values:
        for mr in min_ratio_values:
            wins = losses = up_sig = dn_sig = up_w = dn_w = 0

            for m in data:
                signal = evaluate_signal(flows[m["slug"]], mf, mr)
                if signal is None:
                    continue
                if signal == "UP":
                    up_sig += 1
                else:
                    dn_sig += 1
                if signal == m["outcome"]:
                    wins += 1
                    if signal == "UP":
                        up_w += 1
                    else:
                        dn_w += 1
                else:
                    losses += 1

            tot = wins + losses
            if tot == 0:
                continue
            wr = wins / tot * 100

            print(f"  ${mf:>8,.0f} {mr:>9.1f}x {tot:>7} {wins:>6} {losses:>6} "
                  f"{wr:>5.1f}% {up_sig:>7} {dn_sig:>7} {up_w:>5} {dn_w:>5}")
            all_results.append((mf, mr, tot, wins, losses, wr))

    # Best params
    best20 = sorted([r for r in all_results if r[2] >= 20], key=lambda x: (-x[5], -x[2]))
    best10 = sorted([r for r in all_results if r[2] >= 10], key=lambda x: (-x[5], -x[2]))

    if best20:
        b = best20[0]
        print(f"\n  ✅ Best (≥20 sig): ${b[0]:,.0f} / {b[1]:.1f}x → {b[5]:.1f}% WR "
              f"({b[3]}W/{b[4]}L on {b[2]})")
    if best10 and (not best20 or best10[0][:2] != best20[0][:2]):
        b = best10[0]
        print(f"  ✅ Best (≥10 sig): ${b[0]:,.0f} / {b[1]:.1f}x → {b[5]:.1f}% WR "
              f"({b[3]}W/{b[4]}L on {b[2]})")

    return all_results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print(f"🔍 Backtest: {HOURS}h lookback | intervals: "
          f"{'/'.join(str(i) for i in INTERVALS)}m | cryptos: {', '.join(CRYPTOS)}")
    print(f"   {MAX_WORKERS} threads\n")

    # Discover slugs
    slugs = discover_slugs()
    print(f"📋 {len(slugs)} slugs to check")

    # Fetch market info
    print(f"📡 Fetching market info...", end="", flush=True)
    resolved = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(fetch_market_info, s): s for s in slugs}
        done = 0
        for f in as_completed(futs):
            done += 1
            if done % 50 == 0:
                print(f"\r📡 Fetching market info... {done}/{len(slugs)}", end="", flush=True)
            r = f.result()
            if r:
                resolved.append(r)
    print(f"\r📡 {len(resolved)} resolved markets in {time.time()-t0:.1f}s              ")

    if not resolved:
        print("❌ No resolved markets. Try a longer window (e.g., python backtest.py 24)")
        return

    # Fetch raw trades
    print(f"📊 Fetching trades...", end="", flush=True)
    data = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(fetch_market_with_trades, m): m for m in resolved}
        done = 0
        for f in as_completed(futs):
            done += 1
            if done % 20 == 0:
                print(f"\r📊 Fetching trades... {done}/{len(resolved)}", end="", flush=True)
            r = f.result()
            if r and r["total_trades"] > 0:
                data.append(r)
    total_trades = sum(m["total_trades"] for m in data)
    print(f"\r📊 {len(data)} markets, {total_trades:,} trades in {time.time()-t0:.1f}s              ")

    # Outcome distribution
    up = sum(1 for m in data if m["outcome"] == "UP")
    dn = sum(1 for m in data if m["outcome"] == "DOWN")
    print(f"\n📈 Outcomes: UP={up} ({up/len(data)*100:.0f}%) DOWN={dn} ({dn/len(data)*100:.0f}%)")

    # Per-crypto breakdown
    print(f"\n{'='*60}")
    print("BREAKDOWN BY CRYPTO & INTERVAL")
    print(f"{'='*60}")
    for crypto in sorted(set(m["crypto"] for m in data)):
        for interval in INTERVALS:
            subset = [m for m in data if m["crypto"] == crypto and m["interval"] == interval]
            if not subset:
                continue
            u = sum(1 for m in subset if m["outcome"] == "UP")
            d = sum(1 for m in subset if m["outcome"] == "DOWN")
            avg_t = sum(m["total_trades"] for m in subset) / len(subset)
            print(f"  {crypto} {interval}m — {len(subset)} mkts (UP:{u} DOWN:{d}), "
                  f"avg {avg_t:.0f} trades")

    # Threshold analysis
    min_flow_values = [100, 200, 500, 1000, 1500, 2000, 3000, 5000]
    min_ratio_values = [1.5, 2.0, 3.0, 5.0, 7.0, 10.0]

    if CUTOFF_PCT is not None:
        run_threshold_analysis(data, CUTOFF_PCT, min_flow_values, min_ratio_values)
    else:
        for pct in [30, 40, 50, 60, 70, 80]:
            run_threshold_analysis(data, pct, min_flow_values, min_ratio_values)

    print(f"\n{'='*60}")
    print("INTERPRETATION")
    print(f"{'='*60}")
    print(f"  Cutoff 30-40%  = bot's FIRST scan of a new market")
    print(f"  Cutoff 50-60%  = typical mid-window decision (most realistic)")
    print(f"  Cutoff 70-80%  = late entry (more data but less time to profit)")
    print(f"")
    print(f"  WR ≈ 50% everywhere  → flow is NOT predictive")
    print(f"  WR > 55% at cutoff 40-60% → genuine signal worth trading")
    print(f"  WR climbs with cutoff → signal only works with hindsight (bad)")


if __name__ == "__main__":
    main()
