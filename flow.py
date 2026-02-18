#!/usr/bin/env python3
"""
Flow Bot: Follow the money, not specific wallets.

Strategy:
  For each 5-min and 15-min crypto market, track total shares per side
  over time. When a large, lopsided surge of buying appears on one side,
  follow that money.

  This avoids the stale-wallet problem: we don't care WHO is buying,
  only that disproportionate money is flowing in one direction.

Signals (all must be true to trigger):
  1. Net new shares on chosen side exceed MIN_FLOW threshold
  2. Flow ratio (chosen / other) exceeds MIN_RATIO
  3. The flow appeared recently (within FLOW_LOOKBACK seconds)
  4. We don't already hold a position on this market

Exit:
  - Limit sell at 0.99 (take profit)
  - Copy-sell if the flow reverses (big money exits)
"""

import os
import sys
import time
import math
import requests
import json
import threading
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, OpenOrderParams, BalanceAllowanceParams, AssetType
from py_clob_client.order_builder.constants import BUY, SELL
from py_clob_client import MarketOrderArgs
from dotenv import load_dotenv

load_dotenv()
private_key = os.getenv("PRIVATE_KEY")
founder_address = os.getenv("FUNDER_ADDRESS")

# Configuration
HOST = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"
CHAIN_ID = 137
FUNDER_ADDRESS = founder_address

# =========================================================================
# STRATEGY PARAMETERS
# =========================================================================
BUY_AMOUNT = 20            # Base bet in $
MIN_FLOW = 500             # Minimum new shares on chosen side to trigger
MIN_RATIO = 2.0            # Flow on chosen side must be Nx the other side
POLL_INTERVAL = 1          # Seconds between scans
SELL_PRICE = 0.99          # Limit sell price (take profit)
CRYPTOS = ["btc", "eth", "sol", "xrp"]

# Scaling: bet more when the flow is stronger
# bet = BUY_AMOUNT * min(flow / MIN_FLOW, MAX_BET_MULTIPLIER)
MAX_BET_MULTIPLIER = 3.0

# Log files
DECISION_LOG = "flow_log.txt"
TRADE_LOG = "flow_trades.json"


def log(message):
    timestamp = datetime.now(timezone.utc).isoformat()
    with open(DECISION_LOG, "a") as f:
        f.write(f"[{timestamp}] {message}\n")
    print(message)


# Initialize CLOB client
client = ClobClient(
    host=HOST,
    key=private_key,
    chain_id=CHAIN_ID,
    signature_type=1,
    funder=FUNDER_ADDRESS
)
client.set_api_creds(client.create_or_derive_api_creds())


# =========================================================================
# API HELPERS (same as strategy.py)
# =========================================================================

def get_current_crypto_markets():
    """Fetch current 5-min and 15-minute crypto updown markets."""
    markets = []
    now = datetime.now(timezone.utc)

    for interval in [5, 15]:
        aligned = (now.minute // interval) * interval
        current_window = now.replace(minute=aligned, second=0, microsecond=0)
        window_start = current_window
        elapsed = (now - window_start).total_seconds() / 60
        epoch = int(window_start.timestamp())

        for crypto in CRYPTOS:
            slug = f"{crypto}-updown-{interval}m-{epoch}"
            try:
                response = requests.get(f"{GAMMA_API}/events/slug/{slug}", timeout=10)
                if response.status_code == 200:
                    event = response.json()
                    for market in event.get("markets", []):
                        if not market.get("closed"):
                            market["_elapsed_min"] = elapsed
                            market["_crypto"] = crypto.upper()
                            market["_slug"] = slug
                            market["_interval"] = interval
                            market["_epoch"] = epoch
                            markets.append(market)
            except Exception:
                pass
    return markets


def get_market_holders(condition_id):
    """Get holders for a market. Returns {0: [(wallet, shares), ...], 1: [...]}"""
    try:
        r = requests.get(
            f"{DATA_API}/holders",
            params={"market": condition_id, "limit": 50, "minBalance": 50},
            timeout=15
        )
        if r.status_code != 200:
            return {0: [], 1: []}
        data = r.json()
    except Exception:
        return {0: [], 1: []}

    result = {0: [], 1: []}
    for token_group in data:
        for h in token_group.get("holders", []):
            wallet = h.get("proxyWallet", "").lower()
            amount = float(h.get("amount", 0))
            outcome = h.get("outcomeIndex", 0)
            if wallet and amount > 0:
                result[outcome].append((wallet, amount))
    return result


def get_side_totals(condition_id):
    """Get total shares on each side. Returns (up_total, down_total)."""
    holders = get_market_holders(condition_id)
    up_total = sum(shares for _, shares in holders.get(0, []))
    down_total = sum(shares for _, shares in holders.get(1, []))
    return up_total, down_total


def get_price(token_id, side=BUY):
    try:
        data = client.get_price(token_id, side=side)
        return float(data.get("price", 0))
    except Exception:
        return 0.0


def get_actual_share_balance(token_id):
    """Refresh and read actual on-chain share balance for a token."""
    try:
        client.update_balance_allowance(
            params=BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
                signature_type=1
            )
        )
        ba = client.get_balance_allowance(
            params=BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
                signature_type=1
            )
        )
        raw_balance = float(ba.get("balance", 0))
        return raw_balance / 1e6
    except Exception as e:
        log(f"  ⚠ Failed to get balance: {e}")
        return None


def cancel_all_orders_for_token(token_id):
    """Cancel any open orders for a token."""
    try:
        orders = client.get_orders(OpenOrderParams())
        for order in orders:
            if order.get("asset_id") == token_id:
                client.cancel(order.get("id"))
    except Exception:
        pass


def get_existing_orders():
    try:
        return client.get_orders(OpenOrderParams())
    except Exception:
        return []


# =========================================================================
# FLOW TRACKING
# =========================================================================

# Track snapshots per market: {condition_id: {"first": (up, down), "prev": (up, down), "epoch": int}}
flow_state = {}


def compute_flow(condition_id, epoch):
    """
    Compare current side totals to the first snapshot for this market window.
    Returns (up_delta, down_delta) — net new shares since first seen.
    """
    up_now, down_now = get_side_totals(condition_id)

    key = f"{condition_id}:{epoch}"
    if key not in flow_state:
        # First time seeing this market window — record baseline
        flow_state[key] = {
            "first_up": up_now,
            "first_down": down_now,
            "prev_up": up_now,
            "prev_down": down_now,
        }
        return 0.0, 0.0, up_now, down_now

    state = flow_state[key]
    up_delta = up_now - state["first_up"]
    down_delta = down_now - state["first_down"]

    # Update prev for next cycle
    state["prev_up"] = up_now
    state["prev_down"] = down_now

    return up_delta, down_delta, up_now, down_now


def cleanup_flow_state(active_epochs):
    """Remove flow state for expired market windows."""
    expired = [k for k in flow_state if k.split(":")[-1] not in active_epochs]
    for k in expired:
        del flow_state[k]


# =========================================================================
# MAIN BOT
# =========================================================================

def run_flow():
    """
    Flow strategy:
    1. Scan current markets, track total shares per side over time
    2. When one side gets a disproportionate surge of new shares, buy it
    3. Place limit sell at 0.99
    4. If flow reverses (big money leaves), market sell
    """
    positions = {}  # token_id -> position info

    log(f"\n{'='*60}")
    log(f"Flow bot started")
    log(f"Min flow: {MIN_FLOW} shares, Min ratio: {MIN_RATIO}x")
    log(f"Buy amount: ${BUY_AMOUNT}, Cryptos: {', '.join(CRYPTOS)}")
    log(f"{'='*60}\n")

    while True:
        try:
            os.system('cls' if os.name == 'nt' else 'clear')
            print("💰 Tracking money flow...", flush=True)

            markets = get_current_crypto_markets()
            existing_orders = get_existing_orders()

            # Build set of tokens we already hold
            known_tokens = set(positions.keys())
            for order in existing_orders:
                tid = order.get("asset_id")
                if tid:
                    known_tokens.add(tid)

            # Track active epochs for cleanup
            active_epochs = set()

            # =================================================================
            # STEP 1: Detect flow signals and enter positions
            # =================================================================
            for market in markets:
                question = market.get("question", "N/A")
                crypto = market.get("_crypto", "")
                interval = market.get("_interval", 15)
                elapsed = market.get("_elapsed_min", 0)
                condition_id = market.get("conditionId", "")
                slug = market.get("_slug", "")
                epoch = str(market.get("_epoch", 0))
                tokens = json.loads(market.get("clobTokenIds", "[]"))

                if len(tokens) != 2:
                    continue

                active_epochs.add(epoch)

                # Skip if we already have a position on this market
                if any(t in known_tokens for t in tokens):
                    continue

                # Compute flow since first snapshot
                up_delta, down_delta, up_total, down_total = compute_flow(condition_id, epoch)

                # Log flow status
                if up_delta != 0 or down_delta != 0:
                    log(f"  {crypto} {interval}m: UP Δ{up_delta:+,.0f} ({up_total:,.0f}) | DOWN Δ{down_delta:+,.0f} ({down_total:,.0f})")

                # Check for entry signal
                # Need: one side has MIN_FLOW new shares AND is MIN_RATIO x the other
                if up_delta >= MIN_FLOW and (down_delta <= 0 or up_delta / max(down_delta, 1) >= MIN_RATIO):
                    chosen_idx = 0
                    chosen_side = "UP"
                    chosen_flow = up_delta
                    other_flow = down_delta
                elif down_delta >= MIN_FLOW and (up_delta <= 0 or down_delta / max(up_delta, 1) >= MIN_RATIO):
                    chosen_idx = 1
                    chosen_side = "DOWN"
                    chosen_flow = down_delta
                    other_flow = up_delta
                else:
                    continue  # No signal

                chosen_token = tokens[chosen_idx]
                chosen_price = get_price(chosen_token, side=BUY)

                if chosen_price <= 0 or chosen_price >= 1:
                    continue

                # Scale bet by flow strength
                flow_strength = chosen_flow / MIN_FLOW
                bet_multiplier = min(flow_strength, MAX_BET_MULTIPLIER)
                scaled_amount = round(BUY_AMOUNT * bet_multiplier, 2)

                ratio = chosen_flow / max(abs(other_flow), 1)

                log(f"\n💰 FLOW SIGNAL: {question}")
                log(f"  {chosen_side} flow: {chosen_flow:+,.0f} shares (ratio {ratio:.1f}x)")
                log(f"  Price: {chosen_price:.2f}, Bet: ${scaled_amount:.0f} ({bet_multiplier:.1f}x)")

                # Place market buy
                try:
                    mo = MarketOrderArgs(
                        token_id=chosen_token,
                        amount=scaled_amount,
                        side=BUY,
                        order_type=OrderType.FAK
                    )
                    signed = client.create_market_order(mo)
                    resp = client.post_order(signed, OrderType.FAK)

                    actual_shares = 0
                    if isinstance(resp, dict):
                        taking = resp.get("takingAmount", "0")
                        actual_shares = float(taking) if taking else 0
                    if actual_shares == 0:
                        actual_shares = scaled_amount / chosen_price if chosen_price > 0 else 0

                    positions[chosen_token] = {
                        "market": question,
                        "side": chosen_side,
                        "buy_price": chosen_price,
                        "size": actual_shares,
                        "condition_id": condition_id,
                        "outcome_idx": chosen_idx,
                        "slug": slug,
                        "epoch": epoch,
                        "cost": scaled_amount,
                        "entry_flow": chosen_flow,
                    }

                    log(f"  ✓ Bought {actual_shares:.0f} shares of {chosen_side} at {chosen_price:.2f}")

                    # Place limit sell at SELL_PRICE in background
                    if actual_shares > 0:
                        def place_limit_sell(token, shares, side_s):
                            time.sleep(10)
                            try:
                                actual_balance = get_actual_share_balance(token)
                                if actual_balance and actual_balance > 0.1:
                                    sz = math.floor(actual_balance * 100) / 100
                                else:
                                    sz = math.floor(shares * 100) / 100
                                sell_order = OrderArgs(
                                    token_id=token,
                                    price=SELL_PRICE,
                                    size=sz,
                                    side=SELL
                                )
                                signed_sell = client.create_order(sell_order)
                                client.post_order(signed_sell, OrderType.GTC)
                                log(f"  ✓ Limit sell placed: {sz:.0f} shares of {side_s} at {SELL_PRICE}")
                            except Exception as e:
                                log(f"  ⚠ Limit sell failed for {side_s}: {e}")
                        threading.Thread(
                            target=place_limit_sell,
                            args=(chosen_token, actual_shares, chosen_side),
                            daemon=True
                        ).start()

                    # Log trade
                    record = {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "action": "BUY",
                        "market": question,
                        "side": chosen_side,
                        "token": chosen_token,
                        "price": chosen_price,
                        "amount": scaled_amount,
                        "flow": chosen_flow,
                        "ratio": round(ratio, 1),
                        "response": str(resp)
                    }
                    with open(TRADE_LOG, "a") as f:
                        f.write(json.dumps(record) + "\n")

                except Exception as e:
                    log(f"  ✗ Buy failed: {e}")

            # =================================================================
            # STEP 2: Monitor positions — flow reversal + expiry
            # =================================================================
            tokens_to_remove = []
            current_market_tokens = set()
            for market in markets:
                for t in json.loads(market.get("clobTokenIds", "[]")):
                    current_market_tokens.add(t)

            for token_id, pos in positions.items():
                # Check if market expired
                if token_id not in current_market_tokens:
                    log(f"\n📋 Market expired: {pos['market']} {pos['side']} (entry at {pos['buy_price']:.2f})")
                    tokens_to_remove.append(token_id)
                    continue

                # Still active — check for flow reversal
                # If the shares on our side have dropped significantly, money is leaving
                up_now, down_now = get_side_totals(pos["condition_id"])
                key = f"{pos['condition_id']}:{pos['epoch']}"
                state = flow_state.get(key)
                if not state:
                    continue

                our_total = up_now if pos["outcome_idx"] == 0 else down_now
                our_first = state["first_up"] if pos["outcome_idx"] == 0 else state["first_down"]
                our_delta_now = our_total - our_first

                # Flow reversal: the delta that triggered us has evaporated
                # If current delta dropped below 30% of what triggered our entry, exit
                if pos["entry_flow"] > 0 and our_delta_now < pos["entry_flow"] * 0.3:
                    log(f"\n🔄 FLOW REVERSAL: {pos['market']} {pos['side']}")
                    log(f"  Entry flow was {pos['entry_flow']:+,.0f}, now {our_delta_now:+,.0f}")

                    try:
                        cancel_all_orders_for_token(token_id)
                        time.sleep(1)
                        actual_balance = get_actual_share_balance(token_id)
                        if actual_balance and actual_balance > 0.1:
                            sell_shares = math.floor(actual_balance * 100) / 100
                        else:
                            sell_shares = math.floor(pos["size"] * 100) / 100

                        if sell_shares > 0:
                            current_price = get_price(token_id, side=SELL)
                            mo = MarketOrderArgs(
                                token_id=token_id,
                                amount=sell_shares,
                                side=SELL,
                                order_type=OrderType.FAK
                            )
                            signed = client.create_market_order(mo)
                            resp = client.post_order(signed, OrderType.FAK)

                            sell_proceeds = sell_shares * current_price
                            pnl = sell_proceeds - pos["cost"]
                            log(f"  ✓ Sold {sell_shares:.0f} shares at ~{current_price:.2f} (P&L: ${pnl:+.1f})")

                            record = {
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "action": "FLOW_REVERSAL",
                                "market": pos["market"],
                                "side": pos["side"],
                                "token": token_id,
                                "buy_price": pos["buy_price"],
                                "sell_price": current_price,
                                "shares": sell_shares,
                                "pnl": pnl,
                                "response": str(resp)
                            }
                            with open(TRADE_LOG, "a") as f:
                                f.write(json.dumps(record) + "\n")

                    except Exception as e:
                        log(f"  ✗ Flow reversal sell failed: {e}")

                    tokens_to_remove.append(token_id)

            for token_id in tokens_to_remove:
                del positions[token_id]

            # Cleanup old flow state
            cleanup_flow_state(active_epochs)

            # =================================================================
            # Status display
            # =================================================================
            if positions:
                log(f"\n📊 Holding {len(positions)} positions:")
                for token_id, pos in positions.items():
                    cp = get_price(token_id, side=SELL)
                    gain = cp - pos["buy_price"]
                    status = "📈" if gain >= 0 else "📉"
                    log(f"  {status} {pos['market']} {pos['side']}: "
                        f"entry {pos['buy_price']:.2f} → now {cp:.2f} ({gain:+.2f})")

            now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
            log(f"\n⏰ {now_str} UTC | {len(positions)} positions | Next scan in {POLL_INTERVAL}s")
            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            log("\nBot stopped by user")
            break
        except Exception as e:
            log(f"\n⚠ Error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    run_flow()
