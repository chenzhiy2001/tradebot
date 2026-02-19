#!/usr/bin/env python3
"""
Flow Bot: Follow the money, not specific wallets.

Strategy:
  For each 5-min and 15-min crypto market, pull the live trade feed
  (same data as the Activity tab on polymarket.com).

  Compute net flow = (buy volume) - (sell volume) for each side.
  When one side has heavy net buying that dwarfs the other, follow it.

Signals (all must be true to trigger):
  1. Net buy volume on chosen side exceeds MIN_FLOW ($)
  2. Flow ratio (chosen / other) exceeds MIN_RATIO
  3. We don't already hold a position on this market

Exit:
  - Limit sell at 0.99 (take profit)
  - Flow reversal: if net flow flips against us, market sell
"""

import os
import time
import math
import requests
import json
import threading
from datetime import datetime, timezone, timedelta
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
MIN_FLOW = 300             # Minimum net buy $ on chosen side to trigger
MIN_RATIO = 2.0            # Net buy on chosen side must be Nx the other
POLL_INTERVAL = 1          # Seconds between scans
SELL_PRICE = 0.99          # Limit sell price (take profit)
STOP_LOSS_DELTA = 0.10     # Sell if price drops this much from entry
CRYPTOS = ["btc", "eth", "sol", "xrp"]

# Scaling: bet more when the flow is stronger
# bet = BUY_AMOUNT * min(net_flow / MIN_FLOW, MAX_BET_MULTIPLIER)
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
# API HELPERS
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


def get_market_trades(condition_id):
    """
    Fetch trade activity for a market from the data API.
    Same data as the Activity tab on polymarket.com.
    Returns list of trades sorted by timestamp.
    """
    try:
        r = requests.get(
            f"{DATA_API}/trades",
            params={"market": condition_id, "limit": 100},
            timeout=15
        )
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return []


def compute_trade_flow(condition_id, tokens):
    """
    Analyze the trade feed for a market.
    Returns dict with net buy flow per side ($ volume: buys - sells).
    """
    trades = get_market_trades(condition_id)
    up_token = tokens[0]

    result = {
        "up_buys": 0.0, "up_sells": 0.0,
        "down_buys": 0.0, "down_sells": 0.0,
        "up_net": 0.0, "down_net": 0.0,
        "trade_count": len(trades),
        "big_trades": [],
    }

    for t in trades:
        asset = t.get("asset", "")
        side = t.get("side", "")
        size = float(t.get("size", 0))
        price = float(t.get("price", 0))
        cost = size * price
        is_up = (asset == up_token)

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

        # Track big trades for logging
        if cost >= 50:
            name = t.get("name", "") or t.get("pseudonym", "") or t.get("proxyWallet", "")[:12]
            outcome = "UP" if is_up else "DN"
            result["big_trades"].append(f"{name} {side} {size:.0f} {outcome} @{price:.2f} (${cost:.0f})")

    result["up_net"] = result["up_buys"] - result["up_sells"]
    result["down_net"] = result["down_buys"] - result["down_sells"]

    return result


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
# MAIN BOT
# =========================================================================

def run_flow():
    """
    Flow strategy:
    1. For each market, fetch the trade feed (Activity tab data)
    2. Compute net buy flow per side (buys $ - sells $)
    3. When one side has strong net inflow, buy it
    4. Place limit sell at 0.99
    5. If flow reverses on next check, market sell
    """
    positions = {}  # token_id -> position info

    log(f"\n{'='*60}")
    log(f"Flow bot started (using trade activity feed)")
    log(f"Min flow: ${MIN_FLOW}, Min ratio: {MIN_RATIO}x")
    log(f"Buy amount: ${BUY_AMOUNT}, Cryptos: {', '.join(CRYPTOS)}")
    log(f"{'='*60}\n")

    while True:
        try:
            os.system('cls' if os.name == 'nt' else 'clear')
            print("💰 Analyzing trade activity...", flush=True)

            markets = get_current_crypto_markets()
            existing_orders = get_existing_orders()

            # Build set of tokens we already hold
            known_tokens = set(positions.keys())
            for order in existing_orders:
                tid = order.get("asset_id")
                if tid:
                    known_tokens.add(tid)

            # =================================================================
            # STEP 1: Analyze trade flow and enter positions
            # =================================================================
            for market in markets:
                question = market.get("question", "N/A")
                crypto = market.get("_crypto", "")
                interval = market.get("_interval", 15)
                elapsed = market.get("_elapsed_min", 0)
                condition_id = market.get("conditionId", "")
                slug = market.get("_slug", "")
                epoch = market.get("_epoch", 0)
                tokens = json.loads(market.get("clobTokenIds", "[]"))

                if len(tokens) != 2:
                    continue

                # Skip if we already have a position on this market
                if any(t in known_tokens for t in tokens):
                    continue

                # Fetch and analyze trade activity
                flow = compute_trade_flow(condition_id, tokens)
                up_net = flow["up_net"]
                down_net = flow["down_net"]
                trade_count = flow["trade_count"]

                # Log flow for all markets
                if trade_count > 0:
                    log(f"  {crypto} {interval}m ({trade_count} trades): "
                        f"UP net ${up_net:+,.0f} (${flow['up_buys']:,.0f}B/${flow['up_sells']:,.0f}S) | "
                        f"DOWN net ${down_net:+,.0f} (${flow['down_buys']:,.0f}B/${flow['down_sells']:,.0f}S)")
                else:
                    log(f"  {crypto} {interval}m: no trades yet")
                    continue

                # Check for entry signal
                if up_net >= MIN_FLOW and (down_net <= 0 or up_net / max(down_net, 1) >= MIN_RATIO):
                    chosen_idx = 0
                    chosen_side = "UP"
                    chosen_net = up_net
                    other_net = down_net
                elif down_net >= MIN_FLOW and (up_net <= 0 or down_net / max(up_net, 1) >= MIN_RATIO):
                    chosen_idx = 1
                    chosen_side = "DOWN"
                    chosen_net = down_net
                    other_net = up_net
                else:
                    continue  # No signal

                chosen_token = tokens[chosen_idx]
                chosen_price = get_price(chosen_token, side=BUY)

                if chosen_price <= 0 or chosen_price >= 1:
                    continue

                # Scale bet by flow strength
                flow_strength = chosen_net / MIN_FLOW
                bet_multiplier = min(flow_strength, MAX_BET_MULTIPLIER)
                scaled_amount = round(BUY_AMOUNT * bet_multiplier, 2)

                ratio = chosen_net / max(abs(other_net), 1)

                log(f"\n💰 FLOW SIGNAL: {question}")
                log(f"  {chosen_side} net flow: ${chosen_net:+,.0f} (ratio {ratio:.1f}x)")
                log(f"  Price: {chosen_price:.2f}, Bet: ${scaled_amount:.0f} ({bet_multiplier:.1f}x)")

                # Show big trades that drove the signal
                if flow["big_trades"]:
                    log(f"  Big trades:")
                    for bt in flow["big_trades"][:5]:
                        log(f"    {bt}")

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
                        "entry_net": chosen_net,
                        "tokens": tokens,
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
                        "net_flow": chosen_net,
                        "ratio": round(ratio, 1),
                        "big_trades": flow["big_trades"][:5],
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

                # Still active — check price-based stop loss first
                current_price = get_price(token_id, side=SELL)
                stop_price = pos["buy_price"] - STOP_LOSS_DELTA
                exit_reason = None

                if current_price <= stop_price:
                    exit_reason = "STOP_LOSS"
                    log(f"\n🛑 STOP LOSS: {pos['market']} {pos['side']}")
                    log(f"  Entry {pos['buy_price']:.2f} → now {current_price:.2f} (stop was {stop_price:.2f})")

                # Then check flow reversal
                if not exit_reason:
                    flow = compute_trade_flow(pos["condition_id"], pos["tokens"])
                    our_net = flow["up_net"] if pos["outcome_idx"] == 0 else flow["down_net"]
                    if our_net < 0 and pos["entry_net"] > 0:
                        exit_reason = "FLOW_REVERSAL"
                        log(f"\n🔄 FLOW REVERSAL: {pos['market']} {pos['side']}")
                        log(f"  Entry net was ${pos['entry_net']:+,.0f}, now ${our_net:+,.0f}")

                if exit_reason:
                    try:
                        cancel_all_orders_for_token(token_id)
                        time.sleep(1)
                        actual_balance = get_actual_share_balance(token_id)
                        if actual_balance and actual_balance > 0.1:
                            sell_shares = math.floor(actual_balance * 100) / 100
                        else:
                            sell_shares = math.floor(pos["size"] * 100) / 100

                        if sell_shares > 0:
                            sell_price = get_price(token_id, side=SELL)
                            mo = MarketOrderArgs(
                                token_id=token_id,
                                amount=sell_shares,
                                side=SELL,
                                order_type=OrderType.FAK
                            )
                            signed = client.create_market_order(mo)
                            resp = client.post_order(signed, OrderType.FAK)

                            sell_proceeds = sell_shares * sell_price
                            pnl = sell_proceeds - pos["cost"]
                            log(f"  ✓ Sold {sell_shares:.0f} shares at ~{sell_price:.2f} (P&L: ${pnl:+.1f})")

                            record = {
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "action": exit_reason,
                                "market": pos["market"],
                                "side": pos["side"],
                                "token": token_id,
                                "buy_price": pos["buy_price"],
                                "sell_price": sell_price,
                                "shares": sell_shares,
                                "pnl": pnl,
                                "response": str(resp)
                            }
                            with open(TRADE_LOG, "a") as f:
                                f.write(json.dumps(record) + "\n")

                    except Exception as e:
                        log(f"  ✗ {exit_reason} sell failed: {e}")

                    tokens_to_remove.append(token_id)

            for token_id in tokens_to_remove:
                del positions[token_id]

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
