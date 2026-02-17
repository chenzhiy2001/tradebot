#!/usr/bin/env python3
"""
High-Probability Bot: Buy the high-probability side with a stop loss.

Theory:
  When one side is at 90¢ (≈ 90% probability), buy 100 shares for $90.
  Stop loss at 80¢ limits downside.
  EV = 0.9 * $100 + 0.1 * $80 - $90 = +$8 per trade.

  The stop loss converts a zero-EV fair bet into positive EV by cutting
  losses before resolution instead of riding to $0.

Strategy:
1. Scan current 5-min and 15-min crypto updown markets
2. Find the side priced between MIN_ENTRY and MAX_ENTRY (high prob side)
3. Buy it, scaling shares by price (more at higher prices = more confidence)
4. Immediately place limit sell at 0.99 (take profit) + limit sell at stop loss price
5. Whichever fills first — no polling needed, orders sit on the book
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
CHAIN_ID = 137
FUNDER_ADDRESS = founder_address

# =========================================================================
# STRATEGY PARAMETERS
# =========================================================================
MIN_ENTRY = 0.80           # Only buy if high-prob side price >= this
MAX_ENTRY = 0.95           # Only buy if high-prob side price <= this
STOP_LOSS_DELTA = 0.10     # Sell if price drops this much below buy price
BASE_SHARES = 100          # Base number of shares at MIN_ENTRY price
POLL_INTERVAL = 1          # Seconds between scans
SELL_PRICE = 0.99          # Limit sell price (take profit)
CRYPTOS = ["btc", "eth", "sol", "xrp"]

# Scaling: at 80¢ buy BASE_SHARES, at 95¢ buy more (higher confidence)
# shares = BASE_SHARES * (price / MIN_ENTRY)
# e.g. at 90¢: 100 * (0.90/0.80) = 112 shares ($101 cost)
# e.g. at 95¢: 100 * (0.95/0.80) = 119 shares ($113 cost)

# Log files
DECISION_LOG = "highprob_log.txt"
TRADE_LOG = "highprob_trades.json"

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
                            markets.append(market)
            except Exception:
                pass
    return markets


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


def get_existing_orders():
    try:
        return client.get_orders(OpenOrderParams())
    except Exception:
        return []


def cancel_all_orders_for_token(token_id):
    """Cancel any open orders for a token (e.g. before placing stop loss sell)."""
    try:
        orders = get_existing_orders()
        for order in orders:
            if order.get("asset_id") == token_id:
                client.cancel(order.get("id"))
    except Exception:
        pass


def run_highprob():
    """
    High-probability strategy:
    1. Find markets where one side is priced 80-95¢
    2. Buy that side (high probability)
    3. Place limit sell at 0.99 (take profit) + limit sell at stop price (stop loss)
    4. Both orders on the book — whichever fills first, done
    """
    positions = {}  # token_id -> position info

    log(f"\n{'='*60}")
    log(f"High-Probability bot started")
    log(f"Entry range: {MIN_ENTRY}-{MAX_ENTRY}, Stop loss delta: {STOP_LOSS_DELTA}")
    log(f"Base shares: {BASE_SHARES}, Cryptos: {', '.join(CRYPTOS)}")
    log(f"{'='*60}\n")

    while True:
        try:
            os.system('cls' if os.name == 'nt' else 'clear')
            print("🎯 Scanning for high-probability entries...", flush=True)

            markets = get_current_crypto_markets()

            # Build set of tokens we already hold
            known_tokens = set(positions.keys())
            for order in get_existing_orders():
                tid = order.get("asset_id")
                if tid:
                    known_tokens.add(tid)

            # =================================================================
            # STEP 1: Find and enter high-probability markets
            # =================================================================
            for market in markets:
                question = market.get("question", "N/A")
                crypto = market.get("_crypto", "")
                interval = market.get("_interval", 15)
                slug = market.get("_slug", "")
                condition_id = market.get("conditionId", "")
                tokens = json.loads(market.get("clobTokenIds", "[]"))

                if len(tokens) != 2:
                    continue

                # Skip if we already have a position on this market
                if any(t in known_tokens for t in tokens):
                    continue

                # Check prices for both sides
                up_token, down_token = tokens[0], tokens[1]
                up_price = get_price(up_token, side=BUY)
                down_price = get_price(down_token, side=BUY)

                # Find the high-probability side
                if MIN_ENTRY <= up_price <= MAX_ENTRY:
                    chosen_idx = 0
                    chosen_side = "UP"
                    chosen_token = up_token
                    chosen_price = up_price
                elif MIN_ENTRY <= down_price <= MAX_ENTRY:
                    chosen_idx = 1
                    chosen_side = "DOWN"
                    chosen_token = down_token
                    chosen_price = down_price
                else:
                    # Neither side in our entry range — log prices for visibility
                    log(f"  {crypto} {interval}m: UP={up_price:.2f} DOWN={down_price:.2f} (outside {MIN_ENTRY}-{MAX_ENTRY})")
                    continue

                # Calculate shares: scale up with price (higher price = more confidence)
                num_shares = math.floor(BASE_SHARES * (chosen_price / MIN_ENTRY))
                cost = num_shares * chosen_price
                stop_price = round(chosen_price - STOP_LOSS_DELTA, 2)

                log(f"\n🎯 {question}")
                log(f"  {chosen_side} at {chosen_price:.2f} → {num_shares} shares (${cost:.0f})")
                log(f"  Stop loss at {stop_price:.2f}")

                # EV calculation for logging (sell at 0.99, stop at stop_price)
                prob = chosen_price  # market price ≈ probability
                ev = prob * num_shares * SELL_PRICE + (1 - prob) * (num_shares * stop_price) - cost
                log(f"  EV = {prob:.0%} × ${num_shares * SELL_PRICE:.0f} + {1-prob:.0%} × ${num_shares * stop_price:.0f} - ${cost:.0f} = ${ev:+.1f}")

                # Place market buy
                try:
                    buy_amount_usd = cost
                    mo = MarketOrderArgs(
                        token_id=chosen_token,
                        amount=buy_amount_usd,
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
                        actual_shares = num_shares  # estimate

                    positions[chosen_token] = {
                        "market": question,
                        "side": chosen_side,
                        "buy_price": chosen_price,
                        "stop_price": stop_price,
                        "size": actual_shares,
                        "condition_id": condition_id,
                        "outcome_idx": chosen_idx,
                        "slug": slug,
                        "cost": cost,
                    }

                    log(f"  ✓ Bought {actual_shares:.0f} shares of {chosen_side} at {chosen_price:.2f}")

                    # Place limit sell at SELL_PRICE (take profit) in background
                    # after a short delay for balance to settle
                    if actual_shares > 0:
                        def place_limit_sells(token, shares, buy_p, stop_p, market_q, side_s):
                            time.sleep(10)
                            try:
                                actual_balance = get_actual_share_balance(token)
                                if actual_balance and actual_balance > 0.1:
                                    total_shares = actual_balance
                                else:
                                    total_shares = shares

                                # Split shares: most at take-profit, rest at stop-loss
                                # Take profit gets the bulk
                                tp_shares = math.floor(total_shares * 100) / 100

                                # Place take-profit sell at 0.99
                                sell_order = OrderArgs(
                                    token_id=token,
                                    price=SELL_PRICE,
                                    size=tp_shares,
                                    side=SELL
                                )
                                signed_sell = client.create_order(sell_order)
                                client.post_order(signed_sell, OrderType.GTC)
                                log(f"  ✓ Limit sell placed: {tp_shares:.0f} shares of {side_s} at {SELL_PRICE}")

                            except Exception as e:
                                log(f"  ⚠ Limit sell failed for {side_s}: {e}")
                        threading.Thread(
                            target=place_limit_sells,
                            args=(chosen_token, actual_shares, chosen_price, stop_price, question, chosen_side),
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
                        "shares": actual_shares,
                        "cost": cost,
                        "stop_price": stop_price,
                        "response": str(resp)
                    }
                    with open(TRADE_LOG, "a") as f:
                        f.write(json.dumps(record) + "\n")

                except Exception as e:
                    log(f"  ✗ Buy failed: {e}")

            # =================================================================
            # STEP 2: Monitor positions — stop loss + cleanup
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

                # Still active — check stop loss by polling price
                current_price = get_price(token_id, side=SELL)
                if current_price <= 0:
                    continue

                if current_price <= pos["stop_price"]:
                    # STOP LOSS TRIGGERED — cancel take-profit order AND market sell
                    log(f"\n🛑 STOP LOSS: {pos['market']} {pos['side']}")
                    log(f"  Price {current_price:.2f} ≤ stop {pos['stop_price']:.2f}")

                    try:
                        cancel_all_orders_for_token(token_id)
                        time.sleep(1)

                        actual_balance = get_actual_share_balance(token_id)
                        if actual_balance and actual_balance > 0.1:
                            sell_shares = actual_balance
                        else:
                            sell_shares = pos["size"]

                        sz = math.floor(sell_shares * 100) / 100
                        if sz > 0:
                            mo = MarketOrderArgs(
                                token_id=token_id,
                                amount=sz,
                                side=SELL,
                                order_type=OrderType.FAK
                            )
                            signed = client.create_market_order(mo)
                            resp = client.post_order(signed, OrderType.FAK)

                            sell_proceeds = sz * current_price
                            loss = sell_proceeds - pos["cost"]
                            log(f"  ✓ Sold {sz:.0f} shares at ~{current_price:.2f} (${sell_proceeds:.0f}, P&L: ${loss:+.1f})")

                            record = {
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "action": "STOP_LOSS",
                                "market": pos["market"],
                                "side": pos["side"],
                                "token": token_id,
                                "buy_price": pos["buy_price"],
                                "sell_price": current_price,
                                "shares": sz,
                                "loss": loss,
                                "response": str(resp)
                            }
                            with open(TRADE_LOG, "a") as f:
                                f.write(json.dumps(record) + "\n")

                    except Exception as e:
                        log(f"  ✗ Stop loss sell failed: {e}")

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
                    distance_to_stop = cp - pos["stop_price"]
                    status = "📈" if gain >= 0 else "📉"
                    log(f"  {status} {pos['market']} {pos['side']}: "
                        f"entry {pos['buy_price']:.2f} → now {cp:.2f} ({gain:+.2f}) "
                        f"| stop at {pos['stop_price']:.2f} ({distance_to_stop:+.2f} away)")

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
    run_highprob()
