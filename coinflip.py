import os
import time
import random
import requests
import json
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

# Trading parameters
BUY_AMOUNT = 2            # Amount in $ to buy per coinflip
ENTRY_LOW = 0.48          # Only buy if both sides >= this
ENTRY_HIGH = 0.52         # Only buy if both sides <= this
STOP_LOSS = 0.35          # Sell immediately if price drops below this
TAKE_PROFIT = 0.55        # Set limit sell at 99¢ if price rises above this
LIMIT_SELL_PRICE = 0.99   # Limit sell price for take-profit
SCAN_WINDOWS = 2          # How many 15-min windows ahead to scan (current + N)
POLL_INTERVAL = 1         # Seconds between scans

# Log file
DECISION_LOG = "coinflip_log.txt"

def log(message):
    """Write to log file and print"""
    timestamp = datetime.now(timezone.utc).isoformat()
    with open(DECISION_LOG, "a") as f:
        f.write(f"[{timestamp}] {message}\n")
    print(message)

# Initialize client
client = ClobClient(
    host=HOST,
    key=private_key,
    chain_id=CHAIN_ID,
    signature_type=1,
    funder=FUNDER_ADDRESS
)
client.set_api_creds(client.create_or_derive_api_creds())


def get_15min_crypto_markets(num_windows=SCAN_WINDOWS):
    """Fetch current and upcoming 15-minute crypto markets"""
    markets = []
    now = datetime.now(timezone.utc)
    minutes = (now.minute // 15) * 15
    current_window = now.replace(minute=minutes, second=0, microsecond=0)

    cryptos = ["btc", "eth", "sol", "xrp"]

    for i in range(num_windows):
        window_start = current_window + timedelta(minutes=15 * i)
        epoch = int(window_start.timestamp())

        for crypto in cryptos:
            slug = f"{crypto}-updown-15m-{epoch}"
            try:
                response = requests.get(f"{GAMMA_API}/events/slug/{slug}")
                if response.status_code == 200:
                    event = response.json()
                    for market in event.get("markets", []):
                        if not market.get("closed"):
                            markets.append(market)
            except Exception:
                pass
    return markets


def get_existing_orders():
    """Get all open orders"""
    try:
        return client.get_orders(OpenOrderParams())
    except Exception as e:
        log(f"Warning: Failed to fetch open orders: {e}")
        return []


def get_filled_trades():
    """Get all filled trades"""
    try:
        return client.get_trades()
    except Exception as e:
        log(f"Warning: Failed to fetch trades: {e}")
        return []


def get_price(token_id, side=BUY):
    """Get current price for a token"""
    try:
        data = client.get_price(token_id, side=side)
        return float(data.get("price", 0))
    except Exception:
        return 0.0


def get_actual_share_balance(token_id):
    """Get actual share balance from the CLOB server for a conditional token.
    Refreshes the server's cached balance first to avoid stale data."""
    try:
        # Tell server to re-read on-chain balances
        client.update_balance_allowance(
            params=BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
                signature_type=1
            )
        )
        # Now read the updated balance
        ba = client.get_balance_allowance(
            params=BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
                signature_type=1
            )
        )
        # Balance is returned in raw units (6 decimals), convert to shares
        raw_balance = float(ba.get("balance", 0))
        balance = raw_balance / 1e6
        return balance
    except Exception as e:
        log(f"  ⚠ Failed to get balance for {token_id[:20]}...: {e}")
        return None


def run_coinflip():
    """
    Coinflip strategy:
    1. Find markets where both sides are 48-52¢ and we have no positions
    2. Randomly buy one side
    3. Monitor positions:
       - Below 40¢ → market sell immediately (stop loss)
       - Above 60¢ → place limit sell at 99¢ (take profit)
    """
    # positions: {token_id: {"market": str, "side": str, "buy_price": float, "size": float, "limit_sell_placed": bool}}
    positions = {}

    log(f"\n{'='*60}")
    log(f"Coinflip bot started")
    log(f"Entry range: {ENTRY_LOW}-{ENTRY_HIGH}, Stop loss: {STOP_LOSS}, Take profit: {TAKE_PROFIT}")
    log(f"Buy amount: ${BUY_AMOUNT}, Scan windows: {SCAN_WINDOWS}")
    log(f"{'='*60}\n")

    # =====================================================================
    # STARTUP: Load existing positions from API (from previous runs)
    # =====================================================================
    log("Loading existing positions from API...")
    try:
        startup_trades = get_filled_trades()
        startup_markets = get_15min_crypto_markets()

        # Build token→market mapping from current markets
        token_to_market = {}
        for market in startup_markets:
            question = market.get("question", "N/A")
            tokens = json.loads(market.get("clobTokenIds", "[]"))
            for idx, tok in enumerate(tokens):
                side_name = "UP" if idx == 0 else "DOWN"
                token_to_market[tok] = {"market": question, "side": side_name}

        # Find YOUR trades on current market tokens
        for trade in startup_trades:
            if trade.get("maker_address", "").lower() != FUNDER_ADDRESS.lower():
                continue
            trade_token = str(trade.get("asset_id", ""))
            if trade_token not in token_to_market:
                continue
            if trade_token in positions:
                continue  # Already added

            trade_price = float(trade.get("price", 0))
            # Get actual on-chain balance for this token
            actual_balance = get_actual_share_balance(trade_token)
            if actual_balance is None or actual_balance <= 0:
                continue  # No shares held, skip

            info = token_to_market[trade_token]

            # Check if there's already a limit sell order for this token
            startup_orders = get_existing_orders()
            has_limit_sell = any(
                o.get("asset_id") == trade_token and
                o.get("side") == "sell" and
                abs(float(o.get("price", 0)) - LIMIT_SELL_PRICE) < 0.05
                for o in startup_orders
            )

            positions[trade_token] = {
                "market": info["market"],
                "side": info["side"],
                "buy_price": trade_price,
                "size": actual_balance,
                "limit_sell_placed": has_limit_sell
            }
            log(f"  Loaded: {info['market']} {info['side']} - {actual_balance:.2f} shares at {trade_price} (limit sell: {has_limit_sell})")

        log(f"Loaded {len(positions)} existing positions\n")
    except Exception as e:
        log(f"Warning: Failed to load existing positions: {e}")

    while True:
        try:
            os.system('cls' if os.name == 'nt' else 'clear')
            print("⏳ Scanning markets...", flush=True)
            markets = get_15min_crypto_markets()
            existing_orders = get_existing_orders()
            filled_trades = get_filled_trades()

            # Build sets of tokens we already have orders/trades on
            order_tokens = set()
            for order in existing_orders:
                tid = order.get("asset_id")
                if tid:
                    order_tokens.add(tid)

            trade_tokens = set()
            for trade in filled_trades:
                if trade.get("maker_address", "").lower() == FUNDER_ADDRESS.lower():
                    tid = str(trade.get("asset_id", ""))
                    if tid:
                        trade_tokens.add(tid)

            # Also count our in-memory positions as "already traded"
            all_known_tokens = order_tokens | trade_tokens | set(positions.keys())

            # =================================================================
            # STEP 1: Look for new coinflip opportunities
            # =================================================================
            for market in markets:
                question = market.get("question", "N/A")
                tokens = json.loads(market.get("clobTokenIds", "[]"))

                if len(tokens) != 2:
                    continue

                # Skip if we already have any position/order on this market
                if any(t in all_known_tokens for t in tokens):
                    continue

                # Get prices for both sides
                price_up = get_price(tokens[0], side=BUY)
                price_down = get_price(tokens[1], side=BUY)

                # Check if both sides are within entry range
                if (ENTRY_LOW <= price_up <= ENTRY_HIGH and
                    ENTRY_LOW <= price_down <= ENTRY_HIGH):

                    # Randomly pick a side
                    chosen_idx = random.randint(0, 1)
                    chosen_token = tokens[chosen_idx]
                    chosen_side = "UP" if chosen_idx == 0 else "DOWN"
                    chosen_price = price_up if chosen_idx == 0 else price_down

                    log(f"\n🎲 COINFLIP: {question}")
                    log(f"  Up: {price_up}, Down: {price_down}")
                    log(f"  Chose: {chosen_side} at {chosen_price}")

                    try:
                        mo = MarketOrderArgs(
                            token_id=chosen_token,
                            amount=BUY_AMOUNT,
                            side=BUY,
                            order_type=OrderType.FAK
                        )
                        signed = client.create_market_order(mo)
                        resp = client.post_order(signed, OrderType.FAK)

                        # Parse actual shares received from response
                        # resp contains 'takingAmount' (shares received when buying)
                        actual_shares = 0
                        if isinstance(resp, dict):
                            taking = resp.get("takingAmount", "0")
                            actual_shares = float(taking) if taking else 0
                        
                        # Fallback to approximation if response doesn't have the field
                        if actual_shares == 0:
                            actual_shares = BUY_AMOUNT / chosen_price if chosen_price > 0 else 0
                            log(f"  ⚠ Could not parse actual shares, estimated: {actual_shares:.2f}")

                        positions[chosen_token] = {
                            "market": question,
                            "side": chosen_side,
                            "buy_price": chosen_price,
                            "size": actual_shares,
                            "limit_sell_placed": False
                        }

                        log(f"  ✓ Bought {actual_shares:.2f} shares of {chosen_side} at {chosen_price}")
                        log(f"  Response: {resp}")

                        # Log to file
                        record = {
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "action": "BUY",
                            "market": question,
                            "side": chosen_side,
                            "token": chosen_token,
                            "price": chosen_price,
                            "amount": BUY_AMOUNT,
                            "response": str(resp)
                        }
                        with open("coinflip_trades.json", "a") as f:
                            f.write(json.dumps(record) + "\n")

                    except Exception as e:
                        log(f"  ✗ Buy failed: {e}")
                else:
                    # Prices outside entry range, skip silently
                    pass

            # =================================================================
            # STEP 2: Monitor existing positions
            # =================================================================
            tokens_to_remove = []

            for token_id, pos in positions.items():
                current_price = get_price(token_id, side=SELL)
                market_name = pos["market"]
                side_name = pos["side"]

                # Skip dust positions (less than 0.1 shares)
                actual_balance = get_actual_share_balance(token_id)
                if actual_balance is not None and actual_balance < 0.1:
                    log(f"  Removing dust position: {market_name} {side_name} ({actual_balance:.6f} shares)")
                    tokens_to_remove.append(token_id)
                    continue

                # STOP LOSS: price dropped below threshold → sell immediately
                if current_price < STOP_LOSS and current_price > 0:
                    log(f"\n🔴 STOP LOSS: {market_name} {side_name}")
                    log(f"  Price {current_price} < {STOP_LOSS}, selling immediately")

                    try:
                        # Refresh balance and get actual shares owned
                        actual_balance = get_actual_share_balance(token_id)
                        if actual_balance is not None and actual_balance > 0:
                            sell_shares = actual_balance
                        else:
                            sell_shares = pos["size"]
                        log(f"  Shares to sell: {sell_shares:.2f} (on-chain: {actual_balance}, tracked: {pos['size']:.2f})")
                        sell_dollar_amount = sell_shares * current_price
                        mo = MarketOrderArgs(
                            token_id=token_id,
                            amount=sell_dollar_amount,
                            side=SELL,
                            order_type=OrderType.FAK
                        )
                        signed = client.create_market_order(mo)
                        resp = client.post_order(signed, OrderType.FAK)
                        log(f"  ✓ Sold at ~{current_price}: {resp}")

                        record = {
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "action": "STOP_LOSS_SELL",
                            "market": market_name,
                            "side": side_name,
                            "token": token_id,
                            "price": current_price,
                            "response": str(resp)
                        }
                        with open("coinflip_trades.json", "a") as f:
                            f.write(json.dumps(record) + "\n")

                        tokens_to_remove.append(token_id)
                    except Exception as e:
                        log(f"  ✗ Stop loss sell failed: {e}")

                # TAKE PROFIT: price rose above threshold → place limit sell at 99¢
                elif current_price > TAKE_PROFIT and not pos["limit_sell_placed"]:
                    log(f"\n🟢 TAKE PROFIT: {market_name} {side_name}")
                    log(f"  Price {current_price} > {TAKE_PROFIT}, placing limit sell at {LIMIT_SELL_PRICE}")

                    try:
                        # Refresh balance and get actual shares owned
                        actual_balance = get_actual_share_balance(token_id)
                        if actual_balance is not None and actual_balance > 0:
                            sell_size = round(actual_balance * 0.99, 2)  # 99% to avoid rounding
                        else:
                            sell_size = round(pos["size"] * 0.99, 2)
                        log(f"  On-chain balance: {actual_balance}, tracked: {pos['size']:.2f}, selling: {sell_size:.2f}")
                        order = OrderArgs(
                            token_id=token_id,
                            price=LIMIT_SELL_PRICE,
                            size=sell_size,
                            side=SELL
                        )
                        signed = client.create_order(order)
                        resp = client.post_order(signed, OrderType.GTC)
                        log(f"  ✓ Limit sell placed at {LIMIT_SELL_PRICE}: {resp}")

                        pos["limit_sell_placed"] = True

                        record = {
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "action": "TAKE_PROFIT_LIMIT_SELL",
                            "market": market_name,
                            "side": side_name,
                            "token": token_id,
                            "price": LIMIT_SELL_PRICE,
                            "size": pos["size"],
                            "response": str(resp)
                        }
                        with open("coinflip_trades.json", "a") as f:
                            f.write(json.dumps(record) + "\n")

                    except Exception as e:
                        log(f"  ✗ Limit sell failed: {e}")

            # Remove closed positions
            for token_id in tokens_to_remove:
                del positions[token_id]

            # Status line
            if positions:
                log(f"\n📊 Monitoring {len(positions)} positions:")
                for token_id, pos in positions.items():
                    cp = get_price(token_id, side=SELL)
                    pnl = cp - pos["buy_price"]
                    status = "📈" if pnl >= 0 else "📉"
                    limit_status = " [limit sell placed]" if pos["limit_sell_placed"] else ""
                    log(f"  {status} {pos['market']} {pos['side']}: bought {pos['buy_price']} → now {cp} ({pnl:+.2f}){limit_status}")

            time.sleep(POLL_INTERVAL)

        except Exception as e:
            log(f"Error: {e}")
            time.sleep(10)


if __name__ == "__main__":
    run_coinflip()