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
from private_key import private_key, founder_address

# Configuration
HOST = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"
CHAIN_ID = 137
FUNDER_ADDRESS = founder_address

# Trading parameters
BUY_AMOUNT = 5             # Amount in $ to buy per trade
MAX_PRICE = 0.60           # Only buy if chosen side price < this
PROFIT_EXIT = 0.4         # Sell if price increased by this much since buy
STOP_LOSS = 0.20           # Sell if price dropped by this much since buy
TOP_HOLDERS = 10           # Number of top holders to check per side
SCAN_WINDOWS = 1           # Number of 15-min windows to scan (current)
MIN_ELAPSED = 1            # Minimum minutes elapsed since market start
POLL_INTERVAL = 1          # Seconds between scans
SHOW_HOLDERS = 1           # Number of top holders to display per side

# Log file
DECISION_LOG = "whales_log.txt"


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
    """Fetch current 15-minute crypto markets that started > MIN_ELAPSED minutes ago"""
    markets = []
    now = datetime.now(timezone.utc)
    minutes = (now.minute // 15) * 15
    current_window = now.replace(minute=minutes, second=0, microsecond=0)

    cryptos = ["btc", "eth", "sol", "xrp"]

    for i in range(SCAN_WINDOWS):
        window_start = current_window + timedelta(minutes=15 * i)
        elapsed = (now - window_start).total_seconds() / 60

        # Only include markets that have been running > MIN_ELAPSED minutes
        if elapsed < MIN_ELAPSED:
            continue

        epoch = int(window_start.timestamp())

        for crypto in cryptos:
            slug = f"{crypto}-updown-15m-{epoch}"
            try:
                response = requests.get(f"{GAMMA_API}/events/slug/{slug}", timeout=10)
                if response.status_code == 200:
                    event = response.json()
                    for market in event.get("markets", []):
                        if not market.get("closed"):
                            market["_elapsed_min"] = elapsed
                            market["_crypto"] = crypto.upper()
                            markets.append(market)
            except Exception:
                pass
    return markets


def get_market_top_holders(condition_id):
    """Get top holders for each side of a market using the /holders endpoint.
    Returns {0: [(wallet, shares), ...], 1: [(wallet, shares), ...]}
    where 0=UP, 1=DOWN sorted by balance descending."""
    try:
        r = requests.get(
            f"{DATA_API}/holders",
            params={"market": condition_id, "limit": TOP_HOLDERS, "minBalance": 1},
            timeout=15
        )
        if r.status_code != 200:
            return {0: [], 1: []}
        data = r.json()
    except Exception:
        return {0: [], 1: []}

    result = {0: [], 1: []}
    for token_group in data:
        holders = token_group.get("holders", [])
        for h in holders:
            wallet = h.get("proxyWallet", "")
            amount = float(h.get("amount", 0))
            outcome = h.get("outcomeIndex", 0)
            if wallet and amount > 0:
                result[outcome].append((wallet, amount))

    # Sort each side by shares descending
    for outcome in result:
        result[outcome].sort(key=lambda x: -x[1])

    return result


def get_user_pnl(wallet):
    """Get a user's past-day PnL using the leaderboard endpoint.
    Returns actual realized PnL, not cash flow. Returns 0 for unranked users."""
    try:
        r = requests.get(
            f"{DATA_API}/v1/leaderboard",
            params={
                "timePeriod": "DAY",
                "orderBy": "PNL",
                "user": wallet,
            },
            timeout=10
        )
        if r.status_code != 200:
            return 0.0
        data = r.json()
        if not data:
            return 0.0
        return float(data[0].get("pnl", 0))
    except Exception:
        return 0.0


def get_user_bet_on_market(wallet, condition_id, outcome_index):
    """Get how much money (USDC) a user has bet on a specific side of a market.
    Returns initialValue (shares * avgPrice) for the matching outcome."""
    try:
        r = requests.get(
            f"{DATA_API}/positions",
            params={
                "user": wallet,
                "market": condition_id,
                "sizeThreshold": 0,
            },
            timeout=10
        )
        if r.status_code != 200:
            return 0.0
        positions = r.json()
        for p in positions:
            if p.get("outcomeIndex") == outcome_index:
                return float(p.get("initialValue", 0))
        return 0.0
    except Exception:
        return 0.0


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


def get_filled_trades():
    try:
        return client.get_trades()
    except Exception:
        return []


def run_whales():
    """
    Whale-following strategy:
    1. Find current 15-min crypto markets (started > 2 min ago)
    2. For each market, get top holders per side from trades
    3. For each side's top 20 holders, sum their total PnL
    4. Buy the side with more total PnL if price < 60¢
    5. Sell if price increased > 20¢ since buy
    """
    # positions: {token_id: {"market": str, "side": str, "buy_price": float, "size": float, "condition_id": str, "outcome_idx": int}}
    positions = {}

    log(f"\n{'='*60}")
    log(f"Whale-following bot started")
    log(f"Buy amount: ${BUY_AMOUNT}, Max price: {MAX_PRICE}, Profit exit: +{PROFIT_EXIT}, Stop loss: -{STOP_LOSS}")
    log(f"Top holders to check: {TOP_HOLDERS}, Min elapsed: {MIN_ELAPSED}min")
    log(f"{'='*60}\n")

    # =====================================================================
    # STARTUP: Load existing positions from API (from previous runs)
    # =====================================================================
    log("Loading existing positions from API...")
    try:
        startup_trades = get_filled_trades()
        startup_markets = get_current_crypto_markets()

        # Build token→market mapping from current markets
        token_to_market = {}
        for market in startup_markets:
            question = market.get("question", "N/A")
            condition_id = market.get("conditionId", "")
            tokens = json.loads(market.get("clobTokenIds", "[]"))
            for idx, tok in enumerate(tokens):
                side_name = "UP" if idx == 0 else "DOWN"
                token_to_market[tok] = {
                    "market": question,
                    "side": side_name,
                    "condition_id": condition_id,
                    "outcome_idx": idx
                }

        # Find YOUR trades on current market tokens
        for trade in startup_trades:
            if trade.get("maker_address", "").lower() != FUNDER_ADDRESS.lower():
                continue
            trade_token = str(trade.get("asset_id", ""))
            if trade_token not in token_to_market:
                continue
            if trade_token in positions:
                continue

            trade_price = float(trade.get("price", 0))
            actual_balance = get_actual_share_balance(trade_token)
            if actual_balance is None or actual_balance < 0.1:
                continue

            info = token_to_market[trade_token]
            positions[trade_token] = {
                "market": info["market"],
                "side": info["side"],
                "buy_price": trade_price,
                "size": actual_balance,
                "condition_id": info["condition_id"],
                "outcome_idx": info["outcome_idx"]
            }
            log(f"  Loaded: {info['market']} {info['side']} - {actual_balance:.2f} shares at {trade_price}")

        log(f"Loaded {len(positions)} existing positions\n")
    except Exception as e:
        log(f"Warning: Failed to load existing positions: {e}")

    while True:
        try:
            os.system('cls' if os.name == 'nt' else 'clear')
            print("⏳ Scanning markets...", flush=True)

            markets = get_current_crypto_markets()
            existing_orders = get_existing_orders()
            filled_trades = get_filled_trades()

            # Build set of tokens we already have orders/trades on
            known_tokens = set(positions.keys())
            for order in existing_orders:
                tid = order.get("asset_id")
                if tid:
                    known_tokens.add(tid)
            for trade in filled_trades:
                if trade.get("maker_address", "").lower() == FUNDER_ADDRESS.lower():
                    tid = str(trade.get("asset_id", ""))
                    if tid:
                        known_tokens.add(tid)

            # =================================================================
            # STEP 1: Find new opportunities
            # =================================================================
            for market in markets:
                question = market.get("question", "N/A")
                crypto = market.get("_crypto", "")
                elapsed = market.get("_elapsed_min", 0)
                condition_id = market.get("conditionId", "")
                tokens = json.loads(market.get("clobTokenIds", "[]"))

                if len(tokens) != 2:
                    continue

                # Skip if we already have a position on this market
                if any(t in known_tokens for t in tokens):
                    continue

                log(f"\n📊 Analyzing: {question} ({elapsed:.0f}min elapsed)")

                # Get prices
                price_up = get_price(tokens[0], side=BUY)
                price_down = get_price(tokens[1], side=BUY)
                log(f"  Prices: UP={price_up}, DOWN={price_down}")

                # Get top holders per side
                log(f"  Fetching top holders...")
                holders = get_market_top_holders(condition_id)
                up_holders = holders.get(0, [])[:TOP_HOLDERS]
                down_holders = holders.get(1, [])[:TOP_HOLDERS]

                log(f"  UP holders: {len(up_holders)}, DOWN holders: {len(down_holders)}")

                if not up_holders and not down_holders:
                    log(f"  ⊘ No holders found, skipping")
                    continue

                # Get PnL × bet for top holders on each side (past day)
                log(f"  Fetching past-day PnL × bet for top {TOP_HOLDERS} holders per side...")
                up_total_score = 0.0
                for i, (wallet, shares) in enumerate(up_holders):
                    pnl = get_user_pnl(wallet)
                    bet = get_user_bet_on_market(wallet, condition_id, 0)
                    score = pnl * bet
                    up_total_score += score
                    if i < SHOW_HOLDERS:
                        log(f"    UP #{i+1}: {wallet[:15]}... {shares:.0f} shares, PnL=${pnl:.2f}, Bet=${bet:.2f}, Score={score:.2f}")

                down_total_score = 0.0
                for i, (wallet, shares) in enumerate(down_holders):
                    pnl = get_user_pnl(wallet)
                    bet = get_user_bet_on_market(wallet, condition_id, 1)
                    score = pnl * bet
                    down_total_score += score
                    if i < SHOW_HOLDERS:
                        log(f"    DOWN #{i+1}: {wallet[:15]}... {shares:.0f} shares, PnL=${pnl:.2f}, Bet=${bet:.2f}, Score={score:.2f}")

                log(f"  Total Score: UP={up_total_score:.2f}, DOWN={down_total_score:.2f}")

                # Decide which side to buy
                if up_total_score > down_total_score:
                    chosen_idx = 0
                    chosen_side = "UP"
                    chosen_price = price_up
                    chosen_score = up_total_score
                    other_score = down_total_score
                elif down_total_score > up_total_score:
                    chosen_idx = 1
                    chosen_side = "DOWN"
                    chosen_price = price_down
                    chosen_score = down_total_score
                    other_score = up_total_score
                else:
                    log(f"  ⊘ Equal PnL, skipping")
                    continue

                log(f"  → Whales favor {chosen_side} ({chosen_score:.2f} vs {other_score:.2f})")

                # Only buy if price is below MAX_PRICE
                if chosen_price >= MAX_PRICE:
                    log(f"  ⊘ Price {chosen_price} >= {MAX_PRICE}, too expensive")
                    continue

                chosen_token = tokens[chosen_idx]

                # Place market buy
                try:
                    mo = MarketOrderArgs(
                        token_id=chosen_token,
                        amount=BUY_AMOUNT,
                        side=BUY,
                        order_type=OrderType.FAK
                    )
                    signed = client.create_market_order(mo)
                    resp = client.post_order(signed, OrderType.FAK)

                    # Parse actual shares
                    actual_shares = 0
                    if isinstance(resp, dict):
                        taking = resp.get("takingAmount", "0")
                        actual_shares = float(taking) if taking else 0
                    if actual_shares == 0:
                        actual_shares = BUY_AMOUNT / chosen_price if chosen_price > 0 else 0

                    positions[chosen_token] = {
                        "market": question,
                        "side": chosen_side,
                        "buy_price": chosen_price,
                        "size": actual_shares,
                        "condition_id": condition_id,
                        "outcome_idx": chosen_idx
                    }

                    log(f"  ✓ Bought {actual_shares:.2f} shares of {chosen_side} at {chosen_price}")

                    # Place limit sell in background thread (waits for on-chain settlement)
                    sell_price = min(round(chosen_price + PROFIT_EXIT, 2), 0.99)
                    if actual_shares > 0:
                        def place_limit_sell(token, sp, shares, market_q, side_s):
                            time.sleep(10)
                            try:
                                actual_balance = get_actual_share_balance(token)
                                if actual_balance and actual_balance > 0.1:
                                    sz = math.floor(actual_balance * 100) / 100
                                else:
                                    sz = math.floor(shares * 100) / 100
                                sell_order = OrderArgs(
                                    token_id=token,
                                    price=sp,
                                    size=sz,
                                    side=SELL
                                )
                                signed_sell = client.create_order(sell_order)
                                sell_resp = client.post_order(signed_sell, OrderType.GTC)
                                log(f"  ✓ Limit sell placed: {sz} shares of {side_s} at {sp}")
                            except Exception as e:
                                log(f"  ⚠ Limit sell failed for {side_s}: {e}")
                        threading.Thread(
                            target=place_limit_sell,
                            args=(chosen_token, sell_price, actual_shares, question, chosen_side),
                            daemon=True
                        ).start()

                    record = {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "action": "BUY",
                        "market": question,
                        "side": chosen_side,
                        "token": chosen_token,
                        "price": chosen_price,
                        "amount": BUY_AMOUNT,
                        "up_score": up_total_score,
                        "down_score": down_total_score,
                        "response": str(resp)
                    }
                    with open("whales_trades.json", "a") as f:
                        f.write(json.dumps(record) + "\n")

                except Exception as e:
                    log(f"  ✗ Buy failed: {e}")

            # =================================================================
            # STEP 2: Check for signal reversal on held positions
            # =================================================================
            tokens_to_remove = []

            for token_id, pos in positions.items():
                current_price = get_price(token_id, side=SELL)
                market_name = pos["market"]
                side_name = pos["side"]
                buy_price = pos["buy_price"]
                cid = pos.get("condition_id", "")
                outcome_idx = pos.get("outcome_idx", 0)
                gain = current_price - buy_price

                if not cid:
                    continue

                # STOP LOSS check
                if gain <= -STOP_LOSS:
                    log(f"\n🛑 STOP LOSS: {market_name} {side_name}")
                    log(f"  Price: bought {buy_price} → now {current_price} ({gain:+.2f}), loss exceeds -{STOP_LOSS}")

                    try:
                        # Cancel existing limit sell orders for this token
                        existing_orders = client.get_orders(OpenOrderParams())
                        for order in existing_orders:
                            if order.get("asset_id") == token_id and order.get("side") == "SELL":
                                try:
                                    client.cancel(order.get("id"))
                                    log(f"  ✓ Cancelled limit sell order")
                                except Exception:
                                    pass

                        actual_balance = get_actual_share_balance(token_id)
                        if actual_balance and actual_balance > 0.1:
                            sz = math.floor(actual_balance * 100) / 100
                            sell_order = OrderArgs(
                                token_id=token_id,
                                price=0.01,
                                size=sz,
                                side=SELL
                            )
                            signed = client.create_order(sell_order)
                            resp = client.post_order(signed, OrderType.FOK)
                            log(f"  ✓ Sold {sz} shares at ~{current_price}: {resp}")

                            record = {
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "action": "STOP_LOSS_SELL",
                                "market": market_name,
                                "side": side_name,
                                "token": token_id,
                                "buy_price": buy_price,
                                "sell_price": current_price,
                                "gain": gain,
                                "response": str(resp)
                            }
                            with open("whales_trades.json", "a") as f:
                                f.write(json.dumps(record) + "\n")

                            tokens_to_remove.append(token_id)
                        else:
                            log(f"  ⊘ No balance to sell ({actual_balance})")
                    except Exception as e:
                        log(f"  ✗ Stop loss sell failed: {e}")
                    continue

                # Re-check whale scores
                holders = get_market_top_holders(cid)
                up_holders = holders.get(0, [])[:TOP_HOLDERS]
                down_holders = holders.get(1, [])[:TOP_HOLDERS]

                up_score = 0.0
                for wallet, shares in up_holders:
                    pnl = get_user_pnl(wallet)
                    bet = get_user_bet_on_market(wallet, cid, 0)
                    up_score += pnl * bet

                down_score = 0.0
                for wallet, shares in down_holders:
                    pnl = get_user_pnl(wallet)
                    bet = get_user_bet_on_market(wallet, cid, 1)
                    down_score += pnl * bet

                # Check if signal reversed
                my_score = up_score if outcome_idx == 0 else down_score
                other_score = down_score if outcome_idx == 0 else up_score
                reversed = other_score > my_score

                if reversed and gain >= 0:
                    log(f"\n🔄 SIGNAL REVERSAL: {market_name} {side_name}")
                    log(f"  My side score: {my_score:.2f}, Other side: {other_score:.2f}")
                    log(f"  Price: bought {buy_price} → now {current_price} ({gain:+.2f}), selling at no loss")

                    try:
                        # Cancel existing limit sell orders for this token
                        for order in existing_orders:
                            if order.get("asset_id") == token_id and order.get("side") == "SELL":
                                try:
                                    client.cancel(order.get("id"))
                                    log(f"  ✓ Cancelled limit sell order")
                                except Exception:
                                    pass

                        # Market sell
                        actual_balance = get_actual_share_balance(token_id)
                        if actual_balance and actual_balance > 0.1:
                            sz = math.floor(actual_balance * 100) / 100
                            sell_order = OrderArgs(
                                token_id=token_id,
                                price=0.01,
                                size=sz,
                                side=SELL
                            )
                            signed = client.create_order(sell_order)
                            resp = client.post_order(signed, OrderType.FOK)
                            log(f"  ✓ Sold {sz} shares at ~{current_price}: {resp}")

                            record = {
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "action": "SIGNAL_REVERSAL_SELL",
                                "market": market_name,
                                "side": side_name,
                                "token": token_id,
                                "buy_price": buy_price,
                                "sell_price": current_price,
                                "gain": gain,
                                "my_score": my_score,
                                "other_score": other_score,
                                "response": str(resp)
                            }
                            with open("whales_trades.json", "a") as f:
                                f.write(json.dumps(record) + "\n")

                            tokens_to_remove.append(token_id)
                        else:
                            log(f"  ⊘ No balance to sell ({actual_balance})")
                    except Exception as e:
                        log(f"  ✗ Reversal sell failed: {e}")

            for token_id in tokens_to_remove:
                del positions[token_id]

            # Status line
            if positions:
                log(f"\n📊 Monitoring {len(positions)} positions:")
                for token_id, pos in positions.items():
                    cp = get_price(token_id, side=SELL)
                    gain = cp - pos["buy_price"]
                    target = round(pos["buy_price"] + PROFIT_EXIT, 2)
                    status = "📈" if gain >= 0 else "📉"
                    log(f"  {status} {pos['market']} {pos['side']}: bought {pos['buy_price']} → now {cp} ({gain:+.2f}), sell@{target}")

            time.sleep(POLL_INTERVAL)

        except Exception as e:
            log(f"Error: {e}")
            time.sleep(10)


if __name__ == "__main__":
    run_whales()
