import os
import time
from py_clob_client import MarketOrderArgs, PartialCreateOrderOptions
import requests
import json
from datetime import datetime, timezone, timedelta
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, OpenOrderParams
from py_clob_client.order_builder.constants import BUY
from private_key import private_key,founder_address  # Importing from local file
from config import limit_price

# Configuration
HOST = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
CHAIN_ID = 137  # Polygon mainnet
PRIVATE_KEY = private_key
FUNDER_ADDRESS = founder_address  # Your Polymarket proxy wallet

# Trading parameters
LIMIT_PRICE = limit_price  # Price used in bothsides.py for limit orders (should match bothsides.py)

# Log file for debugging decisions
DECISION_LOG = "decision_log.txt"

def log_decision(message):
    """Write decision to log file for debugging"""
    timestamp = datetime.now(timezone.utc).isoformat()
    with open(DECISION_LOG, "a") as f:
        f.write(f"[{timestamp}] {message}\n")
    print(message)

# Initialize client (use signature_type=2 for browser wallet, 1 for Magic/email)
client = ClobClient(
    host=HOST,
    key=PRIVATE_KEY,
    chain_id=CHAIN_ID,
    signature_type=1,
    funder=FUNDER_ADDRESS
)
client.set_api_creds(client.create_or_derive_api_creds())

def get_15min_crypto_tag_id():
    """Discover the tag_id for 15-minute crypto markets"""
    tags = requests.get(f"{GAMMA_API}/tags?limit=200").json()
    for tag in tags:
        # Look for tags related to 15-min crypto (adjust search as needed)
        if "crypto" in tag.get("label", "").lower() or "15" in tag.get("label", ""):
            print(f"Found tag: {tag}")
    return None  # You'll need to identify the correct tag_id

def get_15min_crypto_markets():
    """Fetch active 15-minute crypto markets using slug pattern"""
    from datetime import datetime, timezone
    
    markets = []
    # Calculate current and next 15-minute window timestamps
    now = datetime.now(timezone.utc)
    # Round down to nearest 15 minutes
    minutes = (now.minute // 15) * 15
    current_window = now.replace(minute=minutes, second=0, microsecond=0)
    current_epoch = int(current_window.timestamp())
    
    # Also get next window
    from datetime import timedelta
    next_window = current_window + timedelta(minutes=15)
    next_epoch = int(next_window.timestamp())
    
    # Crypto slugs: btc, eth, sol, xrp
    cryptos = ["btc", "eth", "sol", "xrp"]
    
    for epoch in [current_epoch, next_epoch]:
        for crypto in cryptos:
            slug = f"{crypto}-updown-15m-{epoch}"
            try:
                response = requests.get(f"{GAMMA_API}/events/slug/{slug}")
                if response.status_code == 200:
                    event = response.json()
                    # Get markets from the event
                    for market in event.get("markets", []):
                        if not market.get("closed"):
                            markets.append(market)
            except Exception as e:
                print(f"Error fetching {slug}: {e}")
    
    return markets

def get_existing_orders():
    """Query Polymarket for all open orders with details"""
    try:
        open_orders = client.get_orders(OpenOrderParams())
        return open_orders
    except Exception as e:
        print(f"Warning: Failed to fetch open orders: {e}")
        return []

def get_filled_trades():
    """Query Polymarket for filled trades with details"""
    try:
        trades = client.get_trades()
        return trades
    except Exception as e:
        print(f"Warning: Failed to fetch trades: {e}")
        return []

def run_bot(threshold=0.80, buy_amount=1.0, max_buys=10):
    """Main bot loop: buy $1 when price > 80%"""
    buy_count = 0  # Track number of successful buys
    recent_buys = []  # Track buys in this session (token, price, amount)
    
    while buy_count < max_buys:
        try:
            markets = get_15min_crypto_markets()
            for market in markets:
                if buy_count >= max_buys:
                    print(f"\n✓ Reached {max_buys} successful buys. Stopping bot.")
                    return
                    
                question = market.get("question", "N/A")
                tokens_string = market.get("clobTokenIds", "[]") # clobTokenIds is a STRING representing a list of token IDs
                tokens = json.loads(tokens_string)
                
                # Fetch fresh data for each market
                existing_orders = get_existing_orders()
                filled_trades = get_filled_trades()
                
                # Build sets for checking
                # 1. filled_limit_tokens: YOUR trades near LIMIT_PRICE (from bothsides.py)
                # 2. filled_any_tokens: ALL YOUR trades for this market's tokens (any price)
                filled_limit_tokens = set()
                filled_any_tokens = set()
                
                log_decision(f"\n=== Checking market: {question} ===")
                log_decision(f"Market tokens: {tokens}")
                log_decision(f"Total filled trades from API: {len(filled_trades)}")
                
                for trade in filled_trades:
                    trade_maker = trade.get("maker_address", "")
                    trade_token = str(trade.get("asset_id", ""))  # Convert to string for comparison
                    trade_price = float(trade.get("price", 0))
                    
                    # Only count YOUR trades (check both maker_address for limit orders)
                    is_your_trade = trade_maker.lower() == FUNDER_ADDRESS.lower()
                    
                    # Log trades for this market's tokens
                    if trade_token in tokens:
                        log_decision(f"  Found trade for market token: price={trade_price}, maker={trade_maker[:10]}..., is_yours={is_your_trade}")
                        if is_your_trade:
                            filled_any_tokens.add(trade_token)
                            log_decision(f"  ✓ Added {trade_token[:20]}... to filled_any_tokens (price {trade_price})")
                    
                    if not is_your_trade:
                        continue
                    
                    # Only count trades near LIMIT_PRICE (limit orders from bothsides.py)
                    if trade_token and abs(trade_price - LIMIT_PRICE) < 0.05:
                        filled_limit_tokens.add(trade_token)
                        log_decision(f"  ✓ Added to filled_limit_tokens: {trade_token[:20]}... at price {trade_price}")
                
                log_decision(f"Filled limit tokens count: {len(filled_limit_tokens)}")
                log_decision(f"Filled ANY tokens for this market: {len(filled_any_tokens)}")
                log_decision(f"Tokens in market: {[t[:20] + '...' for t in tokens]}")
                log_decision(f"Tokens in filled_limit: {[t[:20] + '...' for t in filled_limit_tokens]}")
                log_decision(f"Tokens in filled_any: {[t[:20] + '...' for t in filled_any_tokens]}")
                
                # =============================================================================
                # BOTH-SIDES-FILLED DETECTION
                # =============================================================================
                # We want to SKIP buying if bothsides.py already filled BOTH the UP and DOWN
                # limit orders for this market (meaning we already profited from volatility).
                #
                # PROBLEM: The Polymarket API has delays - trades may be filled but not yet
                # returned by get_trades(). We've seen cases where:
                # - Website shows: "Bought 8 Up at 40¢" and "Bought 8 Down at 40¢" 
                # - But API returns: YOUR Filled trades: None
                # - Result: Bot incorrectly buys at 92¢ when it shouldn't
                #
                # SOLUTION: Use 3 independent detection methods:
                # 1. filled_limit_tokens: Check if API shows fills at ~40¢ for both tokens
                # 2. filled_any_tokens: Check if API shows ANY fills for both tokens  
                # 3. open limit orders: If NEITHER token has an open 40¢ order, both were filled
                #    (because bothsides.py always places orders on both sides)
                #
                # If ANY of these 3 methods detect both sides filled, we skip the market.
                # =============================================================================
                if len(tokens) == 2:
                    # METHOD 1: Check API for fills at limit price (~40¢)
                    token0_filled = tokens[0] in filled_limit_tokens
                    token1_filled = tokens[1] in filled_limit_tokens
                    both_sides_filled = token0_filled and token1_filled
                    
                    # METHOD 2: Check API for ANY fills on these tokens (any price)
                    token0_any = tokens[0] in filled_any_tokens
                    token1_any = tokens[1] in filled_any_tokens
                    both_sides_any_fill = token0_any and token1_any
                    
                    # METHOD 3: Check if open limit orders are MISSING
                    # If bothsides.py placed orders and now they're gone = filled
                    # This catches the API delay issue where trades don't show yet
                    token0_has_limit_order = any(
                        o.get("asset_id") == tokens[0] and abs(float(o.get("price", 0)) - LIMIT_PRICE) < 0.05
                        for o in existing_orders
                    )
                    token1_has_limit_order = any(
                        o.get("asset_id") == tokens[1] and abs(float(o.get("price", 0)) - LIMIT_PRICE) < 0.05
                        for o in existing_orders
                    )
                    neither_has_limit_order = not token0_has_limit_order and not token1_has_limit_order
                    
                    log_decision(f"Token[0] ({tokens[0][:20]}...) filled at limit: {token0_filled}, any fill: {token0_any}, has limit order: {token0_has_limit_order}")
                    log_decision(f"Token[1] ({tokens[1][:20]}...) filled at limit: {token1_filled}, any fill: {token1_any}, has limit order: {token1_has_limit_order}")
                    log_decision(f"Both filled at limit: {both_sides_filled}, both any fill: {both_sides_any_fill}, neither has limit order: {neither_has_limit_order}")
                    
                    # SKIP if ANY detection method triggers:
                    # - both_sides_filled: API confirms both 40¢ orders filled
                    # - both_sides_any_fill: API shows trades on both tokens (any price)
                    # - neither_has_limit_order: No open orders = both were filled (API delay workaround)
                    if both_sides_filled or both_sides_any_fill or neither_has_limit_order:
                        if both_sides_filled:
                            reason = "both limit price orders filled"
                        elif both_sides_any_fill:
                            reason = "both sides have trades"
                        else:
                            reason = "no open limit orders (both likely filled)"
                        log_decision(f"⊘ SKIPPING {question} ({reason})")
                        continue
                    else:
                        log_decision(f"→ PROCEEDING with {question}")
                
                log_decision(f"Processing market: {question}")
                for token in tokens:
                    if buy_count >= max_buys:
                        break
                    
                    price_data = client.get_price(token, side=BUY)
                    price = float(price_data.get("price", 0))
                    
                    # Show all orders and trades for this token
                    log_decision(f"\n  Token: {token}")
                    log_decision(f"  Current price: {price}")
                    
                    # Show open orders for this token
                    token_open_orders = [o for o in existing_orders if o.get("asset_id") == token]
                    if token_open_orders:
                        log_decision(f"  Open orders ({len(token_open_orders)}):")
                        for order in token_open_orders:
                            log_decision(f"    - Price: {order.get('price')}, Size: {order.get('original_size')}, Total: ${float(order.get('original_size', 0)) * float(order.get('price', 0)):.2f}")
                    else:
                        log_decision(f"  Open orders: None")
                    
                    # Show filled trades for this token
                    token_filled_trades = [t for t in filled_trades if str(t.get("asset_id")) == token and t.get("maker_address", "").lower() == FUNDER_ADDRESS.lower()]
                    if token_filled_trades:
                        log_decision(f"  YOUR Filled trades ({len(token_filled_trades)}):")
                        for trade in token_filled_trades:
                            log_decision(f"    - Price: {trade.get('price')}, Size: {trade.get('size')}, Total: ${float(trade.get('size', 0)) * float(trade.get('price', 0)):.2f}")
                    else:
                        log_decision(f"  YOUR Filled trades: None")
                    
                    if price > threshold:
                        # Check if we already have an open order OR filled trade with same token, price, and amount
                        # (This prevents duplicate orders from main.py)
                        has_duplicate = False
                        
                        # Check recent buys in this session first (most reliable)
                        for recent_token, recent_price, recent_amount in recent_buys:
                            if (recent_token == token and 
                                abs(recent_price - price) < 0.1 and
                                abs(recent_amount - buy_amount) < 0.1):
                                has_duplicate = True
                                log_decision(f"⊘ Skipping - already bought in this session at {price} for ${buy_amount}")
                                break
                        
                        # Check open orders
                        if not has_duplicate:
                            for order in existing_orders:
                                if (order.get("asset_id") == token and 
                                    abs(float(order.get("price", 0)) - price) < 0.1 and
                                    abs(float(order.get("original_size", 0)) * price - buy_amount) < 0.1):
                                    has_duplicate = True
                                    log_decision(f"⊘ Skipping - already have OPEN order at {price} for ${buy_amount}")
                                    break
                        
                        # Check filled trades (recent ones)
                        if not has_duplicate:
                            for trade in filled_trades:
                                if (str(trade.get("asset_id")) == token and 
                                    abs(float(trade.get("price", 0)) - price) < 0.1 and
                                    abs(float(trade.get("size", 0)) * float(trade.get("price", 0)) - buy_amount) < 0.1):
                                    has_duplicate = True
                                    log_decision(f"⊘ Skipping - already have FILLED trade at {price} for ${buy_amount}")
                                    break
                        
                        if has_duplicate:
                            continue
                            
                        log_decision(f"Price {price} > {threshold}, attempting to buy ${buy_amount}")
                        log_decision(f"DECISION: BUY token {token[:20]}... at price {price}")
                        
                        # Use create_and_post_order - properly handles signature_type=1
                        try:
                            from datetime import datetime
                            mo = MarketOrderArgs(token_id=token, amount=buy_amount, side=BUY, order_type=OrderType.FAK)  # Get a token ID: https://docs.polymarket.com/developers/gamma-markets-api/get-markets
                            signed = client.create_market_order(mo)
                            resp = client.post_order(signed, OrderType.FAK)
                            
                            buy_count += 1
                            # Add to recent buys cache immediately
                            recent_buys.append((token, price, buy_amount))
                            trade_record = {
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "market": question,
                                "token_id": token,
                                "price": price,
                                "amount": buy_amount,
                                "buy_number": buy_count,
                                "response": str(resp)
                            }
                            
                            # Log to file (write-only)
                            with open("trades.json", "a") as f:
                                f.write(json.dumps(trade_record) + "\n")
                            
                            log_decision(f"✓ Order successful ({buy_count}/{max_buys})")
                        except Exception as e:
                            log_decision(f"✗ Order failed: {e}")

            # time.sleep(3)  # Poll every 5 seconds
            # refresh terminal output
            os.system('cls' if os.name == 'nt' else 'clear')
            
            
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    # Run bot for 15-minute crypto markets (stops after 10 buys)
    run_bot(threshold=0.9, buy_amount=100, max_buys=888)