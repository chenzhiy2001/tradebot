import os
import time
from py_clob_client import MarketOrderArgs, PartialCreateOrderOptions
import requests
import json
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, OpenOrderParams
from py_clob_client.order_builder.constants import BUY
from private_key import private_key,founder_address  # Importing from local file
# Configuration
HOST = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
CHAIN_ID = 137  # Polygon mainnet
PRIVATE_KEY = private_key
FUNDER_ADDRESS = founder_address  # Your Polymarket proxy wallet

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
                
                # Extract token IDs from filled trades for the "both sides filled" check
                filled_trades_tokens = set()
                for trade in filled_trades:
                    token_id = trade.get("asset_id")
                    if token_id:
                        filled_trades_tokens.add(token_id)
                
                # Check if we already have filled trades on BOTH sides (up and down)
                # tokens[0] = UP token ID, tokens[1] = DOWN token ID
                # filled_trades_tokens = set of all token IDs we've ever successfully traded
                # This check returns True only if BOTH token IDs are in the set
                # (meaning we've filled at least one UP trade AND at least one DOWN trade)
                # Examples:
                #   - 2 UP trades + 0 DOWN = False (won't skip)
                #   - 0 UP + 2 DOWN = False (won't skip)
                #   - 1 UP + 1 DOWN = True (will skip)
                if len(tokens) == 2:
                    both_sides_filled = all(token in filled_trades_tokens for token in tokens)
                    if both_sides_filled:
                        print(f"⊘ Skipping {question} (both sides already filled)")
                        continue
                
                print(f"Processing market: {question}")
                for token in tokens:
                    if buy_count >= max_buys:
                        break
                    
                    price_data = client.get_price(token, side=BUY)
                    price = float(price_data.get("price", 0))
                    if price > threshold:
                        # Check if we already have an open order OR filled trade with same token, price, and amount
                        # (This prevents duplicate orders from main.py)
                        has_duplicate = False
                        
                        # Check recent buys in this session first (most reliable)
                        for recent_token, recent_price, recent_amount in recent_buys:
                            if (recent_token == token and 
                                abs(recent_price - price) < 0.01 and
                                abs(recent_amount - buy_amount) < 0.1):
                                has_duplicate = True
                                print(f"⊘ Skipping - already bought in this session at {price} for ${buy_amount}")
                                break
                        
                        # Check open orders
                        if not has_duplicate:
                            for order in existing_orders:
                                if (order.get("asset_id") == token and 
                                    abs(float(order.get("price", 0)) - price) < 0.01 and
                                    abs(float(order.get("original_size", 0)) * price - buy_amount) < 0.1):
                                    has_duplicate = True
                                    print(f"⊘ Skipping - already have OPEN order at {price} for ${buy_amount}")
                                    break
                        
                        # Check filled trades (recent ones)
                        if not has_duplicate:
                            for trade in filled_trades:
                                if (trade.get("asset_id") == token and 
                                    abs(float(trade.get("price", 0)) - price) < 0.01 and
                                    abs(float(trade.get("size", 0)) * float(trade.get("price", 0)) - buy_amount) < 0.1):
                                    has_duplicate = True
                                    print(f"⊘ Skipping - already have FILLED trade at {price} for ${buy_amount}")
                                    break
                        
                        if has_duplicate:
                            continue
                            
                        print(f"Price {price} > {threshold}, attempting to buy ${buy_amount}")
                        
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
                                "timestamp": datetime.utcnow().isoformat(),
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
                            
                            print(f"✓ Order successful ({buy_count}/{max_buys})")
                        except Exception as e:
                            print(f"✗ Order failed: {e}")

            time.sleep(5)  # Poll every 5 seconds
            # refresh terminal output
            os.system('cls' if os.name == 'nt' else 'clear')
            
            
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    # Run bot for 15-minute crypto markets (stops after 10 buys)
    run_bot(threshold=0.9, buy_amount=50, max_buys=15)