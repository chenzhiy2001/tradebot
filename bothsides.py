import os
import time
from py_clob_client import MarketOrderArgs, PartialCreateOrderOptions
import requests
import json
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, OpenOrderParams
from py_clob_client.order_builder.constants import BUY, SELL
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta

load_dotenv()
private_key = os.getenv("PRIVATE_KEY")
founder_address = os.getenv("FUNDER_ADDRESS")
from config import limit_price

# Configuration
HOST = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
CHAIN_ID = 137  # Polygon mainnet
PRIVATE_KEY = private_key
FUNDER_ADDRESS = founder_address
LIMIT_PRICE = limit_price
ORDER_SIZE = 30  # Total size per market (split between both sides)

# Initialize client
client = ClobClient(
    host=HOST,
    key=PRIVATE_KEY,
    chain_id=CHAIN_ID,
    signature_type=1,
    funder=FUNDER_ADDRESS
)
client.set_api_creds(client.create_or_derive_api_creds())

def get_future_15min_crypto_markets():
    """Fetch 15-minute crypto markets that start after now+2 minutes"""
    markets = []
    now = datetime.now(timezone.utc)
    cutoff_time = now + timedelta(minutes=2)
    
    # Start from the current 15-minute window
    minutes = (now.minute // 15) * 15
    start_window = now.replace(minute=minutes, second=0, microsecond=0)
    
    cryptos = ["btc", "eth", "sol", "xrp"]
    
    # Check next 48 fifteen-minute windows (12 hours)
    for i in range(48):
        window_start = start_window + timedelta(minutes=15 * i)
        window_end = window_start + timedelta(minutes=15)
        epoch = int(window_start.timestamp())  # Slug uses START time (eventStartTime)
        
        # Skip if this window starts before cutoff
        if window_start < cutoff_time:
            continue
        
        for crypto in cryptos:
            slug = f"{crypto}-updown-15m-{epoch}"
            try:
                response = requests.get(f"{GAMMA_API}/events/slug/{slug}")
                if response.status_code == 200:
                    event = response.json()
                    for market in event.get("markets", []):
                        if not market.get("closed"):
                            # Add market with start time info
                            market["start_time"] = window_start.isoformat()
                            market["end_time"] = window_end.isoformat()
                            markets.append(market)
            except Exception as e:
                pass  # Silently skip missing markets
    
    return markets

def get_existing_orders_tokens():
    """Query Polymarket for tokens we already have open orders on"""
    try:
        open_orders = client.get_orders(OpenOrderParams())
        # Extract token IDs from open orders
        token_ids = set()
        for order in open_orders:
            token_id = order.get("asset_id")
            if token_id:
                token_ids.add(token_id)
        return token_ids
    except Exception as e:
        print(f"Warning: Failed to fetch open orders: {e}")
        return set()

def save_trade_log(market_question, tokens, start_time):
    """Save trade record to log file (write-only)"""
    record = {
        "timestamp": datetime.utcnow().isoformat(),
        "market": market_question,
        "tokens": tokens,
        "start_time": start_time,
        "limit_price": LIMIT_PRICE,
        "size": ORDER_SIZE
    }
    with open("bothsides_trades.json", "a") as f:
        f.write(json.dumps(record) + "\n")

def place_bothside_orders(max_markets=10):
    # Query Polymarket for existing orders
    print("Checking existing open orders...")
    existing_order_tokens = get_existing_orders_tokens()
    print(f"Found {len(existing_order_tokens)} tokens with open orders")
    
    print("Fetching future 15-minute crypto markets...")
    markets = get_future_15min_crypto_markets()
    print(f"Found {len(markets)} future markets")
    
    orders_placed = 0
    markets_traded = 0
    
    for market in markets:
        if markets_traded >= max_markets:
            print(f"\n✓ Reached limit of {max_markets} markets. Stopping.")
            break
            
        question = market.get("question", "N/A")
        start_time = market.get("start_time", "N/A")
        
        tokens_string = market.get("clobTokenIds", "[]")
        tokens = json.loads(tokens_string)
        
        if len(tokens) != 2:
            print(f"   ⚠ Expected 2 tokens, got {len(tokens)}, skipping")
            continue
        
        # Check if we already have orders on any token in this market
        has_existing_order = any(token in existing_order_tokens for token in tokens)
        if has_existing_order:
            print(f"⊘ Skipping {question} (already has open orders)")
            continue
        
        print(f"\n📊 Market: {question}")
        print(f"   Starts: {start_time}")
        
        # Place orders on both tokens (up and down)
        success = True
        placed_tokens = []
        
        for i, token in enumerate(tokens):
            side_name = "UP" if i == 0 else "DOWN"
            try:
                order = OrderArgs(
                    token_id=token,
                    price=LIMIT_PRICE,
                    size=ORDER_SIZE,
                    side=BUY
                )
                signed = client.create_order(order)
                resp = client.post_order(signed, OrderType.GTC)
                print(f"   ✓ {side_name} order placed: {resp}")
                placed_tokens.append(token)
                orders_placed += 1
            except Exception as e:
                print(f"   ✗ {side_name} order failed: {e}")
                success = False
                break
        
        if success:
            # Log to file
            save_trade_log(question, placed_tokens, start_time)
            markets_traded += 1
            print(f"   ✓ Both sides placed successfully ({markets_traded}/{max_markets})")
        else:
            print(f"   ⚠ Incomplete - only some orders placed")
    
    print(f"\n{'='*60}")
    print(f"Markets traded: {markets_traded}/{max_markets}")
    print(f"Total orders placed: {orders_placed}")
    print(f"{'='*60}")

if __name__ == "__main__":
    place_bothside_orders(max_markets=16*12)  # Check 12 hours of markets (16 per hour)