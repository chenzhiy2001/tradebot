import os
import time
from py_clob_client import MarketOrderArgs, PartialCreateOrderOptions
import requests
import json
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL
from private_key import private_key, founder_address
from datetime import datetime, timezone, timedelta

# Configuration
HOST = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
CHAIN_ID = 137  # Polygon mainnet
PRIVATE_KEY = private_key
FUNDER_ADDRESS = founder_address

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
    """Fetch 15-minute crypto markets that start after now+10 minutes"""
    markets = []
    now = datetime.now(timezone.utc)
    cutoff_time = now + timedelta(minutes=10)
    
    # Check markets starting from now up to 2 hours ahead
    start_window = now.replace(second=0, microsecond=0)
    # Round to next 15-minute mark
    minutes = ((start_window.minute // 15) + 1) * 15
    if minutes >= 60:
        start_window = start_window.replace(hour=start_window.hour + 1, minute=0)
    else:
        start_window = start_window.replace(minute=minutes)
    
    cryptos = ["btc", "eth", "sol", "xrp"]
    
    # Check next 8 fifteen-minute windows (2 hours)
    for i in range(8):
        window = start_window + timedelta(minutes=15 * i)
        epoch = int(window.timestamp())
        
        # Skip if this window starts before cutoff
        if window < cutoff_time:
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
                            market["start_time"] = window.isoformat()
                            markets.append(market)
            except Exception as e:
                pass  # Silently skip missing markets
    
    return markets

def load_traded_markets():
    """Load set of market IDs we've already placed orders on"""
    traded = set()
    if os.path.exists("bothsides_trades.json"):
        with open("bothsides_trades.json", "r") as f:
            for line in f:
                try:
                    record = json.loads(line.strip())
                    # Use question as unique ID
                    traded.add(record.get("market"))
                except:
                    pass
    return traded

def save_trade(market_question, tokens, start_time):
    """Save trade record to file"""
    record = {
        "timestamp": datetime.utcnow().isoformat(),
        "market": market_question,
        "tokens": tokens,
        "start_time": start_time,
        "limit_price": 0.4,
        "size": 25
    }
    with open("bothsides_trades.json", "a") as f:
        f.write(json.dumps(record) + "\n")

def place_bothside_orders(max_markets=10):
    """Place limit orders on both sides of future markets"""
    traded_markets = load_traded_markets()
    
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
        
        # Skip if already traded this market
        if question in traded_markets:
            print(f"⊘ Skipping {question} (already traded)")
            continue
        
        print(f"\n📊 Market: {question}")
        print(f"   Starts: {start_time}")
        
        tokens_string = market.get("clobTokenIds", "[]")
        tokens = json.loads(tokens_string)
        
        if len(tokens) != 2:
            print(f"   ⚠ Expected 2 tokens, got {len(tokens)}, skipping")
            continue
        
        # Place orders on both tokens (up and down)
        success = True
        placed_tokens = []
        
        for i, token in enumerate(tokens):
            side_name = "UP" if i == 0 else "DOWN"
            try:
                order = OrderArgs(
                    token_id=token,
                    price=0.4,
                    size=25.0,
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
            # Save to file
            save_trade(question, placed_tokens, start_time)
            markets_traded += 1
            print(f"   ✓ Both sides placed successfully ({markets_traded}/{max_markets})")
        else:
            print(f"   ⚠ Incomplete - only some orders placed")
    
    print(f"\n{'='*60}")
    print(f"Markets traded: {markets_traded}/{max_markets}")
    print(f"Total orders placed: {orders_placed}")
    print(f"{'='*60}")

if __name__ == "__main__":
    place_bothside_orders(max_markets=10)