import os
import time
from py_clob_client import MarketOrderArgs, PartialCreateOrderOptions
import requests
import json
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
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

def run_bot(threshold=0.80, buy_amount=1.0, max_buys=10):
    """Main bot loop: buy $1 when price > 80%"""
    traded_tokens = set()  # Track tokens we've already bought
    buy_count = 0  # Track number of successful buys
    
    while buy_count < max_buys:
        try:
            markets = get_15min_crypto_markets()
            for market in markets:
                if buy_count >= max_buys:
                    print(f"\n✓ Reached {max_buys} successful buys. Stopping bot.")
                    return
                    
                question = market.get("question", "N/A")
                print(f"Processing market: {question}")
                tokens_string = market.get("clobTokenIds", "[]") # clobTokenIds is a STRING representing a list of token IDs
                tokens = json.loads(tokens_string)
                for token in tokens:
                    if buy_count >= max_buys:
                        break
                        
                    # Skip if we've already bought this token
                    if token in traded_tokens:
                        continue
                        
                    # Get current price
                    price_resp = client.get_price(token, "BUY")
                    price = float(price_resp.get("price", 0))
                    print(f"Current price: {price}")
                    if price > threshold:
                        print(f"Price {price} > {threshold}, attempting to buy ${buy_amount}")
                        
                        # Use create_and_post_order - properly handles signature_type=1
                        try:
                            from datetime import datetime
                            mo = MarketOrderArgs(token_id=token, amount=buy_amount, side=BUY, order_type=OrderType.FAK)  # Get a token ID: https://docs.polymarket.com/developers/gamma-markets-api/get-markets
                            signed = client.create_market_order(mo)
                            resp = client.post_order(signed, OrderType.FAK)
                            
                            buy_count += 1
                            trade_record = {
                                "timestamp": datetime.utcnow().isoformat(),
                                "market": question,
                                "token_id": token,
                                "price": price,
                                "amount": buy_amount,
                                "buy_number": buy_count,
                                "response": str(resp)
                            }
                            
                            # Save to file
                            with open("trades.json", "a") as f:
                                f.write(json.dumps(trade_record) + "\n")
                            
                            print(f"✓ Order successful ({buy_count}/{max_buys})")
                            # Mark this token as traded
                            traded_tokens.add(token)
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
    run_bot(threshold=0.9, buy_amount=10, max_buys=15)