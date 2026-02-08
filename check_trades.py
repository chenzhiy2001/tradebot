import os
from py_clob_client.client import ClobClient
from dotenv import load_dotenv

load_dotenv()
private_key = os.getenv("PRIVATE_KEY")
founder_address = os.getenv("FUNDER_ADDRESS")

HOST = "https://clob.polymarket.com"
CHAIN_ID = 137

client = ClobClient(
    host=HOST,
    key=private_key,
    chain_id=CHAIN_ID,
    signature_type=1,
    funder=founder_address
)
client.set_api_creds(client.create_or_derive_api_creds())

trades = client.get_trades()
print(f"Total trades returned: {len(trades)}")

if trades:
    print("\nFirst trade:")
    first_trade = trades[0]
    for key, value in first_trade.items():
        print(f"  {key}: {value}")
    
    print("\n\nChecking if 'side' or 'maker_address' exists:")
    print(f"  Has 'side': {'side' in first_trade}")
    print(f"  Has 'maker_address': {'maker_address' in first_trade}")
    print(f"  Has 'taker_address': {'taker_address' in first_trade}")
    print(f"  Your address: {founder_address}")
