import json
from collections import defaultdict

with open("strategy_trades.json") as f:
    trades = [json.loads(line) for line in f if line.strip()]

print(f"Total trades: {len(trades)}")
print(f"Total spent: ${sum(t['amount'] for t in trades)}")

# Which wallets triggered most trades?
wallet_trigger_count = defaultdict(int)
for t in trades:
    side = t["side"]
    detected = t.get("tracked_up" if side == "UP" else "tracked_down", [])
    for tag, shares in detected:
        wallet_trigger_count[tag] += 1

print("\n--- Which wallets triggered trades ---")
for tag, count in sorted(wallet_trigger_count.items(), key=lambda x: -x[1]):
    print(f"  {tag}: {count} trades")

# Timeline: bucket trades into 2-hour blocks and show count
print("\n--- Trade count per 2-hour block ---")
from datetime import datetime
hour_blocks = defaultdict(int)
for t in trades:
    ts = t["timestamp"][:13]  # YYYY-MM-DDTHH
    hour_blocks[ts] += 1
for ts, count in sorted(hour_blocks.items()):
    print(f"  {ts}: {count} trades")

# Price distribution
print("\n--- Buy price distribution ---")
price_buckets = defaultdict(int)
for t in trades:
    bucket = round(t["price"], 1)
    price_buckets[bucket] += 1
for p, count in sorted(price_buckets.items()):
    print(f"  {p:.1f}: {count} trades")

# Average price
avg_price = sum(t["price"] for t in trades) / len(trades)
print(f"\nAvg buy price: {avg_price:.2f}")
print(f"Break-even WR needed: {avg_price/(avg_price + (1-avg_price))*100:.1f}%")

# $20 trades vs $10 trades
by_amount = defaultdict(int)
for t in trades:
    by_amount[t["amount"]] += 1
print(f"\nBy trade size: {dict(by_amount)}")

