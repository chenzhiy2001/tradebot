"""Analyze trade log to understand P&L."""
import json

trades = []
with open("flow_trades.json") as f:
    for line in f:
        line = line.strip()
        if line:
            trades.append(json.loads(line))

buys = [t for t in trades if t["action"] == "BUY"]
exits = [t for t in trades if t["action"] in ("FLOW_REVERSAL", "LIMIT_SELL", "EXPIRED")]

print(f"Total entries: {len(trades)}")
print(f"BUY entries: {len(buys)}")
print(f"Exit entries: {len(exits)}")
for e in exits:
    print(f"  {e['action']}: {e['market']} {e['side']} buy@{e.get('buy_price','?')} sell@{e.get('sell_price','?')} pnl={e.get('pnl','?')}")
print()

total_cost = sum(t["amount"] for t in buys)
print(f"Total invested: ${total_cost:.2f}")
print()

print("=== ALL BUYS ===")
for t in buys:
    up_net = t.get("up_net", 0)
    down_net = t.get("down_net", 0)
    side = t["side"]
    chosen_net = up_net if side == "UP" else down_net
    other_net = down_net if side == "UP" else up_net
    ratio = t.get("ratio", 0)
    
    # Flag issues
    flags = []
    if other_net < 0 and abs(other_net) > chosen_net:
        flags.append("OTHER_SIDE_HUGE_NEGATIVE")
    if ratio < 3:
        flags.append(f"LOW_RATIO")
    if t["trade_count"] < 20:
        flags.append("FEW_TRADES")
    
    flag_str = " ⚠️ " + ",".join(flags) if flags else ""
    print(f"  {t['timestamp'][11:19]} {t['crypto']:3s} {t['interval']:2d}m {side:4s} @{t['price']:.2f} "
          f"${t['amount']:.0f}  flow=${chosen_net:+,.0f} other=${other_net:+,.0f} "
          f"ratio={ratio:.1f}x  trades={t['trade_count']}{flag_str}")

# Check the massive negative other-side pattern
print("\n=== PATTERN ANALYSIS ===")
neg_other = [t for t in buys if (
    (t["side"] == "UP" and t.get("down_net", 0) < -100) or
    (t["side"] == "DOWN" and t.get("up_net", 0) < -100)
)]
print(f"Buys where other side net < -$100: {len(neg_other)}/{len(buys)}")
for t in neg_other:
    up_net = t.get("up_net", 0)
    down_net = t.get("down_net", 0)
    side = t["side"]
    chosen_net = up_net if side == "UP" else down_net
    other_net = down_net if side == "UP" else up_net
    print(f"  {t['crypto']} {t['interval']}m {side} @{t['price']:.2f}: "
          f"chosen=${chosen_net:+,.0f} other=${other_net:+,.0f}")
