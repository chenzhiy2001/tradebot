import json
from collections import Counter

buys = []
sells = []
with open('whales_trades.json') as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
            if r['action'] == 'BUY':
                buys.append(r)
            else:
                sells.append(r)
        except:
            pass

print(f'Total BUYs: {len(buys)}')
print(f'Total SELLs: {len(sells)}')

sl = [s for s in sells if s['action'] == 'STOP_LOSS_SELL']
sr = [s for s in sells if s['action'] == 'SIGNAL_REVERSAL_SELL']
print(f'  Stop losses: {len(sl)}')
print(f'  Signal reversals: {len(sr)}')

# P&L from sells
sl_pnl = 0
for s in sl:
    bp = s.get('buy_price', 0)
    sp = s.get('sell_price', 0)
    if bp > 0:
        shares = 10.0 / bp
        sl_pnl += (sp - bp) * shares

sr_pnl = 0
for s in sr:
    bp = s.get('buy_price', 0)
    sp = s.get('sell_price', 0)
    if bp > 0:
        shares = 10.0 / bp
        sr_pnl += (sp - bp) * shares

print(f'\nStop loss P&L: ${sl_pnl:.2f}')
print(f'Signal reversal P&L: ${sr_pnl:.2f}')
print(f'Total realized P&L: ${sl_pnl + sr_pnl:.2f}')
if sl:
    print(f'Avg loss per stop loss: ${sl_pnl / len(sl):.2f}')

# Re-entries per market
market_buys = Counter()
for b in buys:
    market_buys[b['market']] += 1

print(f'\nRe-entry counts (same market):')
for m, c in market_buys.most_common(15):
    if c > 1:
        print(f'  {c}x: {m}')

# Buy price distribution
prices = [b['price'] for b in buys]
high = [p for p in prices if p >= 0.70]
mid = [p for p in prices if 0.40 <= p < 0.70]
low = [p for p in prices if p < 0.40]
print(f'\nBuy price distribution:')
print(f'  >= 0.70 (expensive): {len(high)} trades')
print(f'  0.40-0.69 (mid):     {len(mid)} trades')
print(f'  < 0.40 (cheap):      {len(low)} trades')

# Show the worst re-entry loops
print(f'\nWorst re-entry sequences:')
for m, c in market_buys.most_common():
    if c <= 2:
        continue
    events = []
    for s in sl:
        if s['market'] == m:
            events.append(('SL', s['timestamp'], s.get('sell_price', 0), s.get('gain', 0)))
    for s in sr:
        if s['market'] == m:
            events.append(('REV', s['timestamp'], s.get('sell_price', 0), s.get('gain', 0)))
    for b in buys:
        if b['market'] == m:
            events.append(('BUY', b['timestamp'], b.get('price', 0), 0))
    events.sort(key=lambda x: x[1])
    print(f'\n  {m}:')
    for action, ts, price, gain in events:
        t = ts[11:19]
        if action == 'BUY':
            print(f'    {t} BUY  @ {price:.2f}')
        else:
            print(f'    {t} {action:4s} @ {price:.2f} (gain: {gain:+.2f})')
