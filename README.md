# Tradebot

Automated trading bot suite for **Polymarket crypto up/down markets** — short-duration (5m/15m) binary options on BTC, ETH, SOL, and XRP price movements.

Each market has UP and DOWN tokens priced $0–$1. The winning side resolves to $1.00, the loser to $0.00. These bots exploit various edges — flow imbalance, Bayesian pricing, book imbalance, cross-market arbitrage, and more.

## Strategies

| Bot | File | Entry Signal |
|-----|------|-------------|
| **Flow** | `flow.py` | Net buy flow > $100 AND 3× opposing side, price ≤ $0.60 |
| **Sniper** | `sniper.py` | Bayesian true prob via Binance vs market — enters when \|edge\| > 0.15 |
| **Theta** | `theta.py` | Late-entry BTC scalper — z-score > 1.3, realized vol < 70th percentile |
| **Whale** | `whale.py` | Follows trades ≥ $1,000 from non-market-maker wallets |
| **Liq** | `liq.py` | Binance 5s volume spike > 5× baseline with ≥ 65% directional ratio |
| **Follower** | `follower.py` | BTC UP/DOWN surges ≥ 10¢ → buys same side on ETH/SOL/XRP |
| **Gapbot** | `gapbot.py` | BTC moves ≥ 6¢ but follower lags ≥ 3¢ — arb the gap |
| **Burst** | `burst.py` | GTC limit orders on book imbalance > 3× (maker = 0% fee) |
| **BTC80** | `btc80.py` | Buys BTC at 0.80, limit sell at 0.99, stop-loss at 0.60 |
| **Straddle** | `straddle.py` | Dual limit buys on both sides ~30s pre-window, width by Binance vol |
| **MM** | `mm.py` | Two-sided market maker — buys both sides, sells at entry + 2¢ spread |
| **Arb** | `arb.py` | Conditional token arb when `1 - bid_up - bid_dn > 0.02` |
| **XArb** | `xarb.py` | Cross-exchange arb between Binance spot prices and Polymarket |
| **Maker** | `maker.py` | Bid/ask depth imbalance > 3× with OFI confirmation |
| **Bothsides** | `bothsides.py` | Pre-window limit buys on both UP and DOWN (15m markets) |
| **Coinflip** | `coinflip.py` | Mean reversion when market is balanced (48–52¢) |

## Tools

| File | Purpose |
|------|---------|
| `claimer.py` | Claim resolved positions (batch or loop mode) |
| `check_trades.py` | Inspect trade records from the CLOB API |
| `backtest.py` | Historical backtest over resolved markets |
| `predict.py` | Bayesian price prediction helper |
| `collect.py` | Orderbook + trade data collection |
| `whales.py` | Monitor large holder positions |
| `config.py` | Shared configuration |

## Setup

```bash
# create virtualenv and install deps
make install

# create .env with your Polymarket credentials
cat > .env << EOF
PRIVATE_KEY=0x...
FUNDER_ADDRESS=0x...
EOF
```

### Dependencies

- `py_clob_client` — Polymarket CLOB client
- `websockets` — real-time trade feed
- `requests` — REST API calls
- `scipy` / `numpy` / `pandas` — stats and data
- `python-dotenv` — env config

## Usage

```bash
# run any strategy
.venv/bin/python flow.py
.venv/bin/python sniper.py
.venv/bin/python theta.py

# or via Makefile
make flow
make sniper
make claim
```

Bots write logs to `<name>_log.txt` and trades to `<name>_trades.json`.

## API Endpoints

| Endpoint | URL |
|----------|-----|
| CLOB | `https://clob.polymarket.com` |
| Gamma (discovery) | `https://gamma-api.polymarket.com` |
| Data (history) | `https://data-api.polymarket.com` |
| WS Activity | `wss://ws-live-data.polymarket.com` |
| WS Book | `wss://ws-subscriptions-clob.polymarket.com/ws/market` |

## Architecture

```
Polymarket CLOB (Polygon)
    ├── REST API ── market discovery, order placement, balance queries
    └── WebSocket ── real-time trade feed, order book snapshots

Binance API
    └── Spot prices, volume, liquidation data (used by theta, sniper, liq, straddle)

Bot Loop (per strategy)
    ├── Discover live 5m/15m markets via Gamma API
    ├── Subscribe to real-time feeds (WebSocket or polling)
    ├── Compute entry signal per strategy logic
    ├── Execute via py_clob_client (GTC/GTD/market orders)
    ├── Manage exits (limit sells, stop-losses, trailing stops, or hold to resolution)
    └── Log trades to JSON + text log
```

## Market Structure

Markets use slug pattern: `{crypto}-updown-{interval}m-{epoch}`
- **Cryptos**: BTC, ETH, SOL, XRP
- **Intervals**: 5m, 15m
- **Epoch**: Unix timestamp of window start

Each market has two conditional tokens (UP / DOWN). Winner pays $1.00, loser pays $0.00. Maker orders have 0% fee; taker fees follow a polynomial formula based on price distance from 0.50.