#!/usr/bin/env python3
"""
Flow Bot: Follow the money, not specific wallets.

Strategy:
  For each 5-min and 15-min crypto market, pull the live trade feed
  (same data as the Activity tab on polymarket.com).

  Compute net flow = (buy volume) - (sell volume) for each side.
  When one side has heavy net buying that dwarfs the other, follow it.

Signals (all must be true to trigger):
  1. Net buy volume on chosen side exceeds MIN_FLOW ($)
  2. Flow ratio (chosen / other) exceeds MIN_RATIO
  3. We don't already hold a position on this market

Exit:
  - Limit sell at 0.99 (take profit)
  - Flow reversal: if net flow flips against us, market sell
"""

import os
import sys
import time
import math
import requests
import json
import asyncio
import threading
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, OpenOrderParams, BalanceAllowanceParams, AssetType
from py_clob_client.order_builder.constants import BUY, SELL
from py_clob_client import MarketOrderArgs
from dotenv import load_dotenv

load_dotenv()
private_key = os.getenv("PRIVATE_KEY")
founder_address = os.getenv("FUNDER_ADDRESS")

# Configuration
HOST = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CHAIN_ID = 137
FUNDER_ADDRESS = founder_address

# =========================================================================
# STRATEGY PARAMETERS
# =========================================================================
BUY_AMOUNT = 10             # Base bet in $ (smaller = less risk per trade)
MIN_FLOW = 100             # Minimum net buy $ on chosen side to trigger
                           # Backtest (48h, 172 markets, time-filtered):
                           #   Low threshold is fine — MIN_RATIO does the filtering
MIN_RATIO = 3.0            # Net buy on chosen side must be Nx the other
                           # Backtest: $100/3.0x at 50% cutoff → 89.3% WR (25W/3L)
                           #           $100/3.0x at 60% cutoff → 92.7% WR (38W/3L)
POLL_INTERVAL = 1          # Seconds between scans
SELL_PRICE = 0.99          # Limit sell price (take profit)
MAX_ENTRY_PRICE = 0.52     # Skip if price already above this (tighter — less vig)
CRYPTOS = ["btc", "eth", "sol", "xrp"]

# Scaling: bet more when the flow is stronger
# bet = BUY_AMOUNT * min(net_flow / MIN_FLOW, MAX_BET_MULTIPLIER)
MAX_BET_MULTIPLIER = 2.0   # Was 3.0 — cap risk
MIN_ELAPSED_PCT = 0.50     # Wait for 50% of window before entering
                           # 5m → wait 2.5min, 15m → wait 7.5min
                           # Early flow is noise; signal needs data
MIN_REVERSAL_FLOW = 50     # Post-entry counter-flow must exceed this to exit
                           # Prevents a single random sell from triggering reversal

# Risk management
MAX_CONCURRENT_POSITIONS = 100     # Limit simultaneous exposure
STARTING_BANKROLL = 0          # Set at runtime from actual balance
SESSION_STOP_LOSS_PCT = 0.30   # Halt if down 30% of starting bankroll
MIN_BALANCE_BUFFER = 5         # Keep $5 in reserve, never bet last dollars

# Dry-run mode: log signals without placing orders
DRY_RUN = "--dry-run" in sys.argv

# Log files
DECISION_LOG = "flow_dry_log.txt" if DRY_RUN else "flow_log.txt"
TRADE_LOG = "flow_dry_trades.json" if DRY_RUN else "flow_trades.json"
POSITIONS_FILE = "flow_positions.json"


def log(message):
    timestamp = datetime.now(timezone.utc).isoformat()
    with open(DECISION_LOG, "a") as f:
        f.write(f"[{timestamp}] {message}\n")
    print(message)


def get_usdc_balance():
    """Get current USDC balance available for trading."""
    try:
        ba = client.get_balance_allowance(
            params=BalanceAllowanceParams(
                asset_type=AssetType.COLLATERAL,
                token_id="",
                signature_type=1
            )
        )
        raw = float(ba.get("balance", 0))
        return raw / 1e6
    except Exception as e:
        log(f"  ⚠ Failed to get USDC balance: {e}")
        return None


# Initialize CLOB client
client = ClobClient(
    host=HOST,
    key=private_key,
    chain_id=CHAIN_ID,
    signature_type=1,
    funder=FUNDER_ADDRESS
)
client.set_api_creds(client.create_or_derive_api_creds())


# =========================================================================
# WEBSOCKET TRADE FEED
# =========================================================================

class TradeAccumulator:
    """Thread-safe accumulator for real-time trade events from WebSocket.
    Stores trades per asset_id (token). The WS market channel sends
    'last_trade_price' events with: asset_id, side, size, price, timestamp.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._trades = defaultdict(list)      # asset_id -> [trade_dict, ...]
        self._wanted = set()                   # tokens the main thread wants
        self._active = set()                   # tokens actually subscribed on WS
        self._last_sub_time = 0.0              # epoch of last subscription send
        self._force_reconnect = False          # set True to trigger clean reconnect
        self._ws_connected = False
        self._ws_thread = None
        self._loop = None
        self._event_count = 0                  # total trade events ingested
        self._msg_count = 0                    # total WS messages received

    def start(self):
        """Start the background WebSocket thread."""
        self._ws_thread = threading.Thread(target=self._run_loop, daemon=True)
        self._ws_thread.start()

    def _run_loop(self):
        """Run the asyncio event loop in a background thread."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._ws_loop())

    async def _ws_loop(self):
        """Connect, subscribe, listen. Auto-reconnect on failure."""
        try:
            import websockets
        except ImportError:
            import subprocess
            subprocess.check_call([sys.executable, "-m", "pip", "install", "websockets"])
            import websockets

        while True:
            try:
                async with websockets.connect(WS_URL, close_timeout=5, open_timeout=10) as ws:
                    self._ws_connected = True
                    self._active = set()   # force re-subscribe on reconnect
                    log("  🔌 WebSocket connected")

                    # Heartbeat + listen loop
                    last_ping = time.time()
                    while True:
                        # Sync subscriptions: wanted vs active
                        await self._sync_subscriptions(ws)

                        # If tokens rotated, break to reconnect cleanly
                        if self._force_reconnect:
                            self._force_reconnect = False
                            break

                        # Heartbeat every 10s
                        if time.time() - last_ping > 10:
                            try:
                                await ws.send("ping")
                                last_ping = time.time()
                            except Exception:
                                break

                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            continue

                        try:
                            data = json.loads(msg)
                        except (json.JSONDecodeError, TypeError):
                            continue

                        self._msg_count += 1

                        # Only use last_trade_price events (actual fills).
                        # price_change events are book changes, NOT trades.
                        if isinstance(data, dict):
                            if data.get("event_type") == "last_trade_price":
                                self._ingest_trade(data)
                        elif isinstance(data, list):
                            for item in data:
                                if isinstance(item, dict) and item.get("event_type") == "last_trade_price":
                                    self._ingest_trade(item)

            except Exception as e:
                self._ws_connected = False

                log(f"  ⚠ WebSocket disconnected: {e}, reconnecting in 2s...")
                await asyncio.sleep(2)

    async def _sync_subscriptions(self, ws):
        """Ensure WS subscriptions match what the main thread wants.
        When tokens change (rotation), force a reconnect for clean subscription
        state — the server doesn't reliably deliver last_trade_price events
        for tokens added via re-subscription on an existing connection.
        For periodic refresh (same tokens), re-send on existing connection.
        """
        with self._lock:
            wanted = set(self._wanted)          # snapshot under lock

        now = time.time()
        changed = (wanted != self._active)
        stale = (now - self._last_sub_time) > 5    # periodic refresh

        if not changed and not stale:
            return

        # Clear trades for tokens being removed
        removed = self._active - wanted
        if removed:
            with self._lock:
                for token in removed:
                    self._trades.pop(token, None)

        if changed and self._active:
            # Tokens actually changed on a live connection → reconnect
            # New tokens don't reliably receive events via re-subscription
            added = wanted - self._active
            log(f"  📡 Token rotation (+{len(added)}, -{len(removed)}), reconnecting WS...")
            self._active = set()
            self._force_reconnect = True
            return

        # Fresh connection or stale refresh: send full list
        if wanted:
            await ws.send(json.dumps({"type": "market", "assets_ids": list(wanted)}))

        self._last_sub_time = now

        if changed:
            added = wanted - self._active
            log(f"  📡 WS subscribed to {len(wanted)} tokens"
                f" (+{len(added)}, -{len(removed)})")

        # Only mark active AFTER successful send
        self._active = wanted

    def _ingest_trade(self, data):
        """Store a trade event from the WebSocket."""
        asset_id = data.get("asset_id", "")
        if not asset_id:
            return
        self._event_count += 1
        trade = {
            "asset": asset_id,
            "side": data.get("side", ""),
            "size": float(data.get("size", 0)),
            "price": float(data.get("price", 0)),
            "timestamp": data.get("timestamp", ""),
        }
        with self._lock:
            self._trades[asset_id].append(trade)

    def update_subscriptions(self, wanted_tokens):
        """Set the desired subscription list.  The WS thread will converge."""
        with self._lock:
            self._wanted = set(wanted_tokens)

    def get_trades(self, token_id):
        """Get accumulated trades for a token (thread-safe copy)."""
        with self._lock:
            return list(self._trades.get(token_id, []))

    def get_all_trades_for_market(self, tokens):
        """Get all accumulated trades for a market's tokens."""
        with self._lock:
            result = []
            for t in tokens:
                result.extend(self._trades.get(t, []))
            return result

    def clear_trades_for_market(self, tokens):
        """Clear accumulated trades for a market's tokens.
        Called after entry so flow reversal only sees post-entry trades."""
        with self._lock:
            for t in tokens:
                self._trades.pop(t, None)

    @property
    def connected(self):
        return self._ws_connected

    @property
    def subscribed_count(self):
        return len(self._active)

    @property
    def event_count(self):
        return self._event_count

    @property
    def msg_count(self):
        return self._msg_count


# Global trade accumulator
trade_ws = TradeAccumulator()


# =========================================================================
# API HELPERS
# =========================================================================

def get_current_crypto_markets():
    """Fetch current 5-min and 15-minute crypto updown markets."""
    markets = []
    now = datetime.now(timezone.utc)

    for interval in [5, 15]:
        aligned = (now.minute // interval) * interval
        current_window = now.replace(minute=aligned, second=0, microsecond=0)
        window_start = current_window
        elapsed = (now - window_start).total_seconds() / 60
        epoch = int(window_start.timestamp())

        for crypto in CRYPTOS:
            slug = f"{crypto}-updown-{interval}m-{epoch}"
            try:
                response = requests.get(f"{GAMMA_API}/events/slug/{slug}", timeout=10)
                if response.status_code == 200:
                    event = response.json()
                    for market in event.get("markets", []):
                        if not market.get("closed"):
                            market["_elapsed_min"] = elapsed
                            market["_crypto"] = crypto.upper()
                            market["_slug"] = slug
                            market["_interval"] = interval
                            market["_epoch"] = epoch
                            markets.append(market)
            except Exception:
                pass
    return markets


def compute_trade_flow(condition_id, tokens):
    """
    Analyze real-time trade flow from the WebSocket accumulator.
    Returns dict with net buy flow per side ($ volume: buys - sells).
    """
    trades = trade_ws.get_all_trades_for_market(tokens)
    up_token = tokens[0]

    result = {
        "up_buys": 0.0, "up_sells": 0.0,
        "down_buys": 0.0, "down_sells": 0.0,
        "up_net": 0.0, "down_net": 0.0,
        "trade_count": len(trades),
        "big_trades": [],
    }

    for t in trades:
        asset = t.get("asset", "")
        side = t.get("side", "")
        size = float(t.get("size", 0))
        price = float(t.get("price", 0))
        cost = size * price
        is_up = (asset == up_token)

        if is_up:
            if side == "BUY":
                result["up_buys"] += cost
            else:
                result["up_sells"] += cost
        else:
            if side == "BUY":
                result["down_buys"] += cost
            else:
                result["down_sells"] += cost

        # Track big trades for logging
        if cost >= 50:
            name = t.get("name", "") or t.get("pseudonym", "") or t.get("proxyWallet", "")[:12]
            outcome = "UP" if is_up else "DN"
            result["big_trades"].append(f"{name} {side} {size:.0f} {outcome} @{price:.2f} (${cost:.0f})")

    result["up_net"] = result["up_buys"] - result["up_sells"]
    result["down_net"] = result["down_buys"] - result["down_sells"]

    return result


def get_price(token_id, side=BUY):
    try:
        data = client.get_price(token_id, side=side)
        return float(data.get("price", 0))
    except Exception:
        return 0.0


def get_actual_share_balance(token_id):
    """Refresh and read actual on-chain share balance for a token."""
    try:
        client.update_balance_allowance(
            params=BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
                signature_type=1
            )
        )
        ba = client.get_balance_allowance(
            params=BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
                signature_type=1
            )
        )
        raw_balance = float(ba.get("balance", 0))
        return raw_balance / 1e6
    except Exception as e:
        log(f"  ⚠ Failed to get balance: {e}")
        return None


def cancel_all_orders_for_token(token_id):
    """Cancel any open orders for a token."""
    try:
        orders = client.get_orders(OpenOrderParams())
        for order in orders:
            if order.get("asset_id") == token_id:
                client.cancel(order.get("id"))
    except Exception:
        pass


def get_existing_orders():
    try:
        return client.get_orders(OpenOrderParams())
    except Exception:
        return []


# =========================================================================
# MAIN BOT
# =========================================================================

def save_positions(positions):
    """Save positions to disk so they survive restarts."""
    try:
        with open(POSITIONS_FILE, "w") as f:
            json.dump(positions, f, indent=2)
    except Exception as e:
        log(f"⚠ Failed to save positions: {e}")


def load_positions():
    """Load positions from disk."""
    try:
        with open(POSITIONS_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def run_flow():
    """
    Flow strategy:
    1. For each market, fetch the trade feed (Activity tab data)
    2. Compute net buy flow per side (buys $ - sells $)
    3. When one side has strong net inflow, buy it
    4. Place limit sell at 0.99
    5. If flow reverses on next check, market sell
    """
    resume = "--resume" in sys.argv
    if resume:
        positions = load_positions()
        if positions:
            log(f"\n🔄 Resumed {len(positions)} positions from {POSITIONS_FILE}")
            for tid, pos in positions.items():
                log(f"  {pos['market']} {pos['side']} (entry {pos['buy_price']:.2f})")
        else:
            log(f"\n🔄 --resume specified but no saved positions found")
    else:
        positions = {}

    # Track session P&L and bankroll
    global STARTING_BANKROLL
    initial_balance = get_usdc_balance()
    if initial_balance is not None:
        STARTING_BANKROLL = initial_balance
        log(f"\n💵 Starting USDC balance: ${STARTING_BANKROLL:.2f}")
    else:
        STARTING_BANKROLL = 100  # fallback
        log(f"\n⚠ Could not read balance, assuming ${STARTING_BANKROLL}")

    session_invested = 0.0
    session_returned = 0.0  # from wins that resolve
    trades_this_session = 0
    halted = False

    log(f"\n{'='*60}")
    if DRY_RUN:
        log(f"🧪 DRY-RUN MODE — no orders will be placed")
        log(f"   Logging signals to {TRADE_LOG} for analysis")
    # Start WebSocket trade feed
    trade_ws.start()
    log(f"Flow bot started (using real-time WebSocket trade feed)")
    log(f"Min flow: ${MIN_FLOW}, Min ratio: {MIN_RATIO}x, Min elapsed: {MIN_ELAPSED_PCT*100:.0f}%, Reversal: ${MIN_REVERSAL_FLOW}")
    log(f"Max entry price: {MAX_ENTRY_PRICE}, Max positions: {MAX_CONCURRENT_POSITIONS}")
    log(f"Buy amount: ${BUY_AMOUNT} (max {MAX_BET_MULTIPLIER}x), Cryptos: {', '.join(CRYPTOS)}")
    log(f"Session stop-loss: {SESSION_STOP_LOSS_PCT*100:.0f}% of ${STARTING_BANKROLL:.0f} = ${STARTING_BANKROLL * SESSION_STOP_LOSS_PCT:.0f}")
    log(f"{'='*60}\n")

    while True:
        try:
            os.system('cls' if os.name == 'nt' else 'clear')
            print("💰 Analyzing trade activity...", flush=True)

            markets = get_current_crypto_markets()
            existing_orders = get_existing_orders()

            # Update WebSocket subscriptions for current market tokens
            all_market_tokens = []
            for m in markets:
                all_market_tokens.extend(json.loads(m.get("clobTokenIds", "[]")))
            trade_ws.update_subscriptions(all_market_tokens)

            # Build set of tokens we already hold
            known_tokens = set(positions.keys())
            for order in existing_orders:
                tid = order.get("asset_id")
                if tid:
                    known_tokens.add(tid)

            # =================================================================
            # STEP 0: Check if we should keep trading
            # =================================================================
            if halted:
                # Just monitor existing positions, don't enter new ones
                pass

            # Check session stop-loss
            current_balance = get_usdc_balance()
            if current_balance is not None and STARTING_BANKROLL > 0:
                session_loss = STARTING_BANKROLL - current_balance
                max_loss = STARTING_BANKROLL * SESSION_STOP_LOSS_PCT
                if session_loss >= max_loss and not halted:
                    log(f"\n🛑 SESSION STOP-LOSS HIT: down ${session_loss:.2f} (limit ${max_loss:.2f})")
                    log(f"  Starting: ${STARTING_BANKROLL:.2f}, Current: ${current_balance:.2f}")
                    log(f"  Bot will monitor existing positions but NOT enter new trades.")
                    halted = True
                elif halted and session_loss < max_loss:
                    log(f"\n✅ UNHALT: balance recovered to ${current_balance:.2f} (loss ${session_loss:.2f} < limit ${max_loss:.2f})")
                    halted = False

            # Check position limit
            at_position_limit = len(positions) >= MAX_CONCURRENT_POSITIONS
            if at_position_limit and not halted:
                log(f"  📊 At position limit ({MAX_CONCURRENT_POSITIONS}), waiting for exits...")

            # =================================================================
            # STEP 1: Analyze trade flow and enter positions
            # =================================================================
            for market in markets:
                question = market.get("question", "N/A")
                crypto = market.get("_crypto", "")
                interval = market.get("_interval", 15)
                elapsed = market.get("_elapsed_min", 0)
                condition_id = market.get("conditionId", "")
                slug = market.get("_slug", "")
                epoch = market.get("_epoch", 0)
                tokens = json.loads(market.get("clobTokenIds", "[]"))

                if len(tokens) != 2:
                    continue

                already_holding = any(t in known_tokens for t in tokens)

                # Skip flow fetch if halted/at limit AND not holding (nothing to log or do)
                if (halted or at_position_limit) and not already_holding:
                    continue

                # Fetch and analyze trade activity
                flow = compute_trade_flow(condition_id, tokens)
                up_net = flow["up_net"]
                down_net = flow["down_net"]
                trade_count = flow["trade_count"]

                # Log flow for all markets (including held ones)
                if trade_count > 0:
                    held_tag = " [HELD]" if already_holding else ""
                    log(f"  {question} ({trade_count} trades){held_tag}: "
                        f"UP net ${up_net:+,.0f} (${flow['up_buys']:,.0f}B/${flow['up_sells']:,.0f}S) | "
                        f"DOWN net ${down_net:+,.0f} (${flow['down_buys']:,.0f}B/${flow['down_sells']:,.0f}S)")
                else:
                    log(f"  {question}: no trades yet")
                    continue

                # Don't enter if already holding, halted, or at limit
                if already_holding or halted or at_position_limit:
                    continue

                # Wait for enough data before entering
                min_elapsed = interval * MIN_ELAPSED_PCT
                if elapsed < min_elapsed:
                    continue

                # Check for entry signal
                # Use abs() so large negative flow on other side still requires ratio
                if up_net >= MIN_FLOW and up_net / max(abs(down_net), 1) >= MIN_RATIO:
                    chosen_idx = 0
                    chosen_side = "UP"
                    chosen_net = up_net
                    other_net = down_net
                elif down_net >= MIN_FLOW and down_net / max(abs(up_net), 1) >= MIN_RATIO:
                    chosen_idx = 1
                    chosen_side = "DOWN"
                    chosen_net = down_net
                    other_net = up_net
                else:
                    continue  # No signal

                chosen_token = tokens[chosen_idx]
                chosen_price = get_price(chosen_token, side=BUY)

                if chosen_price <= 0 or chosen_price >= 1:
                    continue

                if chosen_price > MAX_ENTRY_PRICE:
                    log(f"  ⏭ Skipping {question}: price {chosen_price:.2f} > max {MAX_ENTRY_PRICE}")
                    continue

                # Check we have enough balance
                if current_balance is not None:
                    available = current_balance - MIN_BALANCE_BUFFER
                    if available <= 0:
                        log(f"  ⏭ Insufficient balance (${current_balance:.2f}), skipping")
                        continue

                # Scale bet by flow strength
                flow_strength = chosen_net / MIN_FLOW
                bet_multiplier = min(flow_strength, MAX_BET_MULTIPLIER)
                scaled_amount = round(BUY_AMOUNT * bet_multiplier, 2)

                # Cap to available balance
                if current_balance is not None:
                    max_bet = current_balance - MIN_BALANCE_BUFFER
                    scaled_amount = min(scaled_amount, max_bet)
                    if scaled_amount < 1:
                        log(f"  ⏭ Bet too small after balance cap (${scaled_amount:.2f}), skipping")
                        continue

                ratio = chosen_net / max(abs(other_net), 1)

                log(f"\n💰 FLOW SIGNAL: {question}")
                log(f"  {chosen_side} net flow: ${chosen_net:+,.0f} (ratio {ratio:.1f}x)")
                log(f"  Price: {chosen_price:.2f}, Bet: ${scaled_amount:.0f} ({bet_multiplier:.1f}x)")

                # Show big trades that drove the signal
                if flow["big_trades"]:
                    log(f"  Big trades:")
                    for bt in flow["big_trades"][:5]:
                        log(f"    {bt}")

                # Log signal (always, for both dry-run and live)
                record = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "action": "DRY_SIGNAL" if DRY_RUN else "BUY",
                    "market": question,
                    "side": chosen_side,
                    "token": chosen_token,
                    "price": chosen_price,
                    "amount": scaled_amount,
                    "net_flow": chosen_net,
                    "ratio": round(ratio, 1),
                    "up_net": round(flow["up_net"], 2),
                    "down_net": round(flow["down_net"], 2),
                    "trade_count": flow["trade_count"],
                    "big_trades": flow["big_trades"][:5],
                    "slug": slug,
                    "interval": interval,
                    "crypto": crypto,
                }

                if DRY_RUN:
                    # Don't place any orders — just log and track as fake position
                    estimated_shares = scaled_amount / chosen_price if chosen_price > 0 else 0
                    log(f"  🧪 DRY-RUN: would buy {estimated_shares:.0f} shares of {chosen_side} at {chosen_price:.2f}")
                    record["response"] = "DRY_RUN"
                    with open(TRADE_LOG, "a") as f:
                        f.write(json.dumps(record) + "\n")

                    # Track as virtual position so we don't re-signal same market
                    positions[chosen_token] = {
                        "market": question,
                        "side": chosen_side,
                        "buy_price": chosen_price,
                        "size": estimated_shares,
                        "condition_id": condition_id,
                        "outcome_idx": chosen_idx,
                        "slug": slug,
                        "epoch": epoch,
                        "cost": scaled_amount,
                        "entry_net": chosen_net,
                        "tokens": tokens,
                    }
                    # Clear accumulated trades so flow reversal only sees post-entry flow
                    trade_ws.clear_trades_for_market(tokens)
                    continue

                # Place market buy (LIVE only)
                try:
                    mo = MarketOrderArgs(
                        token_id=chosen_token,
                        amount=scaled_amount,
                        side=BUY,
                        order_type=OrderType.FAK
                    )
                    signed = client.create_market_order(mo)
                    resp = client.post_order(signed, OrderType.FAK)

                    actual_shares = 0
                    if isinstance(resp, dict):
                        taking = resp.get("takingAmount", "0")
                        actual_shares = float(taking) if taking else 0
                    if actual_shares == 0:
                        actual_shares = scaled_amount / chosen_price if chosen_price > 0 else 0

                    positions[chosen_token] = {
                        "market": question,
                        "side": chosen_side,
                        "buy_price": chosen_price,
                        "size": actual_shares,
                        "condition_id": condition_id,
                        "outcome_idx": chosen_idx,
                        "slug": slug,
                        "epoch": epoch,
                        "cost": scaled_amount,
                        "entry_net": chosen_net,
                        "tokens": tokens,
                    }

                    log(f"  ✓ Bought {actual_shares:.0f} shares of {chosen_side} at {chosen_price:.2f}")

                    # Clear accumulated trades so flow reversal only sees post-entry flow
                    trade_ws.clear_trades_for_market(tokens)

                    # Place limit sell at SELL_PRICE in background
                    if actual_shares > 0:
                        def place_limit_sell(token, shares, side_s):
                            time.sleep(10)
                            try:
                                actual_balance = get_actual_share_balance(token)
                                if actual_balance and actual_balance > 0.1:
                                    sz = math.floor(actual_balance * 100) / 100
                                else:
                                    sz = math.floor(shares * 100) / 100
                                sell_order = OrderArgs(
                                    token_id=token,
                                    price=SELL_PRICE,
                                    size=sz,
                                    side=SELL
                                )
                                signed_sell = client.create_order(sell_order)
                                client.post_order(signed_sell, OrderType.GTC)
                                log(f"  ✓ Limit sell placed: {sz:.0f} shares of {side_s} at {SELL_PRICE}")
                            except Exception as e:
                                log(f"  ⚠ Limit sell failed for {side_s}: {e}")
                        threading.Thread(
                            target=place_limit_sell,
                            args=(chosen_token, actual_shares, chosen_side),
                            daemon=True
                        ).start()

                    record["response"] = str(resp)
                    with open(TRADE_LOG, "a") as f:
                        f.write(json.dumps(record) + "\n")

                except Exception as e:
                    log(f"  ✗ Buy failed: {e}")

            # =================================================================
            # STEP 2: Monitor positions — flow reversal + expiry
            # =================================================================
            tokens_to_remove = []
            current_market_tokens = set()
            for market in markets:
                for t in json.loads(market.get("clobTokenIds", "[]")):
                    current_market_tokens.add(t)

            for token_id, pos in positions.items():
                # Check if market expired
                if token_id not in current_market_tokens:
                    log(f"\n📋 Market expired: {pos['market']} {pos['side']} (entry at {pos['buy_price']:.2f})")
                    tokens_to_remove.append(token_id)
                    continue

                # Still active — re-check flow for reversal
                flow = compute_trade_flow(pos["condition_id"], pos["tokens"])
                our_net = flow["up_net"] if pos["outcome_idx"] == 0 else flow["down_net"]

                # Flow reversal: post-entry flow on our side has gone significantly negative
                # Require meaningful counter-flow, not just one random sell
                if our_net < -MIN_REVERSAL_FLOW:
                    log(f"\n🔄 FLOW REVERSAL: {pos['market']} {pos['side']}")
                    log(f"  Entry net was ${pos['entry_net']:+,.0f}, now ${our_net:+,.0f}")

                    if DRY_RUN:
                        log(f"  🧪 DRY-RUN: would sell position")
                        record = {
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "action": "DRY_FLOW_REVERSAL",
                            "market": pos["market"],
                            "side": pos["side"],
                            "token": token_id,
                            "buy_price": pos["buy_price"],
                            "entry_net": pos["entry_net"],
                            "current_net": our_net,
                            "response": "DRY_RUN"
                        }
                        with open(TRADE_LOG, "a") as f:
                            f.write(json.dumps(record) + "\n")
                        tokens_to_remove.append(token_id)
                        continue

                    try:
                        cancel_all_orders_for_token(token_id)
                        time.sleep(1)
                        actual_balance = get_actual_share_balance(token_id)
                        if actual_balance and actual_balance > 0.1:
                            sell_shares = math.floor(actual_balance * 100) / 100
                        else:
                            sell_shares = math.floor(pos["size"] * 100) / 100

                        if sell_shares > 0:
                            current_price = get_price(token_id, side=SELL)
                            mo = MarketOrderArgs(
                                token_id=token_id,
                                amount=sell_shares,
                                side=SELL,
                                order_type=OrderType.FAK
                            )
                            signed = client.create_market_order(mo)
                            resp = client.post_order(signed, OrderType.FAK)

                            sell_proceeds = sell_shares * current_price
                            pnl = sell_proceeds - pos["cost"]
                            log(f"  ✓ Sold {sell_shares:.0f} shares at ~{current_price:.2f} (P&L: ${pnl:+.1f})")

                            record = {
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "action": "FLOW_REVERSAL",
                                "market": pos["market"],
                                "side": pos["side"],
                                "token": token_id,
                                "buy_price": pos["buy_price"],
                                "sell_price": current_price,
                                "shares": sell_shares,
                                "pnl": pnl,
                                "response": str(resp)
                            }
                            with open(TRADE_LOG, "a") as f:
                                f.write(json.dumps(record) + "\n")

                    except Exception as e:
                        log(f"  ✗ Flow reversal sell failed: {e}")
                        log(f"  Removing position (likely already resolved/redeemed)")

                    tokens_to_remove.append(token_id)

            for token_id in tokens_to_remove:
                del positions[token_id]

            # =================================================================
            # Status display
            # =================================================================
            if positions:
                log(f"\n📊 Holding {len(positions)}/{MAX_CONCURRENT_POSITIONS} positions:")
                for token_id, pos in positions.items():
                    cp = get_price(token_id, side=SELL)
                    gain = cp - pos["buy_price"]
                    status = "📈" if gain >= 0 else "📉"
                    log(f"  {status} {pos['market']} {pos['side']}: "
                        f"entry {pos['buy_price']:.2f} → now {cp:.2f} ({gain:+.2f})")

            now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
            bal_str = f"${current_balance:.2f}" if current_balance else "?"
            halt_str = " 🛑 HALTED" if halted else ""
            dry_str = " 🧪 DRY-RUN" if DRY_RUN else ""
            ws_str = "🟢" if trade_ws.connected else "🔴"
            ws_detail = f" ({trade_ws.event_count} events, {trade_ws.msg_count} msgs, {trade_ws.subscribed_count} subs)"
            session_pnl = (current_balance - STARTING_BANKROLL) if current_balance else 0
            log(f"\n⏰ {now_str} UTC | {len(positions)} pos | bal {bal_str} | session ${session_pnl:+.2f} | WS {ws_str}{ws_detail}{halt_str}{dry_str}")
            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            if positions:
                save_positions(positions)
                log(f"\n💾 Saved {len(positions)} positions to {POSITIONS_FILE}")
                log("  Restart with --resume to continue monitoring them")
            else:
                # Clean up stale file if no positions
                try:
                    os.remove(POSITIONS_FILE)
                except FileNotFoundError:
                    pass
            log("\nBot stopped by user")
            break
        except Exception as e:
            log(f"\n⚠ Error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    run_flow()
