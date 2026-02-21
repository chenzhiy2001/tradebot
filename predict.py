#!/usr/bin/env python3
"""
Prediction Research Script — Can we predict Polymarket crypto outcomes
using real-time exchange price data?

Hypothesis:
  Polymarket 5m/15m crypto "Up or Down" markets resolve based on whether
  the crypto price at window END > window START. If we track the actual
  price via Binance, we can estimate the true probability and compare it
  to Polymarket's implied probability (token price). Any gap = edge.

Data collected:
  - Binance real-time price for BTC, ETH, SOL, XRP
  - Polymarket implied probability (UP token best_bid/best_ask)
  - Snapshots at 10%, 25%, 50%, 75%, 90%, 95% of each window
  - Final outcome (UP or DOWN based on Binance close vs open)

Output:
  - Console: live dashboard of active windows
  - predict_data.jsonl: one JSON line per completed window for analysis
"""

import os
import sys
import time
import json
import asyncio
import threading
import re
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# =========================================================================
# CONFIG
# =========================================================================
GAMMA_API = "https://gamma-api.polymarket.com"
WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BINANCE_WS_URL = "wss://stream.binance.com:9443/stream"

CRYPTOS = {
    "btc": "btcusdt",
    "eth": "ethusdt",
    "sol": "solusdt",
    "xrp": "xrpusdt",
}

# Checkpoint percentages through the window to snapshot
CHECKPOINTS = [0.10, 0.25, 0.50, 0.75, 0.90, 0.95]

DATA_FILE = "predict_data.jsonl"
MARKET_POLL_INTERVAL = 30  # seconds between market discovery polls


# =========================================================================
# BINANCE PRICE FEED
# =========================================================================
class BinanceFeed:
    """Real-time crypto prices from Binance WebSocket."""

    def __init__(self):
        self._prices = {}       # symbol -> {"price": float, "ts": float}
        self._lock = threading.Lock()
        self._connected = False

    def start(self):
        t = threading.Thread(target=self._run, daemon=True)
        t.start()

    def _run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._ws_loop())

    async def _ws_loop(self):
        import websockets

        # Combined stream for all cryptos
        streams = "/".join(f"{sym}@miniTicker" for sym in CRYPTOS.values())
        url = f"{BINANCE_WS_URL}?streams={streams}"

        while True:
            try:
                async with websockets.connect(url, close_timeout=5, open_timeout=10) as ws:
                    self._connected = True
                    log("🔌 Binance WS connected")

                    while True:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            # Send ping to keep alive
                            await ws.ping()
                            continue

                        try:
                            data = json.loads(msg)
                        except (json.JSONDecodeError, TypeError):
                            continue

                        # Combined stream format: {"stream": "btcusdt@miniTicker", "data": {...}}
                        payload = data.get("data", data)
                        symbol = payload.get("s", "").lower()  # e.g. "btcusdt"
                        close_price = payload.get("c")  # Last price

                        if symbol and close_price:
                            with self._lock:
                                self._prices[symbol] = {
                                    "price": float(close_price),
                                    "ts": time.time(),
                                }

            except Exception as e:
                self._connected = False
                log(f"⚠ Binance WS error: {e}, reconnecting in 2s...")
                await asyncio.sleep(2)

    def get_price(self, crypto):
        """Get latest price for a crypto (e.g. 'btc'). Returns (price, age_seconds) or (None, None)."""
        symbol = CRYPTOS.get(crypto.lower())
        if not symbol:
            return None, None
        with self._lock:
            data = self._prices.get(symbol)
        if not data:
            return None, None
        age = time.time() - data["ts"]
        return data["price"], age

    @property
    def connected(self):
        return self._connected


# =========================================================================
# POLYMARKET PRICE FEED
# =========================================================================
class PolymarketFeed:
    """Real-time Polymarket book data via WebSocket."""

    def __init__(self):
        self._prices = {}       # token_id -> {"best_bid": float, "best_ask": float, "ts": float}
        self._lock = threading.Lock()
        self._wanted = set()
        self._active = set()
        self._connected = False
        self._force_reconnect = False

    def start(self):
        t = threading.Thread(target=self._run, daemon=True)
        t.start()

    def _run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._ws_loop())

    async def _ws_loop(self):
        import websockets

        while True:
            try:
                async with websockets.connect(
                    WS_MARKET_URL, close_timeout=5, open_timeout=10
                ) as ws:
                    self._connected = True
                    self._active = set()
                    log("🔌 Polymarket WS connected")

                    last_ping = time.time()
                    last_sub_time = 0.0

                    while True:
                        # Sync subscriptions
                        with self._lock:
                            wanted = set(self._wanted)

                        if self._force_reconnect:
                            self._force_reconnect = False
                            break

                        changed = wanted != self._active
                        stale = (time.time() - last_sub_time) > 5

                        if (changed or stale) and wanted:
                            if changed and self._active:
                                # Must reconnect to change subscriptions
                                self._active = set()
                                self._force_reconnect = True
                                break

                            await ws.send(json.dumps({
                                "type": "market",
                                "assets_ids": list(wanted),
                                "custom_feature_enabled": True,
                            }))
                            last_sub_time = time.time()
                            if changed:
                                log(f"📡 Polymarket subscribed to {len(wanted)} tokens")
                            self._active = wanted

                        # Heartbeat
                        if time.time() - last_ping > 10:
                            try:
                                await ws.send("ping")
                                last_ping = time.time()
                            except Exception:
                                break

                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=0.1)
                        except asyncio.TimeoutError:
                            continue

                        try:
                            data = json.loads(msg)
                        except (json.JSONDecodeError, TypeError):
                            continue

                        self._handle(data)

            except Exception as e:
                self._connected = False
                log(f"⚠ Polymarket WS error: {e}, reconnecting in 2s...")
                await asyncio.sleep(2)

    def _handle(self, data):
        """Route incoming WS events."""
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    self._handle(item)
            return
        if not isinstance(data, dict):
            return

        etype = data.get("event_type", "")
        if etype == "best_bid_ask":
            self._on_bba(data)
        elif etype == "book":
            self._on_book(data)
        elif etype == "price_change":
            self._on_price_change(data)

    def _on_bba(self, data):
        asset_id = data.get("asset_id", "")
        if not asset_id:
            return
        try:
            bid = float(data.get("best_bid", 0))
            ask = float(data.get("best_ask", 0))
        except (ValueError, TypeError):
            return
        with self._lock:
            self._prices[asset_id] = {"best_bid": bid, "best_ask": ask, "ts": time.time()}

    def _on_book(self, data):
        asset_id = data.get("asset_id", "")
        if not asset_id:
            return
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        best_bid = 0
        best_ask = 0
        if bids:
            best_bid = max(float(b["price"]) for b in bids)
        if asks:
            best_ask = min(float(a["price"]) for a in asks)
        with self._lock:
            self._prices[asset_id] = {"best_bid": best_bid, "best_ask": best_ask, "ts": time.time()}

    def _on_price_change(self, data):
        for pc in data.get("price_changes", []):
            asset_id = pc.get("asset_id", "")
            if not asset_id:
                continue
            try:
                best_bid = float(pc.get("best_bid", 0))
                best_ask = float(pc.get("best_ask", 0))
            except (ValueError, TypeError):
                continue
            if best_bid > 0 or best_ask > 0:
                with self._lock:
                    existing = self._prices.get(asset_id, {})
                    self._prices[asset_id] = {
                        "best_bid": best_bid if best_bid > 0 else existing.get("best_bid", 0),
                        "best_ask": best_ask if best_ask > 0 else existing.get("best_ask", 0),
                        "ts": time.time(),
                    }

    def subscribe(self, token_ids):
        with self._lock:
            self._wanted = set(token_ids)

    def get_price(self, token_id):
        """Returns (best_bid, best_ask, age_seconds) or (None, None, None)."""
        with self._lock:
            data = self._prices.get(token_id)
        if not data:
            return None, None, None
        age = time.time() - data["ts"]
        return data["best_bid"], data["best_ask"], age

    @property
    def connected(self):
        return self._connected


# =========================================================================
# MARKET DISCOVERY
# =========================================================================
def parse_window_times(market_name):
    """Parse start/end UTC times from market name like
    'Bitcoin Up or Down - February 21, 2:15PM-2:30PM ET'.
    Returns (start_utc, end_utc) or (None, None)."""
    match = re.search(
        r'(\w+)\s+(\d+),\s+(\d+:\d+[AP]M)-(\d+:\d+[AP]M)\s+ET',
        market_name
    )
    if not match:
        return None, None

    month_str = match.group(1)
    day_str = match.group(2)
    start_str = match.group(3)
    end_str = match.group(4)
    year = datetime.now().year

    try:
        dt_start = datetime.strptime(f"{month_str} {day_str} {year} {start_str}", "%B %d %Y %I:%M%p")
        dt_end = datetime.strptime(f"{month_str} {day_str} {year} {end_str}", "%B %d %Y %I:%M%p")
    except ValueError:
        return None, None

    # ET: Feb = EST = UTC-5
    est = timezone(timedelta(hours=-5))
    start_utc = dt_start.replace(tzinfo=est).astimezone(timezone.utc)
    end_utc = dt_end.replace(tzinfo=est).astimezone(timezone.utc)
    return start_utc, end_utc


def discover_markets():
    """Fetch current Polymarket crypto updown markets from Gamma API."""
    markets = []
    now = datetime.now(timezone.utc)

    for interval in [5, 15]:
        aligned = (now.minute // interval) * interval
        window_start = now.replace(minute=aligned, second=0, microsecond=0)
        epoch = int(window_start.timestamp())

        for crypto in CRYPTOS:
            slug = f"{crypto}-updown-{interval}m-{epoch}"
            try:
                resp = requests.get(f"{GAMMA_API}/events/slug/{slug}", timeout=10)
                if resp.status_code == 200:
                    event = resp.json()
                    for m in event.get("markets", []):
                        if not m.get("closed"):
                            m["_crypto"] = crypto
                            m["_interval"] = interval
                            markets.append(m)
            except Exception:
                pass
    return markets


# =========================================================================
# WINDOW TRACKER
# =========================================================================
class WindowTracker:
    """Tracks active market windows and collects prediction data."""

    def __init__(self, binance_feed, poly_feed):
        self.binance = binance_feed
        self.poly = poly_feed
        self._windows = {}      # key -> window_state
        self._lock = threading.Lock()
        self._completed = []    # completed window records

    def update_markets(self, markets):
        """Add new market windows, subscribe tokens."""
        all_tokens = []
        now = datetime.now(timezone.utc)

        for m in markets:
            question = m.get("question", "")
            tokens = json.loads(m.get("clobTokenIds", "[]"))
            if len(tokens) != 2:
                continue

            crypto = m["_crypto"]
            interval = m["_interval"]
            start_utc, end_utc = parse_window_times(question)
            if not start_utc or not end_utc:
                continue

            # Skip if window already ended
            if now >= end_utc:
                continue

            key = f"{crypto}_{interval}m_{int(start_utc.timestamp())}"
            all_tokens.extend(tokens)

            with self._lock:
                if key not in self._windows:
                    self._windows[key] = {
                        "key": key,
                        "market": question,
                        "crypto": crypto,
                        "interval": interval,
                        "start_utc": start_utc,
                        "end_utc": end_utc,
                        "up_token": tokens[0],
                        "down_token": tokens[1],
                        "open_price": None,         # Binance price at window start
                        "close_price": None,        # Binance price at window end
                        "checkpoints_done": set(),  # which pct checkpoints we've taken
                        "checkpoints": [],          # list of checkpoint dicts
                        "outcome": None,            # "UP" or "DOWN"
                        "completed": False,
                    }

        self.poly.subscribe(all_tokens)

    def tick(self):
        """Called periodically from main loop. Snapshots checkpoints and finalizes windows."""
        now = datetime.now(timezone.utc)
        to_remove = []

        with self._lock:
            windows = list(self._windows.items())

        for key, w in windows:
            start = w["start_utc"]
            end = w["end_utc"]
            duration = (end - start).total_seconds()
            elapsed = (now - start).total_seconds()
            pct = elapsed / duration if duration > 0 else 0

            crypto = w["crypto"]
            binance_price, binance_age = self.binance.get_price(crypto)

            # Record open price (first Binance price after window starts)
            if w["open_price"] is None and pct >= 0 and binance_price is not None:
                # If we're within the first 5% of the window, use current price as open
                if pct <= 0.05:
                    w["open_price"] = binance_price
                    log(f"  📍 {key}: Open price = ${binance_price:,.2f}")
                elif pct > 0.05:
                    # We missed the open — try REST snapshot
                    w["open_price"] = binance_price  # Best we can do
                    log(f"  📍 {key}: Late open price = ${binance_price:,.2f} (joined at {pct:.0%})")

            # Snapshot checkpoints
            if w["open_price"] is not None and binance_price is not None:
                for cp in CHECKPOINTS:
                    if cp not in w["checkpoints_done"] and pct >= cp:
                        # Get Polymarket implied probability
                        up_bid, up_ask, poly_age = self.poly.get_price(w["up_token"])
                        down_bid, down_ask, _ = self.poly.get_price(w["down_token"])

                        # Mid-price as implied prob
                        implied_up = None
                        if up_bid and up_ask and up_bid > 0 and up_ask > 0:
                            implied_up = (up_bid + up_ask) / 2

                        crypto_return = (binance_price - w["open_price"]) / w["open_price"] * 100

                        checkpoint = {
                            "pct_elapsed": cp,
                            "timestamp": now.isoformat(),
                            "crypto_price": binance_price,
                            "crypto_return_pct": round(crypto_return, 4),
                            "direction": "UP" if crypto_return > 0 else ("DOWN" if crypto_return < 0 else "FLAT"),
                            "poly_up_bid": up_bid,
                            "poly_up_ask": up_ask,
                            "poly_implied_up": round(implied_up, 4) if implied_up else None,
                            "poly_down_bid": down_bid,
                            "poly_down_ask": down_ask,
                        }
                        w["checkpoints"].append(checkpoint)
                        w["checkpoints_done"].add(cp)

            # Finalize: window ended
            if now >= end and not w["completed"]:
                if binance_price is not None:
                    w["close_price"] = binance_price

                if w["open_price"] and w["close_price"]:
                    w["outcome"] = "UP" if w["close_price"] > w["open_price"] else "DOWN"

                    # Compute edge at each checkpoint
                    for cp in w["checkpoints"]:
                        if cp["poly_implied_up"] is not None:
                            true_outcome_up = 1.0 if w["outcome"] == "UP" else 0.0
                            cp["true_outcome_up"] = true_outcome_up
                            cp["edge"] = round(true_outcome_up - cp["poly_implied_up"], 4)
                            # Direction-based prediction accuracy
                            direction_pred = "UP" if cp["crypto_return_pct"] > 0 else "DOWN"
                            cp["direction_correct"] = (direction_pred == w["outcome"])

                    self._save_window(w)
                    ret = ((w["close_price"] - w["open_price"]) / w["open_price"]) * 100
                    log(f"  ✅ {key}: {w['outcome']} | open=${w['open_price']:,.2f} close=${w['close_price']:,.2f} ({ret:+.3f}%)")
                else:
                    log(f"  ⚠ {key}: Window ended but missing price data")

                w["completed"] = True
                to_remove.append(key)

        # Clean up completed windows
        with self._lock:
            for key in to_remove:
                self._windows.pop(key, None)

    def _save_window(self, w):
        """Write completed window to JSONL file."""
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "market": w["market"],
            "crypto": w["crypto"],
            "interval": w["interval"],
            "window_start": w["start_utc"].isoformat(),
            "window_end": w["end_utc"].isoformat(),
            "open_price": w["open_price"],
            "close_price": w["close_price"],
            "outcome": w["outcome"],
            "return_pct": round(
                (w["close_price"] - w["open_price"]) / w["open_price"] * 100, 4
            ) if w["open_price"] and w["close_price"] else None,
            "checkpoints": w["checkpoints"],
        }
        with open(DATA_FILE, "a") as f:
            f.write(json.dumps(record) + "\n")
        self._completed.append(record)

    def get_dashboard(self):
        """Return formatted dashboard string for console."""
        lines = []
        now = datetime.now(timezone.utc)

        with self._lock:
            windows = sorted(self._windows.values(), key=lambda w: w["key"])

        if not windows:
            lines.append("  (no active windows)")
            return "\n".join(lines)

        for w in windows:
            start = w["start_utc"]
            end = w["end_utc"]
            duration = (end - start).total_seconds()
            elapsed = (now - start).total_seconds()
            pct = min(elapsed / duration, 1.0) if duration > 0 else 0
            remaining = max(0, (end - now).total_seconds())

            crypto = w["crypto"].upper()
            binance_price, _ = self.binance.get_price(w["crypto"])
            up_bid, up_ask, _ = self.poly.get_price(w["up_token"])

            # Current state
            price_str = f"${binance_price:,.2f}" if binance_price else "?"
            open_str = f"${w['open_price']:,.2f}" if w["open_price"] else "?"

            if w["open_price"] and binance_price:
                ret = (binance_price - w["open_price"]) / w["open_price"] * 100
                direction = "🟢 UP" if ret > 0 else ("🔴 DN" if ret < 0 else "⚪ FLAT")
                ret_str = f"{ret:+.3f}%"
            else:
                direction = "⚪ ?"
                ret_str = "?"

            implied_str = ""
            if up_bid and up_ask and up_bid > 0:
                mid = (up_bid + up_ask) / 2
                implied_str = f"poly_up={mid:.2f}"

            bar_len = 20
            filled = int(pct * bar_len)
            bar = "█" * filled + "░" * (bar_len - filled)

            lines.append(
                f"  {crypto:4s} {w['interval']}m │{bar}│ {pct:5.1%} {remaining:3.0f}s left │ "
                f"{direction} {ret_str:>8s} │ now={price_str} open={open_str} │ {implied_str}"
            )

            # Show checkpoints
            for cp in w["checkpoints"][-3:]:  # last 3 checkpoints
                cp_pct = cp["pct_elapsed"]
                cp_ret = cp["crypto_return_pct"]
                cp_dir = cp["direction"]
                cp_impl = cp.get("poly_implied_up")
                impl_str = f"impl_up={cp_impl:.2f}" if cp_impl else ""
                lines.append(
                    f"         └ @{cp_pct:.0%}: {cp_dir} {cp_ret:+.3f}% {impl_str}"
                )

        # Summary of completed windows
        if self._completed:
            total = len(self._completed)
            correct_at = defaultdict(lambda: {"correct": 0, "total": 0})
            for rec in self._completed:
                for cp in rec["checkpoints"]:
                    if "direction_correct" in cp:
                        pct_key = cp["pct_elapsed"]
                        correct_at[pct_key]["total"] += 1
                        if cp["direction_correct"]:
                            correct_at[pct_key]["correct"] += 1

            lines.append(f"\n  📊 Completed: {total} windows")
            if correct_at:
                lines.append("  Direction accuracy (if current direction at checkpoint predicts outcome):")
                for pct_key in sorted(correct_at):
                    d = correct_at[pct_key]
                    acc = d["correct"] / d["total"] * 100 if d["total"] > 0 else 0
                    lines.append(f"    @{pct_key:5.0%}: {acc:5.1f}% ({d['correct']}/{d['total']})")

        return "\n".join(lines)


# =========================================================================
# UTILITY
# =========================================================================
def log(msg):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


# =========================================================================
# MAIN
# =========================================================================
def main():
    log("Starting prediction research script...")
    log(f"Tracking: {', '.join(c.upper() for c in CRYPTOS)}")
    log(f"Checkpoints: {', '.join(f'{c:.0%}' for c in CHECKPOINTS)}")
    log(f"Data file: {DATA_FILE}")
    log("")

    # Start feeds
    binance = BinanceFeed()
    binance.start()

    poly = PolymarketFeed()
    poly.start()

    tracker = WindowTracker(binance, poly)

    # Wait for connections
    log("Waiting for WebSocket connections...")
    for _ in range(30):
        if binance.connected and poly.connected:
            break
        time.sleep(1)

    if not binance.connected:
        log("⚠ Binance WS not connected after 30s")
    if not poly.connected:
        log("⚠ Polymarket WS not connected after 30s")

    log("")
    last_discovery = 0

    while True:
        try:
            now = time.time()

            # Discover markets periodically
            if now - last_discovery > MARKET_POLL_INTERVAL:
                markets = discover_markets()
                if markets:
                    tracker.update_markets(markets)
                last_discovery = now

            # Tick: check checkpoints and finalize
            tracker.tick()

            # Dashboard
            now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
            bn_status = "🟢" if binance.connected else "🔴"
            pm_status = "🟢" if poly.connected else "🔴"

            dashboard = tracker.get_dashboard()

            # Clear and redraw
            print(f"\033[2J\033[H", end="")  # clear screen
            print(f"═══ Prediction Research ═══  {now_str} UTC  │  Binance {bn_status}  Polymarket {pm_status}")
            print(f"{'─' * 90}")
            print(dashboard)
            print(f"\n{'─' * 90}")
            print(f"Press Ctrl+C to stop. Data saved to {DATA_FILE}")

            time.sleep(1)

        except KeyboardInterrupt:
            log(f"\nStopped. Collected data in {DATA_FILE}")
            # Print final summary
            if os.path.exists(DATA_FILE):
                lines = open(DATA_FILE).readlines()
                log(f"Total completed windows: {len(lines)}")
            break
        except Exception as e:
            log(f"⚠ Error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
