#!/usr/bin/env python3
"""
collect.py — Record full orderbook + trade flow from Polymarket BTC 5-min markets.

Captures ALL WebSocket events for later analysis:
  - book:             Full orderbook snapshots
  - price_change:     Incremental book deltas (level added/removed/modified)
  - best_bid_ask:     Top-of-book updates
  - last_trade_price: Individual trade fills (side, size, price)
  - chainlink:        Oracle BTC/USD prices (resolution source)

Outputs:
  collect_<timestamp>/book.jsonl      — book snapshots + deltas
  collect_<timestamp>/trades.jsonl    — trade fills
  collect_<timestamp>/bba.jsonl       — best bid/ask updates
  collect_<timestamp>/chainlink.jsonl — oracle prices
  collect_<timestamp>/meta.json       — session metadata

Usage:
  python collect.py              # run for 2 hours (default)
  python collect.py --hours 6    # run for 6 hours
"""

import asyncio
import json
import os
import sys
import time
import threading
import argparse
import signal
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from pathlib import Path

import requests

# ── Config ──────────────────────────────────────────────────────────────
GAMMA_API = "https://gamma-api.polymarket.com"
WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
RTDS_WS_URL = "wss://ws-live-data.polymarket.com"

CHAINLINK_SYMBOL = "btc/usd"
MARKET_INTERVAL = 5  # BTC 5-min windows
ROTATION_POLL = 5    # seconds between market discovery checks

# ── Logging ─────────────────────────────────────────────────────────────
def log(msg):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{ts}] {msg}", flush=True)


# ── Market Discovery ────────────────────────────────────────────────────
def discover_btc_market():
    """Fetch current active BTC 5-min up/down market. Returns dict or None."""
    now = datetime.now(timezone.utc)
    aligned = (now.minute // MARKET_INTERVAL) * MARKET_INTERVAL
    window_start = now.replace(minute=aligned, second=0, microsecond=0)
    window_end = window_start + timedelta(minutes=MARKET_INTERVAL)

    if now >= window_end:
        return None

    epoch = int(window_start.timestamp())
    slug = f"btc-updown-{MARKET_INTERVAL}m-{epoch}"

    try:
        resp = requests.get(f"{GAMMA_API}/events/slug/{slug}", timeout=10)
        if resp.status_code != 200:
            return None
        event = resp.json()
        for m in event.get("markets", []):
            if m.get("closed"):
                continue
            tokens = json.loads(m.get("clobTokenIds", "[]"))
            outcomes = json.loads(m.get("outcomes", "[]"))
            if len(tokens) != 2 or len(outcomes) != 2:
                continue
            up_idx = next((i for i, o in enumerate(outcomes) if "up" in o.lower()), 0)
            down_idx = 1 - up_idx
            return {
                "epoch": epoch,
                "start": window_start.isoformat(),
                "end": window_end.isoformat(),
                "up_token": tokens[up_idx],
                "down_token": tokens[down_idx],
                "condition_id": m.get("conditionId", ""),
                "question": m.get("question", ""),
            }
    except Exception as e:
        log(f"⚠ Discovery error: {e}")
    return None


# ── JSONL Writer (thread-safe, buffered) ────────────────────────────────
class JSONLWriter:
    def __init__(self, path):
        self._f = open(path, "a")
        self._lock = threading.Lock()
        self._count = 0

    def write(self, obj):
        obj["_ts"] = time.time()
        obj["_utc"] = datetime.now(timezone.utc).isoformat()
        line = json.dumps(obj, default=str)
        with self._lock:
            self._f.write(line + "\n")
            self._count += 1
            if self._count % 100 == 0:
                self._f.flush()

    def close(self):
        with self._lock:
            self._f.flush()
            self._f.close()

    @property
    def count(self):
        return self._count


# ── Polymarket WebSocket Collector ──────────────────────────────────────
class MarketCollector:
    """Connects to Polymarket Market WS and records every event."""

    def __init__(self, book_writer, trades_writer, bba_writer):
        self._book_w = book_writer
        self._trades_w = trades_writer
        self._bba_w = bba_writer
        self._lock = threading.Lock()
        self._wanted = set()
        self._active = set()
        self._connected = False
        self._force_reconnect = False
        self._msg_count = 0
        self._trade_count = 0
        self._book_count = 0
        self._current_epoch = None

    def start(self):
        t = threading.Thread(target=self._run, daemon=True, name="market-ws")
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
                    log("🔌 Market WS connected")
                    last_ping = time.time()
                    last_sub_time = 0.0

                    while True:
                        with self._lock:
                            wanted = set(self._wanted)

                        if self._force_reconnect:
                            self._force_reconnect = False
                            break

                        changed = wanted != self._active
                        stale = (time.time() - last_sub_time) > 5

                        if (changed or stale) and wanted:
                            if changed and self._active:
                                self._active = set()
                                self._force_reconnect = True
                                break

                            await ws.send(json.dumps({
                                "type": "market",
                                "assets_ids": list(wanted),
                            }))
                            last_sub_time = time.time()
                            if changed:
                                log(f"📡 Subscribed to {len(wanted)} tokens")
                            self._active = wanted

                        if time.time() - last_ping > 10:
                            try:
                                await ws.send("ping")
                                last_ping = time.time()
                            except Exception:
                                break

                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=0.05)
                        except asyncio.TimeoutError:
                            continue

                        try:
                            data = json.loads(msg)
                        except (json.JSONDecodeError, TypeError):
                            continue

                        self._msg_count += 1
                        self._dispatch(data)

            except Exception as e:
                self._connected = False
                log(f"⚠ Market WS error: {e}, reconnecting in 2s...")
                await asyncio.sleep(2)

    def _dispatch(self, data):
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    self._dispatch(item)
            return
        if not isinstance(data, dict):
            return

        etype = data.get("event_type", "")

        if etype == "last_trade_price":
            self._trade_count += 1
            self._trades_w.write({
                "type": "trade",
                "epoch": self._current_epoch,
                "asset_id": data.get("asset_id", ""),
                "side": data.get("side", ""),
                "size": data.get("size", ""),
                "price": data.get("price", ""),
                "timestamp": data.get("timestamp", ""),
            })

        elif etype == "book":
            self._book_count += 1
            self._book_w.write({
                "type": "book_snapshot",
                "epoch": self._current_epoch,
                "asset_id": data.get("asset_id", ""),
                "bids": data.get("bids", []),
                "asks": data.get("asks", []),
            })

        elif etype == "price_change":
            for pc in data.get("price_changes", []):
                self._book_w.write({
                    "type": "book_delta",
                    "epoch": self._current_epoch,
                    "asset_id": pc.get("asset_id", ""),
                    "price": pc.get("price", ""),
                    "size": pc.get("size", ""),
                    "side": pc.get("side", ""),
                    "best_bid": pc.get("best_bid", ""),
                    "best_ask": pc.get("best_ask", ""),
                })

        elif etype == "best_bid_ask":
            self._bba_w.write({
                "type": "bba",
                "epoch": self._current_epoch,
                "asset_id": data.get("asset_id", ""),
                "best_bid": data.get("best_bid", ""),
                "best_ask": data.get("best_ask", ""),
                "spread": data.get("spread", ""),
            })

    def subscribe(self, token_ids, epoch=None):
        with self._lock:
            self._wanted = set(token_ids)
            self._current_epoch = epoch

    @property
    def connected(self):
        return self._connected

    @property
    def stats(self):
        return {
            "msgs": self._msg_count,
            "trades": self._trade_count,
            "book_events": self._book_count,
        }


# ── Chainlink WebSocket Collector ───────────────────────────────────────
class ChainlinkCollector:
    """Records Chainlink oracle BTC/USD prices from RTDS WebSocket."""

    def __init__(self, writer):
        self._writer = writer
        self._connected = False
        self._count = 0
        self._last_price = None
        self._retries = 0

    def start(self):
        t = threading.Thread(target=self._run, daemon=True, name="chainlink-ws")
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
                    RTDS_WS_URL, close_timeout=5, open_timeout=10
                ) as ws:
                    self._connected = True
                    self._retries = 0
                    log("🔌 Chainlink RTDS connected")

                    await ws.send(json.dumps({
                        "action": "subscribe",
                        "subscriptions": [{
                            "topic": "crypto_prices_chainlink",
                            "type": "*",
                            "filters": "",
                        }],
                    }))

                    last_ping = time.time()

                    while True:
                        if time.time() - last_ping > 4:
                            try:
                                await ws.send("PING")
                                last_ping = time.time()
                            except Exception:
                                break

                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            continue

                        if msg == "PONG":
                            continue

                        try:
                            data = json.loads(msg)
                        except (json.JSONDecodeError, TypeError):
                            continue

                        payload = data.get("payload", data)
                        sym = str(payload.get("symbol", "")).lower()
                        if sym != CHAINLINK_SYMBOL:
                            continue

                        price = payload.get("value")
                        if price is None:
                            continue

                        self._last_price = float(price)
                        self._count += 1
                        self._writer.write({
                            "type": "chainlink",
                            "symbol": sym,
                            "price": self._last_price,
                            "source_ts": payload.get("timestamp", ""),
                        })

            except Exception as e:
                self._connected = False
                backoff = min(2 ** self._retries, 60)
                self._retries += 1
                if "429" in str(e):
                    log(f"⚠ Chainlink rate-limited, retry in {backoff}s...")
                else:
                    log(f"⚠ Chainlink WS error: {e}, reconnecting in {backoff}s...")
                await asyncio.sleep(backoff)

    @property
    def last_price(self):
        return self._last_price

    @property
    def connected(self):
        return self._connected

    @property
    def count(self):
        return self._count


# ── Main ────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Collect Polymarket BTC 5-min orderbook + trade data")
    parser.add_argument("--hours", type=float, default=2.0, help="Hours to collect (default: 2)")
    args = parser.parse_args()

    run_seconds = args.hours * 3600
    start_time = time.time()

    # Create output directory
    ts_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = Path(f"collect_{ts_str}")
    out_dir.mkdir(exist_ok=True)

    # Initialize writers
    book_w = JSONLWriter(out_dir / "book.jsonl")
    trades_w = JSONLWriter(out_dir / "trades.jsonl")
    bba_w = JSONLWriter(out_dir / "bba.jsonl")
    chainlink_w = JSONLWriter(out_dir / "chainlink.jsonl")

    log(f"📂 Output: {out_dir}/")
    log(f"⏱  Recording for {args.hours}h ({run_seconds:.0f}s)")

    # Save metadata
    with open(out_dir / "meta.json", "w") as f:
        json.dump({
            "start_utc": datetime.now(timezone.utc).isoformat(),
            "duration_hours": args.hours,
            "market_interval": MARKET_INTERVAL,
        }, f, indent=2)

    # Start collectors
    market = MarketCollector(book_w, trades_w, bba_w)
    chainlink = ChainlinkCollector(chainlink_w)

    market.start()
    chainlink.start()

    # Graceful shutdown
    shutdown = threading.Event()
    def handle_sig(*_):
        log("🛑 Shutting down...")
        shutdown.set()
    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    current_epoch = None
    windows_seen = 0
    last_status = 0

    log("🔍 Waiting for market...")

    while not shutdown.is_set():
        elapsed = time.time() - start_time
        if elapsed >= run_seconds:
            log("⏱  Time limit reached")
            break

        # Discover current market
        mkt = discover_btc_market()
        if mkt and mkt["epoch"] != current_epoch:
            current_epoch = mkt["epoch"]
            windows_seen += 1
            tokens = [mkt["up_token"], mkt["down_token"]]
            market.subscribe(tokens, epoch=current_epoch)

            # Log market rotation
            book_w.write({
                "type": "market_rotation",
                "epoch": current_epoch,
                "market": mkt,
            })

            log(f"🪟  Window #{windows_seen}: epoch={current_epoch} "
                f"ends={mkt['end']}")

        # Status line every 30s
        if time.time() - last_status > 30:
            st = market.stats
            remaining = run_seconds - elapsed
            log(f"📊 msgs={st['msgs']} trades={st['trades']} "
                f"book={st['book_events']} chainlink={chainlink.count} "
                f"windows={windows_seen} "
                f"remaining={remaining/60:.0f}m "
                f"mkt_ws={'✓' if market.connected else '✗'} "
                f"cl_ws={'✓' if chainlink.connected else '✗'}")
            last_status = time.time()

        shutdown.wait(timeout=ROTATION_POLL)

    # Close writers
    book_w.close()
    trades_w.close()
    bba_w.close()
    chainlink_w.close()

    # Final stats
    st = market.stats
    log(f"\n{'='*50}")
    log(f"COLLECTION COMPLETE")
    log(f"{'='*50}")
    log(f"Duration: {elapsed/60:.1f} minutes")
    log(f"Windows seen: {windows_seen}")
    log(f"WS messages: {st['msgs']}")
    log(f"Trade fills: {st['trades']}")
    log(f"Book events: {st['book_events']}")
    log(f"BBA updates: {bba_w.count}")
    log(f"Chainlink ticks: {chainlink.count}")
    log(f"Output: {out_dir}/")

    # Update metadata
    with open(out_dir / "meta.json", "w") as f:
        json.dump({
            "start_utc": datetime.now(timezone.utc).isoformat(),
            "duration_hours": args.hours,
            "actual_duration_min": elapsed / 60,
            "market_interval": MARKET_INTERVAL,
            "windows_seen": windows_seen,
            "total_messages": st["msgs"],
            "total_trades": st["trades"],
            "total_book_events": st["book_events"],
            "total_bba_updates": bba_w.count,
            "total_chainlink_ticks": chainlink.count,
        }, f, indent=2)


if __name__ == "__main__":
    main()
