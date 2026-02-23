#!/usr/bin/env python3
"""
Auto-claimer for Polymarket — redeems winning positions gaslessly via Builder Relayer.

Usage:
    python claimer.py                    # one-shot: claim all redeemable positions
    python claimer.py --batch 20         # claim up to 20 positions per run
    python claimer.py --loop 120         # poll every 120 s (auto-waits on rate limit)
    python claimer.py --loop 120 --batch 10
"""

import argparse
import logging
import os
import re
import sys
import time

import httpx
from dotenv import load_dotenv

from polymarket_apis.clients.data_client import PolymarketDataClient
from polymarket_apis.clients.web3_client import PolymarketGaslessWeb3Client
from polymarket_apis.types.clob_types import ApiCreds

# ── logging ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("claimer")

# ── constants ───────────────────────────────────────────────────────
CHAIN_ID = 137  # Polygon mainnet
CLAIM_DELAY = 5  # seconds between redemptions
MAX_RETRIES = 5  # per-position retry count
INITIAL_BACKOFF = 10  # seconds (doubles each attempt)


# ── helpers ─────────────────────────────────────────────────────────
_RESET_RE = re.compile(r"resets?\s+in\s+(\d+)\s+seconds?", re.IGNORECASE)


def _parse_reset_seconds(resp: httpx.Response) -> int | None:
    """Extract the quota-reset countdown from a 429 JSON body."""
    try:
        body = resp.json()
        msg = body.get("error", "") if isinstance(body, dict) else ""
        m = _RESET_RE.search(msg)
        if m:
            return int(m.group(1))
    except Exception:
        pass
    return None


class QuotaExhausted(Exception):
    """Raised when the builder quota is fully exhausted (long reset)."""
    def __init__(self, reset_seconds: int):
        self.reset_seconds = reset_seconds
        super().__init__(f"Builder quota exhausted — resets in {reset_seconds}s")


# ── env ─────────────────────────────────────────────────────────────
def load_env():
    """Load credentials from .env."""
    load_dotenv()
    private_key = os.getenv("PRIVATE_KEY")
    funder_address = os.getenv("FUNDER_ADDRESS")
    builder_key = os.getenv("BUILDER_API_KEY")
    builder_secret = os.getenv("BUILDER_API_SECRET")
    builder_passphrase = os.getenv("BUILDER_API_PASSPHRASE")

    missing = [
        name
        for name, val in [
            ("PRIVATE_KEY", private_key),
            ("FUNDER_ADDRESS", funder_address),
            ("BUILDER_API_KEY", builder_key),
            ("BUILDER_API_SECRET", builder_secret),
            ("BUILDER_API_PASSPHRASE", builder_passphrase),
        ]
        if not val
    ]
    if missing:
        log.error("Missing env vars: %s", ", ".join(missing))
        sys.exit(1)

    builder_creds = ApiCreds(
        key=builder_key,
        secret=builder_secret,
        passphrase=builder_passphrase,
    )
    return private_key, funder_address, builder_creds


# ── data fetching ───────────────────────────────────────────────────
def fetch_redeemable(data_client: PolymarketDataClient, user: str):
    """Return all redeemable Position objects (paginated)."""
    all_positions = []
    offset = 0
    page = 500
    while True:
        positions = data_client.get_positions(
            user=user,
            redeemable=True,
            size_threshold=0,
            limit=page,
            offset=offset,
        )
        all_positions.extend(p for p in positions if p.redeemable)
        if len(positions) < page:
            break
        offset += page
    return all_positions


# ── single claim ────────────────────────────────────────────────────
def claim_position(
    web3_client: PolymarketGaslessWeb3Client,
    condition_id: str,
    size: float,
    outcome_index: int,
    neg_risk: bool,
):
    """
    Redeem one position with retry / back-off.

    Raises QuotaExhausted if the 429 body indicates a long reset window (>60 s).
    """
    amounts = [0.0, 0.0]
    amounts[outcome_index] = size

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            receipt = web3_client.redeem_position(
                condition_id=condition_id,
                amounts=amounts,
                neg_risk=neg_risk,
            )
            return receipt
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                reset = _parse_reset_seconds(e.response)
                if reset and reset > 60:
                    # quota truly exhausted — bubble up so caller can wait / abort
                    raise QuotaExhausted(reset) from e
                wait = INITIAL_BACKOFF * (2 ** (attempt - 1))
                if attempt < MAX_RETRIES:
                    log.warning(
                        "    429 rate-limited. Retry %d/%d in %ds…",
                        attempt, MAX_RETRIES, wait,
                    )
                    time.sleep(wait)
                else:
                    raise
            else:
                raise
        except Exception:
            wait = INITIAL_BACKOFF * (2 ** (attempt - 1))
            if attempt < MAX_RETRIES:
                log.warning(
                    "    Transient error. Retry %d/%d in %ds…",
                    attempt, MAX_RETRIES, wait,
                )
                time.sleep(wait)
            else:
                raise


# ── batch claim ─────────────────────────────────────────────────────
def claim_all(
    data_client: PolymarketDataClient,
    web3_client: PolymarketGaslessWeb3Client,
    user: str,
    batch: int | None = None,
) -> tuple[int, int, bool]:
    """
    Find and redeem redeemable positions.

    Returns (claimed, failed, quota_hit).
    """
    positions = fetch_redeemable(data_client, user)
    if not positions:
        log.info("No redeemable positions found.")
        return 0, 0, False

    total_size = sum(p.size for p in positions)
    log.info(
        "Found %d redeemable position(s)  (total shares ≈ %.2f)",
        len(positions),
        total_size,
    )

    if batch and batch < len(positions):
        log.info("Batch size %d — claiming first %d positions this run.", batch, batch)
        positions = positions[:batch]

    for p in positions:
        log.info(
            "  • %s | %s | size=%.4f | neg_risk=%s",
            p.title, p.outcome, p.size, p.negative_risk,
        )

    claimed = 0
    failed = 0
    quota_hit = False

    for i, p in enumerate(positions):
        log.info(
            "[%d/%d] Redeeming %s — %s (%.4f shares)…",
            i + 1, len(positions), p.title, p.outcome, p.size,
        )
        try:
            receipt = claim_position(
                web3_client,
                condition_id=p.condition_id,
                size=p.size,
                outcome_index=p.outcome_index,
                neg_risk=p.negative_risk,
            )
            tx_hash = getattr(receipt, "tx_hash", None) or "?"
            log.info("  ✓ Claimed  tx=%s", tx_hash)
            claimed += 1

        except QuotaExhausted as qe:
            log.warning(
                "  ⏳ Builder quota exhausted — resets in %ds. Stopping batch.",
                qe.reset_seconds,
            )
            quota_hit = True
            break

        except Exception:
            log.exception("  ✗ Failed to redeem %s (%s)", p.title, p.outcome)
            failed += 1

        # throttle between claims
        if i < len(positions) - 1:
            time.sleep(CLAIM_DELAY)

    log.info(
        "Batch done: %d claimed, %d failed, %d remaining.",
        claimed, failed, len(positions) - claimed - failed,
    )
    return claimed, failed, quota_hit


# ── main ────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Polymarket auto-claimer")
    parser.add_argument(
        "--loop",
        type=int,
        default=0,
        metavar="SECS",
        help="Poll interval in seconds (0 = one-shot, default: 0)",
    )
    parser.add_argument(
        "--batch",
        type=int,
        default=None,
        metavar="N",
        help="Max positions to claim per run / cycle (default: all)",
    )
    args = parser.parse_args()

    private_key, funder_address, builder_creds = load_env()

    data_client = PolymarketDataClient()
    web3_client = PolymarketGaslessWeb3Client(
        private_key=private_key,
        signature_type=1,  # Poly proxy wallets
        builder_creds=builder_creds,
        chain_id=CHAIN_ID,
    )

    log.info("Claimer started  (user=%s)", funder_address)

    if args.loop <= 0:
        claim_all(data_client, web3_client, funder_address, batch=args.batch)
    else:
        log.info("Polling every %ds. Ctrl+C to stop.", args.loop)
        try:
            while True:
                _, _, quota_hit = claim_all(
                    data_client, web3_client, funder_address, batch=args.batch
                )
                wait = args.loop
                if quota_hit:
                    # back off longer when quota is exhausted
                    wait = max(args.loop, 120)
                    log.info("Quota hit — sleeping %ds before next cycle.", wait)
                time.sleep(wait)
        except KeyboardInterrupt:
            log.info("Stopped by user.")


if __name__ == "__main__":
    main()
