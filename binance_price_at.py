#!/usr/bin/env python3
"""Fetch spot price for a Binance symbol at a specific timestamp."""

from __future__ import annotations

import argparse
import datetime as dt
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import json
from typing import Any, Dict, Optional

BASE_URL = "https://api.binance.com"
AGG_TRADES_PATH = "/api/v3/aggTrades"
DEFAULT_WINDOW_SECONDS = 60


def parse_timestamp(timestamp_str: str) -> int:
    """Return the timestamp in milliseconds, accepting seconds or milliseconds input."""
    try:
        value = float(timestamp_str)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid timestamp '{timestamp_str}': {exc}"
        ) from exc

    if value <= 0:
        raise argparse.ArgumentTypeError("Timestamp must be positive")

    # Heuristic: treat values with magnitude >= 1e12 as milliseconds, otherwise seconds.
    if value >= 1e12:
        millis = int(value)
    else:
        millis = int(value * 1000)
    return millis


def binance_request(params: Dict[str, Any]) -> Any:
    query = urllib.parse.urlencode(params)
    url = f"{BASE_URL}{AGG_TRADES_PATH}?{query}"
    request = urllib.request.Request(url=url, method="GET")

    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            data = response.read()
    except urllib.error.HTTPError as exc:
        raise RuntimeError(
            f"Binance API error {exc.code} while requesting {url}: {exc.read().decode()}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Failed to reach Binance API: {exc.reason}") from exc

    try:
        return json.loads(data)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Received invalid JSON from Binance API") from exc


def find_trade_at_timestamp(trades: Any, target_ms: int) -> Optional[Dict[str, Any]]:
    """Return the trade closest to the target timestamp, preferring trades at or before it."""
    if not isinstance(trades, list):
        raise RuntimeError("Unexpected Binance response format: expected a list of trades")

    before_or_at = [trade for trade in trades if int(trade.get("T", 0)) <= target_ms]
    if before_or_at:
        return max(before_or_at, key=lambda trade: int(trade.get("T", 0)))

    if trades:
        # No trade before timestamp, fall back to earliest after it.
        return min(trades, key=lambda trade: int(trade.get("T", 0)))

    return None


def fetch_price(symbol: str, timestamp_ms: int, window_seconds: int) -> Dict[str, Any]:
    window_ms = window_seconds * 1000
    start_time = max(timestamp_ms - window_ms, 0)
    end_time = timestamp_ms + window_ms

    params = {
        "symbol": symbol.upper(),
        "startTime": start_time,
        "endTime": end_time,
        "limit": 1000,
    }

    trades = binance_request(params)
    trade = find_trade_at_timestamp(trades, timestamp_ms)
    if not trade:
        raise RuntimeError(
            "No trades found near the requested timestamp. Consider increasing the window."
        )

    trade_time_ms = int(trade["T"])
    price = float(trade["p"])
    qty = float(trade["q"])

    return {
        "symbol": symbol.upper(),
        "requested_timestamp": timestamp_ms,
        "trade_timestamp": trade_time_ms,
        "trade_time_iso": dt.datetime.fromtimestamp(trade_time_ms / 1000, tz=dt.timezone.utc).isoformat(),
        "price": price,
        "quantity": qty,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch Binance spot price for a symbol at a specific timestamp",
    )
    parser.add_argument("symbol", help="Trading pair symbol, e.g. BTCUSDT")
    parser.add_argument(
        "timestamp",
        type=parse_timestamp,
        help="Event timestamp (seconds or milliseconds since epoch)",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=DEFAULT_WINDOW_SECONDS,
        help=(
            "Window size in seconds to search for trades around the timestamp. "
            "Defaults to 60 seconds. Increase if no trades are returned."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        result = fetch_price(args.symbol, args.timestamp, args.window)
    except Exception as exc:  # pylint: disable=broad-except
        parser.error(str(exc))
        return 2

    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
