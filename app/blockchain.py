"""
Layer 1: Blockchain Data Ingestion

Multi-source data pipeline for live Ethereum mainnet data:

- Etherscan V2 API (when ETHERSCAN_API_KEY is configured)
- Blockscout free public API (no key required, Etherscan-compatible format)
- Public JSON-RPC endpoint (used for live ETH balance lookups)

This module is the data engineering layer of the scanner. It is responsible
for fetching raw on-chain data from whichever upstream is available, then
normalizing the responses into pandas DataFrames the analytics layer can
work with. There is no synthetic / demo fallback - if no upstream returns
data, the result is an empty DataFrame and the rest of the pipeline handles
it gracefully.
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional

import httpx
import pandas as pd

from app.config import (
    BLOCKSCOUT_BASE,
    ETHERSCAN_API_KEY,
    ETHERSCAN_V2_BASE_URL,
    PUBLIC_RPC_URL,
)


# Per-request timeout (seconds). Public endpoints can be slow under load
# so this gives them enough room without making the UI feel stuck.
HTTP_TIMEOUT = 25.0

# Default page size for transaction lookups. Etherscan/Blockscout both
# support up to 10000 here but for a quick scan we want responsiveness
# over completeness.
DEFAULT_PAGE_SIZE = 200


# =====================================================================
# Source selection
# =====================================================================

def _has_etherscan_key() -> bool:
    """Return True only when a real Etherscan API key is configured."""
    return bool(ETHERSCAN_API_KEY) and ETHERSCAN_API_KEY != "your_etherscan_api_key_here"


def active_data_source() -> str:
    """
    Identify which upstream the scanner is currently configured to use.
    Exposed via /api/health so prospects can see what data backs the scan.
    """
    return "etherscan_v2" if _has_etherscan_key() else "blockscout"


# =====================================================================
# Public RPC layer (used for live balance + earliest block lookups)
# =====================================================================

async def _rpc_call(method: str, params: list) -> Optional[dict]:
    """Minimal JSON-RPC client. Returns the parsed response or None on error."""
    payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.post(
                PUBLIC_RPC_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            return resp.json()
    except (httpx.HTTPError, ValueError):
        return None


async def fetch_balance_rpc(address: str) -> dict:
    """Fetch live ETH balance via the public JSON-RPC endpoint."""
    data = await _rpc_call("eth_getBalance", [address, "latest"])
    if data and "result" in data:
        try:
            balance_wei = int(data["result"], 16)
            return {"balance_wei": balance_wei, "balance_eth": balance_wei / 1e18}
        except (TypeError, ValueError):
            pass
    return {"balance_wei": 0, "balance_eth": 0.0}


# =====================================================================
# Blockscout layer (free fallback when no Etherscan key is set)
# =====================================================================

async def _blockscout_request(params: dict) -> list[dict]:
    """
    Hit the public Blockscout v1 endpoint with the given query parameters.
    Blockscout mirrors the Etherscan classic schema, so the result rows
    can be normalized by the same downstream code.
    """
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.get(BLOCKSCOUT_BASE, params=params)
            resp.raise_for_status()
            data = resp.json()
    except (httpx.HTTPError, ValueError):
        return []

    # Blockscout uses status="1" on success, "0" when there are no results
    # or on a soft error. Either way an empty list is the safe answer.
    if str(data.get("status")) != "1":
        return []
    result = data.get("result")
    return result if isinstance(result, list) else []


# =====================================================================
# Etherscan V2 layer (used when an API key is provided)
# =====================================================================

async def _etherscan_request(params: dict) -> list[dict]:
    """Hit Etherscan V2 with the given params and return the result list."""
    full_params = {"chainid": "1", "apikey": ETHERSCAN_API_KEY, **params}
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.get(ETHERSCAN_V2_BASE_URL, params=full_params)
            resp.raise_for_status()
            data = resp.json()
    except (httpx.HTTPError, ValueError):
        return []

    if str(data.get("status")) != "1":
        return []
    result = data.get("result")
    return result if isinstance(result, list) else []


# =====================================================================
# Public fetch functions (these are the entry points the rest of the
# pipeline calls - they pick the right upstream automatically)
# =====================================================================

async def fetch_eth_balance(address: str) -> dict:
    """
    Return the address balance. Tries Etherscan first when a key is set,
    then falls back to a public JSON-RPC call. Always returns a dict so
    callers never have to null-check.
    """
    if _has_etherscan_key():
        result = await _etherscan_request({
            "module": "account",
            "action": "balance",
            "address": address,
            "tag": "latest",
        })
        # Etherscan returns the balance as a string in the "result" field,
        # but our wrapper coerces non-list results to []. Re-fetch raw.
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                resp = await client.get(
                    ETHERSCAN_V2_BASE_URL,
                    params={
                        "chainid": "1",
                        "module": "account",
                        "action": "balance",
                        "address": address,
                        "tag": "latest",
                        "apikey": ETHERSCAN_API_KEY,
                    },
                )
                data = resp.json()
                if str(data.get("status")) == "1":
                    balance_wei = int(data["result"])
                    return {"balance_wei": balance_wei, "balance_eth": balance_wei / 1e18}
        except (httpx.HTTPError, ValueError, KeyError):
            pass

    # Default path: public RPC. Works without keys.
    return await fetch_balance_rpc(address)


async def fetch_normal_transactions(
    address: str,
    page: int = 1,
    offset: int = DEFAULT_PAGE_SIZE,
) -> list[dict]:
    """Fetch native ETH transactions for the address."""
    params = {
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": "0",
        "endblock": "99999999",
        "page": str(page),
        "offset": str(offset),
        "sort": "desc",
    }
    if _has_etherscan_key():
        return await _etherscan_request(params)
    return await _blockscout_request(params)


async def fetch_erc20_transfers(
    address: str,
    page: int = 1,
    offset: int = DEFAULT_PAGE_SIZE,
) -> list[dict]:
    """Fetch ERC-20 token transfers for the address."""
    params = {
        "module": "account",
        "action": "tokentx",
        "address": address,
        "page": str(page),
        "offset": str(offset),
        "sort": "desc",
    }
    if _has_etherscan_key():
        return await _etherscan_request(params)
    return await _blockscout_request(params)


async def fetch_internal_transactions(
    address: str,
    page: int = 1,
    offset: int = 100,
) -> list[dict]:
    """Fetch contract-internal transactions (value-bearing message calls)."""
    params = {
        "module": "account",
        "action": "txlistinternal",
        "address": address,
        "page": str(page),
        "offset": str(offset),
        "sort": "desc",
    }
    if _has_etherscan_key():
        return await _etherscan_request(params)
    return await _blockscout_request(params)


async def fetch_first_transaction(address: str) -> Optional[dict]:
    """
    Return the earliest transaction record for the address (used to compute
    address age). Queries normal txs, internal txs, and ERC-20 transfers in
    parallel with ascending sort, and picks the oldest timestamp across all
    three. This catches contract wallets and cold wallets that may only
    show up in one of the three lists.
    """
    common = {"address": address, "startblock": "0", "endblock": "99999999",
              "page": "1", "offset": "1", "sort": "asc"}

    async def _first_of(action: str) -> Optional[dict]:
        params = {"module": "account", "action": action, **common}
        rows = await _etherscan_request(params) if _has_etherscan_key() \
            else await _blockscout_request(params)
        return rows[0] if rows else None

    results = await asyncio.gather(
        _first_of("txlist"),
        _first_of("txlistinternal"),
        _first_of("tokentx"),
        return_exceptions=True,
    )

    earliest: Optional[dict] = None
    earliest_ts: Optional[int] = None
    for row in results:
        if isinstance(row, Exception) or not row:
            continue
        ts_raw = row.get("timeStamp")
        if not ts_raw:
            continue
        try:
            ts = int(ts_raw)
        except (TypeError, ValueError):
            continue
        if earliest_ts is None or ts < earliest_ts:
            earliest = row
            earliest_ts = ts
    return earliest


# =====================================================================
# Data normalization
# =====================================================================

def _safe_numeric(series: pd.Series, default: float = 0.0) -> pd.Series:
    """Coerce a series to numeric, swallowing parse errors."""
    return pd.to_numeric(series, errors="coerce").fillna(default)


def normalize_transactions(raw_txs: list[dict], address: str) -> pd.DataFrame:
    """
    Turn a raw list of transaction dicts into a typed DataFrame with the
    derived columns the analytics layer needs (direction, counterparty,
    eth value, gas cost). Empty input returns an empty frame.
    """
    if not raw_txs:
        return pd.DataFrame()

    df = pd.DataFrame(raw_txs)

    # Ensure required columns exist before we touch them
    for col in ("from", "to", "value", "timeStamp", "gasUsed", "gasPrice", "hash"):
        if col not in df.columns:
            df[col] = ""

    df["value_eth"] = _safe_numeric(df["value"]) / 1e18
    df["timeStamp"] = _safe_numeric(df["timeStamp"]).astype("int64")
    df["datetime"] = pd.to_datetime(df["timeStamp"], unit="s", utc=True)
    df["gasUsed"] = _safe_numeric(df["gasUsed"])
    df["gasPrice"] = _safe_numeric(df["gasPrice"])
    df["gas_cost_eth"] = (df["gasUsed"] * df["gasPrice"]) / 1e18

    addr_lower = address.lower()
    df["from"] = df["from"].fillna("").astype(str)
    df["to"] = df["to"].fillna("").astype(str)
    df["direction"] = df.apply(
        lambda row: "outgoing" if str(row["from"]).lower() == addr_lower else "incoming",
        axis=1,
    )
    df["counterparty"] = df.apply(
        lambda row: row["to"] if str(row["from"]).lower() == addr_lower else row["from"],
        axis=1,
    )

    return df


def normalize_token_transfers(raw_transfers: list[dict], address: str) -> pd.DataFrame:
    """Same shape as normalize_transactions but for ERC-20 transfers."""
    if not raw_transfers:
        return pd.DataFrame()

    df = pd.DataFrame(raw_transfers)

    for col in ("from", "to", "value", "tokenDecimal", "tokenSymbol", "tokenName", "timeStamp", "hash"):
        if col not in df.columns:
            df[col] = ""

    df["tokenDecimal"] = _safe_numeric(df["tokenDecimal"], default=18)
    df["value_raw"] = _safe_numeric(df["value"])
    df["token_amount"] = df["value_raw"] / (10 ** df["tokenDecimal"])
    df["timeStamp"] = _safe_numeric(df["timeStamp"]).astype("int64")
    df["datetime"] = pd.to_datetime(df["timeStamp"], unit="s", utc=True)

    addr_lower = address.lower()
    df["from"] = df["from"].fillna("").astype(str)
    df["to"] = df["to"].fillna("").astype(str)
    df["direction"] = df.apply(
        lambda row: "outgoing" if str(row["from"]).lower() == addr_lower else "incoming",
        axis=1,
    )
    df["counterparty"] = df.apply(
        lambda row: row["to"] if str(row["from"]).lower() == addr_lower else row["from"],
        axis=1,
    )

    return df


# =====================================================================
# Main pipeline
# =====================================================================

async def get_wallet_data(address: str) -> dict:
    """
    Run the full ingestion pipeline for a wallet address. Returns a dict
    of normalized data plus metadata about which upstream was used and
    whether the fetch hit any errors. Never raises - errors are recorded
    in the returned 'errors' list and the affected fields are empty.
    """
    address = address.strip().lower()
    errors: list[str] = []

    # Kick off all upstream calls in parallel
    balance_task = fetch_eth_balance(address)
    txs_task = fetch_normal_transactions(address)
    tokens_task = fetch_erc20_transfers(address)
    internal_task = fetch_internal_transactions(address)
    first_tx_task = fetch_first_transaction(address)

    results = await asyncio.gather(
        balance_task,
        txs_task,
        tokens_task,
        internal_task,
        first_tx_task,
        return_exceptions=True,
    )

    def _unwrap(value, label, default):
        if isinstance(value, Exception):
            errors.append(f"{label}: {type(value).__name__}: {value}")
            return default
        return value

    balance = _unwrap(results[0], "balance", {"balance_wei": 0, "balance_eth": 0.0})
    raw_txs = _unwrap(results[1], "transactions", [])
    raw_tokens = _unwrap(results[2], "token_transfers", [])
    raw_internal = _unwrap(results[3], "internal_transactions", [])
    first_tx = _unwrap(results[4], "first_transaction", None)

    tx_df = normalize_transactions(raw_txs, address)
    token_df = normalize_token_transfers(raw_tokens, address)

    # Address age: prefer the dedicated earliest-tx lookup because the
    # paged tx list is sorted desc and may not contain the very first tx.
    address_age_days: Optional[int] = None
    if first_tx and first_tx.get("timeStamp"):
        try:
            first_ts = int(first_tx["timeStamp"])
            first_dt = datetime.fromtimestamp(first_ts, tz=timezone.utc)
            address_age_days = (datetime.now(timezone.utc) - first_dt).days
        except (TypeError, ValueError):
            pass
    elif not tx_df.empty:
        earliest_tx = tx_df["datetime"].min()
        address_age_days = (datetime.now(timezone.utc) - earliest_tx).days

    # Detect whether the result was truncated at the page limit. Both
    # Etherscan and Blockscout cap txlist results; if we hit the cap we
    # almost certainly missed older activity and should say so.
    data_truncated = (
        len(raw_txs) >= DEFAULT_PAGE_SIZE
        or len(raw_tokens) >= DEFAULT_PAGE_SIZE
    )

    return {
        "address": address,
        "balance": balance,
        "transactions": tx_df,
        "token_transfers": token_df,
        "internal_transactions": raw_internal,
        "transaction_count": len(raw_txs),
        "token_transfer_count": len(raw_tokens),
        "address_age_days": address_age_days,
        "data_source": active_data_source(),
        "data_truncated": data_truncated,
        "page_limit": DEFAULT_PAGE_SIZE,
        "errors": errors,
    }
