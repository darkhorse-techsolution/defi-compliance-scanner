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

Pagination notes
----------------
Both Etherscan V2 and Blockscout cap a single txlist call at 10,000
results (the "max result window" is 10000, meaning ``page * offset`` has
to stay <= 10000). For wallets that have more than 10k transactions
(exchanges, routers, very active DeFi users) we chain multiple calls
using block-range sliding:

    1. Fetch the most recent 10k with sort=desc
    2. Find the oldest block in that page
    3. Fetch the next 10k using endblock = (oldest_block - 1)
    4. Repeat until we have max_results rows or the chain runs out

We also apply a small per-request sleep and an exponential backoff on
429/5xx so we stay inside the documented free-tier limits:

    Etherscan V2 free tier : ~5 req/s, 100k req/day
    Blockscout public      : shared, treat as ~2 req/s safe
"""

import asyncio
import logging
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


logger = logging.getLogger(__name__)


# Per-request timeout (seconds). Public endpoints can be slow under load
# so this gives them enough room without making the UI feel stuck.
HTTP_TIMEOUT = 15.0

# Max rows Etherscan/Blockscout will return in a single page before the
# "Max result window is 10000" error kicks in.
MAX_PAGE_SIZE = 10000

# Default rows per scan for the three depth modes exposed by the API.
DEPTH_PRESETS = {
    "quick": 200,
    "standard": 1000,
    "deep": 5000,
}
DEFAULT_MAX_RESULTS = DEPTH_PRESETS["standard"]

# Throttle between requests so we stay under the documented rate limits.
# Etherscan V2 free tier: ~5 req/s, Blockscout public: treat ~2 req/s as safe.
ETHERSCAN_REQUEST_DELAY = 0.25
BLOCKSCOUT_REQUEST_DELAY = 0.5

# Retry schedule for 429 / 5xx errors (seconds between attempts).
# Kept short so a slow upstream never blocks the UI for more than a
# few seconds total before we fall through to an empty result.
RETRY_BACKOFF = (0.25, 0.5, 1.0)

# Marker strings the upstreams use when a page overruns the 10k window.
MAX_WINDOW_MARKERS = (
    "max result window is 10000",
    "result window is too large",
)


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


def _request_delay() -> float:
    return ETHERSCAN_REQUEST_DELAY if _has_etherscan_key() else BLOCKSCOUT_REQUEST_DELAY


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
# Low-level HTTP with retries
# =====================================================================

class MaxWindowError(Exception):
    """Raised when the upstream reports the 10k max result window."""


async def _get_with_retry(url: str, params: dict) -> Optional[dict]:
    """
    GET ``url`` with ``params`` and return the parsed JSON body.

    Retries on 429 and 5xx using the RETRY_BACKOFF schedule. Connection
    and timeout errors get one retry only (total wall time is bounded
    to about 2 * HTTP_TIMEOUT), and any parse failure bails immediately.
    Returns None when every retry fails. Raises ``MaxWindowError`` when
    the upstream tells us we walked past the 10k result window so the
    caller can break out of its paging loop cleanly.
    """
    last_err: Optional[Exception] = None
    # One initial attempt plus the backoff schedule.
    schedule = (0.0,) + RETRY_BACKOFF
    # Connection/timeout failures get ``max_network_attempts`` total
    # tries - any more than that and we would block the UI.
    max_network_attempts = 2
    network_failures = 0

    for attempt, delay in enumerate(schedule):
        if delay:
            await asyncio.sleep(delay)
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                resp = await client.get(url, params=params)
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout,
                httpx.ReadError, httpx.RemoteProtocolError) as exc:
            last_err = exc
            network_failures += 1
            logger.debug("network error on %s: %s", url, exc)
            if network_failures >= max_network_attempts:
                break
            continue
        except httpx.HTTPError as exc:
            last_err = exc
            logger.debug("http error on %s: %s", url, exc)
            break

        if resp.status_code in (429, 500, 502, 503, 504):
            last_err = httpx.HTTPStatusError(
                f"upstream {resp.status_code}", request=resp.request, response=resp
            )
            logger.debug(
                "retrying %s after HTTP %s (attempt %s)",
                url, resp.status_code, attempt + 1,
            )
            continue
        if resp.status_code >= 400:
            logger.debug("client error %s on %s", resp.status_code, url)
            return None

        try:
            data = resp.json()
        except ValueError as exc:
            last_err = exc
            return None

        # Etherscan/Blockscout use status="0" for both "no results" and
        # "something went wrong". The message field is the only way to
        # tell the two apart, and specifically to detect the 10k cap.
        message = str(data.get("message") or data.get("result") or "").lower()
        if any(marker in message for marker in MAX_WINDOW_MARKERS):
            raise MaxWindowError(message)
        return data

    if last_err:
        logger.warning("giving up on %s after retries: %s", url, last_err)
    return None


async def _blockscout_request(params: dict) -> list[dict]:
    """
    Hit the public Blockscout v1 endpoint with the given query parameters.
    Blockscout mirrors the Etherscan classic schema, so the result rows
    can be normalized by the same downstream code.
    """
    data = await _get_with_retry(BLOCKSCOUT_BASE, params)
    if not data:
        return []
    # Blockscout uses status="1" on success, "0" when there are no results
    # or on a soft error. Either way an empty list is the safe answer.
    if str(data.get("status")) != "1":
        return []
    result = data.get("result")
    return result if isinstance(result, list) else []


async def _etherscan_request(params: dict) -> list[dict]:
    """Hit Etherscan V2 with the given params and return the result list."""
    full_params = {"chainid": "1", "apikey": ETHERSCAN_API_KEY, **params}
    data = await _get_with_retry(ETHERSCAN_V2_BASE_URL, full_params)
    if not data:
        return []
    if str(data.get("status")) != "1":
        return []
    result = data.get("result")
    return result if isinstance(result, list) else []


async def _upstream_request(params: dict) -> list[dict]:
    """Dispatch to the active upstream and wait for the rate limit window."""
    if _has_etherscan_key():
        rows = await _etherscan_request(params)
    else:
        rows = await _blockscout_request(params)
    # Per-request sleep keeps us comfortably under the free-tier limits.
    await asyncio.sleep(_request_delay())
    return rows


# =====================================================================
# Pagination helper (shared by txlist and tokentx)
# =====================================================================

def _row_block(row: dict) -> Optional[int]:
    """Best-effort int parse of the blockNumber field on a raw row."""
    try:
        return int(row.get("blockNumber"))
    except (TypeError, ValueError):
        return None


async def _paginated_fetch(
    params_template: dict,
    max_results: int,
    use_blockrange: bool = True,
) -> tuple[list[dict], bool]:
    """
    Generic paginator for Etherscan-style endpoints.

    Walks pages 1..10 at offset=1000 (covering the whole 10k result
    window) and, if ``use_blockrange`` is set and the caller still wants
    more, slides the ``endblock`` back to before the oldest row we saw
    and keeps walking. Returns ``(rows, complete)`` where ``complete``
    is True when the last page was smaller than the page size (meaning
    the upstream ran out of data before we hit ``max_results``).

    The paginator never raises. Upstream failures are logged and the
    function returns whatever rows it managed to collect.
    """
    collected: list[dict] = []
    # A sensible page size: big enough that one window is one request,
    # small enough that any single page fits in memory comfortably.
    page_size = min(1000, max_results)
    end_block: Optional[int] = None
    complete = False

    while len(collected) < max_results:
        window_rows: list[dict] = []
        # Sliding the endblock down resets the paging cursor back to 1.
        for page in range(1, (MAX_PAGE_SIZE // page_size) + 1):
            remaining = max_results - len(collected) - len(window_rows)
            if remaining <= 0:
                break

            params = {
                **params_template,
                "page": str(page),
                "offset": str(min(page_size, remaining)),
            }
            if end_block is not None:
                params["endblock"] = str(end_block)

            try:
                rows = await _upstream_request(params)
            except MaxWindowError:
                # The upstream says the next page would cross the 10k
                # window. Break out of the page loop and, if block-range
                # paging is enabled, slide the window down using the
                # oldest block we have already seen.
                logger.debug("hit max result window; sliding block range")
                break

            if not rows:
                # No more data at all (not just this window) - the scan
                # is complete and we can stop entirely.
                complete = True
                break

            window_rows.extend(rows)

            # Fewer rows than requested means this is the last page in
            # the current block range. Break and let the outer loop
            # decide whether to slide or stop.
            if len(rows) < page_size:
                break

        collected.extend(window_rows)

        # Stop conditions -------------------------------------------------
        if complete:
            break
        if len(collected) >= max_results:
            break
        if not use_blockrange:
            break
        if not window_rows:
            break

        # Slide the window: pick the oldest block we just saw and ask
        # for everything before it on the next iteration.
        oldest_block: Optional[int] = None
        for row in window_rows:
            blk = _row_block(row)
            if blk is None:
                continue
            if oldest_block is None or blk < oldest_block:
                oldest_block = blk
        if oldest_block is None or oldest_block <= 0:
            # Without a usable blockNumber we cannot slide safely.
            break
        new_end = oldest_block - 1
        if end_block is not None and new_end >= end_block:
            # Didn't move - bail out to avoid an infinite loop.
            break
        end_block = new_end

    # Trim any overshoot so the caller always sees exactly the cap.
    if len(collected) > max_results:
        collected = collected[:max_results]

    return collected, complete


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
    max_results: int = DEFAULT_MAX_RESULTS,
) -> tuple[list[dict], bool]:
    """
    Fetch native ETH transactions for the address.

    Returns ``(rows, complete)`` where ``complete`` is True when we
    drained everything the wallet has. For quick scans (``max_results``
    <= 1000) this is a single page call; for deeper scans it uses the
    sliding-block-range paginator.
    """
    params_template = {
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": "0",
        "endblock": "99999999",
        "sort": "desc",
    }
    return await _paginated_fetch(
        params_template,
        max_results=max_results,
        use_blockrange=max_results > 1000,
    )


async def fetch_erc20_transfers(
    address: str,
    max_results: int = DEFAULT_MAX_RESULTS,
) -> tuple[list[dict], bool]:
    """
    Fetch ERC-20 token transfers for the address.

    Returns ``(rows, complete)`` following the same convention as
    ``fetch_normal_transactions``.
    """
    params_template = {
        "module": "account",
        "action": "tokentx",
        "address": address,
        "startblock": "0",
        "endblock": "99999999",
        "sort": "desc",
    }
    return await _paginated_fetch(
        params_template,
        max_results=max_results,
        use_blockrange=max_results > 1000,
    )


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
        try:
            rows = await asyncio.wait_for(
                _etherscan_request(params) if _has_etherscan_key()
                else _blockscout_request(params),
                timeout=20.0,
            )
        except (asyncio.TimeoutError, Exception):  # noqa: BLE001 - best-effort
            return None
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

def _classify_completeness(row_count: int, complete: bool, max_results: int) -> str:
    """
    Decide what to tell the UI about how complete the returned sample is.

    - "full" means the upstream ran out of data before we hit the cap,
      so we are confident the returned set is everything.
    - "partial" means we saturated the cap (10k+ rows) and the wallet
      likely has more history than we fetched.
    - "sample" means we are in quick-scan mode (<=200 rows) and the
      wallet almost certainly has more.
    """
    if complete:
        return "full"
    if row_count <= 200 and max_results <= 200:
        return "sample"
    return "partial"


async def get_wallet_data(address: str, max_results: int = DEFAULT_MAX_RESULTS) -> dict:
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
    txs_task = fetch_normal_transactions(address, max_results=max_results)
    tokens_task = fetch_erc20_transfers(address, max_results=max_results)
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
    tx_result = _unwrap(results[1], "transactions", ([], False))
    token_result = _unwrap(results[2], "token_transfers", ([], False))
    raw_internal = _unwrap(results[3], "internal_transactions", [])
    first_tx = _unwrap(results[4], "first_transaction", None)

    raw_txs, tx_complete = tx_result if isinstance(tx_result, tuple) else (tx_result, False)
    raw_tokens, tokens_complete = token_result if isinstance(token_result, tuple) else (token_result, False)

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

    # Data completeness classification drives the UI disclaimer. We use
    # the transaction series as the primary signal since that is what
    # the risk engine leans on hardest.
    tx_completeness = _classify_completeness(len(raw_txs), tx_complete, max_results)
    token_completeness = _classify_completeness(len(raw_tokens), tokens_complete, max_results)

    # A single top-level label for the frontend: report the worse of the
    # two series (sample < partial < full).
    order = {"sample": 0, "partial": 1, "full": 2}
    data_completeness = min(
        tx_completeness, token_completeness, key=lambda v: order[v]
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
        "max_results": max_results,
        "data_completeness": data_completeness,
        "tx_completeness": tx_completeness,
        "token_completeness": token_completeness,
        # Kept for backwards compat with the existing frontend hook.
        "data_truncated": data_completeness != "full",
        "page_limit": max_results,
        "errors": errors,
    }
