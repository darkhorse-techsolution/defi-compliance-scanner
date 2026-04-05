"""
Layer 1: Blockchain Data Ingestion

Multi-source data pipeline:
- Etherscan V2 API (when API key is configured)
- Public RPC endpoints (free, no key needed, for balance/basic data)
- Demo dataset (realistic pre-loaded data for portfolio demonstrations)

This is the data engineering layer -- extracting and normalizing
blockchain data into structured formats for analysis.
"""

import asyncio
import httpx
import pandas as pd
from datetime import datetime, timezone
from typing import Optional

from app.config import ETHERSCAN_API_KEY

# Etherscan V2 API base
ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"

# Free public RPC (no API key needed)
PUBLIC_RPC = "https://ethereum-rpc.publicnode.com"


# =====================================================================
# Public RPC Layer (works without any API key)
# =====================================================================

async def fetch_balance_rpc(address: str) -> dict:
    """Fetch ETH balance via free public RPC."""
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.post(
                PUBLIC_RPC,
                json={
                    "jsonrpc": "2.0",
                    "method": "eth_getBalance",
                    "params": [address, "latest"],
                    "id": 1,
                },
                headers={"Content-Type": "application/json"},
            )
            data = resp.json()
            if "result" in data:
                balance_wei = int(data["result"], 16)
                return {"balance_wei": balance_wei, "balance_eth": balance_wei / 1e18}
        except Exception:
            pass
    return {"balance_wei": 0, "balance_eth": 0.0}


# =====================================================================
# Etherscan V2 Layer (requires free API key)
# =====================================================================

def _has_etherscan_key() -> bool:
    """Check if a valid Etherscan API key is configured."""
    return bool(ETHERSCAN_API_KEY) and ETHERSCAN_API_KEY != "your_etherscan_api_key_here"


async def fetch_eth_balance(address: str) -> dict:
    """Fetch current ETH balance using Etherscan V2 or public RPC fallback."""
    if _has_etherscan_key():
        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.get(
                    ETHERSCAN_V2_BASE,
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
                if data.get("status") == "1":
                    balance_wei = int(data["result"])
                    return {"balance_wei": balance_wei, "balance_eth": balance_wei / 1e18}
            except Exception:
                pass

    # Fallback to public RPC
    return await fetch_balance_rpc(address)


async def fetch_normal_transactions(
    address: str, start_block: int = 0, end_block: int = 99999999,
    page: int = 1, offset: int = 200
) -> list[dict]:
    """Fetch normal (ETH) transactions for an address via Etherscan V2."""
    if not _has_etherscan_key():
        return []

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            ETHERSCAN_V2_BASE,
            params={
                "chainid": "1",
                "module": "account",
                "action": "txlist",
                "address": address,
                "startblock": start_block,
                "endblock": end_block,
                "page": page,
                "offset": offset,
                "sort": "desc",
                "apikey": ETHERSCAN_API_KEY,
            },
        )
        data = resp.json()
        if data.get("status") == "1" and isinstance(data.get("result"), list):
            return data["result"]
    return []


async def fetch_erc20_transfers(
    address: str, page: int = 1, offset: int = 200
) -> list[dict]:
    """Fetch ERC-20 token transfers for an address via Etherscan V2."""
    if not _has_etherscan_key():
        return []

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            ETHERSCAN_V2_BASE,
            params={
                "chainid": "1",
                "module": "account",
                "action": "tokentx",
                "address": address,
                "page": page,
                "offset": offset,
                "sort": "desc",
                "apikey": ETHERSCAN_API_KEY,
            },
        )
        data = resp.json()
        if data.get("status") == "1" and isinstance(data.get("result"), list):
            return data["result"]
    return []


async def fetch_internal_transactions(
    address: str, page: int = 1, offset: int = 100
) -> list[dict]:
    """Fetch internal transactions via Etherscan V2."""
    if not _has_etherscan_key():
        return []

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            ETHERSCAN_V2_BASE,
            params={
                "chainid": "1",
                "module": "account",
                "action": "txlistinternal",
                "address": address,
                "page": page,
                "offset": offset,
                "sort": "desc",
                "apikey": ETHERSCAN_API_KEY,
            },
        )
        data = resp.json()
        if data.get("status") == "1" and isinstance(data.get("result"), list):
            return data["result"]
    return []


# =====================================================================
# Demo Data (realistic pre-loaded data for portfolio demonstrations)
# =====================================================================

def _get_demo_data(address: str) -> Optional[dict]:
    """
    Return pre-loaded demo data for known demonstration addresses.
    This ensures the portfolio demo works perfectly without any API keys.

    Data is modeled after real on-chain patterns but uses synthetic
    transaction hashes and timestamps.
    """
    demos = _build_demo_datasets()
    return demos.get(address.lower())


def _build_demo_datasets() -> dict:
    """Build realistic demo datasets for portfolio addresses."""
    import time

    now = int(time.time())
    day = 86400

    datasets = {}

    # --- Demo 1: Normal DeFi User (low risk) ---
    addr1 = "0xd8da6bf26964af9d7eed9e03e53415d37aa96045"  # Vitalik
    datasets[addr1] = {
        "balance": {"balance_wei": 1327200000000000000, "balance_eth": 1.3272},
        "transactions": [
            _tx(addr1, "0x7a250d5630b4cf539739df2c5dacb4c659f2488d", 2.5, now - 2 * day, "out"),
            _tx("0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad", addr1, 0.1, now - 3 * day, "in"),
            _tx(addr1, "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2", 5.0, now - 5 * day, "out"),
            _tx("0xdef1c0ded9bec7f1a1670819833240f027b25eff", addr1, 1.2, now - 7 * day, "in"),
            _tx(addr1, "0xba12222222228d8ba445958a75a0704d566bf2c8", 3.0, now - 10 * day, "out"),
            _tx("0x1111111254eeb25477b68fb85ed929f73a960582", addr1, 0.8, now - 12 * day, "in"),
            _tx(addr1, "0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b", 1.5, now - 15 * day, "out"),
            _tx("0xbebc44782c7db0a1a60cb6fe97d0b483032f535c", addr1, 2.1, now - 20 * day, "in"),
            _tx(addr1, "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45", 0.5, now - 25 * day, "out"),
            _tx("0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f", addr1, 1.0, now - 800 * day, "in"),
        ],
        "token_transfers": [
            _token_tx(addr1, "0x7a250d5630b4cf539739df2c5dacb4c659f2488d", "USDC", 6, 50000, now - 2 * day, "out"),
            _token_tx("0xdef1c0ded9bec7f1a1670819833240f027b25eff", addr1, "USDT", 6, 25000, now - 7 * day, "in"),
            _token_tx(addr1, "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2", "WETH", 18, 3.5, now - 5 * day, "out"),
            _token_tx("0xba12222222228d8ba445958a75a0704d566bf2c8", addr1, "DAI", 18, 10000, now - 10 * day, "in"),
            _token_tx(addr1, "0x1111111254eeb25477b68fb85ed929f73a960582", "UNI", 18, 500, now - 12 * day, "out"),
            _token_tx("0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad", addr1, "AAVE", 18, 25, now - 3 * day, "in"),
        ],
        "internal_transactions": [],
        "address_age_days": 800,
    }

    # --- Demo 2: High-volume exchange wallet (medium risk) ---
    addr2 = "0x47ac0fb4f2d84898e4d9e7b4dab3c24507a6d503"  # Binance
    datasets[addr2] = {
        "balance": {"balance_wei": 245000000000000000000000, "balance_eth": 245000.0},
        "transactions": [
            _tx(addr2, "0xab5801a7d398351b8be11c439e05c5b3259aec9b", 150.0, now - 1 * day, "out"),
            _tx("0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13", addr2, 500.0, now - 1 * day, "in"),
            _tx(addr2, "0xf977814e90da44bfa03b6295a0616a897441acec", 1200.0, now - 2 * day, "out"),
            _tx("0x28c6c06298d514db089934071355e5743bf21d60", addr2, 800.0, now - 2 * day, "in"),
            _tx(addr2, "0x21a31ee1afc51d94c2efccaa2092ad1028285549", 350.0, now - 3 * day, "out"),
            _tx(addr2, "0x56eddb7aa87536c09ccc2793473599fd21a8b17f", 95.0, now - 3 * day, "out"),
            _tx("0xdfd5293d8e347dfe59e90efd55b2956a1343963d", addr2, 2500.0, now - 4 * day, "in"),
            _tx(addr2, "0xf977814e90da44bfa03b6295a0616a897441acec", 3000.0, now - 5 * day, "out"),
            _tx("0x28c6c06298d514db089934071355e5743bf21d60", addr2, 1800.0, now - 300 * day, "in"),
            _tx(addr2, "0xf977814e90da44bfa03b6295a0616a897441acec", 5000.0, now - 600 * day, "out"),
        ],
        "token_transfers": [
            _token_tx(addr2, "0xf977814e90da44bfa03b6295a0616a897441acec", "USDC", 6, 5000000, now - 1 * day, "out"),
            _token_tx("0x28c6c06298d514db089934071355e5743bf21d60", addr2, "USDT", 6, 3000000, now - 2 * day, "in"),
            _token_tx(addr2, "0xf977814e90da44bfa03b6295a0616a897441acec", "BUSD", 18, 2000000, now - 3 * day, "out"),
            _token_tx("0xdfd5293d8e347dfe59e90efd55b2956a1343963d", addr2, "WETH", 18, 500, now - 4 * day, "in"),
            _token_tx(addr2, "0x21a31ee1afc51d94c2efccaa2092ad1028285549", "BNB", 18, 10000, now - 5 * day, "out"),
        ],
        "internal_transactions": [],
        "address_age_days": 1800,
    }

    # --- Demo 3: OFAC Sanctioned address (critical risk) ---
    addr3 = "0x8589427373d6d84e98730d7795d8f6f8731fda16"  # Tornado Cash Router
    datasets[addr3] = {
        "balance": {"balance_wei": 50000000000000000, "balance_eth": 0.05},
        "transactions": [
            _tx("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", addr3, 100.0, now - 30 * day, "in"),
            _tx(addr3, "0xd90e2f925da726b50c4ed8d0fb90ad053324f31b", 50.0, now - 30 * day, "out"),
            _tx(addr3, "0xd96f2b1c14db8458374d9aca76e26c3d18364307", 10.0, now - 31 * day, "out"),
            _tx("0x4736dcf1b7a3d580672cce6e7c65cd5cc9cfbcd6", addr3, 5.0, now - 32 * day, "in"),
            _tx(addr3, "0x722122df12d4e14e13ac3b6895a86e84145b6967", 20.0, now - 33 * day, "out"),
        ],
        "token_transfers": [],
        "internal_transactions": [],
        "address_age_days": 900,
    }

    # --- Demo 4: Binance Hot Wallet (low risk, high volume) ---
    addr4 = "0xbe0eb53f46cd790cd13851d5eff43d12404d33e8"
    datasets[addr4] = {
        "balance": {"balance_wei": 1975000000000000000000000, "balance_eth": 1975000.0},
        "transactions": [
            _tx(addr4, "0x47ac0fb4f2d84898e4d9e7b4dab3c24507a6d503", 5000.0, now - 1 * day, "out"),
            _tx("0xf977814e90da44bfa03b6295a0616a897441acec", addr4, 8000.0, now - 2 * day, "in"),
            _tx(addr4, "0x28c6c06298d514db089934071355e5743bf21d60", 3500.0, now - 3 * day, "out"),
            _tx("0xdfd5293d8e347dfe59e90efd55b2956a1343963d", addr4, 12000.0, now - 4 * day, "in"),
            _tx(addr4, "0x56eddb7aa87536c09ccc2793473599fd21a8b17f", 200.0, now - 5 * day, "out"),
            _tx("0x47ac0fb4f2d84898e4d9e7b4dab3c24507a6d503", addr4, 15000.0, now - 6 * day, "in"),
            _tx(addr4, "0xf977814e90da44bfa03b6295a0616a897441acec", 7000.0, now - 7 * day, "out"),
            _tx("0x28c6c06298d514db089934071355e5743bf21d60", addr4, 20000.0, now - 2000 * day, "in"),
        ],
        "token_transfers": [
            _token_tx(addr4, "0x47ac0fb4f2d84898e4d9e7b4dab3c24507a6d503", "USDC", 6, 10000000, now - 1 * day, "out"),
            _token_tx("0xf977814e90da44bfa03b6295a0616a897441acec", addr4, "USDT", 6, 8000000, now - 2 * day, "in"),
            _token_tx(addr4, "0x28c6c06298d514db089934071355e5743bf21d60", "WBTC", 8, 100, now - 3 * day, "out"),
        ],
        "internal_transactions": [],
        "address_age_days": 2000,
    }

    return datasets


def _tx(from_addr: str, to_addr: str, value_eth: float, timestamp: int, direction: str) -> dict:
    """Create a synthetic transaction record matching Etherscan format."""
    import hashlib
    hash_input = f"{from_addr}{to_addr}{value_eth}{timestamp}".encode()
    tx_hash = "0x" + hashlib.sha256(hash_input).hexdigest()

    return {
        "hash": tx_hash,
        "from": from_addr,
        "to": to_addr,
        "value": str(int(value_eth * 1e18)),
        "timeStamp": str(timestamp),
        "gasUsed": "21000",
        "gasPrice": "20000000000",
        "blockNumber": str(18000000 + (timestamp % 100000)),
        "isError": "0",
        "txreceipt_status": "1",
    }


def _token_tx(from_addr: str, to_addr: str, symbol: str, decimals: int,
              amount: float, timestamp: int, direction: str) -> dict:
    """Create a synthetic token transfer record matching Etherscan format."""
    import hashlib
    hash_input = f"{from_addr}{to_addr}{symbol}{amount}{timestamp}".encode()
    tx_hash = "0x" + hashlib.sha256(hash_input).hexdigest()

    return {
        "hash": tx_hash,
        "from": from_addr,
        "to": to_addr,
        "value": str(int(amount * (10 ** decimals))),
        "tokenName": symbol,
        "tokenSymbol": symbol,
        "tokenDecimal": str(decimals),
        "timeStamp": str(timestamp),
        "gasUsed": "65000",
        "gasPrice": "20000000000",
        "contractAddress": "0x" + hashlib.md5(symbol.encode()).hexdigest()[:40],
    }


# =====================================================================
# Data Normalization (same for live and demo data)
# =====================================================================

def normalize_transactions(raw_txs: list[dict], address: str) -> pd.DataFrame:
    """
    Data engineering: normalize raw blockchain transactions into
    a clean DataFrame with derived fields for analysis.
    """
    if not raw_txs:
        return pd.DataFrame()

    df = pd.DataFrame(raw_txs)

    # Type conversions
    df["value_eth"] = df["value"].astype(float) / 1e18
    df["timeStamp"] = pd.to_numeric(df["timeStamp"])
    df["datetime"] = pd.to_datetime(df["timeStamp"], unit="s", utc=True)
    df["gasUsed"] = pd.to_numeric(df.get("gasUsed", 0), errors="coerce").fillna(0)
    df["gasPrice"] = pd.to_numeric(df.get("gasPrice", 0), errors="coerce").fillna(0)
    df["gas_cost_eth"] = (df["gasUsed"] * df["gasPrice"]) / 1e18

    # Direction classification
    addr_lower = address.lower()
    df["direction"] = df.apply(
        lambda row: "outgoing" if row["from"].lower() == addr_lower else "incoming",
        axis=1,
    )

    # Counterparty extraction
    df["counterparty"] = df.apply(
        lambda row: row["to"] if row["from"].lower() == addr_lower else row["from"],
        axis=1,
    )

    return df


def normalize_token_transfers(raw_transfers: list[dict], address: str) -> pd.DataFrame:
    """Normalize ERC-20 token transfer data."""
    if not raw_transfers:
        return pd.DataFrame()

    df = pd.DataFrame(raw_transfers)

    # Token amount with decimal adjustment
    df["tokenDecimal"] = pd.to_numeric(df["tokenDecimal"], errors="coerce").fillna(18)
    df["value_raw"] = pd.to_numeric(df["value"], errors="coerce").fillna(0)
    df["token_amount"] = df["value_raw"] / (10 ** df["tokenDecimal"])

    df["timeStamp"] = pd.to_numeric(df["timeStamp"])
    df["datetime"] = pd.to_datetime(df["timeStamp"], unit="s", utc=True)

    addr_lower = address.lower()
    df["direction"] = df.apply(
        lambda row: "outgoing" if row["from"].lower() == addr_lower else "incoming",
        axis=1,
    )
    df["counterparty"] = df.apply(
        lambda row: row["to"] if row["from"].lower() == addr_lower else row["from"],
        axis=1,
    )

    return df


# =====================================================================
# Main Pipeline
# =====================================================================

async def get_wallet_data(address: str) -> dict:
    """
    Main data ingestion pipeline: fetches all relevant on-chain data
    for a wallet address and returns normalized DataFrames.

    Strategy:
    1. Check demo data first (for portfolio demonstrations)
    2. If Etherscan API key is configured, use live Etherscan V2 data
    3. Otherwise, use public RPC for balance only

    This demonstrates the data engineering skill -- taking messy
    blockchain API responses and producing clean, analysis-ready data.
    """
    address = address.strip().lower()

    # Check for demo data first
    demo = _get_demo_data(address)
    if demo is not None:
        # Use demo data but try to get live balance
        live_balance = await fetch_balance_rpc(address)
        if live_balance["balance_wei"] > 0:
            demo["balance"] = live_balance

        tx_df = normalize_transactions(demo["transactions"], address)
        token_df = normalize_token_transfers(demo["token_transfers"], address)

        return {
            "address": address,
            "balance": demo["balance"],
            "transactions": tx_df,
            "token_transfers": token_df,
            "internal_transactions": demo["internal_transactions"],
            "transaction_count": len(demo["transactions"]),
            "token_transfer_count": len(demo["token_transfers"]),
            "address_age_days": demo["address_age_days"],
            "data_source": "demo" if not _has_etherscan_key() else "live+demo",
        }

    # Live data path
    balance_task = fetch_eth_balance(address)
    txs_task = fetch_normal_transactions(address)
    tokens_task = fetch_erc20_transfers(address)
    internal_task = fetch_internal_transactions(address)

    balance, raw_txs, raw_tokens, raw_internal = await asyncio.gather(
        balance_task, txs_task, tokens_task, internal_task
    )

    tx_df = normalize_transactions(raw_txs, address)
    token_df = normalize_token_transfers(raw_tokens, address)

    # Calculate address age
    address_age_days = None
    if not tx_df.empty:
        earliest_tx = tx_df["datetime"].min()
        address_age_days = (datetime.now(timezone.utc) - earliest_tx).days

    return {
        "address": address,
        "balance": balance,
        "transactions": tx_df,
        "token_transfers": token_df,
        "internal_transactions": raw_internal,
        "transaction_count": len(raw_txs),
        "token_transfer_count": len(raw_tokens),
        "address_age_days": address_age_days,
        "data_source": "etherscan_v2" if _has_etherscan_key() else "public_rpc",
    }
