"""
Application configuration loaded from environment variables.

Data source notes
-----------------
The scanner reads live Ethereum mainnet data from one of two upstreams,
depending on whether an Etherscan key is provided:

  ETHERSCAN_API_KEY (optional)
      When set, the scanner uses the Etherscan V2 unified API. This is
      the preferred path because it has higher rate limits and includes
      richer historical data. Get a free key at https://etherscan.io/myapikey

  No key configured (default)
      The scanner falls back to the free Blockscout public API at
      https://eth.blockscout.com which returns data in the same schema
      as Etherscan and requires no signup. Rate limits are lower so
      heavy use should configure an Etherscan key.

ETH balance lookups always go through a public JSON-RPC endpoint
because both Etherscan and Blockscout occasionally lag behind chain
state for balance queries.
"""

import os

from dotenv import load_dotenv

load_dotenv()

# --- API keys (optional) -------------------------------------------------
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# --- Server config -------------------------------------------------------
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))

# --- Upstream endpoints --------------------------------------------------
# Etherscan V2 (single endpoint, multi-chain via chainid param)
ETHERSCAN_V2_BASE_URL = "https://api.etherscan.io/v2/api"

# Blockscout free Ethereum mainnet endpoint (no API key required)
BLOCKSCOUT_BASE = "https://eth.blockscout.com/api"

# Public JSON-RPC endpoint used for live balance lookups
PUBLIC_RPC_URL = os.getenv("PUBLIC_RPC_URL", "https://ethereum-rpc.publicnode.com")


# --- OFAC sanctioned addresses (subset for screening demo) ---------------
# In production this would be hydrated from the official OFAC SDN feed.
SANCTIONED_ADDRESSES = {
    "0x8589427373d6d84e98730d7795d8f6f8731fda16",  # Tornado Cash: Router
    "0xd90e2f925da726b50c4ed8d0fb90ad053324f31b",  # Tornado Cash: 100 ETH
    "0xd96f2b1c14db8458374d9aca76e26c3d18364307",  # Tornado Cash: 10 ETH
    "0x4736dcf1b7a3d580672cce6e7c65cd5cc9cfbcd6",  # Tornado Cash: 1 ETH
    "0x722122df12d4e14e13ac3b6895a86e84145b6967",  # Tornado Cash: Proxy
    "0x905b63fff5e9f254ae3a2db10a10e4a3fdf32d75",  # Lazarus Group related
    "0x098b716b8aaf21512996dc57eb0615e2383e2f96",  # Ronin Bridge Exploiter
    "0xa7e5d5a720f06526557c513402f2e6b5fa20b008",  # Ronin Bridge Exploiter 2
    "0x19aa5fe80d33a56d56c78e82ea5e50e5d80b4dff",  # Tornado Cash: 0.1 ETH
    "0xb541fc07bc7619fd4062a54d96268525cbc6ffef",  # Tornado Cash: 1000 ETH
}

# --- Known DeFi protocol addresses for classification --------------------
KNOWN_PROTOCOLS = {
    "0x7a250d5630b4cf539739df2c5dacb4c659f2488d": "Uniswap V2: Router",
    "0xe592427a0aece92de3edee1f18e0157c05861564": "Uniswap V3: Router",
    "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45": "Uniswap V3: Router 2",
    "0xdef1c0ded9bec7f1a1670819833240f027b25eff": "0x: Exchange Proxy",
    "0x1111111254eeb25477b68fb85ed929f73a960582": "1inch v5: Router",
    "0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9": "Aave V2: Lending Pool",
    "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2": "Aave V3: Pool",
    "0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b": "Compound: Comptroller",
    "0xba12222222228d8ba445958a75a0704d566bf2c8": "Balancer: Vault",
    "0xbebc44782c7db0a1a60cb6fe97d0b483032f535c": "Curve: 3pool",
    "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f": "SushiSwap: Router",
    "0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad": "Uniswap: Universal Router",
}

# --- Risk weight configuration -------------------------------------------
RISK_WEIGHTS = {
    "sanctioned_interaction": 100,   # Direct interaction with OFAC sanctioned address
    "high_value_transfer": 15,       # Transfers over a high notional threshold
    "mixer_interaction": 80,         # Known mixer/tumbler usage
    "rapid_movement": 25,            # Funds moved quickly through address
    "new_address": 10,               # Address less than 30 days old
    "concentrated_counterparty": 20, # Most volume to/from a single address
    "bridge_usage": 5,               # Cross-chain bridge interaction
    "known_protocol": -10,           # Verified DeFi protocol use (risk reducer)
}
