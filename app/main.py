"""
DeFi Compliance Risk Scanner
=============================

A prototype demonstrating the intersection of:

- Data Engineering: blockchain data ingestion and normalization pipelines
- Blockchain: on-chain transaction analysis and protocol classification
- AI: intelligent compliance narrative generation

Built by Marc Watters / ComplianceNode.
"""

import re
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.ai_narrative import generate_ai_narrative
from app.blockchain import active_data_source, get_wallet_data
from app.config import ANTHROPIC_API_KEY, ETHERSCAN_API_KEY
from app.risk_engine import run_risk_analysis

# --- App setup -----------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent

app = FastAPI(
    title="DeFi Compliance Risk Scanner",
    description="On-chain compliance intelligence powered by data engineering, blockchain analytics, and AI",
    version="0.2.0",
)

app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# Ethereum address validation
ETH_ADDRESS_REGEX = re.compile(r"^0x[0-9a-fA-F]{40}$")


def _has_etherscan_key() -> bool:
    return bool(ETHERSCAN_API_KEY) and ETHERSCAN_API_KEY != "your_etherscan_api_key_here"


# --- Routes --------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Serve the main application page."""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/health")
async def health():
    """
    Health check endpoint. Returns the upstream the scanner is configured
    to use plus whether optional API keys (Etherscan, Anthropic) are set.
    Useful for prospects who want to verify the deployment is hitting
    real chain data.
    """
    return {
        "status": "ok",
        "service": "defi-compliance-scanner",
        "version": app.version,
        "data_source": active_data_source(),
        "etherscan_api_key": "configured" if _has_etherscan_key() else "not_configured",
        "anthropic_api_key": "configured" if ANTHROPIC_API_KEY else "not_configured",
    }


@app.post("/api/scan")
async def scan_address(request: Request):
    """
    Main scan endpoint. Takes an Ethereum address, runs the full
    compliance analysis pipeline, and returns the result.

    Pipeline:
      1. Fetch on-chain data (data engineering layer)
      2. Run risk analysis (blockchain analytics layer)
      3. Generate compliance narrative (AI layer)
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Request body must be valid JSON.")

    address = (body.get("address") or "").strip()
    if not ETH_ADDRESS_REGEX.match(address):
        raise HTTPException(
            status_code=400,
            detail="Invalid Ethereum address. Must be 0x followed by 40 hexadecimal characters.",
        )

    try:
        wallet_data = await get_wallet_data(address)
        analysis = run_risk_analysis(wallet_data)
        analysis["narrative"] = await generate_ai_narrative(analysis)
        analysis["data_source"] = wallet_data.get("data_source", "unknown")
        return JSONResponse(content=analysis)
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001 - want a friendly error to the UI
        raise HTTPException(
            status_code=500,
            detail=f"Analysis failed: {type(e).__name__}: {e}",
        )


@app.get("/api/example-addresses")
async def example_addresses():
    """
    Return example wallet addresses for the UI to display as quick-start
    chips. These are well-known public addresses on Ethereum mainnet.
    """
    return {
        "addresses": [
            {
                "address": "0x8589427373D6D84E98730D7795D8f6f8731FDA16",
                "label": "Tornado Cash Router",
                "expected_risk": "CRITICAL",
                "description": "OFAC-sanctioned mixer router contract.",
            },
            {
                "address": "0xBE0eB53F46cd790Cd13851d5EFf43D12404d33E8",
                "label": "Binance Cold Wallet",
                "expected_risk": "LOW",
                "description": "Major exchange operational wallet, very high volume.",
            },
            {
                "address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
                "label": "Vitalik Buterin",
                "expected_risk": "LOW",
                "description": "Ethereum co-founder, active DeFi user.",
            },
            {
                "address": "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D",
                "label": "Uniswap V2 Router",
                "expected_risk": "LOW",
                "description": "Canonical Uniswap V2 router contract.",
            },
        ]
    }
