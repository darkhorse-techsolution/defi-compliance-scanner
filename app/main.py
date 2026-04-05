"""
DeFi Compliance Risk Scanner
=============================

A prototype demonstrating the intersection of:
- Data Engineering: blockchain data ingestion and normalization pipelines
- Blockchain: on-chain transaction analysis and protocol classification
- AI: intelligent compliance narrative generation

Built by Dark Horse Solution (Marc Watters)
"""

import re
from pathlib import Path

from fastapi import FastAPI, Request, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse

from app.blockchain import get_wallet_data
from app.risk_engine import run_risk_analysis
from app.ai_narrative import generate_ai_narrative

# App setup
BASE_DIR = Path(__file__).resolve().parent.parent
app = FastAPI(
    title="DeFi Compliance Risk Scanner",
    description="On-chain compliance intelligence powered by data engineering, blockchain analytics, and AI",
    version="0.1.0",
)

app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# Ethereum address validation
ETH_ADDRESS_REGEX = re.compile(r"^0x[0-9a-fA-F]{40}$")


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Serve the main application page."""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/health")
async def health():
    """Health check endpoint."""
    return {"status": "ok", "service": "defi-compliance-scanner"}


@app.post("/api/scan")
async def scan_address(request: Request):
    """
    Main scan endpoint. Takes an Ethereum address, runs the full
    compliance analysis pipeline, and returns results.

    Pipeline:
    1. Fetch on-chain data (data engineering)
    2. Run risk analysis (blockchain analytics)
    3. Generate compliance narrative (AI)
    """
    body = await request.json()
    address = body.get("address", "").strip()

    # Validate address format
    if not ETH_ADDRESS_REGEX.match(address):
        raise HTTPException(
            status_code=400,
            detail="Invalid Ethereum address format. Must be 0x followed by 40 hex characters.",
        )

    try:
        # Layer 1: Data Engineering -- Ingest and normalize blockchain data
        wallet_data = await get_wallet_data(address)

        # Layer 2: Blockchain Analytics -- Risk scoring and pattern detection
        analysis = run_risk_analysis(wallet_data)

        # Layer 3: AI -- Generate compliance narrative report
        narrative = await generate_ai_narrative(analysis)
        analysis["narrative"] = narrative

        # Include data source metadata
        analysis["data_source"] = wallet_data.get("data_source", "unknown")

        return JSONResponse(content=analysis)

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Analysis failed: {str(e)}",
        )


@app.get("/api/demo-addresses")
async def demo_addresses():
    """
    Return example addresses for demonstration purposes.
    These are well-known public addresses.
    """
    return {
        "addresses": [
            {
                "address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
                "label": "Vitalik Buterin (Ethereum Co-founder)",
                "expected_risk": "LOW",
                "description": "Well-known public figure, active DeFi user",
            },
            {
                "address": "0x47ac0Fb4F2D84898e4D9E7b4DaB3C24507a6D503",
                "label": "Binance Cold Wallet",
                "expected_risk": "LOW-MEDIUM",
                "description": "Major exchange cold storage, high volume",
            },
            {
                "address": "0x8589427373D6D84E98730D7795D8f6f8731FDA16",
                "label": "Tornado Cash: Router (OFAC Sanctioned)",
                "expected_risk": "CRITICAL",
                "description": "OFAC-sanctioned mixer contract",
            },
            {
                "address": "0xBE0eB53F46cd790Cd13851d5EFf43D12404d33E8",
                "label": "Binance Hot Wallet",
                "expected_risk": "LOW",
                "description": "Major exchange operational wallet",
            },
        ]
    }
