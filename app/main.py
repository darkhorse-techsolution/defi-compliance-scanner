"""
DeFi Compliance Risk Scanner
=============================

A prototype demonstrating the intersection of:

- Data Engineering: blockchain data ingestion and normalization pipelines
- Blockchain: on-chain transaction analysis and protocol classification
- AI: intelligent compliance narrative generation

Built by Marc Watters / ComplianceNode.
"""

import logging
import re
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.ai_narrative import generate_ai_narrative, has_anthropic_key
from app.blockchain import DEPTH_PRESETS, active_data_source, get_wallet_data
from app.config import ETHERSCAN_API_KEY
from app.risk_engine import run_risk_analysis
from app.sanctions import (
    batch_check_chainalysis_oracle,
    chainalysis_oracle_available,
    check_chainalysis_oracle,
    get_sanctioned_addresses,
    sanctions_status,
)

logger = logging.getLogger(__name__)

# --- App setup -----------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Warm the sanctions cache on boot so the first scan is instant."""
    try:
        addrs = await get_sanctioned_addresses()
        logger.info("sanctions list warmed: %d addresses", len(addrs))
    except Exception as exc:  # noqa: BLE001 - never block startup
        logger.warning("sanctions warmup failed: %s", exc)
    yield


app = FastAPI(
    title="DeFi Compliance Risk Scanner",
    description="On-chain compliance intelligence powered by data engineering, blockchain analytics, and AI",
    version="0.3.0",
    lifespan=lifespan,
)

app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# Ethereum address validation
ETH_ADDRESS_REGEX = re.compile(r"^0x[0-9a-fA-F]{40}$")


def _has_etherscan_key() -> bool:
    return bool(ETHERSCAN_API_KEY) and ETHERSCAN_API_KEY != "your_etherscan_api_key_here"


async def _oracle_check_top_counterparties(
    address: str,
    wallet_data: dict,
    max_counterparties: int = 10,
) -> dict[str, list[dict]]:
    """
    Hit the Chainalysis public sanctions oracle for (1) the scanned
    address itself and (2) the top N counterparties by transaction
    count. Any hits are merged into the live sanctions cache used by
    the risk engine so they get flagged as full CRITICAL findings
    during analysis.

    Returns a dict mapping each sanctioned address to its list of
    Chainalysis identification records. The identification data
    (sanctioning body, designation date, source URL) is used to build
    higher-quality sanctions findings in the report.
    """
    from app.sanctions import _SANCTIONS_CACHE  # local import to avoid cycle

    candidates: list[str] = [address.lower()]

    tx_df = wallet_data.get("transactions")
    try:
        import pandas as pd  # noqa: WPS433 - fine inside a function
        if hasattr(tx_df, "empty") and not tx_df.empty:
            top = (
                tx_df["counterparty"]
                .dropna()
                .astype(str)
                .str.lower()
                .value_counts()
                .head(max_counterparties)
                .index.tolist()
            )
            for cp in top:
                if cp and cp not in candidates:
                    candidates.append(cp)
    except Exception as exc:  # noqa: BLE001
        logger.debug("counterparty extraction failed: %s", exc)

    if not candidates:
        return {}

    try:
        hits = await batch_check_chainalysis_oracle(candidates, max_concurrent=5)
    except Exception as exc:  # noqa: BLE001
        logger.warning("chainalysis oracle batch check failed: %s", exc)
        return {}

    if hits:
        # Merge the hits into the risk-engine-visible cache so the
        # synchronous screen_sanctions call picks them up on this scan.
        _SANCTIONS_CACHE.update(hits.keys())

    return hits


def _resolve_depth(value) -> tuple[str, int]:
    """
    Turn an incoming ``depth`` value into a ``(label, max_results)`` pair.

    Accepts either a preset label (``quick`` / ``standard`` / ``deep``)
    or a raw integer. Unknown strings fall back to ``standard``. Raw
    integers are clamped to the 10k upstream ceiling.
    """
    if value is None or value == "":
        return "standard", DEPTH_PRESETS["standard"]
    if isinstance(value, str):
        label = value.strip().lower()
        if label in DEPTH_PRESETS:
            return label, DEPTH_PRESETS[label]
    try:
        n = int(value)
    except (TypeError, ValueError):
        return "standard", DEPTH_PRESETS["standard"]
    n = max(50, min(n, 10000))
    return "custom", n


# --- Routes --------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Serve the main application page."""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/health")
async def health():
    """
    Health check endpoint. Returns the upstream the scanner is configured
    to use, whether optional API keys are set, and the current state of
    the sanctions cache. Useful for prospects verifying the deployment
    is hitting real data.
    """
    status = sanctions_status()
    return {
        "status": "ok",
        "service": "defi-compliance-scanner",
        "version": app.version,
        "data_source": active_data_source(),
        "etherscan_api_key": "configured" if _has_etherscan_key() else "not_configured",
        "anthropic_api_key": "configured" if has_anthropic_key() else "not_configured",
        "sanctions_list_size": status["size"],
        "sanctions_source": status["source"],
        "sanctions_source_breakdown": status.get("source_breakdown", {}),
        "sanctions_updated_at": status["updated_at"],
        "chainalysis_oracle": "enabled" if chainalysis_oracle_available() else "disabled",
        "depth_presets": DEPTH_PRESETS,
    }


@app.post("/api/scan")
async def scan_address(request: Request):
    """
    Main scan endpoint. Takes an Ethereum address, runs the full
    compliance analysis pipeline, and returns the result.

    Request body:
        {
            "address": "0x...",
            "depth": "quick" | "standard" | "deep"  (optional, default "standard")
        }

    Pipeline:
      1. Warm the OFAC sanctions cache (no-op on warm boots)
      2. Fetch on-chain data (data engineering layer)
      3. Run risk analysis (blockchain analytics layer)
      4. Generate compliance narrative (AI layer)
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

    depth_label, max_results = _resolve_depth(body.get("depth"))

    try:
        # Make sure the risk engine sees a fresh sanctions list even on
        # cold boots where the lifespan warmup may still be in flight.
        await get_sanctioned_addresses()

        wallet_data = await get_wallet_data(address, max_results=max_results)

        # Defence in depth: run the Chainalysis public oracle against
        # the scanned address and the top N counterparties by volume.
        # Any oracle hits get merged into the in-memory sanctions set
        # before the risk engine walks the transaction list, so an
        # address added between cache refreshes is still caught. This
        # is a no-op when no CHAINALYSIS_API_KEY is configured.
        oracle_hits: dict[str, list] = {}
        if chainalysis_oracle_available():
            oracle_hits = await _oracle_check_top_counterparties(address, wallet_data)

        analysis = run_risk_analysis(wallet_data)
        if oracle_hits:
            analysis["oracle_hits"] = {
                addr: ids for addr, ids in oracle_hits.items()
            }
            # Surface the oracle-added hits as real CRITICAL findings
            # that the UI can render alongside the on-chain findings.
            for addr, identifications in oracle_hits.items():
                for ident in identifications[:2]:  # cap per-address
                    analysis.setdefault("sanctions_findings", []).append({
                        "type": "chainalysis_oracle_hit",
                        "severity": "CRITICAL",
                        "description": (
                            f"{ident.get('category', 'sanctions').upper()}: "
                            f"{ident.get('name', 'Unnamed designation')} "
                            f"({addr[:10]}...)"
                        ),
                        "risk_score": 100,
                        "source_url": ident.get("url"),
                    })
            # Force the top-line risk level to CRITICAL if the oracle
            # confirmed anything - even one true positive from a fresh
            # sanctions designation outweighs the on-chain heuristics.
            analysis["risk_score"]["level"] = "CRITICAL"
            analysis["risk_score"]["score"] = 100
            analysis["risk_score"]["recommendation"] = (
                "Critical: Chainalysis sanctions oracle flagged this address "
                "or one of its direct counterparties. Block immediately and "
                "escalate to your compliance officer."
            )
        narrative, narrative_source = await generate_ai_narrative(analysis)
        analysis["narrative"] = narrative
        analysis["narrative_source"] = narrative_source
        analysis["data_source"] = wallet_data.get("data_source", "unknown")
        analysis["data_completeness"] = wallet_data.get("data_completeness")
        analysis["depth"] = depth_label
        analysis["max_results"] = max_results
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
