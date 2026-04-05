# DeFi Compliance Risk Scanner

**Built by Dark Horse Solution (Marc Watters)**

A real-time blockchain compliance analysis tool that scans Ethereum wallet addresses for regulatory risk. Demonstrates the full pipeline that compliance-facing crypto firms need: data ingestion, risk analytics, and actionable reporting.

---

## What It Does

Enter any Ethereum address and the scanner runs a three-layer compliance pipeline:

1. **Data Engineering Layer** -- Ingests raw blockchain transactions from Ethereum mainnet, normalizes messy API responses into structured datasets, and enriches with derived fields (direction classification, counterparty extraction, gas cost computation).

2. **Blockchain Analytics Layer** -- Screens all counterparties against the OFAC sanctions list, detects suspicious transaction patterns (high-value transfers, rapid fund movement, counterparty concentration), classifies DeFi protocol interactions, and computes a composite risk score from 0-100.

3. **AI Intelligence Layer** -- Generates professional compliance narrative reports with regulatory context (MiCA, GENIUS Act, FATF Travel Rule), actionable recommendations, and proper compliance terminology (EDD, SAR, CDD). Reports are ready for a compliance officer to use directly.

## Why This Matters

Crypto firms facing MiCA (July 2026), GENIUS Act (January 2027), and FATF Travel Rule enforcement must implement transaction monitoring and sanctions screening. Enterprise tools cost $100K-$500K/year. This demonstrates what a custom compliance data pipeline looks like at a fraction of that cost.

## Risk Scoring

| Score Range | Level | Action |
|-------------|-------|--------|
| 0-20 | LOW | Standard monitoring |
| 21-40 | MEDIUM-LOW | Routine enhanced monitoring |
| 41-60 | MEDIUM | Enhanced due diligence recommended |
| 61-80 | HIGH | Manual investigation recommended |
| 81-100 | CRITICAL | Immediate review, potential blocking |

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Backend | Python, FastAPI | API server with async request handling |
| Data Pipeline | Pandas, httpx, asyncio | Concurrent data ingestion and normalization |
| Blockchain Data | Etherscan V2 API, Public RPC | Multi-source on-chain data with fallback |
| Risk Engine | Custom Python | Rule-based scoring with configurable weights |
| AI Reports | Claude API (with rule-based fallback) | Professional compliance narrative generation |
| Frontend | Vanilla HTML/CSS/JS | Zero-dependency UI, no build step |
| Deployment | Docker, Uvicorn | Single-container deployment |

## Architecture

```
User enters address
        |
        v
[FastAPI Server]
        |
        v
[Layer 1: Data Engineering]
  - Fetch ETH balance (public RPC)
  - Fetch transactions (Etherscan V2)
  - Fetch token transfers (Etherscan V2)
  - Normalize into DataFrames
  - Derive: direction, counterparty, gas costs
        |
        v
[Layer 2: Risk Analysis]
  - OFAC sanctions screening
  - Transaction pattern detection
  - DeFi protocol classification
  - Composite risk scoring
        |
        v
[Layer 3: AI Narrative]
  - Generate compliance report
  - Include regulatory context
  - Actionable recommendations
        |
        v
[JSON Response -> Frontend Dashboard]
```

## Running Locally

```bash
# 1. Clone and enter the directory
cd prototype/defi-compliance-scanner

# 2. Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. (Optional) Configure API keys for live data
cp .env.example .env
# Edit .env with your Etherscan and Anthropic API keys
# The app works without them using demo data and rule-based reports

# 5. Run the server
python run.py

# 6. Open http://localhost:8000 in your browser
```

## Running with Docker

```bash
docker build -t defi-compliance-scanner .
docker run -p 8000:8000 --env-file .env defi-compliance-scanner
```

## Demo Mode

The scanner includes pre-loaded demo data for four well-known Ethereum addresses, so it works as a portfolio piece with zero configuration:

- **Vitalik Buterin** -- Normal DeFi user, interacts with Uniswap, Aave, Compound, Balancer. LOW risk.
- **Binance Cold Wallet** -- High-volume exchange wallet with large transfers. MEDIUM risk.
- **Tornado Cash Router** -- OFAC-sanctioned mixer contract. CRITICAL risk (100/100).
- **Binance Hot Wallet** -- Large exchange operational wallet. LOW-MEDIUM risk.

ETH balances are always fetched live from the Ethereum network via public RPC, even in demo mode.

## What This Demonstrates

This prototype showcases the rare skill intersection that compliance-facing crypto firms need:

- **Data Engineering**: Building concurrent data pipelines that ingest, normalize, and enrich blockchain data from multiple sources (API + RPC + fallback datasets)
- **Blockchain**: Deep understanding of Ethereum transaction structures, DeFi protocol addresses, OFAC sanctions lists, and on-chain risk patterns
- **AI**: Generating actionable compliance narratives that reference specific regulations (MiCA, GENIUS Act, FATF) and use proper compliance terminology

Most compliance tools are built by either (a) compliance people who cannot engineer data pipelines, or (b) general developers who do not understand blockchain data structures or regulatory requirements. This tool demonstrates all three capabilities in a single pipeline.

## Production Roadmap

This prototype covers the core concept. A production version would add:

- [ ] Multi-chain support (Polygon, Arbitrum, BSC, Solana)
- [ ] Real-time OFAC SDN list synchronization via Treasury API
- [ ] ML-based anomaly detection trained on labeled transaction data
- [ ] Graph analysis for multi-hop counterparty risk (2nd/3rd degree exposure)
- [ ] Travel Rule compliance data formatting (FATF R.16)
- [ ] MiCA CASP reporting templates
- [ ] Webhook alerts for monitored addresses
- [ ] Historical risk score tracking and trend analysis
- [ ] Integration with Chainalysis, Elliptic, TRM Labs APIs
- [ ] Client dashboard with role-based access
