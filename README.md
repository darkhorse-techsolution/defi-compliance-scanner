# DeFi Compliance Risk Scanner

> Real-time blockchain compliance analysis. Scan any Ethereum wallet for OFAC sanctions exposure, transaction risk patterns, and regulatory red flags in under 10 seconds.

**Live demo:** https://defi-compliance-scanner.onrender.com
**Built by:** Marc Watters / [ComplianceNode](https://compliancenode-website.pages.dev)
**Source:** https://github.com/darkhorse-techsolution/defi-compliance-scanner

---

## Try it now

Open the live demo and paste any of these into the scan box.

| Address | Expected result |
|---------|-----------------|
| `0x098B716B8Aaf21512996dC57EB0615e2383E2f96` | **CRITICAL** — Lazarus Group, multiple sanctions identifications |
| `0x8589427373D6D84E98730D7795D8f6f8731FDA16` | **CRITICAL** — Tornado Cash Router, ~21 sanctions findings |
| `0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045` | **LOW** — Vitalik Buterin, clean wallet |
| `0xbe0eb53f46cd790cd13851d5eff43d12404d33e8` | **LOW** — Binance cold wallet, clean (~1.99M ETH balance) |

A finished scan returns a risk score (0–100), severity-tiered findings (sanctions / patterns / DeFi), a regulatory mapping, and a CCO-ready compliance memo.

---

## What this proves

A working compliance product needs four things to be done well at the same time, and most engineers can only do one or two of them. This scanner stitches all four together in a single repo:

1. **Data engineering** — pull live, paginated, rate-limited blockchain data and normalize it into typed pandas frames the rest of the system can trust
2. **Blockchain domain knowledge** — know which counterparties matter, how mixers move funds, what an Etherscan internal-tx vs token-transfer actually means
3. **Risk modeling** — translate transaction graphs into a defensible composite score that maps to MiCA, FATF Travel Rule, and BSA expectations
4. **AI integration** — generate the kind of narrative output a compliance officer will actually paste into a SAR

The deliverable a compliance team or DeFi protocol pays for is the pipeline behind those four steps, productionized against their data. This repo is what I show prospects so they know I can build it.

---

## How it works

Three-layer pipeline. Each layer is a single Python module so it stays readable and easy to swap.

```
   Ethereum address
         |
         v
   ┌───────────────────────┐
   │ 1. Data ingestion     │  app/blockchain.py
   │    Etherscan V2       │  (Blockscout fallback)
   │    paginated, async   │
   └──────────┬────────────┘
              v
   ┌───────────────────────┐
   │ 2. Sanctions screen   │  app/sanctions.py
   │    778 OFAC addresses │  + Chainalysis Oracle
   └──────────┬────────────┘
              v
   ┌───────────────────────┐
   │ 3. Risk analysis      │  app/risk_engine.py
   │    pattern detection  │  composite 0-100 score
   │    regulatory mapping │
   └──────────┬────────────┘
              v
   ┌───────────────────────┐
   │ 4. AI narrative       │  app/ai_narrative.py
   │    Claude or rule-    │  CCO-ready memo
   │    based fallback     │
   └──────────┬────────────┘
              v
       JSON response
```

### 1. Data ingestion (`app/blockchain.py`)
Async fetch through Etherscan V2 (when `ETHERSCAN_API_KEY` is set) with a Blockscout free-tier fallback so the scanner runs end-to-end with zero configuration. Both upstreams cap a single call at 10,000 results, so the module chains paginated calls using block-range sliding for high-volume wallets, with per-request sleep and exponential backoff on 429/5xx.

### 2. Sanctions screening (`app/sanctions.py`)
Pulls and merges 778 sanctioned addresses across 13 chains from the [0xB10C OFAC mirror](https://github.com/0xB10C/ofac-sanctioned-digital-currency-addresses) on top of a hardcoded baseline. When `CHAINALYSIS_API_KEY` is configured, the scanner also hits the [Chainalysis public sanctions oracle](https://go.chainalysis.com/chainalysis-oracle-docs.html) for the scanned address and its top counterparties as a defence-in-depth check against fresh designations.

### 3. Risk analysis (`app/risk_engine.py`)
Direct sanctions hits, indirect counterparty exposure, high-value transfers, rapid in-out movement, counterparty concentration, DeFi-protocol classification. Each finding carries a severity tier (`INFO` / `LOW` / `MEDIUM` / `HIGH` / `CRITICAL`) and contributes to a weighted composite score. Surface output includes a regulatory mapping (MiCA, FATF Travel Rule, BSA) so the report points a CCO at the framework that applies.

### 4. Report generation (`app/ai_narrative.py`)
Structured findings get fed into Claude (when `ANTHROPIC_API_KEY` is set) to produce a markdown memo a compliance officer can drop into an EDD or SAR workflow. With no key configured, the same data renders through a deterministic rule-based template so the demo never breaks.

---

## Tech stack

- **Backend:** Python 3.11, FastAPI, Pandas
- **HTTP:** `httpx` async client with retry and rate limiting
- **Frontend:** Vanilla JavaScript, Chart.js, no build step
- **AI:** Claude API (optional, falls back to rule-based templates)
- **Deploy:** Docker container on Render

The frontend is intentionally framework-free so the entire UI surface — theme toggle, hash-deep-linking, status pills, severity coloring — fits in two readable files (`static/style.css`, `static/app.js`).

---

## Why not just buy Chainalysis?

Chainalysis KYT is the right answer when you need a global labeled-address graph, attribution data sourced from law enforcement partnerships, and a vendor name on the contract that satisfies a regulator out of the box. Most regulated VASPs above a certain size will end up paying for it.

A custom pipeline like this is the right answer when:

- You need to **integrate the screening into your own product** (a wallet, a DEX, an OTC desk), not just receive alerts in a dashboard
- You want **risk logic specific to your business** — your own thresholds, your own counterparty whitelist, your own SAR routing
- You're **filling the gap before** a Chainalysis contract is in place, or augmenting it with a second independent check
- You're a **smaller protocol or fund** where a six-figure annual subscription doesn't pencil but you still need defensible screening

In practice the two coexist. A real client engagement uses Chainalysis as a labeled-data source where it makes sense, and uses a custom pipeline like this to glue it to the parts of the business that actually move money.

---

## Run locally

```bash
git clone https://github.com/darkhorse-techsolution/defi-compliance-scanner
cd defi-compliance-scanner
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python run.py
# open http://localhost:8000
```

All API keys are optional. With zero configuration the scanner runs end-to-end using free Blockscout for on-chain data and the open-source GitHub OFAC mirror for sanctions.

### Docker

```bash
docker build -t defi-compliance-scanner .
docker run -p 8000:8000 defi-compliance-scanner
```

---

## Configuration

Copy `.env.example` to `.env` and fill in any keys you have. Everything is optional.

| Variable | Effect when set |
|----------|-----------------|
| `ETHERSCAN_API_KEY` | Switches the data source from Blockscout to Etherscan V2 (faster, higher rate limit, better coverage) |
| `ANTHROPIC_API_KEY` | Enables Claude-generated compliance narratives instead of the rule-based template |
| `CHAINALYSIS_API_KEY` | Enables a live Chainalysis sanctions-oracle check on the scanned address and its top counterparties |

Hit `GET /api/health` at any time to see which upstreams are currently active and how many sanctions records are cached.

---

## Honest scope

This is a portfolio demonstration of the engineering approach a real compliance data pipeline would use. It is not a replacement for Chainalysis KYT, TRM Labs, or any other licensed compliance product.

In a real client engagement I extend this with:

- Cross-chain coverage beyond Ethereum (Tron and Bitcoin matter most for sanctions screening)
- Deeper graph traversal for indirect sanctions exposure (currently one hop)
- Integration with the client's alerting and SAR workflow (Slack, PagerDuty, case-management systems)
- Labeled address databases beyond the open-source mirrors (Chainalysis, Elliptic, TRM, internal lists)
- Risk models tuned on the client's own historical flagged transactions
- Audit trail, role-based access, and the operational tooling a regulator expects to see

That work is what a retainer engagement pays for. This scanner demonstrates that I can deliver it.

---

## Contact

**Marc Watters** — compliance data engineering for crypto firms, exchanges, and DeFi protocols.

- Portfolio: https://compliancenode-website.pages.dev
- Email: marc@compliancenode.io
- Source: https://github.com/darkhorse-techsolution/defi-compliance-scanner

If you're a CCO, founder, or compliance lead at a regulated digital-asset business and you need someone who can build the data pipeline behind your screening program, get in touch.
