"""
Layer 3: AI-Powered Compliance Narrative Generation

This layer demonstrates the AI skill -- taking structured risk data
and generating human-readable compliance reports that a compliance
officer can use directly. Falls back to rule-based generation if
no API key is configured.

In production, this would use fine-tuned models trained on actual
compliance report templates and regulatory language.
"""

import os
from datetime import datetime, timezone

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False

from app.config import ANTHROPIC_API_KEY


def _build_prompt(analysis: dict) -> str:
    """Build the prompt for AI narrative generation."""
    address = analysis["address"]
    risk = analysis["risk_score"]
    stats = analysis["statistics"]
    sanctions = analysis["sanctions_findings"]
    patterns = analysis["pattern_findings"]
    defi = analysis["defi_interactions"]
    regs = analysis["regulations_applicable"]

    sanctions_text = "None detected." if not sanctions else "\n".join(
        f"  - [{f['severity']}] {f['description']}" for f in sanctions
    )
    patterns_text = "No notable patterns." if not patterns else "\n".join(
        f"  - [{f['severity']}] {f['description']}" for f in patterns
    )
    defi_text = "No known DeFi protocol interactions." if not defi else "\n".join(
        f"  - {name}: {info['count']} interactions" for name, info in defi.items()
    )
    regs_text = "\n".join(
        f"  - {r['name']} (deadline: {r['deadline']}): {r['relevance']}" for r in regs
    )

    return f"""You are a blockchain compliance analyst generating a risk assessment report.
Write a professional compliance report for the following wallet analysis.
The report should be suitable for a compliance officer reviewing this address
as part of their KYC/AML obligations.

WALLET ANALYSIS DATA:
- Address: {address}
- Risk Score: {risk['score']}/100 ({risk['level']})
- ETH Balance: {stats['eth_balance']:.4f} ETH
- Total Transactions: {stats['total_transactions']}
- Token Transfers: {stats['total_token_transfers']}
- Unique Counterparties: {stats['unique_counterparties']}
- Address Age: {stats['address_age_days'] or 'Unknown'} days
- Total ETH Sent: {stats['total_eth_sent']} ETH
- Total ETH Received: {stats['total_eth_received']} ETH

SANCTIONS SCREENING:
{sanctions_text}

PATTERN ANALYSIS:
{patterns_text}

DEFI PROTOCOL INTERACTIONS:
{defi_text}

APPLICABLE REGULATIONS:
{regs_text}

Write the report with these sections:
1. EXECUTIVE SUMMARY (2-3 sentences)
2. RISK ASSESSMENT (explain the score and what drives it)
3. KEY FINDINGS (bullet points of notable items)
4. REGULATORY IMPLICATIONS (which regulations apply and what action is needed)
5. RECOMMENDED ACTIONS (specific next steps for the compliance team)

Keep it professional, factual, and actionable. Do not speculate beyond what the data shows.
Use compliance industry terminology where appropriate (EDD, SAR, CDD, etc.).
Format with markdown headers and bullet points."""


async def generate_ai_narrative(analysis: dict) -> str:
    """
    Generate a compliance narrative using Claude API.
    Falls back to rule-based generation if API is unavailable.
    """
    if HAS_ANTHROPIC and ANTHROPIC_API_KEY:
        try:
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            prompt = _build_prompt(analysis)

            message = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1500,
                messages=[{"role": "user", "content": prompt}],
            )
            return message.content[0].text
        except Exception as e:
            # Fall back to rule-based if API call fails
            return _generate_rule_based_narrative(analysis)
    else:
        return _generate_rule_based_narrative(analysis)


def _generate_rule_based_narrative(analysis: dict) -> str:
    """
    Rule-based compliance narrative generation.
    Used when AI API is not available. Demonstrates that the
    system works standalone without external AI dependencies.
    """
    address = analysis["address"]
    risk = analysis["risk_score"]
    stats = analysis["statistics"]
    sanctions = analysis["sanctions_findings"]
    patterns = analysis["pattern_findings"]
    defi = analysis["defi_interactions"]
    regs = analysis["regulations_applicable"]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Executive summary based on risk level
    if risk["level"] == "CRITICAL":
        exec_summary = (
            f"This address presents **critical compliance risk** with a score of {risk['score']}/100. "
            f"OFAC sanctions exposure has been detected. Immediate review and potential blocking is recommended "
            f"per regulatory obligations under MiCA Article 76 and FinCEN guidelines."
        )
    elif risk["level"] == "HIGH":
        exec_summary = (
            f"This address presents **elevated compliance risk** with a score of {risk['score']}/100. "
            f"Multiple risk indicators have been identified that warrant enhanced due diligence (EDD) "
            f"and potential escalation to the compliance team for manual review."
        )
    elif risk["level"] in ["MEDIUM", "MEDIUM-LOW"]:
        exec_summary = (
            f"This address presents **moderate compliance risk** with a score of {risk['score']}/100. "
            f"Some risk indicators have been identified but none reach critical thresholds. "
            f"Standard customer due diligence (CDD) with periodic monitoring is recommended."
        )
    else:
        exec_summary = (
            f"This address presents **low compliance risk** with a score of {risk['score']}/100. "
            f"No sanctions exposure detected and transaction patterns appear consistent with normal "
            f"DeFi usage. Standard monitoring protocols are sufficient."
        )

    # Build findings section
    findings_items = []
    if sanctions:
        for f in sanctions:
            findings_items.append(f"- **SANCTIONS:** {f['description']}")
    if patterns:
        for f in patterns:
            findings_items.append(f"- **{f['type'].upper()}:** {f['description']}")
    if defi:
        protocol_list = ", ".join(defi.keys())
        findings_items.append(f"- **DeFi Activity:** Interacts with {len(defi)} known protocol(s): {protocol_list}")
    if not findings_items:
        findings_items.append("- No notable risk indicators detected in the analyzed transaction set.")

    findings_text = "\n".join(findings_items)

    # Regulatory implications
    reg_items = []
    for reg in regs:
        reg_items.append(f"- **{reg['name']}** (deadline: {reg['deadline']}): {reg['relevance']}")
    reg_text = "\n".join(reg_items)

    # Recommended actions based on risk level
    if risk["level"] == "CRITICAL":
        actions = (
            "1. **Immediately escalate** to the Chief Compliance Officer (CCO)\n"
            "2. **Consider blocking** transactions with this address pending investigation\n"
            "3. **File a Suspicious Activity Report (SAR)** if applicable in your jurisdiction\n"
            "4. **Document** all interactions with this address for regulatory records\n"
            "5. **Review** all other addresses that have transacted with this wallet"
        )
    elif risk["level"] == "HIGH":
        actions = (
            "1. **Initiate Enhanced Due Diligence (EDD)** on this address and its counterparties\n"
            "2. **Flag for ongoing monitoring** with increased frequency (daily vs. weekly)\n"
            "3. **Review transaction history** in detail for potential structuring patterns\n"
            "4. **Consider** whether a SAR filing threshold has been met\n"
            "5. **Document** risk assessment rationale in compliance records"
        )
    elif risk["level"] in ["MEDIUM", "MEDIUM-LOW"]:
        actions = (
            "1. **Continue standard CDD** monitoring at regular intervals\n"
            "2. **Note** flagged patterns in the address risk profile\n"
            "3. **Re-evaluate** if activity pattern changes significantly\n"
            "4. **No immediate escalation** required unless additional context warrants it"
        )
    else:
        actions = (
            "1. **Standard monitoring** -- no additional action required at this time\n"
            "2. **Periodic re-scan** recommended (monthly or quarterly)\n"
            "3. **Archive** this report for compliance documentation purposes"
        )

    report = f"""## Compliance Risk Assessment Report

**Address:** `{address}`
**Report Generated:** {now}
**Risk Score:** {risk['score']}/100 ({risk['level']})

---

### 1. Executive Summary

{exec_summary}

---

### 2. Wallet Profile

| Metric | Value |
|--------|-------|
| ETH Balance | {stats['eth_balance']:.4f} ETH |
| Total Transactions | {stats['total_transactions']} |
| Token Transfers | {stats['total_token_transfers']} |
| Unique Counterparties | {stats['unique_counterparties']} |
| Address Age | {stats['address_age_days'] or 'Unknown'} days |
| ETH Sent | {stats['total_eth_sent']} ETH |
| ETH Received | {stats['total_eth_received']} ETH |

---

### 3. Key Findings

{findings_text}

---

### 4. Regulatory Implications

{reg_text}

---

### 5. Recommended Actions

{actions}

---

*This report was generated by an automated compliance scanning system. It should be reviewed by a qualified compliance professional before any enforcement actions are taken. Data sourced from Ethereum mainnet via Etherscan API.*
"""
    return report
