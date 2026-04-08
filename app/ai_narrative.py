"""
Layer 3: AI-Powered Compliance Narrative Generation

Takes the structured risk report produced by the analytics layer and
turns it into a human-readable compliance memo. Two backends are
supported:

- **Claude API** (preferred). Activated automatically when the
  ``ANTHROPIC_API_KEY`` environment variable is set. We use the
  Anthropic async SDK with a structured prompt that asks Claude to
  return a short, factual, markdown-formatted compliance report.

- **Rule-based template** (fallback). Used when no key is configured
  or when the Claude call fails for any reason. Produces a report
  with the same section layout so the frontend doesn't have to care
  which backend answered.

The ``narrative_source`` field on the returned analysis dict tells
the frontend which backend was used so it can surface an honest
"Claude-generated" vs "Rule-based" badge.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

try:
    from anthropic import AsyncAnthropic
    HAS_ANTHROPIC = True
except ImportError:
    AsyncAnthropic = None  # type: ignore[assignment]
    HAS_ANTHROPIC = False


logger = logging.getLogger(__name__)


# Claude model used for narrative generation. Haiku is fast and cheap
# and the prompt is tiny, so latency is dominated by network RTT.
CLAUDE_MODEL = "claude-haiku-4-5"
CLAUDE_MAX_TOKENS = 1500
CLAUDE_TIMEOUT = 30.0


NarrativeResult = tuple[str, str]
"""Return type for generators: (markdown_body, source_label)."""


def has_anthropic_key() -> bool:
    """Return True when a Claude-capable API key is configured."""
    if not HAS_ANTHROPIC:
        return False
    return bool(os.getenv("ANTHROPIC_API_KEY"))


# =====================================================================
# Claude prompt construction
# =====================================================================

def _truncate_json(obj, limit: int = 2000) -> str:
    """Dump a structure as JSON, clipping to ``limit`` characters."""
    try:
        text = json.dumps(obj, indent=2, default=str)
    except (TypeError, ValueError):
        return "[]"
    if len(text) <= limit:
        return text
    return text[:limit] + "\n... (truncated)"


def _build_claude_prompt(analysis: dict) -> str:
    """Build the structured prompt we send to Claude."""
    stats = analysis.get("statistics", {}) or {}
    risk = analysis.get("risk_score", {}) or {}
    sanctions = analysis.get("sanctions_findings", []) or []
    patterns = analysis.get("pattern_findings", []) or []
    defi = analysis.get("defi_interactions", {}) or {}

    return (
        "You are a blockchain compliance analyst writing a professional risk "
        "assessment report. Your audience is a compliance officer at a crypto "
        "firm who needs to make a go / no-go decision on this wallet.\n"
        "\n"
        "Data to analyze:\n"
        f"- Wallet address: {analysis.get('address', 'unknown')}\n"
        f"- Risk score: {risk.get('score', 0)}/100 ({risk.get('level', 'UNKNOWN')})\n"
        f"- Sanctions findings: {len(sanctions)}\n"
        f"- Pattern findings: {len(patterns)}\n"
        f"- Total transactions analyzed: {stats.get('total_transactions', 0)}\n"
        f"- Address age (days): {stats.get('address_age_days', 'unknown')}\n"
        f"- ETH balance: {stats.get('eth_balance', 0)}\n"
        f"- Unique counterparties: {stats.get('unique_counterparties', 0)}\n"
        f"- Total ETH sent: {stats.get('total_eth_sent', 0)}\n"
        f"- Total ETH received: {stats.get('total_eth_received', 0)}\n"
        f"- Known DeFi protocols touched: {', '.join(defi.keys()) or 'none'}\n"
        "\n"
        "Sanctions findings detail:\n"
        f"{_truncate_json(sanctions)}\n"
        "\n"
        "Pattern findings detail:\n"
        f"{_truncate_json(patterns)}\n"
        "\n"
        "Write the report in Markdown with exactly these sections:\n"
        "1. Executive Summary (2-3 sentences on overall risk)\n"
        "2. Key Findings (bullet list of the most important observations)\n"
        "3. Regulatory Implications (which of MiCA, FATF Travel Rule, OFAC, "
        "and BSA are triggered and why)\n"
        "4. Recommended Actions (specific, ordered steps a compliance "
        "officer should take)\n"
        "\n"
        "Rules:\n"
        "- Be factual. Reference specific data points by number.\n"
        "- Use professional compliance language (EDD, CDD, SAR, CASP, VASP).\n"
        "- Do not use emojis.\n"
        "- Do not invent findings that are not in the data.\n"
        "- Maximum 600 words total."
    )


# =====================================================================
# Main entry point
# =====================================================================

async def generate_ai_narrative(analysis: dict) -> NarrativeResult:
    """
    Generate a compliance narrative for ``analysis``.

    Returns a ``(markdown, source)`` tuple. ``source`` is one of
    ``"claude"`` or ``"rule_based"`` so the caller can surface a
    trustworthy "which backend wrote this?" badge in the UI.
    """
    if has_anthropic_key() and AsyncAnthropic is not None:
        try:
            return await _generate_claude_narrative(analysis)
        except Exception as exc:  # noqa: BLE001 - fall back on any error
            logger.error("Claude narrative generation failed: %s", exc)

    return _generate_rule_based_narrative(analysis), "rule_based"


async def _generate_claude_narrative(analysis: dict) -> NarrativeResult:
    """Call Claude and format the response for the narrative card."""
    api_key = os.getenv("ANTHROPIC_API_KEY") or ""
    client = AsyncAnthropic(api_key=api_key, timeout=CLAUDE_TIMEOUT)
    prompt = _build_claude_prompt(analysis)

    message = await client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=CLAUDE_MAX_TOKENS,
        messages=[{"role": "user", "content": prompt}],
    )

    # Extract the text blocks from the response. The SDK returns a
    # list of content blocks; we concatenate the text ones and ignore
    # anything else (tool use, etc.) for robustness.
    chunks: list[str] = []
    for block in message.content or []:
        text = getattr(block, "text", None)
        if text:
            chunks.append(text)
    body = "".join(chunks).strip() or _generate_rule_based_narrative(analysis)

    header = (
        "## AI-generated compliance assessment\n\n"
        f"*Generated by Claude ({CLAUDE_MODEL}) on "
        f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}*\n\n"
    )
    return header + body, "claude"


# =====================================================================
# Rule-based fallback
# =====================================================================

def _generate_rule_based_narrative(analysis: dict) -> str:
    """
    Rule-based compliance narrative generation.

    Used when the Claude API is not available or the call fails. Keeps
    the section layout identical to the Claude output so the frontend
    can render either response with the same code path.
    """
    address = analysis.get("address", "")
    risk = analysis.get("risk_score", {}) or {}
    stats = analysis.get("statistics", {}) or {}
    sanctions = analysis.get("sanctions_findings", []) or []
    patterns = analysis.get("pattern_findings", []) or []
    defi = analysis.get("defi_interactions", {}) or {}
    regs = analysis.get("regulations_applicable", []) or []
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    level = risk.get("level", "LOW")
    score = risk.get("score", 0)

    # Executive summary based on risk level
    if level == "CRITICAL":
        exec_summary = (
            f"This address presents **critical compliance risk** with a score of {score}/100. "
            f"OFAC sanctions exposure has been detected. Immediate review and potential blocking is recommended "
            f"per regulatory obligations under MiCA Article 76 and FinCEN guidelines."
        )
    elif level == "HIGH":
        exec_summary = (
            f"This address presents **elevated compliance risk** with a score of {score}/100. "
            f"Multiple risk indicators have been identified that warrant enhanced due diligence (EDD) "
            f"and potential escalation to the compliance team for manual review."
        )
    elif level in ("MEDIUM", "MEDIUM-LOW"):
        exec_summary = (
            f"This address presents **moderate compliance risk** with a score of {score}/100. "
            f"Some risk indicators have been identified but none reach critical thresholds. "
            f"Standard customer due diligence (CDD) with periodic monitoring is recommended."
        )
    else:
        exec_summary = (
            f"This address presents **low compliance risk** with a score of {score}/100. "
            f"No sanctions exposure detected and transaction patterns appear consistent with normal "
            f"DeFi usage. Standard monitoring protocols are sufficient."
        )

    # Build findings section
    findings_items: list[str] = []
    for f in sanctions:
        findings_items.append(f"- **SANCTIONS:** {f.get('description', '')}")
    for f in patterns:
        findings_items.append(
            f"- **{(f.get('type') or 'finding').upper()}:** {f.get('description', '')}"
        )
    if defi:
        protocol_list = ", ".join(defi.keys())
        findings_items.append(
            f"- **DeFi Activity:** Interacts with {len(defi)} known protocol(s): {protocol_list}"
        )
    if not findings_items:
        findings_items.append(
            "- No notable risk indicators detected in the analyzed transaction set."
        )
    findings_text = "\n".join(findings_items)

    # Regulatory implications
    reg_items = [
        f"- **{r.get('name','?')}** (deadline: {r.get('deadline','?')}): {r.get('relevance','')}"
        for r in regs
    ]
    reg_text = "\n".join(reg_items) if reg_items else "- No specific regulations flagged."

    # Recommended actions based on risk level
    if level == "CRITICAL":
        actions = (
            "1. **Immediately escalate** to the Chief Compliance Officer (CCO)\n"
            "2. **Consider blocking** transactions with this address pending investigation\n"
            "3. **File a Suspicious Activity Report (SAR)** if applicable in your jurisdiction\n"
            "4. **Document** all interactions with this address for regulatory records\n"
            "5. **Review** all other addresses that have transacted with this wallet"
        )
    elif level == "HIGH":
        actions = (
            "1. **Initiate Enhanced Due Diligence (EDD)** on this address and its counterparties\n"
            "2. **Flag for ongoing monitoring** with increased frequency (daily vs. weekly)\n"
            "3. **Review transaction history** in detail for potential structuring patterns\n"
            "4. **Consider** whether a SAR filing threshold has been met\n"
            "5. **Document** risk assessment rationale in compliance records"
        )
    elif level in ("MEDIUM", "MEDIUM-LOW"):
        actions = (
            "1. **Continue standard CDD** monitoring at regular intervals\n"
            "2. **Note** flagged patterns in the address risk profile\n"
            "3. **Re-evaluate** if activity pattern changes significantly\n"
            "4. **No immediate escalation** required unless additional context warrants it"
        )
    else:
        actions = (
            "1. **Standard monitoring** - no additional action required at this time\n"
            "2. **Periodic re-scan** recommended (monthly or quarterly)\n"
            "3. **Archive** this report for compliance documentation purposes"
        )

    return f"""## Compliance risk assessment (rule-based)

**Address:** `{address}`
**Report generated:** {now}
**Risk score:** {score}/100 ({level})

---

### 1. Executive Summary

{exec_summary}

---

### 2. Wallet Profile

| Metric | Value |
|--------|-------|
| ETH Balance | {stats.get('eth_balance', 0):.4f} ETH |
| Total Transactions | {stats.get('total_transactions', 0)} |
| Token Transfers | {stats.get('total_token_transfers', 0)} |
| Unique Counterparties | {stats.get('unique_counterparties', 0)} |
| Address Age | {stats.get('address_age_days') or 'Unknown'} days |
| ETH Sent | {stats.get('total_eth_sent', 0)} ETH |
| ETH Received | {stats.get('total_eth_received', 0)} ETH |

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

*This report was generated by the rules-based fallback narrative engine. Set the ANTHROPIC_API_KEY environment variable to enable Claude-generated reports. All data sourced from live Ethereum mainnet.*
"""
