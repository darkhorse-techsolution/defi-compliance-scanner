"""
Layer 2: Risk Analysis Engine

This is the core analytics layer -- applying data engineering techniques
and rule-based risk scoring to blockchain data. In production, this would
integrate ML models trained on labeled transaction data.

Risk factors assessed:
- OFAC sanctions list screening
- Transaction pattern analysis (velocity, concentration)
- Mixer/tumbler interaction detection
- High-value transfer monitoring
- Address age and behavior profiling
- DeFi protocol interaction classification
"""

import pandas as pd
from datetime import datetime, timezone, timedelta
from collections import Counter

from app.config import SANCTIONED_ADDRESSES, KNOWN_PROTOCOLS, RISK_WEIGHTS


def screen_sanctions(wallet_data: dict) -> list[dict]:
    """
    Screen all counterparties against OFAC sanctions list.
    This is the #1 compliance requirement for any crypto firm.
    """
    findings = []
    address = wallet_data["address"]

    # Check if the address itself is sanctioned
    if address.lower() in SANCTIONED_ADDRESSES:
        findings.append({
            "type": "sanctioned_address",
            "severity": "CRITICAL",
            "description": f"Address {address[:10]}... is directly on the OFAC sanctions list",
            "risk_score": 100,
        })

    # Screen transaction counterparties
    tx_df = wallet_data["transactions"]
    if not tx_df.empty:
        for _, row in tx_df.iterrows():
            counterparty = row.get("counterparty", "")
            if counterparty and counterparty.lower() in SANCTIONED_ADDRESSES:
                findings.append({
                    "type": "sanctioned_interaction",
                    "severity": "CRITICAL",
                    "description": (
                        f"{'Sent to' if row['direction'] == 'outgoing' else 'Received from'} "
                        f"OFAC-sanctioned address {counterparty[:10]}... "
                        f"({row['value_eth']:.4f} ETH on {row['datetime'].strftime('%Y-%m-%d')})"
                    ),
                    "risk_score": RISK_WEIGHTS["sanctioned_interaction"],
                    "tx_hash": row.get("hash", ""),
                    "timestamp": row["datetime"].isoformat() if pd.notna(row["datetime"]) else None,
                })

    # Screen token transfer counterparties
    token_df = wallet_data["token_transfers"]
    if not token_df.empty:
        for _, row in token_df.iterrows():
            counterparty = row.get("counterparty", "")
            if counterparty and counterparty.lower() in SANCTIONED_ADDRESSES:
                findings.append({
                    "type": "sanctioned_interaction",
                    "severity": "CRITICAL",
                    "description": (
                        f"Token transfer {'to' if row['direction'] == 'outgoing' else 'from'} "
                        f"OFAC-sanctioned address: {row.get('tokenSymbol', 'Unknown')} "
                        f"on {row['datetime'].strftime('%Y-%m-%d')}"
                    ),
                    "risk_score": RISK_WEIGHTS["sanctioned_interaction"],
                    "tx_hash": row.get("hash", ""),
                    "timestamp": row["datetime"].isoformat() if pd.notna(row["datetime"]) else None,
                })

    return findings


def analyze_transaction_patterns(wallet_data: dict) -> list[dict]:
    """
    Analyze transaction patterns for suspicious behavior.
    This is where data engineering meets compliance analytics.
    """
    findings = []
    tx_df = wallet_data["transactions"]

    if tx_df.empty:
        return findings

    # 1. High-value transfer detection (> 10 ETH as proxy for ~$25K+)
    high_value_txs = tx_df[tx_df["value_eth"] > 10]
    if not high_value_txs.empty:
        count = len(high_value_txs)
        total_eth = high_value_txs["value_eth"].sum()
        findings.append({
            "type": "high_value_transfer",
            "severity": "MEDIUM" if count < 5 else "HIGH",
            "description": (
                f"{count} high-value transfer(s) detected "
                f"(total: {total_eth:,.2f} ETH). "
                f"Largest: {high_value_txs['value_eth'].max():,.2f} ETH"
            ),
            "risk_score": min(RISK_WEIGHTS["high_value_transfer"] * (count / 3), 50),
            "detail": {
                "count": count,
                "total_eth": round(total_eth, 4),
                "max_single": round(high_value_txs["value_eth"].max(), 4),
            },
        })

    # 2. Rapid fund movement (funds in and out within 1 hour)
    if len(tx_df) >= 2:
        tx_sorted = tx_df.sort_values("timeStamp")
        incoming = tx_sorted[tx_sorted["direction"] == "incoming"]
        outgoing = tx_sorted[tx_sorted["direction"] == "outgoing"]

        rapid_count = 0
        for _, in_tx in incoming.iterrows():
            in_time = in_tx["timeStamp"]
            # Check if any outgoing tx within 1 hour after
            rapid_out = outgoing[
                (outgoing["timeStamp"] > in_time)
                & (outgoing["timeStamp"] < in_time + 3600)
                & (outgoing["value_eth"] > 0.1)
            ]
            if not rapid_out.empty:
                rapid_count += len(rapid_out)

        if rapid_count > 0:
            findings.append({
                "type": "rapid_movement",
                "severity": "MEDIUM" if rapid_count < 3 else "HIGH",
                "description": (
                    f"{rapid_count} instance(s) of rapid fund movement detected "
                    f"(funds received and sent within 1 hour). "
                    f"This pattern is common in layering/structuring."
                ),
                "risk_score": min(RISK_WEIGHTS["rapid_movement"] * (rapid_count / 2), 60),
            })

    # 3. Counterparty concentration analysis
    if not tx_df.empty:
        counterparties = tx_df["counterparty"].value_counts()
        total_txs = len(tx_df)
        if len(counterparties) > 0:
            top_counterparty = counterparties.index[0]
            top_count = counterparties.iloc[0]
            concentration = top_count / total_txs

            if concentration > 0.5 and total_txs > 5:
                label = KNOWN_PROTOCOLS.get(top_counterparty.lower(), top_counterparty[:10] + "...")
                findings.append({
                    "type": "concentrated_counterparty",
                    "severity": "LOW",
                    "description": (
                        f"{concentration:.0%} of transactions involve a single counterparty "
                        f"({label}). High concentration may indicate "
                        f"a business relationship or automated activity."
                    ),
                    "risk_score": RISK_WEIGHTS["concentrated_counterparty"] * concentration,
                })

    # 4. Address age check
    age_days = wallet_data.get("address_age_days")
    if age_days is not None and age_days < 30:
        findings.append({
            "type": "new_address",
            "severity": "LOW",
            "description": (
                f"Address is only {age_days} day(s) old. "
                f"New addresses with significant activity may warrant closer review."
            ),
            "risk_score": RISK_WEIGHTS["new_address"],
        })

    return findings


def classify_defi_interactions(wallet_data: dict) -> dict:
    """
    Classify which DeFi protocols the address interacts with.
    This provides context for compliance officers about the address's activity.
    """
    tx_df = wallet_data["transactions"]
    token_df = wallet_data["token_transfers"]
    protocols_seen = {}

    for df in [tx_df, token_df]:
        if df.empty:
            continue
        for _, row in df.iterrows():
            counterparty = row.get("counterparty", "").lower()
            if counterparty in KNOWN_PROTOCOLS:
                protocol = KNOWN_PROTOCOLS[counterparty]
                if protocol not in protocols_seen:
                    protocols_seen[protocol] = {"count": 0, "first_seen": None, "last_seen": None}
                protocols_seen[protocol]["count"] += 1
                ts = row.get("datetime")
                if ts is not None and pd.notna(ts):
                    if protocols_seen[protocol]["first_seen"] is None or ts < protocols_seen[protocol]["first_seen"]:
                        protocols_seen[protocol]["first_seen"] = ts
                    if protocols_seen[protocol]["last_seen"] is None or ts > protocols_seen[protocol]["last_seen"]:
                        protocols_seen[protocol]["last_seen"] = ts

    # Serialize timestamps
    for proto in protocols_seen:
        for key in ["first_seen", "last_seen"]:
            if protocols_seen[proto][key] is not None:
                protocols_seen[proto][key] = protocols_seen[proto][key].isoformat()

    return protocols_seen


def compute_risk_score(sanctions_findings: list, pattern_findings: list, defi_interactions: dict) -> dict:
    """
    Compute overall risk score from all findings.

    Score ranges:
    0-20: LOW risk - Normal activity, no flags
    21-40: MEDIUM-LOW - Minor flags, routine monitoring
    41-60: MEDIUM - Notable patterns, enhanced due diligence recommended
    61-80: HIGH - Significant risk indicators, investigation recommended
    81-100: CRITICAL - Sanctions hits or severe risk patterns, block recommended
    """
    total_score = 0
    max_severity = "LOW"

    # Sanctions findings are weighted heaviest
    for finding in sanctions_findings:
        total_score += finding["risk_score"]
        if finding["severity"] == "CRITICAL":
            max_severity = "CRITICAL"

    # Pattern findings
    for finding in pattern_findings:
        total_score += finding["risk_score"]
        if finding["severity"] == "HIGH" and max_severity not in ["CRITICAL"]:
            max_severity = "HIGH"
        elif finding["severity"] == "MEDIUM" and max_severity in ["LOW"]:
            max_severity = "MEDIUM"

    # DeFi protocol interactions reduce risk (known legitimate activity)
    known_protocol_count = sum(1 for _ in defi_interactions)
    if known_protocol_count > 0:
        reduction = min(known_protocol_count * abs(RISK_WEIGHTS["known_protocol"]), 30)
        total_score = max(0, total_score - reduction)

    # Cap at 100
    total_score = min(total_score, 100)

    # Determine risk level
    if total_score <= 20:
        risk_level = "LOW"
        color = "#22c55e"
        recommendation = "Normal activity. Standard monitoring sufficient."
    elif total_score <= 40:
        risk_level = "MEDIUM-LOW"
        color = "#84cc16"
        recommendation = "Minor flags detected. Routine enhanced monitoring recommended."
    elif total_score <= 60:
        risk_level = "MEDIUM"
        color = "#eab308"
        recommendation = "Notable risk patterns. Enhanced due diligence recommended."
    elif total_score <= 80:
        risk_level = "HIGH"
        color = "#f97316"
        recommendation = "Significant risk indicators. Manual investigation recommended."
    else:
        risk_level = "CRITICAL"
        color = "#ef4444"
        recommendation = "Critical risk. Sanctions exposure detected. Immediate review and potential blocking recommended."

    return {
        "score": round(total_score, 1),
        "level": risk_level,
        "color": color,
        "recommendation": recommendation,
        "max_severity": max_severity,
        "finding_count": len(sanctions_findings) + len(pattern_findings),
    }


def generate_statistics(wallet_data: dict) -> dict:
    """Generate summary statistics for the wallet."""
    tx_df = wallet_data["transactions"]
    token_df = wallet_data["token_transfers"]

    stats = {
        "eth_balance": wallet_data["balance"]["balance_eth"],
        "total_transactions": wallet_data["transaction_count"],
        "total_token_transfers": wallet_data["token_transfer_count"],
        "address_age_days": wallet_data["address_age_days"],
        "unique_counterparties": 0,
        "total_eth_sent": 0,
        "total_eth_received": 0,
        "total_gas_spent_eth": 0,
        "top_tokens": [],
        "activity_period": {"first": None, "last": None},
    }

    if not tx_df.empty:
        stats["unique_counterparties"] = tx_df["counterparty"].nunique()
        outgoing = tx_df[tx_df["direction"] == "outgoing"]
        incoming = tx_df[tx_df["direction"] == "incoming"]
        stats["total_eth_sent"] = round(outgoing["value_eth"].sum(), 4)
        stats["total_eth_received"] = round(incoming["value_eth"].sum(), 4)
        stats["total_gas_spent_eth"] = round(tx_df["gas_cost_eth"].sum(), 6)
        stats["activity_period"]["first"] = tx_df["datetime"].min().isoformat()
        stats["activity_period"]["last"] = tx_df["datetime"].max().isoformat()

    if not token_df.empty:
        token_summary = (
            token_df.groupby("tokenSymbol")["token_amount"]
            .agg(["sum", "count"])
            .sort_values("count", ascending=False)
            .head(5)
        )
        stats["top_tokens"] = [
            {"symbol": sym, "transfer_count": int(row["count"]), "total_volume": round(row["sum"], 2)}
            for sym, row in token_summary.iterrows()
        ]

    return stats


def run_risk_analysis(wallet_data: dict) -> dict:
    """
    Main risk analysis pipeline. Orchestrates all analysis layers
    and produces a complete compliance risk report.
    """
    # Layer 1: Sanctions screening
    sanctions_findings = screen_sanctions(wallet_data)

    # Layer 2: Pattern analysis
    pattern_findings = analyze_transaction_patterns(wallet_data)

    # Layer 3: DeFi classification
    defi_interactions = classify_defi_interactions(wallet_data)

    # Layer 4: Risk scoring
    risk_score = compute_risk_score(sanctions_findings, pattern_findings, defi_interactions)

    # Statistics
    statistics = generate_statistics(wallet_data)

    return {
        "address": wallet_data["address"],
        "risk_score": risk_score,
        "sanctions_findings": sanctions_findings,
        "pattern_findings": pattern_findings,
        "defi_interactions": defi_interactions,
        "statistics": statistics,
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
        "regulations_applicable": [
            {
                "name": "MiCA (EU)",
                "deadline": "2026-07-01",
                "relevance": "Requires transaction monitoring and sanctions screening for all CASPs",
            },
            {
                "name": "GENIUS Act (US)",
                "deadline": "2027-01-18",
                "relevance": "Stablecoin issuers must implement AML/KYC and transaction monitoring",
            },
            {
                "name": "FATF Travel Rule",
                "deadline": "2026 Q3 enforcement",
                "relevance": "VASPs must share originator/beneficiary info for transfers >$1,000",
            },
        ],
    }
