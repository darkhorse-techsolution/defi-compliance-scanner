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

from datetime import datetime, timezone

import pandas as pd

from app.config import KNOWN_PROTOCOLS, RISK_WEIGHTS
from app.sanctions import get_cached_sanctions_sync


def screen_sanctions(wallet_data: dict) -> list[dict]:
    """
    Screen all counterparties against the OFAC sanctions list. This is the
    single most important compliance check for any crypto firm. The function
    is defensive about real-world data: missing fields, NaT timestamps and
    empty frames are all handled.

    The sanctions set is read from the live cache maintained by
    ``app.sanctions``. Main.py warms that cache on startup and at the
    top of every /api/scan call so by the time we get here the lookup
    is a plain in-memory set check.
    """
    findings = []
    address = wallet_data.get("address", "")
    sanctioned = get_cached_sanctions_sync()

    # Check if the address itself is sanctioned
    if address and address.lower() in sanctioned:
        findings.append({
            "type": "sanctioned_address",
            "severity": "CRITICAL",
            "description": f"Address {address[:10]}... is directly on the OFAC sanctions list",
            "risk_score": 100,
        })

    def _format_when(value) -> str:
        if value is None or pd.isna(value):
            return "unknown date"
        try:
            return value.strftime("%Y-%m-%d")
        except AttributeError:
            return "unknown date"

    # Screen native ETH transaction counterparties
    tx_df = wallet_data.get("transactions")
    if isinstance(tx_df, pd.DataFrame) and not tx_df.empty:
        for _, row in tx_df.iterrows():
            counterparty = str(row.get("counterparty", "") or "").lower()
            if counterparty and counterparty in sanctioned:
                value_eth = row.get("value_eth", 0) or 0
                findings.append({
                    "type": "sanctioned_interaction",
                    "severity": "CRITICAL",
                    "description": (
                        f"{'Sent to' if row.get('direction') == 'outgoing' else 'Received from'} "
                        f"OFAC-sanctioned address {counterparty[:10]}... "
                        f"({float(value_eth):.4f} ETH on {_format_when(row.get('datetime'))})"
                    ),
                    "risk_score": RISK_WEIGHTS["sanctioned_interaction"],
                    "tx_hash": row.get("hash", ""),
                    "timestamp": row["datetime"].isoformat()
                        if pd.notna(row.get("datetime")) else None,
                })

    # Screen token transfer counterparties
    token_df = wallet_data.get("token_transfers")
    if isinstance(token_df, pd.DataFrame) and not token_df.empty:
        for _, row in token_df.iterrows():
            counterparty = str(row.get("counterparty", "") or "").lower()
            if counterparty and counterparty in sanctioned:
                findings.append({
                    "type": "sanctioned_interaction",
                    "severity": "CRITICAL",
                    "description": (
                        f"Token transfer {'to' if row.get('direction') == 'outgoing' else 'from'} "
                        f"OFAC-sanctioned address: {row.get('tokenSymbol', 'Unknown')} "
                        f"on {_format_when(row.get('datetime'))}"
                    ),
                    "risk_score": RISK_WEIGHTS["sanctioned_interaction"],
                    "tx_hash": row.get("hash", ""),
                    "timestamp": row["datetime"].isoformat()
                        if pd.notna(row.get("datetime")) else None,
                })

    return findings


def analyze_transaction_patterns(wallet_data: dict) -> list[dict]:
    """
    Analyze transaction patterns for suspicious behavior. This is where
    data engineering meets compliance analytics.
    """
    findings = []
    tx_df = wallet_data.get("transactions")

    if not isinstance(tx_df, pd.DataFrame) or tx_df.empty:
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
    #
    # Concentration is benign when the top counterparty is a known DeFi
    # protocol (Uniswap router, Aave pool, etc.) - that just means the
    # wallet is an active user of one platform. It is a laundering red
    # flag when the counterparty is unknown and concentration is very high.
    if not tx_df.empty:
        counterparties = tx_df["counterparty"].value_counts()
        total_txs = len(tx_df)
        if len(counterparties) > 0:
            top_counterparty = counterparties.index[0]
            top_count = counterparties.iloc[0]
            concentration = top_count / total_txs
            top_lower = top_counterparty.lower() if top_counterparty else ""
            is_known_protocol = top_lower in KNOWN_PROTOCOLS

            if concentration > 0.5 and total_txs > 5:
                label = KNOWN_PROTOCOLS.get(top_lower, top_counterparty[:10] + "...")

                if is_known_protocol:
                    severity = "LOW"
                    description = (
                        f"{concentration:.0%} of transactions involve {label}. "
                        f"Activity is concentrated but anchored in a recognized protocol."
                    )
                    score_multiplier = 0.5
                elif concentration > 0.8:
                    severity = "HIGH"
                    description = (
                        f"{concentration:.0%} of transactions involve a single unlabeled counterparty "
                        f"({label}). Extreme concentration with an unknown party is a layering red flag."
                    )
                    score_multiplier = 2.0
                else:
                    severity = "MEDIUM"
                    description = (
                        f"{concentration:.0%} of transactions involve a single unlabeled counterparty "
                        f"({label}). High concentration with an unknown party warrants review."
                    )
                    score_multiplier = 1.25

                findings.append({
                    "type": "concentrated_counterparty",
                    "severity": severity,
                    "description": description,
                    "risk_score": RISK_WEIGHTS["concentrated_counterparty"] * concentration * score_multiplier,
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
    Classify which DeFi protocols the address interacts with. Useful
    context for compliance officers - it shows that activity is anchored
    in known, legitimate venues rather than obscure smart contracts.
    """
    tx_df = wallet_data.get("transactions")
    token_df = wallet_data.get("token_transfers")
    protocols_seen = {}

    for df in (tx_df, token_df):
        if not isinstance(df, pd.DataFrame) or df.empty:
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
    """Generate summary statistics for the wallet, plus a daily activity series."""
    tx_df = wallet_data.get("transactions")
    token_df = wallet_data.get("token_transfers")
    balance = wallet_data.get("balance") or {}

    stats = {
        "eth_balance": float(balance.get("balance_eth", 0) or 0),
        "total_transactions": int(wallet_data.get("transaction_count", 0) or 0),
        "total_token_transfers": int(wallet_data.get("token_transfer_count", 0) or 0),
        "address_age_days": wallet_data.get("address_age_days"),
        "unique_counterparties": 0,
        "total_eth_sent": 0.0,
        "total_eth_received": 0.0,
        "total_gas_spent_eth": 0.0,
        "top_tokens": [],
        "activity_period": {"first": None, "last": None},
        "timeline": [],
    }

    if isinstance(tx_df, pd.DataFrame) and not tx_df.empty:
        stats["unique_counterparties"] = int(tx_df["counterparty"].nunique())
        outgoing = tx_df[tx_df["direction"] == "outgoing"]
        incoming = tx_df[tx_df["direction"] == "incoming"]
        stats["total_eth_sent"] = round(float(outgoing["value_eth"].sum()), 4)
        stats["total_eth_received"] = round(float(incoming["value_eth"].sum()), 4)
        stats["total_gas_spent_eth"] = round(float(tx_df["gas_cost_eth"].sum()), 6)
        stats["activity_period"]["first"] = tx_df["datetime"].min().isoformat()
        stats["activity_period"]["last"] = tx_df["datetime"].max().isoformat()

        # Daily transaction count timeline (used by the frontend chart)
        daily = (
            tx_df.assign(day=tx_df["datetime"].dt.strftime("%Y-%m-%d"))
            .groupby("day")
            .size()
            .reset_index(name="count")
            .sort_values("day")
            .tail(60)
        )
        stats["timeline"] = [
            {"date": row["day"], "count": int(row["count"])}
            for _, row in daily.iterrows()
        ]

    if isinstance(token_df, pd.DataFrame) and not token_df.empty:
        token_summary = (
            token_df.groupby("tokenSymbol")["token_amount"]
            .agg(["sum", "count"])
            .sort_values("count", ascending=False)
            .head(5)
        )
        stats["top_tokens"] = [
            {
                "symbol": sym,
                "transfer_count": int(row["count"]),
                "total_volume": round(float(row["sum"]), 2),
            }
            for sym, row in token_summary.iterrows()
        ]

    return stats


def run_risk_analysis(wallet_data: dict) -> dict:
    """
    Main risk analysis pipeline. Orchestrates all analysis layers and
    produces a complete compliance risk report. The function never raises
    on bad data - if everything is empty, the report just shows zero
    findings and a LOW score, with the upstream errors surfaced in the
    "fetch_errors" field for the frontend to display.
    """
    sanctions_findings = screen_sanctions(wallet_data)
    pattern_findings = analyze_transaction_patterns(wallet_data)
    defi_interactions = classify_defi_interactions(wallet_data)
    risk_score = compute_risk_score(sanctions_findings, pattern_findings, defi_interactions)
    statistics = generate_statistics(wallet_data)

    # If no data came back at all and no findings, mark the report as
    # informational so the frontend can show an empty-state message.
    has_any_data = (
        statistics["total_transactions"] > 0
        or statistics["total_token_transfers"] > 0
        or statistics["eth_balance"] > 0
    )

    return {
        "address": wallet_data.get("address", ""),
        "risk_score": risk_score,
        "sanctions_findings": sanctions_findings,
        "pattern_findings": pattern_findings,
        "defi_interactions": defi_interactions,
        "statistics": statistics,
        "has_data": has_any_data,
        "data_truncated": bool(wallet_data.get("data_truncated", False)),
        "page_limit": wallet_data.get("page_limit"),
        "fetch_errors": wallet_data.get("errors", []),
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
