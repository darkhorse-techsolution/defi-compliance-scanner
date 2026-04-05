/**
 * DeFi Compliance Scanner -- Frontend Application
 * Handles user interaction, API calls, and result rendering.
 */

// ---- State ----
let currentResults = null;

// ---- Initialization ----
document.addEventListener("DOMContentLoaded", () => {
    loadDemoAddresses();

    // Allow Enter key to trigger scan
    document.getElementById("addressInput").addEventListener("keydown", (e) => {
        if (e.key === "Enter") scanAddress();
    });
});

async function loadDemoAddresses() {
    try {
        const resp = await fetch("/api/demo-addresses");
        const data = await resp.json();
        const container = document.getElementById("demoButtons");

        data.addresses.forEach((addr) => {
            const btn = document.createElement("button");
            btn.className = "demo-btn";

            const riskClass = addr.expected_risk.toLowerCase().includes("critical")
                ? "demo-risk-critical"
                : addr.expected_risk.toLowerCase().includes("medium")
                ? "demo-risk-medium"
                : "demo-risk-low";

            btn.innerHTML = `${addr.label}<span class="demo-risk ${riskClass}">${addr.expected_risk}</span>`;
            btn.title = addr.description;
            btn.onclick = () => {
                document.getElementById("addressInput").value = addr.address;
                scanAddress();
            };
            container.appendChild(btn);
        });
    } catch (e) {
        console.error("Failed to load demo addresses:", e);
    }
}

// ---- Main Scan Function ----
async function scanAddress() {
    const input = document.getElementById("addressInput");
    const address = input.value.trim();

    if (!address) {
        showError("Please enter an Ethereum address.");
        return;
    }

    if (!/^0x[0-9a-fA-F]{40}$/.test(address)) {
        showError("Invalid Ethereum address format. Must be 0x followed by 40 hex characters.");
        return;
    }

    // UI: show loading state
    setLoading(true);
    hideError();
    document.getElementById("resultsSection").style.display = "none";

    try {
        const resp = await fetch("/api/scan", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ address }),
        });

        if (!resp.ok) {
            const err = await resp.json();
            throw new Error(err.detail || "Scan failed");
        }

        const results = await resp.json();
        currentResults = results;
        renderResults(results);
    } catch (e) {
        showError(`Scan failed: ${e.message}`);
    } finally {
        setLoading(false);
    }
}

// ---- Rendering ----
function renderResults(data) {
    const section = document.getElementById("resultsSection");
    section.style.display = "block";

    // Scroll to results
    setTimeout(() => {
        section.scrollIntoView({ behavior: "smooth", block: "start" });
    }, 100);

    renderRiskScore(data.risk_score);
    renderDataSource(data.data_source);
    renderStatistics(data.statistics);
    renderFindings(data.sanctions_findings, data.pattern_findings, data.defi_interactions);
    renderRegulations(data.regulations_applicable);
    renderNarrative(data.narrative);

    // Analyzed timestamp
    const analyzed = new Date(data.analyzed_at);
    document.getElementById("analyzedAt").textContent =
        `Analyzed: ${analyzed.toLocaleString()}`;
}

function renderRiskScore(risk) {
    const scoreNum = document.getElementById("riskScoreNumber");
    const scoreLabel = document.getElementById("riskScoreLabel");
    const meterFill = document.getElementById("riskMeterFill");
    const recommendation = document.getElementById("riskRecommendation");

    // Animate score number
    animateNumber(scoreNum, 0, risk.score, 800);

    scoreNum.style.color = risk.color;
    scoreLabel.textContent = risk.level;
    scoreLabel.style.color = risk.color;

    meterFill.style.width = `${risk.score}%`;
    meterFill.style.background = risk.color;

    recommendation.textContent = risk.recommendation;
}

function renderDataSource(source) {
    // Remove existing data source banner if any
    const existing = document.getElementById("dataSourceBanner");
    if (existing) existing.remove();

    if (source === "demo") {
        const banner = document.createElement("div");
        banner.id = "dataSourceBanner";
        banner.style.cssText =
            "background:rgba(59,130,246,0.08);border:1px solid rgba(59,130,246,0.2);" +
            "border-radius:8px;padding:12px 16px;margin-bottom:16px;font-size:0.82rem;" +
            "color:#94a3b8;display:flex;align-items:center;gap:8px;";
        banner.innerHTML =
            '<strong style="color:#3b82f6;">DEMO MODE</strong> ' +
            "Using pre-loaded sample data. Add an Etherscan API key (.env) for live blockchain data. " +
            "Balance is fetched live via public RPC.";
        const results = document.getElementById("resultsSection");
        results.insertBefore(banner, results.firstChild);
    }
}

function animateNumber(element, from, to, duration) {
    const start = performance.now();
    const update = (now) => {
        const elapsed = now - start;
        const progress = Math.min(elapsed / duration, 1);
        const eased = 1 - Math.pow(1 - progress, 3); // easeOutCubic
        const current = Math.round(from + (to - from) * eased);
        element.textContent = current;
        if (progress < 1) requestAnimationFrame(update);
    };
    requestAnimationFrame(update);
}

function renderStatistics(stats) {
    const grid = document.getElementById("statsGrid");
    grid.innerHTML = "";

    const items = [
        {
            label: "ETH Balance",
            value: `${stats.eth_balance.toFixed(4)}`,
            sub: "ETH",
        },
        {
            label: "Transactions",
            value: stats.total_transactions.toLocaleString(),
            sub: `${stats.total_token_transfers} token transfers`,
        },
        {
            label: "Counterparties",
            value: stats.unique_counterparties.toLocaleString(),
            sub: "unique addresses",
        },
        {
            label: "Address Age",
            value: stats.address_age_days !== null ? stats.address_age_days.toLocaleString() : "N/A",
            sub: "days",
        },
        {
            label: "ETH Sent",
            value: formatNumber(stats.total_eth_sent),
            sub: "ETH total outflow",
        },
        {
            label: "ETH Received",
            value: formatNumber(stats.total_eth_received),
            sub: "ETH total inflow",
        },
        {
            label: "Gas Spent",
            value: stats.total_gas_spent_eth.toFixed(4),
            sub: "ETH in gas fees",
        },
        {
            label: "Top Token",
            value: stats.top_tokens.length > 0 ? stats.top_tokens[0].symbol : "N/A",
            sub:
                stats.top_tokens.length > 0
                    ? `${stats.top_tokens[0].transfer_count} transfers`
                    : "no token activity",
        },
    ];

    items.forEach((item) => {
        const card = document.createElement("div");
        card.className = "stat-card";
        card.innerHTML = `
            <div class="stat-label">${item.label}</div>
            <div class="stat-value">${item.value}</div>
            <div class="stat-sub">${item.sub}</div>
        `;
        grid.appendChild(card);
    });
}

function renderFindings(sanctions, patterns, defi) {
    // Sanctions
    const sanctionsList = document.getElementById("sanctionsList");
    sanctionsList.innerHTML = "";
    if (sanctions.length === 0) {
        sanctionsList.innerHTML = '<p class="finding-none">No sanctions exposure detected. Address is not on the OFAC SDN list and no counterparties match sanctioned entities.</p>';
    } else {
        sanctions.forEach((f) => {
            sanctionsList.appendChild(createFindingItem(f));
        });
    }

    // Patterns
    const patternsList = document.getElementById("patternsList");
    patternsList.innerHTML = "";
    if (patterns.length === 0) {
        patternsList.innerHTML = '<p class="finding-none">No suspicious transaction patterns detected in the analyzed dataset.</p>';
    } else {
        patterns.forEach((f) => {
            patternsList.appendChild(createFindingItem(f));
        });
    }

    // DeFi
    const defiList = document.getElementById("defiList");
    defiList.innerHTML = "";
    const protocols = Object.entries(defi);
    if (protocols.length === 0) {
        defiList.innerHTML = '<p class="finding-none">No interactions with known DeFi protocols detected in the analyzed transactions.</p>';
    } else {
        protocols.forEach(([name, info]) => {
            const item = document.createElement("div");
            item.className = "finding-item";
            item.innerHTML = `
                <span class="finding-severity severity-low">DEFI</span>
                <span class="finding-text"><strong>${name}</strong> -- ${info.count} interaction(s)</span>
            `;
            defiList.appendChild(item);
        });
    }
}

function createFindingItem(finding) {
    const item = document.createElement("div");
    item.className = "finding-item";
    const severityClass = `severity-${finding.severity.toLowerCase()}`;
    item.innerHTML = `
        <span class="finding-severity ${severityClass}">${finding.severity}</span>
        <span class="finding-text">${finding.description}</span>
    `;
    return item;
}

function renderRegulations(regs) {
    const grid = document.getElementById("regulationsGrid");
    grid.innerHTML = "";

    regs.forEach((reg) => {
        const card = document.createElement("div");
        card.className = "regulation-card";
        card.innerHTML = `
            <div class="regulation-name">${reg.name}</div>
            <div class="regulation-deadline">Deadline: ${reg.deadline}</div>
            <div class="regulation-relevance">${reg.relevance}</div>
        `;
        grid.appendChild(card);
    });
}

function renderNarrative(markdown) {
    const container = document.getElementById("narrativeContent");
    container.innerHTML = markdownToHtml(markdown);
}

// ---- Simple Markdown to HTML ----
function markdownToHtml(md) {
    if (!md) return "<p>No narrative generated.</p>";

    let html = md
        // Escape HTML entities first
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        // Headers
        .replace(/^### (.+)$/gm, "<h3>$1</h3>")
        .replace(/^## (.+)$/gm, "<h2>$1</h2>")
        .replace(/^# (.+)$/gm, "<h2>$1</h2>")
        // Bold and italic
        .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
        .replace(/\*(.+?)\*/g, "<em>$1</em>")
        // Inline code
        .replace(/`([^`]+)`/g, "<code>$1</code>")
        // Horizontal rules
        .replace(/^---+$/gm, "<hr>")
        // Unordered lists
        .replace(/^- (.+)$/gm, "<li>$1</li>")
        // Ordered lists
        .replace(/^\d+\. (.+)$/gm, "<li>$1</li>")
        // Paragraphs
        .replace(/\n\n/g, "</p><p>")
        // Line breaks
        .replace(/\n/g, "<br>");

    // Wrap consecutive <li> in <ul>
    html = html.replace(/(<li>.*?<\/li>(?:<br>)?)+/g, (match) => {
        const cleaned = match.replace(/<br>/g, "");
        return `<ul>${cleaned}</ul>`;
    });

    // Handle tables
    html = html.replace(
        /\|(.+)\|(?:<br>)\|[-| ]+\|(?:<br>)((?:\|.+\|(?:<br>)?)+)/g,
        (match, header, body) => {
            const headers = header.split("|").filter(Boolean).map((h) => `<th>${h.trim()}</th>`).join("");
            const rows = body
                .split("<br>")
                .filter(Boolean)
                .map((row) => {
                    const cells = row.split("|").filter(Boolean).map((c) => `<td>${c.trim()}</td>`).join("");
                    return `<tr>${cells}</tr>`;
                })
                .join("");
            return `<table><thead><tr>${headers}</tr></thead><tbody>${rows}</tbody></table>`;
        }
    );

    return `<p>${html}</p>`;
}

// ---- Utilities ----
function setLoading(loading) {
    const btn = document.getElementById("scanButton");
    const text = btn.querySelector(".button-text");
    const spinner = btn.querySelector(".button-loading");

    btn.disabled = loading;
    text.style.display = loading ? "none" : "inline";
    spinner.style.display = loading ? "inline" : "none";
}

function showError(msg) {
    const banner = document.getElementById("errorBanner");
    const message = document.getElementById("errorMessage");
    message.textContent = msg;
    banner.style.display = "block";
}

function hideError() {
    document.getElementById("errorBanner").style.display = "none";
}

function formatNumber(num) {
    if (num >= 1000000) return (num / 1000000).toFixed(2) + "M";
    if (num >= 1000) return (num / 1000).toFixed(2) + "K";
    return num.toFixed(4);
}

function copyNarrative() {
    if (!currentResults || !currentResults.narrative) return;
    navigator.clipboard.writeText(currentResults.narrative).then(() => {
        const btn = document.querySelector(".copy-button");
        btn.textContent = "Copied!";
        setTimeout(() => {
            btn.textContent = "Copy Report";
        }, 2000);
    });
}
