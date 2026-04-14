/*
 * ComplianceNode Scanner - frontend application
 *
 * Vanilla JS, no build step. Drives the scan form, renders the results
 * dashboard, and toggles the dark/light theme.
 */

(function () {
    "use strict";

    // ---- DOM helpers -----------------------------------------------------
    const $ = (id) => document.getElementById(id);

    const els = {};
    let activityChart = null;
    let currentResults = null;

    // ---- Boot ------------------------------------------------------------
    document.addEventListener("DOMContentLoaded", function () {
        cacheEls();
        loadTheme();
        bindEvents();
        loadExampleAddresses();
        loadHealthStatus();
        runHashAddress();
    });

    // Allow deep-linking via #0xADDRESS so a copied report URL re-runs
    // the same scan when opened.
    function runHashAddress() {
        const hash = (window.location.hash || "").replace(/^#/, "").trim();
        if (/^0x[0-9a-fA-F]{40}$/.test(hash)) {
            els.addressInput.value = hash;
            // Defer so health/example calls don't race the first paint
            setTimeout(scanAddress, 100);
        }
    }

    function cacheEls() {
        const ids = [
            "scanForm", "addressInput", "scanButton",
            "exampleChips", "statusPills",
            "errorBanner", "errorTitle", "errorMessage",
            "loadingSkeleton", "resultsSection",
            "riskGaugeFill", "riskScoreNumber", "riskLevelPill",
            "riskFindingCount", "riskHeadline", "riskRecommendation",
            "addressDisplay", "copyAddressBtn",
            "dataSourceTag", "oracleTag", "analyzedAtTag", "copyUrlBtn",
            "statTransactions", "statTransactionsSub",
            "statVolume", "statAge", "statAgeSub", "statCounterparties",
            "activityChart", "chartEmpty", "regList",
            "findingsSub", "sanctionsList", "patternsList", "defiList",
            "narrativeContent", "copyNarrativeBtn", "narrativeSourceBadge",
            "footerPowered",
            "themeToggle", "themeToggleIcon",
        ];
        ids.forEach((id) => { els[id] = $(id); });
    }

    function bindEvents() {
        els.scanForm.addEventListener("submit", function (e) {
            e.preventDefault();
            scanAddress();
        });

        els.themeToggle.addEventListener("click", toggleTheme);

        els.copyAddressBtn.addEventListener("click", function () {
            if (!currentResults) return;
            copyText(currentResults.address, els.copyAddressBtn, "Copy");
        });

        els.copyNarrativeBtn.addEventListener("click", function () {
            if (!currentResults || !currentResults.narrative) return;
            copyText(currentResults.narrative, els.copyNarrativeBtn, "Copy report");
        });

        if (els.copyUrlBtn) {
            els.copyUrlBtn.addEventListener("click", function () {
                if (!currentResults) return;
                const url = window.location.origin + window.location.pathname +
                    "#" + currentResults.address;
                // Update the visible URL too so a refresh would re-run
                try { history.replaceState(null, "", "#" + currentResults.address); } catch (e) { /* ignore */ }
                copyText(url, els.copyUrlBtn, "Copy report URL");
            });
        }
    }

    // ---- Theme -----------------------------------------------------------
    function loadTheme() {
        let stored = null;
        try { stored = localStorage.getItem("cn-theme"); } catch (e) { /* ignore */ }
        const theme = stored === "light" ? "light" : "dark";
        applyTheme(theme);
    }

    function applyTheme(theme) {
        document.documentElement.setAttribute("data-theme", theme);
        els.themeToggleIcon.textContent = theme === "dark" ? "Dark" : "Light";
        try { localStorage.setItem("cn-theme", theme); } catch (e) { /* ignore */ }

        // If a chart exists, re-render so its colors pick up the new tokens
        if (activityChart && currentResults) {
            renderActivityChart(currentResults.statistics.timeline || []);
        }
    }

    function toggleTheme() {
        const current = document.documentElement.getAttribute("data-theme");
        applyTheme(current === "dark" ? "light" : "dark");
    }

    // ---- Example chips ---------------------------------------------------
    async function loadExampleAddresses() {
        try {
            const resp = await fetch("/api/example-addresses");
            const data = await resp.json();
            const container = els.exampleChips;
            container.innerHTML = "";

            data.addresses.forEach((addr) => {
                const chip = document.createElement("button");
                chip.type = "button";
                chip.className = "example-chip";
                chip.title = addr.description;

                const pillClass = riskPillClass(addr.expected_risk);
                chip.innerHTML =
                    `<span>${escapeHtml(addr.label)}</span>` +
                    `<span class="example-chip-pill ${pillClass}">${escapeHtml(addr.expected_risk)}</span>`;

                chip.addEventListener("click", function () {
                    els.addressInput.value = addr.address;
                    scanAddress();
                });
                container.appendChild(chip);
            });
        } catch (e) {
            console.warn("Could not load example addresses:", e);
        }
    }

    function riskPillClass(risk) {
        const r = (risk || "").toLowerCase();
        if (r.includes("critical")) return "pill-critical";
        if (r.includes("high")) return "pill-high";
        if (r.includes("medium")) return "pill-medium";
        return "pill-low";
    }

    // ---- Health / footer "powered by" + status pills --------------------
    let healthState = null;

    async function loadHealthStatus() {
        try {
            const resp = await fetch("/api/health");
            if (!resp.ok) return;
            const data = await resp.json();
            healthState = data;

            const sanctionsSize = Number(data.sanctions_list_size || 0);
            const sourceLabel = data.data_source === "etherscan_v2"
                ? "Etherscan V2"
                : "Blockscout (free tier)";

            // Footer (compact one-liner, kept for cold scrollers)
            if (els.footerPowered) {
                const parts = ["On-chain data via " + sourceLabel];
                if (sanctionsSize > 0) {
                    parts.push(sanctionsSize.toLocaleString() +
                        " addresses in the OFAC sanctions cache");
                }
                if (data.anthropic_api_key === "configured") {
                    parts.push("Claude API enabled");
                }
                els.footerPowered.textContent = "Powered by: " + parts.join(" | ");
            }

            // Hero status pills - live trust signals at first glance
            renderStatusPills(data, sanctionsSize, sourceLabel);
        } catch (e) {
            // non-fatal - just leave the default footer copy in place
        }
    }

    function renderStatusPills(data, sanctionsSize, sourceLabel) {
        if (!els.statusPills) return;
        const pills = [];

        if (sanctionsSize > 0) {
            pills.push({
                dot: "ok",
                text: sanctionsSize.toLocaleString() + " sanctioned addresses cached",
            });
        }

        pills.push({
            dot: "ok",
            text: "Source: " + sourceLabel,
        });

        if (data.chainalysis_oracle === "enabled") {
            pills.push({ dot: "ok", text: "Chainalysis oracle: enabled" });
        } else {
            pills.push({ dot: "muted", text: "Chainalysis oracle: off" });
        }

        if (data.anthropic_api_key === "configured") {
            pills.push({ dot: "ok", text: "Claude narrative: enabled" });
        } else {
            pills.push({ dot: "muted", text: "Claude narrative: rule-based fallback" });
        }

        els.statusPills.innerHTML = pills.map(function (p) {
            const dotClass = p.dot === "muted" ? "status-pill-dot dot-muted"
                : p.dot === "warn" ? "status-pill-dot dot-warn"
                : "status-pill-dot";
            return '<span class="status-pill"><span class="' + dotClass +
                '" aria-hidden="true"></span>' + escapeHtml(p.text) + "</span>";
        }).join("");
        els.statusPills.hidden = false;
    }

    function selectedDepth() {
        const checked = document.querySelector('input[name="depth"]:checked');
        return checked ? checked.value : "standard";
    }

    // ---- Scan ------------------------------------------------------------
    async function scanAddress() {
        const address = (els.addressInput.value || "").trim();
        if (!address) {
            showError("Empty address", "Please paste an Ethereum address into the box.");
            return;
        }
        if (!/^0x[0-9a-fA-F]{40}$/.test(address)) {
            showError(
                "Invalid address format",
                "An Ethereum address must start with 0x and have exactly 40 hexadecimal characters."
            );
            return;
        }

        setLoading(true);
        hideError();
        els.resultsSection.hidden = true;
        els.loadingSkeleton.hidden = false;

        try {
            const resp = await fetch("/api/scan", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ address: address, depth: selectedDepth() }),
            });

            if (!resp.ok) {
                let detail = "Scan failed.";
                try {
                    const err = await resp.json();
                    detail = err.detail || detail;
                } catch (e) { /* ignore */ }
                throw new Error(detail);
            }

            const results = await resp.json();
            currentResults = results;
            renderResults(results);
        } catch (e) {
            showError("Scan failed", e.message || String(e));
        } finally {
            setLoading(false);
            els.loadingSkeleton.hidden = true;
        }
    }

    function setLoading(isLoading) {
        els.scanButton.disabled = isLoading;
        els.scanButton.classList.toggle("is-loading", isLoading);
    }

    // ---- Render results --------------------------------------------------
    function renderResults(data) {
        els.resultsSection.hidden = false;

        renderRisk(data);
        renderStats(data.statistics, data);
        renderActivityChart(data.statistics.timeline || []);
        renderRegulations(data.regulations_applicable || []);
        renderFindings(
            data.sanctions_findings || [],
            data.pattern_findings || [],
            data.defi_interactions || {}
        );
        renderNarrative(data.narrative, data.narrative_source);

        // Empty-state hint when no upstream data was returned
        if (data.has_data === false) {
            const errs = (data.fetch_errors || []).join(" / ");
            const msg = errs
                ? `No on-chain activity returned. Upstream errors: ${errs}`
                : "No transactions were found for this address. The wallet may be brand new or unused.";
            showError("Limited data", msg);
        }

        setTimeout(function () {
            els.resultsSection.scrollIntoView({ behavior: "smooth", block: "start" });
        }, 80);
    }

    function renderRisk(data) {
        const risk = data.risk_score;
        const score = clamp(Number(risk.score) || 0, 0, 100);

        // Animated number
        animateNumber(els.riskScoreNumber, 0, score, 900);

        // Gauge color + fill
        const color = colorForLevel(risk.level, score);
        const circumference = 2 * Math.PI * 84; // r=84
        const offset = circumference * (1 - score / 100);
        // Defer the dashoffset so the transition runs after layout settles
        requestAnimationFrame(function () {
            els.riskGaugeFill.style.stroke = color;
            els.riskGaugeFill.style.strokeDashoffset = String(offset);
        });

        els.riskLevelPill.textContent = risk.level;
        els.riskLevelPill.setAttribute("data-level", risk.level);

        els.riskFindingCount.textContent =
            risk.finding_count + (risk.finding_count === 1 ? " finding" : " findings");

        els.riskHeadline.textContent = headlineForLevel(risk.level);
        els.riskRecommendation.textContent = risk.recommendation || "";

        els.addressDisplay.textContent = truncateAddress(data.address);

        const oracleEnabled = healthState && healthState.chainalysis_oracle === "enabled";
        const sourceParts = [];
        sourceParts.push(data.data_source === "etherscan_v2"
            ? "Etherscan V2"
            : "Blockscout (free)");
        if (oracleEnabled) {
            sourceParts.push("Chainalysis Oracle");
        }
        els.dataSourceTag.textContent = "via " + sourceParts.join(" + ");

        if (els.oracleTag) {
            const wasChecked = oracleEnabled;
            const hadHits = data.oracle_hits && Object.keys(data.oracle_hits).length > 0;
            if (wasChecked) {
                els.oracleTag.hidden = false;
                els.oracleTag.textContent = hadHits
                    ? "Chainalysis oracle: HIT"
                    : "Chainalysis oracle: clean";
            } else {
                els.oracleTag.hidden = true;
            }
        }

        const analyzed = new Date(data.analyzed_at);
        els.analyzedAtTag.textContent = "Analyzed " + analyzed.toLocaleString();
    }

    function colorForLevel(level, score) {
        if (level === "CRITICAL" || score >= 91) return "#ef4444";
        if (level === "HIGH" || score >= 61) return "#f97316";
        if (score >= 31) return "#f59e0b";
        return "#10b981";
    }

    function headlineForLevel(level) {
        if (level === "CRITICAL") return "Critical compliance risk";
        if (level === "HIGH") return "Elevated compliance risk";
        if (level === "MEDIUM") return "Moderate compliance risk";
        if (level === "MEDIUM-LOW") return "Minor compliance flags";
        return "Low compliance risk";
    }

    function renderStats(stats, fullData) {
        const completeness = (fullData && fullData.data_completeness) || "full";
        const maxResults = (fullData && fullData.max_results) || 1000;
        const isSample = completeness !== "full";

        els.statTransactions.textContent = formatInt(stats.total_transactions);
        els.statTransactionsSub.textContent =
            (stats.total_token_transfers || 0).toLocaleString() + " token transfers" +
            (isSample ? " (" + completeness + ")" : "");

        const totalVolume = (stats.total_eth_sent || 0) + (stats.total_eth_received || 0);
        els.statVolume.textContent = formatEth(totalVolume);

        const ageDays = stats.address_age_days;
        if (ageDays === null || ageDays === undefined) {
            els.statAge.textContent = "N/A";
            els.statAgeSub.textContent = "no transaction history";
        } else if (ageDays >= 365) {
            const years = (ageDays / 365);
            els.statAge.textContent = years.toFixed(1) + " yr";
            els.statAgeSub.textContent = formatInt(ageDays) + " days since first tx";
        } else {
            els.statAge.textContent = formatInt(ageDays);
            els.statAgeSub.textContent = ageDays === 1 ? "day old" : "days old";
        }

        els.statCounterparties.textContent = formatInt(stats.unique_counterparties);

        // Show a data-completeness disclaimer when we know the sample is
        // not exhaustive. The message changes depending on whether we hit
        // the quick-scan cap or the 10k upstream ceiling.
        let disclaimer = document.getElementById("dataSampleDisclaimer");
        if (isSample) {
            if (!disclaimer) {
                disclaimer = document.createElement("div");
                disclaimer.id = "dataSampleDisclaimer";
                disclaimer.className = "data-disclaimer";
                els.resultsSection.insertBefore(disclaimer, els.resultsSection.firstChild);
            }
            if (completeness === "sample") {
                disclaimer.innerHTML =
                    "<strong>Quick scan:</strong> Analysis based on the most recent " +
                    maxResults.toLocaleString() +
                    " transactions. Pick <em>Standard</em> or <em>Deep</em> above for a " +
                    "fuller view - address age is still computed from a separate " +
                    "earliest-transaction lookup when possible.";
            } else {
                disclaimer.innerHTML =
                    "<strong>Partial history:</strong> This wallet has more than " +
                    maxResults.toLocaleString() +
                    " transactions. The analysis covers the most recent activity " +
                    "using block-range pagination. Re-run in <em>Deep</em> mode for " +
                    "up to 5,000 results.";
            }
        } else if (disclaimer) {
            disclaimer.remove();
        }
    }

    function renderActivityChart(timeline) {
        const ctx = els.activityChart.getContext("2d");

        if (!timeline || timeline.length === 0) {
            els.chartEmpty.hidden = false;
            els.activityChart.style.display = "none";
            if (activityChart) { activityChart.destroy(); activityChart = null; }
            return;
        }
        els.chartEmpty.hidden = true;
        els.activityChart.style.display = "block";

        const labels = timeline.map(function (p) { return p.date; });
        const counts = timeline.map(function (p) { return p.count; });

        const cs = getComputedStyle(document.documentElement);
        const primary = cs.getPropertyValue("--primary").trim() || "#2563eb";
        const text = cs.getPropertyValue("--text-muted").trim() || "#94a3b8";
        const grid = cs.getPropertyValue("--border").trim() || "rgba(255,255,255,0.08)";

        if (activityChart) { activityChart.destroy(); }

        if (typeof Chart === "undefined") {
            // Chart.js still loading - try again shortly
            setTimeout(function () { renderActivityChart(timeline); }, 200);
            return;
        }

        activityChart = new Chart(ctx, {
            type: "bar",
            data: {
                labels: labels,
                datasets: [{
                    label: "Transactions",
                    data: counts,
                    backgroundColor: primary,
                    borderRadius: 4,
                    barThickness: "flex",
                    maxBarThickness: 14,
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        backgroundColor: "rgba(15, 22, 38, 0.95)",
                        titleColor: "#f8fafc",
                        bodyColor: "#cbd5e1",
                        padding: 10,
                        cornerRadius: 6,
                    },
                },
                scales: {
                    x: {
                        ticks: { color: text, font: { size: 10 }, maxRotation: 0, autoSkip: true, maxTicksLimit: 8 },
                        grid: { display: false },
                    },
                    y: {
                        ticks: { color: text, font: { size: 10 }, precision: 0 },
                        grid: { color: grid },
                        beginAtZero: true,
                    },
                },
            },
        });
    }

    function renderRegulations(regs) {
        els.regList.innerHTML = "";
        if (!regs.length) {
            const li = document.createElement("li");
            li.className = "finding-empty";
            li.textContent = "No regulatory frameworks listed.";
            els.regList.appendChild(li);
            return;
        }
        regs.forEach(function (reg) {
            const li = document.createElement("li");
            li.className = "reg-list-item";
            li.innerHTML =
                `<span class="reg-list-name">${escapeHtml(reg.name)}</span>` +
                `<span class="reg-list-deadline">Deadline: ${escapeHtml(reg.deadline)}</span>` +
                `<span class="reg-list-relevance">${escapeHtml(reg.relevance)}</span>`;
            els.regList.appendChild(li);
        });
    }

    function renderFindings(sanctions, patterns, defi) {
        const totalFindings =
            sanctions.length + patterns.length + Object.keys(defi).length;
        els.findingsSub.textContent =
            totalFindings + (totalFindings === 1 ? " item" : " items");

        renderFindingList(els.sanctionsList, sanctions, "info",
            "No sanctions exposure detected. Address is not on the OFAC SDN list and no counterparties match sanctioned entities.");

        renderFindingList(els.patternsList, patterns, "info",
            "No suspicious transaction patterns detected in the analyzed dataset.");

        // DeFi interactions are rendered as info-level items
        const defiArray = Object.entries(defi).map(function (entry) {
            const name = entry[0];
            const info = entry[1];
            return {
                severity: "INFO",
                title: name,
                description: info.count + " interaction" + (info.count === 1 ? "" : "s"),
            };
        });
        renderFindingList(els.defiList, defiArray, "info",
            "No interactions with known DeFi protocols detected in the analyzed transactions.");
    }

    function renderFindingList(container, findings, defaultSeverity, emptyMsg) {
        container.innerHTML = "";
        if (!findings.length) {
            const p = document.createElement("p");
            p.className = "finding-empty";
            p.textContent = emptyMsg;
            container.appendChild(p);
            return;
        }
        findings.forEach(function (f) {
            const sev = (f.severity || defaultSeverity).toLowerCase();
            const item = document.createElement("div");
            item.className = "finding-item severity-" + sev;
            const titleText = f.title || prettifyType(f.type) || "Finding";
            item.innerHTML =
                `<span class="finding-severity severity-badge-${sev}">${escapeHtml(f.severity || defaultSeverity)}</span>` +
                `<div class="finding-body">` +
                    `<div class="finding-title">${escapeHtml(titleText)}</div>` +
                    `<div class="finding-desc">${escapeHtml(f.description || "")}</div>` +
                `</div>`;
            container.appendChild(item);
        });
    }

    function prettifyType(type) {
        if (!type) return "";
        return type.replace(/_/g, " ").replace(/\b\w/g, function (c) { return c.toUpperCase(); });
    }

    function renderNarrative(markdown, source) {
        els.narrativeContent.innerHTML = markdownToHtml(markdown);
        if (els.narrativeSourceBadge) {
            if (source === "claude") {
                els.narrativeSourceBadge.hidden = false;
                els.narrativeSourceBadge.className =
                    "narrative-source-badge source-claude";
                els.narrativeSourceBadge.textContent = "Claude-generated";
                els.narrativeSourceBadge.title =
                    "Narrative generated by the Claude API.";
            } else if (source === "rule_based") {
                els.narrativeSourceBadge.hidden = false;
                els.narrativeSourceBadge.className =
                    "narrative-source-badge source-rule";
                els.narrativeSourceBadge.textContent = "Rule-based";
                els.narrativeSourceBadge.title =
                    "Rule-based template (set ANTHROPIC_API_KEY to enable Claude).";
            } else {
                els.narrativeSourceBadge.hidden = true;
            }
        }
    }

    // ---- Markdown (lightweight) ------------------------------------------
    function markdownToHtml(md) {
        if (!md) return "<p>No narrative generated.</p>";

        let html = md
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/^### (.+)$/gm, "<h3>$1</h3>")
            .replace(/^## (.+)$/gm, "<h2>$1</h2>")
            .replace(/^# (.+)$/gm, "<h2>$1</h2>")
            .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
            .replace(/\*(.+?)\*/g, "<em>$1</em>")
            .replace(/`([^`]+)`/g, "<code>$1</code>")
            .replace(/^---+$/gm, "<hr>")
            .replace(/^- (.+)$/gm, "<li>$1</li>")
            .replace(/^\d+\. (.+)$/gm, "<li>$1</li>")
            .replace(/\n\n/g, "</p><p>")
            .replace(/\n/g, "<br>");

        html = html.replace(/(<li>.*?<\/li>(?:<br>)?)+/g, function (match) {
            const cleaned = match.replace(/<br>/g, "");
            return "<ul>" + cleaned + "</ul>";
        });

        // Markdown tables
        html = html.replace(
            /\|(.+)\|(?:<br>)\|[-| ]+\|(?:<br>)((?:\|.+\|(?:<br>)?)+)/g,
            function (match, header, body) {
                const headers = header.split("|").filter(Boolean)
                    .map(function (h) { return "<th>" + h.trim() + "</th>"; }).join("");
                const rows = body.split("<br>").filter(Boolean)
                    .map(function (row) {
                        const cells = row.split("|").filter(Boolean)
                            .map(function (c) { return "<td>" + c.trim() + "</td>"; }).join("");
                        return "<tr>" + cells + "</tr>";
                    }).join("");
                return "<table><thead><tr>" + headers + "</tr></thead><tbody>" + rows + "</tbody></table>";
            }
        );

        return "<p>" + html + "</p>";
    }

    // ---- Errors ----------------------------------------------------------
    function showError(title, msg) {
        els.errorTitle.textContent = title;
        els.errorMessage.textContent = msg;
        els.errorBanner.hidden = false;
    }

    function hideError() {
        els.errorBanner.hidden = true;
    }

    // ---- Utilities -------------------------------------------------------
    function clamp(n, min, max) { return Math.max(min, Math.min(max, n)); }

    function formatInt(n) {
        return (Number(n) || 0).toLocaleString();
    }

    function formatEth(n) {
        const v = Number(n) || 0;
        if (v >= 1_000_000_000) return (v / 1_000_000_000).toFixed(2) + "B ETH";
        if (v >= 1_000_000) return (v / 1_000_000).toFixed(2) + "M ETH";
        if (v >= 10_000) return Math.round(v).toLocaleString() + " ETH";
        if (v >= 1_000) return v.toLocaleString(undefined, { maximumFractionDigits: 2 }) + " ETH";
        if (v >= 1) return v.toFixed(2) + " ETH";
        if (v === 0) return "0 ETH";
        return v.toFixed(4) + " ETH";
    }

    function truncateAddress(addr) {
        if (!addr || addr.length < 12) return addr || "";
        return addr.slice(0, 6) + "..." + addr.slice(-4);
    }

    function escapeHtml(s) {
        if (s === null || s === undefined) return "";
        return String(s)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function animateNumber(element, from, to, duration) {
        const start = performance.now();
        function tick(now) {
            const elapsed = now - start;
            const progress = Math.min(elapsed / duration, 1);
            const eased = 1 - Math.pow(1 - progress, 3);
            const current = Math.round(from + (to - from) * eased);
            element.textContent = current;
            if (progress < 1) requestAnimationFrame(tick);
        }
        requestAnimationFrame(tick);
    }

    function copyText(text, button, defaultLabel) {
        if (!navigator.clipboard) return;
        navigator.clipboard.writeText(text).then(function () {
            const original = defaultLabel || button.textContent;
            button.textContent = "Copied";
            setTimeout(function () { button.textContent = original; }, 1500);
        }).catch(function () { /* ignore */ });
    }

})();
