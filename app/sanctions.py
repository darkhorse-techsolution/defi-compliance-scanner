"""
Live OFAC sanctions list.

The scanner screens every counterparty it sees against a set of
sanctioned Ethereum addresses. In the first version this list was
hardcoded to the ten best-known Tornado Cash and Lazarus addresses -
fine for a demo, not fine for production. This module pulls a real
list from public sources and caches it on disk so repeat scans are
fast and survive process restarts.

Source order
------------
1. **Public GitHub mirror** maintained by 0xB10C. This is a machine-
   readable JSON list of every Ethereum address on the OFAC SDN. It
   tracks the Treasury feed within hours and is the fastest of the
   three sources. See:
   https://github.com/0xB10C/ofac-sanctioned-digital-currency-addresses

2. **Treasury OFAC SDN XML feed** (authoritative). Parsed here as a
   last-ditch live source if the GitHub mirror is unavailable. The
   XML is large so the parse is deliberately narrow - it only pulls
   out the Digital Currency Address fields tagged ETH.
   https://ofac.treasury.gov/specially-designated-nationals-list-data-formats-and-feeds

3. **Local on-disk cache** from a previous successful fetch. Used
   when both network sources fail (cold start with no connectivity,
   for example).

4. **Hardcoded fallback** from config.SANCTIONED_ADDRESSES. This is
   the absolute last resort and guarantees the scanner never loses
   the ability to flag the handful of addresses everybody knows are
   sanctioned.

Per-address live lookup
-----------------------
For defense in depth we also expose ``check_chainalysis_oracle``
which hits the free Chainalysis public sanctions API for a single
address. That endpoint is authoritative from Chainalysis and returns
true for any address they have identified as sanctioned. We fire it
alongside the cached-list check so a wallet that landed on the list
five minutes ago still gets caught.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import httpx

from app.config import SANCTIONED_ADDRESSES as FALLBACK_ADDRESSES


logger = logging.getLogger(__name__)


# --- Cache state ---------------------------------------------------------

_SANCTIONS_CACHE: set[str] = set()
_SANCTIONS_UPDATED_AT: Optional[datetime] = None
_SANCTIONS_SOURCE: str = "uninitialized"
_SANCTIONS_SOURCE_COUNTS: dict[str, int] = {}
_CACHE_TTL = timedelta(hours=24)

# Single-flight lock so parallel scans triggered on cold start don't
# all try to refresh the list at the same time.
_REFRESH_LOCK = asyncio.Lock()

_CACHE_FILE = Path(__file__).resolve().parent.parent / "data" / "sanctions_cache.json"

# Upstream URLs
#
# The 0xB10C mirror publishes one file per chain. We pull every
# chain/asset file and use them all - the final sanctions set is just
# the union of everything we can reach. We key the dict by the chain
# label so the health endpoint can report which upstream contributed
# how much.
_GITHUB_BASE = (
    "https://raw.githubusercontent.com/0xB10C/"
    "ofac-sanctioned-digital-currency-addresses/lists/"
)
_GITHUB_LISTS = {
    "ETH":  f"{_GITHUB_BASE}sanctioned_addresses_ETH.json",
    "USDT": f"{_GITHUB_BASE}sanctioned_addresses_USDT.json",
    "USDC": f"{_GITHUB_BASE}sanctioned_addresses_USDC.json",
    "BSC":  f"{_GITHUB_BASE}sanctioned_addresses_BSC.json",
    "ARB":  f"{_GITHUB_BASE}sanctioned_addresses_ARB.json",
    "ETC":  f"{_GITHUB_BASE}sanctioned_addresses_ETC.json",
    "TRX":  f"{_GITHUB_BASE}sanctioned_addresses_TRX.json",
    "XBT":  f"{_GITHUB_BASE}sanctioned_addresses_XBT.json",
    "BCH":  f"{_GITHUB_BASE}sanctioned_addresses_BCH.json",
    "BSV":  f"{_GITHUB_BASE}sanctioned_addresses_BSV.json",
    "BTG":  f"{_GITHUB_BASE}sanctioned_addresses_BTG.json",
    "DASH": f"{_GITHUB_BASE}sanctioned_addresses_DASH.json",
    "LTC":  f"{_GITHUB_BASE}sanctioned_addresses_LTC.json",
    "XMR":  f"{_GITHUB_BASE}sanctioned_addresses_XMR.json",
    "XRP":  f"{_GITHUB_BASE}sanctioned_addresses_XRP.json",
    "XVG":  f"{_GITHUB_BASE}sanctioned_addresses_XVG.json",
    "ZEC":  f"{_GITHUB_BASE}sanctioned_addresses_ZEC.json",
}

# Authoritative US Treasury feed. Parsed with a permissive regex that
# picks up any 0x-prefixed 40-hex-char sequence from the XML body -
# that catches every EVM-family address (ETH, USDT_ETH, USDC, BSC, ARB,
# etc.) without having to parse the full SDN schema.
_OFAC_SDN_XML_URL = (
    "https://sanctionslistservice.ofac.treas.gov/api/publicationpreview/exports/sdn.xml"
)

# Per-address live lookup from Chainalysis. Free, no key required, used
# as a belt-and-braces check on counterparties so anything added to the
# official list after the last cache refresh still gets caught.
_CHAINALYSIS_ORACLE_URL = "https://public.chainalysis.com/api/v1/address/{address}"

_ETH_ADDRESS_RE = re.compile(r"0x[0-9a-fA-F]{40}")
_HTTP_TIMEOUT = 15.0


# =====================================================================
# Helpers
# =====================================================================

def _normalize(addresses, evm_only: bool = False) -> set[str]:
    """
    Lowercase, dedupe, and minimally validate an iterable of address
    strings. By default we accept any non-empty string that looks like
    a plausible address (no whitespace, reasonable length) so the set
    can hold Bitcoin, Monero, Tron and other non-EVM formats for future
    multi-chain support. Set ``evm_only=True`` to restrict to 0x-hex
    Ethereum addresses.
    """
    out: set[str] = set()
    for raw in addresses or []:
        if not isinstance(raw, str):
            continue
        candidate = raw.strip()
        if not candidate:
            continue
        # Lowercase only the EVM 0x-prefixed hex addresses. Other chains
        # use case-sensitive encodings (base58, bech32, etc.) so we must
        # keep their casing intact.
        if candidate.startswith("0x") or candidate.startswith("0X"):
            candidate = candidate.lower()
            if evm_only and not _ETH_ADDRESS_RE.fullmatch(candidate):
                continue
        elif evm_only:
            continue
        # Sanity: skip anything obviously not an address
        if len(candidate) < 26 or len(candidate) > 128 or " " in candidate:
            continue
        out.add(candidate)
    return out


def _save_cache(addresses: set[str], source: str) -> None:
    """Persist the list to disk so warm restarts have data immediately."""
    try:
        _CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "source": source,
            "count": len(addresses),
            "addresses": sorted(addresses),
        }
        _CACHE_FILE.write_text(json.dumps(payload, indent=2))
    except OSError as exc:
        logger.warning("could not write sanctions cache: %s", exc)


def _load_cache() -> Optional[tuple[set[str], datetime, str]]:
    """Load a previously-saved sanctions list from disk, if one exists."""
    if not _CACHE_FILE.exists():
        return None
    try:
        data = json.loads(_CACHE_FILE.read_text())
    except (OSError, ValueError) as exc:
        logger.warning("could not read sanctions cache: %s", exc)
        return None
    addresses = _normalize(data.get("addresses", []))
    if not addresses:
        return None
    try:
        updated_at = datetime.fromisoformat(data.get("updated_at", ""))
    except ValueError:
        updated_at = datetime.now(timezone.utc)
    source = str(data.get("source") or "disk_cache")
    return addresses, updated_at, f"{source}+disk"


# =====================================================================
# Upstream fetchers
# =====================================================================

async def _fetch_github_chain(client: httpx.AsyncClient, label: str, url: str) -> tuple[str, set[str]]:
    """
    Pull a single 0xB10C chain-specific sanctions file. Returns the chain
    label plus whatever addresses we could parse. Failures are logged and
    return an empty set so one broken file cannot take down the refresh.
    Non-EVM chains (BTC, XMR, TRX, etc.) go through the permissive
    normalizer so their base58/bech32 addresses still land in the cache.
    """
    try:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("github list %s unavailable: %s", label, exc)
        return label, set()
    return label, _normalize(data)


async def _fetch_all_github_lists() -> dict[str, set[str]]:
    """
    Pull every 0xB10C chain file in parallel and return a dict keyed by
    chain label. Only non-empty results are included.
    """
    async with httpx.AsyncClient(
        timeout=_HTTP_TIMEOUT,
        follow_redirects=True,
    ) as client:
        tasks = [
            _fetch_github_chain(client, label, url)
            for label, url in _GITHUB_LISTS.items()
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    out: dict[str, set[str]] = {}
    for entry in results:
        if isinstance(entry, Exception):
            continue
        label, addresses = entry
        if addresses:
            out[label] = addresses
    return out


async def _fetch_treasury_sdn() -> set[str]:
    """
    Fetch and parse the authoritative Treasury SDN XML feed.

    We do not use a full XML parser here - the SDN is large and only
    a tiny fraction of it is crypto addresses. A regex over the body
    picks out anything that looks like a 0x-prefixed EVM address, and
    the feed happens to tag crypto wallets inline with their asset
    label so a simple pattern match is robust enough for a fallback
    source. The client follows the 302 redirect that Treasury now
    returns from the legacy download URL.
    """
    async with httpx.AsyncClient(
        timeout=_HTTP_TIMEOUT * 2,
        follow_redirects=True,
    ) as client:
        resp = await client.get(_OFAC_SDN_XML_URL)
        resp.raise_for_status()
        body = resp.text
    matches = _ETH_ADDRESS_RE.findall(body)
    return _normalize(matches, evm_only=True)


# =====================================================================
# Live loader with fallback chain
# =====================================================================

async def _load_fresh_list() -> tuple[set[str], str, dict[str, int]]:
    """
    Pull every upstream list we know about in parallel and merge the
    results. The hardcoded baseline is always folded in so high-
    confidence addresses never drop out of the cache even if an
    upstream omits them.

    Returns a tuple of (merged_set, composite_source_label, per_source_counts).
    The per-source counts are surfaced via /api/health so operators can
    see exactly which lists contributed to the cache at any moment.
    """
    merged: set[str] = set()
    sources: list[str] = []
    source_counts: dict[str, int] = {}

    # Fire both upstream groups at the same time. If one is slow or
    # down, the other still populates the cache.
    github_task = _fetch_all_github_lists()
    treasury_task = _fetch_treasury_sdn()
    github_result, treasury_result = await asyncio.gather(
        github_task, treasury_task, return_exceptions=True,
    )

    # 1. 0xB10C GitHub lists, one per chain
    if isinstance(github_result, dict) and github_result:
        for label, addresses in github_result.items():
            new_rows = addresses - merged
            merged |= addresses
            if new_rows:
                key = f"github_{label.lower()}"
                source_counts[key] = len(new_rows)
                sources.append(key)
    elif isinstance(github_result, Exception):
        logger.warning("github sanctions fetch failed: %s", github_result)

    # 2. Treasury SDN XML - authoritative, and covers EVM chains not
    #    represented in the 0xB10C mirror
    if isinstance(treasury_result, set) and treasury_result:
        new_rows = treasury_result - merged
        merged |= treasury_result
        if new_rows:
            source_counts["treasury_sdn_xml"] = len(new_rows)
            sources.append("treasury_sdn_xml")
    elif isinstance(treasury_result, Exception):
        logger.warning("treasury SDN fetch failed: %s", treasury_result)

    # 3. Disk cache from a previous successful fetch (only if the live
    #    sources gave us nothing at all)
    if not merged:
        cached = _load_cache()
        if cached:
            merged |= cached[0]
            sources.append(cached[2])
            source_counts["disk_cache"] = len(cached[0])

    # 4. Hardcoded high-confidence list - always merged so well-known
    #    Tornado Cash routers, Lazarus wallets, and Ronin exploiters
    #    can never be missing from the set.
    fallback = _normalize(FALLBACK_ADDRESSES)
    new_from_fallback = fallback - merged
    merged |= fallback
    if new_from_fallback:
        sources.append("hardcoded_baseline")
        source_counts["hardcoded_baseline"] = len(new_from_fallback)

    if not sources:
        return merged, "empty", source_counts

    return merged, "+".join(sources), source_counts


# =====================================================================
# Public API
# =====================================================================

async def refresh_sanctions_list(force: bool = False) -> set[str]:
    """
    Refresh the in-memory cache. Safe to call from multiple coroutines
    concurrently - only one refresh runs at a time thanks to the lock.
    """
    global _SANCTIONS_CACHE, _SANCTIONS_UPDATED_AT, _SANCTIONS_SOURCE, _SANCTIONS_SOURCE_COUNTS

    async with _REFRESH_LOCK:
        now = datetime.now(timezone.utc)
        fresh = (
            _SANCTIONS_UPDATED_AT is not None
            and (now - _SANCTIONS_UPDATED_AT) < _CACHE_TTL
            and _SANCTIONS_CACHE
        )
        if fresh and not force:
            return _SANCTIONS_CACHE

        addresses, source, counts = await _load_fresh_list()
        if not addresses:
            # Nothing usable came back. Keep whatever we had.
            return _SANCTIONS_CACHE

        _SANCTIONS_CACHE = addresses
        _SANCTIONS_UPDATED_AT = now
        _SANCTIONS_SOURCE = source
        _SANCTIONS_SOURCE_COUNTS = counts

        # Only persist lists that actually came off the wire. If the
        # only source we could reach was the hardcoded baseline the
        # disk cache would get poisoned with a permanent minimal set.
        came_from_network = any(
            s.startswith("github_") or s == "treasury_sdn_xml"
            for s in source.split("+")
        )
        if came_from_network:
            _save_cache(addresses, source)

        logger.info(
            "sanctions list refreshed: %d addresses from %s",
            len(addresses), source,
        )
        return _SANCTIONS_CACHE


async def get_sanctioned_addresses() -> set[str]:
    """
    Get the current sanctions list, refreshing from upstream if the
    in-memory cache is empty or older than the TTL.
    """
    return await refresh_sanctions_list(force=False)


async def is_address_sanctioned(address: str) -> bool:
    """Convenience check against the cached sanctions set."""
    if not address:
        return False
    sanctions = await get_sanctioned_addresses()
    return address.lower() in sanctions


async def check_chainalysis_oracle(address: str) -> list[dict]:
    """
    Query the Chainalysis public sanctions API for a single address.

    Returns a list of identification records. An empty list means the
    address is clean. Each record has the shape::

        {
            "category": "sanctions" | "sanctioned entity" | ...,
            "name":        str,
            "description": str,
            "url":         str,  # primary source citation
        }

    As of 2026-04 Chainalysis requires an ``X-API-Key`` header on every
    request even for the free-tier sanctions oracle (5000 req / 5 min).
    When the key is missing we skip the call entirely rather than pay
    the round-trip cost of a Cloudflare-challenged request. The key is
    read from ``CHAINALYSIS_API_KEY`` and is completely optional - the
    scanner still works without it, falling back on the cached lists.
    """
    import os

    api_key = os.getenv("CHAINALYSIS_API_KEY", "").strip()
    if not api_key or not address:
        return []

    url = _CHAINALYSIS_ORACLE_URL.format(address=address)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, headers={"X-API-Key": api_key})
            if resp.status_code != 200:
                return []
            data = resp.json()
    except (httpx.HTTPError, ValueError):
        return []
    identifications = data.get("identifications") or []
    return identifications if isinstance(identifications, list) else []


def chainalysis_oracle_available() -> bool:
    """True when a Chainalysis API key is configured for oracle lookups."""
    import os
    return bool(os.getenv("CHAINALYSIS_API_KEY", "").strip())


async def batch_check_chainalysis_oracle(
    addresses: list[str],
    max_concurrent: int = 5,
) -> dict[str, list[dict]]:
    """
    Run the Chainalysis oracle against a batch of addresses with a
    bounded concurrency ceiling. Returns a dict keyed by address, with
    the list of identification records as the value. Addresses not in
    the dict are either clean or failed to look up.

    This is intentionally capped - scanning every counterparty of a
    whale wallet would burn through the 5k/5min free-tier budget. The
    caller should pre-filter to the highest-signal addresses (e.g. the
    top N counterparties by volume) before asking for a batch check.
    """
    if not addresses:
        return {}

    semaphore = asyncio.Semaphore(max_concurrent)
    hits: dict[str, list[dict]] = {}

    async def _guarded(addr: str) -> None:
        async with semaphore:
            identifications = await check_chainalysis_oracle(addr)
            if identifications:
                hits[addr.lower()] = identifications

    await asyncio.gather(*(_guarded(a) for a in addresses), return_exceptions=True)
    return hits


def sanctions_status() -> dict:
    """Expose cache metadata for the /api/health endpoint."""
    return {
        "size": len(_SANCTIONS_CACHE),
        "source": _SANCTIONS_SOURCE,
        "source_breakdown": dict(_SANCTIONS_SOURCE_COUNTS),
        "updated_at": (
            _SANCTIONS_UPDATED_AT.isoformat()
            if _SANCTIONS_UPDATED_AT else None
        ),
    }


# Convenience sync accessor for callers that already have the list
# loaded (the risk engine, which runs inside a sync function).
def get_cached_sanctions_sync() -> set[str]:
    """
    Return the currently-cached sanctions set without hitting the
    network. The risk engine is synchronous so it cannot await
    ``get_sanctioned_addresses`` directly - instead, main.py awaits
    the refresh before running the analysis, and the engine reads
    the primed cache here.
    """
    if _SANCTIONS_CACHE:
        return _SANCTIONS_CACHE
    return _normalize(FALLBACK_ADDRESSES)
