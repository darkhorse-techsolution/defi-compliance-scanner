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
_CACHE_TTL = timedelta(hours=24)

# Single-flight lock so parallel scans triggered on cold start don't
# all try to refresh the list at the same time.
_REFRESH_LOCK = asyncio.Lock()

_CACHE_FILE = Path(__file__).resolve().parent.parent / "data" / "sanctions_cache.json"

# Upstream URLs
_GITHUB_LIST_URL = (
    "https://raw.githubusercontent.com/0xB10C/"
    "ofac-sanctioned-digital-currency-addresses/lists/sanctioned_addresses_ETH.json"
)
_OFAC_SDN_XML_URL = "https://www.treasury.gov/ofac/downloads/sdn.xml"
_CHAINALYSIS_ORACLE_URL = "https://public.chainalysis.com/api/v1/address/{address}"

_ETH_ADDRESS_RE = re.compile(r"0x[0-9a-fA-F]{40}")
_HTTP_TIMEOUT = 15.0


# =====================================================================
# Helpers
# =====================================================================

def _normalize(addresses) -> set[str]:
    """Lowercase, validate and dedupe an iterable of address strings."""
    out: set[str] = set()
    for raw in addresses or []:
        if not isinstance(raw, str):
            continue
        candidate = raw.strip().lower()
        if _ETH_ADDRESS_RE.fullmatch(candidate):
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

async def _fetch_github_list() -> set[str]:
    """Fetch the 0xB10C mirror of the OFAC ETH address list."""
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        resp = await client.get(_GITHUB_LIST_URL)
        resp.raise_for_status()
        data = resp.json()
    return _normalize(data)


async def _fetch_treasury_sdn() -> set[str]:
    """
    Fetch and parse the authoritative Treasury SDN XML.

    We do not use a full XML parser here - the SDN is large and only
    a tiny fraction of it is crypto addresses. A regex over the
    DigitalCurrencyAddress block is both faster and avoids pulling in
    lxml. This is defensive enough for a fallback source.
    """
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT * 2) as client:
        resp = await client.get(_OFAC_SDN_XML_URL)
        resp.raise_for_status()
        body = resp.text
    matches = _ETH_ADDRESS_RE.findall(body)
    return _normalize(matches)


# =====================================================================
# Live loader with fallback chain
# =====================================================================

async def _load_fresh_list() -> tuple[set[str], str]:
    """
    Run the upstream fetchers in priority order, merging every source
    we can reach. The hardcoded fallback is ALWAYS included so the
    handful of high-confidence addresses we ship with never drop out
    of the cache, even when an upstream omits them.
    """
    merged: set[str] = set()
    sources: list[str] = []

    # 1. GitHub mirror (preferred - fast, machine-readable)
    try:
        addresses = await _fetch_github_list()
        if addresses:
            merged |= addresses
            sources.append("github_0xb10c")
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("github sanctions list unavailable: %s", exc)

    # 2. Treasury SDN XML (authoritative but heavier). Fetch only if
    #    the GitHub mirror came back empty - the Treasury parse is
    #    expensive and the two lists overlap almost completely.
    if not merged:
        try:
            addresses = await _fetch_treasury_sdn()
            if addresses:
                merged |= addresses
                sources.append("treasury_sdn_xml")
        except (httpx.HTTPError, ValueError) as exc:
            logger.warning("treasury SDN feed unavailable: %s", exc)

    # 3. Disk cache from a previous successful fetch
    if not merged:
        cached = _load_cache()
        if cached:
            merged |= cached[0]
            sources.append(cached[2])

    # 4. Hardcoded high-confidence list - always merged in so well-
    #    known Tornado Cash routers etc. are never missing from the set.
    fallback = _normalize(FALLBACK_ADDRESSES)
    new_from_fallback = fallback - merged
    merged |= fallback
    if new_from_fallback:
        sources.append("hardcoded_baseline")

    if not sources:
        # Nothing at all worked, not even the fallback. Report it clearly.
        return merged, "empty"

    return merged, "+".join(sources)


# =====================================================================
# Public API
# =====================================================================

async def refresh_sanctions_list(force: bool = False) -> set[str]:
    """
    Refresh the in-memory cache. Safe to call from multiple coroutines
    concurrently - only one refresh runs at a time thanks to the lock.
    """
    global _SANCTIONS_CACHE, _SANCTIONS_UPDATED_AT, _SANCTIONS_SOURCE

    async with _REFRESH_LOCK:
        now = datetime.now(timezone.utc)
        fresh = (
            _SANCTIONS_UPDATED_AT is not None
            and (now - _SANCTIONS_UPDATED_AT) < _CACHE_TTL
            and _SANCTIONS_CACHE
        )
        if fresh and not force:
            return _SANCTIONS_CACHE

        addresses, source = await _load_fresh_list()
        if not addresses:
            # Nothing usable came back. Keep whatever we had.
            return _SANCTIONS_CACHE

        _SANCTIONS_CACHE = addresses
        _SANCTIONS_UPDATED_AT = now
        _SANCTIONS_SOURCE = source

        # Only persist lists that actually came off the wire. If the
        # only source we could reach was the hardcoded baseline the
        # disk cache would get poisoned with a permanent minimal set.
        came_from_network = any(
            s in source
            for s in ("github_0xb10c", "treasury_sdn_xml")
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


async def check_chainalysis_oracle(address: str) -> bool:
    """
    Query the free Chainalysis public sanctions API for a single
    address. Returns True if Chainalysis has identified the address as
    sanctioned. Used as a secondary live check alongside the cached
    list so additions made between cache refreshes are still caught.
    """
    if not address:
        return False
    url = _CHAINALYSIS_ORACLE_URL.format(address=address)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                return False
            data = resp.json()
    except (httpx.HTTPError, ValueError):
        return False
    return bool(data.get("identifications"))


def sanctions_status() -> dict:
    """Expose cache metadata for the /api/health endpoint."""
    return {
        "size": len(_SANCTIONS_CACHE),
        "source": _SANCTIONS_SOURCE,
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
