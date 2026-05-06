#!/usr/bin/env python3
# ============================================================================
#  Terminal
#  ----------------------------------------------------------------------------
#  Copyright (c) 2026 Oscar Zarraga Perez.
#  Created by Oscar Zarraga Perez. All rights reserved.
#
#  This software is the original work of Oscar Zarraga Perez and is released
#  under the MIT License (see LICENSE). Redistribution in source or binary
#  form, with or without modification, MUST retain this copyright notice and
#  the author attribution. Stripping or altering this notice violates the
#  terms of the MIT License under which this code is distributed.
#
#  Author : Oscar Zarraga Perez
#  Project: Terminal - single-file market-data backend
# ============================================================================
"""
Terminal  -  single-file market-data backend.

Created by Oscar Zarraga Perez. Copyright (c) 2026 Oscar Zarraga Perez.
Released under the MIT License. Attribution to the author must be preserved
in all copies and substantial portions of the software.

Aggregates real-time market data from public sources that don't require
auth or crumb tokens:
  - Stooq        : US equities, indices, FX (CSV API)
  - CoinGecko    : crypto prices and OHLC
  - Binance      : crypto fallback
  - exchangerate.host : FX fallback
  - SEC EDGAR    : filings

Usage:
    python server.py
    python server.py --port 8788 --no-browser

Requires Python 3.8+. Standard library only.
"""
from __future__ import annotations

# Author attribution constants. These are referenced throughout the codebase
# (HTTP response headers, /api/about endpoint, server_version) so the credit
# is woven into runtime behaviour, not just comment headers.
__author__    = "Oscar Zarraga Perez"
__copyright__ = "Copyright (c) 2026 Oscar Zarraga Perez"
__license__   = "MIT"
__credits__   = ["Oscar Zarraga Perez (creator, sole author)"]

import argparse, csv, html, io, json, os, re, shutil, subprocess, sys, threading, time, webbrowser
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, quote
from urllib.request import Request, build_opener
from urllib.error import HTTPError

HERE = os.path.dirname(os.path.abspath(__file__))
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
# SEC EDGAR requires a User-Agent identifying the application + contact email.
# Operators forking this code should set their own contact info before running.
SEC_UA = os.environ.get("SEC_USER_AGENT", "Terminal admin@example.com")

# ---------- UA rotation pool (different "profiles" per request) ----------
# Modern real-world browsers across OS/engine combos so rate-limiters don't
# see a single fingerprint hammering from this host.
UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.2651.105",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.6533.88 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
]
_ua_idx = [0]
_ua_lock = threading.Lock()


def _pick_ua():
    """Round-robin through UA_POOL — each request gets a different 'profile'."""
    with _ua_lock:
        ua = UA_POOL[_ua_idx[0] % len(UA_POOL)]
        _ua_idx[0] += 1
    return ua


# ---------- Per-host throttle + 429 cooldown ----------
# Goal: spread requests so we don't burst-fire 15 calls at a single host.
_host_state_lock = threading.Lock()
_host_state: dict = {}  # host -> {"last": ts, "cooldown_until": ts}

# Per-host min interval between sequential requests (seconds). A tighter
# budget here means less chance of tripping the limiter; looser means faster.
HOST_MIN_INTERVAL = {
    "query1.finance.yahoo.com":  0.35,
    "query2.finance.yahoo.com":  0.35,
    "finance.yahoo.com":         0.4,
    "api.nasdaq.com":            0.3,
    "finviz.com":                0.5,
    "stockanalysis.com":         0.4,
    "stooq.com":                 0.2,
    "data.sec.gov":              0.2,
    "www.sec.gov":               0.2,
    "efts.sec.gov":              0.3,
    "api.coingecko.com":         0.2,
    "api.binance.com":           0.2,
    "news.google.com":           0.3,
    # --- keyed provider endpoints (only hit when keys are configured) ---
    "www.alphavantage.co":       0.4,   # free tier 5/min
    "financialmodelingprep.com": 0.25,  # free tier 250/day
    "finnhub.io":                0.25,  # free tier 60/min
    "api.twelvedata.com":        0.25,  # free tier 8/min
    # --- no-key additions ---
    "api.stocktwits.com":        0.5,
    "feeds.finance.yahoo.com":   0.3,
    "www.marketwatch.com":       0.5,
}


def _host_state_get(host):
    with _host_state_lock:
        return _host_state.setdefault(host, {"last": 0.0, "cooldown_until": 0.0})


def _host_in_cooldown(host):
    st = _host_state_get(host)
    return st["cooldown_until"] > time.time()


def _host_throttle(host):
    """Sleep just enough so we respect the per-host min interval."""
    st = _host_state_get(host)
    interval = HOST_MIN_INTERVAL.get(host, 0.1)
    now = time.time()
    wait = st["last"] + interval - now
    if wait > 0:
        time.sleep(min(wait, 1.5))
    with _host_state_lock:
        _host_state[host]["last"] = time.time()


def _host_cooldown(host, seconds):
    with _host_state_lock:
        _host_state[host]["cooldown_until"] = max(
            _host_state.get(host, {}).get("cooldown_until", 0.0),
            time.time() + seconds,
        )
    sys.stderr.write(f"[cooldown] {host} backing off for {seconds}s\n")


def _polite_fetch(url, *, host=None, accept="application/json", referer=None,
                   timeout=15, extra_headers=None, ua=None):
    """GET with UA rotation, per-host throttle, and 429-aware cooldown.
    Returns (status_code, body_text). 0 on network failure, -1 on cooldown."""
    if not host:
        try:
            host = urlparse(url).netloc
        except Exception:
            host = ""
    if host and _host_in_cooldown(host):
        return -1, ""
    if host:
        _host_throttle(host)
    headers = {
        "User-Agent":      ua or _pick_ua(),
        "Accept":          accept,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
    }
    if referer:
        headers["Referer"] = referer
    if extra_headers:
        headers.update(extra_headers)
    try:
        req = Request(url, headers=headers)
        r = build_opener().open(req, timeout=timeout)
        raw = r.read()
        if raw[:2] == b"\x1f\x8b":
            import gzip
            raw = gzip.decompress(raw)
        return r.getcode(), raw.decode("utf-8", "ignore")
    except HTTPError as e:
        if e.code in (429, 999, 401, 403) and host:
            # 60s cooldown on rate limit; 30s for soft-auth errors
            _host_cooldown(host, 60 if e.code in (429, 999) else 30)
        try:
            body = e.read().decode("utf-8", "ignore")
        except Exception:
            body = ""
        return e.code, body
    except Exception as e:
        return 0, str(e)

# ---------- Tiny in-memory response cache ----------
_cache_lock = threading.Lock()
_cache: dict = {}


def _cache_get(key):
    with _cache_lock:
        v = _cache.get(key)
        if v and v[0] > time.time():
            return v[1]
        if v:
            _cache.pop(key, None)
    return None


def _cache_put(key, value, ttl=10):
    with _cache_lock:
        _cache[key] = (time.time() + ttl, value)


def _parallel(tasks, timeout=15, max_workers=12):
    """Run {name: callable} concurrently. Returns {name: result or None}. Exceptions swallowed."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    results = {name: None for name in tasks}
    if not tasks:
        return results
    with ThreadPoolExecutor(max_workers=min(max_workers, len(tasks))) as ex:
        futs = {ex.submit(fn): name for name, fn in tasks.items()}
        try:
            for fut in as_completed(futs, timeout=timeout):
                name = futs[fut]
                try:
                    results[name] = fut.result(timeout=1)
                except Exception as e:
                    sys.stderr.write(f"[parallel {name}] {e}\n")
        except Exception:
            # Overall timeout hit — return whatever completed
            pass
    return results


def _fetch(url, timeout=15, accept="*/*"):
    """Plain GET. Returns (status_code, body_text). 0 on network failure."""
    try:
        req = Request(url, headers={"User-Agent": UA, "Accept": accept})
        r = build_opener().open(req, timeout=timeout)
        return r.getcode(), r.read().decode("utf-8", "ignore")
    except HTTPError as e:
        try:
            body = e.read().decode("utf-8", "ignore")
        except Exception:
            body = ""
        return e.code, body
    except Exception as e:
        return 0, str(e)


# ---------- Optional API keys loader ----------
# keys.json lives next to this script. It is OPTIONAL — every provider that
# needs a key falls back to None (and therefore short-circuits) when its key is
# absent. This keeps the terminal runnable with zero configuration while letting
# power users drop in free-tier keys for extra redundancy and depth.
#
# Expected shape (all optional):
# {
#   "alpha_vantage": "XXXXXXXXXXXXXXXX",
#   "fmp":           "XXXXXXXXXXXXXXXX",
#   "finnhub":       "XXXXXXXXXXXXXXXX",
#   "twelve_data":   "XXXXXXXXXXXXXXXX"
# }
#
# Aliases accepted for each provider (case-insensitive, dashes/underscores):
#   alpha_vantage | alphavantage | av
#   fmp           | financialmodelingprep | financial_modeling_prep
#   finnhub       | fh
#   twelve_data   | twelvedata | td
KEYS_FILE = os.path.join(HERE, "keys.json")

_KEY_ALIASES = {
    "alpha_vantage": ("alpha_vantage", "alphavantage", "av", "alpha-vantage"),
    "fmp":           ("fmp", "financialmodelingprep", "financial_modeling_prep",
                      "financial-modeling-prep"),
    "finnhub":       ("finnhub", "fh"),
    "twelve_data":   ("twelve_data", "twelvedata", "td", "twelve-data"),
}


def _load_keys():
    """Read keys.json if present. Always returns a dict (empty on any issue).

    Also honors env vars as a fallback:
      ALPHAVANTAGE_API_KEY / ALPHA_VANTAGE_KEY
      FMP_API_KEY
      FINNHUB_API_KEY
      TWELVE_DATA_API_KEY / TWELVEDATA_API_KEY
    """
    raw: dict = {}
    try:
        if os.path.isfile(KEYS_FILE):
            with open(KEYS_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f) or {}
    except Exception as e:
        sys.stderr.write(f"[keys] failed to read {KEYS_FILE}: {e}\n")
        raw = {}

    def _pick(aliases):
        for a in aliases:
            for k, v in raw.items():
                if str(k).strip().lower().replace("-", "_") == a.replace("-", "_"):
                    if isinstance(v, str) and v.strip():
                        return v.strip()
        return None

    out = {}
    for canon, aliases in _KEY_ALIASES.items():
        out[canon] = _pick(aliases)

    # Env var fallbacks
    env_map = {
        "alpha_vantage": ("ALPHAVANTAGE_API_KEY", "ALPHA_VANTAGE_KEY"),
        "fmp":           ("FMP_API_KEY",),
        "finnhub":       ("FINNHUB_API_KEY",),
        "twelve_data":   ("TWELVE_DATA_API_KEY", "TWELVEDATA_API_KEY"),
    }
    for canon, names in env_map.items():
        if out.get(canon):
            continue
        for n in names:
            v = os.environ.get(n)
            if v and v.strip():
                out[canon] = v.strip()
                break

    present = [k for k, v in out.items() if v]
    if present:
        sys.stderr.write(f"[keys] loaded: {', '.join(present)}\n")
    else:
        sys.stderr.write("[keys] none configured — keyed providers disabled\n")
    return out


KEYS = _load_keys()
_KEYS_MTIME = 0.0
try:
    _KEYS_MTIME = os.path.getmtime(KEYS_FILE) if os.path.isfile(KEYS_FILE) else 0.0
except Exception:
    _KEYS_MTIME = 0.0

_KEYS_LOCK = threading.Lock()
_KEYS_LAST_CHECK = 0.0


def _maybe_reload_keys():
    """Re-read keys.json if its mtime changed. Rate-limited to once every 2s."""
    global KEYS, _KEYS_MTIME, _KEYS_LAST_CHECK
    now = time.time()
    # Avoid stat'ing the FS on every single request; at most every 2s
    if now - _KEYS_LAST_CHECK < 2.0:
        return
    with _KEYS_LOCK:
        _KEYS_LAST_CHECK = now
        try:
            mtime = os.path.getmtime(KEYS_FILE) if os.path.isfile(KEYS_FILE) else 0.0
        except Exception:
            mtime = 0.0
        if mtime != _KEYS_MTIME:
            sys.stderr.write(f"[keys] keys.json changed, reloading\n")
            _KEYS_MTIME = mtime
            KEYS = _load_keys()


def reload_keys():
    """Force an immediate re-read of keys.json. Returns the new keys_status()."""
    global KEYS, _KEYS_MTIME, _KEYS_LAST_CHECK
    with _KEYS_LOCK:
        try:
            _KEYS_MTIME = os.path.getmtime(KEYS_FILE) if os.path.isfile(KEYS_FILE) else 0.0
        except Exception:
            _KEYS_MTIME = 0.0
        _KEYS_LAST_CHECK = time.time()
        KEYS = _load_keys()
    return keys_status()


def av_key():
    """Alpha Vantage key or None. Auto-reloads keys.json if it changed."""
    _maybe_reload_keys()
    return KEYS.get("alpha_vantage")


def fmp_key():
    """Financial Modeling Prep key or None. Auto-reloads keys.json if it changed."""
    _maybe_reload_keys()
    return KEYS.get("fmp")


def finnhub_key():
    """Finnhub key or None. Auto-reloads keys.json if it changed."""
    _maybe_reload_keys()
    return KEYS.get("finnhub")


def td_key():
    """Twelve Data key or None. Auto-reloads keys.json if it changed."""
    _maybe_reload_keys()
    return KEYS.get("twelve_data")


def keys_status():
    """Return {provider: bool} for /api/keys/status and diagnostics."""
    return {
        "alpha_vantage": bool(av_key()),
        "fmp":           bool(fmp_key()),
        "finnhub":       bool(finnhub_key()),
        "twelve_data":   bool(td_key()),
    }


# ---------- Symbol mapping ----------
INDEX_MAP = {
    "^GSPC":     "^spx",   # S&P 500
    "^DJI":      "^dji",   # Dow Jones
    "^IXIC":     "^ndq",   # Nasdaq Composite
    "^NDX":      "^ndx",   # Nasdaq 100
    "^RUT":      "^rut",   # Russell 2000
    "^VIX":      "^vix",   # Volatility
    "^FTSE":     "^ukx",   # FTSE 100
    "^N225":     "^nkx",   # Nikkei 225
    "^HSI":      "^hsi",   # Hang Seng
    "^STOXX50E": "^stx",   # Euro Stoxx 50
    "^FCHI":     "^cac",   # CAC 40
    "^GDAXI":    "^dax",   # DAX
    "^BVSP":     "^bvp",   # Bovespa
    "^TNX":      "10usy.b",  # US 10Y yield
}

CRYPTO_MAP = {
    "BTC-USD":   "bitcoin",
    "ETH-USD":   "ethereum",
    "SOL-USD":   "solana",
    "DOGE-USD":  "dogecoin",
    "BNB-USD":   "binancecoin",
    "XRP-USD":   "ripple",
    "ADA-USD":   "cardano",
    "AVAX-USD":  "avalanche-2",
    "MATIC-USD": "matic-network",
    "DOT-USD":   "polkadot",
    "LINK-USD":  "chainlink",
    "LTC-USD":   "litecoin",
    "BCH-USD":   "bitcoin-cash",
    "TRX-USD":   "tron",
    "SHIB-USD":  "shiba-inu",
    "BTC":       "bitcoin",
    "ETH":       "ethereum",
}

CRYPTO_BASE = {"BTC", "ETH", "SOL", "DOGE", "BNB", "XRP", "ADA", "AVAX",
               "MATIC", "DOT", "LINK", "LTC", "BCH", "TRX", "SHIB"}


def is_crypto(sym):
    s = sym.upper()
    if s in CRYPTO_MAP:
        return True
    if "-" in s:
        base, _, quote_ccy = s.partition("-")
        return base in CRYPTO_BASE
    return False


def to_stooq(sym):
    s = sym.upper()
    if s in INDEX_MAP:
        return INDEX_MAP[s]
    if s.endswith("=X"):                       # FX (EURUSD=X -> eurusd)
        return s[:-2].lower()
    if s.startswith("^"):
        return s.lower()
    return f"{s.lower()}.us"                   # default: US equity


def to_coingecko(sym):
    s = sym.upper()
    if s in CRYPTO_MAP:
        return CRYPTO_MAP[s]
    base = s.split("-")[0] if "-" in s else s
    return CRYPTO_MAP.get(base) or base.lower()


# ---------- Stooq fetchers ----------
def stooq_quote(sym):
    """Single-row CSV quote from Stooq."""
    cache_key = f"sq::{sym}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    ss = to_stooq(sym)
    url = f"https://stooq.com/q/l/?s={quote(ss)}&f=sd2t2ohlcv&h&e=csv"
    code, body = _fetch(url, accept="text/csv")
    if code != 200 or not body or body.lstrip().startswith("<"):
        _cache_put(cache_key, None, ttl=30)
        return None
    rows = list(csv.DictReader(io.StringIO(body)))
    if not rows:
        _cache_put(cache_key, None, ttl=30)
        return None
    r = rows[0]

    def num(k):
        v = (r.get(k) or "").strip()
        if v in ("", "N/D", "N/A"):
            return None
        try:
            return float(v)
        except ValueError:
            return None

    out = {
        "open":   num("Open"),
        "high":   num("High"),
        "low":    num("Low"),
        "close":  num("Close"),
        "volume": int(num("Volume")) if num("Volume") else None,
        "name":   r.get("Symbol", sym),
        "date":   r.get("Date"),
        "time":   r.get("Time"),
    }
    _cache_put(cache_key, out, ttl=10)
    return out


def stooq_history(sym, days=365, interval="d"):
    """Daily OHLCV CSV from Stooq, truncated to `days` most-recent rows."""
    cache_key = f"sh::{sym}::{interval}"
    cached = _cache_get(cache_key)
    if cached is None:
        ss = to_stooq(sym)
        url = f"https://stooq.com/q/d/l/?s={quote(ss)}&i={interval}"
        code, body = _fetch(url, accept="text/csv", timeout=20)
        if code != 200 or not body or body.lstrip().startswith("<"):
            _cache_put(cache_key, [], ttl=60)
            return []
        rows = list(csv.DictReader(io.StringIO(body)))
        out = []
        for r in rows:
            try:
                out.append({
                    "date":   r.get("Date"),
                    "open":   float(r.get("Open") or 0) or None,
                    "high":   float(r.get("High") or 0) or None,
                    "low":    float(r.get("Low") or 0) or None,
                    "close":  float(r.get("Close") or 0) or None,
                    "volume": int(float(r.get("Volume") or 0)) or None,
                })
            except Exception:
                continue
        _cache_put(cache_key, out, ttl=300)
        cached = out
    if days and len(cached) > days:
        return cached[-days:]
    return cached


# ---------- CoinGecko fetchers ----------
def cg_price(sym_id):
    cache_key = f"cgp::{sym_id}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    url = (f"https://api.coingecko.com/api/v3/simple/price?ids={quote(sym_id)}"
           f"&vs_currencies=usd&include_24hr_change=true&include_24hr_vol=true"
           f"&include_market_cap=true&include_last_updated_at=true")
    code, body = _fetch(url, accept="application/json")
    if code != 200:
        _cache_put(cache_key, None, ttl=30)
        return None
    try:
        d = json.loads(body)
        p = d.get(sym_id, {})
        if not p:
            _cache_put(cache_key, None, ttl=30)
            return None
        out = {
            "price":          p.get("usd"),
            "change_24h_pct": p.get("usd_24h_change"),
            "volume_24h":     p.get("usd_24h_vol"),
            "market_cap":     p.get("usd_market_cap"),
            "updated_at":     p.get("last_updated_at"),
        }
        _cache_put(cache_key, out, ttl=15)
        return out
    except Exception:
        _cache_put(cache_key, None, ttl=30)
        return None


def cg_ohlc(sym_id, days=365):
    """OHLC bars from CoinGecko. Allowed days: 1, 7, 14, 30, 90, 180, 365."""
    if days <= 1:    bucket = 1
    elif days <= 7:  bucket = 7
    elif days <= 14: bucket = 14
    elif days <= 30: bucket = 30
    elif days <= 90: bucket = 90
    elif days <= 180: bucket = 180
    else:            bucket = 365
    cache_key = f"cgo::{sym_id}::{bucket}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    url = f"https://api.coingecko.com/api/v3/coins/{quote(sym_id)}/ohlc?vs_currency=usd&days={bucket}"
    code, body = _fetch(url, accept="application/json")
    if code != 200:
        _cache_put(cache_key, [], ttl=60)
        return []
    try:
        bars = json.loads(body)  # [[ts_ms, o, h, l, c], ...]
        out = []
        for b in bars:
            try:
                ts, o, h, l, c = b
                out.append({"ts": int(ts / 1000), "open": o, "high": h,
                            "low": l, "close": c, "volume": None})
            except Exception:
                continue
        _cache_put(cache_key, out, ttl=120)
        return out
    except Exception:
        return []


# ---------- Yahoo-shaped response builders ----------
def _derive_change(d):
    """Fill missing regularMarketChange / regularMarketChangePercent if we can.
    Tries: direct math, then Yahoo v8 chart (last two closes), then Stooq history."""
    if not d: return d
    price = d.get("regularMarketPrice")
    prev  = d.get("regularMarketPreviousClose")
    chg   = d.get("regularMarketChange")
    pct   = d.get("regularMarketChangePercent")
    # 1) If we have price+prev but no chg, compute.
    if price is not None and prev not in (None, 0) and chg is None:
        chg = price - prev
    # Always recompute pct from chg/prev when both are known. Upstream feeds
    # sometimes return a stale or wrong pct (e.g. chart meta's % vs chart range
    # start, not prior day) — chg/prev is the trustworthy baseline.
    if chg is not None and prev not in (None, 0):
        pct = (chg / prev) * 100.0
    # 2) If prev is still missing but price is here, try chart APIs.
    if price is not None and (prev in (None, 0) or chg is None):
        try:
            raw = yahoo_chart_raw(d.get("symbol") or "", range_str="5d", interval="1d")
            closes = []
            if raw:
                ind = (raw.get("indicators") or {}).get("quote") or []
                if ind and isinstance(ind[0], dict):
                    closes = [c for c in (ind[0].get("close") or []) if c is not None]
            if len(closes) >= 2:
                prev = closes[-2]
                chg  = price - prev
                pct  = (chg / prev) * 100.0 if prev else None
        except Exception:
            pass
    if price is not None and (prev in (None, 0) or chg is None):
        try:
            hist = stooq_history(d.get("symbol") or "", days=5)
            if hist and len(hist) >= 2 and hist[-2].get("close"):
                prev = hist[-2]["close"]
                chg  = price - prev
                pct  = (chg / prev) * 100.0 if prev else None
        except Exception:
            pass
    d["regularMarketPreviousClose"] = prev
    d["regularMarketChange"]        = chg
    d["regularMarketChangePercent"] = pct
    return d


def shape_quote(sym):
    """Build a Yahoo-shaped quote dict for one symbol."""
    d = _shape_quote_inner(sym)
    return _derive_change(d) if d else d


def _shape_quote_inner(sym):
    if is_crypto(sym):
        cgid = to_coingecko(sym)
        p = cg_price(cgid)
        if not p or p.get("price") is None:
            return None
        price = p["price"]
        pct = p.get("change_24h_pct") or 0.0
        change = price * pct / 100.0 if pct else 0.0
        prev = price - change if change else price
        return {
            "symbol": sym,
            "shortName": sym,
            "longName": cgid.replace("-", " ").title(),
            "currency": "USD",
            "fullExchangeName": "CoinGecko",
            "marketState": "REGULAR",
            "regularMarketPrice": price,
            "regularMarketPreviousClose": prev,
            "regularMarketChange": change,
            "regularMarketChangePercent": pct,
            "regularMarketVolume": p.get("volume_24h"),
            "marketCap": p.get("market_cap"),
        }

    # ----- Primary: Yahoo v7 single-symbol quote endpoint -----
    # v7 returns clean regularMarketPreviousClose / Change / ChangePercent
    # straight from Yahoo. v8 chart-meta has a known bug where
    # `chartPreviousClose` is the start of the chart range (which can be
    # 1y ago when a 1y chart is cached), making the daily change% wildly
    # incorrect. v7 is the right primary source.
    yv7_all = yahoo_v7_quotes_batch([sym])
    yv7 = yv7_all.get(sym.upper()) or yv7_all.get(sym)
    if yv7 and yv7.get("regularMarketPrice") is not None:
        return yv7

    # ----- Secondary: Yahoo v8 chart meta (JSON, fast) -----
    yvq = yahoo_v8_quote(sym)
    if yvq and yvq.get("regularMarketPrice") is not None:
        # If Stooq has a fresher tick, blend last/open/high/low/volume from Stooq
        sq = stooq_quote(sym)
        if sq and sq.get("close") is not None:
            yvq.setdefault("regularMarketOpen",    sq.get("open"))
            yvq.setdefault("regularMarketDayHigh", sq.get("high"))
            yvq.setdefault("regularMarketDayLow",  sq.get("low"))
            yvq.setdefault("regularMarketVolume",  sq.get("volume"))
        return yvq

    # ----- Secondary: Stooq CSV -----
    q = stooq_quote(sym)
    if q and q.get("close") is not None:
        hist = stooq_history(sym, days=260)
        prev = None
        if len(hist) >= 2:
            prev = hist[-2]["close"]
        last = q["close"]
        change = (last - prev) if (last is not None and prev) else None
        pct = (change / prev * 100.0) if (change is not None and prev) else None
        yr_hi = max((b["high"] for b in hist if b["high"]), default=None)
        yr_lo = min((b["low"]  for b in hist if b["low"]),  default=None)
        return {
            "symbol": sym,
            "shortName": q.get("name") or sym,
            "longName": q.get("name") or sym,
            "currency": "USD",
            "fullExchangeName": "Stooq",
            "marketState": "REGULAR",
            "regularMarketPrice": last,
            "regularMarketPreviousClose": prev,
            "regularMarketChange": change,
            "regularMarketChangePercent": pct,
            "regularMarketOpen": q.get("open"),
            "regularMarketDayHigh": q.get("high"),
            "regularMarketDayLow": q.get("low"),
            "regularMarketVolume": q.get("volume"),
            "fiftyTwoWeekHigh": yr_hi,
            "fiftyTwoWeekLow": yr_lo,
            "fiftyTwoWeekRange": (f"{yr_lo} - {yr_hi}"
                                  if yr_lo is not None and yr_hi is not None else None),
            "regularMarketDayRange": (f"{q.get('low')} - {q.get('high')}"
                                      if q.get("low") is not None and q.get("high") is not None else None),
        }

    # ----- Tertiary: Yahoo HTML scrape -----
    yfb = _yh_quote_fallback(sym)
    if yfb:
        return yfb

    # ----- Quaternary: Finviz scrape -----
    ffb = _finviz_quote_fallback(sym)
    if ffb:
        return ffb

    # ----- Last resort: stockanalysis.com (covers international / OTC ADRs
    # like MTPLF/3350.T that Yahoo + Stooq + Finviz don't index). -----
    sa_fb = _sa_quote_fallback(sym)
    if sa_fb:
        return sa_fb

    # NOTE: Keyed providers (Alpha Vantage / FMP / Finnhub / Twelve Data) are
    # intentionally NOT in the default quote rotation — each has a small daily
    # quota that burns out instantly when used for every watchlist refresh.
    # They are used on dedicated panel endpoints only. See /api/stats,
    # /api/profile-ext, /api/ratings, /api/statistics.
    return None


def _sa_quote_fallback(sym):
    """Scrape stockanalysis.com's quote overview page as a last-resort quote
    source. Works for /stocks/<sym>/ (US) and /quote/<exch>/<tkr>/ (intl).
    Returns a Yahoo-shaped quote dict or None."""
    if is_crypto(sym) or sym.endswith("=X") or sym.startswith("^"):
        return None
    url = sa_url(sym, "")  # root overview page
    if not url:
        return None
    body = _scrape_fetch(url, ttl=600)
    if not body:
        return None

    def _num(s):
        if s is None:
            return None
        s = str(s).replace(",", "").replace("$", "").replace("¥", "").strip()
        if s.endswith("%"):
            s = s[:-1].strip()
        mult = 1.0
        for suf, m in (("T", 1e12), ("B", 1e9), ("M", 1e6), ("K", 1e3)):
            if s.upper().endswith(suf):
                try:
                    return float(s[:-1]) * m
                except Exception:
                    return None
        try:
            return float(s)
        except Exception:
            return None

    # Price: look for the large price element. SA uses class names with
    # 'text-4xl' or 'svelte' — also pulls from meta description as fallback.
    price = None
    m = re.search(r"class=\"[^\"]*(?:text-4xl|quote-price)[^\"]*\"[^>]*>\s*([^<]+)<",
                  body, re.I)
    if m:
        price = _num(m.group(1))
    if price is None:
        m = re.search(r"<meta[^>]+name=\"description\"[^>]+content=\"([^\"]+)\"", body, re.I)
        if m:
            pm = re.search(r"(?:price|quote)[^\d\-]*([\-\$\¥\d,\.]+)", m.group(1), re.I)
            if pm:
                price = _num(pm.group(1))
    if price is None:
        return None

    # Change % / change $: small sibling elements near the price — parse any
    # signed pct anywhere in the first 600 chars after the price.
    chg = None
    chg_pct = None
    seg = body[max(0, body.find(str(price))):]
    mpct = re.search(r"([\-\+]?\d+\.\d+)\s*%", seg[:1500])
    if mpct:
        chg_pct = _num(mpct.group(1))

    # Name / exchange from <title> or OG tags
    short_name = long_name = sym
    mt = re.search(r"<title[^>]*>(.*?)</title>", body, re.S | re.I)
    if mt:
        title = _strip_tags(mt.group(1))
        long_name = re.split(r"\s+[-–|]\s+", title)[0].strip() or sym
        short_name = long_name
    info = _foreign_info(sym) or {}
    if info.get("name"):
        long_name = info["name"]
        short_name = info.get("shortName") or info["name"]

    prev = None
    if price is not None and chg_pct not in (None, 0):
        try:
            prev = price / (1.0 + chg_pct / 100.0)
            chg = price - prev
        except Exception:
            prev = None

    return {
        "symbol": sym,
        "shortName": short_name,
        "longName":  long_name,
        "currency":  info.get("currency") or "USD",
        "fullExchangeName": info.get("exchange") or "OTC",
        "marketState": "REGULAR",
        "regularMarketPrice": price,
        "regularMarketPreviousClose": prev,
        "regularMarketChange": chg,
        "regularMarketChangePercent": chg_pct,
    }


def _yh_quote_fallback(sym):
    """Build a Yahoo-shaped quote dict from a Yahoo HTML scrape (no auth required)."""
    if is_crypto(sym) or sym.endswith("=X") or sym.startswith("^"):
        return None
    cache_key = f"yh_qfb::{sym.upper()}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    url = f"https://finance.yahoo.com/quote/{sym}"
    body = _scrape_fetch(url, ttl=60)
    if not body:
        _cache_put(cache_key, None, ttl=30)
        return None
    # Parse __NEXT_DATA__ or root.App.main JSON
    blob = None
    m = re.search(r"root\.App\.main\s*=\s*(\{.*?\});\n", body, re.S)
    if m:
        try: blob = json.loads(m.group(1))
        except Exception: blob = None
    if blob is None:
        m2 = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', body, re.S | re.I)
        if m2:
            try: blob = json.loads(m2.group(1))
            except Exception: blob = None
    if not blob:
        # As a last resort, use regex to pluck currentPrice / regularMarketPrice
        pm = re.search(r'"regularMarketPrice"\s*:\s*\{?\s*"raw"\s*:\s*([\d.]+)', body)
        if pm:
            price = float(pm.group(1))
            shape = {
                "symbol": sym, "shortName": sym, "longName": sym,
                "currency": "USD", "fullExchangeName": "Yahoo (HTML)",
                "marketState": "REGULAR",
                "regularMarketPrice": price, "regularMarketPreviousClose": None,
                "regularMarketChange": None, "regularMarketChangePercent": None,
            }
            _cache_put(cache_key, shape, ttl=60)
            return shape
        _cache_put(cache_key, None, ttl=30)
        return None
    # Walk for QuoteSummaryStore.price
    qss = None
    stack = [blob]
    while stack:
        n = stack.pop()
        if isinstance(n, dict):
            if "QuoteSummaryStore" in n:
                qss = n["QuoteSummaryStore"]
                break
            for v in n.values():
                if isinstance(v, (dict, list)): stack.append(v)
        elif isinstance(n, list):
            stack.extend(n)
    if not qss:
        _cache_put(cache_key, None, ttl=30)
        return None
    p = qss.get("price") or {}
    sd = qss.get("summaryDetail") or {}

    def _r(node, key):
        v = node.get(key) if isinstance(node, dict) else None
        if isinstance(v, dict): return v.get("raw")
        return v

    shape = {
        "symbol":            sym,
        "shortName":         _r(p, "shortName") or sym,
        "longName":          _r(p, "longName") or sym,
        "currency":          _r(p, "currency") or "USD",
        "fullExchangeName":  _r(p, "exchangeName") or "Yahoo (HTML)",
        "marketState":       _r(p, "marketState") or "REGULAR",
        "regularMarketPrice":         _r(p, "regularMarketPrice"),
        "regularMarketPreviousClose": _r(p, "regularMarketPreviousClose"),
        "regularMarketChange":        _r(p, "regularMarketChange"),
        "regularMarketChangePercent": _r(p, "regularMarketChangePercent"),
        "regularMarketOpen":          _r(p, "regularMarketOpen"),
        "regularMarketDayHigh":       _r(p, "regularMarketDayHigh"),
        "regularMarketDayLow":        _r(p, "regularMarketDayLow"),
        "regularMarketVolume":        _r(p, "regularMarketVolume"),
        "marketCap":                  _r(p, "marketCap"),
        "fiftyTwoWeekHigh":           _r(sd, "fiftyTwoWeekHigh"),
        "fiftyTwoWeekLow":            _r(sd, "fiftyTwoWeekLow"),
    }
    _cache_put(cache_key, shape, ttl=60)
    return shape


def _finviz_quote_fallback(sym):
    """Build a quote dict from Finviz snapshot. Less rich but always-on."""
    if is_crypto(sym) or sym.endswith("=X") or sym.startswith("^"):
        return None
    cache_key = f"fv_qfb::{sym.upper()}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    fv = finviz_quote(sym)
    if not fv:
        _cache_put(cache_key, None, ttl=30)
        return None
    price = _parse_money(fv.get("Price"))
    if price is None:
        _cache_put(cache_key, None, ttl=30)
        return None
    pct_s = fv.get("Change") or "0"
    pct   = _parse_money(pct_s)
    prev  = (price / (1 + pct/100)) if (pct is not None and pct != 0) else price
    change = price - prev
    rng = (fv.get("52W Range") or "").split(" - ")
    yr_lo = _parse_money(rng[0]) if len(rng) == 2 else None
    yr_hi = _parse_money(rng[1]) if len(rng) == 2 else None
    day = (fv.get("Range") or "").split(" - ")
    d_lo = _parse_money(day[0]) if len(day) == 2 else None
    d_hi = _parse_money(day[1]) if len(day) == 2 else None
    shape = {
        "symbol":            sym,
        "shortName":         fv.get("Company") or sym,
        "longName":          fv.get("Company") or sym,
        "currency":          "USD",
        "fullExchangeName":  "Finviz",
        "marketState":       "REGULAR",
        "regularMarketPrice":         price,
        "regularMarketPreviousClose": prev,
        "regularMarketChange":        change,
        "regularMarketChangePercent": pct,
        "regularMarketDayHigh":       d_hi,
        "regularMarketDayLow":        d_lo,
        "regularMarketVolume":        _parse_int(fv.get("Volume")),
        "marketCap":                  _parse_money(fv.get("Market Cap")),
        "fiftyTwoWeekHigh":           yr_hi,
        "fiftyTwoWeekLow":            yr_lo,
    }
    _cache_put(cache_key, shape, ttl=120)
    return shape


def yahoo_chart_raw(sym, range_str="1y", interval="1d"):
    """Hit Yahoo v8 chart endpoint, return the raw dict (with meta/timestamp/indicators).
    Honors per-host throttle + cooldown via _polite_fetch."""
    cache_key = f"yhchart_raw::{sym.upper()}::{range_str}::{interval}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    hosts = ("query1.finance.yahoo.com", "query2.finance.yahoo.com")
    for host in hosts:
        url = (f"https://{host}/v8/finance/chart/{quote(sym)}"
               f"?range={range_str}&interval={interval}&includePrePost=false&events=div,split")
        code, body = _polite_fetch(url, host=host, accept="application/json",
                                   referer="https://finance.yahoo.com/", timeout=15)
        if code == -1:
            # Host is cooling down — try the sibling host once, then bail
            continue
        if code != 200 or not body:
            sys.stderr.write(f"[yhchart_raw {host} {sym}] code={code}\n")
            continue
        try:
            d = json.loads(body)
        except Exception as e:
            sys.stderr.write(f"[yhchart_raw {host} {sym}] parse {e}\n")
            continue
        res = ((d.get("chart") or {}).get("result") or [])
        if res:
            _cache_put(cache_key, res[0], ttl=120 if interval in ("1d","1wk","1mo") else 30)
            return res[0]
    _cache_put(cache_key, None, ttl=45)
    return None


def yahoo_v8_quote(sym):
    """Build a quote dict using Yahoo v8 chart meta. Reuses any already-cached
    chart range for the same symbol so we don't re-hit Yahoo just to get meta."""
    # Try every plausible cached range first (meta is identical across ranges)
    r0 = None
    for rng in ("1y", "6mo", "3mo", "1mo", "5d", "2y", "5y"):
        c = _cache_get(f"yhchart_raw::{sym.upper()}::{rng}::1d")
        if c:
            r0 = c
            break
    if r0 is None:
        # Only fetch if nothing cached — use short range (5d) to keep response small
        r0 = yahoo_chart_raw(sym, range_str="5d", interval="1d")
    if not r0:
        return None
    meta = r0.get("meta") or {}
    price = meta.get("regularMarketPrice")
    # IMPORTANT: prefer `previousClose` (yesterday's close) over
    # `chartPreviousClose` — the latter is the close at the START of the
    # chart range (e.g. 1 year ago for the 1y range), which produces wildly
    # incorrect daily change %. Only fall back to chartPreviousClose if
    # nothing else is available.
    prev = meta.get("previousClose")
    # Always derive yesterday's close from the indicators array when possible —
    # it's the most reliable single source for the previous trading day.
    try:
        closes = ((r0.get("indicators") or {}).get("quote") or [{}])[0].get("close") or []
        closes = [c for c in closes if c is not None]
        if price is None and closes:
            price = closes[-1]
        if (prev is None or prev == price) and len(closes) >= 2:
            # closes[-1] is today's last bar; closes[-2] is yesterday
            prev = closes[-2]
    except Exception:
        pass
    if prev is None:
        prev = meta.get("chartPreviousClose")
    if price is None:
        return None
    change = (price - prev) if (prev is not None) else None
    pct    = (change / prev * 100.0) if (change is not None and prev) else None
    return {
        "symbol":                     sym,
        "shortName":                  meta.get("shortName") or meta.get("symbol") or sym,
        "longName":                   meta.get("longName")  or meta.get("shortName") or sym,
        "currency":                   meta.get("currency") or "USD",
        "fullExchangeName":           meta.get("fullExchangeName") or meta.get("exchangeName") or "Yahoo Finance",
        "marketState":                meta.get("marketState") or "REGULAR",
        "regularMarketPrice":         price,
        "regularMarketPreviousClose": prev,
        "regularMarketChange":        change,
        "regularMarketChangePercent": pct,
        "regularMarketOpen":          meta.get("regularMarketOpen"),
        "regularMarketDayHigh":       meta.get("regularMarketDayHigh"),
        "regularMarketDayLow":        meta.get("regularMarketDayLow"),
        "regularMarketVolume":        meta.get("regularMarketVolume"),
        "fiftyTwoWeekHigh":           meta.get("fiftyTwoWeekHigh"),
        "fiftyTwoWeekLow":            meta.get("fiftyTwoWeekLow"),
    }


def yahoo_v7_quotes_batch(symbols):
    """Fetch many quotes in a single HTTP call via Yahoo v7. Returns {sym: shape_dict}.
    Massively reduces request count for the watchlist / strip where we'd otherwise
    fire one fetch per ticker."""
    syms = [s.strip().upper() for s in symbols if s and s.strip()]
    if not syms:
        return {}
    # De-dup and preserve order
    seen = set(); ordered = []
    for s in syms:
        if s not in seen:
            seen.add(s); ordered.append(s)
    joined = ",".join(ordered)
    cache_key = f"yhv7::{joined}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    hosts = ("query1.finance.yahoo.com", "query2.finance.yahoo.com")
    out = {}
    for host in hosts:
        url = f"https://{host}/v7/finance/quote?symbols={quote(joined)}"
        code, body = _polite_fetch(url, host=host, accept="application/json",
                                   referer="https://finance.yahoo.com/", timeout=15)
        if code == -1:
            continue
        if code != 200 or not body:
            sys.stderr.write(f"[yhv7 {host}] code={code}\n")
            continue
        try:
            d = json.loads(body)
        except Exception:
            continue
        results = (((d.get("quoteResponse") or {}).get("result")) or [])
        for r in results:
            sym = r.get("symbol")
            if not sym:
                continue
            price = r.get("regularMarketPrice")
            prev  = r.get("regularMarketPreviousClose")
            change = r.get("regularMarketChange")
            pct    = r.get("regularMarketChangePercent")
            if change is None and price is not None and prev is not None:
                change = price - prev
            if pct is None and change is not None and prev:
                pct = change / prev * 100.0
            out[sym] = {
                "symbol":                     sym,
                "shortName":                  r.get("shortName") or sym,
                "longName":                   r.get("longName") or r.get("shortName") or sym,
                "currency":                   r.get("currency") or "USD",
                "fullExchangeName":           r.get("fullExchangeName") or r.get("exchange") or "Yahoo Finance",
                "marketState":                r.get("marketState") or "REGULAR",
                "regularMarketPrice":         price,
                "regularMarketPreviousClose": prev,
                "regularMarketChange":        change,
                "regularMarketChangePercent": pct,
                "regularMarketOpen":          r.get("regularMarketOpen"),
                "regularMarketDayHigh":       r.get("regularMarketDayHigh"),
                "regularMarketDayLow":        r.get("regularMarketDayLow"),
                "regularMarketVolume":        r.get("regularMarketVolume"),
                "fiftyTwoWeekHigh":           r.get("fiftyTwoWeekHigh"),
                "fiftyTwoWeekLow":            r.get("fiftyTwoWeekLow"),
                "marketCap":                  r.get("marketCap"),
            }
        if out:
            # Derive any missing change/pct from chart history when possible.
            for k,v in list(out.items()):
                out[k] = _derive_change(v)
            _cache_put(cache_key, out, ttl=15)
            return out
    _cache_put(cache_key, out, ttl=30)
    return out


def yahoo_chart_api(sym, range_str="1y", interval="1d"):
    """Fetch OHLCV from Yahoo Finance's public v8 chart endpoint. Returns list of bars or []."""
    r0 = yahoo_chart_raw(sym, range_str=range_str, interval=interval)
    if not r0:
        return []
    ts = r0.get("timestamp") or []
    q0 = (((r0.get("indicators") or {}).get("quote") or [{}])[0]) or {}
    opens  = q0.get("open")   or []
    highs  = q0.get("high")   or []
    lows   = q0.get("low")    or []
    closes = q0.get("close")  or []
    vols   = q0.get("volume") or []
    bars = []
    n = min(len(ts), len(opens), len(highs), len(lows), len(closes))
    for i in range(n):
        if closes[i] is None:
            continue
        bars.append({
            "ts":     ts[i],
            "open":   opens[i], "high": highs[i], "low": lows[i], "close": closes[i],
            "volume": vols[i] if i < len(vols) else None,
        })
    return bars


# ============================================================================
# Keyed providers (Alpha Vantage, FMP, Finnhub, Twelve Data)
# ----------------------------------------------------------------------------
# Each provider helper short-circuits to None when its key is absent, so the
# rest of the system can call them unconditionally and treat None as "skip".
# All HTTP goes through _polite_fetch so per-host throttle + cooldown apply.
# ============================================================================

def _av_call(function, **params):
    """Low-level Alpha Vantage call. Returns parsed JSON dict or None."""
    key = av_key()
    if not key:
        return None
    params["function"] = function
    params["apikey"]   = key
    qs = "&".join(f"{k}={quote(str(v))}" for k, v in params.items())
    url = f"https://www.alphavantage.co/query?{qs}"
    code, body = _polite_fetch(url, host="www.alphavantage.co",
                                accept="application/json")
    if code != 200 or not body:
        if code not in (-1, 0):
            sys.stderr.write(f"[AV {function}] http={code} body={body[:200]!r}\n")
        return None
    try:
        d = json.loads(body)
    except Exception:
        sys.stderr.write(f"[AV {function}] non-JSON body={body[:200]!r}\n")
        return None
    if not isinstance(d, dict):
        return None
    # AV signals throttle / errors via these keys; treat as soft-failure but log
    msg = d.get("Note") or d.get("Information") or d.get("Error Message")
    if msg:
        sys.stderr.write(f"[AV {function}] provider msg: {msg[:240]!r}\n")
        return None
    return d


def av_quote(sym):
    """Alpha Vantage GLOBAL_QUOTE. Returns dict {price, change, changePct, prevClose, volume} or None."""
    d = _av_call("GLOBAL_QUOTE", symbol=sym)
    if not d:
        return None
    g = d.get("Global Quote") or d.get("GLOBAL QUOTE") or {}
    if not g:
        return None

    def _f(v):
        try:
            return float(v)
        except Exception:
            return None

    price = _f(g.get("05. price"))
    if price is None:
        return None
    pct_raw = (g.get("10. change percent") or "").rstrip("%").strip()
    pct = _f(pct_raw)
    return {
        "price":      price,
        "change":     _f(g.get("09. change")),
        "changePct":  pct,
        "prevClose":  _f(g.get("08. previous close")),
        "open":       _f(g.get("02. open")),
        "high":       _f(g.get("03. high")),
        "low":        _f(g.get("04. low")),
        "volume":     _f(g.get("06. volume")),
        "source":     "AlphaVantage",
    }


def av_intraday(sym, interval="5min", outputsize="compact"):
    """Alpha Vantage TIME_SERIES_INTRADAY. interval in {1min,5min,15min,30min,60min}.
    Returns list of bars [{ts,open,high,low,close,volume}] sorted ascending, or []."""
    if interval not in ("1min", "5min", "15min", "30min", "60min"):
        interval = "5min"
    d = _av_call("TIME_SERIES_INTRADAY", symbol=sym, interval=interval,
                 outputsize=outputsize, adjusted="true")
    if not d:
        return []
    series_key = next((k for k in d.keys() if k.startswith("Time Series")), None)
    if not series_key:
        return []
    series = d.get(series_key) or {}
    bars = []
    for stamp, row in series.items():
        try:
            ts = int(time.mktime(time.strptime(stamp, "%Y-%m-%d %H:%M:%S")))
        except Exception:
            continue
        try:
            bars.append({
                "ts":     ts,
                "open":   float(row.get("1. open")),
                "high":   float(row.get("2. high")),
                "low":    float(row.get("3. low")),
                "close":  float(row.get("4. close")),
                "volume": float(row.get("5. volume") or 0),
            })
        except Exception:
            continue
    bars.sort(key=lambda b: b["ts"])
    return bars


def av_daily(sym, outputsize="compact"):
    """Alpha Vantage TIME_SERIES_DAILY (adjusted). Returns ascending bar list or []."""
    d = _av_call("TIME_SERIES_DAILY_ADJUSTED", symbol=sym, outputsize=outputsize)
    if not d:
        # Free tier sometimes lacks adjusted; try regular daily
        d = _av_call("TIME_SERIES_DAILY", symbol=sym, outputsize=outputsize)
    if not d:
        return []
    series_key = next((k for k in d.keys() if k.startswith("Time Series")), None)
    if not series_key:
        return []
    series = d.get(series_key) or {}
    bars = []
    for date_s, row in series.items():
        try:
            ts = int(time.mktime(time.strptime(date_s, "%Y-%m-%d")))
        except Exception:
            continue
        try:
            bars.append({
                "ts":     ts,
                "date":   date_s,
                "open":   float(row.get("1. open")),
                "high":   float(row.get("2. high")),
                "low":    float(row.get("3. low")),
                "close":  float(row.get("4. close")),
                "volume": float(row.get("6. volume") or row.get("5. volume") or 0),
            })
        except Exception:
            continue
    bars.sort(key=lambda b: b["ts"])
    return bars


def av_overview(sym):
    """Alpha Vantage OVERVIEW: company fundamentals snapshot. Returns dict or None."""
    d = _av_call("OVERVIEW", symbol=sym)
    if not d or not d.get("Symbol"):
        return None

    def _f(v):
        try:
            f = float(v)
            return f if f == f else None  # filter NaN
        except Exception:
            return None

    return {
        "symbol":            d.get("Symbol"),
        "name":              d.get("Name"),
        "exchange":          d.get("Exchange"),
        "currency":          d.get("Currency"),
        "country":           d.get("Country"),
        "sector":            d.get("Sector"),
        "industry":          d.get("Industry"),
        "description":       d.get("Description"),
        "marketCap":         _f(d.get("MarketCapitalization")),
        "ebitda":            _f(d.get("EBITDA")),
        "peRatio":           _f(d.get("PERatio")),
        "pegRatio":          _f(d.get("PEGRatio")),
        "bookValue":         _f(d.get("BookValue")),
        "dividendPerShare":  _f(d.get("DividendPerShare")),
        "dividendYield":     _f(d.get("DividendYield")),
        "eps":               _f(d.get("EPS")),
        "profitMargin":      _f(d.get("ProfitMargin")),
        "operatingMargin":   _f(d.get("OperatingMarginTTM")),
        "returnOnAssets":    _f(d.get("ReturnOnAssetsTTM")),
        "returnOnEquity":    _f(d.get("ReturnOnEquityTTM")),
        "revenue":           _f(d.get("RevenueTTM")),
        "grossProfit":       _f(d.get("GrossProfitTTM")),
        "dilutedEps":        _f(d.get("DilutedEPSTTM")),
        "quarterlyEarningsGrowth": _f(d.get("QuarterlyEarningsGrowthYOY")),
        "quarterlyRevenueGrowth":  _f(d.get("QuarterlyRevenueGrowthYOY")),
        "analystTargetPrice":      _f(d.get("AnalystTargetPrice")),
        "trailingPE":        _f(d.get("TrailingPE")),
        "forwardPE":         _f(d.get("ForwardPE")),
        "priceToSalesTTM":   _f(d.get("PriceToSalesRatioTTM")),
        "priceToBook":       _f(d.get("PriceToBookRatio")),
        "evToRevenue":       _f(d.get("EVToRevenue")),
        "evToEbitda":        _f(d.get("EVToEBITDA")),
        "beta":              _f(d.get("Beta")),
        "fiftyTwoWeekHigh":  _f(d.get("52WeekHigh")),
        "fiftyTwoWeekLow":   _f(d.get("52WeekLow")),
        "fiftyDayMA":        _f(d.get("50DayMovingAverage")),
        "twoHundredDayMA":   _f(d.get("200DayMovingAverage")),
        "sharesOutstanding": _f(d.get("SharesOutstanding")),
        "source":            "AlphaVantage",
    }


def _av_financials_fetch(function, sym):
    """Shared helper: fetch an AV INCOME_STATEMENT / BALANCE_SHEET / CASH_FLOW
    payload and return (annualReports, quarterlyReports) as lists or ([], [])."""
    d = _av_call(function, symbol=sym)
    if not d or not isinstance(d, dict):
        return [], []
    return (d.get("annualReports") or []), (d.get("quarterlyReports") or [])


def _av_to_float(v):
    if v is None or v == "" or v == "None":
        return None
    try:
        f = float(v)
        return f if f == f else None
    except Exception:
        return None


# Mapping AV -> Yahoo-style normalized row keys so that merge logic downstream
# doesn't care which provider a period came from.
_AV_IS_MAP = {
    "totalRevenue":                "totalRevenue",
    "costOfRevenue":               "costOfRevenue",
    "grossProfit":                 "grossProfit",
    "operatingIncome":             "operatingIncome",
    "operatingExpenses":           "totalOperatingExpenses",
    "netIncome":                   "netIncome",
    "netIncomeFromContinuingOperations": "netIncomeFromContinuingOps",
    "researchAndDevelopment":      "researchDevelopment",
    "sellingGeneralAndAdministrative": "sellingGeneralAdministrative",
    "incomeTaxExpense":            "incomeTaxExpense",
    "incomeBeforeTax":             "incomeBeforeTax",
    "interestExpense":             "interestExpense",
    "interestIncome":              "interestIncome",
    "nonInterestIncome":           "totalOtherIncomeExpenseNet",
    "depreciationAndAmortization": "depreciationAndAmortization",
    "depreciation":                "depreciation",
    "ebit":                        "ebit",
    "ebitda":                      "ebitda",
}
_AV_BS_MAP = {
    "totalAssets":                 "totalAssets",
    "totalLiabilities":            "totalLiab",
    "totalShareholderEquity":      "totalStockholderEquity",
    "cashAndCashEquivalentsAtCarrying":    "cash",
    "cashAndShortTermInvestments": "cashAndShortTermInvestments",
    "shortTermInvestments":        "shortTermInvestments",
    "longTermInvestments":         "longTermInvestments",
    "inventory":                   "inventory",
    "currentNetReceivables":       "netReceivables",
    "currentAccountsPayable":      "accountsPayable",
    "totalCurrentAssets":          "totalCurrentAssets",
    "totalCurrentLiabilities":     "totalCurrentLiabilities",
    "shortTermDebt":               "shortLongTermDebt",
    "longTermDebt":                "longTermDebt",
    "longTermDebtNoncurrent":      "longTermDebt",
    "retainedEarnings":            "retainedEarnings",
    "propertyPlantEquipment":      "propertyPlantEquipment",
    "goodwill":                    "goodWill",
    "intangibleAssets":            "intangibleAssets",
    "intangibleAssetsExcludingGoodwill": "intangibleAssets",
    "commonStock":                 "commonStock",
    "treasuryStock":               "treasuryStock",
    "accumulatedOtherComprehensiveIncome": "accumulatedOtherComprehensiveIncome",
    "deferredRevenue":             "deferredRevenue",
    "commonStockSharesOutstanding": "commonStockSharesOutstanding",
}
_AV_CF_MAP = {
    "operatingCashflow":           "totalCashFromOperatingActivities",
    "cashflowFromInvestment":      "totalCashflowsFromInvestingActivities",
    "cashflowFromFinancing":       "totalCashFromFinancingActivities",
    "netIncome":                   "netIncome",
    "capitalExpenditures":         "capitalExpenditures",
    "dividendPayout":              "dividendsPaid",
    "dividendPayoutCommonStock":   "dividendsPaid",
    "paymentsForRepurchaseOfCommonStock": "repurchaseOfStock",
    "proceedsFromIssuanceOfCommonStock":  "issuanceOfStock",
    "proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet": "debtIssued",
    "proceedsFromRepaymentsOfShortTermDebt": "debtRepaid",
    "paymentsForRepurchaseOfEquity": "repurchaseOfStock",
    "changeInReceivables":         "changeToAccountReceivables",
    "changeInInventory":           "changeToInventory",
    "changeInOperatingAssets":     "changeToOperatingActivities",
    "changeInOperatingLiabilities": "changeToAccountsPayable",
    "depreciationDepletionAndAmortization": "depreciation",
    "stockBasedCompensation":      "stockBasedCompensation",
}


def _av_normalize(row, field_map):
    """Take a raw AV period dict, produce {field: _raw(val)} against field_map."""
    out = {}
    for av_k, std_k in field_map.items():
        val = _av_to_float(row.get(av_k))
        if val is None:
            continue
        out[std_k] = _raw(val)
    end = row.get("fiscalDateEnding")
    if end:
        out["endDate"] = _date_stub(end)
    return out


def av_financials(sym):
    """Fetch income, balance, cashflow statements from Alpha Vantage.
    Returns a Yahoo-shaped merge dict (income/balance/cashflow annual+quarterly).
    Returns {} when AV has no key or no data."""
    if not av_key():
        return {}
    is_ann, is_qtr = _av_financials_fetch("INCOME_STATEMENT", sym)
    bs_ann, bs_qtr = _av_financials_fetch("BALANCE_SHEET", sym)
    cf_ann, cf_qtr = _av_financials_fetch("CASH_FLOW", sym)
    if not (is_ann or is_qtr or bs_ann or bs_qtr or cf_ann or cf_qtr):
        return {}

    def build(rows, mapping, derive_fcf=False):
        out = []
        for r in rows[:8]:
            row = _av_normalize(r, mapping)
            if not row:
                continue
            # Derive FCF for cashflow
            if derive_fcf:
                op = (row.get("totalCashFromOperatingActivities") or {}).get("raw")
                cx = (row.get("capitalExpenditures") or {}).get("raw")
                if op is not None and cx is not None:
                    row["freeCashFlow"] = _raw(op - abs(cx))
            out.append(row)
        return out

    return {
        "incomeStatementHistory":            {"incomeStatementHistory": build(is_ann, _AV_IS_MAP)},
        "incomeStatementHistoryQuarterly":   {"incomeStatementHistory": build(is_qtr, _AV_IS_MAP)},
        "balanceSheetHistory":               {"balanceSheetStatements": build(bs_ann, _AV_BS_MAP)},
        "balanceSheetHistoryQuarterly":      {"balanceSheetStatements": build(bs_qtr, _AV_BS_MAP)},
        "cashflowStatementHistory":          {"cashflowStatements": build(cf_ann, _AV_CF_MAP, derive_fcf=True)},
        "cashflowStatementHistoryQuarterly": {"cashflowStatements": build(cf_qtr, _AV_CF_MAP, derive_fcf=True)},
        "_meta_source_financials":             "AlphaVantage",
    }


# ---------------------------------------------------------------------------
# Financial Modeling Prep (FMP)   —   financialmodelingprep.com
# Free tier: 250 req/day, US stocks only. Richest set of fundamentals of the 4.
# ---------------------------------------------------------------------------

def _fmp_call(path, **params):
    """Low-level FMP call. Returns parsed JSON (list or dict) or None."""
    key = fmp_key()
    if not key:
        return None
    params["apikey"] = key
    qs = "&".join(f"{k}={quote(str(v))}" for k, v in params.items() if v is not None)
    url = f"https://financialmodelingprep.com/api/v3/{path}?{qs}"
    code, body = _polite_fetch(url, host="financialmodelingprep.com",
                                accept="application/json")
    if code != 200 or not body:
        if code not in (-1, 0):
            sys.stderr.write(f"[FMP {path}] http={code} body={body[:200]!r}\n")
        return None
    try:
        d = json.loads(body)
    except Exception:
        sys.stderr.write(f"[FMP {path}] non-JSON body={body[:200]!r}\n")
        return None
    # FMP returns {"Error Message": "..."} on bad key / over-limit
    if isinstance(d, dict) and d.get("Error Message"):
        sys.stderr.write(f"[FMP {path}] provider msg: {d.get('Error Message')[:240]!r}\n")
        return None
    return d


def fmp_quote(sym):
    """FMP /quote/{sym}. Returns normalized quote dict or None."""
    d = _fmp_call(f"quote/{quote(sym)}")
    if not d or not isinstance(d, list) or not d:
        return None
    q = d[0] or {}
    if not isinstance(q, dict) or q.get("price") in (None, ""):
        return None

    def _f(v):
        try:
            return float(v) if v is not None else None
        except Exception:
            return None

    return {
        "symbol":       q.get("symbol") or sym,
        "name":         q.get("name"),
        "price":        _f(q.get("price")),
        "change":       _f(q.get("change")),
        "changePct":    _f(q.get("changesPercentage")),
        "prevClose":    _f(q.get("previousClose")),
        "open":         _f(q.get("open")),
        "high":         _f(q.get("dayHigh")),
        "low":          _f(q.get("dayLow")),
        "volume":       _f(q.get("volume")),
        "avgVolume":    _f(q.get("avgVolume")),
        "marketCap":    _f(q.get("marketCap")),
        "pe":           _f(q.get("pe")),
        "eps":          _f(q.get("eps")),
        "sharesOutstanding": _f(q.get("sharesOutstanding")),
        "fiftyTwoWeekHigh":  _f(q.get("yearHigh")),
        "fiftyTwoWeekLow":   _f(q.get("yearLow")),
        "fiftyDayMA":        _f(q.get("priceAvg50")),
        "twoHundredDayMA":   _f(q.get("priceAvg200")),
        "exchange":     q.get("exchange"),
        "source":       "FMP",
    }


def fmp_history(sym, days=365):
    """FMP historical prices. Returns ascending bar list or []."""
    d = _fmp_call(f"historical-price-full/{quote(sym)}",
                   timeseries=max(1, min(int(days), 3650)))
    if not d:
        return []
    if isinstance(d, dict):
        hist = d.get("historical") or []
    else:
        hist = d if isinstance(d, list) else []
    bars = []
    for row in hist:
        try:
            ts = int(time.mktime(time.strptime(row.get("date"), "%Y-%m-%d")))
        except Exception:
            continue
        try:
            bars.append({
                "ts":     ts,
                "date":   row.get("date"),
                "open":   float(row.get("open")),
                "high":   float(row.get("high")),
                "low":    float(row.get("low")),
                "close":  float(row.get("close")),
                "volume": float(row.get("volume") or 0),
            })
        except Exception:
            continue
    bars.sort(key=lambda b: b["ts"])
    return bars


def fmp_intraday(sym, interval="5min"):
    """FMP historical-chart/{interval}/{sym}. interval in {1min,5min,15min,30min,1hour,4hour}.
    Returns ascending bars or []."""
    if interval not in ("1min", "5min", "15min", "30min", "1hour", "4hour"):
        interval = "5min"
    d = _fmp_call(f"historical-chart/{interval}/{quote(sym)}")
    if not d or not isinstance(d, list):
        return []
    bars = []
    for row in d:
        try:
            ts = int(time.mktime(time.strptime(row.get("date"), "%Y-%m-%d %H:%M:%S")))
        except Exception:
            continue
        try:
            bars.append({
                "ts":     ts,
                "open":   float(row.get("open")),
                "high":   float(row.get("high")),
                "low":    float(row.get("low")),
                "close":  float(row.get("close")),
                "volume": float(row.get("volume") or 0),
            })
        except Exception:
            continue
    bars.sort(key=lambda b: b["ts"])
    return bars


def fmp_profile(sym):
    """FMP /profile/{sym}. Returns company profile dict or None."""
    d = _fmp_call(f"profile/{quote(sym)}")
    if not d or not isinstance(d, list) or not d:
        return None
    p = d[0] or {}
    if not isinstance(p, dict):
        return None

    def _f(v):
        try:
            return float(v) if v is not None else None
        except Exception:
            return None

    return {
        "symbol":       p.get("symbol") or sym,
        "name":         p.get("companyName"),
        "price":        _f(p.get("price")),
        "beta":         _f(p.get("beta")),
        "volAvg":       _f(p.get("volAvg")),
        "marketCap":    _f(p.get("mktCap")),
        "lastDiv":      _f(p.get("lastDiv")),
        "range":        p.get("range"),
        "changes":      _f(p.get("changes")),
        "currency":     p.get("currency"),
        "cik":          p.get("cik"),
        "isin":         p.get("isin"),
        "cusip":        p.get("cusip"),
        "exchange":     p.get("exchange"),
        "exchangeShortName": p.get("exchangeShortName"),
        "industry":     p.get("industry"),
        "website":      p.get("website"),
        "description":  p.get("description"),
        "ceo":          p.get("ceo"),
        "sector":       p.get("sector"),
        "country":      p.get("country"),
        "fullTimeEmployees": p.get("fullTimeEmployees"),
        "phone":        p.get("phone"),
        "address":      p.get("address"),
        "city":         p.get("city"),
        "state":        p.get("state"),
        "zip":          p.get("zip"),
        "image":        p.get("image"),
        "ipoDate":      p.get("ipoDate"),
        "source":       "FMP",
    }


def fmp_ratios(sym):
    """FMP /ratios-ttm/{sym}. Returns TTM ratio snapshot or None."""
    d = _fmp_call(f"ratios-ttm/{quote(sym)}")
    if not d or not isinstance(d, list) or not d:
        return None
    r = d[0] or {}
    if not isinstance(r, dict):
        return None

    def _f(v):
        try:
            return float(v) if v is not None else None
        except Exception:
            return None

    return {
        "peRatioTTM":              _f(r.get("peRatioTTM")),
        "pegRatioTTM":             _f(r.get("pegRatioTTM")),
        "priceToSalesTTM":         _f(r.get("priceToSalesRatioTTM")),
        "priceToBookTTM":          _f(r.get("priceToBookRatioTTM")),
        "priceToFreeCashFlowTTM":  _f(r.get("priceToFreeCashFlowsRatioTTM")),
        "returnOnEquityTTM":       _f(r.get("returnOnEquityTTM")),
        "returnOnAssetsTTM":       _f(r.get("returnOnAssetsTTM")),
        "grossProfitMarginTTM":    _f(r.get("grossProfitMarginTTM")),
        "operatingMarginTTM":      _f(r.get("operatingProfitMarginTTM")),
        "netProfitMarginTTM":      _f(r.get("netProfitMarginTTM")),
        "currentRatioTTM":         _f(r.get("currentRatioTTM")),
        "quickRatioTTM":           _f(r.get("quickRatioTTM")),
        "debtToEquityTTM":         _f(r.get("debtEquityRatioTTM")),
        "dividendYieldTTM":        _f(r.get("dividendYieldTTM")),
        "payoutRatioTTM":          _f(r.get("payoutRatioTTM")),
        "source":                  "FMP",
    }


def fmp_news(sym, limit=20):
    """FMP /stock_news?tickers={sym}. Returns list of news dicts or []."""
    d = _fmp_call("stock_news", tickers=sym, limit=max(1, min(int(limit), 50)))
    if not d or not isinstance(d, list):
        return []
    out = []
    for n in d:
        if not isinstance(n, dict):
            continue
        out.append({
            "title":       n.get("title"),
            "url":         n.get("url"),
            "publishedAt": n.get("publishedDate"),
            "source":      n.get("site") or "FMP",
            "image":       n.get("image"),
            "symbol":      n.get("symbol") or sym,
            "summary":     n.get("text"),
        })
    return out


# ---- FMP financial statements ----

_FMP_IS_MAP = {
    "revenue":                     "totalRevenue",
    "costOfRevenue":               "costOfRevenue",
    "grossProfit":                 "grossProfit",
    "operatingIncome":             "operatingIncome",
    "operatingExpenses":           "totalOperatingExpenses",
    "netIncome":                   "netIncome",
    "researchAndDevelopmentExpenses":     "researchDevelopment",
    "sellingGeneralAndAdministrativeExpenses": "sellingGeneralAdministrative",
    "sellingAndMarketingExpenses": "sellingAndMarketingExpense",
    "generalAndAdministrativeExpenses":   "generalAndAdministrative",
    "incomeTaxExpense":            "incomeTaxExpense",
    "incomeBeforeTax":             "incomeBeforeTax",
    "interestExpense":             "interestExpense",
    "interestIncome":              "interestIncome",
    "totalOtherIncomeExpensesNet": "totalOtherIncomeExpenseNet",
    "depreciationAndAmortization": "depreciationAndAmortization",
    "stockBasedCompensation":      "stockBasedCompensation",
    "eps":                         "epsBasic",
    "epsdiluted":                  "epsDiluted",
    "weightedAverageShsOut":       "weightedAvgSharesBasic",
    "weightedAverageShsOutDil":    "weightedAvgSharesDiluted",
    "ebit":                        "ebit",
    "ebitda":                      "ebitda",
}
_FMP_BS_MAP = {
    "totalAssets":                 "totalAssets",
    "totalLiabilities":            "totalLiab",
    "totalStockholdersEquity":     "totalStockholderEquity",
    "cashAndCashEquivalents":      "cash",
    "cashAndShortTermInvestments": "cashAndShortTermInvestments",
    "shortTermInvestments":        "shortTermInvestments",
    "longTermInvestments":         "longTermInvestments",
    "inventory":                   "inventory",
    "netReceivables":              "netReceivables",
    "accountPayables":             "accountsPayable",
    "totalCurrentAssets":          "totalCurrentAssets",
    "totalCurrentLiabilities":     "totalCurrentLiabilities",
    "shortTermDebt":               "shortLongTermDebt",
    "longTermDebt":                "longTermDebt",
    "totalDebt":                   "totalDebt",
    "retainedEarnings":            "retainedEarnings",
    "propertyPlantEquipmentNet":   "propertyPlantEquipment",
    "goodwill":                    "goodWill",
    "intangibleAssets":            "intangibleAssets",
    "commonStock":                 "commonStock",
    "treasuryStock":               "treasuryStock",
    "accumulatedOtherComprehensiveIncomeLoss": "accumulatedOtherComprehensiveIncome",
    "deferredRevenue":             "deferredRevenue",
    "netDebt":                     "netDebt",
}
_FMP_CF_MAP = {
    "operatingCashFlow":           "totalCashFromOperatingActivities",
    "netCashUsedForInvestingActivites": "totalCashflowsFromInvestingActivities",
    "netCashUsedProvidedByFinancingActivities": "totalCashFromFinancingActivities",
    "netIncome":                   "netIncome",
    "capitalExpenditure":          "capitalExpenditures",
    "acquisitionsNet":             "acquisitionsNet",
    "investmentsInPropertyPlantAndEquipment": "investments",
    "dividendsPaid":               "dividendsPaid",
    "commonStockRepurchased":      "repurchaseOfStock",
    "commonStockIssued":           "issuanceOfStock",
    "debtRepayment":               "debtRepaid",
    "depreciationAndAmortization": "depreciation",
    "stockBasedCompensation":      "stockBasedCompensation",
    "accountsReceivables":         "changeToAccountReceivables",
    "accountsPayables":            "changeToAccountsPayable",
    "inventory":                   "changeToInventory",
    "otherWorkingCapital":         "changeToOperatingActivities",
    "deferredIncomeTax":           "changeToDeferredTax",
    "freeCashFlow":                "freeCashFlow",
    "netChangeInCash":             "changeInCash",
}


def _fmp_to_float(v):
    if v is None or v == "" or v == "None":
        return None
    try:
        f = float(v)
        return f if f == f else None
    except Exception:
        return None


def _fmp_normalize(row, field_map):
    out = {}
    for fmp_k, std_k in field_map.items():
        val = _fmp_to_float(row.get(fmp_k))
        if val is None:
            continue
        out[std_k] = _raw(val)
    end = row.get("date") or row.get("fillingDate") or row.get("fiscalDateEnding")
    if end:
        # FMP sometimes includes full timestamp; keep the date part
        end = str(end)[:10]
        out["endDate"] = _date_stub(end)
    return out


def _fmp_statement(path, sym, period="annual", limit=12):
    """Fetch one FMP statement list (income-statement, balance-sheet-statement,
    cash-flow-statement). Returns list."""
    if not fmp_key():
        return []
    d = _fmp_call(f"{path}/{quote(sym)}", period=period, limit=limit)
    return d if isinstance(d, list) else []


def fmp_financials(sym):
    """Merge income/balance/cashflow statements from FMP into a SEC-shaped block.
    Returns {} if FMP key missing or no data."""
    if not fmp_key():
        return {}
    is_ann = _fmp_statement("income-statement", sym, "annual", 8)
    is_qtr = _fmp_statement("income-statement", sym, "quarter", 8)
    bs_ann = _fmp_statement("balance-sheet-statement", sym, "annual", 8)
    bs_qtr = _fmp_statement("balance-sheet-statement", sym, "quarter", 8)
    cf_ann = _fmp_statement("cash-flow-statement", sym, "annual", 8)
    cf_qtr = _fmp_statement("cash-flow-statement", sym, "quarter", 8)
    if not (is_ann or is_qtr or bs_ann or bs_qtr or cf_ann or cf_qtr):
        return {}

    def build(rows, mapping):
        out = []
        for r in rows[:8]:
            row = _fmp_normalize(r or {}, mapping)
            if row:
                out.append(row)
        return out

    return {
        "incomeStatementHistory":            {"incomeStatementHistory": build(is_ann, _FMP_IS_MAP)},
        "incomeStatementHistoryQuarterly":   {"incomeStatementHistory": build(is_qtr, _FMP_IS_MAP)},
        "balanceSheetHistory":               {"balanceSheetStatements": build(bs_ann, _FMP_BS_MAP)},
        "balanceSheetHistoryQuarterly":      {"balanceSheetStatements": build(bs_qtr, _FMP_BS_MAP)},
        "cashflowStatementHistory":          {"cashflowStatements": build(cf_ann, _FMP_CF_MAP)},
        "cashflowStatementHistoryQuarterly": {"cashflowStatements": build(cf_qtr, _FMP_CF_MAP)},
        "_meta_source_financials":             "FMP",
    }


# ---------------------------------------------------------------------------
# Finnhub   —   finnhub.io
# Free tier: 60 req/min. Real-time US quote, candles, news, recs, earnings.
# ---------------------------------------------------------------------------

def _fh_call(path, **params):
    """Low-level Finnhub call. Returns parsed JSON (dict or list) or None."""
    key = finnhub_key()
    if not key:
        return None
    params["token"] = key
    qs = "&".join(f"{k}={quote(str(v))}" for k, v in params.items() if v is not None)
    url = f"https://finnhub.io/api/v1/{path}?{qs}"
    code, body = _polite_fetch(url, host="finnhub.io", accept="application/json")
    if code != 200 or not body:
        if code not in (-1, 0):
            sys.stderr.write(f"[FH {path}] http={code} body={body[:200]!r}\n")
        return None
    try:
        d = json.loads(body)
    except Exception:
        sys.stderr.write(f"[FH {path}] non-JSON body={body[:200]!r}\n")
        return None
    if isinstance(d, dict) and d.get("error"):
        sys.stderr.write(f"[FH {path}] provider msg: {d.get('error')[:240]!r}\n")
        return None
    return d


def fh_quote(sym):
    """Finnhub /quote. Returns normalized quote dict or None."""
    d = _fh_call("quote", symbol=sym)
    if not d or not isinstance(d, dict):
        return None
    # Finnhub returns 0s when the symbol is unknown
    if not d.get("c"):
        return None

    def _f(v):
        try:
            f = float(v)
            return f if f == f else None
        except Exception:
            return None

    price  = _f(d.get("c"))  # current
    prev   = _f(d.get("pc")) # previous close
    chg    = _f(d.get("d"))  # change
    pct    = _f(d.get("dp")) # change percent
    return {
        "price":      price,
        "change":     chg,
        "changePct":  pct,
        "prevClose":  prev,
        "open":       _f(d.get("o")),
        "high":       _f(d.get("h")),
        "low":        _f(d.get("l")),
        "timestamp":  d.get("t"),
        "source":     "Finnhub",
    }


def fh_candle(sym, resolution="D", days=365):
    """Finnhub /stock/candle. resolution in {1,5,15,30,60,D,W,M}.
    Returns ascending bar list or []."""
    to_ts = int(time.time())
    from_ts = to_ts - int(days) * 86400
    d = _fh_call("stock/candle", symbol=sym, resolution=str(resolution),
                 **{"from": from_ts, "to": to_ts})
    if not d or not isinstance(d, dict) or d.get("s") != "ok":
        return []
    ts = d.get("t") or []
    o = d.get("o") or []
    h = d.get("h") or []
    l = d.get("l") or []
    c = d.get("c") or []
    v = d.get("v") or []
    n = min(len(ts), len(o), len(h), len(l), len(c))
    bars = []
    for i in range(n):
        try:
            bars.append({
                "ts":     int(ts[i]),
                "open":   float(o[i]),
                "high":   float(h[i]),
                "low":    float(l[i]),
                "close":  float(c[i]),
                "volume": float(v[i]) if i < len(v) and v[i] is not None else 0.0,
            })
        except Exception:
            continue
    bars.sort(key=lambda b: b["ts"])
    return bars


def fh_news(sym, days=14, limit=20):
    """Finnhub /company-news. Returns list of news dicts or []."""
    today = time.strftime("%Y-%m-%d", time.gmtime())
    start = time.strftime("%Y-%m-%d", time.gmtime(time.time() - int(days) * 86400))
    d = _fh_call("company-news", symbol=sym, **{"from": start, "to": today})
    if not d or not isinstance(d, list):
        return []
    out = []
    for n in d[: int(limit)]:
        if not isinstance(n, dict):
            continue
        pub = n.get("datetime")
        if isinstance(pub, (int, float)):
            pub = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(pub)))
        out.append({
            "title":       n.get("headline"),
            "url":         n.get("url"),
            "publishedAt": pub,
            "source":      n.get("source") or "Finnhub",
            "image":       n.get("image"),
            "summary":     n.get("summary"),
            "symbol":      sym,
            "category":    n.get("category"),
        })
    return out


def fh_recs(sym):
    """Finnhub /stock/recommendation. Returns latest recommendation row or None."""
    d = _fh_call("stock/recommendation", symbol=sym)
    if not d or not isinstance(d, list) or not d:
        return None
    # Rows come newest-first but be safe and sort
    try:
        d = sorted(d, key=lambda r: r.get("period", ""), reverse=True)
    except Exception:
        pass
    r = d[0] or {}
    if not isinstance(r, dict):
        return None
    return {
        "period":     r.get("period"),
        "strongBuy":  r.get("strongBuy"),
        "buy":        r.get("buy"),
        "hold":       r.get("hold"),
        "sell":       r.get("sell"),
        "strongSell": r.get("strongSell"),
        "source":     "Finnhub",
    }


def fh_earnings(sym, limit=4):
    """Finnhub /stock/earnings. Returns list of recent earnings or []."""
    d = _fh_call("stock/earnings", symbol=sym)
    if not d or not isinstance(d, list):
        return []
    out = []
    for row in d[: int(limit)]:
        if not isinstance(row, dict):
            continue
        out.append({
            "period":    row.get("period"),
            "actual":    row.get("actual"),
            "estimate":  row.get("estimate"),
            "surprise":  row.get("surprise"),
            "surprisePct": row.get("surprisePercent"),
            "quarter":   row.get("quarter"),
            "year":      row.get("year"),
            "source":    "Finnhub",
        })
    return out


def fh_profile(sym):
    """Finnhub /stock/profile2. Returns company profile dict or None."""
    d = _fh_call("stock/profile2", symbol=sym)
    if not d or not isinstance(d, dict) or not d.get("ticker"):
        return None
    return {
        "symbol":         d.get("ticker"),
        "name":           d.get("name"),
        "country":        d.get("country"),
        "currency":       d.get("currency"),
        "exchange":       d.get("exchange"),
        "ipo":            d.get("ipo"),
        "marketCap":      d.get("marketCapitalization"),
        "sharesOutstanding": d.get("shareOutstanding"),
        "industry":       d.get("finnhubIndustry"),
        "phone":          d.get("phone"),
        "weburl":         d.get("weburl"),
        "logo":           d.get("logo"),
        "source":         "Finnhub",
    }


# ---------------------------------------------------------------------------
# Twelve Data   —   twelvedata.com
# Free tier: 8 req/min, 800 req/day. Stocks, ETFs, FX, crypto, indices.
# ---------------------------------------------------------------------------

def _td_call(path, **params):
    """Low-level Twelve Data call. Returns parsed JSON dict/list or None."""
    key = td_key()
    if not key:
        return None
    params["apikey"] = key
    qs = "&".join(f"{k}={quote(str(v))}" for k, v in params.items() if v is not None)
    url = f"https://api.twelvedata.com/{path}?{qs}"
    code, body = _polite_fetch(url, host="api.twelvedata.com",
                                accept="application/json")
    if code != 200 or not body:
        if code not in (-1, 0):
            sys.stderr.write(f"[TD {path}] http={code} body={body[:200]!r}\n")
        return None
    try:
        d = json.loads(body)
    except Exception:
        sys.stderr.write(f"[TD {path}] non-JSON body={body[:200]!r}\n")
        return None
    # Twelve Data reports errors as {"code": ..., "status": "error", ...}
    if isinstance(d, dict) and d.get("status") == "error":
        sys.stderr.write(f"[TD {path}] provider msg: {str(d.get('message'))[:240]!r}\n")
        return None
    return d


# Map Yahoo-style range → TD (start_date, interval) best fit
_TD_RANGE_MAP = {
    "1d":   ("1day",   "5min",  "1"),
    "5d":   ("5day",   "30min", "5"),
    "1mo":  ("30day",  "1h",    "30"),
    "3mo":  ("90day",  "1day",  "90"),
    "6mo":  ("180day", "1day",  "180"),
    "1y":   ("365day", "1day",  "365"),
    "2y":   ("730day", "1day",  "730"),
    "5y":   ("1825day","1week", "1825"),
    "10y":  ("3650day","1week", "3650"),
    "max":  ("10000day","1month","5000"),
}


def td_quote(sym):
    """Twelve Data /quote. Returns normalized quote dict or None."""
    d = _td_call("quote", symbol=sym)
    if not d or not isinstance(d, dict):
        return None
    if d.get("close") in (None, ""):
        return None

    def _f(v):
        try:
            return float(v) if v not in (None, "") else None
        except Exception:
            return None

    price = _f(d.get("close"))
    prev  = _f(d.get("previous_close"))
    change = _f(d.get("change"))
    pct = _f(d.get("percent_change"))
    if change is None and price is not None and prev is not None:
        change = price - prev
    if pct is None and prev not in (None, 0) and change is not None:
        try:
            pct = (change / prev) * 100.0
        except Exception:
            pct = None
    return {
        "symbol":     d.get("symbol") or sym,
        "name":       d.get("name"),
        "price":      price,
        "change":     change,
        "changePct":  pct,
        "prevClose":  prev,
        "open":       _f(d.get("open")),
        "high":       _f(d.get("high")),
        "low":        _f(d.get("low")),
        "volume":     _f(d.get("volume")),
        "avgVolume":  _f(d.get("average_volume")),
        "currency":   d.get("currency"),
        "exchange":   d.get("exchange"),
        "fiftyTwoWeekHigh": _f((d.get("fifty_two_week") or {}).get("high")),
        "fiftyTwoWeekLow":  _f((d.get("fifty_two_week") or {}).get("low")),
        "source":     "TwelveData",
    }


def td_timeseries(sym, interval="1day", outputsize=365):
    """Twelve Data /time_series. interval matches TD formats (1min,5min,...,1day,1week,1month).
    Returns ascending bar list or []."""
    d = _td_call("time_series", symbol=sym, interval=interval,
                 outputsize=max(1, min(int(outputsize), 5000)))
    if not d or not isinstance(d, dict):
        return []
    values = d.get("values") or []
    bars = []
    for row in values:
        stamp = row.get("datetime") or ""
        ts = 0
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                ts = int(time.mktime(time.strptime(stamp, fmt)))
                break
            except Exception:
                continue
        if not ts:
            continue
        try:
            bars.append({
                "ts":     ts,
                "date":   stamp,
                "open":   float(row.get("open")),
                "high":   float(row.get("high")),
                "low":    float(row.get("low")),
                "close":  float(row.get("close")),
                "volume": float(row.get("volume") or 0),
            })
        except Exception:
            continue
    bars.sort(key=lambda b: b["ts"])
    return bars


def td_chart(sym, range_str="1y"):
    """Convenience wrapper: Yahoo-style range → TD time_series bars."""
    _days, interval, outsize = _TD_RANGE_MAP.get(range_str, ("365day", "1day", "365"))
    return td_timeseries(sym, interval=interval, outputsize=int(outsize))


def td_statistics(sym):
    """Twelve Data /statistics. Returns fundamentals snapshot or None."""
    d = _td_call("statistics", symbol=sym)
    if not d or not isinstance(d, dict):
        return None
    stats = d.get("statistics") or d
    val = stats.get("valuations_metrics") or {}
    fin = stats.get("financials") or {}
    dvd = stats.get("dividends_and_splits") or {}
    pri = stats.get("stock_price_summary") or {}

    def _f(v):
        try:
            return float(v) if v not in (None, "") else None
        except Exception:
            return None

    return {
        "marketCap":             _f(val.get("market_capitalization")),
        "enterpriseValue":       _f(val.get("enterprise_value")),
        "trailingPE":            _f(val.get("trailing_pe")),
        "forwardPE":             _f(val.get("forward_pe")),
        "pegRatio":              _f(val.get("peg_ratio")),
        "priceToSales":          _f(val.get("price_to_sales_ttm")),
        "priceToBook":           _f(val.get("price_to_book_mrq")),
        "evToRevenue":           _f(val.get("enterprise_to_revenue")),
        "evToEbitda":            _f(val.get("enterprise_to_ebitda")),
        "profitMargin":          _f((fin.get("margins") or {}).get("profit_margin")),
        "operatingMargin":       _f((fin.get("margins") or {}).get("operating_margin")),
        "returnOnAssets":        _f((fin.get("management_effectiveness") or {}).get("return_on_assets_ttm")),
        "returnOnEquity":        _f((fin.get("management_effectiveness") or {}).get("return_on_equity_ttm")),
        "revenue":               _f((fin.get("income_statement") or {}).get("revenue_ttm")),
        "grossProfit":           _f((fin.get("income_statement") or {}).get("gross_profit_ttm")),
        "ebitda":                _f((fin.get("income_statement") or {}).get("ebitda")),
        "dilutedEps":            _f((fin.get("income_statement") or {}).get("diluted_eps_ttm")),
        "dividendYield":         _f(dvd.get("forward_annual_dividend_yield")),
        "dividendPerShare":      _f(dvd.get("forward_annual_dividend_rate")),
        "payoutRatio":           _f(dvd.get("payout_ratio")),
        "fiftyTwoWeekHigh":      _f(pri.get("fifty_two_week_high")),
        "fiftyTwoWeekLow":       _f(pri.get("fifty_two_week_low")),
        "fiftyDayMA":            _f(pri.get("day_50_ma")),
        "twoHundredDayMA":       _f(pri.get("day_200_ma")),
        "beta":                  _f(stats.get("beta")),
        "sharesOutstanding":     _f((fin.get("stock_statistics") or {}).get("shares_outstanding")),
        "source":                "TwelveData",
    }


# ---------------------------------------------------------------------------
# No-key additions: Yahoo options chain, StockTwits sentiment, multi-source news
# ---------------------------------------------------------------------------

def yahoo_options(sym, expiry=None):
    """Yahoo v7 options chain. Returns normalized options snapshot or None.
    Passes expiry (unix seconds) to fetch a specific chain. Without expiry,
    Yahoo returns the nearest expiration plus the full list of expirationDates.
    """
    cache_key = f"yopt:{sym}:{expiry or ''}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    path = f"https://query2.finance.yahoo.com/v7/finance/options/{quote(sym)}"
    if expiry:
        path += f"?date={int(expiry)}"
    code, body = _polite_fetch(path, host="query2.finance.yahoo.com",
                                accept="application/json",
                                referer=f"https://finance.yahoo.com/quote/{quote(sym)}/options")
    if code != 200 or not body:
        # Try query1 fallback
        path2 = f"https://query1.finance.yahoo.com/v7/finance/options/{quote(sym)}"
        if expiry:
            path2 += f"?date={int(expiry)}"
        code, body = _polite_fetch(path2, host="query1.finance.yahoo.com",
                                    accept="application/json")
    if code != 200 or not body:
        return None
    try:
        d = json.loads(body)
    except Exception:
        return None
    root = (d or {}).get("optionChain") or {}
    result = (root.get("result") or [None])[0]
    if not result:
        return None

    quote_meta = result.get("quote") or {}
    exps = result.get("expirationDates") or []
    strikes = result.get("strikes") or []
    options = (result.get("options") or [None])[0] or {}

    def _norm(row):
        if not isinstance(row, dict):
            return None
        return {
            "contractSymbol":    row.get("contractSymbol"),
            "strike":            row.get("strike"),
            "currency":          row.get("currency"),
            "lastPrice":         row.get("lastPrice"),
            "change":            row.get("change"),
            "percentChange":     row.get("percentChange"),
            "volume":            row.get("volume"),
            "openInterest":      row.get("openInterest"),
            "bid":               row.get("bid"),
            "ask":               row.get("ask"),
            "impliedVolatility": row.get("impliedVolatility"),
            "inTheMoney":        row.get("inTheMoney"),
            "expiration":        row.get("expiration"),
            "lastTradeDate":     row.get("lastTradeDate"),
        }

    out = {
        "symbol":           result.get("underlyingSymbol") or sym,
        "expiration":       options.get("expirationDate"),
        "expirationDates":  exps,
        "strikes":          strikes,
        "hasMiniOptions":   result.get("hasMiniOptions"),
        "underlying": {
            "price":       quote_meta.get("regularMarketPrice"),
            "change":      quote_meta.get("regularMarketChange"),
            "changePct":   quote_meta.get("regularMarketChangePercent"),
            "name":        quote_meta.get("shortName") or quote_meta.get("longName"),
            "currency":    quote_meta.get("currency"),
        },
        "calls": [c for c in (_norm(r) for r in (options.get("calls") or [])) if c],
        "puts":  [p for p in (_norm(r) for r in (options.get("puts")  or [])) if p],
        "source":           "Yahoo",
    }
    _cache_put(cache_key, out, ttl=60)
    return out


# ---------------------------------------------------------------------------
# StockTwits — community sentiment (Bullish / Bearish) + recent posts.
# Public endpoint: api.stocktwits.com/api/2/streams/symbol/{sym}.json
# No auth required for the basic stream.
# ---------------------------------------------------------------------------

def stocktwits_stream(sym, limit=20):
    """Fetch recent StockTwits stream for a symbol. Returns normalized dict or None."""
    cache_key = f"st:stream:{sym.upper()}:{limit}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    url = f"https://api.stocktwits.com/api/2/streams/symbol/{quote(sym)}.json"
    code, body = _polite_fetch(url, host="api.stocktwits.com",
                                accept="application/json",
                                referer=f"https://stocktwits.com/symbol/{quote(sym)}")
    if code != 200 or not body:
        return None
    try:
        d = json.loads(body)
    except Exception:
        return None
    if not isinstance(d, dict) or (d.get("response") or {}).get("status") != 200:
        return None

    msgs = d.get("messages") or []
    bull = bear = 0
    posts = []
    for m in msgs[: int(limit)]:
        if not isinstance(m, dict):
            continue
        sent = ((m.get("entities") or {}).get("sentiment") or {}).get("basic")
        if sent == "Bullish":
            bull += 1
        elif sent == "Bearish":
            bear += 1
        user = (m.get("user") or {})
        posts.append({
            "id":           m.get("id"),
            "body":         (m.get("body") or "").strip(),
            "createdAt":    m.get("created_at"),
            "sentiment":    sent,
            "user":         user.get("username"),
            "userName":     user.get("name"),
            "userFollowers": user.get("followers"),
            "likes":        (m.get("likes") or {}).get("total"),
            "url":          m.get("reshare_message", {}).get("message", {}).get("id")
                              and f"https://stocktwits.com/message/{m.get('id')}"
                              or f"https://stocktwits.com/message/{m.get('id')}",
        })

    total = bull + bear
    bull_pct = round(100.0 * bull / total, 1) if total else None
    bear_pct = round(100.0 * bear / total, 1) if total else None
    sym_info = d.get("symbol") or {}

    out = {
        "symbol":          sym_info.get("symbol") or sym,
        "name":            sym_info.get("title"),
        "watchlistCount":  sym_info.get("watchlist_count"),
        "messageCount":    len(msgs),
        "bullish":         bull,
        "bearish":         bear,
        "bullishPct":      bull_pct,
        "bearishPct":      bear_pct,
        "posts":           posts,
        "source":          "StockTwits",
    }
    _cache_put(cache_key, out, ttl=60)
    return out


def _bars_to_arrays(bars):
    """Common helper: convert a list of {ts,open,high,low,close,volume} dicts to parallel arrays."""
    timestamps = [b.get("ts") or 0 for b in bars]
    opens      = [b.get("open")   for b in bars]
    highs      = [b.get("high")   for b in bars]
    lows       = [b.get("low")    for b in bars]
    closes     = [b.get("close")  for b in bars]
    vols       = [b.get("volume") for b in bars]
    return timestamps, opens, highs, lows, closes, vols


def nasdaq_historical(sym, days=365):
    """Nasdaq's public JSON historical-price endpoint. Free, no key, daily only.
    Returns ascending bar list [{ts,date,open,high,low,close,volume}] or []."""
    import datetime as _dt
    to_d = _dt.date.today()
    from_d = to_d - _dt.timedelta(days=int(days))
    # Nasdaq caps limit around 500 rows per request; use a margin.
    limit = min(max(int(days), 30), 500)
    url = (f"https://api.nasdaq.com/api/quote/{quote(sym)}/historical"
           f"?assetclass=stocks&fromdate={from_d.isoformat()}"
           f"&todate={to_d.isoformat()}&limit={limit}&interval=d&lastsalesort=true")
    data = _nasdaq_fetch(url)
    rows = (((data or {}).get("data") or {}).get("tradesTable") or {}).get("rows") or []
    bars = []
    for r in rows:
        d_s = r.get("date")
        try:
            ts = int(time.mktime(time.strptime(d_s, "%m/%d/%Y")))
        except Exception:
            continue
        o = _parse_money(r.get("open"))
        h = _parse_money(r.get("high"))
        l = _parse_money(r.get("low"))
        c = _parse_money(r.get("close"))
        v = _parse_int  (r.get("volume"))
        if c is None:
            continue
        bars.append({"ts": ts,
                      "date": _dt.date.fromtimestamp(ts).isoformat(),
                      "open": o, "high": h, "low": l, "close": c, "volume": v or 0})
    bars.sort(key=lambda b: b["ts"])
    return bars


def stockanalysis_history(sym, days=365):
    """stockanalysis.com public chart JSON. Free, no key.
    Returns ascending [{ts,date,open,high,low,close,volume}] or []."""
    import datetime as _dt, json as _json
    s = str(sym).lower()
    # Pick range bucket from days (that's what their frontend does)
    rng = "1Y"
    if   days <= 7:    rng = "5D"
    elif days <= 31:   rng = "1M"
    elif days <= 93:   rng = "3M"
    elif days <= 186:  rng = "6M"
    elif days <= 370:  rng = "1Y"
    elif days <= 1100: rng = "3Y"
    elif days <= 1900: rng = "5Y"
    elif days <= 3800: rng = "10Y"
    else:              rng = "ALL"
    # Their page ships this JSON endpoint; falls back gracefully if schema shifts.
    urls = [
        f"https://stockanalysis.com/api/symbol/s/{s}/history?type=chart&range={rng}&period=Daily",
        f"https://stockanalysis.com/api/symbol/s/{s}/history?range={rng}&period=Daily",
    ]
    # International: try the /q/<exch>/<num>/ path too
    finfo = _foreign_info(sym)
    if finfo and finfo.get("slug"):
        slug = finfo["slug"]  # e.g. "tyo/3350"
        urls = [
            f"https://stockanalysis.com/api/symbol/q/{slug}/history?type=chart&range={rng}&period=Daily",
            f"https://stockanalysis.com/api/symbol/q/{slug}/history?range={rng}&period=Daily",
        ] + urls
    data = None
    for url in urls:
        try:
            code, body = _polite_fetch(url, host="stockanalysis.com",
                                        accept="application/json",
                                        referer=f"https://stockanalysis.com/stocks/{s}/chart/")
            if code != 200 or not body:
                continue
            try:
                data = _json.loads(body)
            except Exception:
                data = None
            if data:
                break
        except Exception:
            continue
    if not data:
        return []
    # The payload shape varies; handle the two known forms.
    rows = None
    if isinstance(data, dict):
        for k in ("data", "payload", "historical", "result"):
            v = data.get(k)
            if isinstance(v, list) and v:
                rows = v; break
            if isinstance(v, dict):
                for kk in ("data", "rows", "series", "chart"):
                    vv = v.get(kk)
                    if isinstance(vv, list) and vv:
                        rows = vv; break
                if rows: break
    elif isinstance(data, list):
        rows = data
    if not rows:
        return []
    bars = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        d_s = r.get("t") or r.get("date") or r.get("d") or r.get("Date")
        if not d_s:
            continue
        ts = None
        # Accept several date formats
        for fmt_s in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                ts = int(time.mktime(time.strptime(str(d_s)[:19], fmt_s)))
                break
            except Exception:
                pass
        if ts is None:
            # Maybe epoch seconds already
            try: ts = int(d_s)
            except Exception: continue
        def _f(*ks):
            for k in ks:
                v = r.get(k)
                if v is None: continue
                try: return float(str(v).replace(",", ""))
                except Exception: continue
            return None
        o = _f("open","o","Open")
        h = _f("high","h","High")
        l = _f("low","l","Low")
        c = _f("close","c","Close","adjClose","adj_close")
        v = _f("volume","v","Volume")
        if c is None: continue
        bars.append({"ts": ts,
                      "date": time.strftime("%Y-%m-%d", time.localtime(ts)),
                      "open": o, "high": h, "low": l, "close": c,
                      "volume": int(v) if v is not None else 0})
    bars.sort(key=lambda b: b["ts"])
    return bars


def cboe_history(sym, days=365):
    """Cboe public delayed-quotes chart JSON. Free, no key, daily only.
    Returns ascending bars or []."""
    import datetime as _dt, json as _json
    s = str(sym).upper()
    url = f"https://cdn.cboe.com/api/global/delayed_quotes/charts/historical/{quote(s)}.json"
    try:
        code, body = _polite_fetch(url, host="cboe.com",
                                    accept="application/json",
                                    referer="https://www.cboe.com/")
        if code != 200 or not body:
            return []
        data = _json.loads(body)
    except Exception:
        return []
    # The endpoint returns {"data": {"symbol": ..., "data": [{"date":"YYYY-MM-DD","open":..,"high":..,"low":..,"close":..,"volume":..}]}}
    rows = []
    if isinstance(data, dict):
        blk = data.get("data") or {}
        if isinstance(blk, dict):
            rows = blk.get("data") or blk.get("history") or []
        elif isinstance(blk, list):
            rows = blk
    bars = []
    for r in (rows or []):
        if not isinstance(r, dict): continue
        d_s = r.get("date") or r.get("d")
        try:
            ts = int(time.mktime(time.strptime(str(d_s), "%Y-%m-%d")))
        except Exception:
            continue
        def _f(k):
            v = r.get(k)
            if v is None: return None
            try: return float(v)
            except Exception: return None
        o,h,l,c,v = _f("open"), _f("high"), _f("low"), _f("close"), _f("volume")
        if c is None: continue
        bars.append({"ts": ts, "date": str(d_s),
                      "open": o, "high": h, "low": l, "close": c,
                      "volume": int(v) if v is not None else 0})
    # Trim to requested window
    if days and bars:
        cutoff = int(time.time()) - int(days) * 86400
        bars = [b for b in bars if b["ts"] >= cutoff]
    bars.sort(key=lambda b: b["ts"])
    return bars


def shape_chart(sym, range_str="1y", interval="1d"):
    """Build a Yahoo-shaped chart payload for one symbol.

    Source rotation is FREE / NO-KEY sources only, so the keyed providers
    keep their daily quota for dedicated panel endpoints:
        Yahoo v8 → Stooq → Nasdaq historical → Stockanalysis → Cboe → CoinGecko (crypto)
    Any source already in cooldown is silently skipped.
    """
    range_to_days = {"1d": 2, "5d": 7, "1mo": 30, "3mo": 90, "6mo": 180,
                     "1y": 365, "2y": 730, "5y": 1825, "10y": 3650, "max": 10000}
    days = range_to_days.get(range_str, 365)

    timestamps = opens = highs = lows = closes = vols = None
    exchange = None
    price = None

    if is_crypto(sym):
        cgid = to_coingecko(sym)
        bars = cg_ohlc(cgid, days)
        if bars:
            timestamps = [b["ts"] for b in bars]
            opens  = [b["open"]  for b in bars]
            highs  = [b["high"]  for b in bars]
            lows   = [b["low"]   for b in bars]
            closes = [b["close"] for b in bars]
            vols   = [None] * len(bars)
            p = cg_price(cgid) or {}
            price = p.get("price") if p.get("price") is not None else (closes[-1] if closes else None)
            exchange = "CoinGecko"

    # Try Yahoo v8 first for stocks/indices/fx (also great fallback for crypto)
    if closes is None:
        yhb = yahoo_chart_api(sym, range_str=range_str, interval=interval)
        if yhb:
            timestamps, opens, highs, lows, closes, vols = _bars_to_arrays(yhb)
            price = closes[-1] if closes else None
            exchange = "Yahoo Finance"

    # Fallback: Stooq daily CSV (only serves daily/weekly/monthly, not intraday)
    if closes is None:
        bars = stooq_history(sym, days=days, interval="d")
        if bars:
            timestamps = []
            for b in bars:
                try:
                    timestamps.append(int(time.mktime(time.strptime(b["date"], "%Y-%m-%d"))))
                except Exception:
                    timestamps.append(0)
            opens  = [b["open"]   for b in bars]
            highs  = [b["high"]   for b in bars]
            lows   = [b["low"]    for b in bars]
            closes = [b["close"]  for b in bars]
            vols   = [b["volume"] for b in bars]
            price = closes[-1] if closes else None
            exchange = "Stooq"

    # Fallback: Nasdaq public historical JSON (no key)
    if closes is None and not is_crypto(sym) and not sym.endswith("=X") and not sym.startswith("^"):
        bars = nasdaq_historical(sym, days=days)
        if bars:
            timestamps, opens, highs, lows, closes, vols = _bars_to_arrays(bars)
            price = closes[-1] if closes else None
            exchange = "Nasdaq"

    # Fallback: stockanalysis.com public chart JSON
    if closes is None and not is_crypto(sym) and not sym.endswith("=X") and not sym.startswith("^"):
        try:
            bars = stockanalysis_history(sym, days=days)
        except Exception:
            bars = []
        if bars:
            timestamps, opens, highs, lows, closes, vols = _bars_to_arrays(bars)
            price = closes[-1] if closes else None
            exchange = "Stockanalysis"

    # Fallback: Cboe public historical JSON
    if closes is None and not is_crypto(sym) and not sym.endswith("=X"):
        try:
            bars = cboe_history(sym, days=days)
        except Exception:
            bars = []
        if bars:
            timestamps, opens, highs, lows, closes, vols = _bars_to_arrays(bars)
            price = closes[-1] if closes else None
            exchange = "Cboe"

    if not closes:
        return {"chart": {"result": [],
                          "error": {"code": "Not Found", "description": "no data"}}}

    prev_close = closes[-2] if len(closes) >= 2 else (closes[0] if closes else None)
    yr_hi = max((h for h in highs if h is not None), default=None)
    yr_lo = min((l for l in lows  if l is not None), default=None)

    return {
        "chart": {
            "result": [{
                "meta": {
                    "symbol": sym,
                    "currency": "USD",
                    "exchangeName": exchange,
                    "fullExchangeName": exchange,
                    "regularMarketPrice": price,
                    "previousClose": prev_close,
                    "chartPreviousClose": prev_close,
                    "regularMarketDayHigh": highs[-1] if highs else None,
                    "regularMarketDayLow":  lows[-1]  if lows  else None,
                    "regularMarketVolume":  vols[-1]  if vols  else None,
                    "fiftyTwoWeekHigh": yr_hi,
                    "fiftyTwoWeekLow":  yr_lo,
                    "marketState": "REGULAR",
                },
                "timestamp": timestamps,
                "indicators": {
                    "quote": [{
                        "open":   opens,
                        "high":   highs,
                        "low":    lows,
                        "close":  closes,
                        "volume": vols,
                    }],
                    "adjclose": [{"adjclose": closes}],
                },
            }],
            "error": None,
        }
    }


def shape_summary(sym):
    """Build a Yahoo-shaped quoteSummary payload."""
    q = shape_quote(sym)
    if not q:
        return {"quoteSummary": {"result": None,
                                 "error": {"code": "Not Found", "description": "no data"}}}

    def raw(v):
        if v is None:
            return None
        try:
            return {"raw": v, "fmt": (f"{v:,.2f}" if isinstance(v, (int, float)) else str(v))}
        except Exception:
            return {"raw": v, "fmt": str(v)}

    shape = {
        "quoteSummary": {
            "result": [{
                "price": {
                    "symbol":           q["symbol"],
                    "shortName":        q.get("shortName"),
                    "longName":         q.get("longName"),
                    "currency":         q.get("currency"),
                    "exchangeName":     q.get("fullExchangeName"),
                    "marketState":      q.get("marketState"),
                    "regularMarketPrice":          raw(q.get("regularMarketPrice")),
                    "regularMarketChange":         raw(q.get("regularMarketChange")),
                    "regularMarketChangePercent":  raw(q.get("regularMarketChangePercent")),
                    "regularMarketPreviousClose":  raw(q.get("regularMarketPreviousClose")),
                    "regularMarketOpen":           raw(q.get("regularMarketOpen")),
                    "regularMarketDayHigh":        raw(q.get("regularMarketDayHigh")),
                    "regularMarketDayLow":         raw(q.get("regularMarketDayLow")),
                    "regularMarketVolume":         raw(q.get("regularMarketVolume")),
                    "marketCap":                   raw(q.get("marketCap")),
                },
                "summaryDetail": {
                    "previousClose":   raw(q.get("regularMarketPreviousClose")),
                    "open":            raw(q.get("regularMarketOpen")),
                    "dayLow":          raw(q.get("regularMarketDayLow")),
                    "dayHigh":         raw(q.get("regularMarketDayHigh")),
                    "volume":          raw(q.get("regularMarketVolume")),
                    "fiftyTwoWeekLow": raw(q.get("fiftyTwoWeekLow")),
                    "fiftyTwoWeekHigh":raw(q.get("fiftyTwoWeekHigh")),
                    "currency":        q.get("currency"),
                    "marketCap":       raw(q.get("marketCap")),
                },
                "quoteType": {
                    "symbol":    q["symbol"],
                    "shortName": q.get("shortName"),
                    "longName":  q.get("longName"),
                    "exchange":  q.get("fullExchangeName"),
                },
                "_meta_source": q.get("fullExchangeName"),
            }],
            "error": None,
        }
    }
    # ---------- Parallel SEC + Nasdaq merge ----------
    # All independent fetches fan out at once; total wall-time = slowest fetch, not sum.
    # Heavy HTML scrapers (Finviz, stockanalysis.com, Yahoo HTML) are NOT called here;
    # they're available via their own endpoints and used only as on-demand fallbacks.
    try:
        if not is_crypto(sym) and not sym.startswith("^") and not sym.endswith("=X"):
            block = shape["quoteSummary"]["result"][0]
            price = q.get("regularMarketPrice")

            # Fan out all independent fetches (typical wall-time: ~1 slow HTTP).
            r = _parallel({
                "fin":        lambda: enriched_financials(sym),
                "prof":       lambda: sec_build_profile(sym),
                "fprof":      lambda: foreign_profile(sym),
                "stats":      (lambda: sec_key_stats(sym, price)) if price else (lambda: {}),
                "ystats":     lambda: stooq_year_stats(sym),
                "txns":       lambda: sec_insider_transactions(sym, limit=12),
                "peers":      lambda: sec_peers(sym, limit=15),
                "divs_sec":   lambda: sec_dividend_history(sym, limit=20),
                "ne":         lambda: nasdaq_next_earnings(sym),
                "tp":         lambda: nasdaq_price_targets(sym),
                "ar":         lambda: nasdaq_ratings(sym),
                "es":         lambda: nasdaq_earnings_surprise(sym),
                "ef":         lambda: nasdaq_earnings_forecast(sym),
                "ih":         lambda: nasdaq_institutional_holdings(sym, limit=15),
                "divs_nsd":   lambda: nasdaq_dividend_history(sym, limit=30),
            }, timeout=12)

            # ----- Merge results synchronously -----
            if r["fin"]:
                block.update(r["fin"])
            if r["prof"]:
                block.update(r["prof"])
            # Merge the hardcoded foreign profile stub: fills empty fields left
            # by SEC (e.g. MTPLF/3350.T has no SEC record). Does NOT overwrite
            # any field that SEC already populated.
            if r.get("fprof"):
                fap = (r["fprof"].get("assetProfile") or {})
                bap = block.setdefault("assetProfile", {})
                for k, v in fap.items():
                    if v is None:
                        continue
                    if bap.get(k) in (None, "", [], {}):
                        bap[k] = v
            if r["stats"]:
                extra = r["stats"].pop("summaryDetail_extra", {})
                block.update(r["stats"])
                block.setdefault("summaryDetail", {}).update(
                    {k: v for k, v in extra.items() if v is not None})

            if r["ystats"]:
                sd = block.setdefault("summaryDetail", {})
                for k in ("fiftyTwoWeekHigh", "fiftyTwoWeekLow",
                          "fiftyTwoWeekHighChangePercent", "fiftyTwoWeekLowChangePercent",
                          "fiftyDayAverage", "twoHundredDayAverage",
                          "averageVolume", "averageVolume10days", "averageDailyVolume3Month"):
                    if r["ystats"].get(k) is not None:
                        sd[k] = _raw(r["ystats"][k])
                if r["ystats"].get("52WeekChange") is not None:
                    block.setdefault("defaultKeyStatistics", {})["52WeekChange"] = _raw(
                        r["ystats"]["52WeekChange"])

            if r["txns"]:
                for k, v in sec_insider_summary(r["txns"]).items():
                    block[k] = v

            if r["peers"]:
                block["recommendedSymbols"] = {"recommendedSymbols": [
                    {"symbol": p["symbol"], "name": p["name"], "score": None}
                    for p in r["peers"]
                ]}

            ne = r["ne"] or {}
            if ne:
                cal = block.setdefault("calendarEvents", {}).setdefault("earnings", {})
                if ne.get("nextEarningsDate"):
                    cal["earningsDate"] = [_date_stub(ne["nextEarningsDate"])]
                if ne.get("epsForecastConsensus") is not None:
                    cal["earningsAverage"] = _raw(ne["epsForecastConsensus"])
                if ne.get("epsForecastAnnual") is not None:
                    block.setdefault("defaultKeyStatistics", {})["forwardEps"] = _raw(
                        ne["epsForecastAnnual"])
                if ne.get("lastQuarterlyEPS") is not None:
                    cal["earningsActual"] = _raw(ne["lastQuarterlyEPS"])

            tp = r["tp"] or {}
            if tp:
                fd = block.setdefault("financialData", {})
                for key in ("targetMeanPrice", "targetHighPrice",
                            "targetLowPrice", "targetMedianPrice", "numberOfAnalysts"):
                    if tp.get(key) is not None:
                        fd[key] = _raw(tp[key])
                if tp.get("numberOfAnalysts") is not None:
                    fd["numberOfAnalystOpinions"] = _raw(tp["numberOfAnalysts"])

            ar = r["ar"] or {}
            if ar:
                cur = ar.get("current") or {}
                fd = block.setdefault("financialData", {})
                if cur.get("mean") is not None:
                    fd["recommendationMean"] = _raw(cur["mean"])
                if cur.get("key"):
                    fd["recommendationKey"] = cur["key"]
                block["recommendationTrend"] = {"trend": [{
                    "period":     row.get("period"),
                    "strongBuy":  _raw(row.get("strongBuy")),
                    "buy":        _raw(row.get("buy")),
                    "hold":       _raw(row.get("hold")),
                    "sell":       _raw(row.get("sell")),
                    "strongSell": _raw(row.get("strongSell")),
                } for row in (ar.get("trend") or [])]}
                block.setdefault("recommendationSummary", {}).update({
                    "strongBuy":  _raw(cur.get("strongBuy")),
                    "buy":        _raw(cur.get("buy")),
                    "hold":       _raw(cur.get("hold")),
                    "sell":       _raw(cur.get("sell")),
                    "strongSell": _raw(cur.get("strongSell")),
                    "total":      _raw(cur.get("total")),
                })

            if r["es"]:
                block.setdefault("earningsHistory", {})["history"] = [{
                    "quarter":         _date_stub(row.get("period")),
                    "period":          row.get("period"),
                    "dateReported":    _date_stub(row.get("dateReported")),
                    "epsEstimate":     _raw(row.get("epsEstimate")),
                    "epsActual":       _raw(row.get("epsActual")),
                    "epsSurprise":     _raw(row.get("surprise")),
                    "surprisePercent": _raw(row.get("surprise")),
                } for row in r["es"]]

            ef = r["ef"] or {}
            if ef and (ef.get("eps") or ef.get("revenue")):
                eps_rows = ef.get("eps") or []
                rev_rows = ef.get("revenue") or []
                trend = []
                for i in range(max(len(eps_rows), len(rev_rows))):
                    er = eps_rows[i] if i < len(eps_rows) else {}
                    rr = rev_rows[i] if i < len(rev_rows) else {}
                    trend.append({
                        "period":   er.get("period") or rr.get("period"),
                        "endDate":  _date_stub(er.get("endDate") or rr.get("endDate")),
                        "growth":   _raw(er.get("growth")),
                        "earningsEstimate": {
                            "avg": _raw(er.get("epsAvg")), "low": _raw(er.get("epsLow")),
                            "high": _raw(er.get("epsHigh")),
                            "numberOfAnalysts": _raw(er.get("numAnalysts")),
                            "growth": _raw(er.get("growth")),
                        },
                        "revenueEstimate": {
                            "avg": _raw(rr.get("revAvg") or er.get("revenueAvg")),
                            "low": _raw(rr.get("revLow")), "high": _raw(rr.get("revHigh")),
                            "numberOfAnalysts": _raw(rr.get("numAnalysts")),
                        },
                    })
                block["earningsTrend"] = {"trend": trend}

            ih = r["ih"] or {}
            if ih and (ih.get("top") or ih.get("ownershipSummary")):
                top_rows = ih.get("top") or []
                block["institutionOwnership"] = {"ownershipList": [{
                    "organization": row.get("organization"),
                    "reportDate":   _date_stub(row.get("dateReported")),
                    "pctHeld":      _raw(row.get("percentHeld")),
                    "position":     _raw(row.get("sharesHeld")),
                    "value":        _raw(row.get("value")),
                    "pctChange":    _raw(row.get("sharesChange")),
                } for row in top_rows]}
                summary = ih.get("ownershipSummary") or {}
                if (summary.get("sharesOutstandingPCT") is not None
                        or summary.get("institutionalHoldersTotal") is not None):
                    pct = summary.get("sharesOutstandingPCT")
                    institutions_pct = (pct / 100.0) if (pct and pct > 1) else pct
                    block["majorHoldersBreakdown"] = {
                        "insidersPercentHeld":         None,
                        "institutionsPercentHeld":     _raw(institutions_pct),
                        "institutionsFloatPercentHeld": _raw(institutions_pct),
                        "institutionsCount":           _raw(summary.get("institutionalHoldersTotal")),
                    }

            # Dividends: prefer Nasdaq (has ex/pay/record), fall back to SEC XBRL.
            dividends = r["divs_nsd"] or r["divs_sec"]
            if dividends:
                block["cashflowEvents"] = {"dividends": dividends}

            # Options / upgrade-downgrade / fund ownership: not in fast path.
            # Frontend can call dedicated endpoints if the user drills in.
            block.setdefault("optionChain", {"result": [], "_note": "See /api/options/*"})
            block.setdefault("upgradeDowngradeHistory", {"history": [],
                "_note": "Fetch via /api/sa/ratings/{sym} on demand."})
            block.setdefault("fundOwnership", {"ownershipList": [],
                "_note": "Fetch via /api/yh-html/{sym} on demand."})
    except Exception as e:
        sys.stderr.write(f"[sec-merge {sym}] {e}\n")
    return shape


# ---------- SEC EDGAR: financial statements from XBRL company-facts ----------
_sec_tickers_lock = threading.Lock()
_sec_tickers_map: dict | None = None  # upper-ticker -> (cik_padded, title)


def _sec_fetch(url, timeout=30):
    """Fetch JSON from SEC with the required UA. Throttled + 429-aware via _polite_fetch."""
    code, body = _polite_fetch(url, accept="application/json",
                                timeout=timeout, ua=SEC_UA)
    if code == -1 or code != 200 or not body:
        if code not in (200, -1):
            sys.stderr.write(f"[sec] {url} -> code={code}\n")
        return None
    try:
        return json.loads(body)
    except Exception as e:
        sys.stderr.write(f"[sec parse] {url} -> {e}\n")
        return None


def sec_get_cik(ticker):
    """Map ticker -> (10-digit CIK, company name). Uses cached tickers.json."""
    global _sec_tickers_map
    tick = (ticker or "").upper().strip()
    if not tick:
        return None
    with _sec_tickers_lock:
        if _sec_tickers_map is None:
            data = _sec_fetch("https://www.sec.gov/files/company_tickers.json")
            if not data:
                return None
            m = {}
            # Format: {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}, ...}
            for v in data.values():
                t = (v.get("ticker") or "").upper()
                c = v.get("cik_str")
                if t and c is not None:
                    m[t] = (str(c).zfill(10), v.get("title") or t)
            _sec_tickers_map = m
        return _sec_tickers_map.get(tick)


def sec_company_facts(cik):
    """Fetch all XBRL facts for a company. Cached 1 hour."""
    cache_key = f"sec_facts::{cik}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    data = _sec_fetch(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json")
    # Company XBRL facts only update once a quarter — cache 6h
    _cache_put(cache_key, data if data else {}, ttl=21600)
    return data


def _extract_concept(facts, concept_names, form_types=("10-K", "10-K/A"), top_n=5):
    """Pull the most recent values of a concept (trying multiple XBRL names).
    Returns list of {end, val, form, fy, fp} dicts, newest-first, deduped by end date."""
    us_gaap = (facts or {}).get("facts", {}).get("us-gaap", {})
    for name in concept_names:
        c = us_gaap.get(name)
        if not c:
            continue
        units = c.get("units", {})
        for unit_key in ("USD", "USD/shares", "shares", "pure"):
            if unit_key not in units:
                continue
            vals = units[unit_key]
            relevant = [v for v in vals if v.get("form") in form_types]
            relevant.sort(key=lambda v: v.get("end", ""), reverse=True)
            seen = set()
            out = []
            for v in relevant:
                end = v.get("end")
                if not end or end in seen:
                    continue
                seen.add(end)
                out.append({"end": end, "val": v.get("val"), "form": v.get("form"),
                            "fy": v.get("fy"), "fp": v.get("fp")})
                if len(out) >= top_n:
                    break
            if out:
                return out
    return []


def _raw(v):
    """Yahoo-style {raw,fmt} wrapper; returns None when value missing."""
    if v is None:
        return None
    try:
        if isinstance(v, (int, float)):
            av = abs(v)
            if av >= 1e12: fmt = f"{v/1e12:.2f}T"
            elif av >= 1e9:  fmt = f"{v/1e9:.2f}B"
            elif av >= 1e6:  fmt = f"{v/1e6:.2f}M"
            elif av >= 1e3:  fmt = f"{v:,.2f}"
            else:             fmt = f"{v:.2f}"
        else:
            fmt = str(v)
        return {"raw": v, "fmt": fmt}
    except Exception:
        return {"raw": v, "fmt": str(v)}


def _date_stub(end_date):
    """Convert '2024-09-30' -> {raw: epoch, fmt: 'YYYY-MM-DD'} for Yahoo-style consumers."""
    if not end_date:
        return None
    try:
        ts = int(time.mktime(time.strptime(end_date, "%Y-%m-%d")))
        return {"raw": ts, "fmt": end_date}
    except Exception:
        return {"raw": 0, "fmt": end_date}


def sec_build_financials(ticker):
    """Build Yahoo-shaped income/balance/cashflow/earnings modules for a ticker.
    Returns dict that can be merged into quoteSummary result[0]."""
    info = sec_get_cik(ticker)
    if not info:
        return {}
    cik, _name = info
    facts = sec_company_facts(cik)
    if not facts:
        return {}

    def _pull(names, forms):
        return _extract_concept(facts, names, form_types=forms, top_n=12)

    # --- Income statement concepts (annual = 10-K, quarterly = 10-Q) ---
    def income_rows(forms):
        revenue = _pull(["Revenues",
                         "RevenueFromContractWithCustomerExcludingAssessedTax",
                         "RevenueFromContractWithCustomerIncludingAssessedTax",
                         "SalesRevenueNet", "SalesRevenueGoodsNet"], forms)
        cor     = _pull(["CostOfRevenue", "CostOfGoodsAndServicesSold",
                         "CostOfGoodsSold", "CostOfServices"], forms)
        gross   = _pull(["GrossProfit"], forms)
        op_inc  = _pull(["OperatingIncomeLoss"], forms)
        op_exp  = _pull(["OperatingExpenses", "CostsAndExpenses"], forms)
        net_inc = _pull(["NetIncomeLoss",
                         "ProfitLoss",
                         "NetIncomeLossAvailableToCommonStockholdersBasic"], forms)
        net_cont= _pull(["IncomeLossFromContinuingOperations"], forms)
        net_disc= _pull(["IncomeLossFromDiscontinuedOperationsNetOfTax"], forms)
        rd      = _pull(["ResearchAndDevelopmentExpense"], forms)
        sga     = _pull(["SellingGeneralAndAdministrativeExpense"], forms)
        sm      = _pull(["SellingAndMarketingExpense", "MarketingExpense"], forms)
        ga      = _pull(["GeneralAndAdministrativeExpense"], forms)
        eps_b   = _pull(["EarningsPerShareBasic"], forms)
        eps_d   = _pull(["EarningsPerShareDiluted"], forms)
        sh_b    = _pull(["WeightedAverageNumberOfSharesOutstandingBasic"], forms)
        sh_d    = _pull(["WeightedAverageNumberOfDilutedSharesOutstanding"], forms)
        tax     = _pull(["IncomeTaxExpenseBenefit"], forms)
        pretax  = _pull(["IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
                         "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments"], forms)
        int_exp = _pull(["InterestExpense", "InterestExpenseDebt"], forms)
        int_inc = _pull(["InvestmentIncomeInterest", "InterestIncomeOperating"], forms)
        oth_inc = _pull(["NonoperatingIncomeExpense", "OtherNonoperatingIncomeExpense"], forms)
        depr_is = _pull(["DepreciationDepletionAndAmortization",
                         "DepreciationAndAmortization", "Depreciation"], forms)
        sbc     = _pull(["ShareBasedCompensation",
                         "AllocatedShareBasedCompensationExpense"], forms)
        periods = {}
        def add(field, items):
            for it in items:
                end = it["end"]
                periods.setdefault(end, {"endDate": _date_stub(end)})
                periods[end][field] = _raw(it["val"])
        add("totalRevenue", revenue); add("costOfRevenue", cor)
        add("grossProfit", gross); add("operatingIncome", op_inc)
        add("totalOperatingExpenses", op_exp)
        add("netIncome", net_inc)
        add("netIncomeFromContinuingOps", net_cont)
        add("discontinuedOperations", net_disc)
        add("researchDevelopment", rd)
        add("sellingGeneralAdministrative", sga)
        add("sellingAndMarketingExpense", sm)
        add("generalAndAdministrative", ga)
        add("incomeTaxExpense", tax); add("incomeBeforeTax", pretax)
        add("interestExpense", int_exp); add("interestIncome", int_inc)
        add("totalOtherIncomeExpenseNet", oth_inc)
        add("depreciationAndAmortization", depr_is)
        add("stockBasedCompensation", sbc)
        add("epsBasic", eps_b); add("epsDiluted", eps_d)
        add("weightedAvgSharesBasic", sh_b); add("weightedAvgSharesDiluted", sh_d)
        # Derive EBIT/EBITDA when source data lets us
        for end, p in periods.items():
            ebit = None
            if p.get("operatingIncome") is not None:
                ebit = (p["operatingIncome"] or {}).get("raw")
            elif p.get("incomeBeforeTax") is not None and p.get("interestExpense") is not None:
                pre = (p["incomeBeforeTax"] or {}).get("raw")
                ie  = (p["interestExpense"] or {}).get("raw")
                if pre is not None and ie is not None:
                    ebit = pre + ie
            if ebit is not None:
                p["ebit"] = _raw(ebit)
                if p.get("depreciationAndAmortization"):
                    da = (p["depreciationAndAmortization"] or {}).get("raw")
                    if da is not None:
                        p["ebitda"] = _raw(ebit + da)
        return sorted(periods.values(), key=lambda p: p["endDate"]["fmt"] if p.get("endDate") else "", reverse=True)[:8]

    # --- Balance sheet concepts ---
    def balance_rows(forms):
        assets   = _pull(["Assets"], forms)
        liab     = _pull(["Liabilities"], forms)
        equity   = _pull(["StockholdersEquity",
                          "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"], forms)
        cash     = _pull(["CashAndCashEquivalentsAtCarryingValue",
                          "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"], forms)
        st_inv   = _pull(["ShortTermInvestments", "MarketableSecuritiesCurrent"], forms)
        lt_inv   = _pull(["LongTermInvestments",
                          "MarketableSecuritiesNoncurrent"], forms)
        inventory= _pull(["InventoryNet"], forms)
        ar       = _pull(["AccountsReceivableNetCurrent"], forms)
        ap       = _pull(["AccountsPayableCurrent"], forms)
        cur_ast  = _pull(["AssetsCurrent"], forms)
        cur_lia  = _pull(["LiabilitiesCurrent"], forms)
        st_debt  = _pull(["LongTermDebtCurrent",
                          "DebtCurrent",
                          "ShortTermBorrowings",
                          "CommercialPaper"], forms)
        lt_debt  = _pull(["LongTermDebt", "LongTermDebtNoncurrent"], forms)
        tot_debt = _pull(["LongTermDebtAndCapitalLeaseObligations"], forms)
        retained = _pull(["RetainedEarningsAccumulatedDeficit"], forms)
        ppe      = _pull(["PropertyPlantAndEquipmentNet"], forms)
        gw       = _pull(["Goodwill"], forms)
        intang   = _pull(["IntangibleAssetsNetExcludingGoodwill",
                          "FiniteLivedIntangibleAssetsNet"], forms)
        common   = _pull(["CommonStockSharesIssued",
                          "CommonStockValue",
                          "CommonStocksIncludingAdditionalPaidInCapital"], forms)
        treasury = _pull(["TreasuryStockValue",
                          "TreasuryStockCommonValue"], forms)
        aoci     = _pull(["AccumulatedOtherComprehensiveIncomeLossNetOfTax"], forms)
        defrev   = _pull(["DeferredRevenueCurrent",
                          "ContractWithCustomerLiabilityCurrent"], forms)
        periods = {}
        def add(field, items):
            for it in items:
                end = it["end"]
                periods.setdefault(end, {"endDate": _date_stub(end)})
                periods[end][field] = _raw(it["val"])
        add("totalAssets", assets); add("totalLiab", liab); add("totalStockholderEquity", equity)
        add("cash", cash); add("shortTermInvestments", st_inv); add("inventory", inventory)
        add("netReceivables", ar); add("accountsPayable", ap)
        add("totalCurrentAssets", cur_ast); add("totalCurrentLiabilities", cur_lia)
        add("shortLongTermDebt", st_debt)
        add("longTermDebt", lt_debt); add("longTermDebtTotal", tot_debt)
        add("retainedEarnings", retained); add("propertyPlantEquipment", ppe)
        add("goodWill", gw); add("intangibleAssets", intang)
        add("longTermInvestments", lt_inv)
        add("commonStock", common); add("treasuryStock", treasury)
        add("accumulatedOtherComprehensiveIncome", aoci)
        add("deferredRevenue", defrev)
        # Derive working capital + total debt + cash & equivalents (cash + ST inv) + tangible book
        for end, p in periods.items():
            ca, cl = (p.get("totalCurrentAssets") or {}).get("raw"), (p.get("totalCurrentLiabilities") or {}).get("raw")
            if ca is not None and cl is not None:
                p["workingCapital"] = _raw(ca - cl)
            std, ltd = (p.get("shortLongTermDebt") or {}).get("raw"), (p.get("longTermDebt") or {}).get("raw")
            if std is not None or ltd is not None:
                p["totalDebt"] = _raw((std or 0) + (ltd or 0))
            ca_, st_ = (p.get("cash") or {}).get("raw"), (p.get("shortTermInvestments") or {}).get("raw")
            if ca_ is not None or st_ is not None:
                p["cashAndShortTermInvestments"] = _raw((ca_ or 0) + (st_ or 0))
            eq = (p.get("totalStockholderEquity") or {}).get("raw")
            gw_ = (p.get("goodWill") or {}).get("raw") or 0
            int_ = (p.get("intangibleAssets") or {}).get("raw") or 0
            if eq is not None:
                p["netTangibleAssets"] = _raw(eq - gw_ - int_)
        return sorted(periods.values(), key=lambda p: p["endDate"]["fmt"] if p.get("endDate") else "", reverse=True)[:8]

    # --- Cash flow concepts ---
    def cashflow_rows(forms):
        op_cf  = _pull(["NetCashProvidedByUsedInOperatingActivities"], forms)
        inv_cf = _pull(["NetCashProvidedByUsedInInvestingActivities"], forms)
        fin_cf = _pull(["NetCashProvidedByUsedInFinancingActivities"], forms)
        net_inc= _pull(["NetIncomeLoss", "ProfitLoss"], forms)
        capex  = _pull(["PaymentsToAcquirePropertyPlantAndEquipment"], forms)
        acq    = _pull(["PaymentsToAcquireBusinessesNetOfCashAcquired"], forms)
        invs   = _pull(["PaymentsToAcquireInvestments"], forms)
        sale_inv = _pull(["ProceedsFromSaleAndMaturityOfMarketableSecurities",
                          "ProceedsFromSaleOfAvailableForSaleSecurities"], forms)
        divs   = _pull(["PaymentsOfDividends",
                        "PaymentsOfDividendsCommonStock"], forms)
        buybacks = _pull(["PaymentsForRepurchaseOfCommonStock"], forms)
        issue_eq = _pull(["ProceedsFromIssuanceOfCommonStock",
                          "ProceedsFromStockOptionsExercised"], forms)
        net_borrow = _pull(["ProceedsFromIssuanceOfLongTermDebt"], forms)
        repay_debt = _pull(["RepaymentsOfLongTermDebt"], forms)
        depr   = _pull(["DepreciationDepletionAndAmortization",
                        "DepreciationAndAmortization", "Depreciation"], forms)
        sbc_cf = _pull(["ShareBasedCompensation"], forms)
        chg_ar = _pull(["IncreaseDecreaseInAccountsReceivable"], forms)
        chg_ap = _pull(["IncreaseDecreaseInAccountsPayable"], forms)
        chg_inv= _pull(["IncreaseDecreaseInInventories"], forms)
        chg_oth= _pull(["IncreaseDecreaseInOtherOperatingCapitalNet",
                        "IncreaseDecreaseInOtherOperatingAssets"], forms)
        chg_def= _pull(["IncreaseDecreaseInDeferredRevenue",
                        "IncreaseDecreaseInContractWithCustomerLiability"], forms)
        periods = {}
        def add(field, items):
            for it in items:
                end = it["end"]
                periods.setdefault(end, {"endDate": _date_stub(end)})
                periods[end][field] = _raw(it["val"])
        add("totalCashFromOperatingActivities", op_cf)
        add("totalCashflowsFromInvestingActivities", inv_cf)
        add("totalCashFromFinancingActivities", fin_cf)
        add("netIncome", net_inc)
        add("capitalExpenditures", capex)
        add("acquisitionsNet", acq)
        add("investments", invs)
        add("salesMaturitiesOfInvestments", sale_inv)
        add("dividendsPaid", divs)
        add("repurchaseOfStock", buybacks)
        add("issuanceOfStock", issue_eq)
        add("debtIssued", net_borrow)
        add("debtRepaid", repay_debt)
        add("depreciation", depr); add("stockBasedCompensation", sbc_cf)
        add("changeToAccountReceivables", chg_ar)
        add("changeToAccountsPayable", chg_ap)
        add("changeToInventory", chg_inv)
        add("changeToOperatingActivities", chg_oth)
        add("changeToDeferredRevenue", chg_def)
        # Derive FCF (Operating CF + capex; capex is reported negative)
        for end, p in periods.items():
            op = (p.get("totalCashFromOperatingActivities") or {}).get("raw")
            cx = (p.get("capitalExpenditures") or {}).get("raw")
            if op is not None and cx is not None:
                # capex pulls in positive sign from XBRL → subtract it for FCF
                p["freeCashFlow"] = _raw(op - abs(cx))
            # Net borrowings
            iss = (p.get("debtIssued") or {}).get("raw")
            rep = (p.get("debtRepaid") or {}).get("raw")
            if iss is not None or rep is not None:
                p["netBorrowings"] = _raw((iss or 0) - (rep or 0))
        return sorted(periods.values(), key=lambda p: p["endDate"]["fmt"] if p.get("endDate") else "", reverse=True)[:8]

    annual_forms = ("10-K", "10-K/A", "20-F", "20-F/A", "40-F")
    quart_forms  = ("10-Q", "10-Q/A")

    income_a  = income_rows(annual_forms)
    income_q  = income_rows(quart_forms)
    balance_a = balance_rows(annual_forms)
    balance_q = balance_rows(quart_forms)
    cash_a    = cashflow_rows(annual_forms)
    cash_q    = cashflow_rows(quart_forms)

    # Earnings history: combine quarterly revenue + EPS
    earnings_hist = []
    by_end = {}
    for it in _pull(["EarningsPerShareBasic"], quart_forms):
        by_end.setdefault(it["end"], {})["eps"] = it["val"]
    for it in _pull(["Revenues",
                     "RevenueFromContractWithCustomerExcludingAssessedTax",
                     "SalesRevenueNet"], quart_forms):
        by_end.setdefault(it["end"], {})["rev"] = it["val"]
    for end in sorted(by_end.keys(), reverse=True)[:8]:
        b = by_end[end]
        earnings_hist.append({
            "quarter":  _date_stub(end),
            "epsActual": _raw(b.get("eps")),
            "revenue":   _raw(b.get("rev")),
        })

    return {
        "incomeStatementHistory": {"incomeStatementHistory": income_a},
        "incomeStatementHistoryQuarterly": {"incomeStatementHistory": income_q},
        "balanceSheetHistory": {"balanceSheetStatements": balance_a},
        "balanceSheetHistoryQuarterly": {"balanceSheetStatements": balance_q},
        "cashflowStatementHistory": {"cashflowStatements": cash_a},
        "cashflowStatementHistoryQuarterly": {"cashflowStatements": cash_q},
        "earningsHistory": {"history": earnings_hist},
        "_meta_sec_cik": cik,
    }


# ---------- Multi-source financial statement merger ----------
# For US tickers: SEC XBRL is authoritative (direct from filings), AV / FMP fill
# gaps in older history, stockanalysis.com is a last-resort HTML fallback.
# For international tickers (no SEC filings): SA → FMP → AV fallback chain.

_IS_KEY = "incomeStatementHistory"
_IS_QKEY = "incomeStatementHistoryQuarterly"
_BS_KEY = "balanceSheetHistory"
_BS_QKEY = "balanceSheetHistoryQuarterly"
_CF_KEY = "cashflowStatementHistory"
_CF_QKEY = "cashflowStatementHistoryQuarterly"

# Inner list key differs per statement type ('incomeStatementHistory',
# 'balanceSheetStatements', 'cashflowStatements').
_INNER_KEY = {
    _IS_KEY:   "incomeStatementHistory",
    _IS_QKEY:  "incomeStatementHistory",
    _BS_KEY:   "balanceSheetStatements",
    _BS_QKEY:  "balanceSheetStatements",
    _CF_KEY:   "cashflowStatements",
    _CF_QKEY:  "cashflowStatements",
}


def _fin_period_key(period):
    """Bucket a period by year-month so slightly off fiscal end dates (e.g.
    2024-12-28 vs 2024-12-31) merge into the same row."""
    ed = (period or {}).get("endDate") or {}
    fmt = ed.get("fmt") or ""
    return fmt[:7]  # 'YYYY-MM' bucket


def _merge_period_rows(sources, key):
    """Merge the list of rows found at `source[key][_INNER_KEY[key]]` across
    ordered sources. Earlier sources win on conflict. Returns a merged list,
    newest-first, max 8 rows."""
    buckets = {}
    order = []
    inner_k = _INNER_KEY[key]
    for src in sources:
        if not src:
            continue
        block = src.get(key) or {}
        rows = block.get(inner_k) or []
        for row in rows:
            if not isinstance(row, dict):
                continue
            bk = _fin_period_key(row)
            if not bk:
                continue
            if bk not in buckets:
                buckets[bk] = dict(row)
                order.append(bk)
            else:
                existing = buckets[bk]
                for k, v in row.items():
                    if k == "endDate":
                        # Keep the most authoritative end date (first writer wins)
                        existing.setdefault("endDate", v)
                    elif existing.get(k) in (None, {}) or (
                            isinstance(existing.get(k), dict) and existing[k].get("raw") is None):
                        existing[k] = v
    # Sort newest-first by endDate fmt
    merged = list(buckets.values())
    merged.sort(key=lambda p: (p.get("endDate") or {}).get("fmt") or "", reverse=True)
    return merged[:8]


def _derive_all(rows, kind):
    """Fill in derived metrics (EBIT/EBITDA/FCF/WC/totalDebt/tangibleAssets/
    netBorrowings/cashAndShortTermInvestments) on every merged row."""
    def val(p, k):
        x = p.get(k)
        return x.get("raw") if isinstance(x, dict) else None
    for p in rows:
        if kind == "is":
            if p.get("ebit") is None:
                oi = val(p, "operatingIncome")
                if oi is not None:
                    p["ebit"] = _raw(oi)
                else:
                    pre, ie = val(p, "incomeBeforeTax"), val(p, "interestExpense")
                    if pre is not None and ie is not None:
                        p["ebit"] = _raw(pre + ie)
            if p.get("ebitda") is None:
                ebit = val(p, "ebit"); da = val(p, "depreciationAndAmortization")
                if ebit is not None and da is not None:
                    p["ebitda"] = _raw(ebit + da)
        elif kind == "bs":
            if p.get("workingCapital") is None:
                ca, cl = val(p, "totalCurrentAssets"), val(p, "totalCurrentLiabilities")
                if ca is not None and cl is not None:
                    p["workingCapital"] = _raw(ca - cl)
            if p.get("totalDebt") is None:
                std, ltd = val(p, "shortLongTermDebt"), val(p, "longTermDebt")
                if std is not None or ltd is not None:
                    p["totalDebt"] = _raw((std or 0) + (ltd or 0))
            if p.get("cashAndShortTermInvestments") is None:
                c, sti = val(p, "cash"), val(p, "shortTermInvestments")
                if c is not None or sti is not None:
                    p["cashAndShortTermInvestments"] = _raw((c or 0) + (sti or 0))
            if p.get("netTangibleAssets") is None:
                eq = val(p, "totalStockholderEquity")
                gw = val(p, "goodWill") or 0
                it = val(p, "intangibleAssets") or 0
                if eq is not None:
                    p["netTangibleAssets"] = _raw(eq - gw - it)
        elif kind == "cf":
            if p.get("freeCashFlow") is None:
                op, cx = val(p, "totalCashFromOperatingActivities"), val(p, "capitalExpenditures")
                if op is not None and cx is not None:
                    p["freeCashFlow"] = _raw(op - abs(cx))
            if p.get("netBorrowings") is None:
                iss, rep = val(p, "debtIssued"), val(p, "debtRepaid")
                if iss is not None or rep is not None:
                    p["netBorrowings"] = _raw((iss or 0) - (rep or 0))
    return rows


def enriched_financials(sym):
    """Fan out SEC + FMP + AV + SA financials, merge per-period rows.
    Priority (most → least authoritative): SEC > FMP > AV > stockanalysis.com.
    Returns Yahoo-shaped merge dict suitable for `block.update(...)`."""
    sym = (sym or "").upper()
    if not sym or is_crypto(sym) or sym.startswith("^") or sym.endswith("=X"):
        return {}

    r = _parallel({
        "sec": lambda: sec_build_financials(sym),
        "fmp": lambda: fmp_financials(sym),
        "av":  lambda: av_financials(sym),
        "sa":  lambda: sa_financials(sym),
    }, timeout=16)

    order = [r.get("sec"), r.get("fmp"), r.get("av"), r.get("sa")]
    if not any(order):
        return {}

    merged = {}
    kind_map = {_IS_KEY: "is", _IS_QKEY: "is",
                _BS_KEY: "bs", _BS_QKEY: "bs",
                _CF_KEY: "cf", _CF_QKEY: "cf"}
    for key in (_IS_KEY, _IS_QKEY, _BS_KEY, _BS_QKEY, _CF_KEY, _CF_QKEY):
        rows = _derive_all(_merge_period_rows(order, key), kind_map[key])
        merged[key] = {_INNER_KEY[key]: rows}

    # Preserve earningsHistory from SEC if present (quarterly EPS/revenue series)
    sec = r.get("sec") or {}
    if sec.get("earningsHistory"):
        merged["earningsHistory"] = sec["earningsHistory"]
    if sec.get("_meta_sec_cik"):
        merged["_meta_sec_cik"] = sec["_meta_sec_cik"]

    # Track which source contributed at least one row, for debugging / UI badges
    contributors = []
    for name, d in (("SEC", r.get("sec")), ("FMP", r.get("fmp")),
                    ("AV", r.get("av")), ("SA", r.get("sa"))):
        if d and any((d.get(k) or {}).get(_INNER_KEY[k]) for k in
                     (_IS_KEY, _IS_QKEY, _BS_KEY, _BS_QKEY, _CF_KEY, _CF_QKEY)):
            contributors.append(name)
    if contributors:
        merged["_meta_financial_sources"] = contributors

    return merged


# ---------- Google News RSS (no auth, real stock news) ----------
def fetch_news(query, limit=20):
    """Pull headlines from Google News RSS for a ticker or company."""
    cache_key = f"news::{query}::{limit}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    q = f"{query} stock" if query and len(query) <= 6 else query
    url = f"https://news.google.com/rss/search?q={quote(q)}&hl=en-US&gl=US&ceid=US:en"
    code, body = _fetch(url, accept="application/rss+xml", timeout=10)
    if code != 200 or not body:
        _cache_put(cache_key, [], ttl=120)
        return []
    items = []
    try:
        root = ET.fromstring(body)
        for it in root.iter("item"):
            title = (it.findtext("title") or "").strip()
            link  = (it.findtext("link")  or "").strip()
            pdate = (it.findtext("pubDate") or "").strip()
            src_el = it.find("source")
            src = (src_el.text or "") if src_el is not None else ""
            ts = 0
            try:
                ts = int(parsedate_to_datetime(pdate).timestamp())
            except Exception:
                pass
            items.append({
                "uuid": link,
                "title": html.unescape(title),
                "link": link,
                "publisher": html.unescape(src),
                "providerPublishTime": ts,
                "type": "STORY",
                "relatedTickers": [query] if query else [],
            })
            if len(items) >= limit:
                break
    except Exception as e:
        sys.stderr.write(f"[news] parse error: {e}\n")
    _cache_put(cache_key, items, ttl=180)
    return items


# ---------- Multi-source news aggregator ----------
def _parse_rss(body, source_label, sym=None, limit=20):
    """Parse a generic RSS feed into the same news shape as fetch_news()."""
    items = []
    try:
        root = ET.fromstring(body)
        for it in root.iter("item"):
            title = (it.findtext("title") or "").strip()
            link  = (it.findtext("link")  or "").strip()
            pdate = (it.findtext("pubDate") or "").strip()
            desc  = (it.findtext("description") or "").strip()
            ts = 0
            try:
                ts = int(parsedate_to_datetime(pdate).timestamp())
            except Exception:
                pass
            items.append({
                "uuid":                link,
                "title":               html.unescape(title),
                "link":                link,
                "publisher":           source_label,
                "providerPublishTime": ts,
                "type":                "STORY",
                "relatedTickers":      [sym] if sym else [],
                "summary":             html.unescape(re.sub(r"<[^>]+>", "", desc))[:280],
            })
            if len(items) >= limit:
                break
    except Exception as e:
        sys.stderr.write(f"[rss {source_label}] {e}\n")
    return items


def yahoo_rss_news(sym, limit=20):
    """Yahoo Finance per-ticker RSS feed."""
    if not sym:
        return []
    cache_key = f"news::yahoo::{sym}::{limit}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={quote(sym)}&region=US&lang=en-US"
    code, body = _polite_fetch(url, host="feeds.finance.yahoo.com",
                                accept="application/rss+xml")
    items = []
    if code == 200 and body:
        items = _parse_rss(body, "Yahoo Finance", sym=sym, limit=limit)
    _cache_put(cache_key, items, ttl=180)
    return items


def marketwatch_rss_news(sym, limit=20):
    """MarketWatch per-ticker RSS feed."""
    if not sym:
        return []
    cache_key = f"news::mw::{sym}::{limit}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    url = f"https://feeds.content.dowjones.io/public/rss/mw_topstories"
    # MW per-ticker URL pattern (legacy /rss/marketpulse) is unreliable; use top stories + filter
    code, body = _polite_fetch(url, host="feeds.content.dowjones.io",
                                accept="application/rss+xml")
    items = []
    if code == 200 and body:
        all_items = _parse_rss(body, "MarketWatch", sym=sym, limit=200)
        sym_u = sym.upper()
        # Light keyword filter so this list is symbol-relevant
        for it in all_items:
            t = (it.get("title") or "").upper()
            if sym_u in t or f"({sym_u})" in t:
                items.append(it)
                if len(items) >= limit:
                    break
    _cache_put(cache_key, items, ttl=300)
    return items


def fetch_news_all(sym, limit=40):
    """Aggregate news for a symbol across many sources, deduped by URL/title.

    Sources (in order; missing ones are skipped silently):
      - Google News RSS         (always on)
      - Yahoo Finance RSS       (always on)
      - MarketWatch RSS         (always on, keyword-filtered)
      - Finnhub /company-news   (if FINNHUB key present)
      - FMP /stock_news         (if FMP key present)
    """
    cache_key = f"news::all::{sym}::{limit}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    tasks = {
        "google": (lambda: fetch_news(sym, limit=20)),
        "yahoo":  (lambda: yahoo_rss_news(sym, limit=20)),
        "mw":     (lambda: marketwatch_rss_news(sym, limit=20)),
    }
    if finnhub_key():
        tasks["finnhub"] = (lambda: fh_news(sym, days=14, limit=20))
    if fmp_key():
        tasks["fmp"] = (lambda: fmp_news(sym, limit=20))

    results = _parallel(tasks, timeout=10)

    # Normalize each provider's row shape -> common shape
    def _normalize(rows, default_pub):
        out = []
        for r in rows or []:
            if not isinstance(r, dict):
                continue
            url = r.get("link") or r.get("url") or ""
            title = (r.get("title") or "").strip()
            if not url or not title:
                continue
            ts = r.get("providerPublishTime") or 0
            if not ts and r.get("publishedAt"):
                pa = r.get("publishedAt")
                try:
                    if isinstance(pa, (int, float)):
                        ts = int(pa)
                    else:
                        ts = int(parsedate_to_datetime(str(pa)).timestamp())
                except Exception:
                    try:
                        ts = int(time.mktime(time.strptime(str(pa)[:19],
                                                            "%Y-%m-%dT%H:%M:%S")))
                    except Exception:
                        ts = 0
            out.append({
                "uuid":                url,
                "title":               title,
                "link":                url,
                "publisher":           r.get("publisher") or r.get("source") or default_pub,
                "providerPublishTime": ts,
                "type":                r.get("type") or "STORY",
                "summary":             r.get("summary") or "",
                "image":               r.get("image"),
                "relatedTickers":      r.get("relatedTickers") or [sym],
            })
        return out

    merged = []
    merged += _normalize(results.get("google"),  "Google News")
    merged += _normalize(results.get("yahoo"),   "Yahoo Finance")
    merged += _normalize(results.get("mw"),      "MarketWatch")
    merged += _normalize(results.get("finnhub"), "Finnhub")
    merged += _normalize(results.get("fmp"),     "FMP")

    # Dedupe by URL first, then by lowercased title
    seen_urls = set()
    seen_titles = set()
    unique = []
    for item in merged:
        u = item["link"].split("?")[0]
        t = item["title"].lower()
        if u in seen_urls or t in seen_titles:
            continue
        seen_urls.add(u)
        seen_titles.add(t)
        unique.append(item)

    # Newest first; items without a timestamp sink to the bottom
    unique.sort(key=lambda x: x.get("providerPublishTime") or 0, reverse=True)
    unique = unique[: int(limit)]

    out = {
        "symbol":  sym,
        "count":   len(unique),
        "sources": {k: len(_normalize(v, k)) for k, v in results.items() if v},
        "news":    unique,
    }
    _cache_put(cache_key, out, ttl=120)
    return out


# ---------- Company press releases ----------
# Public companies publish press releases via newswire services (GlobeNewswire,
# PRNewswire, BusinessWire, AccessWire) or directly on their own IR site.
# We aggregate, dedupe, and filter by ticker/company name to drop unrelated noise.

PRESS_WIRE_SOURCES = [
    "globenewswire.com", "prnewswire.com", "businesswire.com",
    "accesswire.com", "newsfilecorp.com", "prlog.org",
    "benzinga.com/press-releases", "investorshub.com",
    "streetinsider.com/press_releases",
]

# International / OTC tickers that have no Yahoo or SEC presence. We reroute
# quote + financials + profile to stockanalysis.com's per-exchange pages and
# supply a hardcoded asset-profile stub so the UI isn't completely empty.
# Slug format: stockanalysis.com uses /quote/<exchangeCode>/<localTicker>/ for
# non-US listings (e.g. /quote/tyo/3350/ for Tokyo 3350). Home ticker is the
# Yahoo symbol for the primary listing (used for Stooq / history).
FOREIGN_TICKER_MAP = {
    # US OTC ADR aliases → Metaplanet (Tokyo: 3350)
    "MTPLF": {
        "slug":    "tyo/3350",
        "home":    "3350.T",
        "name":    "Metaplanet, Inc.",
        "shortName": "Metaplanet",
        "exchange":"Tokyo",
        "country": "Japan",
        "sector":  "Financial Services",
        "industry":"Asset Management",
        "website": "https://metaplanet.jp/en/",
        "currency":"JPY",
        "summary": ("Metaplanet Inc. is a Tokyo-listed investment firm that "
                    "has adopted a bitcoin treasury strategy, accumulating "
                    "BTC as its primary reserve asset. The company also "
                    "operates hospitality assets in Japan. Listed on the "
                    "Tokyo Stock Exchange Standard Market (3350) with an "
                    "OTC ADR in the US (MTPLF)."),
    },
    "3350.T": {
        "slug":    "tyo/3350",
        "home":    "3350.T",
        "name":    "Metaplanet, Inc.",
        "shortName": "Metaplanet",
        "exchange":"Tokyo",
        "country": "Japan",
        "sector":  "Financial Services",
        "industry":"Asset Management",
        "website": "https://metaplanet.jp/en/",
        "currency":"JPY",
        "summary": ("Metaplanet Inc. is a Tokyo-listed investment firm that "
                    "has adopted a bitcoin treasury strategy, accumulating "
                    "BTC as its primary reserve asset. The company also "
                    "operates hospitality assets in Japan. Listed on the "
                    "Tokyo Stock Exchange Standard Market (3350) with an "
                    "OTC ADR in the US (MTPLF)."),
    },
}


def _foreign_info(sym):
    """Return the FOREIGN_TICKER_MAP entry for sym (or its home ticker)."""
    if not sym:
        return None
    s = sym.upper()
    return FOREIGN_TICKER_MAP.get(s)


def foreign_profile(sym):
    """Hardcoded Yahoo-shaped assetProfile for tickers in FOREIGN_TICKER_MAP.
    Used when SEC / FMP / Yahoo have no record of the ticker (e.g. MTPLF).
    Returns {} if the ticker isn't in the map."""
    info = _foreign_info(sym)
    if not info:
        return {}
    return {
        "assetProfile": {
            "name":                info.get("name"),
            "longBusinessSummary": info.get("summary"),
            "website":             info.get("website"),
            "country":             info.get("country"),
            "sector":              info.get("sector"),
            "industry":            info.get("industry"),
            "exchanges":           [info.get("exchange")] if info.get("exchange") else [],
            "tickers":             [sym.upper(), info.get("home")] if info.get("home") else [sym.upper()],
            "longName":            info.get("name"),
            "shortName":           info.get("shortName") or info.get("name"),
            "currency":            info.get("currency"),
        },
        "_meta_source_profile": "foreign-map",
    }


# Known company-specific IR/news pages for tickers that don't show up on US wires
# (international, Bitcoin treasury cos, etc.). Add more as needed.
PRESS_IR_PAGES = {
    "3350.T":  ("https://metaplanet.jp/en/disclosures",  "Metaplanet"),
    "MTPLF":   ("https://metaplanet.jp/en/disclosures",  "Metaplanet"),
    "MSTR":    ("https://www.strategy.com/press",  "Strategy"),
    "BTBT":    ("https://www.btbt.io/news/",       "Bit Digital"),
    "MARA":    ("https://ir.mara.com/news-events/press-releases", "Marathon Digital"),
    "RIOT":    ("https://www.riotplatforms.com/news-media/press-releases", "Riot Platforms"),
    "CLSK":    ("https://investors.cleanspark.com/news-events/news", "CleanSpark"),
    "HUT":     ("https://hut8.io/investors/news/", "Hut 8"),
    "COIN":    ("https://investor.coinbase.com/news/", "Coinbase"),
    "TSLA":    ("https://ir.tesla.com/press",      "Tesla"),
    "NVDA":    ("https://nvidianews.nvidia.com/news", "NVIDIA"),
    "AAPL":    ("https://www.apple.com/newsroom/", "Apple"),
    "MSFT":    ("https://news.microsoft.com/source/", "Microsoft"),
}


def _press_google_news(sym, company_name=None, limit=30):
    """Google News RSS filtered to press-wire publishers AND requiring the
    ticker/company name to appear in the result title (so we don't surface
    unrelated companies that share keywords)."""
    queries = []
    site_filter = " OR ".join(f"site:{s}" for s in PRESS_WIRE_SOURCES)
    needles = []
    if company_name:
        # Clean up company name suffixes that pollute search ('Inc.', 'Corp', etc.)
        cn = re.sub(r"\b(Inc\.?|Corp\.?|Corporation|Company|Co\.?|Ltd\.?|Limited|plc|Holdings?|Group|S\.?A\.?|N\.?V\.?)\b\.?",
                    "", company_name, flags=re.I).strip().rstrip(",")
        if cn:
            needles.append(cn.lower())
            queries.append(f"({site_filter}) \"{cn}\"")
    if sym:
        needles.append(sym.lower())
        # Ticker queries are usually noisy alone; only run if no company name available
        if not company_name:
            queries.append(f"({site_filter}) \"{sym}\"")

    def _matches(title):
        """True if title mentions any of our needles (company name or ticker)."""
        if not needles:
            return True
        tl = title.lower()
        return any(n and n in tl for n in needles)

    out = []
    seen = set()
    for q in queries:
        url = f"https://news.google.com/rss/search?q={quote(q)}&hl=en-US&gl=US&ceid=US:en"
        code, body = _fetch(url, accept="application/rss+xml", timeout=10)
        if code != 200 or not body:
            continue
        try:
            root = ET.fromstring(body)
            for it in root.iter("item"):
                title = html.unescape((it.findtext("title") or "").strip())
                link = (it.findtext("link") or "").strip()
                pdate = (it.findtext("pubDate") or "").strip()
                src_el = it.find("source")
                publisher = html.unescape((src_el.text or "") if src_el is not None else "")
                if not title or not link:
                    continue
                key = link.split("?")[0]
                if key in seen:
                    continue
                if not _matches(title):
                    continue
                seen.add(key)
                ts = 0
                try:
                    ts = int(parsedate_to_datetime(pdate).timestamp())
                except Exception:
                    pass
                out.append({
                    "title":     title,
                    "link":      link,
                    "publisher": publisher or "Newswire",
                    "ts":        ts,
                    "date":      time.strftime("%Y-%m-%d %H:%M", time.gmtime(ts)) if ts else "",
                    "source":    "GoogleNews(press)",
                    "summary":   "",
                })
                if len(out) >= limit:
                    return out
        except Exception as e:
            sys.stderr.write(f"[press-gn {q}] parse error: {e}\n")
    return out


def _press_ir_website(sym, company_name=None, limit=20):
    """Scrape press releases directly off the company's own IR/newsroom page.
    Uses PRESS_IR_PAGES first; falls back to common /news /press paths otherwise."""
    sym_u = (sym or "").upper()
    page_url = None
    page_label = company_name or sym_u
    if sym_u in PRESS_IR_PAGES:
        page_url, page_label = PRESS_IR_PAGES[sym_u]

    # Fallback: derive candidate URLs from FMP profile website (best-effort)
    candidate_urls = []
    if page_url:
        candidate_urls.append(page_url)
    else:
        try:
            prof = fmp_profile(sym_u) or {}
            site = (prof.get("website") or "").rstrip("/")
            if site:
                for path in ("/news", "/press-releases", "/news-releases",
                             "/newsroom", "/investors/news",
                             "/investor-relations/news",
                             "/about/news", "/en/news"):
                    candidate_urls.append(site + path)
        except Exception:
            pass

    out = []
    seen = set()
    for url in candidate_urls:
        if len(out) >= limit:
            break
        body = _scrape_fetch(url, ttl=900)
        if not body:
            continue
        host = re.match(r"https?://([^/]+)", url)
        host = host.group(1) if host else ""

        def _add(title, href, date_hint=""):
            if not title or len(title) < 8:
                return
            # Skip nav / generic / dropdown labels
            if re.search(
                    r"^(home|about(\s*us)?|contact(\s*us)?|menu|next|prev|close|toggle|"
                    r"read more|subscribe|sign\s*up|sign\s*in|log\s*in|register|"
                    r"view all|all news|press releases?|news|more|follow|share|"
                    r"english|japanese|language|copyright|sitemap|ir\s*contact|"
                    r"careers?|support|faqs?|help|"
                    r"analytics|privacy(\s*policy)?|cookies?(\s*policy)?|"
                    r"terms(\s*of\s*(use|service))?|disclaimer|legal|imprint|"
                    r"top|search|investor\s*relations?|company)$",
                    title, re.I):
                return
            # Path-level filter — drop links that are clearly site chrome
            if re.search(r"/(privacy|terms|legal|cookies?|sitemap|contact|careers?|"
                         r"about|search|login|register|imprint|disclaimer|analytics)"
                         r"(/|$|\?|#)", href, re.I):
                return
            if not href or href.startswith("#") or href.startswith("mailto:") \
                    or href.startswith("javascript:"):
                return
            if any(s in href for s in ("twitter.com", "facebook.com", "linkedin.com",
                                        "instagram.com", "youtube.com", "/login", "/share")):
                return
            # Resolve relative URLs
            h = href
            if h.startswith("//"):
                h = "https:" + h
            elif h.startswith("/"):
                h = f"https://{host}{h}"
            elif not h.startswith("http"):
                # Bare relative path ("disclosures/foo.pdf"); resolve against
                # the page URL so JP IR sites with relative PDF links still work.
                base = url.rsplit("/", 1)[0] if "/" in url else url
                h = base + "/" + h.lstrip("./")
            key = h.split("?")[0]
            if key in seen:
                return
            seen.add(key)
            # Strip leading date prefix from the title if present (e.g. "2025.04.21 ...")
            clean = re.sub(r"^[\s•·\-–—]*\d{4}[./\-]\d{1,2}[./\-]\d{1,2}[\s•·\-–—]*",
                           "", title).strip()
            out.append({
                "title":     clean or title,
                "link":      h,
                "publisher": page_label,
                "ts":        0,
                "date":      date_hint,
                "source":    f"IR · {host}",
                "summary":   "",
            })

        # Pass 0: __NEXT_DATA__ JSON (Next.js / modern JP IR sites incl.
        # metaplanet.jp). The full disclosure list is usually embedded as
        # JSON inside a <script id="__NEXT_DATA__"> tag. Walk the blob and
        # pluck objects that look like disclosure entries.
        try:
            nm = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
                           body, re.S | re.I)
            if nm:
                blob = json.loads(nm.group(1))
                stack = [blob]
                while stack and len(out) < limit:
                    node = stack.pop()
                    if isinstance(node, dict):
                        # Heuristic: looks like a disclosure / news entry
                        title = (node.get("title") or node.get("name")
                                 or node.get("subject") or node.get("heading"))
                        # Common link-shaped fields on JP IR Next sites
                        link = (node.get("pdf") or node.get("pdfUrl")
                                or node.get("file") or node.get("fileUrl")
                                or node.get("url") or node.get("link")
                                or node.get("href") or node.get("slug"))
                        if isinstance(link, dict):
                            link = (link.get("url") or link.get("href")
                                    or link.get("src"))
                        date_hint = (node.get("date") or node.get("publishedAt")
                                     or node.get("publishDate")
                                     or node.get("releasedAt") or "")
                        if (isinstance(title, str) and isinstance(link, str)
                                and len(title) >= 6):
                            _add(title.strip(), link.strip(),
                                 str(date_hint or ""))
                        for v in node.values():
                            if isinstance(v, (dict, list)):
                                stack.append(v)
                    elif isinstance(node, list):
                        stack.extend(node)
        except Exception:
            pass

        # Pass 0b: __NUXT__ / window.__INITIAL_STATE__ blobs (Nuxt / Vue)
        try:
            for pat in (r"window\.__NUXT__\s*=\s*(\{.*?\})\s*;?\s*</script>",
                        r"window\.__INITIAL_STATE__\s*=\s*(\{.*?\})\s*;?\s*</script>"):
                if len(out) >= limit:
                    break
                bm = re.search(pat, body, re.S)
                if not bm:
                    continue
                try:
                    blob = json.loads(bm.group(1))
                except Exception:
                    continue
                stack = [blob]
                while stack and len(out) < limit:
                    node = stack.pop()
                    if isinstance(node, dict):
                        title = node.get("title") or node.get("subject")
                        link = (node.get("pdf") or node.get("url")
                                or node.get("link") or node.get("href"))
                        if isinstance(link, dict):
                            link = (link.get("url") or link.get("href"))
                        date_hint = (node.get("date") or node.get("publishedAt")
                                     or "")
                        if (isinstance(title, str) and isinstance(link, str)
                                and len(title) >= 6):
                            _add(title.strip(), link.strip(),
                                 str(date_hint or ""))
                        for v in node.values():
                            if isinstance(v, (dict, list)):
                                stack.append(v)
                    elif isinstance(node, list):
                        stack.extend(node)
        except Exception:
            pass

        # Pass 1: <article>...<a href=X>...title text...</a>...</article>
        # Captures Wordpress / Japanese corporate IR card layouts (incl. metaplanet.jp).
        for am in re.finditer(r"<(?:article|li|div)[^>]*>(.*?)</(?:article|li|div)>",
                              body, re.S | re.I):
            block = am.group(1)
            am2 = re.search(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', block, re.S | re.I)
            if not am2:
                continue
            href = am2.group(1).strip()
            inner = am2.group(2)
            # Prefer <h*> child text as title; otherwise the full anchor text
            ht = re.search(r"<h[1-6][^>]*>(.*?)</h[1-6]>", inner, re.S | re.I) \
                 or re.search(r"<h[1-6][^>]*>(.*?)</h[1-6]>", block, re.S | re.I)
            title = _strip_tags(ht.group(1) if ht else inner)
            # Pull a date hint from a <time>/data-date if present
            dh = re.search(r"<time[^>]*datetime=\"([^\"]+)\"", block, re.I) \
                 or re.search(r"data-date=\"([^\"]+)\"", block, re.I)
            _add(title, href, dh.group(1) if dh else "")
            if len(out) >= limit:
                break

        # Pass 1b: PDF-link rows. JP IR/disclosure pages frequently render each
        # filing as `<a href="...pdf"><time>YYYY.MM.DD</time> Title</a>` or as
        # adjacent siblings. Capture every PDF anchor with non-trivial context.
        if len(out) < limit:
            for m in re.finditer(
                    r'<a[^>]+href="([^"]+\.pdf[^"]*)"[^>]*>(.*?)</a>',
                    body, re.S | re.I):
                href = m.group(1).strip()
                inner = m.group(2)
                # Pull date from <time> if present
                tm = re.search(r"<time[^>]*>(.*?)</time>", inner, re.S | re.I)
                dh = _strip_tags(tm.group(1)) if tm else ""
                title = _strip_tags(inner)
                if dh:
                    title = title.replace(dh, "", 1).strip(" •·-–—")
                if len(title) < 6:
                    continue
                _add(title, href, dh)
                if len(out) >= limit:
                    break

        # Pass 2: bare <a href> with substantial anchor text — fallback heuristic
        if len(out) < limit:
            for m in re.finditer(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', body, re.S | re.I):
                title = _strip_tags(m.group(2))
                if len(title) < 12:
                    continue
                _add(title, m.group(1).strip())
                if len(out) >= limit:
                    break
    # If we got results from a hardcoded IR page, those are 100% on-target.
    # If we got results from a fallback /news scrape, filter by company-name match
    # to avoid grabbing every nav link.
    if sym_u not in PRESS_IR_PAGES and company_name:
        cn_lc = company_name.lower()
        # Also match the first significant token (e.g. "Apple" from "Apple Inc.")
        first = re.split(r"\s+", company_name.strip())[0].lower()
        out = [it for it in out
               if cn_lc in it["title"].lower() or first in it["title"].lower()]
    return out[:limit]


def _press_yahoo_japan_disclosures(sym, limit=30):
    """For Tokyo-listed companies (and their foreign ADRs), fetch official
    TDnet disclosures from Yahoo Finance Japan. The disclosure page is
    server-rendered HTML with the full filing list embedded — perfect for
    companies whose own IR site is JS-rendered (e.g. Metaplanet)."""
    if not sym:
        return []
    sym_u = sym.upper()
    # Resolve to a .T home ticker
    home = None
    info = _foreign_info(sym_u) or {}
    if info.get("home", "").endswith(".T"):
        home = info["home"]
    elif sym_u.endswith(".T"):
        home = sym_u
    if not home:
        return []
    code = home.split(".")[0]  # "3350"

    out = []
    # Yahoo Finance JP exposes a per-stock disclosure listing.
    # Path: /quote/<code>.T/disclosure  (renders timely-disclosure rows
    # straight from TDnet — date, title, PDF/page link).
    urls = [
        f"https://finance.yahoo.co.jp/quote/{code}.T/disclosure",
        f"https://finance.yahoo.co.jp/quote/{code}.T/news",
    ]
    seen = set()
    for url in urls:
        if len(out) >= limit:
            break
        body = _scrape_fetch(url, ttl=600)
        if not body:
            continue

        # Try __NEXT_DATA__ JSON first (Yahoo JP is Next.js)
        try:
            nm = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
                           body, re.S | re.I)
            if nm:
                blob = json.loads(nm.group(1))
                stack = [blob]
                while stack and len(out) < limit:
                    node = stack.pop()
                    if isinstance(node, dict):
                        # Disclosure entries on Yahoo JP typically have
                        # title + url + (publishedAt|date) + a docId.
                        title = (node.get("title") or node.get("subject")
                                 or node.get("headline"))
                        link = (node.get("url") or node.get("link")
                                or node.get("href") or node.get("docUrl"))
                        if isinstance(link, dict):
                            link = link.get("url") or link.get("href")
                        date_hint = (node.get("publishedAt") or node.get("date")
                                     or node.get("releasedAt") or "")
                        # Filter: must have title + link + look like a disclosure
                        # (path contains /disclosure or filename ends .pdf, OR
                        # node has a docId field — strong signal it's TDnet).
                        is_disclosure = False
                        if isinstance(link, str):
                            if (re.search(r"(disclosure|tdnet|release)",
                                          link, re.I)
                                    or link.lower().endswith(".pdf")):
                                is_disclosure = True
                            if node.get("docId") or node.get("documentId"):
                                is_disclosure = True
                        if (isinstance(title, str) and isinstance(link, str)
                                and is_disclosure and len(title) >= 4):
                            key = link.split("?")[0]
                            if key not in seen:
                                seen.add(key)
                                ts = 0
                                try:
                                    if isinstance(date_hint, str) and date_hint:
                                        ts = int(parsedate_to_datetime(
                                            date_hint).timestamp())
                                except Exception:
                                    pass
                                out.append({
                                    "title":     title.strip(),
                                    "link":      (link if link.startswith("http")
                                                  else "https://finance.yahoo.co.jp"
                                                       + link),
                                    "publisher": "Yahoo Finance Japan",
                                    "ts":        ts,
                                    "date":      str(date_hint or ""),
                                    "source":    "TDnet · Yahoo Japan",
                                    "summary":   "",
                                })
                        for v in node.values():
                            if isinstance(v, (dict, list)):
                                stack.append(v)
                    elif isinstance(node, list):
                        stack.extend(node)
        except Exception:
            pass

        # Fallback: HTML row scrape — Yahoo JP disclosure list is a series of
        # `<li>` / `<a>` rows with date prefixes like "2024/12/19 14:30".
        if len(out) < limit:
            for m in re.finditer(
                    r'<a[^>]+href="(https?://finance\.yahoo\.co\.jp[^"]*'
                    r'(?:disclosure|news)[^"]*)"[^>]*>(.*?)</a>',
                    body, re.S | re.I):
                href = m.group(1).strip()
                inner = _strip_tags(m.group(2))
                if len(inner) < 6:
                    continue
                # Pull a date prefix if present
                dm = re.match(r"^\s*(\d{4}[/.\-]\d{1,2}[/.\-]\d{1,2}"
                              r"(?:\s+\d{1,2}:\d{2})?)\s*",
                              inner)
                date_hint = dm.group(1) if dm else ""
                title = inner[len(date_hint):].strip(" •·-–—") if dm else inner
                if len(title) < 4:
                    continue
                key = href.split("?")[0]
                if key in seen:
                    continue
                seen.add(key)
                out.append({
                    "title":     title,
                    "link":      href,
                    "publisher": "Yahoo Finance Japan",
                    "ts":        0,
                    "date":      date_hint,
                    "source":    "TDnet · Yahoo Japan",
                    "summary":   "",
                })
                if len(out) >= limit:
                    break
    return out[:limit]


def _press_kabutan(sym, limit=30):
    """Kabutan (kabutan.jp) is a popular Japanese financial portal whose
    per-stock news pages aggregate TDnet disclosures and JP press wires.
    Fallback for tickers like MTPLF/3350.T when the company's own IR site is
    JS-rendered."""
    if not sym:
        return []
    sym_u = sym.upper()
    home = None
    info = _foreign_info(sym_u) or {}
    if info.get("home", "").endswith(".T"):
        home = info["home"]
    elif sym_u.endswith(".T"):
        home = sym_u
    if not home:
        return []
    code = home.split(".")[0]
    out = []
    seen = set()
    for url in (f"https://kabutan.jp/disclosures/?code={code}",
                f"https://kabutan.jp/stock/news?code={code}"):
        if len(out) >= limit:
            break
        body = _scrape_fetch(url, ttl=600)
        if not body:
            continue
        # Kabutan rows: <tr><td>2024/12/19</td><td><a href="...">Title</a></td></tr>
        for m in re.finditer(
                r"<tr[^>]*>\s*<t[hd][^>]*>\s*(\d{2,4}[/.\-]\d{1,2}[/.\-]\d{1,2}"
                r"(?:\s+\d{1,2}:\d{2})?)\s*</t[hd]>\s*"
                r"<t[hd][^>]*>(.*?)</t[hd]>",
                body, re.S | re.I):
            date_hint = m.group(1).strip()
            cell = m.group(2)
            am = re.search(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', cell, re.S | re.I)
            href = ""
            title = ""
            if am:
                href = am.group(1).strip()
                title = _strip_tags(am.group(2))
            else:
                title = _strip_tags(cell)
            if not href or len(title) < 4:
                continue
            if href.startswith("/"):
                href = "https://kabutan.jp" + href
            key = href.split("?")[0]
            if key in seen:
                continue
            seen.add(key)
            out.append({
                "title":     title,
                "link":      href,
                "publisher": "Kabutan",
                "ts":        0,
                "date":      date_hint,
                "source":    "TDnet · Kabutan",
                "summary":   "",
            })
            if len(out) >= limit:
                break
    return out[:limit]


def _press_yahoo(sym, limit=30):
    """Scrape Yahoo Finance press-releases page. Yahoo embeds the full stream in
    a <script id="__NEXT_DATA__"> JSON blob. The page itself is already filtered
    to press releases for this ticker, so we keep ALL items it lists rather
    than re-filtering by URL substring (which dropped legitimate items)."""
    if not sym:
        return []
    url = f"https://finance.yahoo.com/quote/{quote(sym)}/press-releases"
    body = _scrape_fetch(url, ttl=600)
    if not body:
        return []
    out = []
    seen_links = set()

    def _accept(node):
        """A node looks like a news item iff it has a non-empty title + link."""
        title = node.get("title")
        link = (node.get("link") or node.get("canonicalUrl") or node.get("url")
                or (node.get("clickThroughUrl") or {}).get("url")
                if isinstance(node.get("clickThroughUrl"), dict) else node.get("url"))
        if not (isinstance(title, str) and title.strip()
                and isinstance(link, str) and link.startswith("http")):
            return None, None
        # Only keep absolute URLs that look like article pages
        if "/news/" not in link and "/quote/" not in link \
                and "press-release" not in link.lower() \
                and "news-release" not in link.lower() \
                and "globenewswire.com" not in link \
                and "prnewswire.com" not in link \
                and "businesswire.com" not in link \
                and "accesswire.com" not in link \
                and "yahoo.com/news" not in link:
            return None, None
        return title, link

    # Extract any __NEXT_DATA__ JSON blob
    try:
        m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', body, re.S)
        if m:
            blob = json.loads(m.group(1))
            stack = [blob]
            while stack and len(out) < limit:
                node = stack.pop()
                if isinstance(node, dict):
                    title, link = _accept(node)
                    if title and link and link not in seen_links:
                        seen_links.add(link)
                        ts = node.get("providerPublishTime") or node.get("pubDate") or 0
                        if isinstance(ts, str):
                            try:
                                ts = int(parsedate_to_datetime(ts).timestamp())
                            except Exception:
                                ts = 0
                        out.append({
                            "title":     title,
                            "link":      link,
                            "publisher": node.get("publisher") or "Yahoo Finance",
                            "ts":        int(ts or 0),
                            "date":      (time.strftime("%Y-%m-%d %H:%M", time.gmtime(int(ts)))
                                           if ts else ""),
                            "source":    "Yahoo",
                            "summary":   node.get("summary") or "",
                        })
                    stack.extend(node.values())
                elif isinstance(node, list):
                    stack.extend(node)
    except Exception as e:
        sys.stderr.write(f"[press-yh {sym}] parse error: {e}\n")

    # Fallback: server-rendered <a> tags within the stream
    if not out:
        for m in re.finditer(
                r'<a[^>]+href="([^"]+)"[^>]*data-test[^>]*>(.*?)</a>',
                body, re.S | re.I):
            link = m.group(1)
            if link.startswith("/"):
                link = "https://finance.yahoo.com" + link
            if "/news/" not in link and "press-release" not in link.lower():
                continue
            if link in seen_links:
                continue
            title = _strip_tags(m.group(2))
            if not title or len(title) < 10:
                continue
            seen_links.add(link)
            out.append({
                "title":     title,
                "link":      link,
                "publisher": "Yahoo Finance",
                "ts":        0,
                "date":      "",
                "source":    "Yahoo",
                "summary":   "",
            })
            if len(out) >= limit:
                break
    return out


def _press_globenewswire_rss(sym, company_name, limit=20):
    """GlobeNewswire press releases. We try BOTH the per-stock-ticker RSS
    feed (https://www.globenewswire.com/RssFeed/stockticker/<TICKER>) and
    the per-organization slug feed. The ticker feed is canonical when the
    issuer distributes via GlobeNewswire."""
    out = []
    seen = set()

    def _ingest(url):
        code, body = _fetch(url, accept="application/rss+xml", timeout=10)
        if code != 200 or not body:
            return
        try:
            root = ET.fromstring(body)
            for it in root.iter("item"):
                title = html.unescape((it.findtext("title") or "").strip())
                link = (it.findtext("link") or "").strip()
                pdate = (it.findtext("pubDate") or "").strip()
                desc = html.unescape((it.findtext("description") or "").strip())
                if not title or not link:
                    continue
                key = link.split("?")[0]
                if key in seen:
                    continue
                seen.add(key)
                ts = 0
                try:
                    ts = int(parsedate_to_datetime(pdate).timestamp())
                except Exception:
                    pass
                out.append({
                    "title":     title,
                    "link":      link,
                    "publisher": "GlobeNewswire",
                    "ts":        ts,
                    "date":      time.strftime("%Y-%m-%d %H:%M", time.gmtime(ts)) if ts else "",
                    "source":    "GlobeNewswire",
                    "summary":   _strip_tags(desc)[:300],
                })
                if len(out) >= limit:
                    return
        except Exception as e:
            sys.stderr.write(f"[press-gnw {url}] parse error: {e}\n")

    if sym:
        _ingest(f"https://www.globenewswire.com/RssFeed/stockticker/{quote(sym.upper())}")
    if company_name and len(out) < limit:
        slug = re.sub(r"[^A-Za-z0-9]+", "-", company_name.strip()).strip("-")
        if slug:
            _ingest(f"https://www.globenewswire.com/RssFeed/organization/{quote(slug)}")
    return out


def _press_prnewswire(sym, company_name=None, limit=20):
    """PR Newswire per-ticker page: https://www.prnewswire.com/news/<ticker>/
    Each card links to /news-releases/<slug>.html which is a real press release."""
    if not sym:
        return []
    out = []
    seen = set()
    candidates = [f"https://www.prnewswire.com/news/{sym.lower()}/"]
    # Some companies are filed under their slugified name on PRN
    if company_name:
        slug = re.sub(r"[^A-Za-z0-9]+", "-", company_name.strip()).strip("-").lower()
        if slug and slug != sym.lower():
            candidates.append(f"https://www.prnewswire.com/news/{slug}/")

    for url in candidates:
        body = _scrape_fetch(url, ttl=600)
        if not body:
            continue
        # Match anchors to /news-releases/<slug>.html with their displayed text
        for m in re.finditer(
                r'<a[^>]+href="(/news-releases/[^"#?]+\.html)"[^>]*>(.*?)</a>',
                body, re.S | re.I):
            href = m.group(1)
            title = _strip_tags(m.group(2))
            if not title or len(title) < 12:
                continue
            link = "https://www.prnewswire.com" + href
            if link in seen:
                continue
            seen.add(link)
            out.append({
                "title":     title,
                "link":      link,
                "publisher": "PR Newswire",
                "ts":        0,
                "date":      "",
                "source":    "PR Newswire",
                "summary":   "",
            })
            if len(out) >= limit:
                return out
    return out


def _press_businesswire(sym, company_name=None, limit=20):
    """Business Wire keyword search. BW doesn't expose a per-ticker RSS publicly
    so we search by the most distinctive term (company name preferred, ticker fallback)
    and rely on the title-match filter in press_releases() to drop unrelated hits."""
    term = (company_name or sym or "").strip()
    if not term:
        return []
    url = ("https://www.businesswire.com/portal/site/home/news/"
           f"?ndmViewId=news_view&searchType=news&searchTerm={quote(term)}")
    body = _scrape_fetch(url, ttl=600)
    if not body:
        return []
    out = []
    seen = set()
    for m in re.finditer(
            r'<a[^>]+href="(/news/home/[^"#?]+)"[^>]*>(.*?)</a>',
            body, re.S | re.I):
        href = m.group(1)
        title = _strip_tags(m.group(2))
        if not title or len(title) < 12:
            continue
        # Skip nav links to category roots ("/news/home/business")
        if re.match(r"/news/home/[a-z]+/?$", href, re.I):
            continue
        link = "https://www.businesswire.com" + href
        if link in seen:
            continue
        seen.add(link)
        out.append({
            "title":     title,
            "link":      link,
            "publisher": "Business Wire",
            "ts":        0,
            "date":      "",
            "source":    "Business Wire",
            "summary":   "",
        })
        if len(out) >= limit:
            break
    return out


def _press_nasdaq(sym, limit=20):
    """Nasdaq.com press releases via api.nasdaq.com. Works for any Nasdaq/NYSE
    ticker with no API key. We try BOTH the dedicated press_release topic AND
    the general articlebysymbol topic (filtered to press-release entries) — many
    issuers get tagged inconsistently across the two topics, so combining both
    catches more.

    Uses the existing _nasdaq_fetch helper which sets the Referer + Origin
    headers Nasdaq requires (plain GETs return 403)."""
    if not sym:
        return []
    s = sym.lower()
    out = []
    seen = set()

    def _ingest(url, force_pr=False):
        data = _nasdaq_fetch(url) or {}
        rows = ((data.get("data") or {}).get("rows")) or []
        for r in rows:
            title = (r.get("title") or "").strip()
            href = (r.get("url") or "").strip()
            if not title or not href:
                continue
            # Skip non-PR items when pulling from the general feed.
            # Normalise the publisher/topic/category string by stripping
            # whitespace so "Business Wire" matches "businesswire" etc.
            cat_raw = (r.get("topic") or r.get("category") or r.get("publisher") or "").lower()
            cat = re.sub(r"\s+", "", cat_raw)
            wire_kw = ("press", "release", "newswire", "globenewswire",
                       "businesswire", "prnewswire", "accesswire", "newsfile")
            if (not force_pr) and not any(kw in cat for kw in wire_kw):
                continue
            link = href if href.startswith("http") else f"https://www.nasdaq.com{href}"
            key = link.split("?")[0]
            if key in seen:
                continue
            seen.add(key)
            d = (r.get("publishDate") or r.get("created") or "").strip()
            ts = 0
            for fmt in ("%b %d, %Y", "%B %d, %Y", "%Y-%m-%dT%H:%M:%S",
                        "%Y-%m-%d", "%m/%d/%Y"):
                try:
                    ts = int(time.mktime(time.strptime(d.split("+")[0].strip(), fmt)))
                    break
                except Exception:
                    pass
            out.append({
                "title":     title,
                "link":      link,
                "publisher": (r.get("publisher") or "").strip() or "Nasdaq.com",
                "ts":        ts,
                "date":      d or (r.get("ago") or ""),
                "source":    "Nasdaq",
                "summary":   (r.get("summary") or "").strip(),
            })
            if len(out) >= limit:
                return

    # Topic 1: dedicated press_release feed (most precise, smaller volume)
    _ingest(("https://api.nasdaq.com/api/news/topic/press_release"
             f"?q=symbol:{quote(s)}|assetclass:stocks&limit={int(limit)}&offset=0"),
            force_pr=True)
    # Topic 2: general articlebysymbol feed, filtered to PR-shaped entries
    if len(out) < limit:
        _ingest(("https://api.nasdaq.com/api/news/topic/articlebysymbol"
                 f"?q={quote(s)}|stocks&limit={int(limit)}&offset=0&sortby=newestPublishDate"),
                force_pr=False)
    return out


# 8-K Items that typically attach the press release as Exhibit 99.1.
# 1.01 Material Definitive Agreement   2.02 Results of Operations & Financial Cond.
# 7.01 Reg FD Disclosure               8.01 Other Events
# 5.02 Officers & Directors            5.03 Amendments to Articles
# 5.07 Shareholder Vote Results        2.01 Completion of Acquisition / Disposition
# 2.03 Material Direct Financial Obligation   1.02 Termination of Material Agreement
PRESS_8K_ITEMS = {"1.01", "1.02", "2.01", "2.02", "2.03",
                  "5.02", "5.03", "5.07",
                  "7.01", "8.01", "8.02"}

# Item-code → human-readable label (so titles read like real PRs, not raw codes)
_ITEM_LABELS = {
    "1.01": "Material Agreement",
    "1.02": "Termination of Material Agreement",
    "2.01": "Completion of Acquisition",
    "2.02": "Results of Operations",
    "2.03": "Material Direct Financial Obligation",
    "3.01": "Notice of Delisting",
    "3.02": "Unregistered Sales of Equity",
    "3.03": "Material Modification to Rights of Security Holders",
    "5.02": "Officers & Directors",
    "5.03": "Amendments to Articles",
    "5.07": "Shareholder Vote Results",
    "7.01": "Reg FD Disclosure",
    "8.01": "Other Events",
    "8.02": "Other Events",
    "9.01": "Financial Statements & Exhibits",
}


def _press_marketwatch(sym, limit=20):
    """MarketWatch per-ticker press-releases page. Server-rendered HTML, covers
    virtually any US-listed ticker. URL: /investing/stock/<sym>/press-releases.
    The page lists wire-feed PRs (PR Newswire, Business Wire, GlobeNewswire) for
    the ticker, so it's a single uniform feed regardless of which wire the
    issuer uses."""
    if not sym:
        return []
    url = f"https://www.marketwatch.com/investing/stock/{quote(sym.lower())}/press-releases"
    body = _scrape_fetch(url, ttl=600)
    if not body:
        return []
    out = []
    seen = set()
    # MarketWatch markup: each row is a <div class="article__content"> containing
    # an <a class="link"> with the title and a sibling <time class="article__timestamp">.
    # Be lenient — match any anchor whose href looks like a press-release article.
    for m in re.finditer(
            r'<a[^>]+href="(https?://www\.marketwatch\.com/press-release/[^"#?]+)"[^>]*>(.*?)</a>',
            body, re.S | re.I):
        link = m.group(1)
        title = _strip_tags(m.group(2)).strip()
        if not title or len(title) < 12:
            continue
        if link in seen:
            continue
        seen.add(link)
        out.append({
            "title":     title,
            "link":      link,
            "publisher": "MarketWatch",
            "ts":        0,
            "date":      "",
            "source":    "MarketWatch",
            "summary":   "",
        })
        if len(out) >= limit:
            break
    return out


def _press_stockanalysis(sym, limit=20):
    """stockanalysis.com per-ticker press-releases page. Server-rendered HTML,
    works for any US listing. URL: /stocks/<sym>/press/."""
    if not sym:
        return []
    url = f"https://stockanalysis.com/stocks/{quote(sym.lower())}/press/"
    body = _scrape_fetch(url, ttl=600)
    if not body:
        return []
    out = []
    seen = set()
    # SA's press list is in a server-rendered <table> with rows linking to /press/<id>/
    # AND each row also includes a date and the source publisher.
    for m in re.finditer(
            r'<a[^>]+href="(/stocks/[^"]+/press/[^"#?]+/)"[^>]*>(.*?)</a>',
            body, re.S | re.I):
        href = m.group(1)
        title = _strip_tags(m.group(2)).strip()
        if not title or len(title) < 12:
            continue
        link = "https://stockanalysis.com" + href
        if link in seen:
            continue
        seen.add(link)
        out.append({
            "title":     title,
            "link":      link,
            "publisher": "Stockanalysis",
            "ts":        0,
            "date":      "",
            "source":    "Stockanalysis",
            "summary":   "",
        })
        if len(out) >= limit:
            break
    # Fallback: SA also embeds press data in a __NEXT_DATA__ JSON blob on some pages
    if not out:
        try:
            nm = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
                           body, re.S | re.I)
            if nm:
                blob = json.loads(nm.group(1))
                stack = [blob]
                while stack and len(out) < limit:
                    node = stack.pop()
                    if isinstance(node, dict):
                        title = node.get("title")
                        link = (node.get("link") or node.get("url") or
                                node.get("href") or "")
                        d = (node.get("date") or node.get("published") or
                             node.get("pubDate") or "")
                        if (isinstance(title, str) and len(title) > 12 and
                                isinstance(link, str) and link.startswith("http")
                                and link not in seen):
                            seen.add(link)
                            out.append({
                                "title":     title,
                                "link":      link,
                                "publisher": "Stockanalysis",
                                "ts":        0,
                                "date":      d if isinstance(d, str) else "",
                                "source":    "Stockanalysis",
                                "summary":   "",
                            })
                        stack.extend(node.values())
                    elif isinstance(node, list):
                        stack.extend(node)
        except Exception as e:
            sys.stderr.write(f"[press-sa {sym}] parse error: {e}\n")
    return out


def _press_sec_8k(sym, limit=20):
    """SEC 8-K filings with press-release items (1.01, 2.02, 7.01, 8.01, 5.02, etc.)
    typically attach the actual press release as Exhibit 99.1. We filter to those
    items, then best-effort resolve the Exhibit 99.1 URL via the filing's
    index.json so the link points at the press release itself, not a directory.

    For 6-K filings (foreign private issuers) we surface them all since 6-Ks are
    the canonical international company disclosure form."""
    info = sec_get_cik(sym)
    if not info:
        return []
    cik, _name = info
    data = sec_submissions(cik)
    if not data:
        return []
    recent = ((data.get("filings") or {}).get("recent") or {})
    forms = recent.get("form") or []
    accs  = recent.get("accessionNumber") or []
    dates = recent.get("filingDate") or []
    pribr = recent.get("primaryDocument") or []
    items = recent.get("items") or []
    out = []
    cik_int = int(cik)
    for i, form in enumerate(forms):
        if form not in ("8-K", "8-K/A", "6-K", "6-K/A"):
            continue
        acc = accs[i] if i < len(accs) else ""
        date = dates[i] if i < len(dates) else ""
        primary = pribr[i] if i < len(pribr) else ""
        item = items[i] if i < len(items) else ""
        if not acc:
            continue

        # 8-K: filter to items that typically attach a press release.
        # 6-K: keep all (foreign issuers don't itemize, every 6-K is a disclosure).
        item_codes = set(re.findall(r"\d+\.\d+", item or ""))
        if form.startswith("8-K") and not (item_codes & PRESS_8K_ITEMS):
            continue

        acc_nodash = acc.replace("-", "")
        primary_url = (f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}/{primary}"
                       if primary else
                       f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}/")

        # Resolve direct Exhibit 99.1 URL via the filing's index.json (cheap + cached).
        # Falls back to the primary doc if we can't find an exhibit.
        link = primary_url
        try:
            idx_json_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}/index.json"
            idx = _sec_fetch(idx_json_url) or {}
            best = None
            for f in ((idx.get("directory") or {}).get("item") or []):
                name = (f.get("name") or "")
                lname = name.lower()
                # Exhibit 99.1 (in any of its many filename variants) and only HTML/PDF
                if (re.search(r"(?:^|[/_-])(?:ex|exhibit)[\s_-]*99[._-]?1(?:[._-]|$)", lname)
                        and (lname.endswith(".htm") or lname.endswith(".html") or lname.endswith(".pdf"))):
                    best = name
                    break
            if best:
                link = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}/{best}"
        except Exception:
            pass

        ts = 0
        try:
            ts = int(time.mktime(time.strptime(date, "%Y-%m-%d")))
        except Exception:
            pass

        # Build a human-readable title from item codes when available.
        if item_codes:
            labels = []
            for code in sorted(item_codes):
                labels.append(_ITEM_LABELS.get(code, f"Item {code}"))
            # Dedupe while preserving order
            seen_l = set(); ordered = []
            for lab in labels:
                if lab in seen_l: continue
                seen_l.add(lab); ordered.append(lab)
            title = f"{form}: " + " · ".join(ordered)
        else:
            title = f"{form} Filing"

        out.append({
            "title":     title,
            "link":      link,
            "publisher": "SEC EDGAR",
            "ts":        ts,
            "date":      date,
            "source":    "SEC 8-K",
            "summary":   f"Filing {acc}" + (f" — Items {item}" if item else ""),
        })
        if len(out) >= limit:
            break
    return out


# Sources whose feed URL already isolates by ticker — trust them without applying
# a title-match filter (titles like "Q3 Results" don't mention the ticker but ARE
# the right company's PR because the feed is per-ticker).
_PR_TRUSTED_SOURCES = {
    "SEC 8-K",          # SEC EDGAR per-CIK filings
    "Nasdaq",           # api.nasdaq.com q=symbol:<sym>
    "MarketWatch",      # marketwatch.com/investing/stock/<sym>/press-releases
    "Stockanalysis",    # stockanalysis.com/stocks/<sym>/press/
    "Yahoo",            # finance.yahoo.com/quote/<sym>/press-releases
    "GlobeNewswire",    # /RssFeed/stockticker/<TICKER> (per-ticker) or /RssFeed/organization/<slug>
    "PR Newswire",      # prnewswire.com/news/<ticker>/ (per-ticker page)
    "TDnet",            # JP Tokyo Stock Exchange disclosures
    "Kabutan",          # JP per-ticker disclosures
}


def press_releases(sym, limit=40):
    """Aggregate THIS company's actual press releases (not editorial news) from a
    cross-section of authoritative sources, so it works for ANY public company:

      - SEC 8-K with Exhibit 99.1 (universal for US public cos)
      - PR Newswire per-ticker page
      - Business Wire keyword search
      - GlobeNewswire per-ticker RSS + per-organization RSS
      - Nasdaq.com press_release API (covers Nasdaq/NYSE listings)
      - Yahoo Finance press-releases stream
      - Hardcoded company IR page (PRESS_IR_PAGES) when available
      - JP TDnet (Yahoo Japan) + Kabutan for Tokyo-listed cos / their US ADRs
      - Google News restricted to press-wire publishers (last-resort fallback)

    For sources whose feed URL is itself ticker-scoped (SEC 8-K, Nasdaq, Yahoo
    press-releases, PR Newswire ticker page, GlobeNewswire feeds, TDnet) we trust
    the source filter and keep all results. For unscoped sources (Business Wire
    search, Google News) we additionally require a title match on the ticker or
    the cleaned company name to drop unrelated companies.

    Returns {symbol, companyName, count, sources, items}.
    """
    if not sym:
        return {"symbol": sym, "count": 0, "sources": {}, "items": []}
    cache_key = f"press::{sym.upper()}::{limit}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    # Resolve company display name (preferred for filtering / wire searches)
    company_name = None
    try:
        info = sec_get_cik(sym)
        if info:
            company_name = info[1]
    except Exception:
        pass
    if not company_name:
        # Try FMP profile (works for some non-SEC tickers)
        try:
            prof = fmp_profile(sym) or {}
            company_name = prof.get("companyName") or prof.get("name") or None
        except Exception:
            pass
    if not company_name:
        # Use the IR mapping label (e.g. 'Metaplanet' for 3350.T) when nothing else has a record
        company_name = (PRESS_IR_PAGES.get(sym.upper()) or (None, None))[1]

    tasks = {
        # Universal authoritative sources (work for ANY US-listed ticker)
        "sec":     (lambda: _press_sec_8k(sym, limit=20)),                  # filings + Exhibit 99.1
        "nasdaq":  (lambda: _press_nasdaq(sym, limit=20)),                  # Nasdaq.com
        "mw":      (lambda: _press_marketwatch(sym, limit=20)),             # MarketWatch
        "sa":      (lambda: _press_stockanalysis(sym, limit=20)),           # Stockanalysis.com
        "yahoo":   (lambda: _press_yahoo(sym, limit=25)),                   # Yahoo Finance stream
        # Wire-specific scrapers (cover cos that distribute via that wire)
        "prn":     (lambda: _press_prnewswire(sym, company_name, limit=20)),
        "bw":      (lambda: _press_businesswire(sym, company_name, limit=20)),
        "gnwire":  (lambda: _press_globenewswire_rss(sym, company_name, limit=20)),
        # Company-direct (hardcoded IR pages or fallback /news paths)
        "ir":      (lambda: _press_ir_website(sym, company_name, limit=30)),
        # Japan-specific disclosures (TDnet)
        "yjp":     (lambda: _press_yahoo_japan_disclosures(sym, limit=30)),
        "kabutan": (lambda: _press_kabutan(sym, limit=30)),
        # Last-resort fallback — only consulted when no trusted source returned anything
        "gnews":   (lambda: _press_google_news(sym, company_name, limit=20)),
    }
    results = _parallel(tasks, timeout=14)

    # If ANY trusted source returned items, drop Google News entirely — it returns
    # editorial articles, not press releases, and would only add noise to clean results.
    trusted_keys = ("sec", "nasdaq", "mw", "sa", "yahoo", "prn", "bw", "gnwire",
                    "yjp", "kabutan")
    if any((results.get(k) or []) for k in trusted_keys):
        results["gnews"] = []

    # Priority order. SEC + universal wires first; Google News last.
    merged = []
    for src in ("sec", "nasdaq", "mw", "sa", "prn", "bw", "gnwire",
                "yjp", "kabutan", "ir", "yahoo", "gnews"):
        merged += (results.get(src) or [])

    # Build name-match regexes for sources that DON'T already filter by ticker.
    # IR (hardcoded) and TDnet are URL-authoritative; trusted sources skip
    # the title filter entirely.
    is_hardcoded_ir = sym.upper() in PRESS_IR_PAGES
    cleaned_name = ""
    if company_name:
        cleaned_name = re.sub(
            r"\b(Inc\.?|Corp\.?|Corporation|Company|Co\.?|Ltd\.?|Limited|plc|"
            r"Holdings?|Group|S\.?A\.?|N\.?V\.?)\b\.?",
            "", company_name, flags=re.I).strip().rstrip(",").lower()
    sym_lc = (sym or "").lower()
    sym_re = re.compile(r"\b" + re.escape(sym_lc) + r"\b") if sym_lc else None
    phrases = []
    if company_name:
        full_lc = company_name.lower().rstrip(".")
        phrases.append(full_lc)  # e.g. "apple inc"
    if cleaned_name and (len(cleaned_name) >= 6 or " " in cleaned_name):
        phrases.append(cleaned_name)  # e.g. "metaplanet", "marathon digital"
    phrase_res = [re.compile(r"\b" + re.escape(p) + r"\b") for p in phrases]

    def keep(it):
        src_label = (it.get("source", "") or "")
        # Trusted ticker-scoped sources: keep without title filter.
        if src_label in _PR_TRUSTED_SOURCES:
            return True
        # Hardcoded IR page hits are URL-authoritative.
        if is_hardcoded_ir and src_label.startswith("IR"):
            return True
        # Otherwise require a title hit on the ticker or cleaned company name.
        title_l = (it.get("title") or "").lower()
        if not title_l:
            return False
        if sym_re and sym_re.search(title_l):
            return True
        for pr in phrase_res:
            if pr.search(title_l):
                return True
        return False

    seen_links = set()
    seen_titles = set()
    unique = []
    for it in merged:
        link = (it.get("link") or "").split("?")[0]
        title = (it.get("title") or "").strip().lower()
        if not link or not title:
            continue
        if not keep(it):
            continue
        if link in seen_links:
            continue
        # SEC titles repeat ("8-K: Reg FD Disclosure") — dedupe on link only for SEC.
        if (it.get("source") or "") != "SEC 8-K" and title in seen_titles:
            continue
        seen_links.add(link)
        seen_titles.add(title)
        unique.append(it)

    unique.sort(key=lambda x: x.get("ts") or 0, reverse=True)
    unique = unique[: int(limit)]

    out = {
        "symbol":       sym.upper(),
        "companyName":  company_name,
        "count":        len(unique),
        "sources":      {k: len(v or []) for k, v in results.items() if v},
        "items":        unique,
    }
    _cache_put(cache_key, out, ttl=300)
    return out


# ---------- SEC submissions (company profile, recent filings) ----------
def sec_submissions(cik):
    """Fetch SEC submissions.json: company address, SIC, industry, exchanges, recent filings."""
    cache_key = f"sec_sub::{cik}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    data = _sec_fetch(f"https://data.sec.gov/submissions/CIK{cik}.json")
    # Submissions metadata changes rarely intraday — cache 6h
    _cache_put(cache_key, data or {}, ttl=21600)
    return data


# SIC code -> rough sector (first 2-3 digits drive the bucket)
def _sic_to_sector(sic):
    if sic is None:
        return None
    try:
        s = int(sic)
    except Exception:
        return None
    if  100 <= s <  1000: return "Agriculture, Forestry & Fishing"
    if 1000 <= s <  1500: return "Mining"
    if 1500 <= s <  1800: return "Construction"
    if 2000 <= s <  4000: return "Manufacturing"
    if 4000 <= s <  4900: return "Transportation"
    if 4900 <= s <  5000: return "Utilities"
    if 5000 <= s <  5200: return "Wholesale Trade"
    if 5200 <= s <  6000: return "Retail Trade"
    if 6000 <= s <  6800: return "Finance, Insurance & Real Estate"
    if 7000 <= s <  9000: return "Services"
    if 9000 <= s < 10000: return "Public Administration"
    return None


def sec_build_profile(ticker):
    """Assemble Yahoo-shaped assetProfile + secFilings from SEC submissions."""
    info = sec_get_cik(ticker)
    if not info:
        return {}
    cik, name = info
    sub = sec_submissions(cik)
    if not sub:
        return {}
    addresses = sub.get("addresses", {}) or {}
    addr = addresses.get("business") or addresses.get("mailing") or {}
    filings = (sub.get("filings", {}) or {}).get("recent", {}) or {}
    forms        = filings.get("form", []) or []
    dates        = filings.get("filingDate", []) or []
    accessions   = filings.get("accessionNumber", []) or []
    primary_docs = filings.get("primaryDocument", []) or []
    primary_descs= filings.get("primaryDocDescription", []) or []
    recent = []
    cik_int = int(cik)
    for i in range(min(25, len(forms))):
        acc_plain = (accessions[i] if i < len(accessions) else "").replace("-", "")
        doc = primary_docs[i] if i < len(primary_docs) else ""
        url = (f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_plain}/{doc}"
               if acc_plain and doc else "")
        recent.append({
            "date":     dates[i] if i < len(dates) else None,
            "epochDate": _date_stub(dates[i])["raw"] if i < len(dates) and dates[i] else None,
            "type":     forms[i] if i < len(forms) else None,
            "title":    primary_descs[i] if i < len(primary_descs) else forms[i],
            "edgarUrl": url,
        })
    sic = sub.get("sic")
    return {
        "assetProfile": {
            "address1":            addr.get("street1"),
            "city":                addr.get("city"),
            "state":               addr.get("stateOrCountry"),
            "zip":                 addr.get("zipCode"),
            "country":             addr.get("stateOrCountryDescription"),
            "phone":               sub.get("phone"),
            "website":             sub.get("website"),
            "industry":            sub.get("sicDescription"),
            "sector":              _sic_to_sector(sic),
            "longBusinessSummary":
                (f"{name}. SIC {sic}: {sub.get('sicDescription') or 'n/a'}. "
                 f"Incorporated in {sub.get('stateOfIncorporation') or 'n/a'}. "
                 f"Fiscal year ends {sub.get('fiscalYearEnd') or 'n/a'}. "
                 f"Category: {sub.get('category') or 'n/a'}. "
                 f"Exchanges: {', '.join(sub.get('exchanges') or []) or 'n/a'}."),
            "sic":                 sic,
            "sicDescription":      sub.get("sicDescription"),
            "category":            sub.get("category"),
            "fiscalYearEnd":       sub.get("fiscalYearEnd"),
            "stateOfIncorporation":sub.get("stateOfIncorporation"),
            "exchanges":           sub.get("exchanges") or [],
            "tickers":             sub.get("tickers") or [],
            "formerNames":         [fn.get("name") for fn in (sub.get("formerNames") or [])],
            "name":                name,
        },
        "secFilings": {"filings": recent},
        "calendarEvents": {
            "earnings": {
                "earningsDate": [],
                "earningsAverage": None,
                "revenueAverage": None,
            }
        },
    }


# ---------- Key stats: P/E, market cap, book value, dividend yield, ROE, margins ----------
def sec_key_stats(ticker, current_price):
    """Compute valuation and profitability ratios from SEC XBRL + current price."""
    info = sec_get_cik(ticker)
    if not info:
        return {}
    cik, _name = info
    facts = sec_company_facts(cik)
    if not facts:
        return {}

    ANY = ("10-K", "10-K/A", "10-Q", "10-Q/A", "20-F", "20-F/A", "40-F")
    ANN = ("10-K", "10-K/A", "20-F", "20-F/A", "40-F")

    def latest(names, forms):
        r = _extract_concept(facts, names, form_types=forms, top_n=1)
        return r[0]["val"] if r else None

    def last4(names, forms):
        r = _extract_concept(facts, names, form_types=forms, top_n=4)
        return [x["val"] for x in r if x.get("val") is not None]

    shares_out = (latest(["CommonStockSharesOutstanding",
                          "EntityCommonStockSharesOutstanding",
                          "CommonStockSharesIssued"], ANY))
    eps_annual = latest(["EarningsPerShareDiluted", "EarningsPerShareBasic"], ANN)
    equity     = latest(["StockholdersEquity",
                         "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"], ANY)
    revenue_a  = latest(["Revenues",
                         "RevenueFromContractWithCustomerExcludingAssessedTax",
                         "RevenueFromContractWithCustomerIncludingAssessedTax",
                         "SalesRevenueNet"], ANN)
    net_inc_a  = latest(["NetIncomeLoss"], ANN)
    gross_a    = latest(["GrossProfit"], ANN)
    op_inc_a   = latest(["OperatingIncomeLoss"], ANN)
    op_cf_a    = latest(["NetCashProvidedByUsedInOperatingActivities"], ANN)
    capex_a    = latest(["PaymentsToAcquirePropertyPlantAndEquipment"], ANN)
    debt       = latest(["LongTermDebt", "LongTermDebtNoncurrent"], ANY)
    cash       = latest(["CashAndCashEquivalentsAtCarryingValue",
                         "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"], ANY)
    total_assets = latest(["Assets"], ANY)
    total_liab   = latest(["Liabilities"], ANY)
    current_ast  = latest(["AssetsCurrent"], ANY)
    current_lia  = latest(["LiabilitiesCurrent"], ANY)
    divs_ann     = latest(["PaymentsOfDividends", "PaymentsOfDividendsCommonStock"], ANN)
    rd_a         = latest(["ResearchAndDevelopmentExpense"], ANN)
    depr_a       = latest(["DepreciationDepletionAndAmortization",
                           "DepreciationAndAmortization"], ANN)

    # Compose derived values
    p = current_price
    mcap     = (p * shares_out)            if (p and shares_out) else None
    ev       = ((mcap or 0) + (debt or 0) - (cash or 0)) if (mcap or debt or cash) else None
    pe       = (p / eps_annual)            if (p and eps_annual and eps_annual > 0) else None
    book_ps  = (equity / shares_out)       if (equity and shares_out) else None
    pb       = (p / book_ps)               if (p and book_ps and book_ps > 0) else None
    ps_ratio = (mcap / revenue_a)          if (mcap and revenue_a and revenue_a > 0) else None
    rev_ps   = (revenue_a / shares_out)    if (revenue_a and shares_out) else None
    div_ps   = (divs_ann / shares_out)     if (divs_ann and shares_out) else None
    div_yld  = (div_ps / p)                if (div_ps and p) else None
    payout   = (divs_ann / net_inc_a)      if (divs_ann and net_inc_a and net_inc_a > 0) else None
    gross_m  = (gross_a / revenue_a)       if (gross_a and revenue_a and revenue_a > 0) else None
    op_m     = (op_inc_a / revenue_a)      if (op_inc_a and revenue_a and revenue_a > 0) else None
    net_m    = (net_inc_a / revenue_a)     if (net_inc_a and revenue_a and revenue_a > 0) else None
    roe      = (net_inc_a / equity)        if (net_inc_a and equity and equity > 0) else None
    roa      = (net_inc_a / total_assets)  if (net_inc_a and total_assets and total_assets > 0) else None
    cur_ratio= (current_ast / current_lia) if (current_ast and current_lia and current_lia > 0) else None
    debt_eq  = (debt / equity)             if (debt and equity and equity > 0) else None
    fcf      = ((op_cf_a or 0) - (capex_a or 0)) if (op_cf_a is not None or capex_a is not None) else None
    ebitda   = ((op_inc_a or 0) + (depr_a or 0)) if (op_inc_a is not None or depr_a is not None) else None
    ev_ebitda= (ev / ebitda)               if (ev and ebitda and ebitda > 0) else None

    # Revenue growth: compare last two annual
    revs = last4(["Revenues",
                  "RevenueFromContractWithCustomerExcludingAssessedTax",
                  "SalesRevenueNet"], ANN)
    rev_growth = ((revs[0] - revs[1]) / revs[1]) if (len(revs) >= 2 and revs[1]) else None
    nets = last4(["NetIncomeLoss"], ANN)
    earnings_growth = ((nets[0] - nets[1]) / abs(nets[1])) if (len(nets) >= 2 and nets[1]) else None

    return {
        "defaultKeyStatistics": {
            "sharesOutstanding":   _raw(shares_out),
            "floatShares":         _raw(shares_out),
            "marketCap":           _raw(mcap),
            "enterpriseValue":     _raw(ev),
            "enterpriseToRevenue": _raw((ev / revenue_a) if (ev and revenue_a) else None),
            "enterpriseToEbitda":  _raw(ev_ebitda),
            "trailingEps":         _raw(eps_annual),
            "bookValue":           _raw(book_ps),
            "priceToBook":         _raw(pb),
            "forwardEps":          None,
            "pegRatio":             None,
            "profitMargins":       _raw(net_m),
            "52WeekChange":        None,
        },
        "summaryDetail_extra": {
            "trailingPE":                      _raw(pe),
            "forwardPE":                       None,
            "priceToSalesTrailing12Months":    _raw(ps_ratio),
            "dividendRate":                    _raw(div_ps),
            "dividendYield":                   _raw(div_yld),
            "payoutRatio":                     _raw(payout),
        },
        "financialData": {
            "currentPrice":         _raw(p),
            "targetMeanPrice":      None,
            "recommendationMean":   None,
            "recommendationKey":    None,
            "numberOfAnalystOpinions": None,
            "totalCash":            _raw(cash),
            "totalCashPerShare":    _raw((cash / shares_out) if (cash and shares_out) else None),
            "totalDebt":            _raw(debt),
            "debtToEquity":         _raw(debt_eq),
            "totalRevenue":         _raw(revenue_a),
            "revenuePerShare":      _raw(rev_ps),
            "returnOnAssets":       _raw(roa),
            "returnOnEquity":       _raw(roe),
            "grossProfits":         _raw(gross_a),
            "ebitda":               _raw(ebitda),
            "ebitdaMargins":        _raw((ebitda / revenue_a) if (ebitda and revenue_a and revenue_a > 0) else None),
            "operatingCashflow":    _raw(op_cf_a),
            "freeCashflow":         _raw(fcf),
            "earningsGrowth":       _raw(earnings_growth),
            "revenueGrowth":        _raw(rev_growth),
            "grossMargins":         _raw(gross_m),
            "operatingMargins":     _raw(op_m),
            "profitMargins":        _raw(net_m),
            "quickRatio":           _raw(cur_ratio),
            "currentRatio":         _raw(cur_ratio),
            "researchAndDevelopment": _raw(rd_a),
        },
    }


# ---------- 52-week stats from Stooq daily history ----------
def stooq_year_stats(sym):
    """Derive 52W change, 52W high/low, avg volume (10d + 3M), MA50, MA200 from Stooq daily bars."""
    if is_crypto(sym) or sym.startswith("^") or sym.endswith("=X"):
        return {}
    cache_key = f"ystats::{sym.upper()}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    hist = stooq_history(sym, days=260)
    out = {}
    if hist and len(hist) >= 2:
        closes = [b["close"] for b in hist if b.get("close") is not None]
        vols   = [b["volume"] for b in hist if b.get("volume")]
        highs  = [b["high"] for b in hist if b.get("high") is not None]
        lows   = [b["low"] for b in hist if b.get("low") is not None]
        if len(closes) >= 2:
            year_ago = closes[0]
            latest = closes[-1]
            if year_ago:
                out["52WeekChange"] = (latest - year_ago) / year_ago
        if highs:
            out["fiftyTwoWeekHigh"] = max(highs)
        if lows:
            out["fiftyTwoWeekLow"] = min(lows)
        if closes:
            out["fiftyTwoWeekHighChangePercent"] = (
                (closes[-1] - max(highs)) / max(highs) if highs else None)
            out["fiftyTwoWeekLowChangePercent"] = (
                (closes[-1] - min(lows)) / min(lows) if lows and min(lows) else None)
            if len(closes) >= 50:
                out["fiftyDayAverage"] = sum(closes[-50:]) / 50
            if len(closes) >= 200:
                out["twoHundredDayAverage"] = sum(closes[-200:]) / 200
        if vols:
            out["averageVolume"] = int(sum(vols) / len(vols))
            if len(vols) >= 10:
                out["averageVolume10days"] = int(sum(vols[-10:]) / 10)
            if len(vols) >= 63:
                out["averageDailyVolume3Month"] = int(sum(vols[-63:]) / 63)
    _cache_put(cache_key, out, ttl=600)
    return out


# ---------- SEC Form 4 insider transactions ----------
# Short codes documented at https://www.sec.gov/about/forms/form4data.pdf
TXN_CODE_MAP = {
    "P": "Purchase",      "S": "Sale",          "A": "Grant/Award",
    "M": "Option Exercise","D": "Disposition",  "F": "Tax Withholding",
    "I": "Discretionary", "G": "Gift",          "J": "Other",
    "X": "Exercise OOT",  "C": "Conversion",    "V": "Voluntary",
    "W": "Will/Inherit",  "Z": "Voting Trust",  "K": "Equity Swap",
    "L": "Small-Vol",     "U": "Tender",        "H": "Option Expire",
    "O": "Out-of-Money",  "E": "Short-Swing",
}


def _xml_find_text(elem, path):
    """Find text at path; returns empty string if missing."""
    if elem is None:
        return ""
    try:
        found = elem.find(path)
        if found is None:
            return ""
        # Form 4 often nests <value>text</value> under each field
        val = found.find("value")
        if val is not None and val.text:
            return val.text.strip()
        if found.text:
            return found.text.strip()
    except Exception:
        pass
    return ""


def sec_insider_transactions(ticker, limit=40):
    """Fetch up to `limit` recent Form 4 (insider) transactions via SEC submissions + XML parsing.
    Returns list of dicts: {date, name, relation, action, code, shares, price, value, sharesAfter, filedAt, url}."""
    cache_key = f"insider::{ticker.upper()}::{limit}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    info = sec_get_cik(ticker)
    if not info:
        return []
    cik, _name = info
    sub = sec_submissions(cik) or {}
    filings = (sub.get("filings", {}) or {}).get("recent", {}) or {}
    forms = filings.get("form", []) or []
    dates = filings.get("filingDate", []) or []
    accs  = filings.get("accessionNumber", []) or []
    docs  = filings.get("primaryDocument", []) or []
    cik_int = int(cik)

    form4_indices = [i for i, f in enumerate(forms)
                     if f in ("4", "4/A")][:limit]
    # Build (index, url_xml, filing_url, filed_at) metadata up front so we can
    # fetch all Form 4 XMLs in parallel rather than one-at-a-time.
    filing_url = (f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&"
                  f"CIK={cik}&type=4&dateb=&owner=include&count=40")
    xml_jobs = {}  # name -> url
    meta     = {}  # name -> (filed_at, url_xml)
    for i in form4_indices:
        acc_raw   = accs[i] if i < len(accs) else ""
        acc_plain = acc_raw.replace("-", "")
        doc       = docs[i] if i < len(docs) else ""
        filed_at  = dates[i] if i < len(dates) else None
        if not acc_plain or not doc:
            continue
        xml_doc = doc if doc.endswith(".xml") else re.sub(r"\.html?$", ".xml", doc)
        url_xml = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_plain}/{xml_doc}"
        name = f"f4_{i}"
        xml_jobs[name] = (lambda u=url_xml: _sec_fetch_text(u))
        meta[name]     = (filed_at, url_xml)
    bodies = _parallel(xml_jobs, timeout=10, max_workers=8) if xml_jobs else {}

    out = []
    # Preserve original filing order (most recent first)
    ordered_names = [f"f4_{i}" for i in form4_indices if f"f4_{i}" in meta]
    for name in ordered_names:
        body = bodies.get(name) or ""
        filed_at, url_xml = meta[name]
        if not body:
            continue
        try:
            root = ET.fromstring(body)
        except Exception:
            continue

        owner = root.find("reportingOwner")
        if owner is None:
            continue
        name = _xml_find_text(owner, "reportingOwnerId/rptOwnerName")
        rel  = owner.find("reportingOwnerRelationship")
        relation_parts = []
        if rel is not None:
            if _xml_find_text(rel, "isDirector") in ("1", "true"):
                relation_parts.append("Director")
            if _xml_find_text(rel, "isOfficer") in ("1", "true"):
                title = _xml_find_text(rel, "officerTitle")
                relation_parts.append(title or "Officer")
            if _xml_find_text(rel, "isTenPercentOwner") in ("1", "true"):
                relation_parts.append("10% Owner")
            if _xml_find_text(rel, "isOther") in ("1", "true"):
                relation_parts.append(_xml_find_text(rel, "otherText") or "Other")
        relation = ", ".join(relation_parts) or "Insider"

        # Non-derivative (common stock) transactions
        for txn in root.findall(".//nonDerivativeTransaction"):
            date  = _xml_find_text(txn, "transactionDate")
            code  = _xml_find_text(txn, "transactionCoding/transactionCode")
            shares_s = _xml_find_text(txn, "transactionAmounts/transactionShares")
            price_s  = _xml_find_text(txn, "transactionAmounts/transactionPricePerShare")
            ad    = _xml_find_text(txn, "transactionAmounts/transactionAcquiredDisposedCode")
            after_s = _xml_find_text(txn, "postTransactionAmounts/sharesOwnedFollowingTransaction")
            try:
                shares = float(shares_s) if shares_s else None
            except Exception:
                shares = None
            try:
                price = float(price_s) if price_s else None
            except Exception:
                price = None
            try:
                after = float(after_s) if after_s else None
            except Exception:
                after = None
            signed_shares = shares
            if shares is not None and ad == "D":
                signed_shares = -shares
            value = (abs(signed_shares) * price) if (signed_shares is not None and price) else None
            action = TXN_CODE_MAP.get(code, code or "")
            if ad == "D" and code == "S" and action not in ("Purchase", "Sale"):
                action = "Sale"
            out.append({
                "date":         date or filed_at,
                "filedAt":      filed_at,
                "name":         name,
                "relation":     relation,
                "action":       action,
                "code":         code,
                "shares":       signed_shares,
                "sharesAbs":    shares,
                "price":        price,
                "value":        value,
                "sharesAfter":  after,
                "url":          filing_url,
                "xml":          url_xml,
            })
    # Insider filings don't refresh multiple times per hour — cache 2h
    _cache_put(cache_key, out, ttl=7200)
    return out


def _sec_fetch_text(url, timeout=30):
    """Fetch raw text (XML) from SEC. Throttled + 429-aware via _polite_fetch.
    Returns empty string on failure."""
    cache_key = f"sec_text::{url}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    code, body = _polite_fetch(url, accept="application/xml, text/xml, */*",
                                timeout=timeout, ua=SEC_UA)
    if code != 200 or not body:
        body = ""
    # SEC archive bodies (Form 4 XML etc.) are immutable once filed — cache 6h
    _cache_put(cache_key, body, ttl=21600 if body else 120)
    return body


def sec_insider_summary(txns):
    """Aggregate Form 4 transactions into insiderHolders rows and 6-month netSharePurchaseActivity."""
    if not txns:
        return {"insiderHolders": {"holders": []},
                "insiderTransactions": {"transactions": []},
                "netSharePurchaseActivity": {}}

    # Insider Holders: group by name, keep latest txn per person
    by_name = {}
    for t in txns:
        k = t.get("name") or ""
        if not k:
            continue
        prev = by_name.get(k)
        if prev is None or (t.get("date") or "") > (prev.get("date") or ""):
            by_name[k] = t
    holders = []
    for name, latest in by_name.items():
        holders.append({
            "name":             name,
            "relation":         latest.get("relation"),
            "latestTransDate":  _date_stub(latest.get("date")),
            "transactionDescription": latest.get("action"),
            "positionDirect":   _raw(latest.get("sharesAfter")),
            "positionDirectDate": _date_stub(latest.get("date")),
            "url":              latest.get("url"),
        })

    # 6-month net activity
    import datetime as _dt
    cutoff = (_dt.date.today() - _dt.timedelta(days=182)).isoformat()
    buy_shares  = 0
    sell_shares = 0
    buy_count   = 0
    sell_count  = 0
    for t in txns:
        d = t.get("date") or ""
        if d < cutoff:
            continue
        s = t.get("shares")
        code = t.get("code")
        if s is None or code not in ("P", "S", "A", "M"):
            continue
        if s > 0 and code in ("P", "A", "M"):
            buy_shares += s
            buy_count += 1
        elif s < 0 or code == "S":
            sell_shares += abs(s)
            sell_count += 1
    net_shares = buy_shares - sell_shares
    net_count  = buy_count - sell_count

    # Transactions (Yahoo-style) - keep most recent 30
    recent = sorted(txns, key=lambda t: (t.get("date") or ""), reverse=True)[:30]
    tx_block = []
    for t in recent:
        tx_block.append({
            "filerName":       t.get("name"),
            "filerRelation":   t.get("relation"),
            "transactionText": t.get("action"),
            "moneyText":       "",
            "startDate":       _date_stub(t.get("date")),
            "shares":          _raw(t.get("shares")),
            "value":           _raw(t.get("value")),
            "ownership":       "D",
        })

    return {
        "insiderHolders":      {"holders": holders[:25]},
        "insiderTransactions": {"transactions": tx_block},
        "netSharePurchaseActivity": {
            "period":           "6m",
            "buyInfoCount":     _raw(buy_count),
            "buyInfoShares":    _raw(buy_shares),
            "buyPercentInsiderShares": None,
            "sellInfoCount":    _raw(sell_count),
            "sellInfoShares":   _raw(sell_shares),
            "sellPercentInsiderShares": None,
            "netInfoCount":     _raw(net_count),
            "netInfoShares":    _raw(net_shares),
            "netPercentInsiderShares": None,
            "totalInsiderShares": None,
        },
    }


# ---------- SEC peers by SIC code ----------
def sec_peers(ticker, limit=15):
    """Find companies sharing the same SIC code via EDGAR full-text search."""
    cache_key = f"peers::{ticker.upper()}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    info = sec_get_cik(ticker)
    if not info:
        return []
    cik, _name = info
    sub = sec_submissions(cik) or {}
    sic = sub.get("sic")
    if not sic:
        _cache_put(cache_key, [], ttl=3600)
        return []
    url = (f"https://efts.sec.gov/LATEST/search-index?q=&forms=10-K&dateRange=custom"
           f"&startdt=2023-01-01&enddt=2026-12-31&SIC={sic}")
    data = _sec_fetch(url) or {}
    hits = ((data.get("hits", {}) or {}).get("hits", [])) or []
    seen = set()
    out = []
    # Reuse the loaded SEC tickers map to turn CIKs into symbols
    with _sec_tickers_lock:
        cik2sym = {}
        if _sec_tickers_map:
            for sym, (ck, _n) in _sec_tickers_map.items():
                cik2sym[int(ck)] = sym
    for hit in hits:
        src = hit.get("_source", {}) or {}
        display_names = src.get("display_names") or []
        ciks = src.get("ciks") or []
        for dn, dk in zip(display_names, ciks):
            try:
                dk_int = int(dk)
            except Exception:
                continue
            if dk_int == int(cik):
                continue
            if dk_int in seen:
                continue
            seen.add(dk_int)
            m = re.match(r"^(.*?)\s*\(([A-Z0-9.\-]+)\)\s*\(CIK\s+\d+\)\s*$", dn)
            if m:
                name, sym = m.group(1).strip(), m.group(2).strip()
            else:
                name = dn
                sym = cik2sym.get(dk_int, "")
            out.append({
                "symbol": sym or f"CIK{dk_int}",
                "name":   name,
                "cik":    dk_int,
            })
            if len(out) >= limit:
                break
        if len(out) >= limit:
            break
    # Peers via SIC rarely change — cache 24h
    _cache_put(cache_key, out, ttl=86400)
    return out


# ---------- Dividend history from XBRL ----------
def sec_dividend_history(ticker, limit=20):
    """Extract CommonStockDividendsPerShareDeclared (or Paid) per period from XBRL."""
    cache_key = f"divs::{ticker.upper()}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    info = sec_get_cik(ticker)
    if not info:
        return []
    cik, _ = info
    facts = sec_company_facts(cik)
    if not facts:
        return []
    rows = _extract_concept(
        facts,
        ["CommonStockDividendsPerShareDeclared",
         "CommonStockDividendsPerShareCashPaid"],
        form_types=("10-Q", "10-Q/A", "10-K", "10-K/A"),
        top_n=limit,
    )
    out = []
    for r in rows:
        end = r.get("end")
        val = r.get("val")
        if val is None:
            continue
        out.append({
            "exDate":        _date_stub(end),
            "amount":        _raw(val),
            "form":          r.get("form"),
        })
    # Dividend declarations are quarterly — cache 6h
    _cache_put(cache_key, out, ttl=21600)
    return out


# ---------- Nasdaq public JSON API (analyst estimates, targets, ratings, earnings) ----------
# These are the same undocumented endpoints the nasdaq.com frontend uses.
# No auth required; UA + Origin + Referer headers avoid 403s.
NASDAQ_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")


def _nasdaq_fetch(url, timeout=15):
    """GET a Nasdaq public JSON endpoint. Throttled + 429-aware via _polite_fetch."""
    cache_key = f"nsd::{url}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    code, body = _polite_fetch(url, accept="application/json, text/plain, */*",
                                referer="https://www.nasdaq.com/", timeout=timeout,
                                extra_headers={"Origin": "https://www.nasdaq.com"})
    data = None
    if code == 200 and body:
        try:
            data = json.loads(body)
        except Exception as e:
            sys.stderr.write(f"[nasdaq parse] {url} -> {e}\n")
    elif code not in (-1, 200):
        sys.stderr.write(f"[nasdaq] {url} -> code={code}\n")
    _cache_put(cache_key, data, ttl=900)  # 15 min; analyst data changes slowly
    return data


def _parse_money(s):
    """'$215.37' or '215.37' or '$1.2B' -> float. Returns None if unparseable."""
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return float(s)
    t = str(s).strip().replace("$", "").replace(",", "").replace("%", "")
    if not t or t in ("-", "--", "N/A", "n/a"):
        return None
    mul = 1.0
    if t[-1:].upper() in ("K", "M", "B", "T"):
        mul = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}[t[-1:].upper()]
        t = t[:-1]
    try:
        return float(t) * mul
    except Exception:
        return None


def _parse_int(s):
    """Parse '1,234,567' or '1.2M' or number; returns int or None."""
    v = _parse_money(s)
    return int(v) if v is not None else None


def _parse_nasdaq_date(s):
    """Parse 'Oct 31, 2024' or 'Oct. 31, 2024' or 'MM/DD/YYYY' -> ISO date string."""
    if not s:
        return None
    import datetime as _dt
    s = str(s).strip().replace(".", "")
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return _dt.datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            continue
    return None


def nasdaq_next_earnings(sym):
    """Upcoming earnings date + consensus EPS estimate + annual forecast (equity only)."""
    if is_crypto(sym) or sym.startswith("^") or sym.endswith("=X"):
        return {}
    u = f"https://api.nasdaq.com/api/company/{sym.upper()}/earnings-date"
    d = (_nasdaq_fetch(u) or {}).get("data") or {}
    if not d:
        # Alt: some tickers only expose forecast endpoint
        u2 = f"https://api.nasdaq.com/api/quote/{sym.upper()}/earnings-forecast"
        d = (_nasdaq_fetch(u2) or {}).get("data") or {}
        if not d:
            return {}
    earnings_date = _parse_nasdaq_date(d.get("earningsDate") or d.get("announcementDate"))
    consensus = _parse_money(d.get("consensusEPSForecast")
                             or (d.get("eps") or {}).get("consensusEPSForecast"))
    annual = _parse_money(d.get("annualEarningsForecast")
                          or (d.get("eps") or {}).get("annualEarningsForecast"))
    lastq = _parse_money(d.get("lastQuarterlyEPS")
                         or (d.get("eps") or {}).get("lastQuarterlyEPS"))
    return {
        "nextEarningsDate":        earnings_date,
        "epsForecastConsensus":    consensus,
        "epsForecastAnnual":       annual,
        "lastQuarterlyEPS":        lastq,
    }


def _finviz_earnings_date(sym):
    """Pull earnings string from Finviz snapshot ('Apr 24 AMC' style) -> ISO date + time-of-day."""
    fv = finviz_quote(sym)
    if not fv:
        return None, None
    raw = (fv.get("Earnings") or "").strip()
    if not raw or raw in ("-", "N/A"):
        return None, None
    # Examples: "Apr 24 AMC", "Apr 24/a", "Oct 31 BMO", "Oct 31/b", "Apr 24 AMC*"
    import datetime as _dt, re as _re
    tod = None
    s = raw
    if "AMC" in s.upper() or "/a" in s.lower():
        tod = "AMC"
    elif "BMO" in s.upper() or "/b" in s.lower():
        tod = "BMO"
    # Strip trailing markers
    s = _re.sub(r"[/\s]+(AMC|BMO|a|b)\*?$", "", s, flags=_re.I).strip()
    s = _re.sub(r"\*$", "", s).strip()
    # Finviz omits the year — assume the next occurrence of MMM DD from today
    today = _dt.date.today()
    for fmt in ("%b %d", "%B %d"):
        try:
            d = _dt.datetime.strptime(s, fmt).date()
            d = d.replace(year=today.year)
            # If it already passed by >7 days, roll to next year (post-FY)
            if (today - d).days > 7:
                d = d.replace(year=today.year + 1)
            return d.isoformat(), tod
        except Exception:
            continue
    return None, None


def _yh_html_earnings_date(sym):
    """Pull next earnings date from Yahoo HTML scrape (calendarEvents)."""
    try:
        merged = yh_html_modules(sym, modules="key-statistics,analysis")
    except Exception:
        return None
    cal = (merged or {}).get("calendarEvents") or {}
    e = cal.get("earnings") or {}
    dates = e.get("earningsDate") or []
    if not dates:
        return None
    first = dates[0]
    if isinstance(first, dict):
        # Yahoo gives {"raw": epoch, "fmt": "YYYY-MM-DD"}
        if first.get("fmt"):
            return first["fmt"][:10]
        if first.get("raw"):
            try:
                import datetime as _dt
                return _dt.date.fromtimestamp(int(first["raw"])).isoformat()
            except Exception:
                return None
    return None


def next_earnings_date(sym):
    """Multi-source upcoming earnings date.
    Returns {"date","displayDate","time","epsEstimate","epsForecastAnnual","lastQuarterlyEPS","source"}
    where date is ISO 'YYYY-MM-DD' or None if all sources fail."""
    if is_crypto(sym) or sym.startswith("^") or sym.endswith("=X"):
        return {"date": None, "source": None}

    cache_key = f"next_earnings::{sym.upper()}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    out = {"date": None, "displayDate": None, "time": None,
           "epsEstimate": None, "epsForecastAnnual": None,
           "lastQuarterlyEPS": None, "source": None}

    # 1) Nasdaq (richest payload)
    try:
        ne = nasdaq_next_earnings(sym) or {}
        if ne.get("nextEarningsDate"):
            out["date"]               = ne["nextEarningsDate"]
            out["epsEstimate"]        = ne.get("epsForecastConsensus")
            out["epsForecastAnnual"]  = ne.get("epsForecastAnnual")
            out["lastQuarterlyEPS"]   = ne.get("lastQuarterlyEPS")
            out["source"]             = "nasdaq"
    except Exception:
        pass

    # 2) Finviz (fast, gives time-of-day marker AMC/BMO)
    if not out["date"]:
        try:
            d, tod = _finviz_earnings_date(sym)
            if d:
                out["date"]   = d
                out["time"]   = tod
                out["source"] = "finviz"
        except Exception:
            pass
    elif not out["time"]:
        try:
            _, tod = _finviz_earnings_date(sym)
            if tod:
                out["time"] = tod
        except Exception:
            pass

    # 3) Yahoo HTML scrape (last resort, slower)
    if not out["date"]:
        try:
            d = _yh_html_earnings_date(sym)
            if d:
                out["date"]   = d
                out["source"] = "yahoo-html"
        except Exception:
            pass

    # Pretty display string
    if out["date"]:
        try:
            import datetime as _dt
            d = _dt.date.fromisoformat(out["date"])
            out["displayDate"] = d.strftime("%a, %b %d, %Y")
        except Exception:
            out["displayDate"] = out["date"]

    # Cache 6h: earnings dates change rarely
    _cache_put(cache_key, out, ttl=6 * 3600)
    return out


def nasdaq_price_targets(sym):
    """Analyst target prices (mean/high/low/current) + # analysts."""
    if is_crypto(sym) or sym.startswith("^") or sym.endswith("=X"):
        return {}
    u = f"https://api.nasdaq.com/api/analyst/{sym.upper()}/targetprice"
    d = (_nasdaq_fetch(u) or {}).get("data") or {}
    if not d:
        return {}
    rng = d.get("targetPriceRange") or {}
    return {
        "targetMeanPrice":     _parse_money(rng.get("avgTargetPrice")
                                            or d.get("avgTargetPrice")),
        "targetHighPrice":     _parse_money(rng.get("highTarget")
                                            or d.get("highTargetPrice")),
        "targetLowPrice":      _parse_money(rng.get("lowTarget")
                                            or d.get("lowTargetPrice")),
        "targetMedianPrice":   _parse_money(rng.get("medianTarget")
                                            or d.get("medianTargetPrice")),
        "currentPrice":        _parse_money(rng.get("currentTargetPrice")),
        "numberOfAnalysts":    _parse_int(d.get("numberOfAnalysts")
                                          or d.get("noOfAnalysts")),
    }


def nasdaq_ratings(sym):
    """Analyst recommendation split + current consensus (strongBuy..strongSell)."""
    if is_crypto(sym) or sym.startswith("^") or sym.endswith("=X"):
        return {}
    u = f"https://api.nasdaq.com/api/analyst/{sym.upper()}/ratings"
    d = (_nasdaq_fetch(u) or {}).get("data") or {}
    if not d:
        return {}
    def _cnt(node):
        if isinstance(node, dict):
            return _parse_int(node.get("value"))
        return _parse_int(node)
    current = d.get("currentRatings") or d
    trend = []
    for row in (d.get("ratings") or []):
        trend.append({
            "period":     row.get("period") or row.get("date"),
            "strongBuy":  _cnt(row.get("strongBuy")),
            "buy":        _cnt(row.get("buy")),
            "hold":       _cnt(row.get("hold")),
            "sell":       _cnt(row.get("sell")),
            "strongSell": _cnt(row.get("strongSell")),
        })
    # Compute a numeric mean (1=Strong Buy .. 5=Strong Sell) like Yahoo
    sb = _cnt(current.get("strongBuy"))  or 0
    bb = _cnt(current.get("buy"))        or 0
    hd = _cnt(current.get("hold"))       or 0
    sl = _cnt(current.get("sell"))       or 0
    ss = _cnt(current.get("strongSell")) or 0
    total = sb + bb + hd + sl + ss
    mean = ((1*sb + 2*bb + 3*hd + 4*sl + 5*ss) / total) if total else None
    key = None
    if mean is not None:
        key = ("strong_buy" if mean < 1.5 else
               "buy"        if mean < 2.5 else
               "hold"       if mean < 3.5 else
               "sell"       if mean < 4.5 else "strong_sell")
    return {
        "current": {
            "strongBuy":  sb, "buy": bb, "hold": hd, "sell": sl, "strongSell": ss,
            "total":      total,
            "mean":       mean,
            "key":        key,
        },
        "trend": trend,
    }


def nasdaq_earnings_surprise(sym):
    """Last 4-8 quarters of actual vs estimate EPS."""
    if is_crypto(sym) or sym.startswith("^") or sym.endswith("=X"):
        return []
    u = f"https://api.nasdaq.com/api/company/{sym.upper()}/earnings-surprise"
    d = (_nasdaq_fetch(u) or {}).get("data") or {}
    rows = d.get("earningsSurpriseTable", {}).get("rows") or d.get("rows") or []
    out = []
    for r in rows:
        out.append({
            "period":       r.get("fiscalQtrEnd") or r.get("period"),
            "dateReported": _parse_nasdaq_date(r.get("dateReported")),
            "epsEstimate":  _parse_money(r.get("consensusForecast") or r.get("estimate")),
            "epsActual":    _parse_money(r.get("reportedEPS") or r.get("actual")),
            "surprise":     _parse_money(r.get("surprise") or r.get("percentageSurprise")),
        })
    return out


def nasdaq_earnings_forecast(sym):
    """Forward quarterly / annual EPS + revenue estimates (avg/low/high, # analysts, growth)."""
    if is_crypto(sym) or sym.startswith("^") or sym.endswith("=X"):
        return {}
    u = f"https://api.nasdaq.com/api/quote/{sym.upper()}/eps"
    d = (_nasdaq_fetch(u) or {}).get("data") or {}
    if not d:
        u2 = f"https://api.nasdaq.com/api/quote/{sym.upper()}/earnings-forecast"
        d = (_nasdaq_fetch(u2) or {}).get("data") or {}
    if not d:
        return {}
    # Nasdaq sometimes returns a table keyed by period
    rows = (d.get("earningsForecastTable", {}) or {}).get("rows") or d.get("rows") or []
    trend = []
    for r in rows:
        trend.append({
            "period":       r.get("fiscalPeriod") or r.get("period"),
            "endDate":      _parse_nasdaq_date(r.get("fiscalEndDate")),
            "epsAvg":       _parse_money(r.get("consensusEPSForecast") or r.get("avgEstimate")),
            "epsLow":       _parse_money(r.get("lowEPSForecast") or r.get("lowEstimate")),
            "epsHigh":      _parse_money(r.get("highEPSForecast") or r.get("highEstimate")),
            "numAnalysts":  _parse_int(r.get("numberOfEstimates") or r.get("numAnalysts")),
            "growth":       _parse_money(r.get("yearOverYearGrowthRate") or r.get("growth")),
            "revenueAvg":   _parse_money(r.get("consensusRevenueForecast") or r.get("avgRevenue")),
        })
    # Revenue table (separate endpoint on Nasdaq)
    ur = f"https://api.nasdaq.com/api/quote/{sym.upper()}/revenue"
    dr = (_nasdaq_fetch(ur) or {}).get("data") or {}
    rev_rows = (dr.get("revenueTable", {}) or {}).get("rows") or []
    revenue_trend = [{
        "period":       r.get("fiscalPeriod") or r.get("period"),
        "endDate":      _parse_nasdaq_date(r.get("fiscalEndDate")),
        "revAvg":       _parse_money(r.get("consensusRevenueForecast") or r.get("avgEstimate")),
        "revLow":       _parse_money(r.get("lowRevenueForecast") or r.get("lowEstimate")),
        "revHigh":      _parse_money(r.get("highRevenueForecast") or r.get("highEstimate")),
        "numAnalysts":  _parse_int(r.get("numberOfEstimates")),
    } for r in rev_rows]
    return {"eps": trend, "revenue": revenue_trend}


def nasdaq_institutional_holdings(sym, limit=15):
    """Top institutional holders with shares, % held, and value."""
    if is_crypto(sym) or sym.startswith("^") or sym.endswith("=X"):
        return {}
    s = sym.upper()
    u_total = f"https://api.nasdaq.com/api/quote/{s}/institutional-holdings?limit=30&type=TOTAL"
    u_hold  = f"https://api.nasdaq.com/api/quote/{s}/institutional-holdings?limit={limit}&type=HOLDINGS"
    d_total = (_nasdaq_fetch(u_total) or {}).get("data") or {}
    d_hold  = (_nasdaq_fetch(u_hold)  or {}).get("data") or {}
    summary = d_total.get("ownershipSummary") or {}
    holdings_rows = ((d_hold.get("holdingsTransactions") or {}).get("table") or {}).get("rows") or []
    activity_rows = ((d_total.get("activePositions") or {}).get("rows") or [])
    top = []
    for r in holdings_rows:
        top.append({
            "organization":   r.get("ownerName") or r.get("organization"),
            "dateReported":   _parse_nasdaq_date(r.get("date") or r.get("dateReported")),
            "sharesHeld":     _parse_int(r.get("sharesHeld")),
            "sharesChange":   _parse_int(r.get("sharesChange")),
            "percentHeld":    _parse_money(r.get("percentageOfSharesOutstanding")
                                           or r.get("percentHeld")),
            "value":          _parse_money(r.get("marketValue") or r.get("value")),
        })

    def _pct(node, key):
        v = node.get(key) if isinstance(node, dict) else None
        return _parse_money((v or {}).get("value") if isinstance(v, dict) else v)

    return {
        "ownershipSummary": {
            "sharesOutstandingPCT": _pct(summary, "SharesOutstandingPCT"),
            "institutionalHoldersTotal":
                _parse_int((summary.get("InstitutionalHolders") or {}).get("value")
                           if isinstance(summary.get("InstitutionalHolders"), dict)
                           else summary.get("InstitutionalHolders")),
            "totalHoldingsValue":
                _parse_money((summary.get("TotalHoldingsValue") or {}).get("value")
                             if isinstance(summary.get("TotalHoldingsValue"), dict)
                             else summary.get("TotalHoldingsValue")),
        },
        "top":          top,
        "newPositions": [r for r in activity_rows if (r.get("positions") or "").lower().startswith("new")],
    }


def nasdaq_dividend_history(sym, limit=20):
    """Supplement XBRL with Nasdaq's dividend history when available."""
    if is_crypto(sym) or sym.startswith("^") or sym.endswith("=X"):
        return []
    u = f"https://api.nasdaq.com/api/quote/{sym.upper()}/dividends?assetclass=stocks"
    d = (_nasdaq_fetch(u) or {}).get("data") or {}
    rows = (d.get("dividends", {}) or {}).get("rows") or []
    out = []
    for r in rows[:limit]:
        out.append({
            "exDate":     _parse_nasdaq_date(r.get("exOrEffDate")),
            "payDate":    _parse_nasdaq_date(r.get("paymentDate")),
            "recordDate": _parse_nasdaq_date(r.get("recordDate")),
            "amount":     _parse_money(r.get("amount")),
            "type":       r.get("type"),
        })
    return out


# ---------- HTML scrapers (stockanalysis.com, Finviz, Yahoo HTML) ----------
# These supplement the API calls when the public APIs return empty results.
SCRAPER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")


def _scrape_fetch(url, timeout=20, ttl=900):
    """Generic HTML GET. Uses rotating UA + per-host throttle + 429 cooldown."""
    cache_key = f"scr::{url}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    code, body = _polite_fetch(
        url, timeout=timeout,
        accept=("text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,*/*;q=0.8"),
        referer="https://www.google.com/",
    )
    if code == -1 or code != 200 or not body:
        body = ""
        if code not in (200, -1):
            sys.stderr.write(f"[scrape {url}] code={code}\n")
    _cache_put(cache_key, body, ttl=ttl)
    return body


def _strip_tags(s):
    return re.sub(r"<[^>]+>", "", html.unescape(s or "")).strip()


def _extract_table_pairs(body, cls=None):
    """Extract <tr><th>k</th><td>v</td></tr> pairs from a table. Returns dict."""
    out = {}
    for m in re.finditer(
            r"<tr[^>]*>\s*<t[hd][^>]*>(.*?)</t[hd]>\s*<t[hd][^>]*>(.*?)</t[hd]>",
            body, re.S | re.I):
        k = _strip_tags(m.group(1))
        v = _strip_tags(m.group(2))
        if k and v and v not in ("-", "--", "N/A"):
            out[k] = v
    return out


# stockanalysis.com: rich, server-rendered tables on every stat page.
def sa_url(ticker, page=""):
    if is_crypto(ticker):
        return None
    # International / OTC ADR → route to /quote/<exchange>/<home-ticker>/
    finfo = _foreign_info(ticker)
    if finfo and finfo.get("slug"):
        return f"https://stockanalysis.com/quote/{finfo['slug']}/{page}"
    t = ticker.upper().replace(".", "-")
    return f"https://stockanalysis.com/stocks/{t.lower()}/{page}"


def _sa_financials_base(ticker):
    """Return the stockanalysis.com base URL (without trailing /financials/ etc)
    for either a US stock or a mapped foreign ticker."""
    finfo = _foreign_info(ticker)
    if finfo and finfo.get("slug"):
        return f"https://stockanalysis.com/quote/{finfo['slug']}"
    t = ticker.upper().replace(".", "-").lower()
    return f"https://stockanalysis.com/stocks/{t}"


def sa_statistics(ticker):
    """Scrape key statistics page; returns {label: value} for dozens of metrics."""
    url = sa_url(ticker, "statistics/")
    if not url:
        return {}
    body = _scrape_fetch(url, ttl=1800)
    if not body:
        return {}
    return _extract_table_pairs(body)


def sa_forecast(ticker):
    """Scrape the analyst forecast page. Returns dict of rows and summary."""
    url = sa_url(ticker, "forecast/")
    if not url:
        return {}
    body = _scrape_fetch(url, ttl=3600)
    if not body:
        return {}
    summary = _extract_table_pairs(body)
    # Extract EPS / Revenue forecast tables by looking for <table> blocks after section anchors
    def parse_table(label_rx):
        m = re.search(rf"{label_rx}.*?<table[^>]*>(.*?)</table>", body, re.S | re.I)
        if not m:
            return []
        tbl = m.group(1)
        # Header row first
        hdr_match = re.search(r"<tr[^>]*>(.*?)</tr>", tbl, re.S | re.I)
        headers = []
        if hdr_match:
            headers = [_strip_tags(c) for c in
                       re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", hdr_match.group(1), re.S | re.I)]
        rows = []
        for rm in re.finditer(r"<tr[^>]*>(.*?)</tr>", tbl, re.S | re.I):
            cells = [_strip_tags(c) for c in
                     re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", rm.group(1), re.S | re.I)]
            if cells and cells != headers:
                rows.append(cells)
        return {"headers": headers, "rows": rows}
    return {
        "summary":   summary,
        "epsTable":  parse_table(r"EPS\s+Forecast") or parse_table(r"Earnings\s+Per\s+Share"),
        "revTable":  parse_table(r"Revenue\s+Forecast") or parse_table(r"Revenue"),
    }


def sa_ratings(ticker):
    """Scrape analyst ratings page."""
    url = sa_url(ticker, "ratings/")
    if not url:
        return {}
    body = _scrape_fetch(url, ttl=3600)
    if not body:
        return {}
    summary = _extract_table_pairs(body)
    # Upgrade/downgrade timeline rows: look for tbody that has Date, Firm, Action columns
    upgrades = []
    tbl_match = re.search(r"<table[^>]*class=\"[^\"]*svelte[^\"]*\"[^>]*>(.*?)</table>", body, re.S | re.I)
    if not tbl_match:
        tbl_match = re.search(r"<table[^>]*>(.*?)</table>", body, re.S | re.I)
    if tbl_match:
        for rm in re.finditer(r"<tr[^>]*>(.*?)</tr>", tbl_match.group(1), re.S | re.I):
            cells = [_strip_tags(c) for c in
                     re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", rm.group(1), re.S | re.I)]
            if len(cells) >= 3 and cells[0].lower() != "date":
                upgrades.append(cells)
    return {"summary": summary, "upgrades": upgrades[:40]}


def sa_institutional(ticker):
    """Scrape top institutional investors page."""
    url = sa_url(ticker, "institutional-investors/")
    if not url:
        return []
    body = _scrape_fetch(url, ttl=7200)
    if not body:
        return []
    out = []
    # Find the table of investors
    for tbl_match in re.finditer(r"<table[^>]*>(.*?)</table>", body, re.S | re.I):
        tbl = tbl_match.group(1)
        if "Investor" not in tbl and "Holder" not in tbl:
            continue
        for rm in re.finditer(r"<tr[^>]*>(.*?)</tr>", tbl, re.S | re.I):
            cells = [_strip_tags(c) for c in
                     re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", rm.group(1), re.S | re.I)]
            if len(cells) >= 4 and cells[0].lower() not in ("investor", "holder", "#"):
                out.append(cells)
        if out:
            break
    return out[:25]


def sa_insider(ticker):
    """Scrape insider trading page."""
    url = sa_url(ticker, "insider-trades/")
    if not url:
        return []
    body = _scrape_fetch(url, ttl=7200)
    if not body:
        return []
    out = []
    for tbl_match in re.finditer(r"<table[^>]*>(.*?)</table>", body, re.S | re.I):
        tbl = tbl_match.group(1)
        if "Filer" not in tbl and "Insider" not in tbl and "Type" not in tbl:
            continue
        for rm in re.finditer(r"<tr[^>]*>(.*?)</tr>", tbl, re.S | re.I):
            cells = [_strip_tags(c) for c in
                     re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", rm.group(1), re.S | re.I)]
            if len(cells) >= 4 and cells[0].lower() not in ("date", "filing date"):
                out.append(cells)
        if out:
            break
    return out[:40]


# ---- stockanalysis.com financial statements (scrape three HTML tables) ----

# Map lowercase stockanalysis metric labels -> our standard row keys.
# Keys use substrings so "Revenue" matches "Revenue" exactly and "Revenue Growth" stays excluded.
_SA_IS_MAP = {
    "revenue":                     "totalRevenue",
    "cost of revenue":             "costOfRevenue",
    "gross profit":                "grossProfit",
    "operating income":            "operatingIncome",
    "operating expenses":          "totalOperatingExpenses",
    "net income":                  "netIncome",
    "research & development":      "researchDevelopment",
    "r&d expenses":                "researchDevelopment",
    "selling, general & admin":    "sellingGeneralAdministrative",
    "sg&a expenses":               "sellingGeneralAdministrative",
    "selling & marketing":         "sellingAndMarketingExpense",
    "general & administrative":    "generalAndAdministrative",
    "income tax":                  "incomeTaxExpense",
    "pretax income":               "incomeBeforeTax",
    "interest expense":            "interestExpense",
    "interest income":             "interestIncome",
    "non-operating income":        "totalOtherIncomeExpenseNet",
    "depreciation & amortization": "depreciationAndAmortization",
    "stock-based compensation":    "stockBasedCompensation",
    "eps (basic)":                 "epsBasic",
    "eps (diluted)":               "epsDiluted",
    "shares outstanding (basic)":  "weightedAvgSharesBasic",
    "shares outstanding (diluted)": "weightedAvgSharesDiluted",
    "ebit":                        "ebit",
    "ebitda":                      "ebitda",
    "free cash flow":              "freeCashFlow",
}
_SA_BS_MAP = {
    "total assets":                "totalAssets",
    "total liabilities":           "totalLiab",
    "shareholders' equity":        "totalStockholderEquity",
    "total equity":                "totalStockholderEquity",
    "cash & equivalents":          "cash",
    "cash & short-term investments": "cashAndShortTermInvestments",
    "short-term investments":      "shortTermInvestments",
    "long-term investments":       "longTermInvestments",
    "inventory":                   "inventory",
    "receivables":                 "netReceivables",
    "accounts payable":            "accountsPayable",
    "total current assets":        "totalCurrentAssets",
    "total current liabilities":   "totalCurrentLiabilities",
    "current debt":                "shortLongTermDebt",
    "long-term debt":              "longTermDebt",
    "total debt":                  "totalDebt",
    "retained earnings":           "retainedEarnings",
    "property, plant & equipment": "propertyPlantEquipment",
    "goodwill":                    "goodWill",
    "goodwill & intangibles":      "intangibleAssets",
    "intangible assets":           "intangibleAssets",
    "deferred revenue":            "deferredRevenue",
    "working capital":             "workingCapital",
    "net cash / debt":             "netDebt",
    "book value per share":        "bookValuePerShare",
}
_SA_CF_MAP = {
    "operating cash flow":         "totalCashFromOperatingActivities",
    "investing cash flow":         "totalCashflowsFromInvestingActivities",
    "financing cash flow":         "totalCashFromFinancingActivities",
    "net income":                  "netIncome",
    "capital expenditures":        "capitalExpenditures",
    "acquisitions":                "acquisitionsNet",
    "dividends paid":              "dividendsPaid",
    "share repurchases":           "repurchaseOfStock",
    "share issuance":              "issuanceOfStock",
    "debt issued":                 "debtIssued",
    "debt repaid":                 "debtRepaid",
    "depreciation & amortization": "depreciation",
    "stock-based compensation":    "stockBasedCompensation",
    "change in receivables":       "changeToAccountReceivables",
    "change in payables":          "changeToAccountsPayable",
    "change in inventory":         "changeToInventory",
    "change in working capital":   "changeToOperatingActivities",
    "free cash flow":              "freeCashFlow",
    "net change in cash":          "changeInCash",
}


def _sa_parse_num(txt):
    """Parse stockanalysis.com numeric cell: '1,234.5M', '-2.3B', '(123)', '15.3%'.
    Returns float or None. Values in the tables are in units of the column header
    (usually 'in millions'), but stockanalysis suffixes them with K/M/B, so we honor
    the suffix when present."""
    if txt is None:
        return None
    s = str(txt).strip().replace(",", "").replace("$", "").replace("\u00A0", " ")
    s = s.replace("\u2212", "-")  # Unicode minus
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]
    if not s or s in ("-", "--", "N/A", "n/a"):
        return None
    mult = 1.0
    if s.endswith("%"):
        s = s[:-1]
        mult = 0.01
    elif s.endswith("K"):
        s = s[:-1]; mult = 1e3
    elif s.endswith("M"):
        s = s[:-1]; mult = 1e6
    elif s.endswith("B"):
        s = s[:-1]; mult = 1e9
    elif s.endswith("T"):
        s = s[:-1]; mult = 1e12
    try:
        v = float(s) * mult
        return -v if neg else v
    except Exception:
        return None


def _sa_parse_financial_table(body):
    """Parse the main table on a stockanalysis.com /financials/ page.
    Returns (period_labels: list[str], rows_by_metric: dict[lowercase_label, list[float|None]]).
    Header row has fiscal years like 'FY 2024' or quarter labels; first column is metric."""
    # Find the first <table> that contains financial data headers
    tbl_match = re.search(r"<table[^>]*>(.*?)</table>", body, re.S | re.I)
    if not tbl_match:
        return [], {}
    tbl = tbl_match.group(1)
    # Header row
    hdr_match = re.search(r"<thead[^>]*>(.*?)</thead>", tbl, re.S | re.I)
    if not hdr_match:
        hdr_match = re.search(r"<tr[^>]*>(.*?)</tr>", tbl, re.S | re.I)
    header_html = hdr_match.group(1) if hdr_match else ""
    headers = [_strip_tags(c) for c in
               re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", header_html, re.S | re.I)]
    # Periods = headers except first (metric column) and potentially ignore "TTM" or "Current"
    periods = headers[1:] if headers else []
    # Body rows
    body_html = re.search(r"<tbody[^>]*>(.*?)</tbody>", tbl, re.S | re.I)
    rows_html = body_html.group(1) if body_html else tbl
    rows = {}
    for rm in re.finditer(r"<tr[^>]*>(.*?)</tr>", rows_html, re.S | re.I):
        cells = [_strip_tags(c) for c in
                 re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", rm.group(1), re.S | re.I)]
        if not cells or len(cells) < 2:
            continue
        metric = cells[0].lower()
        if not metric or metric in ("", "metric"):
            continue
        vals = [_sa_parse_num(c) for c in cells[1:]]
        rows[metric] = vals
    return periods, rows


def _sa_period_to_iso(label):
    """Convert 'FY 2024', 'Dec '24', 'Q3 2024', '2024' to a best-effort ISO date stub.
    Returns string like 'YYYY-12-31' or 'YYYY-MM-DD'."""
    if not label:
        return None
    s = label.strip()
    # e.g. 'FY 2024' -> '2024-12-31' (annual FY default)
    m = re.match(r"FY\s*(\d{4})", s, re.I)
    if m:
        return f"{m.group(1)}-12-31"
    # e.g. '2024' bare
    m = re.match(r"^(19|20)\d{2}$", s)
    if m:
        return f"{s}-12-31"
    # e.g. 'Dec '24' -> 2024-12-31
    m = re.match(r"([A-Za-z]{3})\s*'(\d{2})", s)
    if m:
        mo = {"jan":1, "feb":2, "mar":3, "apr":4, "may":5, "jun":6,
              "jul":7, "aug":8, "sep":9, "oct":10, "nov":11, "dec":12}.get(m.group(1).lower())
        yy = int(m.group(2))
        year = 2000 + yy
        if mo:
            return f"{year:04d}-{mo:02d}-28"
    # e.g. 'Q3 2024' -> approximate quarter end
    m = re.match(r"Q([1-4])\s*(\d{4})", s, re.I)
    if m:
        q = int(m.group(1)); y = int(m.group(2))
        month = {1:3, 2:6, 3:9, 4:12}[q]
        day = {1:31, 2:30, 3:30, 4:31}[q]
        return f"{y:04d}-{month:02d}-{day:02d}"
    # e.g. '2024-09' bare
    m = re.match(r"(\d{4})-(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-28"
    return None


def _sa_scrape_statement(url, field_map):
    """Fetch one SA financials page and convert its table to a Yahoo-shaped list."""
    body = _scrape_fetch(url, ttl=3600)
    if not body:
        return []
    periods, rows = _sa_parse_financial_table(body)
    if not periods or not rows:
        return []
    # Build per-period accumulators
    out_by_period = []
    n = len(periods)
    for i in range(n):
        label = periods[i]
        iso = _sa_period_to_iso(label)
        if not iso:
            continue
        period = {"endDate": _date_stub(iso)}
        for metric_lc, vals in rows.items():
            # Longest-match label in map
            std = None
            for fragment, key in field_map.items():
                if fragment in metric_lc:
                    # Prefer the most specific (longest) fragment
                    if std is None or len(fragment) > std[0]:
                        std = (len(fragment), key)
            if std is None:
                continue
            std_key = std[1]
            if i < len(vals) and vals[i] is not None:
                period[std_key] = _raw(vals[i])
        if len(period) > 1:  # has something besides endDate
            out_by_period.append(period)
    # Newest first
    out_by_period.sort(key=lambda p: p["endDate"]["fmt"] if p.get("endDate") else "", reverse=True)
    return out_by_period[:8]


def sa_financials(ticker):
    """Scrape income/balance/cashflow statements from stockanalysis.com.
    Returns Yahoo-shaped merge block. Returns {} when ticker path is unavailable."""
    url = sa_url(ticker, "financials/")
    if not url:
        return {}
    base = f"{_sa_financials_base(ticker)}/financials"
    urls = {
        "is_ann": f"{base}/",
        "is_qtr": f"{base}/?p=quarterly",
        "bs_ann": f"{base}/balance-sheet/",
        "bs_qtr": f"{base}/balance-sheet/?p=quarterly",
        "cf_ann": f"{base}/cash-flow-statement/",
        "cf_qtr": f"{base}/cash-flow-statement/?p=quarterly",
    }

    def pull(u, mapping):
        try:
            return _sa_scrape_statement(u, mapping)
        except Exception as e:
            sys.stderr.write(f"[sa-fin {u}] {e}\n")
            return []

    results = _parallel({
        "is_a": lambda: pull(urls["is_ann"], _SA_IS_MAP),
        "is_q": lambda: pull(urls["is_qtr"], _SA_IS_MAP),
        "bs_a": lambda: pull(urls["bs_ann"], _SA_BS_MAP),
        "bs_q": lambda: pull(urls["bs_qtr"], _SA_BS_MAP),
        "cf_a": lambda: pull(urls["cf_ann"], _SA_CF_MAP),
        "cf_q": lambda: pull(urls["cf_qtr"], _SA_CF_MAP),
    }, timeout=14)
    total = sum(len(results.get(k) or []) for k in results)
    if total == 0:
        return {}

    return {
        "incomeStatementHistory":            {"incomeStatementHistory": results["is_a"]},
        "incomeStatementHistoryQuarterly":   {"incomeStatementHistory": results["is_q"]},
        "balanceSheetHistory":               {"balanceSheetStatements": results["bs_a"]},
        "balanceSheetHistoryQuarterly":      {"balanceSheetStatements": results["bs_q"]},
        "cashflowStatementHistory":          {"cashflowStatements": results["cf_a"]},
        "cashflowStatementHistoryQuarterly": {"cashflowStatements": results["cf_q"]},
        "_meta_source_financials":             "stockanalysis.com",
    }


# Finviz: entire snapshot table on one page at quote.ashx?t={ticker}
def finviz_quote(ticker):
    """Scrape Finviz snapshot table; returns {label: value} for ~70 metrics."""
    if is_crypto(ticker) or ticker.startswith("^") or ticker.endswith("=X"):
        return {}
    url = f"https://finviz.com/quote.ashx?t={ticker.upper()}"
    body = _scrape_fetch(url, ttl=1800)
    if not body:
        return {}
    # Finviz snapshot table has cells alternating label/value
    out = {}
    m = re.search(r"class=\"snapshot-table2\"[^>]*>(.*?)</table>", body, re.S | re.I)
    if not m:
        return {}
    cells = [_strip_tags(c) for c in
             re.findall(r"<td[^>]*>(.*?)</td>", m.group(1), re.S | re.I)]
    for i in range(0, len(cells) - 1, 2):
        k, v = cells[i], cells[i + 1]
        if k and v and v not in ("-", "N/A"):
            out[k] = v
    return out


# Yahoo HTML scrape: Yahoo's per-page HTML embeds full quoteSummary JSON in a <script>.
# This often succeeds when the /v10/finance/quoteSummary JSON endpoint is rate-limited.
def yh_html_modules(ticker, modules="analysis,holders,insider-transactions,key-statistics"):
    """Scrape Yahoo HTML pages and return a merged dict of QuoteSummaryStore-style modules."""
    if is_crypto(ticker) or ticker.endswith("=X") or ticker.startswith("^"):
        return {}
    cache_key = f"yh_html::{ticker.upper()}::{modules}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    merged = {}
    pages = {
        "analysis":             f"https://finance.yahoo.com/quote/{ticker}/analysis",
        "holders":              f"https://finance.yahoo.com/quote/{ticker}/holders",
        "insider-transactions": f"https://finance.yahoo.com/quote/{ticker}/insider-transactions",
        "key-statistics":       f"https://finance.yahoo.com/quote/{ticker}/key-statistics",
        "profile":              f"https://finance.yahoo.com/quote/{ticker}/profile",
    }
    for page in [p.strip() for p in modules.split(",") if p.strip()]:
        url = pages.get(page)
        if not url:
            continue
        body = _scrape_fetch(url, ttl=1800)
        if not body:
            continue
        # Yahoo uses multiple embedded JSON blobs; the older one is root.App.main = {...};
        # the newer one is a <script id="__NEXT_DATA__">{...}</script>
        m = re.search(r"root\.App\.main\s*=\s*(\{.*?\});\n", body, re.S)
        blob = None
        if m:
            try:
                blob = json.loads(m.group(1))
            except Exception:
                blob = None
        if blob is None:
            m2 = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', body, re.S | re.I)
            if m2:
                try:
                    blob = json.loads(m2.group(1))
                except Exception:
                    blob = None
        if not blob:
            continue
        # Walk the blob looking for "QuoteSummaryStore" or anything with known module keys
        stack = [blob]
        while stack:
            node = stack.pop()
            if isinstance(node, dict):
                if "QuoteSummaryStore" in node:
                    qss = node["QuoteSummaryStore"] or {}
                    if isinstance(qss, dict):
                        for k, v in qss.items():
                            if v and k not in merged:
                                merged[k] = v
                else:
                    for v in node.values():
                        if isinstance(v, (dict, list)):
                            stack.append(v)
            elif isinstance(node, list):
                stack.extend(node)
    _cache_put(cache_key, merged, ttl=1800)
    return merged


# ---------- HTTP handler ----------
class Handler(BaseHTTPRequestHandler):
    # server_version becomes the HTTP "Server:" response header. Bakes the
    # author credit into every single response the backend ever serves.
    server_version = "Terminal/2.0 (author: Oscar Zarraga Perez)"

    def _send(self, code, body, ctype="application/json; charset=utf-8"):
        if isinstance(body, (dict, list)):
            body = json.dumps(body).encode()
        elif isinstance(body, str):
            body = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        # Author/credit headers — emitted on every response. Removing these
        # from forks does not remove the underlying authorship.
        self.send_header("X-Author", "Oscar Zarraga Perez")
        self.send_header("X-Created-By", "Oscar Zarraga Perez")
        self.send_header("X-Copyright", "Copyright (c) 2026 Oscar Zarraga Perez")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        sys.stderr.write("[%s] %s\n" % (self.log_date_time_string(), fmt % args))

    def do_GET(self):
        u = urlparse(self.path)
        path = u.path
        qs = parse_qs(u.query)
        try:
            if path in ("/", "/index.html"):
                p = os.path.join(HERE, "terminal.html")
                if not os.path.exists(p):
                    return self._send(404, b"terminal.html not found", "text/plain")
                with open(p, "rb") as f:
                    return self._send(200, f.read(), "text/html; charset=utf-8")

            if path == "/api/quote":
                syms = qs.get("symbols", [""])[0]
                wanted = [x.strip().upper() for x in syms.split(",") if x.strip()]
                # Split: crypto + FX + indices -> per-symbol shape_quote (cheap sources).
                # Plain stocks -> ONE Yahoo v7 batch call (avoids N separate Yahoo hits).
                stock_syms = []
                other_syms = []
                for s in wanted:
                    if is_crypto(s) or s.endswith("=X") or s.startswith("^"):
                        other_syms.append(s)
                    else:
                        stock_syms.append(s)
                results_by_sym = {}
                # Batch fetch the stocks (one HTTP hit for all of them)
                if stock_syms:
                    batch = yahoo_v7_quotes_batch(stock_syms) or {}
                    for s in stock_syms:
                        if s in batch:
                            results_by_sym[s] = batch[s]
                    # Anything the batch couldn't resolve → fall back to shape_quote
                    missing = [s for s in stock_syms if s not in results_by_sym]
                    for s in missing:
                        q = shape_quote(s)
                        if q:
                            results_by_sym[s] = q
                # Cheap single fetches for crypto/fx/indices
                for s in other_syms:
                    q = shape_quote(s)
                    if q:
                        results_by_sym[s] = q
                results = [results_by_sym[s] for s in wanted if s in results_by_sym]
                return self._send(200, {"quoteResponse": {"result": results, "error": None}})

            if path.startswith("/api/chart/"):
                sym = path[len("/api/chart/"):]
                rng = qs.get("range", ["1y"])[0]
                iv = qs.get("interval", ["1d"])[0]
                return self._send(200, shape_chart(sym, rng, iv))

            if path.startswith("/api/summary/"):
                sym = path[len("/api/summary/"):]
                return self._send(200, shape_summary(sym))

            if path.startswith("/api/financials/"):
                sym = path[len("/api/financials/"):]
                data = sec_build_financials(sym)
                return self._send(200, data or {})

            if path == "/api/search":
                q = (qs.get("q", [""])[0] or "").upper()
                quotes = []
                if q:
                    # Try SEC lookup first for a real company name match
                    info = sec_get_cik(q)
                    if info:
                        _cik, name = info
                        quotes = [{"symbol": q, "shortname": name, "longname": name,
                                   "exchDisp": "Stooq/SEC", "typeDisp": "Equity"}]
                    else:
                        quotes = [{"symbol": q, "shortname": q, "longname": q,
                                   "exchDisp": "Stooq", "typeDisp": "Equity"}]
                news = fetch_news(q, limit=15) if q else []
                return self._send(200, {"quotes": quotes, "news": news})

            if path.startswith("/api/news-all/"):
                sym = path[len("/api/news-all/"):]
                limit = int(qs.get("limit", ["40"])[0] or "40")
                return self._send(200, fetch_news_all(sym, limit=limit))

            if path.startswith("/api/press/"):
                sym = path[len("/api/press/"):].upper()
                limit = int(qs.get("limit", ["40"])[0] or "40")
                return self._send(200, press_releases(sym, limit=limit))

            if path.startswith("/api/news"):
                sym = qs.get("q", [""])[0] or (path[len("/api/news/"):] if "/api/news/" in path else "")
                news = fetch_news(sym, limit=25) if sym else []
                return self._send(200, {"news": news})

            if path.startswith("/api/options/"):
                sym = path[len("/api/options/"):]
                expiry = qs.get("expiry", [None])[0] or qs.get("date", [None])[0]
                try:
                    expiry = int(expiry) if expiry else None
                except Exception:
                    expiry = None
                data = yahoo_options(sym, expiry=expiry)
                if not data:
                    return self._send(200, {"optionChain": {"result": [],
                                                             "error": {"code": "Not Found",
                                                                       "description": "no options data"}}})
                return self._send(200, {"optionChain": {"result": [data], "error": None}})

            if path.startswith("/api/sentiment/"):
                sym = path[len("/api/sentiment/"):]
                limit = int(qs.get("limit", ["20"])[0] or "20")
                data = stocktwits_stream(sym, limit=limit)
                if not data:
                    return self._send(200, {"sentiment": None,
                                             "error": "no sentiment data"})
                return self._send(200, data)

            if path == "/api/keys/status":
                status = keys_status()
                return self._send(200, {
                    "keys": status,
                    "any":  any(status.values()),
                    "count": sum(1 for v in status.values() if v),
                })

            # /api/about — authorship metadata. Always available, never
            # cached, never gated. Strips of this endpoint can be detected
            # by comparing against the canonical upstream.
            if path == "/api/about":
                return self._send(200, {
                    "project":   "Terminal",
                    "author":    __author__,
                    "creator":   __author__,
                    "copyright": __copyright__,
                    "license":   __license__,
                    "credits":   __credits__,
                    "notice":    "Created by Oscar Zarraga Perez. "
                                 "Attribution must be preserved per the "
                                 "MIT License. Removing the copyright "
                                 "notice from this project violates the "
                                 "license terms.",
                })

            if path == "/api/keys/reload":
                status = reload_keys()
                return self._send(200, {
                    "reloaded": True,
                    "keys":     status,
                    "count":    sum(1 for v in status.values() if v),
                })

            # ---------- Dedicated keyed-provider endpoints ----------
            # These are the ONLY places the keyed APIs (AV/FMP/Finnhub/TD) are
            # called from HTTP handlers. Every response is long-cached so each
            # symbol hits the provider at most once per cache window. This is
            # how we avoid burning the free-tier daily budget on repeat views.
            # Quote/chart use FREE sources only (Yahoo/Stooq/Nasdaq/CoinGecko).

            # Alpha Vantage OVERVIEW — 25 calls/day is brutal, cache hard.
            if path.startswith("/api/stats/"):
                sym = path[len("/api/stats/"):].upper()
                ck = f"av_overview::{sym}"
                data = _cache_get(ck)
                if data is None:
                    if not av_key():
                        return self._send(200, {"symbol": sym, "source": "alpha_vantage",
                                                 "error": "no alpha_vantage key configured",
                                                 "data": None})
                    data = av_overview(sym) or {}
                    _cache_put(ck, data, ttl=24 * 3600)
                return self._send(200, {"symbol": sym, "source": "alpha_vantage",
                                         "data": data,
                                         "cache_ttl_seconds": 24 * 3600})

            # FMP PROFILE + RATIOS bundle — 12h cache.
            if path.startswith("/api/profile-ext/"):
                sym = path[len("/api/profile-ext/"):].upper()
                ck = f"fmp_profile_ext::{sym}"
                data = _cache_get(ck)
                if data is None:
                    if not fmp_key():
                        return self._send(200, {"symbol": sym, "source": "fmp",
                                                 "error": "no fmp key configured",
                                                 "data": None})
                    res = _parallel({
                        "profile": lambda: fmp_profile(sym),
                        "ratios":  lambda: fmp_ratios(sym),
                    }, timeout=12)
                    data = {"profile": res.get("profile") or {},
                            "ratios":  res.get("ratios")  or {}}
                    _cache_put(ck, data, ttl=12 * 3600)
                return self._send(200, {"symbol": sym, "source": "fmp",
                                         "data": data,
                                         "cache_ttl_seconds": 12 * 3600})

            # Finnhub RECS + EARNINGS — 1h cache (these move more often).
            if path.startswith("/api/ratings/"):
                sym = path[len("/api/ratings/"):].upper()
                ck = f"fh_ratings::{sym}"
                data = _cache_get(ck)
                if data is None:
                    if not finnhub_key():
                        return self._send(200, {"symbol": sym, "source": "finnhub",
                                                 "error": "no finnhub key configured",
                                                 "data": None})
                    res = _parallel({
                        "recs":     lambda: fh_recs(sym),
                        "earnings": lambda: fh_earnings(sym, limit=4),
                        "profile":  lambda: fh_profile(sym),
                    }, timeout=12)
                    data = {"recommendations": res.get("recs")     or [],
                            "earnings":        res.get("earnings") or [],
                            "profile":         res.get("profile")  or {}}
                    _cache_put(ck, data, ttl=3600)
                return self._send(200, {"symbol": sym, "source": "finnhub",
                                         "data": data,
                                         "cache_ttl_seconds": 3600})

            # Twelve Data STATISTICS — 12h cache.
            if path.startswith("/api/statistics/"):
                sym = path[len("/api/statistics/"):].upper()
                ck = f"td_statistics::{sym}"
                data = _cache_get(ck)
                if data is None:
                    if not td_key():
                        return self._send(200, {"symbol": sym, "source": "twelve_data",
                                                 "error": "no twelve_data key configured",
                                                 "data": None})
                    data = td_statistics(sym) or {}
                    _cache_put(ck, data, ttl=12 * 3600)
                return self._send(200, {"symbol": sym, "source": "twelve_data",
                                         "data": data,
                                         "cache_ttl_seconds": 12 * 3600})

            if path.startswith("/api/peers/"):
                sym = path[len("/api/peers/"):]
                return self._send(200, {"peers": sec_peers(sym, limit=20)})

            if path.startswith("/api/insider/"):
                sym = path[len("/api/insider/"):]
                txns = sec_insider_transactions(sym, limit=60)
                summary = sec_insider_summary(txns)
                return self._send(200, {"transactions": txns, **summary})

            if path.startswith("/api/dividends/"):
                sym = path[len("/api/dividends/"):]
                return self._send(200, {"dividends": sec_dividend_history(sym, limit=40)})

            if path.startswith("/api/yearstats/"):
                sym = path[len("/api/yearstats/"):]
                return self._send(200, stooq_year_stats(sym) or {})

            if path.startswith("/api/analyst/"):
                sym = path[len("/api/analyst/"):]
                return self._send(200, {
                    "priceTargets":  nasdaq_price_targets(sym),
                    "ratings":       nasdaq_ratings(sym),
                    "forecast":      nasdaq_earnings_forecast(sym),
                    "surprise":      nasdaq_earnings_surprise(sym),
                })

            if path.startswith("/api/holders/"):
                sym = path[len("/api/holders/"):]
                return self._send(200, nasdaq_institutional_holdings(sym, limit=25))

            if path.startswith("/api/earningsdate/"):
                sym = path[len("/api/earningsdate/"):]
                return self._send(200, next_earnings_date(sym))

            if path.startswith("/api/dividends-nsd/"):
                sym = path[len("/api/dividends-nsd/"):]
                return self._send(200, {"dividends": nasdaq_dividend_history(sym, limit=40)})

            if path.startswith("/api/finviz/"):
                sym = path[len("/api/finviz/"):]
                return self._send(200, finviz_quote(sym))

            if path.startswith("/api/sa/stats/"):
                sym = path[len("/api/sa/stats/"):]
                return self._send(200, sa_statistics(sym))

            if path.startswith("/api/sa/forecast/"):
                sym = path[len("/api/sa/forecast/"):]
                return self._send(200, sa_forecast(sym))

            if path.startswith("/api/sa/ratings/"):
                sym = path[len("/api/sa/ratings/"):]
                return self._send(200, sa_ratings(sym))

            if path.startswith("/api/sa/institutional/"):
                sym = path[len("/api/sa/institutional/"):]
                return self._send(200, {"rows": sa_institutional(sym)})

            if path.startswith("/api/sa/insider/"):
                sym = path[len("/api/sa/insider/"):]
                return self._send(200, {"rows": sa_insider(sym)})

            if path.startswith("/api/yh-html/"):
                sym = path[len("/api/yh-html/"):]
                mods = qs.get("mods", ["analysis,holders,insider-transactions,key-statistics"])[0]
                return self._send(200, yh_html_modules(sym, modules=mods))

            if path == "/api/movers":
                return self._send(200, {"finance": {"result": [{"quotes": []}], "error": None}})

            if path == "/api/trending":
                return self._send(200, {"finance": {"result": [{"quotes": []}], "error": None}})

            if path.startswith("/api/recommendations/"):
                return self._send(200, {"finance": {"result": [], "error": None}})

            if path == "/api/healthz":
                return self._send(200, {"ok": True, "source": "stooq+coingecko",
                                        "server": "terminal"})

            if path == "/api/diag":
                # Reachability probe for every data source we use.
                import time as _t
                def probe(label, url, hdrs=None):
                    t0 = _t.time()
                    try:
                        req = Request(url, headers=(hdrs or {"User-Agent": UA}))
                        r = build_opener().open(req, timeout=8)
                        code = r.getcode()
                        sz = len(r.read())
                        return {"label": label, "url": url, "ok": code == 200,
                                "status": code, "bytes": sz,
                                "ms": int((_t.time() - t0) * 1000)}
                    except HTTPError as e:
                        return {"label": label, "url": url, "ok": False,
                                "status": e.code, "err": str(e),
                                "ms": int((_t.time() - t0) * 1000)}
                    except Exception as e:
                        return {"label": label, "url": url, "ok": False,
                                "status": 0, "err": str(e),
                                "ms": int((_t.time() - t0) * 1000)}
                sym = (qs.get("sym", ["AAPL"])[0] or "AAPL").upper()
                results = [
                    probe("Stooq quote",
                          f"https://stooq.com/q/l/?s={sym.lower()}.us&f=sd2t2ohlcv&h&e=csv"),
                    probe("CoinGecko",
                          "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"),
                    probe("Yahoo v8 chart",
                          f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?range=5d&interval=1d",
                          {"User-Agent": _pick_ua(), "Accept": "application/json",
                           "Referer": "https://finance.yahoo.com/"}),
                    probe("Yahoo v7 quote",
                          f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={sym}",
                          {"User-Agent": _pick_ua(), "Accept": "application/json",
                           "Referer": "https://finance.yahoo.com/"}),
                    probe("Yahoo HTML",
                          f"https://finance.yahoo.com/quote/{sym}",
                          {"User-Agent": SCRAPER_UA, "Accept": "text/html"}),
                    probe("Finviz",
                          f"https://finviz.com/quote.ashx?t={sym}",
                          {"User-Agent": SCRAPER_UA}),
                    probe("stockanalysis.com",
                          f"https://stockanalysis.com/stocks/{sym.lower()}/",
                          {"User-Agent": SCRAPER_UA}),
                    probe("Nasdaq API",
                          f"https://api.nasdaq.com/api/company/{sym}/earnings-date",
                          {"User-Agent": NASDAQ_UA, "Origin": "https://www.nasdaq.com",
                           "Referer": "https://www.nasdaq.com/"}),
                    probe("SEC EDGAR",
                          "https://www.sec.gov/files/company_tickers.json",
                          {"User-Agent": SEC_UA}),
                    probe("Google News RSS",
                          f"https://news.google.com/rss/search?q={sym}"),
                ]
                # Snapshot of current cooldowns so user can see who's in back-off
                now = time.time()
                cooldowns = {}
                with _host_state_lock:
                    for h, st in _host_state.items():
                        remaining = st.get("cooldown_until", 0.0) - now
                        if remaining > 0:
                            cooldowns[h] = round(remaining, 1)
                return self._send(200, {"symbol": sym, "probes": results,
                                         "cooldowns": cooldowns,
                                         "ua_pool": len(UA_POOL),
                                         "keys": keys_status()})

            if path == "/api/cg":
                ep = qs.get("ep", ["simple/price"])[0]
                extra = qs.get("qs", [""])[0]
                url = f"https://api.coingecko.com/api/v3/{ep}" + (f"?{extra}" if extra else "")
                code, body = _fetch(url)
                return self._send(200, body)

            if path == "/api/binance":
                ep = qs.get("ep", ["ticker/24hr"])[0]
                extra = qs.get("qs", [""])[0]
                url = f"https://api.binance.com/api/v3/{ep}" + (f"?{extra}" if extra else "")
                code, body = _fetch(url)
                return self._send(200, body)

            if path == "/api/fx":
                base = qs.get("base", ["USD"])[0]
                url = f"https://api.exchangerate.host/latest?base={quote(base)}"
                code, body = _fetch(url)
                return self._send(200, body)

            if path == "/api/sec":
                cik = (qs.get("cik", [""])[0] or "").zfill(10)
                url = f"https://data.sec.gov/submissions/CIK{cik}.json"
                code, body = _fetch(url)
                return self._send(200, body)

            # Static fallback for any other file under HERE
            local = os.path.normpath(os.path.join(HERE, path.lstrip("/")))
            if local.startswith(HERE) and os.path.isfile(local):
                with open(local, "rb") as f:
                    data = f.read()
                ctype = ("text/html" if local.endswith(".html")
                         else "application/octet-stream")
                return self._send(200, data, ctype)
            return self._send(404, b'{"error":"not found"}')
        except Exception as e:
            sys.stderr.write(f"[handler] {e}\n")
            return self._send(500, json.dumps({"error": str(e)}).encode())


# ---------- Browser launcher ----------
def _find_chromium():
    if sys.platform == "win32":
        bases = [os.environ.get("PROGRAMFILES", r"C:\Program Files"),
                 os.environ.get("PROGRAMFILES(X86)", r"C:\Program Files (x86)"),
                 os.environ.get("LOCALAPPDATA", "")]
        rels = [r"Google\Chrome\Application\chrome.exe",
                r"Microsoft\Edge\Application\msedge.exe",
                r"BraveSoftware\Brave-Browser\Application\brave.exe",
                r"Vivaldi\Application\vivaldi.exe"]
        for b in bases:
            if not b:
                continue
            for rel in rels:
                p = os.path.join(b, rel)
                if os.path.exists(p):
                    return p
    elif sys.platform == "darwin":
        for p in ("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                  "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
                  "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
                  "/Applications/Arc.app/Contents/MacOS/Arc"):
            if os.path.exists(p):
                return p
    else:
        for n in ("google-chrome", "google-chrome-stable", "chromium",
                  "chromium-browser", "microsoft-edge", "brave-browser"):
            p = shutil.which(n)
            if p:
                return p
    return None


def _launch_app_window(url):
    exe = _find_chromium()
    if not exe:
        return False
    profile_dir = os.path.join(os.path.expanduser("~"), ".terminal_profile")
    try:
        os.makedirs(profile_dir, exist_ok=True)
    except Exception:
        pass
    args = [exe, f"--app={url}", f"--user-data-dir={profile_dir}",
            "--window-size=1600,1000", "--no-first-run",
            "--no-default-browser-check", "--disable-features=Translate"]
    try:
        if sys.platform == "win32":
            subprocess.Popen(args, creationflags=0x00000008)  # DETACHED_PROCESS
        else:
            subprocess.Popen(args, stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL, start_new_session=True)
        return True
    except Exception as e:
        sys.stderr.write(f"[launcher] {e}\n")
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8788)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--no-browser", action="store_true")
    ap.add_argument("--tab", action="store_true",
                    help="Open in a normal browser tab instead of a standalone app window")
    args = ap.parse_args()

    port = args.port
    for _ in range(20):
        try:
            srv = ThreadingHTTPServer((args.host, port), Handler)
            break
        except OSError:
            port += 1
    else:
        print(f"Could not bind to any port near {args.port}")
        sys.exit(1)

    url = f"http://{args.host}:{port}/"
    print(f"""
+==============================================================+
|   Terminal — Stooq + CoinGecko (no Yahoo)          |
|                                                              |
|   Listening on: {url:<44} |
|   Press Ctrl+C to stop.                                      |
+==============================================================+
""")

    if not args.no_browser:
        def _open():
            if not args.tab and _launch_app_window(url):
                print("Opened standalone app window.")
            else:
                webbrowser.open(url)
                print("Opened in default browser.")
        threading.Timer(0.6, _open).start()

    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        srv.server_close()


if __name__ == "__main__":
    main()
