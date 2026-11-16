#!/usr/bin/env python3
# ============================================================
#  >>> DESTINATION: TRAPP2 repo  →  pipeline/fetch_data.py  <<<
#  TheMostLocal/TRAPP2/pipeline/fetch_data.py
#
#  Replace the existing pipeline/fetch_data.py in the TRAPP2 repo with
#  THIS file. (One file per repo — do not mix them up; each is labeled.)
# ============================================================
"""
z187 - VALUATION AT THE LIVE PRICE, IN ONE CURRENCY (the z187 block below): P/B, P/S, EV ratios, P/E, market cap and
yield are computed at this run's price from latest-quarter equity and trailing revenue converted to USD, with USD
anchors on the row so the 15-minute refresh re-prices them. Changed company names are flagged (nameWas).

TRAPP2 — Quote + fundamentals fetcher.

Pulls live (or 15-min delayed) quotes for every ticker in data/tickers.txt via
yfinance, plus the fundamentals snapshot. Writes:
  data/master.csv   — flat table for the app
  data/master.json  — same data, JSON shape

Run frequently (every 15 min during market hours via intraday workflow).
The expensive 5-year history fetch lives in fetch_history.py and runs nightly.

Columns produced (lowercase headers, matches what the app expects):
  ticker, name, price, marketcap, volume, volumeavg, priceopen, low, high, close,
  change, changepct, closeyest, date, high52, low52, beta, shares, pe, eps,
  sector, industry, description, exchange, ceo, country, ipodate, isetf, isfund,
  isactive, web_url, image, currency, employees, city, state, phone, address,
  dividend_yield, fetched_at, profile_fetched_at
"""
import csv
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import yfinance as yf

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
TICKERS_FILE = DATA / "tickers.txt"
MASTER_CSV = DATA / "master.csv"
MASTER_JSON = DATA / "master.json"

# Field profile cache — fundamentals don't change intraday so we re-fetch
# the slow .info dict at most once every 24h per ticker. Quotes refresh every run.
PROFILE_CACHE = DATA / ".profile_cache.json"
PROFILE_TTL_HOURS = 24

# Columns in deterministic order for master.csv
COLUMNS = [
    "ticker", "name", "price", "marketcap", "volume", "volumeavg",
    "priceopen", "low", "high", "close", "change", "changepct", "closeyest",
    "date", "high52", "low52", "beta", "shares", "pe", "eps",
    "sector", "industry", "description", "exchange", "ceo", "country",
    "ipodate", "isetf", "isfund", "isactive", "web_url", "image",
    "currency", "employees", "city", "state", "phone", "address",
    "dividend_yield", "fetched_at", "profile_fetched_at",
    "asset_class",  # NEW — Equity / Future / FX / Crypto / Index / Mutual Fund / Private / Option
    # Financial metrics for Research grading + bot engine (camelCase to match app)
    "returnOnEquity", "returnOnAssets", "grossMargin", "operatingMargin",
    "profitMargin", "revenueGrowth", "earningsGrowth", "revenue", "ebitda",
    "freeCashFlow", "netIncome", "stockBasedComp", "priceToBook", "evToEbitda",
    "evToRevenue", "totalDebt", "totalEquity", "totalAssets", "cash",
    # NEW — expense ratio (fund fee) + extra grading ratios
    "expenseRatio", "currentRatio", "quickRatio", "debtToEquity", "payoutRatio",
    "pegRatio", "heldPctInsiders", "heldPctInstitutions", "shortPctFloat",
    "financialCurrency",  # z123: the statements' (reporting) currency - Yahoo info.financialCurrency
]


def log(*args):
    print("[fetch_data]", *args, flush=True)


# Reference tickers the signal engine needs quotes for. Auto-merged with tickers.txt.
def classify_instrument(ticker):
    """
    Classify a ticker by its symbol pattern into asset class + sector for the
    Stock Book. yfinance returns null sector for non-equities, so we infer it
    from the ticker string itself.

    Returns dict with: asset_class, sector, industry, display_ticker.
    The display_ticker is what shows in the app UI (e.g. ^GSPC → "S&P 500").
    """
    t = (ticker or "").strip().upper()

    # === Futures / Commodities (X=F or X!) ===
    if t.endswith("=F") or t.endswith("!"):
        # Detect commodity type from root symbol
        root = t[:-2] if t.endswith("=F") else t.rstrip("!").rstrip("0123456789")
        FUTURES_MAP = {
            # Energy
            "CL": ("Energy", "Crude Oil WTI"),
            "BZ": ("Energy", "Crude Oil Brent"),
            "NG": ("Energy", "Natural Gas"),
            "RB": ("Energy", "RBOB Gasoline"),
            "HO": ("Energy", "Heating Oil"),
            "B0": ("Energy", "Crude Oil Brent (alt)"),
            # Metals
            "GC": ("Metals", "Gold"),
            "SI": ("Metals", "Silver"),
            "HG": ("Metals", "Copper"),
            "PL": ("Metals", "Platinum"),
            "PA": ("Metals", "Palladium"),
            "MGC": ("Metals", "Micro Gold"),
            "SIL": ("Metals", "Silver Micro"),
            # Equity index futures
            "ES": ("Equity Index Future", "S&P 500"),
            "NQ": ("Equity Index Future", "Nasdaq 100"),
            "YM": ("Equity Index Future", "Dow Jones"),
            "RTY": ("Equity Index Future", "Russell 2000"),
            "BTC": ("Equity Index Future", "Bitcoin Future"),
            # Rate futures
            "ZB": ("Treasury Future", "30Y Bond"),
            "ZN": ("Treasury Future", "10Y Note"),
            "ZF": ("Treasury Future", "5Y Note"),
            "ZT": ("Treasury Future", "2Y Note"),
            # Grains
            "ZC": ("Grains", "Corn"),
            "ZS": ("Grains", "Soybeans"),
            "ZW": ("Grains", "Wheat"),
            "ZO": ("Grains", "Oats"),
            "ZR": ("Grains", "Rough Rice"),
            "ZM": ("Grains", "Soybean Meal"),
            "ZL": ("Grains", "Soybean Oil"),
            "KE": ("Grains", "Hard Red Wheat"),
            # Softs
            "CC": ("Softs", "Cocoa"),
            "KC": ("Softs", "Coffee"),
            "CT": ("Softs", "Cotton"),
            "SB": ("Softs", "Sugar"),
            "OJ": ("Softs", "Orange Juice"),
            "LBS": ("Softs", "Lumber"),
            # Livestock
            "GF": ("Livestock", "Feeder Cattle"),
            "HE": ("Livestock", "Lean Hogs"),
            "LE": ("Livestock", "Live Cattle"),
            # Currency futures
            "DX": ("Currency Future", "US Dollar Index"),
        }
        info = FUTURES_MAP.get(root, ("Futures", root))
        return {
            "asset_class": "Future",
            "sector": "Commodities & Futures",
            "industry": info[0],
            "display_name": info[1],
        }

    # === FX (X=X) ===
    if t.endswith("=X"):
        pair = t[:-2]
        return {
            "asset_class": "FX",
            "sector": "Currencies",
            "industry": "Foreign Exchange",
            "display_name": pair if len(pair) >= 6 else f"USD/{pair}",
        }

    # === Crypto (X-USD or X-USDT) ===
    if t.endswith("-USD") or t.endswith("-USDT"):
        base = t.split("-")[0]
        CRYPTO_NAMES = {
            "BTC": "Bitcoin", "ETH": "Ethereum", "SOL": "Solana",
            "DOGE": "Dogecoin", "XRP": "XRP", "BNB": "BNB",
            "USDC": "USD Coin", "USDT": "Tether", "STETH": "Lido Staked Ether",
        }
        return {
            "asset_class": "Crypto",
            "sector": "Cryptocurrency",
            "industry": CRYPTO_NAMES.get(base, base),
            "display_name": CRYPTO_NAMES.get(base, base),
        }

    # === Index (^XXX, Yahoo foreign-exchange suffixes, special formats) ===
    # Yahoo uses .SS (Shanghai), .ME (Moscow), .TA (Tel Aviv), .JO (Johannesburg),
    # etc. for foreign-listed indexes that don't fit the ^XXX pattern.
    INDEX_SUFFIXES = (".SS", ".HK", ".SZ", ".KS", ".KQ", ".SI", ".AX")
    if (t.startswith("^")
        or ".ME" in t
        or "-STRD" in t
        or any(t.endswith(s) for s in INDEX_SUFFIXES)
        or t == "DX-Y.NYB"):
        INDEX_MAP = {
            "^GSPC": "S&P 500", "^DJI": "Dow Jones", "^IXIC": "Nasdaq Composite",
            "^NDX": "Nasdaq 100", "^RUT": "Russell 2000", "^NYA": "NYSE Composite",
            "^VIX": "VIX", "^VIX9D": "VIX 9-Day", "^MOVE": "MOVE Index",
            "^TNX": "US 10-Year Yield", "^FVX": "US 5-Year Yield", "^TYX": "US 30-Year Yield",
            "^FTSE": "FTSE 100", "^GDAXI": "DAX", "^FCHI": "CAC 40",
            "^STOXX50E": "EURO STOXX 50", "^N100": "Euronext 100", "^BFX": "BEL 20",
            "^N225": "Nikkei 225", "^HSI": "Hang Seng", "^KS11": "KOSPI",
            "^TWII": "TAIEX", "^AXJO": "ASX 200", "^BSESN": "BSE Sensex",
            "^KLSE": "FTSE Bursa Malaysia KLCI", "^JKSE": "Jakarta Composite",
            "^NZ50": "NZX 50", "^STI": "Straits Times",
            "^MERV": "S&P MERVAL", "^BVSP": "Bovespa", "^MXX": "IPC Mexico",
            "^IPSA": "S&P IPSA", "^GSPTSE": "S&P/TSX Composite",
            "^TA125.TA": "TA-125", "^CASE30": "EGX 30", "^JN0U.JO": "FTSE/JSE Top 40",
            "^XDE": "USD/EUR Index", "^XDB": "USD/GBP Index", "^XDN": "USD/JPY Index",
            "^XDA": "USD/AUD Index", "^XAX": "AMEX Composite",
            "^BUK100P": "Cboe UK 100",
            "MOEX.ME": "MOEX Russia",
            "000001.SS": "Shanghai Composite",
            "DX-Y.NYB": "US Dollar Index (DXY)",
            "^125904-USD-STRD": "USD-STRD Index",
        }
        display = INDEX_MAP.get(t, t.lstrip("^"))
        return {
            "asset_class": "Index",
            "sector": "Indices",
            "industry": "Stock Index" if not any(k in t for k in ["VIX", "MOVE", "TNX", "FVX", "TYX"]) else "Volatility / Rates",
            "display_name": display,
        }

    # === Mutual fund (5-char ticker ending in X) ===
    if len(t) == 5 and t.endswith("X") and t.isalpha():
        return {
            "asset_class": "Mutual Fund",
            "sector": "Mutual Funds",
            "industry": "Mutual Fund",
            "display_name": t,
        }

    # === Private (X.PVT) ===
    if t.endswith(".PVT"):
        base = t[:-4]
        PVT_NAMES = {
            "SPAX": "SpaceX", "OPAI": "OpenAI", "ANTH": "Anthropic",
            "STRI": "Stripe", "DATB": "Databricks",
        }
        return {
            "asset_class": "Private",
            "sector": "Private Equity",
            "industry": "Private",
            "display_name": PVT_NAMES.get(base, base),
        }

    # === Options (long string with C/P + strike at end) ===
    # Format: ROOT + YYMMDD + (C|P) + 8-digit strike
    if len(t) > 12 and any(c.isdigit() for c in t[-6:]):
        import re
        # Try to parse option symbol
        m = re.match(r"^([A-Z]+)(\d{6})([CP])(\d{8})$", t)
        if m:
            root, dt, cp, strike_x1000 = m.groups()
            strike = int(strike_x1000) / 1000
            exp = f"20{dt[:2]}-{dt[2:4]}-{dt[4:6]}"
            cp_label = "Call" if cp == "C" else "Put"
            return {
                "asset_class": "Option",
                "sector": "Options",
                "industry": f"{root} Options",
                "display_name": f"{root} {exp} {cp_label} ${strike:.0f}",
            }

    # === Default: equity (let yfinance fill sector) ===
    return {
        "asset_class": "Equity",
        "sector": None,    # yfinance will fill
        "industry": None,
        "display_name": None,
    }


REQUIRED_REFERENCE_TICKERS = [
    # US equity / volatility benchmarks
    "SPY", "QQQ", "DIA", "IWM",
    "^VIX", "^VIX9D", "^MOVE",
    # Credit
    "HYG", "LQD", "JNK", "EMB",
    # Sector ETFs (11 SPDRs)
    "XLK", "XLF", "XLV", "XLE", "XLI", "XLP", "XLY", "XLU", "XLB", "XLRE", "XLC",
    # Equity-index futures
    "ES=F", "NQ=F", "YM=F", "RTY=F",
    # Energy futures
    "CL=F", "BZ=F", "NG=F", "RB=F", "HO=F",
    # Metal futures
    "GC=F", "SI=F", "HG=F", "PL=F", "PA=F",
    # Agricultural / grain futures (the user specifically asked for ZC=F)
    "ZC=F", "ZS=F", "ZW=F", "KC=F", "SB=F", "CC=F", "CT=F",
    # Interest rate futures
    "ZB=F", "ZN=F", "ZF=F", "ZT=F",
    # Crypto
    "BTC-USD", "ETH-USD", "SOL-USD",
    # FX pairs (currency)
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "USDCAD=X", "USDCHF=X",
    "NZDUSD=X", "USDCNY=X", "USDINR=X", "USDMXN=X",
    "DX=F",  # Dollar index future
    # Global / foreign equity indexes (user wants these in the FX bucket)
    "^GSPTSE",   # Canada (S&P/TSX Composite)
    "^MERV",     # Argentina (S&P MERVAL)
    "^BVSP",     # Brazil (Bovespa)
    "^MXX",      # Mexico (IPC)
    "^FTSE",     # UK (FTSE 100)
    "^GDAXI",    # Germany (DAX)
    "^FCHI",     # France (CAC 40)
    "^STOXX50E", # Eurozone (EURO STOXX 50)
    "^N225",     # Japan (Nikkei 225)
    "^HSI",      # Hong Kong (Hang Seng)
    "^AXJO",     # Australia (ASX 200)
    "^BSESN",    # India (BSE Sensex)
    "^KS11",     # South Korea (KOSPI)
    "^TWII",     # Taiwan (TAIEX)
    # Dollar / treasury / aggregate
    "UUP",
    "TLT", "IEF", "SHY", "BND", "AGG", "GOVT", "TIP",
]


def load_tickers():
    tickers = []
    if TICKERS_FILE.exists():
        for line in TICKERS_FILE.read_text().splitlines():
            t = line.strip().split("#")[0].strip().upper()
            if t:
                tickers.append(t)
    else:
        log(f"⚠ {TICKERS_FILE} missing — create it with one ticker per line")
    # Always include reference tickers (deduped, preserves order)
    tickers = tickers + REQUIRED_REFERENCE_TICKERS
    seen = set()
    out = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def load_profile_cache():
    if not PROFILE_CACHE.exists():
        return {}
    try:
        return json.loads(PROFILE_CACHE.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def save_profile_cache(cache):
    PROFILE_CACHE.parent.mkdir(parents=True, exist_ok=True)
    PROFILE_CACHE.write_text(json.dumps(cache, separators=(",", ":")))


def profile_is_fresh(cache_entry):
    if not cache_entry or "fetched_at" not in cache_entry:
        return False
    try:
        fetched = datetime.fromisoformat(cache_entry["fetched_at"])
    except (ValueError, TypeError):
        return False
    age_hours = (datetime.now(timezone.utc) - fetched).total_seconds() / 3600
    return age_hours < PROFILE_TTL_HOURS


def safe(d, *keys, default=""):
    for k in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(k)
        if d is None:
            return default
    return d if d is not None else default


def fmt_num(v):
    if v is None or v == "" or v == "N/A":
        return ""
    try:
        f = float(v)
    except (ValueError, TypeError):
        return str(v)
    # yfinance returns NaN/inf for halted, pre-market or missing quotes. Write a
    # blank, never the string "nan" (the app's CSV parse turned it into NaN).
    if f != f or f in (float("inf"), float("-inf")):
        return ""
    return f"{f:.6f}".rstrip("0").rstrip(".") if "." in str(v) else str(v)


# ======================================================================================================== z187
#  VALUATION AT THE LIVE PRICE, IN ONE CURRENCY
#  Yahoo's .info ratios had two problems:
#   1. STALE PRICE - they were priced when the profile was fetched (the profile cache keeps .info 24-96 h) while the
#      row's price is fresh, so P/E, P/B, EV ratios, market cap and yield lagged the price by up to four days.
#   2. MIXED CURRENCIES - for companies that report in another currency than they trade in (ADRs: TSM reports TWD,
#      ASML EUR, TM JPY, BABA CNY) Yahoo divides USD prices by local-currency statements: TSM P/B 76 (true ~12), ASML
#      EV/Revenue 768 (~12), TM P/B 17 (~1); and anyone dividing market cap by revenue got TSM P/S 0.4 (true ~13).
#  Now, at THIS run's price:
#    market cap  = Yahoo's (all share classes) x price now / the price Yahoo computed it at
#    P/B         = market cap / latest-quarter COMMON equity, both in USD        - blank when equity <= 0 ("neg-book")
#    P/S         = market cap / trailing-12-month revenue, both in USD
#    EV          = market cap + total debt - cash + minority interest (USD)     - EV ratios blank for banks / insurers
#    P/E, yield  = Yahoo's (per-share, currency-correct), re-priced to the price now
#  Anchors ride with the row (fxQuote, fxFin, bookUSD, revenueUSD, ebitdaUSD, netDebtUSD) so the 15-minute quote
#  refresh, the app's live quotes and the bot re-price every ratio without another fundamentals call. bookAsOf /
#  revenueAsOf say which quarter the denominators are; valuationNote flags what to treat with care:
#    neg-book   equity <= 0 (P/B not meaningful)      ev-na    bank / insurer (EV not meaningful)
#    mcap?      Yahoo's market cap disagrees with its own per-share P/E by > 1.5x (ADR ratio / one share class)
#    fx-XXX?    no USD rate for the statements' currency (cross-currency ratios left blank)
#    px?        the profile's price is unknown (ratios not re-priced)   split?   price vs profile looks like a split
#    book-annual  equity from last year's balance sheet (a profile cached before z187 - fixed at its next refresh)
import math as _vmath
import os as _vos
import urllib.request as _vurl

VALUATION_COLUMNS = ["priceToSales", "fxQuote", "fxFin", "bookUSD", "revenueUSD", "ebitdaUSD", "netDebtUSD",
                     "bookAsOf", "revenueAsOf", "valuationNote"]
FX_SUBUNIT = {"GBp": ("GBP", 0.01), "GBX": ("GBP", 0.01), "ZAc": ("ZAR", 0.01), "ZAC": ("ZAR", 0.01),
              "ILA": ("ILS", 0.01), "ILa": ("ILS", 0.01), "USX": ("USD", 0.01), "USd": ("USD", 0.01)}
_VOWNER = (_vos.environ.get("VALUATIO_OWNER") or _vos.environ.get("GITHUB_REPOSITORY_OWNER") or "TheMostLocal").strip()
FX_RATES_URL = f"https://raw.githubusercontent.com/{_VOWNER}/TRAPP2-1/main/data/fx/rates.json"
# deposits / policy float aren't "debt" for these, so enterprise value is not a meaningful denominator
EV_NA_INDUSTRY = ("Banks", "Insurance -", "Insurance—", "Mortgage Finance", "Capital Markets", "Financial Conglomerates")
FX = {}                  # USD per 1 main unit, filled once per run by load_fx_rates()


def _vf(v):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if _vmath.isfinite(x) else None


def _fxfmt(v):
    return f"{v:.10g}" if v else ""


def ccy_main(c):
    """'GBp' -> ('GBP', 0.01): the main currency and the factor from the quoted unit to it."""
    c = str(c or "").strip()
    if not c:
        return None, 1.0
    return FX_SUBUNIT.get(c, (c.upper(), 1.0))


def _close_of(df, pair, single):
    for get in ((lambda: df[pair]["Close"]), (lambda: df["Close"][pair]), (lambda: df["Close"])):
        try:
            s = get()
            if single or getattr(s, "name", None) != "Close":
                v = _vf(s.dropna().iloc[-1])
                if v:
                    return v
        except Exception:
            continue
    return None


def load_fx_rates(need=()):
    """USD per 1 unit of each currency: TRAPP2-1/data/fx/rates.json (the rates the app and the bot use, refreshed every
    15 minutes), then Yahoo's <CCY>USD=X for any currency still missing. A currency without a rate leaves only the
    ratios that need it blank - never a mixed-currency number."""
    rates, src = {"USD": 1.0}, "unavailable"
    try:
        local = DATA / "fx" / "rates.json"                  # TRAPP2-1 itself
        if local.exists():
            d, src = json.loads(local.read_text()), "this repo"
        else:
            req = _vurl.Request(f"{FX_RATES_URL}?t={int(time.time() // 300)}", headers={"User-Agent": "ValuatioPipeline"})
            with _vurl.urlopen(req, timeout=30) as r:
                d, src = json.loads(r.read().decode()), "TRAPP2-1"
        for c, v in ((d or {}).get("rates") or {}).items():
            up = _vf(v.get("usdPer")) if isinstance(v, dict) else _vf(v)
            if up and up > 0:
                rates[str(c).upper()] = up
    except Exception as e:
        log(f"  FX rates file unavailable ({type(e).__name__}: {str(e)[:80]})")
    missing = sorted({m for m in (ccy_main(c)[0] for c in need) if m and m not in rates})
    got = []
    if missing:
        try:
            pairs = [f"{c}USD=X" for c in missing]
            df = yf.download(pairs, period="5d", interval="1d", group_by="ticker", auto_adjust=False,
                             progress=False, threads=True)
            for c, p in zip(missing, pairs):
                v = _close_of(df, p, len(pairs) == 1)
                if v and v > 0:
                    rates[c] = v
                    got.append(c)
        except Exception as e:
            log(f"  Yahoo FX fallback failed ({type(e).__name__}: {str(e)[:80]})")
    still = [c for c in missing if c not in rates]
    log(f"  FX: {len(rates) - 1} currencies in USD (rates file: {src}" + (f"; Yahoo: {', '.join(got)}" if got else "") + ")"
        + (f" · no rate for {', '.join(still)} - their cross-currency ratios stay blank" if still else ""))
    FX.clear()
    FX.update(rates)
    return rates


def split_like(k):
    """A price change that looks like a split / reverse split (x2, x1/3, x10 ...) - not a market move to re-price by."""
    if not k or k <= 0 or abs(_vmath.log(k)) < _vmath.log(1.4):
        return False
    r = k if k > 1 else 1 / k
    return any(abs(r / n - 1) < 0.04 for n in (1.5, 2, 2.5, 3, 4, 5, 6, 8, 10, 12, 15, 20, 25, 30, 40, 50, 100))


def profile_price(entry, info):
    """The price a cached profile's ratios were computed at: recorded at fetch (z187), else Yahoo's own quote fields,
    else trailing P/E x EPS. None when nothing says."""
    px = _vf((entry or {}).get("px"))
    if px and px > 0:
        return px
    info = info or {}
    for k in ("currentPrice", "regularMarketPrice"):
        v = _vf(info.get(k))
        if v and v > 0:
            return v
    pe, eps = _vf(info.get("trailingPE")), _vf(info.get("trailingEps"))
    return pe * eps if (pe and eps and pe > 0 and eps > 0) else None


def latest_balance(t):
    """The latest QUARTERLY balance sheet (annual when Yahoo has no quarters for the name): common equity for P/B plus
    the totals the row carries. {} when Yahoo has neither."""
    for basis, attr in (("quarter", "quarterly_balance_sheet"), ("annual", "balance_sheet")):
        try:
            bs = getattr(t, attr)
        except Exception:
            continue
        if bs is None or getattr(bs, "empty", True):
            continue
        try:
            cols = sorted(bs.columns, reverse=True)
        except Exception:
            cols = list(bs.columns)
        for col in cols[:3]:
            def val(*names):
                for n in names:
                    if n in bs.index:
                        try:
                            v = _vf(bs.loc[n, col])
                        except Exception:
                            v = None
                        if v is not None:
                            return v
                return None
            common = val("Common Stock Equity", "Stockholders Equity")
            if common is None:
                continue
            return {"bookEquity": common, "totalEquity": val("Stockholders Equity", "Common Stock Equity"),
                    "totalAssets": val("Total Assets"), "totalDebt": val("Total Debt", "Long Term Debt"),
                    "minorityInterest": val("Minority Interest"), "bookAsOf": str(col)[:10], "bookBasis": basis}
    return {}


def _epoch_date(v):
    v = _vf(v)
    if not v or v <= 0:
        return ""
    try:
        return datetime.fromtimestamp(v, tz=timezone.utc).strftime("%Y-%m-%d")
    except (OverflowError, OSError, ValueError):
        return ""


def apply_valuation(row, info, bs, price, px):
    """Write the price-dependent fields (market cap, P/E, yield, P/B, P/S, EV ratios) at THIS run's price and in one
    currency, plus the USD anchors the 15-minute refresh re-prices them from. Equities only; never raises."""
    info = info or {}
    if str(info.get("quoteType") or "").upper() != "EQUITY":
        return
    try:
        _apply_valuation(row, info, bs or {}, _vf(price), _vf(px))
    except Exception as e:                                    # a valuation bug must never cost the row
        row["valuationNote"] = f"error {type(e).__name__}"


def _apply_valuation(row, info, bs, price, px):
    notes = []
    row["priceToBookYahoo"] = fmt_num(info.get("priceToBook"))      # Yahoo's own figure, for audit (master.json only)
    k = 1.0
    if price and px:
        if split_like(price / px):
            notes.append("split?")
        else:
            k = price / px
    elif price:
        notes.append("px?")
    qb, _ = ccy_main(info.get("currency") or row.get("currency") or "USD")
    fb, _ = ccy_main(info.get("financialCurrency") or info.get("currency") or row.get("currency") or "USD")
    fq, ff = FX.get(qb), FX.get(fb)
    conv = 1.0 if qb == fb else (ff / fq if (ff and fq) else None)   # statements' currency -> market cap's currency
    mc0 = _vf(info.get("marketCap"))
    mc = mc0 * k if (mc0 and mc0 > 0) else None
    if mc:
        row["marketcap"] = fmt_num(round(mc))
    pe0 = _vf(info.get("trailingPE"))
    row["pe"] = fmt_num(pe0 * k) if (pe0 and pe0 > 0) else ""
    dy0 = _vf(info.get("dividendYield"))
    if dy0 is not None:
        row["dividend_yield"] = fmt_num(dy0 / k)
    rev, ebitda = _vf(info.get("totalRevenue")), _vf(info.get("ebitda"))
    debt, cash = _vf(info.get("totalDebt")), _vf(info.get("totalCash"))
    if debt is None:
        debt = _vf(bs.get("totalDebt"))
    book, minority = _vf(bs.get("bookEquity")), (_vf(bs.get("minorityInterest")) or 0.0)
    if book is None and _vf(bs.get("legacyEquity")) is not None:   # a profile cached before z187: last year's equity
        book = _vf(bs.get("legacyEquity"))
        notes.append("book-annual")
    ind = str(info.get("industry") or row.get("industry") or "")
    ev_na = any(ind.startswith(p) for p in EV_NA_INDUSTRY)
    has_nd = debt is not None or cash is not None
    nd = ((debt or 0.0) - (cash or 0.0) + minority) if has_nd else None
    pb = ps = evr = eve = None
    if conv is None:
        notes.append(f"fx-{fb}?")
    elif mc:
        if book is not None:
            if book > 0:
                pb = mc / (book * conv)
            else:
                notes.append("neg-book")
        if rev and rev > 0:
            ps = mc / (rev * conv)
        if not ev_na and nd is not None:
            ev = mc + nd * conv
            if rev and rev > 0:
                evr = ev / (rev * conv)
            if ebitda and ebitda > 0:
                eve = ev / (ebitda * conv)
    if ev_na:
        notes.append("ev-na")
    # market-cap sanity: Yahoo's cap vs its own per-share P/E - an ADR-ratio error or a cap that covers one share
    # class shows up here; P/B and P/S built on that cap are flagged for the consumers to treat with care
    ni = _vf(info.get("netIncomeToCommon"))
    if mc and conv and ni and ni > 0 and pe0 and pe0 > 0 and abs(_vmath.log(mc / (ni * conv) / (pe0 * k))) > _vmath.log(1.5):
        notes.append("mcap?")
    row["priceToBook"] = fmt_num(pb) if pb is not None else ""
    row["priceToSales"] = fmt_num(ps) if ps is not None else ""
    row["evToRevenue"] = fmt_num(evr) if evr is not None else ""
    row["evToEbitda"] = fmt_num(eve) if eve is not None else ""
    usd = (lambda v: fmt_num(round(v * ff)) if (v is not None and ff) else "")
    row["fxQuote"] = _fxfmt(fq)
    row["fxFin"] = _fxfmt(ff)
    row["bookUSD"] = usd(book)
    row["revenueUSD"] = usd(rev if (rev and rev > 0) else None)
    row["ebitdaUSD"] = usd(ebitda)
    row["netDebtUSD"] = usd(nd)
    row["bookAsOf"] = str(bs.get("bookAsOf") or "")
    row["revenueAsOf"] = _epoch_date(info.get("mostRecentQuarter"))
    row["valuationNote"] = " ".join(notes)


_NAME_STOP = {"inc", "corp", "corporation", "co", "company", "ltd", "limited", "plc", "sa", "ag", "nv", "se", "spa",
              "holdings", "holding", "group", "the", "class", "and", "of", "technologies", "technology", "international"}


def name_changed(old, new):
    """True when two company names share no meaningful word - a renamed company or a REASSIGNED symbol (PARA:
    'Paramount Global' -> 'Parabolic Technologies, Inc.'); either way the history and the fundamentals no longer match."""
    import re as _re
    tok = lambda s: {w for w in _re.findall(r"[a-z0-9]+", str(s or "").lower()) if len(w) >= 3 and w not in _NAME_STOP}
    a, b = tok(old), tok(new)
    return bool(a and b and not (a & b))


def carry_name_change(row, prev, today):
    """Keeps a 30-day 'nameWas' marker on a row whose company name changed (master.json only)."""
    if not prev:
        return None
    if name_changed(prev.get("name"), row.get("name")):
        row["nameWas"], row["nameChangedAt"] = prev.get("name"), today
        return prev.get("name")
    was, at = prev.get("nameWas"), prev.get("nameChangedAt")
    try:
        if was and at and (datetime.strptime(today, "%Y-%m-%d") - datetime.strptime(at, "%Y-%m-%d")).days <= 30:
            row["nameWas"], row["nameChangedAt"] = was, at
    except ValueError:
        pass
    return None
COLUMNS += [c for c in VALUATION_COLUMNS if c not in COLUMNS]       # the CSV carries them after financialCurrency
# ===================================================================================================== /z187


def fetch_quote(ticker, profile_cache):
    """Pull current quote + fundamentals for one ticker. Returns row dict."""
    t = yf.Ticker(ticker)

    # Fast price path: yfinance .fast_info is light. Falls back to history if missing.
    fast = {}
    try:
        fast = dict(t.fast_info) if t.fast_info else {}
    except Exception:
        fast = {}

    price = fast.get("last_price") or fast.get("regular_market_price")
    prev_close = fast.get("previous_close") or fast.get("regular_market_previous_close")
    open_p = fast.get("open")
    day_high = fast.get("day_high")
    day_low = fast.get("day_low")
    high52 = fast.get("year_high")
    low52 = fast.get("year_low")
    vol = fast.get("last_volume") or fast.get("regular_market_volume")

    # Backstop: pull last 2 bars via history if fast_info lacked anything
    if price is None or prev_close is None:
        try:
            hist = t.history(period="5d", interval="1d", auto_adjust=False)
            if len(hist) >= 1:
                price = price or float(hist["Close"].iloc[-1])
                if len(hist) >= 2:
                    prev_close = prev_close or float(hist["Close"].iloc[-2])
                open_p = open_p or float(hist["Open"].iloc[-1])
                day_high = day_high or float(hist["High"].iloc[-1])
                day_low = day_low or float(hist["Low"].iloc[-1])
                vol = vol or float(hist["Volume"].iloc[-1])
        except Exception as e:
            log(f"  ✗ {ticker} history backstop failed: {e}")

    if price is None:
        return None

    change = (price - prev_close) if prev_close else None
    changepct = (change / prev_close) if change is not None and prev_close else None

    # Fundamentals — heavy call; cache it 24h. z187: when the price moved like a split since the profile was fetched
    # (its market cap and ratios are on the other side of it), the profile is refreshed now.
    cached = profile_cache.get(ticker, {})
    fresh = profile_is_fresh(cached)
    if fresh:
        _px0 = profile_price(cached, cached.get("info"))
        if _px0 and split_like(price / _px0):
            fresh = False
            log(f"  ↻ {ticker}: price {price:g} vs {_px0:g} when the profile was fetched looks like a split - profile refreshed")
    info = cached.get("info") if fresh else None
    profile_fetched_at = cached.get("fetched_at", "")
    if info is None:
        try:
            info = t.info or {}
            profile_fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
            profile_cache[ticker] = {"info": info, "fetched_at": profile_fetched_at,
                                     "px": profile_price({}, info) or price}         # z187: the price the ratios are at
        except Exception as e:
            log(f"  ⚠ {ticker} .info failed: {e}")
            info = cached.get("info") or {}

    row = {
        "ticker": ticker,
        "name": safe(info, "longName") or safe(info, "shortName"),
        "price": fmt_num(price),
        "marketcap": fmt_num(safe(info, "marketCap")),
        "volume": fmt_num(vol),
        "volumeavg": fmt_num(safe(info, "averageVolume")),
        "priceopen": fmt_num(open_p),
        "low": fmt_num(day_low),
        "high": fmt_num(day_high),
        "close": fmt_num(price),
        "change": fmt_num(change),
        "changepct": fmt_num(changepct * 100 if changepct is not None else ""),
        "closeyest": fmt_num(prev_close),
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "high52": fmt_num(high52),
        "low52": fmt_num(low52),
        "beta": fmt_num(safe(info, "beta")),
        "shares": fmt_num(safe(info, "sharesOutstanding")),
        "pe": fmt_num(safe(info, "trailingPE")),
        "eps": fmt_num(safe(info, "trailingEps")),
        "sector": safe(info, "sector"),
        "industry": safe(info, "industry"),
        "description": (safe(info, "longBusinessSummary") or "")[:2000],
        "exchange": safe(info, "exchange"),
        "ceo": (info.get("companyOfficers") or [{}])[0].get("name", "") if isinstance(info.get("companyOfficers"), list) and info.get("companyOfficers") else "",
        "country": safe(info, "country"),
        "ipodate": safe(info, "ipoExpectedDate") or safe(info, "firstTradeDateEpochUtc"),
        "isetf": "true" if safe(info, "quoteType") == "ETF" else "false",
        "isfund": "true" if safe(info, "quoteType") in ("MUTUALFUND", "FUND") else "false",
        "isactive": "true",
        "web_url": safe(info, "website"),
        "image": "",  # yfinance dropped logo_url. App synthesizes via Clearbit using web_url.
        "currency": safe(info, "currency"),
        "financialCurrency": safe(info, "financialCurrency"),   # z123: TSM trades in USD, reports in TWD
        "employees": fmt_num(safe(info, "fullTimeEmployees")),
        "city": safe(info, "city"),
        "state": safe(info, "state"),
        "phone": safe(info, "phone"),
        "address": safe(info, "address1") or safe(info, "address"),
        "dividend_yield": fmt_num(safe(info, "dividendYield")),
        # --- Financial metrics for Research grading + the bot engine ---
        # Yahoo's .info already carries these; we just extract them. camelCase
        # so they match what the app reads (no client-side remap needed).
        "returnOnEquity": fmt_num(safe(info, "returnOnEquity")),
        "returnOnAssets": fmt_num(safe(info, "returnOnAssets")),
        "grossMargin": fmt_num(safe(info, "grossMargins")),
        "operatingMargin": fmt_num(safe(info, "operatingMargins")),
        "profitMargin": fmt_num(safe(info, "profitMargins")),
        "revenueGrowth": fmt_num(safe(info, "revenueGrowth")),
        "earningsGrowth": fmt_num(safe(info, "earningsGrowth")),
        "revenue": fmt_num(safe(info, "totalRevenue")),
        "ebitda": fmt_num(safe(info, "ebitda")),
        "freeCashFlow": fmt_num(safe(info, "freeCashflow")),
        "netIncome": fmt_num(safe(info, "netIncomeToCommon")),
        "stockBasedComp": fmt_num(safe(info, "stockBasedCompensation")),
        "priceToBook": fmt_num(safe(info, "priceToBook")),
        "evToEbitda": fmt_num(safe(info, "enterpriseToEbitda")),
        "evToRevenue": fmt_num(safe(info, "enterpriseToRevenue")),
        "totalDebt": fmt_num(safe(info, "totalDebt")),
        "totalEquity": fmt_num(safe(info, "totalStockholderEquity")),
        "totalAssets": fmt_num(safe(info, "totalAssets")),
        "cash": fmt_num(safe(info, "totalCash")),
        # ---- NEW: expense ratio (ETFs/funds) + extra ratios for grading ----
        # Expense ratio is an ANNUAL fund fee (decimal, 0.0095 = 0.95%/yr). yfinance
        # exposes it under a few keys depending on the security; take whichever is
        # present. The app factors it into holding/trading cost AND the grade.
        "expenseRatio": fmt_num(
            safe(info, "annualReportExpenseRatio")
            or safe(info, "netExpenseRatio")
            or safe(info, "expenseRatio")
            or safe(info, "fundExpenseRatio")
        ),
        "currentRatio": fmt_num(safe(info, "currentRatio")),
        "quickRatio": fmt_num(safe(info, "quickRatio")),
        "debtToEquity": fmt_num(safe(info, "debtToEquity")),
        "payoutRatio": fmt_num(safe(info, "payoutRatio")),
        "pegRatio": fmt_num(safe(info, "trailingPegRatio") or safe(info, "pegRatio")),
        "heldPctInsiders": fmt_num(safe(info, "heldPercentInsiders")),
        "heldPctInstitutions": fmt_num(safe(info, "heldPercentInstitutions")),
        "shortPctFloat": fmt_num(safe(info, "shortPercentOfFloat")),
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "profile_fetched_at": profile_fetched_at,
    }

    # ---- Balance sheet / cash flow (z187) ----
    # The latest QUARTER's balance sheet (it was last year's annual one, re-requested every night) - common equity for
    # P/B, total equity / assets / debt for the row - plus stock-based comp from the cash-flow statement; both fetched
    # once per profile version and cached with it. Best-effort: fills gaps, never fails the row.
    bs = {}
    try:
        entry = profile_cache.get(ticker) or {}
        bs = dict(entry.get("bs")) if isinstance(entry.get("bs"), dict) else {}
        if bs.get("checked") != profile_fetched_at:
            got = latest_balance(t)
            sbc = bs.get("stockBasedComp")
            if not row.get("stockBasedComp") and not sbc:
                _cf = t.cashflow
                if _cf is not None and not _cf.empty and "Stock Based Compensation" in _cf.index:
                    v = _vf(_cf.loc["Stock Based Compensation", _cf.columns[0]])
                    sbc = v if v is not None else None
            bs = dict(got, stockBasedComp=sbc, checked=profile_fetched_at)
            if ticker in profile_cache:
                profile_cache[ticker]["bs"] = bs
        for _k in ("totalEquity", "totalAssets", "totalDebt", "stockBasedComp"):
            if bs.get(_k) is not None and not row.get(_k):
                row[_k] = fmt_num(bs[_k])
    except Exception:
        pass
    apply_valuation(row, info, bs, price, profile_price(profile_cache.get(ticker), info))   # z187

    # Inject classifier-derived sector/industry/asset_class for non-equity
    # instruments (futures, FX, crypto, indices, mutual funds, private, options).
    # yfinance returns null sector for these, so we infer from the ticker pattern.
    classification = classify_instrument(ticker)
    row["asset_class"] = classification["asset_class"]
    if classification["sector"] and not row["sector"]:
        row["sector"] = classification["sector"]
    if classification["industry"] and not row["industry"]:
        row["industry"] = classification["industry"]
    if classification["display_name"] and not safe(info, "shortName") and not safe(info, "longName"):
        row["name"] = classification["display_name"]

    return row


def _previous_rows():
    try:
        rows = json.loads(MASTER_JSON.read_text())
        return {r.get("ticker"): r for r in rows if isinstance(r, dict) and r.get("ticker")}
    except Exception:
        return {}


def main():
    tickers = load_tickers()
    if not tickers:
        log("No tickers to fetch. Exiting.")
        return 1
    log(f"Fetching {len(tickers)} tickers via yfinance…")

    profile_cache = load_profile_cache()
    load_fx_rates({(e.get("info") or {}).get(k) for e in profile_cache.values() if isinstance(e, dict)
                   for k in ("currency", "financialCurrency")})                          # z187: USD per unit, once
    previous = _previous_rows()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = []
    n_ok = 0
    renamed = []
    fetched = set()
    for i, tic in enumerate(tickers, 1):
        try:
            row = fetch_quote(tic, profile_cache)
            if row:
                was = carry_name_change(row, previous.get(tic), today)                  # z187
                if was:
                    renamed.append(f"{tic} ('{was}' -> '{row.get('name')}')")
                rows.append(row)
                fetched.add(tic)
                n_ok += 1
            else:
                log(f"  ✗ {tic}: no price data")
        except Exception as e:
            log(f"  ✗ {tic}: {e}")
        if i % 25 == 0:
            log(f"  … {i}/{len(tickers)} ({n_ok} OK)")
            save_profile_cache(profile_cache)
        time.sleep(0.05)

    save_profile_cache(profile_cache)

    # z187: a ticker Yahoo didn't answer for keeps yesterday's row (its fetched_at says how old) instead of vanishing
    # from the app, the bot and research until the next run
    carried = [t for t in tickers if t not in fetched and t in previous]
    rows += [previous[t] for t in carried]
    if carried:
        log(f"  {len(carried)} kept their previous row: " + ", ".join(carried[:30]) + (" …" if len(carried) > 30 else ""))
    if renamed:
        log(f"  ⚠ {len(renamed)} company name(s) changed - renamed, or the symbol now belongs to another company "
            f"(check tickers.txt): " + "; ".join(renamed))
    pv = [r for r in rows if r.get("priceToSales") or r.get("priceToBook")]
    log(f"  valuation: {len(pv)} rows with P/B or P/S at today's price · "
        f"{sum(1 for r in rows if 'neg-book' in str(r.get('valuationNote')))} negative book · "
        f"{sum(1 for r in rows if 'mcap?' in str(r.get('valuationNote')))} market cap disagrees with P/E · "
        f"{sum(1 for r in rows if 'fx-' in str(r.get('valuationNote')))} without an FX rate")

    DATA.mkdir(parents=True, exist_ok=True)
    with MASTER_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    MASTER_JSON.write_text(json.dumps(rows, separators=(",", ":")))

    log(f"✓ Wrote {len(rows)} rows ({n_ok}/{len(tickers)} fetched now) to {MASTER_CSV.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
