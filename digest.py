"""
zStonks market update — fetches market data via yfinance and posts to Telegram.

Usage:
    python digest.py                # send to Telegram
    python digest.py --dry-run      # print to stdout, don't send

Required env vars (unless --dry-run):
    TELEGRAM_BOT_TOKEN
    TELEGRAM_CHAT_ID

Optional env vars:
    WATCHLIST   comma-separated tickers (default below)
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import yfinance as yf

DEFAULT_WATCHLIST = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "AMD"]
TG_MAX_LEN = 4096
ET = ZoneInfo("America/New_York")
PT = ZoneInfo("America/Los_Angeles")


# ---------- formatting helpers ----------

def pct(a, b):
    if a is None or b is None or b == 0:
        return None
    return (a - b) / b * 100.0


def arrow(p):
    if p is None:
        return "⚪"
    if p >= 10:
        return "🚀"
    if p <= -10:
        return "💀"
    if p > 0:
        return "🟢"
    if p < 0:
        return "🔴"
    return "⚪"


def fmt_pct(p):
    return "    n/a" if p is None else f"{p:+6.2f}%"


# ---------- data fetching ----------

def pull_last_two_closes(tickers):
    """Return {ticker: (last_close, prev_close)} from a single batched download."""
    if not tickers:
        return {}
    data = yf.download(
        tickers=" ".join(tickers),
        period="5d",
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        progress=False,
        threads=True,
    )
    out = {}
    for t in tickers:
        try:
            df = data[t].dropna()
            if len(df) < 2:
                continue
            out[t] = (float(df["Close"].iloc[-1]), float(df["Close"].iloc[-2]))
        except Exception:
            continue
    return out


# ---------- sections ----------

def section_market_pulse():
    names = {
        "^GSPC": "S&P 500", "^IXIC": "Nasdaq", "^DJI": "Dow",
        "^RUT": "Russell 2K", "^VIX": "VIX",
    }
    data = pull_last_two_closes(list(names))
    lines = ["*Market Pulse*"]
    for sym, label in names.items():
        if sym not in data:
            lines.append(f"`{label:10s}`  n/a")
            continue
        last, prev = data[sym]
        p = pct(last, prev)
        lines.append(f"`{label:10s} {last:>9,.2f}  {fmt_pct(p)}` {arrow(p)}")
    return "\n".join(lines)


def section_rates_fx_commods():
    names = {
        "^TNX": "US 10Y", "DX-Y.NYB": "DXY",
        "GC=F": "Gold", "CL=F": "WTI Oil", "BTC-USD": "Bitcoin",
    }
    data = pull_last_two_closes(list(names))
    lines = ["*Rates / FX / Commods*"]
    for sym, label in names.items():
        if sym not in data:
            lines.append(f"`{label:10s}`  n/a")
            continue
        last, prev = data[sym]
        p = pct(last, prev)
        lines.append(f"`{label:10s} {last:>9,.2f}  {fmt_pct(p)}` {arrow(p)}")
    return "\n".join(lines)


def section_sectors():
    etfs = {
        "XLK": "Tech", "XLF": "Fin", "XLE": "Energy",
        "XLV": "Health", "XLY": "Discr", "XLP": "Staples",
        "XLI": "Indl", "XLB": "Mat", "XLU": "Utils",
        "XLRE": "REIT", "XLC": "Comm",
    }
    data = pull_last_two_closes(list(etfs))
    rows = []
    for sym, label in etfs.items():
        if sym not in data:
            continue
        last, prev = data[sym]
        p = pct(last, prev)
        rows.append((p, label, sym))
    rows.sort(reverse=True)
    green = sum(1 for p, _, _ in rows if p is not None and p > 0)
    lines = [f"*Sector Rotation* _(breadth {green}/{len(rows)} green)_"]
    lines.append("_Leaders_")
    for p, label, sym in rows[:3]:
        lines.append(f"`{label:8s} {sym:4s} {fmt_pct(p)}` {arrow(p)}")
    lines.append("_Laggards_")
    for p, label, sym in rows[-3:]:
        lines.append(f"`{label:8s} {sym:4s} {fmt_pct(p)}` {arrow(p)}")
    return "\n".join(lines)


def section_top_movers():
    lines = ["*Top Movers (US)*"]
    for key, label in [
        ("day_gainers", "Gainers"),
        ("day_losers",  "Losers"),
        ("most_actives", "Most Active"),
    ]:
        try:
            resp = yf.screen(key, count=5)
            quotes = resp.get("quotes", [])[:5]
        except Exception as e:
            lines.append(f"_{label}_: error ({type(e).__name__})")
            continue
        lines.append(f"_{label}_")
        for q in quotes:
            sym = q.get("symbol", "?")
            price = q.get("regularMarketPrice")
            chg = q.get("regularMarketChangePercent")
            if price is None or chg is None:
                continue
            lines.append(f"`{sym:6s} {price:>8,.2f}  {chg:+6.2f}%` {arrow(chg)}")
    return "\n".join(lines)


def section_usd_inr():
    data = pull_last_two_closes(["INR=X"])
    if "INR=X" not in data:
        return "*USD / INR*\n`n/a`"
    last, prev = data["INR=X"]
    p = pct(last, prev)
    return f"*USD / INR*\n`₹{last:,.2f}  {fmt_pct(p)}` {arrow(p)}"


def section_watchlist(tickers):
    data = pull_last_two_closes(tickers)
    lines = ["*Watchlist*"]
    rows = []
    for t in tickers:
        if t not in data:
            continue
        last, prev = data[t]
        p = pct(last, prev)
        rows.append((p, t, last))
    rows.sort(key=lambda r: (r[0] is None, -(r[0] or 0)))
    for p, t, last in rows:
        lines.append(f"`{t:6s} {last:>8,.2f}  {fmt_pct(p)}` {arrow(p)}")
    return "\n".join(lines)


# ---------- message assembly ----------

def build_message(watchlist):
    now_pt = datetime.now(PT)
    header = f"*zStonks — Market Update*\n_{now_pt.strftime('%a %b %d, %I:%M %p %Z')}_"

    # Sections are pulled independently so a single 429 doesn't kill the rest.
    sections = [
        ("market_pulse",   section_market_pulse),
        ("rates_fx",       section_rates_fx_commods),
        ("sectors",        section_sectors),
        ("top_movers",     section_top_movers),
        ("watchlist",      lambda: section_watchlist(watchlist)),
        ("usd_inr",        section_usd_inr),
    ]

    parts = [header]
    for name, fn in sections:
        try:
            parts.append(fn())
        except Exception as e:
            parts.append(f"*{name}*: failed ({type(e).__name__}: {e})")
            print(f"[warn] section {name} failed: {e}", file=sys.stderr)

    msg = "\n\n".join(parts)
    if len(msg) > TG_MAX_LEN:
        msg = msg[:TG_MAX_LEN - 20].rstrip() + "\n\n_…truncated_"
    return msg


# ---------- telegram ----------

def send_telegram(msg, token, chat_id):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    r = requests.post(
        url,
        json={
            "chat_id": chat_id,
            "text": msg,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


# ---------- entrypoint ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="print message to stdout instead of sending")
    args = ap.parse_args()

    watchlist = [t.strip().upper() for t in
                 os.environ.get("WATCHLIST", ",".join(DEFAULT_WATCHLIST)).split(",")
                 if t.strip()]

    msg = build_message(watchlist)

    if args.dry_run:
        print(msg)
        print(f"\n---\nlength: {len(msg)} chars", file=sys.stderr)
        return

    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("error: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set",
              file=sys.stderr)
        sys.exit(2)

    send_telegram(msg, token, chat_id)
    print(f"sent {len(msg)} chars to chat {chat_id}", file=sys.stderr)


if __name__ == "__main__":
    main()
