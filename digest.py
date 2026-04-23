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
import re
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()  # loads .env for local dev; no-op in CI where env vars are already set

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


def pull_ytd(tickers):
    """Return {ticker: (last_close, prev_close, ytd_start_close)} from one batched YTD pull."""
    if not tickers:
        return {}
    data = yf.download(
        tickers=" ".join(tickers),
        period="ytd",
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
            out[t] = (
                float(df["Close"].iloc[-1]),
                float(df["Close"].iloc[-2]),
                float(df["Close"].iloc[0]),
            )
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


def section_did_you_know():
    """Call Gemini for a random stock-market trivia. Silent no-op if key missing."""
    key = os.environ.get("GEMINI_API_KEY")
    if not key or key.startswith("your-"):
        return None
    prompt = (
        "Share ONE interesting, lesser-known fact about the stock market, "
        "a famous company/stock, a historical trading event, or a finance/trading term. "
        "1-2 sentences max. Start directly with the fact, no preamble like 'Did you know'. "
        "Avoid clichés. Vary the topic each time."
    )
    try:
        url = (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-2.5-flash:generateContent?key={key}"
        )
        r = requests.post(
            url,
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": 1.2,
                    "maxOutputTokens": 300,
                    # Gemini 2.5 spends output tokens on internal "thinking" by default;
                    # disable for this simple factual task so the budget goes to the answer.
                    "thinkingConfig": {"thinkingBudget": 0},
                },
            },
            timeout=20,
        )
        r.raise_for_status()
        fact = r.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
        # strip markdown special chars so Telegram doesn't mangle the output
        fact = fact.replace("*", "").replace("_", "").replace("`", "")
        return f"*Did You Know?*\n_{fact}_"
    except Exception as e:
        print(f"[warn] gemini call failed: {e}", file=sys.stderr)
        return None


def section_watchlist(tickers):
    data = pull_ytd(tickers)
    lines = ["*zWatchList*"]
    lines.append(f"`{'Sym':<6} {'Price':>8}  {'Day':>7}  {'YTD':>7}`")
    for t in tickers:
        if t not in data:
            continue
        last, prev, ytd_start = data[t]
        day_p = pct(last, prev)
        ytd_p = pct(last, ytd_start)
        lines.append(
            f"`{t:<6} {last:>8,.2f}  {fmt_pct(day_p)}  {fmt_pct(ytd_p)}` {arrow(day_p)}"
        )
    return "\n".join(lines)


def _news_title(item):
    """Handle both old flat and new nested yfinance news shapes."""
    if "title" in item:
        return item.get("title")
    content = item.get("content") or {}
    return content.get("title")


def _gemini_summarize_headlines(headlines):
    """One batched Gemini call to summarize N headlines. Returns list of summaries aligned to input."""
    key = os.environ.get("GEMINI_API_KEY")
    if not key or key.startswith("your-"):
        return []

    prompt_lines = [
        "Rewrite each market news headline below as ONE concise factual statement "
        "(max 20 words).",
        "Rules:",
        "- State the news directly. Start with the subject (company, person, event).",
        "- NEVER use meta-phrases like 'This headline...', 'The article...', "
        "  'The news...', 'This discusses...', 'This story...'.",
        "- If the headline is vague or a teaser, rephrase its core topic as a "
        "  direct statement. Do not invent facts.",
        "- No opinions, no preamble.",
        "Output format — numbered, one per line, nothing else:",
        "1. summary sentence.",
        "2. summary sentence.",
        "",
        "Headlines:",
    ]
    for i, h in enumerate(headlines, 1):
        prompt_lines.append(f"{i}. {h}")
    prompt = "\n".join(prompt_lines)

    try:
        url = (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-2.5-flash:generateContent?key={key}"
        )
        r = requests.post(
            url,
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": 0.3,
                    "maxOutputTokens": 500,
                    "thinkingConfig": {"thinkingBudget": 0},
                },
            },
            timeout=30,
        )
        r.raise_for_status()
        text = r.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception as e:
        print(f"[warn] gemini batch news: {e}", file=sys.stderr)
        return []

    results = [None] * len(headlines)
    for line in text.splitlines():
        line = line.strip().lstrip("-*• ").strip()
        if not line:
            continue
        m = re.match(r"^(\d+)[\.\:\)]\s*(.+)$", line)
        if not m:
            continue
        idx = int(m.group(1)) - 1
        summary = m.group(2).strip().replace("*", "").replace("_", "").replace("`", "")
        if 0 <= idx < len(headlines) and summary:
            results[idx] = summary
    return results


def section_znews():
    """Top 5 trending market news, summarized via one Gemini call. Uses ^GSPC news feed."""
    key = os.environ.get("GEMINI_API_KEY")
    if not key or key.startswith("your-"):
        return None
    try:
        news = yf.Ticker("^GSPC").news or []
    except Exception as e:
        print(f"[warn] znews fetch: {e}", file=sys.stderr)
        return None

    headlines = []
    for n in news[:5]:
        t = _news_title(n)
        if t:
            headlines.append(t)
    if not headlines:
        return None

    summaries = _gemini_summarize_headlines(headlines)
    if not summaries:
        return None

    lines = ["*zNews*"]
    for i, s in enumerate(summaries, 1):
        if s:
            lines.append(f"{i}. {s}")
    return "\n".join(lines) if len(lines) > 1 else None


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
        ("znews",          section_znews),
        ("usd_inr",        section_usd_inr),
        ("did_you_know",   section_did_you_know),
    ]

    parts = [header]
    for name, fn in sections:
        try:
            result = fn()
            if result:
                parts.append(result)
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
