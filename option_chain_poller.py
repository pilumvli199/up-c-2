#!/usr/bin/env python3
"""
Option Chain poller for NIFTY + TCS
"""

import os, time, logging, requests, html
from urllib.parse import quote_plus

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
OPTION_SYMBOL_NIFTY = os.getenv("OPTION_SYMBOL_NIFTY") or "NSE_INDEX|Nifty 50"
OPTION_EXPIRY_NIFTY = os.getenv("OPTION_EXPIRY_NIFTY") or ""
OPTION_SYMBOL_TCS = os.getenv("OPTION_SYMBOL_TCS") or "NSE_EQ|INE467B01029"
OPTION_EXPIRY_TCS = os.getenv("OPTION_EXPIRY_TCS") or ""
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL") or 60)

HEADERS = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}
CHAIN_URL = "https://api.upstox.com/v3/option/chain"

def send_telegram(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"})
    except Exception as e:
        logging.warning("Telegram send failed: %s", e)

def fetch_chain(symbol, expiry):
    url = CHAIN_URL + "?symbol=" + quote_plus(symbol) + "&expiry_date=" + quote_plus(expiry)
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.json()

def parse_chain(data):
    strikes = []
    if "data" in data and isinstance(data["data"], list):
        for row in data["data"]:
            strikes.append((row.get("strike_price"), row.get("ce"), row.get("pe")))
    return strikes

def main():
    while True:
        try:
            if OPTION_EXPIRY_NIFTY:
                data = fetch_chain(OPTION_SYMBOL_NIFTY, OPTION_EXPIRY_NIFTY)
                strikes = parse_chain(data)
                send_telegram(f"NIFTY Chain: {len(strikes)} strikes fetched")
            if OPTION_EXPIRY_TCS:
                data = fetch_chain(OPTION_SYMBOL_TCS, OPTION_EXPIRY_TCS)
                strikes = parse_chain(data)
                send_telegram(f"TCS Chain: {len(strikes)} strikes fetched")
        except Exception as e:
            logging.warning("Error fetching chain: %s", e)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
