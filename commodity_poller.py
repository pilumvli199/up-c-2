#!/usr/bin/env python3
"""
Commodity poller (Upstox -> Telegram)
Tracks LTP for configured instruments (e.g. GOLD).
"""

import os, time, logging, requests, html
from urllib.parse import quote_plus

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
EXPLICIT_INSTRUMENT_KEYS = os.getenv("EXPLICIT_INSTRUMENT_KEYS") or ""
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL") or 60)

if not UPSTOX_ACCESS_TOKEN or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    logging.error("Missing env vars (UPSTOX_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)")
    raise SystemExit(1)

HEADERS = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}
LTP_URL = "https://api.upstox.com/v3/market-quote/ltp"

def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        return True
    except Exception as e:
        logging.warning("Telegram send failed: %s", e)
        return False

def get_ltps(keys):
    url = LTP_URL + "?instrument_key=" + quote_plus(",".join(keys))
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return r.json()

def parse_resp(resp):
    out = []
    if isinstance(resp, dict) and "data" in resp:
        for k, v in resp["data"].items():
            ltp = v.get("ltp") or v.get("lastPrice") or v.get("last_traded_price")
            sym = v.get("trading_symbol") or k
            out.append((k, sym, ltp))
    return out

def main():
    keys = [k.strip() for k in EXPLICIT_INSTRUMENT_KEYS.split(",") if k.strip()]
    if not keys:
        logging.error("No instrument keys configured")
        return
    logging.info("Starting poller for %d keys", len(keys))
    while True:
        try:
            resp = get_ltps(keys)
            parsed = parse_resp(resp)
            if parsed:
                ts = time.strftime("%Y-%m-%d %H:%M:%S")
                msg = "\n".join([f"{sym}: {ltp}" for _, sym, ltp in parsed])
                send_telegram(f"📈 Upstox LTP Update — {ts}\n{msg}")
                logging.info("Sent update: %s", msg)
        except Exception as e:
            logging.exception("Error: %s", e)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
