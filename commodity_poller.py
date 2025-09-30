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

# ====== UPDATE HERE: default instrument keys (you can override via EXPLICIT_INSTRUMENT_KEYS env) ======
DEFAULT_EXPLICIT_KEYS = ",".join([
    "MCX_FO|463267",   # GOLDTEN FUT 30 SEP 25
    "MCX_FO|458302",   # GOLDGUINEA FUT 30 SEP 25
    "MCX_FO|458303",   # GOLDPETAL FUT 30 SEP 25
    "MCX_FO|440939",   # GOLD FUT 03 OCT 25
    "MCX_FO|463393",   # GOLDM FUT 03 OCT 25
    "MCX_FO|463265",   # GOLDGUINEA FUT 31 OCT 25
    "MCX_FO|463266",   # GOLDPETAL FUT 31 OCT 25
    "MCX_FO|466028",   # GOLDTEN FUT 31 OCT 25
])

# Use env var if provided, otherwise fall back to DEFAULT_EXPLICIT_KEYS
EXPLICIT_INSTRUMENT_KEYS = os.getenv("EXPLICIT_INSTRUMENT_KEYS", DEFAULT_EXPLICIT_KEYS)
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
    if not keys:
        return None
    q = ",".join(keys)
    url = LTP_URL + "?instrument_key=" + quote_plus(q)
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as he:
        body = he.response.text if he.response is not None else ""
        status = getattr(he.response, "status_code", "??")
        logging.error("Upstox LTP HTTPError %s: %s", status, body[:1000])
        return None
    except Exception as e:
        logging.exception("Upstox LTP fetch failed: %s", e)
        return None

def parse_resp(resp):
    out = []
    if not resp:
        return out
    # handle multiple shapes
    if isinstance(resp, dict) and 'data' in resp:
        data = resp['data']
        # data may be dict or list
        if isinstance(data, dict):
            # mapping instrument_key -> payload
            for ik, payload in data.items():
                ltp = None
                if isinstance(payload, dict):
                    for key in ('ltp', 'lastPrice', 'last_traded_price'):
                        if key in payload and payload[key] is not None:
                            ltp = payload[key]; break
                    sym = payload.get('trading_symbol') or payload.get('symbol') or ik
                else:
                    sym = ik
                out.append((ik, sym, ltp))
        elif isinstance(data, list):
            for item in data:
                ik = item.get('instrument_key') or item.get('instrumentKey') or item.get('symbol') or None
                sym = item.get('trading_symbol') or item.get('symbol') or ik
                ltp = item.get('ltp') or item.get('lastPrice') or item.get('last_traded_price')
                out.append((ik, sym, ltp))
    elif isinstance(resp, dict):
        # fallback: try parse mapping
        for ik, payload in resp.items():
            if isinstance(payload, dict):
                ltp = payload.get('ltp') or payload.get('lastPrice') or payload.get('last_traded_price')
                sym = payload.get('trading_symbol') or payload.get('symbol') or ik
                out.append((ik, sym, ltp))
    elif isinstance(resp, list):
        for item in resp:
            ik = item.get('instrument_key') or item.get('symbol') or None
            sym = item.get('trading_symbol') or item.get('symbol') or ik
            ltp = item.get('ltp') or item.get('lastPrice') or item.get('last_traded_price')
            out.append((ik, sym, ltp))
    return out

def main():
    # prepare keys list
    keys = [k.strip() for k in EXPLICIT_INSTRUMENT_KEYS.split(",") if k.strip()]
    logging.info("Starting poller for %d keys", len(keys))
    logging.info("Instrument keys: %s", ", ".join(keys))
    if not keys:
        logging.error("No instrument keys configured")
        return
    while True:
        try:
            resp = get_ltps(keys)
            parsed = parse_resp(resp)
            if parsed:
                ts = time.strftime("%Y-%m-%d %H:%M:%S")
                msg = "\n".join([f"{sym}: {ltp}" for _, sym, ltp in parsed])
                send_telegram(f"📈 Upstox LTP Update — {ts}\n{msg}")
                logging.info("Sent update for %d items", len(parsed))
            else:
                logging.info("No LTP data received for this cycle.")
        except Exception as e:
            logging.exception("Error in main loop: %s", e)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
