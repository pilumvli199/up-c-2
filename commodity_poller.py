#!/usr/bin/env python3
"""
Commodity poller (Upstox -> Telegram) - more robust LTP extraction + debug logging.
Tracks configured instrument keys (FO/MCX) and posts LTP to Telegram every POLL_INTERVAL.
"""
import os, time, logging, requests, html, json
from urllib.parse import quote_plus

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

DEFAULT_EXPLICIT_KEYS = os.getenv("DEFAULT_EXPLICIT_KEYS") or ",".join([
    "MCX_FO|463267","MCX_FO|458302","MCX_FO|458303",
    "MCX_FO|440939","MCX_FO|463393","MCX_FO|463265","MCX_FO|463266","MCX_FO|466028",
])
EXPLICIT_INSTRUMENT_KEYS = os.getenv("EXPLICIT_INSTRUMENT_KEYS", DEFAULT_EXPLICIT_KEYS)
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL") or 60)

if not UPSTOX_ACCESS_TOKEN or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    logging.error("Missing env vars (UPSTOX_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)")
    raise SystemExit(1)

HEADERS = {"Accept":"application/json","Authorization":f"Bearer {UPSTOX_ACCESS_TOKEN}"}
LTP_URL = "https://api.upstox.com/v3/market-quote/ltp"

# --- helpers to extract LTP robustly ---
def find_ltp_in_obj(obj):
    """
    Try many possible field names and nested locations to find a numeric LTP.
    Returns float or None.
    """
    if obj is None:
        return None
    # direct numeric
    if isinstance(obj, (int, float)):
        return float(obj)
    # dict: check common keys first
    if isinstance(obj, dict):
        # common direct keys
        for k in ('ltp','last_traded_price','lastPrice','lastTradedPrice','last','last_price','lt'):
            if k in obj and obj[k] not in (None,""):
                try:
                    return float(obj[k])
                except Exception:
                    pass
        # nested: check standard containers
        for k in obj:
            try:
                val = find_ltp_in_obj(obj[k])
                if val is not None:
                    return val
            except Exception:
                continue
        return None
    # list: search elements
    if isinstance(obj, list):
        for el in obj:
            val = find_ltp_in_obj(el)
            if val is not None:
                return val
    # fallback: try parse numeric-like string
    try:
        s = str(obj).strip()
        if s.replace('.','',1).replace('-','',1).isdigit():
            return float(s)
    except Exception:
        pass
    return None

def get_ltps_for_keys(keys):
    """
    Query Upstox LTP endpoint for a list of instrument keys.
    Returns raw JSON or None, and logs HTTP error body for debug.
    """
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
        logging.error("Upstox LTP HTTPError %s: %.800s", status, body)
        return None
    except Exception as e:
        logging.exception("Upstox LTP fetch failed: %s", e)
        return None

def parse_upstox_resp(resp):
    """
    Return list of tuples: (instrument_key, trading_symbol/display, ltp_or_none, raw_payload)
    Works with responses shaped as {data: {ik: {...}}} or list-of-items.
    """
    out = []
    if not resp:
        return out
    # pattern: resp['data'] often holds mapping or list
    data = None
    if isinstance(resp, dict) and 'data' in resp:
        data = resp['data']
    else:
        data = resp

    # if data is a dict mapping instrument_key -> payload
    if isinstance(data, dict):
        for ik, payload in data.items():
            sym = None
            raw = payload
            if isinstance(payload, dict):
                sym = payload.get('trading_symbol') or payload.get('symbol') or ik
                ltp = find_ltp_in_obj(payload)
            else:
                sym = ik
                ltp = find_ltp_in_obj(payload)
            out.append((ik, sym, ltp, raw))
        return out

    # if data is a list of items
    if isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                continue
            ik = item.get('instrument_key') or item.get('instrumentKey') or item.get('symbol') or None
            sym = item.get('trading_symbol') or item.get('symbol') or ik
            ltp = find_ltp_in_obj(item)
            out.append((ik, sym, ltp, item))
        return out

    # fallback: try to parse top-level
    try:
        ltp = find_ltp_in_obj(resp)
        out.append((None, None, ltp, resp))
    except Exception:
        pass
    return out

def send_telegram(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id":TELEGRAM_CHAT_ID,"text":text,"parse_mode":"HTML","disable_web_page_preview":True}
    try:
        r = requests.post(url, json=payload, timeout=12)
        r.raise_for_status()
        return True
    except Exception as e:
        logging.warning("Telegram send failed: %s", e)
        return False

# --- main loop ---
def main():
    keys = [k.strip() for k in EXPLICIT_INSTRUMENT_KEYS.split(",") if k.strip()]
    if not keys:
        logging.error("No instrument keys configured; set EXPLICIT_INSTRUMENT_KEYS env or DEFAULT_EXPLICIT_KEYS")
        return
    logging.info("Starting poller for %d keys", len(keys))
    logging.info("Instrument keys: %s", ", ".join(keys))

    while True:
        try:
            raw = get_ltps_for_keys(keys)
            parsed = parse_upstox_resp(raw)
            # build message lines
            lines = []
            any_present = False
            for ik, sym, ltp, raw_payload in parsed:
                display = sym or ik or "UNKNOWN"
                if ltp is None:
                    lines.append(f"{display}: NA")
                else:
                    lines.append(f"{display}: {float(ltp):,.2f}")
                    any_present = True
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            header = f"📈 Upstox LTP Update — {ts}"
            text = header + "\n" + "\n".join(lines)
            # if everything NA, attach a tiny debug snippet of raw JSON (short) and log it
            if not any_present:
                # small safe snippet
                try:
                    raw_snip = json.dumps(raw, default=str)[:1500]
                except Exception:
                    raw_snip = str(raw)[:1000]
                debug_text = header + "\nAll LTPs None — raw response snippet:\n" + html.escape(raw_snip)
                logging.warning("All LTPs None this cycle. Raw response snippet: %s", raw_snip[:400])
                # send debug to telegram so you can see the API output too
                send_telegram(debug_text)
            else:
                send_telegram(text)
                logging.info("Sent update: %s", text.splitlines()[1] if len(text.splitlines())>1 else header)
        except Exception as e:
            logging.exception("Unhandled error in main loop: %s", e)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
