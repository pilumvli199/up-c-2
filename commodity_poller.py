#!/usr/bin/env python3
"""
Robust Commodity poller (Upstox -> Telegram)

Features added:
- Chunked requests to Upstox (CHUNK_SIZE)
- Retry for missing keys (RETRY_ATTEMPTS)
- Robust LTP extraction from nested payloads
- Send only when value changed (LAST_LTPS) unless SEND_ALL_EVERY_POLL=true
- Short diagnostic snippet when many values are None
"""
import os, time, logging, requests, html, json
from urllib.parse import quote_plus

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

# ---------- Config (env) ----------
UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Default keys you found earlier (override by EXPLICIT_INSTRUMENT_KEYS env if needed)
DEFAULT_EXPLICIT_KEYS = os.getenv("DEFAULT_EXPLICIT_KEYS") or ",".join([
    "MCX_FO|463267","MCX_FO|458302","MCX_FO|458303",
    "MCX_FO|440939","MCX_FO|463393","MCX_FO|463265","MCX_FO|463266","MCX_FO|466028",
])

EXPLICIT_INSTRUMENT_KEYS = os.getenv("EXPLICIT_INSTRUMENT_KEYS", DEFAULT_EXPLICIT_KEYS)
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL") or 60)
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE") or 4)           # number of keys per LTP request
RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS") or 2)   # per-missing-key retries
RETRY_DELAY = float(os.getenv("RETRY_DELAY") or 1.0)    # seconds between retries
SEND_ALL_EVERY_POLL = os.getenv("SEND_ALL_EVERY_POLL", "false").lower() in ("1","true","yes")
CHANGE_THRESHOLD_PCT = float(os.getenv("CHANGE_THRESHOLD_PCT") or 0.0)

if not UPSTOX_ACCESS_TOKEN or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    logging.error("Missing env vars (UPSTOX_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)")
    raise SystemExit(1)

HEADERS = {"Accept":"application/json","Authorization":f"Bearer {UPSTOX_ACCESS_TOKEN}"}
LTP_URL = "https://api.upstox.com/v3/market-quote/ltp"

# Persist last known LTPs to only send diffs
LAST_LTPS = {}

# ---------- helpers ----------
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

def find_ltp_in_obj(obj):
    """Robust search for numeric LTP inside nested dict/list/values."""
    if obj is None:
        return None
    if isinstance(obj, (int, float)):
        return float(obj)
    if isinstance(obj, dict):
        # check common key names first
        for k in ('ltp','last_traded_price','lastPrice','lastTradedPrice','last','last_price','lt'):
            if k in obj and obj[k] not in (None, ""):
                try:
                    return float(obj[k])
                except Exception:
                    pass
        # search nested values
        for v in obj.values():
            try:
                res = find_ltp_in_obj(v)
                if res is not None:
                    return res
            except Exception:
                continue
        return None
    if isinstance(obj, list):
        for el in obj:
            res = find_ltp_in_obj(el)
            if res is not None:
                return res
    # fallback: parse numeric string
    try:
        s = str(obj).strip()
        if s.replace('.','',1).replace('-','',1).isdigit():
            return float(s)
    except Exception:
        pass
    return None

def fetch_raw_for_chunk(keys_chunk):
    """Fetch raw response for chunk of keys; returns JSON or None (and logs HTTP body)."""
    if not keys_chunk:
        return None
    q = ",".join(keys_chunk)
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

def parse_response_into_map(raw):
    """
    Convert Upstox response into mapping: instrument_key -> payload (dict or value)
    Accepts shapes like {'data':{ik:payload}} or list-of-items or mapping.
    """
    out = {}
    if not raw:
        return out
    data = raw.get('data') if isinstance(raw, dict) and 'data' in raw else raw
    if isinstance(data, dict):
        # direct mapping: key -> payload
        for ik, payload in data.items():
            out[str(ik)] = payload
        return out
    if isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                continue
            ik = item.get('instrument_key') or item.get('instrumentKey') or item.get('symbol')
            if ik:
                out[str(ik)] = item
        return out
    # fallback: if top-level mapping
    if isinstance(raw, dict):
        for k, v in raw.items():
            # if key looks like instrument_key (contains MCX_FO etc) assume mapping
            if isinstance(k, str) and ('MCX' in k.upper() or 'NSE' in k.upper()):
                out[k] = v
    return out

# ---------- main poll loop ----------
def poll_once(keys_list):
    """
    Poll keys_list (list of instrument_key strings) in chunked fashion.
    Returns list of tuples (instrument_key, display_name, ltp_or_none)
    """
    results = []
    # process in chunks to avoid huge request or partial API behavior
    for i in range(0, len(keys_list), CHUNK_SIZE):
        chunk = keys_list[i:i+CHUNK_SIZE]
        raw = fetch_raw_for_chunk(chunk)
        mapping = parse_response_into_map(raw)
        # for each key in chunk, try to extract LTP
        missing = []
        for ik in chunk:
            payload = mapping.get(ik)
            ltp = find_ltp_in_obj(payload) if payload is not None else None
            display = None
            if isinstance(payload, dict):
                display = payload.get('trading_symbol') or payload.get('symbol') or ik
            else:
                display = ik
            results.append((ik, display, ltp, payload))
            if ltp is None:
                missing.append(ik)
        # For missing keys, try individual retries (sometimes chunk request omits some)
        if missing:
            for m in missing:
                retry_ltp = None
                retry_payload = None
                for attempt in range(RETRY_ATTEMPTS):
                    time.sleep(RETRY_DELAY)
                    raw2 = fetch_raw_for_chunk([m])
                    map2 = parse_response_into_map(raw2)
                    p2 = map2.get(m)
                    retry_ltp = find_ltp_in_obj(p2) if p2 is not None else None
                    retry_payload = p2
                    if retry_ltp is not None:
                        # update in results
                        for idx, (ik0, d0, l0, pl0) in enumerate(results):
                            if ik0 == m:
                                results[idx] = (ik0, d0, retry_ltp, retry_payload)
                                break
                        break
    # return simplified tuples
    return [(ik, display, ltp) for (ik, display, ltp, _) in results]

def decide_and_send(entries):
    """
    entries: list of (ik, display, ltp_or_none)
    Sends Telegram only when relevant (change or SEND_ALL_EVERY_POLL).
    """
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    header = f"📈 Upstox LTP Update — {ts}"
    lines = []
    send_any = False
    none_count = 0
    for ik, display, ltp in entries:
        disp = display or ik or "UNKNOWN"
        if ltp is None:
            lines.append(f"{disp}: NA")
            none_count += 1
            continue
        # compare with LAST_LTPS for change threshold
        prev = LAST_LTPS.get(ik)
        try:
            ltp_f = float(ltp)
        except Exception:
            ltp_f = None
        changed = False
        if ltp_f is not None:
            if prev is None:
                changed = True
            else:
                if CHANGE_THRESHOLD_PCT <= 0:
                    changed = (ltp_f != prev)
                else:
                    if prev == 0:
                        changed = (ltp_f != 0)
                    else:
                        diff_pct = abs((ltp_f - prev)/prev) * 100.0
                        changed = diff_pct >= CHANGE_THRESHOLD_PCT
        if ltp_f is not None:
            LAST_LTPS[ik] = ltp_f
        if changed:
            send_any = True
        lines.append(f"{disp}: {format(ltp_f, ',.2f') if ltp_f is not None else 'NA'}")
    # Decide to send
    if send_any or SEND_ALL_EVERY_POLL:
        # If most values are None, include small raw-diagnostic note
        text = header + "\n" + "\n".join(lines)
        if none_count >= max(3, len(entries)//2):
            text += "\n\n<code>Note: many values are NA this cycle. Check instrument keys or API response.</code>"
        send_telegram(text)
        logging.info("Sent Telegram update (%d items, %d NA).", len(entries), none_count)
    else:
        logging.info("No significant changes; skipped Telegram. %d NA.", none_count)

def main():
    keys = [k.strip() for k in EXPLICIT_INSTRUMENT_KEYS.split(",") if k.strip()]
    logging.info("Starting poller for %d keys (chunk=%d, retry=%d).", len(keys), CHUNK_SIZE, RETRY_ATTEMPTS)
    logging.info("Instrument keys: %s", ", ".join(keys))
    if not keys:
        logging.error("No instrument keys configured")
        return
    while True:
        try:
            entries = poll_once(keys)
            if entries:
                decide_and_send(entries)
            else:
                logging.warning("No entries parsed this cycle.")
        except Exception as e:
            logging.exception("Unhandled error in main loop: %s", e)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
