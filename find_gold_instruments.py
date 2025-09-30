#!/usr/bin/env python3
"""
Find GOLD contracts in Upstox MCX instruments
"""
import requests, gzip, io, json

MCX_URL = "https://assets.upstox.com/market-quote/instruments/exchange/MCX.json.gz"

def main():
    print("Downloading MCX instruments...")
    r = requests.get(MCX_URL, timeout=60)
    r.raise_for_status()
    gz = gzip.GzipFile(fileobj=io.BytesIO(r.content))
    txt = gz.read().decode("utf-8", errors="ignore")
    try:
        items = json.loads(txt)
    except Exception:
        items = [json.loads(line) for line in txt.splitlines() if line.strip()]

    for it in items:
        if "GOLD" in str(it).upper():
            print(it.get("instrument_key"), it.get("trading_symbol"), it.get("expiry"))

if __name__ == "__main__":
    main()
