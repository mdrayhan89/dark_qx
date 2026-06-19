#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Quotex Live Candle API – with automatic proxy fallback
"""
import asyncio
import threading
import time
import os
import sys
import requests
import random
from typing import Optional, Dict, List, Tuple, Set
from datetime import datetime

sys.stdout.reconfigure(line_buffering=True)

# ----- Proxy fetching -----
def fetch_free_proxy():
    try:
        url = "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&timeout=10000&country=all"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            proxies = [line.strip() for line in resp.text.splitlines() if line.strip()]
            if proxies:
                proxy = random.choice(proxies)
                return f"socks5://{proxy}"
    except Exception as e:
        print(f"Proxy fetch error: {e}")
    return None

# Get proxy from env or auto-fetch
PROXY_URL = os.environ.get("QUOTEX_PROXY")
if PROXY_URL == "":
    PROXY_URL = None
    print("🔓 Proxy explicitly disabled via QUOTEX_PROXY=''")
elif not PROXY_URL:
    print("🔍 No QUOTEX_PROXY set, fetching free proxy...")
    PROXY_URL = fetch_free_proxy()
    if PROXY_URL:
        print(f"✅ Using proxy: {PROXY_URL}")
    else:
        print("⚠️ No proxy available – will try direct connection.")

# Apply proxy if we have one
if PROXY_URL:
    os.environ['HTTP_PROXY'] = PROXY_URL
    os.environ['HTTPS_PROXY'] = PROXY_URL
    os.environ['ALL_PROXY'] = PROXY_URL

# SSL
import certifi
os.environ['SSL_CERT_FILE'] = certifi.where()
os.environ['WEBSOCKET_CLIENT_CA_BUNDLE'] = certifi.where()

# Patch WebSocket for proxy (only if proxy is set)
if PROXY_URL:
    try:
        import websocket
        original_create = websocket.create_connection
        def patched_create(*args, **kwargs):
            kwargs['proxy'] = PROXY_URL
            return original_create(*args, **kwargs)
        websocket.create_connection = patched_create
        print("🔌 WebSocket proxy patch applied.")
    except Exception as e:
        print(f"⚠️ Could not patch websocket: {e}")

try:
    from pyquotex.stable_api import Quotex
    from pyquotex.utils.processor import process_candles
except ImportError as e:
    print(f"❌ Missing dependency: {e}")
    sys.exit(1)

# ----- Asset maps (same as before) -----
ASSET_DISPLAY_MAP: Dict[str, str] = {}
forex_assets = {
    "AUDCAD": "AUD/CAD", "AUDCAD_otc": "AUD/CAD (OTC)", "AUDCHF": "AUD/CHF", "AUDCHF_otc": "AUD/CHF (OTC)",
    "AUDJPY": "AUD/JPY", "AUDJPY_otc": "AUD/JPY (OTC)", "AUDNZD_otc": "AUD/NZD (OTC)", "AUDUSD": "AUD/USD",
    "AUDUSD_otc": "AUD/USD (OTC)", "CADJPY": "CAD/JPY", "CADJPY_otc": "CAD/JPY (OTC)", "CADCHF_otc": "CAD/CHF (OTC)",
    "CHFJPY": "CHF/JPY", "CHFJPY_otc": "CHF/JPY (OTC)", "EURAUD": "EUR/AUD", "EURAUD_otc": "EUR/AUD (OTC)",
    "EURCAD": "EUR/CAD", "EURCAD_otc": "EUR/CAD (OTC)", "EURCHF": "EUR/CHF", "EURCHF_otc": "EUR/CHF (OTC)",
    "EURGBP": "EUR/GBP", "EURGBP_otc": "EUR/GBP (OTC)", "EURJPY": "EUR/JPY", "EURJPY_otc": "EUR/JPY (OTC)",
    "EURNZD_otc": "EUR/NZD (OTC)", "EURSGD_otc": "EUR/SGD (OTC)", "EURUSD": "EUR/USD", "EURUSD_otc": "EUR/USD (OTC)",
    "GBPAUD": "GBP/AUD", "GBPAUD_otc": "GBP/AUD (OTC)", "GBPCAD": "GBP/CAD", "GBPCAD_otc": "GBP/CAD (OTC)",
    "GBPCHF": "GBP/CHF", "GBPCHF_otc": "GBP/CHF (OTC)", "GBPJPY": "GBP/JPY", "GBPJPY_otc": "GBP/JPY (OTC)",
    "GBPNZD_otc": "GBP/NZD (OTC)", "GBPUSD": "GBP/USD", "GBPUSD_otc": "GBP/USD (OTC)", "NZDCAD_otc": "NZD/CAD (OTC)",
    "NZDCHF_otc": "NZD/CHF (OTC)", "NZDJPY_otc": "NZD/JPY (OTC)", "NZDUSD_otc": "NZD/USD (OTC)", "USDCAD": "USD/CAD",
    "USDCAD_otc": "USD/CAD (OTC)", "USDCHF": "USD/CHF", "USDCHF_otc": "USD/CHF (OTC)", "USDJPY": "USD/JPY",
    "USDJPY_otc": "USD/JPY (OTC)", "USDARS_otc": "USD/ARS (OTC)", "USDBDT_otc": "USD/BDT (OTC)", "USDCOP_otc": "USD/COP (OTC)",
    "USDDZD_otc": "USD/DZD (OTC)", "USDEGP_otc": "USD/EGP (OTC)", "USDIDR_otc": "USD/IDR (OTC)", "USDINR_otc": "USD/INR (OTC)",
    "USDMXN_otc": "USD/MXN (OTC)", "USDNGN_otc": "USD/NGN (OTC)", "USDPHP_otc": "USD/PHP (OTC)", "USDPKR_otc": "USD/PKR (OTC)",
    "USDTRY_otc": "USD/TRY (OTC)", "USDZAR_otc": "USD/ZAR (OTC)",
}
ASSET_DISPLAY_MAP.update(forex_assets)
crypto_assets = {
    "ADAUSD_otc": "Cardano (OTC)", "APTUSD_otc": "Aptos (OTC)", "ARBUSD_otc": "Arbitrum (OTC)", "ATOUSD_otc": "ATO (OTC)",
    "AVAUSD_otc": "Avalanche (OTC)", "AXSUSD_otc": "Axie Infinity (OTC)", "BCHUSD_otc": "Bitcoin Cash (OTC)",
    "BNBUSD_otc": "Binance Coin (OTC)", "BONUSD_otc": "Bonk (OTC)", "BTCUSD_otc": "Bitcoin (OTC)", "DASUSD_otc": "Dash (OTC)",
    "DOGUSD_otc": "Dogecoin (OTC)", "DOTUSD_otc": "Polkadot (OTC)", "ETCUSD_otc": "Ethereum Classic (OTC)",
    "ETHUSD_otc": "Ethereum (OTC)", "FLOUSD_otc": "Floki (OTC)", "GALUSD_otc": "Gala (OTC)", "HMSUSD_otc": "Hamster Kombat (OTC)",
    "LINUSD_otc": "Chainlink (OTC)", "LTCUSD_otc": "Litecoin (OTC)", "MELUSD_otc": "Melania Meme (OTC)",
    "SHIBUSD_otc": "Shiba Inu (OTC)", "SOLUSD_otc": "Solana (OTC)", "TIAUSD_otc": "Celestia (OTC)", "TONUSD_otc": "Toncoin (OTC)",
    "TRUUSD_otc": "TrueFi (OTC)", "TRXUSD_otc": "TRON (OTC)", "WIFUSD_otc": "Dogwifhat (OTC)", "XRPUSD_otc": "Ripple (OTC)",
    "ZECUSD_otc": "Zcash (OTC)",
}
ASSET_DISPLAY_MAP.update(crypto_assets)
commodities_assets = {
    "XAUUSD": "Gold", "XAUUSD_otc": "Gold (OTC)", "XAGUSD": "Silver", "XAGUSD_otc": "Silver (OTC)",
    "UKBrent_otc": "UK Brent (OTC)", "USCrude_otc": "US Crude (OTC)",
}
ASSET_DISPLAY_MAP.update(commodities_assets)
stocks_assets = {
    "AXP_otc": "American Express (OTC)", "BA_otc": "Boeing Company (OTC)", "FB_otc": "Facebook (OTC)",
    "INTC_otc": "Intel (OTC)", "JNJ_otc": "Johnson & Johnson (OTC)", "MCD_otc": "McDonald's (OTC)",
    "MSFT_otc": "Microsoft (OTC)", "PFE_otc": "Pfizer Inc (OTC)", "PEPUSD_otc": "PepsiCo (OTC)",
}
ASSET_DISPLAY_MAP.update(stocks_assets)
indices_assets = {
    "DJIUSD": "Dow Jones", "NDXUSD": "NASDAQ 100", "F40EUR": "CAC 40", "FTSGBP": "FTSE 100",
    "HSIHKD": "Hong Kong 50", "IBXEUR": "IBEX 35", "JPXJPY": "Nikkei 225", "CHIA50": "China A50",
    "STXEUR": "EURO STOXX 50",
}
ASSET_DISPLAY_MAP.update(indices_assets)

DISPLAY_TO_INTERNAL = {v: k for k, v in ASSET_DISPLAY_MAP.items()}

TIMEFRAMES = {
    "5s": 5, "10s": 10, "15s": 15, "30s": 30,
    "1m": 60, "2m": 120, "3m": 180, "5m": 300,
    "10m": 600, "15m": 900, "30m": 1800,
    "1h": 3600, "4h": 14400
}
TIMEFRAME_API_MAP = {
    "5s": "5S", "10s": "10S", "15s": "15S", "30s": "30S",
    "1m": "M1", "2m": "M2", "3m": "M3", "5m": "M5",
    "10m": "M10", "15m": "M15", "30m": "M30",
    "1h": "H1", "4h": "H4"
}

# ---- Global state ----
CLIENT: Optional[Quotex] = None
CANDLES: Dict[str, Dict[str, List[dict]]] = {}
CURRENT_CANDLE: Dict[str, Dict[str, dict]] = {}
STREAMING_ASSETS: Set[str] = set()
CONNECTED = False

# ---- Async loop ----
ASYNC_LOOP = asyncio.new_event_loop()
def start_loop():
    asyncio.set_event_loop(ASYNC_LOOP)
    ASYNC_LOOP.run_forever()
threading.Thread(target=start_loop, daemon=True, name="AsyncLoop").start()

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def is_websocket_connected() -> bool:
    try:
        if not CLIENT or not CLIENT.api:
            return False
        if hasattr(CLIENT.api, '_is_connected'):
            return bool(CLIENT.api._is_connected)
        if hasattr(CLIENT.api, 'websocket_client'):
            ws = CLIENT.api.websocket_client
            if hasattr(ws, 'wss') and hasattr(ws.wss, 'sock'):
                return ws.wss.sock is not None and getattr(ws.wss.sock, 'connected', False)
            if hasattr(ws, 'connected'):
                return bool(ws.connected)
        if hasattr(CLIENT.api, 'check_connect'):
            return CLIENT.api.check_connect()
        return True
    except Exception:
        return False

def process_candle_data(raw_candles: List[dict], period: int) -> List[dict]:
    if not raw_candles:
        return []
    if raw_candles and not raw_candles[0].get("open"):
        try:
            return process_candles(raw_candles, period)
        except Exception as e:
            print(f"⚠️ process_candles failed: {e}")
    formatted = []
    for c in raw_candles:
        if not isinstance(c, dict):
            continue
        try:
            if not all(k in c for k in ("time", "open", "high", "low", "close")):
                continue
            candle_time = int(float(c["time"]))
            aligned_time = (candle_time // period) * period
            formatted.append({
                "time": aligned_time,
                "open": float(c["open"]), "high": float(c["high"]),
                "low": float(c["low"]), "close": float(c["close"])
            })
        except (ValueError, KeyError, TypeError):
            continue
    formatted.sort(key=lambda x: x["time"])
    return formatted

def update_candle(asset_display: str, frame: str, price: float, ts_sec: int):
    global CANDLES, CURRENT_CANDLE
    duration = TIMEFRAMES.get(frame, 60)
    candle_start = (ts_sec // duration) * duration
    curr = CURRENT_CANDLE.get(asset_display, {}).get(frame, {})
    if not curr or curr.get("time") != candle_start:
        if curr:
            if asset_display not in CANDLES:
                CANDLES[asset_display] = {}
            if frame not in CANDLES[asset_display]:
                CANDLES[asset_display][frame] = []
            CANDLES[asset_display][frame].append(curr.copy())
            if len(CANDLES[asset_display][frame]) > 200:
                CANDLES[asset_display][frame] = CANDLES[asset_display][frame][-200:]
        if asset_display not in CURRENT_CANDLE:
            CURRENT_CANDLE[asset_display] = {}
        CURRENT_CANDLE[asset_display][frame] = {
            "time": int(candle_start), "open": float(price), "high": float(price),
            "low": float(price), "close": float(price)
        }
    else:
        if price > curr["high"]: curr["high"] = float(price)
        if price < curr["low"]: curr["low"] = float(price)
        curr["close"] = float(price)

# ------------------------------------------------------------
# Quotex Connection with fallback
# ------------------------------------------------------------
async def connect_to_quotex(email: str, password: str, use_proxy: bool = True) -> Tuple[bool, str]:
    global CLIENT, CONNECTED
    try:
        print(f"🔐 Connecting to Quotex (proxy={use_proxy})...")
        # If we want to bypass proxy, clear environment variables temporarily
        if not use_proxy:
            old_http = os.environ.pop('HTTP_PROXY', None)
            old_https = os.environ.pop('HTTPS_PROXY', None)
            old_all = os.environ.pop('ALL_PROXY', None)
            # Also reset websocket patch? We'll just create a new client without proxy.

        CLIENT = Quotex(email=email, password=password, host="qxbroker.com", lang="en")
        check, reason = await CLIENT.connect()
        if not check:
            print(f"❌ Connection failed: {reason}")
            return False, reason
        await CLIENT.change_account("PRACTICE")
        await CLIENT.get_all_assets()
        CONNECTED = True
        print("✅ Quotex connected successfully!")
        return True, ""
    except Exception as e:
        print(f"❌ Exception during connect: {e}")
        return False, str(e)
    finally:
        # Restore proxy env vars if they were removed
        if not use_proxy and PROXY_URL:
            os.environ['HTTP_PROXY'] = PROXY_URL
            os.environ['HTTPS_PROXY'] = PROXY_URL
            os.environ['ALL_PROXY'] = PROXY_URL

async def start_streaming(asset_display: str):
    global CANDLES, CURRENT_CANDLE, STREAMING_ASSETS
    if not CLIENT or not CONNECTED:
        print("❌ Not connected")
        return
    if asset_display in STREAMING_ASSETS:
        print(f"ℹ️ Already streaming {asset_display}")
        return

    internal = DISPLAY_TO_INTERNAL.get(asset_display)
    if not internal:
        print(f"❌ Unknown asset display name: {asset_display}")
        return

    period_sec = TIMEFRAMES.get("1m", 60)
    try:
        print(f"📥 Loading history for {asset_display}...")
        hist = await CLIENT.get_candles(asset=internal, end_from_time=time.time(),
                                        offset=199 * period_sec, period=period_sec)
        candles = process_candle_data(hist, period_sec)
        if asset_display not in CANDLES:
            CANDLES[asset_display] = {}
        CANDLES[asset_display]["1m"] = candles[-199:]
        print(f"📥 Loaded {len(candles)} candles for {asset_display}")
    except Exception as e:
        print(f"⚠️ Failed to load history for {asset_display}: {e}")

    try:
        await CLIENT.start_realtime_price(internal, period_sec)
        STREAMING_ASSETS.add(asset_display)
        print(f"🔄 Started streaming {asset_display}")
        asyncio.create_task(realtime_price_loop(asset_display))
    except Exception as e:
        print(f"❌ Failed to start streaming {asset_display}: {e}")

async def realtime_price_loop(asset_display: str):
    internal = DISPLAY_TO_INTERNAL.get(asset_display)
    if not internal:
        return
    while asset_display in STREAMING_ASSETS and CONNECTED:
        try:
            data = await CLIENT.get_realtime_price(internal)
            if data and len(data) > 0:
                latest = data[-1]
                price = float(latest.get("price", latest.get("close", 0)))
                timestamp = latest.get("time", time.time())
                if price > 0 and timestamp > 0:
                    ts_sec = int(float(timestamp))
                    for frame in TIMEFRAMES:
                        update_candle(asset_display, frame, price, ts_sec)
            await asyncio.sleep(0.2)
        except Exception as e:
            print(f"⚠️ realtime loop error for {asset_display}: {e}")
            await asyncio.sleep(1)

# ------------------------------------------------------------
# Flask API
# ------------------------------------------------------------
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/')
def root():
    return jsonify({
        "Owner": "DARK-X-RAYHAN",
        "Telegram": "@mdrayhan85",
        "service": "Quotex Live Candle API",
        "status": "running",
        "connected": CONNECTED,
        "proxy_used": bool(PROXY_URL),
        "streaming_assets": list(STREAMING_ASSETS),
        "env_vars_set": bool(os.environ.get("QUOTEX_EMAIL") and os.environ.get("QUOTEX_PASSWORD")),
        "endpoints": {
            "/api/candles?pair=USDPKR_otc&timeframe=1m&count=10": "Get candle data",
            "/api/assets": "List all assets",
            "/api/timeframes": "List all timeframes"
        }
    })

@app.route('/api/candles', methods=['GET'])
def get_candles():
    pair = request.args.get('pair')
    timeframe = request.args.get('timeframe', '1m')
    count = request.args.get('count', default=10, type=int)

    if not pair:
        return jsonify({
            "Owner": "DARK-X-RAYHAN",
            "Telegram": "@mdrayhan85",
            "success": False,
            "error": "Missing 'pair' parameter"
        }), 400

    # Resolve asset
    display_name = None
    internal_name = None

    if pair in ASSET_DISPLAY_MAP:
        internal_name = pair
        display_name = ASSET_DISPLAY_MAP[pair]
    elif pair in DISPLAY_TO_INTERNAL:
        display_name = pair
        internal_name = DISPLAY_TO_INTERNAL[pair]
    else:
        return jsonify({
            "Owner": "DARK-X-RAYHAN",
            "Telegram": "@mdrayhan85",
            "success": False,
            "error": f"Unknown asset: '{pair}'. Use /api/assets to see available pairs."
        }), 400

    # Start streaming if not already
    if CONNECTED and display_name not in STREAMING_ASSETS:
        print(f"⏳ Starting streaming for {display_name}")
        asyncio.run_coroutine_threadsafe(start_streaming(display_name), ASYNC_LOOP)

    # Get candles
    candles = []
    if display_name in CANDLES and timeframe in CANDLES[display_name]:
        candles = CANDLES[display_name][timeframe]
    candles = candles[-count:] if candles else []

    # Format response
    api_tf = TIMEFRAME_API_MAP.get(timeframe, timeframe.upper())
    market_name = ASSET_DISPLAY_MAP.get(internal_name, display_name)
    data = []
    for idx, c in enumerate(candles, start=1):
        dt = datetime.fromtimestamp(c["time"]).strftime("%Y-%m-%d %H:%M:%S")
        open_val = f"{c['open']:.5f}".rstrip('0').rstrip('.')
        high_val = f"{c['high']:.5f}".rstrip('0').rstrip('.')
        low_val = f"{c['low']:.5f}".rstrip('0').rstrip('.')
        close_val = f"{c['close']:.5f}".rstrip('0').rstrip('.')
        color = "green" if c['close'] >= c['open'] else "red"
        data.append({
            "id": str(idx),
            "pair": internal_name.upper(),
            "market_name": market_name,
            "timeframe": api_tf,
            "candle_time": dt,
            "open": open_val,
            "high": high_val,
            "low": low_val,
            "close": close_val,
            "volume": "0",
            "color": color,
            "created_at": dt
        })

    return jsonify({
        "Owner": "DARK-X-RAYHAN",
        "Telegram": "@mdrayhan85",
        "success": True,
        "requested_pair": pair,
        "total_count": len(data),
        "data": data
    })

@app.route('/api/assets', methods=['GET'])
def list_assets():
    assets = [{"internal": k, "display": v} for k, v in ASSET_DISPLAY_MAP.items()]
    return jsonify({
        "Owner": "DARK-X-RAYHAN",
        "Telegram": "@mdrayhan85",
        "success": True,
        "count": len(assets),
        "assets": assets
    })

@app.route('/api/timeframes', methods=['GET'])
def list_timeframes():
    return jsonify({
        "Owner": "DARK-X-RAYHAN",
        "Telegram": "@mdrayhan85",
        "success": True,
        "timeframes": list(TIMEFRAMES.keys())
    })

# ------------------------------------------------------------
# Background startup with fallback
# ------------------------------------------------------------
def startup_background():
    time.sleep(3)
    email = os.environ.get("QUOTEX_EMAIL")
    password = os.environ.get("QUOTEX_PASSWORD")
    if not email or not password:
        print("❌ QUOTEX_EMAIL and QUOTEX_PASSWORD must be set")
        return

    # Try with proxy first (if PROXY_URL is set)
    use_proxy = bool(PROXY_URL)
    for attempt in range(1, 4):
        print(f"🔄 Login attempt {attempt}/3 (proxy={use_proxy})...")
        future = asyncio.run_coroutine_threadsafe(
            connect_to_quotex(email, password, use_proxy),
            ASYNC_LOOP
        )
        try:
            success, msg = future.result(timeout=35)
            if success:
                print("✅ Quotex connected. Ready to stream on demand.")
                return
            else:
                print(f"❌ Attempt {attempt} failed: {msg}")
        except Exception as e:
            print(f"❌ Attempt {attempt} exception: {e}")

        if attempt == 1 and use_proxy:
            # First attempt with proxy failed – fallback to direct
            print("⏳ Proxy failed. Falling back to direct connection...")
            use_proxy = False
            # Also clear proxy env vars for subsequent attempts
            os.environ.pop('HTTP_PROXY', None)
            os.environ.pop('HTTPS_PROXY', None)
            os.environ.pop('ALL_PROXY', None)
            # Continue to next attempt without proxy
        elif attempt < 3:
            print("⏳ Waiting 10 seconds before retry...")
            time.sleep(10)

    print("❌ All connection attempts failed. Check your credentials and network.")

threading.Thread(target=startup_background, daemon=True, name="Startup").start()

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
