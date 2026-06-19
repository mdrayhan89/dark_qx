#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Quotex Live Candle API – Headless version for Render
Now supports on‑demand streaming for any asset.
"""
import asyncio
import threading
import time
import json
import os
import sys
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Set
from datetime import datetime

# SSL setup
import certifi
os.environ['SSL_CERT_FILE'] = certifi.where()
os.environ['WEBSOCKET_CLIENT_CA_BUNDLE'] = certifi.where()

try:
    from pyquotex.stable_api import Quotex
    from pyquotex.utils.processor import process_candles
except ImportError as e:
    print(f"❌ Missing dependency: {e}")
    sys.exit(1)

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
CONSOLE_LEVEL = 1
def log(msg: str, level: int = 1):
    if level <= CONSOLE_LEVEL:
        print(msg)

# Asset maps (same as before)
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

# Global state
CLIENT: Optional[Quotex] = None
CANDLES: Dict[str, Dict[str, List[dict]]] = {}      # keyed by display name
CURRENT_CANDLE: Dict[str, Dict[str, dict]] = {}      # keyed by display name
STREAMING_ASSETS: Set[str] = set()                   # display names currently streaming
REALTIME_RUNNING = False
LAST_TICK_TIME = time.time()
SERVER_TIME_OFFSET = 0
CONNECTED = False

# Async loop and thread
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
            log(f"⚠️ process_candles failed: {e}", level=2)
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
# Quotex Connection and Streaming
# ------------------------------------------------------------
async def connect_to_quotex(email: str, password: str) -> Tuple[bool, str]:
    global CLIENT, CONNECTED
    try:
        log("🔐 Connecting to Quotex...", level=1)
        CLIENT = Quotex(email=email, password=password, host="qxbroker.com", lang="en")
        check, reason = await CLIENT.connect()
        if not check:
            return False, reason
        await CLIENT.change_account("PRACTICE")
        await CLIENT.get_all_assets()
        CONNECTED = True
        log("✅ Connected successfully", level=1)
        return True, ""
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

async def start_streaming(asset_display: str):
    global REALTIME_RUNNING, CURRENT_CANDLE, CANDLES, STREAMING_ASSETS
    if not CLIENT or not CONNECTED:
        log("❌ Not connected", level=0)
        return
    if asset_display in STREAMING_ASSETS:
        log(f"ℹ️ Already streaming {asset_display}", level=2)
        return

    internal = DISPLAY_TO_INTERNAL.get(asset_display)
    if not internal:
        log(f"❌ Unknown asset: {asset_display}", level=0)
        return

    # Stop any previous realtime loop if it's for a different asset? We can have multiple.
    # Actually pyquotex can handle multiple subscriptions, but we'll keep it simple and allow multiple.
    # We'll just start the subscription and a new loop per asset.

    # Load historical data for 1m
    period_sec = TIMEFRAMES.get("1m", 60)
    try:
        hist = await CLIENT.get_candles(asset=internal, end_from_time=time.time(),
                                        offset=199 * period_sec, period=period_sec)
        candles = process_candle_data(hist, period_sec)
        if asset_display not in CANDLES:
            CANDLES[asset_display] = {}
        CANDLES[asset_display]["1m"] = candles[-199:]
        log(f"📥 Loaded {len(candles)} candles for {asset_display}", level=1)
    except Exception as e:
        log(f"⚠️ Failed to load history for {asset_display}: {e}", level=2)

    # Start realtime subscription
    try:
        await CLIENT.start_realtime_price(internal, period_sec)
        STREAMING_ASSETS.add(asset_display)
        log(f"🔄 Started streaming {asset_display}", level=1)
        # Start a background loop for this asset
        asyncio.create_task(realtime_price_loop(asset_display))
    except Exception as e:
        log(f"❌ Failed to start streaming {asset_display}: {e}", level=0)

async def realtime_price_loop(asset_display: str):
    global LAST_TICK_TIME, REALTIME_RUNNING, SERVER_TIME_OFFSET
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
                    LAST_TICK_TIME = time.time()
                    SERVER_TIME_OFFSET = timestamp - time.time()
                    for frame in TIMEFRAMES:
                        update_candle(asset_display, frame, price, ts_sec)
                    if CONSOLE_LEVEL >= 2:
                        print(f"📊 {asset_display} {price:.5f}", end="\r")
            await asyncio.sleep(0.2)
        except Exception as e:
            log(f"⚠️ realtime loop error for {asset_display}: {e}", level=2)
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
        "streaming_assets": list(STREAMING_ASSETS),
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

    # Determine display name
    display_name = pair
    if pair in ASSET_DISPLAY_MAP:
        # pair is internal -> get display name
        display_name = ASSET_DISPLAY_MAP[pair]
    elif pair in DISPLAY_TO_INTERNAL:
        # pair is already display name
        display_name = pair
    else:
        # might be a direct internal not in map? try to find by value
        # but we already have mapping, so just use as is
        # Also, we can still try to find if pair is an internal key
        if pair in ASSET_DISPLAY_MAP:
            display_name = ASSET_DISPLAY_MAP[pair]
        else:
            # fallback: treat as display name
            display_name = pair

    # Check if we have data for this asset
    if display_name not in CANDLES or timeframe not in CANDLES[display_name]:
        # Not yet loaded -> trigger streaming in background (async)
        if CONNECTED and display_name not in STREAMING_ASSETS:
            # start streaming asynchronously
            asyncio.run_coroutine_threadsafe(start_streaming(display_name), ASYNC_LOOP)
            log(f"⏳ Started background streaming for {display_name}", level=1)
        # return empty for now
        candles = []
    else:
        candles = CANDLES[display_name].get(timeframe, [])

    # Take last 'count'
    candles = candles[-count:] if candles else []

    # Get market name from map (if internal)
    market_name = ASSET_DISPLAY_MAP.get(pair, display_name)
    if pair in DISPLAY_TO_INTERNAL:
        # pair is display name, we want internal for market name? Actually we want the display name itself.
        market_name = pair

    api_tf = TIMEFRAME_API_MAP.get(timeframe, timeframe.upper())
    data = []
    for idx, c in enumerate(candles, start=1):
        dt = datetime.fromtimestamp(c["time"]).strftime("%Y-%m-%d %H:%M:%S")
        open_val = f"{c['open']:.5f}".rstrip('0').rstrip('.')
        high_val = f"{c['high']:.5f}".rstrip('0').rstrip('.')
        low_val = f"{c['low']:.5f}".rstrip('0').rstrip('.')
        close_val = f"{c['close']:.5f}".rstrip('0').rstrip('.')
        color = "green" if c['close'] >= c['open'] else "red"
        # For pair, we output the internal name if available, else display
        internal_name = DISPLAY_TO_INTERNAL.get(display_name, display_name)
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
# Background startup for Quotex connection
# ------------------------------------------------------------
def startup_background():
    time.sleep(2)
    email = os.environ.get("QUOTEX_EMAIL")
    password = os.environ.get("QUOTEX_PASSWORD")
    if not email or not password:
        log("❌ QUOTEX_EMAIL and QUOTEX_PASSWORD must be set as environment variables", level=0)
        return
    future = asyncio.run_coroutine_threadsafe(connect_to_quotex(email, password), ASYNC_LOOP)
    try:
        success, msg = future.result(timeout=30)
        if success:
            log("✅ Quotex connected. Ready to stream on demand.", level=1)
        else:
            log(f"❌ Connection failed: {msg}", level=0)
    except Exception as e:
        log(f"❌ Startup error: {e}", level=0)

threading.Thread(target=startup_background, daemon=True, name="Startup").start()

# ------------------------------------------------------------
# For local development (optional)
# ------------------------------------------------------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
