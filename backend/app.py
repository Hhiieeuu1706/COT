from __future__ import annotations

from pathlib import Path
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import logging

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

from cot_service import get_catalog, get_market_series, get_market_series_cache_updated_at, refresh_catalog_cache, refresh_cot_cache
from flow_rotation_service import get_flow_rotation_payload, get_symbol_flow_metrics

# Setup logging
import sys
log_file = Path(__file__).resolve().parent.parent / "data" / "backend.log"
log_file.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True,
    filename=str(log_file),
    filemode='a'
)
logger = logging.getLogger(__name__)
logger.info("="*80)
logger.info("BACKEND STARTED")
logger.info("="*80)

# Silence werkzeug logging noise, keep only errors
logging.getLogger('werkzeug').setLevel(logging.ERROR)
logging.getLogger('flask').setLevel(logging.INFO)

BASE_DIR = Path(__file__).resolve().parents[1]
FRONTEND_DIR = BASE_DIR / "frontend"

app = Flask(__name__, static_folder=str(FRONTEND_DIR), static_url_path="")
CORS(app)

# Pre-load COT catalog on startup for instant response
try:
    print("[STARTUP] Pre-loading COT catalog...")
    get_catalog()
    print("[STARTUP] COT catalog loaded into memory cache.")

    # Pre-fetch DXY series immediately so chart has price history from start
    try:
        dxy_key = "usd-index-ice-futures-u-s"
        print(f"[STARTUP] Pre-fetching DXY series for market_key={dxy_key}...")
        get_market_series(dxy_key, force_refresh=True, prefer_cache=False)
        print("[STARTUP] DXY series prefetch complete.")

        # Refresh catalog cache so has_price_data is updated for DXY as soon as UI loads
        from cot_service import refresh_catalog_cache
        refresh_catalog_cache(force=True)
        print("[STARTUP] Catalog refreshed after DXY prefetch.")
    except Exception as e:
        print(f"[STARTUP] Warning: DXY prefetch failed: {e}")
except Exception as e:
    print(f"[STARTUP] Warning: Failed to pre-load catalog: {e}")

SERIES_REFRESH_POOL = ThreadPoolExecutor(max_workers=2)
SERIES_REFRESH_LOCK = Lock()
SERIES_REFRESH_STATE = {}


def _series_state_snapshot(market_key: str) -> dict:
    with SERIES_REFRESH_LOCK:
        state = SERIES_REFRESH_STATE.get(market_key, {}).copy()
    state.setdefault("running", False)
    state.setdefault("last_started", None)
    state.setdefault("last_finished", None)
    state.setdefault("last_error", None)
    state["cached_updated_at"] = get_market_series_cache_updated_at(market_key)
    return state


def _run_series_refresh_task(market_key: str) -> None:
    logger.debug(f"[BACKGROUND] Starting series refresh for {market_key}")
    with SERIES_REFRESH_LOCK:
        SERIES_REFRESH_STATE.setdefault(market_key, {})
        SERIES_REFRESH_STATE[market_key].update(
            {
                "running": True,
                "last_started": datetime.utcnow().isoformat(),
                "last_error": None,
            }
        )

    error_text = None
    try:
        # Force refresh for latest data, then persist into SQLite cache.
        logger.debug(f"[BACKGROUND] Calling get_market_series for {market_key} with force_refresh=True")
        get_market_series(market_key, force_refresh=True, prefer_cache=False)
        logger.info(f"[BACKGROUND] Series refresh completed for {market_key}")
    except Exception as exc:
        error_text = str(exc)
        logger.error(f"[BACKGROUND] Series refresh failed for {market_key}: {error_text}", exc_info=True)

    with SERIES_REFRESH_LOCK:
        SERIES_REFRESH_STATE.setdefault(market_key, {})
        SERIES_REFRESH_STATE[market_key].update(
            {
                "running": False,
                "last_finished": datetime.utcnow().isoformat(),
                "last_error": error_text,
            }
        )


def _schedule_series_refresh(market_key: str) -> None:
    with SERIES_REFRESH_LOCK:
        running = bool(SERIES_REFRESH_STATE.get(market_key, {}).get("running"))
        if running:
            return
        SERIES_REFRESH_STATE.setdefault(market_key, {})
        SERIES_REFRESH_STATE[market_key]["running"] = True
    SERIES_REFRESH_POOL.submit(_run_series_refresh_task, market_key)


@app.get("/api/health")
def health() -> tuple:
    return jsonify({"status": "ok"}), 200


@app.get("/api/catalog")
def catalog() -> tuple:
    force = request.args.get("force") == "1"
    logger.debug(f"GET /api/catalog force={force}")
    payload = get_catalog(force=force)
    logger.debug(f"Returning catalog with {len(payload.get('markets', []))} markets")
    return jsonify(payload), 200


@app.get("/api/series/<market_key>")
def series(market_key: str) -> tuple:
    force = request.args.get("force") == "1"
    logger.debug(f"GET /api/series/{market_key} force={force}")
    try:
        payload = get_market_series(market_key, force_refresh=force, prefer_cache=not force)
        logger.debug(f"Series loaded for {market_key}: {len(payload.get('price_series', []))} price points")
    except KeyError:
        logger.warning(f"Market not found: {market_key}")
        return jsonify({"error": "Market not found"}), 404
    
    # Schedule background refresh only on normal load (not force) to keep data fresh
    if not force:
        _schedule_series_refresh(market_key)
        logger.debug(f"Scheduled background refresh for {market_key}")
    
    payload["refresh_state"] = _series_state_snapshot(market_key)
    return jsonify(payload), 200


@app.get("/api/series-refresh-status/<market_key>")
def series_refresh_status(market_key: str) -> tuple:
    logger.debug(f"GET /api/series-refresh-status/{market_key}")
    state = _series_state_snapshot(market_key)
    logger.debug(f"Refresh state for {market_key}: {state}")
    return jsonify(state), 200


@app.post("/api/refresh")
def refresh() -> tuple:
    logger.info(f"POST /api/refresh - forcing COT cache refresh")
    payload = refresh_cot_cache(force=True)
    refresh_catalog_cache(force=True)
    logger.info(f"Cache refresh completed: {len(payload.get('records', []))} COT records")
    return jsonify({"status": "ok", "records": len(payload.get("records", []))}), 200


@app.post("/api/flow-rotation")
def flow_rotation() -> tuple:
    request_payload = request.get_json(silent=True) or {}
    force = request.args.get("force") == "1" or bool(request_payload.get("forceRefresh"))
    payload = get_flow_rotation_payload(request_payload, force_refresh=force)
    return jsonify(payload), 200


@app.get("/api/symbol-flow/<market_key>")
def symbol_flow(market_key: str) -> tuple:
    force = request.args.get("force") == "1"
    payload = get_symbol_flow_metrics(market_key, force_refresh=force)
    return jsonify(payload), 200


@app.get("/")
def index() -> tuple:
    return send_from_directory(FRONTEND_DIR, "index.html")


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5056, debug=False)
