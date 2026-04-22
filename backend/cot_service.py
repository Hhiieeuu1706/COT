from __future__ import annotations

import io
import json
import sqlite3
import zipfile
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import yfinance as yf

try:
    import MetaTrader5 as mt5  # type: ignore
    MT5_AVAILABLE = True
except ImportError:
    MT5_AVAILABLE = False
    mt5 = None  # type: ignore

from market_catalog import category_sort_key, categorize_market_strict, infer_price_ticker, slugify_market_name

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
COT_CACHE_PATH = DATA_DIR / "cot_cache.json"
CATALOG_CACHE_PATH = DATA_DIR / "catalog_cache.json"
PRICE_CACHE_PATH = DATA_DIR / "price_cache.json"
SERIES_CACHE_DB_PATH = DATA_DIR / "series_cache.sqlite3"
REPORT_TYPE = "legacy_fut"
CACHE_MAX_AGE = timedelta(hours=24)
PRICE_CACHE_MAX_AGE = timedelta(hours=24)
CATALOG_SCHEMA_VERSION = 4
CATALOG_MEMO: Dict[str, Any] = {"stamp": None, "payload": None}
COT_PAYLOAD_MEMO: Dict[str, Any] = {"stamp": None, "payload": None}
COT_RECORDS_MEMO: Dict[str, Any] = {"stamp": None, "df": None}
SERIES_CACHE_DB_READY = False
COT_REFRESH_LOCK = Lock()

REPORT_DOWNLOAD_SPECS: Dict[str, Dict[str, Dict[str, str]]] = {
    "legacy_fut": {
        "history": {"url_end": "deacot1986_2016", "member": "FUT86_16.txt"},
        "yearly": {"prefix": "deacot", "member": "annual.txt"},
    },
    "legacy_futopt": {
        "history": {"url_end": "deahistfo_1995_2016", "member": "Com95_16.txt"},
        "yearly": {"prefix": "deahistfo", "member": "annualof.txt"},
    },
    "supplemental_futopt": {
        "history": {"url_end": "dea_cit_txt_2006_2016", "member": "CIT06_16.txt"},
        "yearly": {"prefix": "dea_cit_txt_", "member": "annualci.txt"},
    },
    "disaggregated_fut": {
        "history": {"url_end": "fut_disagg_txt_hist_2006_2016", "member": "F_Disagg06_16.txt"},
        "yearly": {"prefix": "fut_disagg_txt_", "member": "f_year.txt"},
    },
    "disaggregated_futopt": {
        "history": {"url_end": "com_disagg_txt_hist_2006_2016", "member": "C_Disagg06_16.txt"},
        "yearly": {"prefix": "com_disagg_txt_", "member": "c_year.txt"},
    },
    "traders_in_financial_futures_fut": {
        "history": {"url_end": "fin_fut_txt_2006_2016", "member": "F_TFF_2006_2016.txt"},
        "yearly": {"prefix": "fut_fin_txt_", "member": "FinFutYY.txt"},
    },
    "traders_in_financial_futures_futopt": {
        "history": {"url_end": "fin_com_txt_2006_2016", "member": "C_TFF_2006_2016.txt"},
        "yearly": {"prefix": "com_fin_txt_", "member": "FinComYY.txt"},
    },
}


@dataclass
class MarketSummary:
    market_key: str
    market_name: str
    symbol: str
    category: str
    price_ticker: Optional[str]
    latest_report_date: str


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_timestamp(timestamp_text: Optional[str]) -> Optional[datetime]:
    if not timestamp_text:
        return None
    try:
        return datetime.fromisoformat(timestamp_text)
    except ValueError:
        return None


def _is_fresh(timestamp_text: Optional[str], max_age: timedelta) -> bool:
    updated_at = _parse_timestamp(timestamp_text)
    if updated_at is None:
        return False
    return datetime.utcnow() - updated_at <= max_age


def _read_cot_zip_csv(url: str, expected_member_name: str) -> pd.DataFrame:
    response = requests.get(url, timeout=120)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        members = [name for name in archive.namelist() if not name.endswith("/")]
        member_name = next(
            (name for name in members if Path(name).name.lower() == expected_member_name.lower()),
            None,
        )
        if member_name is None:
            txt_members = [name for name in members if Path(name).suffix.lower() in {".txt", ".csv"}]
            if len(txt_members) == 1:
                member_name = txt_members[0]
            elif members:
                member_name = members[0]
            else:
                raise ValueError(f"No data file found in archive: {url}")

        with archive.open(member_name) as handle:
            return pd.read_csv(handle, low_memory=False)


def _fetch_cot_history(report_type: str) -> pd.DataFrame:
    spec = REPORT_DOWNLOAD_SPECS[report_type]["history"]
    url = f"https://cftc.gov/files/dea/history/{spec['url_end']}.zip"
    return _read_cot_zip_csv(url, spec["member"])


def _fetch_cot_year(year: int, report_type: str) -> pd.DataFrame:
    spec = REPORT_DOWNLOAD_SPECS[report_type]["yearly"]
    url = f"https://cftc.gov/files/dea/history/{spec['prefix']}{year}.zip"
    return _read_cot_zip_csv(url, spec["member"])


def _fetch_all_cot_data(report_type: str) -> pd.DataFrame:
    frames = [_fetch_cot_history(report_type)]
    current_year = datetime.utcnow().year
    for year in range(2017, current_year + 1):
        frames.append(_fetch_cot_year(year, report_type))
    return pd.concat(frames, ignore_index=True)


def _build_normalized_df_from_cached_records(records: List[Dict[str, Any]]) -> pd.DataFrame:
    cached_df = pd.DataFrame(records)
    if cached_df.empty:
        return cached_df
    cached_df["report_date"] = pd.to_datetime(cached_df["report_date"], errors="coerce")
    cached_df = cached_df.dropna(subset=["report_date", "market_name"])
    cached_df["market_name"] = cached_df["market_name"].astype(str).str.strip()
    cached_df["market_key"] = cached_df["market_name"].map(slugify_market_name)
    cached_df["category"] = cached_df["market_name"].map(categorize_market_strict)
    cached_df["price_ticker"] = cached_df["market_name"].map(infer_price_ticker)
    if "noncommercial_net" not in cached_df.columns:
        cached_df["noncommercial_net"] = pd.to_numeric(cached_df["noncommercial_long"], errors="coerce") - pd.to_numeric(cached_df["noncommercial_short"], errors="coerce")
    return cached_df


def _refresh_cot_cache_from_existing_records(cached_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    cached_df = _build_normalized_df_from_cached_records(cached_records)
    if cached_df.empty:
        raise ValueError("Cached COT payload has no usable records")

    latest_date = cached_df["report_date"].max()
    years_to_check = sorted({int(latest_date.year), datetime.utcnow().year})

    incremental_frames: List[pd.DataFrame] = []
    for year in years_to_check:
        year_raw = _fetch_cot_year(year=year, report_type=REPORT_TYPE)
        year_df = _normalize_cot_dataframe(year_raw)
        year_df = year_df[year_df["report_date"] > latest_date]
        if not year_df.empty:
            incremental_frames.append(year_df)

    if incremental_frames:
        merged = pd.concat([cached_df, *incremental_frames], ignore_index=True)
        merged = merged.sort_values(["market_name", "report_date"])
        merged = merged.drop_duplicates(subset=["market_name", "report_date"], keep="last")
        return _build_cot_payload(merged)

    return _build_cot_payload(cached_df)


def _ensure_series_cache_db() -> None:
    global SERIES_CACHE_DB_READY
    if SERIES_CACHE_DB_READY:
        return

    SERIES_CACHE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(SERIES_CACHE_DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS market_series_cache (
                market_key TEXT PRIMARY KEY,
                updated_at TEXT NOT NULL,
                payload_json TEXT NOT NULL
            )
            """
        )
        conn.commit()

    SERIES_CACHE_DB_READY = True


def _read_market_series_cache(market_key: str) -> Optional[Dict[str, Any]]:
    _ensure_series_cache_db()
    with sqlite3.connect(SERIES_CACHE_DB_PATH) as conn:
        row = conn.execute(
            "SELECT payload_json FROM market_series_cache WHERE market_key = ?",
            (market_key,),
        ).fetchone()
    if not row:
        return None
    try:
        return json.loads(row[0])
    except Exception:
        return None


def _write_market_series_cache(market_key: str, payload: Dict[str, Any]) -> None:
    _ensure_series_cache_db()
    try:
        with sqlite3.connect(SERIES_CACHE_DB_PATH) as conn:
            conn.execute(
                """
                INSERT INTO market_series_cache (market_key, updated_at, payload_json)
                VALUES (?, ?, ?)
                ON CONFLICT(market_key)
                DO UPDATE SET
                    updated_at = excluded.updated_at,
                    payload_json = excluded.payload_json
                """,
                (market_key, str(payload.get("updated_at") or datetime.utcnow().isoformat()), json.dumps(payload, ensure_ascii=False)),
            )
            conn.commit()
    except Exception as e:
        # Log but don't crash if cache write fails
        print(f"Warning: Failed to write market series cache for {market_key}: {e}")


def get_market_series_cache_updated_at(market_key: str) -> Optional[str]:
    _ensure_series_cache_db()
    with sqlite3.connect(SERIES_CACHE_DB_PATH) as conn:
        row = conn.execute(
            "SELECT updated_at FROM market_series_cache WHERE market_key = ?",
            (market_key,),
        ).fetchone()
    if not row:
        return None
    return str(row[0]) if row[0] else None


def _normalize_cot_dataframe(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()
    df = df.rename(
        columns={
            "Market and Exchange Names": "market_name",
            "As of Date in Form YYYY-MM-DD": "report_date",
            "Open Interest (All)": "open_interest",
            "Noncommercial Positions-Long (All)": "noncommercial_long",
            "Noncommercial Positions-Short (All)": "noncommercial_short",
            "Noncommercial Positions-Spreading (All)": "noncommercial_spreading",
        }
    )
    required = [
        "market_name",
        "report_date",
        "open_interest",
        "noncommercial_long",
        "noncommercial_short",
        "noncommercial_spreading",
    ]
    df = df[required].copy()
    df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce")
    numeric_columns = [
        "open_interest",
        "noncommercial_long",
        "noncommercial_short",
        "noncommercial_spreading",
    ]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df = df.dropna(subset=["market_name", "report_date"])
    df["market_name"] = df["market_name"].astype(str).str.strip()
    df["market_key"] = df["market_name"].map(slugify_market_name)
    df["category"] = df["market_name"].map(categorize_market_strict)
    df["price_ticker"] = df["market_name"].map(infer_price_ticker)
    df["noncommercial_net"] = df["noncommercial_long"] - df["noncommercial_short"]
    df = df.sort_values(["market_name", "report_date"])
    df = df.drop_duplicates(subset=["market_name", "report_date"], keep="last")
    return df


def _build_cot_payload(normalized: pd.DataFrame) -> Dict[str, Any]:
    latest_markets_df = normalized.sort_values("report_date").groupby("market_key", as_index=False).tail(1)
    return {
        "updated_at": datetime.utcnow().isoformat(),
        "report_type": REPORT_TYPE,
        "latest_markets": latest_markets_df.assign(report_date=latest_markets_df["report_date"].dt.strftime("%Y-%m-%d")).to_dict(orient="records"),
        "records": normalized.assign(report_date=normalized["report_date"].dt.strftime("%Y-%m-%d")).to_dict(orient="records"),
    }


def refresh_cot_cache(force: bool = False) -> Dict[str, Any]:
    if not force and COT_PAYLOAD_MEMO.get("payload") is not None:
        memo_payload = COT_PAYLOAD_MEMO["payload"]
        if _is_fresh(memo_payload.get("updated_at"), CACHE_MAX_AGE):
            return memo_payload

    cached = _read_json(COT_CACHE_PATH)
    if not force and _is_fresh(cached.get("updated_at"), CACHE_MAX_AGE):
        COT_PAYLOAD_MEMO["stamp"] = cached.get("updated_at")
        COT_PAYLOAD_MEMO["payload"] = cached
        return cached

    with COT_REFRESH_LOCK:
        cached = _read_json(COT_CACHE_PATH)

        if not force and _is_fresh(cached.get("updated_at"), CACHE_MAX_AGE):
            COT_PAYLOAD_MEMO["stamp"] = cached.get("updated_at")
            COT_PAYLOAD_MEMO["payload"] = cached
            return cached

        if cached.get("records"):
            try:
                payload = _refresh_cot_cache_from_existing_records(cached.get("records", []))
                _write_json(COT_CACHE_PATH, payload)
                COT_PAYLOAD_MEMO["stamp"] = payload.get("updated_at")
                COT_PAYLOAD_MEMO["payload"] = payload
                return payload
            except Exception:
                if cached.get("records"):
                    COT_PAYLOAD_MEMO["stamp"] = cached.get("updated_at")
                    COT_PAYLOAD_MEMO["payload"] = cached
                    return cached
                raise

        raw_df = _fetch_all_cot_data(REPORT_TYPE)
        normalized = _normalize_cot_dataframe(raw_df)
        payload = _build_cot_payload(normalized)
        _write_json(COT_CACHE_PATH, payload)
        COT_PAYLOAD_MEMO["stamp"] = payload.get("updated_at")
        COT_PAYLOAD_MEMO["payload"] = payload
        return payload


def refresh_catalog_cache(force: bool = False) -> Dict[str, Any]:
    cached = _read_json(CATALOG_CACHE_PATH)
    try:
        cot_payload = refresh_cot_cache(force=force)
    except Exception:
        # Never block catalog loading if we still have any cached catalog payload.
        if cached.get("categories") and cached.get("schema_version") == CATALOG_SCHEMA_VERSION:
            return cached
        raise

    if (
        not force
        and cached.get("categories")
        and cached.get("schema_version") == CATALOG_SCHEMA_VERSION
        and cached.get("source_updated_at") == cot_payload.get("updated_at")
    ):
        return cached

    latest_rows = cot_payload.get("latest_markets", [])
    if not latest_rows and cot_payload.get("records"):
        # Backward compatibility for old cot_cache schema without latest_markets.
        records_df = pd.DataFrame(cot_payload.get("records", []))
        if not records_df.empty:
            records_df["report_date"] = pd.to_datetime(records_df["report_date"], errors="coerce")
            latest_rows = (
                records_df.sort_values("report_date")
                .groupby("market_key", as_index=False)
                .tail(1)
                .assign(report_date=lambda d: d["report_date"].dt.strftime("%Y-%m-%d"))
                .to_dict(orient="records")
            )

    catalog_payload = {
        "updated_at": datetime.utcnow().isoformat(),
        "source_updated_at": cot_payload.get("updated_at"),
        "report_type": REPORT_TYPE,
        "schema_version": CATALOG_SCHEMA_VERSION,
        **_build_catalog_from_latest_rows(latest_rows),
    }
    _write_json(CATALOG_CACHE_PATH, catalog_payload)
    return catalog_payload


def _load_cot_records(force: bool = False) -> pd.DataFrame:
    payload = refresh_cot_cache(force=force)
    stamp = payload.get("updated_at")
    if not force and COT_RECORDS_MEMO.get("stamp") == stamp and COT_RECORDS_MEMO.get("df") is not None:
        return COT_RECORDS_MEMO["df"]

    df = pd.DataFrame(payload.get("records", []))
    if df.empty:
        COT_RECORDS_MEMO["stamp"] = stamp
        COT_RECORDS_MEMO["df"] = df
        return df
    df["report_date"] = pd.to_datetime(df["report_date"])
    df["market_name"] = df["market_name"].astype(str).str.strip()
    df["market_key"] = df["market_name"].map(slugify_market_name)
    df["category"] = df["market_name"].map(categorize_market_strict)
    df = df.dropna(subset=["category"]).copy()
    df["price_ticker"] = df["market_name"].map(infer_price_ticker)
    if "noncommercial_net" not in df.columns:
        df["noncommercial_net"] = pd.to_numeric(df["noncommercial_long"], errors="coerce") - pd.to_numeric(df["noncommercial_short"], errors="coerce")
    COT_RECORDS_MEMO["stamp"] = stamp
    COT_RECORDS_MEMO["df"] = df
    return df


def _normalize_symbol(market_name: str, price_ticker: Optional[str]) -> str:
    if price_ticker:
        return price_ticker.upper().replace("=F", "").replace("-USD", "").replace("^", "")
    base = market_name.split(" - ")[0].upper()
    words = [token for token in "".join(ch if ch.isalnum() else " " for ch in base).split() if token not in {"AND", "THE", "OF", "NO"}]
    initials = "".join(word[0] for word in words[:4])
    if len(initials) >= 2:
        return initials
    compact = "".join(words)
    return (compact[:6] or "SYM").upper()


def _build_catalog_from_latest_rows(latest_rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    if not latest_rows:
        return {"categories": []}

    # Load price cache to check which markets have price data
    price_cache = _load_price_cache()
    
    category_market_map: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for row in latest_rows:
        market_name = str(row.get("market_name", "")).strip()
        if not market_name:
            continue

        category = categorize_market_strict(market_name)
        if not category:
            continue

        market_key = str(row.get("market_key") or slugify_market_name(market_name))
        price_ticker = row.get("price_ticker") or infer_price_ticker(market_name)
        latest_report_date = str(row.get("report_date") or "")
        symbol = _normalize_symbol(market_name, price_ticker)
        
        # Check if this market has cached price data
        has_price = False
        cached_entry = price_cache.get("markets", {}).get(market_key)
        if cached_entry:
            # If cache exists and is NOT marked as _no_data_flag, then we have price
            if cached_entry.get("series") and len(cached_entry.get("series", [])) > 0:
                has_price = True

        market_payload = {
            "market_key": market_key,
            "market_name": market_name,
            "symbol": symbol,
            "category": category,
            "price_ticker": price_ticker,
            "latest_report_date": latest_report_date,
            "has_price_data": has_price,
        }

        # Keep one latest row per unique market_key; do not collapse different markets that share ticker.
        category_market_map.setdefault(category, {})
        existing = category_market_map[category].get(market_key)
        if existing is None or latest_report_date > existing["latest_report_date"]:
            category_market_map[category][market_key] = market_payload

    category_map: Dict[str, List[Dict[str, Any]]] = {}
    for category, market_map in category_market_map.items():
        category_map[category] = sorted(
            market_map.values(),
            key=lambda item: (item["latest_report_date"], item["symbol"]),
            reverse=True,
        )

    return {
        "categories": [
            {
                "name": category,
                "markets": markets_list,
            }
            for category, markets_list in sorted(category_map.items(), key=lambda item: category_sort_key(item[0]))
        ]
    }


def get_catalog(force: bool = False) -> Dict[str, List[Dict[str, Any]]]:
    payload = refresh_catalog_cache(force=force)
    stamp = payload.get("updated_at")
    if not force and CATALOG_MEMO.get("stamp") == stamp and CATALOG_MEMO.get("payload") is not None:
        return CATALOG_MEMO["payload"]

    catalog_payload = {"categories": payload.get("categories", [])}
    CATALOG_MEMO["stamp"] = stamp
    CATALOG_MEMO["payload"] = catalog_payload
    return catalog_payload


def _load_price_cache() -> Dict[str, Any]:
    cached = _read_json(PRICE_CACHE_PATH)
    if "markets" not in cached:
        cached["markets"] = {}
    return cached


def _is_valid_price_ticker(ticker: Optional[str]) -> bool:
    """Check if ticker is valid for yfinance fetch."""
    if not ticker:
        return False
    ticker = str(ticker).strip()
    # Skip tickers that start with ^ (index symbols) or are obviously invalid
    if ticker.startswith("^"):
        return False
    # Only allow certain formats: SYMBOL, SYMBOL=F, SYMBOL-USD, etc.
    valid_chars = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789=-")
    if not all(c in valid_chars for c in ticker):
        return False
    return len(ticker) >= 2


def _yf_download_with_timeout(ticker: str, start: str, end: str, interval: str, timeout_sec: int = 30):
    """Download from yfinance with timeout protection.
    
    Returns empty DataFrame if timeout or error occurs to prevent hanging.
    """
    import threading
    
    result = {"data": None, "error": None}
    
    def fetch():
        try:
            result["data"] = yf.download(
                ticker,
                start=start,
                end=end,
                interval=interval,
                auto_adjust=True,
                progress=False,
            )
        except Exception as e:
            result["error"] = str(e)
            logger.warning(f"yfinance error for {ticker}: {e}")
    
    thread = threading.Thread(target=fetch, daemon=True)
    thread.start()
    thread.join(timeout=timeout_sec)
    
    if thread.is_alive():
        logger.warning(f"yfinance download timeout ({timeout_sec}s) for {ticker}")
        return pd.DataFrame()  # Return empty DataFrame on timeout
    
    if result["error"]:
        return pd.DataFrame()
    
    return result["data"] if result["data"] is not None else pd.DataFrame()


def _fetch_from_tvdatafeed(ticker: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """Fallback 3: Try to fetch from TradingView via tvDatafeed if available.
    Note: May have connectivity issues or rate limits without login."""
    try:
        import threading
        from tvDatafeed import TvDatafeed, Interval  # type: ignore
        
        logger.debug(f"Attempting tvDatafeed fallback for {ticker}...")
        
        # Map tickers to TradingView symbols
        ticker_mapping = {
            "DXY=F": {"symbol": "DXY", "exchange": "TVC"},  # TVC = TradingView Crypto/Index feed
        }
        
        tv_symbol = ticker_mapping.get(ticker)
        if not tv_symbol:
            logger.debug(f"No tvDatafeed mapping for {ticker}")
            return None
        
        # Use timeout for tvDatafeed since TradingView can be slow
        result = {"data": None, "error": None}
        
        def fetch_tv():
            try:
                tv = TvDatafeed()
                # Calculate number of bars from date range
                start_dt = pd.to_datetime(start_date)
                end_dt = pd.to_datetime(end_date)
                n_bars = max(50, (end_dt - start_dt).days)  # At least 50 bars
                
                data = tv.get_hist(
                    symbol=tv_symbol["symbol"],
                    exchange=tv_symbol["exchange"],
                    interval=Interval.in_daily,
                    n_bars=n_bars
                )
                result["data"] = data
            except Exception as e:
                result["error"] = str(e)
                logger.debug(f"tvDatafeed error: {type(e).__name__}: {str(e)[:100]}")
        
        thread = threading.Thread(target=fetch_tv, daemon=True)
        thread.start()
        thread.join(timeout=20)  # 20 second timeout for tvDatafeed
        
        if thread.is_alive():
            logger.debug(f"tvDatafeed timeout for {ticker}")
            return None
        
        if result["error"]:
            return None
        
        data = result["data"]
        if data is not None and not data.empty:
            logger.info(f"tvDatafeed SUCCESS for {ticker}: got {len(data)} rows")
            return data
        else:
            logger.debug(f"tvDatafeed returned empty data for {ticker}")
            return None
            
    except ImportError:
        logger.debug("tvDatafeed not installed, skipping fallback")
        return None
    except Exception as e:
        logger.debug(f"tvDatafeed fallback error: {e}")
        return None


def _fetch_from_mt5(symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """Fetch price data from MT5 for DXY variants."""
    if not MT5_AVAILABLE:
        logger.debug("MT5 not available")
        return None

    symbol_variants = [symbol]
    if symbol.upper() in {"DXY", "DXY=F"}:
        symbol_variants = ["DX.f", "DX", "DXY", "USDX", "DXY_M6"]

    try:
        # Initialize MT5 with BlackBull path
        mt5_path = r"C:\Program Files\BlackBull Markets MT5\terminal64.exe"
        if not mt5.initialize(mt5_path):
            logger.debug("MT5 initialize failed with BlackBull path")
            # Try without path
            if not mt5.initialize():
                logger.debug("MT5 initialize failed")
                return None

        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date) + pd.Timedelta(days=1)

        for candidate in symbol_variants:
            logger.debug(f"Trying MT5 symbol: {candidate}")
            # Make symbol visible
            if not mt5.symbol_select(candidate, True):
                logger.debug(f"Failed to select symbol {candidate}")
                continue

            symbol_info = mt5.symbol_info(candidate)
            if symbol_info:
                logger.debug(f"Symbol {candidate} info: visible={symbol_info.visible}, path={symbol_info.path}")
            else:
                logger.debug(f"Symbol {candidate} not found")
                continue

            try:
                rates = mt5.copy_rates_range(candidate, mt5.TIMEFRAME_W1, start_dt.to_pydatetime(), end_dt.to_pydatetime())
            except Exception as e:
                logger.debug(f"MT5 symbol lookup failed for {candidate}: {e}")
                continue

            if rates is None or len(rates) == 0:
                logger.debug(f"No MT5 data for {candidate}")
                continue

            df = pd.DataFrame(rates)
            df['time'] = pd.to_datetime(df['time'], unit='s')
            df.set_index('time', inplace=True)
            if 'close' not in df.columns:
                logger.debug(f"MT5 rates for {candidate} missing close column")
                continue

            df = df[['close']]
            df.columns = ['Close']

            logger.debug(f"MT5 fetch successful for {candidate}: {len(df)} rows")
            return df

        logger.debug(f"All MT5 symbol variants failed for {symbol}")
        return None

    except Exception as e:
        logger.debug(f"MT5 fetch error: {e}")
        return None
    finally:
        mt5.shutdown()


def _fetch_price_series(price_ticker: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    logger.debug("_fetch_price_series START: ticker=%s, start=%s, end=%s", price_ticker, start_date, end_date)
    
    if not _is_valid_price_ticker(price_ticker):
        logger.warning("Invalid price ticker: %s", price_ticker)
        return []
    
    try:
        # Special handling for DXY - fetch from MT5
        if price_ticker == "DXY=F":
            logger.debug("Fetching DXY from MT5 (with symbol variants)...")  # noqa: F541
            history = _fetch_from_mt5("DXY", start_date, end_date)
            if history is not None and not history.empty:
                logger.info("MT5 SUCCESS for DXY")  # noqa: F541
            else:
                logger.warning("MT5 failed for DXY, falling back to yfinance")  # noqa: F541
                # Fallback to yfinance
                history = _yf_download_with_timeout(
                    "DXY=F",
                    start=start_date,
                    end=(pd.to_datetime(end_date) + pd.Timedelta(days=7)).strftime("%Y-%m-%d"),
                    interval="1wk",
                    timeout_sec=60,
                )
                if history is not None and not history.empty:
                    logger.info("yfinance fallback SUCCESS for DXY")  # noqa: F541
                else:
                    logger.warning("yfinance fallback failed for DXY")  # noqa: F541
                    return []
        else:
            # Original yfinance logic for other tickers
            logger.debug("Downloading weekly data for %s...", price_ticker)
            history = _yf_download_with_timeout(
                price_ticker,
                start=start_date,
                end=(pd.to_datetime(end_date) + pd.Timedelta(days=7)).strftime("%Y-%m-%d"),
                interval="1wk",
                timeout_sec=60,  # Full history fetch - allow more time
            )
            logger.debug("Weekly download returned %d rows, empty=%s", len(history), history.empty)
            
            if history.empty:
                logger.debug("Weekly empty, trying daily data for %s...", price_ticker)
                history = _yf_download_with_timeout(
                    price_ticker,
                    start=start_date,
                    end=(pd.to_datetime(end_date) + pd.Timedelta(days=7)).strftime("%Y-%m-%d"),
                    interval="1d",
                    timeout_sec=60,  # Full history fetch - allow more time
                )
                logger.debug("Daily download returned %d rows, empty=%s", len(history), history.empty)
                
                # Try fallback from investpy before giving up
                if history.empty:
                    logger.debug("yfinance failed, trying tvDatafeed fallback for %s...", price_ticker)
                    history = _fetch_from_tvdatafeed(price_ticker, start_date, end_date)
                    if history is not None and not history.empty:
                        logger.info("tvDatafeed SUCCESS for %s", price_ticker)
                    else:
                        logger.warning("No price data found for %s (yfinance + tvDatafeed both failed)", price_ticker)
                        return []
                else:
                    logger.debug("Resampling daily data to weekly...")
                    history = history.resample("W-FRI").last()
        
        # Skip if no data
        if history.empty:
            logger.warning("No data for %s after resampling", price_ticker)
            return []

        logger.debug("Columns type: %s, MultiIndex=%s", type(history.columns), isinstance(history.columns, pd.MultiIndex))
        logger.debug("Column names: %s", history.columns.tolist())

        if isinstance(history.columns, pd.MultiIndex):
            logger.debug("MultiIndex detected with levels: %s", history.columns.levels)
            try:
                logger.debug("Trying to extract 'Close' from level 0...")
                close_series = history.xs("Close", axis=1, level=0)
                logger.debug("Successfully extracted from level 0: %s", type(close_series))
            except (KeyError, ValueError) as e:
                logger.debug("Level 0 failed (%s), trying level 1...", e)
                try:
                    close_series = history.xs("Close", axis=1, level=1)
                    logger.debug("Successfully extracted from level 1: %s", type(close_series))
                except (KeyError, ValueError) as e2:
                    logger.debug("Level 1 failed (%s), using fallback first column...", e2)
                    close_series = history.iloc[:, 0]
                    logger.debug("Using fallback first column: %s", type(close_series))
            
            if isinstance(close_series, pd.DataFrame):
                logger.debug("close_series is DataFrame, taking first column...")
                close_series = close_series.iloc[:, 0]
        else:
            logger.debug("Non-MultiIndex columns")
            if "Close" not in history.columns:
                logger.warning("'Close' column not found in %s", history.columns.tolist())
                return []
            close_series = history["Close"]

        series = []
        close_series = close_series.dropna()
        logger.debug("After dropna: %d rows", len(close_series))
        for timestamp, value in close_series.items():
            series.append({"date": pd.Timestamp(timestamp).strftime("%Y-%m-%d"), "close": round(float(value), 6)})
        
        logger.debug("_fetch_price_series DONE: %d data points returned for %s", len(series), price_ticker)
        return series
    except Exception as e:
        logger.error("Exception fetching price for %s: %s", price_ticker, e, exc_info=True)
        return []


def _fetch_price_series_incremental(price_ticker: str, cached_series: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Fetch only new data since last cached date (last 30 days buffer)."""
    logger.debug(f"_fetch_price_series_incremental START: ticker={price_ticker}, cached_rows={len(cached_series)}")
    
    if not _is_valid_price_ticker(price_ticker):
        logger.warning(f"Invalid price ticker: {price_ticker}")
        return cached_series
    
    if not cached_series:
        logger.debug(f"No cached series for {price_ticker}, returning empty")
        return []
    
    try:
        last_date_str = cached_series[-1].get("date")
        if not last_date_str:
            logger.warning(f"No date in last cached item for {price_ticker}")
            return cached_series
        
        last_date = pd.to_datetime(last_date_str)
        fetch_start = (last_date - pd.Timedelta(days=30)).strftime("%Y-%m-%d")
        fetch_end = (pd.to_datetime(datetime.utcnow()) + pd.Timedelta(days=7)).strftime("%Y-%m-%d")
        logger.debug(f"Incremental fetch for {price_ticker}: last_date={last_date_str}, fetch_range={fetch_start} to {fetch_end}")
        
        logger.debug(f"Downloading incremental weekly data for {price_ticker}...")
        history = _yf_download_with_timeout(
            price_ticker,
            start=fetch_start,
            end=fetch_end,
            interval="1wk",
            timeout_sec=30,
        )
        logger.debug(f"Incremental weekly download: {len(history)} rows, empty={history.empty}")
        
        if history.empty:
            logger.debug("Weekly empty, trying daily incremental data...")
            history = _yf_download_with_timeout(
                price_ticker,
                start=fetch_start,
                end=fetch_end,
                interval="1d",
                timeout_sec=30,
            )
            logger.debug(f"Incremental daily download: {len(history)} rows, empty={history.empty}")
            if history.empty:
                logger.debug(f"No new data for {price_ticker}, returning cached")
                return cached_series
            logger.debug("Resampling incremental daily data to weekly...")
            history = history.resample("W-FRI").last()

        if history.empty:
            logger.debug(f"No data for {price_ticker} after resampling, returning cached")
            return cached_series

        logger.debug(f"Columns type: {type(history.columns)}, MultiIndex={isinstance(history.columns, pd.MultiIndex)}")
        logger.debug(f"Column names: {history.columns.tolist()}")

        if isinstance(history.columns, pd.MultiIndex):
            logger.debug("MultiIndex detected, trying level 0...")
            try:
                close_series = history.xs("Close", axis=1, level=0)
                logger.debug("Successfully extracted from level 0")
            except (KeyError, ValueError) as e:
                logger.debug(f"Level 0 failed ({e}), trying level 1...")
                try:
                    close_series = history.xs("Close", axis=1, level=1)
                    logger.debug("Successfully extracted from level 1")
                except (KeyError, ValueError) as e2:
                    logger.debug(f"Level 1 failed ({e2}), using fallback...")
                    close_series = history.iloc[:, 0]
            
            if isinstance(close_series, pd.DataFrame):
                logger.debug("close_series is DataFrame, taking first column")
                close_series = close_series.iloc[:, 0]
        else:
            logger.debug("Non-MultiIndex columns")
            if "Close" not in history.columns:
                logger.warning(f"'Close' not in columns: {history.columns.tolist()}")
                return cached_series
            close_series = history["Close"]

        new_series = {}
        close_series = close_series.dropna()
        logger.debug(f"After dropna: {len(close_series)} new rows")
        
        for timestamp, value in close_series.items():
            date_str = pd.Timestamp(timestamp).strftime("%Y-%m-%d")
            new_series[date_str] = round(float(value), 6)
        
        # Merge with cached (keep cached dates, update with new)
        result = cached_series.copy()
        seen_dates = {item["date"] for item in result}
        
        updates = 0
        additions = 0
        for date_str, close_val in new_series.items():
            if date_str in seen_dates:
                # Update existing date
                for item in result:
                    if item["date"] == date_str:
                        item["close"] = close_val
                        updates += 1
                        break
            else:
                # Add new date
                result.append({"date": date_str, "close": close_val})
                additions += 1
        
        result.sort(key=lambda x: x["date"])
        logger.debug(f"_fetch_price_series_incremental DONE: {updates} updates, {additions} additions, total={len(result)}")
        return result
    except Exception as e:
        logger.error(f"Exception in incremental fetch for {price_ticker}: {e}", exc_info=True)
        logger.debug("Returning cached data due to error")
        return cached_series


def get_price_series(
    market_key: str,
    price_ticker: Optional[str],
    start_date: str,
    end_date: str,
    force_refresh: bool = False,
    fetch_if_missing: bool = False,
) -> List[Dict[str, Any]]:
    logger.debug(f"get_price_series: market_key={market_key}, ticker={price_ticker}, force_refresh={force_refresh}, fetch_if_missing={fetch_if_missing}")
    
    if not price_ticker:
        logger.debug(f"No price_ticker for {market_key}")
        return []
    
    # Validate ticker early - skip invalid tickers completely
    if not _is_valid_price_ticker(price_ticker):
        logger.warning(f"Invalid ticker {price_ticker} for {market_key}")
        return []

    cache = _load_price_cache()
    cached_entry = cache["markets"].get(market_key)
    
    # Always return cached series immediately if it exists (even if old)
    if cached_entry and cached_entry.get("ticker") == price_ticker:
        cached_series = cached_entry.get("series", [])
        logger.debug(f"Found cache for {market_key}: {len(cached_series)} rows, _no_data_flag={cached_entry.get('_no_data_flag')}")
        
        if cached_series or cached_entry.get("_no_data_flag"):
            # If cache exists, return it immediately (no waiting)
            # UNLESS force_refresh is True - then try to get new data even if marked as no-data
            if cached_entry.get("_no_data_flag") and not force_refresh:
                logger.debug(f"Cache marked as no-data for {market_key}, returning [] (use force_refresh to retry)")
                return []
            
            # Then optionally fetch incremental in background
            if force_refresh:
                logger.debug(f"force_refresh=True, retrying fetch for {market_key} (ignoring _no_data_flag)")
                # Fetch fresh data (with new fallbacks) even if previously marked as no-data
                updated_series = _fetch_price_series(price_ticker, start_date, end_date)
                # If refresh failed (empty) but we already have cached data, don't clobber it.
                if len(updated_series) == 0 and len(cached_series) > 0:
                    logger.warning(
                        "Price refresh returned empty for %s (%s); keeping existing cached series (%d rows)",
                        market_key,
                        price_ticker,
                        len(cached_series),
                    )
                    return cached_series

                cache["markets"][market_key] = {
                    "ticker": price_ticker,
                    "updated_at": datetime.utcnow().isoformat(),
                    "series": updated_series,
                    "_no_data_flag": len(updated_series) == 0,
                }
                _write_json(PRICE_CACHE_PATH, cache)
                logger.debug(f"After refresh: {len(updated_series)} rows for {market_key}")
                return updated_series
            else:
                # Cache hit - return immediately
                logger.debug(f"Cache hit for {market_key}, returning {len(cached_series)} rows immediately")
                return cached_series

    # No cache - only fetch if explicitly requested
    if not fetch_if_missing:
        logger.debug(f"No cache and fetch_if_missing=False for {market_key}, returning []")
        return []

    logger.debug(f"No cache for {market_key}, fetching from yfinance with ticker={price_ticker}")
    series = _fetch_price_series(price_ticker, start_date, end_date)
    
    # Mark as no-data if fetch returned empty (avoid retry storm)
    no_data = len(series) == 0
    logger.debug(f"Fetch result for {market_key}: {len(series)} rows, marking no_data={no_data}")
    
    cache["markets"][market_key] = {
        "ticker": price_ticker,
        "updated_at": datetime.utcnow().isoformat(),
        "series": series,
        "_no_data_flag": no_data,
    }
    _write_json(PRICE_CACHE_PATH, cache)
    return series


def get_market_series(market_key: str, force_refresh: bool = False, prefer_cache: bool = True) -> Dict[str, Any]:
    # Try cached payload first - instant load
    if prefer_cache and not force_refresh:
        cached_payload = _read_market_series_cache(market_key)
        if cached_payload:
            return cached_payload

    # Need to rebuild from COT records
    df = _load_cot_records(force=force_refresh)
    df = df.dropna(subset=["category"]).copy()
    market_df = df[df["market_key"] == market_key].copy()
    if market_df.empty:
        raise KeyError(f"Unknown market key: {market_key}")

    market_df = market_df.sort_values("report_date")
    market_name = str(market_df.iloc[-1]["market_name"])
    category = str(market_df.iloc[-1]["category"])
    price_ticker = market_df.iloc[-1].get("price_ticker") or None
    symbol = _normalize_symbol(market_name, price_ticker)
    series = [
        {
            "date": row.report_date.strftime("%Y-%m-%d"),
            "long": int(row.noncommercial_long),
            "short": int(row.noncommercial_short),
            "net": int(row.noncommercial_net),
            "spreading": int(row.noncommercial_spreading),
            "open_interest": int(row.open_interest),
        }
        for row in market_df.itertuples()
    ]
    
    # Use cached price if available, only fetch if missing
    price_series = get_price_series(
        market_key=market_key,
        price_ticker=price_ticker,
        start_date=series[0]["date"],
        end_date=series[-1]["date"],
        force_refresh=force_refresh,
        fetch_if_missing=True,
    )

    payload = {
        "market_key": market_key,
        "market_name": market_name,
        "symbol": symbol,
        "category": category,
        "price_ticker": price_ticker,
        "series": series,
        "price_series": price_series,
        "updated_at": datetime.utcnow().isoformat(),
    }
    _write_market_series_cache(market_key, payload)
    return payload


def get_all_market_records(force_refresh: bool = False) -> pd.DataFrame:
    df = _load_cot_records(force=force_refresh).copy()
    if df.empty:
        return df

    if "symbol" not in df.columns:
        df["symbol"] = df.apply(
            lambda row: _normalize_symbol(
                str(row["market_name"] if "market_name" in row else ""),
                row["price_ticker"] if "price_ticker" in row and pd.notna(row["price_ticker"]) else None
            ),
            axis=1,
        )

    return df


def get_cot_cache_stamp(force_refresh: bool = False) -> str:
    payload = refresh_cot_cache(force=force_refresh)
    stamp = payload.get("updated_at")
    return str(stamp or "")
