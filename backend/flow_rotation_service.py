from __future__ import annotations

from datetime import datetime
import json
import math
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from cot_service import get_all_market_records, get_cot_cache_stamp

QUADRANT_COLORS = {
    "Leading": "#2b8a3e",
    "Weakening": "#f08c00",
    "Lagging": "#c92a2a",
    "Improving": "#1971c2",
    "Neutral": "#6c757d",
}

TIMEFRAME_EMA = {
    "short": 4,
    "medium": 12,
    "long": 26,
}

FLOW_ROTATION_MEMO: Dict[str, Dict[str, Any]] = {}
FLOW_ROTATION_MEMO_MAX = 24


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(number):
        return default
    return number


def _classify_quadrant(rs: float, rs_momentum: float) -> str:
    if rs > 1 and rs_momentum > 0:
        return "Leading"
    if rs > 1 and rs_momentum < 0:
        return "Weakening"
    if rs < 1 and rs_momentum < 0:
        return "Lagging"
    if rs < 1 and rs_momentum > 0:
        return "Improving"
    return "Neutral"


def _resolve_ema_period(config: Dict[str, Any]) -> int:
    timeframe = str(config.get("timeframe", "custom")).lower().strip()
    if timeframe in TIMEFRAME_EMA:
        return TIMEFRAME_EMA[timeframe]
    raw_period = int(config.get("emaPeriod", 6) or 6)
    return max(4, min(12, raw_period))


def _build_records_frame(force_refresh: bool = False) -> pd.DataFrame:
    df = get_all_market_records(force_refresh=force_refresh)
    if df.empty:
        return df

    frame = df.copy()
    frame["report_date"] = pd.to_datetime(frame["report_date"], errors="coerce")
    frame = frame.dropna(subset=["report_date", "market_key", "category"]).copy()

    frame["open_interest"] = pd.to_numeric(frame.get("open_interest"), errors="coerce")
    frame["noncommercial_long"] = pd.to_numeric(frame.get("noncommercial_long"), errors="coerce")
    frame["noncommercial_short"] = pd.to_numeric(frame.get("noncommercial_short"), errors="coerce")

    if "noncommercial_net" in frame.columns:
        frame["net"] = pd.to_numeric(frame["noncommercial_net"], errors="coerce")
    else:
        frame["net"] = frame["noncommercial_long"] - frame["noncommercial_short"]

    frame = frame.sort_values(["market_key", "report_date"]).copy()
    frame = frame.drop_duplicates(subset=["market_key", "report_date"], keep="last")
    return frame


def computeFlowMetrics(data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    if data.empty:
        return data

    ema_period = _resolve_ema_period(config)
    momentum_lag = max(2, min(6, int(config.get("momentumLag", 3) or 3)))
    use_zscore = bool(config.get("useZScore", False))

    frame = data.copy()

    def _per_symbol(group: pd.DataFrame) -> pd.DataFrame:
        series = group.sort_values("report_date").copy()

        oi = series["open_interest"].replace(0, np.nan)
        oi = oi.where(oi > 0, np.nan)

        series["position_pct"] = series["net"] / oi
        series["flow"] = series["position_pct"].ewm(span=ema_period, adjust=False, min_periods=ema_period).mean()

        if use_zscore:
            mean_val = series["flow"].mean(skipna=True)
            std_val = series["flow"].std(skipna=True)
            if std_val and std_val > 0:
                series["flow_model"] = (series["flow"] - mean_val) / std_val
            else:
                series["flow_model"] = 0.0
        else:
            series["flow_model"] = series["flow"]

        series["flow_momentum"] = series["flow_model"] - series["flow_model"].shift(momentum_lag)
        return series

    # Use pd.concat instead of groupby().apply() to avoid FutureWarning
    groups = []
    for market_key, group in frame.groupby("market_key"):
        groups.append(_per_symbol(group))
    frame = pd.concat(groups, ignore_index=False) if groups else frame
    return frame


def computeRRG(data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
    if data.empty:
        return {
            "updated_at": datetime.utcnow().isoformat(),
            "config": config,
            "rrg_points": [],
            "sector_rrg_points": [],
            "heatmap": [],
            "sector_overview": [],
            "ranking": {"top_inflow": [], "top_outflow": []},
            "meta": {"symbols": 0, "rows": 0},
        }

    categories = [str(item) for item in config.get("categories", []) if item]
    symbols = [str(item) for item in config.get("symbols", []) if item]
    relative_mode = "sector" if bool(config.get("useSectorRelative", False)) else "all"
    tail_length = max(5, min(10, int(config.get("tailLength", 8) or 8)))
    threshold = abs(_safe_float(config.get("noiseThreshold", 0.0), 0.0))
    history_points = max(26, min(156, int(config.get("historyPoints", 104) or 104)))

    frame = data.copy()
    if categories:
        frame = frame[frame["category"].isin(categories)]
    if symbols:
        frame = frame[frame["symbol"].isin(symbols)]

    if frame.empty:
        return {
            "updated_at": datetime.utcnow().isoformat(),
            "config": config,
            "rrg_points": [],
            "sector_rrg_points": [],
            "heatmap": [],
            "sector_overview": [],
            "ranking": {"top_inflow": [], "top_outflow": []},
            "meta": {"symbols": 0, "rows": 0},
        }

    if relative_mode == "sector":
        benchmark = frame.groupby(["report_date", "category"])["flow_model"].transform("mean")
    else:
        benchmark = frame.groupby("report_date")["flow_model"].transform("mean")

    benchmark = benchmark.replace(0, np.nan)
    frame["rs"] = frame["flow_model"] / benchmark
    frame["rs_momentum"] = frame.groupby("market_key")["rs"].diff(max(2, min(6, int(config.get("momentumLag", 3) or 3))))

    filtered = frame.dropna(subset=["flow_model", "flow_momentum", "rs", "rs_momentum"]).copy()
    if threshold > 0:
        filtered = filtered[filtered["flow_momentum"].abs() >= threshold]

    if filtered.empty:
        return {
            "updated_at": datetime.utcnow().isoformat(),
            "config": config,
            "rrg_points": [],
            "sector_rrg_points": [],
            "heatmap": [],
            "sector_overview": [],
            "ranking": {"top_inflow": [], "top_outflow": []},
            "meta": {"symbols": 0, "rows": 0},
        }

    latest_rows = (
        filtered.sort_values("report_date")
        .groupby("market_key", as_index=False)
        .tail(1)
        .copy()
    )

    latest_rows["quadrant"] = latest_rows.apply(lambda row: _classify_quadrant(_safe_float(row["rs"]), _safe_float(row["rs_momentum"])), axis=1)

    tail_points: Dict[str, List[Dict[str, Any]]] = {}
    symbol_histories: Dict[str, List[Dict[str, Any]]] = {}
    for market_key, group in filtered.groupby("market_key"):
        sorted_group = group.sort_values("report_date")
        tail_slice = sorted_group.tail(tail_length)
        history_slice = sorted_group.tail(history_points)
        tail_points[market_key] = [
            {
                "date": row.report_date.strftime("%Y-%m-%d"),
                "rs": round(_safe_float(row.rs), 6),
                "rsMomentum": round(_safe_float(row.rs_momentum), 6),
            }
            for row in tail_slice.itertuples()
        ]
        symbol_histories[str(market_key)] = [
            {
                "date": row.report_date.strftime("%Y-%m-%d"),
                "flow": round(_safe_float(row.flow_model), 6),
                "momentum": round(_safe_float(row.flow_momentum), 6),
                "rs": round(_safe_float(row.rs), 6),
                "rsMomentum": round(_safe_float(row.rs_momentum), 6),
                "long": int(_safe_float(getattr(row, "noncommercial_long", 0) or 0)),
                "short": int(_safe_float(getattr(row, "noncommercial_short", 0) or 0)),
                "net": int(_safe_float(getattr(row, "net", 0) or 0)),
                "oi": int(_safe_float(getattr(row, "open_interest", 0) or 0)),
            }
            for row in history_slice.itertuples()
        ]

    rrg_points = []
    heatmap = []
    for row in latest_rows.itertuples():
        point = {
            "marketKey": str(row.market_key),
            "symbol": str(row.symbol),
            "marketName": str(row.market_name),
            "category": str(row.category),
            "date": row.report_date.strftime("%Y-%m-%d"),
            "flow": round(_safe_float(row.flow_model), 6),
            "momentum": round(_safe_float(row.flow_momentum), 6),
            "rs": round(_safe_float(row.rs), 6),
            "rsMomentum": round(_safe_float(row.rs_momentum), 6),
            "quadrant": str(row.quadrant),
            "color": QUADRANT_COLORS.get(str(row.quadrant), QUADRANT_COLORS["Neutral"]),
            "tail": tail_points.get(str(row.market_key), []),
        }
        rrg_points.append(point)
        heatmap.append(
            {
                "marketKey": point["marketKey"],
                "symbol": point["symbol"],
                "category": point["category"],
                "flow": point["flow"],
                "momentum": point["momentum"],
                "rs": point["rs"],
                "quadrant": point["quadrant"],
            }
        )

    heatmap = sorted(heatmap, key=lambda item: (item["flow"], item["momentum"], item["rs"]), reverse=True)

    sector_overview_df = (
        latest_rows.groupby("category", as_index=False)
        .agg(avg_flow=("flow_model", "mean"), avg_momentum=("flow_momentum", "mean"), symbols=("market_key", "count"))
        .sort_values("avg_momentum", ascending=False)
    )

    sector_overview = [
        {
            "category": row.category,
            "avgFlow": round(_safe_float(row.avg_flow), 6),
            "avgMomentum": round(_safe_float(row.avg_momentum), 6),
            "symbols": int(row.symbols),
        }
        for row in sector_overview_df.itertuples()
    ]

    sector_rrg_df = (
        filtered.groupby(["category", "report_date"], as_index=False)
        .agg(
            flow_model=("flow_model", "mean"),
            flow_momentum=("flow_momentum", "mean"),
            rs=("rs", "mean"),
            rs_momentum=("rs_momentum", "mean"),
        )
        .sort_values(["category", "report_date"])
    )

    sector_rrg_points = []
    for category, group in sector_rrg_df.groupby("category"):
        group_sorted = group.sort_values("report_date")
        latest = group_sorted.iloc[-1]
        tail_slice = group_sorted.tail(tail_length)
        rs_val = _safe_float(latest.get("rs"))
        rs_mom_val = _safe_float(latest.get("rs_momentum"))
        quadrant = _classify_quadrant(rs_val, rs_mom_val)
        sector_rrg_points.append(
            {
                "category": str(category),
                "flow": round(_safe_float(latest.get("flow_model")), 6),
                "momentum": round(_safe_float(latest.get("flow_momentum")), 6),
                "rs": round(rs_val, 6),
                "rsMomentum": round(rs_mom_val, 6),
                "quadrant": quadrant,
                "color": QUADRANT_COLORS.get(quadrant, QUADRANT_COLORS["Neutral"]),
                "tail": [
                    {
                        "date": row.report_date.strftime("%Y-%m-%d"),
                        "rs": round(_safe_float(row.rs), 6),
                        "rsMomentum": round(_safe_float(row.rs_momentum), 6),
                    }
                    for row in tail_slice.itertuples()
                ],
            }
        )

    ranking_rows = sorted(rrg_points, key=lambda item: item["momentum"], reverse=True)
    top_n = max(3, min(10, int(config.get("topN", 5) or 5)))
    ranking = {
        "top_inflow": ranking_rows[:top_n],
        "top_outflow": list(reversed(ranking_rows[-top_n:])),
    }

    return {
        "updated_at": datetime.utcnow().isoformat(),
        "config": {
            **config,
            "relativeMode": relative_mode,
            "emaPeriod": _resolve_ema_period(config),
        },
        "rrg_points": rrg_points,
        "sector_rrg_points": sector_rrg_points,
        "heatmap": heatmap,
        "sector_overview": sector_overview,
        "symbol_histories": symbol_histories,
        "ranking": ranking,
        "meta": {
            "symbols": len(rrg_points),
            "rows": int(len(filtered)),
        },
    }


def get_flow_rotation_payload(config: Dict[str, Any], force_refresh: bool = False) -> Dict[str, Any]:
    """Main entry point for Flow Rotation feature - filters and computes RRG metrics."""
    records = _build_records_frame(force_refresh=force_refresh)
    if records.empty:
        return {
            "updated_at": datetime.utcnow().isoformat(),
            "config": config,
            "rrg_points": [],
            "sector_rrg_points": [],
            "heatmap": [],
            "sector_overview": [],
            "symbol_histories": {},
            "ranking": {"top_inflow": [], "top_outflow": []},
            "meta": {"symbols": 0, "rows": 0},
        }

    config_clean = dict(config) if config else {}
    cot_stamp = get_cot_cache_stamp(force_refresh=force_refresh)
    config_json = json.dumps(config_clean, sort_keys=True, default=str)
    cache_key = f"{cot_stamp}|{config_json}"
    if cache_key in FLOW_ROTATION_MEMO:
        return FLOW_ROTATION_MEMO[cache_key]

    # Filter by categories/symbols BEFORE computing metrics
    categories = [str(item) for item in config.get("categories", []) if item]
    symbols = [str(item) for item in config.get("symbols", []) if item]
    filtered_records = records.copy()
    if categories:
        filtered_records = filtered_records[filtered_records["category"].isin(categories)]
    if symbols:
        filtered_records = filtered_records[filtered_records["symbol"].isin(symbols)]

    if filtered_records.empty:
        return {
            "updated_at": datetime.utcnow().isoformat(),
            "config": config,
            "rrg_points": [],
            "sector_rrg_points": [],
            "heatmap": [],
            "sector_overview": [],
            "symbol_histories": {},
            "ranking": {"top_inflow": [], "top_outflow": []},
            "meta": {"symbols": 0, "rows": 0},
        }

    metrics = computeFlowMetrics(filtered_records, config_clean)
    payload = computeRRG(metrics, config_clean)

    FLOW_ROTATION_MEMO[cache_key] = payload
    if len(FLOW_ROTATION_MEMO) > FLOW_ROTATION_MEMO_MAX:
        oldest_key = next(iter(FLOW_ROTATION_MEMO.keys()))
        del FLOW_ROTATION_MEMO[oldest_key]

    return payload


def get_symbol_flow_metrics(market_key: str, force_refresh: bool = False) -> Dict[str, Any]:
    """Compute flow/momentum metrics for a single symbol (for Market View)."""
    records = _build_records_frame(force_refresh=force_refresh)
    if records.empty:
        return {"updated_at": datetime.utcnow().isoformat(), "history": []}
    
    market_records = records[records["market_key"] == market_key].copy()
    if market_records.empty:
        return {"updated_at": datetime.utcnow().isoformat(), "history": []}
    
    config = {"emaPeriod": 6, "momentumLag": 3, "useZScore": False}
    metrics = computeFlowMetrics(market_records, config)
    
    history = []
    for row in metrics.sort_values("report_date").itertuples():
        history.append({
            "date": row.report_date.strftime("%Y-%m-%d"),
            "flow": round(_safe_float(row.flow_model), 6),
            "momentum": round(_safe_float(row.flow_momentum), 6),
            "long": int(_safe_float(getattr(row, "noncommercial_long", 0) or 0)),
            "short": int(_safe_float(getattr(row, "noncommercial_short", 0) or 0)),
            "net": int(_safe_float(getattr(row, "net", 0) or 0)),
            "oi": int(_safe_float(getattr(row, "open_interest", 0) or 0)),
        })
    
    return {"updated_at": datetime.utcnow().isoformat(), "history": history}
