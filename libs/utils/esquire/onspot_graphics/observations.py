from __future__ import annotations

from datetime import date as Date, datetime as dt, timedelta
from io import BytesIO
import math
import os
import threading
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")

from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
from matplotlib.markers import MarkerStyle
from matplotlib.ticker import FuncFormatter

import plotly.express as px

from libs.azure.key_vault import KeyVaultClient


_MPL_RENDER_LOCK = threading.Lock()


class Observations:
    def __init__(self, data: pd.DataFrame):
        """
        Object representing a device observations audience.

        Accepts any of:
          - Datetime column (string or datetime-like)
          - timestamp column (ms since epoch)
          - Date + Time columns (strings/date/time)
        """
        dt_index = _build_datetime_index(data)

        # Persist a guaranteed datetime64[ns] (or tz-aware) column
        data["Datetime"] = pd.Series(dt_index.to_numpy(), index=data.index)

        # Avoid Series.dt (can fail when dtype is object). DatetimeIndex.date/time are safe.
        data["Date"] = pd.Series(dt_index.date, index=data.index)
        data["Time"] = pd.Series(dt_index.time, index=data.index)

        self.raw_data = data.copy()

        # De-dupe per device per date for weekly counts
        data = data.drop_duplicates(subset=["deviceid", "Date"])

        # Week metadata
        data["Week"] = data["Date"].apply(get_week)
        data["EarliestDate"] = data["Date"].apply(lambda x: get_date_by_week_offset(x, 0))
        data["LatestDate"] = data["Date"].apply(lambda x: get_date_by_week_offset(x, 6))
        data["RefDate"] = data["Date"].apply(lambda x: get_date_by_week_offset(x, 3))

        data["EarliestDate"] = pd.to_datetime(data["EarliestDate"], errors="coerce")
        data["LatestDate"] = pd.to_datetime(data["LatestDate"], errors="coerce")
        data["RefDate"] = pd.to_datetime(data["RefDate"], errors="coerce")

        # Weekly count table
        self.obs = (
            data.pivot_table(
                index=["Week", "RefDate", "EarliestDate", "LatestDate"],
                values=["deviceid"],
                aggfunc="count",
            )
            .sort_values("RefDate")
            .reset_index()
        )

        mean_devices = float(self.obs["deviceid"].mean()) if len(self.obs) else 0.0
        if mean_devices > 0:
            self.obs["traffic_pct"] = round(
                100 * (self.obs["deviceid"] - mean_devices) / mean_devices, 1
            )
        else:
            self.obs["traffic_pct"] = 0.0

        # Weeks of consecutive growth
        self.obs["Weeks of Growth"] = 0
        for i in range(1, len(self.obs)):
            if float(self.obs.loc[i, "traffic_pct"]) > float(self.obs.loc[i - 1, "traffic_pct"]):
                self.obs.loc[i, "Weeks of Growth"] = int(self.obs.loc[i - 1, "Weeks of Growth"]) + 1

        # Save with lat/longs retained (for heatmap)
        self.latlongs = data

    def get_latest_week(self) -> Dict[str, Any]:
        week = self.obs.iloc[-1]
        return {
            "Week": week["Week"],
            "Year": int(week["EarliestDate"].year),
            "RefDate": week["RefDate"],
            "Performance": plus_padding(int(round(float(week["traffic_pct"])))),
            "traffic_pct": float(week["traffic_pct"]),
            "Range": dates_to_range(week["EarliestDate"], week["LatestDate"]),
        }

    def get_best_week(self) -> Dict[str, Any]:
        week = self.obs.iloc[int(self.obs["traffic_pct"].idxmax())]
        return {
            "Week": week["Week"],
            "RefDate": week["RefDate"],
            "Performance": plus_padding(int(round(float(week["traffic_pct"])))),
            "traffic_pct": float(week["traffic_pct"]),
            "Range": dates_to_range(week["EarliestDate"], week["LatestDate"]),
        }

    def get_worst_week(self) -> Dict[str, Any]:
        week = self.obs.iloc[int(self.obs["traffic_pct"].idxmin())]
        return {
            "Week": week["Week"],
            "RefDate": week["RefDate"],
            "Performance": plus_padding(int(round(float(week["traffic_pct"])))),
            "traffic_pct": float(week["traffic_pct"]),
            "Range": dates_to_range(week["EarliestDate"], week["LatestDate"]),
        }

    def foot_traffic_graph(self, export_path: Optional[str] = None, return_bytes: bool = False):
        with _MPL_RENDER_LOCK:
            fig = Figure(figsize=(7.5, 3))
            FigureCanvas(fig)
            ax = fig.add_subplot(111)

            if len(self.obs) == 0:
                ax.text(0.5, 0.5, "No data", ha="center", va="center")
                return _export_figure(fig, export_path, return_bytes, dpi=200)

            ref_dates = pd.DatetimeIndex(pd.to_datetime(self.obs["RefDate"], errors="coerce"))

            x = np.arange(len(self.obs), dtype=float)
            y = self.obs["traffic_pct"].to_numpy(dtype=float)

            ax.plot(x, y, linewidth=4)
            ax.axhline(y=0, xmin=0, xmax=1, linestyle="--", color="gray")

            ax.set_xticks(x)
            ax.set_xticklabels([d.strftime("%b %d") for d in ref_dates], rotation=45, ha="right", fontsize=9)
            ax.set_xlabel("Week")
            ax.set_ylabel("")

            bounds = float(max(abs(np.nanmin(y)), abs(np.nanmax(y)), 1.0))
            ax.set_ylim(-bounds * 1.1, bounds * 1.1)
            ax.yaxis.set_major_formatter(FuncFormatter(ytick_formatter))

            ax_top = ax.twiny()
            ax_top.set_xlim(ax.get_xlim())
            month_ticks, month_labels = _month_tick_positions(ref_dates)
            ax_top.set_xticks(month_ticks)
            ax_top.set_xticklabels(month_labels)
            ax_top.tick_params(axis="x", length=0)
            ax_top.set_xlabel("")

            latest = self.get_latest_week()
            best = self.get_best_week()
            worst = self.get_worst_week()

            idx_latest = _single_loc(ref_dates, pd.to_datetime(latest["RefDate"], errors="coerce"))
            idx_best = _single_loc(ref_dates, pd.to_datetime(best["RefDate"], errors="coerce"))
            idx_worst = _single_loc(ref_dates, pd.to_datetime(worst["RefDate"], errors="coerce"))

            ax.plot([idx_latest], [latest["traffic_pct"]], marker="o", markersize=12)
            ax.plot([idx_best], [best["traffic_pct"]], marker="o", markersize=12)
            ax.plot([idx_worst], [worst["traffic_pct"]], marker="o", markersize=12)

            best_fill = "right" if latest["Week"] == best["Week"] else "full"
            worst_fill = "right" if latest["Week"] == worst["Week"] else "full"

            ax.plot([idx_latest], [latest["traffic_pct"]], marker=MarkerStyle("o", fillstyle="full"), markersize=7, markeredgewidth=0)
            ax.plot([idx_best], [best["traffic_pct"]], marker=MarkerStyle("o", fillstyle=best_fill), markersize=7, markeredgewidth=0)
            ax.plot([idx_worst], [worst["traffic_pct"]], marker=MarkerStyle("o", fillstyle=worst_fill), markersize=7, markeredgewidth=0)

            fig.tight_layout()
            return _export_figure(fig, export_path, return_bytes, dpi=200)

    def heatmap_graph(self, export_path: Optional[str] = None, return_bytes: bool = False):
        lat_min = float(self.latlongs["lat"].min())
        lat_max = float(self.latlongs["lat"].max())
        lng_min = float(self.latlongs["lng"].min())
        lng_max = float(self.latlongs["lng"].max())

        lat_bound = abs(lat_max - lat_min)
        lng_bound = abs(lng_max - lng_min)

        lat_mid = lat_min + lat_bound / 2
        lng_mid = lng_min + lng_bound / 2

        lat_bound_km = lat_bound * 111
        lng_bound_km = lng_bound * 111

        max_bound = abs(max(lat_bound_km, lng_bound_km * 1.4))
        zoom = 14 - math.log(max_bound) if max_bound > 0 else 14

        opacity = 0.1 if float(self.obs["deviceid"].sum()) > 2500 else 0.2

        fig = px.scatter_mapbox(
            self.latlongs,
            lat="lat",
            lon="lng",
            opacity=opacity,
            size_max=1,
            color_discrete_sequence=["#F15A29"],
            zoom=zoom,
            center={"lat": lat_mid, "lon": lng_mid},
        )
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), width=579, height=414)

        if os.environ.get("mapbox_token"):
            mapbox_token = os.environ["mapbox_token"]
        else:
            mapbox_vault = KeyVaultClient("mapbox-service")
            mapbox_token = mapbox_vault.get_secret("mapbox-token").value

        fig.update_layout(
            mapbox_style="mapbox://styles/esqtech/cl8nh2452002p15logaud46pv",
            mapbox_accesstoken=mapbox_token,
        )

        if return_bytes:
            buffer = BytesIO()
            fig.write_image(buffer, format="png")
            buffer.seek(0)
            return buffer

        if export_path is not None:
            fig.write_image(export_path, format="png")
            return None

        fig.show()
        return None

    def time_distribution_graph(self, export_path: Optional[str] = None, return_bytes: bool = False):
        hours_of_day = {
            0: "Mid", 1: "1AM", 2: "2AM", 3: "3AM", 4: "4AM", 5: "5AM",
            6: "6AM", 7: "7AM", 8: "8AM", 9: "9AM", 10: "10AM", 11: "11AM",
            12: "Noon", 13: "1PM", 14: "2PM", 15: "3PM", 16: "4PM", 17: "5PM",
            18: "6PM", 19: "7PM", 20: "8PM", 21: "9PM", 22: "10PM", 23: "11PM",
        }
        days_of_week = {0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat"}

        obs = self.raw_data.copy()

        obs["Hour Number"] = obs["Time"].apply(lambda x: x.hour).astype(int)
        obs["Day Number"] = obs["Date"].apply(lambda x: int(dt.strftime(x, "%w"))).astype(int)

        obs = obs.drop_duplicates(subset=["deviceid", "Hour Number", "Date"])

        counts = pd.crosstab(obs["Day Number"], obs["Hour Number"])
        counts = counts.reindex(index=range(7), columns=range(24), fill_value=0)

        total = float(counts.to_numpy().sum())
        weights = (counts / total) * 168.0 if total > 0 else counts.astype(float)

        nx, ny = 24, 7
        width_in = nx / 3.5
        height_in = ny / 3.5

        with _MPL_RENDER_LOCK:
            fig = Figure(figsize=(width_in, height_in))
            FigureCanvas(fig)

            gs = fig.add_gridspec(
                nrows=2,
                ncols=2,
                width_ratios=[1.2, 10.0],
                height_ratios=[1.2, 10.0],
                wspace=0.05,
                hspace=0.05,
            )

            ax_marg_y = fig.add_subplot(gs[1, 0])
            ax_marg_x = fig.add_subplot(gs[0, 1])
            ax_joint = fig.add_subplot(gs[1, 1])

            im = ax_joint.imshow(weights.to_numpy(dtype=float), aspect="equal", interpolation="nearest")

            ax_joint.set_xticks(np.arange(nx))
            ax_joint.set_xticklabels([hours_of_day[i] for i in range(nx)], rotation=90)
            ax_joint.set_yticks(np.arange(ny))
            ax_joint.set_yticklabels([days_of_week[i] for i in range(ny)], rotation=0)
            ax_joint.set_xlabel("(Local Time)", fontsize=8)

            x_sums = weights.sum(axis=0).to_numpy(dtype=float)
            y_sums = weights.sum(axis=1).to_numpy(dtype=float)

            ax_marg_x.bar(np.arange(nx), x_sums)
            ax_marg_y.barh(np.arange(ny), y_sums)

            ax_joint.set_xlim(-0.5, nx - 0.5)
            ax_joint.set_ylim(ny - 0.5, -0.5)
            ax_marg_x.set_xlim(-0.5, nx - 0.5)
            ax_marg_y.set_ylim(ny - 0.5, -0.5)

            ax_marg_x.set_xticks([])
            ax_marg_x.set_yticks([])
            ax_marg_y.set_xticks([])
            ax_marg_y.set_yticks([])
            for ax in (ax_marg_x, ax_marg_y):
                for spine in ax.spines.values():
                    spine.set_visible(False)

            for spine in ax_joint.spines.values():
                spine.set_linewidth(0.8)

            im.set_clim(
                vmin=float(np.nanmin(weights.to_numpy())),
                vmax=float(np.nanmax(weights.to_numpy())),
            )

            return _export_figure(fig, export_path, return_bytes, dpi=300)

    def bullet_current_performance(self) -> str:
        latest_week = self.obs.iloc[-1]
        best_week = self.obs.iloc[int(self.obs["traffic_pct"].idxmax())]
        worst_week = self.obs.iloc[int(self.obs["traffic_pct"].idxmin())]

        latest_performance = float(latest_week["traffic_pct"])
        latest_perf_str = str(round(latest_performance)) if abs(latest_performance) > 1 else str(round(latest_performance, 1))

        if latest_week["Week"] == best_week["Week"]:
            return f"Week {latest_week['Week']} has set a new high in market traffic within the last 4 months, with {latest_perf_str}% above the average."
        if latest_week["Week"] == worst_week["Week"]:
            return f"Week {latest_week['Week']} has set a new low in market traffic within the last 4 months, with {latest_perf_str}% below the average."
        if latest_performance >= 0:
            return f"This market is currently {latest_perf_str}% above the average level of traffic observed over the past 4 months."
        return f"This market is currently {str(abs(float(latest_perf_str)))}% below the average level of traffic observed over the past 4 months."

    def bullet_continuous_growth(self) -> str:
        latest_week = self.obs.iloc[-1]
        max_weeks_of_growth = int(self.obs["Weeks of Growth"].max()) if len(self.obs) else 0

        rev = self.obs.sort_index(ascending=False)
        recent_max = self.obs.iloc[int(rev["Weeks of Growth"].idxmax())]

        if recent_max["Week"] == latest_week["Week"]:
            return f"As of Week {recent_max['Week']} {dates_to_range(recent_max['EarliestDate'], recent_max['LatestDate'])} there have been {max_weeks_of_growth} consecutive weeks of growth."
        return f"The longest period of sustained growth ended in Week {recent_max['Week']} after {max_weeks_of_growth} consecutive weeks of growth."

    def bullet_six_weeks(self) -> str:
        six_weeks = self.obs.iloc[-6:].copy()
        over = six_weeks[six_weeks["traffic_pct"] > 0]
        return f"This market has outperformed its average traffic level in {len(over)} of the last 6 weeks."

    def bullet_budget(self) -> str:
        N = 6
        very_threshold = 30
        recent_performance = float(self.obs.iloc[-N:]["traffic_pct"].mean()) if len(self.obs) else 0.0

        if recent_performance > very_threshold:
            return "We recommend raising advertising budget to take advantage of an increasing market."
        if recent_performance > 0:
            return "We recommend strategic increases in advertising budget while the market continues to grow."
        if recent_performance > -very_threshold:
            return "We recommend a moderate level of advertising budget with an option for increase as the market continues to stabilize."
        return "We recommend maintaining a conservative budget until the market stabilizes fully."

    def stability_score(self) -> float:
        std_dev = float(self.obs["traffic_pct"].std()) if len(self.obs) else 0.0
        stability = 100 - (10 * (std_dev**0.4))
        stabscore = round(stability / 100, 2)
        return float(min(max(stabscore, 0), 1))

    def trend_score(self) -> float:
        trendscore = 50 + trendline(self.obs["traffic_pct"]) * 6
        trendscore = round(trendscore / 100, 2)
        return float(min(max(trendscore, 0), 1))

    def recent_score(self) -> float:
        recentscore = 50 + float(self.obs[-6:]["traffic_pct"].mean()) if len(self.obs) else 50.0
        recentscore = round(recentscore / 100, 2)
        return float(min(max(recentscore, 0), 1))


def _build_datetime_index(df: pd.DataFrame) -> pd.DatetimeIndex:
    if "Datetime" in df.columns:
        parsed = pd.to_datetime(df["Datetime"], errors="coerce")
    elif "timestamp" in df.columns:
        parsed = pd.to_datetime(df["timestamp"], unit="ms", errors="coerce")
    elif "Date" in df.columns and "Time" in df.columns:
        date_str = df["Date"].astype("string").str.strip()
        time_str = df["Time"].astype("string").str.strip()
        combined = date_str + " " + time_str
        parsed = pd.to_datetime(combined, errors="coerce")
    else:
        raise ValueError(
            "Observations data missing datetime columns; expected 'Datetime' or 'timestamp' or ('Date','Time')."
        )

    if not (pd.api.types.is_datetime64_any_dtype(parsed) or pd.api.types.is_datetime64tz_dtype(parsed)):
        parsed = pd.to_datetime(parsed.astype("string"), errors="coerce")

    return pd.DatetimeIndex(parsed)


def _export_figure(fig: Figure, export_path: Optional[str], return_bytes: bool, dpi: int):
    if return_bytes:
        buffer = BytesIO()
        fig.savefig(buffer, format="png", dpi=dpi, bbox_inches="tight", pad_inches=0.1, facecolor="white")
        buffer.seek(0)
        return buffer

    if export_path is not None:
        fig.savefig(export_path, format="png", dpi=dpi, bbox_inches="tight", pad_inches=0.1, facecolor="white")
        return None

    return None


def _single_loc(index: pd.DatetimeIndex, ts: pd.Timestamp) -> int:
    loc = index.get_loc(ts)
    if isinstance(loc, (int, np.integer)):
        return int(loc)
    if isinstance(loc, slice):
        return int(loc.start or 0)
    if isinstance(loc, np.ndarray):
        if loc.dtype == bool:
            positions = np.flatnonzero(loc)
            return int(positions[0]) if len(positions) else 0
        return int(loc[0]) if len(loc) else 0
    return 0


def _month_tick_positions(ref_dates: pd.DatetimeIndex) -> Tuple[np.ndarray, list[str]]:
    if len(ref_dates) == 0:
        return np.array([], dtype=float), []

    months = ref_dates.to_period("M")
    unique_months = months.unique()

    ticks: list[float] = []
    labels: list[str] = []

    for month in unique_months:
        # IMPORTANT: (months == month) may already be a numpy.ndarray in some pandas versions
        mask = np.asarray(months == month)
        positions = np.flatnonzero(mask)
        if positions.size == 0:
            continue

        days = ref_dates[positions].day
        best_idx = int(np.argmin(np.abs(days - 15)))
        best_pos = int(positions[best_idx])

        ticks.append(float(best_pos))
        labels.append(month.to_timestamp(how="start").strftime("%B"))

    return np.array(ticks, dtype=float), labels


def ytick_formatter(x: float, pos: int) -> str:
    s = str(round(x))
    return f"+{s}%" if x > 0 else f"-{s}%" if x < 0 else f"{s}%"


def get_week(d: Date) -> str:
    week = dt.strftime(d, "%W")
    return "53" if week == "00" else week


def get_date_by_week_offset(d: Date, offset: int) -> Date:
    ref_date = d - timedelta(days=d.weekday()) + timedelta(days=offset)
    return ref_date


def dates_to_range(start: Any, end: Any) -> str:
    start_dt = pd.to_datetime(start, errors="coerce").to_pydatetime()
    end_dt = pd.to_datetime(end, errors="coerce").to_pydatetime()
    return f"({start_dt.month}/{start_dt.day}-{end_dt.month}/{end_dt.day})"


def plus_padding(x: int) -> str:
    s = f"+{x}" if x >= 0 else str(x)
    return f" {s}" if len(s) < 3 else s


def trendline(series: pd.Series, order: int = 1) -> float:
    if len(series) == 0:
        return 0.0
    coeffs = np.polyfit(series.index.values, list(series), order)
    slope = coeffs[-2]
    return float(slope)


def sort_by_list(column: pd.Series, sort_list: list[str]) -> pd.Series:
    correspondence = {item: idx for idx, item in enumerate(sort_list)}
    return column.map(correspondence)
