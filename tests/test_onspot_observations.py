"""
Smoke test for the resolved merge conflict in onspot_graphics/observations.py.

The conflict was between a pre-pivot "drop the maximum week" filter and the
post-pivot "keep complete weeks, then the final 16" filter that the same
commit added. These tests confirm the resolution keeps complete weeks only and
does not drop the wrong week across a year boundary.
"""

import sys
import types
from datetime import date, timedelta

import pandas as pd

sys.path.insert(0, "/workspaces/content")

from libs.utils.esquire.onspot_graphics.observations import Observations

LAT = 41.88
LNG = -87.63


def build_observations(start, end, devices_per_day=100):
    """Synthetic Onspot style observations: one row per device per day."""
    rows = []
    day = start
    while day <= end:
        for i in range(devices_per_day):
            rows.append(
                {
                    "Date": day.isoformat(),
                    "Time": "12:00:00",
                    "lat": LAT,
                    "lng": LNG,
                    "deviceid": "dev-{0}-{1}".format(day.isoformat(), i),
                }
            )
        day += timedelta(days=1)
    return pd.DataFrame(rows)


def sunday_on_or_before(day):
    return day - timedelta(days=(day.weekday() + 1) % 7)


def test_conflict_markers_are_gone():
    import inspect

    source = inspect.getsource(Observations.__init__)
    assert "<<<<<<<" not in source
    assert "=======" not in source
    assert ">>>>>>>" not in source


def test_report_keeps_only_complete_weeks():
    """The latest week is a partial one and must not be the last row."""
    end = sunday_on_or_before(date(2026, 3, 15))
    start = end - timedelta(days=130)
    data = build_observations(start, end)

    obs = Observations(data, end_date=end.isoformat())

    # 16 complete weeks are kept.
    assert len(obs.obs) == 16

    # Every kept week ends on or before the requested report end.
    assert obs.obs["LatestDate"].max() <= pd.Timestamp(end)

    # The incomplete trailing days must not be counted as a week.
    assert obs.obs["LatestDate"].max() == pd.Timestamp(end)


def test_latest_week_is_not_the_partial_week():
    """The bug being fixed was a -100% traffic reading on the latest week."""
    end = sunday_on_or_before(date(2026, 3, 15))
    start = end - timedelta(days=130)
    data = build_observations(start, end)

    # Add a partial week: only two days, with a single device.
    partial_start = end + timedelta(days=1)
    partial = build_observations(partial_start, partial_start + timedelta(days=1), devices_per_day=1)
    data = pd.concat([data, partial], ignore_index=True)

    obs = Observations(data, end_date=end.isoformat())

    latest = obs.get_latest_week()
    assert latest["traffic_pct"] != -100.0
    assert latest["Range"] is not None


def test_weeks_across_a_year_boundary_are_kept():
    """ISO week numbers repeat each year, so the wrong week must not be cut."""
    end = sunday_on_or_before(date(2026, 1, 4))
    start = end - timedelta(days=130)
    data = build_observations(start, end)

    obs = Observations(data, end_date=end.isoformat())

    assert len(obs.obs) == 16

    # The kept weeks span both calendar years, so the January week of the
    # earlier year was not dropped by a max-week-number filter.
    assert obs.obs["ISOYear"].nunique() >= 2
    assert obs.obs["LatestDate"].max() == pd.Timestamp(end)


def test_no_observations_raises():
    """Data outside the report window leaves nothing to report."""
    end = sunday_on_or_before(date(2026, 3, 15))

    for start, stop in (
        # entirely before the 111 day window
        (end - timedelta(days=200), end - timedelta(days=150)),
        # entirely after the report end
        (end + timedelta(days=1), end + timedelta(days=3)),
    ):
        data = build_observations(start, stop)
        try:
            Observations(data, end_date=end.isoformat())
        except ValueError as exc:
            assert "No observations remain" in str(exc)
        else:
            raise AssertionError("expected a ValueError for an empty report window")
