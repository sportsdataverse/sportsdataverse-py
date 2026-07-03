"""Tests for :func:`calculate_nfl_series_conversion_rates`.

A faithful polars port of nflfastR's ``calculate_series_conversion_rates``
(``calculate_series_conversion_rates.R``). The function consumes a caller-
supplied play-by-play frame carrying ``series`` / ``series_success`` /
``series_result`` (added by the Track A ``add_series_data`` port) -- these
tests build a small synthetic pbp frame in-process, no live data required.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.nfl import calculate_nfl_series_conversion_rates


def _row(**kw):  # type: ignore[no-untyped-def]
    base = {
        "season": 2023,
        "week": 1,
        "posteam": "AAA",
        "defteam": "BBB",
        "down": 1,
        "series": 1,
        "series_success": 0,
        "series_result": "Punt",
    }
    base.update(kw)
    return base


def _synthetic_pbp() -> pl.DataFrame:
    rows = [
        # --- Week 1: AAA offense vs BBB defense --------------------------
        # Series 1: 3 plays, ends in a punt (no conversion).
        _row(down=1, series=1, series_success=0, series_result="Punt"),
        _row(down=2, series=1, series_success=0, series_result="Punt"),
        _row(down=3, series=1, series_success=0, series_result="Punt"),
        # Series 2: 1 play, converts for a touchdown on 1st down.
        _row(down=1, series=2, series_success=1, series_result="Touchdown"),
        # Series 3: 4 plays, converts a first down on 4th down.
        _row(down=1, series=3, series_success=1, series_result="First down"),
        _row(down=2, series=3, series_success=1, series_result="First down"),
        _row(down=3, series=3, series_success=1, series_result="First down"),
        _row(down=4, series=3, series_success=1, series_result="First down"),
        # Series 4: 3 plays, turnover on downs^H^H a turnover (no conversion).
        _row(down=1, series=4, series_success=0, series_result="Turnover"),
        _row(down=2, series=4, series_success=0, series_result="Turnover"),
        _row(down=3, series=4, series_success=0, series_result="Turnover"),
        # Series 5: a QB kneel -- must be excluded entirely from off_n.
        _row(down=1, series=5, series_success=1, series_result="QB kneel"),
        # A kickoff (down IS NULL) -- must be excluded via the down filter.
        _row(down=None, series=None, series_success=None, series_result=None, play_type="kickoff"),
        # --- Week 1: BBB offense vs AAA defense --------------------------
        # Series A: 2 plays, converts a first down on 2nd down.
        _row(posteam="BBB", defteam="AAA", down=1, series=1, series_success=1, series_result="First down"),
        _row(posteam="BBB", defteam="AAA", down=2, series=1, series_success=1, series_result="First down"),
        # Series B: 1 play, defense returns it for a touchdown (no conversion).
        _row(posteam="BBB", defteam="AAA", down=1, series=2, series_success=0, series_result="Opp touchdown"),
        # --- Week 2: AAA offense vs CCC defense --------------------------
        # Series 1: 1 play, converts for a touchdown on 1st down.
        _row(
            posteam="AAA",
            defteam="CCC",
            week=2,
            down=1,
            series=1,
            series_success=1,
            series_result="Touchdown",
        ),
    ]
    return pl.DataFrame(rows)


def test_series_weekly_grain_exact_rates() -> None:
    df = calculate_nfl_series_conversion_rates(_synthetic_pbp(), weekly=True)

    aaa_wk1 = df.filter((pl.col("season") == 2023) & (pl.col("team") == "AAA") & (pl.col("week") == 1)).row(
        0, named=True
    )
    assert aaa_wk1["off_n"] == 4
    assert aaa_wk1["off_scr"] == 0.5
    assert aaa_wk1["off_scr_1st"] == 0.25
    assert aaa_wk1["off_scr_2nd"] == 0.0
    assert aaa_wk1["off_scr_3rd"] == 0.0
    assert aaa_wk1["off_scr_4th"] == 0.25
    assert aaa_wk1["off_1st"] == 0.25
    assert aaa_wk1["off_td"] == 0.25
    assert aaa_wk1["off_fg"] == 0.0
    assert aaa_wk1["off_punt"] == 0.25
    assert aaa_wk1["off_to"] == 0.25
    # AAA defended BBB's 2-series offense in week 1 too.
    assert aaa_wk1["def_n"] == 2
    assert aaa_wk1["def_scr"] == 0.5
    assert aaa_wk1["def_scr_2nd"] == 0.5
    assert aaa_wk1["def_1st"] == 0.5
    assert aaa_wk1["def_to"] == 0.5

    aaa_wk2 = df.filter((pl.col("season") == 2023) & (pl.col("team") == "AAA") & (pl.col("week") == 2)).row(
        0, named=True
    )
    assert aaa_wk2["off_n"] == 1
    assert aaa_wk2["off_scr"] == 1.0
    assert aaa_wk2["off_scr_1st"] == 1.0
    assert aaa_wk2["off_td"] == 1.0
    assert aaa_wk2["off_punt"] == 0.0

    bbb_wk1 = df.filter((pl.col("season") == 2023) & (pl.col("team") == "BBB") & (pl.col("week") == 1)).row(
        0, named=True
    )
    assert bbb_wk1["off_n"] == 2
    assert bbb_wk1["off_scr"] == 0.5
    assert bbb_wk1["off_scr_2nd"] == 0.5
    assert bbb_wk1["off_1st"] == 0.5
    assert bbb_wk1["off_to"] == 0.5
    # BBB defended AAA's 4-series offense in week 1.
    assert bbb_wk1["def_n"] == 4
    assert bbb_wk1["def_scr"] == 0.5
    assert bbb_wk1["def_scr_1st"] == 0.25
    assert bbb_wk1["def_scr_4th"] == 0.25
    assert bbb_wk1["def_1st"] == 0.25
    assert bbb_wk1["def_td"] == 0.25
    assert bbb_wk1["def_punt"] == 0.25
    assert bbb_wk1["def_to"] == 0.25


def test_series_season_grain_aggregates_across_weeks() -> None:
    df = calculate_nfl_series_conversion_rates(_synthetic_pbp(), weekly=False)
    assert "week" not in df.columns

    aaa = df.filter((pl.col("season") == 2023) & (pl.col("team") == "AAA")).row(0, named=True)
    # 4 series in week 1 + 1 series in week 2 = 5 total.
    assert aaa["off_n"] == 5
    assert aaa["off_scr"] == 0.6
    assert aaa["off_scr_1st"] == 0.4
    assert aaa["off_scr_4th"] == 0.2
    assert aaa["off_1st"] == 0.2
    assert aaa["off_td"] == 0.4
    assert aaa["off_punt"] == 0.2
    assert aaa["off_to"] == 0.2
    assert aaa["off_fg"] == 0.0


def test_series_empty_pbp_returns_zero_row_schema() -> None:
    empty = pl.DataFrame(
        schema={
            "season": pl.Int64,
            "week": pl.Int64,
            "posteam": pl.Utf8,
            "defteam": pl.Utf8,
            "down": pl.Int64,
            "series": pl.Int64,
            "series_success": pl.Int64,
            "series_result": pl.Utf8,
        }
    )
    weekly = calculate_nfl_series_conversion_rates(empty, weekly=True)
    assert weekly.height == 0
    assert "week" in weekly.columns
    assert "off_n" in weekly.columns
    assert "def_to" in weekly.columns

    season = calculate_nfl_series_conversion_rates(empty, weekly=False)
    assert season.height == 0
    assert "week" not in season.columns


def test_series_return_as_pandas() -> None:
    import pandas as pd

    out = calculate_nfl_series_conversion_rates(_synthetic_pbp(), weekly=True, return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)
    assert out.shape[0] > 0
