"""Rebuild ``cfb_ratings_weekly``'s efficiency columns with the current ``efficiency_ratings``.

The input the 2026-11-25 ``CFB_CONSTANTS`` refit and the ``ratings_weekly_2024`` test fixture were
built from. For every published (season, through_week) snapshot it refits ``efficiency_ratings`` on
the plays that snapshot saw -- FBS-vs-FBS, kneels dropped, regular-season weeks ``<= through_week``
-- and replaces ``adj_off_epa`` / ``adj_def_epa`` / ``adj_net``; every other column is kept as
published. ``--engine pre`` refits with the pre-2026-11-25 engine instead (spread-aware
``0.1 <= wp_before <= 0.9`` band, standardized ridge, lambda 0.035), which reproduces the
published values to ~6e-14 and so validates the rebuild.

Usage::

    PYTHONPATH=<sdv-py checkout> python -m tools.validation.cfb_ratings_weekly_rebuild \\
        --out weekly.parquet 2014 2015 ... 2025

Then fit the constants on it: cfbfastR-cfb-data ``cfb_higher_models fit-pregame --seasons 2014 ...
2025 --holdout 2024 2025`` with its ``data.load_ratings`` reading ``weekly.parquet`` (``rt_``-prefix
every numeric non-key, non-rank column, as ``load_ratings`` does).
"""

from __future__ import annotations

import argparse

import polars as pl

from sportsdataverse.cfb import load_cfb_pbp, load_cfb_ratings_weekly, load_cfb_schedule
from sportsdataverse.cfb.cfb_adjusted_epa import (
    _REQUIRED_COLUMNS,
    _RIDGE_LAMBDA,
    _adjust_games,
    _fit_opponent_ridge,
    _prepare,
)
from sportsdataverse.cfb.cfb_ratings import (
    _alias_released_pbp,
    _drop_kneel_downs,
    _home_into_pos_team_namespace,
    efficiency_ratings,
)

_KEY = ["team_id", "season", "through_week"]
_EFF = ["adj_off_epa", "adj_def_epa", "adj_net"]


def _pre_engine(plays: pl.DataFrame) -> pl.DataFrame:
    """The ``efficiency_ratings`` fit before 2026-11-25 (validation only)."""
    base, clean = _prepare(plays, _REQUIRED_COLUMNS)
    off, dfn, _ = _fit_opponent_ridge(clean, _RIDGE_LAMBDA)
    return (
        _adjust_games(base, off, dfn, fill_strength=None)
        .group_by("pos_team_id")
        .agg(adj_off_epa=pl.col("adj_off_epa").mean(), adj_def_epa=pl.col("adj_def_epa").mean())
        .rename({"pos_team_id": "team_id"})
        .with_columns(pl.col(_EFF[:2]).fill_null(0.0))
        .with_columns(adj_net=pl.col("adj_off_epa") - pl.col("adj_def_epa"))
    )


def _season_plays(season: int) -> pl.DataFrame:
    """Regular-season FBS-vs-FBS plays, kneels dropped -- what ``cfb_ratings`` fits on."""
    pbp = load_cfb_pbp([season]).with_columns(pl.col("game_id").cast(pl.Utf8))
    pbp = _home_into_pos_team_namespace(_alias_released_pbp(pbp))
    sched = load_cfb_schedule([season]).select(
        pl.col("game_id").cast(pl.Utf8), "neutral_site", "home_division", "away_division"
    )
    assert pbp.schema["game_id"] == sched.schema["game_id"]
    plays = pbp.join(sched, on="game_id", how="inner").filter(
        (pl.col("home_division") == "fbs") & (pl.col("away_division") == "fbs") & (pl.col("seasonType") == 2)
    )
    return _drop_kneel_downs(plays)


def rebuild(seasons: list[int], *, engine: str = "current") -> pl.DataFrame:
    """Published snapshots for ``seasons`` with the efficiency columns refit by ``engine``."""
    fit = _pre_engine if engine == "pre" else efficiency_ratings
    pub = pl.from_pandas(p) if not isinstance(p := load_cfb_ratings_weekly(seasons), pl.DataFrame) else p
    pub = pub.with_columns(pl.col("team_id").cast(pl.Utf8), pl.col("season").cast(pl.Int64))
    parts = []
    for season in seasons:
        plays = _season_plays(season)
        for week in sorted(pub.filter(pl.col("season") == season)["through_week"].unique().to_list()):
            e = fit(plays.filter(pl.col("week") <= week)).select("team_id", *_EFF)
            parts.append(e.with_columns(season=pl.lit(season, pl.Int64), through_week=pl.lit(week, pl.Int32)))
    new = pl.concat(parts)
    assert all(new.schema[k] == pub.schema[k] for k in _KEY)
    out = pub.join(new, on=_KEY, how="left", suffix="_new")
    d = (out["adj_net"] - out["adj_net_new"]).abs()
    print(f"{out.height} snapshot rows, {out['adj_net_new'].is_not_null().sum()} refit; max |d adj_net| {d.max():.3g}")
    return out.with_columns([pl.coalesce(f"{c}_new", c).alias(c) for c in _EFF]).drop([f"{c}_new" for c in _EFF])


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="cfb_ratings_weekly_rebuild")
    ap.add_argument("seasons", nargs="+", type=int)
    ap.add_argument("--out", required=True)
    ap.add_argument("--engine", choices=("current", "pre"), default="current")
    a = ap.parse_args(argv)
    rebuild(a.seasons, engine=a.engine).write_parquet(a.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
