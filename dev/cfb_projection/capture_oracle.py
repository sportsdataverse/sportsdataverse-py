"""Capture the committed oracle corpus for the CFB projection spine (T2.2 Task 0.2).

Writes two parquet fixtures under tests/fixtures/cfb_projection/:
  * results_2016_2023.parquet  -- game scores 2016-2023 (load_cfb_schedule; works offline
    of the pbp 404). Contract: game_id/season/week/home_team_id/away_team_id/home_score/
    away_score/neutral_site, ids Utf8.
  * talent_247_2023.parquet    -- 247 composite team ranking for the validation season
    (sports247_composite_team_ranking_feed; needs curl_cffi). Contract: season/team_id/
    team/talent_247/talent_rank. team_id here is the 247 `key`; downstream joins on the
    normalized `team` name (247 key != ESPN id) -- same pattern as T2.1's SP+/FEI oracle.

The NFL-draft fixture is deferred to Phase 5 (the ESPN season-draft endpoint 404s for
recent years and needs its own investigation; nothing before Phase 5 consumes it).

Run:  SDV_PY_LIVE_TESTS=1 uv run --with curl_cffi python dev/cfb_projection/capture_oracle.py
"""

from __future__ import annotations

import pathlib

import polars as pl

from sportsdataverse.cfb import load_cfb_schedule, sports247_composite_team_ranking_feed

_FIX = pathlib.Path("tests/fixtures/cfb_projection")
_VALIDATION_SEASON = 2023
_SEASONS = list(range(2016, 2024))


def _utf8_id(col: str) -> pl.Expr:
    """Integer-origin id -> Utf8 (never float->str)."""
    return pl.col(col).cast(pl.Int64).cast(pl.Utf8)


def capture_results() -> None:
    sched = load_cfb_schedule(_SEASONS)
    out = (
        sched.filter(pl.col("home_points").is_not_null() & pl.col("away_points").is_not_null())
        .select(
            _utf8_id("game_id").alias("game_id"),
            pl.col("season").cast(pl.Int64),
            pl.col("week").cast(pl.Int64),
            _utf8_id("home_id").alias("home_team_id"),
            _utf8_id("away_id").alias("away_team_id"),
            pl.col("home_points").cast(pl.Int64).alias("home_score"),
            pl.col("away_points").cast(pl.Int64).alias("away_score"),
            pl.col("neutral_site").cast(pl.Boolean),
        )
        .sort("season", "week", "game_id")
    )
    out.write_parquet(_FIX / "results_2016_2023.parquet")
    print(f"results_2016_2023: {out.height} games, {out['season'].n_unique()} seasons")


def capture_talent() -> None:
    feed = sports247_composite_team_ranking_feed(_VALIDATION_SEASON, sport_key=1, page_size=200, return_as_pandas=False)
    out = (
        feed.select(
            pl.lit(_VALIDATION_SEASON, dtype=pl.Int64).alias("season"),
            _utf8_id("key").alias("team_id"),
            pl.col("full_name").cast(pl.Utf8).alias("team"),
            pl.col("composite_rating").cast(pl.Float64).alias("talent_247"),
            pl.col("composite_overall_rank").cast(pl.Int64).alias("talent_rank"),
        )
        .drop_nulls(["team", "talent_247"])
        # rank 0 = unranked non-FBS (D-II/NAIA) teams the feed also returns; drop them.
        .filter(pl.col("talent_rank") > 0)
        .sort("talent_rank")
    )
    out.write_parquet(_FIX / "talent_247_2023.parquet")
    print(f"talent_247_2023: {out.height} teams; top: {out['team'][:3].to_list()}")


def capture_recruits() -> None:
    """Committed recruit classes 2020-2023 (per-recruit stars/grade) for the Phase-1 gate.

    Paginates sports247_recruits directly (page_size=200, per-page retry + delay) --
    the 247 RDB times out sporadically under larger pages / rapid-fire paging.
    """
    import time

    from sportsdataverse.cfb.cfb_roster_talent import _normalize_recruit_page
    from sportsdataverse.cfb.sports247 import sports247_recruits

    page_size = 200
    frames: list[pl.DataFrame] = []
    for season in range(2020, 2024):
        page = 1
        while True:
            for attempt in range(4):
                try:
                    raw = sports247_recruits(sport_key=1, year=season, page_size=page_size, page=page)
                    break
                except Exception as exc:  # timeout / transient -- back off and retry
                    if attempt == 3:
                        raise
                    print(f"  {season} p{page} attempt {attempt + 1} failed ({type(exc).__name__}); retrying")
                    time.sleep(5 * (attempt + 1))
            if raw is None or raw.height == 0:
                break
            frames.append(_normalize_recruit_page(raw, season))
            print(f"  {season} p{page}: {raw.height} rows")
            if raw.height < page_size:
                break
            page += 1
            time.sleep(1.5)
    out = pl.concat(frames)
    out.write_parquet(_FIX / "recruits_2020_2023.parquet")
    per_season = out.group_by("season").len().sort("season")
    print(f"recruits_2020_2023: {out.height} recruits; per-season: {per_season.to_dicts()}")


def capture_returning() -> None:
    """Committed returning-production 2017-2023 (Task 2.2 retention gate).

    Computes cfb_returning_production per season from the hosted player-stats +
    rosters, then attaches the ESPN team id via the teams crosswalk (norm_key)
    so the gate can join the results fixture (ESPN-id keyed) offline.
    """
    from sportsdataverse.cfb.cfb_crosswalk import _norm_team
    from sportsdataverse.cfb.cfb_loaders import load_cfb_team_info
    from sportsdataverse.cfb.cfb_returning_production import cfb_returning_production

    seasons = list(range(2017, 2024))
    rp = cfb_returning_production(seasons)
    assert isinstance(rp, pl.DataFrame)
    ti = load_cfb_team_info(2023)
    assert isinstance(ti, pl.DataFrame)
    # play-stats team names are school-only ("michigan"), so key on _norm_team(school);
    # the teams-crosswalk norm_key carries the mascot and does NOT match.
    ti_keys = (
        ti.select(
            pl.col("school").cast(pl.Utf8).map_elements(_norm_team, return_dtype=pl.Utf8).alias("school_key"),
            pl.col("team_id").cast(pl.Int64).cast(pl.Utf8).alias("team_id"),
            pl.col("classification").cast(pl.Utf8),
        )
        .drop_nulls(["school_key", "team_id"])
        .unique(subset=["school_key"])
    )
    out = rp.join(ti_keys, left_on="team", right_on="school_key", how="left")
    matched = out.filter(pl.col("team_id").is_not_null()).height
    fbs = out.filter(pl.col("classification") == "fbs").height
    out.write_parquet(_FIX / "returning_2017_2023.parquet")
    print(f"returning_2017_2023: {out.height} team-seasons; espn-id matched {matched}; fbs rows {fbs}")


if __name__ == "__main__":
    import sys

    _FIX.mkdir(parents=True, exist_ok=True)
    only = sys.argv[1] if len(sys.argv) > 1 else None
    if only in (None, "results"):
        capture_results()
    if only in (None, "talent"):
        capture_talent()
    if only in (None, "recruits"):
        capture_recruits()
    if only in (None, "returning"):
        capture_returning()
