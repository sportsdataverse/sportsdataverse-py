"""Apply (NOT re-fit) the box-value formula to the WNBA corpus -> career/rookie labels.

Unlike ``dev/nba_draft/fit_box_value.py``, this script does **not** re-fit
``LEAGUE_CONSTANTS["wnba"].box_value_coef`` -- there is no ``wnba_bpm`` (or any other
independent advanced-metric anchor) to ridge-regress box rates against for WNBA, so that
re-fit is out of scope here (a pre-existing, still-documented caveat: WNBA's
``box_value_coef``/``replacement`` remain the NBA-fit values, see
``nba_draft_constants.py``'s ``LEAGUE_CONSTANTS`` docstring). This script only *applies* the
existing formula to real WNBA per-player-season box totals (``dev/wnba_draft/capture_corpus.py``)
to materialize the two derived fixtures every other WNBA fit script needs:
``career_values.parquet`` (all-era career value per drafted player) and
``rookie_values.parquet`` (rookie/soph season value for the same players) --
these ARE genuine, computed from real WNBA box scores, even though the linear weights that
turn box rates into a value score are borrowed.

Run: ``uv run python dev/wnba_draft/fit_box_value.py`` (offline -- reads only the committed
fixture, no live network needed).
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.nba.nba_draft_constants import box_value_per100

FIXTURE_DIR = "tests/fixtures/wnba_draft"


def add_per100_rates(season_stats: pl.DataFrame) -> pl.DataFrame:
    """Per-player-estimated-possessions per-100 rates (documented approximation).

    Same estimator as ``dev/nba_draft/fit_box_value.py.add_per100_rates`` -- WNBA
    ``playercareerstats`` season totals carry no team-pace context either, so possessions are
    estimated per-player as ``fga + 0.44*fta + tov``.
    """
    pos_est = pl.col("fga") + 0.44 * pl.col("fta") + pl.col("tov")
    return season_stats.with_columns(
        pos_est.alias("_pos_est"),
        pl.col("season_id").str.slice(0, 4).cast(pl.Int64).alias("season"),
    ).with_columns(
        (pl.col("pts") / pl.col("_pos_est").clip(1.0, None) * 100).alias("pts100"),
        (pl.col("reb") / pl.col("_pos_est").clip(1.0, None) * 100).alias("reb100"),
        (pl.col("ast") / pl.col("_pos_est").clip(1.0, None) * 100).alias("ast100"),
        (pl.col("stl") / pl.col("_pos_est").clip(1.0, None) * 100).alias("stl100"),
        (pl.col("blk") / pl.col("_pos_est").clip(1.0, None) * 100).alias("blk100"),
        (pl.col("tov") / pl.col("_pos_est").clip(1.0, None) * 100).alias("tov100"),
        (pl.col("pts") / (2 * (pl.col("fga") + 0.44 * pl.col("fta")).clip(1.0, None))).alias("ts_pct"),
        (pl.col("_pos_est") / pl.col("min").clip(1.0, None) * 100).alias("usg"),
        pl.col("min").alias("minutes"),
    )


def main() -> None:
    season_stats = pl.read_parquet(f"{FIXTURE_DIR}/season_stats_raw.parquet")
    # 1997-2000-era rows occasionally ship a mid-season-trade duplicate the same way the NBA
    # corpus did (a per-team row + a TOT row) -- dedupe defensively (keep the higher-minutes
    # row, which is the TOT row when one exists). maintain_order=True: polars' default sort is
    # unstable under threads, so ties in "min" would otherwise pick a non-deterministic
    # survivor across runs.
    season_stats = season_stats.sort("min", descending=True, maintain_order=True).unique(
        subset=["player_id", "season_id"], keep="first", maintain_order=True
    )
    rates = add_per100_rates(season_stats)
    assert rates.schema["player_id"] == pl.Utf8

    all_rates = rates.with_columns(box_value_per100(rates, league="wnba").alias("_box_value"))
    replacement = -22.6616  # LEAGUE_CONSTANTS["wnba"].replacement (NBA-borrowed, unchanged)
    all_rates = all_rates.with_columns(
        ((pl.col("_box_value") - replacement) * pl.col("minutes") / 1000.0).alias("_season_vorp")
    )

    career_values = (
        all_rates.group_by("player_id")
        .agg(
            pl.col("_season_vorp").sum().alias("career_value"),
            pl.len().alias("seasons_played"),
            pl.col("minutes").sum().alias("total_minutes"),
        )
        .with_columns(pl.col("seasons_played").cast(pl.Int64))
    )
    career_values.write_parquet(f"{FIXTURE_DIR}/career_values.parquet")
    print(f"wrote career_values.parquet ({career_values.height} rows)")
    print(career_values["career_value"].describe())

    # rookie/soph: first two season rows per player, ordered by season.
    ordered = all_rates.sort("player_id", "season").with_columns(
        pl.int_range(0, pl.len()).over("player_id").alias("_season_idx")
    )
    draft_history = (
        pl.read_parquet(f"{FIXTURE_DIR}/draft_history.parquet")
        .select("player_id", "draft_year")
        .unique(subset=["player_id"], keep="first")  # defensive: guard a hypothetical re-entry/dispersal dup
    )
    rookie = ordered.filter(pl.col("_season_idx") == 0).select(
        "player_id", pl.col("_season_vorp").alias("rookie_value"), pl.col("minutes").alias("rookie_min")
    )
    soph = ordered.filter(pl.col("_season_idx") == 1).select("player_id", pl.col("_season_vorp").alias("soph_value"))
    rookie_values = (
        draft_history.join(rookie, on="player_id", how="left")
        .join(soph, on="player_id", how="left")
        .with_columns(
            pl.col("rookie_value").fill_null(0.0),
            pl.col("soph_value").fill_null(0.0),
            pl.col("rookie_min").fill_null(0.0),
        )
    )
    rookie_values.write_parquet(f"{FIXTURE_DIR}/rookie_values.parquet")
    print(f"wrote rookie_values.parquet ({rookie_values.height} rows)")


if __name__ == "__main__":
    main()
