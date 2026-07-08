"""NFL game-script / pace engine ② — sec/play, neutral pace, PROE, expected plays.

Descriptive team-game and team-season pace surface on top of ``load_nfl_pbp``
(EPA / ``pass_oe`` already present).  Expected plays come from the fitted
``PACE_CONSTANTS`` OLS table (``dev/nfl_scheme/fit_pace_constants.py``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional, Union

import polars as pl

from sportsdataverse.nfl.nfl_scheme_constants import PACE_CONSTANTS

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

#: play_type values excluded from the pace universe (clock-kill plays).
_EXCLUDED_PLAY_TYPES: List[str] = ["qb_kneel", "qb_spike", "no_play"]

_PACE_SCHEMA: dict = {
    "game_id": pl.Utf8,
    "season": pl.Int64,
    "week": pl.Int64,
    "posteam": pl.Utf8,
    "off_plays": pl.Int64,
    "sec_per_play": pl.Float64,
    "neutral_plays": pl.Int64,
    "neutral_sec_per_play": pl.Float64,
    "proe": pl.Float64,
    "dropbacks": pl.Int64,
}

_GAME_SCRIPT_SCHEMA: dict = {
    "season": pl.Int64,
    "team": pl.Utf8,
    "games": pl.Int64,
    "off_plays_pg": pl.Float64,
    "sec_per_play": pl.Float64,
    "neutral_sec_per_play": pl.Float64,
    "proe": pl.Float64,
    "exp_plays_pg": pl.Float64,
    "plays_oe": pl.Float64,
    "pace_rank": pl.Int64,
}


def _drive_sec_per_play(df: pl.DataFrame) -> pl.DataFrame:
    """Per (game, posteam): mean over drives of (elapsed clock / plays)."""
    return (
        df.filter(pl.col("drive").is_not_null())
        .group_by("game_id", "posteam", "drive")
        .agg(
            ((pl.col("game_seconds_remaining").max() - pl.col("game_seconds_remaining").min()) / pl.len()).alias(
                "drive_spp"
            )
        )
        .group_by("game_id", "posteam")
        .agg(pl.col("drive_spp").mean().alias("sec_per_play"))
    )


def team_game_pace(pbp: pl.DataFrame) -> pl.DataFrame:
    """Per team-game pace + pass-rate-over-expected.

    ``sec_per_play`` is the per-drive elapsed ``game_seconds_remaining``
    divided by drive plays, averaged over the team's offensive drives
    (kneels / spikes / no_plays excluded).  Neutral = ``wp`` in [0.2, 0.8]
    and ``half_seconds_remaining`` > 120.  ``proe`` is the mean ``pass_oe``
    over dropbacks.

    Args:
        pbp: nflverse-format pbp with ``game_id`` / ``season`` / ``week`` /
            ``posteam`` / ``drive`` / ``play_type`` / ``qb_dropback`` /
            ``pass_oe`` / ``game_seconds_remaining`` / ``wp`` /
            ``half_seconds_remaining``.

    Returns:
        One row per ``(game_id, season, week, posteam)`` with ``off_plays``,
        ``sec_per_play``, ``neutral_plays``, ``neutral_sec_per_play``,
        ``proe``.  Empty input yields a zero-row frame with this schema.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_pbp
            from sportsdataverse.nfl.nfl_gamescript import team_game_pace
            pace = team_game_pace(load_nfl_pbp([2023]))
            print(pace.sort("sec_per_play").head())
    """
    df = pbp.filter(
        pl.col("posteam").is_not_null()
        & pl.col("play_type").is_not_null()
        & ~pl.col("play_type").is_in(_EXCLUDED_PLAY_TYPES)
    )
    if df.height == 0:
        return pl.DataFrame(schema=_PACE_SCHEMA)

    df = df.with_columns(pl.col("posteam").cast(pl.Utf8), pl.col("game_id").cast(pl.Utf8))
    neutral = df.filter((pl.col("wp") >= 0.2) & (pl.col("wp") <= 0.8) & (pl.col("half_seconds_remaining") > 120.0))

    base = df.group_by("game_id", "season", "week", "posteam").agg(
        pl.len().cast(pl.Int64).alias("off_plays"),
        pl.col("pass_oe").filter(pl.col("qb_dropback") == 1).mean().alias("proe"),
        pl.col("pass_oe")
        .filter((pl.col("qb_dropback") == 1) & pl.col("pass_oe").is_not_null())
        .len()
        .cast(pl.Int64)
        .alias("dropbacks"),
    )
    spp = _drive_sec_per_play(df)
    nspp = _drive_sec_per_play(neutral).rename({"sec_per_play": "neutral_sec_per_play"})
    ncount = neutral.group_by("game_id", "posteam").agg(pl.len().cast(pl.Int64).alias("neutral_plays"))

    assert base.schema["game_id"] == spp.schema["game_id"]
    out = (
        base.join(spp, on=["game_id", "posteam"], how="left")
        .join(ncount, on=["game_id", "posteam"], how="left")
        .join(nspp, on=["game_id", "posteam"], how="left")
        .with_columns(
            pl.col("neutral_plays").fill_null(0).cast(pl.Int64),
            pl.col("season").cast(pl.Int64),
            pl.col("week").cast(pl.Int64),
        )
        .select(list(_PACE_SCHEMA.keys()))
        .sort("game_id", "posteam")
    )
    return out


def _game_script_from(pbp: pl.DataFrame, schedule: Optional[pl.DataFrame] = None) -> pl.DataFrame:
    """Team-season game-script aggregate from an in-memory pbp (injectable core)."""
    pace = team_game_pace(pbp)
    if pace.height == 0:
        return pl.DataFrame(schema=_GAME_SCRIPT_SCHEMA)

    # attach opponent per-game neutral pace via the game's other row
    opp = pace.select("game_id", "posteam", "neutral_sec_per_play").rename(
        {"posteam": "opp_team", "neutral_sec_per_play": "opp_neutral_sec_per_play"}
    )
    assert pace.schema["game_id"] == opp.schema["game_id"]
    pg = pace.join(opp, on="game_id", how="inner").filter(pl.col("posteam") != pl.col("opp_team"))
    if schedule is not None and "total_line" in schedule.columns:
        sched = schedule.select(pl.col("game_id").cast(pl.Utf8), "total_line")
        pg = pg.join(sched, on="game_id", how="left")
    else:
        pg = pg.with_columns(pl.lit(None, dtype=pl.Float64).alias("total_line"))

    c = PACE_CONSTANTS
    season_team = (
        pg.group_by("season", "posteam")
        .agg(
            pl.len().cast(pl.Int64).alias("games"),
            pl.col("off_plays").mean().alias("off_plays_pg"),
            pl.col("sec_per_play").mean().alias("sec_per_play"),
            pl.col("neutral_sec_per_play").mean().alias("neutral_sec_per_play"),
            ((pl.col("proe") * pl.col("dropbacks")).sum() / pl.col("dropbacks").sum()).alias("proe"),
            pl.col("opp_neutral_sec_per_play").mean().alias("opp_neutral_sec_per_play"),
            pl.col("total_line").mean().alias("total_line_avg"),
        )
        .with_columns(
            (
                c["intercept"]
                + c["b_pace"] * pl.col("neutral_sec_per_play")
                + c["b_opp_pace"] * pl.col("opp_neutral_sec_per_play")
                + c["b_total"] * pl.col("total_line_avg").fill_null(c["total_mean"])
            ).alias("exp_plays_pg")
        )
        .with_columns((pl.col("off_plays_pg") - pl.col("exp_plays_pg")).alias("plays_oe"))
        .with_columns(pl.col("neutral_sec_per_play").rank("ordinal").over("season").cast(pl.Int64).alias("pace_rank"))
        .rename({"posteam": "team"})
        .select(list(_GAME_SCRIPT_SCHEMA.keys()))
        .sort("season", "team")
    )
    return season_team


def nfl_game_script(
    seasons: Union[int, List[int]],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Team-season pace / PROE / expected-plays engine.

    Loads pbp + schedules for ``seasons``, aggregates per-game pace to the
    team-season level, and computes expected plays per game from the fitted
    :data:`sportsdataverse.nfl.nfl_scheme_constants.PACE_CONSTANTS`.

    Args:
        seasons: Season or list of seasons (nflverse pbp coverage).
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        Per ``(season, team)``: ``games``, ``off_plays_pg``, ``sec_per_play``,
        ``neutral_sec_per_play``, ``proe``, ``exp_plays_pg``, ``plays_oe``,
        ``pace_rank`` (1 = fastest neutral pace).  Empty seasons yield a
        zero-row frame with this schema.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_gamescript import nfl_game_script
            gs = nfl_game_script([2023])
            print(gs.sort("proe", descending=True).head())

        Pipeline next step::

            gs.filter(pl.col("plays_oe") > 0).sort("plays_oe", descending=True).head()

        See Also:
            * `nflfastR`_ -- source of the ``pass_oe`` column PROE aggregates.

        .. _nflfastR: https://www.nflfastr.com
    """
    from sportsdataverse.nfl.nfl_loaders import load_nfl_pbp, load_nfl_schedule

    season_list = [seasons] if isinstance(seasons, int) else list(seasons)
    if not season_list:
        out: pl.DataFrame = pl.DataFrame(schema=_GAME_SCRIPT_SCHEMA)
        return out.to_pandas() if return_as_pandas else out
    pbp = load_nfl_pbp(season_list)
    if "pass_oe" not in pbp.columns or "xpass" not in pbp.columns:
        from sportsdataverse.nfl.ep_wp import calculate_xpass

        pbp = calculate_xpass(pbp)
    sched = load_nfl_schedule(season_list)
    out = _game_script_from(pbp, sched)
    return out.to_pandas() if return_as_pandas else out
