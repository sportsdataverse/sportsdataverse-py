"""Transfer-portal team impact for CFB (T2.2 model ②).

Transfer moves are inferred from year-over-year roster diffs (a player on team
T in season S who was on a different team in S-1 is an incoming transfer to T
and an outgoing one from the prior team). Each move carries the player's
recruit-star talent points; a team-season's ``net_transfer_talent`` is incoming
minus outgoing points, and ``pred_win_delta`` maps that to a projected
win-total change via an on-demand as-of ridge against realized win deltas.
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.cfb.cfb_loaders import load_cfb_rosters, load_cfb_schedule
from sportsdataverse.cfb.cfb_projection_constants import fit_ridge, get_constants, predict_ridge
from sportsdataverse.cfb.cfb_roster_talent import load_recruit_classes

__all__ = ["cfb_transfer_impact", "cfb_transfer_moves"]

_MOVES_SCHEMA: dict[str, pl.PolarsDataType] = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "player_id": pl.Utf8,
    "direction": pl.Utf8,
    "prior_team_id": pl.Utf8,
    "talent_points": pl.Float64,
}

_IMPACT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "net_transfer_talent": pl.Float64,
    "pred_win_delta": pl.Float64,
}


def _load_roster_keys(seasons: list[int]) -> pl.DataFrame:
    """Roster membership keys per season: season/team_id/player_id (Utf8 ids)."""
    frames: list[pl.DataFrame] = []
    for season in seasons:
        r = load_cfb_rosters(season)
        if isinstance(r, pd.DataFrame):
            r = pl.from_pandas(r)
        if r.height == 0:
            continue
        frames.append(
            r.select(
                pl.lit(season, dtype=pl.Int64).alias("season"),
                pl.col("team").cast(pl.Utf8).alias("team_id"),  # school name is the stable roster key
                pl.col("athlete_id").cast(pl.Utf8).alias("player_id"),
                (
                    pl.col("first_name").cast(pl.Utf8).fill_null("")
                    + " "
                    + pl.col("last_name").cast(pl.Utf8).fill_null("")
                )
                .str.strip_chars()
                .alias("player_name"),
            ).drop_nulls(["season", "team_id", "player_id"])
        )
    if not frames:
        return pl.DataFrame(
            schema={"season": pl.Int64, "team_id": pl.Utf8, "player_id": pl.Utf8, "player_name": pl.Utf8}
        )
    return pl.concat(frames).unique(subset=["season", "team_id", "player_id"])


def _talent_points_lookup(seasons: list[int], division: str) -> pl.DataFrame:
    """Case-folded player name -> recruit-star talent points.

    Roster athlete ids (ESPN) and 247 recruit keys are different id spaces, so
    the lookup keys on the normalized player name; unmatched movers fall back
    to the 0-star default points.
    """
    rec = load_recruit_classes(seasons, division=division)
    if isinstance(rec, pd.DataFrame):
        rec = pl.from_pandas(rec)
    consts = get_constants(division)
    if rec.height == 0:
        return pl.DataFrame(schema={"_name": pl.Utf8, "talent_points": pl.Float64})
    return (
        rec.drop_nulls(["player_name"])
        .select(
            pl.col("player_name").str.to_lowercase().str.strip_chars().alias("_name"),
            pl.col("stars")
            .replace_strict(consts.star_points, default=consts.star_points.get(0, 0.0), return_dtype=pl.Float64)
            .alias("talent_points"),
        )
        .group_by("_name")
        .agg(pl.col("talent_points").max())
    )


def cfb_transfer_moves(
    seasons: int | list[int], *, division: str = "fbs", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Transfer moves inferred from year-over-year roster diffs.

    Args:
        seasons: Destination season(s) to extract moves for (each compares S-1 -> S).
        division: Division slug for the star-points constants.
        return_as_pandas: If True, return a pandas DataFrame; otherwise polars.

    Returns:
        One row per move side: ``season`` (Int64, the destination season),
        ``team_id`` (Utf8), ``player_id`` (Utf8), ``direction`` ("in" | "out"),
        ``prior_team_id`` (Utf8, the season S-1 team), ``talent_points``
        (Float64; the 0-star default when the player has no recruit rating).
        Zero-row (typed) when rosters are unavailable.

    Example:
        Quick start::

            from sportsdataverse.cfb import cfb_transfer_moves
            moves = cfb_transfer_moves(2024)
            moves.filter(pl.col("direction") == "in").group_by("team_id").len()

    See Also:
        * `recruitR`_ -- the R companion for CFB recruiting data.

    .. _recruitR: https://github.com/sportsdataverse/recruitR
    """
    season_list = [seasons] if isinstance(seasons, int) else list(seasons)
    need = sorted({s for x in season_list for s in (x - 1, x)})
    rosters = _load_roster_keys(need)
    if rosters.height == 0:
        empty = pl.DataFrame(schema=_MOVES_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty
    prev = rosters.with_columns((pl.col("season") + 1).alias("season")).rename({"team_id": "prior_team_id"})
    assert rosters.schema["player_id"] == prev.schema["player_id"]
    joined = rosters.join(prev, on=["season", "player_id"], how="inner").filter(
        (pl.col("season").is_in(season_list)) & (pl.col("team_id") != pl.col("prior_team_id"))
    )
    consts = get_constants(division)
    default_points = consts.star_points.get(0, 0.0)
    talent = _talent_points_lookup(list(range(min(season_list) - 5, max(season_list) + 1)), division)
    moved = (
        joined.with_columns(pl.col("player_name").str.to_lowercase().str.strip_chars().alias("_name"))
        .join(talent, on="_name", how="left")
        .with_columns(pl.col("talent_points").fill_null(default_points))
        .drop("_name")
    )
    incoming = moved.select(
        "season", "team_id", "player_id", pl.lit("in").alias("direction"), "prior_team_id", "talent_points"
    )
    outgoing = moved.select(
        "season",
        pl.col("prior_team_id").alias("team_id"),
        "player_id",
        pl.lit("out").alias("direction"),
        "prior_team_id",
        "talent_points",
    )
    out = pl.concat([incoming, outgoing]).sort("season", "team_id", "direction", "player_id")
    return out.to_pandas() if return_as_pandas else out


def _net_transfer_talent(seasons: list[int], division: str) -> pl.DataFrame:
    """Per (season, team_id): incoming minus outgoing transfer talent points."""
    moves = cfb_transfer_moves(seasons, division=division)
    assert isinstance(moves, pl.DataFrame)
    if moves.height == 0:
        return pl.DataFrame(schema={"season": pl.Int64, "team_id": pl.Utf8, "net_transfer_talent": pl.Float64})
    signed = moves.with_columns(
        pl.when(pl.col("direction") == "in")
        .then(pl.col("talent_points"))
        .otherwise(-pl.col("talent_points"))
        .alias("signed_points")
    )
    return signed.group_by("season", "team_id").agg(pl.col("signed_points").sum().alias("net_transfer_talent"))


def _realized_win_deltas(seasons: list[int]) -> pl.DataFrame:
    """Per (season, team_id): wins_S - wins_{S-1} from the schedule loader."""
    sched = load_cfb_schedule(sorted({s for x in seasons for s in (x - 1, x)}))
    assert isinstance(sched, pl.DataFrame)
    if sched.height == 0:
        return pl.DataFrame(schema={"season": pl.Int64, "team_id": pl.Utf8, "win_delta": pl.Float64})
    done = sched.filter(pl.col("home_points").is_not_null() & pl.col("away_points").is_not_null())
    home = done.select(
        pl.col("season").cast(pl.Int64),
        pl.col("home_id").cast(pl.Int64).cast(pl.Utf8).alias("team_id"),
        (pl.col("home_points") > pl.col("away_points")).cast(pl.Int64).alias("win"),
    )
    away = done.select(
        pl.col("season").cast(pl.Int64),
        pl.col("away_id").cast(pl.Int64).cast(pl.Utf8).alias("team_id"),
        (pl.col("away_points") > pl.col("home_points")).cast(pl.Int64).alias("win"),
    )
    wins = pl.concat([home, away]).group_by("season", "team_id").agg(pl.col("win").sum().alias("wins"))
    prior = wins.with_columns((pl.col("season") + 1).alias("season")).rename({"wins": "prior_wins"})
    return (
        wins.join(prior, on=["season", "team_id"], how="inner")
        .with_columns((pl.col("wins") - pl.col("prior_wins")).cast(pl.Float64).alias("win_delta"))
        .select("season", "team_id", "win_delta")
    )


def cfb_transfer_impact(
    target_season: int | list[int],
    *,
    division: str = "fbs",
    alpha: float = 1.0,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Net transfer talent and its projected win-total impact per team-season.

    ``pred_win_delta`` comes from an on-demand ridge of realized win deltas on
    ``net_transfer_talent`` fitted over strictly-prior seasons (the as-of
    boundary is enforced internally per target season).

    Args:
        target_season: Season (or list) to score.
        division: Division slug for the star-points constants.
        alpha: Ridge L2 penalty.
        return_as_pandas: If True, return a pandas DataFrame; otherwise polars.

    Returns:
        Per (season, team_id): ``net_transfer_talent`` (Float64),
        ``pred_win_delta`` (Float64). Zero-row (typed) when no data.

    Example:
        Quick start::

            from sportsdataverse.cfb import cfb_transfer_impact
            imp = cfb_transfer_impact(2024)
            imp.sort("net_transfer_talent", descending=True).head(10)

    See Also:
        * `recruitR`_ -- the R companion for CFB recruiting data.

    .. _recruitR: https://github.com/sportsdataverse/recruitR
    """
    targets = [target_season] if isinstance(target_season, int) else list(target_season)
    history = list(range(min(targets) - 5, max(targets) + 1))
    net = _net_transfer_talent(history, division)
    deltas = _realized_win_deltas(history)
    empty = pl.DataFrame(schema=_IMPACT_SCHEMA)
    if net.height == 0:
        return empty.to_pandas() if return_as_pandas else empty
    assert net.schema["team_id"] == deltas.schema["team_id"] == pl.Utf8
    labeled = net.join(deltas, on=["season", "team_id"], how="inner")
    outs: list[pl.DataFrame] = []
    for season in targets:
        train = labeled.filter(pl.col("season") < season)  # the as-of boundary
        score = net.filter(pl.col("season") == season)
        if train.height == 0 or score.height == 0:
            continue
        icept, coef = fit_ridge(
            train.select("net_transfer_talent").to_numpy().astype(float),
            train["win_delta"].to_numpy().astype(float),
            alpha=alpha,
        )
        pred = predict_ridge(icept, coef, score.select("net_transfer_talent").to_numpy().astype(float))
        outs.append(score.with_columns(pl.Series("pred_win_delta", pred, dtype=pl.Float64)))
    if not outs:
        return empty.to_pandas() if return_as_pandas else empty
    out = pl.concat(outs).select(*_IMPACT_SCHEMA).sort("season", "team_id")
    return out.to_pandas() if return_as_pandas else out
