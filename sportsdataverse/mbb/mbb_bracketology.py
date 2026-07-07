"""Bracketology: projected seeds + at-large bid probability.

Phase 5 of the MBB/WBB prediction & tournament stack. Blends the
opponent-adjusted ratings (``mbb_team_ratings``) with the Quad/WAB résumé
(``mbb_strength_of_schedule``) into a committee-style ``resume_score``,
selects a 68-team field (conference auto-bids + best remaining at-larges),
and assigns NET-style projected seeds.
"""

from __future__ import annotations

import datetime
from typing import Literal, Union, overload

import pandas as pd
import polars as pl

__all__ = [
    "mbb_bracketology",
    "project_bracket",
]

# Committee-style resume blend: power (adj_em_z) and resume quality (WAB) carry
# most of the weight; schedule strength and Quad-1 wins refine the ordering.
# Each non-z input is z-scored within season before weighting. Validated
# against the bracketmatrix consensus (Task 5.3 oracle gate).
_RESUME_WEIGHTS = {"adj_em_z": 0.35, "sos": 0.20, "wab": 0.35, "quad1_w": 0.10}

_FIELD_SIZE = 68
_SEED_CAP = 16

_BRACKET_SCHEMA = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "resume_score": pl.Float64,
    "projected_seed": pl.Int64,
    "at_large_prob": pl.Float64,
    "auto_bid": pl.Boolean,
    "bid": pl.Boolean,
}


def _z(col: str) -> pl.Expr:
    return (pl.col(col) - pl.col(col).mean().over("season")) / (pl.col(col).std().over("season") + 1e-9)


def project_bracket(
    resume: pl.DataFrame,
    auto_bids: set[str],
    *,
    league: str = "mens",
    field_size: int = _FIELD_SIZE,
) -> pl.DataFrame:
    """Select and seed a tournament field from a per-team résumé frame.

    Args:
        resume: One row per (season, team_id) with ``adj_em_z, sos, wab,
            quad1_w`` (the ratings + strength-of-schedule outputs joined).
        auto_bids: ``team_id`` set of conference auto-bid winners (see
            :func:`_conference_auto_bids`); always in the field.
        league: ``"mens"`` or ``"womens"`` (kept for shim parity; the blend is
            league-agnostic).
        field_size: Tournament field size (68).

    Returns:
        One row per input team: ``season, team_id, resume_score,
        projected_seed`` (1-16, capped for the First Four; null outside the
        field), ``at_large_prob`` (logistic in ``resume_score`` centred on the
        selection cutoff -- every selected at-large clears 0.5), ``auto_bid``,
        ``bid`` (exactly ``field_size`` true).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_bracketology import project_bracket
            field = project_bracket(resume, auto_bids)
    """
    score = sum(_RESUME_WEIGHTS[c] * (pl.col(c) if c == "adj_em_z" else _z(c)) for c in _RESUME_WEIGHTS)
    df = (
        resume.with_columns(
            score.cast(pl.Float64).alias("resume_score"),
            pl.col("team_id").is_in(sorted(auto_bids)).alias("auto_bid")
            if auto_bids
            else pl.lit(False).alias("auto_bid"),
        )
        .sort("resume_score", descending=True)
        .with_columns(pl.int_range(pl.len()).over("season").alias("_pos"))
    )

    n_auto = df.filter(pl.col("auto_bid") == True).height  # noqa: E712
    at_large_slots = max(0, field_size - n_auto)
    df = df.with_columns(
        (pl.col("auto_bid") == False).cast(pl.Int64).cum_sum().over("season").alias("_al_rank")  # noqa: E712
    ).with_columns(
        (
            (pl.col("auto_bid") == True)  # noqa: E712
            | ((pl.col("auto_bid") == False) & (pl.col("_al_rank") <= at_large_slots))  # noqa: E712
        ).alias("bid")
    )

    # at-large logistic: centred midway between the last at-large in and the
    # best at-large left out, scaled to a quarter of the score spread
    non_auto = df.filter(pl.col("auto_bid") == False)  # noqa: E712
    last_in = non_auto.filter(pl.col("_al_rank") == at_large_slots)
    first_out = non_auto.filter(pl.col("_al_rank") == at_large_slots + 1)
    if last_in.height and first_out.height:
        cutoff = 0.5 * (last_in["resume_score"][0] + first_out["resume_score"][0])
    else:
        cutoff = float(df["resume_score"].min()) - 1.0
    scale = max(float(df["resume_score"].std() or 1.0) * 0.25, 1e-9)
    df = df.with_columns((1.0 / (1.0 + (-(pl.col("resume_score") - cutoff) / scale).exp())).alias("at_large_prob"))

    field = (
        df.filter(pl.col("bid") == True)  # noqa: E712
        .with_columns(pl.int_range(pl.len()).over("season").alias("_fpos"))
        .with_columns(
            pl.min_horizontal((pl.col("_fpos") // 4) + 1, pl.lit(_SEED_CAP)).cast(pl.Int64).alias("projected_seed")
        )
        .select("team_id", "season", "projected_seed")
    )
    out = (
        df.join(field, on=["season", "team_id"], how="left")
        .select(*_BRACKET_SCHEMA)
        .sort("season", "resume_score", descending=[False, True])
    )
    return out


def _conference_auto_bids(standings: pl.DataFrame) -> set[str]:
    """Conference auto-bid winners from the long-form ``load_mbb_standings``.

    Picks, per conference ``group_id``, the team with the best conference win
    percentage (``vsConf_winPercent`` stat rows) -- the regular-season leader,
    which is the best pre-tournament auto-bid proxy (replaced by the actual
    champion once conference tournaments finish).
    """
    conf = standings.filter(pl.col("stat_name") == "vsConf_winPercent")
    if conf.height == 0:
        conf = standings.filter(pl.col("stat_name") == "winPercent")
    leaders = (
        conf.sort("value", descending=True).group_by("group_id", maintain_order=True).agg(pl.col("team_id").first())
    )
    return set(leaders.get_column("team_id").to_list())


@overload
def mbb_bracketology(
    season: int,
    *,
    as_of_date: datetime.date | None = None,
    league: str = "mens",
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def mbb_bracketology(
    season: int,
    *,
    as_of_date: datetime.date | None = None,
    league: str = "mens",
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def mbb_bracketology(
    season: int,
    *,
    as_of_date: datetime.date | None = None,
    league: str = "mens",
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Projected tournament field for a season from the released ESPN data.

    Builds ratings + résumé (optionally as of a date -- games on or after
    ``as_of_date`` are excluded), resolves conference auto-bids from the
    standings, and selects/seeds the 68-team field via
    :func:`project_bracket`.

    Args:
        season: Season to project (e.g. ``2024``).
        as_of_date: Only use games strictly before this date (Selection-Sunday
            style snapshots); ``None`` uses every completed game.
        league: ``"mens"`` or ``"womens"``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per team -- see :func:`project_bracket`.

    Example:
        Quick start::

            from sportsdataverse.mbb import mbb_bracketology
            field = mbb_bracketology(2024)

        Pipeline next step (one line)::

            field.filter(pl.col("bid") == True).sort("projected_seed")

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
    """
    from sportsdataverse.mbb.mbb_loaders import load_mbb_schedule, load_mbb_standings, load_mbb_team_boxscore  # noqa: PLC0415
    from sportsdataverse.mbb.mbb_prediction_constants import as_of_ratings_split  # noqa: PLC0415
    from sportsdataverse.mbb.mbb_strength_of_schedule import strength_of_schedule  # noqa: PLC0415
    from sportsdataverse.mbb.mbb_team_ratings import (  # noqa: PLC0415
        _normalize_schedule,
        adjust_efficiency,
        raw_game_efficiency,
    )

    results = _normalize_schedule(load_mbb_schedule([season])).filter(
        pl.col("home_score").is_not_null() & pl.col("away_score").is_not_null()
    )
    if as_of_date is not None:
        results = as_of_ratings_split(results, as_of_date)
    eff = raw_game_efficiency(results, load_mbb_team_boxscore([season]))
    ratings = adjust_efficiency(eff).with_columns(
        pl.col("adj_em").rank(method="min", descending=True).over("season").cast(pl.Int64).alias("rank"),
        ((pl.col("adj_em") - pl.col("adj_em").mean().over("season")) / pl.col("adj_em").std().over("season")).alias(
            "adj_em_z"
        ),
    )
    resume = strength_of_schedule(results, ratings, league=league).join(
        ratings.select("season", "team_id", "adj_em_z"), on=["season", "team_id"], how="inner"
    )
    auto = _conference_auto_bids(load_mbb_standings([season]))
    out = project_bracket(resume, auto, league=league)
    return out.to_pandas() if return_as_pandas else out
