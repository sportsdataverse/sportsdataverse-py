"""Rating-based résumé metrics for college football (T2.1 Phase 3).

Complements -- does not replace -- the record-based SOV/SOS in
:mod:`sportsdataverse.cfb.cfb_standings` with rating-based strength of schedule,
quality wins, game control, and (Task 3.2) wins-above-bubble. All metrics are
derived from :func:`cfb_ratings.cfb_ratings` (team strength) joined onto the
played schedule (opponents + margins).

``load_cfb_schedule`` / ``cfb_ratings`` are imported at module scope so tests can
monkeypatch them on this module's namespace (same pattern as ``cfb_ratings``).
"""

from __future__ import annotations

import datetime
from typing import Literal, overload

import numpy as np
import pandas as pd
import polars as pl
from scipy.stats import norm

from sportsdataverse.cfb.cfb_loaders import load_cfb_schedule
from sportsdataverse.cfb.cfb_prediction_constants import PredictConfig, get_constants
from sportsdataverse.cfb.cfb_ratings import cfb_ratings

__all__ = ["cfb_resume"]

_RESUME_SCHEMA: dict[str, pl.PolarsDataType] = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "sos": pl.Float64,
    "sos_rank": pl.Int64,
    "quality_wins": pl.Int64,
    "game_control": pl.Float64,
    "wab": pl.Float64,
}
#: Reference teams for the schedule-strength columns: the k-th best rated FBS team that season (0 = league average).
#: ``bubble`` is the 12th: the last at-large place in a 12-team playoff.
_REFERENCES: dict[str, int] = {"top5": 5, "top25": 25, "bubble": 12, "average": 0}
_SPLITS = ("past", "remaining", "season")
_RESUME_SCHEMA.update({f"sos_{ref}_{split}": pl.Float64 for ref in _REFERENCES for split in _SPLITS})
_RESUME_SCHEMA.update({f"record_difficulty_{ref}": pl.Float64 for ref in _REFERENCES})
_RESUME_SCHEMA["wins_above_bubble"] = pl.Float64
_RESUME_COLUMNS = list(_RESUME_SCHEMA)

# Real load_cfb_schedule column names -> the normalized names the core consumes.
_SCHEDULE_RENAME = {
    "home_id": "home_team_id",
    "away_id": "away_team_id",
    "home_points": "home_score",
    "away_points": "away_score",
}


def _normalize_schedule(schedule: pl.DataFrame) -> pl.DataFrame:
    """Map the real loader's ``home_id`` / ``home_points`` names to the core's, and
    pin both team-id columns to ``Utf8`` (the ratings join-key dtype) at the boundary."""
    ren = {src: dst for src, dst in _SCHEDULE_RENAME.items() if src in schedule.columns}
    schedule = schedule.rename(ren)
    return schedule.with_columns(
        pl.col("home_team_id").cast(pl.Utf8),
        pl.col("away_team_id").cast(pl.Utf8),
    )


def _team_game_rows(schedule: pl.DataFrame, ratings: pl.DataFrame) -> pl.DataFrame:
    """Explode each completed game into two team-perspective rows + join opponent adj_net."""
    played = schedule.filter(pl.col("home_score").is_not_null() & pl.col("away_score").is_not_null())
    home = played.select(
        pl.col("home_team_id").alias("team_id"),
        pl.col("away_team_id").alias("opp_team_id"),
        (pl.col("home_score") - pl.col("away_score")).cast(pl.Float64).alias("team_margin"),
        pl.when(pl.col("neutral_site") == True).then(0).otherwise(1).alias("hfa_sign"),  # noqa: E712
    )
    away = played.select(
        pl.col("away_team_id").alias("team_id"),
        pl.col("home_team_id").alias("opp_team_id"),
        (pl.col("away_score") - pl.col("home_score")).cast(pl.Float64).alias("team_margin"),
        pl.when(pl.col("neutral_site") == True).then(0).otherwise(-1).alias("hfa_sign"),  # noqa: E712
    )
    rows = pl.concat([home, away]).with_columns(team_won=pl.col("team_margin") > 0)
    opp = ratings.select(pl.col("team_id").alias("opp_team_id"), pl.col("adj_net").alias("opp_adj_net"))
    assert rows.schema["opp_team_id"] == opp.schema["opp_team_id"], (
        f"opponent join-key dtype {rows.schema['opp_team_id']} != ratings team_id {opp.schema['opp_team_id']}"
    )
    return rows.join(opp, on="opp_team_id", how="left")


def _poisson_binomial_tail(p: np.ndarray, k: int) -> float:
    """P(at least ``k`` successes) in independent trials with success probabilities ``p``: exact, by recursion."""
    dist = np.zeros(len(p) + 1)
    dist[0] = 1.0
    for q in p:
        dist[1:] = dist[1:] * (1.0 - q) + dist[:-1] * q
        dist[0] *= 1.0 - q
    return float(dist[k:].sum())


def _reference_ratings(ratings: pl.DataFrame, schedule: pl.DataFrame) -> dict[str, float | None]:
    """The k-th best rating among the season's rated FBS teams per reference (``None`` when fewer are rated).

    FBS membership comes from the schedule's ``home_division`` / ``away_division`` when it carries them; otherwise
    every rated team counts.
    """
    rated = ratings.filter(pl.col("adj_net").is_not_null())
    if {"home_division", "away_division"} <= set(schedule.columns):
        fbs = pl.concat(
            [
                schedule.filter(pl.col("home_division") == "fbs").select(team_id="home_team_id"),
                schedule.filter(pl.col("away_division") == "fbs").select(team_id="away_team_id"),
            ]
        ).unique()
        rated = rated.join(fbs, on="team_id", how="semi")
    values = sorted(rated["adj_net"].to_list(), reverse=True)
    return {ref: (0.0 if k == 0 else (values[k - 1] if len(values) >= k else None)) for ref, k in _REFERENCES.items()}


def _played(schedule: pl.DataFrame, as_of_date: datetime.date | None) -> pl.Expr:
    """True on completed games, before ``as_of_date`` when it is set (the calendar day, as ``cfb_ratings`` cuts it)."""
    played = pl.col("home_score").is_not_null() & pl.col("away_score").is_not_null()
    if as_of_date is None:
        return played
    date_col = next((c for c in ("date", "start_date") if c in schedule.columns), None)
    if date_col is None:
        raise ValueError("cfb_resume: as_of_date needs a 'date' or 'start_date' column in the schedule")
    day = pl.col(date_col)
    day = day.str.slice(0, 10).str.to_date() if schedule.schema[date_col] == pl.Utf8 else day.cast(pl.Date)
    return played & (day < pl.lit(as_of_date))


def _reference_rows(
    ratings: pl.DataFrame,
    schedule: pl.DataFrame,
    config: PredictConfig,
    as_of_date: datetime.date | None,
    refs: dict[str, float | None],
) -> pl.DataFrame:
    """One row per (team, scheduled game) with ``played``, ``won`` and each reference team's win probability."""
    games = schedule.with_columns(_played=_played(schedule, as_of_date).fill_null(False))
    neutral = pl.col("neutral_site") == True
    sides = [
        games.select(
            team_id=pl.col(f"{team}_team_id"),
            opp_team_id=pl.col(f"{opp}_team_id"),
            h=pl.when(neutral).then(0).otherwise(sign).cast(pl.Float64),
            played="_played",
            won=(pl.col(f"{team}_score") > pl.col(f"{opp}_score")).fill_null(False),
        )
        for team, opp, sign in (("home", "away", 1), ("away", "home", -1))
    ]
    rows = pl.concat(sides).filter(pl.col("team_id").is_not_null())
    opp = ratings.select(opp_team_id="team_id", opp_r="adj_net")
    assert rows.schema["opp_team_id"] == opp.schema["opp_team_id"]
    rows = rows.join(opp, on="opp_team_id", how="left")
    opp_r, h = rows["opp_r"].to_numpy().astype(float), rows["h"].to_numpy()
    for ref, r_ref in refs.items():
        if r_ref is None:  # fewer rated teams than the reference needs: every column for it is null
            rows = rows.with_columns(pl.lit(None, dtype=pl.Float64).alias(f"_p_{ref}"))
            continue
        margin = config.net_points_scale * (r_ref - opp_r) + h * config.hfa_points
        prob = np.where(np.isnan(opp_r), 1.0, norm.cdf(margin / config.margin_sd))
        rows = rows.with_columns(pl.Series(f"_p_{ref}", prob, dtype=pl.Float64))
    return rows


def _record_difficulty(past_rows: pl.DataFrame, refs: dict[str, float | None]) -> pl.DataFrame:
    """Per team and reference: P(the reference team wins at least as many of the past games as the team did)."""
    out: dict[str, list] = {"team_id": [], **{f"record_difficulty_{ref}": [] for ref in _REFERENCES}}
    for (team,), g in past_rows.group_by("team_id"):
        wins = int(g["won"].sum())
        out["team_id"].append(team)
        for ref in _REFERENCES:
            tail = None if refs[ref] is None else _poisson_binomial_tail(g[f"_p_{ref}"].to_numpy(), wins)
            out[f"record_difficulty_{ref}"].append(tail)
    schema = {"team_id": pl.Utf8, **{f"record_difficulty_{ref}": pl.Float64 for ref in _REFERENCES}}
    return pl.DataFrame(out, schema=schema)


def _schedule_strength(
    ratings: pl.DataFrame,
    schedule: pl.DataFrame,
    config: PredictConfig,
    as_of_date: datetime.date | None,
) -> pl.DataFrame:
    """Reference-team schedule strength, record difficulty and wins above bubble, one row per team.

    For a reference team of rating ``r_ref`` against opponent ``j`` at the rated team's site ``h`` (+1 home, -1 away,
    0 neutral): ``P = Phi((net_points_scale * (r_ref - r_j) + h * hfa_points) / margin_sd)``. An unrated opponent
    (FCS) is a win, ``P = 1``. ``past`` games are completed ones before ``as_of_date`` (every completed game when it
    is ``None``); ``remaining`` the rest of the schedule.
    """
    refs = _reference_ratings(ratings, schedule)
    rows = _reference_rows(ratings, schedule, config, as_of_date, refs)
    past = pl.col("played") == True
    aggs: list[pl.Expr] = []
    for ref in _REFERENCES:
        p = pl.col(f"_p_{ref}")
        aggs += [
            p.filter(past).mean().alias(f"sos_{ref}_past"),
            p.filter(~past).mean().alias(f"sos_{ref}_remaining"),
            p.mean().alias(f"sos_{ref}_season"),
        ]
    wab = pl.col("won").filter(past).sum().cast(pl.Float64) - pl.col("_p_bubble").filter(past).sum()
    out = rows.group_by("team_id").agg(*aggs, wins_above_bubble=wab if refs["bubble"] is not None else pl.lit(None))
    out = out.with_columns(pl.col("wins_above_bubble").cast(pl.Float64))
    return out.join(_record_difficulty(rows.filter(past), refs), on="team_id", how="left")


def _resume_core(
    ratings: pl.DataFrame,
    schedule: pl.DataFrame,
    config: PredictConfig,
    season_value: int | None,
    as_of_date: datetime.date | None = None,
) -> pl.DataFrame:
    """Aggregate the per-team résumé metrics from ratings + a normalized schedule."""
    rows = _team_game_rows(schedule, ratings)
    if rows.height == 0:
        return pl.DataFrame(schema=_RESUME_SCHEMA)

    # game_control = mean postgame win-expectancy Phi(team_margin / margin_sd); the same
    # Gaussian as win_prob_from_margin, computed vectorized over the played games.
    control = norm.cdf(rows["team_margin"].to_numpy() / config.margin_sd)

    # WAB: expected wins of a bubble-quality team (bubble_adj_net) playing this schedule.
    # Mirrors predict_margin/win_prob_from_margin, but the HFA is applied on the team's
    # ACTUAL home/away side (hfa_sign in {+1, -1, 0}), so a team's road games correctly
    # deny the bubble team home advantage (the design note's always-home form would
    # over-credit away games). nan where the opponent is unrated -> excluded from WAB.
    opp = rows["opp_adj_net"].to_numpy()
    rated = ~np.isnan(opp)
    bubble_margin = config.net_points_scale * (
        (config.bubble_adj_net - opp) + rows["hfa_sign"].to_numpy() * 2.0 * config.hfa_epa
    )
    bubble_wp = norm.cdf(bubble_margin / config.margin_sd)
    rows = rows.with_columns(
        pl.Series("_control", control, dtype=pl.Float64),
        pl.Series("_bubble_wp", bubble_wp, dtype=pl.Float64),
        pl.Series("_rated", rated, dtype=pl.Boolean),
    )

    agg = (
        rows.group_by("team_id")
        .agg(
            pl.col("opp_adj_net").mean().alias("sos"),
            ((pl.col("team_won") == True) & (pl.col("opp_adj_net") >= config.quality_win_threshold))  # noqa: E712
            .sum()
            .cast(pl.Int64)
            .alias("quality_wins"),
            pl.col("_control").mean().alias("game_control"),
            (
                ((pl.col("team_won") == True) & (pl.col("_rated") == True)).sum().cast(pl.Float64)  # noqa: E712
                - pl.col("_bubble_wp").filter(pl.col("_rated") == True).sum()  # noqa: E712
            ).alias("wab"),
        )
        .with_columns(
            pl.lit(season_value).cast(pl.Int64).alias("season"),
            pl.col("sos").rank(method="dense", descending=True).cast(pl.Int64).alias("sos_rank"),
        )
        .join(_schedule_strength(ratings, schedule, config, as_of_date), on="team_id", how="left")
        .select(_RESUME_COLUMNS)
        .sort("sos_rank", "team_id")
    )
    return agg


@overload
def cfb_resume(
    seasons: int | list[int],
    *,
    as_of_date: datetime.date | None = ...,
    era: str = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...
@overload
def cfb_resume(
    seasons: int | list[int],
    *,
    as_of_date: datetime.date | None = ...,
    era: str = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
def cfb_resume(
    seasons: int | list[int],
    *,
    as_of_date: datetime.date | None = None,
    era: str = "modern",
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Rating-based résumé metrics: SoS, quality wins, game control, wins-above-bubble.

    For each team, joins every played opponent to its :func:`cfb_ratings.cfb_ratings`
    strength and rolls the games up into:

    - ``sos`` -- mean opponent ``adj_net`` over played games (rating-based strength
      of schedule; complements the record-based SOV/SOS in ``cfb_standings``).
    - ``quality_wins`` -- count of wins over opponents with ``adj_net`` at or above
      the era ``quality_win_threshold``.
    - ``game_control`` -- mean postgame win expectancy ``Phi(actual_margin /
      margin_sd)``, i.e. how *dominant* the results were, not just win/loss.
    - ``wab`` -- wins above bubble: actual wins minus the expected wins of a
      bubble-quality team (``bubble_adj_net``) playing the same schedule, using the
      Phase-2 predictors with the HFA applied on the team's actual home/away side.
    - reference-team schedule strength, record difficulty and
      ``wins_above_bubble``: the win probability of a fixed reference team (the
      5th, 25th or 12th-best rated FBS team, or an average one) on the team's
      schedule, ``Phi((net_points_scale * (r_ref - r_opp) + h * hfa_points) /
      margin_sd)``. ``as_of_date`` splits completed from remaining games for these
      columns only; the columns above use every completed game. The reference
      ratings come from the ratings passed in (pooled when several seasons are).

    Args:
        seasons: A single season or list of seasons.
        as_of_date: Leakage boundary forwarded to :func:`cfb_ratings.cfb_ratings`
            (ratings use only games before this date). ``None`` uses the full season.
        era: Era key into :data:`cfb_prediction_constants.CFB_CONSTANTS`.
        return_as_pandas: If True, return a pandas DataFrame; otherwise polars.

    Returns:
        One row per team: ``season``, ``team_id`` (Utf8), ``sos``, ``sos_rank``
        (Int64 dense rank, best = 1), ``quality_wins`` (Int64), ``game_control``
        (Float64), ``wab`` (Float64), then the reference-team columns (Float64): for
        each reference ``top5`` / ``top25`` / ``bubble`` (the 5th / 25th / 12th-best
        rated FBS team) / ``average`` (rating 0), ``sos_<ref>_past`` /
        ``_remaining`` / ``_season`` (the reference team's mean win probability over
        completed, unplayed and all games, at the team's site, an FCS opponent a sure
        win) and ``record_difficulty_<ref>`` (the probability it wins at least as many
        of the completed games as the team did), and ``wins_above_bubble`` (wins minus
        the 12th-best team's expected wins). Zero-row (typed) when no games are
        available.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_resume import cfb_resume
            resume = cfb_resume(2023)
            resume.sort("sos_rank").head()

    See Also:
        * `cfbfastR`_ -- the R companion whose résumé/SoS surface this mirrors.

    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    season_list = [seasons] if isinstance(seasons, int) else list(seasons)
    # cfb_ratings takes a RatingsConfig, not an era; era feeds get_constants below.
    ratings = cfb_ratings(seasons, as_of_date=as_of_date)
    schedule = load_cfb_schedule(season_list)
    if ratings.is_empty() or schedule.is_empty():
        empty = pl.DataFrame(schema=_RESUME_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty

    schedule = _normalize_schedule(schedule)
    season_value = season_list[0] if len(season_list) == 1 else None
    out = _resume_core(ratings, schedule, get_constants(era), season_value, as_of_date)
    return out.to_pandas() if return_as_pandas else out
