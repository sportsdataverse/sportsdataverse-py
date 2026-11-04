"""Team strength (BB-D19): the shared core behind ``{lg}_team_strength``.

One row per ``(season, team_id)``: adjusted offensive / defensive / net rating, strength of
schedule and strength of victory, next to the ``all`` split of BB-D3's season team rates (raw
ratings, pace and the four factors for and against).

* **Adjusted ratings, NBA / WNBA** (:func:`possession_ridge_ratings`): points per 100 on each
  possession, ridge-regressed on an offense indicator, a defense indicator and a home term
  (``+1`` home offense, ``-1`` away, ``0`` neutral site), one fit per season, through the shared
  :func:`sportsdataverse._common.ratings.opponent_adjusted_ridge`. Only the team indicators are
  penalized, so the intercept is the neutral-court league average and each side's coefficients
  sum to zero. ``adj_off_rtg`` / ``adj_def_rtg`` are the intercept plus the team's offense /
  defense coefficient: points scored / allowed per 100 against an average opponent on a neutral
  court. ``adj_net_rtg`` is their difference. The penalty is :data:`RIDGE_LAMBDA`.
* **Adjusted ratings, college**: the published ``mbb_ratings`` / ``wbb_ratings`` (the
  :mod:`sportsdataverse.mbb.mbb_team_ratings` fixed point), ``adj_o`` / ``adj_d`` / ``adj_em``.
* **Strength of schedule** (``sos``): the mean ``adj_net_rtg`` of the opponents of every game the
  team played, one weight per game (an opponent met four times counts four times). Opponents
  with no rating (college non-D1) are left out; ``sos_n`` counts the games that are in.
* **Strength of victory** (``sov``): the same mean over the games the team won only. ``sov_n``
  counts them. A team with no win has a null ``sov``.

Ranks, percentiles and ``_n`` for the five new metrics come from BB-D2's
:mod:`sportsdataverse._common.league_context`, within the season, among teams that played at
least half the most games any team played (the BB-D3 gate), with a floor of
:data:`~sportsdataverse._common.league_context.MIN_COHORT_TEAMS` teams. ``adj_def_rtg`` ranks low
first; ``sos`` ranks the hardest schedule first and ``sov`` the strongest wins first. The BB-D3
columns keep their own ``all``-split ranks.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse._common.league_context import MIN_COHORT_TEAMS, attach_ranks, attach_sample_sizes
from sportsdataverse._common.ratings import opponent_adjusted_ridge
from sportsdataverse._common.team_season_rates import RATE_ROWS_SCHEMA, TEAM_SEASON_RATES_SCHEMA

__all__ = [
    "GAMES_SCHEMA",
    "RATINGS_SCHEMA",
    "RIDGE_LAMBDA",
    "TEAM_STRENGTH_SCHEMA",
    "possession_ridge_ratings",
    "ridge_design",
    "schedule_strength",
    "team_strength",
]

#: league_id -> the ridge penalty, in possessions of prior weight on each team coefficient.
#: Chosen by season-disjoint, game-grouped 5-fold CV in ``tools/fit_team_strength_ridge.py``
#: (2026-11-03), the lowest pooled out-of-fold MSE of points per 100 on the doubling grid
#: 25..3200. NBA 2022-2026 -> 1600 (pooled MSE 14158.503 vs 14158.596 at 800); each held-out
#: season gets 1600 from the other four, regret <= 0.12 (2023, whose own best is 3200).
#: WNBA 2021-2025 -> 800 (pooled MSE 13398.976 vs 13399.498 at 400); each held-out season gets
#: 800, regret 0 except 2021 (0.46, own best 400). The curve is flat near the optimum: the
#: penalty moves predictions little, but it sets how far a team's rating shrinks to average.
RIDGE_LAMBDA: dict[str, float] = {"00": 1600.0, "10": 800.0}

#: Adjusted team ratings, one row per (season, team).
RATINGS_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "team_id": pl.Int64(),
    "adj_off_rtg": pl.Float64(),
    "adj_def_rtg": pl.Float64(),
    "adj_net_rtg": pl.Float64(),
}

#: Completed games, one row per (game, team): the team, its opponent and whether it won.
GAMES_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "game_id": pl.Utf8(),
    "team_id": pl.Int64(),
    "opp_team_id": pl.Int64(),
    "won": pl.Boolean(),
}

_NEW: dict[str, tuple[pl.Expr, bool]] = {
    "adj_off_rtg": (pl.col("poss"), False),
    "adj_def_rtg": (pl.col("opp_poss"), True),
    "adj_net_rtg": (pl.col("poss") + pl.col("opp_poss"), False),
    "sos": (pl.col("_sos_games"), False),
    "sov": (pl.col("_sov_wins"), False),
}
#: The BB-D3 rates carried over from the ``all`` split, ranks and all.
_RATES = tuple(c.removesuffix("_rank") for c in TEAM_SEASON_RATES_SCHEMA if c.endswith("_rank"))
_METRICS = (*_NEW, *_RATES)
# The BB-D3 gate over the season (its `all` split is the whole season).
_QUALIFIER = pl.col("games") >= 0.5 * pl.col("games").max().over("season")

TEAM_STRENGTH_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "team_id": pl.Int64(),
    "games": pl.Int64(),
    "wins": pl.Int64(),
    "losses": pl.Int64(),
    "poss": pl.Int64(),
    "opp_poss": pl.Int64(),
    "pts": pl.Int64(),
    "opp_pts": pl.Int64(),
    **{m: pl.Float64() for m in _METRICS},
    **{f"{m}_rank": pl.Float64() for m in _METRICS},
    **{f"{m}_pct": pl.Float64() for m in _METRICS},
    **{f"{m}_n": pl.Int64() for m in _METRICS},
}


def ridge_design(rows: pl.DataFrame) -> pl.DataFrame:
    """The ridge's regression frame from the BB-D3 possession frame.

    Args:
        rows: Possessions in :data:`~sportsdataverse._common.team_season_rates.RATE_ROWS_SCHEMA`.

    Returns:
        One row per possession with both team ids: ``season``, ``game_id``, ``offense_id`` /
        ``defense_id`` (text, the ridge's team keys), ``y`` (100 * points) and ``hfa`` (``+1``
        home offense, ``-1`` away offense, ``0`` neutral site).

    Raises:
        ValueError: If a non-neutral possession with both team ids has a null ``offense_is_home``.
    """
    neutral = pl.col("neutral_site").fill_null(False)
    rows = rows.filter(pl.col("offense_team_id").is_not_null() & pl.col("defense_team_id").is_not_null())
    unknown = rows.filter(~neutral & pl.col("offense_is_home").is_null()).height
    if unknown:  # the hfa term would silently read these as away possessions
        raise ValueError(f"ridge_design: {unknown} non-neutral possessions have a null offense_is_home")
    return rows.select(
        pl.col("season").cast(pl.Int64),
        pl.col("game_id").cast(pl.Utf8),
        pl.col("offense_team_id").cast(pl.Utf8).alias("offense_id"),
        pl.col("defense_team_id").cast(pl.Utf8).alias("defense_id"),
        (100.0 * pl.col("points").cast(pl.Float64)).alias("y"),
        pl.when(neutral).then(0.0).when(pl.col("offense_is_home")).then(1.0).otherwise(-1.0).alias("hfa"),
    )


def possession_ridge_ratings(rows: pl.DataFrame, *, lam: float) -> pl.DataFrame:
    """Adjusted offensive / defensive / net ratings from possessions, one ridge fit per season.

    Args:
        rows: Possessions in :data:`~sportsdataverse._common.team_season_rates.RATE_ROWS_SCHEMA`
            (the BB-D3 row mappers' output).
        lam: The penalty on the team coefficients, in possessions (:data:`RIDGE_LAMBDA`).

    Returns:
        One row per (season, team) in :data:`RATINGS_SCHEMA`, plus ``hfa``: the fitted home term,
        the home offense's edge over a neutral court in points per 100 (the away offense is the
        same amount under it). Empty input returns that schema with zero rows.

    Raises:
        KeyError: If ``rows`` is missing a schema column.
    """
    missing = sorted(set(RATE_ROWS_SCHEMA) - set(rows.columns))
    if missing:
        raise KeyError(f"rows missing columns: {missing}")
    schema = {**RATINGS_SCHEMA, "hfa": pl.Float64()}
    design = ridge_design(rows)
    frames = []
    for (season,), sub in design.group_by("season", maintain_order=True):
        coef, intercept, hfa = opponent_adjusted_ridge(
            sub,
            off_col="offense_id",
            def_col="defense_id",
            home_col="offense_id",
            resp_col="y",
            lam=lam,
            hfa_col="hfa",
        )
        frames.append(
            coef.select(
                pl.lit(season, dtype=pl.Int64).alias("season"),
                pl.col("team_id").cast(pl.Int64),
                (intercept + pl.col("off_coef")).alias("adj_off_rtg"),
                (intercept + pl.col("def_coef")).alias("adj_def_rtg"),
                (pl.col("off_coef") - pl.col("def_coef")).alias("adj_net_rtg"),
                pl.lit(hfa, dtype=pl.Float64).alias("hfa"),
            )
        )
    if not frames:
        return pl.DataFrame(schema=schema)
    return pl.concat(frames).sort("season", "team_id").cast(schema)


def schedule_strength(games: pl.DataFrame, ratings: pl.DataFrame) -> pl.DataFrame:
    """Wins, losses, strength of schedule and strength of victory from completed games.

    Args:
        games: Completed games in :data:`GAMES_SCHEMA`, one row per (game, team).
        ratings: Adjusted ratings with ``season``, ``team_id`` and ``adj_net_rtg``.

    Returns:
        One row per (season, team): ``wins``, ``losses``, ``sos`` (the mean opponent
        ``adj_net_rtg`` over the team's games, one weight per game), ``sov`` (the same over its
        wins), and ``_sos_games`` / ``_sov_wins`` (the games and wins against a rated opponent).

    Raises:
        TypeError: If the season or team-id dtypes of ``games`` and ``ratings`` differ.
    """
    opp = ratings.select("season", pl.col("team_id").alias("opp_team_id"), pl.col("adj_net_rtg").alias("_opp"))
    for key in ("season", "opp_team_id"):
        if games.schema[key] != opp.schema[key]:
            raise TypeError(f"{key} dtype differs: games {games.schema[key]}, ratings {opp.schema[key]}")
    won = pl.col("won")
    return (
        games.join(opp, on=["season", "opp_team_id"], how="left", validate="m:1")
        .group_by("season", "team_id")
        .agg(
            won.sum().cast(pl.Int64).alias("wins"),
            (~won).sum().cast(pl.Int64).alias("losses"),
            pl.col("_opp").mean().alias("sos"),
            pl.col("_opp").filter(won).mean().alias("sov"),
            pl.col("_opp").count().cast(pl.Int64).alias("_sos_games"),
            pl.col("_opp").filter(won).count().cast(pl.Int64).alias("_sov_wins"),
        )
    )


def team_strength(rates: pl.DataFrame, ratings: pl.DataFrame, games: pl.DataFrame) -> pl.DataFrame:
    """The team-strength table: BB-D3's ``all`` split plus adjusted ratings, SOS and SOV.

    Args:
        rates: A ``*_team_season_rates`` frame (:data:`TEAM_SEASON_RATES_SCHEMA`); only the
            ``all`` split is used.
        ratings: Adjusted ratings in :data:`RATINGS_SCHEMA` (extra columns are ignored).
        games: Completed games in :data:`GAMES_SCHEMA`.

    Returns:
        One row per (season, team) of ``rates`` in :data:`TEAM_STRENGTH_SCHEMA`, sorted by season
        and team. A team with no rating has null adjusted ratings, and no ranks for them; a team
        with no completed game has null ``wins`` / ``losses`` / ``sos`` / ``sov``.

    Raises:
        TypeError: If a join key's dtype differs between the inputs.
    """
    base = rates.filter(pl.col("split") == "all").drop("split")
    rated = ratings.select(list(RATINGS_SCHEMA))
    record = schedule_strength(games.select(list(GAMES_SCHEMA)), rated)
    for name, frame in (("ratings", rated), ("games", record)):
        for key in ("season", "team_id"):
            if base.schema[key] != frame.schema[key]:
                raise TypeError(f"{key} dtype differs: rates {base.schema[key]}, {name} {frame.schema[key]}")
    out = base.join(rated, on=["season", "team_id"], how="left", validate="1:1").join(
        record, on=["season", "team_id"], how="left", validate="1:1"
    )
    return (
        out.pipe(
            attach_ranks,
            list(_NEW),
            min_cohort=MIN_COHORT_TEAMS,
            by=("season",),
            lower_is_better=tuple(m for m, (_, low) in _NEW.items() if low),
            qualifier=_QUALIFIER,
        )
        .pipe(attach_sample_sizes, {m: e.fill_null(0) for m, (e, _) in _NEW.items()})
        .sort("season", "team_id")
        .select(list(TEAM_STRENGTH_SCHEMA))
        .cast(TEAM_STRENGTH_SCHEMA)
    )
