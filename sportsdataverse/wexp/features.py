"""Walk-forward team features: strength of schedule / record, carry-forward weights.

Everything here is computed AS OF a week: a row keyed
``(season, as_of_week, team_id)`` uses only games played strictly before
that week and ratings from that week's vintage, so the output registers
directly on :class:`~sportsdataverse.wexp.store.VintageStore` under the
EXCLUSIVE convention.

Two families:

- :func:`sos_sor_vintages` — strength of schedule (faced and remaining)
  and strength of record, the latter as *actual wins minus the wins a
  league-average team would be expected to take from the same slate*.
- :func:`carry_forward_weights` — how much of last season's team metrics
  should still be believed, given returning production and continuity
  (QB / head coach), decayed to zero once the current season has enough
  evidence of its own.
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import polars as pl

from sportsdataverse.wexp.backtest import normalize_walk_weeks

__all__ = ["carry_forward_weights", "sos_sor_vintages"]


def sos_sor_vintages(
    oracle: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    sigma: float = 15.5,
    hfa: float = 3.0,
) -> pl.DataFrame:
    """As-of strength of schedule + strength of record, per team per week.

    For each ``(season, as_of_week)`` the team's completed games in
    strictly earlier weeks are joined to that vintage's opponent ratings.
    Strength of record uses the standard construction: the wins a
    league-average team (rating 0) would be expected to take from the
    same opponents at the same sites, subtracted from the wins actually
    taken. Positive SOR = the record is better than the schedule alone
    would produce.

    Args:
        oracle: Market-oracle frame (contract columns incl. team ids,
            ``home_win`` and ``neutral_site``).
        ratings: Tidy vintage ratings — ``season``, ``as_of_week``,
            ``team_id`` (Utf8) and ``rating`` in MARGIN points (e.g.
            ``off_coef - def_coef``, or ``adj_net``).
        sigma: Margin SD for the expected-wins normal link.
        hfa: Home-field advantage in margin points (0 at neutral sites).

    Returns:
        One row per ``(season, as_of_week, team_id)``: ``sos_played``
        (mean rating of opponents already faced), ``sos_remaining`` (mean
        rating of opponents still scheduled — the schedule is known in
        advance, the ratings are as-of), ``sor`` (wins above a
        league-average team's expectation), ``games_played``. Weeks with
        no completed games emit no rows.

    Example:
        Quick start::

            from sportsdataverse.wexp.features import sos_sor_vintages
            sos = sos_sor_vintages(oracle, ratings)
            sos.filter(pl.col("as_of_week") == 10).sort("sor", descending=True).head()
    """
    walk = normalize_walk_weeks(oracle)
    # long form: one row per (game, team) with that team's opponent + result
    sides = pl.concat(
        [
            walk.select(
                "season",
                "week",
                team_id="home_team_id",
                opp_id="away_team_id",
                is_home=pl.lit(True),
                neutral_site="neutral_site",
                won=pl.col("home_win").cast(pl.Float64),
            ),
            walk.select(
                "season",
                "week",
                team_id="away_team_id",
                opp_id="home_team_id",
                is_home=pl.lit(False),
                neutral_site="neutral_site",
                won=1.0 - pl.col("home_win").cast(pl.Float64),
            ),
        ]
    )
    played = sides.drop_nulls("won")
    weeks = walk.select("season", "week").unique().sort("season", "week")

    frames: list[pl.DataFrame] = []
    for season, week in weeks.iter_rows():
        vint = ratings.filter((pl.col("season") == season) & (pl.col("as_of_week") == week)).select(
            opp_id="team_id", opp_rating="rating"
        )
        if vint.height == 0:
            continue
        prior = played.filter((pl.col("season") == season) & (pl.col("week") < week)).join(
            vint, on="opp_id", how="inner"
        )
        if prior.height == 0:
            continue
        # expected wins for a rating-0 team facing this slate at these sites
        edge = (
            pl.when(pl.col("neutral_site") == True)
            .then(0.0)
            .otherwise(  # noqa: E712
                pl.when(pl.col("is_home") == True).then(hfa).otherwise(-hfa)  # noqa: E712
            )
        )
        prior = prior.with_columns(exp_win=_norm_cdf((edge - pl.col("opp_rating")) / sigma))
        agg = prior.group_by("team_id").agg(
            sos_played=pl.col("opp_rating").mean(),
            sor=(pl.col("won").sum() - pl.col("exp_win").sum()),
            games_played=pl.len(),
        )
        future = (
            sides.filter((pl.col("season") == season) & (pl.col("week") >= week))
            .join(vint, on="opp_id", how="inner")
            .group_by("team_id")
            .agg(sos_remaining=pl.col("opp_rating").mean())
        )
        frames.append(
            agg.join(future, on="team_id", how="left").with_columns(
                season=pl.lit(season, dtype=pl.Int32),
                as_of_week=pl.lit(week, dtype=pl.Int32),
            )
        )
    if not frames:
        return pl.DataFrame(
            schema={
                "season": pl.Int32,
                "as_of_week": pl.Int32,
                "team_id": pl.Utf8,
                "sos_played": pl.Float64,
                "sos_remaining": pl.Float64,
                "sor": pl.Float64,
                "games_played": pl.Int64,
            }
        )
    return pl.concat(frames, how="vertical").select(
        "season", "as_of_week", "team_id", "sos_played", "sos_remaining", "sor", "games_played"
    )


def _norm_cdf(expr: pl.Expr) -> pl.Expr:
    """Normal CDF via the logistic approximation (polars has no erf).

    Max absolute error ~0.0095 over the whole real line, which is far
    below the noise in any single-season SOS/SOR estimate and keeps the
    computation inside polars instead of round-tripping through numpy
    per week.
    """
    return 1.0 / (1.0 + (-1.702 * expr).exp())


def carry_forward_weights(
    returning: pl.DataFrame,
    *,
    qb_continuity: Optional[pl.DataFrame] = None,
    hc_continuity: Optional[pl.DataFrame] = None,
    base: float = 0.35,
    w_returning: float = 0.35,
    w_qb: float = 0.20,
    w_hc: float = 0.10,
    last_week: int = 4,
) -> pl.DataFrame:
    """How much of last season's metrics to still believe, by team and week.

    The weight blends three pieces of continuity evidence — returning
    production, whether the starting QB is the same, and whether the head
    coach is the same — into a 0-1 credence on last season's team
    metrics. It then RAMPS TO ZERO across the opening weeks: the current
    season carries the estimate on its own merits from week
    ``last_week + 1`` onward.

    Ramp: full weight in week 1, then linearly down so week
    ``last_week + 1`` is exactly 0. With the default ``last_week=4`` the
    multipliers are 1.00 / 0.75 / 0.50 / 0.25 / 0.00 for weeks 1-5.

    Args:
        returning: ``season``, ``team_id``, ``overall_returning`` (0-1).
        qb_continuity: Optional ``season``, ``team_id``, ``qb_continuity``
            (0/1). Missing teams contribute the neutral 0.5.
        hc_continuity: Optional ``season``, ``team_id``, ``hc_continuity``
            (0/1). Missing teams contribute the neutral 0.5. (No free
            historical source exists in-stack today — see the program
            ledger; the term is wired so it can be switched on later.)
        base: Weight floor for a team with zero continuity evidence.
        w_returning: Weight contribution of returning production.
        w_qb: Weight contribution of QB continuity.
        w_hc: Weight contribution of head-coach continuity.
        last_week: Final week that carries ANY prior-season weight.

    Returns:
        One row per ``(season, team_id, week)`` for weeks
        ``1..last_week+1`` with ``carry_weight`` in ``[0, 1]``.

    Example:
        Quick start::

            from sportsdataverse.wexp.features import carry_forward_weights
            w = carry_forward_weights(returning, qb_continuity=qb)
            w.filter((pl.col("week") == 1) & (pl.col("season") == 2019)).head()
    """
    base_frame = returning.select(
        season=pl.col("season").cast(pl.Int32),
        team_id=pl.col("team_id").cast(pl.Int64).cast(pl.Utf8),
        ret=pl.col("overall_returning").cast(pl.Float64).clip(0.0, 1.0),
    )
    for frame, col, name in (
        (qb_continuity, "qb_continuity", "qb"),
        (hc_continuity, "hc_continuity", "hc"),
    ):
        if frame is None:
            base_frame = base_frame.with_columns(pl.lit(0.5).alias(name))
            continue
        tidy = frame.select(
            season=pl.col("season").cast(pl.Int32),
            team_id=pl.col("team_id").cast(pl.Utf8),
            **{name: pl.col(col).cast(pl.Float64)},
        )
        base_frame = base_frame.join(tidy, on=["season", "team_id"], how="left").with_columns(
            pl.col(name).fill_null(0.5)
        )

    scored = base_frame.with_columns(
        credence=(base + w_returning * pl.col("ret") + w_qb * pl.col("qb") + w_hc * pl.col("hc")).clip(0.0, 1.0)
    )
    ramp = pl.DataFrame(
        {
            "week": list(range(1, last_week + 2)),
            "ramp": [max(0.0, 1.0 - (w - 1) / last_week) for w in range(1, last_week + 2)],
        },
        schema={"week": pl.Int32, "ramp": pl.Float64},
    )
    return (
        scored.join(ramp, how="cross")
        .with_columns(carry_weight=pl.col("credence") * pl.col("ramp"))
        .select("season", "team_id", "week", "carry_weight")
        .sort("season", "team_id", "week")
    )


_ = np  # numpy stays imported for downstream matchup work
