"""Four-factor points (BB-D42): a rating's distance from the league average, split by factor.

The columns :func:`attach_factor_points` adds to ``team_season_rates`` and ``lineup_seasons``, per
100 possessions. On offense, ``off_rtg - lg_rtg`` splits into ``off_efg_pts + off_tov_pts +
off_orb_pts + off_ft_pts``; on defense, ``lg_rtg - def_rtg`` splits into the four ``def_*_pts``
(positive is a defense better than the league's). So the eight parts add up to ``net_rtg``,
exactly: the free-throw part is the remainder of the other three, by construction. ``efg_pts``,
``tov_pts``, ``orb_pts`` and ``ft_pts`` are each factor's offense plus defense part, so those four
add up to ``net_rtg`` too.

The prices are fixed linear weights, the per-game accounting of
:func:`~sportsdataverse._common.team_season_rates.team_game_four_factors` (its docstring states
them), with the league values of the row's season: ``lg_rtg`` (points per 100 possessions),
eFG%, turnovers per possession, ORB%, rebound chances per FGA, and ``chance_pts``, a possession
that does not end in a turnover (``lg_ppp / (1 - lg TOV%)``). Nothing is fitted: every weight is
a ratio of the season's own league counts, so there is no fitted constant to cite. For a
side with possessions ``P`` and its counts (``RC`` = own OREB + the other side's DREB):

* ``efg_pts`` = ``2 * FGM + 3PM - 2 * lg eFG% * FGA + chance_pts * lg ORB% * (RC - lg RC/FGA * FGA)``
* ``tov_pts`` = ``(lg TOV% * P - TOV) * chance_pts``
* ``orb_pts`` = ``(OREB - lg ORB% * RC) * chance_pts``
* ``ft_pts`` = ``(pts - lg_ppp * P)`` less the three above: the free throws' price plus the
  possessions and points the box estimate does not explain. On the pros that remainder is the
  possessions free-throw trips used, measured rather than estimated at 0.44 an attempt; in college
  it also carries the play-by-play points' distance from the box's. Its mean over the league is the
  free throws' own price, as the estimate gap sums to zero there.

each divided by ``P`` and times 100. ``tools/fit_factor_point_weights.py`` checks the choice of
fixed weights against a league-season regression of ``net_rtg`` on the factors.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package.
"""

from __future__ import annotations

import polars as pl

__all__ = ["FACTOR_NET_POINTS", "FACTOR_POINTS", "FACTOR_POINTS_SCHEMA", "attach_factor_points", "league_prices"]

#: The factors, in display order.
FACTORS: tuple[str, ...] = ("efg", "tov", "orb", "ft")
#: The eight part columns: offense then defense.
FACTOR_POINTS: tuple[str, ...] = tuple(f"{side}_{f}_pts" for side in ("off", "def") for f in FACTORS)
#: Each factor's offense plus defense part: four columns that add up to ``net_rtg``.
FACTOR_NET_POINTS: tuple[str, ...] = tuple(f"{f}_pts" for f in FACTORS)
#: The columns :func:`attach_factor_points` adds.
FACTOR_POINTS_SCHEMA: dict[str, pl.DataType] = {
    "lg_rtg": pl.Float64(),
    **dict.fromkeys(FACTOR_POINTS, pl.Float64()),
    **dict.fromkeys(FACTOR_NET_POINTS, pl.Float64()),
}

_PRICED = ("pts", "poss", "fga", "fgm", "fg3m", "oreb", "rc", "tov")


#: Rebound chances (own OREB + the other side's DREB) of the offense and the defense, per frame
#: convention. ``team_season_rates`` counts every event on the possession's offense side (``o_dreb``
#: is the defense's rebounds of the team's misses); ``lineup_seasons`` counts a side's own events
#: (``o_dreb`` is the unit's own defensive rebounds).
CHANCES: dict[str, tuple[pl.Expr, pl.Expr]] = {
    "possession": (pl.col("o_oreb") + pl.col("o_dreb"), pl.col("d_oreb") + pl.col("d_dreb")),
    "side": (pl.col("o_oreb") + pl.col("d_dreb"), pl.col("d_oreb") + pl.col("o_dreb")),
}


def _side(o: str, rc: pl.Expr, pts: str, poss: str) -> dict[str, pl.Expr]:
    """One side's counts under short names: ``o`` the side's prefix, ``rc`` its rebound chances."""
    c = {k: pl.col(f"{o}_{k}") for k in ("fga", "fgm", "fg3m", "oreb", "tov")}
    return {**c, "rc": rc, "pts": pl.col(pts), "poss": pl.col(poss)}


def league_prices(rows: pl.DataFrame, *, convention: str) -> pl.DataFrame:
    """The league values of each season, pooled over every row's offense.

    Args:
        rows: One row per team (or five-man unit) and season, a partition of the league's
            possessions, with ``season``, ``pts``, ``poss`` and the ``o_`` / ``d_`` counts
            ``fga``, ``fgm``, ``fg3m``, ``oreb``, ``dreb``, ``tov``. A row missing a count is left
            out of the pool.
        convention: The frame's count convention, a :data:`CHANCES` key.

    Returns:
        One row per season: ``lg_ppp``, ``lg_efg_pct``, ``lg_tov_pct``, ``lg_orb_pct``,
        ``lg_rc_per_fga`` and ``chance_pts``, null where a denominator is zero.
    """
    side = _side("o", CHANCES[convention][0], "pts", "poss")
    lg = {k: pl.col(f"_lg_{k}") for k in _PRICED}

    def ratio(num: pl.Expr, den: pl.Expr) -> pl.Expr:  # a zero denominator is undefined: null, never NaN
        return pl.when(den > 0).then(num / den)

    return (
        rows.select("season", **{k: side[k] for k in _PRICED})
        .filter(pl.all_horizontal(pl.col(k).is_not_null() for k in _PRICED))
        .group_by("season")
        .agg(pl.col(k).sum().alias(f"_lg_{k}") for k in _PRICED)
        .select(
            "season",
            ratio(lg["pts"], lg["poss"]).alias("lg_ppp"),
            ratio(lg["fgm"] + 0.5 * lg["fg3m"], lg["fga"]).alias("lg_efg_pct"),
            ratio(lg["tov"], lg["poss"]).alias("lg_tov_pct"),
            ratio(lg["oreb"], lg["rc"]).alias("lg_orb_pct"),
            ratio(lg["rc"], lg["fga"]).alias("lg_rc_per_fga"),
        )
        .with_columns((pl.col("lg_ppp") / (1 - pl.col("lg_tov_pct"))).alias("chance_pts"))
    )


def _parts(s: dict[str, pl.Expr]) -> dict[str, pl.Expr]:
    """A side's four parts in points (not yet per 100)."""
    v, lg_orb, fga = pl.col("chance_pts"), pl.col("lg_orb_pct"), s["fga"]
    efg = (
        2 * s["fgm"]
        + s["fg3m"]
        - 2 * pl.col("lg_efg_pct") * fga
        + v * lg_orb * (s["rc"] - pl.col("lg_rc_per_fga") * fga)
    )
    tov = (pl.col("lg_tov_pct") * s["poss"] - s["tov"]) * v
    orb = (s["oreb"] - lg_orb * s["rc"]) * v
    ft = s["pts"] - pl.col("lg_ppp") * s["poss"] - efg - tov - orb
    return {"efg": efg, "tov": tov, "orb": orb, "ft": ft}


def attach_factor_points(frame: pl.DataFrame, prices: pl.DataFrame, *, convention: str) -> pl.DataFrame:
    """``frame`` plus ``lg_rtg`` and the eight :data:`FACTOR_POINTS` columns, per 100 possessions.

    Args:
        frame: Rows with ``season``, ``pts``, ``opp_pts``, ``poss``, ``opp_poss`` and the ``o_`` /
            ``d_`` counts of :func:`league_prices`.
        prices: :func:`league_prices` output; a season without one gets null parts.
        convention: The frame's count convention, a :data:`CHANCES` key.

    Returns:
        ``frame`` with the columns of :data:`FACTOR_POINTS_SCHEMA` appended. A side with no
        possessions or a null count has null parts.
    """
    assert frame.schema["season"] == prices.schema["season"], "season dtype differs"
    per100 = {"off": pl.col("poss"), "def": pl.col("opp_poss")}
    o_rc, d_rc = CHANCES[convention]
    sides = {"off": _parts(_side("o", o_rc, "pts", "poss")), "def": _parts(_side("d", d_rc, "opp_pts", "opp_poss"))}
    sign = {"off": 1, "def": -1}  # a defensive part is the opponents' offense part, turned around
    parts = [
        pl.when(per100[side] > 0).then(sign[side] * 100 * e / per100[side]).alias(f"{side}_{f}_pts")
        for side, exprs in sides.items()
        for f, e in exprs.items()
    ]
    joined = frame.join(prices, on="season", how="left", validate="m:1")
    nets = [(pl.col(f"off_{f}_pts") + pl.col(f"def_{f}_pts")).alias(f"{f}_pts") for f in FACTORS]
    return (
        joined.with_columns((100 * pl.col("lg_ppp")).alias("lg_rtg"), *parts)
        .with_columns(nets)
        .drop(prices.columns[1:])
    )
