"""Player-prop distributions + team pace projection (model ⑥) for the NBA stack.

Per-player rate stats from box logs, projected onto a matchup's expected
minutes and pace, returning a mean and a distribution so ``P(stat > line)`` is
directly computable. Counts (reb/ast/fg3m) use a Negative-Binomial (Poisson
fallback when dispersion ~ mean); points use a Normal.

Rate basis (ponytail): rates are per-MINUTE, not per-possession. The box-log
fixtures carry per-player minutes but not per-game team possessions, so a
per-minute rate times projected minutes times a pace factor
(``exp_poss / avg_pace``) is the pace-aware projection without needing a
possession column the loader doesn't provide. `# ponytail: per-minute x pace
factor captures the pace linkage without a per-game team-possession join.`

Rate shrinkage: in the NBA a player's per-minute rate is shrunk toward the pooled rate of his
minutes-and-role tier (``minutes_pg`` in [0,12), [12,20), [20,28), 28+, split by starter share
>= .5) with ``k`` pseudo-minutes per stat: ``rate = (stat_sum + k * tier_rate) / (min_sum + k)``.
The tier rate pools this season's games before the cut and the whole previous regular season, so a
player's first games lean on his tier and a long sample on himself.

Recent-window mixture: in the NBA a player with more than 10 earlier games also gets a "recent" line
from his last 10 games (rates shrunk toward the same tier prior, his last-10 mean minutes), and the
predictive becomes ``(1 - w) F(season line) + w F(recent line)`` with ``w = logistic(a + b |dmin| / sd)``:
``dmin`` is last-10 minus season mean minutes and ``sd`` his season minutes SD, so the recent line
weighs more when his role has moved (a trade, a promotion, a teammate's injury). It was fitted and
tested on regular-season games; in the playoffs the last-10 window can include playoff games.

Fitted-constant note: the Normal points SD law and the count dispersions are per league
(``pts_sd_a/pts_sd_b/count_dispersion/stat_dispersion`` in
:data:`~sportsdataverse.nba.nba_prediction_constants.LEAGUE_CONSTANTS`), fitted from each
league's box-log residuals by ``tools/models/nba_prediction/fit_player_props.py``; refit there
if the Phase-5 gate floors move. The shrinkage ``k`` (``rate_prior_minutes``) and the extra-stat
dispersions (``stat_dispersion``) are fitted by
``tools/models/nba_prediction/fit_player_rate_prior.py`` (NBA only).
"""

from __future__ import annotations

import datetime as dt
import math
import warnings
from typing import Any, Literal, Union, overload

import numpy as np
import pandas as pd
import polars as pl
import scipy.stats as st

from sportsdataverse._temporal import as_date
from sportsdataverse.errors import AsOfCutWarning
from sportsdataverse.nba.nba_game_predict import expected_possessions
from sportsdataverse.nba.nba_prediction_constants import get_constants

__all__ = [
    "nba_player_props",
    "player_rates",
    "project_player_line",
    "prob_over",
    "prop_distribution",
    "team_pace_projection",
]

_STATS = ("pts", "reb", "ast", "fg3m", "stl", "blk", "tov", "fgm", "ftm")
_COUNT_STATS = ("reb", "ast", "fg3m")  # the shared count_dispersion; stl..ftm use stat_dispersion
_TIER_MINUTES = (12.0, 20.0, 28.0)  # minutes_pg edges of the rate-prior tiers (48-minute games)

_RATES_SCHEMA = {
    "player_id": pl.Utf8,
    "team_id": pl.Utf8,
    "games": pl.Int64,
    "minutes_pg": pl.Float64,
    **{f"{s}_per_min": pl.Float64 for s in _STATS},
}


def _played(logs: pl.DataFrame) -> pl.DataFrame:
    return logs.drop_nulls("minutes").filter(pl.col("minutes") > 0)


def _rate_tier(minutes_pg: pl.Expr, starter_share: pl.Expr) -> pl.Expr:
    """Minutes-and-role tier 0-7: minutes tier x 2 + (starter share >= .5)."""
    lo, mid, hi = _TIER_MINUTES
    m = pl.when(minutes_pg < lo).then(0).when(minutes_pg < mid).then(1).when(minutes_pg < hi).then(2).otherwise(3)
    return (m * 2 + (starter_share >= 0.5).cast(pl.Int32)).cast(pl.Int32).alias("_tier")


def _carried(played: pl.DataFrame, stat: str) -> bool:
    """The frame has the stat on at least one played row (the loader null-fills one the release lacks)."""
    return stat in played.columns and played[stat].null_count() < played.height


def _tier_totals(played: pl.DataFrame, stats: list[str]) -> pl.DataFrame:
    """Each player's totals over his played rows, tiered by his profile in that frame.

    ``_min_<stat>`` is the minutes behind ``_<stat>``: zero when the frame does not carry the
    stat, so a missing column never pulls the pooled rate toward 0.
    """
    carried = {s: _carried(played, s) for s in stats}
    return (
        played.group_by("player_id")
        .agg(
            pl.col("minutes").mean().alias("_mpg"),
            pl.col("starter").cast(pl.Float64).fill_null(0.0).mean().alias("_ss"),
            *[(pl.col(s).sum() if carried[s] else pl.lit(0.0)).alias(f"_{s}") for s in stats],
            *[(pl.col("minutes").sum() if carried[s] else pl.lit(0.0)).alias(f"_min_{s}") for s in stats],
        )
        .with_columns(_rate_tier(pl.col("_mpg"), pl.col("_ss")))
    )


def _join_tier_prior(
    totals: pl.DataFrame, df: pl.DataFrame, prior_logs: pl.DataFrame | None, present: list[str], league_id: str | None
) -> pl.DataFrame:
    """Join each player's season tier and the pooled ``_prior_<stat>`` tier rates onto ``totals``."""
    frames = {"box_logs": df} if prior_logs is None else {"box_logs": df, "prior_logs": _played(prior_logs)}
    for name, frame in frames.items():
        if not _carried(frame, "starter"):  # all-null would tier everyone as bench
            raise ValueError(f"player_rates: rate shrinkage (league_id={league_id!r}) needs a starter column in {name}")
    tiered = {name: _tier_totals(frame, present) for name, frame in frames.items()}
    prior = (
        pl.concat([t.drop("player_id", "_mpg", "_ss") for t in tiered.values()])
        .group_by("_tier")
        .agg(*[(pl.col(f"_{s}").sum() / pl.col(f"_min_{s}").sum()).alias(f"_prior_{s}") for s in present])
    )
    return totals.join(tiered["box_logs"].select("player_id", "_tier"), on="player_id").join(
        prior, on="_tier", how="left"
    )


def player_rates(
    box_logs: pl.DataFrame,
    *,
    prior_logs: pl.DataFrame | None = None,
    league_id: str | None = None,
    recent_games: int | None = None,
) -> pl.DataFrame:
    """Per-player per-minute rate stats from box logs, shrunk toward a tier prior in the NBA.

    Rows with null, zero or negative minutes (DNPs) are dropped before anything
    is counted, so ``games`` counts only games the player logged minutes in.
    The raw rate is total stat / total minutes across those games; ``minutes_pg`` is the
    mean minutes. A traded player gets one row: the rates pool every team's
    games, and ``team_id`` is the team on the player's last kept input row
    (sort ``box_logs`` by date to make that the latest team).

    With a ``league_id`` that has fitted ``rate_prior_minutes`` (the NBA, ``"00"``), each
    rate is ``(stat_sum + k * tier_rate) / (min_sum + k)``: ``k`` pseudo-minutes of the
    pooled rate of the player's minutes-and-role tier (``minutes_pg`` in [0,12), [12,20),
    [20,28), 28+, split by starter share >= .5). The tier rate pools ``box_logs`` and
    ``prior_logs``, each player tiered by his own profile in each frame; pass the previous
    regular season as ``prior_logs`` so a player's first games have a prior.

    Args:
        box_logs: Per-player-per-game frame with ``player_id, team_id, minutes`` and the
            stats ``pts, reb, ast, fg3m``; ``stl, blk, tov, fgm, ftm`` are optional (a stat
            missing from the frame, or null on every played row, gets a null rate). Shrinkage
            also needs ``starter``.
        prior_logs: Earlier games (same columns), pooled into the tier rates only; they
            never enter a player's own totals.
        league_id: ``None`` (default) for raw rates; ``"00"`` / ``"10"`` / ``"20"`` for that
            league's fitted shrinkage. Only the NBA has one, so WNBA and G-League stay raw.
        recent_games: When set, each player's own totals (``games``, ``minutes_pg`` and the
            rates) cover only his last ``recent_games`` played rows by ``game_date``, while his
            tier and the tier rates still come from all of ``box_logs`` (needs ``game_date``).

    Returns:
        One row per player: ``player_id, team_id, games, minutes_pg``, then
        ``pts_per_min, reb_per_min, ast_per_min, fg3m_per_min, stl_per_min, blk_per_min,
        tov_per_min, fgm_per_min, ftm_per_min``; ``games`` is
        Int64. ``player_id`` and ``team_id`` keep the input's dtype (String
        from the :func:`nba_player_props` loader; integer ids come back as
        integers);
        empty input returns the documented schema, with String ids, and zero
        rows.

    Raises:
        ValueError: Shrinkage is on and ``box_logs`` or ``prior_logs`` has no ``starter``
            value on any played row (the tiers split on it); or ``recent_games`` is set and
            ``box_logs`` has no ``game_date``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_player_props import player_rates
            rates = player_rates(box_logs)

        Shrunk toward the tier prior, with last season pooled in::

            rates = player_rates(box_logs, prior_logs=last_season_logs, league_id="00")
    """
    if box_logs.height == 0:
        return pl.DataFrame(schema=_RATES_SCHEMA)
    df = _played(box_logs)
    k = get_constants(league_id).rate_prior_minutes if league_id is not None else {}
    present = [s for s in _STATS if _carried(df, s)]  # an all-null stat gets no rate, not 0
    own = df
    if recent_games is not None:
        if "game_date" not in df.columns:
            raise ValueError("player_rates: recent_games needs a game_date column to order the games")
        sort = ["game_date", "game_id"] if "game_id" in df.columns else ["game_date"]
        own = df.sort(sort).group_by("player_id", maintain_order=True).tail(recent_games)
    totals = own.group_by("player_id").agg(
        pl.col("team_id").last(),
        pl.len().alias("games"),
        pl.col("minutes").mean().alias("minutes_pg"),
        pl.col("minutes").sum().alias("_min"),
        *[pl.col(s).sum().alias(f"_{s}") for s in present],
    )
    if k:
        totals = _join_tier_prior(totals, df, prior_logs, present, league_id)

    def rate(s: str) -> pl.Expr:
        ks = float(k.get(s, 0.0))
        if s not in present:
            return pl.lit(None, dtype=pl.Float64).alias(f"{s}_per_min")
        if ks == 0.0:
            return (pl.col(f"_{s}") / pl.col("_min")).alias(f"{s}_per_min")
        return ((pl.col(f"_{s}") + ks * pl.col(f"_prior_{s}")) / (pl.col("_min") + ks)).alias(f"{s}_per_min")

    return (
        totals.with_columns(*[rate(s) for s in _STATS])
        .select(list(_RATES_SCHEMA))
        .cast({"games": pl.Int64, **{f"{s}_per_min": pl.Float64 for s in _STATS}})
        .sort("player_id")
    )


def project_player_line(rate_row: dict[str, Any], exp_minutes: float, pace_factor: float = 1.0) -> dict[str, float]:
    """Project a player's expected counting line from per-minute rates.

    ``exp_stat = rate_per_min * exp_minutes * pace_factor`` -- counting stats
    scale with both projected minutes and pace.

    Args:
        rate_row: One row of :func:`player_rates` (as a dict).
        exp_minutes: Projected minutes for the game.
        pace_factor: Pace multiplier (``exp_poss / avg_pace``); ``1.0`` for a
            league-average-pace matchup.

    Returns:
        ``{"exp_<stat>": value}`` for each ``<stat>_per_min`` in ``rate_row`` that is not
        null: ``exp_pts, exp_reb, exp_ast, exp_fg3m``, plus ``exp_stl`` ... ``exp_ftm``
        from a :func:`player_rates` row whose input carried those stats.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_player_props import player_rates, project_player_line
            r = player_rates(box_logs).row(0, named=True)
            line = project_player_line(r, exp_minutes=32.0, pace_factor=1.02)
    """
    m = exp_minutes * pace_factor
    return {f"exp_{s}": float(rate_row[f"{s}_per_min"] * m) for s in _STATS if rate_row.get(f"{s}_per_min") is not None}


def prop_distribution(
    exp_value: float,
    stat: str,
    *,
    league_id: str = "00",
    recent_value: float | None = None,
    recent_weight: float | None = None,
) -> tuple[str, dict[str, Any]]:
    """Distribution family + parameters for a projected stat mean.

    Points -> Normal ``(mu, sd)`` with ``sd = a + b*sqrt(mu)``; counts
    (reb/ast/fg3m, and stl/blk/tov/fgm/ftm where the league has their fit) ->
    Negative-Binomial ``(r, p)`` matching mean ``mu`` and variance
    ``dispersion*mu`` (Poisson if dispersion <= 1). With a positive
    ``recent_weight`` it is the two-component mixture
    ``(1 - w) F(exp_value) + w F(recent_value)`` of :func:`nba_player_props`; its
    points components use the mixture's own SD law (``recent_mixture`` in
    :data:`~sportsdataverse.nba.nba_prediction_constants.LEAGUE_CONSTANTS`).

    Args:
        exp_value: Projected mean of the stat (``stat_<stat>_exp``).
        stat: One of ``"pts"``, ``"reb"``, ``"ast"``, ``"fg3m"``, ``"stl"``, ``"blk"``,
            ``"tov"``, ``"fgm"``, ``"ftm"``.
        league_id: ``"00"`` NBA / ``"10"`` WNBA / ``"20"`` G-League; selects that
            league's fitted SD law and dispersion.
        recent_value: The recent-window mean (``stat_<stat>_recent_exp``).
        recent_weight: The mixture weight (``recent_weight``); ``None`` or ``0`` gives the
            single distribution.

    Returns:
        ``(family, params)`` where family is ``"normal"``, ``"nbinom"`` or
        ``"poisson"``; or ``"mixture"`` with params ``{"weights": [1 - w, w],
        "components": [(family, params), (family, params)]}``.

    Raises:
        ValueError: ``stat`` is one of stl/blk/tov/fgm/ftm and the league has no fitted
            dispersion for it (only the NBA has one); or ``recent_weight`` is positive and
            the league has no fitted mixture or ``recent_value`` is missing.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_player_props import prop_distribution
            fam, par = prop_distribution(24.0, "pts")

        A player whose last 10 games run hotter than his season::

            fam, par = prop_distribution(24.0, "pts", recent_value=28.0, recent_weight=0.4)
    """
    c = get_constants(league_id)
    if recent_weight:
        mix = c.recent_mixture
        if not mix or recent_value is None:
            raise ValueError(
                f"prop_distribution: a recent_weight needs a recent_value and a fitted mixture (league_id={league_id!r})"
            )
        law = (mix["pts_sd_a"], mix["pts_sd_b"])
        w = float(recent_weight)
        if not 0.0 <= w <= 1.0:  # also rejects NaN
            raise ValueError(f"prop_distribution: recent_weight must be in [0, 1], got {recent_weight!r}")
        return "mixture", {
            "weights": [1.0 - w, w],
            "components": [_single(v, stat, c, league_id, law) for v in (exp_value, recent_value)],
        }
    return _single(exp_value, stat, c, league_id, (c.pts_sd_a, c.pts_sd_b))


def _single(
    exp_value: float, stat: str, c: Any, league_id: str, pts_law: tuple[float, float]
) -> tuple[str, dict[str, float]]:
    mu = max(float(exp_value), 1e-6)
    if stat == "pts":
        return "normal", {"mu": mu, "sd": pts_law[0] + pts_law[1] * math.sqrt(mu)}
    if stat in _STATS and stat not in _COUNT_STATS:
        if stat not in c.stat_dispersion:
            raise ValueError(f"prop_distribution: league_id={league_id!r} has no fitted dispersion for {stat!r}")
        var = c.stat_dispersion[stat] * mu
    else:
        var = c.count_dispersion * mu
    if var <= mu * 1.0001:  # not over-dispersed -> Poisson
        return "poisson", {"mu": mu}
    # NB parameterization: var = mu + mu^2 / r  => r = mu^2 / (var - mu); p = r / (r + mu)
    r = mu * mu / (var - mu)
    p = r / (r + mu)
    return "nbinom", {"r": r, "p": p}


def prob_over(
    exp_value: float,
    line: float,
    stat: str,
    *,
    league_id: str = "00",
    recent_value: float | None = None,
    recent_weight: float | None = None,
) -> float:
    """Probability a stat finishes strictly above ``line``.

    Args:
        exp_value: Projected mean of the stat.
        line: The prop line.
        stat: A :func:`prop_distribution` stat (``"pts"`` ... ``"ftm"``).
        league_id: ``"00"`` NBA / ``"10"`` WNBA / ``"20"`` G-League (passed to
            :func:`prop_distribution`).
        recent_value: The recent-window mean, for the mixture (see :func:`prop_distribution`).
        recent_weight: The mixture weight; ``None`` or ``0`` prices the single distribution.

    Returns:
        ``P(stat > line)`` in ``[0, 1]``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_player_props import prob_over
            prob_over(24.0, 22.5, "pts")

        Priced as the recent-window mixture (the ``nba_player_props`` columns)::

            prob_over(24.0, 22.5, "pts", recent_value=28.0, recent_weight=0.4)
    """
    fam, par = prop_distribution(
        exp_value, stat, league_id=league_id, recent_value=recent_value, recent_weight=recent_weight
    )
    if fam == "mixture":
        return float(sum(w * _sf(f, p, line) for w, (f, p) in zip(par["weights"], par["components"], strict=True)))
    return _sf(fam, par, line)


def _sf(fam: str, par: dict[str, float], line: float) -> float:
    if fam == "normal":
        return float(1.0 - st.norm.cdf(line, loc=par["mu"], scale=par["sd"]))
    floor_line = int(np.floor(line))
    if fam == "poisson":
        return float(st.poisson.sf(floor_line, par["mu"]))
    return float(st.nbinom.sf(floor_line, par["r"], par["p"]))


def team_pace_projection(
    home_team_id: str, away_team_id: str, ratings: pl.DataFrame, *, league_id: str = "00"
) -> float:
    """Expected possessions for a matchup (Phase-3 :func:`expected_possessions`).

    A team with no row in ``ratings`` (no game before the cut yet: opening night,
    or an expansion team's first game) or with a null or non-finite ``adj_pace``
    is given the league's ``avg_pace``, so the projection stays finite.

    Args:
        home_team_id: Home team id (matched against ``ratings['team_id']``).
        away_team_id: Away team id.
        ratings: One row per team with ``team_id, adj_pace``.
        league_id: ``"00"``/``"10"``/``"20"``.

    Returns:
        Expected possessions for the game.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_player_props import team_pace_projection
            poss = team_pace_projection("1", "2", ratings)
    """
    avg = get_constants(league_id).avg_pace
    pace = dict(zip(ratings["team_id"].to_list(), ratings["adj_pace"].to_list()))

    def _pace(team_id: str) -> float:
        p = pace.get(team_id)
        return float(p) if p is not None and math.isfinite(p) else avg

    return expected_possessions(_pace(home_team_id), _pace(away_team_id), league_id=league_id)


_OPTIONAL_BOX = {
    "steals": "stl",
    "blocks": "blk",
    "turnovers": "tov",
    "field_goals_made": "fgm",
    "free_throws_made": "ftm",
}


def _load_player_logs(season: int, league_id: str) -> pl.DataFrame:
    """Live player box logs (the box loader is stubbed in tests); WNBA via league_id=10.

    The five extra stats, ``starter`` and ``season_type`` come back null when the release
    frame lacks them (an older or trimmed capture), so the four core stats still project.
    """
    if league_id == "10":
        from sportsdataverse.wnba.wnba_loaders import load_wnba_player_boxscore

        box = load_wnba_player_boxscore([season])
    else:
        from sportsdataverse.nba.nba_loaders import load_nba_player_boxscore

        box = load_nba_player_boxscore([season])
    # checked before the select, which would otherwise raise polars' ColumnNotFoundError
    if "game_date" not in box.columns:
        raise ValueError("nba_player_props: box logs need game_date for the as-of cut")
    return box.select(
        pl.col("game_id").cast(pl.Int64, strict=False).cast(pl.Utf8),
        as_date(pl.col("game_date")).alias("game_date"),
        pl.col("athlete_id").cast(pl.Int64, strict=False).cast(pl.Utf8).alias("player_id"),
        pl.col("team_id").cast(pl.Int64, strict=False).cast(pl.Utf8),
        pl.col("minutes").cast(pl.Float64),
        pl.col("points").cast(pl.Float64).alias("pts"),
        pl.col("rebounds").cast(pl.Float64).alias("reb"),
        pl.col("assists").cast(pl.Float64).alias("ast"),
        pl.col("three_point_field_goals_made").cast(pl.Float64).alias("fg3m"),
        *[_optional(box, src, pl.Float64).alias(dst) for src, dst in _OPTIONAL_BOX.items()],
        _optional(box, "starter", pl.Boolean).alias("starter"),
        _optional(box, "season_type", pl.Int64).alias("season_type"),
    )


def _optional(box: pl.DataFrame, name: str, dtype: pl.DataType) -> pl.Expr:
    return pl.col(name).cast(dtype) if name in box.columns else pl.lit(None, dtype=dtype)


def _load_ratings(
    season: int, league_id: str, as_of_date: Union[dt.date, None] = None
) -> pl.DataFrame:  # pragma: no cover - live network
    """Live team ratings for pace, from games strictly before ``as_of_date`` (monkeypatched in tests)."""
    from sportsdataverse.nba.nba_team_ratings import nba_team_ratings

    return nba_team_ratings(season, league_id=league_id, as_of_date=as_of_date)


def _load_game_date(season: int, league_id: str, game_id: str) -> Union[dt.date, None]:  # pragma: no cover - live
    """The game's date from the season schedule (the one ``nba_team_ratings`` reads); ``None`` if absent."""
    from sportsdataverse.nba.nba_team_ratings import _league_loaders, _normalize_schedule

    load_schedule, _ = _league_loaders(league_id)
    sched = _normalize_schedule(load_schedule([season]))
    hit = sched.filter(pl.col("game_id").cast(pl.Int64, strict=False).cast(pl.Utf8) == str(game_id))
    return hit.select(as_date(pl.col("date"))).to_series().min() if hit.height else None


def _prior_season_logs(season: int, league_id: str, as_of_date: dt.date | None) -> pl.DataFrame | None:
    """The previous regular season for the rate prior; ``None`` in a league without shrinkage."""
    if not get_constants(league_id).rate_prior_minutes:
        return None
    prior = _load_player_logs(season - 1, league_id).filter(pl.col("season_type") == 2)
    if as_of_date is not None:
        prior = prior.filter(pl.col("game_date") < as_of_date)
    if prior.height == 0:  # e.g. a release without season_type: never let the prior vanish silently
        raise ValueError(f"nba_player_props: no {season - 1} regular-season box logs for the rate prior")
    return prior


def _with_recent(
    rates: pl.DataFrame, logs: pl.DataFrame, prior: pl.DataFrame | None, league_id: str, pace_factor: float
) -> pl.DataFrame:
    """Add ``stat_<stat>_recent_exp`` and ``recent_weight`` (null where the league has no fitted mixture)."""
    mix = get_constants(league_id).recent_mixture
    if not mix:
        nulls = [*(f"stat_{s}_recent_exp" for s in _STATS), "recent_weight"]
        return rates.with_columns(pl.lit(None, dtype=pl.Float64).alias(c) for c in nulls)
    n = int(mix["games"])
    recent = player_rates(logs, prior_logs=prior, league_id=league_id, recent_games=n).join(
        _played(logs).group_by("player_id").agg(pl.col("minutes").std().alias("_sd")), on="player_id", how="left"
    )
    recent = recent.select(
        "player_id",
        *[(pl.col(f"{s}_per_min") * pl.col("minutes_pg") * pace_factor).alias(f"stat_{s}_recent_exp") for s in _STATS],
        pl.col("minutes_pg").alias("_mpg_recent"),
        "_sd",
    )
    sd = (
        pl.when(pl.col("_sd").is_finite())
        .then(pl.max_horizontal("_sd", pl.lit(mix["sd_floor"])))
        .otherwise(pl.lit(mix["sd_fallback"]))
    )
    w = 1.0 / (1.0 + (-(mix["a"] + mix["b"] * (pl.col("_mpg_recent") - pl.col("minutes_pg")).abs() / sd)).exp())
    return (
        rates.join(recent, on="player_id", how="left")
        .with_columns(pl.when(pl.col("games") > n).then(w).otherwise(0.0).alias("recent_weight"))
        .drop("_mpg_recent", "_sd")
    )


_PROPS_SCHEMA = {
    "player_id": pl.Utf8,
    "team_id": pl.Utf8,
    "stat_pts_exp": pl.Float64,
    "stat_reb_exp": pl.Float64,
    "stat_ast_exp": pl.Float64,
    "stat_fg3m_exp": pl.Float64,
    **{f"stat_{s}_exp": pl.Float64 for s in _STATS[4:]},
    **{f"stat_{s}_recent_exp": pl.Float64 for s in _STATS},
    "recent_weight": pl.Float64,
    "minutes_exp": pl.Float64,
    "pace_proj": pl.Float64,
}
_ON_COURT = 5  # players per side, so a team plays 5 x game_minutes (200 WNBA, 240 NBA)


def _expected_minutes(rates: pl.DataFrame, logs: pl.DataFrame, league_id: str) -> pl.DataFrame:
    """Add ``minutes_exp``: each player's share of their team's logged minutes x the team's game minutes.

    The share is the player's minutes with their current team (``rates['team_id']``) over the
    sum of those minutes for every player in ``rates`` on that team, so ``minutes_exp`` sums to
    exactly ``5 * game_minutes`` per team. A player who sits counts at zero, which is what keeps
    the total right; ``minutes_pg`` (the stat lines' basis) is minutes when the player plays.
    """
    own = (
        logs.drop_nulls("minutes")
        .filter(pl.col("minutes") > 0)
        .group_by("player_id", "team_id")
        .agg(pl.col("minutes").sum().alias("_own_min"))
    )
    team_minutes = float(_ON_COURT * get_constants(league_id).game_minutes)
    return (
        rates.join(own, on=["player_id", "team_id"], how="left")
        .with_columns(pl.col("_own_min").fill_null(0.0))
        .with_columns(
            (pl.col("_own_min") / pl.col("_own_min").sum().over("team_id") * team_minutes).alias("minutes_exp")
        )
        .drop("_own_min")
    )


@overload
def nba_player_props(
    season: int,
    game_id: str,
    home_team_id: str,
    away_team_id: str,
    *,
    league_id: str = "00",
    as_of_date: Union[dt.date, None] = None,
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def nba_player_props(
    season: int,
    game_id: str,
    home_team_id: str,
    away_team_id: str,
    *,
    league_id: str = "00",
    as_of_date: Union[dt.date, None] = None,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def nba_player_props(
    season: int,
    game_id: str,
    home_team_id: str,
    away_team_id: str,
    *,
    league_id: str = "00",
    as_of_date: Union[dt.date, None] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Per-player expected prop lines + team pace projection for a matchup.

    Loads the season's player box logs + team ratings, keeps only games played
    strictly before ``as_of_date`` (both inputs), computes per-minute
    :func:`player_rates`, and projects each player's line onto their mean
    minutes and the matchup's pace factor (``exp_poss / avg_pace``). Only the
    two teams in the matchup are returned. NBA rates are shrunk toward a tier prior
    that also pools the previous regular season (one more box-log read).

    Args:
        season: End year of the season (e.g. ``2024``).
        game_id: The game id. When ``as_of_date`` is ``None`` and this game is in
            the season's box logs or schedule, its date becomes the cut, so the
            default call never sees the game's own or any later line.
        home_team_id: Home team id.
        away_team_id: Away team id.
        league_id: ``"00"`` NBA / ``"10"`` WNBA / ``"20"`` G-League.
        as_of_date: Only box lines and ratings games with ``game_date <
            as_of_date`` (strict) are used. ``None`` = the target game's date
            from the box logs, else from the schedule (one extra schedule read,
            only when the game is missing from the logs); a game in neither gets
            no cut and an :class:`~sportsdataverse.errors.AsOfCutWarning`.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per player on either team: ``player_id, team_id``, ``stat_<stat>_exp`` and
        ``stat_<stat>_recent_exp`` for pts, reb, ast, fg3m, stl, blk, tov, fgm, ftm (null for a
        stat the box logs lack), ``recent_weight, minutes_exp, pace_proj``. The lines assume
        the player plays (per-minute rate x mean minutes when playing x pace factor); the recent
        line and weight feed the :func:`prob_over` mixture (weight 0 with <= 10 earlier games;
        both null outside the NBA). ``minutes_exp`` is his share of the team's minutes (a game
        sat counts zero), summing to ``5 * game_minutes`` per team (240 NBA, 200 WNBA). A team
        with no game before the cut has no rows and the league pace. Empty input: that schema.

    Raises:
        ValueError: The box logs carry no ``game_date`` (the as-of cut cannot be applied), or
            (NBA) the previous regular season is empty or has no ``starter`` values.

    Warns:
        AsOfCutWarning: ``as_of_date`` is ``None`` and ``game_id`` is in neither
            the box logs nor the schedule, so no cut was applied.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_player_props import nba_player_props
            props = nba_player_props(2024, "401585828", "2", "6")

        Backtest as of a date::

            import datetime as dt
            props = nba_player_props(2024, "401585828", "2", "6", as_of_date=dt.date(2024, 1, 15))
    """
    logs = _load_player_logs(season, league_id)
    if as_of_date is None:
        own = logs.filter(pl.col("game_id").cast(pl.Utf8) == str(game_id))["game_date"].drop_nulls()
        # A played game can be missing from the box logs (ESPN gap, wrong season): fall back to the schedule.
        as_of_date = own.min() if own.len() else _load_game_date(season, league_id, game_id)
        if as_of_date is None:
            warnings.warn(
                f"nba_player_props: game {game_id} is in neither the box logs nor the schedule; "
                "no as-of cut applied (pass as_of_date to cut)",
                AsOfCutWarning,
                stacklevel=2,
            )
    if as_of_date is not None:
        # Strict `<`: the target game's own line (and every later one) never feeds its projection.
        logs = logs.filter(pl.col("game_date") < as_of_date)
    ratings = _load_ratings(season, league_id, as_of_date=as_of_date)
    prior = _prior_season_logs(season, league_id, as_of_date)
    rates = player_rates(logs, prior_logs=prior, league_id=league_id)
    if rates.height == 0 or ratings.height == 0:
        out = pl.DataFrame(schema=_PROPS_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    pace_proj = team_pace_projection(home_team_id, away_team_id, ratings, league_id=league_id)
    pace_factor = pace_proj / get_constants(league_id).avg_pace
    rates = rates.filter(pl.col("team_id").is_in([home_team_id, away_team_id]))
    out = (
        _expected_minutes(rates, logs, league_id)
        .pipe(_with_recent, logs, prior, league_id, pace_factor)
        .with_columns(
            *[(pl.col(f"{s}_per_min") * pl.col("minutes_pg") * pace_factor).alias(f"stat_{s}_exp") for s in _STATS],
            pl.lit(pace_proj, dtype=pl.Float64).alias("pace_proj"),
        )
        .select(list(_PROPS_SCHEMA))
        .sort("player_id")
    )
    return out.to_pandas() if return_as_pandas else out
