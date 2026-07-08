"""Empirical-Bayes NFL player-prop projections (model 3 of T4.2).

Projects per-player usage x efficiency from ``load_nfl_player_stats``
week-level rows, shrunk toward position priors by games played
(``rate = (n * player_mean + kappa * prior) / (n + kappa)``), then scaled by
the opponent matchup (from the native ratings) and game script (from the
native expected margin). No market input anywhere in the projection: the
optional propbets ``line`` join feeds ``p_over`` display output only.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.nfl.nfl_prediction_constants import get_prop_constants

__all__ = ["player_usage_efficiency"]

# (output column, per-game usage numerator | None, family kappa attr)
_USAGE_STATS: tuple[tuple[str, str, str], ...] = (
    ("attempts", "attempts", "shrink_pass"),
    ("carries", "carries", "shrink_rush"),
    ("targets", "targets", "shrink_rec"),
)

# (output column, numerator, denominator, prior key, family kappa attr)
_RATE_STATS: tuple[tuple[str, str, str, str, str], ...] = (
    ("ypa", "passing_yards", "attempts", "ypa", "shrink_pass"),
    ("pass_td_rate", "passing_tds", "attempts", "pass_td_rate", "shrink_pass"),
    ("ypc", "rushing_yards", "carries", "ypc", "shrink_rush"),
    ("rush_td_rate", "rushing_tds", "carries", "rush_td_rate", "shrink_rush"),
    ("ypt", "receiving_yards", "targets", "ypt", "shrink_rec"),
    ("rec_td_rate", "receiving_tds", "targets", "rec_td_rate", "shrink_rec"),
)

_USAGE_OUTPUT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "player_id": pl.Utf8,
    "position": pl.Utf8,
    "team_id": pl.Utf8,
    "games": pl.Int64,
    "exp_attempts": pl.Float64,
    "exp_carries": pl.Float64,
    "exp_targets": pl.Float64,
    "ypa": pl.Float64,
    "ypc": pl.Float64,
    "ypt": pl.Float64,
    "pass_td_rate": pl.Float64,
    "rush_td_rate": pl.Float64,
    "rec_td_rate": pl.Float64,
}


def player_usage_efficiency(player_stats: pl.DataFrame, *, as_of_week: int, era: str = "modern") -> pl.DataFrame:
    """Per-player as-of usage + efficiency with empirical-Bayes shrinkage.

    Aggregates one season of week-level player stats over weeks strictly
    before ``as_of_week`` (the leakage boundary), then shrinks every usage
    (per-game attempts / carries / targets) and efficiency (yards + TDs per
    opportunity) stat toward its position prior:
    ``(n * player_value + kappa * prior) / (n + kappa)`` with ``n`` = games
    played and ``kappa`` the stat family's fitted shrinkage.

    Args:
        player_stats: One season of ``load_nfl_player_stats()`` rows
            (columns ``player_id``, ``position``, ``recent_team``, ``week``,
            ``attempts``, ``passing_yards``, ``passing_tds``, ``carries``,
            ``rushing_yards``, ``rushing_tds``, ``targets``,
            ``receiving_yards``, ``receiving_tds``).
        as_of_week: Only weeks ``< as_of_week`` are used.
        era: Constants era key (supplies kappas + position priors).

    Returns:
        pl.DataFrame: One row per ``player_id`` (Utf8) whose position has a
        prior table: ``position`` / ``team_id`` (Utf8, latest team),
        ``games`` (Int64), ``exp_attempts`` / ``exp_carries`` /
        ``exp_targets`` (Float64, shrunk per-game usage), ``ypa`` / ``ypc``
        / ``ypt`` / ``pass_td_rate`` / ``rush_td_rate`` / ``rec_td_rate``
        (Float64, shrunk per-opportunity efficiency). Zero-row,
        correctly-typed on empty input.

    Example:
        Quick start::

            import polars as pl
            import sportsdataverse.nfl as nfl
            stats = nfl.load_nfl_player_stats().filter(pl.col("season") == 2023)
            usage = nfl.player_usage_efficiency(stats, as_of_week=10)
            usage.sort("exp_attempts", descending=True).head()
    """
    cfg = get_prop_constants(era)
    past = player_stats.filter(pl.col("week") < as_of_week)
    if past.height == 0:
        return pl.DataFrame(schema=_USAGE_OUTPUT_SCHEMA)

    agg = (
        past.sort("week")
        .group_by(pl.col("player_id").cast(pl.Utf8))
        .agg(
            pl.col("position").last(),
            pl.col("recent_team").cast(pl.Utf8).last().alias("team_id"),
            pl.col("week").n_unique().cast(pl.Int64).alias("games"),
            *[
                pl.col(c).sum().alias(f"sum_{c}")
                for c in (
                    "attempts",
                    "passing_yards",
                    "passing_tds",
                    "carries",
                    "rushing_yards",
                    "rushing_tds",
                    "targets",
                    "receiving_yards",
                    "receiving_tds",
                )
            ],
        )
        .filter(pl.col("position").is_in(list(cfg.pos_priors.keys())))
    )
    if agg.height == 0:
        return pl.DataFrame(schema=_USAGE_OUTPUT_SCHEMA)

    def _prior(key: str) -> pl.Expr:
        expr = pl.lit(0.0)
        for pos, priors in cfg.pos_priors.items():
            expr = pl.when(pl.col("position") == pos).then(pl.lit(priors.get(key, 0.0))).otherwise(expr)
        return expr.cast(pl.Float64)

    n = pl.col("games").cast(pl.Float64)
    exprs: list[pl.Expr] = []
    for out_col, num, kappa_attr in _USAGE_STATS:
        kappa = float(getattr(cfg, kappa_attr))
        per_game = pl.col(f"sum_{num}") / n
        exprs.append(((n * per_game + kappa * _prior(num)) / (n + kappa)).cast(pl.Float64).alias(f"exp_{out_col}"))
    for out_col, num, den, prior_key, kappa_attr in _RATE_STATS:
        kappa = float(getattr(cfg, kappa_attr))
        raw = (
            pl.when(pl.col(f"sum_{den}") > 0)
            .then(pl.col(f"sum_{num}") / pl.col(f"sum_{den}"))
            .otherwise(_prior(prior_key))
        )
        exprs.append(((n * raw + kappa * _prior(prior_key)) / (n + kappa)).cast(pl.Float64).alias(out_col))

    return agg.with_columns(exprs).select(*_USAGE_OUTPUT_SCHEMA.keys())
