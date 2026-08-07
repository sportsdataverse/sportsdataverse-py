"""Market oracle construction: per-game vig-removed close (and open where held).

Two pure builders (frame in -> oracle frame out, offline-testable on real
captured fixtures) plus thin loading wrappers. Output contract (both
leagues; one row per game):

- ``league`` Utf8, ``game_id`` Utf8, ``season`` Int32, ``week`` Int32,
  ``season_type`` Utf8, ``home_team`` Utf8, ``away_team`` Utf8,
  ``home_team_id`` / ``away_team_id`` Utf8 (NFL: the nflverse abbr IS the
  canonical id; CFB: ESPN numeric id cast Int64->Utf8) — the join keys for
  continuity/prior feature tables (talent, returning production, QB logs)
- ``home_win`` Int8 (null on ties), ``home_margin`` Float64
- ``spread_close`` Float64 (expected HOME margin: positive = home favored),
  ``total_close`` Float64, ``ml_home_close`` / ``ml_away_close`` Float64
  (American), ``spread_open`` Float64 (null where the source holds no open)
- ``p_close_spread`` / ``p_close_ml`` / ``p_close`` Float64 — vig-removed
  home probabilities; ``p_close`` is the nfelo-style 70/30 logit blend
  where both exist, else whichever side exists.

NFL source: nflverse schedules (closing PFR lines, 1999+; no opens).
CFB source: the ``cfb_line_odds`` per-book archive (close 100%, opens thin
pre-2021) + ESPN schedules for outcomes; consensus semantics ported from
cfbfastR-cfb-data ``spread_backfill.load_consensus_spreads`` (median across
books, negated to home-positive, clipped).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

__all__ = [
    "CFB_MARGIN_SIGMA",
    "NFL_MARGIN_SIGMA",
    "ORACLE_COLUMNS",
    "SPREAD_CLIP",
    "SPREAD_ML_BLEND_WEIGHT",
    "build_cfb_market_oracle",
    "build_nfl_market_oracle",
    "cfb_market_oracle_from_lines",
    "nfl_market_oracle_from_schedule",
]

# Seed sigmas for the spread->prob normal link; tuned later on the harness
# (Axis E2). NFL ~13.45 is the standard margin SD; CFB is wider.
NFL_MARGIN_SIGMA: float = 13.45
CFB_MARGIN_SIGMA: float = 15.5
SPREAD_ML_BLEND_WEIGHT: float = 0.7  # nfelo: spread 70 / moneyline 30, logit space
SPREAD_CLIP: float = 60.0  # cfb_line_odds outlier guard (ported)

# Canonical oracle column order (both leagues).
ORACLE_COLUMNS: list[str] = [
    "league",
    "game_id",
    "season",
    "week",
    "season_type",
    "home_team",
    "away_team",
    "home_team_id",
    "away_team_id",
    "neutral_site",
    "fbs_vs_fbs",
    "home_margin",
    "home_win",
    "spread_close",
    "total_close",
    "ml_home_close",
    "ml_away_close",
    "spread_open",
    "p_close_spread",
    "p_close_ml",
    "p_close",
]


def _p_american(col: pl.Expr) -> pl.Expr:
    """Raw implied probability from an American price expression."""
    return pl.when(col < 0).then(-col / (-col + 100)).otherwise(100 / (col + 100))


def _logit(p: pl.Expr) -> pl.Expr:
    return (p / (1 - p)).log()


def _blend_probs(p_spread: pl.Expr, p_ml: pl.Expr, weight_spread: float) -> pl.Expr:
    """70/30 logit blend where both exist; coalesce otherwise."""
    blended = 1 / (1 + (-(weight_spread * _logit(p_spread) + (1 - weight_spread) * _logit(p_ml))).exp())
    return pl.when(p_spread.is_not_null() & p_ml.is_not_null()).then(blended).otherwise(pl.coalesce(p_spread, p_ml))


def _p_spread_series(frame: pl.DataFrame, sigma: float) -> pl.Series:
    """P(home win) = Phi(spread_close / sigma), null-preserving.

    polars has no normal-CDF expression, so this computes via scipy on the
    materialized column; NaN (from nulls) is converted back to null — polars
    treats NaN and null differently and NaN would poison downstream means.
    """
    from scipy.stats import norm

    vals = frame["spread_close"].cast(pl.Float64).to_numpy()
    return pl.Series("p_close_spread", norm.cdf(vals / sigma)).fill_nan(None)


def nfl_market_oracle_from_schedule(
    schedule: pl.DataFrame,
    *,
    sigma: float = NFL_MARGIN_SIGMA,
    blend_weight: float = SPREAD_ML_BLEND_WEIGHT,
) -> pl.DataFrame:
    """Build the NFL market oracle from an nflverse schedule frame.

    Args:
        schedule: Frame from ``load_nfl_schedule`` (needs game_id, season,
            week, game_type, teams, result, spread_line, total_line,
            home/away_moneyline).
        sigma: Margin SD for the spread->prob normal link.
        blend_weight: Spread weight in the spread/ML logit blend.

    Returns:
        The oracle frame per the module contract (``spread_open`` all null —
        nflverse holds closing lines only).

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_schedule
            from sportsdataverse.wexp.oracle_market import nfl_market_oracle_from_schedule
            oracle = nfl_market_oracle_from_schedule(load_nfl_schedule([2023]))
    """
    raw_h = _p_american(pl.col("ml_home_close"))
    raw_a = _p_american(pl.col("ml_away_close"))
    p_ml = raw_h / (raw_h + raw_a)
    out = schedule.select(
        league=pl.lit("nfl"),
        game_id=pl.col("game_id").cast(pl.Utf8),
        season=pl.col("season").cast(pl.Int32),
        week=pl.col("week").cast(pl.Int32),
        season_type=pl.col("game_type").cast(pl.Utf8),
        home_team=pl.col("home_team").cast(pl.Utf8),
        away_team=pl.col("away_team").cast(pl.Utf8),
        home_team_id=pl.col("home_team").cast(pl.Utf8),
        away_team_id=pl.col("away_team").cast(pl.Utf8),
        neutral_site=(pl.col("location") == "Neutral"),
        fbs_vs_fbs=pl.lit(True),
        home_margin=pl.col("result").cast(pl.Float64),
        home_win=pl.when(pl.col("result") > 0).then(1).when(pl.col("result") < 0).then(0).otherwise(None).cast(pl.Int8),
        spread_close=pl.col("spread_line").cast(pl.Float64),
        total_close=pl.col("total_line").cast(pl.Float64),
        ml_home_close=pl.col("home_moneyline").cast(pl.Float64),
        ml_away_close=pl.col("away_moneyline").cast(pl.Float64),
        spread_open=pl.lit(None, dtype=pl.Float64),
    ).with_columns(p_close_ml=p_ml)
    out = out.with_columns(_p_spread_series(out, sigma))
    return out.with_columns(p_close=_blend_probs(pl.col("p_close_spread"), pl.col("p_close_ml"), blend_weight)).select(
        ORACLE_COLUMNS
    )


def _abbr_to_name(spread: pl.DataFrame) -> pl.DataFrame:
    """Infer odds ``abbr`` -> team name from game_desc co-occurrence (ported).

    The abbr's own team appears in every one of its games, so it is the modal
    name across that abbr's rows; tie-broken by name for determinism.
    """
    parts = spread.with_columns(
        away_name=pl.col("game_desc").str.split_exact("@", 1).struct.field("field_0").str.strip_chars(),
        home_name=pl.col("game_desc").str.split_exact("@", 1).struct.field("field_1").str.strip_chars(),
    ).select("abbr", "away_name", "home_name")
    stacked = pl.concat(
        [
            parts.select("abbr", name=pl.col("away_name")),
            parts.select("abbr", name=pl.col("home_name")),
        ]
    )
    return (
        stacked.group_by("abbr", "name")
        .agg(c=pl.len())
        .group_by("abbr")
        .agg(team_name=pl.col("name").sort_by(["c", "name"], descending=[True, False]).first())
    )


def cfb_market_oracle_from_lines(
    line_odds: pl.DataFrame,
    schedules: pl.DataFrame,
    *,
    sigma: float = CFB_MARGIN_SIGMA,
    blend_weight: float = SPREAD_ML_BLEND_WEIGHT,
) -> pl.DataFrame:
    """Build the CFB market oracle from the cfb_line_odds archive + schedules.

    Consensus semantics ported from cfbfastR-cfb-data
    ``spread_backfill.load_consensus_spreads``: per game the home spread is
    the NEGATED median home-team line across books (home-positive), clipped
    to +/-60; totals and side moneylines are book medians. Opens use the
    same recipe on ``opening_lines`` and stay null where no book held one.

    Args:
        line_odds: The ``cfb_line_odds`` frame (per-side per-book rows).
        schedules: ESPN CFB schedules frame (game_id, season, week,
            season_type, home/away names + scores) for outcomes.
        sigma: Margin SD for the spread->prob normal link.
        blend_weight: Spread weight in the spread/ML logit blend.

    Returns:
        The oracle frame per the module contract.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.cfb import load_cfb_schedule
            from sportsdataverse.wexp.oracle_market import cfb_market_oracle_from_lines
            lines = pl.read_parquet("cfb_line_odds.parquet")
            oracle = cfb_market_oracle_from_lines(lines, load_cfb_schedule(seasons=[2024]))
    """
    odds = line_odds.filter(pl.col("game_id").is_not_null()).with_columns(pl.col("game_id").cast(pl.Int64))
    spread = odds.filter((pl.col("market_type") == "spread") & pl.col("lines").is_not_null())
    a2n = _abbr_to_name(spread)
    home_name = pl.col("game_desc").str.split_exact("@", 1).struct.field("field_1").str.strip_chars()
    is_home = pl.col("team_name") == pl.col("home_name")

    labelled = spread.with_columns(home_name=home_name).join(a2n, on="abbr", how="left").with_columns(is_home=is_home)
    home_spread = (
        labelled.filter(pl.col("is_home") == True)  # noqa: E712
        .group_by("game_id")
        .agg(
            spread_close=(-pl.col("lines").median()).clip(-SPREAD_CLIP, SPREAD_CLIP),
            spread_open=(-pl.col("opening_lines").median()).clip(-SPREAD_CLIP, SPREAD_CLIP),
        )
    )
    totals = (
        odds.filter((pl.col("market_type") == "total") & pl.col("lines").is_not_null())
        .group_by("game_id")
        .agg(total_close=pl.col("lines").median())
    )
    ml = (
        # |price| < 100 is not a valid American quote (0.0 observed in the
        # archive) — drop garbage rows before the median.
        odds.filter(
            (pl.col("market_type") == "money_line") & pl.col("odds").is_not_null() & (pl.col("odds").abs() >= 100)
        )
        .with_columns(home_name=home_name)
        .join(a2n, on="abbr", how="left")
        .with_columns(is_home=is_home)
        .group_by("game_id")
        .agg(
            ml_home_close=pl.col("odds").filter(pl.col("is_home") == True).median(),  # noqa: E712
            ml_away_close=pl.col("odds").filter(pl.col("is_home") == False).median(),  # noqa: E712
        )
    )

    games = schedules.select(
        game_id=pl.col("game_id").cast(pl.Int64),
        season=pl.col("season").cast(pl.Int32),
        week=pl.col("week").cast(pl.Int32),
        season_type=pl.col("season_type").cast(pl.Utf8),
        home_team=pl.col("home_team").cast(pl.Utf8),
        away_team=pl.col("away_team").cast(pl.Utf8),
        home_team_id=pl.col("home_id").cast(pl.Int64).cast(pl.Utf8),
        away_team_id=pl.col("away_id").cast(pl.Int64).cast(pl.Utf8),
        neutral_site=pl.col("neutral_site").cast(pl.Boolean),
        fbs_vs_fbs=((pl.col("home_division") == "fbs") & (pl.col("away_division") == "fbs")),
        home_margin=(pl.col("home_points") - pl.col("away_points")).cast(pl.Float64),
    )
    out = (
        games.join(home_spread, on="game_id", how="inner")
        .join(totals, on="game_id", how="left")
        .join(ml, on="game_id", how="left")
    )
    raw_h = _p_american(pl.col("ml_home_close"))
    raw_a = _p_american(pl.col("ml_away_close"))
    p_ml = raw_h / (raw_h + raw_a)
    out = out.with_columns(
        league=pl.lit("cfb"),
        game_id=pl.col("game_id").cast(pl.Utf8),
        home_win=pl.when(pl.col("home_margin") > 0)
        .then(1)
        .when(pl.col("home_margin") < 0)
        .then(0)
        .otherwise(None)
        .cast(pl.Int8),
        ml_home_close=pl.col("ml_home_close").cast(pl.Float64),
        ml_away_close=pl.col("ml_away_close").cast(pl.Float64),
    ).with_columns(p_close_ml=p_ml)
    out = out.with_columns(_p_spread_series(out, sigma))
    return out.with_columns(p_close=_blend_probs(pl.col("p_close_spread"), pl.col("p_close_ml"), blend_weight)).select(
        ORACLE_COLUMNS
    )


def build_nfl_market_oracle(seasons: list[int], **kwargs: float) -> pl.DataFrame:
    """Load nflverse schedules and build the NFL market oracle.

    Args:
        seasons: Seasons to include.
        **kwargs: Forwarded to :func:`nfl_market_oracle_from_schedule`.

    Returns:
        The oracle frame.

    Example:
        Quick start::

            from sportsdataverse.wexp.oracle_market import build_nfl_market_oracle
            oracle = build_nfl_market_oracle(list(range(2009, 2026)))
    """
    from sportsdataverse.nfl import load_nfl_schedule

    return nfl_market_oracle_from_schedule(load_nfl_schedule(seasons=seasons), **kwargs)


def build_cfb_market_oracle(
    seasons: list[int], *, line_odds_path: str | Path | None = None, **kwargs: float
) -> pl.DataFrame:
    """Load the cfb_line_odds archive + ESPN schedules and build the CFB oracle.

    Args:
        seasons: Seasons to include.
        line_odds_path: Optional local path to ``cfb_line_odds.parquet``;
            by default the published archive is loaded via
            :func:`sportsdataverse.cfb.load_cfb_betting_lines` (the
            cfbfastR-data raw-main asset).
        **kwargs: Forwarded to :func:`cfb_market_oracle_from_lines`.

    Returns:
        The oracle frame.

    Example:
        Quick start::

            from sportsdataverse.wexp.oracle_market import build_cfb_market_oracle
            oracle = build_cfb_market_oracle([2024])
    """
    from sportsdataverse.cfb import load_cfb_schedule
    from sportsdataverse.cfb.cfb_loaders_extra import load_cfb_betting_lines

    lines = load_cfb_betting_lines() if line_odds_path is None else pl.read_parquet(line_odds_path)
    lines = lines.filter(pl.col("season").cast(pl.Int32).is_in(seasons))
    schedules = load_cfb_schedule(seasons=seasons)
    return cfb_market_oracle_from_lines(lines, schedules, **kwargs)
