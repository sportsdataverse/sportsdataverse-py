"""Season team rates from possessions (BB-D3): the shared core behind ``{lg}_team_season_rates``.

One row per ``(season, team_id, split)``. Every per-possession number is counted from possession
rows, never estimated from minutes or box totals:

* ``off_rtg`` / ``def_rtg`` -- points scored / allowed per 100 possessions; ``net_rtg`` is the
  difference. ``poss`` counts the team's offensive possessions, ``opp_poss`` its defensive ones.
* ``pace`` -- possessions per 48 minutes (NBA) or 40 (WNBA, college): the mean of ``poss`` and
  ``opp_poss`` scaled by the game clock the possessions used.
* The four factors, for (``efg_pct``, ``tov_pct``, ``orb_pct``, ``ft_rate``) and against
  (``opp_*``), as fractions: eFG% ``(FGM + 0.5 * 3PM) / FGA``, turnovers per possession, offensive
  rebounds over offensive-rebound chances (own OREB plus the defense's DREB), and ``FTA / FGA``.

**Splits** (one definition for all four leagues):

* ``all`` -- every possession.
* ``home`` / ``away`` -- the team's home or away games. Neutral-site games are in neither.
* ``clutch`` -- a possession that starts with :data:`CLUTCH_SECONDS` (5:00) or less left in the
  last regulation period (the 4th quarter; the 2nd half in men's college) or in any overtime, with
  the score within :data:`CLUTCH_MARGIN` (5) points as it starts. This is stats.nba.com's clutch
  window (``ClutchTime="Last 5 Minutes"``, ``PointDiff=5``, the definition ``nba_clutch`` reads), applied
  to possessions. A possession starts when the previous one in the period ends (when the ball changes
  hands), and the score is the running total of every earlier possession in the game.
* ``close`` -- every possession of a close game: a game with at least one clutch possession, i.e.
  within 5 points at some point in the last 5 minutes of regulation or in overtime.

**League context** (BB-D2, :mod:`sportsdataverse._common.league_context`): each rated metric gets
``<m>_rank`` (1 = best, the lower-is-better metrics ranked ascending), the Weibull ``<m>_pct`` and
``<m>_n`` (its sample size). Ranks are computed within ``(season, split)`` among teams that played at
least half the most games any team played in that split, with a floor of
:data:`~sportsdataverse._common.league_context.MIN_COHORT_TEAMS` teams. :func:`team_season_rates_league_averages`
produces the league-average rows, whose ``n`` is the cohort count.

The league adapters (``sportsdataverse.nba.nba_team_season_rates``,
``sportsdataverse.mbb.mbb_team_season_rates``) turn each source's possession frame into
:data:`RATE_ROWS_SCHEMA` and call :func:`team_season_rates`.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse._common.league_context import (
    LEAGUE_AVERAGES_SCHEMA,
    MIN_COHORT_TEAMS,
    attach_ranks,
    attach_sample_sizes,
    league_averages,
)

__all__ = [
    "CLUTCH_MARGIN",
    "CLUTCH_SECONDS",
    "COUNT_ROWS_SCHEMA",
    "RATE_ROWS_SCHEMA",
    "SPLITS",
    "TEAM_SEASON_RATES_SCHEMA",
    "team_season_rates",
    "team_season_rates_league_averages",
]

#: The clutch window: the last five minutes of the final regulation period or any overtime...
CLUTCH_SECONDS = 300.0
#: ... with the score within this many points.
CLUTCH_MARGIN = 5

SPLITS: tuple[str, ...] = ("all", "home", "away", "close", "clutch")

#: The four-factor counts a possession or box row carries; null when the source has none.
_COUNTS = ("fga", "fgm", "fg3m", "fta", "oreb", "dreb", "tov", "ff_poss")

#: The canonical possession frame the league adapters build. One row per possession.
#: ``late`` marks the last regulation period or an overtime. ``start_clock`` / ``end_clock`` are
#: the seconds left in the period when the possession starts (the previous possession's end) and
#: ends. ``dreb`` is the defense's defensive rebounds in the possession; ``ff_poss`` is the
#: turnover-rate denominator (1 per possession when the counts come from the possession).
RATE_ROWS_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "game_id": pl.Utf8(),
    "period": pl.Int64(),
    "possession_number": pl.Int64(),
    "late": pl.Boolean(),
    "start_clock": pl.Float64(),
    "end_clock": pl.Float64(),
    "offense_team_id": pl.Int64(),
    "defense_team_id": pl.Int64(),
    "offense_is_home": pl.Boolean(),
    "neutral_site": pl.Boolean(),
    "points": pl.Int64(),
    **{c: pl.Float64() for c in _COUNTS},
}

#: Game-level four-factor counts, one row per (game, offense team), for a source whose possessions
#: carry no event counts (college). They feed the game-level splits; ``clutch`` gets none.
COUNT_ROWS_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "game_id": pl.Utf8(),
    "offense_team_id": pl.Int64(),
    "defense_team_id": pl.Int64(),
    "offense_is_home": pl.Boolean(),
    **{c: pl.Float64() for c in _COUNTS},
}

#: Rated metric -> (sample-size expression, lower is better).
_RATED: dict[str, tuple[pl.Expr, bool]] = {
    "off_rtg": (pl.col("poss"), False),
    "def_rtg": (pl.col("opp_poss"), True),
    "net_rtg": (pl.col("poss") + pl.col("opp_poss"), False),
    "pace": (pl.col("games"), False),
    "efg_pct": (pl.col("o_fga"), False),
    "tov_pct": (pl.col("o_ff_poss"), True),
    "orb_pct": (pl.col("o_oreb") + pl.col("o_dreb"), False),
    "ft_rate": (pl.col("o_fga"), False),
    "opp_efg_pct": (pl.col("d_fga"), True),
    "opp_tov_pct": (pl.col("d_ff_poss"), False),
    "opp_orb_pct": (pl.col("d_oreb") + pl.col("d_dreb"), True),
    "opp_ft_rate": (pl.col("d_fga"), True),
}
_LOWER_IS_BETTER = tuple(m for m, (_, low) in _RATED.items() if low)
#: Who is ranked: at least half the most games any team played in the season's split. It keeps
#: the college non-D1 opponents (a game or two each) out of the cohort.
# ponytail: a share of the max, as nba_team_ratings; an explicit D1 / franchise list if a real
# team ever plays under half the leader's games.
_QUALIFIER = pl.col("games") >= 0.5 * pl.col("games").max().over(["season", "split"])

TEAM_SEASON_RATES_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "team_id": pl.Int64(),
    "split": pl.Utf8(),
    "games": pl.Int64(),
    "poss": pl.Int64(),
    "opp_poss": pl.Int64(),
    "pts": pl.Int64(),
    "opp_pts": pl.Int64(),
    **{m: pl.Float64() for m in _RATED},
    **{f"{m}_rank": pl.Float64() for m in _RATED},
    **{f"{m}_pct": pl.Float64() for m in _RATED},
    **{f"{m}_n": pl.Int64() for m in _RATED},
}


def _with_clutch(rows: pl.DataFrame) -> pl.DataFrame:
    """Add the start-of-possession margin, ``clutch`` and the game-level ``close`` flag."""
    signed = pl.when(pl.col("offense_is_home")).then(pl.col("points")).otherwise(-pl.col("points"))
    return (
        rows.sort(["game_id", "period", "possession_number"])
        .with_columns(signed.fill_null(0).alias("_signed"))
        .with_columns((pl.col("_signed").cum_sum().over("game_id") - pl.col("_signed")).alias("_margin"))
        .with_columns(
            (
                pl.col("late") & (pl.col("start_clock") <= CLUTCH_SECONDS) & (pl.col("_margin").abs() <= CLUTCH_MARGIN)
            ).alias("clutch")
        )
        .with_columns(pl.col("clutch").any().over("game_id").alias("close"))
        .drop("_signed", "_margin")
    )


def _sides(frame: pl.DataFrame, *, n_poss: int) -> pl.DataFrame:
    """One row per (row, team): the offense's view (``o``) and the defense's (``d``)."""
    keep = ["season", "game_id", "home", "away", "close", "clutch", "points", "duration", *_COUNTS]
    home = pl.col("offense_is_home") & ~pl.col("neutral_site").fill_null(False)
    away = ~pl.col("offense_is_home") & ~pl.col("neutral_site").fill_null(False)
    base = frame.with_columns(pl.lit(n_poss, dtype=pl.Int64).alias("n_poss"))
    off = base.with_columns(pl.col("offense_team_id").alias("team_id"), home.alias("home"), away.alias("away"))
    de = base.with_columns(pl.col("defense_team_id").alias("team_id"), away.alias("home"), home.alias("away"))
    return pl.concat(
        [
            off.select("team_id", *keep, "n_poss", pl.lit("o").alias("side")),
            de.select("team_id", *keep, "n_poss", pl.lit("d").alias("side")),
        ]
    ).filter(pl.col("team_id").is_not_null())


def _split_rows(sides: pl.DataFrame) -> pl.DataFrame:
    """Stack ``sides`` once per split it belongs to, tagged with ``split``."""
    masks = {"all": pl.lit(True), "home": pl.col("home"), "away": pl.col("away"), "close": pl.col("close")}
    masks["clutch"] = pl.col("clutch")
    return pl.concat(
        [sides.filter(mask.fill_null(False)).with_columns(pl.lit(name).alias("split")) for name, mask in masks.items()]
    )


def _side_sum(col: str, side: str) -> pl.Expr:
    """Sum of ``col`` over one side's rows; null when no row of that side carries it."""
    v = pl.col(col).filter(pl.col("side") == side)
    return pl.when(v.is_not_null().any()).then(v.sum()).alias(f"{side}_{col}")


def team_season_rates(
    rows: pl.DataFrame,
    *,
    minutes_per_game: int,
    counts: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Season team rates with home/away/close/clutch splits, ranks, percentiles and ``_n``.

    Args:
        rows: The possessions, in :data:`RATE_ROWS_SCHEMA` (extra columns are ignored). A side
            whose team id is null (a college non-D1 opponent) is dropped, but its points still
            count toward the game score that sets the clutch margin.
        minutes_per_game: Regulation minutes, the pace basis: 48 (NBA) or 40 (WNBA, college).
        counts: Game-level four-factor counts in :data:`COUNT_ROWS_SCHEMA` for a source whose
            possessions carry none (college). They replace the possession counts for the
            ``all`` / ``home`` / ``away`` / ``close`` splits; ``clutch`` keeps null four factors.
            Only games present in ``rows`` are used.

    Returns:
        One row per ``(season, team_id, split)`` in :data:`TEAM_SEASON_RATES_SCHEMA`, sorted by
        season, split (``all``, ``home``, ``away``, ``close``, ``clutch``) and team. An empty
        input returns that schema with zero rows.

    Raises:
        KeyError: If ``rows`` or ``counts`` is missing a schema column.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_team_season_rates import nba_team_season_rates
            rates = nba_team_season_rates(2026)
            rates.filter(split="clutch").sort("net_rtg", descending=True).head()
    """
    missing = sorted(set(RATE_ROWS_SCHEMA) - set(rows.columns))
    if missing:
        raise KeyError(f"rows missing columns: {missing}")
    if rows.height == 0:
        return pl.DataFrame(schema=TEAM_SEASON_RATES_SCHEMA)
    rows = _with_clutch(rows.select(list(RATE_ROWS_SCHEMA)).cast(RATE_ROWS_SCHEMA))
    rows = rows.with_columns((pl.col("start_clock") - pl.col("end_clock")).alias("duration"))
    sides = _sides(rows, n_poss=1)
    if counts is not None:
        missing = sorted(set(COUNT_ROWS_SCHEMA) - set(counts.columns))
        if missing:
            raise KeyError(f"counts missing columns: {missing}")
        games = rows.group_by("game_id").agg(pl.col("close").first(), pl.col("neutral_site").first())
        box = counts.select(list(COUNT_ROWS_SCHEMA)).cast(COUNT_ROWS_SCHEMA)
        assert box.schema["game_id"] == games.schema["game_id"], "game_id dtype differs between counts and rows"
        box = box.join(games, on="game_id", how="inner", validate="m:1").with_columns(
            pl.lit(False).alias("clutch"),
            pl.lit(0, dtype=pl.Int64).alias("points"),
            pl.lit(0.0).alias("duration"),
        )
        sides = pl.concat(
            [sides.with_columns(pl.lit(None, dtype=pl.Float64).alias(c) for c in _COUNTS), _sides(box, n_poss=0)]
        )
    agg = (
        _split_rows(sides)
        .group_by(["season", "team_id", "split"])
        .agg(
            pl.col("game_id").filter(pl.col("n_poss") > 0).n_unique().cast(pl.Int64).alias("games"),
            pl.col("n_poss").filter(pl.col("side") == "o").sum().alias("poss"),
            pl.col("n_poss").filter(pl.col("side") == "d").sum().alias("opp_poss"),
            pl.col("points").filter(pl.col("side") == "o").sum().alias("pts"),
            pl.col("points").filter(pl.col("side") == "d").sum().alias("opp_pts"),
            # each possession is in both sides' rows, so this is the clock of the team's games
            pl.col("duration").filter(pl.col("n_poss") > 0).sum().alias("_seconds"),
            *[_side_sum(c, s) for c in _COUNTS for s in ("o", "d")],
        )
    )
    efg = {s: (pl.col(f"{s}_fgm") + 0.5 * pl.col(f"{s}_fg3m")) / pl.col(f"{s}_fga") for s in ("o", "d")}
    rates = agg.with_columns(
        (100 * pl.col("pts") / pl.col("poss")).alias("off_rtg"),
        (100 * pl.col("opp_pts") / pl.col("opp_poss")).alias("def_rtg"),
        (minutes_per_game * 60 * (pl.col("poss") + pl.col("opp_poss")) / 2 / pl.col("_seconds")).alias("pace"),
        efg["o"].alias("efg_pct"),
        (pl.col("o_tov") / pl.col("o_ff_poss")).alias("tov_pct"),
        (pl.col("o_oreb") / (pl.col("o_oreb") + pl.col("o_dreb"))).alias("orb_pct"),
        (pl.col("o_fta") / pl.col("o_fga")).alias("ft_rate"),
        efg["d"].alias("opp_efg_pct"),
        (pl.col("d_tov") / pl.col("d_ff_poss")).alias("opp_tov_pct"),
        (pl.col("d_oreb") / (pl.col("d_oreb") + pl.col("d_dreb"))).alias("opp_orb_pct"),
        (pl.col("d_fta") / pl.col("d_fga")).alias("opp_ft_rate"),
    ).with_columns((pl.col("off_rtg") - pl.col("def_rtg")).alias("net_rtg"))
    # A zero denominator (no possessions or attempts on a side) is undefined, not a rate.
    rates = rates.with_columns(pl.when(pl.col(m).is_finite()).then(pl.col(m)).otherwise(None).alias(m) for m in _RATED)
    order = pl.col("split").replace_strict({s: i for i, s in enumerate(SPLITS)}, return_dtype=pl.Int64)
    return (
        rates.pipe(
            attach_ranks,
            list(_RATED),
            min_cohort=MIN_COHORT_TEAMS,
            by=("season", "split"),
            lower_is_better=_LOWER_IS_BETTER,
            qualifier=_QUALIFIER,
        )
        .pipe(attach_sample_sizes, {m: e for m, (e, _) in _RATED.items()})
        .sort([pl.col("season"), order, pl.col("team_id")])
        .select(list(TEAM_SEASON_RATES_SCHEMA))
        .cast(TEAM_SEASON_RATES_SCHEMA)
    )


def team_season_rates_league_averages(rates: pl.DataFrame, *, level: str) -> pl.DataFrame:
    """The league-average rows of a team-season-rates table, one block per split.

    The football F1 rows of :func:`~sportsdataverse._common.league_context.league_averages`, over
    the same qualified teams the ranks use, so each row's ``n`` is the cohort count (the ``_pct``
    denominator). ``category`` is ``team_season_rates_<split>``. ``qualifier_min`` is null: the
    gate is relative (half the split's most games), not a fixed threshold.

    Args:
        rates: Output of a ``*_team_season_rates`` function.
        level: The population label (``"nba"``, ``"wnba"``, ``"mbb"``, ``"wbb"``).

    Returns:
        One row per ``(season, split, metric)`` in
        :data:`~sportsdataverse._common.league_context.LEAGUE_AVERAGES_SCHEMA`. An empty input
        returns that schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse._common.team_season_rates import team_season_rates_league_averages
            from sportsdataverse.nba.nba_team_season_rates import nba_team_season_rates
            team_season_rates_league_averages(nba_team_season_rates(2026), level="nba")
    """
    frames = [
        league_averages(
            rates.filter(pl.col("split") == s),
            list(_RATED),
            level=level,
            entity="team",
            category=f"team_season_rates_{s}",
            qualifier=_QUALIFIER,
        )
        for s in SPLITS
    ]
    return pl.concat(frames) if frames else pl.DataFrame(schema=LEAGUE_AVERAGES_SCHEMA)
