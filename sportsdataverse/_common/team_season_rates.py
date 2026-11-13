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
* ``clutch`` -- a possession in play with :data:`CLUTCH_SECONDS` (5:00) or less left in the last
  regulation period (the 4th quarter; the 2nd half in men's college and in the WNBA through 2005) or
  in any overtime, with the score within :data:`CLUTCH_MARGIN` (5) points as it starts. That is a
  possession that starts with 5:00 or less left, or the one live at the 5:00 mark (it starts before
  the mark and ends after it). One that starts before the mark and ends exactly at 5:00 is out; one
  that starts at 5:00 is in, even if it also ends there. This is stats.nba.com's clutch window
  (``ClutchTime="Last 5 Minutes"``, ``PointDiff=5``, the definition ``nba_clutch`` reads), applied to
  possessions. A possession starts when the previous one in the period ends (when the ball changes
  hands), and the score is the running total of every earlier possession in the game.
* ``close`` -- every possession of a close game: a game with at least one clutch possession, i.e.
  within 5 points at some point in the last 5 minutes of regulation or in overtime.

**League context** (BB-D2, :mod:`sportsdataverse._common.league_context`): each rated metric gets
``<m>_rank`` (1 = best, the lower-is-better metrics ranked ascending), the Weibull ``<m>_pct`` and
``<m>_n`` (its sample size: the rate's own denominator). Ranks are computed within ``(season, split)``
among teams that played at least half the most games any team played in that split and, given a
``level``, whose ``<m>_n`` reaches the league's gate in :data:`~sportsdataverse._common.rate_gates.TEAM_MIN_N`
(the sample at which the rate is half signal; a clutch split's few hundred possessions rank nothing),
with a floor of :data:`~sportsdataverse._common.league_context.MIN_COHORT_TEAMS` teams.
:func:`team_season_rates_league_averages` produces the league-average rows, whose ``n`` is the cohort count.
``lg_ft_pct`` is the league's free-throw percentage in the row's season and split, pooled over
every team (all FTM over all FTA, the possession counts' own league), so free throws can be priced
without a second read; null where no count carries free throws (college ``clutch``).

**Four-factor accounting** (GOP Q20): :func:`team_game_four_factors` prices each team-game's four
factors in points against that season's league-average offense; its docstring states the prices.

**Bases** (BB-G8): the scoring numbers also come per game and per 75 possessions, so a board's
basis select never converts. ``plus_minus`` is ``pts - opp_pts``; ``pts_pg`` / ``opp_pts_pg`` /
``plus_minus_pg`` divide those totals by ``games``, and ``pts_per75`` / ``opp_pts_per75`` /
``plus_minus_per75`` are 0.75 times ``off_rtg`` / ``def_rtg`` / ``net_rtg``. Each gets ``_rank``,
``_pct`` and ``_n`` ranked where its rating is (the rating's sample size and gate, ``opp_pts_*``
lower is better); a per-75 rank is its rating's, copied.

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
    attach_per75,
    attach_ranks,
    attach_sample_sizes,
    league_averages,
)
from sportsdataverse._common.rate_gates import team_gates

__all__ = [
    "CLUTCH_MARGIN",
    "CLUTCH_SECONDS",
    "COUNT_ROWS_SCHEMA",
    "FT_POSSESSION_WEIGHT",
    "RATE_ROWS_SCHEMA",
    "SIDE_COUNTS",
    "SPLITS",
    "TEAM_GAME_FOUR_FACTORS_SCHEMA",
    "TEAM_GAME_POSSESSIONS_SCHEMA",
    "TEAM_SEASON_RATES_SCHEMA",
    "rates_from_counts",
    "season_counts",
    "team_game_four_factors",
    "team_game_possessions",
    "team_season_rates",
    "team_season_rates_league_averages",
]

#: The clutch window: the last five minutes of the final regulation period or any overtime, the
#: possession live at the mark included...
CLUTCH_SECONDS = 300.0
#: ... with the score within this many points.
CLUTCH_MARGIN = 5

SPLITS: tuple[str, ...] = ("all", "home", "away", "close", "clutch")

#: The four-factor counts a possession or box row carries; null when the source has none.
#: ``ftm`` prices free throws (``lg_ft_pct`` and :func:`team_game_four_factors`); no rate uses it.
_COUNTS = ("fga", "fgm", "fg3m", "fta", "ftm", "oreb", "dreb", "tov", "ff_poss")

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
#: Per-game scoring -> (its total, the rating whose sample size and gate rank it).
_PER_GAME: dict[str, tuple[str, str]] = {
    "pts_pg": ("pts", "off_rtg"),
    "opp_pts_pg": ("opp_pts", "def_rtg"),
    "plus_minus_pg": ("plus_minus", "net_rtg"),
}
#: Per-75 scoring -> its per-100 rating (:func:`~sportsdataverse._common.league_context.attach_per75`).
_PER_75: dict[str, str] = {"pts_per75": "off_rtg", "opp_pts_per75": "def_rtg", "plus_minus_per75": "net_rtg"}
_BASES = (*_PER_GAME, *_PER_75)


def _sizes(minutes_per_game: int) -> dict[str, pl.Expr]:
    """Each rate's ``_n``. Pace's is the clock its possessions used, in regulation games: a full game
    counts 1 (an overtime a little more), a clutch split's five minutes about 0.1, so a split's
    sample is never its games' count."""
    return {**{m: e for m, (e, _) in _RATED.items()}, "pace": pl.col("seconds") / (60 * minutes_per_game)}


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
    "lg_ft_pct": pl.Float64(),
    "plus_minus": pl.Int64(),
    **{m: pl.Float64() for m in _BASES},
    **{f"{m}_rank": pl.Float64() for m in _BASES},
    **{f"{m}_pct": pl.Float64() for m in _BASES},
    **{f"{m}_n": pl.Int64() for m in _BASES},
}


def _league_ft_pct() -> pl.Expr:
    """The league's free-throw percentage: every team's FTM over every team's FTA (null with none)."""
    fta = pl.col("o_fta").sum()
    return pl.when(fta > 0).then(pl.col("o_ftm").sum() / fta)


def _with_bases(rates: pl.DataFrame, gates: dict[str, float]) -> pl.DataFrame:
    """Add ``plus_minus`` and the per-game and per-75 scoring with their ranks (the module's Bases)."""
    totals = rates.with_columns((pl.col("pts") - pl.col("opp_pts")).alias("plus_minus"))
    pg = totals.with_columns(
        pl.when(pl.col("games") > 0).then(pl.col(total) / pl.col("games")).alias(m)
        for m, (total, _) in _PER_GAME.items()
    ).with_columns(pl.col(f"{rating}_n").alias(f"{m}_n") for m, (_, rating) in _PER_GAME.items())
    ranked = pg.pipe(
        attach_ranks,
        list(_PER_GAME),
        min_cohort=MIN_COHORT_TEAMS,
        by=("season", "split"),
        lower_is_better=["opp_pts_pg"],
        qualifier=_QUALIFIER,
        min_n={m: gates[rating] for m, (_, rating) in _PER_GAME.items() if rating in gates},
    )
    return attach_per75(ranked, _PER_75)


#: The per-game grain of the season table's possession counts (:func:`team_game_possessions`).
TEAM_GAME_POSSESSIONS_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "team_id": pl.Int64(),
    "game_id": pl.Utf8(),
    "split": pl.Utf8(),
    "poss": pl.Int64(),
    "opp_poss": pl.Int64(),
    "pts": pl.Int64(),
    "opp_pts": pl.Int64(),
    "seconds": pl.Float64(),
}


def _with_clutch(rows: pl.DataFrame) -> pl.DataFrame:
    """Add ``start_margin`` (home minus away as the possession starts), ``clutch`` and the game-level ``close``."""
    signed = pl.when(pl.col("offense_is_home")).then(pl.col("points")).otherwise(-pl.col("points"))
    return (
        rows.sort(["game_id", "period", "possession_number"])
        .with_columns(signed.fill_null(0).alias("_signed"))
        .with_columns((pl.col("_signed").cum_sum().over("game_id") - pl.col("_signed")).alias("start_margin"))
        .with_columns(
            (
                pl.col("late")
                # in play at or after the 5:00 mark: it starts inside the window, or it is the
                # possession live at 5:00 (starts before the mark and ends after it)
                & ((pl.col("start_clock") <= CLUTCH_SECONDS) | (pl.col("end_clock") < CLUTCH_SECONDS).fill_null(False))
                & (pl.col("start_margin").abs() <= CLUTCH_MARGIN)
            ).alias("clutch")
        )
        .with_columns(pl.col("clutch").any().over("game_id").alias("close"))
        .drop("_signed")
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


def _with_box_sides(rows: pl.DataFrame, counts: pl.DataFrame) -> pl.DataFrame:
    """The possession sides with null four-factor counts, plus the box ``counts`` as zero-possession sides."""
    games = rows.group_by("game_id").agg(pl.col("close").first(), pl.col("neutral_site").first())
    box = counts.select(list(COUNT_ROWS_SCHEMA)).cast(COUNT_ROWS_SCHEMA)
    assert box.schema["game_id"] == games.schema["game_id"], "game_id dtype differs between counts and rows"
    box = box.join(games, on="game_id", how="inner", validate="m:1").with_columns(
        pl.lit(False).alias("clutch"),
        pl.lit(0, dtype=pl.Int64).alias("points"),
        pl.lit(0.0).alias("duration"),
    )
    sides = _sides(rows, n_poss=1).with_columns(pl.lit(None, dtype=pl.Float64).alias(c) for c in _COUNTS)
    return pl.concat([sides, _sides(box, n_poss=0)])


def team_season_rates(
    rows: pl.DataFrame,
    *,
    minutes_per_game: int,
    counts: pl.DataFrame | None = None,
    level: str | None = None,
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
        level: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"``: rank a rate only when its ``<m>_n``
            reaches that league's :data:`~sportsdataverse._common.rate_gates.TEAM_MIN_N` gate.
            ``None`` applies no sample gate (the league adapters always pass one).

    Returns:
        One row per ``(season, team_id, split)`` in :data:`TEAM_SEASON_RATES_SCHEMA`, sorted by
        season, split (``all``, ``home``, ``away``, ``close``, ``clutch``) and team. An empty
        input returns that schema with zero rows.

    Raises:
        KeyError: If ``rows`` or ``counts`` is missing a schema column, or ``level`` is unknown.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_team_season_rates import nba_team_season_rates
            rates = nba_team_season_rates(2026)
            rates.filter(split="clutch").sort("net_rtg", descending=True).head()
    """
    for name, frame, schema in (("rows", rows, RATE_ROWS_SCHEMA), ("counts", counts, COUNT_ROWS_SCHEMA)):
        missing = [] if frame is None else sorted(set(schema) - set(frame.columns))
        if missing:  # checked before the empty-input return, so a bad counts frame always raises
            raise KeyError(f"{name} missing columns: {missing}")
    gates = team_gates(level, _RATED)
    if rows.height == 0:
        return pl.DataFrame(schema=TEAM_SEASON_RATES_SCHEMA)
    rates = rates_from_counts(season_counts(rows, counts=counts), minutes_per_game=minutes_per_game)
    rates = rates.with_columns(_league_ft_pct().over(["season", "split"]).alias("lg_ft_pct"))
    order = pl.col("split").replace_strict({s: i for i, s in enumerate(SPLITS)}, return_dtype=pl.Int64)
    return (
        rates.pipe(attach_sample_sizes, _sizes(minutes_per_game))
        .pipe(
            attach_ranks,
            list(_RATED),
            min_cohort=MIN_COHORT_TEAMS,
            by=("season", "split"),
            lower_is_better=_LOWER_IS_BETTER,
            qualifier=_QUALIFIER,
            min_n=gates,
        )
        .pipe(_with_bases, gates)
        .sort([pl.col("season"), order, pl.col("team_id")])
        .select(list(TEAM_SEASON_RATES_SCHEMA))
        .cast(TEAM_SEASON_RATES_SCHEMA)
    )


#: The counts behind the twelve rates, as :func:`season_counts` sums them per side: ``o_<c>`` the
#: team's, ``d_<c>`` its opponents'. ``season_counts`` also sums ``o_ftm`` / ``d_ftm``, which no
#: rate uses, so a table that carries these counts (``coach_seasons``) keeps its columns.
SIDE_COUNTS: tuple[str, ...] = tuple(f"{s}_{c}" for c in _COUNTS if c != "ftm" for s in ("o", "d"))


def season_counts(rows: pl.DataFrame, *, counts: pl.DataFrame | None = None, by_game: bool = False) -> pl.DataFrame:
    """The summed counts behind :func:`team_season_rates`, one row per ``(season, team_id, split)``.

    ``games``, ``poss``, ``opp_poss``, ``pts``, ``opp_pts`` (possession points), ``seconds`` (the
    game clock the team's possessions used) and the four-factor counts :data:`SIDE_COUNTS`. Sums of
    these over any set of rows give the pooled rates (:func:`rates_from_counts`), so a career is
    the rate of its summed counts, never an average of season rates.

    Args:
        rows: The possessions, in :data:`RATE_ROWS_SCHEMA` (already validated by the caller).
        counts: Game-level counts in :data:`COUNT_ROWS_SCHEMA`, as :func:`team_season_rates`.
        by_game: Group by ``game_id`` as well: one row per ``(season, team_id, game_id, split)``.

    Returns:
        The counts, unsorted, one row per ``(season, team_id, split)`` (``by_game``: per game too).
    """
    rows = _with_clutch(rows.select(list(RATE_ROWS_SCHEMA)).cast(RATE_ROWS_SCHEMA))
    rows = rows.with_columns((pl.col("start_clock") - pl.col("end_clock")).alias("duration"))
    sides = _sides(rows, n_poss=1) if counts is None else _with_box_sides(rows, counts)
    return (
        _split_rows(sides)
        .group_by(["season", "team_id", *(["game_id"] if by_game else []), "split"])
        .agg(
            pl.col("game_id").filter(pl.col("n_poss") > 0).n_unique().cast(pl.Int64).alias("games"),
            pl.col("n_poss").filter(pl.col("side") == "o").sum().alias("poss"),
            pl.col("n_poss").filter(pl.col("side") == "d").sum().alias("opp_poss"),
            pl.col("points").filter(pl.col("side") == "o").sum().alias("pts"),
            pl.col("points").filter(pl.col("side") == "d").sum().alias("opp_pts"),
            # each possession is in both sides' rows, so this is the clock of the team's games
            pl.col("duration").filter(pl.col("n_poss") > 0).sum().alias("seconds"),
            *[_side_sum(c, s) for c in _COUNTS for s in ("o", "d")],
        )
    )


def team_game_possessions(rows: pl.DataFrame) -> pl.DataFrame:
    """The possession counts of :func:`team_season_rates`, one row per team, game and split.

    The per-game grain of the season table's ``poss`` / ``opp_poss`` / ``pts`` / ``opp_pts``: a
    game has a row in each split it belongs to (``all`` always; ``home`` or ``away`` unless it is
    at a neutral site; ``close`` and ``clutch`` when it has clutch possessions). So the rows of a
    team-season's split add up to its season row: their count is ``games`` and their sums are its
    counts, and the season's ``pace`` is ``minutes_per_game * 60 * (poss + opp_poss) / 2 /
    seconds`` over the summed rows. A set of games (wins, losses, the last ten) is rated the same
    way: sum the rows, divide once.

    Args:
        rows: The possessions, in :data:`RATE_ROWS_SCHEMA` (extra columns are ignored); a side
            with a null team id gets no row, its points still count in the clutch margin.

    Returns:
        One row per ``(season, team_id, game_id, split)`` in :data:`TEAM_GAME_POSSESSIONS_SCHEMA`,
        sorted by season, team, game and split (``all``, ``home``, ``away``, ``close``,
        ``clutch``). ``seconds`` is the game clock the team's possessions used. An empty input
        returns that schema with zero rows.

    Raises:
        KeyError: If ``rows`` is missing a schema column.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_team_season_rates import nba_team_game_possessions
            games = nba_team_game_possessions(2026).filter(split="all")
    """
    missing = sorted(set(RATE_ROWS_SCHEMA) - set(rows.columns))
    if missing:
        raise KeyError(f"rows missing columns: {missing}")
    if rows.height == 0:
        return pl.DataFrame(schema=TEAM_GAME_POSSESSIONS_SCHEMA)
    order = pl.col("split").replace_strict({s: i for i, s in enumerate(SPLITS)}, return_dtype=pl.Int64)
    return (
        season_counts(rows, by_game=True)
        .sort([pl.col("season"), pl.col("team_id"), pl.col("game_id"), order])
        .select(list(TEAM_GAME_POSSESSIONS_SCHEMA))
        .cast(TEAM_GAME_POSSESSIONS_SCHEMA)
    )


#: The four-factor accounting (:func:`team_game_four_factors`), one row per team and game.
TEAM_GAME_FOUR_FACTORS_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "team_id": pl.Int64(),
    "game_id": pl.Utf8(),
    "poss": pl.Int64(),
    "pts": pl.Int64(),
    "exp_pts": pl.Float64(),
    "pts_vs_exp": pl.Float64(),
    "shooting_pts": pl.Float64(),
    "turnover_pts": pl.Float64(),
    "rebounding_pts": pl.Float64(),
    "free_throw_pts": pl.Float64(),
    "residual_pts": pl.Float64(),
    "lg_ppp": pl.Float64(),
    "lg_efg_pct": pl.Float64(),
    "lg_tov_pct": pl.Float64(),
    "lg_orb_pct": pl.Float64(),
    "lg_ft_rate": pl.Float64(),
    "lg_ft_pct": pl.Float64(),
    "chance_pts": pl.Float64(),
}

#: The share of a possession one free throw uses: the box possession estimate's weight.
FT_POSSESSION_WEIGHT = 0.44


def _accounting_counts(agg: pl.DataFrame) -> pl.DataFrame:
    """The team's offensive counts under short names, plus rebound chances, box points and box possessions."""
    c = {k: pl.col(f"o_{k}") for k in ("fga", "fgm", "fg3m", "fta", "ftm", "oreb", "dreb", "tov")}
    return agg.with_columns(
        **c,
        rc=c["oreb"] + c["dreb"],
        box_pts=2 * c["fgm"] + c["fg3m"] + c["ftm"],
        box_poss=c["fga"] - c["oreb"] + c["tov"] + FT_POSSESSION_WEIGHT * c["fta"],
    )


#: The counts a team-game must carry to be priced, summed for the league values.
_SUMMED = ("pts", "poss", "fga", "fgm", "fg3m", "fta", "ftm", "oreb", "rc", "tov", "box_pts", "box_poss")


def _league_values(games: pl.DataFrame) -> pl.DataFrame:
    """The season's league values, pooled over the priced team-games (one row per season)."""
    lg = {c: pl.col(f"_lg_{c}") for c in _SUMMED}
    return (
        games.group_by("season")
        .agg(pl.col(c).sum().alias(f"_lg_{c}") for c in _SUMMED)
        .select(
            "season",
            (lg["pts"] / lg["poss"]).alias("lg_ppp"),
            ((lg["fgm"] + 0.5 * lg["fg3m"]) / lg["fga"]).alias("lg_efg_pct"),
            (lg["tov"] / lg["poss"]).alias("lg_tov_pct"),
            (lg["oreb"] / lg["rc"]).alias("lg_orb_pct"),
            (lg["fta"] / lg["fga"]).alias("lg_ft_rate"),
            (lg["ftm"] / lg["fta"]).alias("lg_ft_pct"),
            (lg["rc"] / lg["fga"]).alias("_lg_rc_per_fga"),
        )
        .with_columns((pl.col("lg_ppp") / (1 - pl.col("lg_tov_pct"))).alias("chance_pts"))
    )


def _factor_prices() -> dict[str, pl.Expr]:
    """The four factors' linear prices (:func:`team_game_four_factors` states them)."""
    v, fga, lg_orb = pl.col("chance_pts"), pl.col("fga"), pl.col("lg_orb_pct")
    return {
        "shooting_pts": 2 * pl.col("fgm")
        + pl.col("fg3m")
        - 2 * pl.col("lg_efg_pct") * fga
        + v * lg_orb * (pl.col("rc") - pl.col("_lg_rc_per_fga") * fga),
        "turnover_pts": (pl.col("lg_tov_pct") * pl.col("poss") - pl.col("tov")) * v,
        "rebounding_pts": (pl.col("oreb") - lg_orb * pl.col("rc")) * v,
        "free_throw_pts": (pl.col("fta") - pl.col("lg_ft_rate") * fga)
        * (pl.col("lg_ft_pct") - FT_POSSESSION_WEIGHT * v)
        + (pl.col("ftm") - pl.col("lg_ft_pct") * pl.col("fta")),
    }


def team_game_four_factors(rows: pl.DataFrame, *, counts: pl.DataFrame | None = None) -> pl.DataFrame:
    """Each team-game's four factors in points against the season's league-average offense.

    ``pts_vs_exp`` is the team's points less ``exp_pts``, a league-average offense on the same
    counted possessions (``lg_ppp * poss``). It splits exactly into the four factors and a residual:
    ``pts_vs_exp = shooting_pts + turnover_pts + rebounding_pts + free_throw_pts + residual_pts``.

    The prices are linear, the Game on Paper game page's, with a league free-throw percentage. A
    chance (a possession that does not end in a turnover) is worth ``chance_pts`` =
    ``lg_ppp / (1 - lg_tov_pct)``. Every league value is the season's, pooled over every team-game
    with all its counts: ``lg_tov_pct`` is turnovers per counted possession and ``lg_ft_pct`` all
    FTM over all FTA.

    * ``shooting_pts`` = ``2 * (eFG% - lg eFG%) * FGA``, the points the same shots were worth above
      a league-average shooter's, plus ``chance_pts * lg ORB% * (RC - lg RC/FGA * FGA)``: the
      extra rebound chances (``RC`` = own OREB + opponent DREB) the misses made, which a
      league-average offense gets back at ``lg ORB%``. Without that term a miss is priced as a dead
      possession, while ``rebounding_pts`` prices the rebounds on the same chances, and the four
      do not add up.
    * ``turnover_pts`` = ``(lg TOV% * poss - TOV) * chance_pts``: the turnovers saved, each a chance.
    * ``rebounding_pts`` = ``(OREB - lg ORB% * RC) * chance_pts``: the extra offensive rebounds on
      the team's own rebound chances, each a new chance.
    * ``free_throw_pts`` = ``(FTA - lg FT rate * FGA) * (lg FT% - 0.44 * chance_pts)`` +
      ``(FTM - lg FT% * FTA)``: the extra free throws, each worth a league-average free throw less
      the 0.44 of a chance it uses (:data:`FT_POSSESSION_WEIGHT`), plus the makes above league
      FT% on all of them. A team with no free throws is priced too (it got none of the expected).
    * ``residual_pts`` is the remainder, exactly
      ``(pts - box_pts - FGA * lg_gap_pts) + chance_pts * (box_poss - poss - FGA * lg_gap_poss)``,
      where ``box_pts = 2 * FGM + 3PM + FTM``, ``box_poss = FGA - OREB + TOV + 0.44 * FTA`` and
      ``lg_gap_pts`` / ``lg_gap_poss`` are the league's ``pts - box_pts`` / ``box_poss - poss``
      per FGA. It is the possession count's distance from the box estimate (and, in college,
      the play-by-play points' from the box's), beyond the league's typical gap, so it sums to
      zero over the league. The pros' points are the box's, so only the possession gap is left.

    Args:
        rows: The possessions, in :data:`RATE_ROWS_SCHEMA` (extra columns are ignored).
        counts: Game-level four-factor counts in :data:`COUNT_ROWS_SCHEMA` (college), as
            :func:`team_season_rates`.

    Returns:
        One row per ``(season, team_id, game_id)`` (the ``all`` split) in
        :data:`TEAM_GAME_FOUR_FACTORS_SCHEMA`, sorted by season, team and game. A game without
        its counts (no box row) has ``exp_pts`` but null factors. An empty input returns that
        schema with zero rows.

    Raises:
        KeyError: If ``rows`` or ``counts`` is missing a schema column.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_team_season_rates import nba_team_game_four_factors
            books = nba_team_game_four_factors(2026)
            books.sort("pts_vs_exp", descending=True).head()
    """
    for name, frame, schema in (("rows", rows, RATE_ROWS_SCHEMA), ("counts", counts, COUNT_ROWS_SCHEMA)):
        missing = [] if frame is None else sorted(set(schema) - set(frame.columns))
        if missing:
            raise KeyError(f"{name} missing columns: {missing}")
    if rows.height == 0:
        return pl.DataFrame(schema=TEAM_GAME_FOUR_FACTORS_SCHEMA)
    games = _accounting_counts(season_counts(rows, counts=counts, by_game=True).filter(pl.col("split") == "all"))
    complete = pl.all_horizontal(pl.col(c).is_not_null() for c in _SUMMED) & (pl.col("poss") > 0)
    lg = _league_values(games.filter(complete))
    assert games.schema["season"] == lg.schema["season"], "season dtype differs"
    factors = _factor_prices()
    return (
        games.join(lg, on="season", how="left", validate="m:1")
        .with_columns((pl.col("lg_ppp") * pl.col("poss")).alias("exp_pts"))
        .with_columns((pl.col("pts") - pl.col("exp_pts")).alias("pts_vs_exp"))
        .with_columns(pl.when(complete).then(e).alias(k) for k, e in factors.items())
        .with_columns((pl.col("pts_vs_exp") - pl.sum_horizontal(*factors, ignore_nulls=False)).alias("residual_pts"))
        .sort(["season", "team_id", "game_id"])
        .select(list(TEAM_GAME_FOUR_FACTORS_SCHEMA))
        .cast(TEAM_GAME_FOUR_FACTORS_SCHEMA)
    )


def rates_from_counts(agg: pl.DataFrame, *, minutes_per_game: int) -> pl.DataFrame:
    """``agg`` (:func:`season_counts` columns, any grouping) with the twelve rates appended.

    Args:
        agg: Summed counts: ``poss``, ``opp_poss``, ``pts``, ``opp_pts``, ``seconds`` and
            :data:`SIDE_COUNTS`.
        minutes_per_game: Regulation minutes, the pace basis.

    Returns:
        ``agg`` plus ``off_rtg`` .. ``opp_ft_rate`` and ``net_rtg``; a zero denominator gives null.
    """
    efg = {s: (pl.col(f"{s}_fgm") + 0.5 * pl.col(f"{s}_fg3m")) / pl.col(f"{s}_fga") for s in ("o", "d")}
    rates = agg.with_columns(
        (100 * pl.col("pts") / pl.col("poss")).alias("off_rtg"),
        (100 * pl.col("opp_pts") / pl.col("opp_poss")).alias("def_rtg"),
        (minutes_per_game * 60 * (pl.col("poss") + pl.col("opp_poss")) / 2 / pl.col("seconds")).alias("pace"),
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
    return rates.with_columns(pl.when(pl.col(m).is_finite()).then(pl.col(m)).otherwise(None).alias(m) for m in _RATED)


def team_season_rates_league_averages(rates: pl.DataFrame, *, level: str) -> pl.DataFrame:
    """The league-average rows of a team-season-rates table, one block per split.

    The football F1 rows of :func:`~sportsdataverse._common.league_context.league_averages`, over
    the same qualified teams the ranks use (half the split's most games, and each rate's
    ``level`` sample gate), so each row's ``n`` is the cohort count: the number of qualifying
    teams with a finite value, which is the ``n`` of the ``_pct`` formula (its denominator is
    ``n + 1``) and not a rate's ``<m>_n`` sample size. It ignores the cohort floor, so a split
    below the floor still reports ``n`` beside null ranks; a rate no team's sample reaches (the
    clutch split's) has no row. ``category`` is ``team_season_rates_<split>``. ``qualifier_min``
    is null: the games gate is relative (half the split's most games), not a fixed threshold.

    Args:
        rates: Output of a ``*_team_season_rates`` function.
        level: The population label (``"nba"``, ``"wnba"``, ``"mbb"``, ``"wbb"``), which also
            picks the sample gates of :data:`~sportsdataverse._common.rate_gates.TEAM_MIN_N`.

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
    gates = team_gates(level, _RATED)
    frames = [
        league_averages(
            rates.filter(pl.col("split") == s),
            list(_RATED),
            level=level,
            entity="team",
            category=f"team_season_rates_{s}",
            qualifier=_QUALIFIER,
            min_n=gates,
        )
        for s in SPLITS
    ]
    return pl.concat(frames) if frames else pl.DataFrame(schema=LEAGUE_AVERAGES_SCHEMA)
