"""Per-36 box rates and a usage-adjusted box expectation per player-game: the shared core.

Two tables from one league-season of ESPN player box scores (``load_{lg}_player_boxscore``), keyed
by the ESPN ``athlete_id``:

* :func:`player_season_box_rates` -- one row per ``(season, athlete_id)``: games, minutes, points,
  rebounds and assists, ``pts_per36`` / ``reb_per36`` / ``ast_per36`` (36 x total / minutes) each
  with ``_n`` (the minutes) and ``_rank`` / ``_pct`` among the league-season's players whose minutes
  reach the rate's :data:`PER36_MIN_N` gate, and the season usage ``usg_pct``. ``team_id`` is the
  team of the player's last game of the season (a traded player has one row, as ESPN's season
  line does).
* :func:`player_game_expected_box` -- one row per played player-game: the game's minutes, usage
  and points / rebounds / assists, then two expectations for each stat. ``<s>_base`` is the
  player's season per-minute rate times the game's minutes; ``<s>_exp`` is the usage-adjusted
  model below. Both use the season WITHOUT this game (leave one out), so a game never sits in its
  own baseline.

**The season.** The pros' regular season (ESPN ``season_type`` 2) and every college game, as the
other basketball season tables. A playoff or play-in game gets an expectation from the regular
season, with nothing to leave out.

**Usage** is the box formula ``100 * uses * (team minutes / 5) / (minutes * team uses)`` with
``uses = FGA + 0.44 FTA + TOV`` and the team sums taken over the players of that team-game (team
minutes is the sum of its players' minutes). It is null for a team-game with a player whose box
lacks a count (no turnovers recorded: WBB before 2009, many small-college boxes). The season usage
is the ratio of sums, ``100 * sum(uses) / sum(minutes * team uses / (team minutes / 5))``.

**The model** (:data:`EXPECTED_COEFS`), per league and stat, a Poisson regression with the
baseline as offset::

    <s>_exp = <s>_base * exp(a + b_usg * ln(usg / rate_usg) + b_min * ln(minutes / rate_mpg))

``usg`` is the game's usage (floored at :data:`USG_FLOOR`), ``rate_usg`` and ``rate_mpg`` the
season-without-this-game usage and minutes per game. ``a`` corrects the baseline's level,
``b_usg`` scales the stat with the share of the offense the player used that night (points rise
with it, rebounds barely), ``b_min`` bends the per-minute rate for unusually short or long nights.
The game's minutes and usage are inputs by design: the expectation answers "at his season rates,
with tonight's role, what would he have had?". Fitted by ``tools/fit_player_box_expected.py`` on
each league's TRAIN seasons only (rows with at least :data:`FIT_MIN_RATE_GAMES` other games) and
scored on the later HOLDOUT seasons (``tests/fixtures/bb_player_box_expected/``). It is published
only from the first holdout season (:data:`MODEL_FIRST_SEASON`), so every published expectation
comes from coefficients fitted on earlier seasons; before that ``<s>_exp`` is null and ``<s>_base``
stands alone.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse._common.league_context import MIN_COHORT_PLAYERS, attach_ranks, attach_sample_sizes

__all__ = [
    "EXPECTED_COEFS",
    "FIT_MIN_RATE_GAMES",
    "MODEL_FIRST_SEASON",
    "PER36_MIN_N",
    "PLAYER_GAME_EXPECTED_SCHEMA",
    "PLAYER_GAME_ROWS_SCHEMA",
    "PLAYER_SEASON_BOX_RATES_SCHEMA",
    "STATS",
    "USG_FLOOR",
    "league_player_box_expected",
    "player_game_box_rows",
    "player_game_expected_box",
    "player_season_box_rates",
]

#: The box stats the tables carry: output prefix -> ESPN player box column.
STATS: dict[str, str] = {"pts": "points", "reb": "rebounds", "ast": "assists"}
FT_WEIGHT = 0.44
#: A game usage below this (percent) is read as this in the model's log: a player who used nothing
#: still rebounds and passes.
USG_FLOOR = 1.0
#: The fit's rows: a season-without-this-game of at least this many games.
FIT_MIN_RATE_GAMES = 5
LEAGUES = ("nba", "wnba", "mbb", "wbb")

# Fitted by tools/fit_player_box_expected.py on the train seasons of its SPANS; refit and paste,
# never hand-tune. tests/fixtures/bb_player_box_expected/<lg>_fit.json holds the fit and its holdout
# scores, and the tests pin these to it.
#: League -> stat -> ``(a, b_usg, b_min)`` of the model in the module docstring.
EXPECTED_COEFS: dict[str, dict[str, tuple[float, float, float]]] = {
    "nba": {"pts": (-0.0119, 1.0355, 0.1725), "reb": (0.008, 0.1938, 0.009), "ast": (-0.0051, -0.0244, 0.0738)},
    "wnba": {"pts": (-0.016, 1.0841, 0.2295), "reb": (0.0074, 0.1772, 0.0061), "ast": (-0.0096, -0.1035, 0.0738)},
    "mbb": {"pts": (-0.0207, 1.0873, 0.2624), "reb": (0.0095, 0.1877, 0.025), "ast": (-0.0078, -0.061, 0.0465)},
    "wbb": {"pts": (-0.0197, 1.0686, 0.2597), "reb": (0.0104, 0.1874, 0.0037), "ast": (-0.0077, -0.0649, 0.033)},
}  # fmt: skip
#: League -> the first season ``<s>_exp`` is published: the first holdout season of the fit.
MODEL_FIRST_SEASON: dict[str, int] = {"nba": 2025, "wnba": 2024, "mbb": 2025, "wbb": 2025}
#: League -> per-36 rate -> the fewest minutes it is ranked on (split-half r = 0.5, the
#: ``rate_gates`` rule; fitted by the same tool; a gate the holdout seasons put under a third
#: signal would be withheld, and none is).
PER36_MIN_N: dict[str, dict[str, float]] = {
    "nba": {"pts_per36": 69, "reb_per36": 28, "ast_per36": 41},
    "wnba": {"pts_per36": 82, "reb_per36": 27, "ast_per36": 46},
    "mbb": {"pts_per36": 85, "reb_per36": 42, "ast_per36": 64},
    "wbb": {"pts_per36": 64, "reb_per36": 35, "ast_per36": 67},
}  # fmt: skip

#: One played player-game (minutes > 0), the input both tables read.
PLAYER_GAME_ROWS_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "season_type": pl.Int64(),
    "game_id": pl.Int64(),
    "game_date": pl.Date(),
    "team_id": pl.Int64(),
    "athlete_id": pl.Int64(),
    "minutes": pl.Float64(),
    "points": pl.Int64(),
    "rebounds": pl.Int64(),
    "assists": pl.Int64(),
    "uses": pl.Float64(),
    "team_minutes": pl.Float64(),
    "team_uses": pl.Float64(),
    "usg_pct": pl.Float64(),
    "in_rate": pl.Boolean(),
}

_PER36 = [f"{s}_per36" for s in STATS]

PLAYER_SEASON_BOX_RATES_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "athlete_id": pl.Int64(),
    "team_id": pl.Int64(),
    "games": pl.Int64(),
    "minutes": pl.Float64(),
    "pts": pl.Int64(),
    "reb": pl.Int64(),
    "ast": pl.Int64(),
    "usg_pct": pl.Float64(),
    "usg_pct_n": pl.Int64(),
    **{c: t for m in _PER36 for c, t in ((m, pl.Float64()), (f"{m}_n", pl.Int64()))},
    **{c: pl.Float64() for m in _PER36 for c in (f"{m}_rank", f"{m}_pct")},
}

PLAYER_GAME_EXPECTED_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "season_type": pl.Int64(),
    "game_id": pl.Int64(),
    "team_id": pl.Int64(),
    "athlete_id": pl.Int64(),
    "minutes": pl.Float64(),
    "usg_pct": pl.Float64(),
    "pts": pl.Int64(),
    "reb": pl.Int64(),
    "ast": pl.Int64(),
    "rate_games": pl.Int64(),
    "rate_minutes": pl.Float64(),
    "rate_usg_pct": pl.Float64(),
    **{f"{s}_base": pl.Float64() for s in STATS},
    **{f"{s}_exp": pl.Float64() for s in STATS},
}


def _check_league(league: str) -> None:
    if league not in LEAGUES:
        raise ValueError(f"league {league!r} is not one of {LEAGUES}")


def player_game_box_rows(player_box: pl.DataFrame, *, league: str) -> pl.DataFrame:
    """The played player-games of an ESPN player box, with the game's usage.

    Args:
        player_box: ``load_{lg}_player_boxscore`` rows (any number of games and seasons).
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"``: the pros' rates read the regular
            season (``in_rate``), college's every game.

    Returns:
        :data:`PLAYER_GAME_ROWS_SCHEMA` rows, one per player-game with minutes above 0 and not
        ``did_not_play``. ``usg_pct`` (and ``team_uses``) is null for a team-game where a played
        player lacks FGA, FTA or turnovers.

    Raises:
        ValueError: If ``league`` is not one of the four.
    """
    _check_league(league)
    if player_box.is_empty():
        return pl.DataFrame(schema=PLAYER_GAME_ROWS_SCHEMA)
    played = (pl.col("minutes").fill_null(0.0) > 0) & (pl.col("did_not_play").fill_null(False) == False)  # noqa: E712
    uses = (pl.col("field_goals_attempted") + FT_WEIGHT * pl.col("free_throws_attempted") + pl.col("turnovers")).cast(
        pl.Float64
    )
    tg = ["season", "game_id", "team_id"]
    rows = (
        player_box.filter(played & pl.col("athlete_id").is_not_null() & pl.col("team_id").is_not_null())
        .unique(["game_id", "athlete_id"], keep="first")
        .with_columns(uses.alias("uses"))
        .with_columns(
            pl.col("minutes").sum().over(tg).alias("team_minutes"),
            pl.when(pl.col("uses").null_count().over(tg) == 0).then(pl.col("uses").sum().over(tg)).alias("team_uses"),
        )
    )
    usg = 100.0 * pl.col("uses") * (pl.col("team_minutes") / 5.0) / (pl.col("minutes") * pl.col("team_uses"))
    in_rate = pl.lit(True) if league in ("mbb", "wbb") else pl.col("season_type") == 2
    return rows.with_columns(
        pl.when(pl.col("team_uses") > 0).then(usg).alias("usg_pct"),
        in_rate.fill_null(False).alias("in_rate"),
    ).select([pl.col(c).cast(t) for c, t in PLAYER_GAME_ROWS_SCHEMA.items()])


def _usage_parts(rows: pl.DataFrame) -> pl.DataFrame:
    """Each row's share of the season usage sums: ``(uses, minutes * team uses / (team minutes / 5))``."""
    has = pl.col("usg_pct").is_not_null()
    return rows.with_columns(
        pl.when(has).then(pl.col("uses")).otherwise(0.0).alias("_u_num"),
        pl.when(has)
        .then(pl.col("minutes") * pl.col("team_uses") / (pl.col("team_minutes") / 5.0))
        .otherwise(0.0)
        .alias("_u_den"),
        pl.when(has).then(pl.col("minutes")).otherwise(0.0).alias("_u_min"),
    )


def player_season_box_rates(rows: pl.DataFrame, *, league: str) -> pl.DataFrame:
    """Per-36 points, rebounds and assists per player-season, ranked among qualifiers.

    Args:
        rows: :func:`player_game_box_rows` output.
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"`` (picks the :data:`PER36_MIN_N` gates).

    Returns:
        :data:`PLAYER_SEASON_BOX_RATES_SCHEMA` rows over the ``in_rate`` games, sorted by season
        and ``pts_per36_rank``. ``<m>_n`` is the minutes; a rank is null under the gate or in a
        season with fewer than ten qualifiers.

    Raises:
        ValueError: If ``league`` is not one of the four.
    """
    _check_league(league)
    r = _usage_parts(rows.filter(pl.col("in_rate") == True))  # noqa: E712
    if r.is_empty():
        return pl.DataFrame(schema=PLAYER_SEASON_BOX_RATES_SCHEMA)
    agg = (
        r.sort("season", "athlete_id", "game_date", "game_id")
        .group_by("season", "athlete_id")
        .agg(
            pl.col("team_id").last(),
            pl.len().alias("games"),
            pl.col("minutes").sum(),
            *[pl.col(src).sum().alias(s) for s, src in STATS.items()],
            pl.col("_u_num").sum(),
            pl.col("_u_den").sum(),
            pl.col("_u_min").sum().round(0).cast(pl.Int64).alias("usg_pct_n"),
        )
        .with_columns(
            pl.when(pl.col("_u_den") > 0).then(100.0 * pl.col("_u_num") / pl.col("_u_den")).alias("usg_pct"),
            *[(36.0 * pl.col(s) / pl.col("minutes")).alias(f"{s}_per36") for s in STATS],
        )
    )
    agg = attach_sample_sizes(agg, {m: pl.col("minutes") for m in _PER36})
    agg = attach_ranks(agg, _PER36, min_cohort=MIN_COHORT_PLAYERS, min_n=PER36_MIN_N[league])
    return agg.select([pl.col(c).cast(t) for c, t in PLAYER_SEASON_BOX_RATES_SCHEMA.items()]).sort(
        ["season", "pts_per36_rank", "athlete_id"], nulls_last=True
    )


def player_game_expected_box(
    rows: pl.DataFrame,
    *,
    league: str,
    coefs: dict[str, tuple[float, float, float]] | None = None,
    first_season: int | None = None,
) -> pl.DataFrame:
    """Each played player-game's points, rebounds and assists against two expectations.

    Args:
        rows: :func:`player_game_box_rows` output (whole seasons: the season sums are taken here).
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"``.
        coefs: Stat -> ``(a, b_usg, b_min)``; defaults to the league's frozen
            :data:`EXPECTED_COEFS` (the fitting tool passes its own).
        first_season: The first season ``<s>_exp`` is filled; defaults to
            :data:`MODEL_FIRST_SEASON` (the fitting tool passes ``0``).

    Returns:
        :data:`PLAYER_GAME_EXPECTED_SCHEMA` rows. ``rate_games`` / ``rate_minutes`` /
        ``rate_usg_pct`` describe the season without this game; ``<s>_base`` is null when that
        season has no minutes, ``<s>_exp`` also when either usage is null or the season is before
        ``first_season``.

    Raises:
        ValueError: If ``league`` is not one of the four.
    """
    _check_league(league)
    if rows.is_empty():
        return pl.DataFrame(schema=PLAYER_GAME_EXPECTED_SCHEMA)
    coefs = EXPECTED_COEFS[league] if coefs is None else coefs
    first = MODEL_FIRST_SEASON[league] if first_season is None else first_season
    r = _usage_parts(rows)
    sums = ["minutes", *STATS.values(), "_u_num", "_u_den"]
    tot = (
        r.filter(pl.col("in_rate") == True)  # noqa: E712
        .group_by("season", "athlete_id")
        .agg(pl.len().alias("_g"), *[pl.col(c).sum().alias(f"_t_{c}") for c in sums])
    )
    r = r.join(tot, on=["season", "athlete_id"], how="left")
    own = pl.col("in_rate").cast(pl.Float64)
    loo = {c: pl.col(f"_t_{c}").fill_null(0.0) - own * pl.col(c) for c in sums}
    r = r.with_columns(
        (pl.col("_g").fill_null(0) - pl.col("in_rate").cast(pl.Int64)).alias("rate_games"),
        loo["minutes"].alias("rate_minutes"),
        pl.when(loo["_u_den"] > 1e-9).then(100.0 * loo["_u_num"] / loo["_u_den"]).alias("rate_usg_pct"),
        *[loo[src].alias(f"_l_{s}") for s, src in STATS.items()],
    )
    ok = pl.col("rate_minutes") > 1e-9
    r = r.with_columns(
        *[
            pl.when(ok).then(pl.col("minutes") * pl.col(f"_l_{s}") / pl.col("rate_minutes")).alias(f"{s}_base")
            for s in STATS
        ],
        (pl.col("usg_pct").clip(lower_bound=USG_FLOOR) / pl.col("rate_usg_pct")).log().alias("_x_usg"),
        (pl.col("minutes") / (pl.col("rate_minutes") / pl.col("rate_games"))).log().alias("_x_min"),
    )
    model = ok & (pl.col("season") >= first) & pl.col("_x_usg").is_finite() & (pl.col("rate_games") > 0)
    r = r.with_columns(
        pl.when(model)
        .then(pl.col(f"{s}_base") * (a + bu * pl.col("_x_usg") + bm * pl.col("_x_min")).exp())
        .alias(f"{s}_exp")
        for s, (a, bu, bm) in coefs.items()
    )
    r = r.rename({src: s for s, src in STATS.items()})
    return r.select([pl.col(c).cast(t) for c, t in PLAYER_GAME_EXPECTED_SCHEMA.items()]).sort(
        "season", "game_id", "team_id", "athlete_id"
    )


def league_player_box_expected(league: str, seasons: int | list[int]) -> dict[str, pl.DataFrame]:
    """Load the league's ESPN player box release and build both tables.

    Args:
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"``.
        seasons: A season or list of seasons, named by the year it ends.

    Returns:
        ``{"player_season_box_rates": ..., "player_game_expected_box": ...}``.

    Raises:
        ValueError: If ``league`` is not one of the four.
    """
    import importlib

    _check_league(league)
    wanted = [seasons] if isinstance(seasons, int) else list(seasons)
    mod = importlib.import_module(f"sportsdataverse.{league}.{league}_loaders")
    rows = player_game_box_rows(mod.__dict__[f"load_{league}_player_boxscore"](wanted), league=league)
    return {
        "player_season_box_rates": player_season_box_rates(rows, league=league),
        "player_game_expected_box": player_game_expected_box(rows, league=league),
    }
