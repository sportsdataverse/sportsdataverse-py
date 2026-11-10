"""Game stints and in-game units (BB-D32): the shared core behind ``{lg}_game_stints``, ``{lg}_game_units`` and ``{lg}_game_on_off``.

Three per-game tables, each on the BB-D6 crediting rule (:mod:`sportsdataverse._common.game_impact`):
points go to the lineup at the scoring action, a free throw to the lineup at the game's most recent
foul, the clock to the lineup at each action, and a possession to the lineup on the floor when it
starts.

* **Stints** (:func:`game_stints`): one row per stretch a player spent on the floor, in game order.
  A stint is a run of consecutive actions of one period with the player in his side's lineup, so a
  substitution or a period end closes it. It carries the period, the period clock when he came on
  (``clock_in``) and went off (``clock_out``), the clock, points for and against and the
  possessions in between. The stint rows (:func:`stint_row_schema`) are the impact action rows in
  game order with ``period``, ``clock`` and ``order``; a free throw's points sit on the foul's row
  (:func:`credit_free_throws_to_foul_row`), which is on the lineup at the foul, so a free throw
  after a substitution neither breaks a stint nor opens a new one. A possession is counted on the
  action that starts it, on that action's lineup.
* **Units** (:func:`game_units`): every 1- to 5-man set of teammates that shared the floor in the
  game, with minutes, possessions, +/-, ORtg/DRtg/net, the four factors and the shot zones for and
  against. A unit is summed over every stretch its players were all on the floor, whoever the rest
  were: the five-man rows are :func:`~sportsdataverse._common.game_impact.game_lineups_used` and the
  one-man rows :func:`~sportsdataverse._common.game_impact.game_player_impact`. The counts behind
  the four factors and zones are the BB-D4 count rows
  (:func:`~sportsdataverse._common.lineup_seasons.lineup_count_schema`, with the zones).
* **On/off** (:func:`game_on_off`): per player and game, the team with him on the floor (``_on``)
  and the rest of its game (``_off``). ``<total>_on + <total>_off`` is the team's game total.

Per team and game, every sum over its players' stints is five times the team's: ``seconds``,
``pts``, ``opp_pts`` and ``plus_minus`` (five times the final margin), and ``poss`` / ``opp_poss``.
A stretch whose source lineup lists fewer than five players breaks that identity, as in BB-D6. There
are no ranks or percentiles: one game is not a cohort. The rates carry ``<m>_n``, their own
denominator.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package.
"""

from __future__ import annotations

from itertools import combinations

import polars as pl

from sportsdataverse._common.game_impact import AWAY_SLOTS, HOME_SLOTS, _sides, impact_row_schema
from sportsdataverse._common.league_context import attach_sample_sizes
from sportsdataverse._common.lineup_seasons import _RATED, _ZONE_RATED, COUNTS, ZONE_COUNTS, _rates
from sportsdataverse._common.player_on_off import _RATINGS, _SIDE_COLS, _n_expr, _side_rates

__all__ = [
    "UNIT_SIZES",
    "credit_free_throws_to_foul_row",
    "game_on_off",
    "game_on_off_schema",
    "game_stints",
    "game_stints_schema",
    "game_units",
    "game_units_schema",
    "stint_row_schema",
]

#: The unit sizes :func:`game_units` carries.
UNIT_SIZES: tuple[int, ...] = (1, 2, 3, 4, 5)
_SLOTS = HOME_SLOTS + AWAY_SLOTS
_KEYS = ("season", "game_id", "team_id", "opp_team_id")
_TOTALS = ("seconds", "poss", "opp_poss", "pts", "opp_pts")
_NAMES = COUNTS + ZONE_COUNTS
_KEY_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "game_id": pl.Utf8(),
    "team_id": pl.Int64(),
    "opp_team_id": pl.Int64(),
}
_RATES = {**_RATED, **_ZONE_RATED}


def stint_row_schema(player_dtype: pl.DataType) -> dict[str, pl.DataType]:
    """The stint-row frame :func:`game_stints` reads: impact action rows plus their place in the game.

    ``period``; ``clock``, the period clock left after the action (seconds); ``order``, the action's
    position in the game. The action ran from ``clock + seconds`` down to ``clock``.
    """
    return {**impact_row_schema(player_dtype), "period": pl.Int64(), "clock": pl.Float64(), "order": pl.Int64()}


def game_stints_schema(player_dtype: pl.DataType) -> dict[str, pl.DataType]:
    """Columns of :func:`game_stints` for a league whose player ids are ``player_dtype``."""
    return {
        **_KEY_SCHEMA,
        "player_id": player_dtype,
        "stint_number": pl.Int64(),
        "period": pl.Int64(),
        "clock_in": pl.Float64(),
        "clock_out": pl.Float64(),
        "seconds": pl.Float64(),
        "minutes": pl.Float64(),
        "poss": pl.Int64(),
        "opp_poss": pl.Int64(),
        "pts": pl.Int64(),
        "opp_pts": pl.Int64(),
        "plus_minus": pl.Int64(),
    }


def game_units_schema(player_dtype: pl.DataType) -> dict[str, pl.DataType]:
    """Columns of :func:`game_units` for a league whose player ids are ``player_dtype``."""
    return {
        **_KEY_SCHEMA,
        "n_players": pl.Int64(),
        "lineup_id": pl.Utf8(),
        **{f"player_{i}": player_dtype for i in range(1, 6)},
        "minutes": pl.Float64(),
        "seconds": pl.Float64(),
        "poss": pl.Int64(),
        "opp_poss": pl.Int64(),
        "pts": pl.Int64(),
        "opp_pts": pl.Int64(),
        "plus_minus": pl.Int64(),
        **dict.fromkeys(_RATES, pl.Float64()),
        **{f"{m}_n": pl.Int64() for m in _RATES},
    }


def game_on_off_schema(player_dtype: pl.DataType) -> dict[str, pl.DataType]:
    """Columns of :func:`game_on_off` for a league whose player ids are ``player_dtype``."""
    return {
        **_KEY_SCHEMA,
        "player_id": player_dtype,
        **{f"{c}_{s}": t for s in ("on", "off") for c, t in _SIDE_COLS.items()},
        **{f"{r}_diff": pl.Float64() for r in _RATINGS},
        **{f"{r}_{s}_n": pl.Int64() for s in ("on", "off", "diff") for r in _RATINGS},
    }


def credit_free_throws_to_foul_row(
    actions: pl.DataFrame, *, is_free_throw: pl.Expr, is_foul: pl.Expr, game: str = "game_id"
) -> pl.DataFrame:
    """Move each free throw's points onto the game's most recent foul row.

    The time-axis twin of :func:`~sportsdataverse._common.game_impact.credit_free_throws_to_foul_lineup`:
    the points land on the same lineup (the one at the foul), but every row keeps its own lineup and
    clock, so the on-floor sequence is the source's. A free throw with no earlier foul in the game
    keeps its points.

    Args:
        actions: Action rows with ``home_pts`` / ``away_pts``, sorted in game order.
        is_free_throw: True on a free-throw action.
        is_foul: True on a foul action.
        game: The game key column.

    Returns:
        ``actions`` with the same rows in the same order and the free-throw points moved.
    """
    marked = actions.with_columns(pl.int_range(pl.len()).over(game).alias("_i")).with_columns(
        pl.when(is_foul.fill_null(False)).then(pl.col("_i")).forward_fill().over(game).alias("_f")
    )
    moving = is_free_throw.fill_null(False) & pl.col("_f").is_not_null()
    moved = (
        marked.filter(moving)
        .group_by(game, "_f")
        .agg(pl.col("home_pts").sum().alias("_home"), pl.col("away_pts").sum().alias("_away"))
        .rename({"_f": "_i"})
    )
    kept = marked.with_columns(
        pl.when(moving).then(pl.col(c) * 0).otherwise(pl.col(c)).alias(c) for c in ("home_pts", "away_pts")
    )
    return (
        kept.join(moved, on=[game, "_i"], how="left", validate="1:1", maintain_order="left")
        .with_columns((pl.col(s) + pl.col(f"_{s[:4]}").fill_null(0)).alias(s) for s in ("home_pts", "away_pts"))
        .drop("_i", "_f", "_home", "_away")
    )


def game_stints(rows: pl.DataFrame, *, player_dtype: pl.DataType) -> pl.DataFrame:
    """Per player and game, each stint on the floor: period, clock in and out, points and possessions.

    Args:
        rows: Stint rows (:func:`stint_row_schema`), free-throw points already on the foul row.
        player_dtype: The league's player id dtype.

    Returns:
        One row per ``(season, game_id, team_id, player_id, stint_number)`` in
        :func:`game_stints_schema`, ``stint_number`` counting from 1 in game order. ``clock_in`` /
        ``clock_out`` are the period clock (seconds left) when the stint began and ended. A player
        whose side has no team id (a non-D1 college opponent) gets no row. Sorted by game, team,
        player and stint.
    """
    schema = game_stints_schema(player_dtype)
    if rows.height == 0:
        return pl.DataFrame(schema=schema)
    # Collapse each run of consecutive actions with the same ten players (within a period) to one
    # row first: a stint is a run of runs, and a game has ~10x fewer runs than actions.
    ordered = rows.select(
        "season", "game_id", "home_team_id", "away_team_id", *_SLOTS, "period", "order",
        "clock", "seconds", "home_pts", "away_pts", "home_poss", "away_poss",
    ).sort("game_id", "order")  # fmt: skip
    changed = pl.any_horizontal([pl.col(c).ne_missing(pl.col(c).shift(1)) for c in (*_SLOTS, "period")]) | pl.col(
        "game_id"
    ).ne_missing(pl.col("game_id").shift(1))
    runs = (
        ordered.with_columns(changed.cast(pl.Int64).cum_sum().alias("_seq"))
        .group_by("_seq", maintain_order=True)
        .agg(
            *[pl.col(c).first() for c in ("season", "game_id", "home_team_id", "away_team_id", *_SLOTS, "period")],
            (pl.col("clock") + pl.col("seconds")).first().alias("clock_in"),
            pl.col("clock").last().alias("clock_out"),
            pl.col("seconds", "home_pts", "away_pts", "home_poss", "away_poss").sum(),
        )
    )
    sides = pl.concat(
        [
            runs.select(
                "season",
                "game_id",
                pl.col(f"{us}_team_id").alias("team_id"),
                pl.col(f"{them}_team_id").alias("opp_team_id"),
                pl.concat_list(slots).list.drop_nulls().list.unique().alias("player_id"),
                "_seq",
                "period",
                "clock_in",
                "clock_out",
                "seconds",
                pl.col(f"{us}_poss").alias("poss"),
                pl.col(f"{them}_poss").alias("opp_poss"),
                pl.col(f"{us}_pts").alias("pts"),
                pl.col(f"{them}_pts").alias("opp_pts"),
            )
            for us, them, slots in (("home", "away", HOME_SLOTS), ("away", "home", AWAY_SLOTS))
        ]
    ).filter(pl.col("team_id").is_not_null())
    player = ["season", "game_id", "team_id", "player_id"]
    on = sides.explode("player_id").drop_nulls("player_id").sort([*player, "_seq"])
    # A new stint when the player missed a run (he was off) or the period turned.
    new = (pl.col("_seq").diff().over(player) != 1) | (pl.col("period").diff().over(player) != 0)
    on = on.with_columns(new.fill_null(True).cast(pl.Int64).cum_sum().over(player).alias("stint_number"))
    out = (
        on.group_by([*player, "stint_number"], maintain_order=True)
        .agg(
            pl.col("opp_team_id").first(),
            pl.col("period").first(),
            pl.col("clock_in").first(),
            pl.col("clock_out").last(),
            *[pl.col(c).sum() for c in _TOTALS],
        )
        .with_columns(
            # float clock sums depend on the summation order; rounding keeps rebuilds identical
            pl.col("seconds").round(6),
            (pl.col("seconds") / 60).round(6).alias("minutes"),
            (pl.col("pts") - pl.col("opp_pts")).alias("plus_minus"),
        )
    )
    return out.select(list(schema)).cast(schema).sort([*player, "stint_number"])


def _unit_sides(rows: pl.DataFrame, counts: pl.DataFrame) -> pl.DataFrame:
    """Each team's on-floor set per game (a sorted player list) with its totals and o_/d_ counts."""
    value = [*_TOTALS[:1], "home_pts", "away_pts", "home_poss", "away_poss"]
    value += [f"{s}_{c}" for s in ("home", "away") for c in _NAMES]
    keys = ["season", "game_id", "home_team_id", "away_team_id", *_SLOTS]
    both = pl.concat([rows.select(*keys, *value[:5]), counts.select(*keys, *value[5:])], how="diagonal")
    stints = both.group_by(keys).agg(pl.col(value).fill_null(0).sum())
    sides = pl.concat(
        [
            stints.select(
                "season",
                "game_id",
                pl.col(f"{us}_team_id").alias("team_id"),
                pl.col(f"{them}_team_id").alias("opp_team_id"),
                pl.concat_list(slots).list.drop_nulls().list.unique().list.sort().alias("players"),
                "seconds",
                pl.col(f"{us}_poss").alias("poss"),
                pl.col(f"{them}_poss").alias("opp_poss"),
                pl.col(f"{us}_pts").alias("pts"),
                pl.col(f"{them}_pts").alias("opp_pts"),
                *[pl.col(f"{us}_{c}").alias(f"o_{c}") for c in _NAMES],
                *[pl.col(f"{them}_{c}").alias(f"d_{c}") for c in _NAMES],
            )
            for us, them, slots in (("home", "away", HOME_SLOTS), ("away", "home", AWAY_SLOTS))
        ]
    ).filter(pl.col("team_id").is_not_null())
    # One row per distinct on-floor set: the opponent's lineup no longer splits it.
    return sides.group_by([*_KEYS, "players"]).agg(pl.exclude("players").sum())


def game_units(rows: pl.DataFrame, counts: pl.DataFrame, *, player_dtype: pl.DataType) -> pl.DataFrame:
    """Per 1- to 5-man unit and game: minutes, possessions, ratings, four factors and shot zones.

    Args:
        rows: Impact rows (:func:`~sportsdataverse._common.game_impact.impact_row_schema`).
        counts: Count rows in :func:`~sportsdataverse._common.lineup_seasons.lineup_count_schema`
            with the zones. Only the games in ``rows`` are read.
        player_dtype: The league's player id dtype.

    Returns:
        One row per ``(season, game_id, team_id, n_players, lineup_id)`` in
        :func:`game_units_schema`. ``lineup_id`` is the unit's player ids sorted and joined with
        ``-``; ``player_1..5`` are those ids in that order (null past ``n_players``). The rates are
        fractions (null on a 0 denominator), ratings per 100 possessions. Sorted by game, team,
        unit size and minutes (most first).

    Raises:
        KeyError: If ``counts`` is missing a count column.
    """
    schema = game_units_schema(player_dtype)
    missing = sorted({f"{s}_{c}" for s in ("home", "away") for c in _NAMES} - set(counts.columns))
    if missing:
        raise KeyError(f"counts missing columns: {missing}")
    if rows.height == 0:
        return pl.DataFrame(schema=schema)
    counts = counts.join(rows.select("season", "game_id").unique(), on=["season", "game_id"], how="semi")
    sides = _unit_sides(rows, counts)
    value_cols = [c for c in sides.columns if c not in (*_KEYS, "players")]
    parts = []
    for k in UNIT_SIZES:
        # ponytail: one frame per size; a set of five has C(5, k) subsets. A set with fewer known
        # players only gives the subsets of the players it has.
        subsets = [
            sides.filter(pl.col("players").list.len() > max(idx)).select(
                *_KEYS,
                pl.lit(k, dtype=pl.Int64).alias("n_players"),
                pl.concat_list([pl.col("players").list.get(i) for i in idx]).alias("unit"),
                *value_cols,
            )
            for idx in combinations(range(5), k)  # the set is sorted, so each subset stays sorted
        ]
        stacked = pl.concat(subsets)
        parts.append(
            stacked.with_columns(pl.col("unit").cast(pl.List(pl.Utf8)).list.join("-").alias("lineup_id"))
            .group_by([*_KEYS, "n_players", "lineup_id"])
            .agg(pl.col("unit").first(), *[pl.col(c).sum() for c in value_cols])
        )
    units = pl.concat(parts).with_columns(
        pl.col("unit").list.get(i, null_on_oob=True).alias(f"player_{i + 1}") for i in range(5)
    )
    return (
        _rates(units, zones=True)
        .with_columns(pl.col("seconds").round(6))  # summation order, as minutes
        .pipe(attach_sample_sizes, {m: e for m, (e, _) in _RATES.items()})
        .select(list(schema))
        .cast(schema)
        .sort(
            ["season", "game_id", "team_id", "n_players", "minutes", "lineup_id"],
            descending=[False, False, False, False, True, False],
        )
    )


def game_on_off(rows: pl.DataFrame, *, player_dtype: pl.DataType) -> pl.DataFrame:
    """Per player and game: the team with him on the floor and the rest of its game.

    Args:
        rows: Impact rows (:func:`~sportsdataverse._common.game_impact.impact_row_schema`).
        player_dtype: The league's player id dtype.

    Returns:
        One row per ``(season, game_id, team_id, player_id)`` in :func:`game_on_off_schema`: the
        ``_on`` and ``_off`` totals and ratings, ``<rating>_diff`` (on minus off; null when either
        side is) and every rating's ``_n`` (a difference's is its thinner side's). Sorted by game,
        team and minutes on (most first).
    """
    schema = game_on_off_schema(player_dtype)
    if rows.height == 0:
        return pl.DataFrame(schema=schema)
    sides = _sides(rows)
    sums = [pl.col(c).sum() for c in _TOTALS]
    team = sides.group_by(list(_KEYS)).agg(sums)
    on = (
        sides.explode("players")  # a stint with no known player is nobody's
        .drop_nulls("players")
        .group_by([*_KEYS, "players"])
        .agg(sums)
        .rename({"players": "player_id"})
    )
    if on.height == 0:
        return pl.DataFrame(schema=schema)
    out = on.rename({c: f"{c}_on" for c in _TOTALS}).join(
        team.rename({c: f"{c}_team" for c in _TOTALS}), on=list(_KEYS), how="left", validate="m:1", nulls_equal=True
    )
    out = out.with_columns((pl.col(f"{c}_team") - pl.col(f"{c}_on")).alias(f"{c}_off") for c in _TOTALS)
    out = _side_rates(_side_rates(out, "on"), "off").with_columns(
        (pl.col(f"{r}_on") - pl.col(f"{r}_off")).alias(f"{r}_diff") for r in _RATINGS
    )
    n = {f"{r}_{s}": _n_expr(r, s) for s in ("on", "off") for r in _RATINGS}
    n |= {f"{r}_diff": pl.min_horizontal(_n_expr(r, "on"), _n_expr(r, "off")) for r in _RATINGS}
    return (
        out.pipe(attach_sample_sizes, n)
        .select(list(schema))
        .cast(schema)
        .sort(
            ["season", "game_id", "team_id", "minutes_on", "player_id"], descending=[False, False, False, True, False]
        )
    )
