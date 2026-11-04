"""Game impact box (BB-D6): the shared core behind ``{lg}_game_player_impact`` and ``{lg}_game_lineups_used``.

Both tables are built from one frame of **impact rows** (:func:`impact_row_schema`), each carrying the
ten players on the floor (``home_1..5`` / ``away_1..5``) and what happened while they were there:

* **Action rows**, one per play-by-play action: the game clock that ran into the action
  (``seconds``) and the points each team scored on it (``home_pts`` / ``away_pts``).
* **Possession rows**, one per possession row of the possessions release: ``home_poss`` /
  ``away_poss`` is 1 for the side that had the ball.

Points and minutes come from the actions, possessions from the possessions table. A possession's
lineup is the one on the floor when it starts, but substitutions happen at dead balls inside a
possession (after a foul, a timeout, a violation), so the points of a possession often belong to a
lineup that came on halfway through it. Crediting the points to the action that scored them is what
makes the on-court +/- equal the official box score's.

**Free throws** take the lineup on the floor at the most recent foul
(:func:`credit_free_throws_to_foul_lineup`), the official scorer's rule: a player subbed in between
two free throws is not credited with them. On the 2025-26 NBA season this moves the share of
player-games whose +/- equals stats.nba.com's from 37% to 91%; the rest are on-court lineup errors in
the source (the possession engine infers most 2025-26 lineups from substitutions).

Definitions, per player (or five-man unit) and game:

* ``seconds`` / ``minutes`` -- game clock on the floor.
* ``poss`` / ``opp_poss`` -- offensive / defensive possessions on the floor.
* ``pts`` / ``opp_pts`` -- the team's / the opponent's points while on the floor.
* ``plus_minus`` -- ``pts - opp_pts``.
* ``off_rtg`` / ``def_rtg`` -- ``pts`` / ``opp_pts`` per 100 ``poss`` / ``opp_poss``; ``net_rtg`` is
  their difference. Null with no possessions.
* ``<m>_n`` -- the sample size (BB-D2 grammar): ``poss`` for ``off_rtg``, ``opp_poss`` for
  ``def_rtg``, both for ``net_rtg``.

So per team and game, every sum over its players is five times the team's: ``poss``, ``opp_poss``,
``pts``, ``opp_pts``, ``seconds`` and ``plus_minus`` (five times the final margin). A row that lists
the same player twice (a source glitch) counts that player once, so the identity can miss by the
glitched rows. There are no ranks or percentiles: one game is not a cohort.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package.
"""

from __future__ import annotations

import polars as pl

__all__ = [
    "HOME_SLOTS",
    "AWAY_SLOTS",
    "credit_free_throws_to_foul_lineup",
    "game_lineups_used",
    "game_lineups_used_schema",
    "game_player_impact",
    "game_player_impact_schema",
    "impact_row_schema",
]

HOME_SLOTS = tuple(f"home_{i}" for i in range(1, 6))
AWAY_SLOTS = tuple(f"away_{i}" for i in range(1, 6))
_SLOTS = HOME_SLOTS + AWAY_SLOTS
_KEYS = ("season", "game_id", "team_id", "opp_team_id")
_SUMS = ("seconds", "poss", "opp_poss", "pts", "opp_pts")


def impact_row_schema(player_dtype: pl.DataType) -> dict[str, pl.DataType]:
    """The impact-row frame the two builders read; ``player_dtype`` is the league's player id dtype."""
    return {
        "season": pl.Int64(),
        "game_id": pl.Utf8(),
        "home_team_id": pl.Int64(),
        "away_team_id": pl.Int64(),
        **dict.fromkeys(_SLOTS, player_dtype),
        "seconds": pl.Float64(),
        "home_pts": pl.Int64(),
        "away_pts": pl.Int64(),
        "home_poss": pl.Int64(),
        "away_poss": pl.Int64(),
    }


_TOTALS: dict[str, pl.DataType] = {
    "minutes": pl.Float64(),
    "seconds": pl.Float64(),
    "poss": pl.Int64(),
    "opp_poss": pl.Int64(),
    "pts": pl.Int64(),
    "opp_pts": pl.Int64(),
    "plus_minus": pl.Int64(),
    "off_rtg": pl.Float64(),
    "def_rtg": pl.Float64(),
    "net_rtg": pl.Float64(),
    "off_rtg_n": pl.Int64(),
    "def_rtg_n": pl.Int64(),
    "net_rtg_n": pl.Int64(),
}
_KEY_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "game_id": pl.Utf8(),
    "team_id": pl.Int64(),
    "opp_team_id": pl.Int64(),
}


def game_player_impact_schema(player_dtype: pl.DataType) -> dict[str, pl.DataType]:
    """Columns of :func:`game_player_impact` for a league whose player ids are ``player_dtype``."""
    return {**_KEY_SCHEMA, "player_id": player_dtype, **_TOTALS}


def game_lineups_used_schema(player_dtype: pl.DataType) -> dict[str, pl.DataType]:
    """Columns of :func:`game_lineups_used` for a league whose player ids are ``player_dtype``."""
    players = {f"player_{i}": player_dtype for i in range(1, 6)}
    return {**_KEY_SCHEMA, "lineup_id": pl.Utf8(), **players, **_TOTALS}


def credit_free_throws_to_foul_lineup(
    actions: pl.DataFrame, *, is_free_throw: pl.Expr, is_foul: pl.Expr, game: str = "game_id"
) -> pl.DataFrame:
    """Give each free throw the lineup on the floor at the game's most recent foul.

    ``actions`` must be sorted in game order. A free throw with no earlier foul in the game keeps
    its own lineup.

    Args:
        actions: Action rows carrying ``home_1..5`` / ``away_1..5``, in game order.
        is_free_throw: True on a free-throw action.
        is_foul: True on a foul action (technical and flagrant fouls included).
        game: The game key column.

    Returns:
        ``actions`` with the ten slot columns replaced on free-throw rows.
    """
    # The whole lineup of the foul row (nulls included), never a slot-by-slot fill across fouls.
    marked = actions.with_row_index("_i").with_columns(
        pl.when(is_free_throw.fill_null(False))
        .then(pl.when(is_foul.fill_null(False)).then(pl.col("_i")).forward_fill().over(game))
        .alias("_foul_i")
    )
    at_foul = marked.select(pl.col("_i").alias("_foul_i"), *[pl.col(c).alias(f"_foul_{c}") for c in _SLOTS])
    joined = marked.join(at_foul, on="_foul_i", how="left").sort("_i")
    has = pl.col("_foul_i").is_not_null()
    return joined.with_columns(
        **{c: pl.when(has).then(pl.col(f"_foul_{c}")).otherwise(pl.col(c)) for c in _SLOTS}
    ).drop("_i", "_foul_i", *[f"_foul_{c}" for c in _SLOTS])


def _sides(rows: pl.DataFrame) -> pl.DataFrame:
    """Each impact row twice, once from each team's side, with that side's five players as a list."""
    return pl.concat(
        [
            rows.select(
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
            )
            for us, them, slots in (("home", "away", HOME_SLOTS), ("away", "home", AWAY_SLOTS))
        ]
    ).filter(pl.col("team_id").is_not_null())


def _per100(num: str, den: str) -> pl.Expr:
    """``num`` per 100 ``den``; null when ``den`` is 0."""
    return pl.when(pl.col(den) > 0).then(100 * pl.col(num) / pl.col(den))


def _rates(grouped: pl.DataFrame) -> pl.DataFrame:
    """Add minutes, +/-, the three ratings and their ``_n`` to summed totals."""
    return grouped.with_columns(
        (pl.col("seconds") / 60).alias("minutes"),
        (pl.col("pts") - pl.col("opp_pts")).alias("plus_minus"),
        _per100("pts", "poss").alias("off_rtg"),
        _per100("opp_pts", "opp_poss").alias("def_rtg"),
        pl.col("poss").alias("off_rtg_n"),
        pl.col("opp_poss").alias("def_rtg_n"),
        (pl.col("poss") + pl.col("opp_poss")).alias("net_rtg_n"),
    ).with_columns((pl.col("off_rtg") - pl.col("def_rtg")).alias("net_rtg"))


def _sum(frame: pl.DataFrame, by: list[str], *extra: pl.Expr) -> pl.DataFrame:
    """Sum the on-floor totals over ``by``."""
    return frame.group_by(by).agg(*[pl.col(c).sum() for c in _SUMS], *extra)


def game_player_impact(rows: pl.DataFrame, *, player_dtype: pl.DataType) -> pl.DataFrame:
    """Per player and game: minutes, on-court possessions, +/- and ORtg/DRtg/net.

    Args:
        rows: Impact rows (:func:`impact_row_schema`).
        player_dtype: The league's player id dtype.

    Returns:
        One row per ``(season, game_id, team_id, player_id)`` in
        :func:`game_player_impact_schema`, sorted by game, team and minutes (most first). A player
        whose side has no team id (a non-D1 college opponent) gets no row.
    """
    schema = game_player_impact_schema(player_dtype)
    if rows.height == 0:
        return pl.DataFrame(schema=schema)
    players = _sides(rows).explode("players").rename({"players": "player_id"}).drop_nulls("player_id")
    out = _rates(_sum(players, [*_KEYS, "player_id"]))
    return (
        out.select(list(schema))
        .cast(schema)
        .sort(["season", "game_id", "team_id", "minutes", "player_id"], descending=[False, False, False, True, False])
    )


def game_lineups_used(rows: pl.DataFrame, *, player_dtype: pl.DataType) -> pl.DataFrame:
    """Per five-man unit and game: minutes, possessions, +/- and ORtg/DRtg/net.

    Args:
        rows: Impact rows (:func:`impact_row_schema`).
        player_dtype: The league's player id dtype.

    Returns:
        One row per ``(season, game_id, team_id, lineup_id)`` in :func:`game_lineups_used_schema`.
        ``lineup_id`` is the five player ids, sorted and joined with ``-``; ``player_1..5`` are the
        same ids in that order. A unit is every stretch the same five shared the floor, so a unit
        that played twice in a game is one row. A stretch with fewer than five known players (a gap
        in the source lineup) is left out. Sorted by game, team and minutes (most first).
    """
    schema = game_lineups_used_schema(player_dtype)
    if rows.height == 0:
        return pl.DataFrame(schema=schema)
    sides = _sides(rows).filter(pl.col("players").list.len() == 5)
    sides = sides.with_columns(pl.col("players").cast(pl.List(pl.Utf8)).list.join("-").alias("lineup_id"))
    totals = _sum(sides, [*_KEYS, "lineup_id"], pl.col("players").first())
    out = _rates(totals).with_columns(
        pl.col("players").list.get(i, null_on_oob=True).alias(f"player_{i + 1}") for i in range(5)
    )
    return (
        out.select(list(schema))
        .cast(schema)
        .sort(["season", "game_id", "team_id", "minutes", "lineup_id"], descending=[False, False, False, True, False])
    )
