"""Game story: how the winner won it, as tidy frames (the shared core behind every ``<lg>_game_story``).

The core fits nothing and holds no constants: the league adapters price each edge with the models and
accounting the package already ships, and map their frames onto four inputs, all keyed by ``game_id``
(one game or many):

* ``games`` (:data:`GAMES_SCHEMA`) -- one row per game: the teams, the official final and the
  regulation period count.
* ``edges`` (:data:`EDGE_ROWS_SCHEMA`) -- one row per game, team and edge: the side's ``value`` in the
  edge's own unit (a rate, a count or points) and ``points``, its worth in the sport's scoring unit.
  ``kind`` is ``"accounting"`` for edges that partition the game (their winner-minus-loser worths add
  up to the margin, exactly or nearly, as each adapter documents) or ``"split"`` for overlapping cuts.
* ``plays`` (:data:`PLAY_ROWS_SCHEMA`) -- one row per play in game order: the running score after it
  and ``wpa``, the home win probability after the play less before it (null where unscored).
* ``players`` (:data:`PLAYER_ROWS_SCHEMA`) -- one row per game and player: the adapter's performer
  ``metric``, its ``value`` and a ``tiebreak``.

:func:`game_story` returns six frames (:data:`GAME_STORY_SCHEMAS`):

* ``summary`` -- the winner (null on a tie), the margin, whether the period sums reach the official
  final (``periods_agree``) and whether the game went to overtime.
* ``edges`` -- the winner's edges, ``points_worth`` = the winner's ``points`` less the loser's, ranked
  (1 = largest; equal worths keep the adapter's edge order). On a tie the home side stands in for the
  winner, so the frame reads from the home side's view.
* ``moment`` -- the play with the largest ``|wpa|`` (earliest on ties; ``rule`` ``"max_abs_wpa"``). A
  game with no ``wpa`` falls back to ``"deciding_score"``: the winner's score that put it ahead for
  good. A tie with no ``wpa`` has no moment row.
* ``periods`` -- each period's points from the running score (its last score less the previous
  period's), so the periods sum to the play-by-play's final. A period after regulation is overtime.
* ``runs`` -- each side's largest unanswered run. A score correction (the running score going down)
  stays in the run in progress and counts against that side, as in
  :mod:`sportsdataverse._common.game_flow`; both sides scoring on one row answers any run.
* ``performers`` -- each side's top player by the adapter's metric (then ``tiebreak``, then the smaller
  ``player_id``).

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package.
"""

from __future__ import annotations

import polars as pl

__all__ = [
    "EDGE_ROWS_SCHEMA",
    "GAMES_SCHEMA",
    "GAME_STORY_SCHEMAS",
    "PERIOD_PREFIX",
    "PLAYER_ROWS_SCHEMA",
    "PLAY_ROWS_SCHEMA",
    "empty_story",
    "game_story",
    "period_labels",
]

_G = "game_id"

GAMES_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Int64(),
    "home_team_id": pl.Int64(),
    "away_team_id": pl.Int64(),
    "home_score": pl.Int64(),
    "away_score": pl.Int64(),
    "regulation_periods": pl.Int64(),
}
EDGE_ROWS_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Int64(),
    "team_id": pl.Int64(),
    "edge": pl.Utf8(),
    "edge_order": pl.Int64(),
    "kind": pl.Utf8(),
    "unit": pl.Utf8(),
    "value": pl.Float64(),
    "points": pl.Float64(),
}
PLAY_ROWS_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Int64(),
    "play_id": pl.Int64(),
    "order": pl.Int64(),
    "period": pl.Int64(),
    "clock": pl.Utf8(),
    "team_id": pl.Int64(),
    "text": pl.Utf8(),
    "home_score": pl.Int64(),
    "away_score": pl.Int64(),
    "wpa": pl.Float64(),
}
PLAYER_ROWS_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Int64(),
    "team_id": pl.Int64(),
    "player_id": pl.Int64(),
    "player_name": pl.Utf8(),
    "metric": pl.Utf8(),
    "value": pl.Float64(),
    "tiebreak": pl.Float64(),
}

GAME_STORY_SCHEMAS: dict[str, dict[str, pl.DataType]] = {
    "summary": {
        "game_id": pl.Int64(),
        "home_team_id": pl.Int64(),
        "away_team_id": pl.Int64(),
        "home_score": pl.Int64(),
        "away_score": pl.Int64(),
        "winner_team_id": pl.Int64(),
        "loser_team_id": pl.Int64(),
        "margin": pl.Int64(),
        "decided": pl.Boolean(),
        "periods_agree": pl.Boolean(),
        "n_periods": pl.Int64(),
        "overtime": pl.Boolean(),
    },
    "edges": {
        "game_id": pl.Int64(),
        "winner_team_id": pl.Int64(),
        "loser_team_id": pl.Int64(),
        "edge": pl.Utf8(),
        "kind": pl.Utf8(),
        "unit": pl.Utf8(),
        "winner_value": pl.Float64(),
        "loser_value": pl.Float64(),
        "points_worth": pl.Float64(),
        "rank": pl.Int64(),
    },
    "moment": {
        "game_id": pl.Int64(),
        "play_id": pl.Int64(),
        "order": pl.Int64(),
        "period": pl.Int64(),
        "clock": pl.Utf8(),
        "team_id": pl.Int64(),
        "text": pl.Utf8(),
        "wpa": pl.Float64(),
        "winner_wpa": pl.Float64(),
        "home_score": pl.Int64(),
        "away_score": pl.Int64(),
        "rule": pl.Utf8(),
    },
    "periods": {
        "game_id": pl.Int64(),
        "period": pl.Int64(),
        "period_label": pl.Utf8(),
        "is_overtime": pl.Boolean(),
        "home_pts": pl.Int64(),
        "away_pts": pl.Int64(),
        "home_cum": pl.Int64(),
        "away_cum": pl.Int64(),
    },
    "runs": {
        "game_id": pl.Int64(),
        "team_id": pl.Int64(),
        "points": pl.Int64(),
        "start_order": pl.Int64(),
        "end_order": pl.Int64(),
        "start_period": pl.Int64(),
        "end_period": pl.Int64(),
        "home_score_after": pl.Int64(),
        "away_score_after": pl.Int64(),
    },
    "performers": {
        "game_id": pl.Int64(),
        "team_id": pl.Int64(),
        "player_id": pl.Int64(),
        "player_name": pl.Utf8(),
        "metric": pl.Utf8(),
        "value": pl.Float64(),
    },
}


def empty_story() -> dict[str, pl.DataFrame]:
    """The six frames of :data:`GAME_STORY_SCHEMAS` with zero rows.

    Returns:
        ``{name: zero-row frame}`` for each of the six frames, with its documented schema.
    """
    return {name: pl.DataFrame(schema=schema) for name, schema in GAME_STORY_SCHEMAS.items()}


#: regulation periods -> the label prefix: halves, hockey periods, quarters, innings (bare numbers)
PERIOD_PREFIX: dict[int, str] = {2: "H", 3: "P", 4: "Q", 9: ""}


def period_labels(period: pl.Expr, regulation: pl.Expr) -> pl.Expr:
    """``H1`` / ``P1`` / ``Q1`` / ``1`` (:data:`PERIOD_PREFIX` by regulation) within regulation, then ``OT``, ``OT2``, ...

    Args:
        period: The period number, from 1.
        regulation: The game's regulation period count.

    Returns:
        A Utf8 expression with the label.
    """
    ot = period - regulation
    prefix = regulation.replace_strict(PERIOD_PREFIX, default="", return_dtype=pl.Utf8)
    return (
        pl.when(period <= regulation)
        .then(prefix + period.cast(pl.Utf8))
        .when(ot == 1)
        .then(pl.lit("OT"))
        .otherwise(pl.lit("OT") + ot.cast(pl.Utf8))
    )


def _check(frame: pl.DataFrame, schema: dict[str, pl.DataType], name: str) -> pl.DataFrame:
    missing = sorted(set(schema) - set(frame.columns))
    if missing:
        raise KeyError(f"{name} missing columns: {missing}")
    return frame.select(list(schema)).cast(schema)


def _sides(games: pl.DataFrame) -> pl.DataFrame:
    """Winner / loser per game; on a tie the home side stands in for the winner."""
    home_won = pl.col("home_score") >= pl.col("away_score")
    return games.with_columns(
        (pl.col("home_score") != pl.col("away_score")).alias("decided"),
        pl.when(home_won).then("home_team_id").otherwise("away_team_id").alias("_w"),
        pl.when(home_won).then("away_team_id").otherwise("home_team_id").alias("_l"),
        (pl.col("home_score") - pl.col("away_score")).abs().alias("margin"),
    )


def _edges(sides: pl.DataFrame, edges: pl.DataFrame) -> pl.DataFrame:
    keys = sides.select(_G, "_w", "_l", "decided")
    w = edges.join(keys, left_on=[_G, "team_id"], right_on=[_G, "_w"], how="inner").select(
        _G, "edge", "edge_order", "kind", "unit", pl.col("value").alias("winner_value"), pl.col("points").alias("_pw")
    )
    lo = edges.join(keys, left_on=[_G, "team_id"], right_on=[_G, "_l"], how="inner").select(
        _G,
        "edge",
        pl.col("edge_order").alias("_lo"),
        pl.col("kind").alias("_lk"),
        pl.col("unit").alias("_lu"),
        pl.col("value").alias("loser_value"),
        pl.col("points").alias("_pl"),
    )
    out = (
        w.join(lo, on=[_G, "edge"], how="full", coalesce=True)
        # an edge only the loser has keeps its order, kind and unit
        .with_columns(
            pl.coalesce("edge_order", "_lo").alias("edge_order"),
            pl.coalesce("kind", "_lk").alias("kind"),
            pl.coalesce("unit", "_lu").alias("unit"),
        )
        .join(keys, on=_G, how="left")
        .with_columns((pl.col("_pw") - pl.col("_pl")).alias("points_worth"))
        .sort(_G, pl.col("points_worth").fill_null(float("-inf")), "edge_order", descending=[False, True, False])
        .with_columns(pl.int_range(1, pl.len() + 1, dtype=pl.Int64).over(_G).alias("rank"))
        .rename({"_w": "winner_team_id", "_l": "loser_team_id"})
    )
    # on a tie there is no winner: the home side's view, with null team ids so it is never read as one
    out = out.with_columns(
        pl.when(pl.col("decided")).then(pl.col("winner_team_id")).alias("winner_team_id"),
        pl.when(pl.col("decided")).then(pl.col("loser_team_id")).alias("loser_team_id"),
    )
    return out.select(list(GAME_STORY_SCHEMAS["edges"]))


def _scores(plays: pl.DataFrame) -> pl.DataFrame:
    """Plays in order, the running score carried forward (0-0 before the first), with each side's change."""
    p = plays.sort(_G, "order").with_columns(
        pl.col("home_score", "away_score").forward_fill().over(_G).fill_null(0),
        pl.col("period").forward_fill().over(_G),
    )
    return p.with_columns(
        (pl.col("home_score") - pl.col("home_score").shift(1).over(_G).fill_null(0)).alias("_dh"),
        (pl.col("away_score") - pl.col("away_score").shift(1).over(_G).fill_null(0)).alias("_da"),
    )


def _periods(sides: pl.DataFrame, sc: pl.DataFrame, labels: pl.DataFrame | None) -> pl.DataFrame:
    last = (
        sc.filter(pl.col("period").is_not_null())
        .group_by(_G, "period")
        .agg(pl.col("home_score").last().alias("home_cum"), pl.col("away_score").last().alias("away_cum"))
        .sort(_G, "period")
    )
    reg = sides.select(_G, "regulation_periods")
    out = last.join(reg, on=_G, how="left", maintain_order="left").with_columns(
        (pl.col("home_cum") - pl.col("home_cum").shift(1).over(_G).fill_null(0)).alias("home_pts"),
        (pl.col("away_cum") - pl.col("away_cum").shift(1).over(_G).fill_null(0)).alias("away_pts"),
        (pl.col("period") > pl.col("regulation_periods")).alias("is_overtime"),
        period_labels(pl.col("period"), pl.col("regulation_periods")).alias("period_label"),
    )
    if labels is not None:  # an adapter's own label for a period (hockey's shootout, "SO")
        out = out.join(labels.rename({"period_label": "_own"}), on=[_G, "period"], how="left").with_columns(
            pl.coalesce("_own", "period_label").alias("period_label")
        )
    return out.sort(_G, "period").select(list(GAME_STORY_SCHEMAS["periods"]))


def _runs(sides: pl.DataFrame, sc: pl.DataFrame) -> pl.DataFrame:
    """Each side's largest unanswered run (earliest on ties), as in ``game_flow._runs`` plus where it was."""
    dh, da = pl.col("_dh"), pl.col("_da")
    scorer = (
        pl.when((dh > 0) & (da <= 0))
        .then(pl.lit("home"))
        .when((da > 0) & (dh <= 0))
        .then(pl.lit("away"))
        .when((dh > 0) | (da > 0))
        .then(pl.lit("both"))
    )
    ch = sc.filter((dh != 0) | (da != 0)).with_columns(scorer.alias("_s"))
    ch = ch.with_columns(pl.col("_s").forward_fill().shift(1).over(_G).alias("_prev"))
    ch = ch.with_columns(
        (pl.col("_s").is_not_null() & (pl.col("_s") != pl.col("_prev")).fill_null(True))
        .cast(pl.Int64)
        .cum_sum()
        .over(_G)
        .alias("_run"),
        pl.col("_s").forward_fill().over(_G).alias("side"),
    )
    runs = (
        ch.filter(pl.col("side").is_in(["home", "away"]))
        .group_by(_G, "_run")
        .agg(
            pl.col("side").first(),
            pl.when(pl.col("side") == "home").then(dh).otherwise(da).sum().alias("points"),
            pl.col("order").first().alias("start_order"),
            pl.col("order").last().alias("end_order"),
            pl.col("period").first().alias("start_period"),
            pl.col("period").last().alias("end_period"),
            pl.col("home_score").last().alias("home_score_after"),
            pl.col("away_score").last().alias("away_score_after"),
        )
        .sort(_G, "side", "points", "start_order", descending=[False, False, True, False])
        .group_by(_G, "side", maintain_order=True)
        .first()
    )
    teams = sides.select(_G, "home_team_id", "away_team_id")
    return (
        runs.join(teams, on=_G, how="left")
        .with_columns(pl.when(pl.col("side") == "home").then("home_team_id").otherwise("away_team_id").alias("team_id"))
        .sort(_G, "side", descending=[False, True])  # home first
        .select(list(GAME_STORY_SCHEMAS["runs"]))
    )


def _moment(sides: pl.DataFrame, sc: pl.DataFrame) -> pl.DataFrame:
    keys = sides.select(_G, "_w", "home_team_id", "decided")
    wp = (
        sc.filter(pl.col("wpa").is_not_null() & pl.col("wpa").is_not_nan())
        .sort(_G, pl.col("wpa").abs(), "order", descending=[False, True, False])
        .group_by(_G, maintain_order=True)
        .first()
        .with_columns(pl.lit("max_abs_wpa").alias("rule"))
    )
    # no win probability: the winner's score that put it ahead for good
    no_wp = keys.join(wp.select(_G), on=_G, how="anti").filter(pl.col("decided"))
    lead = (
        sc.join(no_wp, on=_G, how="semi")
        .join(keys, on=_G, how="left")
        .sort(_G, "order")
        .with_columns(
            pl.when(pl.col("_w") == pl.col("home_team_id"))
            .then(pl.col("home_score") - pl.col("away_score"))
            .otherwise(pl.col("away_score") - pl.col("home_score"))
            .alias("_m")
        )
        .with_columns(
            pl.col("_m").shift(1).over(_G).fill_null(0).alias("_pm"),
            pl.col("_m").last().over(_G).alias("_final_m"),
        )
    )
    # only where the plays end with the winner ahead (otherwise no lead in them was "for good"), and only on the
    # winner's own score (a correction to the other side's score is not one)
    own = pl.when(pl.col("_w") == pl.col("home_team_id")).then(pl.col("_dh")).otherwise(pl.col("_da"))
    deciding = (
        lead.filter((pl.col("_m") > 0) & (pl.col("_pm") <= 0) & (pl.col("_final_m") > 0) & (own > 0))
        .group_by(_G, maintain_order=True)
        .last()
        .with_columns(pl.lit("deciding_score").alias("rule"))
    )
    cols = [c for c in GAME_STORY_SCHEMAS["moment"] if c != "winner_wpa"]
    both = pl.concat([wp.select(cols), deciding.select(cols)], how="vertical_relaxed")
    return (
        both.join(keys, on=_G, how="left")
        .with_columns(
            pl.when(pl.col("decided") == False)
            .then(None)
            .when(pl.col("_w") == pl.col("home_team_id"))
            .then(pl.col("wpa"))
            .otherwise(-pl.col("wpa"))
            .alias("winner_wpa")
        )
        .sort(_G)
        .select(list(GAME_STORY_SCHEMAS["moment"]))
    )


def _performers(players: pl.DataFrame) -> pl.DataFrame:
    return (
        players.filter(pl.col("value").is_not_null() & pl.col("value").is_not_nan() & pl.col("team_id").is_not_null())
        .sort(
            _G,
            "team_id",
            "value",
            pl.col("tiebreak").fill_null(float("-inf")),
            "player_id",
            descending=[False, False, True, True, False],
        )
        .group_by(_G, "team_id", maintain_order=True)
        .first()
        .select(list(GAME_STORY_SCHEMAS["performers"]))
    )


def game_story(
    games: pl.DataFrame,
    edges: pl.DataFrame,
    plays: pl.DataFrame,
    players: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """The six game-story frames from the four normalized inputs.

    Args:
        games: :data:`GAMES_SCHEMA` rows, one per game (extra columns ignored).
        edges: :data:`EDGE_ROWS_SCHEMA` rows. A team that is neither side of its game is dropped.
        plays: :data:`PLAY_ROWS_SCHEMA` rows, any order (sorted by ``order`` within each game). A
            null score means no change and is carried forward. An optional ``period_label`` column
            names a period in place of the default label (hockey's shootout, ``"SO"``).
        players: :data:`PLAYER_ROWS_SCHEMA` rows.

    Returns:
        ``{"summary", "edges", "moment", "periods", "runs", "performers"}`` in
        :data:`GAME_STORY_SCHEMAS`, each sorted by ``game_id``. Games without plays get no period,
        run or moment rows and ``periods_agree`` false; a game with a null final is dropped. An
        empty ``games`` returns :func:`empty_story`.

    Raises:
        KeyError: If an input is missing a schema column.
    """
    # a game without both finals has no winner to explain
    games = (
        _check(games, GAMES_SCHEMA, "games")
        .filter(pl.col("home_score").is_not_null() & pl.col("away_score").is_not_null())
        .unique(_G, keep="first", maintain_order=True)
    )
    if games.height == 0:
        return empty_story()
    edges = _check(edges, EDGE_ROWS_SCHEMA, "edges")
    labels = None
    if "period_label" in plays.columns:
        labels = (
            plays.select(
                pl.col(_G).cast(pl.Int64), pl.col("period").cast(pl.Int64), pl.col("period_label").cast(pl.Utf8)
            )
            .drop_nulls()
            .unique([_G, "period"], keep="first", maintain_order=True)
        )
    plays = _check(plays, PLAY_ROWS_SCHEMA, "plays").join(games.select(_G), on=_G, how="semi")
    players = _check(players, PLAYER_ROWS_SCHEMA, "players")
    sides = _sides(games)
    sc = _scores(plays)
    periods = _periods(sides, sc, labels)
    finals = periods.group_by(_G).agg(
        pl.col("home_pts").sum().alias("_hs"),
        pl.col("away_pts").sum().alias("_as"),
        pl.col("period").max().alias("n_periods"),
        pl.col("is_overtime").any().alias("overtime"),
    )
    summary = (
        sides.join(finals, on=_G, how="left")
        .with_columns(
            ((pl.col("_hs") == pl.col("home_score")) & (pl.col("_as") == pl.col("away_score")))
            .fill_null(False)
            .alias("periods_agree"),
            pl.col("overtime").fill_null(False),
            pl.when(pl.col("decided")).then("_w").alias("winner_team_id"),
            pl.when(pl.col("decided")).then("_l").alias("loser_team_id"),
        )
        .sort(_G)
        .select(list(GAME_STORY_SCHEMAS["summary"]))
    )
    out = {
        "summary": summary,
        "edges": _edges(sides, edges),
        "moment": _moment(sides, sc),
        "periods": periods,
        "runs": _runs(sides, sc),
        "performers": _performers(players.join(games.select(_G), on=_G, how="semi")),
    }
    return {k: v.cast(GAME_STORY_SCHEMAS[k]) for k, v in out.items()}
