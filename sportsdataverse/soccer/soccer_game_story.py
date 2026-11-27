"""Soccer game story: how the winner won it, in goals, for any league ESPN covers.

The soccer adapter of :mod:`sportsdataverse._common.game_story`, on raw ESPN Site v2 match summaries
(``espn_soccer_summary(league, event_id, return_parsed=False)``): the competitors' final score, the
``keyEvents`` (goals, with their scorer and assister) and the box's shots on target
(:func:`~sportsdataverse.soccer.soccer_espn_parsers.parse_soccer_summary`'s ``team_stats``).

* **Accounting** (every goal once, so the winner-minus-loser worths add up to the margin exactly):
  ``open_play_goals``, ``set_piece_goals`` (a direct free kick, or a goal ESPN's text marks "following a
  corner" or "following a set piece situation"), ``penalty_goals`` and ``own_goals_for`` (an own goal, which
  ESPN credits to the team it counts for). A penalty shootout is not a period and adds no goal: a match
  decided on penalties is a draw in the score (``decided`` false).
* **Split**: ``shots_on_target`` (value: the count; points: the count times the match's goals per shot on
  target, so the gap is priced at the match's own rate).

The package has no soccer in-game win-probability model, so the moment is the ``deciding_score``: the
winner's goal that put it ahead for good. Periods are ``H1``, ``H2``, then ``ET1``, ``ET2``. The top
performer is the side's leader in goals plus assists (ties: goals), from the goal events' scorer and
assister.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

import polars as pl

from sportsdataverse._common.game_story import game_story
from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_summary

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["soccer_game_story"]

_REGULATION = 2
_SHOOTOUT = 5
_EDGES = (
    ("open_play_goals", "accounting"),
    ("set_piece_goals", "accounting"),
    ("penalty_goals", "accounting"),
    ("own_goals_for", "accounting"),
    ("shots_on_target", "split"),
)
_SET_PIECE = re.compile(r"following a (corner|set piece)", re.IGNORECASE)


def _int(x: Any) -> int | None:
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


def _goal_kind(kind: str, text: str) -> str:
    k = kind.lower()
    if "own goal" in k:
        return "own_goals_for"
    if "penalty" in k:
        return "penalty_goals"
    if "free-kick" in k or "free kick" in k or _SET_PIECE.search(text or ""):
        return "set_piece_goals"
    return "open_play_goals"


def _one(payload: dict) -> tuple[dict, list[dict], list[dict], pl.DataFrame]:
    """One summary's game row, goal rows, player rows and team stats."""
    comp = ((payload.get("header") or {}).get("competitions") or [{}])[0]
    by_side = {c.get("homeAway"): c for c in comp.get("competitors") or []}
    home, away = by_side.get("home") or {}, by_side.get("away") or {}
    gid = _int((payload.get("header") or {}).get("id"))
    game = {
        "game_id": gid,
        "home_team_id": _int((home.get("team") or {}).get("id")),
        "away_team_id": _int((away.get("team") or {}).get("id")),
        "home_score": _int(home.get("score")),
        "away_score": _int(away.get("score")),
    }
    goals, players = [], []
    for i, ev in enumerate(payload.get("keyEvents") or []):
        period = _int((ev.get("period") or {}).get("number"))
        if period is None or period >= _SHOOTOUT:
            continue
        scored = bool(ev.get("scoringPlay"))
        kind = ((ev.get("type") or {}).get("text")) or ""
        team = _int((ev.get("team") or {}).get("id"))
        who = [p.get("athlete") or {} for p in ev.get("participants") or []]
        goals.append(
            {
                "game_id": gid,
                "order": i,
                "play_id": _int(ev.get("id")),
                "period": period,
                "clock": (ev.get("clock") or {}).get("displayValue"),
                "team_id": team,
                "text": ev.get("text"),
                # every key event is a play (so a goalless half still has one); a goal has its kind
                "edge": _goal_kind(kind, ev.get("text") or "") if scored else None,
            }
        )
        if scored and "own goal" not in kind.lower():  # an own goal's player scored for the other side
            for j, a in enumerate(who[:2]):
                players.append(
                    {
                        "game_id": gid,
                        "team_id": team,
                        "player_id": _int(a.get("id")),
                        "player_name": a.get("displayName"),
                        "goal": float(j == 0),
                        "assist": float(j == 1),
                    }
                )
    stats = parse_soccer_summary(payload, section="team_stats")
    return (
        game,
        goals,
        players,
        stats if isinstance(stats, pl.DataFrame) else pl.DataFrame(),
    )


def _frames(summaries: dict | list[dict]) -> tuple[pl.DataFrame, pl.DataFrame, list[pl.DataFrame], list[dict]]:
    """The games, every key event with the running score after it, the box's shots on target and the goal participants."""
    payloads = [summaries] if isinstance(summaries, dict) else list(summaries)
    games, goals, players, stats = [], [], [], []
    for p in payloads:
        g, gl, pr, st = _one(p)
        if g["game_id"] is None or g["home_score"] is None or g["away_score"] is None:
            continue
        games.append(g)
        goals += gl
        players += pr
        if st.height and {"team_id", "shots_on_target"} <= set(st.columns):
            stats.append(
                st.select(
                    pl.lit(g["game_id"], dtype=pl.Int64).alias("game_id"),
                    pl.col("team_id").cast(pl.Int64, strict=False),
                    pl.col("shots_on_target").cast(pl.Float64, strict=False),
                )
            )
    gschema = {
        "game_id": pl.Int64,
        "home_team_id": pl.Int64,
        "away_team_id": pl.Int64,
        "home_score": pl.Int64,
        "away_score": pl.Int64,
    }
    gdf = pl.DataFrame(games, schema=gschema).with_columns(
        pl.lit(_REGULATION, dtype=pl.Int64).alias("regulation_periods")
    )
    goal_schema = {
        "game_id": pl.Int64,
        "order": pl.Int64,
        "play_id": pl.Int64,
        "period": pl.Int64,
        "clock": pl.Utf8,
        "team_id": pl.Int64,
        "text": pl.Utf8,
        "edge": pl.Utf8,
    }
    gl = pl.DataFrame(goals, schema=goal_schema).join(
        gdf.select("game_id", "home_team_id", "away_team_id"), on="game_id"
    )
    # the running score after each event
    goal = pl.col("edge").is_not_null()
    gl = gl.sort("game_id", "order").with_columns(
        (goal & (pl.col("team_id") == pl.col("home_team_id")))
        .fill_null(False)
        .cast(pl.Int64)
        .cum_sum()
        .over("game_id")
        .alias("home_score"),
        (goal & (pl.col("team_id") == pl.col("away_team_id")))
        .fill_null(False)
        .cast(pl.Int64)
        .cum_sum()
        .over("game_id")
        .alias("away_score"),
    )
    return gdf, gl, stats, players


def _soccer_edges(gl: pl.DataFrame, gdf: pl.DataFrame, stats: list[pl.DataFrame]) -> pl.DataFrame:
    """The goals-by-kind accounting edges and the shots-on-target split for every game and side."""
    goal = pl.col("edge").is_not_null()
    teams = pl.concat(
        [
            gdf.select("game_id", pl.col("home_team_id").alias("team_id")),
            gdf.select("game_id", pl.col("away_team_id").alias("team_id")),
        ]
    )
    found = gl.filter(goal).group_by("game_id", "team_id", "edge").agg(pl.len().cast(pl.Float64).alias("value"))
    found = found.with_columns(pl.col("value").alias("points"))
    if stats:
        sot = pl.concat(stats).join(teams, on=["game_id", "team_id"], how="semi")
        goals_per_sot = gdf.select(
            "game_id",
            ((pl.col("home_score") + pl.col("away_score")).cast(pl.Float64)).alias("_g"),
        ).join(
            sot.group_by("game_id").agg(pl.col("shots_on_target").sum().alias("_s")),
            on="game_id",
        )
        found = pl.concat(
            [
                found,
                sot.join(goals_per_sot, on="game_id").select(
                    "game_id",
                    "team_id",
                    pl.lit("shots_on_target").alias("edge"),
                    pl.col("shots_on_target").alias("value"),
                    pl.when(pl.col("_s") > 0)
                    .then(pl.col("shots_on_target") * pl.col("_g") / pl.col("_s"))
                    .alias("points"),
                ),
            ],
            how="vertical_relaxed",
        )
    edges_used = [e for e in _EDGES if e[1] == "accounting" or stats]
    order = {e: i for i, (e, _) in enumerate(edges_used)}
    edges = (
        teams.join(pl.DataFrame({"edge": list(order)}), how="cross")
        .join(found, on=["game_id", "team_id", "edge"], how="left")
        .with_columns(
            pl.when(pl.col("edge") == "shots_on_target")
            .then(pl.col("value"))
            .otherwise(pl.col("value").fill_null(0.0))
            .alias("value"),
            pl.when(pl.col("edge") == "shots_on_target")
            .then(pl.col("points"))
            .otherwise(pl.col("points").fill_null(0.0))
            .alias("points"),
            pl.col("edge").replace_strict(order, return_dtype=pl.Int64).alias("edge_order"),
            pl.col("edge").replace_strict(dict(edges_used), return_dtype=pl.Utf8).alias("kind"),
            pl.lit("goals").alias("unit"),
        )
        .filter(pl.col("points").is_not_null())
    )
    return edges


def soccer_game_story(
    summaries: dict | list[dict],
    *,
    return_as_pandas: bool = False,
) -> dict[str, pl.DataFrame] | dict[str, pd.DataFrame]:
    """How the winner won it, for one or many soccer matches: edges in goals, the moment, halves, runs, performers.

    Args:
        summaries: One raw ESPN match summary or a list of them
            (``espn_soccer_summary(league, event_id, return_parsed=False)``), any league.
        return_as_pandas: Return pandas frames instead of polars.

    Returns:
        ``{"summary", "edges", "moment", "periods", "runs", "performers"}``, the frames of
        :data:`~sportsdataverse._common.game_story.GAME_STORY_SCHEMAS`: the goals by kind (adding up
        to the margin exactly) and shots on target, in goals and ranked; the winner's go-ahead-for-good
        goal (``rule`` ``deciding_score``); ``H1``, ``H2``, ``ET1``, ``ET2``; each side's largest unanswered
        run of goals; the goals-plus-assists leader. A summary without both teams' final score is
        skipped.

    Example:
        Quick start::

            from sportsdataverse.soccer.soccer_espn_ext import espn_soccer_summary
            from sportsdataverse.soccer.soccer_game_story import soccer_game_story
            story = soccer_game_story(espn_soccer_summary("eng.1", event_id=704279, return_parsed=False))
            story["edges"]

        A matchday::

            from sportsdataverse.soccer.soccer_espn_ext import espn_soccer_scoreboard
            day = espn_soccer_scoreboard("eng.1", dates="20250525", return_parsed=False)
            ids = [e["id"] for e in day["events"]]
            soccer_game_story([espn_soccer_summary("eng.1", event_id=i, return_parsed=False) for i in ids])

        See Also:
            * :func:`~sportsdataverse.soccer.soccer_espn_parsers.parse_soccer_summary` -- the summary's tidy frames
            * :func:`~sportsdataverse.soccer.asa.asa_games_xgoals` -- match xG for MLS, NWSL and USL
    """
    gdf, gl, stats, players = _frames(summaries)
    plays = gl.select(
        "game_id",
        "play_id",
        "order",
        "period",
        "clock",
        "team_id",
        "text",
        "home_score",
        "away_score",
        pl.lit(None, dtype=pl.Float64).alias("wpa"),
        pl.when(pl.col("period") > _REGULATION)
        .then(pl.lit("ET") + (pl.col("period") - _REGULATION).cast(pl.Utf8))
        .alias("period_label"),
    )
    edges = _soccer_edges(gl, gdf, stats)
    pschema = {
        "game_id": pl.Int64,
        "team_id": pl.Int64,
        "player_id": pl.Int64,
        "player_name": pl.Utf8,
        "goal": pl.Float64,
        "assist": pl.Float64,
    }
    perf = (
        pl.DataFrame(players, schema=pschema)
        .filter(pl.col("player_id").is_not_null())
        .group_by("game_id", "team_id", "player_id")
        .agg(
            pl.col("player_name").drop_nulls().first(),
            (pl.col("goal").sum() + pl.col("assist").sum()).alias("value"),
            pl.col("goal").sum().alias("tiebreak"),
        )
        .with_columns(pl.lit("goals_assists").alias("metric"))
    )
    story = game_story(gdf, edges, plays, perf)
    if return_as_pandas:
        return {k: v.to_pandas() for k, v in story.items()}
    return story
