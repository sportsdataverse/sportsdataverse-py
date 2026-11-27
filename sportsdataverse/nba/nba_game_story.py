"""Basketball game story: how the winner won it (NBA, WNBA, men's and women's college).

The basketball adapter of :mod:`sportsdataverse._common.game_story`. It reads the hoopR / wehoop
frames :mod:`sportsdataverse.nba.nba_game_page` reads (``load_<lg>_pbp``, ``load_<lg>_team_boxscore``,
``load_<lg>_player_boxscore``, or :func:`~sportsdataverse.nba.nba_game_page.game_page_frames` on a live
ESPN payload).

**Accounting edges** split the margin exactly into the four factors. Each side's parts are the
:mod:`sportsdataverse._common.factor_points` parts, priced with that module's
:func:`~sportsdataverse._common.factor_points.league_prices` on the game's own two team rows (the two
teams pooled, not the league), and with one possession count for both sides: the mean of the two box
estimates ``FGA - OREB + TOV + w * FTA`` (``w`` 0.44 in the pros, 0.475 in college). With ``RC`` = own
OREB + the other side's DREB, ``c = g_ppp / (1 - g_tov)`` and every ``g_`` the game's pooled rate:

* ``shooting`` = ``2*FGM + 3PM - 2*g_efg*FGA + c*g_orb*(RC - g_rc_per_fga*FGA)``
* ``turnovers`` = ``(g_tov*P - TOV) * c``
* ``rebounding`` = ``(OREB - g_orb*RC) * c``
* ``free_throws`` = ``pts - g_ppp*P`` less the other three.

A side's four parts add up to ``pts - g_ppp*P``, so the winner's less the loser's is the margin.

**Split edges** are the box's points: ``three_pt_points`` (3 x 3PM), ``points_off_turnovers``,
``fast_break_points`` and ``points_in_paint`` (winner less loser). A box without a count drops that edge;
a game where either side lacks one of the four factors' counts gets no accounting edges (never a partial sum).

The moment is the play with the largest ``|wp_delta|`` from
:func:`~sportsdataverse.nba.nba_game_page.score_win_prob` (the pbp's own ``wp_delta`` when it has one).
The top performer is the side's leading scorer (ties: assists plus rebounds).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse._common.factor_points import _parts, _side, league_prices
from sportsdataverse._common.game_story import game_story
from sportsdataverse._common.possessions import ft_possession_weight
from sportsdataverse.nba.nba_game_page import game_regulation_periods, score_win_prob

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["nba_game_story"]

_LEAGUES = ("nba", "wnba", "mbb", "wbb")
_ACCOUNTING = (("shooting", "efg"), ("turnovers", "tov"), ("rebounding", "orb"), ("free_throws", "ft"))
#: split edge -> the team-box expression of its points
_SPLITS: dict[str, pl.Expr] = {
    "three_pt_points": 3 * pl.col("three_point_field_goals_made"),
    "points_off_turnovers": pl.col("turnover_points"),
    "fast_break_points": pl.col("fast_break_points"),
    "points_in_paint": pl.col("points_in_paint"),
}
_BOX_INT = (
    "field_goals_made",
    "field_goals_attempted",
    "three_point_field_goals_made",
    "free_throws_made",
    "free_throws_attempted",
    "offensive_rebounds",
    "defensive_rebounds",
    "team_score",
)


def _num(frame: pl.DataFrame, col: str) -> pl.Expr:
    """``col`` as Float64, null where the box lacks it."""
    return pl.col(col).cast(pl.Float64) if col in frame.columns else pl.lit(None, dtype=pl.Float64)


def _team_rows(team_box: pl.DataFrame, league: str) -> pl.DataFrame:
    """One row per game and team: the counts, the opponent's DREB and the shared possession count."""
    w = ft_possession_weight(league)
    tb = team_box.select(
        pl.col("game_id").cast(pl.Int64),
        pl.col("team_id").cast(pl.Int64),
        pl.col("team_home_away"),
        pl.col("season").cast(pl.Int64)
        if "season" in team_box.columns
        else pl.lit(None, dtype=pl.Int64).alias("season"),
        *[_num(team_box, c).alias(c) for c in _BOX_INT],
        # a box can leave total_turnovers (team turnovers included) null on one row: fall back per row
        pl.coalesce(_num(team_box, "total_turnovers"), _num(team_box, "turnovers")).alias("tov"),
        *[
            e.cast(pl.Float64).alias(k)
            for k, e in _SPLITS.items()
            if all(c in team_box.columns for c in e.meta.root_names())
        ],
    )
    est = (
        pl.col("field_goals_attempted")
        - pl.col("offensive_rebounds")
        + pl.col("tov")
        + w * pl.col("free_throws_attempted")
    )
    return tb.with_columns(
        pl.col("defensive_rebounds").reverse().over("game_id").alias("opp_dreb"),
        est.mean().over("game_id").alias("poss"),
        pl.len().over("game_id").alias("_n"),
    ).filter(pl.col("_n") == 2)


def _edges(rows: pl.DataFrame) -> pl.DataFrame:
    """The accounting and split edge rows (:data:`~sportsdataverse._common.game_story.EDGE_ROWS_SCHEMA`)."""
    # factor_points' frames: the o_ prefix is the side's offense; dreb is the defense's rebounds of its misses
    counts = rows.select(
        "game_id",
        "team_id",
        pl.col("game_id").alias("season"),  # ponytail: league_prices pools by "season"; the pool here is the game
        pl.col("team_score").alias("pts"),
        "poss",
        pl.col("field_goals_attempted").alias("o_fga"),
        pl.col("field_goals_made").alias("o_fgm"),
        pl.col("three_point_field_goals_made").alias("o_fg3m"),
        pl.col("offensive_rebounds").alias("o_oreb"),
        pl.col("opp_dreb").alias("o_dreb"),
        pl.col("tov").alias("o_tov"),
    )
    prices = league_prices(counts, convention="possession")
    side = _side("o", pl.col("o_oreb") + pl.col("o_dreb"), "pts", "poss")
    parts = _parts(side)
    priced = counts.join(prices, on="season", how="left", validate="m:1")
    acct = priced.select("game_id", "team_id", *[parts[f].alias(name) for name, f in _ACCOUNTING])
    # the parts add up to the margin only when both sides have every count: otherwise price none of the game
    names = [name for name, _ in _ACCOUNTING]
    whole = pl.all_horizontal(pl.col(n).is_not_null() & pl.col(n).is_not_nan() for n in names).all().over("game_id")
    acct = acct.with_columns(pl.when(whole).then(pl.col(n)).alias(n) for n in names)
    splits = [k for k in _SPLITS if k in rows.columns]
    long = []
    for i, (name, _) in enumerate(_ACCOUNTING):
        long.append(
            acct.select(
                "game_id",
                "team_id",
                pl.lit(name).alias("edge"),
                pl.lit(i).alias("edge_order"),
                pl.lit("accounting").alias("kind"),
                pl.col(name).alias("value"),
                pl.col(name).alias("points"),
            )
        )
    for j, name in enumerate(splits, start=len(_ACCOUNTING)):
        long.append(
            rows.select(
                "game_id",
                "team_id",
                pl.lit(name).alias("edge"),
                pl.lit(j).alias("edge_order"),
                pl.lit("split").alias("kind"),
                pl.col(name).alias("value"),
                pl.col(name).alias("points"),
            )
        )
    return (
        pl.concat(long, how="vertical_relaxed")
        .filter(pl.col("points").is_not_null() & pl.col("points").is_not_nan())
        .with_columns(pl.lit("points").alias("unit"))
    )


def _wp(pbp: pl.DataFrame, league: str) -> pl.DataFrame:
    """``pbp`` with ``wp_delta``: the pbp's own, else :func:`score_win_prob` per game."""
    if "wp_delta" in pbp.columns:
        return pbp
    pbp = pbp.sort("game_id", "game_play_number", maintain_order=True)  # score_win_prob reads plays in order
    parts = []
    for _, one in pbp.group_by("game_id", maintain_order=True):
        season = one["season"].drop_nulls()
        reg = game_regulation_periods(league, season=int(season[0]) if season.len() else None)
        neutral = bool(one["neutral_site"][0]) if "neutral_site" in one.columns else False
        parts.append(score_win_prob(one, league=league, regulation_periods=reg, neutral_site=neutral))
    return (
        pl.concat(parts, how="diagonal_relaxed")
        if parts
        else pbp.with_columns(pl.lit(None, dtype=pl.Float64).alias("wp_delta"))
    )


def nba_game_story(
    pbp: pl.DataFrame,
    team_box: pl.DataFrame,
    player_box: pl.DataFrame,
    *,
    league: str = "nba",
    return_as_pandas: bool = False,
) -> dict[str, pl.DataFrame] | dict[str, pd.DataFrame]:
    """How the winner won it, for one or many basketball games: edges in points, the moment, periods, runs, performers.

    Args:
        pbp: Plays in the hoopR / wehoop pbp schema (``load_<lg>_pbp`` or ``game_page_frames(...)["pbp"]``),
            one or many games. With ``wp_delta`` (the game page's scored plays) it is used as given;
            without, each game is scored with :func:`~sportsdataverse.nba.nba_game_page.score_win_prob`
            (a ``neutral_site`` column is honoured; otherwise home court counts).
        team_box: The games' team box rows (``load_<lg>_team_boxscore``); a game needs both rows for edges.
        player_box: The games' player box rows (``load_<lg>_player_boxscore``).
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"``.
        return_as_pandas: Return pandas frames instead of polars.

    Returns:
        ``{"summary", "edges", "moment", "periods", "runs", "performers"}``, the frames of
        :data:`~sportsdataverse._common.game_story.GAME_STORY_SCHEMAS`. Edges are the four accounting
        parts (``shooting``, ``turnovers``, ``rebounding``, ``free_throws``, which add up to the
        margin) and the box's point splits, ranked by ``points_worth``. Periods are quarters (``Q1``)
        or halves (``H1``), then ``OT``, ``OT2``.

    Raises:
        ValueError: If ``league`` is not one of the four.
        polars.exceptions.ColumnNotFoundError: If a frame lacks a column named above (``pbp``'s ``id``,
            ``game_play_number``, ``period_number``, ``clock_display_value``, ``team_id``, ``text``,
            ``home_score``, ``away_score``; the boxes' ids, counts and ``team_home_away``).

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba import load_nba_pbp, load_nba_player_boxscore, load_nba_team_boxscore
            from sportsdataverse.nba.nba_game_story import nba_game_story
            gid = 401705718
            pbp = load_nba_pbp([2025]).filter(pl.col("game_id") == gid)
            tb = load_nba_team_boxscore([2025]).filter(pl.col("game_id") == gid)
            pb = load_nba_player_boxscore([2025]).filter(pl.col("game_id") == gid)
            story = nba_game_story(pbp, tb, pb)
            story["edges"].head(3)

        A men's college game::

            story = nba_game_story(pbp, tb, pb, league="mbb")
            story["periods"]  # H1, H2 (and OT)

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college play-by-play
            * `wehoop`_ -- the R companion for WNBA and women's college play-by-play

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    if league not in _LEAGUES:
        raise ValueError(f"league must be one of {list(_LEAGUES)}, got {league!r}")
    rows = _team_rows(team_box, league)
    home = rows.filter(pl.col("team_home_away") == "home").select(
        "game_id",
        "season",
        pl.col("team_id").alias("home_team_id"),
        pl.col("team_score").cast(pl.Int64).alias("home_score"),
    )
    away = rows.filter(pl.col("team_home_away") == "away").select(
        "game_id", pl.col("team_id").alias("away_team_id"), pl.col("team_score").cast(pl.Int64).alias("away_score")
    )
    games = home.join(away, on="game_id", how="inner")
    regs = {s: game_regulation_periods(league, season=s) for s in games["season"].drop_nulls().unique().to_list()}
    games = games.with_columns(
        pl.col("season")
        .replace_strict(regs, default=game_regulation_periods(league), return_dtype=pl.Int64)
        .alias("regulation_periods")
    )
    pbp = pbp.with_columns(pl.col("game_id").cast(pl.Int64)).join(games.select("game_id"), on="game_id", how="semi")
    scored = _wp(pbp, league)
    plays = scored.select(
        "game_id",
        pl.col("id").cast(pl.Int64).alias("play_id"),
        pl.col("game_play_number").cast(pl.Int64).alias("order"),
        pl.col("period_number").cast(pl.Int64).alias("period"),
        pl.col("clock_display_value").cast(pl.Utf8).alias("clock"),
        pl.col("team_id").cast(pl.Int64),
        pl.col("text").cast(pl.Utf8),
        pl.col("home_score").cast(pl.Int64),
        pl.col("away_score").cast(pl.Int64),
        pl.col("wp_delta").cast(pl.Float64).alias("wpa"),
    )
    pb = player_box
    if "did_not_play" in pb.columns:
        pb = pb.filter(pl.col("did_not_play").fill_null(False) == False)
    players = pb.select(
        pl.col("game_id").cast(pl.Int64),
        pl.col("team_id").cast(pl.Int64),
        pl.col("athlete_id").cast(pl.Int64).alias("player_id"),
        pl.col("athlete_display_name").cast(pl.Utf8).alias("player_name"),
        pl.lit("points").alias("metric"),
        pl.col("points").cast(pl.Float64).alias("value"),
        (pl.col("assists").cast(pl.Float64) + pl.col("rebounds").cast(pl.Float64)).alias("tiebreak"),
    )
    story = game_story(games, _edges(rows), plays, players)
    if return_as_pandas:
        return {k: v.to_pandas() for k, v in story.items()}
    return story
