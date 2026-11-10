# sportsdataverse/nba/nba_game_splits.py
"""Game situational splits for one NBA, WNBA, men's or women's college basketball game (BB-D31).

:func:`team_game_splits` and :func:`player_game_splits` count from the ESPN play-by-play in the
hoopR / wehoop ``load_<league>_pbp`` schema after
:func:`~sportsdataverse.nba.nba_game_page.shot_coordinates`, so they work on a game in progress;
:func:`~sportsdataverse.nba.nba_game_page.game_page_tables` (and so ``game_page_frames``, the live
Game on Paper processor) returns both.

Definitions:

* **Possession** -- counted, not estimated. A field-goal attempt, a free throw, a turnover or a
  rebound names the offense (a defensive rebound is the new offense's first play), and a possession
  runs until the next such play is the other team's or the period ends. On the test fixtures the
  count runs 0.95-1.01 of the box estimate (FGA - ORB + TOV + 0.44 FTA) in the pros and 0.99-1.05 in
  college, where 0.44 undercounts the one-and-one trips. A technical free throw neither starts nor
  ends a possession; its points land in ``non_possession_points`` when the shooting team was on
  defense. ESPN's college feed has no technical marker on its free throws, so a college technical
  counts as a possession of the shooting team.
* **Transition** -- the possession started live (after a made shot, a defensive rebound or a steal;
  not a period's first) and took its first field-goal attempt within
  :data:`TRANSITION_WINDOW_SECONDS` of the change of possession, with no timeout in between.
  **Half-court** is every other possession.
* **Second chance** -- the possession had an offensive rebound, team rebounds included, but not the
  dead-ball one between two free throws of a trip, one after a technical free throw, or one with no
  time left. **First chance** is every other possession.
* **Live turnover** -- a steal is credited on it: in the turnover's text, or (college) a ``Steal``
  play by the other team beside it within :data:`STEAL_GAP_SECONDS`. ``pts_off_live_tov`` /
  ``pts_off_dead_tov`` are the points a team scored on possessions started by the opponent's live /
  dead turnovers.
* **Shot zones** -- ``rim`` is a two under 4 feet (play coordinates), ``mid`` any other located two,
  ``corner3`` a three at or below 8.75 feet above the rim's line (``y_legacy`` 87.5) and
  ``above_break3`` any other located three; plays without a location count as ``fg2_unlocated`` /
  ``fg3_unlocated``. A three is ``points_attempted`` 3; a frame without that column (older
  captures) reads the text ("three point", or 24 feet or more) and the location (22 feet or more to
  the side, or 23.75 out). ``layup_dunk`` is any shot whose play type names a layup or a dunk, and
  an assisted make is one whose text credits an assist.

Each split pair partitions the team's possessions and their points: ``transition`` + ``half_court``
and ``second_chance`` + ``first_chance`` add up to ``possessions`` and to ``points -
non_possession_points``; ``rim + mid + fg2_unlocated + fg3 = fga`` and ``corner3 + above_break3 +
fg3_unlocated = fg3``.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse._common.game_flow import _runs, _states
from sportsdataverse.nba.nba_game_page import (
    _REGULATION_MINUTES,
    _box_season,
    _period_clock,
    _ratio,
    _resolve_league,
    game_regulation_periods,
)
from sportsdataverse.nba.nba_play_context_constants import CORNER_THREE_MAX_Y, RIM_DISTANCE_FT
from sportsdataverse.nba.nba_possession_flags import TRANSITION_SECONDS

__all__ = [
    "STEAL_GAP_SECONDS",
    "TRANSITION_WINDOW_SECONDS",
    "player_game_splits",
    "team_game_splits",
]

#: The first-shot window of a transition possession: BB-D7's 8 seconds for the pros, the college
#: possession release's (bigballR's) 10 seconds for men's and women's college basketball.
TRANSITION_WINDOW_SECONDS: dict[str, float] = {
    "nba": TRANSITION_SECONDS,
    "wnba": TRANSITION_SECONDS,
    "mbb": 10.0,
    "wbb": 10.0,
}
#: A college ``Steal`` play this many seconds or less from its turnover marks it live (2013-14 WBB
#: logs the steal one to two seconds after the turnover).
STEAL_GAP_SECONDS: float = 3.0
#: How a possession started: live after a ``made_shot``, a ``def_rebound`` or a ``live_turnover``,
#: dead after a ``dead_turnover``, at a ``period_start``, or ``other``. Only the first three can be transition.
_LIVE_STARTS: tuple[str, ...] = ("made_shot", "def_rebound", "live_turnover")
_FAMILIES: tuple[str, ...] = ("transition", "half_court", "second_chance", "first_chance")
_ZONES: tuple[str, ...] = (
    "rim",
    "mid",
    "fg2_unlocated",
    "fg3",
    "corner3",
    "above_break3",
    "fg3_unlocated",
    "layup_dunk",
)

_TEAM_SPLITS_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Int32,
    "team_id": pl.Int32,
    "points": pl.Int64,
    "possessions": pl.Int64,
    "ppp": pl.Float64,
    "non_possession_points": pl.Int64,
    "avg_possession_seconds": pl.Float64,
    **{
        f"{family}_{m}": dt
        for family in _FAMILIES
        for m, dt in (("poss", pl.Int64), ("pts", pl.Int64), ("ppp", pl.Float64))
    },
    "tov": pl.Int64,
    "live_tov": pl.Int64,
    "dead_tov": pl.Int64,
    "tov_pct": pl.Float64,
    "pts_off_live_tov": pl.Int64,
    "pts_off_dead_tov": pl.Int64,
    "fga": pl.Int64,
    "fgm": pl.Int64,
    **{
        f"{zone}_{m}": dt for zone in _ZONES for m, dt in (("fga", pl.Int64), ("fgm", pl.Int64), ("fg_pct", pl.Float64))
    },
    "rim_rate": pl.Float64,
    "fg3a_rate": pl.Float64,
    "avg_fg2_distance": pl.Float64,
    "assisted_fgm": pl.Int64,
    "assisted_share": pl.Float64,
    "fta": pl.Int64,
    "ftm": pl.Int64,
    "ft_rate": pl.Float64,
    "bench_points": pl.Int64,
    "largest_lead": pl.Int64,
    "max_run": pl.Int64,
}
_PLAYER_SPLITS_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Int32,
    "athlete_id": pl.Int32,
    "team_id": pl.Int32,
    "minutes": pl.Float64,
    "est_possessions": pl.Float64,
    "points": pl.Int64,
    "fga": pl.Int64,
    "fgm": pl.Int64,
    "assisted_fgm": pl.Int64,
    "assisted_share": pl.Float64,
    "rim_fga": pl.Int64,
    "rim_fgm": pl.Int64,
    "rim_assisted_fgm": pl.Int64,
    "rim_assisted_share": pl.Float64,
    "fg3a": pl.Int64,
    "fg3m": pl.Int64,
    "fg3_assisted_fgm": pl.Int64,
    "fg3_assisted_share": pl.Float64,
    "assists": pl.Int64,
    "rebounds": pl.Int64,
    "tov": pl.Int64,
    "live_tov": pl.Int64,
    **{f"{m}_per100": pl.Float64 for m in ("pts", "ast", "reb", "tov")},
}


def _shot_kind() -> pl.Expr:
    """``"fga"``, ``"ft"``, ``"tech"`` (a technical free throw) or null, from the ``_flags`` columns."""
    return (
        pl.when(pl.col("_fga"))
        .then(pl.lit("fga"))
        .when(pl.col("_tech"))
        .then(pl.lit("tech"))
        .when(pl.col("_ft"))
        .then(pl.lit("ft"))
    )


def _three(pbp: pl.DataFrame) -> pl.Expr:
    """A three: ``points_attempted`` 3 where the frame has it; otherwise the text and the location.

    Older NBA misses read "misses 25-foot" or nothing, so without ``points_attempted`` a shot whose
    text gives 24 feet or more, or that sits 22 feet or more to the side or 23.75 feet or more out
    (past every league's arc), is a three unless the text says "two point".
    """
    txt = pl.col("text").fill_null("")
    if "points_attempted" in pbp.columns:
        return (pl.col("points_attempted") == 3).fill_null(False) | txt.str.contains("(?i)three point")
    x, y = pl.col("x_legacy").cast(pl.Float64), pl.col("y_legacy").cast(pl.Float64)
    beyond = ((x.abs() >= 220.0) | ((x**2 + y**2).sqrt() >= 237.5)).fill_null(False)
    return txt.str.contains("(?i)three point") | (
        (txt.str.contains(r"\b(2[4-9]|[3-9]\d)-foot") | beyond) & ~txt.str.contains("(?i)two point")
    )


def _flags(pbp: pl.DataFrame) -> pl.DataFrame:
    """Every play with its event flags (``_fga``, ``_ft``, ``_tov``, ...) and its score change per side."""
    tt, txt = pl.col("type_text").fill_null(""), pl.col("text").fill_null("")
    shoot = pl.col("shooting_play").fill_null(False)
    is_ft = shoot & tt.str.contains("(?i)free ?throw")
    clk, per, team = pl.col("_clk"), pl.col("period_number"), pl.col("team_id")
    steal = tt == "Steal"  # college logs the steal as its own play beside the turnover

    def beside(n: int) -> pl.Expr:
        near = (clk.shift(n) - clk).abs() <= STEAL_GAP_SECONDS
        return steal.shift(n) & near & (per.shift(n) == per) & (team.shift(n) != team)

    rows = pbp.with_row_index("_i").with_columns(
        _period_clock(pbp).alias("_clk"),
        team.cast(pl.Int32).alias("_team"),
        *[
            pl.col(c).forward_fill().fill_null(0).cast(pl.Int64).alias(a)
            for c, a in (("home_score", "_hs"), ("away_score", "_as"))
        ],
    )
    return rows.with_columns(
        (shoot & ~is_ft).alias("_fga"),
        (shoot & ~is_ft & _three(pbp)).alias("_three"),
        is_ft.alias("_ft"),
        (is_ft & (tt.str.contains("(?i)technical") | txt.str.contains("(?i)technical"))).alias("_tech"),
        pl.col("scoring_play").fill_null(False).alias("_made"),
        (tt.str.contains("(?i)turnover") | (tt == "Traveling")).alias("_tov"),
        tt.str.contains("(?i)rebound").alias("_reb"),
        (txt.str.contains("(?i)steal") | (beside(1) | beside(-1)).fill_null(False)).alias("_stolen"),
        tt.str.contains("(?i)time ?out").cast(pl.Int64).cum_sum().alias("_timeouts"),
        (pl.col("_hs") - pl.col("_hs").shift(1, fill_value=0)).alias("_dh"),
        (pl.col("_as") - pl.col("_as").shift(1, fill_value=0)).alias("_da"),
    )


def _split_events(pbp: pl.DataFrame) -> pl.DataFrame:
    """:func:`_flags` plus the possession each play belongs to (``_poss``), its start (``_new``) and ``_oreb``.

    Every play that does not name the offense (fouls, substitutions, timeouts, steals) belongs to the
    possession in progress.
    """
    home, away = int(pbp["home_team_id"][0]), int(pbp["away_team_id"][0])
    clk, per = pl.col("_clk"), pl.col("period_number")
    rows = _flags(pbp).with_columns(
        (
            (pl.col("_fga") | (pl.col("_ft") & ~pl.col("_tech")) | pl.col("_tov") | pl.col("_reb"))
            & pl.col("_team").is_in([home, away])
        ).alias("_off"),
        # the most recent shot before the play, and the next shot after it, with its clock
        _shot_kind().forward_fill().shift(1).alias("_prev_shot"),
        _shot_kind().backward_fill().shift(-1).alias("_next_shot"),
        pl.when(_shot_kind().is_not_null()).then(clk).backward_fill().shift(-1).alias("_next_shot_clk"),
    )
    ev = rows.filter(pl.col("_off")).with_columns(
        ((pl.col("_team") != pl.col("_team").shift(1)) | (per != per.shift(1))).fill_null(True).alias("_new")
    )
    dead_ball = (
        (pl.col("_prev_shot") == "ft") & pl.col("_next_shot").is_in(["ft", "tech"]) & (pl.col("_next_shot_clk") == clk)
    ).fill_null(False)
    ev = ev.with_columns(
        pl.col("_new").cast(pl.Int64).cum_sum().alias("_poss"),
        (
            pl.col("_reb") & ~pl.col("_new") & (clk > 0) & (pl.col("_prev_shot") != "tech").fill_null(True) & ~dead_ball
        ).alias("_oreb"),
    )
    tagged = rows.join_asof(ev.select("_i", "_poss"), on="_i", strategy="backward")
    return tagged.join(ev.select("_i", "_new", "_oreb"), on="_i", how="left", maintain_order="left").with_columns(
        pl.col("_new", "_oreb").fill_null(False), pl.lit(home).alias("_home"), pl.lit(away).alias("_away")
    )


def _start_type() -> pl.Expr:
    """How each possession (one row each, in order) started, from the previous possession's end."""
    same = (pl.col("period") == pl.col("period").shift(1)).fill_null(False)
    return (
        pl.when(~same)
        .then(pl.lit("period_start"))
        .when(pl.col("_starts_on_reb"))
        .then(pl.lit("def_rebound"))
        .when(pl.col("_ends_made").shift(1))
        .then(pl.lit("made_shot"))
        .when(pl.col("_ends_live_tov").shift(1))
        .then(pl.lit("live_turnover"))
        .when(pl.col("_ends_tov").shift(1))
        .then(pl.lit("dead_turnover"))
        .otherwise(pl.lit("other"))
    )


def _possessions(rows: pl.DataFrame, lg: str, reg: int) -> pl.DataFrame:
    """One row per possession: offense, start type, transition, second chance, points, length."""
    poss = (
        rows.filter(pl.col("_off"))
        .group_by("_poss", maintain_order=True)
        .agg(
            pl.col("period_number").first().alias("period"),
            pl.col("_team").first().alias("offense"),
            pl.col("_reb").first().alias("_starts_on_reb"),
            pl.col("_i").last().alias("_last_i"),
            pl.col("_clk").last().alias("_end_clk"),
            (pl.col("_made") & (pl.col("_fga") | pl.col("_ft"))).last().alias("_ends_made"),
            pl.col("_tov").last().alias("_ends_tov"),
            (pl.col("_tov") & pl.col("_stolen")).last().alias("_ends_live_tov"),
            pl.col("_i").filter(pl.col("_fga")).first().alias("_shot_i"),
            pl.col("_clk").filter(pl.col("_fga")).first().alias("_shot_clk"),
            pl.col("_oreb").any().alias("second_chance"),
        )
        .sort("_poss")
    )
    same = (pl.col("period") == pl.col("period").shift(1)).fill_null(False)
    period_secs = pl.when(pl.col("period") <= reg).then(pl.lit(60.0 * _REGULATION_MINUTES[lg] / reg)).otherwise(300.0)
    poss = poss.with_columns(
        _start_type().alias("start_type"),
        pl.when(same).then(pl.col("_end_clk").shift(1)).otherwise(period_secs).alias("_start_clk"),
        pl.col("_last_i").shift(1).alias("_prev_i"),
    )
    # a timeout between the change of possession and the first shot is a set play, not transition
    timeouts = rows["_timeouts"]
    stopped = [
        s is not None and p is not None and timeouts[s] > timeouts[p]
        for s, p in zip(poss["_shot_i"].to_list(), poss["_prev_i"].to_list(), strict=True)
    ]
    own = pl.when(pl.col("offense") == pl.col("_home")).then(pl.col("_dh")).otherwise(pl.col("_da"))
    pts = (
        rows.filter(pl.col("_poss").is_not_null())
        .join(poss.select("_poss", "offense"), on="_poss")
        .group_by("_poss")
        .agg(own.sum().alias("pts"))
    )
    window = TRANSITION_WINDOW_SECONDS[lg]
    quick = ((pl.col("_start_clk") - pl.col("_shot_clk")) <= window).fill_null(False)
    return (
        poss.with_columns(pl.Series("_stopped", stopped, dtype=pl.Boolean))
        .join(pts, on="_poss", how="left", maintain_order="left")
        .with_columns(
            (pl.col("start_type").is_in(_LIVE_STARTS) & quick & ~pl.col("_stopped")).alias("transition"),
            (pl.col("_start_clk") - pl.col("_end_clk")).clip(lower_bound=0.0).alias("seconds"),
            pl.col("pts").fill_null(0),
        )
    )


def _zone_counts(shots: pl.DataFrame, by: list[str]) -> pl.DataFrame:
    """FGA / FGM by zone, assisted makes, and the mean located-two distance, per ``by`` group."""
    dist = (pl.col("x_legacy") ** 2 + pl.col("y_legacy") ** 2).sqrt() / 10.0
    three, made = pl.col("_three"), pl.col("_made")
    located = pl.col("x_legacy").is_not_null() & pl.col("y_legacy").is_not_null()
    assisted = made & pl.col("text").fill_null("").str.contains("(?i)assist")
    zones = {
        "rim": ~three & located & (dist < RIM_DISTANCE_FT),
        "mid": ~three & located & (dist >= RIM_DISTANCE_FT),
        "fg2_unlocated": ~three & ~located,
        "fg3": three,
        "corner3": three & located & (pl.col("y_legacy") <= CORNER_THREE_MAX_Y),
        "above_break3": three & located & (pl.col("y_legacy") > CORNER_THREE_MAX_Y),
        "fg3_unlocated": three & ~located,
        "layup_dunk": pl.col("type_text").fill_null("").str.contains("(?i)lay ?up|dunk"),
    }
    return shots.group_by(by).agg(
        pl.len().cast(pl.Int64).alias("fga"),
        made.sum().cast(pl.Int64).alias("fgm"),
        *[m.sum().cast(pl.Int64).alias(f"{z}_fga") for z, m in zones.items()],
        *[(m & made).sum().cast(pl.Int64).alias(f"{z}_fgm") for z, m in zones.items()],
        assisted.sum().cast(pl.Int64).alias("assisted_fgm"),
        dist.filter(~three & located).mean().alias("avg_fg2_distance"),
        *[(assisted & zones[z]).sum().cast(pl.Int64).alias(f"{z}_assisted_fgm") for z in ("rim", "fg3")],
    )


def _offense_rows(poss: pl.DataFrame) -> pl.DataFrame:
    """Per offense team: possessions, possession points, length, the four families, points off turnovers."""

    def family(name: str, mask: pl.Expr) -> list[pl.Expr]:
        return [
            mask.sum().cast(pl.Int64).alias(f"{name}_poss"),
            pl.col("pts").filter(mask).sum().cast(pl.Int64).alias(f"{name}_pts"),
        ]

    started = pl.col("start_type")
    return poss.group_by(pl.col("offense").alias("team_id")).agg(
        pl.len().cast(pl.Int64).alias("possessions"),
        pl.col("pts").sum().cast(pl.Int64).alias("_poss_pts"),
        pl.col("seconds").mean().alias("avg_possession_seconds"),
        *family("transition", pl.col("transition")),
        *family("half_court", ~pl.col("transition")),
        *family("second_chance", pl.col("second_chance")),
        *family("first_chance", ~pl.col("second_chance")),
        pl.col("pts").filter(started == "live_turnover").sum().cast(pl.Int64).alias("pts_off_live_tov"),
        pl.col("pts").filter(started == "dead_turnover").sum().cast(pl.Int64).alias("pts_off_dead_tov"),
    )


def _score_rows(rows: pl.DataFrame, home: int, away: int) -> dict[str, dict[int, int]]:
    """``points``, ``largest_lead`` and ``max_run`` per team, from the running score."""
    total = rows.select(pl.col("_dh").sum(), pl.col("_da").sum()).row(0)
    margin = rows["_hs"] - rows["_as"]
    score = rows.select(
        pl.lit("g").alias("game_id"),
        pl.lit(0.0).alias("elapsed"),
        pl.lit(0.0).alias("game_seconds"),
        pl.col("_hs").alias("home_score"),
        pl.col("_as").alias("away_score"),
    )
    runs = _runs(_states(score))  # BB-D27's run rule: a score correction stays in the run
    return {
        "points": {home: int(total[0]), away: int(total[1])},
        "largest_lead": {home: max(int(margin.max() or 0), 0), away: max(-int(margin.min() or 0), 0)},
        "max_run": {
            home if side == "home" else away: int(n)
            for side, n in zip(runs["side"].to_list(), runs["max_run"].to_list(), strict=True)
        },
    }


def _team_rows(rows: pl.DataFrame, poss: pl.DataFrame, player_box: pl.DataFrame | None) -> pl.DataFrame:
    """Both teams' split row from the tagged plays and the possessions."""
    home, away = int(rows["_home"][0]), int(rows["_away"][0])
    ev = rows.filter(pl.col("_team").is_in([home, away])).with_columns(pl.col("_team").alias("team_id"))
    parts = [
        _offense_rows(poss),
        ev.filter(pl.col("_tov"))
        .group_by("team_id")
        .agg(pl.len().cast(pl.Int64).alias("tov"), pl.col("_stolen").sum().cast(pl.Int64).alias("live_tov")),
        ev.filter(pl.col("_ft"))
        .group_by("team_id")
        .agg(pl.len().cast(pl.Int64).alias("fta"), pl.col("_made").sum().cast(pl.Int64).alias("ftm")),
        _zone_counts(ev.filter(pl.col("_fga")), ["team_id"]),
    ]
    has_bench = (
        player_box is not None and player_box.height and {"starter", "points", "team_id"} <= set(player_box.columns)
    )
    if has_bench:
        assert player_box is not None
        parts.append(
            player_box.filter(pl.col("starter").fill_null(False) == False)
            .group_by(pl.col("team_id").cast(pl.Int32))
            .agg(pl.col("points").sum().cast(pl.Int64).alias("bench_points"))
        )
    out = pl.DataFrame({"team_id": [home, away]}, schema={"team_id": pl.Int32})
    for part in parts:
        out = out.join(
            part.with_columns(pl.col("team_id").cast(pl.Int32)), on="team_id", how="left", maintain_order="left"
        )
    counts = [c for c, dt in _TEAM_SPLITS_SCHEMA.items() if dt == pl.Int64 and c in out.columns and c != "bench_points"]
    score = _score_rows(rows, home, away)
    return out.with_columns(
        pl.col(counts).fill_null(0),
        *[pl.col("team_id").replace_strict(m, default=0, return_dtype=pl.Int64).alias(c) for c, m in score.items()],
        *([] if has_bench else [pl.lit(None, dtype=pl.Int64).alias("bench_points")]),
        pl.lit(int(rows["game_id"][0]), dtype=pl.Int32).alias("game_id"),
    )


def team_game_splits(
    pbp: pl.DataFrame,
    player_box: pl.DataFrame | None = None,
    *,
    league: str | None = None,
    regulation_periods: int | None = None,
) -> pl.DataFrame:
    """Situational splits for both teams of one game, counted from its play-by-play (works mid-game).

    Possessions, transition / half-court, second / first chance, live / dead turnovers and the
    points off each, shot zones, assisted share, free throws, bench points, largest lead and biggest
    run, as the module docstring defines them. ``points`` is every point the team scored (the
    running score) and ``ppp`` is ``points / possessions``; each family's ``_pts`` add up to
    ``points - non_possession_points``.

    Args:
        pbp: One game's plays in the basketball pbp schema, after
            :func:`~sportsdataverse.nba.nba_game_page.shot_coordinates` (``type_text``, ``text``,
            ``shooting_play``, ``scoring_play``, ``points_attempted`` if present, ``team_id``,
            ``home_team_id``, ``away_team_id``, ``home_score``, ``away_score``, ``period_number``, a
            period clock and ``x_legacy`` / ``y_legacy``), in play order.
        player_box: The game's player box, for ``bench_points`` (``team_id``, ``points``, ``starter``;
            null without them).
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"`` (default NBA).
        regulation_periods: The game's regulation periods (2 or 4), for a period's first clock;
            default :func:`~sportsdataverse.nba.nba_game_page.game_regulation_periods` from ``season``.

    Returns:
        One row per team: ``game_id``, ``team_id``, the totals, the four families as
        ``<family>_poss`` / ``_pts`` / ``_ppp``, the turnover, zone, assist and free-throw columns,
        ``bench_points``, ``largest_lead`` and ``max_run``; an empty frame with those columns before
        tip-off.

    Raises:
        ValueError: If the league is unknown or ``pbp`` holds more than one game.
        polars.exceptions.ColumnNotFoundError: If a required column is missing.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_page import game_page_frames
            from sportsdataverse.nba.nba_pbp import espn_nba_pbp
            frames = game_page_frames(espn_nba_pbp(401766458, return_parsed=False))
            frames["team_game_splits"].select("team_id", "possessions", "ppp", "transition_ppp")

        From warehouse frames::

            import polars as pl
            from sportsdataverse.nba.nba_game_page import shot_coordinates
            from sportsdataverse.nba.nba_game_splits import team_game_splits
            from sportsdataverse.wbb.wbb_loaders import load_wbb_pbp, load_wbb_player_boxscore
            pbp = load_wbb_pbp([2024]).filter(pl.col("game_id") == 401638645)
            box = load_wbb_player_boxscore([2024]).filter(pl.col("game_id") == 401638645)
            team_game_splits(shot_coordinates(pbp), box, league="wbb")

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college play-by-play
            * `wehoop`_ -- the R companion for WNBA and women's college play-by-play

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    lg = _resolve_league(league, None)
    if pbp.height == 0:
        return pl.DataFrame(schema=_TEAM_SPLITS_SCHEMA)
    if pbp["game_id"].n_unique() > 1:
        raise ValueError(f"team_game_splits reads one game; got {pbp['game_id'].n_unique()} game_id values")
    reg = regulation_periods if regulation_periods is not None else game_regulation_periods(lg, season=_box_season(pbp))
    rows = _split_events(pbp)
    out = _team_rows(rows, _possessions(rows, lg, reg), player_box)
    return out.with_columns(
        (pl.col("tov") - pl.col("live_tov")).alias("dead_tov"),
        *[_ratio(pl.col(f"{z}_fgm"), pl.col(f"{z}_fga")).alias(f"{z}_fg_pct") for z in _ZONES],
        *[_ratio(pl.col(f"{f}_pts"), pl.col(f"{f}_poss")).alias(f"{f}_ppp") for f in _FAMILIES],
        _ratio(pl.col("points"), pl.col("possessions")).alias("ppp"),
        (pl.col("points") - pl.col("_poss_pts")).alias("non_possession_points"),
        _ratio(pl.col("tov"), pl.col("possessions")).alias("tov_pct"),
        _ratio(pl.col("rim_fga"), pl.col("fga")).alias("rim_rate"),
        _ratio(pl.col("fg3_fga"), pl.col("fga")).alias("fg3a_rate"),
        _ratio(pl.col("assisted_fgm"), pl.col("fgm")).alias("assisted_share"),
        _ratio(pl.col("fta"), pl.col("fga")).alias("ft_rate"),
    ).select([pl.col(c).cast(dt) for c, dt in _TEAM_SPLITS_SCHEMA.items()])


def _player_counts(rows: pl.DataFrame, box: pl.DataFrame | None) -> pl.DataFrame:
    """Per ``(athlete_id, team_id)`` on the plays: shots by zone, points, rebounds, turnovers, assists.

    Only the counted plays: a foul's ``team_id`` can be the other side's (WNBA 2017). ESPN's older
    plays can also name the stealer first on a turnover ("A bad pass (B steals)", ``athlete_id_1``
    B): with a box, a turnover whose first athlete plays for the other team goes to the second.
    """
    key = ["athlete_id", "team_id"]
    a1, a2 = pl.col("athlete_id_1").cast(pl.Int32), pl.col("athlete_id_2").cast(pl.Int32)
    actor = a1
    if box is not None:
        roster = dict(box.select(pl.col("athlete_id").cast(pl.Int32), pl.col("team_id").cast(pl.Int32)).iter_rows())
        other_side = (a1.replace_strict(roster, default=None) != pl.col("_team")).fill_null(False)
        actor = pl.when(pl.col("_tov") & other_side & a2.is_not_null()).then(a2).otherwise(a1)
    mine = rows.filter(pl.col("athlete_id_1").is_not_null() & (pl.col("_off") | pl.col("_ft"))).with_columns(
        actor.alias("athlete_id"), pl.col("_team").alias("team_id")
    )
    shots = _zone_counts(mine.filter(pl.col("_fga")), key).rename({"fg3_fga": "fg3a", "fg3_fgm": "fg3m"})
    # points from the plays, not score_value: 2015-16 NBA free throws all ship scoreValue 0
    points = (
        pl.when(pl.col("_made") & pl.col("_ft"))
        .then(1)
        .when(pl.col("_made") & pl.col("_fga"))
        .then(2 + pl.col("_three").cast(pl.Int64))
        .otherwise(0)
    )
    other = mine.group_by(key).agg(
        points.sum().cast(pl.Int64).alias("points"),
        pl.col("_reb").sum().cast(pl.Int64).alias("rebounds"),
        pl.col("_tov").sum().cast(pl.Int64).alias("tov"),
        (pl.col("_tov") & pl.col("_stolen")).sum().cast(pl.Int64).alias("live_tov"),
    )
    passer = (
        pl.col("_fga") & pl.col("_made") & pl.col("text").fill_null("").str.contains("(?i)assist") & a2.is_not_null()
    )
    ast = (
        rows.filter(passer)
        .group_by(a2.alias("athlete_id"), pl.col("_team").alias("team_id"))
        .agg(pl.len().cast(pl.Int64).alias("assists"))
    )
    return other.join(shots, on=key, how="full", coalesce=True).join(ast, on=key, how="full", coalesce=True)


def player_game_splits(
    pbp: pl.DataFrame,
    player_box: pl.DataFrame | None = None,
    *,
    league: str | None = None,
    regulation_periods: int | None = None,
) -> pl.DataFrame:
    """Per-player shot, assist and turnover splits of one game, counted from its play-by-play (works mid-game).

    Counted from the plays the player is the first athlete on: field goals (rim, three and their
    assisted makes, as in :func:`team_game_splits`), turnovers (``live_tov`` with a steal credited),
    rebounds and points; ``assists`` are the made shots crediting the player as the passer
    (``athlete_id_2`` on a make whose text names an assist). ``est_possessions`` is an estimate, not an
    on-court count (that needs the lineup on each play): the team's counted possessions times the
    player's share of the team's minutes (``minutes`` over the team's minutes / 5), so a mid-game box
    gives the share so far. The per-100 rates divide by it and are null without minutes or before the
    team has a counted possession.

    Args:
        pbp: One game's plays, as :func:`team_game_splits` takes them (plus ``athlete_id_1`` /
            ``athlete_id_2``).
        player_box: The game's player box (``athlete_id``, ``team_id``, ``minutes``): every boxed
            player gets one row (a play crediting a player to the other team is left out); without a
            box, every player on the plays gets one, with null minutes.
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"`` (default NBA).
        regulation_periods: The game's regulation periods; see :func:`team_game_splits`.

    Returns:
        One row per player: ``game_id``, ``athlete_id``, ``team_id``, ``minutes``, ``est_possessions``,
        the shot, assist, rebound and turnover counts and shares, and ``pts`` / ``ast`` / ``reb`` /
        ``tov_per100``; an empty frame with those columns when there are no plays and no box.

    Raises:
        ValueError: If the league is unknown or ``pbp`` holds more than one game.
        polars.exceptions.ColumnNotFoundError: If a required column is missing.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_page import game_page_frames
            from sportsdataverse.wnba.wnba_pbp import espn_wnba_pbp
            frames = game_page_frames(espn_wnba_pbp(401726992, return_parsed=False), league="wnba")
            frames["player_game_splits"].sort("est_possessions", descending=True).head()

        Who scored at the rim off passes::

            frames["player_game_splits"].select("athlete_id", "rim_fgm", "rim_assisted_share")

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college play-by-play
            * `wehoop`_ -- the R companion for WNBA and women's college play-by-play

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    lg = _resolve_league(league, None)
    box = player_box if player_box is not None and player_box.height and "athlete_id" in player_box.columns else None
    if pbp.height == 0 and box is None:
        return pl.DataFrame(schema=_PLAYER_SPLITS_SCHEMA)
    if pbp.height and pbp["game_id"].n_unique() > 1:
        raise ValueError(f"player_game_splits reads one game; got {pbp['game_id'].n_unique()} game_id values")
    key = ["athlete_id", "team_id"]
    team = pl.DataFrame(schema={"team_id": pl.Int32, "possessions": pl.Int64})
    counted = pl.DataFrame(schema={"athlete_id": pl.Int32, "team_id": pl.Int32})
    src = pbp if pbp.height else box
    assert src is not None  # the no-plays, no-box case returned above
    game_id = int(src["game_id"][0])
    if pbp.height:
        team = team_game_splits(pbp, league=lg, regulation_periods=regulation_periods).select("team_id", "possessions")
        rows = _split_events(pbp).filter(pl.col("_team").is_in(team["team_id"].to_list()))
        counted = _player_counts(rows, box)
    if box is not None:
        minutes = pl.col("minutes").cast(pl.Float64) if "minutes" in box.columns else pl.lit(None, dtype=pl.Float64)
        players = box.select(
            pl.col("athlete_id").cast(pl.Int32), pl.col("team_id").cast(pl.Int32), minutes.alias("minutes")
        )
        counted = players.join(counted, on=key, how="left", maintain_order="left")  # the box's players only
    else:
        counted = counted.with_columns(pl.lit(None, dtype=pl.Float64).alias("minutes"))
    out = counted.join(
        team.with_columns(pl.col("team_id").cast(pl.Int32)), on="team_id", how="left", maintain_order="left"
    )
    counts = [c for c, dt in _PLAYER_SPLITS_SCHEMA.items() if dt == pl.Int64]
    out = out.with_columns(
        [(pl.col(c) if c in out.columns else pl.lit(None, dtype=pl.Int64)).fill_null(0).alias(c) for c in counts]
    )
    share = pl.col("minutes") / (pl.col("minutes").sum().over("team_id") / 5.0)
    est = pl.when(pl.col("minutes") > 0).then(pl.col("possessions").cast(pl.Float64) * share)
    per100 = (("pts", "points"), ("ast", "assists"), ("reb", "rebounds"), ("tov", "tov"))
    return (
        out.with_columns(pl.lit(game_id, dtype=pl.Int32).alias("game_id"), est.alias("est_possessions"))
        .with_columns(
            _ratio(pl.col("assisted_fgm"), pl.col("fgm")).alias("assisted_share"),
            _ratio(pl.col("rim_assisted_fgm"), pl.col("rim_fgm")).alias("rim_assisted_share"),
            _ratio(pl.col("fg3_assisted_fgm"), pl.col("fg3m")).alias("fg3_assisted_share"),
            *[_ratio(100.0 * pl.col(src), pl.col("est_possessions")).alias(f"{m}_per100") for m, src in per100],
        )
        .select([pl.col(c).cast(dt) for c, dt in _PLAYER_SPLITS_SCHEMA.items()])
    )
