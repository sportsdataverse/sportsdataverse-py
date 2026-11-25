"""ESPN NFL play-by-play in nflverse's shape: keys, teams, situation, clock, flags, yards and game data.

:func:`espn_nfl_pbp_nflverse` turns the frame :class:`~sportsdataverse.nfl.NFLPlayProcess` builds from an
ESPN game (or the published ``espn_nfl_pbp`` release, which is the same frame) into nflverse
``play_by_play`` names, conventions and dtypes. It is additive: the cfbfastR-shape ESPN frame is not
changed. Every rule here is scored against nflverse on whole seasons (2007, 2014, 2019, 2024), joined on
``(game_id, play_id)``; ESPN's play ``id`` is the ESPN game id followed by the GSIS ``play_id`` from 2006 on.
"""

from __future__ import annotations

from typing import Optional

import polars as pl

from sportsdataverse.nfl.datasets import team_abbr_mapping
from sportsdataverse.nfl.model_vars import PAT_FIRST_SEASON_FROM_15, PAT_YARDLINE_POST_2015
from sportsdataverse.nfl.nfl_pbp import _espn_play_type_nfl, split_pat_rows
from sportsdataverse.nfl.shield_pbp.description import add_description_features, add_pass_rush
from sportsdataverse.nfl.shield_pbp.drives import drive_level_tmp_result, last_or_first_result, swapped_posteam
from sportsdataverse.nfl.shield_pbp.series import add_series_data

__all__ = ["espn_nfl_pbp_nflverse"]

#: ESPN columns the view reads; a frame without them is not processor output.
_REQUIRED = (
    "id",
    "game_id",
    "seasonType",
    "homeTeamId",
    "awayTeamId",
    "start.pos_team.id",
    "period.number",
    "clock.minutes",
    "clock.seconds",
    "start.down",
    "start.distance",
    "start.yardsToEndzone",
    "type.text",
    "text",
    "kickoff_play",
    "xp_attempt",
    "two_point_attempt",
    "start.pos_team_score",
    "start.def_pos_team_score",
    "start.pos_score_diff",
    "pos_team_score",
    "def_pos_team_score",
    "homeScore",
    "awayScore",
    "start.homeTeamTimeouts",
    "start.awayTeamTimeouts",
    "end.homeTeamTimeouts",
    "end.awayTeamTimeouts",
    "start.posTeamTimeouts",
    "start.defPosTeamTimeouts",
    "statYardage",
    "yds_receiving",
    "yds_rushed",
    "yds_sacked",
    "yds_penalty",
    "penalized_team",
    "penalty_flag",
    "penalty_declined",
    "penalty_no_play",
    "pass",
    "rush",
    "sack",
    "pass_attempt",
    "completion",
    "kneel_down",
    "two_point_pass",
    "defense_score_play",
    "homeTeamSpread",
    "overUnder",
    "drive.id",
)
#: nflverse 0/1 flag -> the processor's boolean column it renames (Int64 0/1, as the Shield builder emits).
_FLAG_RENAMES = {
    "complete_pass": "completion",
    "interception": "int",
    "sack": "sack",
    "rush_attempt": "rush",
    "touchdown": "touchdown",
    "pass_touchdown": "pass_td",
    "rush_touchdown": "rush_td",
    "safety": "safety",
    "fumble": "fumble_vec",
    "fumble_lost": "fumble_lost",
    "first_down_penalty": "first_down_penalty",
    "kickoff_attempt": "kickoff_play",
    "kickoff_out_of_bounds": "kickoff_oob",
    "kickoff_downed": "kickoff_downed",
    "kickoff_fair_catch": "kickoff_fair_catch",
    "punt_attempt": "punt_play",
    "punt_blocked": "punt_blocked",
    "punt_out_of_bounds": "punt_oob",
    "punt_downed": "punt_downed",
    "punt_fair_catch": "punt_fair_catch",
    "field_goal_attempt": "fg_attempt",
    "extra_point_attempt": "xp_attempt",
    "two_point_attempt": "two_point_attempt",
    "qb_kneel": "kneel_down",
    "special": "sp",
    "tackled_for_loss": "TFL_rush",
    "goal_to_go": "goal_to_go",
}
#: nflverse column -> ESPN column carried over as is (drive detail and the play's own results).
_RENAMES: dict[str, tuple[str, pl.DataType]] = {
    "desc": ("text", pl.Utf8),
    "field_goal_result": ("field_goal_result", pl.Utf8),
    "extra_point_result": ("extra_point_result", pl.Utf8),
    "two_point_conv_result": ("two_point_conv_result", pl.Utf8),
    "run_location": ("rush_direction", pl.Utf8),
    "pass_location": ("pass_location", pl.Utf8),
    "pass_length": ("pass_length", pl.Utf8),
    "drive_play_count": ("drive.offensivePlays", pl.Float64),
    "drive_time_of_possession": ("drive.timeElapsed.displayValue", pl.Utf8),
    "drive_quarter_start": ("drive.start.period.number", pl.Float64),
    "drive_quarter_end": ("drive.end.period.number", pl.Float64),
    "ydsnet": ("drive.yards", pl.Float64),
}
#: nflverse schedule column -> the play-by-play column nflfastR's ``add_game_data`` writes it to.
_SCHEDULE_COLUMNS = {
    "old_game_id": "old_game_id",
    "gameday": "game_date",
    "location": "location",
    "result": "result",
    "total": "total",
    "home_score": "home_score",
    "away_score": "away_score",
    "div_game": "div_game",
    "roof": "roof",
    "surface": "surface",
    "temp": "temp",
    "wind": "wind",
    "home_coach": "home_coach",
    "away_coach": "away_coach",
    "stadium": "game_stadium",
    "stadium_id": "stadium_id",
    "spread_line": "spread_line",
    "total_line": "total_line",
}


def _int_id(expr: pl.Expr, dtype: pl.DataType, name: str) -> pl.Expr:
    """An integer id column as Int64; a float id is refused (it may already have lost digits)."""
    if dtype.is_float():
        raise ValueError(f"espn_nfl_pbp_nflverse: {name} is {dtype}; pass integer or string ids")
    return expr.cast(pl.Utf8).cast(pl.Int64)


def _attach_nflverse_game_id(plays: pl.DataFrame, schedule: Optional[pl.DataFrame]) -> pl.DataFrame:
    """``nflverse_game_id``: the schedule's (on its ``espn`` event id) where a schedule is given, else the frame's own
    (``NFLPlayProcess`` stamps it from ESPN's header).

    A game between non-NFL sides (the Pro Bowl, which nflverse does not carry) is dropped; any other game left
    without an id is refused.
    """
    have = "nflverse_game_id" in plays.columns
    out = plays.with_columns(_int_id(pl.col("game_id"), plays.schema["game_id"], "game_id").alias("_espn"))
    if schedule is not None and {"game_id", "espn"} <= set(schedule.columns):
        ids = schedule.select(
            _int_id(pl.col("espn"), schedule.schema["espn"], "schedule espn").alias("_espn"),
            pl.col("game_id").cast(pl.Utf8).alias("_sched_id"),
        ).drop_nulls("_espn")
        assert out.schema["_espn"] == ids.schema["_espn"] == pl.Int64
        out = out.join(ids.unique("_espn", keep="first", maintain_order=True), on="_espn", how="left")
        stamped = [pl.col("nflverse_game_id").cast(pl.Utf8)] if have else []
        out = out.with_columns(pl.coalesce("_sched_id", *stamped).alias("nflverse_game_id")).drop("_sched_id")
    elif not have:
        raise ValueError(
            "espn_nfl_pbp_nflverse: plays carry no nflverse_game_id; pass the nflverse schedule "
            "(with its game_id and espn columns) to map the ESPN game id"
        )
    missing = out.filter(pl.col("nflverse_game_id").is_null())
    if missing.height:
        clubs = set(team_abbr_mapping.values()) - {"AFC", "NFC", "NFL"}
        sides = [c for c in ("homeTeamAbbrev", "awayTeamAbbrev") if c in out.columns]
        nfl = (
            pl.all_horizontal([pl.col(c).replace(team_abbr_mapping).is_in(clubs) for c in sides])
            if sides
            else pl.lit(True)
        )
        refused = missing.filter(nfl.fill_null(True))["_espn"].unique().sort().to_list()
        if refused:
            raise ValueError(f"espn_nfl_pbp_nflverse: no nflverse game id for ESPN game id(s) {refused[:5]}")
        out = out.filter(pl.col("nflverse_game_id").is_not_null())
    return out.drop("_espn")


#: A free kick ESPN types as a penalty (offside on the kick, delay of kickoff): nflverse's ``kickoff`` row.
_PENALIZED_KICK = r"(?i)(?:\bkicks (?:onside )?-?\d+ yards? from\b)|(?:delay of kickoff)"


def _penalized_kick() -> pl.Expr:
    """A kickoff ESPN files as a penalty: free-kick text on a row the processor does not flag as a kickoff."""
    kickoff = (pl.col("kickoff_play") == True).fill_null(False)  # noqa: E712
    return pl.col("text").fill_null("").str.contains(_PENALIZED_KICK) & ~kickoff


def _keys_and_teams(df: pl.DataFrame) -> list[pl.Expr]:
    """Keys, teams and field position: nflverse ``game_id`` / ``play_id``, abbreviations, ``yardline_100``.

    Teams come from the team ids and the nflverse game id (``YYYY_WW_AWAY_HOME``), never from ESPN's
    abbreviations. nflverse writes the franchise's current abbreviation in the play-by-play (``LA`` for the
    2007 Rams, ``LV`` for the 2019 Raiders) while the game id keeps the old one, so the abbreviation goes
    through :data:`~sportsdataverse.nfl.team_abbr_mapping`.
    """
    parts = pl.col("nflverse_game_id").str.split("_")

    def abbr(i: int) -> pl.Expr:
        return parts.list.get(i).replace(team_abbr_mapping)

    home, away = abbr(3), abbr(2)
    pos = pl.col("start.pos_team.id").cast(pl.Int64)
    home_ball = pos == pl.col("homeTeamId").cast(pl.Int64)
    away_ball = pos == pl.col("awayTeamId").cast(pl.Int64)
    # a penalized kickoff keeps ESPN's kicking team; nflverse gives it to the receiver, as on a kickoff row
    pen_kick = _penalized_kick()
    is_home = pl.when(pen_kick).then(away_ball).otherwise(home_ball)
    is_away = pl.when(pen_kick).then(home_ball).otherwise(away_ball)
    posteam = pl.when(is_home).then(home).when(is_away).then(away)
    defteam = pl.when(is_home).then(away).when(is_away).then(home)
    kickoff = (pl.col("kickoff_play") == True).fill_null(False) | pen_kick  # noqa: E712
    # ESPN measures a kickoff from the kicking team's side; nflverse gives it to the receiving team
    yl = pl.when(kickoff).then(100 - pl.col("start.yardsToEndzone")).otherwise(pl.col("start.yardsToEndzone"))
    side = pl.when(yl == 50).then(pl.lit("50")).when(yl > 50).then(posteam).otherwise(defteam)
    yard = pl.when(yl > 50).then(100 - yl).otherwise(yl)
    seasontype = pl.col("seasonType").cast(pl.Int64)
    play_id = pl.col("id").cast(pl.Utf8).str.slice(pl.col("game_id").cast(pl.Utf8).str.len_chars()).cast(pl.Int64)
    return [
        play_id.alias("play_id"),
        pl.col("nflverse_game_id").cast(pl.Utf8).alias("game_id"),
        pl.col("game_id").cast(pl.Int64).alias("espn_game_id"),
        pl.col("id").cast(pl.Int64).alias("espn_play_id"),
        home.alias("home_team"),
        away.alias("away_team"),
        pl.when(seasontype == 3)
        .then(pl.lit("POST"))
        .when(seasontype == 1)
        .then(pl.lit("PRE"))
        .otherwise(pl.lit("REG"))
        .alias("season_type"),
        parts.list.get(1).cast(pl.Int64, strict=False).alias("week"),  # a preseason PRE<n> week is null
        parts.list.get(0).cast(pl.Int64).alias("season"),
        posteam.alias("posteam"),
        pl.when(is_home).then(pl.lit("home")).when(is_away).then(pl.lit("away")).alias("posteam_type"),
        defteam.alias("defteam"),
        side.alias("side_of_field"),
        yl.cast(pl.Float64).alias("yardline_100"),
        pl.when(yl == 50).then(pl.lit("MID 50")).otherwise(pl.format("{} {}", side, yard)).alias("yrdln"),
    ]


def _timeouts(side: str) -> pl.Expr:
    """A team's timeouts left: before the play, except on a timeout row, which nflverse counts after it."""
    timeout = (pl.col("type.text") == "Timeout").fill_null(False)
    start, end = pl.col(f"start.{side}TeamTimeouts"), pl.col(f"end.{side}TeamTimeouts")
    return pl.when(timeout).then(end).otherwise(start).cast(pl.Float64)


def _clock_and_situation() -> list[pl.Expr]:
    """Quarter and clock, down / distance, scores and timeouts. Overtime counts only its own period."""
    q = pl.col("period.number").cast(pl.Int64)
    # ESPN stamps some plays with the clock after the snap; the "(10:29)" the NFL text opens with
    # (2009-2023 feeds) is the clock at the snap, so it wins where it is there
    lead = pl.col("text").str.extract_groups(r"^\((\d{0,2}):(\d{2})\)")
    mins = pl.coalesce(lead.struct[0].replace("", "0").cast(pl.Int64), pl.col("clock.minutes").cast(pl.Int64))
    secs = pl.coalesce(lead.struct[1].cast(pl.Int64), pl.col("clock.seconds").cast(pl.Int64))
    qs = mins * 60 + secs
    kickoff = (pl.col("kickoff_play") == True).fill_null(False)  # noqa: E712
    try_row = ((pl.col("xp_attempt") == True) | (pl.col("two_point_attempt") == True)).fill_null(False)  # noqa: E712
    no_down = kickoff | try_row | (pl.col("start.down") == 0).fill_null(True)
    mmss = pl.format("{}:{}", mins.cast(pl.Utf8).str.zfill(2), secs.cast(pl.Utf8).str.zfill(2))
    # a penalized kickoff is the receiver's ball (_keys_and_teams): its posteam-relative columns swap sides
    flip = _penalized_kick()

    def side(own: str, other: str) -> pl.Expr:
        return pl.when(flip).then(pl.col(other)).otherwise(pl.col(own))

    pos_post, def_post = side("pos_team_score", "def_pos_team_score"), side("def_pos_team_score", "pos_team_score")
    half = pl.when(q <= 2).then(pl.lit("Half1")).when(q <= 4).then(pl.lit("Half2")).otherwise(pl.lit("Overtime"))
    return [
        q.cast(pl.Float64).alias("qtr"),
        qs.cast(pl.Float64).alias("quarter_seconds_remaining"),
        pl.when(q.is_in([1, 3])).then(qs + 900).otherwise(qs).cast(pl.Float64).alias("half_seconds_remaining"),
        pl.when(q <= 4).then(qs + (4 - q) * 900).otherwise(qs).cast(pl.Float64).alias("game_seconds_remaining"),
        half.alias("game_half"),
        mmss.alias("time"),
        pl.when(no_down).then(None).otherwise(pl.col("start.down")).cast(pl.Float64).alias("down"),
        pl.when(kickoff).then(0).otherwise(pl.col("start.distance")).cast(pl.Float64).alias("ydstogo"),
        side("start.pos_team_score", "start.def_pos_team_score").cast(pl.Float64).alias("posteam_score"),
        side("start.def_pos_team_score", "start.pos_team_score").cast(pl.Float64).alias("defteam_score"),
        pl.when(flip)
        .then(-pl.col("start.pos_score_diff"))
        .otherwise(pl.col("start.pos_score_diff"))
        .cast(pl.Float64)
        .alias("score_differential"),
        pos_post.cast(pl.Float64).alias("posteam_score_post"),
        def_post.cast(pl.Float64).alias("defteam_score_post"),
        (pos_post - def_post).cast(pl.Float64).alias("score_differential_post"),
        pl.col("homeScore").cast(pl.Float64).alias("total_home_score"),
        pl.col("awayScore").cast(pl.Float64).alias("total_away_score"),
        _timeouts("home").alias("home_timeouts_remaining"),
        _timeouts("away").alias("away_timeouts_remaining"),
        side("start.posTeamTimeouts", "start.defPosTeamTimeouts").cast(pl.Float64).alias("posteam_timeouts_remaining"),
        side("start.defPosTeamTimeouts", "start.posTeamTimeouts").cast(pl.Float64).alias("defteam_timeouts_remaining"),
    ]


def _flags(df: pl.DataFrame) -> list[pl.Expr]:
    """nflverse's 0/1 play flags (Int64), from the processor's booleans and a few text markers."""

    def b(c: str) -> pl.Expr:
        return (pl.col(c) == True).fill_null(False) if c in df.columns else pl.lit(False)  # noqa: E712

    text = pl.col("text").fill_null("")
    sack, comp, intc = b("sack"), b("completion"), b("int")
    attempt = b("pass_attempt") | sack  # nflverse counts a sack as a pass attempt
    scramble = b("rush") & text.str.contains(r"(?i)\bscrambles?\b")
    spike = text.str.contains(r"(?i)\bspiked?\b")
    # an offensive touchdown counts as a first down; a declined penalty is no penalty (nflverse)
    first = b("first_down_earned") | b("first_down_penalty") | b("pass_td") | b("rush_td")
    forced = b("fumble_vec") & text.str.contains(r"\bFUMBLES \(")  # "FUMBLES (J.Love)" names the forcer
    down = pl.col("start.down")
    converted = first | (b("touchdown") & ~b("defense_score_play"))
    live = ~b("penalty_no_play") & (attempt | b("rush"))
    flags = {
        **{nv: b(espn) for nv, espn in _FLAG_RENAMES.items()},
        "pass_attempt": attempt,
        "incomplete_pass": attempt & ~comp & ~intc & ~sack,
        "fumble_forced": forced,
        # nflverse leaves a made field goal out of special_teams_play (it is in special)
        "special_teams_play": b("sp") & ~b("fg_made"),
        "fumble_not_forced": b("fumble_vec") & ~forced,
        "penalty": b("penalty_flag") & ~b("penalty_declined"),
        "return_touchdown": b("touchdown") & ~b("pass_td") & ~b("rush_td"),
        "touchback": b("kickoff_tb") | b("punt_tb"),
        "qb_spike": spike,
        "qb_scramble": scramble,
        "qb_dropback": attempt | scramble | spike,
        "first_down": first,
        "first_down_rush": (b("first_down_earned") | b("rush_td")) & b("rush"),
        "first_down_pass": (b("first_down_earned") | b("pass_td")) & attempt,
        "third_down_converted": (down == 3).fill_null(False) & converted,
        "third_down_failed": (down == 3).fill_null(False) & live & ~converted,
        "fourth_down_converted": (down == 4).fill_null(False) & converted,
        "fourth_down_failed": (down == 4).fill_null(False) & live & ~converted,
        "aborted_play": text.str.contains("Aborted", literal=True),
        "drive_ended_with_score": b("drive.isScore"),
    }
    return [e.cast(pl.Int64).alias(nv) for nv, e in flags.items()]


#: nflverse ``play_type`` for the special-teams ``play_type_nfl`` values (the rest are set by rule).
_PLAY_TYPE = {
    "PUNT": "punt",
    "FIELD_GOAL": "field_goal",
    "KICK_OFF": "kickoff",
    "XP_KICK": "extra_point",
}


def _yards_and_types(df: pl.DataFrame) -> list[pl.Expr]:
    """Yardage, ``play_type`` / ``play_type_nfl``, the scoring / penalized team, lines and drive number."""

    def b(c: str) -> pl.Expr:
        return (pl.col(c) == True).fill_null(False)  # noqa: E712

    # the processor's own column where it has one; split_pat_rows sets it on its PAT rows alone
    ptn = _espn_play_type_nfl()
    if "play_type_nfl" in df.columns:
        ptn = pl.coalesce(pl.col("play_type_nfl"), ptn)
    text = pl.col("text").fill_null("")
    pass_play = b("pass_attempt") & ~b("sack")
    play_type = (
        pl.when(b("kneel_down"))
        .then(pl.lit("qb_kneel"))
        .when(text.str.contains(r"(?i)\bspiked?\b"))
        .then(pl.lit("qb_spike"))
        .when(ptn.is_in(["PASS", "SACK", "INTERCEPTION"]))
        .then(pl.lit("pass"))
        .when(ptn == "RUSH")
        .then(pl.lit("run"))
        .when(ptn == "PAT2")
        # a split try carries no two_point_pass flag; split_pat_rows types it "Two Point Pass"
        .then(
            pl.when(b("two_point_pass") | (pl.col("type.text") == "Two Point Pass"))
            .then(pl.lit("pass"))
            .otherwise(pl.lit("run"))
        )
        .when(_penalized_kick())
        .then(pl.lit("kickoff"))
        .when(ptn.is_in(["PENALTY", "TIMEOUT"]))
        .then(pl.lit("no_play"))
        .otherwise(ptn.replace_strict(_PLAY_TYPE, default=None, return_dtype=pl.Utf8))
    )
    hid, aid = pl.col("homeTeamId").cast(pl.Int64), pl.col("awayTeamId").cast(pl.Int64)

    def team(id_col: str) -> pl.Expr:
        i = pl.col(id_col).cast(pl.Int64)
        return pl.when(i == hid).then(pl.col("home_team")).when(i == aid).then(pl.col("away_team"))

    scorer = pl.when(b("defense_score_play")).then(pl.col("defteam")).otherwise(pl.col("posteam"))
    drive_tail = pl.col("drive.id").cast(pl.Utf8).str.slice(pl.col("espn_game_id").cast(pl.Utf8).str.len_chars())
    rec = pl.col("yds_receiving")
    penalty = b("penalty_flag") & ~b("penalty_declined")
    # the play's own yards: ESPN's statYardage also counts an accepted penalty's yards after the play
    play_yards = (
        pl.when(b("rush"))
        .then(pl.col("yds_rushed"))
        .when(b("sack"))
        .then(pl.col("yds_sacked"))
        .when(pass_play)
        .then(pl.when(b("completion")).then(rec).otherwise(0))
        .otherwise(pl.col("statYardage"))
    )
    return [
        ptn.cast(pl.Utf8).alias("play_type_nfl"),
        play_type.alias("play_type"),
        # nflverse gives a kick and a no-play penalty no yards gained
        pl.when(b("penalty_no_play") | play_type.is_in(list(_PLAY_TYPE.values())).fill_null(False))
        .then(0)
        # the split yards come from the text; a pre-2009 stub ("Rush", "Pass Completion") has none
        .when(text.str.contains(r"(?i)\bfor (?:-?\d+ yards?|no gain)"))
        .then(play_yards)
        .otherwise(pl.col("statYardage"))
        .cast(pl.Float64)
        .alias("yards_gained"),
        pl.when(pass_play & b("completion")).then(rec).cast(pl.Float64).alias("passing_yards"),
        pl.when(b("completion")).then(rec).cast(pl.Float64).alias("receiving_yards"),
        pl.when(b("rush")).then(pl.col("yds_rushed")).cast(pl.Float64).alias("rushing_yards"),
        pl.when(penalty).then(pl.col("yds_penalty").cast(pl.Float64, strict=False)).alias("penalty_yards"),
        pl.when(penalty).then(team("penalized_team")).alias("penalty_team"),
        pl.when(b("touchdown")).then(scorer).alias("td_team"),
        pl.col("homeTeamSpread").cast(pl.Float64).alias("spread_line"),
        pl.col("overUnder").cast(pl.Float64).alias("total_line"),
        drive_tail.cast(pl.Float64, strict=False).alias("drive"),
    ]


def _add_schedule(view: pl.DataFrame, schedule: pl.DataFrame) -> pl.DataFrame:
    """nflfastR's ``add_game_data``: the schedule's game columns joined on ``game_id``.

    The schedule's ``spread_line`` / ``total_line`` win over ESPN's own lines (nflverse's come from it).
    """
    cols = [c for c in _SCHEDULE_COLUMNS if c in schedule.columns]
    picked = [
        (_int_id(pl.col(c), schedule.schema[c], c).cast(pl.Utf8) if c == "old_game_id" else pl.col(c)).alias(f"_s_{c}")
        for c in cols
    ]
    games = schedule.select(pl.col("game_id").cast(pl.Utf8), *picked).unique("game_id")
    assert view.schema["game_id"] == games.schema["game_id"] == pl.Utf8
    out = view.join(games, on="game_id", how="left")
    lines = {"spread_line", "total_line"}
    return out.with_columns(
        (pl.coalesce(pl.col(f"_s_{c}").cast(pl.Float64), pl.col(c)) if c in lines else pl.col(f"_s_{c}")).alias(
            _SCHEDULE_COLUMNS[c]
        )
        for c in cols
    ).drop([f"_s_{c}" for c in cols])


def _pat_possession(df: pl.DataFrame) -> pl.DataFrame:
    """A split PAT row's possession: :func:`split_pat_rows` sets the scoring side's numeric team id
    (``pos_team`` in processor output, ``pos_team_id`` in the flattened release); the view reads
    ``start.pos_team.id``, which the split leaves null."""
    ids = [c for c in ("pos_team_id", "pos_team") if c in df.columns and df.schema[c].is_integer()]
    if not ids:
        return df
    pat = pl.col("pat_of_id").is_not_null()
    target = df.schema["start.pos_team.id"]
    return df.with_columns(
        pl.when(pat).then(pl.col(ids[0])).otherwise(pl.col("start.pos_team.id")).cast(target).alias("start.pos_team.id")
    )


#: A try's snap spot before 2015, and every two-point try's: the 2 (nflverse ``yardline_100`` on its tries).
_TRY_SNAP_YARDLINE = 2
#: The timeouts a split try copies from its touchdown row (no timeout is called between the two): the home / away
#: counts, since after a defensive touchdown the try's ``posteam`` is the touchdown row's defense.
_PAT_TIMEOUTS = ("home_timeouts_remaining", "away_timeouts_remaining")


def _pat_field(view: pl.DataFrame) -> pl.DataFrame:
    """A split try's field position and timeouts, which :func:`split_pat_rows` leaves null.

    nflverse spots a kick try at the 2 before 2015 and the 15 from 2015 on, every two-point try at the 2, on the
    defense's side with ``ydstogo`` 0; the timeouts are the touchdown row's. Without a spot the FG model scores
    the try's make probability (its ``ep``) off a null yard line and the WP models see no field position.
    """
    pat = pl.col("pat_of_id").is_not_null()
    two = (pl.col("two_point_attempt") == 1).fill_null(False)
    spot = (
        pl.when(~two & (pl.col("season") >= PAT_FIRST_SEASON_FROM_15))
        .then(PAT_YARDLINE_POST_2015)
        .otherwise(_TRY_SNAP_YARDLINE)
        .cast(pl.Float64)
    )
    td = view.filter(pat.not_()).select(
        pl.col("espn_play_id").alias("pat_of_id"), *[pl.col(c).alias(f"_td_{c}") for c in _PAT_TIMEOUTS]
    )
    assert view.schema["pat_of_id"] == td.schema["pat_of_id"] == pl.Int64
    v = view.join(td, on="pat_of_id", how="left", maintain_order="left")
    yl = pl.when(pat).then(spot).otherwise(pl.col("yardline_100"))
    v = v.with_columns(
        yl.alias("yardline_100"),
        pl.when(pat).then(pl.col("defteam")).otherwise(pl.col("side_of_field")).alias("side_of_field"),
        pl.when(pat)
        .then(pl.format("{} {}", pl.col("defteam"), spot.cast(pl.Int64)))
        .otherwise(pl.col("yrdln"))
        .alias("yrdln"),
        pl.when(pat).then(0.0).otherwise(pl.col("ydstogo")).alias("ydstogo"),
        *[pl.when(pat).then(pl.col(f"_td_{c}")).otherwise(pl.col(c)).alias(c) for c in _PAT_TIMEOUTS],
    ).drop([f"_td_{c}" for c in _PAT_TIMEOUTS])
    home = pl.col("posteam_type") == "home"
    home_to, away_to = pl.col("home_timeouts_remaining"), pl.col("away_timeouts_remaining")
    return v.with_columns(
        pl.when(pat)
        .then(pl.when(home).then(home_to).otherwise(away_to))
        .otherwise(pl.col("posteam_timeouts_remaining"))
        .alias("posteam_timeouts_remaining"),
        pl.when(pat)
        .then(pl.when(home).then(away_to).otherwise(home_to))
        .otherwise(pl.col("defteam_timeouts_remaining"))
        .alias("defteam_timeouts_remaining"),
    )


def _pat_scores(view: pl.DataFrame) -> pl.DataFrame:
    """Take a split try's points off its touchdown row, as nflverse scores the two rows.

    ESPN's touchdown row already counts the try (``homeScore`` / ``awayScore`` after it); the split PAT row
    copies those post-try totals. The touchdown row loses the try's points (1 for a good kick, 2 for a
    successful two-point try) and the PAT row's pre-play scores are the touchdown's post-play ones. A
    defensive two-point return (the try's text: ``DEFENSIVE TWO-POINT ATTEMPT ... ATTEMPT SUCCEEDS``) is taken
    off the other side the same way, but only where the touchdown row counts it: 2019 ESPN does (2019_13_OAK_KC),
    2024 ESPN puts it on a row of its own (2024_04_PHI_TB), which is left as it is.
    """
    desc = pl.col("desc").fill_null("")
    pts = (
        pl.when(pl.col("extra_point_result") == "good")
        .then(1)
        .when(pl.col("two_point_conv_result") == "success")
        .then(2)
        .otherwise(0)
    )
    dpts = pl.when(
        desc.str.contains(r"(?i)defensive two-point attempt") & desc.str.contains(r"(?i)attempt succeeds")
    ).then(2)
    pat = pl.col("pat_of_id").is_not_null()
    tries = view.filter(pat).select(
        pl.col("pat_of_id").alias("espn_play_id"), pts.alias("_pts"), dpts.otherwise(0).alias("_dpts")
    )
    assert view.schema["espn_play_id"] == tries.schema["espn_play_id"] == pl.Int64
    # the touchdown row: whoever went up by 6+ on it scored the try; the other side scored any return
    off = (pl.col("posteam_score_post") - pl.col("posteam_score")) >= 6
    other_gain = (
        pl.when(off)
        .then(pl.col("defteam_score_post") - pl.col("defteam_score"))
        .otherwise(pl.col("posteam_score_post") - pl.col("posteam_score"))
    )
    v = view.join(tries, on="espn_play_id", how="left").with_columns(
        pl.when(~pat & (pl.col("_dpts") > 0) & (other_gain >= pl.col("_dpts")))
        .then(pl.col("_dpts"))
        .otherwise(0)
        .cast(pl.Float64)
        .alias("_dinc")
    )
    back = v.select(pl.col("espn_play_id").alias("pat_of_id"), pl.col("_dinc").alias("_dinc_td"))
    v = v.join(back, on="pat_of_id", how="left").with_columns(
        pl.when(pat).then(pts).otherwise(pl.col("_pts").fill_null(0)).cast(pl.Float64).alias("_pts"),
        pl.when(pat).then(pl.col("_dinc_td").fill_null(0)).otherwise(pl.col("_dinc")).alias("_dinc"),
    )
    home, away = pl.col("posteam_type") == "home", pl.col("posteam_type") == "away"
    pos_total = pl.when(home).then(pl.col("total_home_score")).otherwise(pl.col("total_away_score"))
    def_total = pl.when(home).then(pl.col("total_away_score")).otherwise(pl.col("total_home_score"))
    scorer_home = (off & home) | (~off & away)
    p_, d_ = pl.col("_pts"), pl.col("_dinc")
    v = v.with_columns(
        pl.when(pat)
        .then(pos_total)
        .otherwise(pl.col("posteam_score_post") - pl.when(off).then(p_).otherwise(d_))
        .alias("posteam_score_post"),
        pl.when(pat)
        .then(def_total)
        .otherwise(pl.col("defteam_score_post") - pl.when(off).then(d_).otherwise(p_))
        .alias("defteam_score_post"),
        pl.when(pat)
        .then(pl.col("total_home_score"))
        .otherwise(pl.col("total_home_score") - pl.when(scorer_home).then(p_).otherwise(d_))
        .alias("total_home_score"),
        pl.when(pat)
        .then(pl.col("total_away_score"))
        .otherwise(pl.col("total_away_score") - pl.when(scorer_home).then(d_).otherwise(p_))
        .alias("total_away_score"),
    ).with_columns(
        pl.when(pat).then(pl.col("posteam_score_post") - p_).otherwise(pl.col("posteam_score")).alias("posteam_score"),
        pl.when(pat).then(pl.col("defteam_score_post") - d_).otherwise(pl.col("defteam_score")).alias("defteam_score"),
    )
    return v.with_columns(
        (pl.col("posteam_score") - pl.col("defteam_score")).alias("score_differential"),
        (pl.col("posteam_score_post") - pl.col("defteam_score_post")).alias("score_differential_post"),
    ).drop("_pts", "_dpts", "_dinc", "_dinc_td")


#: Columns nflverse leaves null on a timeout row (every season 2007-2024); its ``ydstogo`` there is 0.
_TIMEOUT_NULL = (
    "posteam",
    "posteam_type",
    "defteam",
    "side_of_field",
    "yardline_100",
    "down",
    "posteam_score",
    "defteam_score",
    "score_differential",
    "posteam_score_post",
    "defteam_score_post",
    "score_differential_post",
    "posteam_timeouts_remaining",
    "defteam_timeouts_remaining",
)


def _timeout_rows(view: pl.DataFrame) -> pl.DataFrame:
    """nflverse's timeout row: no possession, down, field position or possession-relative score."""
    t = (pl.col("play_type_nfl") == "TIMEOUT").fill_null(False)
    return view.with_columns(
        *[pl.when(t).then(None).otherwise(pl.col(c)).alias(c) for c in _TIMEOUT_NULL],
        pl.when(t).then(0.0).otherwise(pl.col("ydstogo")).alias("ydstogo"),
    )


def _carried(df: pl.DataFrame) -> list[pl.Expr]:
    """ESPN columns the later steps read: the play's wall clock, timeouts, air yards, and the drive record.

    ``time_of_day`` is ESPN's ``wallclock`` (to the second; nflverse's NFL-feed stamp has milliseconds).
    ``_c_*`` columns are inputs only and are dropped before the frame is returned.
    """

    def col(c: str, t: pl.DataType) -> pl.Expr:
        return (pl.col(c) if c in df.columns else pl.lit(None)).cast(t)

    timeout = (pl.col("type.text") == "Timeout").fill_null(False)
    home_to, away_to = (
        (pl.col(c) == True).fill_null(False) if c in df.columns else pl.lit(False)  # noqa: E712
        for c in ("homeTimeoutCalled", "awayTimeoutCalled")
    )
    return [
        col("wallclock", pl.Utf8).alias("time_of_day"),
        timeout.cast(pl.Int64).alias("timeout"),
        pl.when(timeout & home_to)
        .then(pl.col("home_team"))
        .when(timeout & away_to)
        .then(pl.col("away_team"))
        .alias("timeout_team"),
        col("air_yards", pl.Float64).alias("air_yards"),
        col("yards_after_catch", pl.Float64).alias("yards_after_catch"),
        col("drive.start.clock.displayValue", pl.Utf8).alias("_c_clock_start"),
        col("drive.end.clock.displayValue", pl.Utf8).alias("_c_clock_end"),
        col("drive.start.text", pl.Utf8).alias("_c_start_text"),
        col("rusher_player_name", pl.Utf8).alias("_c_rusher"),
        col("end_of_half", pl.Boolean).alias("_c_end_of_half"),
    ]


#: ``add_description_features`` columns the view takes from the Shield regexes run on ESPN's text; each beats
#: (or equals) the view's own value on 2007 / 2014 / 2019 / 2024. ``pass_length`` / ``pass_location`` /
#: ``qb_kneel`` / ``qb_spike`` / ``aborted_play`` / ``play_type`` keep the view's (equal, or better in 2014).
_TEXT_FEATURES = (
    "run_location",
    "run_gap",
    "qb_scramble",
    "shotgun",
    "no_huddle",
    "touchback",
    "out_of_bounds",
    "replay_or_challenge",
    "replay_or_challenge_result",
    "penalty_type",
)


def _text_features(view: pl.DataFrame) -> pl.DataFrame:
    """nflfastR's description columns from ESPN's text (the Shield regexes), and ``clean_pbp``'s ``pass`` / ``rush``."""
    if view.height == 0:  # the Shield helpers hand an empty frame back as it is
        flags = {"run_location", "run_gap", "replay_or_challenge_result", "penalty_type"}
        return view.with_columns(
            *[pl.lit(None, dtype=pl.Utf8 if c in flags else pl.Int64).alias(c) for c in _TEXT_FEATURES],
            pl.lit(None, dtype=pl.Int64).alias("pass"),
            pl.lit(None, dtype=pl.Int64).alias("rush"),
        )
    rx = add_description_features(view.select("desc", "play_type", "penalty")).select(_TEXT_FEATURES)
    v = view.with_columns(rx.get_column(c) for c in _TEXT_FEATURES)  # same height: rx is computed from view
    # a rusher named by either the GSIS slot or the processor (an athlete the players table lacks)
    rusher = pl.coalesce(*[c for c in ("rusher_player_name", "_c_rusher") if c in v.columns])
    with_pr = add_pass_rush(
        v.select(
            "game_id",
            "play_id",
            # the play's own text: a touchdown row can still carry its folded try ("... TWO-POINT CONVERSION
            # ATTEMPT. J.Allen pass to ..."), whose " pass " would make a rushing touchdown a pass
            # ESPN's scoring-summary form ("D.Prescott 10 Yd Pass From ...") names the pass in title case
            pl.col("desc")
            .str.replace(r"(?i)TWO-POINT CONVERSION ATTEMPT.*$", "")
            .str.replace_all(r" Pass [Ff]rom ", " pass from ")
            .alias("desc"),
            "qb_scramble",
            "qb_kneel",
            "kickoff_attempt",
            rusher.alias("rusher_player_name"),
        )
    )
    # a split two-point try is a pass or a run by its own type (its rusher may be on the touchdown row only)
    two = (pl.col("play_type_nfl") == "PAT2").fill_null(False)
    return v.with_columns(with_pr["pass"], with_pr["rush"]).with_columns(
        pl.when(two).then((pl.col("play_type") == "pass").cast(pl.Int64)).otherwise(pl.col("pass")).alias("pass"),
        pl.when(two).then((pl.col("play_type") == "run").cast(pl.Int64)).otherwise(pl.col("rush")).alias("rush"),
    )


def _drive_detail(view: pl.DataFrame) -> pl.DataFrame:
    """nflfastR's drive and series columns on ESPN's own drives.

    ``fixed_drive`` is ESPN's drive number: nflfastR's ``new_drive`` cascade run on this frame scores 86-92%
    against nflverse (2014-2024), ESPN's own drives 97-99%. ``fixed_drive_result`` / ``series*`` are nflfastR's
    result cascades (the Shield ports) grouped by it; ``drive_game_clock_*`` and ``drive_start_yard_line`` come
    from ESPN's drive record, the counts from the drive's plays.
    """
    g = ["game_id", "fixed_drive"]
    v = view.with_row_index("_order").with_columns(
        fixed_drive=pl.col("drive"), play_seq=pl.int_range(pl.len()).over("game_id")
    )
    # NFLPlayProcess drops the period-end text ("END QUARTER 2") the cascade reads and flags end_of_half instead;
    # a stronger result earlier in the drive (Touchdown, Field goal) still wins in last_or_first_result
    half_end = pl.when((pl.col("_c_end_of_half") == True).fill_null(False)).then(pl.lit("End of half"))  # noqa: E712
    v = v.with_columns(_swap=swapped_posteam(v)).with_columns(
        _tmp=pl.coalesce(drive_level_tmp_result(pl.col("_swap")), half_end)
    )
    # a play outside any drive (null fixed_drive) matches no drive's result: null keys never join
    v = last_or_first_result(v, g, "_tmp", "fixed_drive_result")
    v = add_series_data(v.drop("_swap", "_tmp"))
    real = pl.col("play_type").is_not_null() & (pl.col("play_type_nfl") != "TIMEOUT").fill_null(True)
    is_try = ((pl.col("extra_point_attempt") == 1) | (pl.col("two_point_attempt") == 1)).fill_null(False)
    first_down = (
        (pl.col("first_down_rush") == 1) | (pl.col("first_down_pass") == 1) | (pl.col("first_down_penalty") == 1)
    )
    agg = (
        v.filter(real & pl.col("fixed_drive").is_not_null())
        .group_by(g)
        .agg(
            drive_first_downs=first_down.sum().cast(pl.Float64),
            # a try is spotted at the 2 / the 15 on every touchdown drive: only the drive's own plays count
            drive_inside20=(pl.col("yardline_100").filter(~is_try).min() <= 20).cast(pl.Float64),
            drive_play_id_started=pl.col("play_id").min().cast(pl.Float64),
        )
    )
    v = v.join(agg, on=g, how="left")

    def clock(c: str) -> pl.Expr:  # ESPN's "7:47" -> nflverse's "07:47"
        return pl.col(c).str.zfill(5)

    start = pl.col("_c_start_text").str.split(" ")
    club = start.list.get(0, null_on_oob=True).replace({"WSH": "WAS", "LAR": "LA"}).replace(team_abbr_mapping)
    return (
        v.with_columns(
            pl.col("series").cast(pl.Float64),
            pl.col("series_success").cast(pl.Int64),
            clock("_c_clock_start").alias("drive_game_clock_start"),
            clock("_c_clock_end").alias("drive_game_clock_end"),
            pl.when(pl.col("_c_start_text") == "50")
            .then(pl.lit("50"))
            .otherwise(pl.concat_str(club, start.list.get(1, null_on_oob=True), separator=" "))
            .alias("drive_start_yard_line"),
        )
        .sort("_order")
        .drop("play_seq", "_order")
    )


#: ESPN participant role -> the processor's own athlete-id columns for it (used without a participants frame).
_PROCESSOR_ROLE_IDS = {
    "passer": ("passer_player_id",),
    "rusher": ("rusher_player_id",),
    "receiver": ("receiver_player_id",),
    "interceptor": ("interception_player_id",),
    "sacked_by": ("sack_player_id", "sack_player_id2"),
    "pass_defender": ("pass_breakup_player_id",),
    "forced_by": ("fumble_forced_player_id",),
    "recoverer": ("fumble_recovered_player_id",),
    "fumbler": ("fumble_player_id",),
    "punter": ("punter_player_id",),
    "returner": ("punt_return_player_id", "kickoff_return_player_id"),
    "blocker": ("punt_block_player_id", "fg_block_player_id"),
    "kicker": ("kickoff_player_id", "fg_kicker_player_id", "xp_kicker_player_id"),
}
#: Roles whose processor columns name one athlete each (a list); the others are alternatives (first non-null).
_PROCESSOR_LIST_ROLES = {"sacked_by"}
#: ESPN participant roles the player slots read (``espn_nfl_play_participants`` ``{role}_player_ids``).
_PARTICIPANT_ROLES = (
    "passer",
    "rusher",
    "receiver",
    "tackler",
    "assisted_by",
    "sacked_by",
    "forced_by",
    "fumbler",
    "recoverer",
    "pass_defender",
    "kicker",
    "punter",
    "returner",
    "penalized",
    "blocker",
    "scorer",
    "pat_passer",
    "pat_rusher",
    "pat_scorer",
)
_ROLES = tuple(dict.fromkeys((*_PARTICIPANT_ROLES, *_PROCESSOR_ROLE_IDS)))


def _str_id(expr: pl.Expr, dtype: pl.DataType, name: str) -> pl.Expr:
    """An athlete id as a string; a float id is refused (``"123.0"`` would never match)."""
    if dtype in (pl.Float32, pl.Float64):
        raise ValueError(f"espn_nfl_pbp_nflverse: {name} is {dtype}; pass integer or string athlete ids")
    return expr.cast(pl.Utf8)


def _id_list(expr: pl.Expr, dtype: pl.DataType, name: str) -> pl.Expr:
    """A participants ``{role}_player_ids`` column as a list of id strings, whatever form it comes in."""
    if isinstance(dtype, pl.List):
        _str_id(expr, dtype.inner, name)  # refuses a list of floats
        return expr.cast(pl.List(pl.Utf8))
    # the release's text: '["4039007","4568624"]' or '3121003'; split on commas, never inside a number
    text = _str_id(expr, dtype, name).str.replace_all(r'[\[\]"\s]', "")
    empty = pl.lit([], dtype=pl.List(pl.Utf8))
    return pl.when(text == "").then(empty).otherwise(text.str.split(","))  # "[]": no athlete


def _role_lists(source: pl.DataFrame, key: str, participants: bool) -> pl.DataFrame:
    """One row per ESPN play: ``_key`` (the ESPN play id) and ``_r_<role>``, the role's ESPN athlete ids in order.

    From an :func:`~sportsdataverse.nfl.espn_nfl_play_participants` frame (``{role}_player_ids``: a list column as
    the function returns it, or the release's JSON text / single id) or, without one, from the processor's own
    ``*_player_id`` columns.
    """
    lists = {}
    for r in _ROLES:
        if participants:
            many, one = f"{r}_player_ids", f"{r}_player_id"
            if many in source.columns:
                lists[r] = _id_list(pl.col(many), source.schema[many], many)
            elif one in source.columns:
                lists[r] = pl.concat_list(_str_id(pl.col(one), source.schema[one], one)).list.drop_nulls()
        else:
            cols = [c for c in _PROCESSOR_ROLE_IDS.get(r, ()) if c in source.columns]
            ids = [_str_id(pl.col(c), source.schema[c], c) for c in cols]
            if ids:
                lists[r] = pl.concat_list(ids) if r in _PROCESSOR_LIST_ROLES else pl.concat_list(pl.coalesce(ids))
                lists[r] = lists[r].list.drop_nulls()
    out = source.select(
        _int_id(pl.col(key), source.schema[key], key).alias("_key"),
        *[e.alias(f"_r_{r}") for r, e in lists.items()],
    )
    out = out.drop_nulls("_key").unique("_key", keep="first", maintain_order=True)
    empty = pl.lit([], dtype=pl.List(pl.Utf8))
    return out.with_columns(empty.alias(f"_r_{r}") for r in _ROLES if f"_r_{r}" not in out.columns)


def _players_map(players: pl.DataFrame) -> tuple[dict[str, str], dict[str, str]]:
    """``espn_id -> gsis_id`` and ``gsis_id -> name`` from the nflverse players table (nflverse's ``J.Allen`` form)."""
    missing = {"gsis_id", "espn_id"} - set(players.columns)
    if missing:
        raise ValueError(f"espn_nfl_pbp_nflverse: players lacks {sorted(missing)}; pass load_nfl_players()")

    def col(c: str) -> pl.Expr:
        return (pl.col(c) if c in players.columns else pl.lit(None)).cast(pl.Utf8)

    # nflverse's "A.St. Brown" form; a player the table has no short name for gets it from the display name
    full = col("display_name").str.strip_chars()
    initial = pl.format("{}.{}", full.str.slice(0, 1), full.str.splitn(" ", 2).struct[1])
    t = players.select(
        _str_id(pl.col("espn_id"), players.schema["espn_id"], "players espn_id").alias("e"),
        pl.col("gsis_id").cast(pl.Utf8).alias("g"),
        pl.coalesce(col("short_name"), initial).alias("n"),
    ).drop_nulls(["e", "g"])
    dupes = t.group_by("e").agg(pl.col("g").n_unique()).filter(pl.col("g") > 1)["e"].sort().to_list()
    if dupes:
        raise ValueError(f"espn_nfl_pbp_nflverse: players maps ESPN id(s) {dupes[:5]} to more than one gsis_id")
    e2g = dict(t.unique("e", keep="first", maintain_order=True).select("e", "g").iter_rows())
    g2n = dict(t.drop_nulls("n").unique("g", keep="first", maintain_order=True).select("g", "n").iter_rows())
    return e2g, g2n


def _player_slots(view: pl.DataFrame) -> dict[str, pl.Expr]:
    """nflverse's player slot -> its ESPN athlete id, from the ``_r_<role>`` lists and the play's flags.

    Measured on whole seasons: a tackle with an ESPN ``assisted_by`` is nflverse's ``tackle_with_assist_1`` (the
    assisters ``assist_tackle_1/2``), else ``solo_tackle_1``; one sacker is ``sack``, two are ``half_sack_1/2``;
    the interceptor is ESPN's first ``pass_defender`` on an interception; a no-play keeps only the penalized
    player; a split try reads its touchdown row's PAT kicker / passer / rusher / scorer and the touchdown row
    loses them.
    """

    def r(role: str, i: int = 0) -> pl.Expr:
        return pl.col(f"_r_{role}").list.get(i, null_on_oob=True)

    def n(role: str) -> pl.Expr:
        return pl.col(f"_r_{role}").list.len().fill_null(0)

    def f(c: str) -> pl.Expr:
        return (pl.col(c) == 1).fill_null(False)

    pt = pl.col("play_type")
    pat = pl.col("pat_of_id").is_not_null() if "pat_of_id" in view.columns else pl.lit(False)
    two = f("two_point_attempt")
    kick_play = pt.is_in(["kickoff", "field_goal", "extra_point"]).fill_null(False)
    lone_sack = f("sack") & (n("sacked_by") == 1)
    tackler_alone = (n("assisted_by") == 0) & (n("tackler") > 0)
    slots = {
        "passer": r("passer"),
        "receiver": r("receiver"),
        "rusher": r("rusher"),
        "interception": pl.when(f("interception")).then(pl.coalesce(r("interceptor"), r("pass_defender"))),
        "punt_returner": pl.when(pt == "punt").then(r("returner")),
        "kickoff_returner": pl.when(pt == "kickoff").then(r("returner")),
        "punter": r("punter"),
        "kicker": pl.when(kick_play).then(r("kicker")),
        "blocked": r("blocker"),
        # nflverse credits a lone sacker with the tackle for loss too (its tackled_for_loss flag stays 0)
        "tackle_for_loss_1": pl.when(f("tackled_for_loss"))
        .then(pl.coalesce(r("tackler"), r("assisted_by")))
        .when(lone_sack & (pl.col("yards_gained") < 0).fill_null(False))
        .then(r("sacked_by")),
        "forced_fumble_player_1": r("forced_by"),
        "solo_tackle_1": pl.when(tackler_alone)
        .then(r("tackler"))
        .when(lone_sack & (n("tackler") == 0))
        .then(r("sacked_by")),
        "tackle_with_assist_1": pl.when(n("assisted_by") > 0).then(r("tackler")),
        "assist_tackle_1": pl.coalesce(r("assisted_by"), pl.when(n("sacked_by") == 2).then(r("sacked_by"))),
        "assist_tackle_2": pl.coalesce(r("assisted_by", 1), pl.when(n("sacked_by") == 2).then(r("sacked_by", 1))),
        "pass_defense_1": r("pass_defender"),
        "fumbled_1": r("fumbler"),
        "fumble_recovery_1": r("recoverer"),
        "sack": pl.when(lone_sack).then(r("sacked_by")),
        "half_sack_1": pl.when(f("sack") & (n("sacked_by") == 2)).then(r("sacked_by")),
        "half_sack_2": pl.when(f("sack") & (n("sacked_by") == 2)).then(r("sacked_by", 1)),
        "penalty": r("penalized"),
        "td": pl.when(f("touchdown")).then(r("scorer")),
    }
    # a split try: its touchdown row's PAT participants, nothing else
    on_pat = {
        "passer": r("pat_passer"),
        "rusher": r("pat_rusher"),
        "receiver": pl.when(pt == "pass").then(r("pat_scorer")),
        "kicker": pl.when(~two).then(r("kicker")),
    }
    no_play = (pt == "no_play").fill_null(False)
    out = {}
    for s, e in slots.items():
        e = pl.when(pat).then(on_pat.get(s, pl.lit(None, dtype=pl.Utf8))).otherwise(e)
        out[s] = e if s == "penalty" else pl.when(no_play).then(None).otherwise(e)
    return out


def _add_players(view: pl.DataFrame, roles: pl.DataFrame, players: pl.DataFrame) -> pl.DataFrame:
    """nflverse's GSIS player ids and names (``<slot>_player_id`` / ``_player_name``) from ESPN athlete ids.

    ESPN ids go to GSIS through the nflverse players table's ``espn_id`` (never by name); an athlete the table
    does not carry stays null. A split try is keyed on its touchdown row (``pat_of_id``), which holds its PAT
    participants.
    """
    e2g, g2n = _players_map(players)
    key = pl.coalesce("pat_of_id", "espn_play_id") if "pat_of_id" in view.columns else pl.col("espn_play_id")
    v = view.with_columns(key.alias("_key"))
    assert v.schema["_key"] == roles.schema["_key"] == pl.Int64
    v = v.join(roles, on="_key", how="left")
    slots = _player_slots(v)
    gsis = {s: e.replace_strict(e2g, default=None, return_dtype=pl.Utf8) for s, e in slots.items()}
    named = []
    for s, g in gsis.items():
        named += [
            g.alias(f"{s}_player_id"),
            g.replace_strict(g2n, default=None, return_dtype=pl.Utf8).alias(f"{s}_player_name"),
        ]
    v = v.with_columns(named).drop("_key", *[c for c in roles.columns if c != "_key"])
    # the defenders' side: the punting team on a punt, the passing team on an interception, else the defense
    swap = (pl.col("play_type") == "punt") | (pl.col("interception") == 1)
    defense = pl.when(swap.fill_null(False)).then(pl.col("posteam")).otherwise(pl.col("defteam"))
    carrier = pl.when(swap.fill_null(False)).then(pl.col("defteam")).otherwise(pl.col("posteam"))
    lost = (pl.col("fumble_lost") == 1).fill_null(False)
    teams = {
        "solo_tackle_1": defense,
        "tackle_with_assist_1": defense,
        "assist_tackle_1": defense,
        "assist_tackle_2": defense,
        "forced_fumble_player_1": defense,
        "fumbled_1": carrier,
        "fumble_recovery_1": pl.when(lost).then(defense).otherwise(carrier),
    }
    return v.with_columns(
        pl.when(pl.col(f"{s}_player_id").is_not_null()).then(t).alias(f"{s}_team") for s, t in teams.items()
    )


def _merged_roles(df: pl.DataFrame, participants: Optional[pl.DataFrame]) -> pl.DataFrame:
    """The ``_r_<role>`` lists: ESPN's participants win, the processor's own id fills a role they leave empty."""
    own = df.filter(pl.col("pat_of_id").is_null()) if "pat_of_id" in df.columns else df
    roles = _role_lists(own, "id", participants=False)
    if participants is None:
        return roles
    # the processor's id fills a role the participants leave empty (2024 kick returners)
    feed = _role_lists(participants, "play_id", participants=True)
    assert roles.schema["_key"] == feed.schema["_key"] == pl.Int64
    return roles.join(feed, on="_key", how="full", coalesce=True, suffix="_feed").select(
        "_key",
        *[
            pl.when(pl.col(f"_r_{r}_feed").list.len().fill_null(0) > 0)
            .then(pl.col(f"_r_{r}_feed"))
            .otherwise(pl.col(f"_r_{r}"))
            .alias(f"_r_{r}")
            for r in _ROLES
        ],
    )


def _build_view(
    plays: pl.DataFrame,
    schedule: Optional[pl.DataFrame],
    split_pats: bool,
    participants: Optional[pl.DataFrame],
    players: Optional[pl.DataFrame],
    enrich: bool,
) -> pl.DataFrame:
    """:func:`espn_nfl_pbp_nflverse`'s steps, in order."""
    missing = [c for c in _REQUIRED if c not in plays.columns]
    if missing:
        raise ValueError(f"espn_nfl_pbp_nflverse: plays lack {missing}; pass NFLPlayProcess output")
    _int_id(pl.col("id"), plays.schema["id"], "id")  # refuses a float play id ...
    _int_id(pl.col("game_id"), plays.schema["game_id"], "game_id")  # ... and a float game id
    df = _attach_nflverse_game_id(plays, schedule)
    if split_pats and "pat_of_id" not in df.columns:  # a frame split_pat_rows already split is kept as it is
        df = split_pat_rows(df)
    if "pat_of_id" in df.columns:
        df = _pat_possession(df)
    keys = _keys_and_teams(df)
    df = df.with_columns(keys)
    mid = [
        *_clock_and_situation(),
        *_flags(df),
        # an older feed can lack a column (2007 has no extra_point_result): null, so the schema is fixed
        *[(pl.col(e) if e in df.columns else pl.lit(None)).cast(t).alias(nv) for nv, (e, t) in _RENAMES.items()],
        *_carried(df),
    ]
    df = df.with_columns(mid)
    last = _yards_and_types(df)
    names = [e.meta.output_name() for e in (*keys, *mid, *last)]
    view = df.with_columns(last).select([*names, *(["pat_of_id"] if "pat_of_id" in df.columns else [])])
    view = _timeout_rows(_pat_field(_pat_scores(view)) if "pat_of_id" in view.columns else view)
    if players is not None:
        view = _add_players(view, _merged_roles(df, participants), players)
    view = _drive_detail(_text_features(view.drop("pat_of_id", strict=False)))
    view = view.drop([c for c in view.columns if c.startswith("_c_")])
    if schedule is not None:
        view = _add_schedule(view, schedule)
    if enrich:
        from sportsdataverse.nfl.ep_wp import enrich_nfl_pbp

        view = enrich_nfl_pbp(view, add_fourth_down=False)  # nflverse play_by_play has no 4th-down columns
    return view


def espn_nfl_pbp_nflverse(
    plays: pl.DataFrame,
    schedule: Optional[pl.DataFrame] = None,
    *,
    split_pats: bool = True,
    participants: Optional[pl.DataFrame] = None,
    players: Optional[pl.DataFrame] = None,
    enrich: bool = False,
) -> pl.DataFrame:
    """ESPN NFL play-by-play in nflverse ``play_by_play`` names, conventions and dtypes.

    Turns :class:`~sportsdataverse.nfl.NFLPlayProcess` output (``plays_frame``, one game or many, or the
    published ``espn_nfl_pbp`` release) into nflverse's play-by-play: the keys, teams, field position,
    clock, down and distance, scores, timeouts, play types, the 0/1 play flags, yardage, nflfastR's
    description columns (``run_gap``, ``shotgun``, ``out_of_bounds``, ``penalty_type``, ...), drive and series
    detail, with ``schedule`` nflverse's game columns (nflfastR's ``add_game_data``), with ``players`` the
    GSIS player ids and names, and with ``enrich`` nflfastR's model columns
    (:func:`~sportsdataverse.nfl.ep_wp.enrich_nfl_pbp`: EP / EPA / WP / WPA, try values, the air / YAC
    splits). It is additive: the cfbfastR-shape ESPN frame is not changed. Nothing is fetched unless
    ``enrich`` needs the xYAC model, which is not bundled (it is downloaded and cached on first use; without
    ESPN air yards its step is skipped).

    Conventions follow nflverse, not ESPN: ``game_id`` is nflverse's ``YYYY_WW_AWAY_HOME`` and ``play_id`` the
    GSIS play id ESPN embeds in its play ``id`` (2006 on), so the frame joins nflverse ``play_by_play`` on
    ``(game_id, play_id)``. Teams come from the team ids and the game id, never from ESPN's abbreviations, in
    the franchise's current abbreviation as nflverse writes them (``LA`` for the 2007 Rams). A kickoff
    belongs to the receiving team; a timeout row has no possession, down or field position; an offensive
    touchdown counts as a first down; a declined penalty is no penalty. With ``split_pats`` the try ESPN
    folds into a touchdown row (2014 on) gets its own ``extra_point`` / two-point row
    (:func:`~sportsdataverse.nfl.split_pat_rows`) and the touchdown row's scores leave the try's points out.

    Agreement with nflverse on whole seasons (2007, 2014, 2019, 2024) is 99%+ for most columns. The known
    gaps are ESPN's: ``desc`` is ESPN's text, not the NFL feed's; ESPN stamps some plays (scoring plays,
    whole 2024 games) with the clock after the snap; before 2009 many plays carry a stub text ("Rush",
    "Pass Completion"), so the text-derived columns (pass / run direction, completion, yards) are thin
    there; the timeouts are the processor's own count; a split PAT row's ``play_id`` is the touchdown's
    plus one (ESPN gives the try no id), so it does not join nflverse's PAT row.

    Players: ESPN participant roles go to nflverse's slots by rules measured on whole seasons (two sackers are
    ``half_sack_1/2``, a no-play keeps only the penalized player, a split try takes its touchdown's PAT players),
    and ESPN athlete ids go to GSIS through the players table's ``espn_id``, never by name. ``fixed_drive`` is
    ESPN's own drive number, ``fixed_drive_result`` / ``series`` nflfastR's cascades on it. ESPN's air yards are
    empty, so ``cp`` / ``cpoe`` / xYAC / the air-yards splits stay null; ``qb_hit`` and jersey numbers are not built.

    Args:
        plays: ``NFLPlayProcess(...).plays_frame`` or the ``espn_nfl_pbp`` release (one or many games).
        schedule: The nflverse schedule (:func:`~sportsdataverse.nfl.load_nfl_schedule`). Adds the game
            columns, and supplies ``nflverse_game_id`` (from its ``espn`` id) when ``plays`` has none
            (``NFLPlayProcess`` stamps it from ESPN's header since this release).
        split_pats: Give each folded PAT / two-point try its own row, as nflverse does (default ``True``).
        participants: :func:`~sportsdataverse.nfl.espn_nfl_play_participants` output (one or many games, or
            the ``espn_nfl_play_participants`` release), keyed on the ESPN play id. Its roles win; without it
            (or for a role it leaves empty) the processor's own athlete ids are read. Used only with ``players``.
        players: The nflverse players table (:func:`~sportsdataverse.nfl.load_nfl_players`; ``gsis_id``,
            ``espn_id`` and ``short_name``). Adds the ``<slot>_player_id`` / ``_player_name`` columns and the
            tackler / fumble teams.
        enrich: Run :func:`~sportsdataverse.nfl.ep_wp.enrich_nfl_pbp` (without the fourth-down surface,
            which nflverse's play-by-play does not carry) on the view (default ``False``).

    Returns:
        pl.DataFrame: One row per ESPN play (plus one per split try) in nflverse's column names, flags as
        ``Int64`` 0/1, measures as ``Float64``, ids and names as strings, plus ``espn_game_id`` /
        ``espn_play_id`` to join back to ESPN. The schedule columns are present only with ``schedule``, the
        player columns only with ``players``, the model columns only with ``enrich``.

    Raises:
        ValueError: If ``plays`` lacks the processor's columns, or carries no ``nflverse_game_id`` and no
            ``schedule`` (with ``game_id`` and ``espn``) is given or the schedule lacks one of its games, or
            an id column is a float (a play / game id, or an athlete id in ``plays``, ``participants`` or
            ``players``), or ``players`` lacks ``gsis_id`` / ``espn_id``, or a split try's synthetic id is
            already a play's (:func:`~sportsdataverse.nfl.split_pat_rows`).

    Example:
        Process a game (fetched from ESPN) and read it as nflverse play-by-play::

            from sportsdataverse.nfl import NFLPlayProcess, espn_nfl_pbp_nflverse, load_nfl_schedule
            proc = NFLPlayProcess(gameId=401671740)
            proc.espn_nfl_pbp()
            proc.run_processing_pipeline()
            pbp = espn_nfl_pbp_nflverse(proc.plays_frame, load_nfl_schedule([2024]))

        With GSIS player ids from ESPN's participants, and the model columns::

            from sportsdataverse.nfl import espn_nfl_play_participants, load_nfl_players
            pbp = espn_nfl_pbp_nflverse(
                proc.plays_frame,
                participants=espn_nfl_play_participants(401671740),
                players=load_nfl_players(),
                enrich=True,
            )

    See Also:
        * :func:`~sportsdataverse.nfl.split_pat_rows` -- the PAT split on the ESPN frame itself
        * :func:`~sportsdataverse.nfl.load_nfl_pbp` -- nflverse's own play-by-play, the oracle
        * :func:`~sportsdataverse.nfl.espn_nfl_play_participants` -- the ESPN roles the player slots read
        * `nflfastR`_ -- the R package whose play-by-play conventions this follows

    .. _nflfastR: https://www.nflfastr.com
    """
    return _build_view(plays, schedule, split_pats, participants, players, enrich)
