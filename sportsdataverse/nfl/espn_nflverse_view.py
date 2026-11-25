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
from sportsdataverse.nfl.nfl_pbp import _espn_play_type_nfl, split_pat_rows

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
    """``nflverse_game_id`` from the frame itself, else from the schedule's ``espn`` event id."""
    if "nflverse_game_id" in plays.columns:
        return plays
    if schedule is None or not {"game_id", "espn"} <= set(schedule.columns):
        raise ValueError(
            "espn_nfl_pbp_nflverse: plays carry no nflverse_game_id; pass the nflverse schedule "
            "(with its game_id and espn columns) to map the ESPN game id"
        )
    ids = schedule.select(
        _int_id(pl.col("espn"), schedule.schema["espn"], "schedule espn").alias("_espn"),
        pl.col("game_id").cast(pl.Utf8).alias("nflverse_game_id"),
    ).drop_nulls("_espn")
    out = plays.with_columns(_int_id(pl.col("game_id"), plays.schema["game_id"], "game_id").alias("_espn"))
    assert out.schema["_espn"] == ids.schema["_espn"] == pl.Int64
    out = out.join(ids.unique("_espn", keep="first", maintain_order=True), on="_espn", how="left")
    unmatched = out.filter(pl.col("nflverse_game_id").is_null())["_espn"].unique().sort().to_list()
    if unmatched:
        raise ValueError(f"espn_nfl_pbp_nflverse: the schedule has no game for ESPN game id(s) {unmatched[:5]}")
    return out.drop("_espn")


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
    is_home = pos == pl.col("homeTeamId").cast(pl.Int64)
    is_away = pos == pl.col("awayTeamId").cast(pl.Int64)
    posteam = pl.when(is_home).then(home).when(is_away).then(away)
    defteam = pl.when(is_home).then(away).when(is_away).then(home)
    kickoff = (pl.col("kickoff_play") == True).fill_null(False)  # noqa: E712
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
        parts.list.get(1).cast(pl.Int64).alias("week"),
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
    pos_post, def_post = pl.col("pos_team_score"), pl.col("def_pos_team_score")
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
        pl.col("start.pos_team_score").cast(pl.Float64).alias("posteam_score"),
        pl.col("start.def_pos_team_score").cast(pl.Float64).alias("defteam_score"),
        pl.col("start.pos_score_diff").cast(pl.Float64).alias("score_differential"),
        pos_post.cast(pl.Float64).alias("posteam_score_post"),
        def_post.cast(pl.Float64).alias("defteam_score_post"),
        (pos_post - def_post).cast(pl.Float64).alias("score_differential_post"),
        pl.col("homeScore").cast(pl.Float64).alias("total_home_score"),
        pl.col("awayScore").cast(pl.Float64).alias("total_away_score"),
        _timeouts("home").alias("home_timeouts_remaining"),
        _timeouts("away").alias("away_timeouts_remaining"),
        pl.col("start.posTeamTimeouts").cast(pl.Float64).alias("posteam_timeouts_remaining"),
        pl.col("start.defPosTeamTimeouts").cast(pl.Float64).alias("defteam_timeouts_remaining"),
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
    ).drop("_pts", "_dpts", "_dinc", "_dinc_td", "pat_of_id")


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


def espn_nfl_pbp_nflverse(
    plays: pl.DataFrame,
    schedule: Optional[pl.DataFrame] = None,
    *,
    split_pats: bool = True,
) -> pl.DataFrame:
    """ESPN NFL play-by-play in nflverse ``play_by_play`` names, conventions and dtypes.

    Turns :class:`~sportsdataverse.nfl.NFLPlayProcess` output (``plays_frame``, one game or many, or the
    published ``espn_nfl_pbp`` release) into nflverse's base play-by-play: the keys, teams, field position,
    clock, down and distance, scores, timeouts, play types, the 0/1 play flags, yardage and drive detail,
    and, with ``schedule``, nflverse's game columns (nflfastR's ``add_game_data``). It is additive: the
    cfbfastR-shape ESPN frame is not changed, and nothing is fetched.

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

    Args:
        plays: ``NFLPlayProcess(...).plays_frame`` or the ``espn_nfl_pbp`` release (one or many games).
        schedule: The nflverse schedule (:func:`~sportsdataverse.nfl.load_nfl_schedule`). Adds the game
            columns, and supplies ``nflverse_game_id`` (from its ``espn`` id) when ``plays`` has none.
        split_pats: Give each folded PAT / two-point try its own row, as nflverse does (default ``True``).

    Returns:
        pl.DataFrame: One row per ESPN play (plus one per split try) in nflverse's column names, flags as
        ``Int64`` 0/1, measures as ``Float64``, plus ``espn_game_id`` / ``espn_play_id`` to join back to
        ESPN (participants are keyed on the ESPN play id). The schedule columns are present only when
        ``schedule`` is given.

    Raises:
        ValueError: If ``plays`` lacks the processor's columns, or carries no ``nflverse_game_id`` and no
            ``schedule`` (with ``game_id`` and ``espn``) is given or the schedule lacks one of its games, or
            an id column is a float, or a split try's synthetic id is already a play's
            (:func:`~sportsdataverse.nfl.split_pat_rows`).

    Example:
        Process a game (fetched from ESPN) and read it as nflverse play-by-play::

            from sportsdataverse.nfl import NFLPlayProcess, espn_nfl_pbp_nflverse, load_nfl_schedule
            proc = NFLPlayProcess(gameId=401671740)
            proc.espn_nfl_pbp()
            proc.run_processing_pipeline()
            pbp = espn_nfl_pbp_nflverse(proc.plays_frame, load_nfl_schedule([2024]))

        See Also:
            * :func:`~sportsdataverse.nfl.split_pat_rows` -- the PAT split on the ESPN frame itself
            * :func:`~sportsdataverse.nfl.load_nfl_pbp` -- nflverse's own play-by-play, the oracle
            * `nflfastR`_ -- the R package whose play-by-play conventions this follows

        .. _nflfastR: https://www.nflfastr.com
    """
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
    ]
    df = df.with_columns(mid)
    last = _yards_and_types(df)
    names = [e.meta.output_name() for e in (*keys, *mid, *last)]
    view = df.with_columns(last).select([*names, *(["pat_of_id"] if "pat_of_id" in df.columns else [])])
    view = _timeout_rows(_pat_scores(view) if "pat_of_id" in view.columns else view)
    return view if schedule is None else _add_schedule(view, schedule)
