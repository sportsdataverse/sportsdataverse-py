"""Oracle-anchored whole-game characterization of ``NFLPlayProcess`` (quality program task 1.1).

Every committed ESPN capture (``tests/nfl/fixtures/README.md``: 31 games, 2002-2026) is run offline
through the full pipeline and each column that has an oracle OUTSIDE this processor is asserted
against it:

* nflfastR's R source (``nflverse-dev/nflfastR/R/helper_add_ep_wp.R:710-724``: a non-scoring play
  that ends a half, regulation or the game has ``epa == -ep``);
* the team-summary producers' ``opportunity_run``
  (``cfbfastR-cfb-data/R/espn_cfb_15_team_summaries_creation.R:610``, ``nfl-data`` ``build.py:244``);
* the standard adjusted-line-yards band (losses 120%, yards 0-4 at 100%, 5-10 at 50%, beyond 10 at
  0%: the program's ruling for task 2.1; the producers' own ``line_yards`` reading is not it);
* the CFB processor's fixed copy of the same stage (``cfb_pbp.py``: the ``-3`` rushing block of
  ``24dd2fdec6``, the kickoff-return ``end.down`` rewrite);
* ESPN's own feed (``scoringPlay``, the play text, the summary's ``boxscore``);
* the committed nflverse slices of two of the games (``fixtures/nflverse/``), joined play by play:
  ESPN's play id is the game id followed by the GSIS ``play_id``.

Snapshot ban: no expected value is a stored copy of this processor's output. A check that is red
today carries ``xfail(strict=True, raises=AssertionError, reason="<finding id>: ...")`` on exactly
the games where it is red, so the fix turns it XPASS (a failure) and must drop the marker in the
same PR, and any other exception (a renamed column, a crash) still fails; every other
game asserts the oracle plainly. Tolerances on model columns are the agreement measured on HEAD
36f7a02f1b (2026-10-11), rounded one step toward failing; raise them as fixes land, never lower.
"""

from __future__ import annotations

import math

import polars as pl
import pytest

from sportsdataverse.nfl.ep_wp import _derive_wpa, calculate_completion_probability, calculate_epa, calculate_wpa
from sportsdataverse.nfl.model_vars import clock_stoppage_vec, kickoff_vec
from tests.nfl._whole_game import CAPTURES, FIX, run

pytestmark = [pytest.mark.whole_game, pytest.mark.xdist_group("nfl_whole_game")]


def _t(name: str) -> pl.Expr:
    return (pl.col(name) == True).fill_null(False)  # noqa: E712


def _xfail(reason: str) -> pytest.MarkDecorator:
    """Red today on the oracle; only a failed assertion counts as the expected failure."""
    return pytest.mark.xfail(strict=True, raises=AssertionError, reason=reason)


def _cases(known: dict[int, str] | None = None, games=CAPTURES) -> list:
    """One param per game; the games in ``known`` (game -> "<finding>: <why>") are strict xfails."""
    known = known or {}
    return [pytest.param(g, marks=_xfail(known[g])) if g in known else g for g in games]


def _all(reason: str) -> dict[int, str]:
    return dict.fromkeys(CAPTURES, reason)


# ESPN admin row of the 2002-04 feed, typed "Unknown" (C-M2): not a play.
_ADMIN_END = pl.col("text").str.contains(r"(?i)^\s*end of (the )?game").fill_null(False)
_PLAY = ~pl.col("type.text").is_in(clock_stoppage_vec).fill_null(False) & ~_ADMIN_END
_RUSH = _t("rush")
_YDS = pl.col("yds_rushed")


def _require(ok: bool, why: str) -> None:
    """A broken precondition fails outright, never as an xfail's expected AssertionError."""
    if not ok:
        pytest.fail(why)


def _bad(frame: pl.DataFrame, ok: pl.Expr, cols: list[str]) -> list[tuple]:
    return frame.filter(~ok.fill_null(False)).select("id", *cols).rows()


# ---------------------------------------------------------------------------------------------
# C-C1 (critical, FIXED on HEAD by #739): the regression gate
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("game_id", _cases())
def test_opportunity_run_is_a_rush_of_four_or_more(game_id):
    # Oracle: cfbfastR-cfb-data espn_cfb_15_team_summaries_creation.R:610
    # `opportunity_run = (rush == 1) & (yds_rushed >= 4)` (nfl-data build.py:244 the same).
    f = run(game_id).plays
    oracle = (_RUSH & (_YDS >= 4)).fill_null(False)
    assert _bad(f, _t("opportunity_run") == oracle, ["opportunity_run", "rush", "yds_rushed"]) == []
    assert f.filter(pl.col("opp_highlight_yards") > 0).height > 0  # it read 0 on every row (C-C1)


# ---------------------------------------------------------------------------------------------
# C-H1 / C-NF-3: the rushing decomposition
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "game_id",
    _cases(_all("C-H1: second_level_yards is 0.5*(adj-4), the sum is 0.5 short on every 4+ carry")),
)
def test_rushing_decomposition_sums_to_the_carry(game_id):
    # Oracle: the CFB copy's fix (24dd2fdec6, cfb_pbp.py second_level_yards `- 3`): line + second
    # level + open field is the carry, on every rush of 4+ yards.
    f = run(game_id).plays.filter(_RUSH & (_YDS >= 4))
    parts = pl.col("line_yards") + pl.col("second_level_yards") + pl.col("open_field_yards")
    _require(f.height > 0, "no 4+ yard rush in the capture")
    assert _bad(f, (parts - _YDS).abs() < 1e-9, ["yds_rushed", "line_yards", "second_level_yards"]) == []


@pytest.mark.parametrize("game_id", _cases())
def test_every_rush_has_all_three_parts(game_id):
    # #739 moved the gates to `rush`: a "Rushing Touchdown" or fumbled carry has a full decomposition.
    f = run(game_id).plays.filter(_RUSH)
    full = pl.all_horizontal(pl.col(c).is_not_null() for c in ("line_yards", "second_level_yards", "open_field_yards"))
    assert _bad(f, full, ["type.text", "yds_rushed"]) == []


@pytest.mark.parametrize(
    "game_id",
    _cases(_all("C-NF-3: line_yards is on a 0-3 / 4-8 / cap 5.5 band, not the standard 0-4 / 5-10 / cap 7")),
)
def test_line_yards_on_the_standard_band(game_id):
    # Oracle: standard adjusted line yards (Football Outsiders ALY, Connelly's college version, the
    # plan's "cap 7"), per yard: losses 120%, yards 0-4 at 100%, yards 5-10 at 50%, beyond 10 at 0%.
    # The program's ruling for task 2.1 (ClaudeCowork plan audit/EXECUTION-FINDINGS.md). The three
    # summary producers' reading (0.5 * the whole carry on 5-10, 0 from 11) is not the standard: it
    # credits a 5-yard run less than a 4-yard run.
    band = (
        pl.when(_YDS < 0).then(1.2 * _YDS).when(_YDS <= 4).then(_YDS).when(_YDS <= 10).then(4 + 0.5 * (_YDS - 4))
    ).otherwise(7.0)
    f = run(game_id).plays.filter(_RUSH & _YDS.is_not_null())
    assert _bad(f, (pl.col("line_yards") - band).abs() < 1e-9, ["yds_rushed", "line_yards"]) == []


# ---------------------------------------------------------------------------------------------
# C-H2 (and the C-NF-12 timeout twin): the play that ends a half, regulation or the game
# ---------------------------------------------------------------------------------------------

_OT_GAMES = (260917021, 271209010, 401547449, 401671835, 401772636)  # a period 5 in the capture
_H2 = "C-H2: end_of_half reads the unfilled lead_half, null on the game's last play / False at regulation end"
_HALF_END_KNOWN = {
    **{(g, "game"): _H2 for g in CAPTURES},
    **{(g, "regulation"): _H2 for g in _OT_GAMES},
    (400791590, "half"): "C-NF-12: the half ends on a Timeout row, which takes end_of_half from the last play",
}
_BOUNDARIES = [(g, "half") for g in CAPTURES] + [(g, "regulation") for g in _OT_GAMES] + [(g, "game") for g in CAPTURES]


@pytest.mark.parametrize(
    ("game_id", "boundary"),
    [
        pytest.param(g, b, marks=_xfail(_HALF_END_KNOWN[(g, b)])) if (g, b) in _HALF_END_KNOWN else (g, b)
        for g, b in _BOUNDARIES
    ],
)
def test_half_ending_play(game_id, boundary):
    # Oracle: nflfastR helper_add_ep_wp.R:710-724 -- the last play of Q2, of Q4 when the game goes to
    # overtime, and of the game ends a half; when it did not score, epa = 0 - ep.
    plays = run(game_id).plays.filter(_PLAY)
    if boundary == "half":
        plays = plays.filter(pl.col("period.number") == 2)
    elif boundary == "regulation":
        plays = plays.filter(pl.col("period.number") == 4)
    last = plays.tail(1).row(0, named=True)
    assert last["end_of_half"] is True, (last["id"], last["type.text"], last["end_of_half"])
    if last["scoringPlay"] is not True:
        assert last["EPA"] is not None and abs(last["EPA"] + last["EP_start"]) < 1e-9, (last["id"], last["EPA"])


# ---------------------------------------------------------------------------------------------
# C-H5, C-M1, C-NF-6, C-M2: rows the CFB copy or ESPN's feed already gets right
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "game_id",
    _cases(
        dict.fromkeys(
            (400791590, 400874512, 401671740),
            "C-H5: ESPN's end.down -1 on a returned kickoff reaches the EP model (CFB rewrites it to 1)",
        )
    ),
)
def test_kickoff_return_ends_on_a_real_down(game_id):
    # Oracle: the CFB copy (cfb_pbp.py, __add_downs_data): a non-scoring kickoff ends at a down 1-4.
    f = run(game_id).plays.filter(pl.col("type.text").is_in(kickoff_vec) & ~_t("scoringPlay"))
    assert _bad(f, pl.col("end.down").is_in([1, 2, 3, 4]), ["type.text", "end.down"]) == []


@pytest.mark.parametrize(
    "game_id",
    _cases(
        dict.fromkeys(
            (251113015, 260917021, 261022011, 261126002, 271209010, 301003002, 331103020),
            "EX-1.1-c: a pre-2014 missed field goal keeps ESPN's end.down -1, so EP_end is scored at down -1",
        )
    ),
)
def test_missed_field_goal_ends_on_a_real_down(game_id):
    # Oracle: the opponent takes over at a first down (the 2015+ feeds ship down 1 or 3); C-H5's
    # mechanism on a second play type. Measured: 8 rows in 7 games, EPA off by 0.6-3.0 points.
    f = run(game_id).plays.filter(pl.col("type.text").is_in(["Field Goal Missed", "Blocked Field Goal"]))
    assert _bad(f.filter(~_t("scoringPlay")), pl.col("end.down").is_in([1, 2, 3, 4]), ["type.text", "end.down"]) == []


@pytest.mark.parametrize(
    "game_id",
    _cases(
        dict.fromkeys(
            (291018018, 401030824, 401671835),
            "C-M1: td_play is True on 'TOUCHDOWN NULLIFIED by Penalty' rows (CFB has _touchdown_negated)",
        )
    ),
)
def test_nullified_touchdown_is_not_a_touchdown(game_id):
    f = run(game_id).plays.filter(pl.col("text").str.contains(r"(?i)TOUCHDOWN NULLIFIED").fill_null(False))
    assert _bad(f, ~_t("td_play"), ["td_play", "scoringPlay"]) == []


@pytest.mark.parametrize(
    "game_id",
    _cases(
        dict.fromkeys(
            (291018018, 400791590, 401128075, 401671740, 401772726),
            "C-NF-6: a replay-REVERSED score (scoringPlay False) keeps td_play / pass_td / rush_td True",
        )
    ),
)
def test_reversed_touchdown_is_not_a_touchdown(game_id):
    # Oracle: ESPN's own scoringPlay is False on these rows ("... and the play was REVERSED").
    f = run(game_id).plays.filter(pl.col("text").str.contains("(?i)REVERSED").fill_null(False) & ~_t("scoringPlay"))
    assert _bad(f, ~(_t("td_play") | _t("pass_td") | _t("rush_td")), ["type.text", "td_play"]) == []


@pytest.mark.parametrize(
    "game_id",
    _cases(
        dict.fromkeys(
            (221013007, 221013028, 231221029, 241010028),
            "C-M2: the 2002-04 'End of the game.' row is typed Unknown and survives the type.text marker filter",
        )
    ),
)
def test_no_end_of_game_admin_row_is_a_play(game_id):
    # Oracle: nflfastR helper_add_ep_wp.R:706-708, 728-731 -- the END GAME row is no play (epa, ep NA).
    assert run(game_id).plays.filter(_ADMIN_END).select("id", "type.text", "EPA").rows() == []


# ---------------------------------------------------------------------------------------------
# Team box score vs ESPN's own box (C-L17's live-gate-free box-score test, the NFL half)
# ---------------------------------------------------------------------------------------------

_FEED_GAP = (
    "EX-1.1-b: ESPN's play feed disagrees with its own box on passing TDs (251113015 MIA: a null-text "
    "TD typed Fumble; 260917021 NYG: a 'Pass Completion' stub typed Passing Touchdown; 400951676 SEA: no TD row)"
)


@pytest.mark.parametrize(
    "game_id",
    _cases(
        {
            **dict.fromkeys(
                (291018018, 401128075, 401671740, 401772726),
                "C-NF-6: a replay-REVERSED touchdown is counted in the team box Pass_TD / Rush_TD",
            ),
            **dict.fromkeys((251113015, 260917021, 400951676), _FEED_GAP),
        }
    ),
)
def test_team_box_touchdowns_match_espn_box(game_id):
    r = run(game_id)
    espn = {}
    for team in r.summary["boxscore"]["players"]:
        for cat in team["statistics"]:
            for name, key in (("passing", "passingTouchdowns"), ("rushing", "rushingTouchdowns")):
                if cat["name"] == name:
                    espn[(int(team["team"]["id"]), name)] = int(cat["totals"][cat["keys"].index(key)])
    ours = {}
    for name, table, col in (("passing", "pass", "Pass_TD"), ("rushing", "rush", "Rush_TD")):
        for team_id, n in pl.DataFrame(r.game["advBoxScore"][table]).group_by("pos_team").agg(pl.col(col).sum()).rows():
            ours[(int(team_id), name)] = int(n)
    _require(len(espn) == 4, f"ESPN box lacks a team passing/rushing total: {espn}")
    assert {k: (v, ours.get(k, 0)) for k, v in espn.items() if ours.get(k, 0) != v} == {}


# ---------------------------------------------------------------------------------------------
# nflverse slices: 401547449 = 2023_04_LA_IND, 401671740 = 2024_04_PHI_TB (C-H3, C-M3, C-M5)
# ---------------------------------------------------------------------------------------------

SLICES = {401547449: "2023_04_LA_IND", 401671740: "2024_04_PHI_TB"}
_MATCHED = {401547449: 171, 401671740: 159}  # ESPN rows carrying a GSIS play_id nflverse has


def _nflverse(game_id: int) -> pl.DataFrame:
    return pl.read_parquet(FIX / "nflverse" / f"{SLICES[game_id]}.parquet")


def _joined(game_id: int) -> pl.DataFrame:
    """ESPN rows joined to nflverse on play_id (ESPN id = game id + GSIS play_id); nflverse cols ``*_nv``."""
    nv = _nflverse(game_id).with_columns(pl.col("play_id").cast(pl.Int64))
    espn = run(game_id).plays.with_columns(
        play_id=pl.col("id").cast(pl.Utf8).str.slice(len(str(game_id))).cast(pl.Int64)
    )
    joined = espn.join(nv.rename({c: f"{c}_nv" for c in nv.columns if c != "play_id"}), on="play_id", how="inner")
    _require(
        joined.height == _MATCHED[game_id], f"{joined.height} ESPN rows match nflverse, expected {_MATCHED[game_id]}"
    )
    return joined


_H3 = (
    "C-H3 + C-M3: penalty_1st_conv needs the CFB phrase '1st down', so no penalty first down opens a "
    "series; fd_gain keys on ESPN's stale end.down"
)


@pytest.mark.parametrize("game_id", _cases(dict.fromkeys(SLICES, _H3), games=tuple(SLICES)))
def test_series_count_matches_nflverse(game_id):
    # measured: 57 vs 62 (401547449), 57 vs 59 (401671740)
    assert run(game_id).plays["series"].n_unique() == _nflverse(game_id)["series"].n_unique()


@pytest.mark.parametrize("game_id", _cases(dict.fromkeys(SLICES, _H3), games=tuple(SLICES)))
def test_first_down_penalty_count_matches_nflverse(game_id):
    # measured: 2 vs 6, 1 vs 3
    ours = run(game_id).plays["first_down_penalty"].cast(pl.Int64).sum()
    assert ours == _nflverse(game_id)["first_down_penalty"].sum()


@pytest.mark.parametrize(
    "game_id",
    _cases(dict.fromkeys(SLICES, f"{_H2} -> the final drive's fixed_drive_result is null"), games=tuple(SLICES)),
)
def test_fixed_drive_result_has_nflverse_null_count(game_id):
    # measured: 1 and 7 nulls; nflverse has 0 in both games
    assert (
        run(game_id).plays["fixed_drive_result"].null_count() == _nflverse(game_id)["fixed_drive_result"].null_count()
    )


_SNAP = pl.col("play_type_nv").is_in(["pass", "run", "punt", "field_goal", "qb_kneel", "qb_spike"])
# ESPN states a kickoff from the receiving side (distance 10, 65 to go) where nflverse writes
# ydstogo 0 / yardline_100 35, and ESPN's touchdown row carries the try's point in its score:
# those rows are outside the domain of the columns below, by the feed's convention. Rushing yards
# are compared on ESPN rushes (nflverse also books an aborted snap as a 0-yard run; the `rush`
# definitions differ, the yardage does not).
_EXACT = [
    ("period", "qtr", _SNAP),
    ("start.down", "down", _SNAP),
    ("start.distance", "ydstogo", _SNAP),
    ("start.yardsToEndzone", "yardline_100", _SNAP),
    ("yds_rushed", "rushing_yards", _RUSH & pl.col("rushing_yards_nv").is_not_null()),
    ("yds_receiving", "receiving_yards", pl.col("receiving_yards_nv").is_not_null()),
    ("end.homeScore", "total_home_score", pl.col("touchdown_nv") == 0),
    ("end.awayScore", "total_away_score", pl.col("touchdown_nv") == 0),
    ("td_play", "touchdown", pl.lit(True)),
    ("pass_td", "pass_touchdown", pl.lit(True)),
    ("rush_td", "rush_touchdown", pl.lit(True)),
    ("fixed_drive", "fixed_drive", pl.lit(True)),
]
_EXACT_KNOWN = {
    (401671740, "td_play"): "C-NF-6: two replay-REVERSED scores keep td_play True (nflverse touchdown 0)",
    (401671740, "rush_td"): "C-NF-6: the replay-REVERSED 2-yard run keeps rush_td True",
    (401671740, "fixed_drive"): (
        "EX-1.1-a: ESPN's muffed 'Punt' (RECOVERED by the kicking team) is re-typed 'Punt Team Fumble "
        "Recovery' with fumble_vec / fumble_lost False, so nflfastR helper_add_fixed_drives.R:83-112 never "
        "opens a drive (20 vs 21)"
    ),
}


@pytest.mark.parametrize(
    ("game_id", "ours", "theirs", "domain"),
    [
        pytest.param(g, a, b, d, id=f"{g}-{a}", marks=_xfail(_EXACT_KNOWN[(g, a)]))
        if (g, a) in _EXACT_KNOWN
        else pytest.param(g, a, b, d, id=f"{g}-{a}")
        for g in SLICES
        for a, b, d in _EXACT
    ],
)
def test_play_column_equals_nflverse(game_id, ours, theirs, domain):
    j = _joined(game_id).filter(domain)
    _require(j.height > 0, f"no {ours} rows in the comparison domain")
    left, right = pl.col(ours).cast(pl.Float64), pl.col(f"{theirs}_nv").cast(pl.Float64)
    assert j.filter(left.ne_missing(right)).select("id", ours, f"{theirs}_nv").rows() == []


# (corr floor, MAE ceiling, rows) per game and column: measured on HEAD, see the module docstring.
# Rows that carry an open finding (C-H2/C-NF-12 half-ending plays, C-H5 kickoffs, C-M1/C-NF-6
# negated scores) are left out so the floor is not loosened by them. When 2.2 (C-H2), 2.3 (C-H5) and
# 2.5 (C-M1/C-NF-6) land, each drops its exclusion in _open_finding_rows and re-measures the floors
# here (raise only).
_MODEL = {
    (401547449, "EP_start", "ep"): (0.996, 0.117, 167),  # 0.99611, 0.11615
    (401547449, "EPA", "epa"): (0.961, 0.247, 167),  # 0.96175, 0.24653
    (401547449, "wp", "wp"): (0.993, 0.024, 167),  # 0.99339, 0.02345
    (401547449, "vegas_wp", "vegas_wp"): (0.987, 0.063, 167),  # 0.98767, 0.06284
    (401671740, "EP_start", "ep"): (0.992, 0.126, 154),  # 0.99278, 0.12545
    (401671740, "EPA", "epa"): (0.927, 0.328, 154),  # 0.92781, 0.32713
    (401671740, "wp", "wp"): (0.999, 0.007, 154),  # 0.99980, 0.00649
    (401671740, "vegas_wp", "vegas_wp"): (0.991, 0.062, 154),  # 0.99174, 0.06128
}


def _open_finding_rows(game_id: int) -> list[int]:
    f = run(game_id).plays
    plays = f.filter(_PLAY)
    ends = [plays.tail(1)["id"].item()]
    if game_id in _OT_GAMES:
        ends.append(plays.filter(pl.col("period.number") == 4).tail(1)["id"].item())
    h5 = pl.col("type.text").is_in(kickoff_vec) & ~_t("scoringPlay") & ~pl.col("end.down").is_in([1, 2, 3, 4])
    negated = pl.col("text").str.contains("(?i)NULLIFIED|REVERSED").fill_null(False)
    return ends + f.filter(h5.fill_null(False) | negated)["id"].to_list()


@pytest.mark.parametrize(("game_id", "ours", "theirs"), list(_MODEL), ids=lambda v: str(v))
def test_model_column_agrees_with_nflverse(game_id, ours, theirs):
    corr_floor, mae_ceiling, rows = _MODEL[(game_id, ours, theirs)]
    j = _joined(game_id).filter(
        ~pl.col("id").is_in(_open_finding_rows(game_id))
        & pl.col(ours).is_not_null()
        & pl.col(f"{theirs}_nv").is_not_null()
    )
    a, b = pl.col(ours).cast(pl.Float64), pl.col(f"{theirs}_nv").cast(pl.Float64)
    corr, mae = j.select(pl.corr(a, b).alias("corr"), (a - b).abs().mean().alias("mae")).row(0)
    assert j.height >= rows
    assert corr >= corr_floor, corr
    assert mae <= mae_ceiling, mae


# C-L20: the CP booster scored on nflverse's own inputs must reproduce nflverse's cp (a constant
# cp = 0.5 passed 121 tests). Measured: corr 0.99492 / 0.99396, MAE 0.01552 / 0.01551, 63 / 76 rows.
_CP = {401547449: (0.994, 0.016, 63), 401671740: (0.993, 0.016, 76)}
_CP_INPUTS = [
    "play_id", "air_yards", "season", "ydstogo", "down", "posteam", "home_team", "roof",
    "pass_location", "qb_hit", "complete_pass", "yardline_100",
]  # fmt: skip


@pytest.mark.parametrize("game_id", list(_CP))
def test_completion_probability_reproduces_nflverse(game_id):
    corr_floor, mae_ceiling, rows = _CP[game_id]
    nv = _nflverse(game_id)
    out = calculate_completion_probability(nv.select(_CP_INPUTS)).join(
        nv.select("play_id", "cp"), on="play_id", suffix="_nv"
    )
    out = out.filter(pl.col("cp").is_not_null() & pl.col("cp_nv").is_not_null())
    corr, mae = out.select(
        pl.corr("cp", "cp_nv").alias("corr"), (pl.col("cp") - pl.col("cp_nv")).abs().mean().alias("mae")
    ).row(0)
    assert out.height >= rows
    assert not math.isnan(corr) and corr >= corr_floor, corr
    assert mae <= mae_ceiling, mae


# ---------------------------------------------------------------------------------------------
# C-L12: two real games concatenated give the per-game answer, column by column
# ---------------------------------------------------------------------------------------------


def _same_per_game(fn, frames: list[pl.DataFrame], key: str) -> list[str]:
    """Columns where ``fn(concat)`` differs from ``concat(fn(each game))`` (row order kept)."""
    flat = [f.select(c for c, t in f.schema.items() if not t.is_nested()) for f in frames]
    both = pl.concat(flat, how="diagonal_relaxed")
    joint = fn(both)
    per_game = pl.concat([fn(both.filter(pl.col(key) == g)) for g in both[key].unique(maintain_order=True)])
    assert joint.columns == per_game.columns and joint.height == per_game.height == both.height
    return [c for c in joint.columns if not joint[c].equals(per_game[c], null_equal=True)]


@pytest.mark.parametrize("step", ["calculate_epa", "calculate_wpa"])
def test_real_concat_equals_per_game_runs(step):
    # The pipeline's own inputs to the step, recorded for the two slice games and stacked.
    fn = {"calculate_epa": calculate_epa, "calculate_wpa": calculate_wpa}[step]
    frames = [getattr(run(g), f"{step.split('_')[1]}_input") for g in SLICES]
    assert _same_per_game(fn, frames, "game_id") == []


def test_real_concat_reaches_the_post_score_clock_stoppage_guard():
    """Reordered real rows: game 1 ends on a score, game 2 opens on its own first official timeout.

    No capture opens on a clock-stoppage row, so the stacked captures above never reach the
    ``scoring_play.shift(1).over("game_id")`` guard of ``calculate_epa``. Here every row is the
    pipeline's own input for 401547449 / 401671740; only their order is changed (game 1 truncated
    after its last scoring play, game 2's first clock-stoppage row moved to its head), and the
    oracle is still the per-game run of the same rows.
    """
    first, second = run(401547449).epa_input, run(401671740).epa_input
    last_score = first.with_row_index("_i").filter(_t("scoring_play"))["_i"].max()
    first = first.head(last_score + 1)
    stop = second.with_row_index("_i").filter(pl.col("type.text").is_in(clock_stoppage_vec))["_i"].min()
    second = pl.concat([second.slice(stop, 1), second.slice(0, stop), second.slice(stop + 1)])
    _require(first["scoring_play"][-1] is True, "game 1 must end on a scoring play")
    assert _same_per_game(calculate_epa, [first, second], "game_id") == []


def test_real_concat_equals_per_game_runs_nflverse_wpa():
    # The nflverse-path WPA derivation (enrich_nfl_pbp step 4) on the two committed slices.
    frames = [
        _nflverse(g).select(
            "game_id", "posteam", "home_team", "down", "wp", "vegas_wp", "desc", "home_score", "away_score"
        )
        for g in SLICES
    ]
    assert _same_per_game(_derive_wpa, frames, "game_id") == []
