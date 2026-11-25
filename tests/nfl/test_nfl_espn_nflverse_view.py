"""``espn_nfl_pbp_nflverse``: ESPN processor output in nflverse ``play_by_play`` shape, scored against nflverse.

Oracle: ``fixtures/nflverse/espn_nflverse_view.parquet`` (nflverse's play-by-play for 2024_04_PHI_TB,
2014_11_MIN_CHI and 2007_14_SD_TEN, the games of the committed ESPN captures 401671740, 400554340 and
271209010) and ``espn_nflverse_view_schedule.parquet`` (their nflverse schedule rows). Provenance:
``fixtures/README.md``. The captures run through ``NFLPlayProcess`` offline, so the view is checked on the
processor's own frame. Phase 3 reads ``fixtures/espn_nflverse_view/play_participants.parquet`` (the
``espn_nfl_play_participants`` release rows of the 2014-on captures) and ``players.parquet`` (the nflverse players
rows of their athletes).
"""

from __future__ import annotations

import functools
import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nfl import NFLPlayProcess, espn_nfl_pbp_nflverse

FIX = Path(__file__).parent / "fixtures"
GAMES = {401671740: "2024_04_PHI_TB", 400554340: "2014_11_MIN_CHI", 271209010: "2007_14_SD_TEN"}
#: Captures for single rules, kept out of the whole-game capture set (``fixtures/espn_nflverse_view/``):
#: 2019_13_OAK_KC has a defensive two-point return its touchdown row counts; 2024_01_NYJ_SF has a split
#: two-point pass and run (and a whole game ESPN clocks after the snap, so it is not in the floor set).
EXTRA = {
    401128067: "2019_13_OAK_KC",
    401671696: "2024_01_NYJ_SF",
    401671886: "2024_20_BAL_BUF",
    401128090: "2019_09_GB_LAC",
    401671712: "2024_01_MIN_NYG",
}

#: Every view column nflverse also has must agree on >= 97% of each game's joined plays, except these
#: (floor = the lowest observed game rate, rounded down; the whole-season rates are in the PR body):
FLOORS = {
    # the processor's own timeout count; before 2014 ESPN lists few timeouts (2007 game: 0.54-0.62)
    "home_timeouts_remaining": 0.54,
    "away_timeouts_remaining": 0.57,
    "posteam_timeouts_remaining": 0.61,
    "defteam_timeouts_remaining": 0.58,
    # pre-2009 stub texts ("Rush", "Pass Completion") carry no direction / completion / yards (2007 game)
    "pass_length": 0.64,
    "pass_location": 0.64,
    "run_location": 0.64,
    "complete_pass": 0.82,
    "incomplete_pass": 0.82,
    "passing_yards": 0.82,
    "receiving_yards": 0.82,
    "yards_gained": 0.95,
    "punt_attempt": 0.91,
    "touchback": 0.95,
    # ESPN stamps scoring plays (and whole 2024 games) with the clock after the snap (2024 game: 0.905)
    "quarter_seconds_remaining": 0.90,
    "half_seconds_remaining": 0.90,
    "game_seconds_remaining": 0.90,
    "time": 0.90,
    # ESPN's 2007 drive splits differ from the NFL feed's
    "drive": 0.96,
    "drive_time_of_possession": 0.91,
    "drive_ended_with_score": 0.96,
    "ydsnet": 0.96,
    "penalty_yards": 0.96,
    # phase 4: nflfastR's drive-result cascade on ESPN's drives; ESPN's own drive-end clock (2024 game: 0.956)
    "fixed_drive_result": 0.95,
    "drive_game_clock_end": 0.95,
    "drive_first_downs": 0.89,
    # ESPN names the team on few 2007 timeouts
    "timeout_team": 0.95,
}
#: The 2007 game's own floors for the phase 4-5 columns: its stub texts ("Rush", "Pass Completion") carry no gap,
#: formation or rusher, and ESPN's 2007 drive splits differ from the NFL feed's (series is cumulative).
FLOORS_2007 = {
    "run_gap": 0.75,
    "shotgun": 0.80,
    "out_of_bounds": 0.90,
    "penalty_type": 0.95,
    "pass": 0.59,
    "rush": 0.65,
    "fixed_drive": 0.96,
    "series": 0.35,
    "series_result": 0.95,
    "drive_play_id_started": 0.77,
    "drive_game_clock_start": 0.96,
    "drive_game_clock_end": 0.90,
}
#: Player slots (ids, names, teams) are scored in test_player_ids_agree_with_nflverse, on the participants games.
PLAYER_TEAMS = {
    "solo_tackle_1_team",
    "tackle_with_assist_1_team",
    "assist_tackle_1_team",
    "assist_tackle_2_team",
    "forced_fumble_player_1_team",
    "fumbled_1_team",
    "fumble_recovery_1_team",
}
#: ESPN's text is not the NFL feed's description: carried for the text rules, never equal. ESPN's wall clock is to
#: the second (nflverse's NFL-feed stamp has milliseconds); ESPN's air yards / YAC are empty.
UNSCORED = {"desc", "time_of_day", "air_yards", "yards_after_catch"}
#: nflfastR's ``add_game_data`` columns: straight from the schedule, so exact.
SCHEDULE_COLUMNS = (
    "old_game_id",
    "game_date",
    "location",
    "result",
    "total",
    "home_score",
    "away_score",
    "div_game",
    "roof",
    "surface",
    "temp",
    "wind",
    "home_coach",
    "away_coach",
    "game_stadium",
    "stadium_id",
    "spread_line",
    "total_line",
)


@functools.cache
def _plays(espn_game_id: int) -> pl.DataFrame:
    path = FIX / ("espn_nflverse_view" if espn_game_id in EXTRA else "") / f"summary_{espn_game_id}_trimmed.json.gz"
    summary = json.load(gzip.open(path, "rt", encoding="utf-8"))
    proc = NFLPlayProcess(gameId=espn_game_id, join_participants=False)
    proc.espn_nfl_pbp(summary=summary)
    proc.run_processing_pipeline()
    return proc.plays_frame


def _schedule() -> pl.DataFrame:
    return pl.read_parquet(FIX / "nflverse" / "espn_nflverse_view_schedule.parquet")


def _participants() -> pl.DataFrame:
    return pl.read_parquet(FIX / "espn_nflverse_view" / "play_participants.parquet")


def _players() -> pl.DataFrame:
    return pl.read_parquet(FIX / "espn_nflverse_view" / "players.parquet")


@functools.cache
def _view() -> pl.DataFrame:
    return pl.concat(
        [espn_nfl_pbp_nflverse(_plays(g), _schedule(), participants=_participants(), players=_players()) for g in GAMES]
    )


def _joined() -> pl.DataFrame:
    nv = pl.read_parquet(FIX / "nflverse" / "espn_nflverse_view.parquet")
    view = _view()
    assert view.schema["game_id"] == nv.schema["game_id"] == pl.Utf8
    assert view.schema["play_id"] == nv.schema["play_id"] == pl.Int64
    return view.join(nv, on=["game_id", "play_id"], how="inner", suffix="__nv")


def _rate(j: pl.DataFrame, c: str) -> float:
    a, b = pl.col(c), pl.col(f"{c}__nv")
    if j.schema[c].is_numeric():
        eq = (a.cast(pl.Float64) - b.cast(pl.Float64)).abs() <= 1e-4
    else:
        eq = a.cast(pl.Utf8) == b.cast(pl.Utf8)
    return j.select((eq | (a.is_null() & b.is_null())).fill_null(False).mean()).item()


def test_keys_join_nflverse_and_teams_are_nflverse_abbreviations():
    view, j = _view(), _joined()
    assert view.select("game_id", "play_id").n_unique() == view.height
    assert set(view["game_id"].unique()) == set(GAMES.values())
    # every ESPN play of the three games joins except the split tries and ESPN-only rows (497 of 540)
    assert j.height >= 490
    old = j.filter(pl.col("game_id") == "2007_14_SD_TEN")
    # the game id keeps "SD"; nflverse's play-by-play (and the view) write the franchise's "LAC"
    assert set(old["home_team"].unique()) == {"TEN"} and set(old["away_team"].unique()) == {"LAC"}
    assert set(old["posteam"].drop_nulls().unique()) == {"LAC", "TEN"}


@pytest.mark.parametrize("game_id", list(GAMES.values()))
def test_every_shared_column_meets_its_floor(game_id):
    j = _joined().filter(pl.col("game_id") == game_id)
    player = lambda c: c.endswith(("_player_id", "_player_name")) or c in PLAYER_TEAMS  # noqa: E731
    shared = [c for c in _view().columns if f"{c}__nv" in j.columns and c not in UNSCORED and not player(c)]
    assert len(shared) >= 120
    floors = {**FLOORS, **(FLOORS_2007 if game_id.startswith("2007") else {})}
    low = {c: round(r, 4) for c in shared if (r := _rate(j, c)) < floors.get(c, 0.97)}
    assert low == {}, f"{game_id}: columns below their floor: {low}"


def test_schedule_columns_are_exact():
    j = _joined()
    rates = {c: _rate(j, c) for c in SCHEDULE_COLUMNS}
    assert rates == {c: 1.0 for c in SCHEDULE_COLUMNS}


def test_split_try_rows_and_touchdown_scores():
    """2024: each folded try is its own extra_point / two-point row; the touchdown row leaves its points out."""
    view = _view().filter(pl.col("game_id") == "2024_04_PHI_TB")
    nv = pl.read_parquet(FIX / "nflverse" / "espn_nflverse_view.parquet").filter(pl.col("game_id") == "2024_04_PHI_TB")
    tries = view.filter(pl.col("play_type_nfl").is_in(["XP_KICK", "PAT2"]))
    assert tries.height == nv.filter(pl.col("play_type_nfl").is_in(["XP_KICK", "PAT2"])).height == 6
    assert tries["posteam"].null_count() == 0  # the scoring side, from the split's numeric team id
    tds = _joined().filter((pl.col("game_id") == "2024_04_PHI_TB") & (pl.col("touchdown") == 1))
    assert tds.height > 0
    for c in ("posteam_score_post", "total_home_score", "total_away_score"):
        assert (tds[c] == tds[f"{c}__nv"]).all(), c


def test_missing_game_id_and_float_ids_are_refused():
    plays = _plays(401671740)
    with pytest.raises(ValueError, match="nflverse_game_id"):
        espn_nfl_pbp_nflverse(plays.drop("nflverse_game_id"))
    with pytest.raises(ValueError, match="lack"):
        espn_nfl_pbp_nflverse(plays.drop("start.pos_team.id"), _schedule())
    with pytest.raises(ValueError, match="Float64"):
        espn_nfl_pbp_nflverse(plays.drop("nflverse_game_id"), _schedule().with_columns(pl.col("espn").cast(pl.Float64)))


def test_a_declined_penalty_is_no_penalty():
    """nflverse's ``penalty`` is 0 on a play whose only penalty was declined (ESPN flags it)."""
    j = _joined().filter(pl.col("desc").str.contains("(?i)declined") & (pl.col("penalty__nv") == 0))
    assert j.height >= 3
    assert (j["penalty"] == 0).all()


def test_a_frame_already_split_gives_the_same_view():
    """``split_pat_rows`` output passed in (either ``split_pats``) is the view of the raw frame."""
    from sportsdataverse.nfl import split_pat_rows

    plays, sched = _plays(401671740), _schedule()
    want = espn_nfl_pbp_nflverse(plays, sched)
    for split in (True, False):
        assert espn_nfl_pbp_nflverse(split_pat_rows(plays), sched, split_pats=split).equals(want)


def test_a_schedule_without_the_game_is_refused():
    """Mapping ESPN's game id through the schedule (a frame with no nflverse_game_id) needs the game there."""
    other = _schedule().filter(pl.col("game_id") != "2024_04_PHI_TB")
    with pytest.raises(ValueError, match="401671740"):
        espn_nfl_pbp_nflverse(_plays(401671740).drop("nflverse_game_id"), other)


def _nflverse(game_id: str) -> pl.DataFrame:
    return pl.read_parquet(FIX / "nflverse" / "espn_nflverse_view.parquet").filter(pl.col("game_id") == game_id)


def test_a_float_game_id_is_refused_when_nflverse_game_id_is_there():
    """The release shape (``nflverse_game_id`` already on the rows) still goes through the game-id dtype guard."""
    plays = _plays(401671740).with_columns(pl.lit("2024_04_PHI_TB").alias("nflverse_game_id"))
    assert espn_nfl_pbp_nflverse(plays)["game_id"].unique().to_list() == ["2024_04_PHI_TB"]
    with pytest.raises(ValueError, match="game_id is Float64"):
        espn_nfl_pbp_nflverse(plays.with_columns(pl.col("game_id").cast(pl.Float64)))


def test_split_two_point_tries_keep_pass_and_run():
    """A split two-point row carries no two_point_pass flag; its play_type still follows nflverse's (in game order)."""
    view = espn_nfl_pbp_nflverse(_plays(401671696), _schedule())
    ours = view.filter(pl.col("play_type_nfl") == "PAT2").sort("espn_play_id")
    nv = _nflverse("2024_01_NYJ_SF").filter(pl.col("play_type_nfl") == "PAT2").sort("play_id")
    assert ours.height == nv.height == 2
    assert ours["play_type"].to_list() == nv["play_type"].to_list() == ["run", "pass"]


def test_a_defensive_two_point_return_is_scored_on_the_try_row():
    """2019_13_OAK_KC: KC returns LV's blocked PAT for 2. ESPN's touchdown row counts them; nflverse's PAT row does."""
    view = espn_nfl_pbp_nflverse(_plays(401128067), _schedule())
    cols = ["posteam_score", "defteam_score", "posteam_score_post", "defteam_score_post", "total_home_score"]
    td = view.filter(pl.col("play_id") == 3688)
    pat = view.filter(pl.col("espn_play_id") == td["espn_play_id"][0] + 1)
    nv = _nflverse("2019_13_OAK_KC")
    nv_td, nv_pat = nv.filter(pl.col("play_id") == 3688), nv.filter(pl.col("play_id") == 3713)
    assert pat["play_type_nfl"].to_list() == nv_pat["play_type_nfl"].to_list() == ["XP_KICK"]
    for c in cols:
        assert td[c][0] == nv_td[c][0], c
        assert pat[c][0] == nv_pat[c][0], c


def test_the_processor_stamps_the_nflverse_game_id():
    """``NFLPlayProcess`` writes nflverse's game id from ESPN's header, so the view needs no schedule live."""
    for espn_id, game_id in {**GAMES, **EXTRA}.items():
        plays = _plays(espn_id)
        assert plays["nflverse_game_id"].unique().to_list() == [game_id]
        assert espn_nfl_pbp_nflverse(plays)["game_id"].unique().to_list() == [game_id]


def test_espn_header_to_nflverse_game_id():
    """Postseason weeks continue the regular season (ESPN's Pro Bowl week 4 skipped); abbreviations by season."""
    from sportsdataverse.nfl.nfl_pbp import _espn_nflverse_game_id

    rows = pl.DataFrame(
        {
            "season": [2024, 2024, 2020, 2015, 2019, 2024, 2024],
            "seasonType": [3, 3, 3, 2, 2, 1, 3],
            "week": [1, 5, 5, 3, 7, 2, 4],
            "awayTeamAbbrev": ["WSH", "KC", "TB", "LAR", "LV", "JAX", "AFC"],
            "homeTeamAbbrev": ["TB", "PHI", "KC", "SD", "SD", "DET", "NFC"],
        }
    )
    got = rows.select(_espn_nflverse_game_id().alias("g"))["g"].to_list()
    assert got == [
        "2024_19_WAS_TB",
        "2024_22_KC_PHI",
        "2020_21_TB_KC",
        "2015_03_STL_SD",
        "2019_07_OAK_SD",
        "2024_PRE2_JAX_DET",
        None,
    ]


#: Player-id slots scored on the 2014-on captures (participants feed); floor = the lowest game, rounded down.
ID_FLOORS = {"tackle_with_assist_1_player_id": 0.95, "blocked_player_id": 0.95}


#: Names are the players table's short name, which is not always the feed's form nflverse writes (2014: "Ad.Peterson").
NAME_FLOOR = 0.80


@pytest.mark.parametrize("espn_id", [401671740, 400554340, 401128067, 401671696])
def test_player_ids_agree_with_nflverse(espn_id):
    """Every GSIS id slot and player team agrees with nflverse on >= 97% of the game's plays (null == null counts)."""
    game_id = {**GAMES, **EXTRA}[espn_id]
    view = espn_nfl_pbp_nflverse(_plays(espn_id), _schedule(), participants=_participants(), players=_players())
    j = view.join(_nflverse(game_id), on=["game_id", "play_id"], how="inner", suffix="__nv")
    ids = [c for c in view.columns if (c.endswith("_player_id") or c in PLAYER_TEAMS) and f"{c}__nv" in j.columns]
    assert len(ids) >= 27
    low = {c: round(r, 4) for c in ids if (r := _rate(j, c)) < ID_FLOORS.get(c, 0.97)}
    assert low == {}, f"{game_id}: player ids below their floor: {low}"
    names = [c for c in view.columns if c.endswith("_player_name") and f"{c}__nv" in j.columns]
    low = {c: round(r, 4) for c in names if (r := _rate(j, c)) < NAME_FLOOR}
    assert low == {}, f"{game_id}: player names below {NAME_FLOOR}: {low}"
    # and they are there: most passes name nflverse's passer
    passes = j.filter(pl.col("passer_player_id__nv").is_not_null())
    assert passes.height > 30 and (passes["passer_player_id"] == passes["passer_player_id__nv"]).mean() >= 0.98


def test_without_participants_the_processor_ids_are_read():
    """No participants frame: the processor's own athlete ids still map (2024 game, passer / rusher / receiver)."""
    view = espn_nfl_pbp_nflverse(_plays(401671740), players=_players())
    j = view.join(_nflverse("2024_04_PHI_TB"), on=["game_id", "play_id"], suffix="__nv")
    for c in ("passer_player_id", "rusher_player_id", "receiver_player_id"):
        has = j.filter(pl.col(f"{c}__nv").is_not_null())
        assert (has[c] == has[f"{c}__nv"]).fill_null(False).mean() >= 0.95, c


def test_a_split_try_takes_its_touchdowns_kicker():
    """2024: the PAT row names the kicker ESPN lists on the touchdown row; the touchdown row names none."""
    view = _view().filter(pl.col("game_id") == "2024_04_PHI_TB")
    pats = view.filter(pl.col("play_type") == "extra_point")
    assert pats.height >= 4 and pats["kicker_player_id"].null_count() == 0
    nv = _nflverse("2024_04_PHI_TB").filter(pl.col("play_type") == "extra_point")
    assert set(pats["kicker_player_id"]) == set(nv["kicker_player_id"])
    assert view.filter(pl.col("touchdown") == 1)["kicker_player_id"].null_count() == view["touchdown"].sum()


def test_a_no_play_keeps_only_the_penalized_player():
    j = _joined().filter((pl.col("play_type") == "no_play") & pl.col("penalty_player_id__nv").is_not_null())
    assert j.height >= 3
    assert (j["penalty_player_id"] == j["penalty_player_id__nv"]).all()
    assert j["passer_player_id"].null_count() == j.height


def test_players_table_is_checked():
    with pytest.raises(ValueError, match="espn_id"):
        espn_nfl_pbp_nflverse(_plays(401671740), players=_players().drop("espn_id"))
    with pytest.raises(ValueError, match="Float64"):
        espn_nfl_pbp_nflverse(_plays(401671740), players=_players().with_columns(pl.col("espn_id").cast(pl.Float64)))


def test_fixed_drive_is_espns_drive_and_series_follow():
    """``fixed_drive`` is ESPN's drive number; nflfastR's result cascades on it match nflverse."""
    j = _joined()
    assert _rate(j, "fixed_drive") >= 0.97
    assert _rate(j, "series_result") >= 0.97 and _rate(j, "fixed_drive_result") >= 0.93


@functools.cache
def _enriched() -> pl.DataFrame:
    return pl.concat(
        [espn_nfl_pbp_nflverse(_plays(g), _schedule(), players=_players(), enrich=True) for g in GAMES],
        how="diagonal_relaxed",
    )


#: Model columns: Pearson r with nflverse on the three games >= floor (whole seasons are in the PR body).
MODEL_FLOORS = {"ep": 0.97, "wp": 0.97, "vegas_wp": 0.97, "epa": 0.9, "xpass": 0.95}


def test_enrich_adds_nflverse_model_columns():
    nv = pl.read_parquet(FIX / "nflverse" / "espn_nflverse_view.parquet")
    j = _enriched().join(nv, on=["game_id", "play_id"], suffix="__nv")
    r = {c: j.select(pl.corr(c, f"{c}__nv")).item() for c in MODEL_FLOORS}
    assert all(r[c] >= f for c, f in MODEL_FLOORS.items()), r
    # nflverse carries no fourth-down surface, so the view does not build it
    assert "go_boost" not in _enriched().columns


def test_live_participants_with_list_ids_give_the_same_view():
    """``espn_nfl_play_participants`` returns ``{role}_player_ids`` as lists; the release stores JSON text."""
    parts = _participants().filter(pl.col("game_id") == 401671740)
    ids = [c for c in parts.columns if c.endswith("_player_ids")]
    listed = parts.with_columns(
        pl.when(pl.col(c).str.starts_with("["))
        .then(pl.col(c))
        .otherwise(pl.format('["{}"]', pl.col(c)))
        .str.json_decode(pl.List(pl.Utf8))
        for c in ids
    )
    assert isinstance(listed.schema[ids[0]], pl.List)
    plays, players = _plays(401671740), _players()
    want = espn_nfl_pbp_nflverse(plays, participants=parts, players=players)
    assert espn_nfl_pbp_nflverse(plays, participants=listed, players=players).equals(want)


def test_an_empty_frame_keeps_the_schema():
    plays = _plays(401671740)
    full = espn_nfl_pbp_nflverse(plays, players=_players())
    empty = espn_nfl_pbp_nflverse(plays.head(0), players=_players())
    assert empty.height == 0 and empty.columns == full.columns


def test_a_game_between_non_nfl_sides_is_dropped():
    """The Pro Bowl has no nflverse game id (nflverse does not carry it): its rows leave the view, others stay."""
    plays = _plays(401671740)
    pro_bowl = plays.with_columns(
        pl.lit(None, dtype=pl.Utf8).alias("nflverse_game_id"),
        pl.lit("AFC").alias("homeTeamAbbrev"),
        pl.lit("NFC").alias("awayTeamAbbrev"),
    )
    assert espn_nfl_pbp_nflverse(pro_bowl).height == 0
    with pytest.raises(ValueError, match="401671740"):
        espn_nfl_pbp_nflverse(plays.with_columns(pl.lit(None, dtype=pl.Utf8).alias("nflverse_game_id")))


def test_a_split_two_point_run_is_a_rush_without_players():
    """2024_01_NYJ_SF: the split run try is ``rush`` 1 by its own type, players table or not (nflverse: run 1/0)."""
    nv = _nflverse("2024_01_NYJ_SF").filter(pl.col("play_type_nfl") == "PAT2").sort("play_id")
    for players in (None, _players()):
        view = espn_nfl_pbp_nflverse(_plays(401671696), players=players)
        ours = view.filter(pl.col("play_type_nfl") == "PAT2").sort("espn_play_id")
        assert ours.select("play_type", "pass", "rush").rows() == [("run", 0, 1), ("pass", 1, 0)]
        assert ours["rush"].to_list() == nv["rush"].cast(pl.Int64).to_list()


def test_a_rushing_touchdown_with_a_folded_passing_try_is_a_rush():
    """2024_20_BAL_BUF: "D.Henry right guard for 5 yards, TOUCHDOWN. TWO-POINT CONVERSION ATTEMPT. L.Jackson pass" is a run."""
    j = espn_nfl_pbp_nflverse(_plays(401671886)).join(
        _nflverse("2024_20_BAL_BUF"), on=["game_id", "play_id"], suffix="__nv"
    )
    folded = j.filter((pl.col("touchdown") == 1) & pl.col("desc").str.contains("TWO-POINT CONVERSION ATTEMPT"))
    rushing = folded.filter(pl.col("rush__nv") == 1)
    assert rushing.height >= 1 and rushing["desc"].str.contains(" pass ").all()
    assert (folded["rush"] == folded["rush__nv"]).all() and (folded["pass"] == folded["pass__nv"]).all()


def test_a_drive_that_ends_the_half_is_end_of_half():
    """The processor flags end_of_half (it drops the period-end text): nflverse's "End of half" drives match."""
    j = _joined()
    half = j.filter(pl.col("fixed_drive_result__nv") == "End of half")
    assert half.height >= 10
    assert (half["fixed_drive_result"] == "End of half").mean() >= 0.9
    # a stronger result earlier in the drive still wins
    assert (
        j.filter(pl.col("fixed_drive_result__nv") == "Touchdown")["fixed_drive_result"] == "Touchdown"
    ).mean() >= 0.97


def test_a_split_try_is_spotted_with_its_touchdowns_timeouts():
    """nflverse spots a kick try at the 2 (before 2015) or the 15, a two-point try at the 2, with the TD's timeouts."""
    view = _view()
    for gid, kick_spot in (("2024_04_PHI_TB", 15.0), ("2014_11_MIN_CHI", 2.0)):
        g = view.filter(pl.col("game_id") == gid)
        prev = g.with_columns(pl.col("posteam_timeouts_remaining").shift(1).alias("_td_to"))
        tries = prev.filter((pl.col("extra_point_attempt") == 1) | (pl.col("two_point_attempt") == 1))
        assert tries.height >= 4, gid
        spot = pl.when(pl.col("two_point_attempt") == 1).then(2.0).otherwise(kick_spot)
        assert tries.select((pl.col("yardline_100") == spot).all()).item(), gid
        assert (tries["ydstogo"] == 0).all() and (tries["side_of_field"] == tries["defteam"]).all()
        assert tries["posteam_timeouts_remaining"].null_count() == 0
        assert (tries["posteam_timeouts_remaining"] == tries["_td_to"]).all(), gid
    # the FG model scores the kick from the spot: nflverse's make probability, not one off a null yard line
    kicks = _enriched().filter((pl.col("game_id") == "2024_04_PHI_TB") & (pl.col("extra_point_attempt") == 1))
    assert kicks["ep"].is_between(0.9, 1.0).all(), kicks["ep"].to_list()


def test_a_penalized_kickoff_is_the_receivers_kickoff():
    """ESPN files an offside-on-the-kick replay as a penalty with the kicking team's ball; nflverse's is a kickoff."""
    view = espn_nfl_pbp_nflverse(_plays(401128090), _schedule())
    nv = _nflverse("2019_09_GB_LAC")
    j = view.join(nv, on=["game_id", "play_id"], suffix="__nv")
    row = j.filter(pl.col("play_id") == 3203)
    assert row.select("play_type", "posteam", "yardline_100").row(0) == ("kickoff", "LAC", 35.0)
    assert row["yardline_100"].item() == row["yardline_100__nv"].item()
    # its posteam-relative score and timeouts are the receiver's too
    for c in ("score_differential", "posteam_score", "posteam_timeouts_remaining"):
        assert row[c].item() == row[f"{c}__nv"].item(), c
    ko = j.filter(pl.col("play_type__nv") == "kickoff")
    assert (ko["posteam"] == ko["posteam__nv"]).all() and (ko["play_type"] == "kickoff").all()


def test_a_defensive_touchdowns_try_takes_the_scorers_timeouts():
    """After a defensive touchdown the try is the scoring defense's: its own timeouts, not the old offense's."""
    view = espn_nfl_pbp_nflverse(_plays(401671712), _schedule())
    prev = view.with_columns(pl.col("posteam").shift(1).alias("_td_posteam"))
    tries = prev.filter((pl.col("extra_point_attempt") == 1) & (pl.col("posteam") != pl.col("_td_posteam")))
    assert tries.height == 1
    nv = _nflverse("2024_01_MIN_NYG").filter(pl.col("play_id") == 2831)
    cols = ("posteam", "posteam_timeouts_remaining", "defteam_timeouts_remaining")
    assert tries.select(cols).row(0) == nv.select(cols).row(0) == ("MIN", 1.0, 3.0)
