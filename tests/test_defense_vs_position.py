"""Defense vs position over real released 2024 rows (CFB espn_cfb_pbp + cfb_rosters, NFL nfl_model_pbp + rosters).

Provenance and the hand counts behind every literal here: tests/fixtures/defense_vs_position/README.md.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from polars.testing import assert_frame_equal

from sportsdataverse.defense_vs_position import (
    MIN_GAMES,
    OUTPUT_SCHEMA,
    PBP_COLUMNS,
    _assign,
    _plays,
    _roster_groups,
    defense_vs_position,
)

FIX = Path(__file__).parent / "fixtures" / "defense_vs_position"


@pytest.fixture(scope="module")
def cfb_pbp() -> pl.DataFrame:
    return pl.read_parquet(FIX / "cfb_pbp_2024_psu3_uga2_def.parquet")


@pytest.fixture(scope="module")
def cfb_rosters() -> pl.DataFrame:
    return pl.read_parquet(FIX / "cfb_rosters_2024_slice.parquet")


@pytest.fixture(scope="module")
def cfb(cfb_pbp, cfb_rosters) -> pl.DataFrame:
    return defense_vs_position(cfb_pbp, cfb_rosters, "cfb")


@pytest.fixture(scope="module")
def nfl() -> pl.DataFrame:
    return defense_vs_position(
        pl.read_parquet(FIX / "nfl_model_pbp_2024_phi_def_wk1_3.parquet"),
        pl.read_parquet(FIX / "nfl_rosters_2024_slice.parquet"),
        "nfl",
    )


def _cell(df: pl.DataFrame, team: str, group: str) -> dict:
    rows = df.filter((pl.col("team_id") == team) & (pl.col("position_group") == group)).to_dicts()
    assert len(rows) == 1, rows
    return rows[0]


def test_output_schema_and_keys(cfb, nfl):
    for df in (cfb, nfl):
        assert df.schema == pl.Schema(OUTPUT_SCHEMA)
        assert df.select("season", "team_id", "position_group").is_duplicated().sum() == 0
    # the DEFENSES, never their opponents
    assert set(cfb["team_id"]) == {"213", "61"}
    assert set(nfl["team_id"]) == {"PHI"}
    assert set(cfb["position_group"]) == {"QB", "RB", "WR", "TE"}


def test_cfb_penn_state_te_cell_matches_hand_count(cfb):
    c = _cell(cfb, "213", "TE")
    assert (c["season"], c["plays"], c["games"], c["targets"]) == (2024, 14, 2, 14)
    assert c["epa_per_play_allowed"] == pytest.approx(12.263268947601318 / 14)
    assert c["success_rate_allowed"] == pytest.approx(10 / 14)
    assert c["explosive_rate_allowed"] == pytest.approx(1 / 14)
    assert c["yards_per_target_allowed"] == pytest.approx(165 / 14)
    assert c["dropbacks"] is None and c["sack_rate_allowed"] is None
    assert c["carries"] is None and c["rush_yards_per_carry_allowed"] is None
    assert c["qualified"] is False


def test_cfb_te_plays_are_the_hand_listed_ones(cfb_pbp, cfb_rosters):
    long = _assign(_plays(cfb_pbp, "cfb"), _roster_groups(cfb_rosters, "cfb"))
    te = long.filter((pl.col("team_id") == "213") & (pl.col("position_group") == "TE"))
    got = set(zip(te["game_id"].to_list(), te["play"].to_list()))
    wvu = {("401628457", p) for p in (6, 40, 60)}
    bgsu = {("401628470", p) for p in (2, 7, 17, 42, 43, 53, 67, 125, 143, 157, 160)}
    assert got == wvu | bgsu


@pytest.mark.parametrize(
    ("game_id", "play", "groups"),
    [
        ("401628323", 120, ["QB"]),  # sack: the dropback is the QB's
        ("401628323", 20, ["QB"]),  # incompletion with no receiver id: QB only
        ("401628470", 42, ["QB", "TE"]),  # completion to a TE: QB and TE
        ("401628323", 16, ["RB"]),  # RB carry
        ("401628323", 95, ["QB"]),  # QB carry
        ("401628339", 9, ["WR"]),  # WR carry goes to WR
        ("401628339", 147, []),  # DB carry: an `other` position, no group
        ("401628323", 74, []),  # TEAM carry: no roster row, no group
        ("401628457", 12, []),  # carry with no rusher id
    ],
)
def test_group_assignment_per_play_type(cfb_pbp, cfb_rosters, game_id, play, groups):
    long = _assign(_plays(cfb_pbp, "cfb"), _roster_groups(cfb_rosters, "cfb"))
    hit = long.filter((pl.col("game_id") == game_id) & (pl.col("play") == play))
    assert sorted(hit["position_group"].to_list()) == groups


def test_only_scrimmage_plays_on_a_down_count(cfb_pbp, cfb):
    pop = cfb_pbp.filter(pl.col("EPA_scrimmage").is_not_null() & pl.col("down").is_between(1, 4))
    dropbacks = pop.filter(pl.col("pass") == True)  # noqa: E712
    for team, rows in dropbacks.group_by("def_pos_team_id"):
        assert _cell(cfb, str(team[0]), "QB")["dropbacks"] == rows.height


def test_qualified_is_the_three_game_floor(cfb):
    assert MIN_GAMES == 3
    assert_frame_equal(cfb.select("qualified"), cfb.select(qualified=pl.col("games") >= 3))
    psu = {r["position_group"]: r["qualified"] for r in cfb.filter(pl.col("team_id") == "213").to_dicts()}
    assert psu == {"QB": True, "RB": True, "WR": True, "TE": False}  # TE only in 2 of the 3 games
    assert cfb.filter(pl.col("team_id") == "61")["qualified"].to_list() == [False] * 4  # 2 games


def test_older_roster_shape_gives_the_same_rows(cfb_pbp, cfb_rosters, cfb):
    """sdv-db #124: abbreviation in ``position``, no ``position_abbreviation``, text ids."""
    old = cfb_rosters.select(
        "season",
        pl.col("athlete_id").cast(pl.Utf8),
        position=pl.col("position_abbreviation"),
    )
    assert_frame_equal(defense_vs_position(cfb_pbp, old, "cfb"), cfb)


def test_player_listed_under_two_groups_counts_nowhere(cfb_pbp, cfb_rosters, cfb):
    fannin = cfb_rosters.filter(pl.col("athlete_id") == 5083076)  # Harold Fannin Jr., BGSU
    assert fannin["position_abbreviation"].to_list() == ["TE"]
    twice = pl.concat([cfb_rosters, fannin.with_columns(position_abbreviation=pl.lit("WR"))])
    out = defense_vs_position(cfb_pbp, twice, "cfb")
    assert _cell(out, "213", "TE")["plays"] == 3  # his 11 targets leave TE ...
    assert _cell(out, "213", "WR")["plays"] == _cell(cfb, "213", "WR")["plays"]  # ... and join no group


def test_cfb_carry_with_null_yds_rushed_reads_stat_yardage(cfb_pbp, cfb_rosters, cfb):
    """ESPN leaves ``yds_rushed`` null on 150 carries in 2024 (118 of them 0-yard rushes,
    ``statYardage`` 0); a skipped null would inflate yards per carry. Null one real
    0-yard carry the same way: the RB row must not move."""
    play = (pl.col("game_id") == 401628457) & (pl.col("game_play_number") == 25)  # CJ Donaldson Jr., no gain
    assert cfb_pbp.filter(play).select("rush", "yds_rushed", "statYardage").row(0) == (True, 0, 0)
    nulled = cfb_pbp.with_columns(yds_rushed=pl.when(play).then(None).otherwise(pl.col("yds_rushed")))
    assert_frame_equal(defense_vs_position(nulled, cfb_rosters, "cfb"), cfb)


def test_pbp_with_no_counted_play_gives_the_empty_schema(cfb_pbp, cfb_rosters):
    special_teams = cfb_pbp.filter(pl.col("EPA_scrimmage").is_null())
    assert special_teams.height > 0
    out = defense_vs_position(special_teams, cfb_rosters, "cfb")
    assert out.height == 0
    assert out.schema == pl.Schema(OUTPUT_SCHEMA)


@pytest.mark.parametrize(
    ("frame", "col"),
    [("pbp", "receiver_player_id"), ("pbp", "def_pos_team_id"), ("rosters", "athlete_id")],
)
def test_float_id_raises(cfb_pbp, cfb_rosters, frame, col):
    pbp, rosters = cfb_pbp, cfb_rosters
    if frame == "pbp":
        pbp = pbp.with_columns(pl.col(col).cast(pl.Float64))
    else:
        rosters = rosters.with_columns(pl.col(col).cast(pl.Float64))
    with pytest.raises(TypeError, match=col):
        defense_vs_position(pbp, rosters, "cfb")


def test_nfl_twin_matches_hand_count(nfl):
    rb = _cell(nfl, "PHI", "RB")
    assert (rb["plays"], rb["games"], rb["carries"], rb["qualified"]) == (88, 3, 70, True)
    assert rb["epa_per_play_allowed"] == pytest.approx(-3.1620409803072107 / 88)
    assert rb["success_rate_allowed"] == pytest.approx(37 / 88)
    assert rb["explosive_rate_allowed"] == pytest.approx(2 / 88)
    assert rb["rush_yards_per_carry_allowed"] == pytest.approx(366 / 70)
    assert rb["targets"] is None
    qb = _cell(nfl, "PHI", "QB")
    assert (qb["plays"], qb["dropbacks"]) == (96, 95)
    assert qb["sack_rate_allowed"] == pytest.approx(4 / 95)


def test_nfl_reads_gsis_ids(nfl):
    rosters = pl.read_parquet(FIX / "nfl_rosters_2024_slice.parquet")
    assert rosters.schema["gsis_id"] == pl.Utf8
    assert rosters["gsis_id"].str.contains(r"^00-\d{7}$").all()
    assert set(nfl["position_group"]) == {"QB", "RB", "WR", "TE"}  # carriers and targets matched by gsis id


def test_empty_pbp_carries_the_schema(cfb_rosters):
    out = defense_vs_position(pl.DataFrame(schema={c: pl.Int64 for c in PBP_COLUMNS["cfb"]}), cfb_rosters, "cfb")
    assert out.height == 0
    assert out.schema == pl.Schema(OUTPUT_SCHEMA)


def test_unknown_league_raises(cfb_pbp, cfb_rosters):
    with pytest.raises(ValueError, match="league"):
        defense_vs_position(cfb_pbp, cfb_rosters, "nba")
