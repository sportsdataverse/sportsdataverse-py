"""Rate curves along a continuous axis over real released data (CFB pbp, nfl_model_pbp, stats.nba shots)."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from polars.testing import assert_frame_equal

from sportsdataverse.metric_curves import (
    ATTEMPT_SCHEMA,
    BUCKET_EDGES,
    FOOTBALL_ATTEMPT_COLUMNS,
    OUTPUT_SCHEMA,
    SHOT_ATTEMPT_COLUMNS,
    football_attempts,
    metric_curves,
    nflfastr_attempts,
    shot_attempts,
)


def _bucket(col: str, edges) -> pl.Expr:
    """``pl.col(col).cut(edges, left_closed=True)`` as a bucket index, nulls kept null.

    ``cut`` warns on polars 2.0 and its ``bin_intervals`` replacement does not exist on 1.x, so the
    hand count spells the bucket out: the number of edges at or below the value.
    """
    idx = pl.sum_horizontal([(pl.col(col) >= e).cast(pl.Int32) for e in edges])
    return pl.when(pl.col(col).is_not_null()).then(idx).alias(col)


FIX = Path(__file__).parent / "fixtures"
CURRY = "201939"
KEY = ["season", "entity_type", "entity_id", "metric", "down", "x_lo"]


@pytest.fixture(scope="module")
def cfb_pbp() -> pl.DataFrame:
    return pl.read_parquet(FIX / "rolling_windows" / "cfb_pbp_4433971_2021_2024.parquet")


@pytest.fixture(scope="module")
def cfb_curves(cfb_pbp) -> pl.DataFrame:
    return metric_curves(football_attempts(cfb_pbp), "cfb")


@pytest.fixture(scope="module")
def nfl_pbp() -> pl.DataFrame:
    return pl.read_parquet(FIX / "metric_curves" / "nfl_model_pbp_BUF_2024.parquet")


@pytest.fixture(scope="module")
def nfl_curves(nfl_pbp) -> pl.DataFrame:
    return metric_curves(nflfastr_attempts(nfl_pbp), "nfl")


@pytest.fixture(scope="module")
def curry_shots() -> pl.DataFrame:
    return pl.read_parquet(FIX / "rolling_windows" / "nba_stats_shots_201939_2024_2025.parquet")


@pytest.fixture(scope="module")
def nba_2026_shots() -> pl.DataFrame:
    return pl.read_parquet(FIX / "metric_curves" / "nba_stats_shots_2026_slice.parquet")


@pytest.fixture(scope="module")
def wnba_2025_shots() -> pl.DataFrame:
    # the WNBA shots carry no season_type_id; the producer derives it from the game id's type digit
    shots = pl.read_parquet(FIX / "metric_curves" / "wnba_stats_shots_2025_slice.parquet")
    return shots.with_columns(season_type_id=pl.col("game_id").str.slice(2, 1))


def _coordinate_ft(df: pl.DataFrame) -> list[float]:
    """The exact release distance: legacy coordinates are tenths of a foot with the rim at the origin."""
    ft = (pl.col("x_legacy").cast(pl.Float64) ** 2 + pl.col("y_legacy").cast(pl.Float64) ** 2).sqrt() / 10
    return df.select(ft).to_series().to_list()


def _league(df: pl.DataFrame, metric: str) -> pl.DataFrame:
    return df.filter((pl.col("entity_type") == "league") & (pl.col("metric") == metric))


# --------------------------------------------------------------------------- CFB (ESPN shape)


def test_fg_attempts_and_makes_match_the_fixture(cfb_pbp, cfb_curves):
    fg = cfb_pbp.filter((pl.col("fg_attempt") == True) & pl.col("yds_fg").is_not_null())  # noqa: E712
    league = _league(cfb_curves, "fg_pct_by_distance")
    assert league["attempts"].sum() == fg.height == 102
    assert league["successes"].sum() == fg.filter(pl.col("fg_made") == True).height == 74  # noqa: E712
    # one league row per populated season x bucket; the per-bucket counts are a hand cut of the same plays
    hand = fg.group_by(_bucket("yds_fg", BUCKET_EDGES["fg_pct_by_distance"][1:-1])).len()
    assert sorted(league.group_by("x_lo").agg(pl.col("attempts").sum())["attempts"].to_list()) == sorted(hand["len"])


def test_a_44_yard_attempt_lands_in_40_45(cfb_pbp):
    two = cfb_pbp.filter((pl.col("fg_attempt") == True) & (pl.col("yds_fg") == 44))  # noqa: E712
    assert two.height == 2
    row = _league(metric_curves(football_attempts(two), "cfb"), "fg_pct_by_distance")
    assert row.select("x_lo", "x_hi", "attempts").rows() == [(40.0, 45.0, 2)]
    # an attempt exactly on an edge belongs to the bucket it opens, never the one it closes
    four = cfb_pbp.filter((pl.col("fg_attempt") == True) & (pl.col("yds_fg") == 45))  # noqa: E712
    assert four.height == 4
    rows = _league(metric_curves(football_attempts(four), "cfb"), "fg_pct_by_distance")  # one row per season
    assert rows.group_by("x_lo", "x_hi").agg(pl.col("attempts").sum()).rows() == [(45.0, 50.0, 4)]


def test_success_by_down_distance_has_4x5_league_rows_over_the_standing_plays(cfb_pbp, cfb_curves):
    standing = cfb_pbp.filter(
        (pl.col("scrimmage_play") == True)  # noqa: E712
        & (pl.col("penalty_no_play").fill_null(False) == False)  # noqa: E712
        & pl.col("down").is_between(1, 4)
        & pl.col("distance").is_not_null()
    )
    league = (
        _league(cfb_curves, "success_by_down_distance").group_by("down", "x_lo", "x_hi").agg(pl.col("attempts").sum())
    )
    assert league.height == 4 * 5
    assert set(league["down"]) == {1, 2, 3, 4}
    assert set(zip(league["x_lo"], league["x_hi"])) == {(1.0, 2.0), (2.0, 4.0), (4.0, 7.0), (7.0, 11.0), (11.0, 100.0)}
    assert league["attempts"].sum() == standing.height == 4704
    assert _league(cfb_curves, "success_by_down_distance")["successes"].sum() == standing["EPA_success"].sum()


def test_fourth_down_conversions_follow_standing_scrimmage_semantics(cfb_pbp, cfb_curves):
    fourth = cfb_pbp.filter(
        (pl.col("scrimmage_play") == True)  # noqa: E712
        & (pl.col("penalty_no_play").fill_null(False) == False)  # noqa: E712
        & (pl.col("down") == 4)
        & ((pl.col("rush") == True) | (pl.col("pass") == True))  # noqa: E712
        & pl.col("distance").is_not_null()
    )
    league = _league(cfb_curves, "fourth_conv_by_ytg")
    assert league["attempts"].sum() == fourth.height == 126
    converted = fourth.filter((pl.col("first_down_created") == True) | (pl.col("touchdown") == True))  # noqa: E712
    assert league["successes"].sum() == converted.height == 70
    assert league["down"].null_count() == league.height  # down is only an axis for success_by_down_distance
    assert (
        cfb_curves.filter((pl.col("metric") == "fourth_conv_by_ytg") & (pl.col("entity_type") == "player")).height == 0
    )


def test_rate_is_exact_and_empty_buckets_are_absent(cfb_curves, nfl_curves):
    for df in (cfb_curves, nfl_curves):
        assert (df["attempts"] > 0).all()
        assert df["rate"].to_list() == [s / a for s, a in zip(df["successes"], df["attempts"])]
        assert df["rate"].is_between(0.0, 1.0).all()
    # the fixture's longest field goal is 57 yards: no 60-65 or 65-80 row, not a zero row
    assert _league(cfb_curves, "fg_pct_by_distance")["x_lo"].max() == 55.0


def test_ids_are_strings_and_a_float_id_raises(cfb_pbp, cfb_curves):
    ids = cfb_curves.filter(pl.col("entity_type") != "league")["entity_id"]
    assert ids.null_count() == 0 and ids.str.contains(r"^\d+$").all()  # never "4694399.0"
    players = cfb_curves.filter(pl.col("entity_type") == "player")
    assert players["team_id"].str.contains(r"^\d+$").all()
    assert set(players["metric"]) == {"fg_pct_by_distance"}  # no CFB air yards before 2025
    teams = cfb_curves.filter(pl.col("entity_type") == "team")
    assert teams["team_id"].null_count() == teams.height  # team_id labels player rows only
    assert set(cfb_curves["id_source"]) == {"espn"}
    with pytest.raises(TypeError, match="fg_kicker_player_id"):
        football_attempts(cfb_pbp.with_columns(pl.col("fg_kicker_player_id").cast(pl.Float64)))
    with pytest.raises(TypeError, match="player_id"):
        metric_curves(football_attempts(cfb_pbp).with_columns(pl.col("player_id").cast(pl.Float64)), "cfb")
    # a numeric-string id is the same curve
    as_str = cfb_pbp.with_columns(pl.col("fg_kicker_player_id").cast(pl.Utf8))
    assert metric_curves(football_attempts(as_str), "cfb").height == cfb_curves.height


def test_kicker_rows_carry_their_team_and_sum_to_the_team_row(cfb_curves):
    fielding = cfb_curves.filter((pl.col("entity_type") == "player") & (pl.col("entity_id") == "5081814"))
    assert fielding["attempts"].sum() == 18 and set(fielding["entity_name"]) == {"Jayden Fielding"}
    assert set(fielding["team_id"]) == {"194"} and set(fielding["season"]) == {2023}
    team = cfb_curves.filter(
        (pl.col("entity_type") == "team")
        & (pl.col("entity_id") == "194")
        & (pl.col("metric") == "fg_pct_by_distance")
        & (pl.col("season") == 2023)
    )
    assert team["attempts"].sum() == 18 and team["team_id"].null_count() == team.height


# --------------------------------------------------------------------------- NFL (nflfastR shape)


def test_nfl_air_yards_attempts_match_the_spec_population(nfl_pbp, nfl_curves):
    pop = nfl_pbp.filter((pl.col("pass_attempt") == 1) & (pl.col("sack") == 0) & pl.col("air_yards").is_not_null())
    assert pop.height == 602
    for metric in ("cmp_pct_by_air_yards", "epa_by_air_yards"):
        league = _league(nfl_curves, metric)
        assert league["attempts"].sum() == pop.height
        assert league["epa_per_att"].null_count() == 0
        assert league["x_lo"].min() == -10.0 and league["x_hi"].max() == 70.0
    assert _league(nfl_curves, "cmp_pct_by_air_yards")["successes"].sum() == pop["complete_pass"].sum()
    assert _league(nfl_curves, "epa_by_air_yards")["successes"].sum() == (pop["epa"] > 0).sum()
    passers = nfl_curves.filter((pl.col("entity_type") == "player") & (pl.col("metric") == "cmp_pct_by_air_yards"))
    assert set(passers["entity_id"]) == set(pop["passer_player_id"])  # gsis ids, re-keyed to ESPN by the producer
    assert set(passers["team_id"]) == {"BUF"} and set(nfl_curves["id_source"]) == {"gsis"}


def test_nfl_kicks_and_fourth_downs(nfl_pbp, nfl_curves):
    fg = _league(nfl_curves, "fg_pct_by_distance")
    assert fg["attempts"].sum() == 35 and fg["successes"].sum() == 30
    bass = nfl_curves.filter((pl.col("entity_type") == "player") & (pl.col("metric") == "fg_pct_by_distance"))
    assert set(bass["entity_id"]) == {"00-0036162"} and set(bass["entity_name"]) == {"T.Bass"}
    fourth = nfl_pbp.filter((pl.col("down") == 4) & pl.col("play_type").is_in(["pass", "run"]))
    league = _league(nfl_curves, "fourth_conv_by_ytg")
    assert league["attempts"].sum() == fourth.height == 31 and league["successes"].sum() == 23
    sdd = _league(nfl_curves, "success_by_down_distance")
    standing = nfl_pbp.filter(pl.col("play_type").is_in(["pass", "run"]) & pl.col("down").is_between(1, 4))
    assert sdd["attempts"].sum() == standing.height == 1198
    # one team-season does not reach every down x distance cell (no 4th-and-11+ attempt): absent, not zero
    cells = standing.group_by("down", _bucket("ydstogo", [2, 4, 7, 11])).len()
    assert sdd.height == cells.height == 19


# --------------------------------------------------------------------------- hoops (stats.nba shots)


def test_curry_shot_attempts_sum_to_2833(curry_shots):
    curves = metric_curves(shot_attempts(curry_shots), "nba")
    league = _league(curves, "fg_pct_by_shot_distance")
    assert league["attempts"].sum() == 2833  # regular season + playoffs; play-in rows are not counted
    assert set(league["season"]) == {2024, 2025}
    assert (
        league["successes"].sum()
        == curry_shots.filter(pl.col("season_type_id").is_in(["2", "4"]) & (pl.col("shot_result") == "Made")).height
    )
    assert league["epa_per_att"].null_count() == league.height
    player = curves.filter(pl.col("entity_type") == "player")
    assert set(player["entity_id"]) == {CURRY} and set(player["entity_name"]) == {"Curry"}
    assert set(player["team_id"]) == {"1610612744"} and set(curves["id_source"]) == {"nba_stats"}
    assert player["attempts"].sum() == 2833
    # 1-ft bins to 35 ft, then 35-50 and 50-95
    assert set(zip(league["x_lo"], league["x_hi"])) <= set(
        zip(BUCKET_EDGES["fg_pct_by_shot_distance"][:-1], BUCKET_EDGES["fg_pct_by_shot_distance"][1:])
    )
    assert (35.0, 50.0) in set(zip(league["x_lo"], league["x_hi"]))
    assert metric_curves(shot_attempts(curry_shots, league="wnba"), "wnba")["id_source"].unique().to_list() == [
        "wnba_stats"
    ]


def test_zeroed_corner_threes_bin_at_their_coordinate_distance(nba_2026_shots):
    # stats.nba playbyplayv3 reports shotDistance 0 for every three released under 23.5 ft
    # (the corner); binned on that field they sat in the 0-1 ft bucket and emptied 22-24 ft
    zeroed = nba_2026_shots.filter((pl.col("shot_value") == 3) & (pl.col("shot_distance") == 0))
    assert zeroed.height == 40 and min(_coordinate_ft(zeroed)) > 22
    league = _league(
        metric_curves(shot_attempts(nba_2026_shots.select(SHOT_ATTEMPT_COLUMNS)), "nba"), "fg_pct_by_shot_distance"
    )
    # floor of the coordinate distance; the 40 rim twos (20 tips at the origin) are all that stay under 1 ft
    assert dict(zip(league["x_lo"], league["attempts"])) == {
        0.0: 40,
        21.0: 5,
        22.0: 22,
        23.0: 25,
        24.0: 4,
        25.0: 9,
        26.0: 5,
    }
    assert league["attempts"].sum() == nba_2026_shots.height == 110
    assert league["successes"].sum() == nba_2026_shots.filter(pl.col("shot_result") == "Made").height == 51


def test_every_shot_attempt_carries_its_coordinate_distance(nba_2026_shots, wnba_2025_shots):
    for shots, league in ((nba_2026_shots, "nba"), (wnba_2025_shots, "wnba")):
        att = shot_attempts(shots.select(SHOT_ATTEMPT_COLUMNS), league=league)
        assert att.height == shots.height
        assert att["x"].to_list() == pytest.approx(_coordinate_ft(shots))
        # where the feed's whole-foot distance is not zeroed it is the coordinate distance rounded
        kept = shots["shot_distance"] > 0
        assert ((att["x"] - shots["shot_distance"]).abs().filter(kept) <= 0.5).all()


def test_wnba_zeroed_threes_bin_at_their_coordinate_distance(wnba_2025_shots):
    # same feed defect, worse share: the WNBA arc is 22.146 ft, so every corner and near-arc three is zeroed
    league = _league(
        metric_curves(shot_attempts(wnba_2025_shots.select(SHOT_ATTEMPT_COLUMNS), league="wnba"), "wnba"),
        "fg_pct_by_shot_distance",
    )
    assert dict(zip(league["x_lo"], league["attempts"])) == {0.0: 10, 22.0: 12, 23.0: 25, 24.0: 3}
    assert league["attempts"].sum() == wnba_2025_shots.height == 50


def test_a_null_coordinate_falls_back_to_shot_distance(nba_2026_shots):
    # no sampled 1997-2026 NBA / WNBA shots season has a null coordinate, so null one on real rows
    shots = (
        nba_2026_shots.select(SHOT_ATTEMPT_COLUMNS)
        .with_row_index("i")
        .with_columns(x_legacy=pl.when(pl.col("i") < 10).then(None).otherwise(pl.col("x_legacy")))
        .drop("i")
    )
    att = shot_attempts(shots)
    assert att.height == shots.height
    assert att["x"].head(10).to_list() == shots["shot_distance"].head(10).cast(pl.Float64).to_list()
    assert att["x"].tail(100).to_list() == pytest.approx(_coordinate_ft(shots.tail(100)))


def test_unprojected_pbp_bins_like_the_projected_columns(cfb_pbp):
    # load_cfb_pbp carries ESPN's raw start.down / start.distance beside the repaired down /
    # distance, and usage_box._standing_scrimmage prefers start.* when present; the adapter
    # projects to FOOTBALL_ATTEMPT_COLUMNS first so a full frame reads the repaired columns.
    assert {"start.down", "start.distance"} <= set(cfb_pbp.columns)
    assert_frame_equal(football_attempts(cfb_pbp), football_attempts(cfb_pbp.select(FOOTBALL_ATTEMPT_COLUMNS)))


def test_id_source_comes_from_the_adapter_not_the_league(cfb_pbp, nfl_pbp, curry_shots):
    att = football_attempts(cfb_pbp)
    assert set(att["id_source"]) == {"espn"}
    assert set(metric_curves(att, "nfl")["id_source"]) == {"espn"}  # ESPN NFL pbp keeps ESPN ids
    assert set(nflfastr_attempts(nfl_pbp)["id_source"]) == {"gsis"}
    assert set(shot_attempts(curry_shots)["id_source"]) == {"nba_stats"}
    assert set(shot_attempts(curry_shots, league="wnba")["id_source"]) == {"wnba_stats"}
    with pytest.raises(ValueError, match="namespace"):
        metric_curves(pl.concat([att, nflfastr_attempts(nfl_pbp)]), "nfl")
    assert set(metric_curves(att.drop("id_source"), "cfb")["id_source"]) == {"espn"}  # no column: league default
    with pytest.raises(ValueError, match="league"):
        shot_attempts(curry_shots, league="cfb")


# --------------------------------------------------------------------------- contract


def test_output_schema_one_row_per_key_and_sorted(cfb_curves, nfl_curves):
    for df in (cfb_curves, nfl_curves):
        assert df.schema == pl.Schema(OUTPUT_SCHEMA)
        assert df.select(KEY).is_duplicated().sum() == 0
        assert (
            df.filter(pl.col("entity_type") == "league")["entity_id"].null_count()
            == df.filter(pl.col("entity_type") == "league").height
        )
        assert df.equals(df.sort(["metric", "entity_type", "entity_id", "season", "down", "x_lo"], nulls_last=False))


def test_bucket_edges_are_the_spec_edges():
    assert BUCKET_EDGES["fg_pct_by_distance"] == (15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 80)
    assert (
        BUCKET_EDGES["cmp_pct_by_air_yards"] == BUCKET_EDGES["epa_by_air_yards"] == (-10, 0, 5, 10, 15, 20, 30, 40, 70)
    )
    assert BUCKET_EDGES["fourth_conv_by_ytg"] == (1, 2, 3, 4, 6, 11, 100)
    assert BUCKET_EDGES["success_by_down_distance"] == (1, 2, 4, 7, 11, 100)
    assert BUCKET_EDGES["fg_pct_by_shot_distance"] == tuple(range(36)) + (50, 95)
    for edges in BUCKET_EDGES.values():
        assert list(edges) == sorted(set(edges))


def test_empty_and_invalid_inputs(cfb_pbp):
    assert football_attempts(pl.DataFrame()).schema == pl.Schema(ATTEMPT_SCHEMA)
    assert nflfastr_attempts(pl.DataFrame()).schema == pl.Schema(ATTEMPT_SCHEMA)
    assert shot_attempts(pl.DataFrame()).schema == pl.Schema(ATTEMPT_SCHEMA)
    assert metric_curves(pl.DataFrame(schema=ATTEMPT_SCHEMA), "cfb").schema == pl.Schema(OUTPUT_SCHEMA)
    with pytest.raises(ValueError, match="league"):
        metric_curves(football_attempts(cfb_pbp), "xfl")
    with pytest.raises(ValueError, match="metric"):
        metric_curves(football_attempts(cfb_pbp).with_columns(metric=pl.lit("fg_pct_by_mood")), "cfb")
    # an attempt outside every bucket is dropped, not mis-binned (no 14-yard field goal bucket exists)
    short = (
        football_attempts(cfb_pbp).filter(pl.col("metric") == "fg_pct_by_distance").head(1).with_columns(x=pl.lit(14.0))
    )
    assert metric_curves(short, "cfb").height == 0


def test_package_level_export_and_reference_docs():
    import sportsdataverse

    assert sportsdataverse.metric_curves is metric_curves
    assert sportsdataverse.football_attempts is football_attempts
    assert sportsdataverse.nflfastr_attempts is nflfastr_attempts
    assert sportsdataverse.shot_attempts is shot_attempts
    # the page is split into family pages under reference/python-helpers/ (the overview stays at the old path)
    reference = Path(__file__).parents[1] / "docs" / "docs" / "reference"
    pages = [reference / "python-helpers.md", *sorted((reference / "python-helpers").glob("*.md"))]
    doc = "\n".join(page.read_text(encoding="utf-8") for page in pages)
    for name in ("metric_curves", "football_attempts", "nflfastr_attempts", "shot_attempts"):
        assert f"{{#{name}}}" in doc, name
