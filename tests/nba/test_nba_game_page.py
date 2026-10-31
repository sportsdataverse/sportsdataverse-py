"""nba_game_page: the game-page derivations shared by the GOP processor and the sdv-db stage."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nba import nba_game_page as gp
from sportsdataverse.nba import nba_game_predict
from sportsdataverse.nba.nba_game_predict import win_prob_from_margin
from sportsdataverse.nba.nba_prediction_constants import get_constants

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "espn"


def test_in_game_win_prob_is_not_memoised_on_the_frame() -> None:
    # lru_cache keyed by DataFrame identity would pin every scored game in memory for the
    # life of the Flask processor; only the artifact loader may cache.
    assert not hasattr(nba_game_predict.nba_in_game_win_prob, "cache_info")
    assert hasattr(nba_game_predict._load_in_game_artifact, "cache_info")


def _pbp(rows: list[dict]) -> pl.DataFrame:
    base = {
        "game_id": 1,
        "home_team_id": 19,
        "team_id": 19,
        "home_score": 0,
        "away_score": 0,
        "period_number": 1,
        "start_game_seconds_remaining": 2880.0,
        "start_quarter_seconds_remaining": 720.0,
        "home_team_spread": 3.0,
        "game_spread_available": True,
    }
    return pl.DataFrame([{**base, **r} for r in rows])


def test_home_prior_uses_the_signed_home_spread() -> None:
    p, src = gp.home_prior(3.0, True)
    assert src == "spread" and p == pytest.approx(win_prob_from_margin(3.0))
    p_dog, _ = gp.home_prior(-3.0, True)
    assert p_dog == pytest.approx(1 - p)  # away favoured by 3 mirrors home favoured by 3


def test_home_prior_without_a_line_is_home_court_only() -> None:
    # hoopR and helper_nba_pickcenter both default to +2.5 when no line exists; the flag gates it
    p, src = gp.home_prior(2.5, False)
    assert src == "hfa" and p == pytest.approx(win_prob_from_margin(get_constants("00").hfa))
    assert gp.home_prior(None, None) == (p, "hfa")


def test_ot_rows_use_the_period_clock_and_are_tagged() -> None:
    df = _pbp(
        [
            {"period_number": 4, "start_game_seconds_remaining": 10.0, "start_quarter_seconds_remaining": 10.0},
            {"period_number": 5, "start_game_seconds_remaining": -300.0, "start_quarter_seconds_remaining": 300.0},
            {"period_number": 5, "start_game_seconds_remaining": -590.0, "start_quarter_seconds_remaining": 10.0},
        ]
    )
    out = gp.ot_adjusted_seconds(df)
    assert out["wp_regime"].to_list() == ["reg", "ot", "ot"]
    assert out["start_game_seconds_remaining"].to_list() == [10.0, 300.0, 10.0]


def test_score_win_prob_adds_the_columns_and_first_delta_is_from_the_prior() -> None:
    df = _pbp(
        [
            {"start_game_seconds_remaining": 2880.0},
            {"start_game_seconds_remaining": 1440.0, "home_score": 50, "away_score": 48},
            {"start_game_seconds_remaining": 10.0, "home_score": 99, "away_score": 100, "team_id": 1},
        ]
    )
    out = gp.score_win_prob(df)
    for c in ["pregame_home_prob", "prior_source", "wp_regime", "home_win_prob", "wp_delta"]:
        assert c in out.columns
    assert out.height == 3 and out["prior_source"][0] == "spread"
    prior = out["pregame_home_prob"][0]
    assert out["wp_delta"][0] == pytest.approx(out["home_win_prob"][0] - prior)
    assert out["wp_delta"][1] == pytest.approx(out["home_win_prob"][1] - out["home_win_prob"][0])
    assert 0.0 < out["home_win_prob"][2] < 0.5  # trailing by one with ten seconds left


def test_score_win_prob_tie_at_ot_start_is_near_half() -> None:
    df = _pbp(
        [
            {
                "period_number": 5,
                "start_game_seconds_remaining": -300.0,
                "start_quarter_seconds_remaining": 300.0,
                "home_score": 101,
                "away_score": 101,
                "home_team_spread": 0.0,
            },
        ]
    )
    out = gp.score_win_prob(df)
    assert out["wp_regime"][0] == "ot"
    assert abs(out["home_win_prob"][0] - 0.5) < 0.10  # the clipped-at-zero model gave 0.39 here


def test_score_win_prob_on_an_empty_frame_keeps_the_schema() -> None:
    out = gp.score_win_prob(_pbp([{}]).clear())
    assert out.height == 0 and "home_win_prob" in out.columns


def test_shot_coordinates_convert_feet_to_legacy_tenths_and_null_everything_else() -> None:
    df = pl.DataFrame(
        {
            "shooting_play": [True, True, True, False],
            "type_text": ["Jump Shot", "Free Throw - 1 of 2", "Layup Shot", "Substitution"],
            "coordinate_x_raw": [3.0, 25.0, 48.0, 10.0],
            "coordinate_y_raw": [0.0, 13.75, 11.0, 5.0],
        }
    )
    out = gp.shot_coordinates(df)
    # ESPN: feet across the court, hoop at (25, 0): (3,0) is a left-corner three 22 ft out
    assert out["x_legacy"].to_list() == [-220.0, None, 230.0, None]
    assert out["y_legacy"].to_list() == [0.0, None, 110.0, None]


def test_shot_coordinates_treat_the_espn_sentinel_as_no_location() -> None:
    df = pl.DataFrame(
        {
            "shooting_play": [True],
            "type_text": ["Jump Shot"],
            "coordinate_x_raw": [-214748340.0],
            "coordinate_y_raw": [-214748365.0],
        }
    )
    assert gp.shot_coordinates(df)["x_legacy"].to_list() == [None]


def _team_box() -> pl.DataFrame:
    # 401766458 ATL @ ORL: scores, FGA, FTA, rebounds and turnovers are the Data API's rows
    # (read 2026-10-11); FGM and 3PM are illustrative. The test is hand arithmetic on these inputs.
    return pl.DataFrame(
        {
            "game_id": [401766458, 401766458],
            "team_id": [1, 19],
            "team_score": [95, 120],
            "field_goals_made": [33, 44],
            "field_goals_attempted": [84, 91],
            "three_point_field_goals_made": [9, 14],
            "free_throws_attempted": [34, 27],
            "offensive_rebounds": [9, 12],
            "defensive_rebounds": [33, 42],
            "total_turnovers": [8, 10],
        }
    )


def test_team_game_rates_match_hand_arithmetic() -> None:
    out = gp.team_game_rates(_team_box()).sort("team_id")
    atl, orl = out.row(0, named=True), out.row(1, named=True)
    poss_atl = 84 - 9 + 8 + 0.44 * 34
    poss_orl = 91 - 12 + 10 + 0.44 * 27
    poss = (poss_atl + poss_orl) / 2
    assert atl["efg_pct"] == pytest.approx((33 + 0.5 * 9) / 84)
    assert atl["tov_pct"] == pytest.approx(8 / (84 + 0.44 * 34 + 8))
    assert atl["orb_pct"] == pytest.approx(9 / (9 + 42))  # own ORB over own ORB + opponent DRB
    assert atl["ft_rate"] == pytest.approx(34 / 84)
    assert atl["possessions"] == pytest.approx(poss) and orl["possessions"] == pytest.approx(poss)
    assert atl["off_rtg"] == pytest.approx(100 * 95 / poss) and atl["def_rtg"] == pytest.approx(100 * 120 / poss)
    assert orl["off_rtg"] == pytest.approx(atl["def_rtg"])
    assert atl["pace"] == pytest.approx(48 * poss / 48) and atl["game_minutes"] == 48


def test_team_game_rates_pace_uses_overtime_minutes() -> None:
    out = gp.team_game_rates(_team_box(), periods=5)
    assert out["game_minutes"][0] == 53
    assert out["pace"][0] == pytest.approx(48 * out["possessions"][0] / 53)


def test_team_game_rates_zero_denominator_is_null() -> None:
    tb = _team_box().with_columns(pl.lit(0).alias("field_goals_attempted"))
    out = gp.team_game_rates(tb)
    assert out["efg_pct"].to_list() == [None, None] and out["ft_rate"].to_list() == [None, None]


def test_team_game_rates_rejects_non_two_row_input() -> None:
    with pytest.raises(ValueError, match="two team rows"):
        gp.team_game_rates(_team_box().head(1))


def test_team_game_rates_rejects_mixed_games_and_duplicate_teams() -> None:
    with pytest.raises(ValueError, match="same game_id"):
        gp.team_game_rates(_team_box().with_columns(pl.Series("game_id", [1, 2])))
    with pytest.raises(ValueError, match="same game_id"):
        gp.team_game_rates(_team_box().with_columns(pl.lit(19).alias("team_id")))


def test_score_win_prob_rejects_missing_columns_and_multi_game_frames() -> None:
    with pytest.raises(pl.exceptions.ColumnNotFoundError):
        gp.score_win_prob(_pbp([{}]).drop("home_team_spread"))
    with pytest.raises(pl.exceptions.ColumnNotFoundError):
        gp.score_win_prob(_pbp([{}]).drop("home_team_spread").clear())
    with pytest.raises(ValueError, match="one game"):
        gp.score_win_prob(_pbp([{"game_id": 1}, {"game_id": 2}]))


def test_home_prior_nan_spread_falls_back_to_hfa() -> None:
    assert gp.home_prior(float("nan"), True)[1] == "hfa"


def test_player_game_rates_match_hand_arithmetic_and_null_the_dnp() -> None:
    pb = pl.DataFrame(
        {
            "game_id": [401766458] * 3,
            "athlete_id": [1, 2, 3],
            "team_id": [1, 1, 1],
            "minutes": [33.0, 0.0, None],
            "points": [11, 0, 0],
            "field_goals_attempted": [10, 0, 0],
            "free_throws_attempted": [0, 0, 0],
            "turnovers": [0, 0, 0],
            "did_not_play": [False, False, True],
        }
    )
    out = gp.player_game_rates(pb, _team_box()).sort("athlete_id")
    team_tsa = 84 + 0.44 * 34 + 8
    assert out["ts_pct"][0] == pytest.approx(11 / (2 * (10 + 0.44 * 0)))
    assert out["usg_pct"][0] == pytest.approx(100 * ((10 + 0 + 0) * (240 / 5)) / (33 * team_tsa))
    assert out["ts_pct"][1] is None and out["usg_pct"][1] is None  # 0 FGA, 0 minutes
    assert out["ts_pct"][2] is None and out["usg_pct"][2] is None  # DNP


class _Resp:
    def __init__(self, payload: dict) -> None:
        self._p = payload

    def json(self) -> dict:
        return self._p


def test_game_page_frames_on_the_espn_fixture(monkeypatch: pytest.MonkeyPatch) -> None:
    from sportsdataverse.nba import nba_pbp

    summary = json.loads((FIX / "summary_nba.json").read_text(encoding="utf-8"))
    monkeypatch.setattr(nba_pbp, "download", lambda url, **kwargs: _Resp(copy.deepcopy(summary)))
    final = nba_pbp.espn_nba_pbp(game_id=int(summary["header"]["id"]), return_parsed=False)
    frames = gp.game_page_frames(final)
    pbp = frames["pbp"]
    assert pbp.height == 450 and "home_win_prob" in pbp.columns and "x_legacy" in pbp.columns
    assert frames["prior"]["source"] == "hfa"  # this fixture ships no pickcenter line
    assert frames["periods"] == 4
    assert frames["team_game_rates"].height == 2 and set(frames["team_game_rates"]["team_id"]) == {19, 28}
    assert frames["player_game_rates"].height == frames["player_box"].height
    assert pbp["wp_delta"].abs().max() > 0.0
    # every located shot came from a shooting play that is not a free throw
    shots = pbp.filter(pl.col("x_legacy").is_not_null())
    assert shots.height > 100 and (shots["shooting_play"] == True).all()
    assert not shots["type_text"].str.contains("(?i)free throw").any()


def test_game_page_frames_before_tip_off_has_no_plays_to_score(monkeypatch: pytest.MonkeyPatch) -> None:
    """A live game with 10 or fewer plays parses to a zero-column pbp; the frames must still build."""
    from sportsdataverse.nba import nba_pbp

    summary = json.loads((FIX / "summary_nba.json").read_text(encoding="utf-8"))
    summary["plays"] = summary["plays"][:5]  # the shape ESPN serves in the first seconds of a game
    monkeypatch.setattr(nba_pbp, "download", lambda url, **kwargs: _Resp(copy.deepcopy(summary)))
    frames = gp.game_page_frames(nba_pbp.espn_nba_pbp(game_id=int(summary["header"]["id"]), return_parsed=False))
    assert frames["pbp"].height == 0
    assert frames["periods"] == gp.REGULATION_PERIODS
    assert frames["prior"]["source"] == "hfa"


def _fixture_pbp(monkeypatch: pytest.MonkeyPatch) -> pl.DataFrame:
    from sportsdataverse.nba import nba_pbp
    from sportsdataverse.nba.nba_play_by_play import helper_nba_play_by_play

    summary = json.loads((FIX / "summary_nba.json").read_text(encoding="utf-8"))
    monkeypatch.setattr(nba_pbp, "download", lambda url, **kwargs: _Resp(copy.deepcopy(summary)))
    return helper_nba_play_by_play(nba_pbp.espn_nba_pbp(game_id=int(summary["header"]["id"]), return_parsed=False))


def test_final_win_probability_settles_at_0_or_1(monkeypatch: pytest.MonkeyPatch) -> None:
    wp = gp.score_win_prob(_fixture_pbp(monkeypatch))
    last = wp.row(wp.height - 1, named=True)
    assert last["type_text"] == "End Game"
    assert last["home_score"] != last["away_score"]
    assert last["home_win_prob"] == (1.0 if last["home_score"] > last["away_score"] else 0.0)
    assert last["wp_delta"] == pytest.approx(last["home_win_prob"] - wp["home_win_prob"][wp.height - 2])
    # 0 seconds alone does not settle: other plays keep the model value
    pbp = _fixture_pbp(monkeypatch)
    raw = gp.nba_in_game_win_prob(gp.ot_adjusted_seconds(pbp), wp["pregame_home_prob"][0])["home_win_prob"].cast(
        pl.Float64
    )
    zero = (pbp["end_game_seconds_remaining"] == 0) & ~pbp["type_text"].is_in(["End Game", "End Period"])
    assert zero.sum() > 0
    assert wp["home_win_prob"].filter(zero).to_list() == raw.filter(zero).to_list()


def test_tied_end_period_is_not_settled(monkeypatch: pytest.MonkeyPatch) -> None:
    pbp = _fixture_pbp(monkeypatch)
    ends = pbp.filter((pl.col("type_text") == "End Period") & (pl.col("period_number") >= 4))
    assert ends.height, "fixture has a 4th-period End Period row"
    i = ends.row(0, named=True)["sequence_number"]
    tied = pbp.with_columns(
        pl.when(pl.col("sequence_number") == i)
        .then(pl.col("home_score"))
        .otherwise(pl.col("away_score"))
        .alias("away_score")
    )
    row = gp.score_win_prob(tied).filter(pl.col("sequence_number") == i).row(0, named=True)
    assert row["home_win_prob"] not in (0.0, 1.0)


def _team_box_from(name: str) -> pl.DataFrame:
    from sportsdataverse.nba.nba_team_box import helper_nba_team_box

    return helper_nba_team_box(json.loads((FIX / name).read_text(encoding="utf-8")))


def test_pre_2013_doubled_team_turnovers_are_undone() -> None:
    # 271128023 GS @ SAC (2007-11-29), ESPN's own payload: turnovers=17 teamTurnovers=17 totalTurnovers=34 (GS), 20/20/40 (SAC)
    box = _team_box_from("summary_nba_2007_team_box.json")
    assert sorted(zip(box["turnovers"], box["team_turnovers"], box["total_turnovers"])) == [(17, 17, 34), (20, 20, 40)]
    out = gp.team_game_rates(box).sort("team_id")
    assert out["total_turnovers"].to_list() == [17, 20]
    # 111.6 is this game's pace (176 FGA, 37 turnovers, 62 FTA; 130 with the doubled 34/40); a 360-game raw sample of
    # 2007/2009/2012 averages 95-96 possessions with corrected turnovers
    assert 85 < out["pace"][0] < 115


def test_null_total_turnovers_is_player_plus_team() -> None:
    box = _team_box_from("summary_nba_2007_team_box.json").with_columns(
        pl.lit(None, dtype=pl.Int32).alias("total_turnovers"),
        pl.when(pl.col("team_id") == pl.col("team_id").min())
        .then(None)
        .otherwise(pl.col("team_turnovers") - 15)
        .cast(pl.Int32)
        .alias("team_turnovers"),
    )
    out = gp.team_game_rates(box).sort("team_id")
    assert (
        out["total_turnovers"].to_list()
        == (box.sort("team_id")["turnovers"] + box.sort("team_id")["team_turnovers"].fill_null(0)).to_list()
    )


def test_modern_total_turnovers_unchanged() -> None:
    box = _team_box_from("summary_nba.json")
    assert (box["total_turnovers"] == box["turnovers"] + box["team_turnovers"]).all()
    out = gp.team_game_rates(box.head(2)).sort("team_id")
    assert out["total_turnovers"].to_list() == box.head(2).sort("team_id")["total_turnovers"].to_list()


def test_usage_uses_the_repaired_team_turnovers() -> None:
    from sportsdataverse.nba.nba_player_box import helper_nba_player_box

    final = json.loads((FIX / "summary_nba_2007_team_box.json").read_text(encoding="utf-8"))
    team_box, player_box = _team_box_from("summary_nba_2007_team_box.json"), helper_nba_player_box(final)
    out = gp.player_game_rates(player_box, team_box)
    p = player_box.filter((pl.col("minutes") > 20) & (pl.col("did_not_play") == False)).row(0, named=True)  # noqa: E712
    tm = team_box.filter(pl.col("team_id") == p["team_id"]).row(0, named=True)
    assert tm["turnovers"] in (17, 20) and tm["total_turnovers"] == 2 * tm["turnovers"]

    def usg(tov: float) -> float:
        tsa = p["field_goals_attempted"] + 0.44 * p["free_throws_attempted"]
        team_tsa = tm["field_goals_attempted"] + 0.44 * tm["free_throws_attempted"] + tov
        return 100.0 * (tsa + p["turnovers"]) * (240 / 5) / (p["minutes"] * team_tsa)

    got = out.filter(pl.col("athlete_id") == p["athlete_id"])["usg_pct"][0]
    assert got == pytest.approx(usg(tm["turnovers"]))
    assert got != pytest.approx(usg(tm["total_turnovers"]))


def test_unreconciled_pre_2013_rebound_split_nulls_orb_pct() -> None:
    # 271128023 GS @ SAC: ESPN shows 12+13 vs 71 (GS) and 5+19 vs 80 (SAC) rebounds (#757)
    out = gp.team_game_rates(_team_box_from("summary_nba_2007_team_box.json"))
    assert out["orb_pct"].to_list() == [None, None]
    assert out["pace"].null_count() == 0


def test_null_total_rebounds_keeps_orb_pct() -> None:
    box = _team_box_from("summary_nba.json").head(2).with_columns(pl.lit(None, dtype=pl.Int64).alias("total_rebounds"))
    assert gp.team_game_rates(box)["orb_pct"].null_count() == 0


def test_reconciled_rebound_split_keeps_orb_pct() -> None:
    box = _team_box_from("summary_nba.json").head(2)
    assert ((box["offensive_rebounds"] + box["defensive_rebounds"]) == box["total_rebounds"]).all()
    out = gp.team_game_rates(box).sort("team_id")
    assert out["orb_pct"].null_count() == 0
    # one team's split breaking nulls orb_pct for both rows (each uses the other's DRB)
    broken = box.with_columns(
        pl.when(pl.col("team_id") == pl.col("team_id").min())
        .then(pl.col("total_rebounds") + 1)
        .otherwise(pl.col("total_rebounds"))
        .alias("total_rebounds")
    )
    assert gp.team_game_rates(broken)["orb_pct"].to_list() == [None, None]


def test_wnba_regulation_is_40_minutes_and_overtime_5() -> None:
    assert gp.game_minutes(4, league_id="10") == 40
    assert gp.game_minutes(5, league_id="10") == 45
    assert gp.game_minutes(5) == 53 and gp.game_minutes(4, league_id="00") == 48
    with pytest.raises(ValueError, match="league_id"):
        gp.game_minutes(4, league_id="99")


def test_wnba_pace_and_usage_scale_with_the_shorter_game() -> None:
    nba = gp.team_game_rates(_team_box(), periods=5).sort("team_id")
    wnba = gp.team_game_rates(_team_box(), periods=5, league_id="10").sort("team_id")
    assert wnba["game_minutes"][0] == 45
    assert wnba["pace"][0] == pytest.approx(40 * wnba["possessions"][0] / 45)  # pace per 40 minutes
    assert wnba["pace"][0] != pytest.approx(nba["pace"][0])
    pb = pl.DataFrame(
        {
            "game_id": [401766458],
            "athlete_id": [1],
            "team_id": [1],
            "minutes": [30.0],
            "points": [11],
            "field_goals_attempted": [10],
            "free_throws_attempted": [0],
            "turnovers": [0],
            "did_not_play": [False],
        }
    )
    u_nba = gp.player_game_rates(pb, _team_box())["usg_pct"][0]
    u_wnba = gp.player_game_rates(pb, _team_box(), league_id="10")["usg_pct"][0]
    assert u_wnba == pytest.approx(u_nba * 40 / 48)  # team minutes are 5 x 40, not 5 x 48


def test_unsupported_league_ids_raise_value_error(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValueError, match="'00'.*'10'"):
        gp.score_win_prob(_pbp([{}]), league_id="20")
    with pytest.raises(ValueError, match="'00'.*'10'"):
        gp.game_page_frames({}, league_id="20")


def _no_teams_frames(monkeypatch: pytest.MonkeyPatch, league_id: str = "00") -> dict:
    from sportsdataverse.nba import nba_pbp

    summary = json.loads((FIX / "summary_nba.json").read_text(encoding="utf-8"))
    summary["boxscore"]["teams"] = []
    monkeypatch.setattr(nba_pbp, "download", lambda url, **kwargs: _Resp(copy.deepcopy(summary)))
    return gp.game_page_frames(
        nba_pbp.espn_nba_pbp(game_id=int(summary["header"]["id"]), return_parsed=False), league_id=league_id
    )


def test_game_page_frames_without_team_rows_carries_the_rate_schemas(monkeypatch: pytest.MonkeyPatch) -> None:
    from sportsdataverse.nba.nba_player_box import helper_nba_player_box
    from sportsdataverse.nba.nba_team_box import helper_nba_team_box

    summary = json.loads((FIX / "summary_nba.json").read_text(encoding="utf-8"))
    tb, pb = helper_nba_team_box(summary), helper_nba_player_box(summary)
    frames = _no_teams_frames(monkeypatch)
    assert frames["team_box"].height == 0
    assert frames["team_game_rates"].height == 0 and frames["player_game_rates"].height == 0
    assert frames["team_game_rates"].schema == gp.team_game_rates(tb).schema
    assert frames["player_game_rates"].schema == gp.player_game_rates(pb, tb).schema
    assert frames["team_game_rates"].select("efg_pct").height == 0  # selectable before tip-off


def test_player_game_rates_rejects_a_duplicated_team_row() -> None:
    pb = pl.DataFrame(
        {
            "game_id": [401766458],
            "athlete_id": [1],
            "team_id": [1],
            "minutes": [30.0],
            "points": [11],
            "field_goals_attempted": [10],
            "free_throws_attempted": [0],
            "turnovers": [0],
            "did_not_play": [False],
        }
    )
    dup = pl.concat([_team_box().head(1), _team_box().head(1), _team_box().tail(1)])
    with pytest.raises(ValueError, match="team rows"):
        gp.player_game_rates(pb, dup)
    with pytest.raises(ValueError, match="same game_id"):
        gp.player_game_rates(pb, _team_box().with_columns(pl.lit(19).alias("team_id")))


def test_null_score_mid_game_is_carried_forward(monkeypatch: pytest.MonkeyPatch) -> None:
    pbp = _fixture_pbp(monkeypatch)
    base = gp.score_win_prob(pbp)
    same = pbp.with_row_index("i").filter(
        (pl.col("home_score") == pl.col("home_score").shift(1))
        & (pl.col("period_number") == 2)
        & (pl.col("type_text") != "End Period")
    )
    assert same.height
    i = same["i"][same.height // 2]
    holed = pbp.with_columns(
        pl.when(pl.int_range(pl.len()) == i).then(None).otherwise(pl.col("home_score")).alias("home_score"),
        pl.when(pl.int_range(pl.len()) == i).then(None).otherwise(pl.col("away_score")).alias("away_score"),
    )
    out = gp.score_win_prob(holed)
    assert out["home_win_prob"][i] == pytest.approx(base["home_win_prob"][i], abs=1e-9)
    assert (out["wp_delta"] - base["wp_delta"]).abs().max() < 1e-9
    # the fill feeds the model only; the caller's score columns come back as given
    assert out["home_score"][i] is None and out["away_score"][i] is None
