"""nba_game_page across leagues (nba / wnba / mbb / wbb) on real ESPN captures.

Fixtures: tests/fixtures/espn/summary_*.json and tests/fixtures/espn/basketball_pbp/*.json.gz
(provenance in tests/fixtures/espn/basketball_pbp/README.md).
"""

from __future__ import annotations

import copy
import gzip
import importlib
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.mbb import mbb_game_predict as ncaa_predict
from sportsdataverse.mbb import mbb_prediction_constants as ncaa_constants
from sportsdataverse.mbb.mbb_game_predict import mbb_in_game_win_prob
from sportsdataverse.nba import nba_game_page as gp

ESPN = Path(__file__).resolve().parents[1] / "fixtures" / "espn"
BB = ESPN / "basketball_pbp"


def _raw(name: str) -> dict:
    if name.endswith(".gz"):
        with gzip.open(BB / name, "rt", encoding="utf-8") as f:
            return json.load(f)
    return json.loads((ESPN / name).read_text(encoding="utf-8"))


class _Resp:
    def __init__(self, payload: dict) -> None:
        self._p = payload

    def json(self) -> dict:
        return self._p


def _final(monkeypatch: pytest.MonkeyPatch, lg: str, name: str) -> dict:
    """``espn_<lg>_pbp(game_id)`` on the fixture, offline (the fixture is served as the HTTP body)."""
    raw = _raw(name)
    mod = importlib.import_module(f"sportsdataverse.{lg}.{lg}_pbp")
    monkeypatch.setattr(mod, "download", lambda url, **kwargs: _Resp(copy.deepcopy(raw)))
    kw = {"return_parsed": False} if lg in ("nba", "mbb") else {}  # the dict; nba / mbb default to the release frame
    return getattr(mod, f"espn_{lg}_pbp")(game_id=int(raw["header"]["id"]), **kw)


def _frames(
    monkeypatch: pytest.MonkeyPatch, lg: str, name: str
) -> tuple[dict, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """``(final, pbp, team_box, player_box)`` through the league's hoopR-schema converters."""
    final = _final(monkeypatch, lg, name)

    def helper(kind: str):  # noqa: ANN202 - test-local lookup
        return getattr(importlib.import_module(f"sportsdataverse.{lg}.{lg}_{kind}"), f"helper_{lg}_{kind}")

    return final, helper("play_by_play")(final), helper("team_box")(final), helper("player_box")(final)


@pytest.mark.parametrize(
    ("name", "season", "periods", "neutral", "max_period", "plays"),
    [
        ("wnba_250603003.json.gz", 2005, 2, False, 3, 459),
        ("wbb_293170052.json.gz", 2010, 2, False, 2, 351),
        ("wbb_273130043.json.gz", 2008, 2, False, 2, 358),
        ("wbb_283430164.json.gz", 2009, 2, False, 2, 187),
        ("wbb_290232368.json.gz", 2009, 2, False, 3, 273),
        ("wbb_401364640.json.gz", 2022, 4, False, 4, 298),
        ("mbb_401812161.json.gz", 2026, 2, False, 2, 425),
        ("mbb_401830342.json.gz", 2026, 2, True, 4, 426),
        ("summary_mbb.json", 2024, 2, True, 2, 294),
        ("summary_wbb.json", 2024, 4, True, 4, 360),
        ("summary_wnba.json", 2024, 4, False, 5, 418),
    ],
)
def test_capture_provenance(name: str, season: int, periods: int, neutral: bool, max_period: int, plays: int) -> None:
    raw = _raw(name)
    comp = raw["header"]["competitions"][0]
    got = (
        raw["header"]["season"]["year"],
        raw["format"]["regulation"]["periods"],
        comp["neutralSite"],
        max(p["period"]["number"] for p in raw["plays"]),
        len(raw["plays"]),
    )
    assert got == (season, periods, neutral, max_period, plays)


def test_league_id_callers_keep_their_meaning() -> None:
    assert gp.game_minutes(5) == 53 and gp.game_minutes(5, league_id="00") == 53
    assert gp.game_minutes(5, league_id="10") == 45 == gp.game_minutes(5, league="wnba")
    with pytest.raises(ValueError, match="'00'.*'10'"):
        gp.game_minutes(4, league_id="20")
    with pytest.raises(ValueError, match="contradicts"):
        gp.game_minutes(4, league="mbb", league_id="00")
    with pytest.raises(ValueError, match="'nba', 'wnba', 'mbb', 'wbb'"):
        gp.game_minutes(4, league="nhl")
    # league and league_id may both be given when they agree
    assert gp.game_minutes(5, league="wnba", league_id="10") == 45
    assert gp.game_minutes(5, league="nba", league_id="00") == 53


@pytest.mark.parametrize(
    ("league", "season", "fmt", "want"),
    [
        ("nba", 2024, None, 4),
        ("mbb", 2026, None, 2),
        ("mbb", None, 2.0, 2),
        ("wnba", 2005, None, 2),
        ("wnba", 2006, None, 4),
        ("wnba", None, None, 4),
        ("wbb", 2015, None, 2),
        ("wbb", 2016, None, 4),
        ("wbb", 2016, 2.0, 2),  # the game's own format wins over the season cutoff
        ("wnba", 2005, float("nan"), 2),  # a null schedule cell falls back to the cutoff
        ("mbb", 2026, 3.0, 2),  # only 2 or 4 is believed
        ("wnba", 2005, float("inf"), 2),  # a broken cell (inf) falls back too
        ("wnba", float("nan"), None, 4),  # a NaN season is an unknown season
        (None, 2005, None, 4),  # no league means NBA
    ],
)
def test_game_regulation_periods(league: str | None, season: float | None, fmt: float | None, want: int) -> None:
    assert gp.game_regulation_periods(league, season=season, format_regulation_periods=fmt) == want


def test_game_minutes_count_overtime_past_the_games_own_regulation() -> None:
    assert gp.game_minutes(2, league="mbb") == 40
    assert gp.game_minutes(4, league="mbb") == 50  # 2OT in a halves game (mbb_401830342)
    assert gp.game_minutes(4, league="wbb") == 40 and gp.game_minutes(5, league="wbb") == 45
    assert gp.game_minutes(3, league="wnba", regulation_periods=2) == 45  # 2005 halves, one OT
    assert gp.game_minutes(2, league="wbb", regulation_periods=2) == 40


def test_college_constants_are_the_ones_the_spec_names() -> None:
    mens, womens = ncaa_constants.get_constants("mens"), ncaa_constants.get_constants("womens")
    assert (mens.hfa, mens.margin_sd, womens.hfa, womens.margin_sd) == (2.9281, 11.2196, 2.6432, 12.1716)


def test_college_prior_uses_the_college_model_constants() -> None:
    p, src = gp.home_prior(None, None, league="mbb")
    assert src == "hfa" and p == pytest.approx(ncaa_predict.win_prob_from_margin(2.9281, league="mens"))
    p_w, _ = gp.home_prior(None, None, league="wbb")
    assert p_w == pytest.approx(ncaa_predict.win_prob_from_margin(2.6432, league="womens"))
    # league_id="10" used to be the only way to score WBB: it gave the WNBA prior, which is wrong
    assert p_w != pytest.approx(gp.home_prior(None, None, league_id="10")[0])
    p_s, src_s = gp.home_prior(-1.5, True, league="mbb")  # mbb_401812161: home +1.5 underdog
    assert src_s == "spread" and p_s == pytest.approx(ncaa_predict.win_prob_from_margin(-1.5, league="mens"))


@pytest.mark.parametrize("league", ["nba", "wnba", "mbb", "wbb"])
def test_neutral_site_without_a_line_has_no_home_court(league: str) -> None:
    p, src = gp.home_prior(None, None, league=league, neutral_site=True)
    assert src == "neutral" and p == pytest.approx(0.5)
    # the +2.5 the pickcenter helpers default to with no line never reaches the model
    assert gp.home_prior(2.5, False, league=league, neutral_site=True)[1] == "neutral"
    # a real line already prices the venue
    assert gp.home_prior(-3.0, True, league=league, neutral_site=True)[1] == "spread"
    # a null warehouse cell is not a neutral site
    assert gp.home_prior(None, None, league=league, neutral_site=None)[1] == "hfa"
    assert gp.home_prior(None, None, league=league, neutral_site=float("nan"))[1] == "hfa"  # type: ignore[arg-type]


def test_nba_prior_is_unchanged() -> None:
    from sportsdataverse.nba.nba_game_predict import win_prob_from_margin

    assert gp.home_prior(None, None) == (win_prob_from_margin(2.184, league_id="00"), "hfa")
    assert gp.home_prior(None, None, league_id="10") == (win_prob_from_margin(1.184, league_id="10"), "hfa")
    assert gp.home_prior(3.0, True) == (win_prob_from_margin(3.0, league_id="00"), "spread")


def _filled(pbp: pl.DataFrame) -> pl.DataFrame:
    return pbp.with_columns(pl.col("home_score", "away_score").forward_fill().fill_null(0))


def test_mbb_double_overtime_reads_the_period_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    _, pbp, _, _ = _frames(monkeypatch, "mbb", "mbb_401830342.json.gz")
    assert "start_quarter_seconds_remaining" not in pbp.columns  # the KeyError the NBA-only module hit
    tagged = gp.ot_adjusted_seconds(pbp, regulation_periods=2)
    ot = tagged.filter(pl.col("wp_regime") == "ot")
    assert ot.height == pbp.filter(pl.col("period_number") > 2).height == 69
    assert (
        ot["start_game_seconds_remaining"].to_list() == ot["start_period_seconds_remaining"].cast(pl.Float64).to_list()
    )
    assert ot["start_game_seconds_remaining"].max() <= 300.0


def test_mbb_halves_settle_and_overtime(monkeypatch: pytest.MonkeyPatch) -> None:
    _, pbp, _, _ = _frames(monkeypatch, "mbb", "mbb_401830342.json.gz")
    wp = gp.score_win_prob(pbp, league="mbb", neutral_site=True)  # regulation 2 from league, season 2026
    ends = wp.filter(pl.col("type_text").is_in(["End Period", "End Game"]))
    by = {(r["period_number"], r["type_text"]): r["home_win_prob"] for r in ends.iter_rows(named=True)}
    assert by[(1, "End Period")] not in (0.0, 1.0)  # first half: the game goes on
    assert by[(2, "End Period")] not in (0.0, 1.0)  # 65-65: overtime, not the end
    assert by[(3, "End Period")] not in (0.0, 1.0)  # 73-73: second overtime
    assert by[(4, "End Game")] == 0.0  # 75-81, the home side lost
    assert wp["prior_source"][0] == "neutral" and wp["pregame_home_prob"][0] == pytest.approx(0.5)
    # every unsettled play is the men's model on the OT-adjusted frame
    raw = mbb_in_game_win_prob(gp.ot_adjusted_seconds(_filled(pbp), regulation_periods=2), 0.5, league="mens")
    keep = ~pbp["type_text"].is_in(["End Period", "End Game"])
    assert wp["home_win_prob"].filter(keep).to_list() == raw["home_win_prob"].cast(pl.Float64).filter(keep).to_list()


def test_regulation_end_of_a_halves_game_settles(monkeypatch: pytest.MonkeyPatch) -> None:
    _, pbp, _, _ = _frames(monkeypatch, "mbb", "mbb_401812161.json.gz")
    wp = gp.score_win_prob(pbp, league="mbb")
    end2 = wp.filter((pl.col("period_number") == 2) & pl.col("type_text").is_in(["End Period", "End Game"]))
    assert end2["home_win_prob"].to_list() == [1.0, 1.0]  # 75-62 home win; the NBA rule (period >= 4) never fired
    assert wp["prior_source"][0] == "spread" and wp["home_team_spread"][0] == -1.5


def test_wnba_2005_halves_overtime(monkeypatch: pytest.MonkeyPatch) -> None:
    _, pbp, _, _ = _frames(monkeypatch, "wnba", "wnba_250603003.json.gz")
    wp = gp.score_win_prob(pbp, league="wnba")  # regulation 2 from season 2005
    assert wp.filter(pl.col("wp_regime") == "ot").height == pbp.filter(pl.col("period_number") == 3).height == 58
    tied = wp.filter((pl.col("period_number") == 2) & (pl.col("type_text") == "End Period"))
    assert tied["home_score"][0] == tied["away_score"][0] == 54 and tied["home_win_prob"][0] not in (0.0, 1.0)
    assert wp["home_win_prob"][-1] == 1.0 and wp["prior_source"][0] == "hfa"
    # the NBA-only module scored period 3 as regulation and never settled period 2
    assert gp.score_win_prob(pbp, league="wnba", regulation_periods=4)["wp_regime"].unique().to_list() == ["reg"]


def test_wbb_scores_with_the_womens_model(monkeypatch: pytest.MonkeyPatch) -> None:
    _, pbp, _, _ = _frames(monkeypatch, "wbb", "summary_wbb.json")
    wp = gp.score_win_prob(pbp, league="wbb", neutral_site=True)
    womens = mbb_in_game_win_prob(gp.ot_adjusted_seconds(_filled(pbp)), 0.5, league="womens")["home_win_prob"]
    unsettled = ~pbp["type_text"].is_in(["End Period", "End Game"])
    assert wp["home_win_prob"].filter(unsettled).to_list() == womens.cast(pl.Float64).filter(unsettled).to_list()
    wnba = gp.score_win_prob(pbp, league="wnba", neutral_site=True)["home_win_prob"]
    assert (wp["home_win_prob"] - wnba).abs().max() > 0.01  # the old league_id="10" route used the WNBA model


def test_a_frame_without_either_period_clock_raises() -> None:
    df = pl.DataFrame(
        {
            "game_id": [1],
            "home_team_id": [1],
            "team_id": [1],
            "home_score": [0],
            "away_score": [0],
            "period_number": [1],
            "start_game_seconds_remaining": [2400.0],
            "home_team_spread": [2.5],
            "game_spread_available": [False],
        }
    )
    with pytest.raises(pl.exceptions.ColumnNotFoundError, match="period clock"):
        gp.score_win_prob(df, league="mbb")


def test_college_free_throws_get_no_location(monkeypatch: pytest.MonkeyPatch) -> None:
    _, pbp, _, _ = _frames(monkeypatch, "mbb", "mbb_401830342.json.gz")
    fts = pbp.filter(pl.col("type_text") == "MadeFreeThrow")
    assert fts.height == 39 and (fts["shooting_play"] == True).all()  # noqa: E712
    assert set(zip(fts["coordinate_x_raw"], fts["coordinate_y_raw"])) == {(25.0, 0.0)}  # ESPN pins them to the rim
    out = gp.shot_coordinates(pbp)
    assert out.filter(pl.col("type_text") == "MadeFreeThrow")["x_legacy"].null_count() == 39
    assert (
        out["x_legacy"].is_not_null().sum()
        == pbp.filter(
            (pl.col("shooting_play") == True) & (pl.col("type_text") != "MadeFreeThrow")  # noqa: E712
        ).height
    )


def test_wbb_2010_turnovers_come_from_team_turnovers(monkeypatch: pytest.MonkeyPatch) -> None:
    _, _, tb, _ = _frames(monkeypatch, "wbb", "wbb_293170052.json.gz")
    assert sorted(zip(tb["team_id"], tb["turnovers"], tb["team_turnovers"], tb["total_turnovers"])) == [
        (52, 0, 23, 0),
        (2454, 0, 28, 0),
    ]
    out = gp.team_game_rates(tb, league="wbb", regulation_periods=2).sort("team_id")
    assert out["total_turnovers"].to_list() == [23, 28]
    assert out["tov_pct"].null_count() == 0 and 60 < out["pace"][0] < 95
    # the NBA turnover rule (the old league_id="10" route) read 0 turnovers here
    assert gp.team_game_rates(tb, league_id="10")["total_turnovers"].to_list() == [0, 0]


def test_wbb_2008_has_no_turnovers_so_the_rates_that_need_them_are_null(monkeypatch: pytest.MonkeyPatch) -> None:
    _, _, tb, pb = _frames(monkeypatch, "wbb", "wbb_273130043.json.gz")
    out = gp.team_game_rates(tb, league="wbb", regulation_periods=2)
    for col in ("total_turnovers", "tov_pct", "possessions", "off_rtg", "def_rtg", "pace"):
        assert out[col].to_list() == [None, None], col
    for col in ("efg_pct", "orb_pct", "ft_rate"):
        assert out[col].null_count() == 0, col
    # usage needs the team's turnovers too: a player with minutes still gets null, never a number built on 0
    played = pb.with_columns(pl.lit(30.0).alias("minutes"), pl.lit(False).alias("did_not_play"))
    rates = gp.player_game_rates(played, tb, league="wbb", regulation_periods=2)
    assert rates["usg_pct"].null_count() == rates.height


def test_modern_college_turnovers_are_the_box_turnovers(monkeypatch: pytest.MonkeyPatch) -> None:
    _, pbp, tb, _ = _frames(monkeypatch, "mbb", "summary_mbb.json")
    out = gp.team_game_rates(tb, league="mbb").sort("team_id")
    assert out["total_turnovers"].to_list() == tb.sort("team_id")["turnovers"].to_list() == [8, 9]
    tos = pbp.filter(pl.col("type_text").str.contains("(?i)turnover")).group_by("team_id").agg(pl.len()).sort("team_id")
    assert tos["len"].to_list() == [8, 9]  # the box count matches the plays


def test_genuine_zero_college_turnovers_stay_zero_after_2009(monkeypatch: pytest.MonkeyPatch) -> None:
    # D12: a college 0 is "not recorded" (null) only in WBB before 2009. GMU (2244) committed no
    # turnover in MBB 401828345 (2026): box 0 under every key, and no turnover play for 2244.
    _, pbp, tb, _ = _frames(monkeypatch, "mbb", "mbb_401828345.json.gz")
    assert tb["season"].unique().to_list() == [2026]
    tos = pbp.filter(pl.col("type_text").str.contains("(?i)turnover")).group_by("team_id").agg(pl.len())
    assert dict(tos.rows()) == {2325: 4}
    out = gp.team_game_rates(tb, league="mbb").sort("team_id")
    assert out["team_id"].to_list() == [2244, 2325]
    assert out["total_turnovers"].to_list() == [0, 4]
    assert out["tov_pct"][0] == 0.0 and out["possessions"].null_count() == 0 and out["pace"].null_count() == 0


def test_wbb_zero_turnovers_before_2009_or_without_a_season_are_null(monkeypatch: pytest.MonkeyPatch) -> None:
    # The 2008 box (every turnover key 0) is "not recorded", and a box whose season is unknown is never
    # read as 2009+. The same real box with its season set to 2010 in-test (a mutation of the real
    # capture, not a synthetic frame) is still null: the game's total (0) is below 5, the game-level rule.
    _, _, tb, _ = _frames(monkeypatch, "wbb", "wbb_273130043.json.gz")
    assert tb["season"].unique().to_list() == [2008]
    unknown = gp.team_game_rates(tb.drop("season"), league="wbb", regulation_periods=2)
    assert unknown["total_turnovers"].to_list() == [None, None]
    as_2010 = tb.with_columns(pl.lit(2010).cast(tb.schema["season"]).alias("season"))
    assert gp.team_game_rates(as_2010, league="wbb", regulation_periods=2)["total_turnovers"].to_list() == [None, None]


def test_wbb_2009_game_where_both_teams_show_zero_turnovers_is_null(monkeypatch: pytest.MonkeyPatch) -> None:
    # F2: WBB 283430164 (2008-12-09 UGA @ RUTG, season 2009) records no turnover under any box key for
    # either team, yet its plays log 13 RUTG and 10 UGA turnovers: the 0s are "not recorded".
    final, pbp, tb, pb = _frames(monkeypatch, "wbb", "wbb_283430164.json.gz")
    assert tb["season"].unique().to_list() == [2009]
    assert sorted(zip(tb["team_id"], tb["turnovers"], tb["team_turnovers"], tb["total_turnovers"])) == [
        (61, 0, 0, 0),
        (164, 0, 0, 0),
    ]
    tos = pbp.filter(pl.col("type_text").str.contains("(?i)turnover")).group_by("team_id").agg(pl.len())
    assert dict(tos.rows()) == {164: 13, 61: 10}
    out = gp.team_game_rates(tb, league="wbb")
    for col in ("total_turnovers", "tov_pct", "possessions", "off_rtg", "def_rtg", "pace"):
        assert out[col].to_list() == [None, None], col
    for col in ("efg_pct", "orb_pct", "ft_rate"):
        assert out[col].null_count() == 0, col
    assert gp.player_game_rates(pb, tb, league="wbb")["usg_pct"].null_count() == pb.height
    live = gp.game_page_frames(final, league="wbb")
    assert live["team_game_rates"]["tov_pct"].to_list() == [None, None]


def test_wbb_2009_game_total_below_five_is_unrecorded(monkeypatch: pytest.MonkeyPatch) -> None:
    # Re-ruling: a WBB game whose known total is < 5 did not record turnovers. WBB 290232368 (2009-01-23
    # FAIR @ MRST, season 2009): FAIR (2217) 0 under every key, MRST (2368) 1 under teamTurnovers (total 1),
    # while the plays log 12 FAIR / 16 MRST turnovers: both rows null.
    _, pbp, tb, _ = _frames(monkeypatch, "wbb", "wbb_290232368.json.gz")
    assert tb["season"].unique().to_list() == [2009]
    assert sorted(zip(tb["team_id"], tb["turnovers"], tb["team_turnovers"], tb["total_turnovers"])) == [
        (2217, 0, 0, 0),
        (2368, 0, 1, 0),
    ]
    tos = pbp.filter(pl.col("type_text").str.contains("(?i)turnover")).group_by("team_id").agg(pl.len())
    assert dict(tos.rows()) == {2217: 12, 2368: 16}
    out = gp.team_game_rates(tb, league="wbb", periods=3)
    for col in ("total_turnovers", "tov_pct", "possessions", "pace"):
        assert out[col].to_list() == [None, None], col


def test_wbb_genuine_low_turnover_game_keeps_its_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    # WBB 401364640 (2022-01-15 BOIS @ SDSU): a genuine low total of 8 (BOIS 2, SDSU 6, matching the
    # plays) is at or above the threshold, so the counts and every rate stay.
    _, pbp, tb, _ = _frames(monkeypatch, "wbb", "wbb_401364640.json.gz")
    assert tb["season"].unique().to_list() == [2022]
    tos = pbp.filter(pl.col("type_text").str.contains("(?i)turnover")).group_by("team_id").agg(pl.len())
    assert dict(tos.rows()) == {68: 2, 21: 6}
    out = gp.team_game_rates(tb, league="wbb").sort("team_id")
    assert out["team_id"].to_list() == [21, 68]
    assert out["total_turnovers"].to_list() == [6, 2]
    assert out["tov_pct"].null_count() == 0 and out["possessions"].null_count() == 0 and out["pace"].null_count() == 0


def test_halves_overtime_pace_and_usage_use_the_games_minutes(monkeypatch: pytest.MonkeyPatch) -> None:
    _, _, tb, pb = _frames(monkeypatch, "mbb", "mbb_401830342.json.gz")
    out = gp.team_game_rates(tb, periods=4, league="mbb")
    assert out["game_minutes"].to_list() == [50, 50]  # 2 x 20 + 2 x 5
    assert out["pace"][0] == pytest.approx(40 * out["possessions"][0] / 50)
    reg = gp.player_game_rates(pb, tb, periods=4, league="mbb")
    assert reg.height == pb.height and reg["usg_pct"].drop_nulls().max() < 60


def test_wbb_halves_box_without_regulation_reads_the_box_season(monkeypatch: pytest.MonkeyPatch) -> None:
    # The real WBB 2010 box (two halves) with one overtime played: the default regulation comes from the
    # box's own season, so period 3 is an overtime (45 minutes), not a third quarter (40).
    _, _, tb, pb = _frames(monkeypatch, "wbb", "wbb_293170052.json.gz")
    assert tb["season"].unique().to_list() == [2010]
    assert gp.team_game_rates(tb, periods=3, league="wbb")["game_minutes"].to_list() == [45, 45]
    # a float season column with a NaN (a warehouse frame) still reads the season
    nan_season = tb.with_columns(pl.Series("season", [float("nan"), 2010.0]))
    assert gp.team_game_rates(nan_season, periods=3, league="wbb")["game_minutes"].to_list() == [45, 45]
    want = gp.player_game_rates(pb, tb, periods=3, league="wbb", regulation_periods=2)
    assert gp.player_game_rates(pb, tb, periods=3, league="wbb").equals(want)


GOLDEN = json.loads((BB / "nba_game_page_golden.json").read_text(encoding="utf-8"))
PBP_COLS = ["home_win_prob", "wp_delta", "wp_regime", "prior_source", "pregame_home_prob", "x_legacy", "y_legacy"]


@pytest.mark.parametrize(
    ("lg", "name", "kwargs"),
    [
        ("nba", "summary_nba.json", {}),
        ("nba", "summary_nba.json", {"league_id": "00"}),
        ("nba", "summary_nba.json", {"league": "nba"}),
        ("wnba", "summary_wnba.json", {"league_id": "10"}),
        ("wnba", "summary_wnba.json", {"league": "wnba"}),
    ],
)
def test_nba_and_wnba_frames_are_byte_identical_to_the_shipped_module(
    monkeypatch: pytest.MonkeyPatch, lg: str, name: str, kwargs: dict
) -> None:
    # the golden was written by the shipped (NBA-only) module at e4894f0817; never regenerate it from this one
    fr = gp.game_page_frames(_final(monkeypatch, lg, name), **kwargs)
    want = GOLDEN[name]
    assert fr["pbp"].select(PBP_COLS).to_dict(as_series=False) == want["pbp"]
    assert fr["team_game_rates"].sort("team_id").to_dict(as_series=False) == want["team_game_rates"]
    assert fr["player_game_rates"].sort("athlete_id").to_dict(as_series=False) == want["player_game_rates"]
    assert (fr["periods"], fr["prior"]) == (want["periods"], want["prior"])


@pytest.mark.parametrize(
    ("lg", "name", "reg", "kind", "neutral", "periods", "source"),
    [
        ("mbb", "summary_mbb.json", 2, "half", True, 2, "neutral"),
        ("mbb", "mbb_401830342.json.gz", 2, "half", True, 4, "neutral"),
        ("mbb", "mbb_401856600.json.gz", 2, "half", True, 2, "spread"),
        ("mbb", "mbb_401812161.json.gz", 2, "half", False, 2, "spread"),
        ("wbb", "summary_wbb.json", 4, "quarter", True, 4, "neutral"),
        ("wbb", "wbb_293170052.json.gz", 2, "half", False, 2, "hfa"),
        ("wbb", "wbb_273130043.json.gz", 2, "half", False, 2, "hfa"),
        ("wnba", "wnba_250603003.json.gz", 2, "half", False, 3, "hfa"),
        ("wnba", "summary_wnba.json", 4, "quarter", False, 5, "hfa"),
    ],
)
def test_game_page_frames_for_every_league(
    monkeypatch: pytest.MonkeyPatch, lg: str, name: str, reg: int, kind: str, neutral: bool, periods: int, source: str
) -> None:
    final = _final(monkeypatch, lg, name)
    fr = gp.game_page_frames(final, league=lg)
    assert (fr["league"], fr["regulation_periods"], fr["period_kind"], fr["neutral_site"], fr["periods"]) == (
        lg,
        reg,
        kind,
        neutral,
        periods,
    )
    pbp = fr["pbp"]
    assert fr["prior"]["source"] == source == pbp["prior_source"][0]
    assert pbp["home_win_prob"].null_count() == 0
    # overtime is every period past the game's own regulation
    assert pbp.filter(pl.col("wp_regime") == "ot").height == pbp.filter(pl.col("period_number") > reg).height
    # the final play settles on the official result
    comp = final["header"]["competitions"][0]
    score = {c["homeAway"]: int(c["score"]) for c in comp["competitors"]}
    assert pbp["home_win_prob"][-1] == (1.0 if score["home"] > score["away"] else 0.0)
    # no free throw is placed on the court
    shots = pbp.filter(pl.col("x_legacy").is_not_null())
    assert not shots["type_text"].str.contains("(?i)free ?throw").any()
    assert fr["team_game_rates"].height == 2
    assert fr["team_game_rates"]["game_minutes"].to_list() == [40 + 5 * max(periods - reg, 0)] * 2
    assert fr["player_game_rates"].height == fr["player_box"].height


def test_game_page_tables_is_the_warehouse_path(monkeypatch: pytest.MonkeyPatch) -> None:
    final, pbp, tb, pb = _frames(monkeypatch, "wnba", "wnba_250603003.json.gz")
    live = gp.game_page_frames(final, league="wnba")
    # the warehouse has no ESPN header: regulation comes from the frame's season (2005 -> halves)
    wh = gp.game_page_tables(pbp, tb, pb, league="wnba")
    assert (wh["regulation_periods"], wh["period_kind"], wh["periods"]) == (2, "half", 3)
    assert wh["pbp"].select(PBP_COLS).equals(live["pbp"].select(PBP_COLS))
    assert wh["team_game_rates"].equals(live["team_game_rates"])


def test_game_page_tables_before_tip_off(monkeypatch: pytest.MonkeyPatch) -> None:
    _, _, tb, pb = _frames(monkeypatch, "mbb", "summary_mbb.json")
    out = gp.game_page_tables(pl.DataFrame(), tb.clear(), pb.clear(), league="mbb", neutral_site=True)
    assert (out["periods"], out["regulation_periods"], out["period_kind"]) == (2, 2, "half")
    assert out["prior"]["source"] == "neutral" and out["prior"]["home_win_prob"] == pytest.approx(0.5)
    assert out["team_game_rates"].height == 0 and "efg_pct" in out["team_game_rates"].columns


def test_pre_tip_off_regulation_reads_the_team_box_season(monkeypatch: pytest.MonkeyPatch) -> None:
    # no plays yet, but the real WBB 2010 team box carries its season: halves, not the season-less quarters
    _, _, tb, pb = _frames(monkeypatch, "wbb", "wbb_293170052.json.gz")
    out = gp.game_page_tables(pl.DataFrame(), tb, pb, league="wbb")
    assert (out["periods"], out["regulation_periods"], out["period_kind"]) == (2, 2, "half")


def test_score_win_prob_reads_a_warehouse_season_with_a_nan_first_cell(monkeypatch: pytest.MonkeyPatch) -> None:
    # a float season column whose first cell is NaN (a warehouse frame) still reads 2005 (halves)
    _, pbp, _, _ = _frames(monkeypatch, "wnba", "wnba_250603003.json.gz")
    seasons = [float("nan")] + [2005.0] * (pbp.height - 1)
    nan_first = pbp.with_columns(pl.Series("season", seasons))
    got = gp.score_win_prob(nan_first, league="wnba")
    assert got["home_win_prob"].equals(gp.score_win_prob(pbp, league="wnba", regulation_periods=2)["home_win_prob"])


def test_neutral_flag_is_a_python_bool() -> None:
    import numpy as np

    assert gp._is_neutral(np.bool_(True)) is True and gp._is_neutral(np.bool_(False)) is False


@pytest.mark.parametrize("cell", [None, float("nan"), "pd.NA"])
def test_a_null_warehouse_neutral_site_is_not_neutral(monkeypatch: pytest.MonkeyPatch, cell: object) -> None:
    # a schedule's neutral_site can arrive as None, NaN or pandas' nullable-boolean NA (which raises in `if`)
    import pandas as pd

    na = pd.NA if cell == "pd.NA" else cell
    final, pbp, tb, pb = _frames(monkeypatch, "mbb", "summary_mbb.json")
    assert final["header"]["competitions"][0]["neutralSite"] is True
    out = gp.game_page_tables(pbp, tb, pb, league="mbb", neutral_site=na)  # type: ignore[arg-type]
    assert out["prior"]["source"] == "hfa" == out["pbp"]["prior_source"][0]
    assert gp.home_prior(None, None, league="mbb", neutral_site=na)[1] == "hfa"  # type: ignore[arg-type]


def test_league_pbp_functions_give_the_flask_route_its_fetch_and_parse(monkeypatch: pytest.MonkeyPatch) -> None:
    for lg in gp.GAME_PAGE_LEAGUES:
        fetch, parse = gp.league_pbp_functions(lg)
        assert (fetch.__name__, parse.__name__) == (f"espn_{lg}_pbp", f"helper_{lg}_pbp")
    with pytest.raises(ValueError, match="nhl"):
        gp.league_pbp_functions("nhl")
    # the Flask flow: one raw fetch, then the parse, then the frames
    raw = _raw("wbb_293170052.json.gz")
    mod = importlib.import_module("sportsdataverse.wbb.wbb_pbp")
    monkeypatch.setattr(mod, "download", lambda url, **kwargs: _Resp(copy.deepcopy(raw)))
    fetch, parse = gp.league_pbp_functions("wbb")
    summary = fetch(293170052, raw=True)
    frames = gp.game_page_frames(parse(293170052, summary), league="wbb")
    assert frames["regulation_periods"] == 2 and frames["team_game_rates"]["total_turnovers"].sort().to_list() == [
        23,
        28,
    ]


def test_contradicting_league_and_league_id_raise(monkeypatch: pytest.MonkeyPatch) -> None:
    final = _final(monkeypatch, "mbb", "summary_mbb.json")
    with pytest.raises(ValueError, match="contradicts"):
        gp.game_page_frames(final, league="mbb", league_id="10")


@pytest.mark.parametrize("fmt", ["4 quarters", {"regulation": "4"}, None])
def test_a_format_that_is_not_an_object_falls_back_to_the_season_era(
    monkeypatch: pytest.MonkeyPatch, fmt: object
) -> None:
    final = _final(monkeypatch, "wnba", "summary_wnba.json")
    final["format"] = fmt  # game-on-paper-app's route test sends a string here; it used to raise AttributeError
    assert gp.game_page_frames(final, league="wnba")["regulation_periods"] == 4  # WNBA 2024: quarters
