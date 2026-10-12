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
    return getattr(mod, f"espn_{lg}_pbp")(game_id=int(raw["header"]["id"]))


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
    ],
)
def test_game_regulation_periods(league: str, season: int | None, fmt: float | None, want: int) -> None:
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
