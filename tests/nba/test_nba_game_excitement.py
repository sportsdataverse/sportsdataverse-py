"""Game excitement index (BB-D28) on real ESPN captures, all four leagues.

Basketball fixtures: tests/fixtures/espn/summary_*.json and tests/fixtures/espn/basketball_pbp/*.json.gz
(provenance in tests/fixtures/espn/basketball_pbp/README.md). The football WP series for the parity
test is the ``winprobability`` array of tests/fixtures/espn/summary_{nfl,cfb}.json.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nba import nba_game_page as gp
from tests.nba.test_nba_game_page_leagues import _frames

ESPN = Path(__file__).resolve().parents[1] / "fixtures" / "espn"
#: Game on Paper's football constant (astro/src/resources/python.ts calculateGEI, python/excitement.py).
FOOTBALL_SCALE = 179.01777401608126

GAMES = [
    ("nba", "summary_nba.json"),
    ("nba", "nba_401766458.json.gz"),
    ("nba", "nba_400829116.json.gz"),
    ("wnba", "summary_wnba.json"),
    ("wnba", "wnba_250603003.json.gz"),
    ("mbb", "summary_mbb.json"),
    ("mbb", "mbb_401830342.json.gz"),
    ("wbb", "summary_wbb.json"),
    ("wbb", "wbb_401364640.json.gz"),
]
MBB_2026 = ["mbb_401812161.json.gz", "mbb_401828345.json.gz", "mbb_401829457.json.gz", "mbb_401830342.json.gz"]


def _scored(monkeypatch: pytest.MonkeyPatch, lg: str, name: str) -> pl.DataFrame:
    _, pbp, _, _ = _frames(monkeypatch, lg, name)
    return gp.score_win_prob(pbp, league=lg)


def _football_gei(before: list[float], final: float) -> float:
    """calculateGEI restated: each play's home WP before it, the next play's, the last play to ``final``."""
    nxt = [*before[1:], final]
    return FOOTBALL_SCALE / len(before) * sum(abs(n - b) for b, n in zip(before, nxt))


def _restated(scored: pl.DataFrame, scale: float, completed: bool) -> float:
    """The basketball GEI in plain Python: home WP after each play, the prior before the first."""
    wp = scored["home_win_prob"].to_list()
    if completed:
        h, a = (scored[c].fill_null(strategy="forward").fill_null(0)[-1] for c in ("home_score", "away_score"))
        wp[-1] = 1.0 if h > a else 0.0 if h < a else 0.5
    prev = [scored["pregame_home_prob"][0], *wp[:-1]]
    return scale / len(wp) * sum(abs(w - p) for w, p in zip(wp, prev))


@pytest.mark.parametrize("sport", ["nfl", "cfb"])
def test_gei_is_footballs_formula_on_a_football_wp_series(monkeypatch: pytest.MonkeyPatch, sport: str) -> None:
    raw = json.loads((ESPN / f"summary_{sport}.json").read_text(encoding="utf-8"))
    before = [p["homeWinPercentage"] for p in raw["winprobability"]]
    comp = raw["header"]["competitions"][0]["competitors"]
    score = {c["homeAway"]: int(c["score"]) for c in comp}
    final = 1.0 if score["home"] > score["away"] else 0.0
    # the basketball framing: WP after each play is the next play's WP before; the first play's
    # before is the prior. The last row's own WP is replaced by the result in a completed game.
    frame = pl.DataFrame(
        {
            "home_win_prob": [*before[1:], 0.123],
            "pregame_home_prob": before[0],
            "home_score": score["home"],
            "away_score": score["away"],
        }
    )
    monkeypatch.setitem(gp.GEI_SCALE, "nba", FOOTBALL_SCALE)
    got = gp.game_excitement(frame, completed=True)
    assert got["gei"] == pytest.approx(_football_gei(before, final), rel=1e-12)
    # live: football sums the newest play's after-before, which is the last row's own WP here
    live = gp.game_excitement(frame, completed=False)["gei"]
    assert live == pytest.approx(_football_gei(before, 0.123), rel=1e-12)
    assert live != pytest.approx(got["gei"])


@pytest.mark.parametrize(("lg", "name"), GAMES)
def test_gei_on_real_games(monkeypatch: pytest.MonkeyPatch, lg: str, name: str) -> None:
    scored = _scored(monkeypatch, lg, name)
    got = gp.game_excitement(scored, league=lg)
    assert got["completed"] is True  # every capture is a final
    assert got["gei"] == pytest.approx(_restated(scored, gp.GEI_SCALE[lg], completed=True), rel=1e-12)
    # the biggest swing is score_win_prob's own largest play-to-play change
    assert got["max_swing_pts"] == pytest.approx(100 * scored["wp_delta"].abs().max())
    assert 0 < got["gei"] < 20  # a total WP movement on football's scale, not hundreds


def test_final_play_goes_to_the_result(monkeypatch: pytest.MonkeyPatch) -> None:
    # drop the settled End Game / End Period rows: the last real play's WP is the model's, not 1.0
    scored = _scored(monkeypatch, "mbb", "summary_mbb.json")
    open_end = scored.filter(~pl.col("type_text").is_in(["End Game", "End Period"]))
    assert 0.5 < open_end["home_win_prob"][-1] < 1.0  # home won 75-60; the model is not yet certain
    done = gp.game_excitement(open_end, league="mbb", completed=True)["gei"]
    live = gp.game_excitement(open_end, league="mbb", completed=False)["gei"]
    assert done == pytest.approx(_restated(open_end, gp.GEI_SCALE["mbb"], completed=True), rel=1e-12)
    assert live == pytest.approx(_restated(open_end, gp.GEI_SCALE["mbb"], completed=False), rel=1e-12)
    assert done > live
    # no type_text: nothing says the game ended, so it is live unless told
    assert gp.game_excitement(open_end.drop("type_text"), league="mbb")["completed"] is False


def test_a_tied_end_of_regulation_is_not_the_end(monkeypatch: pytest.MonkeyPatch) -> None:
    # 2005 NYL @ TUL: End Period 54-54 after the second half (row 400), then one overtime
    scored = _scored(monkeypatch, "wnba", "wnba_250603003.json.gz")
    tied = scored.head(401)
    assert tied.tail(1).select("type_text", "home_score", "away_score").row(0) == ("End Period", 54, 54)
    assert gp.game_excitement(tied, league="wnba")["completed"] is False
    assert gp.game_excitement(scored, league="wnba")["completed"] is True
    # declared final while tied, the last play goes to 0.5 (football's tie)
    as_tie = gp.game_excitement(tied, league="wnba", completed=True)["gei"]
    assert as_tie == pytest.approx(_restated(tied, gp.GEI_SCALE["wnba"], completed=True), rel=1e-12)
    # declared final 18-28 after the first half: the swing to the result is the biggest one
    early = scored.head(177)
    assert early.tail(1).select("home_score", "away_score").row(0) == (18, 28)
    swing = gp.game_excitement(early, league="wnba", completed=True)["max_swing_pts"]
    assert swing == pytest.approx(100 * early["home_win_prob"][-2])
    assert swing > 100 * early["wp_delta"].abs().max()


def test_the_scale_is_per_league(monkeypatch: pytest.MonkeyPatch) -> None:
    scored = _scored(monkeypatch, "wbb", "summary_wbb.json")
    by_league = {lg: gp.game_excitement(scored, league=lg)["gei"] for lg in gp.GAME_PAGE_LEAGUES}
    for lg, gei in by_league.items():
        assert gei == pytest.approx(by_league["nba"] * gp.GEI_SCALE[lg] / gp.GEI_SCALE["nba"])
    assert set(gp.GEI_SCALE) == set(gp.GAME_PAGE_LEAGUES)
    assert all(300 < k < 600 for k in gp.GEI_SCALE.values())  # several hundred WP points a game


def _football_lead_changes(scored: pl.DataFrame) -> int:
    """excitement._lead_changes restated: over scoring plays, a tie is not a lead."""
    leader, changes = 0, 0
    for play in scored.iter_rows(named=True):
        if not play["scoring_play"]:
            continue
        margin = (play["home_score"] or 0) - (play["away_score"] or 0)
        now = (margin > 0) - (margin < 0)
        if now:
            changes += leader not in (0, now)
            leader = now
    return changes


@pytest.mark.parametrize(("lg", "name"), GAMES)
def test_lead_changes_are_footballs(monkeypatch: pytest.MonkeyPatch, lg: str, name: str) -> None:
    scored = _scored(monkeypatch, lg, name)
    assert gp.game_excitement(scored, league=lg)["lead_changes"] == _football_lead_changes(scored)


def test_lead_changes_by_hand(monkeypatch: pytest.MonkeyPatch) -> None:
    # 2021-22 WBB 401364640: the home side never trailed (tied on a scoring play, then led to the end)
    wire = _scored(monkeypatch, "wbb", "wbb_401364640.json.gz")
    margins = wire.filter(pl.col("scoring_play") == True).select(pl.col("home_score") - pl.col("away_score"))  # noqa: E712
    assert margins.to_series().min() == 0 and margins.to_series().max() > 0
    assert gp.game_excitement(wire, league="wbb")["lead_changes"] == 0
    # home ahead, tied, away ahead (one change), tied, home ahead again (two)
    frame = pl.DataFrame(
        {
            "home_win_prob": [0.5] * 5,
            "pregame_home_prob": 0.5,
            "home_score": [2, 2, 2, 4, 6],
            "away_score": [0, 2, 4, 4, 4],
        }
    )
    assert gp.game_excitement(frame, completed=False)["lead_changes"] == 2


def _mbb_season(monkeypatch: pytest.MonkeyPatch, names: list[str]) -> pl.DataFrame:
    return pl.concat([_frames(monkeypatch, "mbb", n)[1] for n in names], how="diagonal_relaxed")


def test_season_ranks(monkeypatch: pytest.MonkeyPatch) -> None:
    pbp = _mbb_season(monkeypatch, MBB_2026)
    truncated = _frames(monkeypatch, "mbb", "mbb_401856600.json.gz")[1].head(200)  # pbp stops mid-game
    out = gp.season_game_excitement(pl.concat([pbp, truncated], how="diagonal_relaxed"), league="mbb")
    assert out.schema == pl.Schema(gp._EXCITEMENT_SCHEMA)
    assert out.height == 5 and out["season"].unique().to_list() == [2026]
    done = out.filter(pl.col("completed"))
    assert done.height == 4
    # each row is game_excitement on score_win_prob of that game
    for gid in done["game_id"]:
        one = pbp.filter(pl.col("game_id") == gid)
        assert done.filter(pl.col("game_id") == gid)["gei"][0] == pytest.approx(
            gp.game_excitement(gp.score_win_prob(one, league="mbb"), league="mbb")["gei"]
        )
    ranked = done.sort("gei", descending=True)
    assert ranked["gei_rank"].to_list() == [1, 2, 3, 4]
    assert ranked["gei_pct"].to_list() == [1.0, 0.75, 0.5, 0.25]
    assert ranked["gei_n"].to_list() == [4] * 4
    # the 2OT game is the season's most exciting of these
    assert ranked["game_id"][0] == 401830342
    stub = out.filter(~pl.col("completed"))
    assert stub["game_id"].to_list() == [401856600]
    assert stub["gei"][0] is not None
    assert stub.select("gei_rank", "gei_pct", "gei_n").row(0) == (None, None, None)


def test_season_ranks_are_per_season(monkeypatch: pytest.MonkeyPatch) -> None:
    # two 2009 WBB games and one 2010: each season ranks its own games
    pbp = pl.concat(
        [
            _frames(monkeypatch, "wbb", n)[1]
            for n in ("wbb_283430164.json.gz", "wbb_290232368.json.gz", "wbb_293170052.json.gz")
        ],
        how="diagonal_relaxed",
    )
    out = gp.season_game_excitement(pbp, league="wbb")
    assert out.filter(pl.col("season") == 2009).sort("gei_rank")["gei_n"].to_list() == [2, 2]
    assert out.filter(pl.col("season") == 2009)["gei_rank"].sort().to_list() == [1, 2]
    assert out.filter(pl.col("season") == 2010).select("gei_rank", "gei_pct", "gei_n").row(0) == (1, 1.0, 1)


def test_season_schedule_sets_the_neutral_site(monkeypatch: pytest.MonkeyPatch) -> None:
    # 401830342 is neutral (UTU @ MVSU); the schedule's flag reaches the prior
    _, one, _, _ = _frames(monkeypatch, "mbb", "mbb_401830342.json.gz")
    sched = pl.DataFrame({"game_id": [401830342], "neutral_site": [True]})
    with_sched = gp.season_game_excitement(one, league="mbb", schedule=sched)["gei"][0]
    without = gp.season_game_excitement(one, league="mbb")["gei"][0]
    want = gp.game_excitement(gp.score_win_prob(one, league="mbb", neutral_site=True), league="mbb")["gei"]
    assert with_sched == pytest.approx(want)
    assert with_sched != pytest.approx(without)


def test_empty_and_bad_input(monkeypatch: pytest.MonkeyPatch) -> None:
    _, one, _, _ = _frames(monkeypatch, "mbb", "mbb_401812161.json.gz")
    empty = gp.season_game_excitement(one.head(0), league="mbb")
    assert empty.height == 0 and empty.schema == pl.Schema(gp._EXCITEMENT_SCHEMA)
    assert gp.game_excitement(gp.score_win_prob(one.head(0), league="mbb"), league="mbb") == {
        "gei": None,
        "lead_changes": 0,
        "max_swing_pts": None,
        "completed": False,
    }
    two = _mbb_season(monkeypatch, MBB_2026[:2])
    with pytest.raises(ValueError, match="one game"):
        gp.game_excitement(two.with_columns(home_win_prob=0.5, pregame_home_prob=0.5), league="mbb")
    with pytest.raises(pl.exceptions.ColumnNotFoundError, match="home_win_prob"):
        gp.game_excitement(one, league="mbb")
    with pytest.raises(pl.exceptions.ColumnNotFoundError, match="season"):
        gp.season_game_excitement(one.drop("season"), league="mbb")


#: Real ``load_<league>_schedule`` rows (pro_exhibitions_schedule.parquet): every NBA 2002-26 and WNBA
#: 2006-25 All-Star / Rising Stars game, plus real games from the same calendar (finals, Cup finals,
#: play-ins, the Mexico City game, the 2025 WNBA capture week).
EXHIBITION_SCHEDULE = ESPN / "basketball_pbp" / "pro_exhibitions_schedule.parquet"
WNBA_2025 = ["wnba_401781604.json.gz", "wnba_401736255.json.gz", "wnba_401736256.json.gz"]


def test_exhibitions_on_the_real_pro_schedules() -> None:
    sched = pl.read_parquet(EXHIBITION_SCHEDULE)
    ids = set(gp._exhibition_ids(sched))
    by_lg = {lg: set(sched.filter(pl.col("league") == lg)["game_id"]) & ids for lg in ("nba", "wnba")}
    # 24 typed ALLSTAR, the 2013-17 and 2019 All-Star games typed STD, and the 2018-20 Rising Stars
    assert len(by_lg["nba"]) == 33 and len(by_lg["wnba"]) == 16
    # the STD-typed ones are caught by their headline
    assert {400436572, 400517994, 400606285, 400829482, 400935635, 401098037} <= by_lg["nba"]
    assert {401018033, 401098038, 401197814} <= by_lg["nba"]  # Rising Stars
    assert 401781604 in by_lg["wnba"]  # 2025 TEAM CLARK vs TEAM COLLIER
    # nothing real: every flagged game is played by non-franchise sides
    names = sched.filter(pl.col("game_id").is_in(ids)).select("home_display_name", "away_display_name")
    assert names.unpivot()["value"].str.contains(r"(?i)all-stars|^team |^east$|^west$|^usa$|^world$").all()
    assert not sched.filter(pl.col("type_abbreviation").is_in(["CC", "FINAL"]))["game_id"].is_in(ids).any()


def test_season_ranks_leave_out_the_all_star_game(monkeypatch: pytest.MonkeyPatch) -> None:
    pbp = pl.concat([_frames(monkeypatch, "wnba", n)[1] for n in WNBA_2025], how="diagonal_relaxed")
    sched = pl.read_parquet(EXHIBITION_SCHEDULE).filter(pl.col("league") == "wnba").drop("league")
    out = gp.season_game_excitement(pbp, league="wnba", schedule=sched)
    assert out["game_id"].sort().to_list() == [401736255, 401736256]
    assert out["gei_rank"].sort().to_list() == [1, 2] and out["gei_n"].to_list() == [2, 2]
    # without the schedule's markers the All-Star game is scored and ranked like any other
    bare = gp.season_game_excitement(pbp, league="wnba", schedule=sched.select("game_id", "neutral_site"))
    assert bare.height == 3 and bare["gei_n"].to_list() == [3, 3, 3]
    # college ignores the markers (its schedules have no exhibitions)
    _, one, _, _ = _frames(monkeypatch, "mbb", "mbb_401812161.json.gz")
    marked = pl.DataFrame({"game_id": [401812161], "type_abbreviation": ["ALLSTAR"]})
    assert gp.season_game_excitement(one, league="mbb", schedule=marked).height == 1
