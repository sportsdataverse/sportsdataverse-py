"""Offline tests for the Basketball-Reference scrapers (:mod:`sportsdataverse.nba.bref`).

No network: every test injects a fake transport by monkeypatching ``download``.
The fixtures below reproduce the two Sports-Reference page quirks the parser
exists to handle -- a first visible table, a second table wrapped in
``<!-- ... -->``, and cells carrying ``data-stat`` attributes under a multi-row
over-header whose rendered names are useless.
"""

from __future__ import annotations

from typing import Any, Dict, List

import polars as pl
import pytest

from sportsdataverse.nba import bref

# A page shaped like a real Basketball-Reference season page:
#   * table 1 is visible, with a two-row header (over-header + real header) whose
#     rendered labels ("Per Game", "Unnamed", "Shooting") are useless,
#   * table 2 is hidden inside an HTML comment,
#   * every body cell carries the canonical `data-stat` key,
#   * one mid-table `class="thead"` header-repeat row, and one all-blank spacer row.
_PAGE = """
<html><body>
<table id="per_game_stats">
  <thead>
    <tr class="over_header"><th colspan="2">Unnamed</th><th colspan="2">Per Game</th></tr>
    <tr><th>Rk</th><th>Player</th><th>PTS</th><th>TS%</th></tr>
  </thead>
  <tbody>
    <tr>
      <th data-stat="ranker">1</th>
      <td data-stat="name_display">Nikola Jokic</td>
      <td data-stat="pts_per_g">26.4</td>
      <td data-stat="ts_pct">.646</td>
    </tr>
    <tr class="thead"><th data-stat="ranker">Rk</th><td data-stat="name_display">Player</td></tr>
    <tr>
      <th data-stat="ranker">2</th>
      <td data-stat="name_display">Luka Doncic</td>
      <td data-stat="pts_per_g">33.9</td>
      <td data-stat="ts_pct">.617</td>
    </tr>
    <tr><th data-stat="ranker"></th><td data-stat="name_display"></td>
        <td data-stat="pts_per_g"></td><td data-stat="ts_pct"></td></tr>
  </tbody>
</table>
<!--
<table id="advanced-team">
  <thead>
    <tr class="over_header"><th colspan="3">Advanced</th></tr>
    <tr><th>Rk</th><th>Team</th><th>ORtg</th></tr>
  </thead>
  <tbody>
    <tr><th data-stat="ranker">1</th>
        <td data-stat="team_name">Boston Celtics</td>
        <td data-stat="off_rtg">122.2</td></tr>
    <tr><th data-stat="ranker">2</th>
        <td data-stat="team_name">League Average</td>
        <td data-stat="off_rtg">115.3</td></tr>
  </tbody>
</table>
-->
</body></html>
"""

_STANDINGS_PAGE = """
<html><body>
<table id="confs_standings_E"><tbody>
  <tr><th data-stat="team_name">Boston Celtics*</th>
      <td data-stat="wins">64</td><td data-stat="losses">18</td>
      <td data-stat="win_loss_pct">.780</td></tr>
</tbody></table>
<!--
<table id="confs_standings_W"><tbody>
  <tr><th data-stat="team_name">Denver Nuggets*</th>
      <td data-stat="wins">57</td><td data-stat="losses">25</td>
      <td data-stat="win_loss_pct">.695</td></tr>
  <tr><th data-stat="team_name">Utah Jazz</th>
      <td data-stat="wins">31</td><td data-stat="losses">51</td>
      <td data-stat="win_loss_pct">.378</td></tr>
</tbody></table>
-->
</body></html>
"""

_BIOS_PAGE = """
<html><body>
<!--
<table id="players"><tbody>
  <tr><th data-stat="player"><a href="/players/j/jamesle01.html">LeBron James</a></th>
      <td data-stat="year_min">2004</td><td data-stat="year_max">2025</td>
      <td data-stat="pos">F-G</td><td data-stat="DUMMY"></td></tr>
  <tr><th data-stat="player"><a href="/players/j/jokicni01.html">Nikola Jokic</a></th>
      <td data-stat="year_min">2016</td><td data-stat="year_max">2025</td>
      <td data-stat="pos">C</td><td data-stat="DUMMY"></td></tr>
</tbody></table>
-->
</body></html>
"""


class _FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text
        self.status_code = 200


@pytest.fixture(autouse=True)
def _no_throttle(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the 3s Basketball-Reference pacing out of the unit tests."""
    monkeypatch.setenv("SDV_PY_BREF_RATE_DELAY", "0")


class _Transport:
    """Records every ``download`` call and serves a page per URL substring."""

    def __init__(self) -> None:
        self.recorded: List[Dict[str, Any]] = []
        self.pages: Dict[str, str] = {}

    def __call__(self, url: str = "", **kwargs: Any) -> _FakeResponse:
        self.recorded.append({"url": url, **kwargs})
        for needle, body in self.pages.items():
            if needle in url:
                return _FakeResponse(body)
        return _FakeResponse(_PAGE)

    def __getitem__(self, index: int) -> Dict[str, Any]:
        return self.recorded[index]

    def urls(self) -> List[str]:
        return [call["url"] for call in self.recorded]

    def serve(self, needle: str, body: str) -> None:
        self.pages[needle] = body


@pytest.fixture()
def calls(monkeypatch: pytest.MonkeyPatch) -> _Transport:
    """Fake transport injected over ``download``; default page is ``_PAGE``."""
    transport = _Transport()
    monkeypatch.setattr(bref, "download", transport)
    return transport


# ---------------------------------------------------------------------------
# The two quirks
# ---------------------------------------------------------------------------


def test_comment_stripping_reaches_the_hidden_table() -> None:
    """The second table is inside `<!-- -->`; without stripping it is invisible."""
    hidden = bref._bref_table(_PAGE, "advanced-team")
    assert hidden.height == 2
    assert hidden["team_name"].to_list() == ["Boston Celtics", "League Average"]

    # And a plain (non-stripping) reader would not find it at all.
    from bs4 import BeautifulSoup

    assert BeautifulSoup(_PAGE, "lxml").find("table", id="advanced-team") is None


def test_data_stat_names_beat_the_rendered_over_header() -> None:
    """Columns come from `data-stat`, never from the two-row rendered header."""
    df = bref._bref_table(_PAGE)
    assert df.columns == ["ranker", "name_display", "pts_per_g", "ts_pct"]
    # The rendered header would have produced these instead:
    for mangled in ("rk", "player", "pts", "ts", "unnamed", "per_game"):
        assert mangled not in df.columns


def test_mid_table_header_repeat_rows_are_dropped() -> None:
    """Sports Reference repeats the header as `class="thead"` inside `<tbody>`."""
    df = bref._bref_table(_PAGE)
    assert "Player" not in df["name_display"].to_list()
    assert df.height == 3  # 2 players + 1 all-blank spacer row (dropped in _finish)


def test_empty_and_missing_tables_return_empty_frames_not_exceptions() -> None:
    assert bref._bref_table("").shape == (0, 0)
    assert bref._bref_table("<html><body><p>no tables here</p></body></html>").shape == (0, 0)
    assert bref._bref_table(_PAGE, "does_not_exist").shape == (0, 0)
    assert bref._bref_table("<table id='t'><tbody></tbody></table>", "t").shape == (0, 0)


def test_finish_drops_blank_rows_and_dummy_columns_and_casts_numerics() -> None:
    df = bref._finish(bref._bref_table(_PAGE))
    assert df.height == 2  # the all-blank spacer row is gone
    assert df.schema["pts_per_g"] == pl.Float64
    assert df.schema["ranker"] == pl.Float64
    assert df.schema["name_display"] == pl.Utf8  # names stay text


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------


def test_players_stats_renames_and_echoes(calls: _Transport) -> None:
    df = bref.bref_players_stats(season=2024)
    assert "NBA_2024_per_game.html" in calls[0]["url"]
    assert "player" in df.columns and "name_display" not in df.columns
    assert df["season"].to_list() == [2024, 2024]
    assert df["league"].to_list() == ["nba", "nba"]
    assert df["table"].to_list() == ["per_game", "per_game"]


def test_league_param_switches_the_url_family(calls: _Transport) -> None:
    bref.bref_players_stats(season=2024, league="wnba")
    bref.bref_teams_stats(season=2024, league="wnba")
    bref.bref_teams_stats(season=2024, league="nba")
    urls = calls.urls()
    assert urls[0].endswith("/wnba/years/2024_per_game.html")
    assert urls[1].endswith("/wnba/years/2024.html")
    assert urls[2].endswith("/leagues/NBA_2024.html")


def test_proxy_and_kwargs_are_forwarded(calls: _Transport) -> None:
    bref.bref_injuries(proxy={"https": "http://p:8080"}, timeout=7)
    assert calls[0]["proxy"] == {"https": "http://p:8080"}
    assert calls[0]["timeout"] == 7
    assert "User-Agent" in calls[0]["headers"]


def test_teams_stats_reads_the_comment_hidden_table(calls: _Transport) -> None:
    df = bref.bref_teams_stats(season=2024, table="advanced")
    assert df["team"].to_list() == ["Boston Celtics", "League Average"]
    # wehoop drops the footer row on the WNBA page; the NBA wrapper does not.
    wnba = bref.bref_teams_stats(season=2024, table="advanced", league="wnba")
    assert wnba["team"].to_list() == ["Boston Celtics"]


def test_standings_stacks_conferences_and_flags_playoff_teams(calls: _Transport) -> None:
    calls.serve("standings", _STANDINGS_PAGE)
    df = bref.bref_standings(season=2024)
    assert df["conference"].to_list() == ["E", "W", "W"]
    assert df["team"].to_list() == ["Boston Celtics", "Denver Nuggets", "Utah Jazz"]
    assert df["playoffs"].to_list() == [True, True, False]
    assert df.filter(pl.col("playoffs") == True).height == 2  # noqa: E712 - polars mask


def test_awards_stacks_every_award_with_an_award_column(calls: _Transport) -> None:
    page = """
    <html><body>
    <table id="mvp"><tbody>
      <tr><th data-stat="rank">1</th><td data-stat="name_display">Nikola Jokic</td>
          <td data-stat="team_id">DEN</td><td data-stat="award_share">.674</td></tr>
    </tbody></table>
    <!--
    <table id="roy"><tbody>
      <tr><th data-stat="rank">1</th><td data-stat="name_display">Victor Wembanyama</td>
          <td data-stat="team_id">SAS</td><td data-stat="award_share">.989</td></tr>
    </tbody></table>
    -->
    </body></html>
    """
    calls.serve("awards", page)
    df = bref.bref_awards(season=2024)
    assert df["award"].to_list() == ["mvp", "roy"]
    assert df["player"].to_list() == ["Nikola Jokic", "Victor Wembanyama"]
    assert df["team"].to_list() == ["DEN", "SAS"]
    assert set(df.columns) >= {"rank", "player", "team", "award_share", "award", "season"}


def test_player_bios_pulls_the_id_slug_off_the_row_link(calls: _Transport) -> None:
    calls.serve("/players/j/", _BIOS_PAGE)
    df = bref.bref_player_bios(letter="J")
    assert calls[0]["url"].endswith("/players/j/")
    assert df.columns[:2] == ["player", "player_id"]
    assert df["player_id"].to_list() == ["jamesle01", "jokicni01"]
    assert df["letter"].to_list() == ["j", "j"]
    assert "DUMMY" not in df.columns  # blank spacer column dropped


def test_player_game_log_drops_dateless_rows(calls: _Transport) -> None:
    page = """
    <html><body><!--
    <table id="player_game_log_reg"><tbody>
      <tr><th data-stat="ranker">1</th><td data-stat="date">2023-10-24</td>
          <td data-stat="team_name_abbr">DEN</td><td data-stat="game_location">@</td>
          <td data-stat="opp_name_abbr">LAL</td><td data-stat="game_result">W (+12)</td>
          <td data-stat="pts">29</td></tr>
      <tr><th data-stat="ranker"></th><td data-stat="date"></td>
          <td data-stat="team_name_abbr">DEN</td><td data-stat="game_location"></td>
          <td data-stat="opp_name_abbr"></td><td data-stat="game_result"></td>
          <td data-stat="pts"></td></tr>
    </tbody></table>
    --></body></html>
    """
    calls.serve("gamelog", page)
    df = bref.bref_player_game_log(player_id="JokicNi01", season=2024)
    assert "/players/j/jokicni01/gamelog/2024/" in calls[0]["url"]
    assert df.height == 1
    assert df["team"].to_list() == ["DEN"]
    assert df["opp"].to_list() == ["LAL"]
    assert df["location"].to_list() == ["@"]
    assert df["result"].to_list() == ["W (+12)"]
    assert df["player_id"].to_list() == ["jokicni01"]


def test_team_roster_upcases_the_team_code(calls: _Transport) -> None:
    bref.bref_team_roster(team="bos", season=2024)
    assert calls[0]["url"].endswith("/teams/BOS/2024.html")


def test_draft_and_injuries_hit_the_documented_paths(calls: _Transport) -> None:
    bref.bref_draft(season=2003)
    bref.bref_injuries()
    assert calls[0]["url"].endswith("/draft/NBA_2003.html")
    assert calls[1]["url"].endswith("/friv/injuries.fcgi")


def test_return_as_pandas(calls: _Transport) -> None:
    import pandas as pd

    df = bref.bref_players_stats(season=2024, return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)


def test_page_with_no_tables_returns_empty_frames_not_exceptions(calls: _Transport) -> None:
    calls.serve("basketball-reference.com", "<html><body><p>404</p></body></html>")
    for frame in (
        bref.bref_players_stats(season=1800),
        bref.bref_teams_stats(season=1800),
        bref.bref_standings(season=1800),
        bref.bref_awards(season=1800),
        bref.bref_draft(season=1800),
        bref.bref_team_roster(team="ZZZ", season=1800),
        bref.bref_player_game_log(player_id="nobodyxx01", season=1800),
        bref.bref_player_bios(letter="q"),
        bref.bref_injuries(),
    ):
        assert isinstance(frame, pl.DataFrame)
        assert frame.height == 0


def test_a_404_is_swallowed_into_an_empty_frame(monkeypatch: pytest.MonkeyPatch) -> None:
    from sportsdataverse.errors import NoDataError

    def boom(**kwargs: Any) -> None:
        raise NoDataError("404")

    monkeypatch.setattr(bref, "download", boom)
    assert bref.bref_injuries().height == 0


# ---------------------------------------------------------------------------
# Argument validation + rate limiting
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("fn", "kwargs"),
    [
        (bref.bref_players_stats, {"league": "ncaa"}),
        (bref.bref_players_stats, {"table": "nonsense"}),
        (bref.bref_players_stats, {"table": "per_poss", "league": "wnba"}),
        (bref.bref_teams_stats, {"league": "ncaa"}),
        (bref.bref_teams_stats, {"table": "nonsense"}),
        (bref.bref_teams_stats, {"table": "opponent", "league": "wnba"}),
        (bref.bref_standings, {"league": "ncaa"}),
        (bref.bref_player_bios, {"letter": "1"}),
    ],
)
def test_bad_arguments_raise_value_error(fn: Any, kwargs: Dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        fn(season=2024, **kwargs) if "letter" not in kwargs else fn(**kwargs)


def test_rate_delay_is_env_tunable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SDV_PY_BREF_RATE_DELAY", raising=False)
    assert bref._rate_delay() == 3.0
    monkeypatch.setenv("SDV_PY_BREF_RATE_DELAY", "0.25")
    assert bref._rate_delay() == 0.25
    monkeypatch.setenv("SDV_PY_BREF_RATE_DELAY", "not-a-number")
    assert bref._rate_delay() == 3.0
    monkeypatch.setenv("SDV_PY_BREF_RATE_DELAY", "-1")
    assert bref._rate_delay() == 3.0


def test_throttle_sleeps_between_consecutive_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    slept: List[float] = []
    monkeypatch.setenv("SDV_PY_BREF_RATE_DELAY", "3")
    monkeypatch.setattr(bref.time, "sleep", lambda s: slept.append(s))
    monkeypatch.setattr(bref, "_LAST_REQUEST", bref.time.monotonic())
    bref._throttle()
    assert slept and 0 < slept[0] <= 3
