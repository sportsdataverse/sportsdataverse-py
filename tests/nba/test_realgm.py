"""Offline tests for the RealGM scrapers (:mod:`sportsdataverse.nba.realgm`).

Every test injects a fake ``fetcher`` (or a fake ``playwright.sync_api`` module), so the
suite never launches a browser and never touches the network -- which is the point of the
injectable transport, RealGM being behind a Cloudflare JS challenge.
"""

from __future__ import annotations

import sys
from typing import Any, Dict, List, Optional

import polars as pl
import pytest

from sportsdataverse.nba import realgm

# --- fixtures: minimal HTML shaped like a RealGM page ------------------------------------

# A nav/filter table (RealGM puts one ahead of the real table) + the real data table.
PLAYERS_HTML = """
<html><body>
<table id="nav"><tr><th>Season</th></tr><tr><td>2025-26</td></tr></table>
<table id="players">
  <thead><tr><th>#</th><th>Player</th><th>Pos</th><th>Current Team</th><th>Pre-Draft Team</th></tr></thead>
  <tbody>
    <tr><td>1</td><td>Nikola Jokic</td><td>C</td><td>Denver</td><td>KK Mega Bemax (Serbia)</td></tr>
    <tr><td>2</td><td>Luka Doncic</td><td>G</td><td>Los Angeles</td><td>Real Madrid (Spain)</td></tr>
    <tr><td>3</td><td>Jayson Tatum</td><td>F</td><td>Boston</td><td>Duke</td></tr>
  </tbody>
</table>
</body></html>
"""

STANDINGS_HTML = """
<html><body>
<table id="filters"><tr><th>View</th></tr><tr><td>Conference</td></tr></table>
{east}
{west}
</body></html>
"""


def _conference_table(table_id: str, teams: List[str]) -> str:
    rows = "".join(
        f"<tr><td>{i + 1}</td><td>{team}</td><td>{50 - i}</td><td>{20 + i}</td><td>0.700</td></tr>"
        for i, team in enumerate(teams)
    )
    return (
        f'<table id="{table_id}"><thead><tr><th>#</th><th>Team</th><th>W</th><th>L</th>'
        f"<th>PCT</th></tr></thead><tbody>{rows}</tbody></table>"
    )


EAST_TEAMS = [f"East {i}" for i in range(1, 16)]
WEST_TEAMS = [f"West {i}" for i in range(1, 16)]

TEAMS_HTML = """
<html><body>
<table id="atlantic">
  <thead><tr><th>Atlantic Division</th><th>Atlantic Division</th></tr></thead>
  <tbody>
    <tr><td>logo</td><td>Boston Celtics</td></tr>
    <tr><td>logo</td><td>Brooklyn Nets</td></tr>
    <tr><td>logo</td><td>New York Knicks</td></tr>
  </tbody>
</table>
<table id="pacific">
  <thead><tr><th>Pacific Division</th><th>Pacific Division</th></tr></thead>
  <tbody>
    <tr><td>logo</td><td>Golden State Warriors</td></tr>
    <tr><td>logo</td><td>LA Clippers</td></tr>
    <tr><td>logo</td><td>Phoenix Suns</td></tr>
  </tbody>
</table>
</body></html>
"""

DRAFT_HTML = """
<html><body>
<table id="round1">
  <thead><tr><th>Pick</th><th>Player</th><th>Pos</th><th>Ht</th></tr></thead>
  <tbody>
    <tr><td>1</td><td>Pick One</td><td>G</td><td>6-3</td></tr>
    <tr><td>30</td><td>Pick Thirty</td><td>F</td><td>6-8</td></tr>
    <tr><td>31</td><td>Pick ThirtyOne</td><td>C</td><td>6-11</td></tr>
  </tbody>
</table>
<table id="undrafted">
  <thead><tr><th>Player</th><th>Pos</th><th>Ht</th></tr></thead>
  <tbody>
    <tr><td>Undrafted A</td><td>G</td><td>6-2</td></tr>
    <tr><td>Undrafted B</td><td>F</td><td>6-7</td></tr>
    <tr><td>Undrafted C</td><td>C</td><td>7-0</td></tr>
  </tbody>
</table>
</body></html>
"""

TRANSACTIONS_HTML = """
<html><body>
<div class="transByMonth">
  <div class="portal widget fullpage">
    <h3>Aug 26, 2026</h3>
    <ul><li>Denver signed C Nikola Jokic.</li><li>Boston waived G Some Guy.</li></ul>
  </div>
  <div class="portal widget fullpage">
    <h3>Aug 25, 2026</h3>
    <ul><li>Miami converted F Another Guy to a two-way contract.</li></ul>
  </div>
  <div class="portal widget fullpage">
    <h3>Not A Date</h3>
    <ul><li>ignored</li></ul>
  </div>
</div>
</body></html>
"""


def _fetcher(html: str, calls: Optional[List[str]] = None):
    """A fake transport returning fixed HTML and recording the paths it was asked for."""

    def fetch(path: str, proxy: Optional[str]) -> str:
        if calls is not None:
            calls.append(path)
        return html

    return fetch


@pytest.fixture(autouse=True)
def _no_browser_left_open():
    """Every test starts and ends with no cached browser session."""
    realgm.realgm_close_browser()
    yield
    realgm.realgm_close_browser()


# --- the injected transport is actually used ----------------------------------------------


def test_injected_fetcher_is_used_and_no_browser_is_launched(monkeypatch):
    def explode(path, proxy=None):
        raise AssertionError(f"the default browser fetch must not run (path={path})")

    monkeypatch.setattr(realgm, "_playwright_html", explode)
    calls: List[str] = []
    frame = realgm.realgm_players(fetcher=_fetcher(PLAYERS_HTML, calls))

    assert calls == ["/nba/players"]
    assert isinstance(frame, pl.DataFrame)
    assert realgm._SESSION == {}


def test_every_public_endpoint_routes_through_the_fetcher(monkeypatch):
    """All 17 ported endpoints hit the injected transport -- none reaches the browser."""

    def explode(path, proxy=None):
        raise AssertionError("the default browser fetch must not run")

    monkeypatch.setattr(realgm, "_playwright_html", explode)
    endpoints = [
        realgm.realgm_players,
        realgm.realgm_players_abroad,
        realgm.realgm_future_free_agents,
        realgm.realgm_coaches,
        realgm.realgm_gms,
        realgm.realgm_standings,
        realgm.realgm_teams,
        realgm.realgm_individual_seasons,
        realgm.realgm_individual_games,
        realgm.realgm_draft_prospects,
        realgm.realgm_early_entry,
        realgm.realgm_salary_cap,
        realgm.realgm_rookie_scale,
        realgm.realgm_transactions,
        realgm.realgm_draft,
        realgm.realgm_player_stats,
        realgm.realgm_team_stats,
    ]
    assert len(endpoints) == 17
    calls: List[str] = []
    fetch = _fetcher(PLAYERS_HTML, calls)
    for endpoint in endpoints:
        out = endpoint(fetcher=fetch)
        assert isinstance(out, pl.DataFrame)
    assert len(calls) == 17
    assert all(path.startswith("/nba/") for path in calls)


def test_season_endpoints_build_the_documented_paths():
    calls: List[str] = []
    fetch = _fetcher(PLAYERS_HTML, calls)
    realgm.realgm_player_stats(season=2025, stat_type="Advanced_Stats", fetcher=fetch)
    realgm.realgm_team_stats(season=2025, stat_type="Advanced_Stats", fetcher=fetch)
    realgm.realgm_team_stats(season=2025, stat_type="Averages", fetcher=fetch)
    realgm.realgm_draft(year=2020, fetcher=fetch)

    assert calls[0] == "/nba/stats/2025/Advanced_Stats/Qualified/points/All/desc/1/Regular_Season"
    # Advanced_Stats sorts on ortg; everything else on ppg (RealGM 404s on a bad sort key).
    assert calls[1] == "/nba/team-stats/2025/Advanced_Stats/Team_Totals/Regular_Season/ortg/desc"
    assert calls[2] == "/nba/team-stats/2025/Averages/Team_Totals/Regular_Season/ppg/desc"
    assert calls[3] == "/nba/draft/past-drafts/2020"


# --- missing Playwright ------------------------------------------------------------------


def test_missing_playwright_raises_a_clear_import_error(monkeypatch):
    monkeypatch.setitem(sys.modules, "playwright.sync_api", None)
    with pytest.raises(ImportError) as excinfo:
        realgm.realgm_players()
    message = str(excinfo.value)
    assert "playwright install chromium" in message
    assert "sportsdataverse[pff]" in message
    assert "fetcher=" in message


# --- table selection ---------------------------------------------------------------------


def test_players_picks_the_data_table_not_the_nav_table():
    frame = realgm.realgm_players(fetcher=_fetcher(PLAYERS_HTML))

    assert frame.height == 3
    assert "player" in frame.columns
    assert "season" not in frame.columns  # the nav table's only column
    assert frame.get_column("player").to_list()[0] == "Nikola Jokic"
    assert frame.get_column("pre_draft_team").to_list()[0] == "KK Mega Bemax (Serbia)"


def test_must_have_predicate_falls_back_to_the_tallest_table():
    """A header rename upstream must degrade to the tallest table, not to an empty frame."""
    renamed = PLAYERS_HTML.replace("<th>Player</th>", "<th>Athlete</th>")
    frame = realgm.realgm_players(fetcher=_fetcher(renamed))

    assert frame.height == 3
    assert "athlete" in frame.columns


def test_standings_stacks_and_labels_both_conferences():
    html = STANDINGS_HTML.format(
        east=_conference_table("east", EAST_TEAMS),
        west=_conference_table("west", WEST_TEAMS),
    )
    frame = realgm.realgm_standings(fetcher=_fetcher(html))

    assert frame.height == 30
    assert frame.filter(pl.col("conference") == "Eastern").height == 15
    assert frame.filter(pl.col("conference") == "Western").height == 15
    assert frame.filter(pl.col("team") == "East 1").get_column("conference").to_list() == ["Eastern"]


def test_standings_ignores_short_tables():
    """A conference table needs >= 10 rows; a small lookalike must not be mislabelled."""
    html = STANDINGS_HTML.format(
        east=_conference_table("east", EAST_TEAMS),
        west=_conference_table("tiny", WEST_TEAMS[:4]),
    )
    frame = realgm.realgm_standings(fetcher=_fetcher(html))

    assert frame.height == 15
    assert frame.get_column("conference").unique().to_list() == ["Eastern"]


def test_teams_maps_division_to_conference():
    frame = realgm.realgm_teams(fetcher=_fetcher(TEAMS_HTML))

    assert frame.columns == ["team", "division", "conference"]
    assert frame.height == 6
    row = frame.filter(pl.col("team") == "Boston Celtics").to_dicts()[0]
    assert row == {"team": "Boston Celtics", "division": "Atlantic", "conference": "Eastern"}
    assert frame.filter(pl.col("division") == "Pacific").get_column("conference").to_list() == ["Western"] * 3


def test_draft_derives_round_and_echoes_the_year():
    frame = realgm.realgm_draft(year=2020, fetcher=_fetcher(DRAFT_HTML))

    assert frame.height == 6
    assert frame.get_column("draft_year").unique().to_list() == [2020]
    by_player = {row["player"]: row["round"] for row in frame.to_dicts()}
    assert by_player["Pick One"] == 1
    assert by_player["Pick Thirty"] == 1
    assert by_player["Pick ThirtyOne"] == 2
    assert by_player["Undrafted A"] is None  # no `pick` column on the undrafted table


def test_player_stats_echoes_the_request_parameters():
    frame = realgm.realgm_player_stats(
        season=2025,
        stat_type="Totals",
        season_type="Playoffs",
        fetcher=_fetcher(PLAYERS_HTML),
    )

    assert frame.get_column("season").unique().to_list() == [2025]
    assert frame.get_column("stat_type").unique().to_list() == ["Totals"]
    assert frame.get_column("season_type").unique().to_list() == ["Playoffs"]


def test_transactions_parses_the_dom_list():
    frame = realgm.realgm_transactions(fetcher=_fetcher(TRANSACTIONS_HTML))

    assert frame.columns == ["date", "transaction"]
    assert frame.schema["date"] == pl.Date
    assert frame.height == 3  # the undated block is skipped
    first = frame.to_dicts()[0]
    assert str(first["date"]) == "2026-08-26"
    assert first["transaction"] == "Denver signed C Nikola Jokic."


# --- empty / malformed input -------------------------------------------------------------


@pytest.mark.parametrize(
    "endpoint",
    [
        realgm.realgm_players,
        realgm.realgm_standings,
        realgm.realgm_teams,
        realgm.realgm_early_entry,
        realgm.realgm_transactions,
        realgm.realgm_draft,
        realgm.realgm_player_stats,
    ],
)
@pytest.mark.parametrize("html", ["", "<html><body>Just a moment...</body></html>", "not html"])
def test_empty_or_malformed_page_returns_an_empty_frame(endpoint, html):
    frame = endpoint(fetcher=_fetcher(html))

    assert isinstance(frame, pl.DataFrame)
    assert frame.height == 0


def test_return_as_pandas():
    pd = pytest.importorskip("pandas")
    frame = realgm.realgm_players(fetcher=_fetcher(PLAYERS_HTML), return_as_pandas=True)

    assert isinstance(frame, pd.DataFrame)
    assert len(frame) == 3


# --- pacing / browser reuse ---------------------------------------------------------------


def test_pace_is_env_tunable(monkeypatch):
    monkeypatch.setenv("SDV_PY_REALGM_DELAY", "2.5")
    assert realgm._env_float("SDV_PY_REALGM_DELAY", 1.0) == 2.5
    monkeypatch.setenv("SDV_PY_REALGM_DELAY", "not-a-number")
    assert realgm._env_float("SDV_PY_REALGM_DELAY", 1.0) == 1.0
    monkeypatch.delenv("SDV_PY_REALGM_DELAY")
    assert realgm._env_float("SDV_PY_REALGM_DELAY", 1.0) == 1.0


class _StubPage:
    def __init__(self, owner: "_StubPlaywright") -> None:
        self.owner = owner

    def goto(self, url: str, **kwargs: Any) -> None:
        self.owner.gotos.append(url)

    def title(self) -> str:
        return self.owner.titles.pop(0) if self.owner.titles else "RealGM"

    def content(self) -> str:
        return PLAYERS_HTML

    def wait_for_timeout(self, ms: int) -> None:
        self.owner.waits.append(ms)

    def close(self) -> None:
        self.owner.closed.append("page")


class _StubContext:
    def __init__(self, owner: "_StubPlaywright") -> None:
        self.owner = owner

    def new_page(self) -> _StubPage:
        return _StubPage(self.owner)

    def close(self) -> None:
        self.owner.closed.append("context")


class _StubBrowser:
    def __init__(self, owner: "_StubPlaywright") -> None:
        self.owner = owner

    def new_context(self, **kwargs: Any) -> _StubContext:
        self.owner.contexts += 1
        return _StubContext(self.owner)

    def close(self) -> None:
        self.owner.closed.append("browser")


class _StubChromium:
    def __init__(self, owner: "_StubPlaywright") -> None:
        self.owner = owner

    def launch(self, **kwargs: Any) -> _StubBrowser:
        self.owner.launches.append(kwargs)
        return _StubBrowser(self.owner)


class _StubPlaywright:
    """A stand-in for ``playwright.sync_api`` that records what the runtime asked it to do."""

    def __init__(self, titles: Optional[List[str]] = None) -> None:
        self.launches: List[Dict[str, Any]] = []
        self.contexts = 0
        self.gotos: List[str] = []
        self.waits: List[int] = []
        self.closed: List[str] = []
        self.titles = list(titles or [])
        self.stopped = False
        self.chromium = _StubChromium(self)

    # sync_playwright() -> object with .start() -> the playwright handle
    def __call__(self) -> "_StubPlaywright":
        return self

    def start(self) -> "_StubPlaywright":
        return self

    def stop(self) -> None:
        self.stopped = True


def _install_stub_playwright(monkeypatch, stub: _StubPlaywright) -> None:
    module = type(sys)("playwright.sync_api")
    module.sync_playwright = stub  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "playwright.sync_api", module)


def test_browser_is_launched_once_and_reused_across_calls(monkeypatch):
    monkeypatch.setenv("SDV_PY_REALGM_DELAY", "0")
    monkeypatch.setenv("SDV_PY_REALGM_TTL", "300")
    stub = _StubPlaywright()
    _install_stub_playwright(monkeypatch, stub)

    realgm.realgm_players()
    realgm.realgm_coaches()
    realgm.realgm_salary_cap()

    assert len(stub.launches) == 1, "one browser per window, not one per request"
    assert stub.contexts == 1
    assert stub.gotos == [
        "https://basketball.realgm.com/nba/players",
        "https://basketball.realgm.com/nba/staff-members/20/Head-Coach/Current",
        "https://basketball.realgm.com/nba/info/salary_cap",
    ]

    realgm.realgm_close_browser()
    assert stub.stopped is True
    assert set(stub.closed) == {"page", "context", "browser"}
    assert realgm._SESSION == {}


def test_expired_idle_window_relaunches_the_browser(monkeypatch):
    monkeypatch.setenv("SDV_PY_REALGM_DELAY", "0")
    monkeypatch.setenv("SDV_PY_REALGM_TTL", "0")
    stub = _StubPlaywright()
    _install_stub_playwright(monkeypatch, stub)

    realgm.realgm_players()
    realgm.realgm_players()

    assert len(stub.launches) == 2


def test_challenge_title_is_polled_until_it_clears(monkeypatch):
    monkeypatch.setenv("SDV_PY_REALGM_DELAY", "0")
    monkeypatch.setenv("SDV_PY_REALGM_POLL", "0")
    stub = _StubPlaywright(titles=["Just a moment...", "Just a moment...", "NBA Players | RealGM"])
    _install_stub_playwright(monkeypatch, stub)

    frame = realgm.realgm_players()

    assert len(stub.waits) == 2  # polled twice, then the real title appeared
    assert frame.height == 3


def test_proxy_is_forwarded_to_the_browser_launch(monkeypatch):
    monkeypatch.setenv("SDV_PY_REALGM_DELAY", "0")
    monkeypatch.delenv("SDV_PY_REALGM_PROXY", raising=False)
    monkeypatch.delenv("SDV_PY_PROXY", raising=False)
    stub = _StubPlaywright()
    _install_stub_playwright(monkeypatch, stub)

    realgm.realgm_players(proxy="http://proxy.example:8080")

    assert stub.launches[0]["proxy"] == {"server": "http://proxy.example:8080"}


def test_proxy_falls_back_to_the_environment(monkeypatch):
    monkeypatch.delenv("SDV_PY_REALGM_PROXY", raising=False)
    monkeypatch.setenv("SDV_PY_PROXY", "http://env.example:3128")
    assert realgm._resolve_proxy(None) == "http://env.example:3128"
    assert realgm._resolve_proxy("http://explicit:1") == "http://explicit:1"
    monkeypatch.setenv("SDV_PY_REALGM_PROXY", "http://realgm.example:3128")
    assert realgm._resolve_proxy(None) == "http://realgm.example:3128"
