"""Offline tests for the hoopR salary / mock-draft / injury ports.

No network: every test injects a fake transport by monkeypatching ``_fetch``'s
underlying ``download`` in :mod:`sportsdataverse.nba.nba_salary_draft`. Fixtures
are minimal inline HTML/JSON that reproduce the documented gotchas -- Spotrac's
duplicated team token and ``$`` money, NBADraft's repeated third table, and
HoopsHype's ``__NEXT_DATA__`` blob.
"""

from __future__ import annotations

import json

import polars as pl
import pytest

from sportsdataverse.nba import nba_salary_draft as nsd


class FakeResponse:
    """Minimal ``requests.Response`` stand-in (``.text`` / ``.json()``)."""

    def __init__(self, text: str = "", payload=None):
        self.text = text
        self._payload = payload
        self.status_code = 200

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


SPOTRAC_HTML = """
<html><body>
<noscript>Please enable JavaScript to view this site.</noscript>
<table id="table">
  <thead><tr><th>Rank</th><th>Team</th><th>Record</th><th>Players Active</th>
  <th>Avg Age Team</th><th>Total Cap Allocations</th><th>Cap Space All</th></tr></thead>
  <tbody>
    <tr><td>1</td><td>ORL ORL</td><td>41-41</td><td>15</td><td>25.4</td>
        <td>$186,000,000</td><td>-$45,500,000</td></tr>
    <tr><td>2</td><td>BOS BOS</td><td>60-22</td><td>16</td><td>27.1</td>
        <td>$194,250,000</td><td>$1,200,000</td></tr>
  </tbody>
</table>
</body></html>
"""

# Tables 1 and 2 are rounds 1 and 2; table 3 REPEATS round 1.
_R1 = """
<table><thead><tr><th>#</th><th>Team</th><th>Player</th><th>H</th><th>W</th>
<th>P</th><th>School</th><th>C</th></tr></thead>
<tbody>
<tr><td>1</td><td>WAS</td><td>Player One</td><td>6-8</td><td>210</td><td>SF</td>
    <td>Duke</td><td>Fr</td></tr>
<tr><td>2</td><td>UTA*</td><td>Player Two</td><td>7-0</td><td>240</td><td>C</td>
    <td>Baylor</td><td>So</td></tr>
</tbody></table>
"""
_R2 = """
<table><thead><tr><th>#</th><th>Team</th><th>Player</th><th>H</th><th>W</th>
<th>P</th><th>School</th><th>C</th></tr></thead>
<tbody>
<tr><td>31</td><td>MIN</td><td>Player Three</td><td>6-4</td><td>195</td><td>SG</td>
    <td>Kansas</td><td>Jr</td></tr>
</tbody></table>
"""
NBADRAFT_HTML = f"<html><body><noscript>enable JavaScript</noscript>{_R1}{_R2}{_R1}</body></html>"

ROTOWIRE_PAYLOAD = [
    {
        "ID": 4321,
        "player": "Injured Guy",
        "firstname": "Injured",
        "lastname": "Guy",
        "team": "BOS",
        "position": "PG",
        "injury": "Ankle",
        "status": "Out",
        "rDate": "<span class='lock'>Subscribers Only</span>",
        "URL": "/basketball/player/injured-guy-4321",
    },
    {
        "ID": 8765,
        "player": "Sore Forward",
        "firstname": "Sore",
        "lastname": "Forward",
        "team": "LAL",
        "position": "PF",
        "injury": "Knee",
        "status": "GTD",
        "rDate": "<b>Mar 3</b>",
        "URL": "/basketball/player/sore-forward-8765",
    },
]


def _hoopshype_html(slug: str) -> str:
    """A team page whose ``__NEXT_DATA__`` carries one two-season contract."""
    blob = {
        "props": {
            "pageProps": {
                "dehydratedState": {
                    "queries": [
                        # A decoy query that has no contracts -- the parser must skip it.
                        {"state": {"data": {"somethingElse": True}}},
                        {
                            "state": {
                                "data": {
                                    "contracts": {
                                        "numResults": 1,
                                        "contracts": [
                                            {
                                                "playerID": 100 + len(slug),
                                                "playerName": f"{slug} Star",
                                                "player": {
                                                    "firstName": "First",
                                                    "lastName": "Last",
                                                    "team": {
                                                        "id": 7,
                                                        "location": "Orlando",
                                                        "nickname": "Magic",
                                                    },
                                                },
                                                "seasons": [
                                                    {
                                                        "season": 2026,
                                                        "salary": 12345678,
                                                        "capAllocation": 12345678,
                                                        "teamOption": False,
                                                        "playerOption": False,
                                                        "twoWayContract": False,
                                                        "qualifyingOffer": False,
                                                    },
                                                    {
                                                        "season": 2027,
                                                        "salary": 13000000,
                                                        "capAllocation": 13000000,
                                                        "teamOption": True,
                                                        "playerOption": False,
                                                        "twoWayContract": False,
                                                        "qualifyingOffer": False,
                                                    },
                                                ],
                                            }
                                        ],
                                    }
                                }
                            }
                        },
                    ]
                }
            }
        }
    }
    return f'<html><body><script id="__NEXT_DATA__" type="application/json">{json.dumps(blob)}</script></body></html>'


@pytest.fixture(autouse=True)
def no_pacing(monkeypatch):
    """Never sleep in tests, whatever the environment says."""
    monkeypatch.setenv("SDV_PY_HOOPSHYPE_DELAY", "0")


def _stub(monkeypatch, handler):
    """Replace ``download`` with ``handler(url, params, headers, proxy)``."""
    calls = []

    def fake_download(url=None, params=None, headers=None, proxy=None, **kwargs):
        calls.append({"url": url, "params": params, "headers": headers, "proxy": proxy})
        return handler(url)

    monkeypatch.setattr(nsd, "download", fake_download)
    return calls


# --------------------------------------------------------------------------- Spotrac


def test_spotrac_collapses_duplicate_team_token_and_parses_currency(monkeypatch):
    _stub(monkeypatch, lambda url: FakeResponse(text=SPOTRAC_HTML))

    df = nsd.spotrac_team_cap(season=2024)

    assert df.height == 2
    assert df["team"].to_list() == ["ORL", "BOS"]  # "ORL ORL" -> "ORL"
    assert df.schema["total_cap_allocations"] == pl.Float64
    assert df["total_cap_allocations"].to_list() == [186000000.0, 194250000.0]
    assert df["cap_space_all"].to_list() == [-45500000.0, 1200000.0]  # sign survives
    assert df["season"].to_list() == [2024, 2024]


def test_spotrac_season_lands_in_the_url_and_ua_is_sent(monkeypatch):
    calls = _stub(monkeypatch, lambda url: FakeResponse(text=SPOTRAC_HTML))

    nsd.spotrac_team_cap(season=2019, proxy={"https": "http://p:8080"})

    assert calls[0]["url"] == "https://www.spotrac.com/nba/cap/_/year/2019/"
    assert "Mozilla" in calls[0]["headers"]["User-Agent"]
    assert calls[0]["proxy"] == {"https": "http://p:8080"}


def test_spotrac_empty_page_returns_empty_frame(monkeypatch):
    _stub(monkeypatch, lambda url: FakeResponse(text="<html><body>no tables</body></html>"))

    with pytest.warns(UserWarning):
        df = nsd.spotrac_team_cap(season=2024)

    assert df.height == 0
    assert df.columns == list(nsd._SPOTRAC_SCHEMA)


def test_spotrac_transport_failure_returns_empty_frame(monkeypatch):
    def boom(url=None, **kwargs):
        raise RuntimeError("connection reset")

    monkeypatch.setattr(nsd, "download", boom)

    with pytest.warns(UserWarning):
        df = nsd.spotrac_team_cap(season=2024)

    assert df.height == 0
    assert df.columns == list(nsd._SPOTRAC_SCHEMA)


def test_spotrac_return_as_pandas(monkeypatch):
    _stub(monkeypatch, lambda url: FakeResponse(text=SPOTRAC_HTML))

    df = nsd.spotrac_team_cap(season=2024, return_as_pandas=True)

    assert list(df["team"]) == ["ORL", "BOS"]
    assert hasattr(df, "iloc")


# --------------------------------------------------------------------------- NBADraft


def test_nbadraft_ignores_the_repeated_third_table(monkeypatch):
    _stub(monkeypatch, lambda url: FakeResponse(text=NBADRAFT_HTML))

    df = nsd.nbadraft_mock_draft()

    # 3 rows, not 5: the third table repeats round 1 and must not be concatenated.
    assert df.height == 3
    assert df["round"].to_list() == [1, 1, 2]
    assert df["player"].to_list() == ["Player One", "Player Two", "Player Three"]
    assert df["player"].n_unique() == 3


def test_nbadraft_renames_terse_headers_and_strips_traded_marker(monkeypatch):
    _stub(monkeypatch, lambda url: FakeResponse(text=NBADRAFT_HTML))

    df = nsd.nbadraft_mock_draft()

    assert df.columns[:4] == ["round", "pick", "team", "player"]
    assert set(["height", "weight", "position", "school", "class"]).issubset(df.columns)
    assert df["team"].to_list() == ["WAS", "UTA", "MIN"]  # "UTA*" -> "UTA"
    assert df["pick"].to_list() == [1, 2, 31]


def test_nbadraft_year_uses_the_dated_path(monkeypatch):
    calls = _stub(monkeypatch, lambda url: FakeResponse(text=NBADRAFT_HTML))

    nsd.nbadraft_mock_draft(year=2025)

    assert calls[0]["url"] == "https://www.nbadraft.net/nba-mock-drafts/2025/"


def test_nbadraft_page_without_pick_tables_returns_empty_frame(monkeypatch):
    other = "<table><tr><th>Nav</th></tr><tr><td>Home</td></tr></table>"
    _stub(monkeypatch, lambda url: FakeResponse(text=f"<html><body>{other}</body></html>"))

    with pytest.warns(UserWarning):
        df = nsd.nbadraft_mock_draft()

    assert df.height == 0
    assert df.columns == list(nsd._NBADRAFT_SCHEMA)


# --------------------------------------------------------------------------- HoopsHype


def test_hoopshype_fanout_is_driven_by_one_team_list(monkeypatch):
    calls = _stub(monkeypatch, lambda url: FakeResponse(text=_hoopshype_html("team")))

    df = nsd.hoopshype_salaries()

    assert len(nsd.HOOPSHYPE_TEAMS) == 30
    assert len(calls) == 30
    assert [c["url"] for c in calls] == [f"https://www.hoopshype.com/salaries/{s}/" for s in nsd.HOOPSHYPE_TEAMS]
    # one contract x two seasons per team page
    assert df.height == 60


def test_hoopshype_next_data_extraction(monkeypatch):
    _stub(monkeypatch, lambda url: FakeResponse(text=_hoopshype_html("atlanta_hawks")))

    df = nsd.hoopshype_salaries()

    row = df.row(0, named=True)
    assert row["player"] == "atlanta_hawks Star"
    assert row["first_name"] == "First"
    assert row["team"] == "Orlando Magic"
    assert row["team_id"] == "7"  # int id -> "7", never "7.0"
    assert row["season"] == 2026
    assert row["salary"] == 12345678.0
    assert row["team_option"] is False
    assert df.schema["player_id"] == pl.Utf8
    assert df.schema["salary"] == pl.Float64
    assert df.schema["team_option"] == pl.Boolean
    assert df.filter(pl.col("team_option") == True).height == 30  # noqa: E712 - polars mask


def test_hoopshype_skips_pages_without_next_data(monkeypatch):
    def handler(url):
        if url.endswith("/atlanta_hawks/"):
            return FakeResponse(text="<html><body>nothing here</body></html>")
        return FakeResponse(text=_hoopshype_html("t"))

    _stub(monkeypatch, handler)

    with pytest.warns(UserWarning):
        df = nsd.hoopshype_salaries()

    assert df.height == 58  # 29 teams x 2 seasons


def test_hoopshype_all_pages_empty_returns_empty_frame(monkeypatch):
    _stub(monkeypatch, lambda url: FakeResponse(text="<html></html>"))

    with pytest.warns(UserWarning):
        df = nsd.hoopshype_salaries()

    assert df.height == 0
    assert df.columns == list(nsd._HOOPSHYPE_SCHEMA)


def test_hoopshype_pacing_is_env_tunable(monkeypatch):
    monkeypatch.setenv("SDV_PY_HOOPSHYPE_DELAY", "1.5")
    assert nsd._env_float("SDV_PY_HOOPSHYPE_DELAY", 0.5) == 1.5

    monkeypatch.setenv("SDV_PY_HOOPSHYPE_DELAY", "not-a-number")
    with pytest.warns(UserWarning):
        assert nsd._env_float("SDV_PY_HOOPSHYPE_DELAY", 0.5) == 0.5


# --------------------------------------------------------------------------- RotoWire


def test_rotowire_parses_rows_and_gates_the_return_date(monkeypatch):
    calls = _stub(monkeypatch, lambda url: FakeResponse(payload=ROTOWIRE_PAYLOAD))

    df = nsd.rotowire_injuries()

    assert calls[0]["url"] == "https://www.rotowire.com/basketball/tables/injury-report.php"
    assert calls[0]["params"] == {"team": "ALL", "pos": "ALL"}
    assert df.height == 2
    assert df["player_id"].to_list() == ["4321", "8765"]
    assert df["status"].to_list() == ["Out", "GTD"]
    assert df["return_date"].to_list() == [None, "Mar 3"]  # "Subscribers Only" -> null, tags stripped
    assert df["url"][0] == "https://www.rotowire.com/basketball/player/injured-guy-4321"


def test_rotowire_non_list_body_returns_empty_frame(monkeypatch):
    _stub(monkeypatch, lambda url: FakeResponse(payload={"error": "nope"}))

    with pytest.warns(UserWarning):
        df = nsd.rotowire_injuries()

    assert df.height == 0
    assert df.columns == list(nsd._ROTOWIRE_SCHEMA)


def test_rotowire_non_json_body_returns_empty_frame(monkeypatch):
    _stub(monkeypatch, lambda url: FakeResponse(text="<html>maintenance</html>"))

    with pytest.warns(UserWarning):
        df = nsd.rotowire_injuries()

    assert df.height == 0
