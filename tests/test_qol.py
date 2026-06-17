"""Offline tests for the QoL helpers added in 0.0.51:

* ``sportsdataverse.parsed.*`` — DataFrame-by-default mirror namespace
* ``sportsdataverse.find.*`` — name-to-ID resolvers (find_team, find_athlete, find_event)
* ``sportsdataverse.discover.*`` — searchable function index (list_functions, function_count)

These tests monkey-patch the HTTP layer so they run without network.
"""

from __future__ import annotations

import pandas as pd
import polars as pl
import pytest

from tests.conftest import load_fixture


# ===========================================================================
# sportsdataverse.parsed.* namespace
# ===========================================================================


def _fake_get_for(payload):
    """Return a fake _get that always returns the given payload."""
    return lambda *args, **kwargs: payload


def test_parsed_namespace_defaults_to_polars_dataframe():
    """A wrapper imported from sportsdataverse.parsed.nba should return
    a polars frame by default, no return_parsed=True kwarg needed."""
    import sportsdataverse._common_espn as ce

    fake_teams = {
        "sports": [
            {
                "leagues": [
                    {
                        "teams": [
                            {"team": {"id": "1", "displayName": "Test", "abbreviation": "TST"}},
                        ]
                    }
                ]
            }
        ]
    }
    original = ce._get
    ce._get = _fake_get_for(fake_teams)
    try:
        from sportsdataverse.parsed.nba import espn_nba_teams_site

        df = espn_nba_teams_site()
        assert isinstance(df, pl.DataFrame), f"expected polars from parsed.*, got {type(df)}"
        assert df.height >= 1
    finally:
        ce._get = original


def test_parsed_namespace_still_supports_return_parsed_false_override():
    """Pass return_parsed=False from a parsed.* module → raw Dict."""
    import sportsdataverse._common_espn as ce

    fake = {"sports": [{"leagues": [{"teams": []}]}]}
    original = ce._get
    ce._get = _fake_get_for(fake)
    try:
        from sportsdataverse.parsed.nba import espn_nba_teams_site

        raw = espn_nba_teams_site(return_parsed=False)
        assert isinstance(raw, dict)
        assert "sports" in raw
    finally:
        ce._get = original


def test_parsed_namespace_supports_return_as_pandas():
    import sportsdataverse._common_espn as ce

    fake = {
        "sports": [
            {
                "leagues": [
                    {
                        "teams": [
                            {"team": {"id": "1", "displayName": "Test"}},
                        ]
                    }
                ]
            }
        ]
    }
    original = ce._get
    ce._get = _fake_get_for(fake)
    try:
        from sportsdataverse.parsed.nba import espn_nba_teams_site

        pdf = espn_nba_teams_site(return_as_pandas=True)
        assert isinstance(pdf, pd.DataFrame)
        assert len(pdf) >= 1
    finally:
        ce._get = original


def test_raw_module_not_mutated_by_parsed_import():
    """Importing the parsed namespace must NOT mutate the raw module's
    behavior — the parsed.* build must not lock the raw wrapper into a
    single mode. Under the 0.0.54 contract the raw module itself defaults
    to a DataFrame, and ``return_parsed=False`` still recovers the Dict;
    both paths must survive a parsed.* import."""
    import sportsdataverse._common_espn as ce
    import sportsdataverse.parsed.nba  # noqa: F401  triggers parsed-mod build

    fake = {
        "sports": [
            {
                "leagues": [
                    {
                        "teams": [
                            {"team": {"id": "1", "displayName": "Test"}},
                        ]
                    }
                ]
            }
        ]
    }
    original = ce._get
    ce._get = _fake_get_for(fake)
    try:
        from sportsdataverse.nba import espn_nba_teams_site

        # 0.0.54 default flip: no kwarg → DataFrame (raw module, post parsed import).
        df = espn_nba_teams_site()
        assert isinstance(df, pl.DataFrame), (
            f"Raw module mutated by parsed import — expected DataFrame default, got {type(df)}"
        )
        assert df.height >= 1
        # return_parsed=False must still recover the raw Dict from the raw module.
        raw = espn_nba_teams_site(return_parsed=False)
        assert isinstance(raw, dict), f"return_parsed=False broken on raw module — got {type(raw)}"
        assert "sports" in raw
    finally:
        ce._get = original


def test_parsed_namespace_exposes_all_eight_leagues():
    """Every league should be reachable as `sportsdataverse.parsed.<league>`."""
    import sportsdataverse.parsed as parsed

    for league in ("nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb", "nhl"):
        assert hasattr(parsed, league), f"sportsdataverse.parsed.{league} missing"


def test_parsed_namespace_docstring_notes_the_default_flip():
    """The wrapper's docstring should explain the parsed.* contract."""
    from sportsdataverse.parsed.nba import espn_nba_teams_site

    doc = espn_nba_teams_site.__doc__ or ""
    assert "parsed.*" in doc, "parsed.* docstring must mention the default flip"


# ===========================================================================
# sportsdataverse.find.* resolvers
# ===========================================================================


@pytest.fixture
def fake_nba_endpoints(monkeypatch):
    """Replace the generated wrappers' HTTP sink with deterministic fixture data."""
    teams_site = {
        "sports": [
            {
                "leagues": [
                    {
                        "teams": [
                            {
                                "team": {
                                    "id": "13",
                                    "displayName": "Los Angeles Lakers",
                                    "location": "Los Angeles",
                                    "abbreviation": "LAL",
                                    "name": "Lakers",
                                    "shortDisplayName": "Lakers",
                                }
                            },
                            {
                                "team": {
                                    "id": "2",
                                    "displayName": "Boston Celtics",
                                    "location": "Boston",
                                    "abbreviation": "BOS",
                                    "name": "Celtics",
                                    "shortDisplayName": "Celtics",
                                }
                            },
                            {
                                "team": {
                                    "id": "6",
                                    "displayName": "Dallas Mavericks",
                                    "location": "Dallas",
                                    "abbreviation": "DAL",
                                    "name": "Mavericks",
                                    "shortDisplayName": "Mavericks",
                                }
                            },
                        ]
                    }
                ]
            }
        ]
    }
    roster_lal = load_fixture("espn", "team_roster_nba")
    scoreboard = {
        "events": [
            {
                "id": "401585607",
                "name": "Dallas Mavericks at Boston Celtics",
                "competitions": [
                    {
                        "competitors": [
                            {"homeAway": "home", "team": {"displayName": "Boston Celtics", "abbreviation": "BOS"}},
                            {"homeAway": "away", "team": {"displayName": "Dallas Mavericks", "abbreviation": "DAL"}},
                        ]
                    }
                ],
            }
        ]
    }

    def fake_get(url):
        if "teams/13/roster" in url:
            return roster_lal
        if "/teams" in url and "/roster" not in url:
            return teams_site
        if "/scoreboard" in url:
            return scoreboard
        return {}

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

    def fake_download(url, params=None, **kw):
        return _Resp(fake_get(url))

    # The generated wrappers (find.py uses espn_*_teams_site / _scoreboard) import
    # _get from _codegen_runtime, which calls _codegen_runtime.download -- so patch
    # there, not ce._get (a re-export the generated modules don't see).
    import sportsdataverse._codegen_runtime as rt

    monkeypatch.setattr(rt, "download", fake_download)
    from sportsdataverse.find import clear_team_cache

    clear_team_cache()
    yield
    clear_team_cache()


def test_find_team_resolves_full_name(fake_nba_endpoints):
    from sportsdataverse.find import find_team

    t = find_team("lakers", league="nba")
    assert t is not None
    assert t["id"] == "13"
    assert t["abbreviation"] == "LAL"


def test_find_team_resolves_abbreviation(fake_nba_endpoints):
    from sportsdataverse.find import find_team

    t = find_team("LAL", league="nba")
    assert t["id"] == "13"


def test_find_team_returns_none_on_no_match(fake_nba_endpoints):
    from sportsdataverse.find import find_team

    assert find_team("ZZZZ", league="nba") is None


def test_find_team_multi_returns_list(fake_nba_endpoints):
    from sportsdataverse.find import find_team

    # "Los" matches Lakers (Los Angeles) — single team, but list-wrapped
    matches = find_team("Los", league="nba", multi=True)
    assert isinstance(matches, list)
    assert len(matches) == 1
    assert matches[0]["id"] == "13"


def test_find_team_raises_on_unknown_league():
    from sportsdataverse.find import find_team

    with pytest.raises(ValueError, match="Unknown league"):
        find_team("Lakers", league="not-a-league")


def test_find_event_by_home(fake_nba_endpoints):
    from sportsdataverse.find import find_event

    e = find_event(date="2024-06-17", league="nba", home="Boston")
    assert e["id"] == "401585607"


def test_find_event_by_away(fake_nba_endpoints):
    from sportsdataverse.find import find_event

    e = find_event(date="20240617", league="nba", away="Dallas")
    assert e["id"] == "401585607"


def test_find_event_returns_none_on_no_match(fake_nba_endpoints):
    from sportsdataverse.find import find_event

    e = find_event(date="2024-06-17", league="nba", home="Nonexistent")
    assert e is None


def test_find_athlete_with_team_filter(fake_nba_endpoints):
    from sportsdataverse.find import find_athlete

    a = find_athlete("lebron", league="nba", team="lakers")
    if a is None:
        pytest.skip("LeBron not in the captured LAL roster fixture")
    assert "lebron" in a.get("fullName", "").lower()
    assert a["team_id"] == "13"


# ===========================================================================
# sportsdataverse.discover.* — list_functions + function_count
# ===========================================================================


def test_list_functions_returns_dict_grouped_by_league():
    from sportsdataverse.discover import list_functions

    out = list_functions()
    assert isinstance(out, dict)
    # Every active league should be present
    for league in ("nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb", "nhl"):
        assert league in out, f"{league} missing from list_functions() output"
        assert isinstance(out[league], list)
        assert len(out[league]) > 50, f"{league} has only {len(out[league])} functions — expected >= 50"


def test_list_functions_filter_by_league_returns_flat_list():
    from sportsdataverse.discover import list_functions

    names = list_functions(league="nba")
    assert isinstance(names, list)
    assert "espn_nba_scoreboard" in names
    assert "espn_nba_team_roster" in names


def test_list_functions_search_filter_is_case_insensitive():
    from sportsdataverse.discover import list_functions

    out = list_functions(search="ROSTER")  # uppercase intentional
    # NBA / MLB / NHL all have *_roster wrappers
    assert any("roster" in n.lower() for n in out.get("nba", []))


def test_list_functions_parsers_only_filters_to_parse_prefix():
    from sportsdataverse.discover import list_functions

    out = list_functions(league="mlb", parsers_only=True)
    assert all(n.startswith("parse_") for n in out)
    assert "parse_mlb_api_schedule" in out


def test_list_functions_wrappers_only_excludes_parsers():
    from sportsdataverse.discover import list_functions

    out = list_functions(league="mlb", wrappers_only=True)
    assert not any(n.startswith("parse_") for n in out)


def test_list_functions_parsers_only_and_wrappers_only_are_mutually_exclusive():
    from sportsdataverse.discover import list_functions

    with pytest.raises(ValueError, match="mutually exclusive"):
        list_functions(parsers_only=True, wrappers_only=True)


def test_function_count_per_league():
    from sportsdataverse.discover import function_count

    counts = function_count()
    assert isinstance(counts, dict)
    # core 8 leagues must always be present
    core = {"cfb", "mbb", "mlb", "nba", "nfl", "nhl", "wbb", "wnba"}
    assert core <= set(counts), f"Missing core leagues: {core - set(counts)}"
    # ESPN additional leagues — all minor/alias leagues nested under sport-group
    # packages (0.0.65+); ahl/ohl/qmjhl/whl moved under hockey/ (Task 5).
    # function_count() keys by flat leaf (not dotted path) for back-compat.
    additional = {
        "soccer",
        "cricket",
        # nested under sport-group packages — keyed by flat leaf
        "ufl",
        "xfl",
        "cfl",
        "college_baseball",
        "college_softball",
        "mch",
        "wch",
        "ahl",
        "ohl",
        "qmjhl",
        "whl",
    }
    assert additional <= set(counts), f"Missing additional leagues: {additional - set(counts)}"
    # MLB has the largest surface post-0.0.51 (ESPN + Stats API + Statcast)
    assert counts["mlb"] >= 150
    # NHL second-largest (api-web + EDGE + Stats REST + Records + ESPN)
    assert counts["nhl"] >= 150
    # soccer param-mode catch-all has ≥100 wrappers
    assert counts["soccer"] >= 100


def test_function_count_single_league_returns_int():
    from sportsdataverse.discover import function_count

    n = function_count(league="nba")
    assert isinstance(n, int)
    assert n >= 100


def test_top_level_qol_helpers_importable():
    """The 5 QoL helpers (find_team / find_athlete / find_event /
    list_functions / function_count) must be reachable from the
    top-level sportsdataverse namespace."""
    import sportsdataverse as sdv

    for name in ("find_team", "find_athlete", "find_event", "list_functions", "function_count", "clear_team_cache"):
        assert hasattr(sdv, name), f"sdv.{name} not exposed at top level"
        assert callable(getattr(sdv, name)), f"sdv.{name} is not callable"
