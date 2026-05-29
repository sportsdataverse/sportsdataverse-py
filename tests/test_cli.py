"""Offline tests for the sdv CLI (sportsdataverse.cli)."""

from __future__ import annotations

import json

import pytest

from sportsdataverse.cli import _build_parser, main

# ---------------------------------------------------------------------------
# Parser shape
# ---------------------------------------------------------------------------


def test_parser_has_all_subcommands():
    parser = _build_parser()
    # Probe the subparsers action — argparse doesn't expose them directly,
    # but we can test via help text
    help_text = parser.format_help()
    for sub in ("find-team", "find-athlete", "find-event", "list-functions", "function-count", "cache"):
        assert sub in help_text, f"subcommand {sub!r} missing from help"


def test_parser_requires_a_subcommand():
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])


@pytest.mark.parametrize(
    "subcommand",
    [
        "find-team",
        "find-athlete",
        "find-event",
    ],
)
def test_find_subcommands_require_league(subcommand):
    """--league is required on every find-* subcommand so the CLI never
    silently picks one."""
    parser = _build_parser()
    with pytest.raises(SystemExit):
        # Missing --league
        parser.parse_args([subcommand, "name"])


# ---------------------------------------------------------------------------
# Handlers (mocked so we don't hit the network)
# ---------------------------------------------------------------------------


def test_function_count_default_prints_aligned_table(capsys):
    """Default (non-JSON) output should be a two-column aligned table."""
    exit_code = main(["function-count"])
    assert exit_code == 0
    out = capsys.readouterr().out
    # Each league line is "  LEAGUE  COUNT"
    for league in ("nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb", "nhl"):
        assert league in out


def test_function_count_with_league_flag_prints_int(capsys):
    exit_code = main(["function-count", "--league", "mlb"])
    assert exit_code == 0
    out = capsys.readouterr().out.strip()
    # Single integer line
    assert out.isdigit()
    assert int(out) > 100


def test_function_count_json_emits_dict(capsys):
    exit_code = main(["--json", "function-count"])
    assert exit_code == 0
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert isinstance(payload, dict)
    assert set(payload) == {"nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb", "nhl"}


def test_list_functions_with_search_filters(capsys):
    exit_code = main(["list-functions", "--league", "mlb", "--search", "statcast"])
    assert exit_code == 0
    out = capsys.readouterr().out
    # Every printed line should contain 'statcast'
    lines = [line for line in out.strip().splitlines() if line.strip()]
    assert all("statcast" in line.lower() for line in lines)
    assert "statcast_search" in out


def test_list_functions_parsers_only_filters_to_parse_prefix(capsys):
    exit_code = main(["list-functions", "--league", "mlb", "--parsers-only"])
    assert exit_code == 0
    out = capsys.readouterr().out
    lines = [line for line in out.strip().splitlines() if line.strip()]
    assert all(line.startswith("parse_") for line in lines)


def test_list_functions_parsers_only_and_wrappers_only_are_mutex(capsys):
    """argparse mutual-exclusion should reject the combo."""
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["list-functions", "--parsers-only", "--wrappers-only"])


# ---------------------------------------------------------------------------
# find-team / find-athlete / find-event (mocked endpoints)
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_nba_lookup(monkeypatch):
    """Replace ESPN download path with deterministic team + scoreboard data."""
    import sportsdataverse._common_espn as ce

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
                                },
                            },
                        ],
                    },
                ],
            },
        ],
    }
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
                        ],
                    },
                ],
            },
        ],
    }

    def fake_get(url, **kw):
        if "/teams" in url and "/roster" not in url:
            return teams_site
        if "/scoreboard" in url:
            return scoreboard
        return {}

    original = ce._get
    ce._get = fake_get
    # Reset find's per-process team cache
    from sportsdataverse.find import clear_team_cache

    clear_team_cache()
    yield
    ce._get = original
    clear_team_cache()


def test_cli_find_team_pretty_output(fake_nba_lookup, capsys):
    exit_code = main(["find-team", "lakers", "--league", "nba"])
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "id: 13" in out
    assert "Lakers" in out


def test_cli_find_team_json_output(fake_nba_lookup, capsys):
    exit_code = main(["--json", "find-team", "lakers", "--league", "nba"])
    assert exit_code == 0
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload["id"] == "13"
    assert payload["abbreviation"] == "LAL"


def test_cli_find_team_no_match_exits_with_code_1(fake_nba_lookup, capsys):
    exit_code = main(["find-team", "ZZZZZ", "--league", "nba"])
    assert exit_code == 1
    out = capsys.readouterr().out
    assert "(no match)" in out


def test_cli_find_event_with_home_filter(fake_nba_lookup, capsys):
    exit_code = main(
        [
            "--json",
            "find-event",
            "2024-06-17",
            "--league",
            "nba",
            "--home",
            "Boston",
        ],
    )
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["id"] == "401585607"


# ---------------------------------------------------------------------------
# cache subcommand
# ---------------------------------------------------------------------------


def test_cli_cache_mode_get_returns_current(capsys):
    from sportsdataverse import cache as cache_mod

    cache_mod.set_cache_mode("off")
    exit_code = main(["cache", "mode"])
    assert exit_code == 0
    assert capsys.readouterr().out.strip() == "off"


def test_cli_cache_mode_set_and_get_roundtrip(capsys):
    from sportsdataverse import cache as cache_mod

    try:
        exit_code = main(["cache", "mode", "--set", "memory"])
        assert exit_code == 0
        assert cache_mod.get_cache_mode() == "memory"
        capsys.readouterr()  # discard
        main(["cache", "mode"])
        assert capsys.readouterr().out.strip() == "memory"
    finally:
        cache_mod.set_cache_mode("off")


def test_cli_cache_stats_shows_entries(capsys):
    from sportsdataverse import cache as cache_mod

    try:
        cache_mod.set_cache_mode("memory")
        cache_mod._MEMORY_CACHE.clear()
        cache_mod.cache_set("https://x.example/a", None, {"v": 1})
        capsys.readouterr()  # drain
        exit_code = main(["cache", "stats"])
        assert exit_code == 0
        out = capsys.readouterr().out
        assert "mode: memory" in out
        assert "entries: 1" in out
    finally:
        cache_mod._MEMORY_CACHE.clear()
        cache_mod.set_cache_mode("off")


def test_cli_cache_clear_drops_entries(capsys):
    from sportsdataverse import cache as cache_mod

    try:
        cache_mod.set_cache_mode("memory")
        cache_mod._MEMORY_CACHE.clear()
        for i in range(3):
            cache_mod.cache_set(f"https://x.example/{i}", None, {"i": i})
        capsys.readouterr()  # drain
        exit_code = main(["cache", "clear"])
        assert exit_code == 0
        assert "cleared 3 entries" in capsys.readouterr().out
        assert cache_mod.cache_stats()["entries"] == 0
    finally:
        cache_mod._MEMORY_CACHE.clear()
        cache_mod.set_cache_mode("off")
