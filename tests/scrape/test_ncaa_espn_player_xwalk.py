"""Offline tests for the NCAA player -> ESPN athlete crosswalk tiers."""

import polars as pl

from sportsdataverse.scrape.ncaa.espn_player_xwalk import _collapse_espn, match_players, summarize


def _ncaa(rows):
    return pl.DataFrame(
        rows,
        schema={"ncaa_team_id": pl.Utf8, "id": pl.Utf8, "name": pl.Utf8, "jersey": pl.Utf8, "espn_team_id": pl.Utf8},
        orient="row",
    )


def _espn(rows):
    return pl.DataFrame(
        rows, schema={"espn_team_id": pl.Utf8, "id": pl.Utf8, "name": pl.Utf8, "jersey": pl.Utf8}, orient="row"
    )


def _by_player(frame):
    return {r["ncaa_player_id"]: r for r in frame.iter_rows(named=True)}


def test_tiers_ties_and_reasons():
    ncaa = _ncaa(
        [
            ("1", "a", "Caitlin Clark", "22", "10"),  # name_jersey
            ("1", "b", "Clark, Kate", "5", "10"),  # "Last, First" -> name (jersey differs)
            ("1", "c", "Mike Jones", "3", "10"),  # last_jersey (Michael on ESPN)
            ("1", "d", "Jonathon Smithers", "9", "10"),  # jaro_winkler, no jersey on ESPN
            ("1", "e", "Sam Lee", "11", "10"),  # tie: two Sam Lees on ESPN, no jersey match
            ("1", "f", "Walk On", "40", "10"),  # no_candidate
            ("2", "g", "Anyone", "1", None),  # no_espn_team
            ("3", "h", "Someone", "1", "99"),  # no_espn_players
        ]
    )
    espn = _espn(
        [
            ("10", "100", "Caitlin Clark", "22"),
            ("10", "101", "Kate Clark", "15"),
            ("10", "102", "Michael Jones", "3"),
            ("10", "103", "Jonathan Smithers", None),
            ("10", "104", "Sam Lee", "1"),
            ("10", "105", "Sam Lee", "2"),
        ]
    )
    out = _by_player(match_players(ncaa, espn))
    assert (out["a"]["espn_athlete_id"], out["a"]["match_method"]) == ("100", "name_jersey")
    assert (out["b"]["espn_athlete_id"], out["b"]["match_method"]) == ("101", "name")
    assert (out["c"]["espn_athlete_id"], out["c"]["match_method"]) == ("102", "last_jersey")
    assert (out["d"]["espn_athlete_id"], out["d"]["match_method"]) == ("103", "jaro_winkler")
    assert out["e"]["espn_athlete_id"] is None and out["e"]["unmatched_reason"] == "ambiguous"
    assert out["f"]["unmatched_reason"] == "no_candidate"
    assert out["g"]["unmatched_reason"] == "no_espn_team"
    assert out["h"]["unmatched_reason"] == "no_espn_players"
    for row in out.values():
        assert (row["match_method"] is None) != (row["unmatched_reason"] is None)
    s = summarize(match_players(ncaa, espn))
    assert s["players"] == 8 and s["matched"] == 4 and s["ambiguous"] == 1


def test_athlete_claimed_by_two_ncaa_players_is_voided():
    # two NCAA team ids resolving to one ESPN team, each with a different player who name-matches
    ncaa = _ncaa([("1", "a", "Pat Doe", "4", "10"), ("2", "b", "Pat Doe", "4", "10")])
    espn = _espn([("10", "100", "Pat Doe", "4")])
    out = _by_player(match_players(ncaa, espn))
    assert out["a"]["espn_athlete_id"] is None and out["b"]["espn_athlete_id"] is None
    assert {out["a"]["unmatched_reason"], out["b"]["unmatched_reason"]} == {"ambiguous"}


def test_same_player_on_two_rosters_keeps_both_rows():
    ncaa = _ncaa([("1", "a", "Pat Doe", "4", "10"), ("2", "a", "Pat Doe", "4", "11")])
    espn = _espn([("10", "100", "Pat Doe", "4"), ("11", "100", "Pat Doe", "4")])
    out = match_players(ncaa, espn)
    assert out.get_column("espn_athlete_id").to_list() == ["100", "100"]


def test_collapse_espn_modal_jersey_and_float_free_ids():
    rows = pl.DataFrame(
        {
            "espn_team_id": ["10", "10", "10"],
            "id": ["100", "100", "100"],
            "name": ["A B", "A B", "A B"],
            "jersey": ["3", "3", "30"],
        }
    )
    out = _collapse_espn(rows)
    assert out.to_dicts() == [{"espn_team_id": "10", "id": "100", "name": "A B", "jersey": "3"}]
