"""A record that is not an object is drift, on pandas 2 and 3 alike (WB-providers-2).

``pd.json_normalize`` reads a scalar record as an empty row on pandas 2 (an N x 0 frame) and raises on
pandas 3. The provider flatteners check first (``errors._object_records``): zero rows and a
``SchemaDriftWarning`` naming the parser, whichever pandas is installed. The name matches the 3.10
(pandas 2) CI job's ``tests/test_*parsers.py`` (#941), so CI runs it on both.
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.errors import SchemaDriftWarning


def _drifts(parse, body, where):
    with pytest.warns(SchemaDriftWarning, match=where):
        out = parse(body)
    assert isinstance(out, pl.DataFrame) and out.height == 0, out.shape


@pytest.mark.parametrize(
    ("name", "body", "where"),
    [
        ("parse_nfl_team", ["a", "b", "c"], "parse_nfl_team: payload"),
        ("parse_nfl_team", [{"id": 1}, "b"], "parse_nfl_team: payload"),
        ("parse_nfl_weeks_by_date", ["a", "b"], "parse_nfl_weeks_by_date: payload"),
        ("parse_nfl_weekly_game_details", ["a", "b"], "parse_nfl_weekly_game_details: games/data"),
        ("parse_nfl_rosters", {"rosters": ["a", "b"]}, "parse_nfl_rosters: rosters"),
        ("parse_nfl_standings", {"weeks": ["a", "b"]}, "parse_nfl_standings: weeks"),  # was AttributeError
    ],
)
def test_nfl_api_scalar_records_are_drift(name, body, where):
    from sportsdataverse.nfl import nfl_api_parsers

    _drifts(getattr(nfl_api_parsers, name), body, where)


def test_nfl_api_null_record_is_no_row_and_real_records_still_parse():
    from sportsdataverse.nfl.nfl_api_parsers import parse_nfl_rosters

    assert parse_nfl_rosters({"rosters": [None]}).shape == (0, 0)  # was a 1 x 0 frame
    df = parse_nfl_rosters({"rosters": [{"teamId": "1", "team": {"abbr": "KC"}}, None]})
    assert df.columns == ["team_id", "team_abbr"] and df.height == 1


@pytest.mark.parametrize(
    ("name", "body", "where"),
    [
        ("parse_mlb_statcast_gamefeed", {"team_home": ["a", "b"]}, "parse_mlb_statcast_gamefeed"),
        ("parse_mlb_statcast_gamefeed", {"exit_velocity": [1, 2]}, "parse_mlb_statcast_gamefeed"),
        ("parse_mlb_statcast_schedule", {"schedule": {"dates": [{"games": ["a"]}]}}, "parse_mlb_statcast_schedule"),
        ("parse_mlb_statcast_html_leaderboard", "<script>const data = [1, 2];</script>", "html_leaderboard: data"),
        ("parse_mlb_statcast_player", '<script>var serverVals = {"statcast": ["a"]};</script>', "serverVals.statcast"),
    ],
)
def test_statcast_scalar_records_are_drift(name, body, where):
    from sportsdataverse.mlb import mlb_statcast_parsers

    _drifts(getattr(mlb_statcast_parsers, name), body, where)


def test_sports247_wraps_every_scalar_not_only_a_leading_one():
    """A scalar after a record raised on pandas 3 and became an all-null row on pandas 2."""
    from sportsdataverse.cfb.sports247_parsers import parse_sports247_result_set

    df = parse_sports247_result_set([{"x": 1}, "b"])
    assert df.columns == ["x", "value"] and df["value"].to_list() == [None, "b"]
