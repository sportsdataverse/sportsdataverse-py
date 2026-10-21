"""A record that is not an object is drift, on pandas 2 and 3 alike (WB-providers-2).

``pd.json_normalize`` reads a scalar record as an empty row on pandas 2 (an N x 0 frame) and raises on
pandas 3. The provider flatteners check first (``errors._object_rows``): a null row is no row; a
records value that is not a list, or any other row that is not an object, is zero rows and a
``SchemaDriftWarning`` naming the parser and pointing at its caller, whichever pandas is installed. The
name matches the 3.10 (pandas 2) CI job's ``tests/test_*parsers.py`` (#941), so CI runs it on both.
"""

from __future__ import annotations

import json
import warnings
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from sportsdataverse.errors import SchemaDriftWarning

_FIX = Path(__file__).parent / "fixtures"


def _drifts(parse, body, where):
    with pytest.warns(SchemaDriftWarning, match=where) as record:
        out = parse(body)
    assert isinstance(out, pl.DataFrame) and out.height == 0, out.shape
    # R945-7: the warning lands on the parser's caller, not inside the library
    assert [w.filename for w in record if issubclass(w.category, SchemaDriftWarning)] == [__file__]


def _quiet(parse, body):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        return parse(body)


_NFL_KEYED = [
    ("parse_nfl_combine_profiles", "combineProfiles"),
    ("parse_nfl_draft_picks", "picks"),
    ("parse_nfl_game_summaries", "data"),
    ("parse_nfl_injuries", "injuries"),
    ("parse_nfl_rosters", "rosters"),
    ("parse_nfl_teams_history", "teams"),
    ("parse_nfl_weekly_game_details", "games"),
    ("parse_nfl_weeks", "weeks"),
]


@pytest.mark.parametrize(
    ("name", "body", "where"),
    [
        ("parse_nfl_team", ["a", "b", "c"], "parse_nfl_team: payload"),
        ("parse_nfl_team", [{"id": 1}, "b"], "parse_nfl_team: payload"),
        ("parse_nfl_team", 5, r"parse_nfl_team: payload: .*\(TypeError: rows is int"),
        ("parse_nfl_weeks_by_date", ["a", "b"], "parse_nfl_weeks_by_date: payload"),
        ("parse_nfl_weekly_game_details", ["a", "b"], "parse_nfl_weekly_game_details: games/data"),
        ("parse_nfl_rosters", {"rosters": ["a", "b"]}, "parse_nfl_rosters: rosters"),
        ("parse_nfl_standings", {"weeks": ["a", "b"]}, r"parse_nfl_standings: weeks: .*\(TypeError: a row is str"),
        ("parse_nfl_standings", {"weeks": 5}, r"parse_nfl_standings: weeks: .*\(TypeError: rows is int"),
        ("parse_nfl_standings", {"weeks": [{"standings": 5}]}, r"weeks\[\]\.standings: .*\(TypeError: rows is int"),
        (
            "parse_nfl_standings",
            {"weeks": [{"standings": ["a"]}]},
            r"weeks\[\]\.standings: .*\(TypeError: a row is str",
        ),
        # R945-8: one good week and one drifted week is zero rows, never a partial table
        (
            "parse_nfl_standings",
            {"weeks": [{"standings": [{"team": {"abbr": "KC"}}]}, {"standings": 5}]},
            r"weeks\[\]\.standings: .*\(TypeError: rows is int",
        ),
    ]
    + [(name, {key: 5}, rf"{name}: \S+: .*\(TypeError: rows is int, not a list") for name, key in _NFL_KEYED]
    + [(name, {key: {"a": 1}}, rf"{name}: \S+: .*\(TypeError: rows is dict, not a list") for name, key in _NFL_KEYED],
)
def test_nfl_api_scalar_records_are_drift(name, body, where):
    from sportsdataverse.nfl import nfl_api_parsers

    _drifts(getattr(nfl_api_parsers, name), body, where)


def test_nfl_api_null_rows_are_no_rows_and_real_records_still_parse():
    from sportsdataverse.nfl.nfl_api_parsers import parse_nfl_rosters, parse_nfl_team

    assert _quiet(parse_nfl_rosters, {"rosters": [None]}).shape == (0, 0)  # was a 1 x 0 frame
    df = _quiet(parse_nfl_rosters, {"rosters": [{"teamId": "1", "team": {"abbr": "KC"}}, None, float("nan"), pd.NA]})
    assert df.columns == ["team_id", "team_abbr"] and df.height == 1
    for body in ("", 0, {}, [], None):  # an empty body is no data, not drift
        assert _quiet(parse_nfl_team, body).height == 0


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


@pytest.mark.parametrize(
    ("module", "name"),
    [
        ("sportsdataverse.cfb.sports247_parsers", "parse_sports247_teams"),
        ("sportsdataverse.cfb.sports247_parsers", "parse_sports247_institution_rankings"),
        ("sportsdataverse.soccer.mls.mls_api_parsers", "parse_mls_entity"),
    ],
)
def test_record_payload_parsers_read_a_scalar_array_as_drift(module, name):
    """R945-3: their documented rows are records, so ``["a", "b", "c"]`` is drift, not three ``value`` rows."""
    import importlib

    _drifts(getattr(importlib.import_module(module), name), ["a", "b", "c"], f"{name}: payload")


@pytest.mark.parametrize("name", ["parse_sports247_teams", "parse_sports247_institution_rankings"])
@pytest.mark.parametrize("value", [5, "x", {"a": 1}])
def test_sports247_record_envelope_with_a_non_list_collection_is_drift(name, value):
    """R945-9: ``{"pagination": ..., "list": 5}`` was one invented row from the flat-object fallback."""
    from sportsdataverse.cfb import sports247_parsers

    body = {"pagination": {"page": 1}, "list": value}
    _drifts(
        getattr(sports247_parsers, name), body, rf"{name}: payload\.list: .*\(TypeError: rows is {type(value).__name__}"
    )


def test_sports247_wraps_every_scalar_not_only_a_leading_one_and_drops_null_rows():
    """A scalar after a record raised on pandas 3 and became an all-null row on pandas 2."""
    from sportsdataverse.cfb.sports247_parsers import parse_sports247_result_set

    df = parse_sports247_result_set([{"x": 1}, "b"])
    assert df.columns == ["x", "value"] and df["value"].to_list() == [None, "b"]
    assert parse_sports247_result_set([{"x": 1}, None]).columns == ["x"]  # no invented value column


def test_sports247_gaps_stay_null_on_real_captures():
    """R945-4: pandas 2 read a missing nested field as NaN and the stringify pass wrote ``"nan"``."""
    from sportsdataverse.cfb.sports247_parsers import parse_sports247_result_set
    from sportsdataverse.cfb.sports247_site_pages_parsers import parse_sports247_site_page

    coaches = parse_sports247_result_set(
        json.loads((_FIX / "sports247/sports247_coaches_fb_2026.json").read_text("utf-8"))
    )
    assert coaches["current_job_name"].null_count() == 2
    pages = parse_sports247_site_page(
        json.loads((_FIX / "sports247_site_pages/institution_list.json").read_text("utf-8"))
    )
    for frame in (coaches, pages):
        text = [c for c, t in frame.schema.items() if t == pl.String]
        assert [c for c in text if (frame[c] == "nan").any()] == []
