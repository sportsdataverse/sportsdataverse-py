"""Wire-value coercion at the shared runtime boundary (quality task 4.8).

Every value a caller hands a generated wrapper or loader reaches the host through four
helpers in :mod:`sportsdataverse._codegen_runtime`: ``_csv`` (list-valued query params),
``bool_str`` (flags), ``_as_season_list`` (every generated release loader) and the path
segment the ESPN template interpolates into the URL. Before 4.8 each trusted its input:
``np.int64(2024)`` and ``2024.0`` raised ``TypeError`` in every loader (A-M6), a numpy
array or a polars Series went to the wire as its repr (A-M16), ``bool_str("false")`` was
``"true"`` (A-M17), ``team_id="13/roster"`` changed the route (A-N2), and a misspelled
query kwarg failed naming ``download()`` instead of the wrapper (A-N3).

These tests drive the real generated wrappers and loaders with the transport patched, and
assert what would have gone on the wire.
"""

from unittest.mock import patch

import numpy as np
import polars as pl
import pytest
import requests

from sportsdataverse._codegen_runtime import _as_season_list, _csv, _path_seg, bool_str
from sportsdataverse.cfb import cfb_loaders
from sportsdataverse.cfb.cfb_espn_ext import espn_cfb_players_index, espn_cfb_scoreboard, espn_cfb_team_roster
from sportsdataverse.soccer.soccer_espn_ext import espn_soccer_team_roster


class _FakeResp:
    status_code = 200
    text = "{}"

    def json(self):
        return {}


def _download_spy():
    return patch("sportsdataverse._codegen_runtime.download", return_value=_FakeResp())


def _query_string(params: dict) -> str:
    """The query string ``download`` puts on the wire for ``params`` (requests' own encoding)."""
    url = requests.Request("GET", "https://host.test/p", params=params).prepare().url
    return url.split("?", 1)[1]


# --- A-M16: _csv ------------------------------------------------------------------------------

_CSV_CASES = [
    # characterization: the inputs that already worked keep their output
    ([1, 2, 3], "1,2,3"),
    (("a", "b"), "a,b"),
    ("x", "x"),
    (5, "5"),
    # the inputs that went to the wire wrong
    (np.array([1, 2]), "1,2"),  # was "[1 2]"
    (pl.Series([1, 2]), "1,2"),  # was the Series' multi-line repr
    (range(3), "0,1,2"),  # was "range(0, 3)"
    ({3, 1, 2}, "1,2,3"),  # was hash order
    (2024.0, "2024"),  # was "2024.0"
    ([2024.0, 2025.0], "2024,2025"),
    (np.int64(2024), "2024"),
    (np.array([2024.0]), "2024"),
    ([True, False], "true,false"),  # the lowercase the hosts expect, not "True,False"
]


@pytest.mark.parametrize(("values", "expected"), _CSV_CASES)
def test_csv_wire_value(values, expected):
    assert _csv(values) == expected


def test_csv_none_passes_through():
    assert _csv(None) is None


def test_csv_exact_query_string():
    """What a list-valued param looks like in the request ``download`` sends."""
    params = {"sportIds": _csv(np.array([1, 11])), "leagueId": _csv({104, 103}), "season": _csv(2024.0)}
    assert _query_string(params) == "sportIds=1%2C11&leagueId=103%2C104&season=2024"


def test_csv_rejects_a_nested_value():
    with pytest.raises(ValueError, match="single value"):
        _csv([[1, 2], [3]])


# --- A-M6: _as_season_list, and the generated loaders that call it -----------------------------


@pytest.mark.parametrize(
    ("seasons", "expected"),
    [
        (2024, [2024]),
        ("2024", [2024]),
        ([2021, 2022], [2021, 2022]),
        (range(2022, 2024), [2022, 2023]),
        (np.int64(2024), [2024]),  # raised TypeError: 'numpy.int64' object is not iterable
        (2024.0, [2024]),  # raised TypeError: 'float' object is not iterable
        (np.array([2023, 2024]), [2023, 2024]),
        (pl.Series([2023, 2024]), [2023, 2024]),
        ([2023.0, np.int32(2024)], [2023, 2024]),
    ],
)
def test_season_list_accepts_integral_years(seasons, expected):
    out = _as_season_list(seasons)
    assert out == expected
    assert all(type(s) is int for s in out)


@pytest.mark.parametrize("seasons", [2024.5, [2023, 2024.5], True, [True], "2024-25", None, [[2024]]])
def test_season_list_rejects_non_years_naming_seasons(seasons):
    # 2024.5 / [2024.5] were silently truncated to 2024; True became [1]; None raised TypeError.
    with pytest.raises(ValueError, match="seasons"):
        _as_season_list(seasons)


@pytest.mark.parametrize("seasons", [np.int64(2024), 2024.0, pl.Series([2024])])
def test_generated_loader_takes_a_numpy_or_float_season(seasons):
    """``load_cfb_pbp(np.int64(2024))``: the URL carries ``2024``, never ``2024.0``."""
    urls: list[str] = []
    with patch.object(cfb_loaders, "_read_release_parquet", side_effect=lambda u: urls.append(u) or pl.DataFrame()):
        cfb_loaders.load_cfb_pbp(seasons)
    assert urls == [
        "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_pbp/play_by_play_2024.parquet"
    ]


def test_generated_loader_rejects_a_fractional_season_before_any_request():
    with patch.object(cfb_loaders, "_read_release_parquet") as read, pytest.raises(ValueError, match="seasons"):
        cfb_loaders.load_cfb_pbp(2024.5)
    read.assert_not_called()


# --- A-M17: bool_str ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (True, "true"),
        (False, "false"),
        (np.bool_(False), "false"),
        ("false", "false"),  # was "true": any non-empty string is truthy
        ("False", "false"),
        (" FALSE ", "false"),
        ("0", "false"),
        ("true", "true"),
        ("True", "true"),
        ("1", "true"),
    ],
)
def test_bool_str(value, expected):
    assert bool_str(value) == expected


def test_bool_str_none_passes_through():
    assert bool_str(None) is None


def test_bool_str_rejects_an_unknown_string_naming_the_parameter():
    with pytest.raises(ValueError, match="active"):
        bool_str("yes", name="active")


def test_generated_wrapper_sends_false_for_the_string_false():
    with _download_spy() as dl:
        espn_cfb_players_index(active="false", return_parsed=False)
    assert dl.call_args.kwargs["params"]["active"] == "false"


def test_generated_wrapper_rejects_a_bad_flag_before_any_request():
    with _download_spy() as dl, pytest.raises(ValueError, match="active"):
        espn_cfb_players_index(active="maybe", return_parsed=False)
    dl.assert_not_called()


# --- A-N2: path params are quoted --------------------------------------------------------------

_ROSTER = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{}/roster"


@pytest.mark.parametrize(
    ("team_id", "segment"),
    [
        (333, "333"),  # characterization
        ("333", "333"),
        (np.int64(333), "333"),
        (333.0, "333"),  # a float id never reaches the wire as "333.0"
        ("13/roster", "13%2Froster"),  # was /teams/13/roster/roster: a different route
        ("13?x=1", "13%3Fx%3D1"),  # was a query string
        ("13#f", "13%23f"),  # was a fragment
    ],
)
def test_espn_path_param_is_quoted(team_id, segment):
    with _download_spy() as dl:
        espn_cfb_team_roster(team_id, return_parsed=False)
    assert dl.call_args.kwargs["url"] == _ROSTER.format(segment)


def test_espn_param_mode_league_is_quoted():
    """A param-mode family (soccer) interpolates the caller's ``league`` into the path too."""
    with _download_spy() as dl:
        espn_soccer_team_roster("eng.1", 359, return_parsed=False)
    assert dl.call_args.kwargs["url"].endswith("/sports/soccer/eng.1/teams/359/roster")
    with _download_spy() as dl:
        espn_soccer_team_roster("eng.1/teams/1", 359, return_parsed=False)
    assert dl.call_args.kwargs["url"].endswith("/sports/soccer/eng.1%2Fteams%2F1/teams/359/roster")


def test_path_seg_rejects_a_list():
    with pytest.raises(ValueError, match="single value"):
        _path_seg([1, 2])


# --- A-N3: a misspelled query kwarg names the wrapper ------------------------------------------


def test_misspelled_kwarg_names_the_wrapper_before_any_request():
    with _download_spy() as dl, pytest.raises(TypeError, match=r"espn_cfb_scoreboard\(\).*'date'"):
        espn_cfb_scoreboard(date="20240115", return_parsed=False)  # the param is ``dates``
    dl.assert_not_called()


def test_transport_kwargs_still_reach_download():
    with _download_spy() as dl:
        espn_cfb_scoreboard(dates="20240115", cache_ttl=0, timeout=5, return_parsed=False)
    assert dl.call_args.kwargs["cache_ttl"] == 0
    assert dl.call_args.kwargs["timeout"] == 5
