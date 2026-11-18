"""ESPN quirks verified live on 2026-11-17 and how the generated wrappers now handle them."""

import inspect

import pytest

from sportsdataverse._codegen_runtime import _check_status
from sportsdataverse.errors import NoDataError

_CORE = "https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/events/1/competitions/1/probabilities"
_NOT_SUPPORTED = (
    '{"error":{"message":"Probabilities are not supported for sport: hockey, league: nhl, '
    'competition: 401802532","code":400}}'
)


def test_espn_not_supported_400_is_no_data():
    with pytest.raises(NoDataError, match="not supported"):
        _check_status(_CORE, 400, _NOT_SUPPORTED)


def test_other_400s_stay_value_errors():
    # A range on a league scoreboard is a bad request, not an absent resource.
    with pytest.raises(ValueError, match="HTTP 400"):
        _check_status(
            "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
            400,
            '{"code":400,"message":"Failed to get events endpoint."}',
        )
    # "not supported" from a host that is not ESPN keeps the repo-wide 400 rule.
    with pytest.raises(ValueError):
        _check_status("https://api.example.com/x", 400, "not supported")


@pytest.mark.parametrize("prefix", ["mbb", "wbb"])
def test_college_basketball_scoreboard_defaults_to_division_one(prefix):
    import importlib

    mod = importlib.import_module(f"sportsdataverse.{prefix}.{prefix}_espn_ext")
    fn = getattr(mod, f"espn_{prefix}_scoreboard")
    assert inspect.signature(fn).parameters["groups"].default == 50


def test_pro_scoreboard_keeps_no_group_default():
    from sportsdataverse.nba.nba_espn_ext import espn_nba_scoreboard

    assert inspect.signature(espn_nba_scoreboard).parameters["groups"].default is None


def test_empty_team_route_warns_and_names_its_replacement(monkeypatch):
    from sportsdataverse.nba import nba_espn_ext as mod

    monkeypatch.setattr(mod, "_get", lambda url, params=None, **kw: {})
    with pytest.warns(DeprecationWarning, match=r"espn_nba_team_injuries is deprecated.*use espn_nba_injuries"):
        mod.espn_nba_team_injuries(team_id=13, return_parsed=False)
    with pytest.warns(
        DeprecationWarning, match=r"espn_nba_team_history is deprecated and will be removed in 0\.4\.0\."
    ):
        mod.espn_nba_team_history(team_id=13, return_parsed=False)
