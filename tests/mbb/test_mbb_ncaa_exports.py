"""B-GB-25: the public ``mbb_ncaa_*`` module functions are exported from ``sportsdataverse.mbb`` (and documented)."""

import importlib

import pytest


_GB25_NAMES = [
    "current_ncaa_team_alts",
    "normalize_player_key",
    "build_player_xwalk",
    "resolve_possessions",
    "expand_xwalk_aliases",
    "observed_pairs",
    "build_person_keys",
    "aggregate_stints",
    "team_aggregate",
    "stint_exposure",
    "stack_seasons",
    "season_slice",
    "solve_rapm_league",
    "split_half_se_check",
    "possession_deciles",
    "sides_from_box",
]


@pytest.mark.parametrize("name", _GB25_NAMES)
def test_mbb_ncaa_module_functions_are_exported_from_the_league(name: str) -> None:
    mbb = importlib.import_module("sportsdataverse.mbb")
    fn = getattr(mbb, name)
    assert fn.__module__.startswith("sportsdataverse.mbb.mbb_ncaa_")
