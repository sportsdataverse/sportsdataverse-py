"""The WNBA league_id bindings keep their NBA core's Returns section.

Each is ``functools.partial(nba_fn, league_id="10")`` with a one-line ``__doc__`` pointing at the
NBA function. The partial returns exactly what the core returns, so the binding repeats the core's
``Returns:`` section instead of dropping it.
"""

from __future__ import annotations

import importlib
import inspect
import re

import pytest

_BINDINGS = [
    ("wnba_clutch", "wnba_team_clutch", "nba_clutch", "nba_team_clutch"),
    ("wnba_game_predict", "wnba_predict_games", "nba_game_predict", "nba_predict_games"),
    ("wnba_game_predict", "wnba_in_game_win_prob", "nba_game_predict", "nba_in_game_win_prob"),
    ("wnba_game_predict", "wnba_predict_margin", "nba_game_predict", "predict_margin"),
    ("wnba_game_predict", "wnba_predict_total", "nba_game_predict", "predict_total"),
    ("wnba_game_predict", "wnba_win_prob_from_margin", "nba_game_predict", "win_prob_from_margin"),
    ("wnba_game_predict", "wnba_expected_possessions", "nba_game_predict", "expected_possessions"),
    ("wnba_player_props", "wnba_player_props", "nba_player_props", "nba_player_props"),
    ("wnba_team_ratings", "wnba_team_ratings", "nba_team_ratings", "nba_team_ratings"),
]


def _returns(doc: str) -> str:
    m = re.search(r"(?ms)^Returns:\n.*?(?=^\S|\Z)", doc)
    return m.group(0).rstrip() if m else ""


@pytest.mark.parametrize(("mod", "name", "core_mod", "core_name"), _BINDINGS)
def test_the_binding_repeats_its_cores_returns_section(mod, name, core_mod, core_name):
    binding = getattr(importlib.import_module(f"sportsdataverse.wnba.{mod}"), name)
    core = getattr(importlib.import_module(f"sportsdataverse.nba.{core_mod}"), core_name)
    doc = inspect.getdoc(binding) or ""
    assert doc.startswith("WNBA ")
    assert _returns(doc) and _returns(doc) == _returns(inspect.getdoc(core) or "")
