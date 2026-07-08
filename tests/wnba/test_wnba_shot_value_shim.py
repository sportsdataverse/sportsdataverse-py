"""WNBA shot-value shim is the nba core bound to league_id=10; G-League verified."""

import importlib

from sportsdataverse.nba.nba_shot_value import (
    make_prob_by_context as nba_make_prob_by_context,
)
from sportsdataverse.nba.nba_shot_value import (
    nba_shot_value,
)
from sportsdataverse.nba.nba_shot_value import (
    score_shot_xpoints as nba_score_shot_xpoints,
)
from sportsdataverse.nba.nba_shot_value_constants import get_court
from sportsdataverse.wnba.wnba_shot_value import (
    make_prob_by_context,
    score_shot_xpoints,
    wnba_shot_value,
)


def test_wnba_shot_value_binds_league_10(monkeypatch):
    # thin wrapper: delegates to the nba core with league_id="10" (offline spy)
    seen = {}

    def _spy(player_ids, season, *, league_id="00", **kw):
        seen.update(player_ids=player_ids, season=season, league_id=league_id, **kw)
        return {}

    # __init__ star-export rebinds `wnba_shot_value` on the package, shadowing the
    # submodule attr — resolve the real module via importlib to patch its import.
    wnba_mod = importlib.import_module("sportsdataverse.wnba.wnba_shot_value")
    monkeypatch.setattr(wnba_mod, "nba_shot_value", _spy)
    wnba_shot_value([1628886], "2024", include_context=True)
    assert seen == {
        "player_ids": [1628886],
        "season": "2024",
        "league_id": "10",
        "include_context": True,
        "return_as_pandas": False,
    }


def test_model_functions_reexported_by_reference():
    assert score_shot_xpoints is nba_score_shot_xpoints
    assert make_prob_by_context is nba_make_prob_by_context


def test_court_geometry_league_differences():
    # WNBA differs from NBA; G-League court == NBA court
    assert get_court("10").corner3_loc_x_abs != get_court("00").corner3_loc_x_abs
    assert get_court("10").three_point_radius_ft != get_court("00").three_point_radius_ft
    assert get_court("20").rim_radius_ft == get_court("00").rim_radius_ft
    assert get_court("20").corner3_loc_x_abs == get_court("00").corner3_loc_x_abs


def test_gleague_path_is_nba_core():
    # G-League is not a shim — it's nba_shot_value with league_id="20"
    import inspect

    assert "league_id" in inspect.signature(nba_shot_value).parameters
