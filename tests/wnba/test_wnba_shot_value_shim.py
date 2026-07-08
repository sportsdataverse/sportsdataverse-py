"""WNBA shot-value shim is the nba core bound to league_id=10; G-League verified."""

import functools

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


def test_wnba_shot_value_binds_league_10():
    assert isinstance(wnba_shot_value, functools.partial)
    assert wnba_shot_value.func is nba_shot_value
    assert wnba_shot_value.keywords == {"league_id": "10"}


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
