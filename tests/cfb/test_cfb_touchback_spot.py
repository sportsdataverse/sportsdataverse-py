"""The CFB kickoff-spot helpers keep their own names at the top level (PR #829 review, R829-3).

``sportsdataverse/__init__.py`` star-imports ``cfb`` before ``nfl``, so a CFB helper named
``touchback_yardline_expr`` would sit beside NFL's same-named one and a CFB caller importing it from the top
level would silently get NFL's 2016 cut.
"""

import polars as pl

import sportsdataverse
from sportsdataverse.cfb import model_vars as cfb_model_vars
from sportsdataverse.nfl import model_vars as nfl_model_vars


def _spots(expr_fn, seasons):
    df = pl.DataFrame({"season": seasons})
    return df.select(expr_fn(pl.col("season"))).to_series().to_list()


def test_top_level_names_resolve_to_their_own_league():
    assert sportsdataverse.cfb_touchback_yardline_expr is cfb_model_vars.cfb_touchback_yardline_expr
    assert sportsdataverse.cfb_kickoff_out_of_bounds_spot_expr is cfb_model_vars.cfb_kickoff_out_of_bounds_spot_expr
    assert sportsdataverse.touchback_yardline_expr is nfl_model_vars.touchback_yardline_expr


def test_cfb_spots_follow_the_2012_rule():
    seasons = [2011, 2012, 2014, 2015]
    assert _spots(sportsdataverse.cfb_touchback_yardline_expr, seasons) == [80, 75, 75, 75]
    assert _spots(sportsdataverse.cfb_kickoff_out_of_bounds_spot_expr, seasons) == [40, 35, 35, 35]
    # NFL's twin cuts at 2016, so 2012-2015 is where a mix-up would show
    assert _spots(sportsdataverse.touchback_yardline_expr, seasons) == [80, 80, 80, 80]
