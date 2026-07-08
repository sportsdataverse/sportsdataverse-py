"""WBB shot-quality shims are the mbb implementations by reference."""

import functools

import polars as pl
import pytest

from sportsdataverse.mbb.mbb_shooter_talent import mbb_shooter_talent
from sportsdataverse.mbb.mbb_shot_quality import mbb_shot_quality, mbb_shot_quality_model
from sportsdataverse.mbb.mbb_shot_selection import mbb_shot_selection
from sportsdataverse.mbb.mbb_shots_adapter import mbb_shot_data
from sportsdataverse.wbb.wbb_shooter_talent import wbb_shooter_talent
from sportsdataverse.wbb.wbb_shot_quality import wbb_shot_quality, wbb_shot_quality_model
from sportsdataverse.wbb.wbb_shot_selection import wbb_shot_selection
from sportsdataverse.wbb.wbb_shots_adapter import wbb_shot_data

_PAIRS = [
    (wbb_shot_data, mbb_shot_data),
    (wbb_shot_quality_model, mbb_shot_quality_model),
    (wbb_shot_quality, mbb_shot_quality),
    (wbb_shot_selection, mbb_shot_selection),
    (wbb_shooter_talent, mbb_shooter_talent),
]


@pytest.mark.parametrize(("shim", "core"), _PAIRS, ids=[c.__name__ for _, c in _PAIRS])
def test_shim_is_mbb_partial_with_womens_league(shim, core):
    assert isinstance(shim, functools.partial)
    assert shim.func is core
    assert shim.keywords == {"league": "womens"}


def test_constants_shim_reexports_by_reference():
    import sportsdataverse.mbb.mbb_shot_quality_constants as m
    import sportsdataverse.wbb.wbb_shot_quality_constants as w

    for name in w.__all__:
        assert getattr(w, name) is getattr(m, name), name


def test_wbb_shim_runs_on_synthetic_input():
    shots = pl.DataFrame(
        {
            "shot_zone": ["rim"] * 20,
            "shot_type": ["rim"] * 20,
            "made": [True] * 11 + [False] * 9,
            "point_value": [2] * 20,
        }
    ).with_columns(pl.col("point_value").cast(pl.Int8))
    m = wbb_shot_quality_model(shots)
    assert m.height == 1
    scored = wbb_shot_quality(shots, model=m)
    assert scored.get_column("xpoints").null_count() == 0
