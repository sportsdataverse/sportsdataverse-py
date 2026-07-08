"""WBB player-value shims are the mbb implementations by reference."""

import functools

import pytest

from sportsdataverse.mbb.mbb_archetypes import mbb_archetypes
from sportsdataverse.mbb.mbb_box_bpm import mbb_box_bpm
from sportsdataverse.mbb.mbb_draft_projection import mbb_draft_projection
from sportsdataverse.mbb.mbb_recruiting_projection import mbb_recruiting_projection
from sportsdataverse.mbb.mbb_transfer_projection import mbb_transfer_projection
from sportsdataverse.mbb.mbb_transfer_projection import transfer_cohort as mbb_transfer_cohort
from sportsdataverse.wbb.wbb_archetypes import wbb_archetypes
from sportsdataverse.wbb.wbb_box_bpm import wbb_box_bpm
from sportsdataverse.wbb.wbb_draft_projection import wbb_draft_projection
from sportsdataverse.wbb.wbb_recruiting_projection import wbb_recruiting_projection
from sportsdataverse.wbb.wbb_transfer_projection import transfer_cohort, wbb_transfer_projection

_PAIRS = [
    (wbb_box_bpm, mbb_box_bpm),
    (wbb_archetypes, mbb_archetypes),
    (wbb_recruiting_projection, mbb_recruiting_projection),
    (wbb_transfer_projection, mbb_transfer_projection),
    (wbb_draft_projection, mbb_draft_projection),
]


@pytest.mark.parametrize(("shim", "core"), _PAIRS, ids=[c.__name__ for _, c in _PAIRS])
def test_shim_is_mbb_partial_with_womens_league(shim, core):
    assert isinstance(shim, functools.partial)
    assert shim.func is core
    assert shim.keywords == {"league": "womens"}


def test_transfer_cohort_reexported_by_reference():
    assert transfer_cohort is mbb_transfer_cohort


def test_constants_module_reexports_by_reference():
    import sportsdataverse.mbb.mbb_player_value_constants as m
    import sportsdataverse.wbb.wbb_player_value_constants as w

    for name in w.__all__:
        assert getattr(w, name) is getattr(m, name), name
    assert w.get_player_value_constants("womens").bundle_prefix == "wbb"
