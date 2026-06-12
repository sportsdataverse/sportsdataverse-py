from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.nfl.nfl_pbp import NFLPlayProcess
from tests.conftest import fetch_pbp_or_skip, skip_if_no_live


def test_nfl_pbp_missing_competitions_raises_noespndata():
    """Guard (offline): an ESPN summary with no ``header.competitions`` raises a
    clean ``NoESPNDataError`` instead of a bare ``KeyError: 'competitions'``."""
    from sportsdataverse.errors import NoESPNDataError

    proc = NFLPlayProcess(gameId=401220403)
    with pytest.raises(NoESPNDataError):
        proc._NFLPlayProcess__helper_nfl_game_data({"header": {}}, {})


@pytest.fixture()
def generated_nfl_data():
    test = NFLPlayProcess(gameId=401220403)
    fetch_pbp_or_skip(test)
    test.run_processing_pipeline()
    yield test


@pytest.fixture()
def nfl_box_score(generated_nfl_data):
    yield generated_nfl_data.create_box_score(pl.DataFrame(generated_nfl_data.plays_json, infer_schema_length=400))


def test_basic_nfl_pbp(generated_nfl_data):
    assert generated_nfl_data.json is not None

    generated_nfl_data.run_processing_pipeline()
    assert len(generated_nfl_data.plays_json) > 0
    assert generated_nfl_data.ran_pipeline == True
    assert isinstance(pl.DataFrame(generated_nfl_data.plays_json, infer_schema_length=400), pl.DataFrame)


def test_nfl_adv_box_score(nfl_box_score):
    assert nfl_box_score is not None
    assert not set(nfl_box_score.keys()).difference(
        {
            "win_pct",
            "pass",
            "team",
            "situational",
            "rush",
            "receiver",
            "defensive",
            "turnover",
            "drives",
        },
    )


def test_havoc_rate(nfl_box_score):
    defense_home = nfl_box_score["defensive"][0]
    passes_defended = defense_home.get("pass_breakups", 0)
    # The defensive box exposes interceptions as `def_int` (the `Int` field
    # lives on the turnover box, not the defensive box). Reading `Int`
    # silently defaulted to 0, masking the missing field.
    home_int = defense_home.get("def_int", 0)
    tfl = defense_home.get("TFL", 0)
    fum = defense_home.get("fumbles", 0)
    plays = defense_home.get("scrimmage_plays", 0)

    assert plays > 0
    # `havoc_total` counts UNIQUE plays with any havoc flag; the category
    # sum multi-counts plays carrying multiple flags simultaneously. The
    # correct relationship is bounded:
    #   max(category) <= havoc_total <= sum(categories)
    # Equality with the sum holds only when no play has multiple flags.
    category_sum = passes_defended + home_int + tfl + fum
    assert defense_home["havoc_total"] <= category_sum
    assert defense_home["havoc_total"] >= max(passes_defended, home_int, tfl, fum)
    assert round(defense_home["havoc_total_rate"], 4) == round(defense_home["havoc_total"] / plays, 4)


@skip_if_no_live
def test_modern_nfl_game_gets_real_spread_not_default():
    """NFL has the same pickcenter gap as CFB. The modern core-odds endpoint
    (``sports.core.api.espn.com/v2/.../leagues/nfl/events/{gid}/competitions/{gid}/odds``)
    has 14 items for ``401547500``, so the cascade should pull a real
    spread / total instead of the defaults ``(2.5, 55.0/55.5, True)``
    when the legacy ``pickcenter`` array is empty.
    """
    test = NFLPlayProcess(gameId=401547500)
    fetch_pbp_or_skip(test)
    test.run_processing_pipeline()
    assert test.ran_pipeline is True
    assert len(test.plays_json) > 0
    assert test.gameSpreadAvailable is True
    # If the modern fallback fired (legacy was empty), the spread/total
    # MUST not equal the defaults. If the legacy path fired (legacy was
    # populated), the values are real anyway. Either way they should not
    # be the sentinel defaults.
    assert float(test.gameSpread) != 2.5 or float(test.overUnder) not in (55.0, 55.5)


@skip_if_no_live
def test_modern_nfl_game_pbp_handles_python_float_overUnder():
    """Regression guard: NFL's ``__helper_nfl_pbp_features`` previously
    called ``init["overUnder"].astype(float)`` which fails on a Python
    float. With the defensive cast (mirroring the cfb_pbp version),
    running the full pipeline on a recent game must not raise.
    """
    test = NFLPlayProcess(gameId=401547500)
    fetch_pbp_or_skip(test)
    test.run_processing_pipeline()  # must not raise
    assert test.ran_pipeline is True
    assert len(test.plays_json) > 0
