"""WNBA player props use WNBA-fitted distribution constants, not the NBA ones.

The points SD law and count dispersion were fitted on NBA 2024 box logs only, and the WNBA
shim re-exported ``prop_distribution`` / ``prob_over`` / ``team_pace_projection`` unbound, so
every WNBA prop priced with NBA spread. The WNBA row is fitted on the committed
``wnba_player_box_logs_2024.parquet`` (real ESPN capture; see the fixture README).
"""

import importlib
import importlib.util
import math
from pathlib import Path

import pytest

from sportsdataverse.nba.nba_player_props import prob_over as nba_prob_over
from sportsdataverse.nba.nba_player_props import prop_distribution as nba_prop_distribution
from sportsdataverse.nba.nba_prediction_constants import get_constants

wnba_props = importlib.import_module("sportsdataverse.wnba.wnba_player_props")

_FIT = Path(__file__).resolve().parents[2] / "tools" / "models" / "nba_prediction" / "fit_player_props.py"


def _fit(league: str) -> dict[str, float]:
    spec = importlib.util.spec_from_file_location("fit_player_props", _FIT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.fit(league)


@pytest.mark.parametrize(("league", "league_id"), [("nba", "00"), ("wnba", "10")])
def test_prop_constants_match_their_own_league_fit(league: str, league_id: str) -> None:
    # Each league's row is reproduced by refitting on that league's committed box logs.
    c = get_constants(league_id)
    fitted = _fit(league)
    assert (c.pts_sd_a, c.pts_sd_b, c.count_dispersion) == (
        fitted["pts_sd_a"],
        fitted["pts_sd_b"],
        fitted["count_dispersion"],
    )


def test_wnba_prop_distribution_uses_wnba_spread() -> None:
    c = get_constants("10")
    fam, par = wnba_props.prop_distribution(15.0, "pts")
    assert fam == "normal"
    assert par["sd"] == pytest.approx(c.pts_sd_a + c.pts_sd_b * math.sqrt(15.0))
    # ~5% narrower than the NBA law at 15 points (bootstrap 95% CIs barely touch).
    assert par["sd"] < nba_prop_distribution(15.0, "pts")[1]["sd"] - 0.2
    assert wnba_props.prob_over(15.0, 18.5, "pts") != nba_prob_over(15.0, 18.5, "pts")


def test_wnba_helpers_bind_league_10() -> None:
    for fn in (wnba_props.prop_distribution, wnba_props.prob_over, wnba_props.team_pace_projection):
        assert fn.keywords == {"league_id": "10"}, fn
