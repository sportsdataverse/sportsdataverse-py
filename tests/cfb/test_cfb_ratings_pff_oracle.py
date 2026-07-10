"""PFF-grade external-validity oracle for cfb_ratings (T2.1 addendum).

A third published peer for the efficiency ratings, alongside ESPN FPI and SP+
(``test_cfb_ratings_oracle.py``). PFF team grades are a fundamentally different
measurement — aggregated player grades, not play-level EPA — so they agree with
``adj_net`` more loosely than FPI/SP+ do with each other (~0.80 vs the ~0.92 the
efficiency ratings hit against FPI/SP+); that looser-but-clear agreement is the
point of a *diverse* oracle.

Fixture: ``pff_team_grades_2023.parquet`` — PFF ``teams/overview`` (league=ncaa,
season 2023) parsed via the shipped ``parse_pff_report`` and name-bridged to the
ESPN ``team_id`` (see ``tests/fixtures/cfb_pff/README.md``). Ratings are built
from the committed ``pbp_2023_sample.parquet`` — the same offline source the
FPI/SP+ oracle uses. Rule: never lower a gate to pass — debug the model instead.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_prediction_constants import spearman_corr
from sportsdataverse.cfb.cfb_ratings import efficiency_ratings

_FIX = Path(__file__).resolve().parents[1] / "fixtures"
_PBP = _FIX / "cfb_prediction" / "pbp_2023_sample.parquet"
_PFF = _FIX / "cfb_pff" / "pff_team_grades_2023.parquet"

pytestmark = pytest.mark.skipif(not (_PBP.exists() and _PFF.exists()), reason="cfb_pff / pbp fixtures not present")


def _joined() -> pl.DataFrame:
    ratings = efficiency_ratings(pl.read_parquet(_PBP))
    pff = pl.read_parquet(_PFF)
    assert ratings.schema["team_id"] == pff.schema["team_id"] == pl.Utf8
    j = ratings.join(pff, on="team_id", how="inner")
    assert j.height >= 100, f"PFF oracle join matched only {j.height} FBS teams"
    return j


def test_adj_net_tracks_pff_overall() -> None:
    """Net EPA vs PFF overall team grade (observed 0.802). Floor 0.75."""
    j = _joined()
    r = spearman_corr(j["adj_net"].to_numpy(), j["pff_overall"].to_numpy())
    assert r >= 0.75, r


def test_adj_off_tracks_pff_offense() -> None:
    """Offensive EPA vs PFF offense grade (observed 0.794; both higher=better). Floor 0.72."""
    j = _joined()
    r = spearman_corr(j["adj_off_epa"].to_numpy(), j["pff_offense"].to_numpy())
    assert r >= 0.72, r


def test_adj_def_inversely_tracks_pff_defense() -> None:
    """Defensive EPA-allowed vs PFF defense grade (observed -0.669).

    The correlation is NEGATIVE by construction and that is the pass condition:
    ``adj_def_epa`` is EPA *allowed* per play (lower = better defense) while the
    PFF defense grade is higher = better, so a strong negative correlation means
    the two agree on defensive quality. A positive value here would be the bug.
    """
    j = _joined()
    r = spearman_corr(j["adj_def_epa"].to_numpy(), j["pff_defense"].to_numpy())
    assert r <= -0.55, r
