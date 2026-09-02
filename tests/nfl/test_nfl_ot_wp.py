"""Overtime WP overlay — nflfastR ``add_wp_variables`` L820-899.

Why this file exists
--------------------
nflfastR **never scores overtime with the WP boosters.**  It splits the frame
before prediction: the models run on ``qtr <= 4`` and ``qtr > 4`` gets a closed
form off the EP class probabilities, with ``vegas_wp`` assigned the same value
as ``wp`` (the spread model is not consulted at all).  sdv-py scored overtime
with the boosters, which is a model extrapolation outside the domain nflverse
ever asks it about.

Measured on real data before the overlay (2025, ``enrich_nfl_pbp`` on the
published nflverse frame vs nflverse's own values, n = 324 overtime rows):
``wp`` MAE **0.1731**, bias **-0.1308**, p90 |err| **0.4187**, r **0.379**;
our ``vegas_wp == wp`` on **0.000 %** of overtime rows against nflverse's
**96.6 %**.

Why sudden-death only
---------------------
The R source also carries a ``One_FG_WP`` branch for the first overtime drive
from 2012.  It is deliberately **not** implemented, and that is a measured
decision rather than an omission.  Upstream's ``First_Drive`` is a minimum over
every overtime row in whatever multi-game batch ``add_wp_variables`` was
handed, so the branch is close to dead in the data nflverse publishes, and
every deterministic reconstruction of it scores worse.  Overtime ``wp`` MAE vs
nflverse, same harness:

===================================  ======  ======  ======
``First_Drive`` scoping               2016    2022    2025
===================================  ======  ======  ======
per overtime game (the R intent)     0.1609  0.1102  0.1431
frame-global over overtime rows      0.0394  0.0503  0.0594
sudden death only (shipped)          0.0513  0.0534  0.0599
===================================  ======  ======  ======

The frame-global row wins, but it is frame-dependent — enriching one game gives
a different ``wp`` than enriching the season containing it, and
``build_nfl_season`` enriches per game, under which it collapses to the worst
row.  Among deterministic, per-game-stable options sudden death wins by 2-3x.
Narrower One-FG gates were tried and are also worse (trailing-by-3 clause
only: 0.0735 / 0.0585 / 0.0809).

What is pinned
--------------
* the closed form, exactly, on every overtime row;
* ``vegas_wp == wp`` inside overtime and ONLY inside overtime;
* regulation rows are untouched and row order is preserved;
* the overlay demonstrably CHANGES overtime output (a no-op implementation
  fails ``test_overlay_actually_moves_overtime_wp``);
* a missing input WARNS instead of silently skipping;
* the real-data gate, and that it FAILS without the overlay.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nfl.ep_wp import (
    _EP_CLASS_NAMES,
    OT_WP_EP_CLASSES,
    _apply_ot_wp_overlay,
    enrich_nfl_pbp,
)

# EP class probabilities held fixed so the closed form has an exact expected
# value.  Order matches _EP_CLASS_NAMES.
_TD, _OPP_TD, _FG, _OPP_FG, _SAF, _OPP_SAF, _NS = 0.31, 0.19, 0.22, 0.14, 0.02, 0.01, 0.11
_SUDDEN_DEATH = _FG + _TD + _SAF  # 0.55


def _ot_frame(
    *,
    season: int = 2023,
    drives: tuple[float, ...] = (21.0, 22.0, 23.0),
    score_differential: tuple[float, ...] | None = None,
    qtr: tuple[float, ...] | None = None,
) -> pl.DataFrame:
    """A minimal post-``calculate_win_probability`` frame, one game, N rows."""
    n = len(drives)
    score_differential = score_differential if score_differential is not None else (0.0,) * n
    qtr = qtr if qtr is not None else (5.0,) * n
    assert len(score_differential) == n and len(qtr) == n
    return pl.DataFrame(
        {
            "game_id": [f"{season}_01_AAA_BBB"] * n,
            "season": [season] * n,
            "qtr": list(qtr),
            "drive": list(drives),
            "score_differential": list(score_differential),
            "td_prob": [_TD] * n,
            "opp_td_prob": [_OPP_TD] * n,
            "fg_prob": [_FG] * n,
            "opp_fg_prob": [_OPP_FG] * n,
            "safety_prob": [_SAF] * n,
            "opp_safety_prob": [_OPP_SAF] * n,
            "no_score_prob": [_NS] * n,
            # Placeholder booster output the overlay must overwrite.  Deliberately
            # far from any plausible closed-form value so the "did it move"
            # assertion cannot pass by coincidence.
            "wp": [0.999] * n,
            "vegas_wp": [0.001] * n,
        }
    )


# ---------------------------------------------------------------------------
# The closed form
# ---------------------------------------------------------------------------


def test_the_three_classes_are_the_posteam_scores_next_classes():
    assert OT_WP_EP_CLASSES == ("fg_prob", "td_prob", "safety_prob")
    assert set(OT_WP_EP_CLASSES) <= set(_EP_CLASS_NAMES)


def test_every_overtime_row_gets_the_sudden_death_closed_form():
    out = _apply_ot_wp_overlay(_ot_frame())
    assert np.allclose(out["wp"].to_numpy(), _SUDDEN_DEATH, atol=1e-12)


@pytest.mark.parametrize("season", [2005, 2011, 2012, 2017, 2022, 2025])
def test_the_form_does_not_vary_by_season_drive_or_score(season):
    """The One-FG branch is deliberately unimplemented — see the module docstring.

    If someone re-adds it, this fails and they have to re-read why it was left
    out, rather than discovering the 2-3x parity regression in production.
    """
    out = _apply_ot_wp_overlay(_ot_frame(season=season, drives=(21.0, 22.0), score_differential=(0.0, -3.0)))
    assert np.allclose(out["wp"].to_numpy(), _SUDDEN_DEATH, atol=1e-12)


def test_the_form_does_not_depend_on_which_rows_share_the_frame():
    """Deterministic per game: another game's rows cannot move a value.

    This is the property the frame-global ``First_Drive`` variant lacks, and the
    reason it is not shipped despite scoring marginally better against nflverse.
    """
    alone = _apply_ot_wp_overlay(_ot_frame())["wp"].to_list()
    other = _ot_frame(drives=(25.0, 26.0, 27.0)).with_columns(pl.lit("2023_02_CCC_DDD").alias("game_id"))
    together = _apply_ot_wp_overlay(pl.concat([_ot_frame(), other]))["wp"].to_list()
    assert together[:3] == alone
    assert together[3:] == alone


def test_regulation_drive_numbers_in_the_frame_change_nothing():
    """The production shape a unit fixture of overtime-only rows cannot represent.

    A real frame from ``enrich_nfl_pbp`` carries the whole game, so any rule
    keyed on a per-``game_id`` drive minimum reads the opening REGULATION drive
    rather than the first overtime drive — the defect the first cut of this
    overlay shipped with, invisible to an overtime-only fixture because there
    the two minima coincide.  The form that ships is drive-independent, so the
    overtime values must be identical with and without regulation rows present.
    """
    ot = _ot_frame(drives=(21.0, 22.0, 23.0))
    reg = _ot_frame(drives=(1.0, 2.0, 3.0), qtr=(1.0, 1.0, 2.0))
    with_reg = _apply_ot_wp_overlay(pl.concat([reg, ot]))
    assert with_reg["wp"].to_list()[:3] == [0.999, 0.999, 0.999]
    assert with_reg["wp"].to_list()[3:] == _apply_ot_wp_overlay(ot)["wp"].to_list()


# ---------------------------------------------------------------------------
# vegas_wp identity, scope, and the silent-no-op guard
# ---------------------------------------------------------------------------


def test_vegas_wp_equals_wp_inside_overtime():
    out = _apply_ot_wp_overlay(_ot_frame())
    assert np.allclose(out["wp"].to_numpy(), out["vegas_wp"].to_numpy(), atol=0.0, rtol=0.0)


def test_regulation_rows_are_untouched():
    out = _apply_ot_wp_overlay(_ot_frame(qtr=(4.0, 5.0, 5.0)))
    assert out["wp"][0] == 0.999
    assert out["vegas_wp"][0] == 0.001
    assert out["wp"][2] == pytest.approx(_SUDDEN_DEATH, abs=1e-12)


def test_a_frame_with_no_overtime_is_returned_unchanged():
    df = _ot_frame(qtr=(1.0, 2.0, 4.0))
    assert _apply_ot_wp_overlay(df).equals(df)


def test_row_order_is_preserved():
    df = pl.concat(
        [
            _ot_frame(drives=(1.0, 2.0), qtr=(1.0, 2.0)),
            _ot_frame(drives=(21.0, 22.0, 23.0)),
            _ot_frame(drives=(4.0,), qtr=(4.0,)),
        ]
    ).with_columns(pl.arange(0, 6).alias("_probe"))
    out = _apply_ot_wp_overlay(df)
    assert out["_probe"].to_list() == list(range(6))
    assert out["qtr"].to_list() == [1.0, 2.0, 5.0, 5.0, 5.0, 4.0]


def test_overlay_actually_moves_overtime_wp():
    """The silent-no-op assertion: assert the OUTPUT changed, not that it ran."""
    df = _ot_frame()
    out = _apply_ot_wp_overlay(df)
    assert bool(((out["wp"] - df["wp"]).abs() > 0.01).all())
    assert bool(((out["vegas_wp"] - df["vegas_wp"]).abs() > 0.01).all())


@pytest.mark.parametrize("missing", ["wp", "vegas_wp", "fg_prob", "td_prob", "safety_prob"])
def test_missing_input_warns_rather_than_silently_skipping(missing):
    df = _ot_frame().drop(missing)
    with pytest.warns(RuntimeWarning, match=f"overtime WP overlay skipped.*{missing}"):
        out = _apply_ot_wp_overlay(df)
    assert out.equals(df)


# ---------------------------------------------------------------------------
# Real-data gate — five whole overtime games, one per rules era
# ---------------------------------------------------------------------------

_FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "nfl_ep_wp" / "overtime_games.parquet"

#: Thresholds for the fixture's 96 paired overtime rows, set from the values
#: observed 2026-09-02 with the overlay in place — MAE **0.0209**, bias
#: **-0.0149**, r **0.9945** — each placed strictly on the passing side of the
#: observation and strictly inside the un-overlaid values (MAE **0.1677**,
#: bias **+0.0225**, r **0.4996**), so they detect a regression rather than
#: restate the present.  ``test_the_gate_bites_without_the_overlay`` proves the
#: second half.
#:
#: MAE and r are the criteria that bite: the un-overlaid bias (+0.0225) already
#: sits inside the bias ceiling, so bias alone would not catch a no-op.  That is
#: why the bite test asserts on the MAE ceiling and the r floor specifically.
OT_FIXTURE_MAE_CEILING = 0.035
OT_FIXTURE_ABS_BIAS_CEILING = 0.030
OT_FIXTURE_R_FLOOR = 0.98


def _enrich(df: pl.DataFrame, models_dir: str) -> pl.DataFrame:
    """Run the production nflverse path over the fixture (no network).

    ``models_dir`` points at an empty directory so the 34 MB xYAC model is
    skipped gracefully instead of downloaded.
    """
    base = df.drop([c for c in ("ep", "epa", "wp", "vegas_wp", "wpa", *_EP_CLASS_NAMES) if c in df.columns])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return enrich_nfl_pbp(base, method="lead_diff", add_fourth_down=False, models_dir=models_dir)


def _ot_scores(df: pl.DataFrame, out: pl.DataFrame, col: str, oracle: str) -> tuple[int, float, float, float]:
    ot = out.with_columns(df[oracle]).filter(
        (pl.col("qtr") >= 5) & pl.col(col).is_finite() & pl.col(oracle).is_finite()
    )
    a, b = ot[col].to_numpy(), ot[oracle].to_numpy()
    d = a - b
    return len(d), float(np.abs(d).mean()), float(d.mean()), float(np.corrcoef(a, b)[0, 1])


@pytest.fixture(scope="module")
def ot_games() -> pl.DataFrame:
    if not _FIXTURE.exists():  # pragma: no cover - fixture ships with the repo
        pytest.skip(f"missing fixture {_FIXTURE}")
    return pl.read_parquet(_FIXTURE)


def test_real_overtime_matches_nflverse(ot_games, tmp_path):
    out = _enrich(ot_games, str(tmp_path))
    for col, oracle in (("wp", "nflverse_wp"), ("vegas_wp", "nflverse_vegas_wp")):
        n, mae, bias, r = _ot_scores(ot_games, out, col, oracle)
        assert n >= 90, f"{col}: only {n} paired overtime rows"
        assert mae <= OT_FIXTURE_MAE_CEILING, f"{col} MAE {mae:.4f}"
        assert abs(bias) <= OT_FIXTURE_ABS_BIAS_CEILING, f"{col} bias {bias:+.4f}"
        assert r >= OT_FIXTURE_R_FLOOR, f"{col} r {r:.4f}"


def test_real_overtime_vegas_wp_equals_wp(ot_games, tmp_path):
    """nflverse holds this on 96.6 % of published 2025 overtime rows, against
    0.033 % in regulation.  We hold it exactly, because we never run the spread
    model in overtime."""
    out = _enrich(ot_games, str(tmp_path))
    ot = out.filter((pl.col("qtr") >= 5) & pl.col("wp").is_not_null() & pl.col("vegas_wp").is_not_null())
    assert ot.height >= 90
    assert bool((ot["wp"] - ot["vegas_wp"]).abs().lt(1e-12).all())

    reg = out.filter((pl.col("qtr") <= 4) & pl.col("wp").is_not_null() & pl.col("vegas_wp").is_not_null())
    assert float((reg["wp"] - reg["vegas_wp"]).abs().lt(1e-12).mean()) < 0.01


def test_the_gate_bites_without_the_overlay(ot_games, tmp_path, monkeypatch):
    """A no-op overlay must FAIL the gate — otherwise the gate proves nothing."""
    import sportsdataverse.nfl.ep_wp as _ep_wp

    monkeypatch.setattr(_ep_wp, "_apply_ot_wp_overlay", lambda d: d)
    out = _enrich(ot_games, str(tmp_path))
    n, mae, bias, r = _ot_scores(ot_games, out, "wp", "nflverse_wp")
    assert n >= 90
    assert mae > OT_FIXTURE_MAE_CEILING, f"un-overlaid MAE {mae:.4f} would have passed the ceiling"
    assert r < OT_FIXTURE_R_FLOOR, f"un-overlaid r {r:.4f} would have passed the floor"
