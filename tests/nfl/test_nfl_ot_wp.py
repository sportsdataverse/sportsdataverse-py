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
**96.6 %**.  After: MAE **0.0353**, bias **-0.0195**, p90 **0.0860**,
r **0.938**, identity **100 %**.  All-plays parity improved too
(MAE 0.0145 -> 0.0135), so nothing was traded away for it.

What is pinned
--------------
* the two closed forms and the branch rule, exactly;
* ``vegas_wp == wp`` inside overtime and ONLY inside overtime;
* regulation rows are byte-identical with and without the overlay;
* the overlay demonstrably CHANGES overtime output (a no-op implementation
  fails ``test_overlay_actually_moves_overtime_wp``);
* a missing input WARNS instead of silently skipping;
* ``Win_Back`` is scored at the play's own yardline — nflfastR's
  ``overtime_df_ko$yrdline100`` write (L855) targets a column
  ``ep_model_select()`` does not read, so the touchback substitution is dead
  upstream and reproducing the *effective* behaviour is what matches the
  published data.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nfl.ep_wp import (
    OT_ONE_FG_FIRST_YEAR,
    _EP_CLASS_NAMES,
    _apply_ot_wp_overlay,
    calculate_expected_points,
    enrich_nfl_pbp,
)

# EP class probabilities held fixed so the closed forms have exact expected
# values.  Order matches _EP_CLASS_NAMES.
_TD, _OPP_TD, _FG, _OPP_FG, _SAF, _OPP_SAF, _NS = 0.31, 0.19, 0.22, 0.14, 0.02, 0.01, 0.11
_SUDDEN_DEATH = _FG + _TD + _SAF  # 0.55


def _ot_frame(
    *,
    season: int = 2023,
    drives: tuple[float, ...] = (21.0, 22.0, 23.0),
    score_differential: tuple[float, ...] = (0.0, 0.0, 0.0),
    qtr: tuple[float, ...] = (5.0, 5.0, 5.0),
    yardline_100: float = 75.0,
) -> pl.DataFrame:
    """A minimal post-``calculate_win_probability`` frame, one game, N rows."""
    n = len(drives)
    return pl.DataFrame(
        {
            "game_id": [f"{season}_01_AAA_BBB"] * n,
            "season": [season] * n,
            "qtr": list(qtr),
            "drive": list(drives),
            "score_differential": list(score_differential),
            "half_seconds_remaining": [600.0] * n,
            "game_seconds_remaining": [600.0] * n,
            "yardline_100": [yardline_100] * n,
            "ydstogo": [10.0] * n,
            "down": [1] * n,
            "posteam": ["AAA"] * n,
            "home_team": ["BBB"] * n,
            "roof": ["outdoors"] * n,
            "posteam_timeouts_remaining": [2] * n,
            "defteam_timeouts_remaining": [2] * n,
            "spread_line": [-3.0] * n,
            "receive_2h_ko": [0] * n,
            "td_prob": [_TD] * n,
            "opp_td_prob": [_OPP_TD] * n,
            "fg_prob": [_FG] * n,
            "opp_fg_prob": [_OPP_FG] * n,
            "safety_prob": [_SAF] * n,
            "opp_safety_prob": [_OPP_SAF] * n,
            "no_score_prob": [_NS] * n,
            # Placeholder booster output the overlay must overwrite.  Deliberately far
            # from any plausible closed-form value so the "did it move" assertion
            # cannot pass by coincidence.
            "wp": [0.999] * n,
            "vegas_wp": [0.001] * n,
        }
    )


def _win_back(df: pl.DataFrame) -> np.ndarray:
    """Independent recomputation of nflfastR's ``Win_Back`` for the same rows."""
    scored = calculate_expected_points(
        df.with_columns(
            pl.lit(1, dtype=pl.Int64).alias("down"),
            pl.lit(10, dtype=pl.Int64).alias("ydstogo"),
        )
    )
    return (
        scored["no_score_prob"] + scored["opp_fg_prob"] + scored["opp_safety_prob"] + scored["opp_td_prob"]
    ).to_numpy()


# ---------------------------------------------------------------------------
# The two closed forms and the branch rule
# ---------------------------------------------------------------------------


def test_sudden_death_form_on_a_later_overtime_drive():
    # drives 21/22/23 -> drive_diff 0/1/2; row 2 (diff 2) can only be sudden death.
    out = _apply_ot_wp_overlay(_ot_frame())
    assert out["wp"][2] == pytest.approx(_SUDDEN_DEATH, abs=1e-12)


def test_one_fg_form_on_the_first_overtime_drive():
    df = _ot_frame()
    out = _apply_ot_wp_overlay(df)
    expected = _TD + _FG * _win_back(df)[0]
    assert out["wp"][0] == pytest.approx(expected, abs=1e-12)
    # and it is genuinely a different number from the sudden-death form
    assert abs(out["wp"][0] - _SUDDEN_DEATH) > 0.01


def test_one_fg_form_on_the_second_drive_only_when_trailing_by_exactly_three():
    trailing = _apply_ot_wp_overlay(_ot_frame(score_differential=(0.0, -3.0, 0.0)))
    assert trailing["wp"][1] != pytest.approx(_SUDDEN_DEATH, abs=1e-9)

    for other in (-2.0, -4.0, 3.0, 0.0):
        out = _apply_ot_wp_overlay(_ot_frame(score_differential=(0.0, other, 0.0)))
        assert out["wp"][1] == pytest.approx(_SUDDEN_DEATH, abs=1e-12), other


def test_pre_2012_seasons_always_use_sudden_death():
    out = _apply_ot_wp_overlay(_ot_frame(season=OT_ONE_FG_FIRST_YEAR - 1))
    assert np.allclose(out["wp"].to_numpy(), _SUDDEN_DEATH, atol=1e-12)

    on = _apply_ot_wp_overlay(_ot_frame(season=OT_ONE_FG_FIRST_YEAR))
    assert on["wp"][0] != pytest.approx(_SUDDEN_DEATH, abs=1e-9)


def test_first_drive_is_scoped_per_game_not_across_the_frame():
    """Two games whose overtimes start at different drive numbers."""
    a = _ot_frame(drives=(21.0, 22.0, 23.0))
    b = _ot_frame(drives=(25.0, 26.0, 27.0)).with_columns(pl.lit("2023_02_CCC_DDD").alias("game_id"))
    out = _apply_ot_wp_overlay(pl.concat([a, b]))
    # Row 3 is game B's first overtime drive.  Under a frame-global minimum it
    # would be drive_diff 4 (sudden death); per game it is 0 (One-FG).
    assert out["wp"][3] != pytest.approx(_SUDDEN_DEATH, abs=1e-9)
    assert out["wp"][3] == pytest.approx(out["wp"][0], abs=1e-12)


# ---------------------------------------------------------------------------
# vegas_wp identity, scope, and the silent-no-op guard
# ---------------------------------------------------------------------------


def test_vegas_wp_equals_wp_inside_overtime():
    out = _apply_ot_wp_overlay(_ot_frame())
    assert np.allclose(out["wp"].to_numpy(), out["vegas_wp"].to_numpy(), atol=0.0, rtol=0.0)


def test_regulation_rows_are_untouched():
    df = _ot_frame(qtr=(4.0, 5.0, 5.0))
    out = _apply_ot_wp_overlay(df)
    assert out["wp"][0] == 0.999
    assert out["vegas_wp"][0] == 0.001
    # ...and the overtime rows next to them still moved
    assert out["wp"][2] == pytest.approx(_SUDDEN_DEATH, abs=1e-12)


def test_a_frame_with_no_overtime_is_returned_unchanged():
    df = _ot_frame(qtr=(1.0, 2.0, 4.0))
    out = _apply_ot_wp_overlay(df)
    assert out.equals(df)


def test_overlay_actually_moves_overtime_wp():
    """The silent-no-op assertion: assert the OUTPUT changed, not that it ran."""
    df = _ot_frame()
    out = _apply_ot_wp_overlay(df)
    moved = (out["wp"] - df["wp"]).abs()
    assert bool((moved > 0.01).all()), moved.to_list()
    vmoved = (out["vegas_wp"] - df["vegas_wp"]).abs()
    assert bool((vmoved > 0.01).all()), vmoved.to_list()


@pytest.mark.parametrize("missing", ["drive", "score_differential", "fg_prob"])
def test_missing_input_warns_rather_than_silently_skipping(missing):
    df = _ot_frame().drop(missing)
    with pytest.warns(RuntimeWarning, match=f"overtime WP overlay skipped.*{missing}"):
        out = _apply_ot_wp_overlay(df)
    assert out.equals(df)


# ---------------------------------------------------------------------------
# The upstream dead-code decision, pinned
# ---------------------------------------------------------------------------


def test_win_back_uses_the_plays_own_yardline_not_the_touchback_spot():
    """nflfastR's ``overtime_df_ko$yrdline100 <- ...`` (L855) is dead.

    ``ep_model_select()`` reads ``yardline_100``; the R line writes
    ``yrdline100``, so the kickoff-spot substitution never reaches the EP
    model and ``Win_Back`` is evaluated at the play's own field position.  If
    someone "fixes" the port to substitute 75, the One-FG value moves and this
    test fails — which is the point.
    """
    near = _ot_frame(yardline_100=20.0)
    far = _ot_frame(yardline_100=90.0)
    wp_near = _apply_ot_wp_overlay(near)["wp"][0]
    wp_far = _apply_ot_wp_overlay(far)["wp"][0]

    # own-yardline semantics => the two differ
    assert abs(wp_near - wp_far) > 0.01

    # and each equals td_prob + fg_prob * Win_Back(own yardline)
    assert wp_near == pytest.approx(_TD + _FG * _win_back(near)[0], abs=1e-12)
    assert wp_far == pytest.approx(_TD + _FG * _win_back(far)[0], abs=1e-12)

    # a touchback-substituted Win_Back would give a different answer
    touchback = near.with_columns(pl.lit(75.0).alias("yardline_100"))
    assert wp_near != pytest.approx(_TD + _FG * _win_back(touchback)[0], abs=1e-6)


# ---------------------------------------------------------------------------
# Real-data gate — five whole overtime games, one per rules era
# ---------------------------------------------------------------------------

_FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "nfl_ep_wp" / "overtime_games.parquet"

#: Ceilings/floors for the fixture's 96 paired overtime rows, derived from the
#: values observed 2026-09-02 with the overlay in place — MAE **0.0209**,
#: bias **-0.0149**, r **0.9945** — and set strictly on the passing side of
#: those, while sitting far inside the un-overlaid values (MAE **0.1677**,
#: r **0.4996**) so a regression to the un-ported state fails loudly.
#: `test_the_gate_bites_without_the_overlay` proves that second half.
OT_FIXTURE_MAE_CEILING = 0.035
OT_FIXTURE_ABS_BIAS_CEILING = 0.030
OT_FIXTURE_R_FLOOR = 0.98


def _enrich(df: pl.DataFrame, models_dir: str) -> pl.DataFrame:
    """Run the production nflverse path over the fixture (no network)."""
    base = df.drop([c for c in ("ep", "epa", "wp", "vegas_wp", "wpa", *_EP_CLASS_NAMES) if c in df.columns])
    # models_dir points at an empty dir so the 34 MB xYAC model is skipped
    # gracefully instead of downloaded.
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
    """nflverse holds this on 96.6% of published 2025 overtime rows (0.033% in
    regulation).  We hold it exactly, because we do not run the spread model in
    overtime at all."""
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
