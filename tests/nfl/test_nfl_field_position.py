"""NFL field-position curve: the bundled artifact and the fit entry point."""

from __future__ import annotations

import numpy as np
import polars as pl

from sportsdataverse.nfl import fit_nfl_field_position_ep, load_nfl_fp_curve


def test_bundled_nfl_curve_grid_monotone_and_anchors():
    """Fit 2026-09-14 on espn_nfl_pbp 2016-2025 (59,026 drives); anchors are the
    observed values with a band wide enough to survive a refresh on more seasons
    but tight enough to catch a broken orientation (a reversed curve fails)."""
    curve = load_nfl_fp_curve().sort("yardline_own")
    assert curve.height == 99
    assert curve["yardline_own"].to_list() == list(range(1, 100))
    ep = curve["ep"].to_numpy()
    assert (np.diff(ep) >= -1e-9).all()
    lookup = dict(zip(curve["yardline_own"].to_list(), ep.tolist()))
    for yl, ref in ((25, 1.82), (50, 2.50), (75, 3.62), (90, 4.51)):
        assert abs(lookup[yl] - ref) <= 0.4, (yl, lookup[yl])
    assert lookup[99] > lookup[1] + 4.0


def test_fit_nfl_field_position_ep_from_released_shape():
    rng = np.random.default_rng(1)
    n = 3000
    yl = rng.integers(1, 100, n)
    home = rng.integers(0, 2, n)
    # drive.start.yardLine is the HOME team's own yard line; the away offense's
    # own yard line is 100 - that, so orient the raw field by who has the ball
    raw_yl = np.where(home == 1, yl, 100 - yl)
    pts_true = 0.05 * yl
    result = np.where(rng.random(n) < pts_true / 7.0, "TD", "PUNT")
    pbp = pl.DataFrame(
        {
            "season": [2025] * n,
            "game_id": (rng.integers(1, 40, n) + 401000000).tolist(),
            "wallclock": [None] * n,
            "period": rng.integers(1, 5, n).tolist(),
            "start.down": [1] * n,
            "start.distance": [10] * n,
            "statYardage": [0] * n,
            "EPA": [0.0] * n,
            "pass": [False] * n,
            "rush": [True] * n,
            "havoc": [False] * n,
            "scrimmage_play": [True] * n,
            "pos_team_score": [0] * n,
            "def_pos_team_score": [0] * n,
            "start.pos_team.id": np.where(home == 1, 1, 2).tolist(),
            "start.def_pos_team.id": np.where(home == 1, 2, 1).tolist(),
            "drive.id": [str(401000000 + i) for i in range(n)],
            "drive.result": result.tolist(),
            "drive.start.yardLine": raw_yl.tolist(),
            "homeTeamId": [1] * n,
        }
    )
    curve = fit_nfl_field_position_ep(pbp, exclude_garbage=False)
    assert curve.height == 99
    ep = curve.sort("yardline_own")["ep"].to_numpy()
    assert (np.diff(ep) >= -1e-9).all() and ep[-1] > ep[0]
    assert fit_nfl_field_position_ep(pbp.clear()).height == 0
