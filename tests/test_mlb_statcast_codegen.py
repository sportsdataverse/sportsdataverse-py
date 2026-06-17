"""Offline tests: generated mlb_statcast wrappers are importable + drift gate is clean."""

from __future__ import annotations


def test_generated_wrappers_importable():
    from sportsdataverse.mlb import (
        mlb_statcast_gamefeed,
        mlb_statcast_leaderboard_expected_stats,
        mlb_statcast_player_percentile_rankings,
        mlb_statcast_search,  # hand-written (mlb_statcast_extra)
    )

    assert all(
        callable(f)
        for f in (
            mlb_statcast_leaderboard_expected_stats,
            mlb_statcast_gamefeed,
            mlb_statcast_player_percentile_rankings,
            mlb_statcast_search,
        )
    )


def test_codegen_drift_clean():
    import subprocess
    import sys

    r = subprocess.run(
        [sys.executable, "tools/codegen/generate.py", "--check"],
        cwd=".",
        capture_output=True,
        text=True,
    )
    assert r.returncode == 0, r.stderr
