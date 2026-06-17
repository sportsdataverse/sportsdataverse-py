"""Offline tests: generated mlb_statcast wrappers are importable + drift gate is clean."""

from __future__ import annotations


def test_generated_wrappers_importable():
    from sportsdataverse.mlb import (
        mlb_statcast_gamefeed,
        mlb_statcast_leaderboard_custom,
        mlb_statcast_leaderboard_expected_stats,
        mlb_statcast_leaderboard_fielding_run_value,  # HTML-embedded-JSON family
        mlb_statcast_leaderboard_percentile_rankings,
        mlb_statcast_leaderboard_sprint_speed,
        mlb_statcast_player,  # hand-written (mlb_statcast_extra)
        mlb_statcast_schedule,
        mlb_statcast_search,  # hand-written
        mlb_statcast_search_minors,  # hand-written
        mlb_statcast_search_wbc,  # hand-written
    )

    assert all(
        callable(f)
        for f in (
            mlb_statcast_leaderboard_expected_stats,
            mlb_statcast_leaderboard_percentile_rankings,
            mlb_statcast_leaderboard_sprint_speed,
            mlb_statcast_leaderboard_custom,
            mlb_statcast_leaderboard_fielding_run_value,
            mlb_statcast_gamefeed,
            mlb_statcast_schedule,
            mlb_statcast_search,
            mlb_statcast_search_minors,
            mlb_statcast_search_wbc,
            mlb_statcast_player,
        )
    )


def test_generated_surface_is_comprehensive():
    """The generated flat-API family covers the full discovered leaderboard surface."""
    import sportsdataverse.mlb.mlb_statcast as gen

    leaderboards = [n for n in gen.__all__ if n.startswith("mlb_statcast_leaderboard_")]
    # 35 CSV + 2 HTML leaderboards discovered from the live Savant surface.
    assert len(leaderboards) >= 37, f"expected >=37 leaderboard wrappers, got {len(leaderboards)}"
    assert "mlb_statcast_gamefeed" in gen.__all__
    assert "mlb_statcast_schedule" in gen.__all__
    # The old released names must be gone (clean rename, no aliases).
    for old in ("statcast_search", "statcast_leaderboard_expected_statistics", "statcast_gamefeed"):
        assert not hasattr(gen, old)


def test_old_statcast_names_removed():
    """The pre-rename ``statcast_*`` names are no longer exported from sportsdataverse.mlb."""
    import sportsdataverse.mlb as mlb

    for old in (
        "statcast_search",
        "statcast_search_chunked",
        "statcast_leaderboard_expected_statistics",
        "statcast_leaderboard_sprint_speed",
        "statcast_gamefeed",
        "statcast_player_page",
    ):
        assert not hasattr(mlb, old), f"{old} should have been renamed to mlb_statcast_*"


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
