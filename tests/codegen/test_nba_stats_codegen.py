import importlib


def test_nba_stats_module_imports_and_has_workhorse():
    mod = importlib.import_module("sportsdataverse.nba.nba_stats")
    assert hasattr(mod, "nba_stats_leaguedashplayerstats")
    assert hasattr(mod, "nba_stats_playercareerstats")
    assert hasattr(mod, "nba_stats_playbyplayv3")
    # endpoints resolved live by the capture sweep (commonallplayers) and ones
    # materialized from the hoopR-only param list (teamdashboardbyclutch) are included
    assert hasattr(mod, "nba_stats_commonallplayers")
    assert hasattr(mod, "nba_stats_teamdashboardbyclutch")
    # upstream-deprecated endpoints stay excluded from codegen
    assert not hasattr(mod, "nba_stats_scoreboardv2")
    assert not hasattr(mod, "nba_stats_boxscoretraditionalv2")
    assert len(mod.__all__) == 112  # live, non-deprecated stats endpoints


def test_wnba_stats_module_imports():
    mod = importlib.import_module("sportsdataverse.wnba.wnba_stats")
    assert hasattr(mod, "wnba_stats_leaguedashplayerstats")
    assert hasattr(mod, "wnba_stats_playbyplayv3")
    # wehoop hard-deprecated (lifecycle::deprecate_stop) endpoints stay excluded
    assert not hasattr(mod, "wnba_stats_hustlestatsboxscore")
    assert len(mod.__all__) == 95  # live, non-deprecated stats endpoints
