import importlib


def test_nba_stats_module_imports_and_has_workhorse():
    mod = importlib.import_module("sportsdataverse.nba.nba_stats")
    assert hasattr(mod, "nba_stats_leaguedashplayerstats")
    assert hasattr(mod, "nba_stats_playercareerstats")
    assert hasattr(mod, "nba_stats_playbyplayv3")
    assert not hasattr(mod, "nba_stats_commonallplayers")
    assert len(mod.__all__) == 67  # live, non-deprecated stats endpoints


def test_wnba_stats_module_imports():
    mod = importlib.import_module("sportsdataverse.wnba.wnba_stats")
    assert hasattr(mod, "wnba_stats_leaguedashplayerstats")
    assert hasattr(mod, "wnba_stats_playbyplayv3")
    assert len(mod.__all__) == 52  # live, non-deprecated stats endpoints
