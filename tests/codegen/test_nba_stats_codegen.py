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
    # capture-confirmed-live endpoints ship even when a source marks them
    # deprecated (2026-08-23 probe sweep): scoreboardv2 probed live on both
    # hosts; boxscoretraditionalv2 is sweep-live under nba_api's v3-preference
    assert hasattr(mod, "nba_stats_scoreboardv2")
    assert hasattr(mod, "nba_stats_boxscoretraditionalv2")
    # endpoints the probes confirmed dead stay excluded: playercareerbycollege
    # 500s on both hosts; the v1 scoreboard returns an HTML page
    assert not hasattr(mod, "nba_stats_playercareerbycollege")
    assert not hasattr(mod, "nba_stats_scoreboard")
    # drafthistory joined at 113: LeagueID=00 was mis-marked "barren" in the
    # catalog, so it had never been generated for the NBA side.
    assert hasattr(mod, "nba_stats_drafthistory")
    # video endpoints joined at 128: the 2026-08 capture sweep's shape detector
    # misread their dict resultSets envelope ({Meta:{videoUrls},playlist}) as dead;
    # a 2026-08-26 re-probe confirmed all three live on stats.nba.com.
    assert hasattr(mod, "nba_stats_videodetailsasset")
    assert hasattr(mod, "nba_stats_videoevents")
    assert hasattr(mod, "nba_stats_videoeventsasset")
    # videodetails (no "asset") is genuinely dead (500-empty) and stays excluded
    assert not hasattr(mod, "nba_stats_videodetails")
    assert len(mod.__all__) == 128  # live or capture-confirmed-live stats endpoints


def test_wnba_stats_module_imports():
    mod = importlib.import_module("sportsdataverse.wnba.wnba_stats")
    assert hasattr(mod, "wnba_stats_leaguedashplayerstats")
    assert hasattr(mod, "wnba_stats_playbyplayv3")
    # capture-live overrides wehoop's client-side deprecate_stop opinion
    assert hasattr(mod, "wnba_stats_hustlestatsboxscore")
    # probe-dead on stats.wnba.com (2026-08-23): 500s with wehoop's own defaults
    assert not hasattr(mod, "wnba_stats_teamhistoricalleaders")
    assert not hasattr(mod, "wnba_stats_playercareerbycollege")
    assert len(mod.__all__) == 111  # live or capture-confirmed-live stats endpoints
