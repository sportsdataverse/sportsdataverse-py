from __future__ import annotations


def test_leagues_registry_has_all_verified_hockeytech_leagues():
    from sportsdataverse.hockeytech import LEAGUES

    assert set(LEAGUES) == {
        "pwhl",
        "ahl",
        "ohl",
        "whl",
        "qmjhl",
        "echl",
        "sphl",
        "chl",
        "ushl",
        "bchl",
        "ajhl",
        "sjhl",
        "ojhl",
        "cchl",
        "gojhl",
        "mhl",
        "nojhl",
        "vijhl",
        "kijhl",
        "mjhl",
    }


def test_new_leagues_use_single_league_client_defaults():
    from sportsdataverse.hockeytech import LEAGUES

    # The 15 leagues promoted 2026-07-12 are single-league clients: league_id=1,
    # site_id=0, small-canvas pbp dialect (standings-verified live).
    for lg in ("echl", "sphl", "chl", "ushl", "bchl", "cchl", "kijhl", "mjhl"):
        cfg = LEAGUES[lg]
        assert cfg.league_id == 1
        assert cfg.site_id == 0
        assert cfg.pbp_style == "hockeytech_b"
        assert "lscluster.hockeytech.com" in cfg.base_url


def test_pwhl_config_matches_known_values():
    from sportsdataverse.hockeytech import LEAGUES

    pwhl = LEAGUES["pwhl"]
    assert pwhl.client_code == "pwhl"
    assert pwhl.league_id == 1
    assert pwhl.site_id == 0
    assert pwhl.pbp_style == "hockeytech_a"
    assert "lscluster.hockeytech.com" in pwhl.base_url


def test_qmjhl_uses_leaguestat_host_and_lhjmq_code():
    from sportsdataverse.hockeytech import LEAGUES

    q = LEAGUES["qmjhl"]
    assert q.client_code == "lhjmq"
    assert "cluster.leaguestat.com" in q.base_url
    assert q.pbp_style == "hockeytech_b"


def test_env_var_overrides_api_key(monkeypatch):
    from sportsdataverse.hockeytech._leagues import resolve_api_key

    monkeypatch.setenv("SDV_PWHL_API_KEY", "override123")
    assert resolve_api_key("pwhl") == "override123"
    monkeypatch.delenv("SDV_PWHL_API_KEY")
    assert resolve_api_key("pwhl") == "446521baf8c38984"
