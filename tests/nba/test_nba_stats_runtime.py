from sportsdataverse.nba.nba_stats_runtime import _get, stats_headers


def test_stats_headers_token_and_host():
    h = stats_headers("stats.nba.com")
    assert h["x-nba-stats-token"] == "true" and h["Host"] == "stats.nba.com"
    assert "nba.com" in h["Referer"]


def test_get_builds_url_strips_none_and_pads_gameid():
    captured = {}

    def transport(url, params, headers, proxy_url):
        captured["url"] = url
        captured["params"] = params
        return 200, '{"resultSets": [{"name": "X", "headers": ["A"], "rowSet": [[1]]}]}'

    out = _get(
        "leaguedashplayerstats",
        {"LeagueID": "00", "Empty": None, "GameID": "61"},
        transport=transport,
    )
    assert captured["url"] == "https://stats.nba.com/stats/leaguedashplayerstats"
    assert "Empty" not in captured["params"]
    assert captured["params"]["GameID"] == "0000000061"
    assert out["resultSets"][0]["name"] == "X"


def test_get_returns_empty_dict_on_non_200():
    out = _get("x", {}, transport=lambda *a: (500, "boom"))
    assert out == {}


def test_get_passes_full_url_through():
    captured = {}

    def transport(url, params, headers, proxy_url):
        captured["url"] = url
        return 200, '{"resultSets": []}'

    _get("https://stats.nba.com/stats/leaguedashplayerstats", {}, transport=transport)
    assert captured["url"] == "https://stats.nba.com/stats/leaguedashplayerstats"


def test_wnba_runtime_fixes_host():
    from sportsdataverse.wnba.wnba_stats_runtime import _get as wget

    captured = {}

    def transport(url, params, headers, proxy_url):
        captured["url"] = url
        captured["host_header"] = headers.get("Host")
        return 200, "{}"

    wget("leaguedashplayerstats", {}, transport=transport)
    assert captured["url"] == "https://stats.wnba.com/stats/leaguedashplayerstats"
    assert captured["host_header"] == "stats.wnba.com"
