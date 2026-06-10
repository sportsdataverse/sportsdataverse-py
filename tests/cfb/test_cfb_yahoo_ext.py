from sportsdataverse.cfb import cfb_yahoo_ext as y

MODERN = {
    "data": {
        "leagues": [
            {
                "footballStats": [
                    {
                        "player": {
                            "playerId": "ncaaf.p.1",
                            "displayName": "QB One",
                            "team": {"displayName": "Team A", "abbreviation": "TA"},
                        },
                        "stats": [
                            {"statId": "PASSING_YARDS", "value": "4000"},
                            {"statId": "PASSING_TOUCHDOWNS", "value": "40"},
                        ],
                    },
                    {
                        "player": {
                            "playerId": "ncaaf.p.2",
                            "displayName": "QB Two",
                            "team": {"displayName": "Team B", "abbreviation": "TB"},
                        },
                        "stats": [{"statId": "PASSING_YARDS", "value": "3500"}],
                    },
                ]
            }
        ]
    },
    "extensions": {},
}


def test_flatten_modern_pivots_wide():
    rows = y._flatten_modern(MODERN, "footballStats")
    assert len(rows) == 2
    r0 = rows[0]
    assert r0["player_id"] == "ncaaf.p.1"
    assert r0["display_name"] == "QB One"
    assert r0["team"] == "Team A"
    assert r0["team_abbreviation"] == "TA"
    assert r0["passing_yards"] == "4000"
    assert r0["passing_touchdowns"] == "40"
    # missing stat is absent (or None) on row 2, but pivot keys exist on row 1
    assert "passing_yards" in rows[1]
