"""Tests for nba_box_logs: per-100 box features + fetch interface."""

from __future__ import annotations

import pytest
import polars as pl

from sportsdataverse.nba.nba_box_logs import box_features


def _logs():
    # 2 games, one team (id 1), one player (id 10) who is on court wire-to-wire.
    # A player's ``min`` is elapsed game minutes (48); the TEAM's is the sum over
    # its five on-court slots (240). Giving the player 240 here -- as this fixture
    # once did -- is physically impossible and hid a 5x error in the per-100
    # denominator, because it made min/team_min accidentally equal 1.
    player = pl.DataFrame(
        {
            "game_id": ["G1", "G2"],
            "team_id": [1, 1],
            "player_id": [10, 10],
            "min": [48.0, 48.0],
            "pts": [20, 30],
            "fg3m": [2, 3],
            "fga": [15, 20],
            "fta": [4, 6],
            "ast": [5, 7],
            "oreb": [1, 2],
            "dreb": [4, 5],
            "stl": [1, 2],
            "blk": [0, 1],
            "tov": [3, 2],
            "pf": [2, 3],
        }
    )
    team = pl.DataFrame(
        {
            "game_id": ["G1", "G2"],
            "team_id": [1, 1],
            "min": [240.0, 240.0],
            "fga": [80, 85],
            "oreb": [10, 12],
            "tov": [14, 12],
            "fta": [20, 22],
        }
    )
    return player, team


def test_box_features_per100_and_totals():
    player, team = _logs()
    f = box_features(player, team)
    assert f.height == 1 and f["player_id"][0] == 10
    assert f["gp"][0] == 2 and abs(f["min"][0] - 96.0) < 1e-9  # 2 full games
    # team_poss = (80-10+14+0.44*20)+(85-12+12+0.44*22) = 92.8 + 94.68 = 187.48
    # player plays all minutes -> player_poss = 187.48 ; pts/100 = 50/187.48*100
    assert abs(f["pts"][0] - (50 / 187.48 * 100)) < 1e-6


def test_box_features_real_parser_fg3_m_column():
    # The live ``leaguegamelog`` parser snake-cases ``FG3M`` -> ``"fg3_m"`` (underscore
    # before the trailing M), NOT ``"fg3m"``. ``box_features`` must canonicalize that at
    # the boundary or it raises ``ColumnNotFoundError`` on real data (synthetic fixtures
    # hid the bug by using ``"fg3m"`` directly). This exercises the real column name.
    player, team = _logs()
    player = player.rename({"fg3m": "fg3_m"})
    f = box_features(player, team)
    # Same team_poss as test_box_features_per100_and_totals (187.48); fg3m total = 2+3 = 5
    assert f.height == 1 and f["player_id"][0] == 10
    assert "fg3m" in f.columns and "fg3_m" not in f.columns
    assert abs(f["fg3m"][0] - (5 / 187.48 * 100)) < 1e-6
    # pts unaffected by the rename
    assert abs(f["pts"][0] - (50 / 187.48 * 100)) < 1e-6


def test_box_features_game_id_restriction():
    player, team = _logs()
    only_g1 = box_features(player, team, game_ids=["G1"])
    # G1 only: team_poss = 92.8 ; pts/100 = 20/92.8*100
    assert abs(only_g1["pts"][0] - (20 / 92.8 * 100)) < 1e-6


def test_box_features_traded_player_uses_per_game_pace():
    # player 10 plays G1 for team 1 (fast) and G2 for team 2 (slow), wire-to-wire
    # in each -- 48 elapsed minutes, against a team total of 240 across five slots
    player = pl.DataFrame(
        {
            "game_id": ["G1", "G2"],
            "team_id": [1, 2],
            "player_id": [10, 10],
            "min": [48.0, 48.0],
            "pts": [20, 20],
            "fg3m": [2, 2],
            "fga": [15, 15],
            "fta": [4, 4],
            "ast": [5, 5],
            "oreb": [1, 1],
            "dreb": [4, 4],
            "stl": [1, 1],
            "blk": [0, 0],
            "tov": [3, 3],
            "pf": [2, 2],
        }
    )
    team = pl.DataFrame(
        {
            "game_id": ["G1", "G2"],
            "team_id": [1, 2],
            "min": [240.0, 240.0],
            "fga": [100, 70],
            "oreb": [10, 8],
            "tov": [14, 10],
            "fta": [20, 15],
        }
    )
    f = box_features(player, team)
    # per-game player_poss: G1 team_poss=100-10+14+0.44*20=112.8 ; G2=70-8+10+0.44*15=78.6
    # player plays all minutes -> player_poss = 112.8 + 78.6 = 191.4 ; pts/100 = 40/191.4*100
    assert f.height == 1 and f["player_id"][0] == 10
    assert abs(f["pts"][0] - (40 / 191.4 * 100)) < 1e-6


def test_full_game_player_gets_the_teams_possessions():
    """Oracle for the per-100 denominator: a player on court for the whole game
    faced exactly the team's possessions, so a 100-possession team-game must give
    that player player_poss == 100 and pts-per-100 == their actual points.

    A team-game's ``min`` sums five on-court slots (240 in regulation), so
    dividing by it directly -- rather than by team_min/5 -- understates a
    player's possessions fivefold and inflates every per-100 rate by 5x. SPM
    hides that (its coefficients are fitted on these features); BPM does not,
    because it applies fixed published coefficients.
    """
    import polars as pl

    from sportsdataverse.nba.nba_box_logs import box_features

    # team-game engineered to exactly 100 possessions: 100 - 10 + 8 + 0.44*5 ~= 100.2
    team = pl.DataFrame(
        {
            "game_id": ["0022300001"],
            "team_id": [1610612737],
            "min": [240],
            "fga": [100],
            "oreb": [10],
            "tov": [8],
            "fta": [5],
        }
    )
    team_poss = 100 - 10 + 8 + 0.44 * 5
    player = pl.DataFrame(
        {
            "game_id": ["0022300001"],
            "team_id": [1610612737],
            "player_id": [201939],
            "min": [48],  # wire-to-wire
            "pts": [30],
            "fg3m": [5],
            "fga": [20],
            "fta": [4],
            "ast": [6],
            "oreb": [1],
            "dreb": [5],
            "stl": [2],
            "blk": [1],
            "tov": [3],
            "pf": [2],
        }
    )
    bf = box_features(player, team)
    assert bf.height == 1
    # 30 points over the team's ~100 possessions -> ~30 per 100, not ~150
    assert bf["pts"][0] == pytest.approx(30 / team_poss * 100, rel=1e-9)
    assert 25.0 < bf["pts"][0] < 35.0, "per-100 scoring outside any plausible NBA range"


def _idlogs():
    """Two players; one traded mid-season (more minutes with the second team)."""
    import polars as pl

    return pl.DataFrame(
        {
            "game_id": ["G1", "G2", "G3", "G1"],
            "player_id": [10, 10, 10, 20],
            "player_name": ["Traded Guy", "Traded Guy", "Traded Guy", "Stayer"],
            "team_id": [1, 1, 2, 3],
            "team_abbreviation": ["AAA", "AAA", "BBB", "CCC"],
            "team_name": ["Alpha Aces", "Alpha Aces", "Beta Bears", "Gamma Cats"],
            "min": [10.0, 10.0, 30.0, 25.0],
        }
    )


def test_player_identity_primary_team_is_by_minutes_and_lists_all():
    """A trade must be visible, not silently collapsed: team_* is the team the
    player actually spent most minutes with, and `teams` shows every stop."""
    from sportsdataverse.nba.nba_box_logs import nba_player_identity

    out = nba_player_identity(_idlogs())
    assert out.height == 2
    traded = out.filter(out["player_id"] == 10).to_dicts()[0]
    # 30 minutes with BBB beats 20 with AAA -> BBB is primary
    assert traded["team_abbreviation"] == "BBB"
    assert traded["team_name"] == "Beta Bears"
    assert traded["teams"] == "BBB,AAA"  # descending minutes
    assert traded["player_name"] == "Traded Guy"
    stayer = out.filter(out["player_id"] == 20).to_dicts()[0]
    assert stayer["teams"] == "CCC" and stayer["team_name"] == "Gamma Cats"


def test_player_identity_empty_and_missing_columns_give_typed_frame():
    """Callers join this unconditionally, so a bad input must not raise -- and the
    frame must carry the DTYPES too, or a downstream join on player_id silently
    mismatches on a Null column instead of failing loudly.

    A frame missing ``min`` is malformed, not merely minute-less: aggregating it
    would total zero minutes for every team and pick a "primary" team by team_id
    order -- a wrong answer wearing the shape of a right one.
    """
    import polars as pl

    from sportsdataverse.nba.nba_box_logs import (
        PLAYER_IDENTITY_SCHEMA,
        nba_player_identity,
    )

    complete = _idlogs()
    for bad in (
        pl.DataFrame(),
        pl.DataFrame({"player_id": [1]}),
        complete.drop("min"),  # the silent-degradation case
        complete.drop("team_name"),
    ):
        out = nba_player_identity(bad)
        assert out.height == 0
        assert dict(out.schema) == PLAYER_IDENTITY_SCHEMA


def test_player_identity_primary_team_does_not_depend_on_group_order():
    """The primary pick and the `teams` order must come from an explicit sort, not
    from group_by happening to preserve a prior one -- polars only guarantees that
    with maintain_order, so relying on it is undocumented behaviour that holds
    until it doesn't."""
    import polars as pl

    from sportsdataverse.nba.nba_box_logs import nba_player_identity

    n = 200
    logs = pl.DataFrame(
        {
            "game_id": [f"G{i}" for i in range(n) for _ in (0, 1)],
            "player_id": [i for i in range(n) for _ in (0, 1)],
            "player_name": [f"P{i}" for i in range(n) for _ in (0, 1)],
            # the HIGH-minutes team deliberately carries the LOWER team_id, so a
            # team_id-ordered fallback would pick the wrong club
            "team_id": [t for _ in range(n) for t in (900, 100)],
            "team_abbreviation": [a for _ in range(n) for a in ("HI", "LO")],
            "team_name": [t for _ in range(n) for t in ("High Id", "Low Id")],
            "min": [m for _ in range(n) for m in (5.0, 50.0)],
        }
    )
    out = nba_player_identity(logs)
    assert out.height == n
    assert out.filter(pl.col("team_abbreviation") != "LO").height == 0
    assert out.filter(pl.col("teams") != "LO,HI").height == 0
