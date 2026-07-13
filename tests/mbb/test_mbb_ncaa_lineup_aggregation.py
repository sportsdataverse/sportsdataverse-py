from datetime import datetime

from sportsdataverse.mbb.mbb_ncaa_models import (
    LineupEvent,
    LineupEventStats,
    LineupId,
    LocationType,
    PlayerCodeId,
    Score,
    ScoreInfo,
    ShotClockStats,
    TeamId,
    TeamSeasonId,
    Year,
)
from sportsdataverse.mbb import mbb_ncaa_lineup_aggregation as agg


def _minimal_lineup_event(players: list[PlayerCodeId]) -> LineupEvent:
    team = TeamSeasonId(team=TeamId(name="Duke"), year=Year(value=2024))
    opponent = TeamSeasonId(team=TeamId(name="UNC"), year=Year(value=2024))
    return LineupEvent(
        date=datetime(2024, 1, 1),
        location_type=LocationType.HOME,
        start_min=0.0,
        end_min=1.0,
        duration_mins=1.0,
        score_info=ScoreInfo(
            start=Score(scored=0, allowed=0),
            end=Score(scored=0, allowed=0),
            start_diff=0,
            end_diff=0,
        ),
        team=team,
        opponent=opponent,
        lineup_id=LineupId.unknown,
        players=players,
        players_in=[],
        players_out=[],
        raw_game_events=[],
        team_stats=LineupEventStats.empty(),
        opponent_stats=LineupEventStats.empty(),
    )


def test_leaf_selects_suffix_and_coalesces_none():
    s = ShotClockStats(total=10, orb=3, early=2)
    assert agg._leaf(s, ".total") == 10.0
    assert agg._leaf(s, ".orb") == 3.0
    assert agg._leaf(s, ".early") == 2.0
    assert agg._leaf(None, ".total") == 0.0  # Optional stat → 0
    assert agg._leaf(ShotClockStats(total=5), ".orb") == 0.0  # None leaf → 0


def test_bucket_key_is_sorted_codes_joined():
    players = [PlayerCodeId(code=c, id=c) for c in ["JaSmith", "AaWiggins", "ErAyala", "AnCowan", "DaMorsell"]]
    ev = _minimal_lineup_event(players)
    assert agg._bucket_key(ev) == "AaWiggins_AnCowan_DaMorsell_ErAyala_JaSmith"


def test_players_array_is_top_hits_shaped():
    players = [PlayerCodeId(code="AaWiggins", id="Wiggins, Aaron")]
    ev = _minimal_lineup_event(players)
    pa = agg._players_array(ev)
    assert pa["hits"]["hits"][0]["_source"]["players"] == [{"code": "AaWiggins", "id": "Wiggins, Aaron"}]
