from datetime import datetime

from sportsdataverse.mbb.mbb_ncaa_models import (
    FieldGoalStats,
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


def _stats_with_rim(att_total, made_total, att_orb=0):
    s = LineupEventStats()
    s.fg_rim = FieldGoalStats(
        attempts=ShotClockStats(total=att_total, orb=att_orb),
        made=ShotClockStats(total=made_total),
    )
    s.fg = FieldGoalStats(attempts=ShotClockStats(total=att_total))
    s.pts = 12
    s.num_possessions = 20
    return s


def test_sum_fields_base_prefix_reads_total():
    s = _stats_with_rim(att_total=8, made_total=5)
    out = agg._sum_fields(s, dst="off", prefix="", suffix=".total")
    assert out["total_off_2prim_attempts"] == 8.0
    assert out["total_off_2prim_made"] == 5.0
    assert out["total_off_fga"] == 8.0
    assert out["total_off_pts"] == 12.0  # scalar, prefix "" only
    assert out["total_off_poss"] == 20.0


def test_sum_fields_scramble_prefix_reads_orb():
    s = _stats_with_rim(att_total=8, made_total=5, att_orb=3)
    out = agg._sum_fields(s, dst="off", prefix="scramble_", suffix=".orb")
    assert out["total_off_scramble_2prim_attempts"] == 3.0  # .orb leaf
