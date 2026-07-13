from datetime import datetime

from sportsdataverse.mbb.mbb_ncaa_models import (
    AssistInfo,
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

# T3 base-prefix (prefix="") off_* rate names -- commonAverageAggs.ts:291-397.
BASE_OFF_RATE_NAMES = {
    "off_2p",
    "off_2p_ast",
    "off_3p",
    "off_3p_ast",
    "off_2prim",
    "off_2prim_ast",
    "off_2pmid",
    "off_2pmid_ast",
    "off_ft",
    "off_ftr",
    "off_2primr",
    "off_2pmidr",
    "off_3pr",
    "off_assist",
    "off_ppp",
    "off_to",
    "off_efg",
    "off_orb",
    "off_ast_rim",
    "off_ast_mid",
    "off_ast_3p",
}


def _full_synthetic_totals(dst: str, prefix: str) -> dict[str, float]:
    """Every ``total_{dst}_{prefix}*`` stem set to ``1.0``.

    Derived by running :func:`agg._sum_fields` over a fully populated
    synthetic :class:`LineupEventStats` (every leaf ``.total`` == 1) rather
    than hand-enumerating stems, so this helper can't drift from the
    module's actual ``_sum_fields`` output shape.
    """
    one = ShotClockStats(total=1)
    fg_full = FieldGoalStats(
        attempts=ShotClockStats(total=1), made=ShotClockStats(total=1), ast=ShotClockStats(total=1)
    )
    s = LineupEventStats(
        fg=fg_full,
        fg_rim=fg_full,
        fg_mid=fg_full,
        fg_2p=fg_full,
        fg_3p=fg_full,
        ft=fg_full,
        orb=one,
        drb=one,
        to=ShotClockStats(total=1),
        stl=one,
        blk=one,
        assist=one,
        ast_rim=AssistInfo(counts=one),
        ast_mid=AssistInfo(counts=one),
        ast_3p=AssistInfo(counts=one),
        foul=one,
        pts=1,
        num_possessions=1,
    )
    return agg._sum_fields(s, dst=dst, prefix=prefix, suffix=".total")


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


def test_rate_guarded_formula_and_ppp_factor():
    totals = {
        "total_off_2prim_made": 5.0,
        "total_off_2prim_attempts": 8.0,
        "total_off_pts": 12.0,
        "total_off_poss": 20.0,
        "total_off_2prim_attempts_zero": 0.0,
    }
    fields = agg._rate_fields(totals, dst="off", prefix="", oppo_totals={})
    assert fields["off_2prim"]["value"] == 5.0 / 8.0  # cross-check ES off_2prim
    assert fields["off_ppp"]["value"] == 100.0 * 12.0 / 20.0


def test_rate_zero_guard_returns_zero_not_nan():
    totals = {"total_off_3p_made": 0.0, "total_off_3p_attempts": 0.0}
    fields = agg._rate_fields(totals, dst="off", prefix="", oppo_totals={})
    assert fields["off_3p"]["value"] == 0.0  # (num>0)?...:0 guard


def test_efg_weights_threes_by_1_5():
    totals = {"total_off_fga": 10.0, "total_off_2p_made": 3.0, "total_off_3p_made": 2.0}
    fields = agg._rate_fields(totals, dst="off", prefix="", oppo_totals={})
    assert fields["off_efg"]["value"] == (1.0 * 3.0 + 1.5 * 2.0) / 10.0


def test_orb_rate_uses_cross_side_drb():
    totals = {"total_off_orb": 6.0}
    oppo = {"total_def_drb": 14.0}
    fields = agg._rate_fields(totals, dst="off", prefix="", oppo_totals=oppo)
    assert fields["off_orb"]["value"] == 6.0 / (6.0 + 14.0)


def test_base_prefix_emits_all_rate_names():
    totals = _full_synthetic_totals("off", "")
    fields = agg._rate_fields(totals, dst="off", prefix="", oppo_totals=_full_synthetic_totals("def", ""))
    missing = BASE_OFF_RATE_NAMES - set(fields)
    assert not missing, f"missing rate fields: {missing}"


def test_scramble_and_trans_rates_emitted():
    tp = {
        "": _full_synthetic_totals("off", ""),
        "scramble_": {
            "total_off_scramble_2prim_made": 2.0,
            "total_off_scramble_2prim_attempts": 4.0,
            "total_off_scramble_pts": 6.0,
            "total_off_scramble_poss": 10.0,
        },
        "trans_": {"total_off_trans_3p_made": 1.0, "total_off_trans_3p_attempts": 3.0},
    }
    fields = agg._all_rate_fields(
        tp,
        dst="off",
        oppo_totals_by_prefix={"": _full_synthetic_totals("def", ""), "scramble_": {}, "trans_": {}},
    )
    assert fields["off_scramble_2prim"]["value"] == 2.0 / 4.0
    assert fields["off_scramble_ppp"]["value"] == 100.0 * 6.0 / 10.0
    assert fields["off_trans_3p"]["value"] == 1.0 / 3.0
    assert "off_scramble_orb" not in fields  # orb is prefix "" only


def test_play_type_pts_poss_formula():
    # TS :342-380 -- pts = 3*made3p + 2*made2p + ftm; poss = fgm + (1-rebound_pct)*fgMiss
    # + 0.475*fta + to, rebound_pct from the BASE (prefix "") cross-side orb/drb.
    off_totals = {
        "": {"total_off_orb": 4.0},
        "scramble_": {
            "total_off_scramble_3p_made": 2.0,
            "total_off_scramble_2p_made": 1.0,
            "total_off_scramble_ftm": 3.0,
            "total_off_scramble_fga": 10.0,
            "total_off_scramble_fgm": 4.0,
            "total_off_scramble_fta": 4.0,
            "total_off_scramble_to": 2.0,
        },
        "trans_": {},
    }
    def_totals = {"": {"total_def_drb": 6.0}, "scramble_": {}, "trans_": {}}
    out = agg._play_type_pts_poss(off_totals, def_totals)
    assert out["total_off_scramble_pts"] == 3.0 * 2.0 + 2.0 * 1.0 + 3.0  # 11.0
    rebound_pct = 4.0 / (4.0 + 6.0)
    fg_missed = 10.0 - 4.0
    expected_poss = 4.0 + (1.0 - rebound_pct) * fg_missed + 0.475 * 4.0 + 2.0
    assert out["total_off_scramble_poss"] == expected_poss
    assert out["total_off_trans_pts"] == 0.0
    assert out["total_def_scramble_pts"] == 0.0  # def-side totals empty here


def test_adj_ppp_fallback_equals_raw_ppp_when_no_baselines():
    f = agg._adj_fields(pts=30.0, poss=25.0, dst="off", opponent_baselines=None, avg_eff=100.0)
    assert f["off_adj_ppp"]["value"] == 100.0 * 30.0 / 25.0  # faithful fallback
    assert f["off_adj_opp"]["value"] == 100.0  # avg_eff


def test_adj_ppp_zero_poss_guarded():
    f = agg._adj_fields(pts=0.0, poss=0.0, dst="off", opponent_baselines=None, avg_eff=100.0)
    assert f["off_adj_ppp"]["value"] == 0.0


def _full_stats(pts: int, poss: int) -> LineupEventStats:
    # ponytail: every ShotClockStats.total/orb/early set equal so all 3 prefixes
    # (""/"scramble_"/"trans_") read nonzero data -- exercises the play-type
    # pts/poss merge, not just the base "" family.
    def sc(v: int) -> ShotClockStats:
        return ShotClockStats(total=v, orb=v, early=v)

    fg_full = FieldGoalStats(attempts=sc(10), made=sc(5), ast=sc(3))
    return LineupEventStats(
        fg=fg_full,
        fg_rim=fg_full,
        fg_mid=fg_full,
        fg_2p=fg_full,
        fg_3p=fg_full,
        ft=fg_full,
        orb=sc(4),
        drb=sc(6),
        to=sc(2),
        stl=sc(1),
        blk=sc(1),
        assist=sc(3),
        ast_rim=AssistInfo(counts=sc(1)),
        ast_mid=AssistInfo(counts=sc(1)),
        ast_3p=AssistInfo(counts=sc(1)),
        foul=sc(1),
        pts=pts,
        num_possessions=poss,
    )


def _enriched_lineup_event() -> LineupEvent:
    players = [PlayerCodeId(code=c, id=c) for c in ["JaSmith", "AaWiggins", "ErAyala", "AnCowan", "DaMorsell"]]
    ev = _minimal_lineup_event(players)
    ev.team_stats = _full_stats(pts=20, poss=15)
    ev.opponent_stats = _full_stats(pts=18, poss=16)
    return ev


def test_bucket_has_structural_keys_and_wrapped_fields():
    ev = _enriched_lineup_event()
    b = agg.lineup_stats_bucket(ev, doc_count=7)
    assert b["key"] == agg._bucket_key(ev)
    assert b["players_array"]["hits"]["hits"][0]["_source"]["players"]
    assert b["doc_count"] == 7
    assert isinstance(b["off_ppp"], dict) and "value" in b["off_ppp"]
    assert isinstance(b["def_ppp"], dict) and "value" in b["def_ppp"]
    assert isinstance(b["total_off_fga"], dict)
    # Step 3's ordering requirement: play-type pts/poss must be folded into the
    # per-prefix totals BEFORE minting rates, else off_scramble_ppp reads 0.
    assert b["off_scramble_ppp"]["value"] != 0.0
