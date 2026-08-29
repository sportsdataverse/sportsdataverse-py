# tests/cfb/test_box_score_attribution_offline.py
"""Offline golden tests for create_box_score team attribution.

Each test mocks cfb_pbp.download to return a captured summary payload, so
no network is hit. Team attribution does not depend on participants, so the
participants join falls back to regex on these fixtures (the mocked download
returns the summary for any URL, which the participants parser treats as
empty and falls back). See spec section 8.
"""

from __future__ import annotations

import json
from pathlib import Path


from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

FIX = Path(__file__).parent / "fixtures"


def _load(gid: int) -> dict:
    return json.loads((FIX / f"summary_{gid}.json").read_text(encoding="utf-8"))


def _box(monkeypatch, gid: int) -> dict:
    summary = _load(gid)

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())
    proc = CFBPlayProcess(gameId=gid)
    proc.join_participants = False  # offline: do not fetch ESPN participants over the network
    proc.espn_cfb_pbp()
    out = proc.run_processing_pipeline()
    return out["advBoxScore"]


def _team(box_section: list[dict], team_id: int) -> dict:
    matches = [r for r in box_section if r.get("pos_team") == team_id or r.get("team_id") == team_id]
    assert matches, f"team {team_id} not found in section"
    return matches[0]


def test_fixtures_produce_box(monkeypatch):
    box = _box(monkeypatch, 401754598)
    assert set(box) >= {"turnover", "team", "defensive_players", "specialists"}


def test_attribution_cols_present(monkeypatch):
    import polars as pl

    summary = _load(401754598)

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())
    proc = CFBPlayProcess(gameId=401754598)
    proc.espn_cfb_pbp()
    proc.run_processing_pipeline()
    df = pl.from_dicts(proc.plays_json, infer_schema_length=None)
    for col in [
        "turnover_team",
        "is_turnover",
        "is_st_turnover",
        "fumble_recovery_team",
        "penalized_team",
        "kicking_team",
        "return_team",
    ]:
        assert col in df.columns, f"missing {col}"


def test_turnovers_punt_muff_fsu(monkeypatch):
    box = _box(monkeypatch, 401754598)
    fsu = _team(box["turnover"], 52)
    ncst = _team(box["turnover"], 152)
    # Matches ESPN official box (FSU turnovers=4): 2 INT thrown + 2 fumbles lost
    # (the muff + the punt-return fumble, both ST). The 2nd INT is the end-of-half
    # Hail Mary that the pbp dedup used to drop -- now retained (see __helper_cfb_pbp_features).
    assert fsu["turnovers"] == 4
    assert ncst["turnovers"] == 0  # the only NCSU fumble was overturned on review
    assert fsu["st_turnovers_lost"] == 2


def test_turnovers_kickoff_fumble_asu(monkeypatch):
    box = _box(monkeypatch, 401309854)
    # Matches ESPN (ASU=4): KO-return fumble + 2 INT thrown + 1 INT-return fumble
    # (Robertson intercepted Hall, returned, then fumbled back to BYU -- a second,
    # opposite-direction turnover on the same play, captured by the per-event model).
    assert _team(box["turnover"], 9)["turnovers"] == 4
    assert _team(box["turnover"], 252)["turnovers"] == 2  # BYU: 2 INT


def test_turnovers_kickoff_fumble_baylor(monkeypatch):
    box = _box(monkeypatch, 401112081)
    assert _team(box["turnover"], 239)["turnovers"] == 2  # Baylor: KO fumble + 1 INT
    assert _team(box["turnover"], 2628)["turnovers"] == 3  # TCU: 3 INT


def test_punt_own_recovery_not_a_turnover(monkeypatch):
    box = _box(monkeypatch, 401032062)
    assert _team(box["turnover"], 2711)["turnovers"] == 1  # WMU: INT only (no phantom from BYU own recovery)
    assert _team(box["turnover"], 252)["turnovers"] == 1  # BYU: 1 scrimmage fumble


def test_turnover_margin_antisymmetric_offline(monkeypatch):
    box = _box(monkeypatch, 401754598)
    margins = [r["turnover_margin"] for r in box["turnover"]]
    assert margins[0] == -margins[1]
    assert all("team_id" in r for r in box["turnover"])


def test_own_recovery_excluded_from_defensive_players(monkeypatch):
    # #93: `defensive_players.fumble_recoveries` counts TAKEAWAYS only. In 401135269
    # BYU's (252) returner recovered his own kickoff fumble -- an own recovery, not a
    # defensive event, so it must NOT appear in any defensive_players row. Hawaii's
    # (62) Manly Williams recovery of a BYU fumble IS a takeaway and must remain.
    box = _box(monkeypatch, 401135269)
    byu_recs = [
        d for d in box["defensive_players"] if d.get("def_pos_team") == 252 and d.get("fumble_recoveries", 0) > 0
    ]
    assert not byu_recs, "BYU own kickoff-return recovery leaked into defensive_players"
    haw_recs = [
        d for d in box["defensive_players"] if d.get("def_pos_team") == 62 and d.get("fumble_recoveries", 0) > 0
    ]
    assert haw_recs, "Hawaii takeaway recovery dropped from defensive_players"


def test_split_sacks_credit_half_each(monkeypatch):
    # #93: an assisted sack (`sack_player_name2` populated) splits 0.5/0.5 so per-player
    # sacks sum to the team's sack-play count. 401032062 has 3 sack plays per side, two
    # of Western Michigan's (2711) and two of BYU's (252) being assisted.
    box = _box(monkeypatch, 401032062)
    sacks = {
        (d["def_pos_team"], d["player_name"]): d.get("sacks", 0) for d in box["defensive_players"] if d.get("sacks", 0)
    }
    assert sacks[(2711, "Corvin Moment")] == 0.5
    assert sacks[(2711, "Ali Fayad")] == 0.5
    assert sacks[(2711, "Eric Assoua")] == 1.0
    assert sacks[(252, "Lorenzo Fauatea")] == 1.5  # one solo + one assisted
    # team totals stay whole -- 3 sack plays per side
    for team in (2711, 252):
        assert sum(v for (t, _), v in sacks.items() if t == team) == 3.0


def test_split_sack_yards_split_half_each(monkeypatch):
    # Sack yardage follows the same 0.5/0.5 convention as the sack credit itself.
    box = _box(monkeypatch, 401032062)
    yards = {
        (d["def_pos_team"], d["player_name"]): d.get("sacks_yards", 0)
        for d in box["defensive_players"]
        if d.get("sacks", 0)
    }
    assert yards[(2711, "Corvin Moment")] == -5.5  # half of the -11 yard split sack
    assert yards[(2711, "Ali Fayad")] == -5.5


def test_forced_fumble_on_punt_return_credits_covering_team(monkeypatch):
    # #93: on a punt return the fumbler is the RETURNER, so the forcing player is on
    # the covering (punting) team. 401754571: Georgia Tech (59) punts, Syracuse (183)
    # returns and fumbles, forced by a GT coverage player -> credit GT, not SU.
    box = _box(monkeypatch, 401754571)
    ff = {
        (d["def_pos_team"], d["player_name"]): d.get("forced_fumbles", 0)
        for d in box["defensive_players"]
        if d.get("forced_fumbles", 0)
    }
    hamilton = [k for k in ff if "Hamilton" in k[1]]
    assert hamilton, "punt-return forced fumble missing from defensive_players"
    assert hamilton[0][0] == 59, "punt-return forced fumble credited to the fumbling team"


def test_kick_returns_credited_to_receiving_team(monkeypatch):
    # #93: kickoff returns are credited to the receiving team, the same convention punt
    # returns use. ESPN files a kickoff under the receiving team's possession, so
    # `kick_return_team` and `pos_team` agree on every kick-return play here; the
    # specialists section must never file a returner under the kicking team.
    import polars as pl

    summary = _load(401135269)

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())
    proc = CFBPlayProcess(gameId=401135269)
    proc.join_participants = False
    proc.espn_cfb_pbp()
    out = proc.run_processing_pipeline()
    df = pl.from_dicts(proc.plays_json, infer_schema_length=None)
    kr = df.filter(pl.col("kickoff_return_player_name").is_not_null())
    assert kr.height > 0, "expected kickoff returns in this game"
    assert (kr["kick_return_team"] == kr["pos_team"]).all()
    assert (kr["kick_return_team"] != kr["def_pos_team"]).all()
    returners = {
        (d["pos_team"], d["player_name"]) for d in out["advBoxScore"]["specialists"] if d.get("kick_returns", 0)
    }
    assert returners, "kick returners missing from specialists"
    per_play = {
        (r["pos_team"], r["kickoff_return_player_name"])
        for r in kr.select("pos_team", "kickoff_return_player_name").to_dicts()
    }
    assert returners == per_play


def test_punt_returns_not_credited_to_punting_team(monkeypatch):
    # A punt return / fair catch is credited to the RETURNING team (def_pos_team on
    # the punt), never the punting team (pos_team). Both teams punt to each other, so
    # each legitimately fields the other's punts -- the invariant is per-play, not
    # "team 152 never returns". (Regression guard: the group-index extract bug used to
    # drop "fair catch by" / "returned by" returners, which masked this entirely.)
    import polars as pl

    summary = _load(401754598)

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())
    proc = CFBPlayProcess(gameId=401754598)
    proc.join_participants = False
    proc.espn_cfb_pbp()
    proc.run_processing_pipeline()
    df = pl.from_dicts(proc.plays_json, infer_schema_length=None)
    punts = df.filter((pl.col("punt") == True) & pl.col("punt_return_player_name").is_not_null())
    assert punts.height > 0, "expected punt returns/fair catches to be extracted in this game"
    # returner filed under the returning team, never the punting team (pos_team)
    assert (punts["punt_return_team"] == punts["def_pos_team"]).all()
    assert (punts["punt_return_team"] != punts["pos_team"]).all()


def test_penalty_yards_charged_to_penalized_team(monkeypatch):
    # In 401754598 the defensive-pass-interference flags occur on NC State (152) pass
    # plays but are FSU (52) fouls -> FSU must carry penalty_yards > 0.
    box = _box(monkeypatch, 401754598)
    fsu = _team(box["team"], 52)
    assert fsu.get("penalty_yards", 0) > 0


def test_penalty_box_has_both_teams(monkeypatch):
    box = _box(monkeypatch, 401754598)
    have_pen = {r["pos_team"]: r.get("penalty_yards") for r in box["team"]}
    assert 52 in have_pen and 152 in have_pen


def test_mirrored_end_yardline_is_repaired(monkeypatch):
    """A present-but-mirrored end.yardsToEndzone must be corrected, safely.

    ESPN sometimes reports end.yardsToEndzone from the wrong side of the field.
    Game 401868040: "pass complete ... for 34 yards to the VIL05" carries
    end.yardLine=5 with end.yardsToEndzone=95, so the EP model scored the
    offense as backed up on its own 5 rather than at the goal line -- the play
    took EPA -2.37 for a 34-yard gain, and the next play (a penalty, whose EPA
    folds in the discontinuity since the previous play ended) inherited the
    error as +6.45.

    There is deliberately no same-row test for this: yardsToEndzone equals
    yardLine on about half of plays and its complement on the other half,
    depending which side of the fifty the ball is on. Only the next play's
    start settles it, and only when possession is unchanged.
    """
    import polars as pl

    # end state mirrored (95) vs the next play's start (5), possession unchanged
    frame = pl.DataFrame(
        {
            "end.yardsToEndzone": [95, 40, 30, 65],
            "start.yardsToEndzone": [39, 5, 30, 35],
            "end.pos_team.id": ["1", "1", "1", "2"],
            "start.pos_team.id": ["1", "1", "1", "1"],
            "type.text": ["Pass Reception", "Rush", "Rush", "Punt"],
            "text": [
                "pass complete for 34 yards to the VIL05",
                "rush middle for 4 yards",
                "rush for no gain",
                "punt 40 yards",
            ],
        }
    )
    kickoff_vec = ["Kickoff", "Kickoff Return (Offense)"]
    out = frame.with_columns(
        pl.when(
            (pl.col("end.yardsToEndzone").is_null() == False)
            .and_(pl.col("start.yardsToEndzone").shift(-1).is_null() == False)
            .and_(pl.col("start.pos_team.id").shift(-1) == pl.col("end.pos_team.id"))
            .and_(pl.col("start.pos_team.id") == pl.col("end.pos_team.id"))
            .and_(pl.col("type.text").is_in(kickoff_vec) == False)
            .and_(
                pl.col("text").str.contains(r"(?i)kickoff|punt|field goal|extra point|touchback|kick attempt") == False
            )
            .and_(pl.col("text").str.contains(r"(?i)penalty") == False)
            .and_(pl.col("end.yardsToEndzone") != pl.col("start.yardsToEndzone").shift(-1))
            .and_(pl.col("end.yardsToEndzone") == (pl.lit(100) - pl.col("start.yardsToEndzone").shift(-1)))
        )
        .then(pl.col("start.yardsToEndzone").shift(-1))
        .otherwise(pl.col("end.yardsToEndzone"))
        .alias("end.yardsToEndzone")
    )["end.yardsToEndzone"].to_list()

    assert out[0] == 5, "mirrored end state should adopt the next play's start"
    assert out[1] == 40, "a non-mirrored disagreement must be left alone"
    assert out[2] == 30, "an already-consistent end state must not move"
    assert out[3] == 65, "a punt must never be touched: possession legitimately flips"
