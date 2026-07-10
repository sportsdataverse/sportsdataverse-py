from __future__ import annotations

import polars as pl

from sportsdataverse.nhl.nhl_zone_transitions import infer_zone_transitions, nhl_zone_transitions


def _ev(idx: int, team: str, zone: str, tdk: str, player: str, secs: int) -> dict:
    mm, ss = divmod(secs, 60)
    return {
        "game_id": "G1",
        "season": 2024,
        "event_idx": idx,
        "period": 1,
        "time_in_period": f"{mm:02d}:{ss:02d}",
        "type_desc_key": tdk,
        "event_owner_team_id": team,
        "zone_code": zone,
        "x_coord": 0.0,
        "y_coord": 0.0,
        "situation_code": "1551",
        "home_team_id": "10",
        "home_team_defending_side": "left",
        "winning_player_id": player if tdk == "faceoff" else None,
        "losing_player_id": None,
        "scoring_player_id": None,
        "assist1_player_id": None,
        "assist2_player_id": None,
        "shooting_player_id": player if tdk in ("shot-on-goal", "goal", "missed-shot") else None,
        "committed_player_id": None,
        "drawn_player_id": None,
        "penalty_type_code": None,
        "shot_type": None,
    }


def test_infer_entries_controlled_vs_dump() -> None:
    # team 10 carries N->O then shoots quickly (controlled);
    # team 20 carries N->O then loses it to a team-10 takeaway 6s later (dump).
    rows = [
        _ev(0, "10", "N", "faceoff", "A", 0),
        _ev(1, "10", "O", "shot-on-goal", "A", 2),  # entry (N->O) controlled: next 10-event soon
        _ev(2, "10", "O", "shot-on-goal", "A", 3),
        _ev(3, "20", "N", "takeaway", "B", 20),
        _ev(4, "20", "O", "missed-shot", "B", 22),  # entry (N->O) for 20
        _ev(5, "10", "D", "takeaway", "C", 30),  # 20's next own-event is >window later -> dump
        _ev(6, "20", "O", "shot-on-goal", "B", 45),
    ]
    pbp = pl.DataFrame(rows)
    tr = infer_zone_transitions(pbp).filter(pl.col("transition_type") == "entry").sort("event_idx")
    assert tr.height == 2
    first = tr.row(0, named=True)
    second = tr.row(1, named=True)
    assert first["player_id"] == "A" and first["controlled"] is True
    assert second["player_id"] == "B" and second["controlled"] is False


def test_zone_transitions_player_value() -> None:
    # 3 controlled entries (each N->O followed by a same-team event within 4s)
    # + 1 dump entry (last event, no quick follow-up).
    rows = [
        _ev(0, "10", "N", "faceoff", "A", 0),
        _ev(1, "10", "O", "shot-on-goal", "A", 2),  # entry, next @3 -> controlled
        _ev(2, "10", "O", "shot-on-goal", "A", 3),
        _ev(3, "10", "N", "giveaway", "A", 30),
        _ev(4, "10", "O", "shot-on-goal", "A", 32),  # entry, next @33 -> controlled
        _ev(5, "10", "O", "shot-on-goal", "A", 33),
        _ev(6, "10", "N", "giveaway", "A", 60),
        _ev(7, "10", "O", "shot-on-goal", "A", 62),  # entry, next @63 -> controlled
        _ev(8, "10", "O", "shot-on-goal", "A", 63),
        _ev(9, "10", "N", "giveaway", "A", 90),
        _ev(10, "10", "O", "shot-on-goal", "A", 120),  # entry, no next -> dump
    ]
    pbp = pl.DataFrame(rows)
    out = nhl_zone_transitions(pbp)
    a = out.filter(pl.col("player_id") == "A").row(0, named=True)
    assert a["controlled_entries"] == 3
    assert a["dump_entries"] == 1
    assert abs(a["controlled_entry_rate"] - 0.75) < 1e-9
    assert a["entry_value"] > 0


def test_empty_pbp_returns_schema() -> None:
    empty = nhl_zone_transitions(pl.DataFrame(schema={"type_desc_key": pl.Utf8}))
    assert empty.height == 0
    assert "entry_value" in empty.columns


def test_tags_override_controlled() -> None:
    rows = [
        _ev(0, "10", "N", "faceoff", "A", 0),
        _ev(1, "10", "O", "shot-on-goal", "A", 90),  # heuristic: dump (no quick follow)
    ]
    pbp = pl.DataFrame(rows)
    tags = pl.DataFrame({"game_id": ["G1"], "event_idx": [1], "controlled": [True]})
    tr = infer_zone_transitions(pbp, tags=tags).filter(pl.col("transition_type") == "entry")
    assert tr.row(0, named=True)["controlled"] is True  # tag overrides heuristic
