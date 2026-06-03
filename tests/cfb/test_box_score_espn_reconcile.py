"""Reconciliation tests: computed advanced box score vs ESPN official box score.

Each game's ESPN official box is embedded in its fixture summary payload under
``summary["boxscore"]["teams"]``.  These tests assert that the computed
``advBoxScore["turnover"]`` rows (turnovers / Int / fumbles_lost) match
ESPN's own numbers exactly, plus two invariant checks (antisymmetric margin
and the decomposition turnovers == Int + fumbles_lost).

All 5 fixtures are tested offline -- no network is hit.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

GIDS = [401754598, 401309854, 401112081, 401135269, 401032062]

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
    proc.espn_cfb_pbp()
    out = proc.run_processing_pipeline()
    return out["advBoxScore"]


def _espn_team_stats(summary: dict) -> dict[int, dict]:
    """Return {team_id -> {turnovers, fumblesLost, interceptions}} from ESPN box.

    Keys are cast to int.  Missing statistics default to 0.
    """
    result: dict[int, dict] = {}
    for team_entry in summary.get("boxscore", {}).get("teams", []):
        raw_id = team_entry.get("team", {}).get("id")
        if raw_id is None:
            continue
        team_id = int(raw_id)
        stat_map = {st["name"]: st["displayValue"] for st in team_entry.get("statistics", [])}
        result[team_id] = {
            "turnovers": int(stat_map.get("turnovers", 0)),
            "fumblesLost": int(stat_map.get("fumblesLost", 0)),
            "interceptions": int(stat_map.get("interceptions", 0)),
        }
    return result


@pytest.mark.parametrize("gid", GIDS)
def test_turnovers_match_espn_official_box(monkeypatch, gid):
    """Computed turnovers / Int / fumbles_lost must equal ESPN's official box for every team."""
    summary = _load(gid)
    espn_stats = _espn_team_stats(summary)
    computed_rows = _box(monkeypatch, gid)["turnover"]

    for team_id, espn in espn_stats.items():
        matches = [r for r in computed_rows if r.get("team_id") == team_id]
        assert matches, (
            f"gid={gid}: team_id={team_id} not found in computed turnover rows; "
            f"available team_ids={[r.get('team_id') for r in computed_rows]}"
        )
        computed = matches[0]

        # The output fields are sourced directly from ESPN's official box (espn_sourced).
        assert computed["turnovers"] == espn["turnovers"], (
            f"gid={gid}, team_id={team_id}: "
            f"computed turnovers={computed['turnovers']} != ESPN turnovers={espn['turnovers']}"
        )
        assert computed["Int"] == espn["interceptions"], (
            f"gid={gid}, team_id={team_id}: "
            f"computed Int={computed['Int']} != ESPN interceptions={espn['interceptions']}"
        )
        assert computed["fumbles_lost"] == espn["fumblesLost"], (
            f"gid={gid}, team_id={team_id}: "
            f"computed fumbles_lost={computed['fumbles_lost']} != ESPN fumblesLost={espn['fumblesLost']}"
        )
        # The independent play-by-play DERIVATION must also match ESPN -- this is the real
        # cross-check that the attribution logic is correct (the output above is sourced
        # from ESPN, so it would pass trivially; *_pbp validates the computation).
        assert computed["turnovers_pbp"] == espn["turnovers"], (
            f"gid={gid}, team_id={team_id}: pbp-derived turnovers={computed['turnovers_pbp']} "
            f"!= ESPN turnovers={espn['turnovers']}"
        )
        assert computed["Int_pbp"] == espn["interceptions"], (
            f"gid={gid}, team_id={team_id}: pbp-derived Int={computed['Int_pbp']} "
            f"!= ESPN interceptions={espn['interceptions']}"
        )
        assert computed["fumbles_lost_pbp"] == espn["fumblesLost"], (
            f"gid={gid}, team_id={team_id}: pbp-derived fumbles_lost={computed['fumbles_lost_pbp']} "
            f"!= ESPN fumblesLost={espn['fumblesLost']}"
        )


@pytest.mark.parametrize("gid", GIDS)
def test_turnover_margin_antisymmetric(monkeypatch, gid):
    """Turnover margin must be equal and opposite for the two teams."""
    computed_rows = _box(monkeypatch, gid)["turnover"]
    assert len(computed_rows) == 2, f"gid={gid}: expected 2 turnover rows, got {len(computed_rows)}"
    margins = [r["turnover_margin"] for r in computed_rows]
    assert margins[0] == -margins[1], f"gid={gid}: turnover margins are not antisymmetric: {margins[0]} vs {margins[1]}"


@pytest.mark.parametrize("gid", GIDS)
def test_turnovers_equal_int_plus_fumbles_lost(monkeypatch, gid):
    """For each computed row: turnovers == Int + fumbles_lost."""
    computed_rows = _box(monkeypatch, gid)["turnover"]
    for row in computed_rows:
        team_id = row.get("team_id")
        expected = row["Int"] + row["fumbles_lost"]
        assert row["turnovers"] == expected, (
            f"gid={gid}, team_id={team_id}: "
            f"turnovers={row['turnovers']} != Int({row['Int']}) + fumbles_lost({row['fumbles_lost']}) = {expected}"
        )


@pytest.mark.parametrize("gid", GIDS)
def test_espn_team_section_matches_official_box(monkeypatch, gid):
    """The espn_team section surfaces ESPN's official team totals verbatim."""
    summary = _load(gid)
    espn_stats = _espn_team_stats(summary)
    box = _box(monkeypatch, gid)
    assert "espn_team" in box and len(box["espn_team"]) == 2, f"gid={gid}: espn_team section missing/incomplete"
    by_id = {r["team_id"]: r for r in box["espn_team"]}
    for team_id, espn in espn_stats.items():
        row = by_id.get(team_id)
        assert row is not None, f"gid={gid}: team {team_id} missing from espn_team"
        assert row["turnovers"] == espn["turnovers"]
        assert row["fumblesLost"] == espn["fumblesLost"]
        assert row["interceptions"] == espn["interceptions"]
        # authoritative yardage is present and self-consistent (total = passing + rushing)
        assert row["totalYards"] == row["netPassingYards"] + row["rushingYards"]


@pytest.mark.parametrize("gid", GIDS)
def test_espn_players_section_present(monkeypatch, gid):
    """The espn_players section is populated with ESPN's official player stat lines."""
    box = _box(monkeypatch, gid)
    players = box.get("espn_players")
    assert players, f"gid={gid}: espn_players section empty"
    cats = {r["category"] for r in players}
    # offensive categories are always present; defensive/special-teams categories are
    # included by ESPN inconsistently across games, so they are not required here.
    assert {"passing", "rushing", "receiving"} <= cats, f"gid={gid}: missing categories, got {cats}"
    # every row carries a team id (athlete may be null for ESPN's aggregate "Team" rows)
    assert all(r.get("team_id") is not None for r in players)
    # the vast majority of rows are real athletes (allow a few null-name "Team" rows)
    named = [r for r in players if r.get("athlete")]
    assert len(named) >= 0.8 * len(players), f"gid={gid}: too many unnamed player rows"
