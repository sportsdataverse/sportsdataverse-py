"""Tests for NBA possession construction — boxscore-reconciled oracle gate.

The primary gate is INDEPENDENT: total possession points per offense team
must equal the boxscore team points.  No fixture is regenerated from the
engine's own output — the boxscore is an external oracle.
"""

from __future__ import annotations

import json
import pathlib

import polars as pl
import pytest

from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_possessions import POSSESSIONS_SCHEMA, build_possessions

FXROOT = pathlib.Path("tests/fixtures/nba_engine")
GAMES = ["0022200001", "0022300001", "0022100001"]


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _enh(game_id: str) -> pl.DataFrame:
    payload = json.loads((FXROOT / game_id / "playbyplayv3.json").read_text())
    return enhanced_pbp_from_payload(payload)


def _box(game_id: str) -> dict:
    return json.loads((FXROOT / game_id / "boxscoretraditionalv3.json").read_text())


def _box_team_points(box: dict) -> dict[int, int]:
    """Return {team_id: points} from boxscore — sum of player points."""
    b = box["boxScoreTraditional"]
    out: dict[int, int] = {}
    for side in ("homeTeam", "awayTeam"):
        t = b[side]
        pts = sum(int(p.get("statistics", {}).get("points", 0) or 0) for p in t["players"])
        out[int(t["teamId"])] = pts
    return out


# ---------------------------------------------------------------------------
# Schema / empty-frame contract
# ---------------------------------------------------------------------------


def test_possessions_schema_matches_constant() -> None:
    """POSSESSIONS_SCHEMA constant must define all required columns."""
    required = {
        "game_id",
        "period",
        "possession_number",
        "offense_team_id",
        "defense_team_id",
        "start_order_index",
        "end_order_index",
        "start_seconds_remaining",
        "end_seconds_remaining",
        "points",
        "is_second_chance",
    }
    assert required <= set(POSSESSIONS_SCHEMA.keys())
    assert POSSESSIONS_SCHEMA["game_id"] == pl.Utf8
    assert POSSESSIONS_SCHEMA["offense_team_id"] == pl.Int64
    assert POSSESSIONS_SCHEMA["defense_team_id"] == pl.Int64
    assert POSSESSIONS_SCHEMA["points"] == pl.Int64
    assert POSSESSIONS_SCHEMA["is_second_chance"] == pl.Boolean


def test_build_possessions_empty_input_never_raises() -> None:
    """Empty enhanced PBP returns a zero-row frame with the correct schema."""
    empty = pl.DataFrame(schema=POSSESSIONS_SCHEMA)
    result = build_possessions(empty)
    assert isinstance(result, pl.DataFrame)
    assert result.height == 0
    assert result.schema == pl.Schema(POSSESSIONS_SCHEMA)


# ---------------------------------------------------------------------------
# Independent oracle: boxscore points reconciliation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("game_id", GAMES)
def test_possessions_reconcile_boxscore_points(game_id: str) -> None:
    """Total possession points per offense team MUST equal boxscore points.

    This is the primary gate.  The boxscore is an independent external oracle —
    not derived from the engine's own output.
    """
    poss = build_possessions(_enh(game_id))
    assert poss.height > 0, f"Game {game_id}: build_possessions returned empty frame"

    # Verify schema compliance
    assert poss.schema["game_id"] == pl.Utf8
    assert poss.schema["offense_team_id"] == pl.Int64
    assert poss.schema["defense_team_id"] == pl.Int64
    assert poss.schema["points"] == pl.Int64
    assert poss.schema["is_second_chance"] == pl.Boolean

    # Sane possession count per team (NBA typically 90–115 per game)
    by_team = poss.group_by("offense_team_id").len()
    for n in by_team["len"].to_list():
        assert 80 <= n <= 125, f"Game {game_id}: implausible possession count {n} for a team (expected 80–125)"

    # INDEPENDENT ORACLE: possession points == boxscore points, per team
    eng: dict[int, int] = {
        int(r["offense_team_id"]): int(r["points"])
        for r in poss.group_by("offense_team_id").agg(pl.col("points").sum().alias("points")).to_dicts()
    }
    oracle = _box_team_points(_box(game_id))

    for team_id, expected_pts in oracle.items():
        got_pts = eng.get(team_id, 0)
        assert got_pts == expected_pts, (
            f"Game {game_id}, team {team_id}: possession points={got_pts} != boxscore={expected_pts}"
        )


# ---------------------------------------------------------------------------
# Structural sanity
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("game_id", GAMES)
def test_possessions_structural_sanity(game_id: str) -> None:
    """Structural invariants: ordering, IDs, non-negative points, second-chance."""
    poss = build_possessions(_enh(game_id))

    # possession_number must be monotonically increasing
    pn = poss["possession_number"].to_list()
    assert pn == sorted(pn), f"Game {game_id}: possession_number not monotone"
    assert pn[0] == 1, f"Game {game_id}: first possession_number should be 1"

    # offense != defense on every row
    assert poss.filter(pl.col("offense_team_id") == pl.col("defense_team_id")).height == 0, (
        f"Game {game_id}: offense_team_id == defense_team_id on some rows"
    )

    # points should be non-negative (a possession can score 0 but not negative)
    neg = poss.filter(pl.col("points") < 0)
    assert neg.height == 0, f"Game {game_id}: {neg.height} possessions with negative points"

    # is_second_chance: there should be some True and some False in an NBA game
    sc_count = poss.filter(pl.col("is_second_chance") == True).height  # noqa: E712
    assert sc_count > 0, f"Game {game_id}: no second-chance possessions found"

    # game_id column must match the fixture game_id
    assert poss["game_id"].unique().to_list() == [game_id], f"Game {game_id}: game_id column mismatch"

    # start_order_index <= end_order_index for every possession
    bad_order = poss.filter(pl.col("start_order_index") > pl.col("end_order_index"))
    assert bad_order.height == 0, f"Game {game_id}: {bad_order.height} possessions with start > end order_index"
