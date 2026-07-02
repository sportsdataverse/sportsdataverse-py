"""Tests for NBA possession construction — boxscore-reconciled oracle gate.

The primary gate is INDEPENDENT: total possession points per offense team
must equal the boxscore team points.  No fixture is regenerated from the
engine's own output — the boxscore is an external oracle.
"""

from __future__ import annotations

import json
import pathlib

import pandas as pd
import polars as pl
import pytest

from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_lineups import (
    boxscore_home_away,
    parse_rotation_resultsets,
    players_on_court_from_rotation,
)
from sportsdataverse.nba.nba_possessions import (
    POSSESSIONS_SCHEMA,
    _is_last_ft,
    attach_possession_lineups,
    build_possessions,
)
from tests.conftest import skip_if_no_nba_stats_live

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


def _box_team_rosters(box: dict) -> dict[int, set[int]]:
    """Return {team_id: {personId, ...}} from boxscore — every player on each team.

    This is the INDEPENDENT roster oracle used to detect a home/away off↔def
    swap: a possession's offense players must belong to the offense team's
    roster, and defense players to the defense team's roster.
    """
    b = box["boxScoreTraditional"]
    out: dict[int, set[int]] = {}
    for side in ("homeTeam", "awayTeam"):
        t = b[side]
        out[int(t["teamId"])] = {int(p["personId"]) for p in t["players"]}
    return out


def _oncourt(g: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return (oncourt_frame, enhanced_pbp) for fixture game *g*."""
    enh = _enh(g)
    rot = parse_rotation_resultsets(json.loads((FXROOT / g / "gamerotation.json").read_text()))
    home, away = boxscore_home_away(_box(g))
    return players_on_court_from_rotation(enh, rot, home_team_id=home, away_team_id=away), enh


# ---------------------------------------------------------------------------
# _is_last_ft unit tests
# ---------------------------------------------------------------------------


def test_is_last_ft_nba_behavior() -> None:
    """NBA/WNBA 'N of N' behavior preserved; G-League '{N}PT' closes its trip."""
    # NBA/WNBA: last FT of sequence → True
    assert _is_last_ft("Free Throw 2 of 2") is True
    assert _is_last_ft("Free Throw 1 of 1") is True
    assert _is_last_ft("Free Throw Flagrant 3 of 3") is True

    # NBA/WNBA: not last FT of sequence → False
    assert _is_last_ft("Free Throw 1 of 2") is False
    assert _is_last_ft("Free Throw 2 of 3") is False

    # Technical FT: never closes a possession trip (unchanged)
    assert _is_last_ft("Free Throw Technical") is False

    # G-League single-FT format: standalone trip → always last → True
    assert _is_last_ft("Free Throw 1PT") is True
    assert _is_last_ft("Free Throw 2PT") is True
    assert _is_last_ft("Free Throw 3PT") is True

    # G-League single-FT format is case-SENSITIVE (matches the existing
    # _FT_NofN_RE convention):
    assert _is_last_ft("Free Throw 2pt") is False  # lowercase 'pt' must NOT match
    assert _is_last_ft("Free Throw 2 PT") is True  # the regex's \s* tolerates a space variant


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
    assert set(POSSESSIONS_SCHEMA.keys()) == required
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


def test_unattributed_group_points_attributed_to_scoring_team() -> None:
    """A scoring group with no offense-attributable event must NOT leak points.

    Crafted minimal frame: a standalone made free throw whose possession group
    contains no scoring/shooting/rebound/turnover event with a usable
    ``location`` to set offense (the FT here carries an empty ``location``),
    yet ``score_home`` increments.  The point-leak fix must attribute that
    point to the scoring team (home) via score-delta direction, and total
    points must be preserved.

    This path is NOT exercised by the three real fixture games — it is a
    deliberate unit test of the structural correctness guard.
    """
    from sportsdataverse.nba import nba_pbp_constants as C

    home_id = 1610612738
    away_id = 1610612755

    # Build a tiny enhanced-pbp frame.  Rows (in order_index order):
    #   0: period start (no team, no score)
    #   1: an away-team made shot -> anchors away as a real team via location 'v'
    #      and gives a home/away identity anchor for both sides
    #   2: a home-team made shot  -> anchors home via location 'h'
    #   3: a standalone made free throw with EMPTY location but score_home++
    #      -> its group has no location-bearing scoring event, so offense
    #         resolves to 0; the fix must attribute the +1 to home by delta.
    rows = [
        {
            "game_id": "0099900001",
            "action_number": 1,
            "period": 1,
            "order_index": 0,
            "event_type": "period",
            "sub_type": "start",
            "location": "",
            "team_id": 0,
            "person_id": 0,
            "seconds_remaining": 720.0,
            "score_home": "",
            "score_away": "",
        },
        {
            "game_id": "0099900001",
            "action_number": 2,
            "period": 1,
            "order_index": 1,
            "event_type": "made_shot",
            "sub_type": "Jump Shot",
            "location": "v",
            "team_id": away_id,
            "person_id": 201,
            "seconds_remaining": 700.0,
            "score_home": "0",
            "score_away": "2",
        },
        {
            "game_id": "0099900001",
            "action_number": 3,
            "period": 1,
            "order_index": 2,
            "event_type": "made_shot",
            "sub_type": "Jump Shot",
            "location": "h",
            "team_id": home_id,
            "person_id": 101,
            "seconds_remaining": 680.0,
            "score_home": "2",
            "score_away": "2",
        },
        {
            # Standalone made FT: empty location -> offense unresolvable from
            # location, but score_home increments by 1.
            "game_id": "0099900001",
            "action_number": 4,
            "period": 1,
            "order_index": 3,
            "event_type": "free_throw",
            "sub_type": "Free Throw Technical",
            "location": "",
            "team_id": 0,
            "person_id": 0,
            "seconds_remaining": 660.0,
            "score_home": "3",
            "score_away": "2",
        },
    ]

    # Coerce to the enhanced-pbp dtypes the builder relies on.
    df = pl.DataFrame(
        rows,
        schema_overrides={
            "game_id": pl.Utf8,
            "action_number": pl.Int64,
            "period": pl.Int64,
            "order_index": pl.Int64,
            "event_type": pl.Utf8,
            "sub_type": pl.Utf8,
            "location": pl.Utf8,
            "team_id": pl.Int64,
            "person_id": pl.Int64,
            "seconds_remaining": pl.Float64,
            "score_home": pl.Utf8,
            "score_away": pl.Utf8,
        },
    )
    # Add the event-flag columns the enhanced frame normally carries (unused by
    # the builder, but keeps the input shape faithful).
    df = df.with_columns([(pl.col("event_type") == et.removeprefix("is_")).alias(et) for et in C.EVENT_FLAG_COLUMNS])

    poss = build_possessions(df)

    pts_by_team: dict[int, int] = {
        int(r["offense_team_id"]): int(r["points"])
        for r in poss.group_by("offense_team_id").agg(pl.col("points").sum().alias("points")).to_dicts()
    }

    # The standalone technical FT point (+1 home) MUST land on home, not be
    # absorbed into another team's possession.
    assert pts_by_team.get(home_id, 0) == 3, f"home points leaked: expected 3, got {pts_by_team.get(home_id, 0)}"
    assert pts_by_team.get(away_id, 0) == 2, f"away points wrong: expected 2, got {pts_by_team.get(away_id, 0)}"

    # Total points preserved: 3 (home) + 2 (away) == 5
    assert int(poss["points"].sum()) == 5


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


# ---------------------------------------------------------------------------
# Task 3: on-court lineup attachment (RAPM stint matrix)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("game_id", GAMES)
def test_attach_possession_lineups(game_id: str) -> None:
    """Every possession must carry exactly 5 offense + 5 defense player IDs, no nulls.

    The 10 on-court players at each possession's start action are assigned to
    ``off_player_1..5`` / ``def_player_1..5`` by comparing ``offense_team_id``
    to the home team.  Both quintuples must be sets of 5 distinct Int64 IDs.

    The roster-membership assertion is the no-swap guard: 5-distinct alone
    cannot detect a home/away off↔def swap, so we also assert each
    possession's offense players belong to the offense team's boxscore roster
    and defense players to the defense team's roster.
    """
    home, _away = boxscore_home_away(_box(game_id))
    oncourt, enh = _oncourt(game_id)
    poss = attach_possession_lineups(build_possessions(enh), oncourt, enh, home_team_id=home)

    off_cols = [f"off_player_{i}" for i in range(1, 6)]
    def_cols = [f"def_player_{i}" for i in range(1, 6)]

    # Every cell must be non-null.
    assert poss.select(off_cols + def_cols).null_count().sum_horizontal().sum() == 0, (
        f"Game {game_id}: null player IDs in lineup columns"
    )

    # Each possession's 5 offense players and 5 defense players must be distinct.
    for r in poss.head(20).to_dicts():
        off_set = {r[c] for c in off_cols}
        def_set = {r[c] for c in def_cols}
        assert len(off_set) == 5, f"Game {game_id}: duplicate offense player IDs in possession {r}"
        assert len(def_set) == 5, f"Game {game_id}: duplicate defense player IDs in possession {r}"

    # NO-SWAP INVARIANT (independent oracle): offense players ⊆ offense team's
    # roster, defense players ⊆ defense team's roster — on EVERY possession.
    rosters = _box_team_rosters(_box(game_id))
    for r in poss.to_dicts():
        off_team = int(r["offense_team_id"])
        def_team = int(r["defense_team_id"])
        off_players = {int(r[c]) for c in off_cols}
        def_players = {int(r[c]) for c in def_cols}
        assert off_players <= rosters[off_team], (
            f"Game {game_id}: offense players {off_players - rosters[off_team]} "
            f"not on offense team {off_team}'s roster (possible off/def swap)"
        )
        assert def_players <= rosters[def_team], (
            f"Game {game_id}: defense players {def_players - rosters[def_team]} "
            f"not on defense team {def_team}'s roster (possible off/def swap)"
        )


# ---------------------------------------------------------------------------
# Task 4: offline monkeypatch tests for the public nba_possessions() fetcher
# ---------------------------------------------------------------------------


def test_nba_possessions_offline(monkeypatch: pytest.MonkeyPatch) -> None:
    """nba_possessions() offline: monkeypatch _fetch_* to committed fixtures.

    Asserts:
    - Returns non-empty polars frame with required stint columns.
    - Frame matches the in-process pipeline (same height and schema).
    - return_as_pandas=True returns a pandas DataFrame.
    """
    import sportsdataverse.nba.nba_possessions as P

    g = "0022200001"
    monkeypatch.setattr(P, "_fetch_pbp", lambda gid, lg: json.loads((FXROOT / g / "playbyplayv3.json").read_text()))
    monkeypatch.setattr(
        P, "_fetch_rotation", lambda gid, lg: json.loads((FXROOT / g / "gamerotation.json").read_text())
    )
    monkeypatch.setattr(
        P, "_fetch_box", lambda gid, lg: json.loads((FXROOT / g / "boxscoretraditionalv3.json").read_text())
    )

    df = P.nba_possessions(g)

    # Non-empty frame with required columns
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    required_cols = {"off_player_1", "def_player_1", "points"}
    assert required_cols.issubset(set(df.columns)), f"Missing columns: {required_cols - set(df.columns)}"

    # Must match in-process pipeline exactly (excluding the lineup_source column
    # added by the public function — the reference is built from lower-level helpers).
    enh = enhanced_pbp_from_payload(json.loads((FXROOT / g / "playbyplayv3.json").read_text()))
    box = json.loads((FXROOT / g / "boxscoretraditionalv3.json").read_text())
    rot = parse_rotation_resultsets(json.loads((FXROOT / g / "gamerotation.json").read_text()))
    home, away = boxscore_home_away(box)
    oc = players_on_court_from_rotation(enh, rot, home_team_id=home, away_team_id=away)
    poss_ref = attach_possession_lineups(build_possessions(enh), oc, enh, home_team_id=home)
    assert df.height == poss_ref.height, f"Row count mismatch: fetcher={df.height}, in-process={poss_ref.height}"
    # Schema check: nba_possessions() adds lineup_source; drop it before comparing.
    assert "lineup_source" in df.columns, "nba_possessions() must include lineup_source column"
    assert df.drop("lineup_source").schema == poss_ref.schema, (
        f"Schema mismatch: fetcher={df.schema}, in-process={poss_ref.schema}"
    )

    # return_as_pandas=True
    # Re-patch since monkeypatch lambdas are stateless
    df_pd = P.nba_possessions(g, return_as_pandas=True)
    assert isinstance(df_pd, pd.DataFrame), f"Expected pd.DataFrame, got {type(df_pd)}"
    assert len(df_pd) > 0


# ---------------------------------------------------------------------------
# Task 4: gated live smoke test for nba_possessions()
# ---------------------------------------------------------------------------


@skip_if_no_nba_stats_live
def test_nba_possessions_live() -> None:
    """Live smoke: nba_possessions() returns non-empty frame and reconciles with boxscore.

    Gated behind SDV_PY_NBA_STATS_LIVE=1 — stats.nba.com hangs on datacenter
    IPs; run only from a residential IP.
    """
    from sportsdataverse.nba.nba_possessions import nba_possessions

    g = "0022200001"
    df = nba_possessions(g)

    assert isinstance(df, pl.DataFrame)
    assert df.height > 0, "Live nba_possessions() returned empty frame"
    assert {"off_player_1", "def_player_1", "points"}.issubset(set(df.columns))

    # Boxscore reconciliation (independent oracle): sum of possession points
    # per offense team must equal the boxscore team points.
    from sportsdataverse.nba.nba_possessions import _fetch_box

    box = _fetch_box(g)
    oracle = _box_team_points(box)

    eng: dict[int, int] = {
        int(r["offense_team_id"]): int(r["points"])
        for r in df.group_by("offense_team_id").agg(pl.col("points").sum().alias("points")).to_dicts()
    }
    for team_id, expected_pts in oracle.items():
        got_pts = eng.get(team_id, 0)
        assert got_pts == expected_pts, (
            f"Live game {g}, team {team_id}: possession points={got_pts} != boxscore={expected_pts}"
        )


# ---------------------------------------------------------------------------
# Task 5: lineup_source selector tests
# ---------------------------------------------------------------------------

import sportsdataverse.nba.nba_possessions as npm


def _install_fixture_fetchers(
    monkeypatch: pytest.MonkeyPatch,
    game_id: str,
    *,
    rotation_raises: bool = False,
    rotation_empty: bool = False,
) -> dict:
    root = FXROOT / game_id
    monkeypatch.setattr(npm, "_fetch_pbp", lambda g, lg: json.loads((root / "playbyplayv3.json").read_text()))
    monkeypatch.setattr(npm, "_fetch_box", lambda g, lg: json.loads((root / "boxscoretraditionalv3.json").read_text()))
    calls: dict = {"rotation": 0}

    def _rot(g: str, lg: str) -> dict:
        calls["rotation"] += 1
        if rotation_raises:
            raise RuntimeError("gamerotation throttled")
        if rotation_empty:
            return {"resultSets": []}
        return json.loads((root / "gamerotation.json").read_text())

    monkeypatch.setattr(npm, "_fetch_rotation", _rot)
    return calls


def test_lineup_source_pbp_skips_rotation(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _install_fixture_fetchers(monkeypatch, "0022200001")
    df = npm.nba_possessions("0022200001", lineup_source="pbp")
    assert calls["rotation"] == 0  # gamerotation never called
    assert df["lineup_source"].unique().to_list() == ["pbp"]
    assert df.height > 0


def test_lineup_source_auto_falls_back_on_rotation_error(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _install_fixture_fetchers(monkeypatch, "0022200001", rotation_raises=True)
    df = npm.nba_possessions("0022200001", lineup_source="auto")
    assert calls["rotation"] == 1  # tried rotation, then fell back
    assert df["lineup_source"].unique().to_list() == ["pbp"]


def test_lineup_source_rotation_default_marks_rotation(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fixture_fetchers(monkeypatch, "0022200001")
    df = npm.nba_possessions("0022200001", lineup_source="rotation")
    assert df["lineup_source"].unique().to_list() == ["rotation"]


@pytest.mark.parametrize("game_id", ["0022100001", "0022200001", "0022300001"])
def test_pbp_source_reconciles_boxscore_points(monkeypatch: pytest.MonkeyPatch, game_id: str) -> None:
    _install_fixture_fetchers(monkeypatch, game_id)
    df = npm.nba_possessions(game_id, lineup_source="pbp")
    got = {
        int(r["offense_team_id"]): int(r["points"])
        for r in df.group_by("offense_team_id").agg(pl.col("points").sum().alias("points")).to_dicts()
    }
    oracle = _box_team_points(_box(game_id))
    for team_id, expected in oracle.items():
        assert got.get(team_id, 0) == expected, f"{game_id} team {team_id}: {got.get(team_id, 0)} != {expected}"
