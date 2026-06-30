"""Offline parse-parity tests for sportsdataverse.nbagl.nbagl_engine.

Monkeypatches the three module-level _fetch_* helpers so no network calls are
made.  All assertions use the real columns emitted by the shared nba/ cores.
"""

import json
import pathlib
import re

import numpy as np
import pandas as pd
import polars as pl
import pytest

import sportsdataverse.nbagl.nbagl_engine as G
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_lineups import (
    boxscore_home_away,
    parse_rotation_resultsets,
    players_on_court_from_rotation,
)
from sportsdataverse.nba.nba_possessions import attach_possession_lineups, build_possessions
from sportsdataverse.nba.nba_rapm import RAPM_SCHEMA

FXR = pathlib.Path("tests/fixtures/nbagl_engine")
GAMES = ["2022400003", "2022400009"]


def _patch(monkeypatch: pytest.MonkeyPatch, gid: str) -> None:
    fx = FXR / gid
    monkeypatch.setattr(G, "_fetch_pbp", lambda g: json.loads((fx / "playbyplayv3.json").read_text()))
    monkeypatch.setattr(G, "_fetch_rotation", lambda g: json.loads((fx / "gamerotation.json").read_text()))
    monkeypatch.setattr(G, "_fetch_box", lambda g: json.loads((fx / "boxscoretraditionalv3.json").read_text()))


def _raw_box(gid: str) -> dict:
    return json.loads((FXR / gid / "boxscoretraditionalv3.json").read_text())


def _raw_rotation(gid: str) -> dict:
    return json.loads((FXR / gid / "gamerotation.json").read_text())


def test_nbagl_enhanced_pbp_offline(monkeypatch: pytest.MonkeyPatch) -> None:
    """nbagl_enhanced_pbp() returns a non-empty frame with the ENHANCED_PBP_SCHEMA columns."""
    _patch(monkeypatch, GAMES[0])
    df = G.nbagl_enhanced_pbp(GAMES[0])
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    for c in ("game_id", "action_number", "period", "seconds_remaining", "is_substitution", "team_id"):
        assert c in df.columns, f"missing column {c!r}"


def test_nbagl_enhanced_pbp_second_game(monkeypatch: pytest.MonkeyPatch) -> None:
    """nbagl_enhanced_pbp() works for the second fixture game."""
    _patch(monkeypatch, GAMES[1])
    df = G.nbagl_enhanced_pbp(GAMES[1])
    assert df.height > 0
    assert df.schema["game_id"] == pl.Utf8


def test_nbagl_on_court_offline(monkeypatch: pytest.MonkeyPatch) -> None:
    """nbagl_on_court() returns a non-empty frame with home_player_1..5 / away_player_1..5."""
    _patch(monkeypatch, GAMES[0])
    oc = G.nbagl_on_court(GAMES[0])
    assert isinstance(oc, pl.DataFrame)
    assert oc.height > 0
    cols = [f"home_player_{i}" for i in range(1, 6)] + [f"away_player_{i}" for i in range(1, 6)]
    for c in cols:
        assert c in oc.columns, f"missing column {c!r}"


def test_nbagl_possessions_offline(monkeypatch: pytest.MonkeyPatch) -> None:
    """nbagl_possessions() returns a non-empty possession frame with lineup columns."""
    _patch(monkeypatch, GAMES[0])
    poss = G.nbagl_possessions(GAMES[0])
    assert isinstance(poss, pl.DataFrame)
    assert poss.height > 0
    for c in ("points", "offense_team_id", "off_player_1", "def_player_1"):
        assert c in poss.columns, f"missing column {c!r}"


def test_nbagl_enhanced_pbp_return_as_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    """nbagl_enhanced_pbp(return_as_pandas=True) returns a pandas DataFrame."""
    _patch(monkeypatch, GAMES[0])
    result = G.nbagl_enhanced_pbp(GAMES[0], return_as_pandas=True)
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0
    assert "game_id" in result.columns


def test_nbagl_on_court_return_as_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    """nbagl_on_court(return_as_pandas=True) returns a pandas DataFrame."""
    _patch(monkeypatch, GAMES[0])
    result = G.nbagl_on_court(GAMES[0], return_as_pandas=True)
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


def test_nbagl_possessions_return_as_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    """nbagl_possessions(return_as_pandas=True) returns a pandas DataFrame."""
    _patch(monkeypatch, GAMES[0])
    result = G.nbagl_possessions(GAMES[0], return_as_pandas=True)
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


# ---------------------------------------------------------------------------
# Task 3: nbagl_rapm_from_games() tests
# ---------------------------------------------------------------------------


def test_nbagl_rapm_from_games_empty_list() -> None:
    """Empty game_ids returns a zero-row frame with RAPM_SCHEMA, no network call, no raise."""
    out = G.nbagl_rapm_from_games([])
    assert out.height == 0
    assert dict(out.schema) == RAPM_SCHEMA


def test_nbagl_rapm_from_games_skips_empty_games(monkeypatch: pytest.MonkeyPatch) -> None:
    """A game whose possession frame is empty is silently skipped; valid game still produces output."""
    from sportsdataverse.nba.nba_lineups import (
        parse_rotation_resultsets,
    )
    from sportsdataverse.nba.nba_possessions import build_possessions

    def _game_poss_gl(gid: str) -> pl.DataFrame:
        enh = enhanced_pbp_from_payload(
            json.loads((FXR / gid / "playbyplayv3.json").read_text()),
            league_id="20",
        )
        home, away = boxscore_home_away(_raw_box(gid))
        oc = players_on_court_from_rotation(
            enh,
            parse_rotation_resultsets(_raw_rotation(gid)),
            home_team_id=home,
            away_team_id=away,
        )
        return attach_possession_lineups(build_possessions(enh), oc, enh, home_team_id=home)

    by_game: dict[str, pl.DataFrame] = {
        "bad_game": pl.DataFrame(),
        GAMES[0]: _game_poss_gl(GAMES[0]),
    }
    monkeypatch.setattr(G, "_fetch_possessions", lambda gid: by_game[gid])
    out = G.nbagl_rapm_from_games(["bad_game", GAMES[0]])
    assert out.height > 0
    assert dict(out.schema) == RAPM_SCHEMA


def test_nbagl_rapm_from_games_all_empty_nonempty_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-empty game_ids but every fetch empty → hits `if not frames` guard, returns RAPM_SCHEMA frame."""
    monkeypatch.setattr(G, "_fetch_possessions", lambda gid: pl.DataFrame())
    out = G.nbagl_rapm_from_games(["x", "y"])
    assert out.height == 0
    assert dict(out.schema) == RAPM_SCHEMA


# ---------------------------------------------------------------------------
# Task 3: public export smoke
# ---------------------------------------------------------------------------


def test_nbagl_engine_public_exports() -> None:
    """nbagl_enhanced_pbp / nbagl_on_court / nbagl_possessions / nbagl_rapm_from_games are importable from sportsdataverse.nbagl."""
    from sportsdataverse.nbagl import (  # noqa: F401
        nbagl_enhanced_pbp,
        nbagl_on_court,
        nbagl_possessions,
        nbagl_rapm_from_games,
    )


# ---------------------------------------------------------------------------
# Task 2: independent-oracle validation gates (G-League)
# ---------------------------------------------------------------------------
#
# Three oracle gates prove G-League reconstruction correctness using the same
# external sources as the NBA/WNBA keystones — no fixture is regenerated from
# the engine's own output:
#
# 1. MINUTES RECONCILIATION: sum each player's on-court real-time from the
#    G-League rotation stints (``(OUT_TIME_REAL - IN_TIME_REAL) / 10``) and
#    assert it matches that player's ``boxScoreTraditional`` minutes within
#    1.5 s (same tolerance as NBA/WNBA keystones — NOT widened).
#
# 2. POSSESSION POINTS: total possession ``points`` per ``offense_team_id``
#    must equal the boxscore team points (exact).
#
# 3. ROSTER MEMBERSHIP: every player id in ``home_player_1..5`` /
#    ``away_player_1..5`` from ``nbagl_on_court()`` must belong to the
#    corresponding team's boxscore roster — no phantom or stray ids.

# Tolerance for minutes reconciliation: the boxscore rounds minutes to whole
# seconds and the rotation IN/OUT times are tenths-of-a-second, so a small
# amount of rounding slack is expected.  Mirror the NBA/WNBA keystone constant.
_MINUTES_TOLERANCE_SEC = 1.5


def _parse_box_minutes(minutes_str: str) -> float:
    """Parse a boxscore minutes value into seconds.

    Handles both the "MM:SS" form and the ISO-8601 ``PT..M..S`` form.

    Copied from tests.nba.test_nba_lineups (same helper, same contract);
    mirrored verbatim from tests.wnba.test_wnba_engine.
    """
    s = (minutes_str or "").strip()
    if not s:
        return 0.0
    iso = re.match(r"PT(\d+)M([\d.]+)S", s)
    if iso:
        return int(iso.group(1)) * 60.0 + float(iso.group(2))
    parts = s.split(":")
    if len(parts) == 2:
        return int(parts[0]) * 60.0 + float(parts[1])
    return 0.0


@pytest.mark.parametrize("game_id", GAMES)
def test_nbagl_minutes_reconciliation(game_id: str) -> None:
    """Rotation stint minutes must match the independent boxscore minutes oracle (G-League).

    Summing ``(OUT_TIME_REAL - IN_TIME_REAL) / 10`` per player from the G-League
    rotation feed and comparing to ``boxScoreTraditional`` minutes (an
    independent source) is the strongest available correctness check on the
    stint data that drives the on-court reconstruction.

    Uses the same tolerance and method as the NBA keystone
    ``test_minutes_reconciliation`` in ``tests/nba/test_nba_lineups.py`` and
    the WNBA mirror in ``tests/wnba/test_wnba_engine.py``.
    """
    # No monkeypatch needed — reads the fixture files directly via _raw_rotation/_raw_box.

    rotation = parse_rotation_resultsets(_raw_rotation(game_id))

    # Sum rotation on-court seconds per player (tenths-of-a-second → seconds).
    rotation_sec: dict[int, float] = {}
    for team in ("HomeTeam", "AwayTeam"):
        for stint in rotation.get(team, []):
            pid = int(stint["PERSON_ID"])
            dur = (float(stint["OUT_TIME_REAL"]) - float(stint["IN_TIME_REAL"])) / 10.0
            rotation_sec[pid] = rotation_sec.get(pid, 0.0) + dur

    # Boxscore minutes per player (independent oracle).
    bxt = _raw_box(game_id).get("boxScoreTraditional", {})
    box_sec: dict[int, float] = {}
    for side in ("homeTeam", "awayTeam"):
        for p in bxt.get(side, {}).get("players", []):
            pid = int(p["personId"])
            box_sec[pid] = _parse_box_minutes(p.get("statistics", {}).get("minutes", ""))

    all_pids = set(rotation_sec) | set(box_sec)
    failures: list[str] = []
    max_err = 0.0
    for pid in all_pids:
        # DNP players (box_sec[pid]=0; rotation_sec miss → .get(pid, 0.0)) give diff=0 and pass — correct.
        diff = abs(rotation_sec.get(pid, 0.0) - box_sec.get(pid, 0.0))
        max_err = max(max_err, diff)
        if diff > _MINUTES_TOLERANCE_SEC:
            failures.append(
                f"  pid={pid}: rotation={rotation_sec.get(pid, 0.0):.1f}s "
                f"boxscore={box_sec.get(pid, 0.0):.1f}s diff={diff:.1f}s"
            )

    print(f"[{game_id}] G-League minutes reconciliation: max_error={max_err:.1f}s over {len(rotation_sec)} players")
    assert not failures, (
        f"[{game_id}] G-League minutes reconciliation FAILED "
        f"({len(failures)} players over {_MINUTES_TOLERANCE_SEC}s):\n" + "\n".join(failures[:10])
    )


@pytest.mark.parametrize("game_id", GAMES)
def test_nbagl_possession_points_reconcile(monkeypatch: pytest.MonkeyPatch, game_id: str) -> None:
    """Total possession points per offense team MUST equal boxscore points (G-League).

    The boxscore is an independent external oracle — not derived from the
    engine's own output.  Mirrors ``test_possessions_reconcile_boxscore_points``
    in ``tests/nba/test_nba_possessions.py`` and the WNBA mirror in
    ``tests/wnba/test_wnba_engine.py``.
    """

    _patch(monkeypatch, game_id)

    payload = json.loads((FXR / game_id / "playbyplayv3.json").read_text())
    enh = enhanced_pbp_from_payload(payload, league_id="20")
    poss = build_possessions(enh)

    assert poss.height > 0, f"Game {game_id}: build_possessions returned empty frame"

    # Engine totals: possession points grouped by offense team.
    eng: dict[int, int] = {
        int(r["offense_team_id"]): int(r["points"])
        for r in poss.group_by("offense_team_id").agg(pl.col("points").sum().alias("points")).to_dicts()
    }

    # Independent oracle: sum player points per team from the boxscore.
    bxt = _raw_box(game_id)["boxScoreTraditional"]
    oracle: dict[int, int] = {}
    for side in ("homeTeam", "awayTeam"):
        t = bxt[side]
        pts = sum(int(p.get("statistics", {}).get("points", 0) or 0) for p in t["players"])
        oracle[int(t["teamId"])] = pts

    for team_id, expected_pts in oracle.items():
        got_pts = eng.get(team_id, 0)
        assert got_pts == expected_pts, (
            f"Game {game_id}, team {team_id}: possession points={got_pts} != boxscore={expected_pts}"
        )


@pytest.mark.parametrize("game_id", GAMES)
def test_nbagl_oncourt_ids_in_roster(monkeypatch: pytest.MonkeyPatch, game_id: str) -> None:
    """Every player id in the on-court frame must be on the team's boxscore roster.

    Checks that ``nbagl_on_court()`` never emits phantom or stray player ids —
    all ``home_player_1..5`` ids must belong to the home team's boxscore
    roster and all ``away_player_1..5`` ids to the away team's roster.
    This is the no-swap / no-stray guard analogous to the NBA roster-membership
    assertion in ``test_attach_possession_lineups`` and the WNBA mirror in
    ``tests/wnba/test_wnba_engine.py``.
    """
    _patch(monkeypatch, game_id)
    oc = G.nbagl_on_court(game_id)

    # Build independent roster oracle from boxscore.
    bxt = _raw_box(game_id)["boxScoreTraditional"]
    home_team_id = int(bxt["homeTeam"]["teamId"])
    away_team_id = int(bxt["awayTeam"]["teamId"])
    home_roster: set[int] = {int(p["personId"]) for p in bxt["homeTeam"]["players"]}
    away_roster: set[int] = {int(p["personId"]) for p in bxt["awayTeam"]["players"]}

    home_cols = [f"home_player_{i}" for i in range(1, 6)]
    away_cols = [f"away_player_{i}" for i in range(1, 6)]

    stray: list[str] = []
    for r in oc.to_dicts():
        for c in home_cols:
            pid = r.get(c)
            if pid is not None and int(pid) not in home_roster:
                stray.append(
                    f"  row action_number={r.get('action_number')}: "
                    f"home col {c!r} pid={pid} not in home roster "
                    f"(home_team_id={home_team_id})"
                )
        for c in away_cols:
            pid = r.get(c)
            if pid is not None and int(pid) not in away_roster:
                stray.append(
                    f"  row action_number={r.get('action_number')}: "
                    f"away col {c!r} pid={pid} not in away roster "
                    f"(away_team_id={away_team_id})"
                )

    assert not stray, (
        f"[{game_id}] G-League on-court roster-membership FAILED ({len(stray)} stray id(s)):\n" + "\n".join(stray[:10])
    )


# ---------------------------------------------------------------------------
# Task 3: helper for building possessions from fixtures
# ---------------------------------------------------------------------------


def _game_poss_nbagl(gid: str) -> pl.DataFrame:
    """Build G-League possession frame from captured fixtures for game *gid*."""
    enh = enhanced_pbp_from_payload(
        json.loads((FXR / gid / "playbyplayv3.json").read_text()),
        league_id="20",
    )
    home, away = boxscore_home_away(_raw_box(gid))
    oc = players_on_court_from_rotation(
        enh,
        parse_rotation_resultsets(_raw_rotation(gid)),
        home_team_id=home,
        away_team_id=away,
    )
    return attach_possession_lineups(build_possessions(enh), oc, enh, home_team_id=home)


# ---------------------------------------------------------------------------
# Task 3: RAPM smoke test
# ---------------------------------------------------------------------------


def test_nbagl_rapm_smoke(monkeypatch: pytest.MonkeyPatch) -> None:
    """nbagl_rapm_from_games returns a valid RAPM frame over 2 offline games."""
    by_game = {g: _game_poss_nbagl(g) for g in GAMES}
    monkeypatch.setattr(G, "_fetch_possessions", lambda gid: by_game[gid])
    out = G.nbagl_rapm_from_games(GAMES)
    assert out.height > 0
    assert np.isfinite(out["rapm"].to_numpy()).all()
    assert dict(out.schema) == RAPM_SCHEMA
    assert abs(out["rapm"].mean()) < 5.0  # ridge-centered
    # Deterministic: same input → same sorted output
    out2 = G.nbagl_rapm_from_games(GAMES)
    assert out.sort("player_id").equals(out2.sort("player_id"))
    # pandas path
    assert isinstance(G.nbagl_rapm_from_games(GAMES, return_as_pandas=True), pd.DataFrame)


# ---------------------------------------------------------------------------
# Task 3: gated live tests
# ---------------------------------------------------------------------------

from tests.conftest import skip_if_no_nba_stats_live


@skip_if_no_nba_stats_live
def test_nbagl_on_court_live() -> None:
    oc = G.nbagl_on_court(GAMES[0])
    assert oc.height > 0


@skip_if_no_nba_stats_live
def test_nbagl_rapm_from_games_live() -> None:
    out = G.nbagl_rapm_from_games([GAMES[0]])
    assert out.height > 0
    assert np.isfinite(out["rapm"].to_numpy()).all()
