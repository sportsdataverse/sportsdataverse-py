"""Offline parse-parity tests for sportsdataverse.wnba.wnba_engine.

Monkeypatches the three module-level _fetch_* helpers so no network calls are
made.  All assertions use the real columns emitted by the shared nba/ cores.

Independent-oracle validation gates (Task 2)
---------------------------------------------
Three oracle gates prove WNBA reconstruction correctness using the same
external sources as the NBA keystone — no fixture is regenerated from the
engine's own output:

1. MINUTES RECONCILIATION: sum each player's on-court real-time from the WNBA
   rotation stints (``(OUT_TIME_REAL - IN_TIME_REAL) / 10``) and assert it
   matches that player's ``boxScoreTraditional`` minutes within 1.5 s.

2. POSSESSION POINTS: total possession ``points`` per ``offense_team_id`` must
   equal the boxscore team points (exact).

3. ROSTER MEMBERSHIP: every player id in ``home_player_1..5`` /
   ``away_player_1..5`` from ``wnba_on_court()`` must belong to the
   corresponding team's boxscore roster — no phantom or stray ids.
"""

import json
import pathlib
import re

import numpy as np
import pandas as pd
import polars as pl
import pytest

import sportsdataverse.wnba.wnba_engine as W
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_lineups import (
    boxscore_home_away,
    parse_rotation_resultsets,
    players_on_court_from_rotation,
)
from sportsdataverse.nba.nba_possessions import attach_possession_lineups, build_possessions
from sportsdataverse.nba.nba_rapm import RAPM_SCHEMA

FXR = pathlib.Path("tests/fixtures/wnba_engine")
GAMES = ["1022400001", "1022400003"]


def _patch(monkeypatch, gid: str) -> None:
    fx = FXR / gid
    monkeypatch.setattr(W, "_fetch_pbp", lambda g: json.loads((fx / "playbyplayv3.json").read_text()))
    monkeypatch.setattr(W, "_fetch_rotation", lambda g: json.loads((fx / "gamerotation.json").read_text()))
    monkeypatch.setattr(W, "_fetch_box", lambda g: json.loads((fx / "boxscoretraditionalv3.json").read_text()))


def test_wnba_enhanced_pbp_offline(monkeypatch: pytest.MonkeyPatch) -> None:
    """wnba_enhanced_pbp() returns a non-empty frame with the ENHANCED_PBP_SCHEMA columns."""
    _patch(monkeypatch, GAMES[0])
    df = W.wnba_enhanced_pbp(GAMES[0])
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    for c in ("game_id", "action_number", "period", "seconds_remaining", "is_substitution", "team_id"):
        assert c in df.columns, f"missing column {c!r}"


def test_wnba_enhanced_pbp_second_game(monkeypatch: pytest.MonkeyPatch) -> None:
    """wnba_enhanced_pbp() works for the second fixture game."""
    _patch(monkeypatch, GAMES[1])
    df = W.wnba_enhanced_pbp(GAMES[1])
    assert df.height > 0
    assert df.schema["game_id"] == pl.Utf8


def test_wnba_on_court_offline(monkeypatch: pytest.MonkeyPatch) -> None:
    """wnba_on_court() returns a non-empty frame with home_player_1..5 / away_player_1..5."""
    _patch(monkeypatch, GAMES[0])
    oc = W.wnba_on_court(GAMES[0])
    assert isinstance(oc, pl.DataFrame)
    assert oc.height > 0
    cols = [f"home_player_{i}" for i in range(1, 6)] + [f"away_player_{i}" for i in range(1, 6)]
    for c in cols:
        assert c in oc.columns, f"missing column {c!r}"


def test_wnba_possessions_offline(monkeypatch: pytest.MonkeyPatch) -> None:
    """wnba_possessions() returns a non-empty possession frame with lineup columns."""
    _patch(monkeypatch, GAMES[0])
    poss = W.wnba_possessions(GAMES[0])
    assert isinstance(poss, pl.DataFrame)
    assert poss.height > 0
    for c in ("points", "offense_team_id", "off_player_1", "def_player_1"):
        assert c in poss.columns, f"missing column {c!r}"


def test_wnba_enhanced_pbp_return_as_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    """wnba_enhanced_pbp(return_as_pandas=True) returns a pandas DataFrame."""
    _patch(monkeypatch, GAMES[0])
    result = W.wnba_enhanced_pbp(GAMES[0], return_as_pandas=True)
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0
    assert "game_id" in result.columns


def test_wnba_on_court_return_as_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    """wnba_on_court(return_as_pandas=True) returns a pandas DataFrame."""
    _patch(monkeypatch, GAMES[0])
    result = W.wnba_on_court(GAMES[0], return_as_pandas=True)
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0
    assert "home_player_1" in result.columns


def test_wnba_possessions_return_as_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    """wnba_possessions(return_as_pandas=True) returns a pandas DataFrame."""
    _patch(monkeypatch, GAMES[0])
    result = W.wnba_possessions(GAMES[0], return_as_pandas=True)
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0
    assert "off_player_1" in result.columns


# ---------------------------------------------------------------------------
# Independent-oracle validation gates (Task 2)
# ---------------------------------------------------------------------------

# Tolerance for minutes reconciliation: the boxscore rounds minutes to whole
# seconds and the rotation IN/OUT times are tenths-of-a-second, so a small
# amount of rounding slack is expected.  Mirror the NBA keystone constant.
_MINUTES_TOLERANCE_SEC = 1.5


def _parse_box_minutes(minutes_str: str) -> float:
    """Parse a boxscore minutes value into seconds.

    Handles both the "MM:SS" form and the ISO-8601 ``PT..M..S`` form.

    Copied from tests.nba.test_nba_lineups (same helper, same contract).
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


def _raw_box(game_id: str) -> dict:
    return json.loads((FXR / game_id / "boxscoretraditionalv3.json").read_text())


def _raw_rotation(game_id: str) -> dict:
    return json.loads((FXR / game_id / "gamerotation.json").read_text())


@pytest.mark.parametrize("game_id", GAMES)
def test_wnba_minutes_reconciliation(game_id: str) -> None:
    """Rotation stint minutes must match the independent boxscore minutes oracle (WNBA).

    Summing ``(OUT_TIME_REAL - IN_TIME_REAL) / 10`` per player from the WNBA
    rotation feed and comparing to ``boxScoreTraditional`` minutes (an
    independent source) is the strongest available correctness check on the
    stint data that drives the on-court reconstruction.

    Uses the same tolerance and method as the NBA keystone
    ``test_minutes_reconciliation`` in ``tests/nba/test_nba_lineups.py``.
    """
    # No monkeypatch needed — reads the fixture files directly via _raw_rotation/_raw_box.
    from sportsdataverse.nba.nba_lineups import parse_rotation_resultsets

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

    print(f"[{game_id}] WNBA minutes reconciliation: max_error={max_err:.1f}s over {len(rotation_sec)} players")
    assert not failures, (
        f"[{game_id}] WNBA minutes reconciliation FAILED "
        f"({len(failures)} players over {_MINUTES_TOLERANCE_SEC}s):\n" + "\n".join(failures[:10])
    )


@pytest.mark.parametrize("game_id", GAMES)
def test_wnba_possession_points_reconcile(monkeypatch: pytest.MonkeyPatch, game_id: str) -> None:
    """Total possession points per offense team MUST equal boxscore points (WNBA).

    The boxscore is an independent external oracle — not derived from the
    engine's own output.  Mirrors ``test_possessions_reconcile_boxscore_points``
    in ``tests/nba/test_nba_possessions.py``.
    """
    from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
    from sportsdataverse.nba.nba_possessions import build_possessions

    _patch(monkeypatch, game_id)

    payload = json.loads((FXR / game_id / "playbyplayv3.json").read_text())
    enh = enhanced_pbp_from_payload(payload, league_id="10")
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
def test_wnba_oncourt_ids_in_roster(monkeypatch: pytest.MonkeyPatch, game_id: str) -> None:
    """Every player id in the on-court frame must be on the team's boxscore roster.

    Checks that ``wnba_on_court()`` never emits phantom or stray player ids —
    all ``home_player_1..5`` ids must belong to the home team's boxscore
    roster and all ``away_player_1..5`` ids to the away team's roster.
    This is the no-swap / no-stray guard analogous to the NBA roster-membership
    assertion in ``test_attach_possession_lineups``.
    """
    _patch(monkeypatch, game_id)
    oc = W.wnba_on_court(game_id)

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

    assert not stray, f"[{game_id}] WNBA on-court roster-membership FAILED ({len(stray)} stray id(s)):\n" + "\n".join(
        stray[:10]
    )


# ---------------------------------------------------------------------------
# Task 3: wnba_rapm_from_games() tests
# ---------------------------------------------------------------------------


def _game_poss_wnba(gid: str) -> pl.DataFrame:
    """Build WNBA possession frame from captured fixtures for game *gid*."""
    enh = enhanced_pbp_from_payload(json.loads((FXR / gid / "playbyplayv3.json").read_text()))
    home, away = boxscore_home_away(_raw_box(gid))
    oc = players_on_court_from_rotation(
        enh,
        parse_rotation_resultsets(_raw_rotation(gid)),
        home_team_id=home,
        away_team_id=away,
    )
    return attach_possession_lineups(build_possessions(enh), oc, enh, home_team_id=home)


def test_wnba_rapm_smoke(monkeypatch: pytest.MonkeyPatch) -> None:
    """wnba_rapm_from_games returns a valid RAPM frame over 2 offline games."""
    by_game = {g: _game_poss_wnba(g) for g in GAMES}
    monkeypatch.setattr(W, "_fetch_possessions", lambda gid: by_game[gid])
    out = W.wnba_rapm_from_games(GAMES)
    assert out.height > 0
    assert np.isfinite(out["rapm"].to_numpy()).all()
    assert dict(out.schema) == RAPM_SCHEMA
    assert abs(out["rapm"].mean()) < 5.0  # ridge-centered
    # Deterministic: same input → same sorted output
    out2 = W.wnba_rapm_from_games(GAMES)
    assert out.sort("player_id").equals(out2.sort("player_id"))
    # pandas path
    import pandas as pd

    assert isinstance(W.wnba_rapm_from_games(GAMES, return_as_pandas=True), pd.DataFrame)


def test_wnba_rapm_from_games_empty_list() -> None:
    """Empty game_ids returns a zero-row frame with RAPM_SCHEMA, no network call, no raise."""
    out = W.wnba_rapm_from_games([])
    assert out.height == 0
    assert dict(out.schema) == RAPM_SCHEMA


def test_wnba_rapm_from_games_skips_empty_games(monkeypatch: pytest.MonkeyPatch) -> None:
    """A game whose possession frame is empty is silently skipped; valid game still produces output."""
    by_game: dict[str, pl.DataFrame] = {
        "bad_game": pl.DataFrame(),
        GAMES[0]: _game_poss_wnba(GAMES[0]),
    }
    monkeypatch.setattr(W, "_fetch_possessions", lambda gid: by_game[gid])
    out = W.wnba_rapm_from_games(["bad_game", GAMES[0]])
    assert out.height > 0
    assert dict(out.schema) == RAPM_SCHEMA


def test_wnba_rapm_from_games_all_empty_nonempty_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-empty game_ids but every fetch empty → hits `if not frames` guard, returns RAPM_SCHEMA frame."""
    monkeypatch.setattr(W, "_fetch_possessions", lambda gid: pl.DataFrame())
    out = W.wnba_rapm_from_games(["x", "y"])
    assert out.height == 0
    assert dict(out.schema) == RAPM_SCHEMA


# ---------------------------------------------------------------------------
# Task 3: public export smoke
# ---------------------------------------------------------------------------


def test_wnba_engine_public_exports() -> None:
    """wnba_enhanced_pbp / wnba_on_court / wnba_possessions / wnba_rapm_from_games are importable from sportsdataverse.wnba."""
    from sportsdataverse.wnba import (  # noqa: F401
        wnba_enhanced_pbp,
        wnba_on_court,
        wnba_possessions,
        wnba_rapm_from_games,
    )


# ---------------------------------------------------------------------------
# Task 3: gated live tests
# ---------------------------------------------------------------------------

from tests.conftest import skip_if_no_nba_stats_live


@skip_if_no_nba_stats_live
def test_wnba_on_court_live() -> None:
    oc = W.wnba_on_court(GAMES[0])
    assert oc.height > 0


@skip_if_no_nba_stats_live
def test_wnba_rapm_from_games_live() -> None:
    out = W.wnba_rapm_from_games([GAMES[0]])
    assert out.height > 0
    assert np.isfinite(out["rapm"].to_numpy()).all()


# ---------------------------------------------------------------------------
# Rotation clock: the mapper must time events on WNBA periods, not the NBA's
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("game_id", GAMES)
def test_wnba_rotation_lineups_reproduce_box_minutes(game_id: str) -> None:
    """On-court time read off the MAPPED lineups must match the box minutes.

    ``test_wnba_minutes_reconciliation`` sums the raw stints, so it cannot see
    the mapping. Timed on NBA 720 s quarters, every event landed 120 s per period
    behind the rotation clock: on these games a player's mapped time was off the
    box by up to 466 s (mean 113 s). Each event's lineup holds until the next
    event in the period, so event granularity alone leaves well under a minute.
    """
    enh = enhanced_pbp_from_payload(json.loads((FXR / game_id / "playbyplayv3.json").read_text()))
    box = _raw_box(game_id)
    home, away = boxscore_home_away(box)
    oc = players_on_court_from_rotation(
        enh, parse_rotation_resultsets(_raw_rotation(game_id)), home_team_id=home, away_team_id=away
    )
    ev = oc.join(enh.select("action_number", "seconds_remaining"), on="action_number", how="left").with_columns(
        (pl.col("seconds_remaining") - pl.col("seconds_remaining").shift(-1).over("period").fill_null(0.0)).alias("dur")
    )
    slots = [f"{s}_player_{i}" for s in ("home", "away") for i in range(1, 6)]
    onc = dict(
        ev.select("dur", *slots)
        .unpivot(index="dur", value_name="pid")
        .drop_nulls("pid")
        .group_by("pid")
        .agg(pl.col("dur").sum())
        .iter_rows()
    )
    box_sec = {
        int(p["personId"]): _parse_box_minutes(p["statistics"].get("minutes"))
        for side in ("homeTeam", "awayTeam")
        for p in box["boxScoreTraditional"][side]["players"]
    }
    err = [abs(onc.get(pid, 0.0) - sec) for pid, sec in box_sec.items()]
    assert max(err) <= 60.0, f"[{game_id}] mapped on-court time off the box by {max(err):.0f}s"
    assert sum(err) / len(err) <= 20.0, f"[{game_id}] mean mapped-minutes error {sum(err) / len(err):.0f}s"


def test_regulation_clock_by_league_and_era() -> None:
    """WNBA quarters (2006+) are 600 s, its halves (1997-2005) 1200 s; NBA / G League 720 s."""
    from sportsdataverse.nba.nba_lineups import _elapsed_time, _is_period_start, _regulation_clock

    assert _regulation_clock("1022400001") == (4, 600.0)
    assert _regulation_clock("1020500001") == (2, 1200.0)
    assert _regulation_clock("1020600001") == (4, 600.0)
    assert _regulation_clock("0022200001") == (4, 720.0)
    assert _regulation_clock("2022300001") == (4, 720.0)
    assert _regulation_clock("1522400001") == (4, 600.0)  # Summer League: 10-minute quarters
    # An undecodable id (enhanced pbp can carry game_id "None") must not hand a WNBA game the NBA clock.
    assert _regulation_clock("None", "10") == (4, 600.0)
    assert _regulation_clock("None") == (4, 720.0)
    assert _regulation_clock("1022400001", "00") == (4, 720.0)  # explicit league wins
    assert _regulation_clock("None", 10) == (4, 600.0)  # an int league id is normalised, not misread as NBA
    wnba = _regulation_clock("1022400001")
    # The real 2021 case: checked in at IN_TIME_REAL 23921 = 7.9 s left in Q4.
    assert _elapsed_time(4, 7.9, wnba) == pytest.approx(23921.0)
    assert _elapsed_time(5, 300.0, wnba) == pytest.approx(24000.0)  # OT opens at end of regulation
    assert _elapsed_time(3, 300.0, (2, 1200.0)) == pytest.approx(24000.0)  # halves-era OT tip
    assert _elapsed_time(4, 7.9) == pytest.approx(28721.0)  # NBA default unchanged
    assert _is_period_start(2, 600.0, wnba) and not _is_period_start(2, 599.0, wnba)


@pytest.mark.parametrize("game_id", GAMES)
def test_rotation_coverage_error_flags_overlapping_stints(game_id: str) -> None:
    """A real payload covers five players' game exactly; a duplicated stint does not."""
    from sportsdataverse.nba.nba_lineups import _rotation_coverage_error

    rot = parse_rotation_resultsets(_raw_rotation(game_id))
    assert _rotation_coverage_error(rot) < 0.005
    key = "HomeTeam" if rot.get("HomeTeam") else "homeTeam"
    longest = max(rot[key], key=lambda s: float(s["OUT_TIME_REAL"]) - float(s["IN_TIME_REAL"]))
    assert _rotation_coverage_error({**rot, key: [*rot[key], longest]}) > 0.02
    # One team missing entirely, or stopping before the other (no overtime rows), is malformed too.
    assert _rotation_coverage_error({**rot, key: []}) == pytest.approx(1.0)
    other = "AwayTeam" if key == "HomeTeam" else "awayTeam"
    last = max(float(s["OUT_TIME_REAL"]) for s in rot[other])
    extended = [{**s, "OUT_TIME_REAL": last + 3000.0} if float(s["OUT_TIME_REAL"]) == last else s for s in rot[other]]
    assert _rotation_coverage_error({**rot, other: extended}) > 0.02
    # Negative clocks (as in the WNBA 2004-05 payloads) are malformed whatever they sum to.
    shifted = [{**s, "IN_TIME_REAL": -2320.0} if s is longest else s for s in rot[key]]
    assert _rotation_coverage_error({**rot, key: shifted}) == float("inf")


def _overlapping(raw: dict) -> dict:
    """The fixture's rotation with every home stint duplicated (ten 'on court')."""
    out = json.loads(json.dumps(raw))
    for rs in out["resultSets"]:
        if rs["name"] == "HomeTeam":
            rs["rowSet"] = rs["rowSet"] + rs["rowSet"]
    return out


@pytest.mark.parametrize("game_id", GAMES)
def test_malformed_rotation_maps_to_an_empty_frame(game_id: str) -> None:
    """The guard lives in the mapper itself, so every caller sees the rejection."""
    enh = enhanced_pbp_from_payload(json.loads((FXR / game_id / "playbyplayv3.json").read_text()))
    home, away = boxscore_home_away(_raw_box(game_id))
    rot = parse_rotation_resultsets(_overlapping(_raw_rotation(game_id)))
    oc = players_on_court_from_rotation(enh, rot, home_team_id=home, away_team_id=away, league_id="10")
    assert oc.is_empty()


@pytest.mark.parametrize("game_id", GAMES)
def test_wnba_engine_falls_back_to_pbp_lineups_on_malformed_rotation(
    monkeypatch: pytest.MonkeyPatch, game_id: str
) -> None:
    """wnba_on_court / wnba_possessions have no quarter_box step: a rejected rotation must not
    leave null lineups on every possession; the pbp substitution chain stands in."""
    _patch(monkeypatch, game_id)
    raw = _raw_rotation(game_id)
    monkeypatch.setattr(W, "_fetch_rotation", lambda g: _overlapping(raw))
    oc = W.wnba_on_court(game_id)
    assert oc.height > 0
    poss = W.wnba_possessions(game_id)
    slots = [f"{s}_player_{i}" for s in ("off", "def") for i in range(1, 6)]
    assert poss.height > 0
    assert poss.select(pl.sum_horizontal([pl.col(c).is_null() for c in slots]).sum()).item() == 0
