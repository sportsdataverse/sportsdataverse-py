"""
gen_nbagl_engine_fixtures.py — dev-only fixture generator for the NBA G-League possession engine.

Usage
-----
    cd <worktree>
    uv run python tools/fixtures/gen_nbagl_engine_fixtures.py

    # Capture a specific game id pair (skips discovery):
    uv run python tools/fixtures/gen_nbagl_engine_fixtures.py --game-ids 2042400001 2042400002

This script is intentionally NOT part of the package wheel — it is a dev helper
that runs ONCE (or when fixtures need refreshing) and commits the output to
``tests/fixtures/nbagl_engine/<game_id>/``.

G-League game ids use LeagueID=20.  Their prefix format is confirmed by
the live leaguegamelog capture (see README.md in fixture_root once run).

Captured endpoints
------------------
- ``nba_stats_playbyplayv3``          → stats.nba.com (curl_cffi runtime, no league_id param)
- ``nba_stats_boxscoretraditionalv3`` → stats.nba.com (curl_cffi runtime, no league_id param)
- ``nba_stats_gamerotation``          → stats.nba.com (curl_cffi runtime, league_id="20" REQUIRED)

Rate-limit discipline
---------------------
- All live calls are spaced ≥ 3 seconds apart.
- Each call retries up to 3 times with exponential backoff (5s → 15s → 45s).
- If persistently blocked, the script reports and exits rather than hammering
  the API.

Rotation coverage
-----------------
G-League ``gamerotation`` is NOT universal — some games have empty HomeTeam/AwayTeam
rowSets.  This script scans up to MAX_ROTATION_CANDIDATES game ids and selects
the first 2 that have non-empty rotation stints before capturing the full fixture
set.  The coverage fraction is reported at the end.

Output schema (per game_id directory)
--------------------------------------
- ``playbyplayv3.json``          : raw v3 play-by-play payload
- ``boxscoretraditionalv3.json`` : raw v3 box score payload
- ``gamerotation.json``          : raw gamerotation payload
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Rate-limit helpers
# ---------------------------------------------------------------------------

_MIN_CALL_GAP_SECS: float = 3.0
_LAST_CALL_TIME: float = 0.0


def _throttle() -> None:
    """Sleep until at least _MIN_CALL_GAP_SECS have passed since the last call."""
    global _LAST_CALL_TIME
    elapsed = time.monotonic() - _LAST_CALL_TIME
    if elapsed < _MIN_CALL_GAP_SECS:
        time.sleep(_MIN_CALL_GAP_SECS - elapsed)
    _LAST_CALL_TIME = time.monotonic()


def _call_with_backoff(label: str, fn, *, max_retries: int = 3) -> dict:
    """Call *fn* with exponential backoff on failure.

    Backoff schedule: 5s → 15s → 45s.

    Args:
        label: Human-readable description for logging.
        fn: Zero-argument callable that returns a raw dict payload.
        max_retries: Maximum number of attempts.

    Returns:
        Raw dict payload on success.

    Raises:
        RuntimeError: If all retries are exhausted.
    """
    backoff_secs = [5.0, 15.0, 45.0]
    last_exc: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            _throttle()
            print(f"  [attempt {attempt}/{max_retries}] {label}")
            raw = fn()
            if not raw:
                raise ValueError(f"{label}: empty payload returned")
            return raw
        except Exception as exc:
            last_exc = exc
            print(f"  attempt {attempt} failed: {exc}")
            if attempt < max_retries:
                wait = backoff_secs[attempt - 1]
                print(f"  backing off {wait:.0f}s before retry...")
                time.sleep(wait)

    raise RuntimeError(f"All {max_retries} attempts failed for: {label}") from last_exc


# ---------------------------------------------------------------------------
# Step 1: discover G-League game ids via leaguegamelog
# ---------------------------------------------------------------------------

#: Maximum number of candidate game ids to probe for rotation coverage.
MAX_ROTATION_CANDIDATES: int = 20


def _rotation_is_populated(rot_raw: dict) -> bool:
    """Return True if rot_raw has non-empty HomeTeam AND AwayTeam rowSets.

    Args:
        rot_raw: Raw gamerotation payload (resultSets list).

    Returns:
        True when both HomeTeam and AwayTeam resultSets have ≥ 1 stint row;
        False otherwise (empty, missing, or only one side present).
    """
    result_sets = (rot_raw or {}).get("resultSets") or []
    counts: dict[str, int] = {}
    for rs in result_sets:
        name = rs.get("name", "")
        if name in ("HomeTeam", "AwayTeam"):
            counts[name] = len(rs.get("rowSet") or [])
    return counts.get("HomeTeam", 0) > 0 and counts.get("AwayTeam", 0) > 0


def discover_candidate_game_ids(season: str = "2024", n: int = MAX_ROTATION_CANDIDATES) -> list[str]:
    """Fetch G-League LeagueGameLog for *season* and return up to *n* distinct game ids.

    The game ids are returned in log order (most recent first for "Regular Season").
    We collect a pool of candidates so the caller can filter by rotation coverage.

    Args:
        season: G-League season year string (e.g. ``"2024"``).
        n: Maximum number of distinct game ids to return.

    Returns:
        List of up to *n* distinct G-League game id strings.

    Raises:
        RuntimeError: If the game log cannot be fetched or yields no game ids.
    """
    from sportsdataverse.nba.nba_stats import nba_stats_leaguegamelog  # noqa: PLC0415

    print(f"\n[Step 1] Fetching G-League LeagueGameLog for season={season!r} (league_id='20') ...")
    raw = _call_with_backoff(
        f"nba_stats_leaguegamelog(league_id='20', season={season!r}, season_type_all_star='Regular Season')",
        lambda: nba_stats_leaguegamelog(
            league_id="20",
            season=season,
            season_type_all_star="Regular Season",
            player_or_team_abbreviation="T",
            return_parsed=False,
        ),
    )

    # Parse resultSets to extract GAME_ID values
    result_sets = (raw or {}).get("resultSets") or []
    game_ids: list[str] = []
    seen: set[str] = set()

    for rs in result_sets:
        headers: list[str] = rs.get("headers") or []
        row_set: list[list] = rs.get("rowSet") or []
        if not headers or not row_set:
            continue
        try:
            gid_idx = headers.index("GAME_ID")
        except ValueError:
            continue
        for row in row_set:
            gid = str(row[gid_idx])
            if gid not in seen:
                seen.add(gid)
                game_ids.append(gid)
            if len(game_ids) >= n:
                break
        if len(game_ids) >= n:
            break

    if not game_ids:
        raise RuntimeError(
            f"No GAME_ID values found in G-League LeagueGameLog response for season={season!r}. "
            "Try season='2023' or '2025', or check the raw payload structure."
        )

    # Report the confirmed prefix format
    if game_ids:
        print(f"  CONFIRMED game_id prefix/format: first few ids = {game_ids[:5]}")
        print(f"  Total distinct game ids in pool: {len(game_ids)}")

    return game_ids


def select_games_with_rotation(
    candidate_ids: list[str],
    needed: int = 2,
) -> tuple[list[str], int, int]:
    """Probe candidate game ids for rotation coverage; return the first *needed* with data.

    Makes one live ``gamerotation`` call per candidate (≥3s apart).  Stops
    as soon as *needed* games with populated HomeTeam+AwayTeam rowSets are found
    or candidates are exhausted.

    Args:
        candidate_ids: Pool of G-League game id strings to probe (in order).
        needed: How many games with good rotation we want.

    Returns:
        Tuple of (``chosen_ids``, ``candidates_tried``, ``candidates_with_rotation``).
        ``chosen_ids`` has length ≤ *needed*.
    """
    from sportsdataverse.nba.nba_stats import nba_stats_gamerotation  # noqa: PLC0415

    print(f"\n[Step 2] Scanning up to {len(candidate_ids)} candidate(s) for populated rotation ...")

    chosen: list[str] = []
    tried: int = 0
    with_rotation: int = 0

    for gid in candidate_ids:
        if len(chosen) >= needed:
            break
        tried += 1
        try:
            rot_raw = _call_with_backoff(
                f"nba_stats_gamerotation(game_id={gid!r}, league_id='20')",
                lambda g=gid: nba_stats_gamerotation(game_id=g, league_id="20", return_parsed=False),
            )
        except RuntimeError as exc:
            print(f"  BLOCKED on {gid}: {exc}")
            print("  Rate-limit or persistent failure — stopping rotation scan early.")
            break

        populated = _rotation_is_populated(rot_raw)
        result_sets = (rot_raw or {}).get("resultSets") or []
        home_rows = next((len(rs.get("rowSet") or []) for rs in result_sets if rs.get("name") == "HomeTeam"), 0)
        away_rows = next((len(rs.get("rowSet") or []) for rs in result_sets if rs.get("name") == "AwayTeam"), 0)
        status = "GOOD" if populated else "SKIP (empty rotation)"
        print(f"  {gid}: HomeTeam={home_rows} stints, AwayTeam={away_rows} stints — {status}")

        if populated:
            with_rotation += 1
            chosen.append(gid)

    coverage_pct = (with_rotation / tried * 100) if tried > 0 else 0.0
    print(
        f"\n  Rotation scan complete: {tried} candidates tried, "
        f"{with_rotation} had populated rotation ({coverage_pct:.0f}% coverage rate), "
        f"{len(chosen)} selected for full capture."
    )

    return chosen, tried, with_rotation


# ---------------------------------------------------------------------------
# Step 3: capture the 3 payloads per chosen game
# ---------------------------------------------------------------------------


def capture_game_fixtures(game_id: str, fixture_dir: Path) -> dict[str, dict]:
    """Capture playbyplayv3, boxscoretraditionalv3, and gamerotation for one G-League game.

    Writes each payload to *fixture_dir* as JSON (indent=2).  Skips files that
    already exist on disk (idempotent re-runs).

    Args:
        game_id: G-League game id string.
        fixture_dir: Directory to write JSON fixtures into (created if absent).

    Returns:
        Dict with keys ``"pbp"``, ``"box"``, ``"rotation"`` → raw payload dicts.

    Raises:
        RuntimeError: If any endpoint fails all retries.
    """
    from sportsdataverse.nba.nba_stats import (  # noqa: PLC0415
        nba_stats_boxscoretraditionalv3,
        nba_stats_gamerotation,
        nba_stats_playbyplayv3,
    )

    fixture_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n[Step 3] Capturing payloads for game_id={game_id!r} -> {fixture_dir}")

    # --- play-by-play v3 ---
    pbp_path = fixture_dir / "playbyplayv3.json"
    if pbp_path.exists():
        print("  playbyplayv3.json already exists — loading from disk")
        pbp_raw = json.loads(pbp_path.read_text(encoding="utf-8"))
    else:
        pbp_raw = _call_with_backoff(
            f"nba_stats_playbyplayv3(game_id={game_id!r})",
            lambda: nba_stats_playbyplayv3(game_id=game_id, return_parsed=False),
        )
        pbp_path.write_text(json.dumps(pbp_raw, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"  wrote {pbp_path}")

    actions = (pbp_raw.get("game") or {}).get("actions") or []
    print(f"  playbyplayv3: {len(actions)} actions")

    # --- boxscore traditional v3 ---
    box_path = fixture_dir / "boxscoretraditionalv3.json"
    if box_path.exists():
        print("  boxscoretraditionalv3.json already exists — loading from disk")
        box_raw = json.loads(box_path.read_text(encoding="utf-8"))
    else:
        box_raw = _call_with_backoff(
            f"nba_stats_boxscoretraditionalv3(game_id={game_id!r})",
            lambda: nba_stats_boxscoretraditionalv3(game_id=game_id, return_parsed=False),
        )
        box_path.write_text(json.dumps(box_raw, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"  wrote {box_path}")

    bxt = (box_raw or {}).get("boxScoreTraditional") or {}
    print(f"  boxscoretraditionalv3: homeTeamId={bxt.get('homeTeamId')}, awayTeamId={bxt.get('awayTeamId')}")

    # --- game rotation (league_id="20" is required for G-League) ---
    rot_path = fixture_dir / "gamerotation.json"
    if rot_path.exists():
        print("  gamerotation.json already exists — loading from disk")
        rot_raw = json.loads(rot_path.read_text(encoding="utf-8"))
    else:
        rot_raw = _call_with_backoff(
            f"nba_stats_gamerotation(game_id={game_id!r}, league_id='20')",
            lambda: nba_stats_gamerotation(game_id=game_id, league_id="20", return_parsed=False),
        )
        rot_path.write_text(json.dumps(rot_raw, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"  wrote {rot_path}")

    result_sets = (rot_raw or {}).get("resultSets") or []
    print(f"  gamerotation: {len(result_sets)} resultSets")

    return {"pbp": pbp_raw, "box": box_raw, "rotation": rot_raw}


# ---------------------------------------------------------------------------
# Step 4: verify payload-shape parity vs NBA v3 cores
# ---------------------------------------------------------------------------


def verify_parity(game_id: str, payloads: dict[str, dict]) -> bool:
    """Assert that G-League v3 payloads have the structural keys the nba/ cores require.

    Args:
        game_id: G-League game id (for logging).
        payloads: Dict with keys ``"pbp"``, ``"box"``, ``"rotation"``.

    Returns:
        True if all checks pass; False on any divergence (divergences are printed).
    """
    print(f"\n[Step 4a] Verifying payload-shape parity for game_id={game_id!r}")
    ok = True

    # --- pbp checks ---
    pbp = payloads["pbp"]
    actions = (pbp.get("game") or {}).get("actions")
    if not actions:
        print("  FAIL pbp: payload['game']['actions'] missing or empty")
        ok = False
    else:
        sample = actions[0]
        required_pbp_keys = {"actionNumber", "period", "clock", "personId", "description"}
        missing = required_pbp_keys - set(sample.keys())
        if missing:
            print(f"  FAIL pbp: action dict missing keys: {sorted(missing)}")
            ok = False
        else:
            print(f"  PASS pbp: {len(actions)} actions; keys {sorted(required_pbp_keys)} present")

    # --- box checks ---
    box = payloads["box"]
    bxt = (box or {}).get("boxScoreTraditional") or {}
    home_id = bxt.get("homeTeamId")
    away_id = bxt.get("awayTeamId")
    if home_id is None or away_id is None:
        print("  FAIL box: boxScoreTraditional missing homeTeamId or awayTeamId")
        ok = False
    else:
        home_players = (bxt.get("homeTeam") or {}).get("players") or []
        away_players = (bxt.get("awayTeam") or {}).get("players") or []
        sample_player = (home_players or away_players or [None])[0]
        player_ok = True
        if sample_player is None:
            print("  WARN box: no players found in homeTeam or awayTeam")
        else:
            stats = sample_player.get("statistics") or {}
            if "minutes" not in stats:
                print("  FAIL box: player statistics missing 'minutes' key")
                player_ok = False
                ok = False
            if "position" not in sample_player:
                print("  FAIL box: player missing 'position' key")
                player_ok = False
                ok = False
        if player_ok:
            print(f"  PASS box: homeTeamId={home_id}, awayTeamId={away_id}; player statistics.minutes+position present")

    # --- rotation checks ---
    rot = payloads["rotation"]
    result_sets = (rot or {}).get("resultSets") or []
    rot_ok = True
    found_sides: list[str] = []
    for rs in result_sets:
        name = rs.get("name", "")
        if name not in ("HomeTeam", "AwayTeam"):
            continue
        found_sides.append(name)
        headers = rs.get("headers") or []
        row_set = rs.get("rowSet") or []
        required_rot_cols = {"PERSON_ID", "IN_TIME_REAL", "OUT_TIME_REAL"}
        missing_h = required_rot_cols - set(headers)
        if missing_h:
            print(f"  FAIL rotation {name}: missing columns {sorted(missing_h)}")
            rot_ok = False
            ok = False
        elif not row_set:
            print(f"  WARN rotation {name}: resultSet present but rowSet is empty")
        else:
            print(f"  PASS rotation {name}: {len(row_set)} stints; required columns present")

    if rot_ok and not any(side in found_sides for side in ("HomeTeam", "AwayTeam")):
        print("  FAIL rotation: no HomeTeam or AwayTeam resultSet found")
        ok = False

    verdict = "PASS" if ok else "FAIL"
    print(f"  Parity verdict for {game_id}: {verdict}")
    return ok


# ---------------------------------------------------------------------------
# Step 4b: smoke the nba/ cores on the captured fixtures (offline)
# ---------------------------------------------------------------------------


def smoke_nba_cores(game_id: str, fixture_dir: Path) -> bool:
    """Run the nba/ possession-engine cores on captured G-League fixtures.

    Checks: enh.height > 0; home/away are two distinct positive ints;
    oc non-empty; spot-check that an on-court row carries exactly 10 distinct ids.

    Args:
        game_id: G-League game id string.
        fixture_dir: Directory containing the three fixture JSON files.

    Returns:
        True if all smoke checks pass; False otherwise.
    """
    from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload  # noqa: PLC0415
    from sportsdataverse.nba.nba_lineups import (  # noqa: PLC0415
        boxscore_home_away,
        parse_rotation_resultsets,
        players_on_court_from_rotation,
    )

    print(f"\n[Step 4b] Smoking nba/ cores on G-League fixture for game_id={game_id!r}")

    pbp_raw = json.loads((fixture_dir / "playbyplayv3.json").read_text(encoding="utf-8"))
    box_raw = json.loads((fixture_dir / "boxscoretraditionalv3.json").read_text(encoding="utf-8"))
    rot_raw = json.loads((fixture_dir / "gamerotation.json").read_text(encoding="utf-8"))

    ok = True

    # enhanced_pbp_from_payload — G-League uses league_id="20"
    try:
        enh = enhanced_pbp_from_payload(pbp_raw, league_id="20")
    except Exception as exc:
        print(f"  FAIL enhanced_pbp_from_payload: {exc}")
        return False

    if enh.height == 0:
        print("  FAIL enh.height == 0 (expected > 0)")
        ok = False
    else:
        print(f"  enh.height = {enh.height}  OK")

    # boxscore_home_away
    try:
        home, away = boxscore_home_away(box_raw)
    except Exception as exc:
        print(f"  FAIL boxscore_home_away: {exc}")
        return False

    if home <= 0 or away <= 0 or home == away:
        print(f"  FAIL home/away: home={home}, away={away} (expected two distinct positive ints)")
        ok = False
    else:
        print(f"  home={home}, away={away}  OK")

    # players_on_court_from_rotation
    try:
        rot_parsed = parse_rotation_resultsets(rot_raw)
        oc = players_on_court_from_rotation(
            enh,
            rot_parsed,
            home_team_id=home,
            away_team_id=away,
        )
    except Exception as exc:
        print(f"  FAIL players_on_court_from_rotation: {exc}")
        return False

    if oc.height == 0:
        print("  FAIL oc.height == 0 (expected > 0)")
        ok = False
    else:
        print(f"  oc.height = {oc.height}  OK")

        # Spot-check: one on-court row has exactly 10 distinct player ids
        home_cols = [f"home_player_{i}" for i in range(1, 6)]
        away_cols = [f"away_player_{i}" for i in range(1, 6)]
        all_oc_cols = home_cols + away_cols
        if all(c in oc.columns for c in all_oc_cols):
            sample_row = oc.row(0, named=True)
            ids = {sample_row[c] for c in all_oc_cols}
            if len(ids) == 10:
                print(f"  10-distinct-ids check: PASS (row 0 has {len(ids)} distinct ids)")
            else:
                print(
                    f"  10-distinct-ids check: WARN — row 0 has {len(ids)} distinct ids (expected 10); "
                    "lineup gap at game start is acceptable"
                )
        else:
            missing_cols = [c for c in all_oc_cols if c not in oc.columns]
            print(f"  10-distinct-ids check: WARN — on-court frame missing columns: {missing_cols}")

    return ok


# ---------------------------------------------------------------------------
# Step 5: write README + commit instructions
# ---------------------------------------------------------------------------


def write_readme(
    captured_ids: list[str],
    fixture_root: Path,
    capture_date: str = "2026-06-29",
    game_id_prefix_note: str = "",
    rotation_coverage_note: str = "",
) -> None:
    """Write tests/fixtures/nbagl_engine/README.md documenting provenance.

    Args:
        captured_ids: List of captured G-League game id strings.
        fixture_root: Root fixture directory (``tests/fixtures/nbagl_engine/``).
        capture_date: ISO date string of capture.
        game_id_prefix_note: One-line note about the confirmed game_id prefix format.
        rotation_coverage_note: One-line note about rotation coverage findings.
    """
    lines: list[str] = [
        "# NBA G-League Engine Fixture Provenance",
        "",
        "Raw `return_parsed=False` payloads captured from `stats.nba.com` for the",
        "NBA G-League possession-engine offline test suite (Phase 5.1, Task 0).",
        "",
        "## Capture metadata",
        "",
        f"- **Capture date:** {capture_date}",
        "- **League ID:** 20 (NBA G-League)",
        "- **Season:** 2024 (Regular Season)",
        "- **Source:** `stats.nba.com` via `sportsdataverse.nba.nba_stats` curl_cffi runtime",
        "",
    ]

    if game_id_prefix_note:
        lines += [
            "## Confirmed game_id prefix / format",
            "",
            game_id_prefix_note,
            "",
        ]

    if rotation_coverage_note:
        lines += [
            "## G-League rotation coverage",
            "",
            rotation_coverage_note,
            "",
            "> **Important:** `gamerotation` coverage is not universal for G-League games.",
            "> Fixtures were selected to include only games with non-empty HomeTeam",
            "> AND AwayTeam stint rowSets (required for the keystone minutes-recon gate).",
            "",
        ]

    lines += [
        "## Game ids captured",
        "",
    ]
    for gid in captured_ids:
        lines.append(f"- `{gid}`")

    lines += [
        "",
        "## Files per game directory",
        "",
        "| File | Source endpoint | Notes |",
        "|---|---|---|",
        '| `playbyplayv3.json` | `GET https://stats.nba.com/stats/playbyplayv3?GameID=<id>` | Raw v3 PBP; `payload["game"]["actions"]` is the action list; no LeagueID param |',
        '| `boxscoretraditionalv3.json` | `GET https://stats.nba.com/stats/boxscoretraditionalv3?GameID=<id>` | Raw v3 box score; home/away team ids under `payload["boxScoreTraditional"]`; no LeagueID param |',
        "| `gamerotation.json` | `GET https://stats.nba.com/stats/gamerotation?GameID=<id>&LeagueID=20` | Raw rotation stints; HomeTeam/AwayTeam resultSets with PERSON_ID + IN/OUT_TIME_REAL; **LeagueID=20 required** |",
        "",
        "## Shape parity with NBA v3",
        "",
        "G-League v3 endpoints share the same payload shape as their NBA counterparts.",
        "The `sportsdataverse.nba` possession-engine cores",
        "(`enhanced_pbp_from_payload`, `boxscore_home_away`,",
        "`players_on_court_from_rotation`) accept these payloads directly with",
        '`league_id="20"` passed to `enhanced_pbp_from_payload`.',
        "",
        "## Rate-limit notes",
        "",
        "Calls were spaced ≥ 3 seconds apart with 3-retry exponential backoff",
        "(5s → 15s → 45s). `stats.nba.com` enforces TLS-fingerprint gating;",
        "the `curl_cffi` impersonation runtime is required.",
        "",
    ]

    readme_path = fixture_root / "README.md"
    readme_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n[Step 5] Wrote {readme_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(game_ids: Optional[list[str]] = None, season: str = "2024") -> None:
    """Discover G-League game ids, capture fixtures, verify parity, smoke cores, write README.

    Args:
        game_ids: Explicit game ids to capture (skips discovery + rotation scan if provided).
        season: G-League season to query for discovery (default ``"2024"``).
    """
    worktree = Path(__file__).resolve().parent.parent.parent
    fixture_root = worktree / "tests" / "fixtures" / "nbagl_engine"
    fixture_root.mkdir(parents=True, exist_ok=True)

    print("\n=== NBA G-League engine fixture generator ===")
    print(f"    worktree     : {worktree}")
    print(f"    fixture root : {fixture_root}")

    # Step 1 + 2: discover game ids and filter by rotation coverage
    candidates_tried: int = 0
    candidates_with_rotation: int = 0
    confirmed_prefix_note: str = ""

    if game_ids:
        chosen_ids = game_ids[:2]
        print(f"\n[Step 1+2] Using provided game_ids (skipping rotation scan): {chosen_ids}")
    else:
        # Discover a pool of candidates first (one live call)
        try:
            candidate_pool = discover_candidate_game_ids(season=season, n=MAX_ROTATION_CANDIDATES)
        except RuntimeError as exc:
            print(f"\nERROR during G-League game id discovery: {exc}")
            print("BLOCKED — cannot proceed. Report this to the session owner.")
            sys.exit(1)

        if candidate_pool:
            first_id = candidate_pool[0]
            confirmed_prefix_note = (
                f"First game id from live leaguegamelog (season={season}): `{first_id}` "
                f"(format: {len(first_id)}-digit string, prefix=`{first_id[:2]}`). "
                f"Sample pool: {candidate_pool[:5]}"
            )
            print(f"\n  CONFIRMED prefix note: {confirmed_prefix_note}")

        # Rotation coverage scan
        try:
            chosen_ids, candidates_tried, candidates_with_rotation = select_games_with_rotation(
                candidate_pool, needed=2
            )
        except Exception as exc:
            print(f"\nERROR during rotation scan: {exc}")
            sys.exit(1)

        if not chosen_ids:
            print(
                f"\nNo games with populated rotation found after {candidates_tried} candidate(s). "
                "G-League rotation coverage may be very low for this season. "
                "Try a different season or provide explicit --game-ids."
            )
            sys.exit(1)

        cov_pct = candidates_with_rotation / candidates_tried * 100 if candidates_tried else 0.0
        rotation_coverage_note = (
            f"{candidates_tried} candidates tried; {candidates_with_rotation} had populated "
            f"HomeTeam+AwayTeam rotation ({cov_pct:.0f}% coverage rate)."
        )

    captured_ids: list[str] = []
    all_parity_ok = True
    all_smoke_ok = True
    rotation_coverage_note_final = (
        f"{candidates_tried} candidates tried; {candidates_with_rotation} had populated rotation."
        if not game_ids
        else "Game ids provided explicitly — rotation scan skipped."
    )

    for gid in chosen_ids:
        fixture_dir = fixture_root / gid
        print(f"\n{'=' * 60}")
        print(f"Processing game_id={gid!r}")
        print(f"{'=' * 60}")

        try:
            payloads = capture_game_fixtures(gid, fixture_dir)
        except RuntimeError as exc:
            print(f"\nERROR capturing payloads for {gid}: {exc}")
            print("Rate-limit or network issue — stopping after partial capture.")
            break

        captured_ids.append(gid)

        parity_ok = verify_parity(gid, payloads)
        if not parity_ok:
            print(f"\nParity DIVERGENCE detected for {gid} — check above for details.")
            print("This may indicate a G-League-vs-NBA shape difference; do NOT silently force it.")
            all_parity_ok = False

        smoke_ok = smoke_nba_cores(gid, fixture_dir)
        if not smoke_ok:
            print(f"\nSmoke check FAILED for {gid} — see details above.")
            all_smoke_ok = False

        # Space games apart too
        if gid != chosen_ids[-1]:
            print("\n  Sleeping 5s before next game...")
            time.sleep(5.0)

    if not captured_ids:
        print("\nNo games captured successfully. Exiting.")
        sys.exit(1)

    # Step 5: README
    write_readme(
        captured_ids,
        fixture_root,
        capture_date="2026-06-29",
        game_id_prefix_note=confirmed_prefix_note,
        rotation_coverage_note=rotation_coverage_note_final,
    )

    # Summary
    print(f"\n{'=' * 60}")
    print("Summary")
    print(f"{'=' * 60}")
    print(f"  Games captured : {len(captured_ids)} / {len(chosen_ids)}")
    print(f"  Game ids       : {captured_ids}")
    print(f"  Parity OK      : {all_parity_ok}")
    print(f"  Smoke OK       : {all_smoke_ok}")
    if not game_ids:
        print(f"  Rotation coverage: {rotation_coverage_note_final}")
    print("\nNext steps:")
    print("  git add tests/fixtures/nbagl_engine/ tools/fixtures/gen_nbagl_engine_fixtures.py")
    print('  git commit -m "test(nbagl): capture G-League v3 possession-engine fixtures"')
    print("  (No AI Co-Authored-By trailer)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Capture NBA G-League v3 possession-engine fixtures for offline testing."
    )
    parser.add_argument(
        "--game-ids",
        nargs="+",
        metavar="GAME_ID",
        help="Explicit G-League game ids to capture (skips discovery and rotation scan).",
    )
    parser.add_argument(
        "--season",
        default="2024",
        help="G-League season year for discovery (default: 2024).",
    )
    args = parser.parse_args()
    main(game_ids=args.game_ids, season=args.season)
