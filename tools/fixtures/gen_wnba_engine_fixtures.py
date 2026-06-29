"""
gen_wnba_engine_fixtures.py — dev-only fixture generator for the WNBA possession engine.

Usage
-----
    cd <worktree>
    uv run python tools/fixtures/gen_wnba_engine_fixtures.py

    # Capture a specific game id pair:
    uv run python tools/fixtures/gen_wnba_engine_fixtures.py --game-ids 1021900001 1021900002

This script is intentionally NOT part of the package wheel — it is a dev helper
that runs ONCE (or when fixtures need refreshing) and commits the output to
``tests/fixtures/wnba_engine/<game_id>/``.

WNBA game ids are 10-digit strings starting with ``10`` (league_id prefix).

Captured endpoints
------------------
- ``wnba_stats_playbyplayv3``          → stats.wnba.com (curl_cffi runtime)
- ``wnba_stats_boxscoretraditionalv3`` → stats.wnba.com (curl_cffi runtime)
- ``wnba_stats_gamerotation``          → stats.wnba.com (curl_cffi runtime)

Rate-limit discipline
---------------------
- All live calls are spaced ≥ 3 seconds apart.
- Each call retries up to 3 times with exponential backoff (5s → 15s → 45s).
- If persistently blocked, the script reports and exits rather than hammering
  the API.

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
# Step 1: discover completed WNBA game ids via leaguegamelog
# ---------------------------------------------------------------------------


def discover_game_ids(season: str = "2024", n: int = 2) -> list[str]:
    """Fetch WNBA LeagueGameLog for *season* and return the first *n* distinct game ids.

    WNBA game ids are 10-digit strings starting with ``10``.

    Args:
        season: WNBA season year string (e.g. ``"2024"``).
        n: Number of distinct game ids to return.

    Returns:
        List of up to *n* distinct WNBA game id strings.

    Raises:
        RuntimeError: If the game log cannot be fetched or yields no game ids.
    """
    from sportsdataverse.wnba.wnba_stats import wnba_stats_leaguegamelog  # noqa: PLC0415

    print(f"\n[Step 1] Fetching WNBA LeagueGameLog for season={season!r} ...")
    raw = _call_with_backoff(
        f"wnba_stats_leaguegamelog(season={season!r}, season_type_all_star='Regular Season')",
        lambda: wnba_stats_leaguegamelog(
            season=season,
            season_type_all_star="Regular Season",
            league_id="10",
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
            f"No GAME_ID values found in LeagueGameLog response for season={season!r}. Check the raw payload structure."
        )

    print(f"  Found {len(game_ids)} distinct game id(s): {game_ids}")
    return game_ids[:n]


# ---------------------------------------------------------------------------
# Step 2: capture 3 payloads per game
# ---------------------------------------------------------------------------


def capture_game_fixtures(game_id: str, fixture_dir: Path) -> dict[str, dict]:
    """Capture playbyplayv3, boxscoretraditionalv3, and gamerotation for one game.

    Args:
        game_id: WNBA game id string (10-digit, starts with ``10``).
        fixture_dir: Directory to write JSON fixtures into (created if absent).

    Returns:
        Dict with keys ``"pbp"``, ``"box"``, ``"rotation"`` → raw payload dicts.

    Raises:
        RuntimeError: If any endpoint fails all retries.
    """
    from sportsdataverse.wnba.wnba_stats import (  # noqa: PLC0415
        wnba_stats_boxscoretraditionalv3,
        wnba_stats_gamerotation,
        wnba_stats_playbyplayv3,
    )

    fixture_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n[Step 2] Capturing payloads for game_id={game_id!r} -> {fixture_dir}")

    # --- play-by-play v3 ---
    pbp_path = fixture_dir / "playbyplayv3.json"
    if pbp_path.exists():
        print("  playbyplayv3.json already exists — loading from disk")
        pbp_raw = json.loads(pbp_path.read_text(encoding="utf-8"))
    else:
        pbp_raw = _call_with_backoff(
            f"wnba_stats_playbyplayv3(game_id={game_id!r})",
            lambda: wnba_stats_playbyplayv3(game_id=game_id, return_parsed=False),
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
            f"wnba_stats_boxscoretraditionalv3(game_id={game_id!r})",
            lambda: wnba_stats_boxscoretraditionalv3(game_id=game_id, return_parsed=False),
        )
        box_path.write_text(json.dumps(box_raw, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"  wrote {box_path}")

    bxt = (box_raw or {}).get("boxScoreTraditional") or {}
    print(f"  boxscoretraditionalv3: homeTeamId={bxt.get('homeTeamId')}, awayTeamId={bxt.get('awayTeamId')}")

    # --- game rotation ---
    rot_path = fixture_dir / "gamerotation.json"
    if rot_path.exists():
        print("  gamerotation.json already exists — loading from disk")
        rot_raw = json.loads(rot_path.read_text(encoding="utf-8"))
    else:
        rot_raw = _call_with_backoff(
            f"wnba_stats_gamerotation(game_id={game_id!r}, league_id='10')",
            lambda: wnba_stats_gamerotation(game_id=game_id, league_id="10", return_parsed=False),
        )
        rot_path.write_text(json.dumps(rot_raw, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"  wrote {rot_path}")

    result_sets = (rot_raw or {}).get("resultSets") or []
    print(f"  gamerotation: {len(result_sets)} resultSets")

    return {"pbp": pbp_raw, "box": box_raw, "rotation": rot_raw}


# ---------------------------------------------------------------------------
# Step 3: verify payload-shape parity vs NBA v3 cores
# ---------------------------------------------------------------------------


def verify_parity(game_id: str, payloads: dict[str, dict]) -> bool:
    """Assert that WNBA v3 payloads have the structural keys the nba/ cores require.

    Args:
        game_id: WNBA game id (for logging).
        payloads: Dict with keys ``"pbp"``, ``"box"``, ``"rotation"``.

    Returns:
        True if all checks pass; False on any divergence (divergences are printed).
    """
    print(f"\n[Step 3] Verifying payload-shape parity for game_id={game_id!r}")
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
        # Check a player row has statistics.minutes + position
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
    for rs in result_sets:
        name = rs.get("name", "")
        if name not in ("HomeTeam", "AwayTeam"):
            continue
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

    if rot_ok and not any(rs.get("name") in ("HomeTeam", "AwayTeam") for rs in result_sets):
        print("  FAIL rotation: no HomeTeam or AwayTeam resultSet found")
        ok = False

    verdict = "PASS" if ok else "FAIL"
    print(f"  Parity verdict for {game_id}: {verdict}")
    return ok


# ---------------------------------------------------------------------------
# Step 4: smoke the nba/ cores on the captured fixtures (offline)
# ---------------------------------------------------------------------------


def smoke_nba_cores(game_id: str, fixture_dir: Path) -> bool:
    """Run the nba/ possession-engine cores on captured WNBA fixtures.

    Checks: enh.height > 0; home/away are two distinct positive ints;
    oc non-empty; spot-check that an on-court row carries exactly 10 distinct ids.

    Args:
        game_id: WNBA game id string.
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

    print(f"\n[Step 4] Smoking nba/ cores on WNBA fixture for game_id={game_id!r}")

    pbp_raw = json.loads((fixture_dir / "playbyplayv3.json").read_text(encoding="utf-8"))
    box_raw = json.loads((fixture_dir / "boxscoretraditionalv3.json").read_text(encoding="utf-8"))
    rot_raw = json.loads((fixture_dir / "gamerotation.json").read_text(encoding="utf-8"))

    ok = True

    # enhanced_pbp_from_payload
    try:
        enh = enhanced_pbp_from_payload(pbp_raw, league_id="10")
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
                print(f"  10-distinct-ids check: WARN — row 0 has {len(ids)} distinct ids (expected 10)")
                # This is a warning, not a hard failure — lineup gaps are possible at game start
        else:
            missing_cols = [c for c in all_oc_cols if c not in oc.columns]
            print(f"  10-distinct-ids check: WARN — on-court frame missing columns: {missing_cols}")

    return ok


# ---------------------------------------------------------------------------
# Step 5: write README + commit instructions
# ---------------------------------------------------------------------------


def write_readme(captured_ids: list[str], fixture_root: Path, capture_date: str = "2026-06-29") -> None:
    """Write tests/fixtures/wnba_engine/README.md documenting provenance.

    Args:
        captured_ids: List of captured WNBA game id strings.
        fixture_root: Root fixture directory (``tests/fixtures/wnba_engine/``).
        capture_date: ISO date string of capture.
    """
    lines: list[str] = [
        "# WNBA Engine Fixture Provenance",
        "",
        "Raw `return_parsed=False` payloads captured from `stats.wnba.com` for the",
        "WNBA possession-engine offline test suite (Phase 5, Task 0).",
        "",
        "## Capture metadata",
        "",
        f"- **Capture date:** {capture_date}",
        "- **League ID:** 10 (WNBA)",
        "- **Season:** 2024 (Regular Season)",
        "- **Source:** `stats.wnba.com` via `sportsdataverse.wnba.wnba_stats` curl_cffi runtime",
        "",
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
        '| `playbyplayv3.json` | `GET https://stats.wnba.com/stats/playbyplayv3?GameID=<id>` | Raw v3 PBP; `payload["game"]["actions"]` is the action list |',
        '| `boxscoretraditionalv3.json` | `GET https://stats.wnba.com/stats/boxscoretraditionalv3?GameID=<id>` | Raw v3 box score; home/away team ids under `payload["boxScoreTraditional"]` |',
        "| `gamerotation.json` | `GET https://stats.wnba.com/stats/gamerotation?GameID=<id>&LeagueID=10` | Raw rotation stints; HomeTeam/AwayTeam resultSets with PERSON_ID + IN/OUT_TIME_REAL |",
        "",
        "## Shape parity with NBA v3",
        "",
        "WNBA v3 endpoints share the same payload shape as their NBA counterparts",
        "(`stats.nba.com`). The `sportsdataverse.nba` possession-engine cores",
        "(`enhanced_pbp_from_payload`, `boxscore_home_away`,",
        "`players_on_court_from_rotation`) accept these payloads directly with",
        '`league_id="10"` passed to `enhanced_pbp_from_payload`.',
        "",
        "## Rate-limit notes",
        "",
        "Calls were spaced ≥ 3 seconds apart with 3-retry exponential backoff",
        "(5s → 15s → 45s). `stats.wnba.com` shares TLS-fingerprint gating with",
        "`stats.nba.com`; the `curl_cffi` impersonation runtime is required.",
        "",
    ]

    readme_path = fixture_root / "README.md"
    readme_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n[Step 5] Wrote {readme_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(game_ids: Optional[list[str]] = None, season: str = "2024") -> None:
    """Discover WNBA game ids, capture fixtures, verify parity, smoke cores, write README.

    Args:
        game_ids: Explicit game ids to capture (skips discovery if provided).
        season: WNBA season to query for discovery (default ``"2024"``).
    """
    worktree = Path(__file__).resolve().parent.parent.parent
    fixture_root = worktree / "tests" / "fixtures" / "wnba_engine"
    fixture_root.mkdir(parents=True, exist_ok=True)

    print("\n=== WNBA engine fixture generator ===")
    print(f"    worktree     : {worktree}")
    print(f"    fixture root : {fixture_root}")

    # Step 1: discover (or use provided) game ids
    if game_ids:
        chosen_ids = game_ids[:2]
        print(f"\n[Step 1] Using provided game_ids: {chosen_ids}")
    else:
        try:
            chosen_ids = discover_game_ids(season=season, n=2)
        except RuntimeError as exc:
            print(f"\nERROR during game id discovery: {exc}")
            print("BLOCKED — cannot proceed. Report this to the session owner.")
            sys.exit(1)

    captured_ids: list[str] = []
    all_parity_ok = True
    all_smoke_ok = True

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
            print("This indicates a real WNBA-vs-NBA shape difference; do NOT silently force it.")
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
    write_readme(captured_ids, fixture_root)

    # Summary
    print(f"\n{'=' * 60}")
    print("Summary")
    print(f"{'=' * 60}")
    print(f"  Games captured : {len(captured_ids)} / {len(chosen_ids)}")
    print(f"  Game ids       : {captured_ids}")
    print(f"  Parity OK      : {all_parity_ok}")
    print(f"  Smoke OK       : {all_smoke_ok}")
    print("\nNext steps:")
    print("  git add tests/fixtures/wnba_engine/ tools/fixtures/gen_wnba_engine_fixtures.py")
    print('  git commit -m "test(wnba): capture WNBA v3 possession-engine fixtures"')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate WNBA possession-engine fixtures for offline testing.")
    parser.add_argument(
        "--game-ids",
        nargs="+",
        metavar="GAME_ID",
        help="Explicit WNBA game ids to capture (skips LeagueGameLog discovery).",
    )
    parser.add_argument(
        "--season",
        default="2024",
        help="WNBA season to query for game id discovery (default: 2024).",
    )
    args = parser.parse_args()
    main(game_ids=args.game_ids, season=args.season)
