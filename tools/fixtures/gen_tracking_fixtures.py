"""
Generator: capture leaguedashptstats Drives / Player / Totals fixtures.

Usage (from repo root):
    uv run python tools/fixtures/gen_tracking_fixtures.py

Writes:
    tests/fixtures/nba_engine/tracking/leaguedashptstats_drives_player_2223.json
    tests/fixtures/nba_engine/tracking/leaguedashptstats_drives_player_2324.json

Dev-only tool — not shipped in the package wheel.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

FIXTURE_DIR = Path(__file__).parent.parent.parent / "tests" / "fixtures" / "nba_engine" / "tracking"

SEASONS = [
    ("2022-23", "leaguedashptstats_drives_player_2223.json"),
    ("2023-24", "leaguedashptstats_drives_player_2324.json"),
]

# Retry config — stats.nba.com is rate-limit-flaky
MAX_ATTEMPTS = 5
BACKOFF_BASE = 3.0  # seconds


def _row_count(payload: dict) -> int:
    """Return total row count across all resultSets in the payload."""
    result_sets = payload.get("resultSets", payload.get("resultSet", []))
    if isinstance(result_sets, dict):
        result_sets = [result_sets]
    total = 0
    for rs in result_sets:
        rows = rs.get("rowSet", [])
        total += len(rows)
    return total


def fetch_season(season: str) -> dict:
    """Fetch leaguedashptstats with retries/backoff. Raises on exhaustion."""
    # Import here so failures are obvious at call time, not import time
    from sportsdataverse.nba import nba_stats  # noqa: PLC0415

    last_exc: Exception | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            log.info("  attempt %d/%d for season %s", attempt, MAX_ATTEMPTS, season)
            payload = nba_stats.nba_stats_leaguedashptstats(
                season=season,
                season_type_all_star="Regular Season",
                pt_measure_type="Drives",
                per_mode_simple="Totals",
                player_or_team="Player",
                league_id="00",
                return_parsed=False,
            )
            rows = _row_count(payload)
            if rows == 0:
                raise ValueError(f"Payload has 0 rows for season {season}")
            log.info("  OK: %d player rows", rows)
            return payload
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            wait = BACKOFF_BASE * (2 ** (attempt - 1))
            log.warning("  attempt %d failed: %s — retrying in %.1fs", attempt, exc, wait)
            if attempt < MAX_ATTEMPTS:
                time.sleep(wait)

    raise RuntimeError(f"All {MAX_ATTEMPTS} attempts failed for season {season}. Last error: {last_exc}") from last_exc


def main() -> None:
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)

    for season, filename in SEASONS:
        out_path = FIXTURE_DIR / filename
        log.info("Fetching season %s -> %s", season, out_path.name)
        payload = fetch_season(season)
        rows = _row_count(payload)
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        log.info("Written %s (%d rows)", out_path, rows)

    log.info("Done. All fixtures written.")


if __name__ == "__main__":
    main()
