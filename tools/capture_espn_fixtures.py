# tools/capture_espn_fixtures.py
"""One-off: capture per-league ESPN payloads for the endpoints whose return
schema is sport-specific (scoreboard/teams/standings/leaders) so the schema
introspector (generate.py --schemas) can derive per-sport columns offline.

Run once WITH network:  python -m tools.capture_espn_fixtures
Commit the resulting tests/fixtures/espn/{endpoint}_{league}.json files.

Design notes
------------
* ``scoreboard`` / ``standings`` / ``leaders`` wrappers accept
  ``return_parsed=False`` and return a raw ``dict``; the fixture is that dict.
* ``teams`` wrappers are always-parsed (no ``return_parsed`` parameter).
  Instead we fetch the raw ESPN site v2 teams URL directly so the fixture
  contains the raw dict that ``parse_teams(payload)`` expects.
  URL pattern: https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/teams
* ``espn_wnba_standings`` / ``espn_wbb_standings`` use ``raw=True`` instead of
  ``return_parsed=False`` (different wrapper generation era); handled below.
* In-season dates / seasons chosen to guarantee non-empty payloads:
  - NBA/NHL:  Oct-Jun season → 2024-12-01 / season 2024
  - WNBA:     May-Oct season → 2024-06-22 (mid-regular-season Sat)
  - NFL:      Sep-Jan season → 2024-10-13 week 6 regular season
  - CFB:      Sep-Dec season → 2024-10-12 week 7 regular season
  - MBB/WBB:  Nov-Mar season → 2024-12-01 / season 2024
  - MLB:      Apr-Oct season → 2024-10-25 (World Series)
"""

from __future__ import annotations

import importlib
import json
import urllib.request
from pathlib import Path
from typing import Any

import polars as pl
import pandas as pd

# ESPN site v2 teams URL: sport/league slugs for direct raw fetch.
# espn_{league}_teams wrappers are always-parsed (no return_parsed param),
# so we bypass them and hit the URL directly.
_TEAMS_SLUGS: dict[str, str] = {
    "nba": "basketball/nba",
    "wnba": "basketball/wnba",
    "mbb": "basketball/mens-college-basketball",
    "wbb": "basketball/womens-college-basketball",
    "cfb": "football/college-football",
    "nfl": "football/nfl",
    "mlb": "baseball/mlb",
    "nhl": "hockey/nhl",
}
_TEAMS_BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/{slug}/teams?limit=1000"
_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

OUT = Path("tests/fixtures/espn")
LEAGUES = ["nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb", "nhl"]

# Per-league per-endpoint kwargs overrides.  The default kwargs in ENDPOINTS
# are used when a league has no entry here.
_SCOREBOARD_KWARGS: dict[str, dict] = {
    "nba": {"dates": 20241201},
    "wnba": {"dates": 20240622},  # mid-WNBA regular season Saturday
    "mbb": {"dates": 20241201},
    "wbb": {"dates": 20241201},
    "cfb": {"dates": 20241012, "week": 7, "season_type": 2},
    "nfl": {"week": 6, "season_type": 2},  # no dates filter → full week slate
    "mlb": {"dates": 20241025},  # 2024 World Series
    "nhl": {"dates": 20241201},
}
_STANDINGS_KWARGS: dict[str, dict] = {
    # Most leagues: return_parsed=False, season=2024
    # WNBA/WBB use raw=True and require positional season arg (passed as kwarg)
    "nba": {"return_parsed": False, "season": 2024},
    "wnba": {"raw": True, "season": 2024},
    "mbb": {"return_parsed": False, "season": 2024},
    "wbb": {"raw": True, "season": 2024},
    "cfb": {"return_parsed": False, "season": 2024},
    "nfl": {"return_parsed": False, "season": 2024},
    "mlb": {"return_parsed": False, "season": 2024},
    "nhl": {"return_parsed": False, "season": 2024},
}
_LEADERS_KWARGS: dict[str, dict] = {
    "nba": {"return_parsed": False, "season": 2024},
    "wnba": {"return_parsed": False, "season": 2024},
    "mbb": {"return_parsed": False, "season": 2024},
    "wbb": {"return_parsed": False, "season": 2024},
    "cfb": {"return_parsed": False, "season": 2024, "season_type": 2},
    "nfl": {"return_parsed": False, "season": 2024, "season_type": 2},
    "mlb": {"return_parsed": False, "season": 2024},
    "nhl": {"return_parsed": False, "season": 2024},
}

# (endpoint stem, wrapper short name suffix, per-league-kwargs-map | None)
ENDPOINTS = [
    ("scoreboard", "scoreboard", _SCOREBOARD_KWARGS),
    ("teams", "teams", None),  # no return_parsed; handled specially
    ("standings", "standings", _STANDINGS_KWARGS),
    ("leaders", "leaders", _LEADERS_KWARGS),
]

MIN_BYTES = 500  # fixtures smaller than this are considered empty/error stubs


def _serialize(raw: Any) -> str:
    """Convert raw API payload to a JSON string we can write to disk."""
    if isinstance(raw, pl.DataFrame):
        return json.dumps(raw.to_pandas().to_dict(orient="records"), default=str)
    if isinstance(raw, pd.DataFrame):
        return json.dumps(raw.to_dict(orient="records"), default=str)
    return json.dumps(raw, default=str)


def _fetch_teams_raw(league: str) -> dict:
    """Fetch raw ESPN site v2 teams payload for a league via direct HTTP GET."""
    slug = _TEAMS_SLUGS[league]
    url = _TEAMS_BASE_URL.format(slug=slug)
    req = urllib.request.Request(url, headers=_HTTP_HEADERS)
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read()
    return json.loads(body)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    written = 0
    failed: list[str] = []

    for league in LEAGUES:
        mod = importlib.import_module(f"sportsdataverse.{league}")
        for stem, short, kwargs_map in ENDPOINTS:
            fn_name = f"espn_{league}_{short}"

            try:
                # --- teams: bypass the always-parsed wrapper; fetch raw directly ---
                if stem == "teams":
                    raw = _fetch_teams_raw(league)
                    if not isinstance(raw, dict) or "sports" not in raw:
                        raise ValueError(
                            f"unexpected teams payload structure: "
                            f"type={type(raw).__name__}, "
                            f"top-level keys={list(raw.keys())[:5] if isinstance(raw, dict) else 'N/A'}"
                        )
                    payload_str = json.dumps(raw, default=str)
                    if len(payload_str) < MIN_BYTES:
                        raise ValueError(f"payload too small ({len(payload_str)} bytes) — likely empty or error stub")
                    path = OUT / f"{stem}_{league}.json"
                    path.write_text(payload_str, encoding="utf-8")
                    print(f"wrote {path} ({path.stat().st_size:,} bytes)")
                    written += 1
                    continue

                # --- all other endpoints: call the wrapper as before ---
                fn = getattr(mod, fn_name, None)
                if fn is None:
                    print(f"skip  {league}/{stem}: no wrapper '{fn_name}'")
                    failed.append(f"{league}/{stem}: no wrapper")
                    continue

                # Determine kwargs for this (league, endpoint) combination
                call_kwargs = kwargs_map.get(league, {}) if kwargs_map is not None else {}

                # Clear lru_cache between leagues so we don't cross-contaminate
                clear_fn = getattr(fn, "cache_clear", None)
                if clear_fn is not None:
                    clear_fn()

                raw = fn(**call_kwargs)

                if raw is None:
                    raise ValueError("wrapper returned None")

                payload_str = _serialize(raw)

                if len(payload_str) < MIN_BYTES:
                    raise ValueError(f"payload too small ({len(payload_str)} bytes) — likely empty or error stub")

                path = OUT / f"{stem}_{league}.json"
                path.write_text(payload_str, encoding="utf-8")
                print(f"wrote {path} ({path.stat().st_size:,} bytes)")
                written += 1

            except Exception as e:  # noqa: BLE001
                msg = f"{league}/{stem}: {e}"
                print(f"FAIL  {msg}")
                failed.append(msg)

    print(f"\n--- summary: {written}/32 written, {len(failed)} failures ---")
    for f in failed:
        print(f"  FAIL: {f}")


if __name__ == "__main__":
    main()
