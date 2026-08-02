"""Season-level (non per-game) captures for the raw store.

``scrape_raw_json.py`` fills the per-game half of the store through sdv-py's
read-through cache, which keys on ``game_id``. Season-level endpoints are keyed on
*(season, parameters)* instead and cannot use that store, so they land here under

    {endpoint}/{season}/{variant}.json     (parameterized)
    {endpoint}/{season}.json               (no variants)

Which endpoints, and which parameter matrix each one gets, comes from
:mod:`endpoints` -- derived from the endpoints' own signatures rather than a
hand-maintained list, so a new upstream endpoint is captured without an edit here.

Writes are atomic (tmp + rename) and idempotent: an existing payload is skipped
without parsing, so a sweep is resumable after Ctrl-C.

Rate discipline: every fetch shares the ProxyBonanza rotation and the single
stats.{nba,wnba}.com budget with the per-game sweep. These are fetched
**sequentially** -- a few hundred calls per season against thousands of games --
so there is nothing to gain from parallelising them.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from .endpoints import discover, season_variants, slug

__all__ = [
    "capture_season",
    "game_ids_from_gamelog",
    "payload_path",
    "plan_season",
    "slug",
    "write_payload",
]


def payload_path(root: str | Path, endpoint: str, season: int, variant: str | None = None) -> Path:
    """Where a season-level capture lives. ``variant=None`` means unparameterized."""
    base = Path(root) / endpoint
    return base / str(season) / f"{variant}.json" if variant else base / f"{season}.json"


def is_contentless(payload: Any) -> bool:
    """True when ``payload`` carries nothing and must not be persisted.

    The stats hosts never answer with an empty body: an endpoint with no rows
    still returns the full envelope with ``rowSet: []``. A bare ``{}`` therefore
    means the getter could not parse a response -- a failed fetch, not "no data".

    That distinction is load-bearing because resume is ``path.exists()``, i.e.
    presence rather than content. One empty write is permanent: every later
    sweep counts the file present, never refetches, and reports the season
    complete, so a backfill no-ops. 3,347 files in hoopR-nba-stats-raw and
    3,872 in wehoop-wnba-stats-raw reached that state before this guard.

    Deliberately narrow -- ``{"resultSets": []}`` and a v3 payload whose entity
    list is empty are REAL answers and are persisted.
    """
    return payload is None or not isinstance(payload, (dict, list)) or len(payload) == 0


def write_payload(path: Path, payload: Any) -> bool:
    """Persist ``payload`` atomically. Returns False (writing nothing) if it is
    contentless.

    Atomic so a killed sweep never leaves half a file; guarded so a failed fetch
    never becomes a permanent gap.
    """
    if is_contentless(payload):
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.partial")
    try:
        tmp.write_text(json.dumps(payload), encoding="utf-8")
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    return True


def _result_tables(payload: Any) -> list[dict]:
    """Every ``{name, headers, rowSet}`` table in a stats-host payload.

    The envelope is not uniform, and iterating ``payload["resultSets"]`` blindly
    is wrong for two of its shapes:

    * ``resultSets`` as a LIST of tables -- the common case.
    * ``resultSets`` as a single DICT (the shot-locations family). Iterating
      that yields its KEYS, so the caller ends up calling ``.get()`` on a
      string and raises AttributeError.
    * ``resultSet`` SINGULAR, holding one table as a dict (leagueleaders,
      *estimatedmetrics) -- invisible to anything looking only at the plural.

    Returns an empty list for the v3 ``{<entity>, meta}`` payloads, which carry
    no result tables at all.
    """
    if not isinstance(payload, dict):
        return []
    tables: list[dict] = []
    for key in ("resultSets", "resultSet"):
        value = payload.get(key)
        if isinstance(value, dict):
            tables.append(value)
        elif isinstance(value, list):
            tables.extend(t for t in value if isinstance(t, dict))
    return tables


def _ids_from(payload: Any, column: str) -> list[str]:
    """Distinct values of ``column`` across every result table in ``payload``."""
    out: set[str] = set()
    for rs in _result_tables(payload):
        # The grouped family's headers are column-GROUP dicts, not name
        # strings; str() on a dict cannot match a column name, so those tables
        # are skipped rather than mis-indexed.
        headers = [str(h).upper() for h in rs.get("headers") or []]
        if column not in headers:
            continue
        idx = headers.index(column)
        for row in rs.get("rowSet") or []:
            if isinstance(row, list) and idx < len(row) and row[idx] is not None:
                out.add(str(row[idx]))
    return sorted(out)


def game_ids_from_gamelog(payload: Any) -> list[str]:
    """Zero-padded game ids from a raw ``leaguegamelog`` payload.

    Lets the per-game sweep enumerate from the persisted capture instead of making
    its own call for the same thing.
    """
    return [g.zfill(10) for g in _ids_from(payload, "GAME_ID")]


def plan_season(
    season: int, module: Any, prefix: str, league_id: str
) -> Iterator[tuple[str, str | None, dict[str, Any]]]:
    """Yield ``(endpoint, variant, kwargs)`` for every season-level capture.

    ``commonteamroster`` is absent: it is team-keyed, so :func:`capture_season`
    schedules it separately once team ids are known.
    """
    _game, season_endpoints = discover(module, prefix)
    for endpoint in season_endpoints:
        fn = getattr(module, f"{prefix}_{endpoint}")
        for variant, kwargs in season_variants(fn, season, league_id):
            yield endpoint, variant, kwargs


def capture_season(
    season: int,
    root: str | Path,
    fetch: Callable[[str, dict[str, Any]], Any],
    module: Any,
    prefix: str,
    league_id: str,
    log: Callable[[str], None] = lambda _m: None,
    skip_endpoints: frozenset[str] | set[str] = frozenset(),
) -> tuple[int, int, int]:
    """Fetch every season-level payload for ``season``. Returns (written, skipped, failed).

    ``fetch(endpoint, kwargs)`` performs one call and returns the raw payload; the
    caller supplies it so proxy rotation and transport stay in the scraper and this
    module stays offline-testable.

    ``skip_endpoints`` names season-level endpoints to omit entirely -- parked
    endpoints, and anything below its season floor.
    """
    written = skipped = failed = 0
    team_source: Any = None

    def _is_team_source(endpoint: str, kwargs: dict[str, Any]) -> bool:
        """The one team-stats capture whose rows enumerate the league's teams.

        Matched on kwargs, not on the variant slug: the slug is composed from the
        axis order in ``endpoints._SWEEPS``, so reordering those axes would silently
        stop this from ever matching and no team rosters would be captured.
        """
        if endpoint != "leaguedashteamstats":
            return False
        return all(
            any(k.startswith(p) and v == want for k, v in kwargs.items())
            for p, want in (
                ("season_type", "Regular Season"),
                ("measure_type", "Base"),
                ("per_mode", "Totals"),
            )
        )

    for endpoint, variant, kwargs in plan_season(season, module, prefix, league_id):
        if endpoint in skip_endpoints:  # parked, or below its season floor
            continue
        path = payload_path(root, endpoint, season, variant)
        is_team_source = _is_team_source(endpoint, kwargs)
        if path.exists():
            skipped += 1
            if is_team_source:
                try:
                    team_source = json.loads(path.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    team_source = None
            continue
        try:
            payload = fetch(endpoint, kwargs)
        except Exception as exc:  # noqa: BLE001 - one endpoint gap must not kill the season
            log(f"season {season} {endpoint}[{variant}]: {exc}")
            failed += 1
            continue
        if not write_payload(path, payload):
            # Counted as a failure, not a write: leaving no file is what lets the
            # next sweep retry it.
            log(f"season {season} {endpoint}[{variant}]: empty payload, not persisted")
            failed += 1
            continue
        written += 1
        if is_team_source:
            team_source = payload

    # commonteamroster is per (season, team); team ids come from the team-stats
    # capture above rather than a second index call.
    if hasattr(module, f"{prefix}_commonteamroster"):
        for team_id in _ids_from(team_source, "TEAM_ID"):
            path = payload_path(root, "commonteamroster", season, team_id)
            if path.exists():
                skipped += 1
                continue
            try:
                payload = fetch(
                    "commonteamroster",
                    {"season": str(season), "team_id": team_id, "league_id": league_id},
                )
            except Exception as exc:  # noqa: BLE001
                log(f"season {season} commonteamroster[{team_id}]: {exc}")
                failed += 1
                continue
            if not write_payload(path, payload):
                log(f"season {season} commonteamroster[{team_id}]: empty payload, not persisted")
                failed += 1
                continue
            written += 1

    return written, skipped, failed
