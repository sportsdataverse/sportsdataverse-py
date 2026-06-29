"""On-court lineup reconstruction for the v3 pbp engine.

Provides utilities consumed by the Phase 1 lineup engine:

- :func:`boxscore_home_away` — extract home/away team ids from a
  ``boxScoreTraditional`` payload.
- :func:`boxscore_name_map` — build a ``{team_id: {familyName_lower: person_id}}``
  lookup used to resolve player names from narrative text.
- :func:`period_starters` — infer which five players started each period for
  each team using the boxscore (period 1) and play-by-play substitution logic
  (periods 2+).
- :func:`parse_rotation_resultsets` — convert raw ``nba_stats_gamerotation``
  resultSets JSON into a ``{"HomeTeam": [...], "AwayTeam": [...]}`` dict.
- :func:`players_on_court_from_rotation` — pure rotation-based on-court
  reconstruction (no network calls) from a pre-parsed rotation dict.
- :func:`players_on_court` — public entry point; delegates to
  :func:`players_on_court_from_rotation`.

Algorithm (hoopR port)
----------------------
The rotation-based reconstruction is a faithful port of hoopR's
``.players_on_court_v3()`` (R/nba_stats_pbp.R lines 857-1041).

For each team, ``nba_gamerotation`` provides stint rows with
``IN_TIME_REAL`` / ``OUT_TIME_REAL`` as tenths-of-second elapsed from
game start.  Each PBP row's elapsed time is computed from its period and
``seconds_remaining`` clock using the same formula:

  periods 1-4:  elapsed = ((period - 1) * 720 + (720 - seconds_remaining)) * 10
  OT (periods 5+): elapsed = (2880 + (period - 5) * 300 + (300 - seconds_remaining)) * 10

``_resolve_team_oncourt`` maps elapsed times to a sorted 5-player lineup
using a boundary-interval approach (equivalent to R's ``findInterval``):

1. Collect unique in/out time boundaries and compute midpoints.
2. For each interval, the active players are those whose stint spans the
   midpoint (``in_time <= mid`` and ``out_time > mid``).
3. A final "game-end" row covers ``t == max_boundary``.
4. ``np.searchsorted(boundaries, times, side='right') - 1`` maps each PBP
   elapsed time to its interval index (clamped to ``[0, n_intervals]``).
"""

from __future__ import annotations

import logging
import re

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_pbp_constants import LINEUPS_SCHEMA

logger = logging.getLogger(__name__)

# Regex to parse the incoming player's family name from a SUB description.
# Format: "SUB: <IN_familyName> FOR <OUT_familyName>"
# Lazy match so multi-word names like "House Jr." are captured fully.
_SUB_RE = re.compile(r"SUB:\s*(.+?)\s+FOR\s+", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _bt(box: dict) -> dict:
    """Return the ``boxScoreTraditional`` sub-dict, or empty dict if absent."""
    return (box or {}).get("boxScoreTraditional") or {}


def _box_starters(box: dict) -> dict[int, list[int]]:
    """Return period-1 starters keyed by team_id from boxscore ``position`` field.

    A player is a starter iff their ``position`` string is non-empty.

    Args:
        box: Raw boxscore payload dict.

    Returns:
        ``{team_id: [person_id, ...]}`` with up to 5 ids per team.  A malformed
        or empty payload returns ``{}`` rather than raising.
    """
    b = _bt(box)
    out: dict[int, list[int]] = {}
    for side in ("homeTeam", "awayTeam"):
        t = b.get(side) or {}
        if t.get("teamId") is None:
            continue
        tid = int(t.get("teamId"))
        out[tid] = [int(p["personId"]) for p in (t.get("players") or []) if str(p.get("position", "")).strip()]
    return out


def _resolve_team_oncourt(stints: list[dict], times: list[float]) -> list[list[int | None]]:
    """Map PBP elapsed times to sorted 5-player lineups using rotation stints.

    Port of hoopR's ``.resolve_team_oncourt()`` (nba_stats_pbp.R).

    Args:
        stints: List of stint dicts with keys ``PERSON_ID``, ``IN_TIME_REAL``,
            ``OUT_TIME_REAL`` (all numeric, in tenths-of-second elapsed from
            game start).
        times: Per-row elapsed times for the PBP frame, in the same units.

    Returns:
        List of length ``len(times)``, each element a list of up to 5 ``int``
        player IDs (sorted ascending) padded with ``None`` to length 5.
    """
    if not stints or not times:
        lineup: list[int | None] = [None] * 5
        return [lineup] * len(times)

    in_times = np.array([float(s["IN_TIME_REAL"]) for s in stints])
    out_times = np.array([float(s["OUT_TIME_REAL"]) for s in stints])
    person_ids = np.array([int(s["PERSON_ID"]) for s in stints])
    times_arr = np.array(times)

    boundaries = np.unique(np.concatenate([in_times, out_times]))
    n_bounds = len(boundaries)

    if n_bounds < 2:
        # Edge case: all stints have the same in/out time.
        active = list(dict.fromkeys(person_ids.tolist()))[:5]
        lineup_edge: list[int | None] = (active + [None] * 5)[:5]
        return [lineup_edge] * len(times)

    n_intervals = n_bounds - 1
    midpoints = (boundaries[:-1] + boundaries[1:]) / 2.0

    # Build lineup for each interval k via its midpoint.
    all_lineups: list[list[int | None]] = []
    for k in range(n_intervals):
        mid = float(midpoints[k])
        mask = (in_times <= mid) & (out_times > mid)
        active_raw = person_ids[mask].tolist()
        active_dedup = list(dict.fromkeys(active_raw))
        active_sorted: list[int] = sorted(active_dedup)[:5]
        row_lineup: list[int | None] = active_sorted + [None] * (5 - len(active_sorted))
        all_lineups.append(row_lineup)

    # Game-end lineup: players with in_time <= max_t AND out_time >= max_t.
    max_t = float(boundaries[n_bounds - 1])
    mask_end = (in_times <= max_t) & (out_times >= max_t)
    active_end_raw = person_ids[mask_end].tolist()
    active_end_dedup = list(dict.fromkeys(active_end_raw))
    active_end_sorted: list[int] = sorted(active_end_dedup)[:5]
    final_lineup: list[int | None] = active_end_sorted + [None] * (5 - len(active_end_sorted))
    all_lineups.append(final_lineup)

    # findInterval equivalent: searchsorted(boundaries, t, side='right') - 1
    # then clamp to [0, n_intervals] (all_lineups has n_intervals+1 rows).
    idx = np.searchsorted(boundaries, times_arr, side="right") - 1
    idx = np.clip(idx, 0, n_intervals).tolist()

    return [all_lineups[i] for i in idx]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def boxscore_home_away(box: dict) -> tuple[int, int]:
    """Return ``(home_team_id, away_team_id)`` from a boxscore payload.

    Args:
        box: Raw boxscore payload dict (the ``boxScoreTraditional`` wrapper
            from ``nba_stats_boxscoretraditionalv3``).

    Returns:
        A ``(home_team_id, away_team_id)`` tuple of ``int``.  A malformed or
        empty payload returns ``(0, 0)`` rather than raising.

    Example:
        Quick start::

            import json, pathlib
            from sportsdataverse.nba.nba_lineups import boxscore_home_away
            box = json.loads(pathlib.Path("boxscoretraditionalv3.json").read_text())
            home, away = boxscore_home_away(box)
            print(home, away)
    """
    b = _bt(box)
    return int(b.get("homeTeamId") or 0), int(b.get("awayTeamId") or 0)


def boxscore_name_map(box: dict) -> dict[int, dict[str, int]]:
    """Build a family-name lookup for in-player resolution.

    Args:
        box: Raw boxscore payload dict.

    Returns:
        ``{team_id: {familyName_lower: person_id}}`` mapping.  Both home and
        away teams are included.  Values are ``int`` (never ``float``).  A
        malformed or empty payload returns ``{}`` rather than raising.

    Example:
        Quick start::

            import json, pathlib
            from sportsdataverse.nba.nba_lineups import boxscore_name_map
            box = json.loads(pathlib.Path("boxscoretraditionalv3.json").read_text())
            nm = boxscore_name_map(box)
            # resolve "brown" for the home team
            home_id = list(nm.keys())[0]
            print(nm[home_id].get("brown"))
    """
    b = _bt(box)
    out: dict[int, dict[str, int]] = {}
    for side in ("homeTeam", "awayTeam"):
        t = b.get(side) or {}
        if t.get("teamId") is None:
            continue
        tid = int(t.get("teamId"))
        out[tid] = {str(p.get("familyName", "")).lower(): int(p["personId"]) for p in (t.get("players") or [])}
    return out


def period_starters(enhanced_pbp: pl.DataFrame, box: dict) -> dict[int, dict[int, list[int]]]:
    """Infer the five-man lineup that started each period for each team.

    Period 1 starters are read directly from the boxscore ``position`` field —
    players with a non-empty ``position`` value were in the starting lineup.

    For periods 2+, starters are inferred from the play-by-play using a
    **sub-aware first-appearance** heuristic.  The v3 feed does not publish
    explicit "period-start" lineup events, so the algorithm walks rows in
    ``order_index`` order and applies two rules:

    1. When a substitution event is encountered, parse the incoming player from
       the description (``"SUB: <in_name> FOR <out_name>"``).  If that player
       has **not yet appeared** in this period they are a confirmed bench
       sub-in — they are excluded from the starter candidate pool.  If they
       have already appeared (returning starter subbed out and back in), they
       are NOT excluded.
    2. The first five ``person_id`` values per team that are not confirmed
       bench sub-ins are the period starters.

    Args:
        enhanced_pbp: Output of :func:`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`.
            Must contain columns ``period``, ``order_index``, ``is_substitution``,
            ``team_id``, ``person_id``, and ``description``.
        box: Raw boxscore payload dict (same as passed to :func:`boxscore_home_away`).

    Returns:
        ``{period: {team_id: [person_id_1, ..., person_id_5]}}`` covering all
        periods present in *enhanced_pbp*.  Each inner list holds up to 5 ids.

    Example:
        Quick start::

            import json, pathlib
            import polars as pl
            from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
            from sportsdataverse.nba.nba_lineups import period_starters
            box = json.loads(pathlib.Path("boxscoretraditionalv3.json").read_text())
            pbp = json.loads(pathlib.Path("playbyplayv3.json").read_text())
            s = period_starters(enhanced_pbp_from_payload(pbp), box)
            print(s[1])  # period-1 starters keyed by team_id

        See Also:
            * `hoopR`_ -- R package providing equivalent lineup utilities
            * `nba_api`_ -- reference Python client for stats.nba.com

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
    """
    name_map = boxscore_name_map(box)
    starters: dict[int, dict[int, list[int]]] = {}
    box_st = _box_starters(box)
    periods: list[int] = enhanced_pbp["period"].unique().sort().to_list()

    for period in periods:
        if period == 1:
            starters[1] = {tid: list(ids[:5]) for tid, ids in box_st.items()}
            continue

        pe = enhanced_pbp.filter(pl.col("period") == period).sort("order_index")

        confirmed_bench: dict[int, set[int]] = {}
        seen: dict[int, list[int]] = {}

        for r in pe.iter_rows(named=True):
            tid = r["team_id"]
            pid = r["person_id"]

            if r["is_substitution"] and tid is not None and tid > 0:
                desc: str = r["description"] or ""
                tid_int_sub = int(tid)
                m = _SUB_RE.search(desc)
                if m:
                    in_name = m.group(1).strip().lower()
                    in_pid = name_map.get(tid_int_sub, {}).get(in_name)
                    if in_pid is not None:
                        if in_pid not in (seen.get(tid_int_sub) or []):
                            confirmed_bench.setdefault(tid_int_sub, set()).add(in_pid)

            if tid is None or pid is None or tid <= 0 or pid <= 0:
                continue
            tid_int = int(tid)
            pid_int = int(pid)
            if pid_int in confirmed_bench.get(tid_int, set()):
                continue
            team_seen = seen.setdefault(tid_int, [])
            if pid_int not in team_seen:
                team_seen.append(pid_int)

        starters[period] = {tid: ids[:5] for tid, ids in seen.items()}

    return starters


def parse_rotation_resultsets(raw_rotation: dict) -> dict[str, list[dict]]:
    """Convert raw ``nba_stats_gamerotation`` payload into a tidy team-keyed dict.

    Args:
        raw_rotation: Raw dict returned by
            ``nba_stats_gamerotation(return_parsed=False)``.  Expected shape::

                {
                  "resultSets": [
                    {"name": "HomeTeam", "headers": [...], "rowSet": [...]},
                    {"name": "AwayTeam", "headers": [...], "rowSet": [...]},
                  ]
                }

    Returns:
        ``{"HomeTeam": [{col: val, ...}, ...], "AwayTeam": [...]}`` where each
        dict has at least the keys ``PERSON_ID``, ``TEAM_ID``,
        ``IN_TIME_REAL``, ``OUT_TIME_REAL``.  Returns ``{}`` on malformed input
        rather than raising.

    Example:
        Quick start::

            import json, pathlib
            from sportsdataverse.nba.nba_lineups import parse_rotation_resultsets
            raw = json.loads(pathlib.Path("gamerotation.json").read_text())
            rotation = parse_rotation_resultsets(raw)
            print(len(rotation["HomeTeam"]), "home stints")
    """
    out: dict[str, list[dict]] = {}
    result_sets = (raw_rotation or {}).get("resultSets") or []
    for rs in result_sets:
        name = rs.get("name", "")
        headers: list[str] = rs.get("headers") or []
        row_set: list[list] = rs.get("rowSet") or []
        if not headers or not row_set:
            continue
        records = [dict(zip(headers, row)) for row in row_set]
        out[name] = records
    return out


def players_on_court_from_rotation(
    enhanced_pbp: pl.DataFrame,
    rotation: dict[str, list[dict]],
    *,
    home_team_id: int,
    away_team_id: int,
) -> pl.DataFrame:
    """Reconstruct the 5-on-5 on-court lineup via the rotation (gamerotation) algorithm.

    Pure function — no network calls.  Port of hoopR's ``.players_on_court_v3()``
    (R/nba_stats_pbp.R lines 857-1041).

    The rotation dict may use either ``"HomeTeam"``/``"AwayTeam"`` or
    ``"homeTeam"``/``"awayTeam"`` as keys — both are accepted.

    Args:
        enhanced_pbp: Output of
            :func:`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`.
            Must contain ``game_id``, ``action_number``, ``period``, and
            ``seconds_remaining`` (clock remaining in the current period, seconds).
        rotation: Parsed rotation dict, typically from
            :func:`parse_rotation_resultsets`.  Each team's list contains stint
            dicts with numeric ``PERSON_ID``, ``IN_TIME_REAL``, ``OUT_TIME_REAL``.
        home_team_id: Integer team ID of the home team.
        away_team_id: Integer team ID of the away team.

    Returns:
        :class:`polars.DataFrame` conforming to ``LINEUPS_SCHEMA`` with one
        row per action in *enhanced_pbp* (same row count, same ordering).
        Never raises — empty/malformed rotation returns a zero-row frame.

    Example:
        Quick start::

            import json, pathlib
            import polars as pl
            from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
            from sportsdataverse.nba.nba_lineups import (
                boxscore_home_away, parse_rotation_resultsets,
                players_on_court_from_rotation,
            )
            box = json.loads(pathlib.Path("boxscoretraditionalv3.json").read_text())
            pbp = json.loads(pathlib.Path("playbyplayv3.json").read_text())
            rot = json.loads(pathlib.Path("gamerotation.json").read_text())
            enh = enhanced_pbp_from_payload(pbp)
            home, away = boxscore_home_away(box)
            rotation = parse_rotation_resultsets(rot)
            df = players_on_court_from_rotation(
                enh, rotation, home_team_id=home, away_team_id=away
            )
            print(df.shape)

        See Also:
            * `hoopR`_ -- R package providing equivalent lineup utilities
            * `nba_api`_ -- reference Python client for stats.nba.com

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
    """
    if enhanced_pbp.is_empty():
        return pl.DataFrame(schema=LINEUPS_SCHEMA)

    # Accept both capitalisation variants.
    home_stints = rotation.get("HomeTeam") or rotation.get("homeTeam") or []
    away_stints = rotation.get("AwayTeam") or rotation.get("awayTeam") or []

    if not home_stints and not away_stints:
        logger.warning("players_on_court_from_rotation: rotation dict has no stints — returning empty frame")
        return pl.DataFrame(schema=LINEUPS_SCHEMA)

    # Compute elapsed time (tenths-of-second from game start) for each PBP row.
    # periods 1-4: ((period - 1) * 720 + (720 - seconds_remaining)) * 10
    # OT (periods 5+): (2880 + (period - 5) * 300 + (300 - seconds_remaining)) * 10
    pbp_rows = enhanced_pbp.select(["game_id", "action_number", "period", "seconds_remaining"]).to_dicts()

    pbp_times: list[float] = []
    for r in pbp_rows:
        period = int(r["period"])
        sr = float(r["seconds_remaining"] or 0.0)
        if period <= 4:
            elapsed = ((period - 1) * 720.0 + (720.0 - sr)) * 10.0
        else:
            elapsed = (2880.0 + (period - 5) * 300.0 + (300.0 - sr)) * 10.0
        pbp_times.append(elapsed)

    # Resolve lineups for each team.
    home_lineups = _resolve_team_oncourt(home_stints, pbp_times)
    away_lineups = _resolve_team_oncourt(away_stints, pbp_times)

    out_rows: list[dict] = []
    for i, r in enumerate(pbp_rows):
        h = home_lineups[i]
        a = away_lineups[i]
        row: dict = {
            "game_id": r["game_id"],
            "action_number": int(r["action_number"]),
            "period": int(r["period"]),
        }
        for j in range(5):
            row[f"home_player_{j + 1}"] = h[j] if j < len(h) and h[j] is not None else None
        for j in range(5):
            row[f"away_player_{j + 1}"] = a[j] if j < len(a) and a[j] is not None else None
        out_rows.append(row)

    return pl.DataFrame(out_rows, schema=LINEUPS_SCHEMA)


def players_on_court(
    enhanced_pbp: pl.DataFrame,
    rotation: dict[str, list[dict]],
    *,
    home_team_id: int,
    away_team_id: int,
) -> pl.DataFrame:
    """Reconstruct the 5-on-5 on-court lineup for every play-by-play action.

    Delegates to :func:`players_on_court_from_rotation` using the
    ``nba_gamerotation`` rotation data (hoopR algorithm port).

    The rotation dict should be produced by :func:`parse_rotation_resultsets`.
    Both ``"HomeTeam"``/``"AwayTeam"`` and ``"homeTeam"``/``"awayTeam"`` keys
    are accepted.

    Args:
        enhanced_pbp: Output of
            :func:`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`.
            Must contain ``game_id``, ``action_number``, ``period``, and
            ``seconds_remaining``.
        rotation: Parsed rotation dict from :func:`parse_rotation_resultsets`.
        home_team_id: Integer team ID of the home team.
        away_team_id: Integer team ID of the away team.

    Returns:
        :class:`polars.DataFrame` conforming to ``LINEUPS_SCHEMA`` with one
        row per action in *enhanced_pbp*.  Never raises.

    Example:
        Quick start::

            import json, pathlib
            import polars as pl
            from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
            from sportsdataverse.nba.nba_lineups import (
                boxscore_home_away, parse_rotation_resultsets, players_on_court,
            )
            box = json.loads(pathlib.Path("boxscoretraditionalv3.json").read_text())
            pbp = json.loads(pathlib.Path("playbyplayv3.json").read_text())
            rot = json.loads(pathlib.Path("gamerotation.json").read_text())
            enh = enhanced_pbp_from_payload(pbp)
            home, away = boxscore_home_away(box)
            rotation = parse_rotation_resultsets(rot)
            df = players_on_court(enh, rotation, home_team_id=home, away_team_id=away)
            print(df.shape)

        See Also:
            * `hoopR`_ -- R package providing equivalent lineup utilities
            * `nba_api`_ -- reference Python client for stats.nba.com

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
    """
    return players_on_court_from_rotation(
        enhanced_pbp,
        rotation,
        home_team_id=home_team_id,
        away_team_id=away_team_id,
    )
