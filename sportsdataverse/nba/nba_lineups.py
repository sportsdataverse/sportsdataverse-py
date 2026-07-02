"""On-court lineup reconstruction for the v3 pbp engine.

Provides utilities consumed by the Phase 1 lineup engine:

- :func:`boxscore_home_away` — extract home/away team ids from a
  ``boxScoreTraditional`` payload.
- :func:`parse_rotation_resultsets` — convert raw ``nba_stats_gamerotation``
  resultSets JSON into a ``{"HomeTeam": [...], "AwayTeam": [...]}`` dict.
- :func:`players_on_court_from_rotation` — pure rotation-based on-court
  reconstruction (no network calls) from a pre-parsed rotation dict.
- :func:`players_on_court_from_pbp` — pure gamerotation-*free* on-court
  reconstruction from boxscore starters + play-by-play substitutions only.
- :func:`players_on_court` — public entry point; delegates to
  :func:`players_on_court_from_rotation`.

Two independent producers reconstruct the same ``LINEUPS_SCHEMA`` frame:

- The **rotation** producer (:func:`players_on_court_from_rotation`) uses the
  ``nba_gamerotation`` endpoint (per-player stints keyed on ``PERSON_ID``).
- The **pbp** producer (:func:`players_on_court_from_pbp`) needs no rotation
  feed — it seeds each team's 5 and updates them from the play-by-play itself
  (see the pbp-producer algorithm note below).  It agrees with the rotation
  producer on ≈0.97 of fully-covered actions across the fixture games, and is
  validated against it by the ``test_pbp_agrees_with_rotation`` meta-oracle.

pbp-producer algorithm (per-period first-appearance seeding)
------------------------------------------------------------
The NBA play-by-play stream emits **no** substitution events at period starts,
so a period-ending lineup cannot simply be carried into the next period.  The
pbp producer therefore processes periods in ascending order and RE-SEEDS each
one from the play-by-play:

- **Period 1**: seed both teams from the boxscore starters
  (:func:`_starters_from_boxscore_v3`).
- **Each later period P**: re-seed via :func:`_period_starters` — a player who
  appears in an event (or is subbed *out*) before ever being subbed *in* that
  period must have been on court at the period start.  The prior period's
  ending lineup is used only as a carry-forward fallback for a silent starter.

Within a period the running 5-slot lists are updated on each row: a
substitution's own action row snapshots the PRE-sub lineup (the swap takes
effect on the next action), and a *contiguous run* of substitutions sharing one
game-clock tick is applied together so mid-run rows see the settled lineup —
both conventions match the boundary-based rotation producer.  Same-family
teammates in ``SUB: X FOR Y`` strings are disambiguated by the first-initial
keys :func:`_boxscore_name_map` registers (e.g. ``"t. antetokounmpo"``).

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
4. For each PBP event, the interval index is determined by boundary
   disambiguation (see Boundary disambiguation below).

Boundary disambiguation
-----------------------
When a PBP event's elapsed time exactly coincides with a rotation boundary,
we need to decide whether the event sees the lineup *before* or *after* the
rotation change.  Two heuristics resolve this:

- **Period-start**: an event with ``seconds_remaining >= 720`` (regulation)
  or ``>= 300`` (OT) is at the very start of a period — it sees the new
  lineup set after the period-transition substitutions.  POST-boundary
  (``side='right'``) is correct here.

- **Team-separated substitution ordering**: for each team, we track the
  minimum ``action_number`` of any substitution recorded at each elapsed
  time in the PBP.  If a same-team substitution at time T has a lower
  ``action_number`` than the current event, the rotation boundary at T was
  created by that substitution *before* this event — POST-sub (``side='right'``).
  If no same-team sub preceded this event at T, the event sees the
  PRE-boundary lineup (``side='left'``).

  Home and away sub maps are kept *separate* so that a substitution by the
  away team at time T does not influence the boundary resolution for a home
  team event at the same T.
"""

from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd
import polars as pl

from sportsdataverse.nba.nba_pbp_constants import LINEUPS_SCHEMA

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Network fetchers (module-level so tests can monkeypatch them)
# ---------------------------------------------------------------------------


def _fetch_pbp(game_id: str, league_id: str = "00") -> dict:
    """Fetch raw play-by-play v3 payload from stats.nba.com.

    Args:
        game_id: Ten-character NBA game identifier.
        league_id: League identifier (accepted for API symmetry; not forwarded
            to ``nba_stats_playbyplayv3`` which does not expose it).

    Returns:
        Raw ``dict`` from ``nba_stats_playbyplayv3``.
    """
    from sportsdataverse.nba.nba_stats import nba_stats_playbyplayv3

    return nba_stats_playbyplayv3(game_id=game_id, return_parsed=False)


def _fetch_rotation(game_id: str, league_id: str = "00") -> dict:
    """Fetch raw gamerotation payload from stats.nba.com.

    Args:
        game_id: Ten-character NBA game identifier.
        league_id: League identifier (default ``"00"`` for NBA).

    Returns:
        Raw ``dict`` from ``nba_stats_gamerotation``.
    """
    from sportsdataverse.nba.nba_stats import nba_stats_gamerotation

    return nba_stats_gamerotation(game_id=game_id, league_id=league_id, return_parsed=False)


def _fetch_box(game_id: str, league_id: str = "00") -> dict:
    """Fetch raw boxscore traditional v3 payload from stats.nba.com.

    Args:
        game_id: Ten-character NBA game identifier.
        league_id: League identifier (accepted for API symmetry; not forwarded
            to ``nba_stats_boxscoretraditionalv3`` which does not expose it).

    Returns:
        Raw ``dict`` from ``nba_stats_boxscoretraditionalv3``.
    """
    from sportsdataverse.nba.nba_stats import nba_stats_boxscoretraditionalv3

    return nba_stats_boxscoretraditionalv3(game_id=game_id, return_parsed=False)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _bt(box: dict) -> dict:
    """Return the ``boxScoreTraditional`` sub-dict, or empty dict if absent."""
    return (box or {}).get("boxScoreTraditional") or {}


def _played(stats: dict) -> bool:
    """Return True when *stats* contains non-zero recorded playing time.

    The ``minutes`` field in ``boxscoretraditionalv3`` is a ``"MM:SS"``
    string (e.g. ``"34:12"``).  A DNP player has ``""`` or ``None``.
    A player who entered but recorded no clock time has ``"0:00"``.

    Args:
        stats: The ``statistics`` sub-dict from a player entry, or ``{}``.

    Returns:
        ``True`` when the player recorded positive minutes; ``False``
        for DNP (empty string, ``None``) or zero-duration (``"0:00"``).
    """
    mins = (stats or {}).get("minutes") or ""
    if not mins:
        return False
    # Parse "MM:SS"; treat "0:00" and "00:00" as not played
    parts = mins.split(":")
    try:
        total_seconds = int(parts[0]) * 60 + int(parts[1]) if len(parts) == 2 else 0
    except (ValueError, IndexError):
        return False
    return total_seconds > 0


def _starters_from_boxscore_v3(raw_box: dict) -> Dict[int, List[int]]:
    """Extract each team's 5 starters from a boxscoretraditionalv3 payload.

    A player is a starter when its ``position`` field is non-empty.  If a
    side yields != 5 positional starters, pad from remaining players who
    recorded playing time (``minutes`` > ``"0:00"`` in their ``statistics``)
    until 5.  Did-not-play players (empty or zero ``minutes``) are excluded
    from the pad pool.

    Args:
        raw_box: Raw dict from ``nba_stats_boxscoretraditionalv3``.

    Returns:
        ``{team_id: [person_id, ...]}`` (5 ids per team). Empty dict on
        malformed/empty input (never raises).

    Example:
        Quick start::

            import json, pathlib
            from sportsdataverse.nba.nba_lineups import _starters_from_boxscore_v3
            box = json.loads(pathlib.Path("boxscoretraditionalv3.json").read_text())
            print(_starters_from_boxscore_v3(box))
    """
    bt = (raw_box or {}).get("boxScoreTraditional") or {}
    out: Dict[int, List[int]] = {}
    for side in ("homeTeam", "awayTeam"):
        team = bt.get(side) or {}
        tid = team.get("teamId")
        players = team.get("players") or []
        if not tid or not players:
            continue
        starters = [int(p["personId"]) for p in players if p.get("personId") and (p.get("position") or "").strip()]
        if len(starters) != 5:
            pool = [int(p["personId"]) for p in players if p.get("personId") and _played(p.get("statistics") or {})]
            starters = (starters + [p for p in pool if p not in starters])[:5]
        out[int(tid)] = starters
    return out


def _boxscore_name_map(raw_box: dict) -> Dict[int, Dict[str, List[int]]]:
    """Map each team's roster family-name -> [person_ids] (list handles collisions).

    In addition to the bare ``familyname_lower`` key, an ``"i. familyname"`` key
    (first-initial + family name) is registered for every player.  This is how
    the NBA pbp stream disambiguates same-family teammates in substitution
    strings — e.g. ``"SUB: T. Antetokounmpo FOR G. Antetokounmpo"`` — so
    :func:`_resolve_sub_in` can match the incoming name uniquely even when the
    bare family name collides.

    Args:
        raw_box: Raw dict from ``nba_stats_boxscoretraditionalv3``.

    Returns:
        ``{team_id: {familyname_lower: [person_id, ...],
        "i. familyname": [person_id]}}``.  Empty on malformed input (never raises).
    """
    bt = (raw_box or {}).get("boxScoreTraditional") or {}
    out: Dict[int, Dict[str, List[int]]] = {}
    for side in ("homeTeam", "awayTeam"):
        team = bt.get(side) or {}
        tid = team.get("teamId")
        if not tid:
            continue
        m: Dict[str, List[int]] = {}
        for p in team.get("players") or []:
            pid = p.get("personId")
            fam = (p.get("familyName") or "").strip().lower()
            if not (pid and fam):
                continue
            m.setdefault(fam, []).append(int(pid))
            first = (p.get("firstName") or "").strip().lower()
            if first:
                m.setdefault(f"{first[0]}. {fam}", []).append(int(pid))
        out[int(tid)] = m
    return out


def _parse_sub_in_name(description: str) -> Optional[str]:
    """Return the lowercased INCOMING family name from a ``SUB: X FOR Y`` string.

    Args:
        description: Play description string, e.g. ``"SUB: Vonleh FOR Horford"``.

    Returns:
        Lowercased family name of the incoming player, or ``None`` if the
        description is not a substitution string.
    """
    if not description:
        return None
    match = re.search(r"SUB:\s*(.+?)\s+FOR\s+", description, flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(1).strip().lower()


def _elapsed_time(period: int, seconds_remaining: float) -> float:
    """Convert period + seconds_remaining to tenths-of-second elapsed from game start.

    Mirrors the formula used by ``nba_gamerotation`` for ``IN_TIME_REAL`` and
    ``OUT_TIME_REAL``:

    - Periods 1-4 (12-minute quarters): ``((period-1)*720 + (720-sr))*10``
    - OT periods (5+, 5-minute periods): ``(2880 + (period-5)*300 + (300-sr))*10``
    """
    if period <= 4:
        return ((period - 1) * 720.0 + (720.0 - seconds_remaining)) * 10.0
    return (2880.0 + (period - 5) * 300.0 + (300.0 - seconds_remaining)) * 10.0


def _is_period_start(period: int, seconds_remaining: float) -> bool:
    """Return True if this clock value is at the very start of *period*.

    For regulation periods (1-4) the period is 720 seconds; for OT (5+) it is
    300 seconds.  An event whose ``seconds_remaining`` equals or exceeds the
    period length is at the start boundary and should see the POST-rotation lineup.
    """
    return seconds_remaining >= (720.0 if period <= 4 else 300.0)


def _build_sub_map(
    pbp_rows: list[dict],
    pbp_times: list[float],
    team_id: int,
) -> dict[float, int]:
    """Return a map of ``elapsed -> min_action_number`` for *team_id* substitutions.

    Only rows where ``is_substitution is True`` and ``team_id`` matches are
    considered.  The minimum ``action_number`` across all substitutions at a
    given elapsed time gives the earliest boundary-creating event for that team
    at that time.
    """
    sub_map: dict[float, int] = {}
    for row, elapsed in zip(pbp_rows, pbp_times):
        if row.get("is_substitution") and row.get("team_id") == team_id:
            an = int(row["action_number"])
            if elapsed not in sub_map or an < sub_map[elapsed]:
                sub_map[elapsed] = an
    return sub_map


def _resolve_team_oncourt(
    stints: list[dict],
    pbp_rows: list[dict],
    pbp_times: list[float],
    pbp_period_start: list[bool],
    sub_map: dict[float, int],
) -> list[list[int | None]]:
    """Map PBP elapsed times to sorted 5-player lineups using rotation stints.

    Port of hoopR's ``.resolve_team_oncourt()`` (nba_stats_pbp.R), extended
    with period-start and team-separated substitution-ordering disambiguation
    for events that land exactly on rotation boundaries.

    Args:
        stints: List of stint dicts with keys ``PERSON_ID``, ``IN_TIME_REAL``,
            ``OUT_TIME_REAL`` (all numeric, in tenths-of-second elapsed from
            game start).
        pbp_rows: Per-row dicts from the PBP frame (used for ``action_number``).
        pbp_times: Per-row elapsed times in tenths-of-second (same units as
            ``IN_TIME_REAL``/``OUT_TIME_REAL``).
        pbp_period_start: Per-row boolean — True if the event is at the very
            start of its period (``seconds_remaining >= period_length``).
        sub_map: ``{elapsed: min_action_number}`` for substitutions belonging
            to *this* team only (see :func:`_build_sub_map`).

    Returns:
        List of length ``len(pbp_times)``, each element a list of up to 5
        ``int`` player IDs (sorted ascending) padded with ``None`` to length 5.
    """
    if not stints or not pbp_times:
        lineup: list[int | None] = [None] * 5
        return [lineup] * len(pbp_times)

    in_times = np.array([float(s["IN_TIME_REAL"]) for s in stints])
    out_times = np.array([float(s["OUT_TIME_REAL"]) for s in stints])
    person_ids = np.array([int(s["PERSON_ID"]) for s in stints])

    boundaries = np.unique(np.concatenate([in_times, out_times]))
    n_bounds = len(boundaries)

    if n_bounds < 2:
        # Edge case: all stints have the same in/out time.
        active = list(dict.fromkeys(person_ids.tolist()))[:5]
        lineup_edge: list[int | None] = (active + [None] * 5)[:5]
        return [lineup_edge] * len(pbp_times)

    n_intervals = n_bounds - 1
    midpoints = (boundaries[:-1] + boundaries[1:]) / 2.0

    # Build lineup for each interval k via its midpoint.
    all_lineups: list[list[int | None]] = []
    for k in range(n_intervals):
        mid = float(midpoints[k])
        mask = (in_times <= mid) & (out_times > mid)
        active_raw = person_ids[mask].tolist()
        active_dedup = list(dict.fromkeys(active_raw))
        # [:5] matches hoopR: if >5 stints overlap a midpoint (a rare NBA-API
        # data quirk), keep the first 5 by ascending id.
        active_sorted: list[int] = sorted(active_dedup)[:5]
        row_lineup: list[int | None] = active_sorted + [None] * (5 - len(active_sorted))
        all_lineups.append(row_lineup)

    # Game-end lineup: players with in_time <= max_t AND out_time >= max_t.
    max_t = float(boundaries[n_bounds - 1])
    mask_end = (in_times <= max_t) & (out_times >= max_t)
    active_end_raw = person_ids[mask_end].tolist()
    active_end_dedup = list(dict.fromkeys(active_end_raw))
    # [:5] matches hoopR: keep the first 5 by ascending id if >5 stints overlap
    # the game-end boundary (same rare NBA-API quirk as the per-interval case).
    active_end_sorted: list[int] = sorted(active_end_dedup)[:5]
    final_lineup: list[int | None] = active_end_sorted + [None] * (5 - len(active_end_sorted))
    all_lineups.append(final_lineup)

    # Build a fast boundary-membership set for O(1) lookup.
    boundary_set: set[float] = set(boundaries.tolist())

    result: list[list[int | None]] = []
    for row, elapsed, is_ps in zip(pbp_rows, pbp_times, pbp_period_start):
        an = int(row["action_number"])

        if elapsed not in boundary_set:
            # Interior of an interval — standard right-biased searchsorted.
            idx = int(np.searchsorted(boundaries, elapsed, side="right") - 1)
        elif is_ps:
            # Exactly on a boundary AND this is the period-start event.
            # The rotation boundary here was created by the period transition,
            # so this event sees the POST-boundary (new-period) lineup.
            idx = int(np.searchsorted(boundaries, elapsed, side="right") - 1)
        else:
            # Exactly on a boundary; use same-team substitution ordering to
            # decide whether this event sees the pre- or post-rotation lineup.
            min_sub_an = sub_map.get(elapsed)
            if min_sub_an is not None and min_sub_an < an:
                # A same-team sub at this time preceded this event → POST-sub.
                idx = int(np.searchsorted(boundaries, elapsed, side="right") - 1)
            else:
                # No same-team sub preceded this event → PRE-boundary lineup.
                idx = int(np.searchsorted(boundaries, elapsed, side="left") - 1)

        idx = max(0, min(idx, n_intervals))
        result.append(all_lineups[idx])

    return result


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
            Must contain ``game_id``, ``action_number``, ``period``,
            ``seconds_remaining``, ``is_substitution``, and ``team_id``.
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

    # Fetch per-row elapsed times, period-start flags, and team-separated sub maps.
    need_cols = ["game_id", "action_number", "period", "seconds_remaining", "is_substitution", "team_id"]
    pbp_rows = enhanced_pbp.select(need_cols).to_dicts()

    pbp_times: list[float] = []
    pbp_period_start: list[bool] = []
    for r in pbp_rows:
        period = int(r["period"])
        sr = float(r["seconds_remaining"] or 0.0)
        pbp_times.append(_elapsed_time(period, sr))
        pbp_period_start.append(_is_period_start(period, sr))

    # Build per-team substitution ordering maps (elapsed → min action_number).
    # Kept separate so that an away sub at time T does not affect home boundary
    # resolution at the same T.
    home_sub_map = _build_sub_map(pbp_rows, pbp_times, home_team_id)
    away_sub_map = _build_sub_map(pbp_rows, pbp_times, away_team_id)

    # Resolve lineups for each team independently.
    home_lineups = _resolve_team_oncourt(home_stints, pbp_rows, pbp_times, pbp_period_start, home_sub_map)
    away_lineups = _resolve_team_oncourt(away_stints, pbp_rows, pbp_times, pbp_period_start, away_sub_map)

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


def _seed_five(starters: List[int]) -> List[Optional[int]]:
    """Return a 5-slot list seeded from *starters* (first 5, rest None).

    Args:
        starters: List of player ids (typically 5 from boxscore starters).

    Returns:
        A ``List[Optional[int]]`` of length 5.
    """
    five: List[Optional[int]] = [None] * 5
    for i, pid in enumerate(starters[:5]):
        five[i] = int(pid)
    return five


def _backfill_five(five: List[Optional[int]], pid: Optional[int]) -> None:
    """Fill the first ``None`` slot in *five* with *pid*, if *pid* is not already present.

    Mutates *five* in place.

    Args:
        five: Running 5-slot list for a team.
        pid: Player id to insert, or ``None`` (no-op).
    """
    if not pid or pid in five:
        return
    for i, v in enumerate(five):
        if v is None:
            five[i] = pid
            return


def _resolve_sub_in(
    description: str,
    name_map_team: Dict[str, List[int]],
    five: Sequence[Optional[int]],
) -> Optional[int]:
    """Resolve the incoming player id from a substitution description.

    Prefers a candidate currently **off** court (not in *five*) when
    there are name collisions on the roster.

    Args:
        description: Raw substitution description, e.g. ``"SUB: Vonleh FOR Horford"``.
        name_map_team: ``{familyname_lower: [person_id, ...]}`` for the
            substituting team (from :func:`_boxscore_name_map`).
        five: The team's current running 5-slot list.

    Returns:
        Resolved ``person_id`` int, or ``None`` if the name cannot be matched
        or the match is ambiguous.
    """
    name = _parse_sub_in_name(description)
    if not name:
        return None
    candidates = name_map_team.get(name, [])
    if len(candidates) == 1:
        return candidates[0]
    # collision: prefer a candidate currently OFF court
    off = [c for c in candidates if c not in five]
    if len(off) == 1:
        return off[0]
    return None  # ambiguous -> leave slot for first-appearance backfill


def _apply_pbp_sub(five: List[Optional[int]], sub_out: Optional[int], sub_in: Optional[int]) -> None:
    """Apply a substitution to *five* in place.

    When *sub_out* is present in *five*, it is replaced by *sub_in*.
    If *sub_out* is not found (data gap), *sub_in* is backfilled into a
    free slot instead.

    Args:
        five: The team's current running 5-slot list (mutated in place).
        sub_out: The outgoing player id (``person_id`` on a sub row).
        sub_in: The incoming player id resolved via :func:`_resolve_sub_in`.
    """
    if sub_out and sub_out in five:
        five[five.index(sub_out)] = sub_in
    elif sub_in and sub_in not in five:
        _backfill_five(five, sub_in)


def _period_starters(
    period_rows: List[dict],
    team_id: int,
    name_map: Dict[int, Dict[str, List[int]]],
    carry: List[Optional[int]],
) -> List[Optional[int]]:
    """Infer *team_id*'s on-court-at-period-start 5 from that period's pbp rows.

    Re-seeds a period's lineup directly from the play-by-play — the NBA stream
    emits **no** substitution events at period starts, so the ending lineup of
    the prior period cannot simply be carried forward. Instead, a player who
    appears in an event (or is subbed *out*) before ever being subbed *in* this
    period must have been on court at the period start.

    Args:
        period_rows: The rows for one period, in order. Each dict carries
            ``team_id``, ``person_id``, ``description``, ``is_substitution``.
        team_id: The team whose 5 starters we are inferring.
        name_map: ``{team_id: {familyname_lower: [person_id, ...]}}`` from
            :func:`_boxscore_name_map` (used to resolve ``SUB: X FOR Y`` in-names).
        carry: The team's lineup as it stood at the END of the prior period —
            used only to fill remaining slots for a silent starter who stayed on
            court but produced no event before being subbed off.

    Returns:
        A 5-slot ``List[Optional[int]]`` (from :func:`_seed_five`) of the
        players on court for *team_id* at the start of this period.
    """
    starters: List[int] = []  # ordered, <=5: on-court-at-start
    entered_via_sub: set[int] = set()  # subbed IN this period -> off the bench -> not a starter
    for r in period_rows:
        if int(r["team_id"] or 0) != team_id:
            continue
        pid = int(r["person_id"]) if r["person_id"] else None
        if r["is_substitution"]:
            # OUTGOING (person_id) was on court before this sub -> a starter
            # unless they entered via an earlier sub this period.
            if pid and pid not in entered_via_sub and pid not in starters and len(starters) < 5:
                starters.append(pid)
            sub_in = _resolve_sub_in(r["description"] or "", name_map.get(team_id, {}), starters)
            if sub_in:
                entered_via_sub.add(sub_in)
        else:
            # non-sub event: actor was on court; a starter unless they entered via sub earlier.
            if pid and pid not in entered_via_sub and pid not in starters and len(starters) < 5:
                starters.append(pid)
    # Fill any remaining slots from carry-forward (silent starter who stayed on
    # from the prior period but produced no event before being subbed off).
    if carry:
        for pid in carry:
            if pid and pid not in starters and pid not in entered_via_sub and len(starters) < 5:
                starters.append(pid)
    return _seed_five(starters)


def _row_elapsed_key(r: dict) -> Tuple[int, float]:
    """Return a ``(period, clock)`` tick key for grouping same-clock rows.

    Two rows share a key when they are in the same period at the same game
    clock — i.e. they land on the same rotation boundary.  Real clocks group
    by tick; null clocks are treated as individually distinct so that two
    consecutive null-clock subs never collapse into the same boundary.

    Args:
        r: A per-row dict carrying ``period``, ``seconds_remaining``, and
            ``action_number``.

    Returns:
        A ``(period, clock)`` tuple usable as a dict/equality key.
    """
    sr = r.get("seconds_remaining")
    if sr is not None:
        return (int(r["period"]), float(sr))
    # Null-clock rows get a per-row-unique sentinel so each is its own group.
    return (int(r["period"]), -1.0 - float(r["action_number"]))


def _snapshot_row(
    r: dict,
    home5: List[Optional[int]],
    away5: List[Optional[int]],
) -> dict:
    """Return one output row snapshotting the current *home5* / *away5* lineups.

    Args:
        r: The source pbp row (for ``game_id`` / ``action_number`` / ``period``).
        home5: The home team's running 5-slot list.
        away5: The away team's running 5-slot list.

    Returns:
        A dict with the ``LINEUPS_SCHEMA`` id columns plus the 10 slot columns.
    """
    return {
        "game_id": r["game_id"],
        "action_number": r["action_number"],
        "period": r["period"],
        **{f"home_player_{i + 1}": home5[i] for i in range(5)},
        **{f"away_player_{i + 1}": away5[i] for i in range(5)},
    }


def _apply_tick_subs(
    group: List[dict],
    name_map: Dict[int, Dict[str, List[int]]],
    home5: List[Optional[int]],
    away5: List[Optional[int]],
    home_team_id: int,
    away_team_id: int,
) -> None:
    """Apply every substitution in a same-tick *group* to *home5* / *away5* in place.

    The rotation producer collapses all substitutions on one game-clock tick
    into a single boundary, so a mid-tick event sees the *fully-settled*
    post-batch lineup rather than a partially-applied intermediate.  Applying
    the whole tick's subs at once reproduces that convention.

    Args:
        group: Consecutive rows sharing one elapsed tick (see
            :func:`_row_elapsed_key`).
        name_map: ``{team_id: {familyname_lower: [person_id, ...]}}``.
        home5: Home running 5-slot list (mutated in place).
        away5: Away running 5-slot list (mutated in place).
        home_team_id: Home team id.
        away_team_id: Away team id.
    """
    for r in group:
        if not r["is_substitution"]:
            continue
        tid = int(r["team_id"] or 0)
        team5 = home5 if tid == home_team_id else away5 if tid == away_team_id else None
        if team5 is None:
            continue
        pid = int(r["person_id"]) if r["person_id"] else None
        sub_in = _resolve_sub_in(r["description"] or "", name_map.get(tid, {}), team5)
        _apply_pbp_sub(team5, pid, sub_in)  # pid = OUTGOING player


def players_on_court_from_pbp(
    enhanced_pbp: pl.DataFrame,
    raw_box: dict,
    *,
    home_team_id: int,
    away_team_id: int,
) -> pl.DataFrame:
    """Reconstruct the 5-on-5 on-court lineup from pbp subs + boxscore starters.

    Pure function (no network). A gamerotation-free alternative to
    :func:`players_on_court_from_rotation` returning the identical
    ``LINEUPS_SCHEMA`` frame (one row per action, slots sorted ascending or
    ``None``). See the module design for the algorithm.

    Args:
        enhanced_pbp: Output of ``enhanced_pbp_from_payload``. Must carry
            ``game_id``, ``action_number``, ``order_index``, ``period``,
            ``team_id``, ``person_id``, ``description``, ``is_substitution``.
        raw_box: Raw ``boxscoretraditionalv3`` dict (starters + name map).
        home_team_id: Home team id (from ``boxscore_home_away``).
        away_team_id: Away team id (from ``boxscore_home_away``).

    Returns:
        :class:`polars.DataFrame` conforming to ``LINEUPS_SCHEMA``. Empty input
        returns a zero-row frame (never raises).

    Example:
        Quick start::

            import json, pathlib
            from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
            from sportsdataverse.nba.nba_lineups import (
                boxscore_home_away, players_on_court_from_pbp,
            )
            box = json.loads(pathlib.Path("boxscoretraditionalv3.json").read_text())
            pbp = json.loads(pathlib.Path("playbyplayv3.json").read_text())
            enh = enhanced_pbp_from_payload(pbp)
            home, away = boxscore_home_away(box)
            oc = players_on_court_from_pbp(enh, box, home_team_id=home, away_team_id=away)
            print(oc.shape)

        See Also:
            * `hoopR`_ -- R package providing equivalent lineup utilities
            * `nba_api`_ -- reference Python client for stats.nba.com

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
    """
    if enhanced_pbp.is_empty():
        return pl.DataFrame(schema=LINEUPS_SCHEMA)

    starters = _starters_from_boxscore_v3(raw_box)
    name_map = _boxscore_name_map(raw_box)
    home5: List[Optional[int]] = _seed_five(starters.get(home_team_id, []))
    away5: List[Optional[int]] = _seed_five(starters.get(away_team_id, []))

    sort_col = "order_index" if "order_index" in enhanced_pbp.columns else "action_number"
    rows = (
        enhanced_pbp.sort(sort_col)
        .select(
            [
                "game_id",
                "action_number",
                "period",
                "seconds_remaining",
                "team_id",
                "person_id",
                "description",
                "is_substitution",
            ]
        )
        .to_dicts()
    )

    # Group rows by period, preserving the sorted (order_index) order within each.
    periods: List[int] = sorted({int(r["period"]) for r in rows})
    rows_by_period: Dict[int, List[dict]] = {p: [] for p in periods}
    for r in rows:
        rows_by_period[int(r["period"])].append(r)

    out_rows: List[dict] = []
    for pi, period in enumerate(periods):
        period_rows = rows_by_period[period]
        # Re-seed each period's 5-per-team FROM the play-by-play. The NBA stream
        # emits no substitution events at period starts, so the prior period's
        # ending lineup cannot be carried unchanged (that is the bug this fixes).
        # Period 1 keeps the boxscore-starters seeding; every later period is
        # re-seeded via first-appearance inference, using the prior period's
        # ending lineup only as a carry-forward fallback for silent starters.
        if pi > 0:
            home5 = _period_starters(period_rows, home_team_id, name_map, home5)
            away5 = _period_starters(period_rows, away_team_id, name_map, away5)

        # Walk the period's rows in order.  Substitutions are contiguously
        # batched by game-clock tick: consecutive substitution rows that share
        # one elapsed tick are applied together, so a mid-batch sub row sees the
        # fully-settled lineup (matching the rotation producer, which collapses
        # all subs on a tick into a single boundary).  Only an *unbroken run of
        # substitutions* is batched — a non-sub event ends the run — so the
        # coarse 1-second pbp clock never lumps unrelated plays together.
        i = 0
        n = len(period_rows)
        while i < n:
            r = period_rows[i]
            tid = int(r["team_id"] or 0)
            pid = int(r["person_id"]) if r["person_id"] else None
            team5: Optional[List[Optional[int]]] = (
                home5 if tid == home_team_id else away5 if tid == away_team_id else None
            )
            is_sub = bool(r["is_substitution"]) and team5 is not None
            if not is_sub:
                if team5 is not None and pid:
                    _backfill_five(team5, pid)  # first-appearance backfill for actors
                out_rows.append(_snapshot_row(r, home5, away5))
                i += 1
                continue

            # First substitution of a same-tick run.  Gather the contiguous run
            # of substitutions sharing this exact elapsed tick.
            key = _row_elapsed_key(r)
            j = i
            while j < n and bool(period_rows[j]["is_substitution"]) and _row_elapsed_key(period_rows[j]) == key:
                j += 1
            run = period_rows[i:j]
            # The first sub row snapshots the PRE-batch lineup; then apply all of
            # the run's subs so the remaining sub rows see the settled lineup.
            out_rows.append(_snapshot_row(r, home5, away5))
            _apply_tick_subs(run, name_map, home5, away5, home_team_id, away_team_id)
            for r2 in run[1:]:
                out_rows.append(_snapshot_row(r2, home5, away5))
            i = j

    df = pl.DataFrame(out_rows, schema=LINEUPS_SCHEMA)

    # ffill/bfill each positional slot within game to patch unresolved-sub gaps
    slot_cols = [f"home_player_{i}" for i in range(1, 6)] + [f"away_player_{i}" for i in range(1, 6)]
    df = df.with_columns([pl.col(c).forward_fill().backward_fill().over("game_id") for c in slot_cols])

    # Sort each team's 5 ascending per row (matches rotation producer convention)
    df = (
        df.with_columns(
            [
                pl.concat_list([f"home_player_{i}" for i in range(1, 6)]).list.sort().alias("_h"),
                pl.concat_list([f"away_player_{i}" for i in range(1, 6)]).list.sort().alias("_a"),
            ]
        )
        .with_columns(
            [pl.col("_h").list.get(i - 1).alias(f"home_player_{i}") for i in range(1, 6)]
            + [pl.col("_a").list.get(i - 1).alias(f"away_player_{i}") for i in range(1, 6)]
        )
        .drop(["_h", "_a"])
    )

    return df.select(list(LINEUPS_SCHEMA.keys()))


def nba_on_court(
    game_id: str,
    league_id: str = "00",
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Fetch and reconstruct the 5-on-5 on-court lineup for every play-by-play action.

    Makes three live network calls (play-by-play v3, game rotation, boxscore
    traditional v3), then chains
    :func:`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`,
    :func:`parse_rotation_resultsets`, :func:`boxscore_home_away`, and
    :func:`players_on_court_from_rotation` to build a tidy lineup frame.

    The three module-level fetchers (:func:`_fetch_pbp`, :func:`_fetch_rotation`,
    :func:`_fetch_box`) are monkeypatchable for offline tests.

    Args:
        game_id: Ten-character NBA game identifier (e.g. ``"0022200001"``).
        league_id: League identifier (default ``"00"`` for NBA).  In Phase 1,
            only ``nba_gamerotation`` forwards ``league_id``; ``playbyplayv3``
            and ``boxscoretraditionalv3`` have no ``league_id`` parameter, so
            a non-``"00"`` value does not change the pbp or boxscore output.
            Full WNBA/G-League support is a later phase.
        return_as_pandas: If ``True``, return a :class:`pandas.DataFrame`
            instead of :class:`polars.DataFrame`.

    Returns:
        Polars (or pandas) DataFrame with schema ``LINEUPS_SCHEMA``, one row
        per play-by-play action.  Never raises — empty/malformed payloads
        return a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_lineups import nba_on_court
            df = nba_on_court("0022200001")
            print(df.shape)

        Pandas output::

            df_pd = nba_on_court("0022200001", return_as_pandas=True)
            print(type(df_pd))

        See Also:
            * `hoopR`_ -- R package providing equivalent lineup utilities
            * `nba_api`_ -- reference Python client for stats.nba.com

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
    """
    from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload

    raw_pbp = _fetch_pbp(game_id, league_id)
    raw_rot = _fetch_rotation(game_id, league_id)
    raw_box = _fetch_box(game_id, league_id)

    enh = enhanced_pbp_from_payload(raw_pbp, league_id=league_id)
    rot = parse_rotation_resultsets(raw_rot)
    home, away = boxscore_home_away(raw_box)

    df = players_on_court_from_rotation(enh, rot, home_team_id=home, away_team_id=away)
    if return_as_pandas:
        return df.to_pandas()
    return df
