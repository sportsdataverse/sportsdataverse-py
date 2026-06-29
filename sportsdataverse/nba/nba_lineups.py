"""On-court lineup reconstruction for the v3 pbp engine.

Provides utilities consumed by the Phase 1 lineup engine:

- :func:`boxscore_home_away` — extract home/away team ids from a
  ``boxScoreTraditional`` payload.
- :func:`parse_rotation_resultsets` — convert raw ``nba_stats_gamerotation``
  resultSets JSON into a ``{"HomeTeam": [...], "AwayTeam": [...]}`` dict.
- :func:`players_on_court_from_rotation` — pure rotation-based on-court
  reconstruction (no network calls) from a pre-parsed rotation dict.
- :func:`players_on_court` — public entry point; delegates to
  :func:`players_on_court_from_rotation`.

The lineup source is the ``nba_gamerotation`` endpoint (per-player stints
keyed on ``PERSON_ID``), so the earlier name-resolution + first-appearance
starter heuristics (``boxscore_name_map`` / ``period_starters`` /
``_box_starters``) are no longer needed and have been removed.

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
from typing import Union

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
