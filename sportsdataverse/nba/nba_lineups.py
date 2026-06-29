"""On-court lineup reconstruction for the v3 pbp engine.

Provides three utilities consumed by the Phase 1 lineup engine:

- :func:`boxscore_home_away` — extract home/away team ids from a
  ``boxScoreTraditional`` payload.
- :func:`boxscore_name_map` — build a ``{team_id: {familyName_lower: person_id}}``
  lookup used to resolve player names from narrative text.
- :func:`period_starters` — infer which five players started each period for
  each team using the boxscore (period 1) and play-by-play substitution logic
  (periods 2+).
"""

from __future__ import annotations

import polars as pl


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------


def _bt(box: dict) -> dict:
    """Return the ``boxScoreTraditional`` sub-dict, or empty dict if absent."""
    return (box or {}).get("boxScoreTraditional") or {}


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


def period_starters(enhanced_pbp: pl.DataFrame, box: dict) -> dict[int, dict[int, list[int]]]:
    """Infer the five-man lineup that started each period for each team.

    Period 1 starters are read directly from the boxscore ``position`` field —
    players with a non-empty ``position`` value were in the starting lineup.

    For periods 2+, starters are inferred from the play-by-play:

    * A player who appears in the period **before** being substituted out
      started that period (they were already on the floor at the tip).
    * A player who is substituted out **before** appearing as an actor started
      that period (the substitution event carries the OUT player's ``person_id``
      per the v3 schema).

    In practice this means we walk the period's rows in ``order_index`` order,
    collecting each unique ``(team_id, person_id)`` pair in first-seen order,
    and take the first five per team.

    Args:
        enhanced_pbp: Output of :func:`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`.
            Must contain columns ``period``, ``order_index``, ``team_id``, and
            ``person_id``.
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
    starters: dict[int, dict[int, list[int]]] = {}
    box_st = _box_starters(box)
    periods: list[int] = enhanced_pbp["period"].unique().sort().to_list()

    for period in periods:
        if period == 1:
            # NOTE: best-effort — a boxscore with !=5 flagged starters yields <5 silently (pbpstats convention).
            starters[1] = {tid: list(ids[:5]) for tid, ids in box_st.items()}
            continue

        pe = enhanced_pbp.filter(pl.col("period") == period).sort("order_index")
        seen: dict[int, list[int]] = {}

        for r in pe.iter_rows(named=True):
            tid = r["team_id"]
            pid = r["person_id"]
            if tid is None or pid is None or tid <= 0 or pid <= 0:
                continue
            team_seen = seen.setdefault(tid, [])
            if pid not in team_seen:
                team_seen.append(pid)

        # NOTE: best-effort — a period with !=5 detected players yields <5 silently (pbpstats convention).
        starters[period] = {tid: ids[:5] for tid, ids in seen.items()}

    return starters
