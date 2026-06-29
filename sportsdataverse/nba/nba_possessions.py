"""NBA possession construction from enhanced play-by-play.

Consumes the enhanced PBP frame produced by
:func:`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload` and
emits one row per possession with offense/defense attribution and points.

Points are reconciled against the boxscore oracle (total possession points
per offense team == boxscore team points) for all three canonical fixture
games (0022100001, 0022200001, 0022300001).
"""

from __future__ import annotations

import re
from typing import Optional

import polars as pl

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

POSSESSIONS_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Utf8,
    "period": pl.Int64,
    "possession_number": pl.Int64,
    "offense_team_id": pl.Int64,
    "defense_team_id": pl.Int64,
    "start_order_index": pl.Int64,
    "end_order_index": pl.Int64,
    "start_seconds_remaining": pl.Float64,
    "end_seconds_remaining": pl.Float64,
    "points": pl.Int64,
    "is_second_chance": pl.Boolean,
}

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_FT_NofN_RE = re.compile(r"(\d+)\s+of\s+(\d+)")


def _is_last_ft(sub_type: str) -> bool:
    """Return True if *sub_type* represents the final free throw of a trip.

    Matches patterns like ``'Free Throw 2 of 2'``, ``'Free Throw 1 of 1'``,
    ``'Free Throw 3 of 3'``, ``'Free Throw Flagrant 3 of 3'`` — any
    ``'N of N'`` where both numbers are equal.  ``'Free Throw Technical'``
    does NOT match (no ``N of N`` substring).
    """
    m = _FT_NofN_RE.search(sub_type or "")
    return bool(m and m.group(1) == m.group(2))


def _is_technical_ft(sub_type: str) -> bool:
    """Return True if *sub_type* indicates a technical free throw."""
    return "Technical" in (sub_type or "") or "technical" in (sub_type or "")


def _offense_from_events(
    events: list[dict],
    home_id: int,
    away_id: int,
) -> int:
    """Determine the offense team for a possession from its events.

    Priority: first scoring/shooting/rebound/turnover event with a
    non-empty ``location`` field (these are reliably attributed to the
    ball-holding team).  Falls back to any non-foul event with a
    ``location``.  Returns 0 if attribution is impossible (e.g.
    period-boundary-only groups).
    """
    scoring_types = frozenset(("made_shot", "missed_shot", "free_throw", "turnover", "rebound"))
    non_foul_types = frozenset(("foul", "period", "timeout", "substitution"))
    for ev in events:
        et = ev.get("event_type") or ""
        loc = ev.get("location") or ""
        if et in scoring_types and loc:
            return home_id if loc == "h" else (away_id if loc == "v" else 0)
    for ev in events:
        loc = ev.get("location") or ""
        et = ev.get("event_type") or ""
        if loc and et not in non_foul_types:
            return home_id if loc == "h" else (away_id if loc == "v" else 0)
    return 0


def _resolve_teams(df: pl.DataFrame) -> tuple[int, int]:
    """Return ``(home_team_id, away_team_id)`` from the PBP frame.

    Uses ``location='h'``/``'v'`` on non-zero-team events to identify teams.
    """
    h = df.filter((pl.col("location") == "h") & (pl.col("team_id") != 0))["team_id"].unique().to_list()
    v = df.filter((pl.col("location") == "v") & (pl.col("team_id") != 0))["team_id"].unique().to_list()
    return (h[0] if h else 0), (v[0] if v else 0)


# ---------------------------------------------------------------------------
# Core possession builder
# ---------------------------------------------------------------------------

_NON_BOUNDARY_EVENT_TYPES = frozenset(("period", "timeout", "substitution", "replay", "other", "foul", "jump_ball"))


def _build_possession_groups(
    rows: list[dict],
    home_id: int,
    away_id: int,
) -> list[tuple[list[dict], bool, int]]:
    """Partition sorted PBP rows into possession groups.

    Returns a list of ``(events, is_second_chance, offense_team_id)`` tuples.
    Groups with ``offense_team_id == 0`` have no attributable offense and
    are excluded from point counting (but included for score-tracking).
    """
    groups: list[tuple[list[dict], bool, int]] = []
    current: list[dict] = []
    is_sc = False
    current_offense = 0
    prev_period: Optional[int] = None

    def _flush() -> None:
        nonlocal current, is_sc, current_offense
        if current:
            off = _offense_from_events(current, home_id, away_id)
            groups.append((current, is_sc, off))
        current = []
        is_sc = False
        current_offense = 0

    for row in rows:
        et = row.get("event_type") or ""
        loc = row.get("location") or ""
        sub_type = row.get("sub_type") or ""
        period: int = row.get("period") or 0

        # Period change → flush current possession
        if prev_period is not None and period != prev_period:
            _flush()
        prev_period = period

        current.append(row)

        # Track offense team: first non-foul event with a location that isn't
        # a period/timeout/substitution sets the possession's offense.
        ev_team = home_id if loc == "h" else (away_id if loc == "v" else 0)
        if current_offense == 0 and ev_team != 0 and et not in ("foul", "period", "timeout", "substitution"):
            current_offense = ev_team

        # Non-boundary events — just accumulate
        if et in _NON_BOUNDARY_EVENT_TYPES:
            continue

        # Boundary detection
        ends_possession = False

        if et == "made_shot":
            # Made field goal always ends possession.
            # And-1 FTs are in the NEXT possession group and scored separately.
            ends_possession = True

        elif et == "turnover":
            ends_possession = True

        elif et == "rebound":
            # Determine rebounding team:
            #   - Player rebound: team_id = player's team
            #   - Team rebound:   team_id=0, person_id=team_id, location reliable
            reb_team = row.get("team_id") or 0
            if reb_team == 0:
                # team rebound — use location
                reb_team = ev_team
            if current_offense != 0 and reb_team != 0:
                if reb_team == current_offense:
                    # Offensive rebound → extends possession, mark second-chance
                    is_sc = True
                else:
                    # Defensive rebound → ends possession
                    ends_possession = True

        elif et == "free_throw":
            # Technical FTs don't end a possession trip.
            # A regular last-FT that was MADE ends the possession.
            # A missed last-FT lets the defensive rebound end it naturally.
            if not _is_technical_ft(sub_type) and _is_last_ft(sub_type):
                sh = (row.get("score_home") or "").strip()
                sa = (row.get("score_away") or "").strip()
                if sh or sa:
                    ends_possession = True

        if ends_possession:
            _flush()

    _flush()  # remaining events
    return groups


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_possessions(enhanced_pbp: pl.DataFrame) -> pl.DataFrame:
    """Build one row per possession from an enhanced play-by-play DataFrame.

    Consumes the output of
    :func:`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`.
    Possession boundaries follow pbpstats-core rules: made field goal,
    turnover, defensive rebound, made last free throw of a trip, or end of
    period.  An offensive rebound extends the current possession and sets
    ``is_second_chance = True``.

    Points are the offense team's score delta over the possession, derived
    by forward-filling ``score_home`` / ``score_away`` and differencing at
    possession boundaries.  The sum of ``points`` per offense team is
    reconciled against the boxscore oracle for the three canonical fixture
    games.

    Args:
        enhanced_pbp: Polars DataFrame with schema
            ``ENHANCED_PBP_SCHEMA`` (from
            :func:`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`).
            An empty or malformed frame returns a zero-row frame with
            ``POSSESSIONS_SCHEMA`` — never raises.

    Returns:
        Polars DataFrame with schema :data:`POSSESSIONS_SCHEMA`.  One row
        per possession, ordered by ``possession_number`` ascending.

    Example:
        Quick start::

            import json, pathlib
            from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
            from sportsdataverse.nba.nba_possessions import build_possessions

            payload = json.loads(pathlib.Path("playbyplayv3.json").read_text())
            pbp = enhanced_pbp_from_payload(payload)
            poss = build_possessions(pbp)
            print(poss.shape, poss.schema["offense_team_id"])

        Boxscore reconciliation check::

            import polars as pl
            pts = poss.group_by("offense_team_id").agg(pl.col("points").sum())
            print(pts)

        See Also:
            * `nba_api`_ -- reference Python client for stats.nba.com
            * `nflverse`_ -- analogous NFL possession engine

        .. _nba_api: https://github.com/swar/nba_api
        .. _nflverse: https://nflverse.nflverse.com
    """
    empty = pl.DataFrame(schema=POSSESSIONS_SCHEMA)

    if enhanced_pbp is None or enhanced_pbp.height == 0:
        return empty

    try:
        home_id, away_id = _resolve_teams(enhanced_pbp)
    except Exception:
        return empty

    if home_id == 0 or away_id == 0:
        return empty

    try:
        game_id: str = str(enhanced_pbp["game_id"][0])
    except Exception:
        return empty

    # Sort by order_index and convert to row-dicts for imperative traversal
    rows = enhanced_pbp.sort("order_index").to_dicts()

    # Forward-fill scores (score_home / score_away only populated on scoring events)
    last_home = 0
    last_away = 0
    for row in rows:
        sh = (row.get("score_home") or "").strip()
        sa = (row.get("score_away") or "").strip()
        if sh:
            last_home = int(sh)
        if sa:
            last_away = int(sa)
        row["_home"] = last_home
        row["_away"] = last_away

    groups = _build_possession_groups(rows, home_id, away_id)

    # Build output rows with score-delta points
    prev_home = 0
    prev_away = 0
    records: list[dict] = []
    poss_num = 0

    for events, is_sc, offense in groups:
        end_home: int = events[-1]["_home"]
        end_away: int = events[-1]["_away"]

        if offense == 0:
            # Unattributable group — advance score tracking, skip
            prev_home = end_home
            prev_away = end_away
            continue

        defense = away_id if offense == home_id else home_id
        start_ev = events[0]
        end_ev = events[-1]

        pts = (end_home - prev_home) if offense == home_id else (end_away - prev_away)

        poss_num += 1
        records.append(
            {
                "game_id": game_id,
                "period": int(start_ev.get("period") or 0),
                "possession_number": poss_num,
                "offense_team_id": int(offense),
                "defense_team_id": int(defense),
                "start_order_index": int(start_ev.get("order_index") or 0),
                "end_order_index": int(end_ev.get("order_index") or 0),
                "start_seconds_remaining": float(start_ev.get("seconds_remaining") or 0.0),
                "end_seconds_remaining": float(end_ev.get("seconds_remaining") or 0.0),
                "points": int(pts),
                "is_second_chance": bool(is_sc),
            }
        )

        prev_home = end_home
        prev_away = end_away

    if not records:
        return empty

    return pl.DataFrame(records, schema=POSSESSIONS_SCHEMA)
