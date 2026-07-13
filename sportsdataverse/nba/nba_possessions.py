"""NBA possession construction from enhanced play-by-play.

Consumes the enhanced PBP frame produced by
:func:`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload` and
emits one row per possession with offense/defense attribution and points.

Points are reconciled against the boxscore oracle (total possession points
per offense team == boxscore team points) for all three canonical fixture
games (0022100001, 0022200001, 0022300001).
"""

from __future__ import annotations

import logging
import re
from typing import Dict, Optional, Union

import pandas as pd
import polars as pl

from sportsdataverse.nba.nba_possession_rules import (
    build_event_context,
    is_possession_ending_event,
    is_real_rebound,
    is_technical_ft_row,
    resolve_event_team,
)

logger = logging.getLogger(__name__)

# Columns added by attach_possession_lineups (the RAPM stint design matrix).
LINEUP_COLUMNS: list[str] = [f"off_player_{i}" for i in range(1, 6)] + [f"def_player_{i}" for i in range(1, 6)]

# Fraction of on-court rows that may have at least one null player slot before
# we treat the rotation payload as degraded. A healthy gamerotation produces
# ~0% null rows; a one-sided-missing payload produces ~100%. Threshold of 2%
# gives generous headroom while still catching the one-sided failure mode.
_ROTATION_NULL_TOLERANCE = 0.02  # >2% of actions missing a player slot => degraded rotation, fall back to pbp

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
    "number_in_period": pl.Int64,
    "possession_start_type": pl.Utf8,
    "count_as_possession": pl.Boolean,
    # WP1 event detail (team-level counts within the possession)
    "fg2a": pl.Int64,
    "fg2m": pl.Int64,
    "fg3a": pl.Int64,
    "fg3m": pl.Int64,
    "fta": pl.Int64,
    "ftm": pl.Int64,
    "oreb": pl.Int64,
    "dreb": pl.Int64,
    "tov": pl.Int64,
}

#: Schema of the per-shooter companion frame from :func:`build_possession_shooting`.
POSSESSION_SHOOTING_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Utf8,
    "possession_number": pl.Int64,
    "player_id": pl.Int64,
    "team_id": pl.Int64,
    "fg2a": pl.Int64,
    "fg2m": pl.Int64,
    "fg3a": pl.Int64,
    "fg3m": pl.Int64,
    "fta": pl.Int64,
    "ftm": pl.Int64,
}

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_FT_NofN_RE = re.compile(r"(\d+)\s+of\s+(\d+)")

# G-League single-FT format: one free throw worth N points ("Free Throw 1PT",
# "Free Throw 2PT", "Free Throw 3PT").  Each event is a standalone FT trip and
# is therefore always the *last* (and only) FT of that trip.
_FT_GL_PT_RE = re.compile(r"Free Throw \d+\s*PT\b")


def _is_last_ft(sub_type: str) -> bool:
    """Return True if *sub_type* represents the final free throw of a trip.

    NBA/WNBA: matches ``'N of N'`` patterns where both numbers are equal —
    ``'Free Throw 2 of 2'``, ``'Free Throw 1 of 1'``, ``'Free Throw Flagrant
    3 of 3'``, etc.  ``'Free Throw Technical'`` does NOT match (no ``N of N``
    substring).

    G-League: also matches ``'Free Throw {N}PT'`` (``'Free Throw 1PT'``,
    ``'Free Throw 2PT'``, ``'Free Throw 3PT'`` — and the optional-space variant
    ``'Free Throw 2 PT'``, since the regex tolerates the space via ``\\s*``) —
    the G-League single-FT rule where one free throw is worth the value of the
    fouled shot.  These are standalone trips, so the event is always the last
    (and only) FT of its trip.
    """
    s = sub_type or ""
    if _FT_GL_PT_RE.search(s):
        return True
    m = _FT_NofN_RE.search(s)
    return bool(m and m.group(1) == m.group(2))


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

    A TECHNICAL free throw is skipped in both passes: its ``location``
    reflects whoever benefits from the opponent's technical foul, not the
    team about to have the ball once play resumes, so it must not drive the
    group's offense attribution any more than it may seed
    :func:`_build_possession_groups`'s ``current_offense`` (same rationale,
    same real-fixture case — see that function's docstring/comment).
    """
    scoring_types = frozenset(("made_shot", "missed_shot", "free_throw", "turnover", "rebound"))
    non_foul_types = frozenset(("foul", "period", "timeout", "substitution"))
    for ev in events:
        et = ev.get("event_type") or ""
        loc = ev.get("location") or ""
        if et == "free_throw" and is_technical_ft_row(ev):
            continue
        if et in scoring_types and loc:
            return home_id if loc == "h" else (away_id if loc == "v" else 0)
    for ev in events:
        loc = ev.get("location") or ""
        et = ev.get("event_type") or ""
        if et == "free_throw" and is_technical_ft_row(ev):
            continue
        if loc and et not in non_foul_types:
            return home_id if loc == "h" else (away_id if loc == "v" else 0)
    return 0


def _ft_made(ev: dict) -> bool:
    """A made free throw carries a score string (same signal as boundary detection)."""
    return bool((ev.get("score_home") or "").strip() or (ev.get("score_away") or "").strip())


def _event_detail(
    events: list[dict],
    offense: int,
    home_id: int,
    away_id: int,
) -> dict[str, int]:
    """Team-level counting-stat detail for one possession group, offense-filtered.

    Every count is filtered to the event's *resolved* team
    (:func:`~sportsdataverse.nba.nba_possession_rules.resolve_event_team`):
    ``fg2*``/``fg3*``/``fta``/``ftm`` only count when the shot/FT's team ==
    ``offense``; ``tov`` only counts a turnover whose team == ``offense``.
    This keeps ``points == 2*fg2m + 3*fg3m + ftm`` exact now that technical
    FTs are ordinary inline events: a DEFENSE technical FT shooter is present
    in the group but its team != offense, so it contributes to NEITHER
    ``points`` NOR ``ftm``.

    ``oreb``/``dreb`` additionally require the rebound to be a *real* rebound
    (``ev.get("_is_real_rebound", True)`` — the placeholder-exclusion flag
    :func:`_build_possession_groups` annotates during the scan via
    :func:`~sportsdataverse.nba.nba_possession_rules.is_real_rebound`):
    ``oreb`` counts real rebounds whose resolved team == ``offense``; ``dreb``
    counts real rebounds whose resolved team is nonzero and != ``offense``.

    Args:
        events: The possession group's event rows (row-dicts from
            ``enhanced_pbp.to_dicts()``), rebound rows carrying the
            ``_is_real_rebound`` annotation from the scan.
        offense: Resolved offense team ID for this possession (0 if
            unattributable at the time of the call — see
            :func:`build_possessions`, which resolves ``offense`` via
            delta-attribution before calling this helper).
        home_id: Home team ID.
        away_id: Away team ID.

    Returns:
        Dict with keys ``fg2a, fg2m, fg3a, fg3m, fta, ftm, oreb, dreb, tov``
        (all ``int`` counts for this possession group).
    """
    d = {
        "fg2a": 0,
        "fg2m": 0,
        "fg3a": 0,
        "fg3m": 0,
        "fta": 0,
        "ftm": 0,
        "oreb": 0,
        "dreb": 0,
        "tov": 0,
    }
    for ev in events:
        et = ev.get("event_type") or ""
        team = resolve_event_team(ev, home_id, away_id)
        if et in ("made_shot", "missed_shot"):
            if offense == 0 or team != offense:
                continue
            three = int(ev.get("shot_value") or 0) == 3
            d["fg3a" if three else "fg2a"] += 1
            if et == "made_shot":
                d["fg3m" if three else "fg2m"] += 1
        elif et == "free_throw":
            if offense == 0 or team != offense:
                continue
            d["fta"] += 1
            if _ft_made(ev):
                d["ftm"] += 1
        elif et == "turnover":
            if offense == 0 or team != offense:
                continue
            d["tov"] += 1
        elif et == "rebound":
            if not ev.get("_is_real_rebound", True):
                continue
            if team == 0:
                continue
            if offense != 0 and team == offense:
                d["oreb"] += 1
            elif team != offense:
                d["dreb"] += 1
    return d


def _shooting_rows(events: list[dict], game_id: str, poss_num: int, home_id: int, away_id: int) -> list[dict]:
    """Per-shooter shooting lines for one possession group (person_id-attributed events only).

    Companion to :func:`_event_detail`: instead of one team-level row per
    possession, emits one row per distinct shooter (``person_id``) who
    attempted a shot or free throw during the possession's events. Reuses the
    ``shot_value`` 2/3 split convention and the :func:`_ft_made` score-string
    signal for free-throw makes. Unlike :func:`_event_detail`, ALL shooters
    are kept regardless of resolved team — a defense technical FT shooter
    (excluded from the team-level columns) still appears here with its own
    ``team_id``.

    Args:
        events: The possession group's event rows (row-dicts from
            ``enhanced_pbp.to_dicts()``).
        game_id: The game identifier to stamp onto each output row.
        poss_num: The 1-indexed possession number to stamp onto each output row.
        home_id: Home team ID (for :func:`resolve_event_team`).
        away_id: Away team ID (for :func:`resolve_event_team`).

    Returns:
        List of row-dicts (one per shooter), each with keys ``game_id``,
        ``possession_number``, ``player_id``, ``team_id``, and the six
        shooting-count keys ``fg2a, fg2m, fg3a, fg3m, fta, ftm``. Events with
        ``person_id == 0`` (unattributable to a shooter) are skipped — they
        still count toward the team-level :func:`_event_detail` totals but
        cannot be attributed to a player here.
    """
    by_shooter: dict[int, dict[str, int]] = {}
    for ev in events:
        et = ev.get("event_type") or ""
        if et not in ("made_shot", "missed_shot", "free_throw"):
            continue
        pid = int(ev.get("person_id") or 0)
        if pid == 0:
            continue
        s = by_shooter.setdefault(
            pid,
            {
                "team_id": resolve_event_team(ev, home_id, away_id),
                "fg2a": 0,
                "fg2m": 0,
                "fg3a": 0,
                "fg3m": 0,
                "fta": 0,
                "ftm": 0,
            },
        )
        if et == "free_throw":
            s["fta"] += 1
            if _ft_made(ev):
                s["ftm"] += 1
        else:
            three = int(ev.get("shot_value") or 0) == 3
            s["fg3a" if three else "fg2a"] += 1
            if et == "made_shot":
                s["fg3m" if three else "fg2m"] += 1
    return [{"game_id": game_id, "possession_number": poss_num, "player_id": pid, **s} for pid, s in by_shooter.items()]


def _resolve_teams(df: pl.DataFrame) -> tuple[int, int]:
    """Return ``(home_team_id, away_team_id)`` from the PBP frame.

    Uses ``location='h'``/``'v'`` on non-zero-team events to identify teams.
    """
    h = df.filter((pl.col("location") == "h") & (pl.col("team_id") != 0))["team_id"].unique().to_list()
    v = df.filter((pl.col("location") == "v") & (pl.col("team_id") != 0))["team_id"].unique().to_list()
    return (h[0] if h else 0), (v[0] if v else 0)


def _possession_start_type(
    prev_end_row: Optional[dict],
    prev_rows: Optional[list[dict]],
    cur_rows: list[dict],
) -> str:
    """Coarse pbpstats ``possession_start_type`` (``possession.py:206-242``; no shot-type buckets).

    ``OffDeadball``: period start / team rebound / dead-ball turnover /
    unresolved. ``OffTimeout``: a timeout event in the previous OR current
    possession's rows. ``OffMadeShot`` / ``OffMissedShot``: prior boundary was
    a made shot-or-FT / a player defensive rebound. ``OffLiveBallTurnover``:
    prior boundary was a steal.

    Real-fixture verification note: the v3 feed's ``"STEAL"`` text does NOT
    live on the ``Turnover``-type row's own ``description`` (verified against
    all three canonical fixtures -- zero ``event_type == "turnover"`` rows
    contain ``"steal"``). It lives on a *companion* row sharing the same
    ``action_number`` with an empty ``actionType`` (``event_type == "other"``,
    e.g. ``"Nwora STEAL (1 STL)"``). Because the turnover row is itself the
    boundary event that flushes the previous possession group, that companion
    row always lands as the first row of the NEXT group -- i.e. ``cur_rows``,
    not ``prev_end_row``. The steal signal is therefore read from
    ``cur_rows`` (``.casefold()`` applied for case-insensitivity), not from
    ``prev_end_row``'s own description.
    """

    def _has_timeout(rows: Optional[list[dict]]) -> bool:
        return any((r.get("event_type") or "") == "timeout" for r in rows or [])

    if prev_end_row is None:
        return "OffDeadball"
    if _has_timeout(prev_rows) or _has_timeout(cur_rows):
        return "OffTimeout"
    et = prev_end_row.get("event_type") or ""
    if et in ("made_shot", "free_throw"):
        return "OffMadeShot"
    if et == "rebound":
        return "OffMissedShot" if (prev_end_row.get("person_id") or 0) else "OffDeadball"
    if et == "turnover":
        has_steal = any("steal" in (r.get("description") or "").casefold() for r in cur_rows or [])
        return "OffLiveBallTurnover" if has_steal else "OffDeadball"
    return "OffDeadball"


def _count_as_possession(
    prev_poss_end_seconds: float,
    end_seconds: float,
    group_rows: list[dict],
    rows_after_in_period: list[dict],
) -> bool:
    """pbpstats ``count_as_possession`` (``enhanced_pbp_item.py:180-208``).

    A possession STARTING with <=2s left in the period counts only if a made
    FT or made FG occurs before the period ends.

    The possession's *start* reference is ``prev_poss_end_seconds`` — the
    ``seconds_remaining`` of the event that ended the PREVIOUS possession, i.e.
    the moment the ball changed hands (period start → a large sentinel). This
    is the faithful port of pbpstats, whose salvage branch walks ``prev_event``
    back to the previous possession-ending event and tests
    ``prev_event.seconds_remaining > 2`` (``enhanced_pbp_item.py:194-197``) —
    NOT the current possession's own first-event clock. A possession whose
    first event is already inside the final 2s (e.g. a defensive rebound of a
    last-second miss) still counts as a real possession as long as it *started*
    (the ball changed hands) with >2s on the clock. Passing the group's own
    first-event seconds here (the prior bug) under-counted exactly those
    end-of-period possessions relative to the pbpstats-live oracle.

    Deliberate divergence: the salvage scan here is scoped to the same period
    (``rows_after_in_period`` stops at the period boundary); pbpstats' own
    ``next_event`` walk is unscoped and could in principle look past a period
    boundary. Documented trade-off, not a bug.
    """
    if end_seconds > 2.0 or prev_poss_end_seconds > 2.0:
        return True
    for r in list(group_rows) + list(rows_after_in_period):
        if (r.get("event_type") or "") in ("free_throw", "made_shot"):
            return True
    return False


# ---------------------------------------------------------------------------
# Core possession builder
# ---------------------------------------------------------------------------

# Event types that reliably indicate which team is on offense (shot attempts and
# turnovers).  Administrative events such as ``"other"`` (Delay of Game),
# ``"jump_ball"``, ``"replay"``, and ``"foul"`` carry a ``location`` but do NOT
# tell us who is shooting, so they must not seed ``current_offense`` — doing so
# mis-labels the subsequent rebound as offensive or defensive.
_OFFENSE_SEEDING_TYPES = frozenset(("missed_shot", "made_shot", "free_throw", "turnover"))


def _build_possession_groups(
    rows: list[dict],
    home_id: int,
    away_id: int,
) -> list[tuple[list[dict], bool, int]]:
    """Partition sorted PBP rows into possession groups (pbpstats-faithful).

    Boundary decisions are fully delegated to the rule dispatcher
    :func:`~sportsdataverse.nba.nba_possession_rules.is_possession_ending_event`
    (built on a per-game :class:`~sportsdataverse.nba.nba_possession_rules.EventContext`),
    which folds in the and-1 / away-from-play / transition-take / inbound FT
    exceptions and the rare jump-ball-changes-possession case. Rebound rows
    are annotated in-place with ``row["_is_real_rebound"]`` (the placeholder
    exclusion from :func:`~sportsdataverse.nba.nba_possession_rules.is_real_rebound`)
    so downstream consumers (:func:`_event_detail`'s ``oreb``/``dreb``) don't
    have to recompute it. Technical free throws are ordinary inline events —
    the dispatcher's free-throw branch already returns False for them via
    ``is_technical_ft_row`` inside ``ft_ends_possession``, so no separate
    isolation is needed.

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

    ctx = build_event_context(rows)

    for i, row in enumerate(rows):
        et = row.get("event_type") or ""
        loc = row.get("location") or ""
        period: int = row.get("period") or 0

        # Period change → flush current possession
        if prev_period is not None and period != prev_period:
            _flush()
        prev_period = period

        # Annotate rebound rows with the real-rebound placeholder-exclusion
        # verdict once here, so both this scan's second-chance flagging and
        # _event_detail's oreb/dreb attribution read the same computed value.
        if et == "rebound":
            row["_is_real_rebound"] = is_real_rebound(ctx, i)

        current.append(row)

        # Track offense team: only shot attempts and turnovers seed this —
        # administrative events (``"other"``, ``"foul"``, ``"jump_ball"``,
        # ``"replay"``) carry a location but do not identify the offensive team,
        # so they must be excluded to avoid mis-classifying the subsequent
        # rebound as offensive vs defensive. A TECHNICAL free throw is also
        # excluded here even though it belongs to the ordinary ``"free_throw"``
        # event category -- its shooter is whoever benefits from the opponent's
        # technical foul, not the team about to have the ball once play resumes.
        # Since techs are no longer isolated into their own group, seeding from
        # one would mislabel the real shot/rebound that follows (verified
        # against a real fixture case: a tech FT wrongly seeding the opposing
        # team, causing the next team's own missed shot + defensive rebound to
        # be attributed to the tech-FT shooter's team instead).
        ev_team = home_id if loc == "h" else (away_id if loc == "v" else 0)
        can_seed = et in _OFFENSE_SEEDING_TYPES and not (et == "free_throw" and is_technical_ft_row(row))
        if current_offense == 0 and ev_team != 0 and can_seed:
            current_offense = ev_team

        # Second-chance flagging: a REAL offensive rebound extends the
        # possession (the dispatcher itself never treats it as a boundary,
        # since it only returns True for a rebound resolving to the DEFENSE).
        if et == "rebound" and row.get("_is_real_rebound", True):
            reb_team = resolve_event_team(row, home_id, away_id)
            if current_offense != 0 and reb_team != 0 and reb_team == current_offense:
                is_sc = True

        if is_possession_ending_event(ctx, i, current_offense, home_id, away_id):
            _flush()

    _flush()  # remaining events
    return groups


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _assemble(enhanced_pbp: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Shared possession-construction pass: build both companion frames in one traversal.

    Does the full possession-group traversal exactly once — empty guards, home/away
    team resolution, score forward-fill, and the group loop — and emits both the
    team-level possession frame (:data:`POSSESSIONS_SCHEMA`) and the per-shooter
    companion frame (:data:`POSSESSION_SHOOTING_SCHEMA`) from it.
    :func:`build_possessions` and :func:`build_possession_shooting` are thin
    wrappers over this function's two return slots.

    Args:
        enhanced_pbp: Polars DataFrame with schema ``ENHANCED_PBP_SCHEMA`` (from
            :func:`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`).
            An empty or malformed frame returns ``(empty_possessions,
            empty_shooting)`` — never raises.

    Returns:
        Tuple ``(possessions, shooting)`` — the first with schema
        :data:`POSSESSIONS_SCHEMA`, the second with schema
        :data:`POSSESSION_SHOOTING_SCHEMA`.
    """
    empty_poss = pl.DataFrame(schema=POSSESSIONS_SCHEMA)
    empty_shooting = pl.DataFrame(schema=POSSESSION_SHOOTING_SCHEMA)

    if enhanced_pbp is None or enhanced_pbp.height == 0:
        return empty_poss, empty_shooting

    try:
        home_id, away_id = _resolve_teams(enhanced_pbp)
    except Exception:
        return empty_poss, empty_shooting

    if home_id == 0 or away_id == 0:
        return empty_poss, empty_shooting

    try:
        game_id: str = str(enhanced_pbp["game_id"][0])
    except Exception:
        return empty_poss, empty_shooting

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

    # Per-period last-row index, for count_as_possession's period-scoped salvage scan.
    # rows[i]["order_index"] == i (the frame is sorted by order_index before to_dicts()),
    # so this can be sliced by plain list index.
    period_end_index: dict[int, int] = {}
    for idx, r in enumerate(rows):
        period_end_index[int(r.get("period") or 0)] = idx

    # Build output rows with score-delta points
    prev_home = 0
    prev_away = 0
    records: list[dict] = []
    shooting: list[dict] = []
    poss_num = 0

    # possession_start_type context: tracked across EVERY group (including ones
    # dropped for lacking an attributable offense) so a timeout/turnover inside
    # a dropped group still informs the next emitted possession's start type —
    # this reflects true event chronology, not output chronology.
    prev_end_row: Optional[dict] = None
    prev_group_rows: Optional[list[dict]] = None
    prev_group_period: Optional[int] = None
    # number_in_period resets on the period of the last EMITTED record (dropped
    # groups don't consume a slot in the per-period possession count).
    prev_record_period: Optional[int] = None
    number_in_period = 0

    for events, is_sc, offense in groups:
        end_home: int = events[-1]["_home"]
        end_away: int = events[-1]["_away"]
        cur_period = int(events[0].get("period") or 0)

        if prev_group_period is not None and cur_period != prev_group_period:
            start_type = "OffDeadball"
        else:
            start_type = _possession_start_type(prev_end_row, prev_group_rows, events)

        if offense == 0:
            # Unattributable group (no scoring/shooting/rebound/turnover event
            # to assign offense from — e.g. a standalone technical FT or an
            # opening tip-off group).  Never silently drop a score delta: if
            # the score moved inside this group, attribute the points to the
            # team whose score actually increased (delta direction), so total
            # points still reconcile.  If no score change, dropping it is fine.
            home_delta = end_home - prev_home
            away_delta = end_away - prev_away
            if home_delta <= 0 and away_delta <= 0:
                prev_home = end_home
                prev_away = end_away
                prev_end_row = events[-1]
                prev_group_rows = events
                prev_group_period = cur_period
                continue
            offense = home_id if home_delta > 0 else away_id

        defense = away_id if offense == home_id else home_id
        start_ev = events[0]
        end_ev = events[-1]

        pts = (end_home - prev_home) if offense == home_id else (end_away - prev_away)

        poss_num += 1
        if prev_record_period is None or cur_period != prev_record_period:
            number_in_period = 1
        else:
            number_in_period += 1
        prev_record_period = cur_period

        start_seconds = float(start_ev.get("seconds_remaining") or 0.0)
        end_seconds = float(end_ev.get("seconds_remaining") or 0.0)
        end_idx = int(end_ev.get("order_index") or 0)
        period_last_idx = period_end_index.get(cur_period, end_idx)
        rows_after_in_period = rows[end_idx + 1 : period_last_idx + 1]
        # pbpstats-faithful count_as_possession start reference: the moment the
        # ball changed hands = the PREVIOUS possession-ending event's clock, not
        # this possession's own first-event clock. The first possession of a
        # period (no same-period predecessor) started at the period tip → a
        # large sentinel so it always counts (end_seconds > 2 dominates anyway).
        if prev_end_row is not None and prev_group_period == cur_period:
            prev_poss_end_seconds = float(prev_end_row.get("seconds_remaining") or 0.0)
        else:
            prev_poss_end_seconds = 720.0
        count_flag = _count_as_possession(prev_poss_end_seconds, end_seconds, events, rows_after_in_period)

        detail = _event_detail(events, int(offense), home_id, away_id)
        records.append(
            {
                "game_id": game_id,
                "period": int(start_ev.get("period") or 0),
                "possession_number": poss_num,
                "offense_team_id": int(offense),
                "defense_team_id": int(defense),
                "start_order_index": int(start_ev.get("order_index") or 0),
                "end_order_index": int(end_ev.get("order_index") or 0),
                "start_seconds_remaining": start_seconds,
                "end_seconds_remaining": end_seconds,
                "points": int(pts),
                "is_second_chance": bool(is_sc),
                "number_in_period": number_in_period,
                "possession_start_type": start_type,
                "count_as_possession": count_flag,
                **detail,
            }
        )
        shooting.extend(_shooting_rows(events, game_id, poss_num, home_id, away_id))

        prev_home = end_home
        prev_away = end_away
        prev_end_row = end_ev
        prev_group_rows = events
        prev_group_period = cur_period

    if not records:
        return empty_poss, empty_shooting

    return (
        pl.DataFrame(records, schema=POSSESSIONS_SCHEMA),
        pl.DataFrame(shooting, schema=POSSESSION_SHOOTING_SCHEMA) if shooting else empty_shooting,
    )


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

    Thin wrapper over :func:`_assemble`, which does the shared possession-group
    traversal once and also produces the per-shooter companion frame consumed
    by :func:`build_possession_shooting`.

    Args:
        enhanced_pbp: Polars DataFrame with schema
            ``ENHANCED_PBP_SCHEMA`` (from
            :func:`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`).
            An empty or malformed frame returns a zero-row frame with
            ``POSSESSIONS_SCHEMA`` — never raises.

    Returns:
        Polars DataFrame with schema :data:`POSSESSIONS_SCHEMA`.  One row
        per possession, ordered by ``possession_number`` ascending.  Includes
        nine team-level event-detail count columns (``fg2a``, ``fg2m``,
        ``fg3a``, ``fg3m``, ``fta``, ``ftm``, ``oreb``, ``dreb``, ``tov``)
        computed by :func:`_event_detail` from the possession's events, each
        filtered to the offense team (except ``dreb``, which counts the
        defense's real rebounds); ``points == 2*fg2m + 3*fg3m + ftm`` holds
        exactly on every possession.  Also includes three pbpstats-parity
        columns from :func:`_possession_start_type` / :func:`_count_as_possession`:
        ``number_in_period`` (Int64, the flat ``possession_number`` reset to 1
        at the start of each period -- pbpstats' ``number``), ``possession_start_type``
        (Utf8, one of ``OffDeadball``/``OffTimeout``/``OffMadeShot``/
        ``OffMissedShot``/``OffLiveBallTurnover`` -- every period's first
        possession is ``OffDeadball``), and ``count_as_possession`` (Boolean,
        False only for a possession starting with <=2s left in the period
        with no made FT/FG salvaging it before the period ends).

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
    return _assemble(enhanced_pbp)[0]


def build_possession_shooting(enhanced_pbp: pl.DataFrame) -> pl.DataFrame:
    """Build the per-shooter companion frame from an enhanced play-by-play DataFrame.

    Companion to :func:`build_possessions`: instead of one team-level row per
    possession, emits one row per distinct shooter (``player_id``) per
    possession, with their own ``fg2a/fg2m/fg3a/fg3m/fta/ftm`` counts. Shares
    the same possession-group traversal as :func:`build_possessions` via
    :func:`_assemble` — the two frames are always built from a single
    consistent pass over the play-by-play. Consumed by WP2's luck-adjusted
    shooting response.

    Args:
        enhanced_pbp: Polars DataFrame with schema
            ``ENHANCED_PBP_SCHEMA`` (from
            :func:`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`).
            An empty or malformed frame returns a zero-row frame with
            ``POSSESSION_SHOOTING_SCHEMA`` — never raises.

    Returns:
        Polars DataFrame with schema :data:`POSSESSION_SHOOTING_SCHEMA`. One
        row per ``(possession_number, player_id)`` pair. Events with
        ``person_id == 0`` are skipped (unattributable to a shooter — they
        still count toward :func:`build_possessions`' team-level totals).
        Per-possession sums of the six shooting columns match the
        corresponding :func:`build_possessions` columns exactly.

    Example:
        Quick start::

            import json, pathlib
            from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
            from sportsdataverse.nba.nba_possessions import build_possession_shooting

            payload = json.loads(pathlib.Path("playbyplayv3.json").read_text())
            pbp = enhanced_pbp_from_payload(payload)
            sh = build_possession_shooting(pbp)
            print(sh.shape, sh.schema["player_id"])

        Per-player shooting totals::

            import polars as pl
            totals = sh.group_by("player_id").agg(
                pl.col("fg3m").sum(), pl.col("ftm").sum()
            )
            print(totals.head())

        See Also:
            * `nba_api`_ -- reference Python client for stats.nba.com
            * `nflverse`_ -- analogous NFL possession engine

        .. _nba_api: https://github.com/swar/nba_api
        .. _nflverse: https://nflverse.nflverse.com
    """
    return _assemble(enhanced_pbp)[1]


# ---------------------------------------------------------------------------
# Task 3: on-court lineup attachment
# ---------------------------------------------------------------------------

#: Schema for the 10 lineup columns appended by :func:`attach_possession_lineups`.
_LINEUP_ATTACHMENT_SCHEMA: dict[str, pl.DataType] = {
    **{f"off_player_{i}": pl.Int64 for i in range(1, 6)},
    **{f"def_player_{i}": pl.Int64 for i in range(1, 6)},
}


def attach_possession_lineups(
    possessions: pl.DataFrame,
    oncourt: pl.DataFrame,
    enhanced_pbp: pl.DataFrame,
    *,
    home_team_id: int,
) -> pl.DataFrame:
    """Attach the 5v5 on-court lineup to each possession (the RAPM stint matrix).

    For each possession, looks up the 10 players on court at the possession's
    first action (``start_order_index``), then splits them into
    ``off_player_1..5`` (offense) and ``def_player_1..5`` (defense) by
    comparing ``offense_team_id`` to *home_team_id*.

    The *oncourt* frame is home/away-keyed
    (``home_player_1..5`` / ``away_player_1..5``).  When ``offense_team_id``
    matches *home_team_id*, ``home_player_*`` become ``off_player_*`` and
    ``away_player_*`` become ``def_player_*``; otherwise the assignment is
    flipped.

    *home_team_id* MUST come from the canonical, deterministic
    :func:`~sportsdataverse.nba.nba_lineups.boxscore_home_away` — the **same**
    source the *oncourt* frame was built with.  Passing it explicitly avoids
    a non-deterministic home/away inference that could silently swap the
    whole game's offense/defense columns.

    Mapping ``start_order_index`` → ``action_number`` is done via the
    *enhanced_pbp* frame which carries both columns.

    Args:
        possessions: Output of :func:`build_possessions`.  Must contain
            ``start_order_index`` (Int64) and ``offense_team_id`` (Int64).
        oncourt: Output of
            :func:`~sportsdataverse.nba.nba_lineups.players_on_court_from_rotation`.
            Must contain ``action_number`` (Int64) and
            ``home_player_1..5`` / ``away_player_1..5`` (Int64).
        enhanced_pbp: The same enhanced PBP frame passed to both
            :func:`build_possessions` and
            :func:`~sportsdataverse.nba.nba_lineups.players_on_court_from_rotation`.
            Used to map ``order_index`` → ``action_number``.
        home_team_id: Integer team ID of the home team, from
            :func:`~sportsdataverse.nba.nba_lineups.boxscore_home_away`.

    Returns:
        The *possessions* frame with ten additional Int64 columns:
        ``off_player_1..5`` and ``def_player_1..5``.  Every row is populated
        (no nulls) when the on-court frame covers all actions.  Returns the
        possessions frame with null-filled lineup columns on genuinely
        empty/malformed input — never raises on empty input.  Real lookup or
        column errors (e.g. a renamed column) are NOT swallowed; they surface.

    Example:
        Quick start::

            import json, pathlib
            from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
            from sportsdataverse.nba.nba_lineups import (
                boxscore_home_away, parse_rotation_resultsets,
                players_on_court_from_rotation,
            )
            from sportsdataverse.nba.nba_possessions import (
                build_possessions, attach_possession_lineups,
            )

            root = pathlib.Path("tests/fixtures/nba_engine/0022200001")
            enh = enhanced_pbp_from_payload(json.loads((root / "playbyplayv3.json").read_text()))
            box = json.loads((root / "boxscoretraditionalv3.json").read_text())
            rot = parse_rotation_resultsets(json.loads((root / "gamerotation.json").read_text()))
            home, away = boxscore_home_away(box)
            oncourt = players_on_court_from_rotation(enh, rot, home_team_id=home, away_team_id=away)
            poss = attach_possession_lineups(build_possessions(enh), oncourt, enh, home_team_id=home)
            print(poss[["off_player_1", "def_player_1"]].head())

        See Also:
            * `nba_api`_ -- reference Python client for stats.nba.com
            * `hoopR`_ -- R package providing equivalent lineup utilities

        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    # Never-raise contract: ONLY genuinely empty/malformed input is tolerated.
    if possessions is None or possessions.height == 0:
        return (
            possessions.with_columns([pl.lit(None).cast(pl.Int64).alias(c) for c in _LINEUP_ATTACHMENT_SCHEMA])
            if possessions is not None
            else pl.DataFrame(schema={**POSSESSIONS_SCHEMA, **_LINEUP_ATTACHMENT_SCHEMA})
        )

    if oncourt is None or oncourt.height == 0 or enhanced_pbp is None or enhanced_pbp.height == 0:
        # Genuinely empty oncourt / pbp → null lineup columns (no source data).
        null_cols = [pl.lit(None).cast(pl.Int64).alias(c) for c in _LINEUP_ATTACHMENT_SCHEMA]
        return possessions.with_columns(null_cols)

    # ----------------------------------------------------------------------
    # Step 1: build order_index → action_number map from enhanced_pbp.
    # ----------------------------------------------------------------------
    idx_to_action: dict[int, int] = {
        int(r["order_index"]): int(r["action_number"])
        for r in enhanced_pbp.select(["order_index", "action_number"]).to_dicts()
    }

    # ----------------------------------------------------------------------
    # Step 2: build action_number → lineup dict from oncourt frame.
    # ----------------------------------------------------------------------
    home_cols = [f"home_player_{i}" for i in range(1, 6)]
    away_cols = [f"away_player_{i}" for i in range(1, 6)]
    action_to_lineup: dict[int, dict] = {
        int(r["action_number"]): r for r in oncourt.select(["action_number"] + home_cols + away_cols).to_dicts()
    }

    # ----------------------------------------------------------------------
    # Step 3: for each possession, resolve the lineup and flip to
    #         offense/defense orientation using the explicit home_team_id.
    # ----------------------------------------------------------------------
    off_cols_data: dict[str, list[Optional[int]]] = {f"off_player_{i}": [] for i in range(1, 6)}
    def_cols_data: dict[str, list[Optional[int]]] = {f"def_player_{i}": [] for i in range(1, 6)}

    for r in possessions.select(["start_order_index", "offense_team_id"]).to_dicts():
        order_idx = int(r["start_order_index"])
        offense_id = int(r["offense_team_id"])

        action_num = idx_to_action.get(order_idx)
        lineup = action_to_lineup.get(action_num) if action_num is not None else None

        if lineup is None:
            # No on-court coverage for this action — leave null (test catches it).
            for i in range(1, 6):
                off_cols_data[f"off_player_{i}"].append(None)
                def_cols_data[f"def_player_{i}"].append(None)
            continue

        # Deterministic flip: offense is home iff offense_team_id == home_team_id.
        offense_is_home = offense_id == home_team_id

        if offense_is_home:
            for i in range(1, 6):
                off_cols_data[f"off_player_{i}"].append(lineup[f"home_player_{i}"])
                def_cols_data[f"def_player_{i}"].append(lineup[f"away_player_{i}"])
        else:
            for i in range(1, 6):
                off_cols_data[f"off_player_{i}"].append(lineup[f"away_player_{i}"])
                def_cols_data[f"def_player_{i}"].append(lineup[f"home_player_{i}"])

    # ----------------------------------------------------------------------
    # Step 4: build the lineup DataFrame and hstack onto possessions.
    # ----------------------------------------------------------------------
    lineup_df = pl.DataFrame(
        {**off_cols_data, **def_cols_data},
        schema=_LINEUP_ATTACHMENT_SCHEMA,
    )
    return possessions.hstack(lineup_df)


# ---------------------------------------------------------------------------
# Task 4: Network fetchers (module-level so tests can monkeypatch them)
# ---------------------------------------------------------------------------


def _fetch_pbp(game_id: str, league_id: str = "00", *, proxy_url: Optional[str] = None) -> dict:
    """Fetch raw play-by-play v3 payload from stats.nba.com.

    Args:
        game_id: Ten-character NBA game identifier.
        league_id: League identifier (accepted for API symmetry; not forwarded
            to ``nba_stats_playbyplayv3`` which does not expose it).
        proxy_url: Optional proxy URL forwarded to the underlying transport.

    Returns:
        Raw ``dict`` from ``nba_stats_playbyplayv3``.
    """
    from sportsdataverse.nba.nba_stats import nba_stats_playbyplayv3

    return nba_stats_playbyplayv3(game_id=game_id, return_parsed=False, proxy_url=proxy_url)


def _fetch_rotation(game_id: str, league_id: str = "00", *, proxy_url: Optional[str] = None) -> dict:
    """Fetch raw gamerotation payload from stats.nba.com.

    Args:
        game_id: Ten-character NBA game identifier.
        league_id: League identifier (default ``"00"`` for NBA).
        proxy_url: Optional proxy URL forwarded to the underlying transport.

    Returns:
        Raw ``dict`` from ``nba_stats_gamerotation``.
    """
    from sportsdataverse.nba.nba_stats import nba_stats_gamerotation

    return nba_stats_gamerotation(game_id=game_id, league_id=league_id, return_parsed=False, proxy_url=proxy_url)


def _fetch_box(game_id: str, league_id: str = "00", *, proxy_url: Optional[str] = None) -> dict:
    """Fetch raw boxscore traditional v3 payload from stats.nba.com.

    Args:
        game_id: Ten-character NBA game identifier.
        league_id: League identifier (accepted for API symmetry; not forwarded
            to ``nba_stats_boxscoretraditionalv3`` which does not expose it).
        proxy_url: Optional proxy URL forwarded to the underlying transport.

    Returns:
        Raw ``dict`` from ``nba_stats_boxscoretraditionalv3``.
    """
    from sportsdataverse.nba.nba_stats import nba_stats_boxscoretraditionalv3

    return nba_stats_boxscoretraditionalv3(game_id=game_id, return_parsed=False, proxy_url=proxy_url)


def _fetch_box_periods(
    game_id: str,
    n_periods: int,
    *,
    league_id: str = "00",
    proxy_url: Optional[str] = None,
) -> Dict[int, dict]:
    """Fetch per-period range-boxscores (one ``boxscoretraditionalv3`` call per period).

    Each period's payload is captured at that period's opening-tick window
    (:func:`~sportsdataverse.nba.nba_lineups._period_start_range`, the Task 1
    quarter-box grounding window) via
    :data:`~sportsdataverse.nba.nba_lineups._QUARTER_BOX_RANGE_TYPE`. Feeds
    :func:`~sportsdataverse.nba.nba_lineups.players_on_court_from_quarter_boxscores`
    for ``lineup_source="quarter_box"``.

    Module-level so tests can monkeypatch it, mirroring :func:`_fetch_pbp` /
    :func:`_fetch_rotation` / :func:`_fetch_box`.

    Args:
        game_id: Ten-character NBA game identifier.
        n_periods: Number of periods to fetch, 1-indexed (e.g. ``4`` for a
            regulation game with no overtime).
        league_id: League identifier (accepted for API symmetry; not
            forwarded to ``nba_stats_boxscoretraditionalv3``, which has no
            ``league_id`` parameter).
        proxy_url: Optional proxy URL forwarded to the underlying transport.

    Returns:
        ``{period: raw_boxscoretraditionalv3_range_payload}`` — one entry for
        every period from 1 to *n_periods* inclusive.
    """
    from sportsdataverse.nba.nba_lineups import _QUARTER_BOX_RANGE_TYPE, _period_start_range
    from sportsdataverse.nba.nba_stats import nba_stats_boxscoretraditionalv3

    out: Dict[int, dict] = {}
    for period in range(1, n_periods + 1):
        start_range, end_range = _period_start_range(period)
        out[period] = nba_stats_boxscoretraditionalv3(
            game_id=game_id,
            start_range=start_range,
            end_range=end_range,
            range_type=_QUARTER_BOX_RANGE_TYPE,
            return_parsed=False,
            proxy_url=proxy_url,
        )
    return out


# ---------------------------------------------------------------------------
# Public fetcher
# ---------------------------------------------------------------------------

_FULL_SCHEMA: dict[str, pl.DataType] = {
    **POSSESSIONS_SCHEMA,
    **_LINEUP_ATTACHMENT_SCHEMA,
}


def nba_possessions(
    game_id: str,
    league_id: str = "00",
    *,
    lineup_source: str = "auto",
    period_boxscores: Optional[Dict[int, dict]] = None,
    proxy_url: Optional[str] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Fetch and build the possession-level lineup stint matrix for a single game.

    Makes two to four live network calls (play-by-play v3, optionally game
    rotation, boxscore traditional v3, and optionally one
    ``boxscoretraditionalv3`` range call per period) then chains
    :func:`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`,
    :func:`~sportsdataverse.nba.nba_lineups.boxscore_home_away`,
    the selected on-court lineup producer,
    :func:`build_possessions`, and :func:`attach_possession_lineups` to
    produce the RAPM stint design matrix.

    The four module-level fetchers (:func:`_fetch_pbp`, :func:`_fetch_rotation`,
    :func:`_fetch_box`, :func:`_fetch_box_periods`) are monkeypatchable for
    offline tests.

    Args:
        game_id: Ten-character NBA game identifier (e.g. ``"0022200001"``).
        league_id: League identifier (default ``"00"`` for NBA).  In Phase 2,
            only ``nba_gamerotation`` forwards ``league_id``; ``playbyplayv3``
            and ``boxscoretraditionalv3`` have no ``league_id`` parameter, so
            a non-``"00"`` value does not change the pbp or boxscore output.
            Full WNBA/G-League support is a later phase.
        lineup_source: Which on-court lineup producer to use.  One of:

            - ``"rotation"`` — fetch ``gamerotation`` and use
              :func:`~sportsdataverse.nba.nba_lineups.players_on_court_from_rotation`.
              **Strict mode**: raises :exc:`ValueError` if the gamerotation
              endpoint returns no on-court data; there is no fallback.
            - ``"quarter_box"`` — fetch one ``boxscoretraditionalv3`` range
              payload per period (or use *period_boxscores* if supplied) and
              use
              :func:`~sportsdataverse.nba.nba_lineups.players_on_court_from_quarter_boxscores`
              (exact-seeds a period from its range-box when unambiguous,
              otherwise falls back to the same pbp inference as ``"pbp"`` —
              never raises on a missing/empty period map, so this mode has
              no strict failure case of its own).
            - ``"pbp"`` — skip the rotation and per-period-box fetches
              entirely and use
              :func:`~sportsdataverse.nba.nba_lineups.players_on_court_from_pbp`
              (~96.7 % agreement with rotation; requires no extra network call).
            - ``"auto"`` (default) — try rotation first; on failure or an
              empty/degraded on-court frame, try quarter_box; on failure or an
              empty on-court frame there, fall back to pbp.

            The returned frame gains a constant ``lineup_source`` column
            (``"rotation"``, ``"quarter_box"``, or ``"pbp"``) recording which
            producer was used.
        period_boxscores: Optional pre-fetched ``{period:
            raw_boxscoretraditionalv3_range_payload}`` map for
            ``lineup_source="quarter_box"`` (or the ``"auto"`` chain's
            quarter_box step). When ``None`` (default) and quarter_box is
            reached, :func:`_fetch_box_periods` fetches it. Ignored for
            ``"rotation"``/``"pbp"``.
        proxy_url: Optional proxy URL forwarded to every network call this
            function makes (pbp, rotation, box, and the per-period boxes).
            ``stats.nba.com`` hangs rather than errors on datacenter/cloud
            IPs, so an unattended host (CI, a droplet) MUST supply a proxy;
            see :func:`~sportsdataverse.nba.nba_season_compile.compile_nba_season`'s
            ``proxy_provider`` to rotate one per game across a season.
        return_as_pandas: If ``True``, return a :class:`pandas.DataFrame`
            instead of :class:`polars.DataFrame`.

    Returns:
        Polars (or pandas) DataFrame with schema combining
        :data:`POSSESSIONS_SCHEMA`, the ten lineup columns
        ``off_player_1..5`` / ``def_player_1..5``, and a ``lineup_source``
        Utf8 column.  One row per possession.
        Empty or malformed payloads return a zero-row frame (never raises).

    Example:
        Quick start (rotation, default)::

            from sportsdataverse.nba.nba_possessions import nba_possessions
            df = nba_possessions("0022200001")
            print(df.shape, df["off_player_1"].dtype)

        Pure-pbp lineups (no rotation fetch)::

            df_pbp = nba_possessions("0022200001", lineup_source="pbp")
            print(df_pbp["lineup_source"].unique())

        Quarter-box lineups (per-period range-boxscore exact seeding)::

            df_qb = nba_possessions("0022200001", lineup_source="quarter_box")
            print(df_qb["lineup_source"].unique())

        Pandas output::

            df_pd = nba_possessions("0022200001", return_as_pandas=True)
            print(type(df_pd))

        RAPM stint aggregation::

            import polars as pl
            stints = df.group_by(
                [f"off_player_{i}" for i in range(1, 6)]
                + [f"def_player_{i}" for i in range(1, 6)]
            ).agg(pl.col("points").sum(), pl.len().alias("possessions"))
            print(stints.head())

        See Also:
            * `nba_api`_ -- reference Python client for stats.nba.com
            * `hoopR`_ -- R package providing equivalent lineup utilities

        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
    from sportsdataverse.nba.nba_lineups import (
        boxscore_home_away,
        parse_rotation_resultsets,
        players_on_court_from_pbp,
        players_on_court_from_quarter_boxscores,
        players_on_court_from_rotation,
    )

    if lineup_source not in ("auto", "rotation", "pbp", "quarter_box"):
        raise ValueError(f"lineup_source must be 'auto'|'rotation'|'pbp'|'quarter_box', got {lineup_source!r}")

    raw_pbp = _fetch_pbp(game_id, league_id, proxy_url=proxy_url)
    raw_box = _fetch_box(game_id, league_id, proxy_url=proxy_url)
    enh = enhanced_pbp_from_payload(raw_pbp, league_id=league_id)
    home, away = boxscore_home_away(raw_box)

    def _from_pbp() -> "tuple[pl.DataFrame, str]":
        return players_on_court_from_pbp(enh, raw_box, home_team_id=home, away_team_id=away), "pbp"

    def _from_rotation() -> "tuple[pl.DataFrame, str]":
        raw_rot = _fetch_rotation(game_id, league_id, proxy_url=proxy_url)
        rot = parse_rotation_resultsets(raw_rot)
        oc = players_on_court_from_rotation(enh, rot, home_team_id=home, away_team_id=away)
        if oc.is_empty():
            raise ValueError("rotation produced empty on-court frame")
        # Coverage guard: a partially-degraded rotation payload (e.g. one side missing
        # stints) yields a non-empty frame with null player slots. Treat that as a
        # failure so "auto" falls back to the complete pbp reconstruction.
        _slot_cols = [f"home_player_{i}" for i in range(1, 6)] + [f"away_player_{i}" for i in range(1, 6)]
        _null_row_frac = oc.select(
            (pl.sum_horizontal([pl.col(c).is_null() for c in _slot_cols]) > 0).mean().alias("f")
        )["f"][0]
        if _null_row_frac is not None and _null_row_frac > _ROTATION_NULL_TOLERANCE:
            raise ValueError(f"rotation on-court frame has {_null_row_frac:.1%} rows with null slots")
        return oc, "rotation"

    def _from_quarter_box() -> "tuple[pl.DataFrame, str]":
        pb = period_boxscores
        if pb is None:
            n_periods = int(enh["period"].max() or 0) if not enh.is_empty() else 0
            pb = _fetch_box_periods(game_id, n_periods, league_id=league_id, proxy_url=proxy_url)
        oc = players_on_court_from_quarter_boxscores(enh, pb, raw_box, home_team_id=home, away_team_id=away)
        if oc.is_empty():
            raise ValueError("quarter_box produced empty on-court frame")
        return oc, "quarter_box"

    if lineup_source == "pbp":
        oc, used = _from_pbp()
    elif lineup_source == "rotation":
        oc, used = _from_rotation()
    elif lineup_source == "quarter_box":
        oc, used = _from_quarter_box()
    else:  # auto: rotation primary, quarter_box secondary, pbp final fallback
        try:
            oc, used = _from_rotation()
        except Exception as exc:  # noqa: BLE001 - fall back on any rotation failure
            logger.warning("nba_possessions(%s): rotation failed (%s) -> quarter_box fallback", game_id, exc)
            try:
                oc, used = _from_quarter_box()
            except Exception as exc2:  # noqa: BLE001 - fall back on any quarter_box failure
                logger.warning("nba_possessions(%s): quarter_box failed (%s) -> pbp fallback", game_id, exc2)
                oc, used = _from_pbp()

    poss = build_possessions(enh)
    df = attach_possession_lineups(poss, oc, enh, home_team_id=home).with_columns(pl.lit(used).alias("lineup_source"))

    if return_as_pandas:
        return df.to_pandas()
    return df
