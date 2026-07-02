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
from typing import Optional, Union

import pandas as pd
import polars as pl

logger = logging.getLogger(__name__)

# Columns added by attach_possession_lineups (the RAPM stint design matrix).
LINEUP_COLUMNS: list[str] = [f"off_player_{i}" for i in range(1, 6)] + [f"def_player_{i}" for i in range(1, 6)]

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

        # Track offense team: only shot attempts and turnovers seed this —
        # administrative events (``"other"``, ``"foul"``, ``"jump_ball"``,
        # ``"replay"``) carry a location but do not identify the offensive team,
        # so they must be excluded to avoid mis-classifying the subsequent
        # rebound as offensive vs defensive.
        ev_team = home_id if loc == "h" else (away_id if loc == "v" else 0)
        if current_offense == 0 and ev_team != 0 and et in _OFFENSE_SEEDING_TYPES:
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
                continue
            offense = home_id if home_delta > 0 else away_id

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
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Fetch and build the possession-level lineup stint matrix for a single game.

    Makes two or three live network calls (play-by-play v3, optionally game
    rotation, and boxscore traditional v3) then chains
    :func:`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`,
    :func:`~sportsdataverse.nba.nba_lineups.boxscore_home_away`,
    the selected on-court lineup producer,
    :func:`build_possessions`, and :func:`attach_possession_lineups` to
    produce the RAPM stint design matrix.

    The three module-level fetchers (:func:`_fetch_pbp`, :func:`_fetch_rotation`,
    :func:`_fetch_box`) are monkeypatchable for offline tests.

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
            - ``"pbp"`` — skip the rotation fetch entirely and use
              :func:`~sportsdataverse.nba.nba_lineups.players_on_court_from_pbp`
              (~96.7 % agreement with rotation; requires no extra network call).
            - ``"auto"`` (default) — try rotation first; if the rotation fetch
              raises or produces an empty on-court frame, fall back to pbp.

            The returned frame gains a constant ``lineup_source`` column
            (``"rotation"`` or ``"pbp"``) recording which producer was used.
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
        players_on_court_from_rotation,
    )

    if lineup_source not in ("auto", "rotation", "pbp"):
        raise ValueError(f"lineup_source must be 'auto'|'rotation'|'pbp', got {lineup_source!r}")

    raw_pbp = _fetch_pbp(game_id, league_id)
    raw_box = _fetch_box(game_id, league_id)
    enh = enhanced_pbp_from_payload(raw_pbp, league_id=league_id)
    home, away = boxscore_home_away(raw_box)

    def _from_pbp() -> "tuple[pl.DataFrame, str]":
        return players_on_court_from_pbp(enh, raw_box, home_team_id=home, away_team_id=away), "pbp"

    def _from_rotation() -> "tuple[pl.DataFrame, str]":
        raw_rot = _fetch_rotation(game_id, league_id)
        rot = parse_rotation_resultsets(raw_rot)
        oc = players_on_court_from_rotation(enh, rot, home_team_id=home, away_team_id=away)
        if oc.is_empty():
            raise ValueError("rotation produced empty on-court frame")
        return oc, "rotation"

    if lineup_source == "pbp":
        oc, used = _from_pbp()
    elif lineup_source == "rotation":
        oc, used = _from_rotation()
    else:  # auto: rotation primary, pbp fallback
        try:
            oc, used = _from_rotation()
        except Exception as exc:  # noqa: BLE001 - fall back on any rotation failure
            logger.warning("nba_possessions(%s): rotation failed (%s) -> pbp fallback", game_id, exc)
            oc, used = _from_pbp()

    poss = build_possessions(enh)
    df = attach_possession_lineups(poss, oc, enh, home_team_id=home).with_columns(pl.lit(used).alias("lineup_source"))

    if return_as_pandas:
        return df.to_pandas()
    return df
