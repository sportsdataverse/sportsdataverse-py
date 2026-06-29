"""On-court lineup reconstruction for the v3 pbp engine.

Provides four utilities consumed by the Phase 1 lineup engine:

- :func:`boxscore_home_away` — extract home/away team ids from a
  ``boxScoreTraditional`` payload.
- :func:`boxscore_name_map` — build a ``{team_id: {familyName_lower: person_id}}``
  lookup used to resolve player names from narrative text.
- :func:`period_starters` — infer which five players started each period for
  each team using the boxscore (period 1) and play-by-play substitution logic
  (periods 2+).
- :func:`players_on_court` — reconstruct the 5-on-5 on-court lineup for every
  play-by-play action via v3 substitution replay.
"""

from __future__ import annotations

import logging
import re

import polars as pl

from sportsdataverse.nba.nba_pbp_constants import LINEUPS_SCHEMA

logger = logging.getLogger(__name__)

# Regex to parse the incoming player's family name from a SUB description.
# Format: "SUB: <IN_familyName> FOR <OUT_familyName>"
# Lazy match so multi-word names like "House Jr." are captured fully.
_SUB_RE = re.compile(r"SUB:\s*(.+?)\s+FOR\s+", re.IGNORECASE)


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

    This correctly handles coaches' between-period lineup changes (a player who
    sat out period 1 and returns for period 2 appears early and is picked up)
    as well as bench players who receive early playing time (they are marked
    as sub-ins before their first ``person_id`` appearance and are excluded).

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
            # NOTE: best-effort — a boxscore with !=5 flagged starters yields <5 silently (pbpstats convention).
            starters[1] = {tid: list(ids[:5]) for tid, ids in box_st.items()}
            continue

        pe = enhanced_pbp.filter(pl.col("period") == period).sort("order_index")

        # Single-pass scan that distinguishes period starters from mid-period
        # substitutions:
        #
        # For each row (in order_index order):
        #   • If it is a substitution event, parse the IN-player from the
        #     description.  If that IN-player has *not yet appeared* in this
        #     period (i.e., they were sitting on the bench), mark them as a
        #     confirmed bench sub-in.  If they have already appeared, they
        #     are a returning starter — do not mark them.
        #   • When a non-substitution row (or a sub-out row) exposes a person_id
        #     that has not been seen yet AND is not a confirmed bench sub-in,
        #     add them to the starter list.
        #
        # A player who started the period appears via person_id (sub-out or
        # action) before any sub-in event involving them.  A bench player first
        # appears in person_id only AFTER their sub-in event stamps them as a
        # non-starter.  Returning starters (subbed out then back in) are already
        # in ``seen`` by the time the second sub-in is processed, so the second
        # sub-in event is correctly ignored.
        confirmed_bench: dict[int, set[int]] = {}  # {team_id: {person_id, ...}}
        seen: dict[int, list[int]] = {}

        for r in pe.iter_rows(named=True):
            tid = r["team_id"]
            pid = r["person_id"]

            # Process sub-in detection from description (team and description must be valid).
            if r["is_substitution"] and tid is not None and tid > 0:
                desc: str = r["description"] or ""
                tid_int_sub = int(tid)
                m = _SUB_RE.search(desc)
                if m:
                    in_name = m.group(1).strip().lower()
                    in_pid = name_map.get(tid_int_sub, {}).get(in_name)
                    if in_pid is not None:
                        # Only mark as bench sub-in if not already seen in this period.
                        if in_pid not in (seen.get(tid_int_sub) or []):
                            confirmed_bench.setdefault(tid_int_sub, set()).add(in_pid)

            # Collect starters from person_id appearances.
            if tid is None or pid is None or tid <= 0 or pid <= 0:
                continue
            tid_int = int(tid)
            pid_int = int(pid)
            if pid_int in confirmed_bench.get(tid_int, set()):
                continue  # bench player — skip
            team_seen = seen.setdefault(tid_int, [])
            if pid_int not in team_seen:
                team_seen.append(pid_int)

        # NOTE: best-effort — a period with !=5 detected starters yields <5 silently (pbpstats convention).
        starters[period] = {tid: ids[:5] for tid, ids in seen.items()}

    return starters


def players_on_court(
    enhanced_pbp: pl.DataFrame,
    period_starters: dict[int, dict[int, list[int]]],
    name_map: dict[int, dict[str, int]],
    *,
    home_team_id: int,
    away_team_id: int,
) -> pl.DataFrame:
    """Reconstruct the 5-on-5 on-court lineup for every play-by-play action.

    Seeds each period from *period_starters*, then replays substitution events
    in ``order_index`` order to track which ten players are on the floor at
    each action.

    The stamping rule mirrors the pbpstats oracle:

    * **Non-substitution rows** — stamp the current lineup (post any subs
      already applied in this period).
    * **Isolated substitution** (only one sub at this clock tick, and the
      next row has a higher action_number than the current row) — stamp
      **pre-sub**, then apply.  This is the most common case.
    * **Isolated substitution with reversed action_number ordering** (only
      one sub at this clock tick, but the next row has a *lower*
      action_number) — apply the sub first, then stamp **post-sub**.  This
      covers a rare v3 ordering quirk where a later-numbered action is
      interleaved before an earlier-numbered one at the same clock.
    * **Clustered substitution** (two or more subs share the same
      ``seconds_remaining`` within a period **and have consecutive
      action_numbers with no gaps**) — all subs in the cluster are stamped
      with the **pre-cluster** lineup (the lineup as it stood *before the
      first* sub in the cluster).  Each sub is still applied to ``current``
      so that the first non-sub row after the cluster sees the fully-updated
      lineup.  A gap of >1 between consecutive action_numbers at the same
      clock breaks the chain — those subs are treated as isolated.

    Player IDs within each team's five are sorted ascending before stamping,
    so ``home_player_1 < home_player_2 < ... < home_player_5`` (and likewise
    for away).

    Args:
        enhanced_pbp: Output of
            :func:`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`.
            Must contain ``game_id``, ``action_number``, ``period``,
            ``order_index``, ``seconds_remaining``, ``is_substitution``,
            ``person_id``, ``team_id``, and ``description``.
        period_starters: Output of :func:`period_starters` — maps
            ``{period: {team_id: [person_id, ...]}}`` for each period.
        name_map: Output of :func:`boxscore_name_map` — maps
            ``{team_id: {familyName_lower: person_id}}`` for name resolution.
        home_team_id: Integer team ID of the home team.
        away_team_id: Integer team ID of the away team.

    Returns:
        :class:`polars.DataFrame` conforming to ``LINEUPS_SCHEMA`` with one
        row per action in *enhanced_pbp* (same row count, same ordering).
        Never raises — name-resolution failures are logged and the lineup is
        left unchanged for that substitution event.

    Example:
        Quick start::

            import json, pathlib
            import polars as pl
            from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
            from sportsdataverse.nba.nba_lineups import (
                boxscore_home_away, boxscore_name_map,
                period_starters, players_on_court,
            )
            box = json.loads(pathlib.Path("boxscoretraditionalv3.json").read_text())
            pbp = json.loads(pathlib.Path("playbyplayv3.json").read_text())
            enh = enhanced_pbp_from_payload(pbp)
            home, away = boxscore_home_away(box)
            df = players_on_court(
                enh,
                period_starters(enh, box),
                boxscore_name_map(box),
                home_team_id=home,
                away_team_id=away,
            )
            print(df.shape)  # (468, 13)

        See Also:
            * `hoopR`_ -- R package providing equivalent lineup utilities
            * `nba_api`_ -- reference Python client for stats.nba.com

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
    """
    if enhanced_pbp.is_empty():
        return pl.DataFrame(schema=LINEUPS_SCHEMA)

    # Work in order_index order within each period.
    sorted_pbp = enhanced_pbp.sort(["period", "order_index"])

    # Pre-compute per-clock chain size.  A substitution is "clustered" when
    # two or more subs share the same (period, seconds_remaining) AND have
    # consecutive action_numbers (no gaps).  A gap > 1 between consecutive
    # action_numbers within the same clock group breaks the chain — those subs
    # are isolated.  Cross-team subs that share a clock and are truly consecutive
    # (e.g. an=126 HOME + 127 AWAY + 128 AWAY with no gaps) form one cluster;
    # subs with gaps (e.g. an=188 HOME + 190 AWAY, gap at 189) are isolated.
    subs_only = (
        sorted_pbp.filter(pl.col("is_substitution") == True)  # noqa: E712
        .select(["period", "seconds_remaining", "action_number"])
        .sort(["period", "seconds_remaining", "action_number"])
    )
    if subs_only.is_empty():
        sorted_pbp = sorted_pbp.with_columns(pl.lit(0).cast(pl.Int64).alias("_sub_count_at_clock"))
    else:
        # Assign chain IDs in Python (few dozen subs per game, O(n) cost).
        chain_rows: list[dict] = []
        chain_id = 0
        prev_period: int | None = None
        prev_sr: float | None = None
        prev_an: int | None = None
        for sr_row in subs_only.to_dicts():
            p, s, a = int(sr_row["period"]), float(sr_row["seconds_remaining"]), int(sr_row["action_number"])
            if p != prev_period or s != prev_sr:
                chain_id += 1  # new clock group → new chain
            elif prev_an is not None and a - prev_an > 1:
                chain_id += 1  # action_number gap within same clock → isolated
            chain_rows.append({"action_number": a, "_chain_id": chain_id})
            prev_period, prev_sr, prev_an = p, s, a

        chain_df = pl.DataFrame(chain_rows, schema={"action_number": pl.Int64, "_chain_id": pl.Int64})
        chain_sizes = chain_df.group_by("_chain_id").agg(pl.len().alias("_sub_count_at_clock"))
        chain_with_count = chain_df.join(chain_sizes, on="_chain_id", how="left").select(
            ["action_number", "_sub_count_at_clock"]
        )
        sorted_pbp = sorted_pbp.join(chain_with_count, on="action_number", how="left").with_columns(
            pl.col("_sub_count_at_clock").fill_null(0).cast(pl.Int64)
        )

    rows: list[dict] = sorted_pbp.select(
        [
            "game_id",
            "action_number",
            "period",
            "order_index",
            "seconds_remaining",
            "is_substitution",
            "person_id",
            "team_id",
            "description",
            "_sub_count_at_clock",
        ]
    ).to_dicts()

    n = len(rows)

    # current[team_id] = set of person_ids currently on the floor.
    current: dict[int, set[int]] = {}
    cur_period: int | None = None
    out_rows: list[dict] = []

    # Cluster tracking: when multiple subs share the same (period, sr) we stamp
    # all of them with the lineup as it stood before the *first* sub in that
    # cluster.
    cluster_period: int | None = None
    cluster_sr: float | None = None
    cluster_home: list[int] = []
    cluster_away: list[int] = []

    for idx, r in enumerate(rows):
        period: int = int(r["period"])

        # Seed period on first row of each new period.
        if period != cur_period:
            cur_period = period
            period_map = period_starters.get(period, {})
            current = {tid: set(ids) for tid, ids in period_map.items()}
            cluster_period = None
            cluster_sr = None

        if r["is_substitution"]:
            sr: float = r["seconds_remaining"]
            sub_count: int = int(r["_sub_count_at_clock"])
            tid_val = r["team_id"]
            tid_int: int | None = int(tid_val) if tid_val is not None else None
            out_pid = r["person_id"]
            desc: str = r["description"] or ""

            def _apply(tid: int, out_p: object, desc_str: str) -> None:
                """Apply a substitution to *current* in-place."""
                m = _SUB_RE.search(desc_str)
                if m:
                    in_name = m.group(1).strip().lower()
                    in_pid = name_map.get(tid, {}).get(in_name)
                    if in_pid is not None and out_p is not None:
                        current[tid].discard(int(out_p))
                        current[tid].add(int(in_pid))
                    else:
                        logger.debug(
                            "players_on_court: name miss — game=%s an=%s team=%s "
                            "in_name=%r in_pid=%s out_pid=%s desc=%r",
                            r["game_id"],
                            r["action_number"],
                            tid,
                            in_name,
                            in_pid,
                            out_p,
                            desc_str,
                        )
                else:
                    logger.debug(
                        "players_on_court: no SUB pattern — game=%s an=%s desc=%r",
                        r["game_id"],
                        r["action_number"],
                        desc_str,
                    )

            if sub_count > 1:
                # Clustered: stamp pre-cluster lineup for all subs in cluster.
                is_new_cluster = cluster_period != period or cluster_sr != sr
                if is_new_cluster:
                    cluster_period = period
                    cluster_sr = sr
                    cluster_home = sorted(current.get(home_team_id, set()))
                    cluster_away = sorted(current.get(away_team_id, set()))
                home_ids = cluster_home
                away_ids = cluster_away
                if tid_int is not None and tid_int in current:
                    _apply(tid_int, out_pid, desc)
            else:
                # Isolated: one sub at this clock.
                next_an = rows[idx + 1]["action_number"] if idx + 1 < n else int(r["action_number"]) + 1
                reversed_order = next_an < int(r["action_number"])
                if reversed_order:
                    # Apply THEN stamp post-sub (pbpstats stamps post when
                    # action_number ordering is inverted relative to clock order).
                    if tid_int is not None and tid_int in current:
                        _apply(tid_int, out_pid, desc)
                    home_ids = sorted(current.get(home_team_id, set()))
                    away_ids = sorted(current.get(away_team_id, set()))
                else:
                    # Normal isolated sub: stamp pre-sub, then apply.
                    home_ids = sorted(current.get(home_team_id, set()))
                    away_ids = sorted(current.get(away_team_id, set()))
                    if tid_int is not None and tid_int in current:
                        _apply(tid_int, out_pid, desc)
        else:
            # Non-sub: stamp current lineup (post any subs already applied).
            home_ids = sorted(current.get(home_team_id, set()))
            away_ids = sorted(current.get(away_team_id, set()))

        row: dict = {
            "game_id": r["game_id"],
            "action_number": int(r["action_number"]),
            "period": period,
        }
        for i in range(5):
            row[f"home_player_{i + 1}"] = home_ids[i] if i < len(home_ids) else None
        for i in range(5):
            row[f"away_player_{i + 1}"] = away_ids[i] if i < len(away_ids) else None
        out_rows.append(row)

    return pl.DataFrame(out_rows, schema=LINEUPS_SCHEMA)
