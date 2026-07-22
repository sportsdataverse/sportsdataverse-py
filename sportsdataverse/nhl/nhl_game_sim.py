"""NHL event-stream Monte Carlo game simulation (WS4, hockey family).

Hockey's sim unit is the EVENT STREAM, not a discrete possession: real
api-web ``play-by-play`` payloads classify into an event vocabulary
(conservation gate: goal events reconstruct the real final exactly), the
event PMF keys on (period, strength state, score-differential bucket), and
the engine walks the stream — faceoffs, hits, shots on goal, goals,
misses, blocks, give/takeaways, stoppages — while a penalty node opens
2-minute power plays that re-key the PMF, goals flip the score, and a
tied regulation resolves through sudden-death overtime and a shootout.

Nodes and their parameters (all fitted from the real stream; the
``models2shelf`` upgrade slots in per node exactly like basketball):

* :class:`EventNode` — event-type PMF | (period, strength, score bucket);
* :class:`OwnerNode` — which side owns the event (per-type home share);
* :class:`PenaltyNode` — opens/expires power plays (2:00, goal-ended);
* :class:`ShootoutNode` — Bernoulli conversion rounds when OT stays tied.

v1 ceilings (documented seams): minors only (majors/coincidentals fold
in), the PP re-key falls back to a fitted global PP multiplier where the
capture's PP segments are sparse, and one game of PMFs is thin — the
fixture README documents multi-game builds.
"""

from __future__ import annotations

import dataclasses
import re
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import polars as pl

from sportsdataverse._common.play_text import get_templates

#: Sampleable stream events (period markers and delayed calls excluded).
NHL_EVENTS: Tuple[str, ...] = (
    "faceoff",
    "hit",
    "shot_on_goal",
    "goal",
    "missed_shot",
    "blocked_shot",
    "giveaway",
    "takeaway",
    "penalty",
    "stoppage",
)

_TYPE_MAP = {
    "faceoff": "faceoff",
    "hit": "hit",
    "shot-on-goal": "shot_on_goal",
    "goal": "goal",
    "missed-shot": "missed_shot",
    "blocked-shot": "blocked_shot",
    "giveaway": "giveaway",
    "takeaway": "takeaway",
    "penalty": "penalty",
    "stoppage": "stoppage",
}

_TIME_RE = re.compile(r"(\d+):(\d+)")
REGULATION_PERIODS = 3
PERIOD_SECONDS = 1200.0
OT_SECONDS = 300.0
PP_SECONDS = 120.0
#: League-typical shootout conversion when the capture has no shootout.
DEFAULT_SHOOTOUT_CONVERT = 0.32
#: Goal-odds multiplier on the power play when PP segments are too sparse
#: to key their own PMF (fitted global fallback).
DEFAULT_PP_GOAL_BOOST = 2.0


def _time_seconds(display: str) -> float:
    match = _TIME_RE.search(display or "")
    if not match:
        return 0.0
    return float(match.group(1)) * 60.0 + float(match.group(2))


def _strength(situation: str, owner_is_home: bool) -> str:
    """ev/pp/sh from the 4-digit situation code (away goalie, away skaters,
    home skaters, home goalie), owner-relative."""
    if not situation or len(situation) != 4 or not situation.isdigit():
        return "ev"
    away_skaters, home_skaters = int(situation[1]), int(situation[2])
    own = home_skaters if owner_is_home else away_skaters
    opp = away_skaters if owner_is_home else home_skaters
    if own > opp:
        return "pp"
    if own < opp:
        return "sh"
    return "ev"


def events_from_nhl_pbp(payload: Dict[str, Any]) -> pl.DataFrame:
    """Classify a real api-web play-by-play payload into stream events.

    Args:
        payload: ``GET /v1/gamecenter/{id}/play-by-play`` dict (top-level
            ``plays`` + ``homeTeam``/``awayTeam``).

    Returns:
        One row per retained play: ``period``, ``time_seconds`` (elapsed in
        period), ``event`` (one of :data:`NHL_EVENTS`), ``is_home`` (event
        owner), ``strength`` (owner-relative ev/pp/sh), ``goal_total``
        (cumulative, goal rows only — the conservation column).

    Raises:
        ValueError: When the payload has no plays.

    Example:
        Quick start::

            import json
            from sportsdataverse.nhl.nhl_game_sim import events_from_nhl_pbp
            events = events_from_nhl_pbp(json.load(open("pbp.json")))
    """
    plays = payload.get("plays") or []
    if not plays:
        raise ValueError("payload has no plays[]")
    home_id = int((payload.get("homeTeam") or {}).get("id") or 0)
    rows: List[Dict[str, Any]] = []
    goal_total = 0
    for play in plays:
        event = _TYPE_MAP.get(str(play.get("typeDescKey") or ""))
        if event is None:
            continue
        details = play.get("details") or {}
        owner = details.get("eventOwnerTeamId")
        is_home = bool(owner is not None and int(owner) == home_id)
        if event == "goal":
            home_score = details.get("homeScore")
            away_score = details.get("awayScore")
            if home_score is not None and away_score is not None:
                goal_total = int(home_score) + int(away_score)
            else:
                goal_total += 1
        rows.append(
            {
                "period": int((play.get("periodDescriptor") or {}).get("number") or 0),
                "time_seconds": _time_seconds(str(play.get("timeInPeriod") or "")),
                "event": event,
                "is_home": is_home,
                "strength": _strength(str(play.get("situationCode") or ""), is_home),
                "goal_total": goal_total if event == "goal" else None,
            }
        )
    schema = {
        "period": pl.Int64,
        "time_seconds": pl.Float64,
        "event": pl.Utf8,
        "is_home": pl.Boolean,
        "strength": pl.Utf8,
        "goal_total": pl.Int64,
    }
    return pl.DataFrame(rows, schema=schema)


def _score_bucket(diff: int) -> str:
    if diff <= -2:
        return "down2"
    if diff == -1:
        return "down1"
    if diff == 0:
        return "tied"
    if diff == 1:
        return "up1"
    return "up2"


def nhl_key(period: int, strength: str, home_diff: int) -> str:
    """Shelf key: period (OT folded to 3) + strength + home score bucket."""
    return f"p{min(period, 3)}|{strength}|{_score_bucket(home_diff)}"


@dataclasses.dataclass
class NhlShelf:
    """Event PMFs + node parameters for the NHL engine.

    Attributes:
        event_pmfs: ``{nhl_key: {event: prob}}``.
        all_pmf: Global fallback event PMF.
        home_share: Per-event probability the home side owns it.
        seconds_per_event: Mean clock burn between stream events.
        pp_goal_boost: Goal-probability multiplier while a PP is live
            (used when the PP segments were too sparse to key their own
            PMF).
        shootout_convert: Per-round shootout conversion probability.
        meta: Provenance.
    """

    event_pmfs: Dict[str, Dict[str, float]]
    all_pmf: Dict[str, float]
    home_share: Dict[str, float]
    seconds_per_event: float
    pp_goal_boost: float
    shootout_convert: float
    meta: Dict[str, Any] = dataclasses.field(default_factory=dict)
    _hits: int = dataclasses.field(default=0, repr=False)
    _fallbacks: int = dataclasses.field(default=0, repr=False)

    def get_pmf(self, key: str) -> Dict[str, float]:
        """Event PMF for a key (global fallback, counted)."""
        pmf = self.event_pmfs.get(key)
        if pmf is None:
            self._fallbacks += 1
            return self.all_pmf
        self._hits += 1
        return pmf

    def fallback_rate(self) -> float:
        """Fraction of lookups served by the global fallback."""
        total = self._hits + self._fallbacks
        return self._fallbacks / total if total else 0.0


def build_nhl_shelf(events: pl.DataFrame) -> NhlShelf:
    """Build the NHL shelf from classified real stream events.

    Args:
        events: Output of :func:`events_from_nhl_pbp` (one or more games).

    Returns:
        The populated :class:`NhlShelf`.

    Raises:
        ValueError: On an empty frame.
    """
    if events.height == 0:
        raise ValueError("no stream events — cannot build an NHL shelf")

    def _pmf(frame: pl.DataFrame) -> Dict[str, float]:
        counts = frame.group_by("event").agg(pl.len().alias("n"))
        total = int(counts["n"].sum())
        by = {r["event"]: r["n"] / total for r in counts.to_dicts()}
        return {e: float(by.get(e, 0.0)) for e in NHL_EVENTS}

    # key on the HOME-perspective score bucket derived from the running goal
    # count (goal rows carry it; forward-fill between goals)
    filled = events.with_columns(pl.col("goal_total").forward_fill().fill_null(0))
    # score diff needs home/away split of goals: rebuild from goal rows
    home_goals = away_goals = 0
    diffs: List[int] = []
    for row in events.to_dicts():
        if row["event"] == "goal":
            if row["is_home"]:
                home_goals += 1
            else:
                away_goals += 1
        diffs.append(home_goals - away_goals)
    keyed = filled.with_columns(pl.Series("home_diff", diffs, dtype=pl.Int64)).with_columns(
        pl.struct(["period", "strength", "home_diff"])
        .map_elements(
            lambda s: nhl_key(s["period"], s["strength"], s["home_diff"]),
            return_dtype=pl.Utf8,
        )
        .alias("key")
    )
    event_pmfs = {str(k[0]): _pmf(g) for k, g in keyed.group_by("key", maintain_order=True)}
    all_pmf = _pmf(events)

    home_share = {e: float(events.filter(pl.col("event") == e)["is_home"].mean() or 0.5) for e in NHL_EVENTS}

    n_games = int(events["period"].n_unique() and 1)
    reg_events = events.filter(pl.col("period") <= REGULATION_PERIODS).height
    seconds_per_event = (REGULATION_PERIODS * PERIOD_SECONDS) / max(1, reg_events)

    # fitted PP boost: goal share on the PP vs even strength (sparse-guarded)
    pp = events.filter(pl.col("strength") == "pp")
    ev = events.filter(pl.col("strength") == "ev")
    pp_goal_boost = DEFAULT_PP_GOAL_BOOST
    if pp.height >= 30 and ev.height:
        pp_rate = float((pp["event"] == "goal").mean())
        ev_rate = float((ev["event"] == "goal").mean())
        if ev_rate > 0 and pp_rate > 0:
            pp_goal_boost = min(5.0, pp_rate / ev_rate)

    return NhlShelf(
        event_pmfs=event_pmfs,
        all_pmf=all_pmf,
        home_share=home_share,
        seconds_per_event=seconds_per_event,
        pp_goal_boost=pp_goal_boost,
        shootout_convert=DEFAULT_SHOOTOUT_CONVERT,
        meta={"n_events": events.height, "n_keys": len(event_pmfs), "n_games": n_games},
    )


class EventNode:
    """Stream-event draw from the keyed PMF (with a live-PP goal boost)."""

    def sample(
        self,
        shelf: NhlShelf,
        key: str,
        rng: np.random.Generator,
        pp_live: bool = False,
    ) -> str:
        pmf = dict(shelf.get_pmf(key))
        if pp_live:
            pmf["goal"] = pmf.get("goal", 0.0) * shelf.pp_goal_boost
        names = list(NHL_EVENTS)
        probs = np.array([pmf[e] for e in names], dtype=float)
        total = probs.sum()
        probs = probs / total if total > 0 else np.full(len(names), 1.0 / len(names))
        idx = int(np.searchsorted(np.cumsum(probs), rng.random()))
        return names[min(idx, len(names) - 1)]


class OwnerNode:
    """Which side owns the event — fitted per-type home share."""

    def sample(self, shelf: NhlShelf, event: str, rng: np.random.Generator) -> bool:
        return bool(rng.random() < shelf.home_share.get(event, 0.5))


class ShootoutNode:
    """Best-of-3-then-sudden-death shootout; returns True when home wins."""

    def sample(self, shelf: NhlShelf, rng: np.random.Generator) -> bool:
        p = shelf.shootout_convert
        home = away = 0
        for _ in range(3):
            home += int(rng.random() < p)
            away += int(rng.random() < p)
        while home == away:
            home += int(rng.random() < p)
            away += int(rng.random() < p)
        return home > away


@dataclasses.dataclass
class NhlState:
    """Mid-game state for the NHL walk."""

    score_home: int = 0
    score_away: int = 0
    period: int = 1
    clock_seconds: float = PERIOD_SECONDS
    pp_for_home: Optional[float] = None  # seconds remaining on a home PP
    pp_for_away: Optional[float] = None


def simulate_nhl_game_pbp(
    shelf: NhlShelf,
    rng: np.random.Generator,
) -> Tuple[NhlState, List[Dict[str, Any]]]:
    """Simulate one full NHL game, emitting the event-stream pbp log.

    Args:
        shelf: The NHL shelf.
        rng: Numpy generator.

    Returns:
        ``(final_state, pbp)`` — one row per stream event with period,
        clock, event, owner, live strength, and the running score. Tied
        regulation runs a sudden-death OT; a still-tied OT resolves by
        shootout (one goal awarded to the winner). Finals never tie.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nhl.nhl_game_sim import simulate_nhl_game_pbp
            final, pbp = simulate_nhl_game_pbp(shelf, np.random.default_rng(7))
    """
    event_node = EventNode()
    owner_node = OwnerNode()
    shootout = ShootoutNode()
    st = NhlState()
    pbp: List[Dict[str, Any]] = []

    def _strength_now(is_home: bool) -> str:
        if st.pp_for_home is not None:
            return "pp" if is_home else "sh"
        if st.pp_for_away is not None:
            return "pp" if not is_home else "sh"
        return "ev"

    while True:
        while st.clock_seconds > 0:
            pp_live = st.pp_for_home is not None or st.pp_for_away is not None
            key = nhl_key(st.period, "pp" if pp_live else "ev", st.score_home - st.score_away)
            event = event_node.sample(shelf, key, rng, pp_live=pp_live)
            is_home = owner_node.sample(shelf, event, rng)
            if event == "goal":
                if is_home:
                    st.score_home += 1
                    if st.pp_for_home is not None:
                        st.pp_for_home = None  # minor expires on the PP goal
                else:
                    st.score_away += 1
                    if st.pp_for_away is not None:
                        st.pp_for_away = None
            elif event == "penalty":
                if is_home:
                    st.pp_for_away = PP_SECONDS
                else:
                    st.pp_for_home = PP_SECONDS
            burn = float(np.clip(rng.uniform(0.6, 1.4) * shelf.seconds_per_event, 2.0, 60.0))
            st.clock_seconds -= burn
            for side in ("pp_for_home", "pp_for_away"):
                remaining = getattr(st, side)
                if remaining is not None:
                    remaining -= burn
                    setattr(st, side, remaining if remaining > 0 else None)
            pbp.append(
                {
                    "period": st.period,
                    "clock_seconds": round(max(0.0, st.clock_seconds), 0),
                    "event": event,
                    "is_home": is_home,
                    "strength": _strength_now(is_home),
                    "score_home": st.score_home,
                    "score_away": st.score_away,
                }
            )
            if st.period > REGULATION_PERIODS and st.score_home != st.score_away:
                return st, pbp  # sudden death
        if st.period >= REGULATION_PERIODS:
            if st.score_home != st.score_away:
                return st, pbp
            if st.period >= REGULATION_PERIODS + 1:
                # shootout: the deciding goal goes on the board
                if shootout.sample(shelf, rng):
                    st.score_home += 1
                else:
                    st.score_away += 1
                pbp.append(
                    {
                        "period": st.period + 1,
                        "clock_seconds": 0.0,
                        "event": "shootout",
                        "is_home": st.score_home > st.score_away,
                        "strength": "so",
                        "score_home": st.score_home,
                        "score_away": st.score_away,
                    }
                )
                return st, pbp
            st.period += 1
            st.clock_seconds = OT_SECONDS
            continue
        st.period += 1
        st.clock_seconds = PERIOD_SECONDS


def simulate_nhl_ensemble(
    shelf: NhlShelf,
    *,
    n_sim: int = 500,
    seed: Optional[int] = None,
) -> Dict[str, Any]:
    """Monte Carlo ensemble of NHL games.

    Args:
        shelf: The NHL shelf.
        n_sim: Number of simulated games.
        seed: RNG seed (same seed = identical ensemble).

    Returns:
        Dict with ``score_home`` / ``score_away`` / ``total`` / ``margin``
        vectors, ``win_prob_home``, ``mean_total``, per-event
        ``event_counts``, and ``n_sim``.

    Example:
        Total-goals market::

            from sportsdataverse.nhl.nhl_game_sim import simulate_nhl_ensemble
            from sportsdataverse.odds.odds_math import prob_over
            ens = simulate_nhl_ensemble(shelf, n_sim=1000, seed=7)
            p = prob_over(ens["total"], 5.5)
    """
    rng = np.random.default_rng(seed)
    home: np.ndarray = np.empty(n_sim, dtype=np.int64)
    away: np.ndarray = np.empty(n_sim, dtype=np.int64)
    event_arrays: Dict[str, np.ndarray] = {e: np.zeros(n_sim, dtype=np.int64) for e in NHL_EVENTS}
    for i in range(n_sim):
        final, pbp = simulate_nhl_game_pbp(shelf, rng)
        home[i] = final.score_home
        away[i] = final.score_away
        for row in pbp:
            if row["event"] in event_arrays:
                event_arrays[row["event"]][i] += 1
    margin = home - away
    return {
        "score_home": home,
        "score_away": away,
        "total": home + away,
        "margin": margin,
        "win_prob_home": float((margin > 0).mean()),
        "mean_total": float((home + away).mean()),
        "event_counts": event_arrays,
        "n_sim": n_sim,
    }


_PLAYER_ID_KEYS = {
    "shot_on_goal": "shootingPlayerId",
    "missed_shot": "shootingPlayerId",
    "blocked_shot": "shootingPlayerId",
    "goal": "scoringPlayerId",
    "hit": "hittingPlayerId",
    "faceoff": "winningPlayerId",
    "giveaway": "playerId",
    "takeaway": "playerId",
    "penalty": "committedByPlayerId",
}

_NHL_TEXT = {
    "faceoff": "{p} won faceoff",
    "hit": "{p} credited with hit",
    "shot_on_goal": "{p} wrist shot on goal saved",
    "missed_shot": "{p} shot missed wide",
    "blocked_shot": "{p} blocked shot",
    "giveaway": "{p} giveaway in defensive zone",
    "takeaway": "{p} takeaway in neutral zone",
    "penalty": "{p} 2 minute minor penalty",
    "stoppage": "Play stopped",
}


def nhl_names_from_pbp(payload: Dict[str, Any]) -> Dict[int, str]:
    """``{player_id: "F. Last"}`` from a payload's roster spots.

    Args:
        payload: api-web play-by-play dict with ``rosterSpots``.

    Returns:
        The name map.
    """
    names: Dict[int, str] = {}
    for spot in payload.get("rosterSpots") or []:
        pid = spot.get("playerId")
        if pid is None:
            continue
        first = str((spot.get("firstName") or {}).get("default") or "")
        last = str((spot.get("lastName") or {}).get("default") or "")
        initial = f"{first[0]}. " if first else ""
        names[int(pid)] = f"{initial}{last}".strip()
    return names


def nhl_event_shares_from_pbp(payload: Dict[str, Any]) -> Dict[Tuple[bool, str], List[Tuple[int, float]]]:
    """Per-side, per-event player shares fitted from the real stream.

    Args:
        payload: api-web play-by-play dict.

    Returns:
        ``{(is_home, event): [(player_id, share), ...]}`` — who takes the
        shots / hits / faceoff wins / penalties for each side, plus the
        ``"assist"`` and ``"shot_blocked_by"`` credit pools.
    """
    home_id = int((payload.get("homeTeam") or {}).get("id") or 0)
    counts: Dict[Tuple[bool, str], Dict[int, int]] = {}
    for play in payload.get("plays") or []:
        event = _TYPE_MAP.get(str(play.get("typeDescKey") or ""))
        key_name = _PLAYER_ID_KEYS.get(event or "")
        if event is None or key_name is None:
            continue
        details = play.get("details") or {}
        pid = details.get(key_name)
        owner = details.get("eventOwnerTeamId")
        if pid is None or owner is None:
            continue
        bucket = counts.setdefault((int(owner) == home_id, event), {})
        bucket[int(pid)] = bucket.get(int(pid), 0) + 1
        if event == "blocked_shot" and details.get("blockingPlayerId") is not None:
            # the blocker belongs to the NON-owner side (owner = shooter team)
            blocker_bucket = counts.setdefault((int(owner) != home_id, "shot_blocked_by"), {})
            blocker = int(details["blockingPlayerId"])
            blocker_bucket[blocker] = blocker_bucket.get(blocker, 0) + 1
        if event == "goal":
            for assist_key in ("assist1PlayerId", "assist2PlayerId"):
                aid = details.get(assist_key)
                if aid is not None:
                    assist_bucket = counts.setdefault((int(owner) == home_id, "assist"), {})
                    assist_bucket[int(aid)] = assist_bucket.get(int(aid), 0) + 1
    shares: Dict[Tuple[bool, str], List[Tuple[int, float]]] = {}
    for key, bucket in counts.items():
        total = sum(bucket.values())
        shares[key] = [(pid, n / total) for pid, n in sorted(bucket.items(), key=lambda kv: -kv[1])]
    return shares


def render_nhl_game_pbp(
    shelf: NhlShelf,
    shares: Dict[Tuple[bool, str], List[Tuple[int, float]]],
    names: Dict[int, str],
    rng: np.random.Generator,
    context: Optional[Dict[str, Any]] = None,
    *,
    provider: str = "nhl_rtss",
) -> Tuple[NhlState, List[Dict[str, Any]]]:
    """Simulate one game and render RTSS-style event report lines.

    The NHL's canonical text is the RTSS play-by-play report format:
    ``"FLA ONGOAL - #19 TKACHUK, Wrist, Off. Zone, 15 ft."``,
    ``"FLA won Neu. Zone - FLA #16 BARKOV vs EDM #97 MCDAVID"``, penalty
    lines with the real infraction vocabulary, and stoppage reasons —
    all fitted from the real stream (sweater numbers from rosterSpots,
    penalty/stoppage pools from the payload's own distributions).

    Args:
        shelf: The NHL shelf.
        shares: Fitted per-side event shares (:func:`nhl_event_shares_from_pbp`).
        names: Roster name map (:func:`nhl_names_from_pbp`).
        rng: Numpy generator.
        context: RTSS context (:func:`nhl_render_context_from_pbp`);
            defaults to plain surnames and generic pools when omitted.
        provider: Template provider (default ``"nhl_rtss"``; register
            alternatives in ``sportsdataverse._common.play_text``).

    Returns:
        ``(final_state, rows)`` with ``player_id`` / ``player_name`` /
        ``text`` added per event, plus the report's status rows —
        ``period_start`` / ``period_end`` / ``game_end`` wall-clock lines
        and ``delayed_penalty`` flags before penalties — and fitted
        ``"Assists: #16 BARKOV(5); #5 EKBLAD(3)"`` suffixes on goals,
        all gated on the provider's template keys.

    Example:
        Quick start::

            context = nhl_render_context_from_pbp(payload)
            final, rows = render_nhl_game_pbp(shelf, shares, names, rng, context)
            print(rows[0]["text"])
    """
    tpl = get_templates("hockey", provider)
    final, rows = simulate_nhl_game_pbp(shelf, rng)
    context = context or {}
    numbers: Dict[int, int] = context.get("numbers", {})
    abbr_home, abbr_away = context.get("abbr", ("HOME", "AWAY"))
    loser_pools = context.get("losers", {})
    penalty_pool = context.get("penalties", [("tripping", 1.0)])
    stop_pool = context.get("stop_reasons", [("puck-frozen", 1.0)])
    delayed_rate = float(context.get("delayed_rate", 0.0))
    goal_counts: Dict[int, int] = {}
    assist_counts: Dict[int, int] = {}
    out: List[Dict[str, Any]] = []
    wall_minutes = 8 * 60 + 3  # synthetic 8:03 local puck drop behind the report rows
    prev_period: Optional[int] = None
    prev_row: Dict[str, Any] = {}

    def _pick(pool: List[Tuple[Any, float]]) -> Any:
        if not pool:
            return None
        probs = np.array([share for _, share in pool], dtype=float)
        probs = probs / probs.sum()
        idx = int(np.searchsorted(np.cumsum(probs), rng.random()))
        return pool[min(idx, len(pool) - 1)][0]

    def _tag(pid: int) -> str:
        last = names.get(pid, "PLAYER").split(". ")[-1].upper()
        number = numbers.get(pid)
        return f"#{number} {last}" if number is not None else last

    def _wall(minutes: int) -> str:
        hour = (minutes // 60) % 12 or 12
        return f"{hour}:{minutes % 60:02d}"

    def _status(event: str, ref: Dict[str, Any], text: str) -> None:
        out.append(
            {
                "period": int(ref["period"]),
                "clock_seconds": float(ref["clock_seconds"]),
                "event": event,
                "is_home": bool(ref["is_home"]),
                "strength": "ev",
                "score_home": int(ref["score_home"]),
                "score_away": int(ref["score_away"]),
                "player_id": 0,
                "player_name": "",
                "text": text,
            }
        )

    for row in rows:
        period = int(row["period"])
        if period != prev_period:
            if prev_period is not None and "period_end" in tpl:
                wall_minutes += 38 + int(rng.integers(0, 5))
                _status("period_end", prev_row, tpl["period_end"].format(wall_time=_wall(wall_minutes)))
            if "period_start" in tpl and str(row["event"]) != "shootout":
                wall_minutes += int(rng.integers(0, 4)) if prev_period is None else 2 + int(rng.integers(0, 4))
                _status("period_start", row, tpl["period_start"].format(wall_time=_wall(wall_minutes)))
            prev_period = period
        event = str(row["event"])
        is_home = bool(row["is_home"])
        abbr = abbr_home if is_home else abbr_away
        opp_abbr = abbr_away if is_home else abbr_home
        if event == "penalty" and "delayed_penalty" in tpl and rng.random() < delayed_rate:
            # the referee's arm goes up first — rate fitted by the context builder
            _status("delayed_penalty", row, tpl["delayed_penalty"].format(abbr=abbr))
        pool = shares.get((is_home, event)) or shares.get((not is_home, event)) or []
        pid = int(_pick(pool) or 0) if event not in ("stoppage", "shootout") else 0
        distance = int(rng.integers(8, 46))
        ctx = {
            "abbr": abbr,
            "opp_abbr": opp_abbr,
            "home_abbr": abbr_home,
            "away_abbr": abbr_away,
            "player": _tag(pid),
            "distance": distance,
        }
        if event == "goal":
            goal_counts[pid] = goal_counts.get(pid, 0) + 1
            text = tpl["goal"].format(count=goal_counts[pid], **ctx)
            if "assists_suffix" in tpl:
                assist_pool = [(a, share) for a, share in shares.get((is_home, "assist"), []) if int(a) != pid]
                # ponytail: 0/1/2 assists at the NHL-wide ~15/35/50 split
                n_assists = int(rng.choice([0, 1, 2], p=[0.15, 0.35, 0.50])) if assist_pool else 0
                picked: List[int] = []
                while len(picked) < n_assists and assist_pool:
                    candidate = int(_pick(assist_pool) or 0)
                    if candidate:
                        picked.append(candidate)
                    assist_pool = [(a, share) for a, share in assist_pool if int(a) != candidate]
                if picked:
                    items = []
                    for assister in picked:
                        assist_counts[assister] = assist_counts.get(assister, 0) + 1
                        items.append(tpl["assist_item"].format(player=_tag(assister), count=assist_counts[assister]))
                    text += tpl["assists_suffix"].format(assists="; ".join(items))
        elif event == "blocked_shot":
            blocker = int(_pick(shares.get((not is_home, "shot_blocked_by"), [])) or 0)
            text = tpl["blocked_shot"].format(blocker=_tag(blocker), **ctx)
        elif event == "hit":
            victim = int(_pick(shares.get((not is_home, "giveaway"), [])) or 0)
            text = tpl["hit"].format(victim=_tag(victim), **ctx)
        elif event == "faceoff":
            loser = int(_pick(loser_pools.get(not is_home, [])) or 0)
            text = tpl["faceoff"].format(
                zone=("Neu", "Off", "Def")[int(rng.integers(0, 3))],
                home_player=_tag(pid) if is_home else _tag(loser),
                away_player=_tag(loser) if is_home else _tag(pid),
                **ctx,
            )
        elif event == "penalty":
            infraction = str(_pick(penalty_pool)).replace("-", " ").title()
            text = tpl["penalty"].format(infraction=infraction, **ctx)
        elif event == "stoppage":
            text = tpl["stoppage"].format(reason=str(_pick(stop_pool)).replace("-", " ").upper())
        else:
            text = tpl[event].format(**ctx)
        row["player_id"] = pid
        row["player_name"] = names.get(pid, "") if pid else ""
        row["text"] = text
        out.append(row)
        prev_row = row
    if rows:
        if "period_end" in tpl:
            wall_minutes += 38 + int(rng.integers(0, 5))
            _status("period_end", rows[-1], tpl["period_end"].format(wall_time=_wall(wall_minutes)))
        if "game_end" in tpl:
            wall_minutes += 1 + int(rng.integers(0, 3))
            _status("game_end", rows[-1], tpl["game_end"].format(wall_time=_wall(wall_minutes)))
    return final, out


def nhl_render_context_from_pbp(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Everything the RTSS renderer needs, fitted from the real payload.

    Args:
        payload: api-web play-by-play dict.

    Returns:
        Dict with ``numbers`` (sweater numbers), ``abbr`` (home, away),
        ``losers`` (faceoff-loss shares per side), ``penalties`` (descKey
        share pool), ``stop_reasons`` (stoppage reason share pool), and
        ``delayed_rate`` (delayed-penalty flags per penalty).
    """
    numbers: Dict[int, int] = {}
    for spot in payload.get("rosterSpots") or []:
        pid = spot.get("playerId")
        if pid is not None and spot.get("sweaterNumber") is not None:
            numbers[int(pid)] = int(spot["sweaterNumber"])
    home_id = int((payload.get("homeTeam") or {}).get("id") or 0)
    losers: Dict[bool, Dict[int, int]] = {True: {}, False: {}}
    delayed = 0
    penalties: Dict[str, int] = {}
    stop_reasons: Dict[str, int] = {}
    for play in payload.get("plays") or []:
        kind = str(play.get("typeDescKey") or "")
        details = play.get("details") or {}
        if kind == "faceoff" and details.get("losingPlayerId") is not None:
            owner_home = int(details.get("eventOwnerTeamId") or 0) == home_id
            bucket = losers[not owner_home]
            pid = int(details["losingPlayerId"])
            bucket[pid] = bucket.get(pid, 0) + 1
        elif kind == "delayed-penalty":
            delayed += 1
        elif kind == "penalty" and details.get("descKey"):
            penalties[str(details["descKey"])] = penalties.get(str(details["descKey"]), 0) + 1
        elif kind == "stoppage" and details.get("reason"):
            stop_reasons[str(details["reason"])] = stop_reasons.get(str(details["reason"]), 0) + 1

    def _share(bucket: Dict[Any, int]) -> List[Tuple[Any, float]]:
        total = sum(bucket.values())
        return [(k, n / total) for k, n in sorted(bucket.items(), key=lambda kv: -kv[1])] if total else []

    return {
        "numbers": numbers,
        "abbr": (
            str((payload.get("homeTeam") or {}).get("abbrev") or "HOME"),
            str((payload.get("awayTeam") or {}).get("abbrev") or "AWAY"),
        ),
        "losers": {side: _share(bucket) for side, bucket in losers.items()},
        "penalties": _share(penalties) or [("tripping", 1.0)],
        "stop_reasons": _share(stop_reasons) or [("puck-frozen", 1.0)],
        "delayed_rate": delayed / max(1, sum(penalties.values())),
    }
