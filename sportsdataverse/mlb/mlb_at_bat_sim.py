"""MLB at-bat-level Monte Carlo game simulation (WS4, baseball family).

The reference baseball keystone: a multiclass at-bat-outcome PMF walked through
an inning → at-bat → base-out state machine. Real statsapi ``playByPlay``
payloads are classified into a 9-class outcome vocabulary (conservation
gate: per-play score deltas reconstruct the real final), the PMF is built
from those real at-bats, and the engine simulates half-innings with a
deterministic bases-advance model, the extra-innings ghost runner, and
walk-off endings.

v1 scope (documented ceilings): a global outcome PMF (no batter identity /
base-out conditioning yet), hit advancement is exactly the hit's bases
(singles slightly under-score), GIDP counts one out, and bullpen state is
out of scope until multi-game PMFs land. Each is a seam, not a rewrite.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import polars as pl

from sportsdataverse._common.play_text import get_templates

#: At-bat outcome vocabulary (the OutcomePMF classes).
AB_OUTCOMES: Tuple[str, ...] = (
    "out_inplay",
    "so",
    "bb",
    "single",
    "double",
    "triple",
    "hr",
    "reach_other",
    "other_out",
    "gidp",
    "sac_fly",
)

_EVENT_MAP = {
    "strikeout": "so",
    "strikeout_double_play": "so",
    "walk": "bb",
    "intent_walk": "bb",
    "hit_by_pitch": "bb",
    "single": "single",
    "double": "double",
    "triple": "triple",
    "home_run": "hr",
    "grounded_into_double_play": "gidp",
    "double_play": "gidp",
    "sac_fly": "sac_fly",
    "sac_fly_double_play": "gidp",
    "field_error": "reach_other",
    "catcher_interf": "reach_other",
    "fielders_choice": "reach_other",
    "caught_stealing_2b": "other_out",
    "caught_stealing_3b": "other_out",
    "caught_stealing_home": "other_out",
    "pickoff_1b": "other_out",
    "pickoff_2b": "other_out",
    "pickoff_3b": "other_out",
    "other_out": "other_out",
}
#: How many bases the batter (and every runner) advances per outcome.
_ADVANCE = {"single": 1, "double": 2, "triple": 3, "hr": 4, "bb": 1, "reach_other": 1}


def at_bats_from_pbp(payload: Dict[str, Any]) -> pl.DataFrame:
    """Classify a real statsapi ``playByPlay`` payload into at-bat outcomes.

    Args:
        payload: A ``GET .../playByPlay`` dict (top-level ``allPlays``) or a
            full ``feed/live`` dict (``liveData.plays.allPlays``).

    Returns:
        One row per play: ``inning``, ``is_top``, ``outs_before``,
        ``outcome`` (one of :data:`AB_OUTCOMES`), ``event_type``,
        ``runs_scored`` (score delta on the play), ``away_score`` /
        ``home_score`` (post-play cumulative).

    Example:
        Quick start::

            import json
            from sportsdataverse.mlb.mlb_at_bat_sim import at_bats_from_pbp
            plays = at_bats_from_pbp(json.load(open("play_by_play.json")))
            print(plays.group_by("outcome").agg(pl.len()))
    """
    plays = payload.get("allPlays")
    if plays is None:
        plays = payload.get("liveData", {}).get("plays", {}).get("allPlays", [])
    rows: List[Dict[str, Any]] = []
    prev_away = prev_home = 0
    for play in plays:
        result = play.get("result", {})
        about = play.get("about", {})
        event_type = str(result.get("eventType") or "")
        outcome = _EVENT_MAP.get(event_type)
        if outcome is None:
            outcome = "out_inplay" if result.get("isOut") else "reach_other"
        away = int(result.get("awayScore") or prev_away)
        home = int(result.get("homeScore") or prev_home)
        outs_after = int(play.get("count", {}).get("outs") or 0)
        rows.append(
            {
                "inning": int(about.get("inning") or 0),
                "is_top": bool(about.get("isTopInning", True)),
                "outs_before": max(0, outs_after - (1 if result.get("isOut") else 0)),
                "outcome": outcome,
                "event_type": event_type,
                "runs_scored": (away - prev_away) + (home - prev_home),
                "away_score": away,
                "home_score": home,
            }
        )
        prev_away, prev_home = away, home
    schema = {
        "inning": pl.Int64,
        "is_top": pl.Boolean,
        "outs_before": pl.Int64,
        "outcome": pl.Utf8,
        "event_type": pl.Utf8,
        "runs_scored": pl.Int64,
        "away_score": pl.Int64,
        "home_score": pl.Int64,
    }
    return pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)


@dataclasses.dataclass(frozen=True)
class AtBatPMF:
    """The multiclass at-bat outcome distribution (built from real plays).

    Attributes:
        probs: Probability per :data:`AB_OUTCOMES` class.
        meta: Provenance (play count, games).
    """

    probs: Dict[str, float]
    meta: Dict[str, Any] = dataclasses.field(default_factory=dict)


def build_at_bat_pmf(at_bats: pl.DataFrame) -> AtBatPMF:
    """Build the outcome PMF from classified real at-bats.

    Args:
        at_bats: Output of :func:`at_bats_from_pbp` (one or more games).

    Returns:
        The :class:`AtBatPMF`.

    Raises:
        ValueError: On an empty frame.
    """
    if at_bats.height == 0:
        raise ValueError("no at-bats — cannot build a PMF")
    counts = at_bats.group_by("outcome").agg(pl.len().alias("n"))
    total = int(counts["n"].sum())
    by = {row["outcome"]: row["n"] / total for row in counts.to_dicts()}
    return AtBatPMF(
        probs={o: float(by.get(o, 0.0)) for o in AB_OUTCOMES},
        meta={"n_plays": total},
    )


def _half_inning(
    pmf: AtBatPMF,
    rng: np.random.Generator,
    *,
    ghost_runner: bool = False,
    stop_if_leads: Optional[Tuple[int, int]] = None,
    pbp_sink: Optional[List[Dict[str, Any]]] = None,
    event_sink: Optional[Dict[str, int]] = None,
    inning: int = 0,
    is_top: bool = True,
) -> int:
    """Simulate one half-inning; returns runs scored.

    Args:
        pmf: The outcome PMF.
        rng: Numpy generator.
        ghost_runner: Start with a runner on second (extras rule).
        stop_if_leads: ``(own_runs_so_far, opponent_total)`` — walk-off mode:
            stop the half as soon as own cumulative total exceeds the
            opponent's.
    """
    probs = np.array([pmf.probs[o] for o in AB_OUTCOMES], dtype=float)
    probs = probs / probs.sum()
    cumulative: np.ndarray = np.cumsum(probs)
    outs = 0
    runs = 0
    bases = [False, False, ghost_runner]  # 1B, 2B, 3B
    while outs < 3:
        outcome = AB_OUTCOMES[min(int(np.searchsorted(cumulative, rng.random())), len(AB_OUTCOMES) - 1)]
        runs_before = runs
        outs_before = outs
        if outcome in ("so", "out_inplay", "other_out"):
            outs += 1
        elif outcome == "gidp":
            # batter + the lead force runner; a bases-empty "GIDP" draw is
            # just a ground out
            if bases[0]:
                outs += 2
                bases[0] = False
            else:
                outs += 1
        elif outcome == "sac_fly":
            outs += 1
            if outs < 3 and bases[2]:
                runs += 1
                bases[2] = False
        elif outcome == "bb":
            # walk: force advances only
            if bases[0] and bases[1] and bases[2]:
                runs += 1
            elif bases[0] and bases[1]:
                bases[2] = True
            elif bases[0]:
                bases[1] = True
            bases[0] = True
        else:
            advance = _ADVANCE[outcome]
            # ponytail: every runner (and the batter) advances exactly the
            # hit's bases — deterministic v1 model, singles under-score a bit
            new_bases = [False, False, False]
            for i in (2, 1, 0):
                if bases[i]:
                    landing = i + advance
                    if landing >= 3:
                        runs += 1
                    else:
                        new_bases[landing] = True
            batter_landing = advance - 1
            if batter_landing >= 3:
                runs += 1
            else:
                new_bases[batter_landing] = True
            bases = new_bases
        if event_sink is not None:
            event_sink[outcome] = event_sink.get(outcome, 0) + 1
        if pbp_sink is not None:
            pbp_sink.append(
                {
                    "inning": inning,
                    "is_top": is_top,
                    "outcome": outcome,
                    "runs_on_play": runs - runs_before,
                    "outs_after": outs if outs > outs_before or outcome in ("so", "out_inplay", "other_out") else outs,
                }
            )
        if stop_if_leads is not None:
            own, opponent = stop_if_leads
            if own + runs > opponent:
                break  # walk-off
    return runs


def simulate_mlb_game(
    pmf: AtBatPMF,
    rng: np.random.Generator,
    *,
    pbp_sink: Optional[List[Dict[str, Any]]] = None,
    event_sink: Optional[Dict[str, int]] = None,
) -> Tuple[int, int]:
    """Simulate one game (9 innings, ghost-runner extras, walk-offs).

    Args:
        pmf: The outcome PMF.
        rng: Numpy generator.

    Returns:
        ``(away_runs, home_runs)`` — never tied.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.mlb.mlb_at_bat_sim import simulate_mlb_game
            away, home = simulate_mlb_game(pmf, np.random.default_rng(7))
    """
    away = home = 0
    inning = 1
    while True:
        ghost = inning > 9
        away += _half_inning(
            pmf,
            rng,
            ghost_runner=ghost,
            pbp_sink=pbp_sink,
            event_sink=event_sink,
            inning=inning,
            is_top=True,
        )
        # bottom half: skipped in the 9th+ when home already leads
        if inning < 9 or home <= away:
            home += _half_inning(
                pmf,
                rng,
                ghost_runner=ghost,
                stop_if_leads=(home, away) if inning >= 9 else None,
                pbp_sink=pbp_sink,
                event_sink=event_sink,
                inning=inning,
                is_top=False,
            )
        if inning >= 9 and away != home:
            return away, home
        inning += 1


def simulate_mlb_ensemble(
    pmf: AtBatPMF,
    *,
    n_sim: int = 1000,
    seed: Optional[int] = None,
    collect_event_counts: bool = False,
) -> Dict[str, Any]:
    """Monte Carlo ensemble of full games.

    Args:
        pmf: The outcome PMF.
        n_sim: Number of simulated games.
        seed: RNG seed (same seed = identical ensemble).

    Returns:
        Dict with ``away`` / ``home`` / ``total`` / ``margin`` sample
        vectors, ``win_prob_home``, ``mean_total``, ``n_sim`` — same shape
        as the NBA ensemble; price with ``odds_math``.

    Example:
        Total-runs market::

            from sportsdataverse.mlb.mlb_at_bat_sim import simulate_mlb_ensemble
            from sportsdataverse.odds.odds_math import prob_over
            ens = simulate_mlb_ensemble(pmf, n_sim=2000, seed=7)
            p = prob_over(ens["total"], 8.5)
    """
    rng = np.random.default_rng(seed)
    away: np.ndarray = np.empty(n_sim, dtype=np.int64)
    home: np.ndarray = np.empty(n_sim, dtype=np.int64)
    event_arrays: Optional[Dict[str, np.ndarray]] = None
    if collect_event_counts:
        event_arrays = {o: np.zeros(n_sim, dtype=np.int64) for o in AB_OUTCOMES}
    for i in range(n_sim):
        event_sink: Optional[Dict[str, int]] = {} if collect_event_counts else None
        away[i], home[i] = simulate_mlb_game(pmf, rng, event_sink=event_sink)
        if event_arrays is not None and event_sink:
            for outcome_name, count in event_sink.items():
                event_arrays[outcome_name][i] = count
    margin = home - away
    return {
        "away": away,
        "home": home,
        "total": away + home,
        "margin": margin,
        "win_prob_home": float((margin > 0).mean()),
        "mean_total": float((away + home).mean()),
        "n_sim": n_sim,
        "event_counts": event_arrays,
    }


def simulate_mlb_game_pbp(
    pmf: AtBatPMF,
    rng: np.random.Generator,
) -> Tuple[Tuple[int, int], List[Dict[str, Any]]]:
    """Simulate one game AND emit its full at-bat-level pbp log.

    Args:
        pmf: The outcome PMF.
        rng: Numpy generator.

    Returns:
        ``((away, home), pbp)`` where ``pbp`` has one row per at-bat:
        ``inning``, ``is_top``, ``outcome``, ``runs_on_play``, and the
        running ``away_score`` / ``home_score``.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.mlb.mlb_at_bat_sim import simulate_mlb_game_pbp
            (away, home), pbp = simulate_mlb_game_pbp(pmf, np.random.default_rng(7))
            print(len(pbp), pbp[-1])
    """
    rows: List[Dict[str, Any]] = []
    away, home = simulate_mlb_game(pmf, rng, pbp_sink=rows)
    run_away = run_home = 0
    for row in rows:
        if row["is_top"]:
            run_away += int(row["runs_on_play"])
        else:
            run_home += int(row["runs_on_play"])
        row["away_score"] = run_away
        row["home_score"] = run_home
    return (away, home), rows


def lineups_from_pbp(payload: Dict[str, Any]) -> Dict[str, List[Tuple[int, str]]]:
    """Real batting orders + pitcher appearance orders from a playByPlay payload.

    Args:
        payload: statsapi ``playByPlay`` (or ``feed/live``) dict.

    Returns:
        ``{"away": [(batter_id, name), ...], "home": [...],
        "away_pitchers": [...], "home_pitchers": [...]}`` — batters in
        first-plate-appearance order (the real lineup card) and each side's
        pitchers in first-appearance order (starter first, bullpen after).
    """
    plays = payload.get("allPlays")
    if plays is None:
        plays = payload.get("liveData", {}).get("plays", {}).get("allPlays", [])
    lineups: Dict[str, List[Tuple[int, str]]] = {
        "away": [],
        "home": [],
        "away_pitchers": [],
        "home_pitchers": [],
    }
    seen: Dict[str, set] = {key: set() for key in lineups}
    for play in plays:
        matchup = play.get("matchup") or {}
        is_top = (play.get("about") or {}).get("isTopInning", True)
        batting = "away" if is_top else "home"
        pitching = "home_pitchers" if is_top else "away_pitchers"
        for key, blob in ((batting, matchup.get("batter") or {}), (pitching, matchup.get("pitcher") or {})):
            raw = blob.get("id")
            if raw is None:
                continue
            pid = int(raw)
            if pid not in seen[key]:
                seen[key].add(pid)
                lineups[key].append((pid, str(blob.get("fullName") or f"#{pid}")))
    return lineups


def render_mlb_game_pbp(
    pmf: AtBatPMF,
    lineups: Dict[str, List[Tuple[int, str]]],
    rng: np.random.Generator,
    *,
    provider: str = "mlb_statsapi",
) -> Tuple[Tuple[int, int], List[Dict[str, Any]]]:
    """Simulate one game and render statsapi-style play descriptions.

    Batters rotate through the REAL batting orders (persistent across
    half-innings, like the actual lineup card) and the renderer replays the
    at-bat engine's exact base-advancement rules over NAMED runners, so
    scoring plays carry the feed's runner sentences (``"... Smith scores.
    Jones to 3rd."``) that reconcile with ``runs_on_play``. Between plate
    appearances it also emits the feed's non-PA event rows — the pregame
    status advisory, batter timeouts, mound visits, and pitching changes
    walking each side's REAL bullpen appearance order — gated on the
    provider's template keys.

    Args:
        pmf: The outcome PMF.
        lineups: Real batting + pitching orders (:func:`lineups_from_pbp`).
        rng: Numpy generator.
        provider: Template provider (default ``"mlb_statsapi"``, the feed's
            result-description style; register alternatives in
            ``sportsdataverse._common.play_text``).

    Returns:
        ``((away, home), rows)`` — plate-appearance rows (``kind="pa"``,
        the :func:`simulate_mlb_game_pbp` columns plus ``batter_id`` /
        ``batter_name`` / ``text``) interleaved with event rows
        (``kind="event"`` with ``event`` and ``text``).

    Example:
        Quick start::

            (away, home), rows = render_mlb_game_pbp(pmf, lineups, rng)
            print(rows[1]["text"])
    """
    tpl = get_templates("baseball", provider)
    (away, home), rows = simulate_mlb_game_pbp(pmf, rng)
    # batting order = the real starting nine; later first-appearance entries
    # are the bench (they entered the real game as substitutes)
    orders: Dict[str, List[Tuple[int, str]]] = {}
    bench: Dict[str, List[Tuple[int, str]]] = {}
    for side_key in ("away", "home"):
        full = list(lineups.get(side_key) or [(0, "Batter")])
        orders[side_key] = full[:9] if len(full) >= 9 else full
        bench[side_key] = full[9:]
    order_index = {"away": 0, "home": 0}
    hr_counts: Dict[int, int] = {}
    last_batter: Dict[str, Tuple[int, str]] = {}
    out: List[Dict[str, Any]] = []
    bases: List[Optional[str]] = [None, None, None]
    cur_half: Tuple[int, bool] = (0, True)
    pitcher_idx = {"away": 0, "home": 0}

    def _event(name: str, inning: int, is_top: bool, text: str) -> None:
        out.append({"inning": inning, "is_top": is_top, "kind": "event", "event": name, "text": text})

    if "game_advisory" in tpl:
        _event("game_advisory", 1, True, tpl["game_advisory"].format(status="In Progress"))
    for row in rows:
        side = "away" if row["is_top"] else "home"
        lineup = orders[side]
        inning, is_top = int(row["inning"]), bool(row["is_top"])
        if (inning, is_top) != cur_half:
            cur_half = (inning, is_top)
            bases = [None, None, None]
            if inning > 9:
                # the extras ghost runner is the previous batter in the order;
                # the engine seats it at index 2 (3B) — mirror the engine
                bases[2] = lineup[(order_index[side] - 1) % len(lineup)][1]
            pitching = "home" if is_top else "away"
            arms = lineups.get(f"{pitching}_pitchers") or []
            # ponytail: bullpen pacing — a change on ~half the late half-innings
            # walks the real appearance order (the fixture game used 4 arms)
            if "pitching_sub" in tpl and inning >= 6 and pitcher_idx[pitching] + 1 < len(arms) and rng.random() < 0.45:
                outgoing = arms[pitcher_idx[pitching]][1]
                pitcher_idx[pitching] += 1
                _event(
                    "pitching_sub",
                    inning,
                    is_top,
                    tpl["pitching_sub"].format(incoming=arms[pitcher_idx[pitching]][1], outgoing=outgoing),
                )
        if "batter_timeout" in tpl and rng.random() < 0.15:
            # ponytail: 11 batter timeouts in the fixture game (~0.15/PA)
            _event("batter_timeout", inning, is_top, tpl["batter_timeout"])
        if "mound_visit" in tpl and rng.random() < 0.04:
            # ponytail: 3 mound visits in the fixture game
            _event("mound_visit", inning, is_top, tpl["mound_visit"])
        outcome = str(row["outcome"])
        if outcome != "other_out" and "offensive_sub" in tpl and inning >= 7 and bench[side] and rng.random() < 0.05:
            # ponytail: ~1 pinch hitter per game in the fixture (late innings)
            slot = order_index[side] % len(lineup)
            sub_in = bench[side].pop(0)
            sub_out = lineup[slot]
            lineup[slot] = sub_in
            _event(
                "offensive_sub",
                inning,
                is_top,
                tpl["offensive_sub"].format(incoming=sub_in[1], outgoing=sub_out[1]),
            )
        if outcome == "other_out":
            pid, name = last_batter.get(side, lineup[0])
            text = tpl["other_out"].format(runner=name)
        else:
            pid, name = lineup[order_index[side] % len(lineup)]
            order_index[side] += 1
            last_batter[side] = (pid, name)
            entry = tpl[outcome]
            if isinstance(entry, tuple):
                entry = entry[int(rng.integers(0, len(entry)))]
            if outcome == "hr":
                hr_counts[pid] = hr_counts.get(pid, 0) + 1
                text = entry.format(batter=name, count=hr_counts[pid])
            else:
                text = entry.format(batter=name)
        # replay the engine's base-advancement over NAMED runners
        scorers: List[str] = []
        advances: List[Tuple[str, str]] = []
        if outcome == "gidp":
            if bases[0] is not None:
                bases[0] = None
        elif outcome == "sac_fly":
            if int(row["runs_on_play"]) and bases[2] is not None:
                scorers.append(bases[2])
                bases[2] = None
        elif outcome == "bb":
            if bases[0] is not None:
                if bases[1] is not None:
                    if bases[2] is not None:
                        scorers.append(bases[2])
                    advances.append((bases[1], "3rd"))
                    bases[2] = bases[1]
                advances.append((bases[0], "2nd"))
                bases[1] = bases[0]
            bases[0] = name
        elif outcome in _ADVANCE:
            advance = _ADVANCE[outcome]
            new_bases: List[Optional[str]] = [None, None, None]
            for i in (2, 1, 0):
                runner = bases[i]
                if runner is None:
                    continue
                landing = i + advance
                if landing >= 3:
                    scorers.append(runner)
                else:
                    new_bases[landing] = runner
                    advances.append((runner, ("2nd", "3rd")[landing - 1]))
            if advance - 1 < 3:
                new_bases[advance - 1] = name
            bases = new_bases
        for runner in scorers:
            text += tpl["score_suffix"].format(runner=runner)
        for runner, base_label in advances:
            text += tpl["advance_suffix"].format(runner=runner, base=base_label)
        pa_row = dict(row)
        pa_row["kind"] = "pa"
        pa_row["batter_id"] = pid
        pa_row["batter_name"] = name
        pa_row["text"] = text
        out.append(pa_row)
    return (away, home), out
