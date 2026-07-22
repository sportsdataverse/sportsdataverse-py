"""Football drive/play Monte Carlo simulation — NFL core, CFB shim (WS4).

The football family of the reference sim architecture: real ESPN summary
``drives.previous[].plays`` classify into a snap-outcome vocabulary
(conservation gate: per-play score deltas reconstruct the real final), the
node PMFs key on (down, distance bucket, field zone), and the engine walks
a down-distance state machine — play call → yards → first downs → fourth-
down decisions (empirical) → punts / field goals (distance-conditioned
make model) / turnovers → touchdowns with extra points — through quarters,
halftime and an alternating overtime round, emitting the full pbp log.

Nodes and their models:

* :class:`SnapNode` — play-class PMF | (down, distance, zone), empirical
  per key with global fallback (the model upgrade slots in exactly like
  basketball's ``models2shelf``);
* :class:`YardsNode` — per-class empirical yardage distributions sampled
  directly (rush / completion / sack);
* :class:`FourthDownNode` — go / punt / field-goal decision fitted from
  the real 4th-down snaps;
* :class:`FieldGoalNode` — make probability by distance bucket, fitted
  from real attempts with a distance-decay default where unobserved.

v1 ceilings (documented seams): penalties and kick returns are not
simulated as snaps (their real points still count in conservation), the
XP is folded into a 7-point touchdown at the real league make rate, and
one game of PMFs is thin — the fixture provenance README documents how
multi-game shelves tighten the bands.
"""

from __future__ import annotations

import dataclasses
import re
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import polars as pl

from sportsdataverse._common.play_text import get_templates

#: Snap outcome vocabulary.
SNAP_CLASSES: Tuple[str, ...] = (
    "rush",
    "pass_complete",
    "pass_incomplete",
    "sack",
    "interception",
    "fumble_lost",
)

_TYPE_MAP = {
    "Rush": "rush",
    "Rushing Touchdown": "rush",
    "Pass Reception": "pass_complete",
    "Passing Touchdown": "pass_complete",
    "Pass Incompletion": "pass_incomplete",
    "Sack": "sack",
    "Pass Interception Return": "interception",
    "Interception Return Touchdown": "interception",
    "Fumble Recovery (Opponent)": "fumble_lost",
}
_SPECIAL_TYPES = {
    "Penalty": "penalty",
    "Punt": "punt",
    "Field Goal Good": "fg_good",
    "Field Goal Missed": "fg_miss",
    "Blocked Field Goal": "fg_miss",
}

#: Two-point conversion probability in college-OT rounds (league-typical;
#: no conversion attempts exist in the committed fixture to fit from).
TWO_POINT_CONVERT = 0.45

_CLOCK_RE = re.compile(r"(\d+):(\d+)")


def _clock_seconds(display: str) -> float:
    match = _CLOCK_RE.search(display or "")
    if not match:
        return 0.0
    return float(match.group(1)) * 60.0 + float(match.group(2))


def plays_from_espn_drives(summary: Dict[str, Any]) -> pl.DataFrame:
    """Classify a football summary's drive plays into snap outcomes.

    Args:
        summary: Site v2 ``summary`` with ``drives.previous[]`` (NFL and
            CFB ship the same shape).

    Returns:
        One row per retained play: ``period``, ``clock_seconds``, ``down``,
        ``distance``, ``yards_to_endzone``, ``play_class`` (snap class,
        special class, or ``non_snap``), ``yards`` (statYardage),
        ``points_delta`` (real scoring on the play — non-snap rows keep
        theirs so conservation stays exact), ``offense_team_id``.

    Raises:
        ValueError: When the summary has no previous drives.

    Example:
        Quick start::

            import json
            from sportsdataverse.nfl.nfl_drive_sim import plays_from_espn_drives
            plays = plays_from_espn_drives(json.load(open("summary_nfl.json")))
    """
    drives = (summary.get("drives") or {}).get("previous") or []
    if not drives:
        raise ValueError("summary has no drives.previous[]")
    rows: List[Dict[str, Any]] = []
    prev_away = prev_home = 0
    for drive in drives:
        team_id = str((drive.get("team") or {}).get("id") or "")
        for play in drive.get("plays") or []:
            type_text = str((play.get("type") or {}).get("text") or "")
            start = play.get("start") or {}
            away = int(play.get("awayScore") or prev_away)
            home = int(play.get("homeScore") or prev_home)
            play_class = _TYPE_MAP.get(type_text) or _SPECIAL_TYPES.get(type_text) or "non_snap"
            rows.append(
                {
                    "period": int((play.get("period") or {}).get("number") or 0),
                    "clock_seconds": _clock_seconds(str((play.get("clock") or {}).get("displayValue") or "")),
                    "down": int(start.get("down") or 0),
                    "distance": int(start.get("distance") or 0),
                    "yards_to_endzone": int(start.get("yardsToEndzone") or 0),
                    "play_class": play_class,
                    "yards": int(play.get("statYardage") or 0),
                    "points_delta": (away - prev_away) + (home - prev_home),
                    "offense_team_id": team_id,
                }
            )
            prev_away, prev_home = away, home
    schema = {
        "period": pl.Int64,
        "clock_seconds": pl.Float64,
        "down": pl.Int64,
        "distance": pl.Int64,
        "yards_to_endzone": pl.Int64,
        "play_class": pl.Utf8,
        "yards": pl.Int64,
        "points_delta": pl.Int64,
        "offense_team_id": pl.Utf8,
    }
    return pl.DataFrame(rows, schema=schema)


def espn_football_final_total(summary: Dict[str, Any]) -> int:
    """Real final combined score from the drive play stream (max cumulative)."""
    best = 0
    for drive in (summary.get("drives") or {}).get("previous") or []:
        for play in drive.get("plays") or []:
            best = max(best, int(play.get("awayScore") or 0) + int(play.get("homeScore") or 0))
    return best


def _distance_bucket(distance: int) -> str:
    if distance <= 3:
        return "short"
    if distance <= 7:
        return "med"
    return "long"


def _zone(yards_to_endzone: int) -> str:
    if yards_to_endzone <= 20:
        return "red"
    if yards_to_endzone <= 60:
        return "mid"
    return "own"


def snap_key(down: int, distance: int, yards_to_endzone: int) -> str:
    """Shelf key for a snap: down + distance bucket + field zone."""
    return f"d{min(down, 3)}|{_distance_bucket(distance)}|{_zone(yards_to_endzone)}"


@dataclasses.dataclass
class FootballShelf:
    """PMFs + node parameters for the football engine.

    Attributes:
        snap_pmfs: ``{snap_key: {snap_class: prob}}`` for downs 1-3.
        all_pmf: Global snap-class fallback PMF.
        yards: Per-class empirical yardage sample vectors.
        fourth: ``{distance_bucket: {go/punt/fg: prob}}`` fitted 4th-down
            decisions.
        fg_make: ``{zone_bucket: make prob}`` by yards-to-endzone decile.
        punt_net: Mean punt net yards.
        xp_make: Extra-point make probability.
        penalty_rate: P(accepted penalty no-play) per snap opportunity.
        penalty_yards: Empirical accepted-penalty yardage pool.
        seconds_per_snap: Mean clock burn per snap.
        meta: Provenance.
    """

    snap_pmfs: Dict[str, Dict[str, float]]
    all_pmf: Dict[str, float]
    yards: Dict[str, np.ndarray]
    fourth: Dict[str, Dict[str, float]]
    fg_make: Dict[int, float]
    punt_net: float
    xp_make: float
    seconds_per_snap: float
    penalty_rate: float = 0.0
    penalty_yards: np.ndarray = dataclasses.field(default_factory=lambda: np.array([5.0, 10.0]))
    meta: Dict[str, Any] = dataclasses.field(default_factory=dict)
    _hits: int = dataclasses.field(default=0, repr=False)
    _fallbacks: int = dataclasses.field(default=0, repr=False)

    def get_snap_pmf(self, key: str) -> Dict[str, float]:
        """Snap-class PMF for a key (global fallback, counted)."""
        pmf = self.snap_pmfs.get(key)
        if pmf is None:
            self._fallbacks += 1
            return self.all_pmf
        self._hits += 1
        return pmf

    def fallback_rate(self) -> float:
        """Fraction of snap lookups served by the global fallback."""
        total = self._hits + self._fallbacks
        return self._fallbacks / total if total else 0.0


def build_football_shelf(plays: pl.DataFrame) -> FootballShelf:
    """Build the football shelf from classified real plays.

    Args:
        plays: Output of :func:`plays_from_espn_drives` (one or more games).

    Returns:
        The populated :class:`FootballShelf`.

    Raises:
        ValueError: When no snap rows exist.
    """
    snaps = plays.filter(pl.col("play_class").is_in(list(SNAP_CLASSES)))
    if snaps.height == 0:
        raise ValueError("no snap plays — cannot build a football shelf")

    def _pmf(frame: pl.DataFrame) -> Dict[str, float]:
        counts = frame.group_by("play_class").agg(pl.len().alias("n"))
        total = int(counts["n"].sum())
        by = {r["play_class"]: r["n"] / total for r in counts.to_dicts()}
        return {c: float(by.get(c, 0.0)) for c in SNAP_CLASSES}

    early = snaps.filter((pl.col("down") >= 1) & (pl.col("down") <= 3))
    keyed = early.with_columns(
        pl.struct(["down", "distance", "yards_to_endzone"])
        .map_elements(
            lambda s: snap_key(s["down"], s["distance"], s["yards_to_endzone"]),
            return_dtype=pl.Utf8,
        )
        .alias("key")
    )
    snap_pmfs = {str(k[0]): _pmf(g) for k, g in keyed.group_by("key", maintain_order=True)}

    yards = {
        cls: snaps.filter(pl.col("play_class") == cls)["yards"].to_numpy().astype(float)
        for cls in ("rush", "pass_complete", "sack")
    }
    for cls, default in (("rush", 4.0), ("pass_complete", 10.0), ("sack", -7.0)):
        if yards[cls].size == 0:
            yards[cls] = np.array([default])

    fourth_rows = plays.filter((pl.col("down") == 4) & (pl.col("play_class") != "non_snap"))
    fourth: Dict[str, Dict[str, float]] = {}
    if fourth_rows.height:
        decided = fourth_rows.with_columns(
            pl.when(pl.col("play_class") == "punt")
            .then(pl.lit("punt"))
            .when(pl.col("play_class").str.starts_with("fg_"))
            .then(pl.lit("fg"))
            .otherwise(pl.lit("go"))
            .alias("choice"),
            pl.col("distance").map_elements(lambda d: _distance_bucket(int(d)), return_dtype=pl.Utf8).alias("bucket"),
        )
        for bucket_key, group in decided.group_by("bucket"):
            counts = group.group_by("choice").agg(pl.len().alias("n"))
            total = int(counts["n"].sum())
            fourth[str(bucket_key[0])] = {
                c: float(next((r["n"] for r in counts.to_dicts() if r["choice"] == c), 0) / total)
                for c in ("go", "punt", "fg")
            }

    fgs = plays.filter(pl.col("play_class").str.starts_with("fg_"))
    fg_make: Dict[int, float] = {}
    if fgs.height:
        binned = fgs.with_columns((pl.col("yards_to_endzone") // 10).alias("bin"))
        for bin_key, group in binned.group_by("bin"):
            made = group.filter(pl.col("play_class") == "fg_good").height
            fg_make[int(bin_key[0])] = made / group.height

    punts = plays.filter(pl.col("play_class") == "punt")
    # ESPN statYardage on punt rows is feed-inconsistent (kick vs return
    # yardage); only a plausible fitted net overrides the league-typical 40
    punt_net = 40.0
    if punts.height:
        fitted_net = float(punts["yards"].mean() - 7.0)
        if 25.0 <= fitted_net <= 55.0:
            punt_net = fitted_net

    # XP folded into the TD: league-typical 0.94 unless the stream shows kicks
    xp_make = 0.94

    pens = plays.filter(pl.col("play_class") == "penalty")
    penalty_rate = pens.height / max(1, snaps.height + pens.height)
    pen_pool = np.abs(pens["yards"].to_numpy().astype(float))
    pen_pool = pen_pool[(pen_pool >= 3.0) & (pen_pool <= 20.0)]
    if pen_pool.size == 0:
        pen_pool = np.array([5.0, 10.0, 10.0, 15.0])

    n_snaps = snaps.height
    n_games = 1 if "game_id" not in plays.columns else plays["game_id"].n_unique()
    # pace divides regulation seconds by the plays the ENGINE burns clock on
    # (snaps + punts + field goals) — not non-snaps or no-play penalties
    burned = plays.filter(~pl.col("play_class").is_in(["non_snap", "penalty"])).height
    seconds_per_snap = (4 * 900.0 * n_games) / max(1, burned)

    return FootballShelf(
        snap_pmfs=snap_pmfs,
        all_pmf=_pmf(snaps),
        yards=yards,
        fourth=fourth,
        fg_make=fg_make,
        punt_net=punt_net,
        xp_make=xp_make,
        seconds_per_snap=seconds_per_snap,
        penalty_rate=penalty_rate,
        penalty_yards=pen_pool,
        meta={"n_snaps": n_snaps, "n_keys": len(snap_pmfs)},
    )


class SnapNode:
    """Play-class draw for downs 1-3 (keyed PMF, counted fallback)."""

    def sample(self, shelf: FootballShelf, key: str, rng: np.random.Generator) -> str:
        pmf = shelf.get_snap_pmf(key)
        names = list(SNAP_CLASSES)
        probs = np.array([pmf[c] for c in names], dtype=float)
        total = probs.sum()
        probs = probs / total if total > 0 else np.full(len(names), 1.0 / len(names))
        idx = int(np.searchsorted(np.cumsum(probs), rng.random()))
        return names[min(idx, len(names) - 1)]


class YardsNode:
    """Empirical yardage draw for a snap class."""

    def sample(self, shelf: FootballShelf, play_class: str, rng: np.random.Generator) -> int:
        if play_class == "pass_incomplete":
            return 0
        samples = shelf.yards.get(play_class)
        if samples is None or samples.size == 0:
            return 0
        return int(samples[int(rng.integers(0, samples.size))])


class FourthDownNode:
    """Go / punt / field-goal decision, fitted from real 4th downs."""

    def sample(self, shelf: FootballShelf, distance: int, yards_to_endzone: int, rng: np.random.Generator) -> str:
        # a makeable kick dominates the decision when in range
        if yards_to_endzone <= 35 and rng.random() < 0.8:
            return "fg" if yards_to_endzone <= 30 else ("fg" if rng.random() < 0.5 else "punt")
        choice_pmf = shelf.fourth.get(_distance_bucket(distance))
        if not choice_pmf:
            return "punt" if yards_to_endzone > 40 else "fg"
        names = ["go", "punt", "fg"]
        probs = np.array([choice_pmf.get(n, 0.0) for n in names], dtype=float)
        total = probs.sum()
        probs = probs / total if total > 0 else np.array([0.2, 0.5, 0.3])
        idx = int(np.searchsorted(np.cumsum(probs), rng.random()))
        return names[min(idx, len(names) - 1)]


class FieldGoalNode:
    """Distance-conditioned make probability (fitted, decay default)."""

    def sample(self, shelf: FootballShelf, yards_to_endzone: int, rng: np.random.Generator) -> bool:
        fitted = shelf.fg_make.get(yards_to_endzone // 10)
        if fitted is None:
            kick_distance = yards_to_endzone + 17
            fitted = max(0.05, min(0.98, 1.05 - 0.012 * kick_distance))
        return bool(rng.random() < fitted)


@dataclasses.dataclass
class FootballState:
    """Mid-game state for the football walk."""

    score_home: int = 0
    score_away: int = 0
    quarter: int = 1
    clock_seconds: float = 900.0
    offense_is_home: bool = True
    yards_to_endzone: int = 75
    down: int = 1
    distance: int = 10


def simulate_football_game_pbp(
    shelf: FootballShelf,
    rng: np.random.Generator,
    *,
    college_ot: bool = False,
) -> Tuple[FootballState, List[Dict[str, Any]]]:
    """Simulate one full football game, emitting the play-by-play log.

    Args:
        shelf: The football shelf.
        rng: Numpy generator.
        college_ot: Resolve regulation ties with the college format —
            alternating possessions from the opponent 25, mandatory
            two-point tries after a round-2+ touchdown, and alternating
            two-point attempts from round 3 — instead of the NFL's single
            OT period. College games never tie.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nfl.nfl_drive_sim import simulate_football_game_pbp
            final, pbp = simulate_football_game_pbp(shelf, np.random.default_rng(7))
    """
    snap = SnapNode()
    yards_node = YardsNode()
    fourth = FourthDownNode()
    fg = FieldGoalNode()
    st = FootballState(offense_is_home=bool(rng.random() < 0.5))
    second_half_receiver = not st.offense_is_home
    pbp: List[Dict[str, Any]] = []

    def _log(play_class: str, yards: int, points: int) -> None:
        pbp.append(
            {
                "quarter": st.quarter,
                # touchdown rows log after the snap's burn — clamp at 0:00
                "clock_seconds": round(max(0.0, st.clock_seconds), 0),
                "offense_is_home": st.offense_is_home,
                "down": st.down,
                "distance": st.distance,
                "yards_to_endzone": st.yards_to_endzone,
                "play_class": play_class,
                "yards": yards,
                "points": points,
                "score_home": st.score_home,
                "score_away": st.score_away,
            }
        )

    def _new_possession(offense_is_home: bool, yards_to_endzone: int = 75) -> None:
        st.offense_is_home = offense_is_home
        st.yards_to_endzone = max(1, min(99, yards_to_endzone))
        st.down = 1
        st.distance = min(10, st.yards_to_endzone)

    def _score(points: int) -> None:
        if st.offense_is_home:
            st.score_home += points
        else:
            st.score_away += points

    def _burn() -> None:
        st.clock_seconds -= float(np.clip(rng.uniform(0.7, 1.3) * shelf.seconds_per_snap, 5.0, 45.0))

    def _touchdown() -> None:
        points = 6 + (1 if rng.random() < shelf.xp_make else 0)
        _score(points)
        _log("touchdown", 0, points)
        _new_possession(not st.offense_is_home)

    while True:
        while st.clock_seconds > 0:
            if shelf.penalty_rate > 0.0 and rng.random() < shelf.penalty_rate:
                pen = int(shelf.penalty_yards[int(rng.integers(0, len(shelf.penalty_yards)))])
                # ponytail: 55% of accepted penalties go against the offense;
                # yards sign encodes the penalized side (negative = offense)
                if rng.random() < 0.55:
                    st.yards_to_endzone = min(99, st.yards_to_endzone + pen)
                    st.distance += pen
                    _log("penalty", -pen, 0)
                else:
                    st.yards_to_endzone = max(1, st.yards_to_endzone - pen)
                    if pen >= st.distance:
                        st.down = 1
                        st.distance = min(10, st.yards_to_endzone)
                    else:
                        st.distance -= pen
                    _log("penalty", pen, 0)
                continue  # no play — the down replays
            if st.down <= 3:
                play_class = snap.sample(shelf, snap_key(st.down, st.distance, st.yards_to_endzone), rng)
            else:
                decision = fourth.sample(shelf, st.distance, st.yards_to_endzone, rng)
                if decision == "punt":
                    net = int(np.clip(rng.normal(shelf.punt_net, 6.0), 20, 65))
                    _log("punt", net, 0)
                    _burn()
                    _new_possession(not st.offense_is_home, 100 - max(1, st.yards_to_endzone - net))
                    continue
                if decision == "fg":
                    made = fg.sample(shelf, st.yards_to_endzone, rng)
                    _log("fg_good" if made else "fg_miss", 0, 3 if made else 0)
                    if made:
                        _score(3)
                        _burn()
                        _new_possession(not st.offense_is_home)
                    else:
                        _burn()
                        _new_possession(not st.offense_is_home, 100 - st.yards_to_endzone)
                    continue
                play_class = snap.sample(shelf, snap_key(3, st.distance, st.yards_to_endzone), rng)

            yards = yards_node.sample(shelf, play_class, rng)
            if play_class in ("interception", "fumble_lost"):
                _log(play_class, yards, 0)
                _burn()
                _new_possession(not st.offense_is_home, 100 - st.yards_to_endzone)
                continue
            gained = min(yards, st.yards_to_endzone)
            st.yards_to_endzone -= gained
            _log(play_class, gained, 0)
            _burn()
            if st.yards_to_endzone <= 0:
                _touchdown()
                continue
            if gained >= st.distance:
                st.down = 1
                st.distance = min(10, st.yards_to_endzone)
            else:
                st.down += 1
                st.distance -= gained
                if st.down > 4:
                    _new_possession(not st.offense_is_home, 100 - st.yards_to_endzone)

        if st.quarter >= 4:
            if st.score_home != st.score_away or (not college_ot and st.quarter >= 5):
                return st, pbp
            if college_ot:
                _college_overtime(shelf, st, rng, pbp)
                return st, pbp
            st.quarter += 1
            st.clock_seconds = 600.0
            _new_possession(bool(rng.random() < 0.5))
            continue
        st.quarter += 1
        st.clock_seconds = 900.0
        if st.quarter == 3:
            _new_possession(second_half_receiver)


def simulate_football_ensemble(
    shelf: FootballShelf,
    *,
    n_sim: int = 500,
    seed: Optional[int] = None,
    college_ot: bool = False,
) -> Dict[str, Any]:
    """Monte Carlo ensemble of football games.

    Args:
        shelf: The football shelf.
        n_sim: Number of simulated games.
        seed: RNG seed (same seed = identical ensemble).

    Returns:
        Dict with ``score_home`` / ``score_away`` / ``total`` / ``margin``
        vectors, ``win_prob_home``, ``mean_total``, per-class
        ``event_counts``, and ``n_sim`` — priced through ``odds_math``
        like every other sport.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_drive_sim import simulate_football_ensemble
            ens = simulate_football_ensemble(shelf, n_sim=500, seed=7)
    """
    rng = np.random.default_rng(seed)
    home: np.ndarray = np.empty(n_sim, dtype=np.int64)
    away: np.ndarray = np.empty(n_sim, dtype=np.int64)
    tracked = (*SNAP_CLASSES, "punt", "fg_good", "fg_miss", "touchdown")
    event_arrays: Dict[str, np.ndarray] = {c: np.zeros(n_sim, dtype=np.int64) for c in tracked}
    for i in range(n_sim):
        final, pbp = simulate_football_game_pbp(shelf, rng, college_ot=college_ot)
        home[i] = final.score_home
        away[i] = final.score_away
        for row in pbp:
            if row["play_class"] in event_arrays:
                event_arrays[row["play_class"]][i] += 1
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


def _ot_possession(
    shelf: FootballShelf,
    rng: np.random.Generator,
    pbp: List[Dict[str, Any]],
    *,
    quarter: int,
    is_home: bool,
    must_go_for_two: bool,
) -> int:
    """One untimed college-OT possession from the opponent 25; returns points."""
    snap = SnapNode()
    yards_node = YardsNode()
    fourth = FourthDownNode()
    fg = FieldGoalNode()
    yards_to_endzone, down, distance = 25, 1, 10

    def _log(play_class: str, yards: int, points: int) -> None:
        pbp.append(
            {
                "quarter": quarter,
                "clock_seconds": 0.0,
                "offense_is_home": is_home,
                "down": down,
                "distance": distance,
                "yards_to_endzone": yards_to_endzone,
                "play_class": play_class,
                "yards": yards,
                "points": points,
                "score_home": -1,  # stamped by the caller after the round
                "score_away": -1,
            }
        )

    while True:
        if down > 3:
            decision = fourth.sample(shelf, distance, yards_to_endzone, rng)
            if decision == "fg":
                made = fg.sample(shelf, yards_to_endzone, rng)
                _log("fg_good" if made else "fg_miss", 0, 3 if made else 0)
                return 3 if made else 0
            if decision == "punt":  # never punt in college OT — treat as go
                decision = "go"
        play_class = snap.sample(shelf, snap_key(min(down, 3), distance, yards_to_endzone), rng)
        yards = yards_node.sample(shelf, play_class, rng)
        if play_class in ("interception", "fumble_lost"):
            _log(play_class, yards, 0)
            return 0
        gained = min(yards, yards_to_endzone)
        yards_to_endzone -= gained
        _log(play_class, gained, 0)
        if yards_to_endzone <= 0:
            if must_go_for_two:
                two = bool(rng.random() < TWO_POINT_CONVERT)
                points = 6 + (2 if two else 0)
                _log("two_point_good" if two else "two_point_fail", 0, 2 if two else 0)
            else:
                points = 6 + (1 if rng.random() < shelf.xp_make else 0)
            _log("touchdown", 0, 6)
            return points
        if gained >= distance:
            down, distance = 1, min(10, yards_to_endzone)
        else:
            down += 1
            distance -= gained
            if down > 4:
                _log("turnover_on_downs", 0, 0)
                return 0


def _college_overtime(
    shelf: FootballShelf,
    st: FootballState,
    rng: np.random.Generator,
    pbp: List[Dict[str, Any]],
) -> None:
    """College OT: alternating 25-yard possessions, 2-pt rounds from 3."""
    ot_round = 1
    home_first = bool(rng.random() < 0.5)
    while st.score_home == st.score_away:
        quarter = 4 + ot_round
        if ot_round >= 3:
            # alternating two-point attempts only
            for is_home in (home_first, not home_first):
                converted = bool(rng.random() < TWO_POINT_CONVERT)
                if is_home:
                    st.score_home += 2 if converted else 0
                else:
                    st.score_away += 2 if converted else 0
                pbp.append(
                    {
                        "quarter": quarter,
                        "clock_seconds": 0.0,
                        "offense_is_home": is_home,
                        "down": 1,
                        "distance": 3,
                        "yards_to_endzone": 3,
                        "play_class": "two_point_good" if converted else "two_point_fail",
                        "yards": 0,
                        "points": 2 if converted else 0,
                        "score_home": st.score_home,
                        "score_away": st.score_away,
                    }
                )
        else:
            must_two = ot_round >= 2
            for is_home in (home_first, not home_first):
                marker = len(pbp)
                points = _ot_possession(shelf, rng, pbp, quarter=quarter, is_home=is_home, must_go_for_two=must_two)
                if is_home:
                    st.score_home += points
                else:
                    st.score_away += points
                for row in pbp[marker:]:
                    row["score_home"] = st.score_home
                    row["score_away"] = st.score_away
        home_first = not home_first
        ot_round += 1


_NAME = r"([A-Z][a-z]?\.[\w'\-]+)"
_FULL = r"([A-Z][\w'\-]+ [A-Z][\w'\-]+)"
_C_PASSER_RE = re.compile(_FULL + r" pass (?:complete|incomplete)")
_C_RUSHER_RE = re.compile("^" + _FULL + r" run for")
_C_RECEIVER_RE = re.compile(r"pass (?:complete|incomplete) to " + _FULL)
_C_KICKER_RE = re.compile(_FULL + r" \d+ yd FG")
_C_PUNTER_RE = re.compile(_FULL + r" punt for")
_PASSER_RE = re.compile(r"(?:\(.*?\) )?" + _NAME + r" (?:pass|sacked)")
_RECEIVER_RE = re.compile(r"(?:to|intended for) " + _NAME)
_RUSHER_RE = re.compile(r"^(?:\(.*?\) )?" + _NAME + r" ")
_KICKER_RE = re.compile(_NAME + r" \d+ [Yy]ard [Ff]ield [Gg]oal")
_PUNTER_RE = re.compile(_NAME + r" punts")


@dataclasses.dataclass
class TeamNames:
    """Rendering name pool for one side, fitted from real play text.

    Attributes:
        abbr: Team abbreviation ("PHI").
        passer: The primary quarterback name.
        rushers: (name, share) rushing attribution pool.
        receivers: (name, share) target attribution pool.
        kicker: Field-goal kicker name.
        punter: Punter name.
        tacklers: (name, share) defensive tackle-credit pool.
        name: Team display location ("Ohio State") for dialects that
            write the team out; falls back to ``abbr`` when unfitted.
        penalized: (name, share) penalized-player pool parsed from the
            feed's PENALTY sentences (falls back to ``tacklers``).
    """

    abbr: str
    passer: str
    rushers: List[Tuple[str, float]]
    receivers: List[Tuple[str, float]]
    kicker: str
    punter: str
    tacklers: List[Tuple[str, float]] = dataclasses.field(default_factory=list)
    name: str = ""
    penalized: List[Tuple[str, float]] = dataclasses.field(default_factory=list)

    def sample(self, pool: List[Tuple[str, float]], rng: np.random.Generator) -> str:
        """Draw a name from a share pool (passer as last resort)."""
        if not pool:
            return self.passer
        probs = np.array([share for _, share in pool], dtype=float)
        probs = probs / probs.sum()
        idx = int(np.searchsorted(np.cumsum(probs), rng.random()))
        return pool[min(idx, len(pool) - 1)][0]


@dataclasses.dataclass
class FootballNames:
    """Home/away rendering pools (see :func:`football_names_from_espn`)."""

    home: TeamNames
    away: TeamNames
    #: Game-level (infraction, share) pool parsed from PENALTY sentences.
    penalties: List[Tuple[str, float]] = dataclasses.field(default_factory=list)

    def side(self, offense_is_home: bool) -> "TeamNames":
        """The offense side's pool."""
        return self.home if offense_is_home else self.away


def _count_share(counts: Dict[str, int]) -> List[Tuple[str, float]]:
    total = sum(counts.values())
    return [(name, n / total) for name, n in sorted(counts.items(), key=lambda kv: -kv[1])] if total else []


def football_names_from_espn(summary: Dict[str, Any]) -> FootballNames:
    """Fit rendering name pools from a real summary's play text.

    Passer/rusher/receiver/kicker/punter names are extracted per team with
    the standard ESPN text patterns ("J.Hurts pass short left to
    D.Goedert...") and turned into attribution shares.

    Args:
        summary: Site v2 football ``summary`` with drives + header.

    Returns:
        The :class:`FootballNames` pair.

    Raises:
        ValueError: When the home/away sides cannot be resolved.
    """
    competitors = (summary.get("header", {}).get("competitions") or [{}])[0].get("competitors") or []
    side_ids: Dict[str, str] = {}
    abbrs: Dict[str, str] = {}
    locations: Dict[str, str] = {}
    for competitor in competitors:
        side = str(competitor.get("homeAway") or "")
        team_id = str(competitor.get("team", {}).get("id") or "")
        side_ids[side] = team_id
        abbrs[side] = str(competitor.get("team", {}).get("abbreviation") or side.upper())
        locations[side] = str(competitor.get("team", {}).get("location") or abbrs[side])
    if "home" not in side_ids or "away" not in side_ids:
        raise ValueError("could not resolve home/away sides from the summary header")

    pools: Dict[str, Dict[str, Dict[str, int]]] = {
        team_id: {
            "passer": {},
            "rusher": {},
            "receiver": {},
            "kicker": {},
            "punter": {},
            "tackler": {},
            "penalized": {},
        }
        for team_id in side_ids.values()
    }
    infractions: Dict[str, int] = {}
    token_to_id = {
        **{abbrs[side].upper(): side_ids[side] for side in side_ids},
        **{locations[side].replace(" ", "").upper(): side_ids[side] for side in side_ids},
    }
    other_team = {
        side_ids["home"]: side_ids["away"],
        side_ids["away"]: side_ids["home"],
    }
    for drive in (summary.get("drives") or {}).get("previous") or []:
        team_id = str((drive.get("team") or {}).get("id") or "")
        if team_id not in pools:
            continue
        bucket = pools[team_id]
        for play in drive.get("plays") or []:
            text = str(play.get("text") or "")
            type_text = str((play.get("type") or {}).get("text") or "")
            trailer = re.search(r"\(([^()]+)\)\.?\s*$", text)
            if trailer and ("Pass" in type_text or "Rush" in type_text or "Sack" in type_text):
                for tackler in trailer.group(1).split(";"):
                    tackler = tackler.strip()
                    if re.fullmatch(_NAME.strip("()"), tackler):
                        defense_bucket = pools[other_team[team_id]]["tackler"]
                        defense_bucket[tackler] = defense_bucket.get(tackler, 0) + 1
            # accepted-penalty grammar: NFL "PENALTY on PHI-A.Brown,
            # Offensive Pass Interference, 10 yards" / college "PENALTY
            # NOTREDAME holding (Jagusah, Charles) 10 yards"
            nfl_pen = re.search(r"PENALTY on ([A-Z]+)-([A-Z]\.[\w'\-]+), ([^,]+), \d+ yards", text)
            cfb_pen = re.search(r"PENALTY ([A-Z]+) ([a-z][\w ]*?) \(([^)]+)\) \d+ yards", text)
            pen_match = nfl_pen or cfb_pen
            if pen_match:
                infraction = pen_match.group(3) if pen_match is nfl_pen else pen_match.group(2)
                infractions[infraction] = infractions.get(infraction, 0) + 1
                pen_team = token_to_id.get(pen_match.group(1).upper())
                if pen_team is not None:
                    player = pen_match.group(2) if pen_match is nfl_pen else pen_match.group(3)
                    pen_bucket = pools[pen_team]["penalized"]
                    pen_bucket[player] = pen_bucket.get(player, 0) + 1
            sacked_by = re.search(r"sacked by " + r"([A-Z][\w'\-]+ [A-Z][\w'\-]+)", text)
            if sacked_by:
                defense_bucket = pools[other_team[team_id]]["tackler"]
                defense_bucket[sacked_by.group(1)] = defense_bucket.get(sacked_by.group(1), 0) + 1
            if "Pass" in type_text or "Sack" in type_text:
                match = _PASSER_RE.search(text) or _C_PASSER_RE.search(text)
                if match:
                    bucket["passer"][match.group(1)] = bucket["passer"].get(match.group(1), 0) + 1
                target = _RECEIVER_RE.search(text) or _C_RECEIVER_RE.search(text)
                if target:
                    bucket["receiver"][target.group(1)] = bucket["receiver"].get(target.group(1), 0) + 1
            elif "Rush" in type_text:
                match = _RUSHER_RE.search(text) or _C_RUSHER_RE.search(text)
                if match:
                    bucket["rusher"][match.group(1)] = bucket["rusher"].get(match.group(1), 0) + 1
            elif "Field Goal" in type_text:
                match = _KICKER_RE.search(text) or _C_KICKER_RE.search(text)
                if match:
                    bucket["kicker"][match.group(1)] = bucket["kicker"].get(match.group(1), 0) + 1
            elif "Punt" in type_text:
                match = _PUNTER_RE.search(text) or _C_PUNTER_RE.search(text)
                if match:
                    bucket["punter"][match.group(1)] = bucket["punter"].get(match.group(1), 0) + 1

    def _team(side: str) -> TeamNames:
        bucket = pools[side_ids[side]]
        passers = sorted(bucket["passer"].items(), key=lambda kv: -kv[1])
        return TeamNames(
            abbr=abbrs[side],
            passer=passers[0][0] if passers else "QB",
            rushers=_count_share(bucket["rusher"]),
            receivers=_count_share(bucket["receiver"]),
            kicker=max(bucket["kicker"], key=lambda n: bucket["kicker"][n], default="K"),
            punter=max(bucket["punter"], key=lambda n: bucket["punter"][n], default="P"),
            tacklers=_count_share(bucket["tackler"]),
            name=locations[side],
            penalized=_count_share(bucket["penalized"]),
        )

    return FootballNames(home=_team("home"), away=_team("away"), penalties=_count_share(infractions))


_ORDINAL = {1: "1st", 2: "2nd", 3: "3rd", 4: "4th"}


def render_football_pbp(
    pbp: List[Dict[str, Any]],
    names: FootballNames,
    rng: np.random.Generator,
    *,
    provider: str = "nfl_gsis",
) -> List[Dict[str, Any]]:
    """Add provider-formulaic play text to a simulated football pbp log.

    Args:
        pbp: Rows from :func:`simulate_football_game_pbp`.
        names: Fitted rendering pools (:func:`football_names_from_espn`).
        rng: Numpy generator (samples rushers/targets/directions/tacklers).
        provider: Template provider — ``"nfl_gsis"`` (default; NFL.com
            GameCenter text with clock prefix, formations, and tackler
            credits), ``"espn"`` (the CFB sentence style), or any provider
            registered in ``sportsdataverse._common.play_text`` (aliases
            ``gsis`` / ``college`` resolve).

    Returns:
        New rows with ``text`` and (on snaps) ``down_distance_text`` added,
        plus inserted boundary rows (``play_class`` ``kickoff`` /
        ``end_period`` / ``two_minute_warning`` / ``end_game``) at the
        opening, after scores, at quarter breaks, and at the final gun —
        gated on the provider's template keys.

    Example:
        Quick start::

            final, pbp = simulate_football_game_pbp(shelf, rng)
            rendered = render_football_pbp(pbp, names, rng, provider="nfl_gsis")
            print(rendered[0]["down_distance_text"], "|", rendered[0]["text"])
    """
    tpl = get_templates("football", provider)
    rendered: List[Dict[str, Any]] = []

    def _boundary(play_class: str, ref: Dict[str, Any], text: str) -> Dict[str, Any]:
        return {
            "quarter": int(ref["quarter"]),
            "clock_seconds": float(ref["clock_seconds"]),
            "offense_is_home": bool(ref["offense_is_home"]),
            "down": 0,
            "distance": 0,
            "yards_to_endzone": int(ref["yards_to_endzone"]),
            "play_class": play_class,
            "yards": 0,
            "points": 0,
            "score_home": int(ref["score_home"]),
            "score_away": int(ref["score_away"]),
            "text": text,
        }

    def _kickoff(kicking_home: bool, next_row: Dict[str, Any]) -> Dict[str, Any]:
        kicking = names.side(kicking_home)
        receiving = names.side(not kicking_home)
        end_yd = max(1, 100 - int(next_row["yards_to_endzone"]))
        kick_yards = int(rng.integers(58, 71))
        # ponytail: ~60% of fixture kickoffs are touchbacks; a return lands the
        # drive at the exact start the state machine already chose
        if rng.random() < 0.6 or end_yd <= 12:
            text = tpl["kickoff_touchback"].format(
                kicker=kicking.kicker,
                kick_yards=kick_yards,
                kick_from=f"{kicking.abbr} 35",
                recv_spot=f"{receiving.abbr} {end_yd}",
            )
        else:
            landing = max(1, end_yd - int(rng.integers(12, 31)))
            text = tpl["kickoff_return"].format(
                kicker=kicking.kicker,
                kick_yards=kick_yards,
                kick_from=f"{kicking.abbr} 35",
                land_spot=f"{receiving.abbr} {landing}",
                recv_spot=f"{receiving.abbr} {end_yd}",
                ret_yards=end_yd - landing,
                returner=receiving.sample(receiving.receivers, rng),
                tackler=kicking.sample(kicking.tacklers, rng),
            )
        return _boundary("kickoff", next_row, text)

    def _end_period(ref: Dict[str, Any], quarter: int) -> Dict[str, Any]:
        return _boundary(
            "end_period", ref, tpl["end_period"].format(quarter=quarter, ordinal=_ORDINAL.get(quarter, "4th"))
        )

    prev: Optional[Dict[str, Any]] = None
    team_timeouts: Dict[Tuple[bool, int], int] = {}
    for row in pbp:
        if prev is None:
            rendered.append(_kickoff(not bool(row["offense_is_home"]), row))
        else:
            quarter_changed = int(prev["quarter"]) != int(row["quarter"])
            if quarter_changed and "end_period" in tpl and int(prev["quarter"]) <= 3:
                rendered.append(_end_period(prev, int(prev["quarter"])))
            if (
                "two_minute_warning" in tpl
                and not quarter_changed
                and int(row["quarter"]) in (2, 4)
                and float(prev["clock_seconds"]) > 120.0 >= float(row["clock_seconds"])
            ):
                rendered.append(_boundary("two_minute_warning", row, tpl["two_minute_warning"]))
            starts_half = quarter_changed and int(row["quarter"]) == 3
            ot_kick = quarter_changed and int(row["quarter"]) == 5 and float(row["clock_seconds"]) > 0
            post_score = str(prev["play_class"]) in ("touchdown", "fg_good")
            if starts_half or ot_kick:
                rendered.append(_kickoff(not bool(row["offense_is_home"]), row))
            elif post_score and (int(row["quarter"]) <= 4 or float(row["clock_seconds"]) > 0):
                rendered.append(_kickoff(bool(prev["offense_is_home"]), row))
            if not quarter_changed:
                mmss = f"{int(float(row['clock_seconds'])) // 60:02d}:{int(float(row['clock_seconds'])) % 60:02d}"
                half = 1 if int(row["quarter"]) <= 2 else 2
                # ponytail: ~6 team + ~4 official stoppages/game in the feeds
                if "timeout_team" in tpl and rng.random() < 0.045:
                    side_is_home = bool(rng.random() < 0.5)
                    used = team_timeouts.get((side_is_home, half), 0)
                    if used < 3:
                        team_timeouts[(side_is_home, half)] = used + 1
                        caller = names.side(side_is_home)
                        rendered.append(
                            _boundary(
                                "timeout_team",
                                row,
                                tpl["timeout_team"].format(
                                    count=used + 1,
                                    abbr=caller.abbr,
                                    team=caller.name or caller.abbr,
                                    clock=mmss,
                                ),
                            )
                        )
                if "timeout_official" in tpl and rng.random() < 0.03:
                    rendered.append(_boundary("timeout_official", row, tpl["timeout_official"].format(clock=mmss)))
        offense = names.side(bool(row["offense_is_home"]))
        defense = names.side(not bool(row["offense_is_home"]))
        yards_to_endzone = int(row["yards_to_endzone"])
        spot = (
            f"{offense.abbr} {100 - yards_to_endzone}"
            if yards_to_endzone > 50
            else f"{defense.abbr} {yards_to_endzone}"
        )
        yards = int(row["yards"])
        direction = ("left", "middle", "right")[int(rng.integers(0, 3))]
        depth = "deep" if yards >= 15 else "short"
        play_class = str(row["play_class"])
        clock_s = float(row["clock_seconds"])
        prefix = ""
        if "clock_prefix" in tpl:
            prefix = tpl["clock_prefix"].format(minutes=int(clock_s) // 60, seconds=int(clock_s) % 60)
        shotgun = tpl.get("shotgun_prefix", "") if ("shotgun_prefix" in tpl and rng.random() < 0.55) else ""
        tackle = (
            tpl["tackle_suffix"].format(tackler=defense.sample(defense.tacklers, rng)) if "tackle_suffix" in tpl else ""
        )
        first_down = (
            tpl["first_down_suffix"]
            if "first_down_suffix" in tpl and yards >= int(row["distance"]) and yards > 0
            else ""
        )
        ctx = {
            "rusher": offense.sample(offense.rushers, rng),
            "receiver": offense.sample(offense.receivers, rng),
            "passer": offense.passer,
            "defender": defense.sample(defense.tacklers, rng),
            "kicker": offense.kicker,
            "punter": offense.punter,
            "offense_abbr": offense.abbr,
            "defense_abbr": defense.abbr,
            "spot": spot,
            "yards": yards,
            "loss": -yards if yards < 0 else yards,
            "plural": "s" if yards != -1 else "",
            "depth": depth,
            "direction": direction,
            "tackle": tackle,
            "first_down": first_down,
            "kick_distance": yards_to_endzone + 17,
            "lane": tpl.get("rush_lanes", {}).get(direction, direction),
            "xp": tpl.get("xp_good", "") if int(row["points"]) == 7 else tpl.get("xp_fail", ""),
        }
        if play_class == "penalty" and "penalty" in tpl:
            # negative logged yards = penalty on the offense
            penalized_home = bool(row["offense_is_home"]) == (yards < 0)
            flagged = names.side(penalized_home)
            infraction = (
                flagged.sample(names.penalties, rng)
                if names.penalties
                else ("Holding" if "clock_prefix" in tpl else "holding")
            )
            text = tpl["penalty"].format(
                abbr=flagged.abbr,
                player=flagged.sample(flagged.penalized or flagged.tacklers, rng),
                infraction=infraction,
                pen_yards=abs(yards),
                spot=spot,
                team=flagged.name or flagged.abbr,
            )
        elif play_class == "rush":
            if "rush" in tpl:
                text = prefix + shotgun + tpl["rush"].format(**ctx)
            elif yards < 0:
                text = tpl["rush_loss"].format(**ctx)
            else:
                text = tpl["rush_gain"].format(**ctx)
        elif play_class in (
            "pass_complete",
            "pass_incomplete",
            "sack",
            "interception",
            "fumble_lost",
            "punt",
            "fg_good",
            "fg_miss",
            "touchdown",
        ):
            body = tpl[play_class].format(**ctx)
            use_prefix = play_class != "touchdown"
            use_shotgun = play_class in ("pass_complete", "pass_incomplete", "interception")
            text = (prefix if use_prefix else "") + (shotgun if use_shotgun else "") + body
        elif play_class in ("two_point_good", "two_point_fail"):
            verdict = "SUCCEEDS" if play_class == "two_point_good" else "FAILS"
            text = f"{offense.abbr} two-point conversion attempt {verdict}"
        elif play_class == "turnover_on_downs":
            text = f"{offense.abbr} turnover on downs at {spot}"
        else:
            text = f"{offense.abbr} {play_class.replace('_', ' ')}"
        new_row = dict(row)
        new_row["text"] = text
        if play_class in SNAP_CLASSES:
            pre_ytez = min(99, yards_to_endzone + max(0, yards))
            pre_spot = f"{offense.abbr} {100 - pre_ytez}" if pre_ytez > 50 else f"{defense.abbr} {pre_ytez}"
            down = _ORDINAL.get(int(row["down"]), "4th")
            new_row["down_distance_text"] = f"{down} & {int(row['distance'])} at {pre_spot}"
        rendered.append(new_row)
        prev = row
    if pbp and "end_game" in tpl:
        last_q = min(4, int(pbp[-1]["quarter"]))
        rendered.append(
            _boundary("end_game", pbp[-1], tpl["end_game"].format(quarter=last_q, ordinal=_ORDINAL.get(last_q, "4th")))
        )
    return rendered
