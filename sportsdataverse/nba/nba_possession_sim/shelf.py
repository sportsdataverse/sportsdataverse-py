"""PMF shelf — precomputed possession-outcome distributions (WS4).

The reference shelf pattern without the infrastructure: possession outcomes are
classified from real ``playbyplayv3`` action streams, binned by
:func:`~sportsdataverse.nba.nba_possession_sim.keygen.gamestate_key`, and
stored as plain outcome→probability dicts (parquet round-trip, no external
key-value store). Sim-time sampling is a dict lookup — never a live model
call.

Coverage is a first-class metric: :class:`Shelf.get_pmf` counts hits vs
fallbacks to the global ``"all"`` PMF, and :meth:`Shelf.fallback_rate` is
asserted in the gates so the reference silent-fallback failure mode stays loud.

The two player-prior builders (:func:`player_usage_priors`,
:func:`player_shot_mix_priors`) are the WS3 FeatureSet pilot consumers:
declarative trailing-window specs over real per-player game logs, point-in-
time via the engine's as-of guard. They feed the player-attribution upgrade
(out of the v1 team-level sim) and stand alone as projection inputs.
"""

from __future__ import annotations

import dataclasses
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import polars as pl

from sportsdataverse._common.feature_set import FeatureSetSpec, rolling_features
from sportsdataverse.nba.nba_possession_sim.keygen import LearnedGamestateKeyer, gamestate_key, parse_clock

#: Sampleable possession outcomes (the OutcomeNode vocabulary).
OUTCOMES: Tuple[str, ...] = (
    "rim_make",
    "rim_miss",
    "mid_make",
    "mid_miss",
    "three_make",
    "three_miss",
    "ft_trip_1",
    "ft_trip_2",
    "ft_trip_3",
    "tov",
)

#: Distance (feet) at or under which a two-point shot counts as a rim attempt.
RIM_DISTANCE_FT = 4.0

_META_KEY = "__meta__"

#: "(Name N AST)" credit on made-shot descriptions.
_AST_RE = re.compile(r"\(([^()]+?) \d+ AST\)")

_RENAMES = {
    "actionNumber": "action_number",
    "actionType": "action_type",
    "subType": "sub_type",
    "shotResult": "shot_result",
    "shotValue": "shot_value",
    "shotDistance": "shot_distance",
    "teamId": "team_id",
    "personId": "person_id",
    "scoreHome": "score_home",
    "scoreAway": "score_away",
}


def _normalize_actions(actions: pl.DataFrame) -> pl.DataFrame:
    renames = {old: new for old, new in _RENAMES.items() if old in actions.columns}
    out = actions.rename(renames)
    required = [
        "game_id",
        "action_number",
        "period",
        "clock",
        "action_type",
        "shot_value",
        "shot_distance",
        "shot_result",
        "sub_type",
        "description",
        "team_id",
        "person_id",
        "score_home",
        "score_away",
    ]
    missing = sorted(set(required) - set(out.columns))
    if missing:
        raise ValueError(f"actions frame missing columns {missing}")
    # dtype discipline at the boundary: some captures ship numerics as strings
    # (G-League actionNumber) — a lexicographic sort would scramble event order
    return out.with_columns(
        pl.col("action_number").cast(pl.Int64, strict=False),
        pl.col("period").cast(pl.Int64, strict=False),
        pl.col("team_id").cast(pl.Int64, strict=False),
        pl.col("person_id").cast(pl.Int64, strict=False),
        pl.col("shot_value").cast(pl.Int64, strict=False),
        pl.col("shot_distance").cast(pl.Float64, strict=False),
    )


def _shot_outcome(shot_value: int, shot_distance: float, made: bool) -> str:
    if shot_value == 3:
        return "three_make" if made else "three_miss"
    if shot_distance <= RIM_DISTANCE_FT:
        return "rim_make" if made else "rim_miss"
    return "mid_make" if made else "mid_miss"


def possessions_from_pbp(actions: pl.DataFrame) -> pl.DataFrame:
    """Classify real ``playbyplayv3`` actions into possession-outcome events.

    Emits two row kinds: ``"outcome"`` rows (one per sampleable possession
    outcome — shots, free-throw trips, turnovers, gamestate attached) and
    ``"rebound"`` rows (``oreb``/``dreb`` following each missed shot).
    Classification is conservative: non-event actions (substitutions,
    timeouts, replays) are not outcomes and are simply skipped.

    Args:
        actions: Action rows (camelCase v3 keys or snake_case accepted) for
            one or more games; must carry ``game_id``.

    Returns:
        Long event frame: ``game_id``, ``period``, ``clock_seconds``,
        ``score_diff`` (offense perspective, pre-event), ``kind``,
        ``outcome``, ``team_id``, ``points`` (observed points incl. FT makes).

    Example:
        Quick start::

            import json, polars as pl
            from sportsdataverse.nba.nba_possession_sim.shelf import possessions_from_pbp
            payload = json.load(open("playbyplayv3.json"))
            df = pl.DataFrame(payload["game"]["actions"]).with_columns(
                pl.lit(payload["game"]["gameId"]).alias("game_id"))
            events = possessions_from_pbp(df)
    """
    frame = _normalize_actions(actions)
    rows: List[Dict[str, Any]] = []
    for game_id, game in frame.group_by("game_id", maintain_order=True):
        game = game.sort("action_number")
        records = game.to_dicts()

        def _score(value: Any, previous: int) -> int:
            try:
                return int(value)
            except (TypeError, ValueError):
                return previous

        # infer the home team from the first scoring action: a make that
        # increments score_home was by the home team; one that increments
        # score_away identifies the away team (home = the other id).
        home_team: Optional[int] = None
        team_ids = {int(r["team_id"]) for r in records if r.get("team_id")} - {0}
        prev_home = prev_away = 0
        saw_score = False
        for rec in records:
            new_home = _score(rec.get("score_home"), prev_home)
            new_away = _score(rec.get("score_away"), prev_away)
            saw_score = saw_score or new_home > prev_home or new_away > prev_away
            tid = int(rec["team_id"]) if rec.get("team_id") else 0
            if home_team is None and tid:
                if new_home > prev_home:
                    home_team = tid
                elif new_away > prev_away and len(team_ids - {tid}) == 1:
                    home_team = (team_ids - {tid}).pop()
            prev_home, prev_away = new_home, new_away
        if home_team is None:
            if not saw_score:
                # degenerate capture (season releases occasionally ship a
                # game as one all-null placeholder row): zero information,
                # zero events — skip it rather than kill a season build
                continue
            raise ValueError(f"{game_id}: could not infer home team from score stream")

        prev_home = prev_away = 0
        # free-throw trips interleave with rebounds/substitutions, so they
        # accumulate in a pending map keyed by shooter until "N of N" lands
        pending_trips: Dict[int, Dict[str, Any]] = {}

        def _emit_row(
            rec: Dict[str, Any],
            kind: str,
            outcome: str,
            points: int,
            *,
            clock_seconds: float,
            diff: float,
            team_id: int,
        ) -> None:
            rows.append(
                {
                    "game_id": str(rec["game_id"]),
                    "period": int(rec["period"]),
                    "clock_seconds": clock_seconds,
                    "score_diff": float(diff),
                    "kind": kind,
                    "outcome": outcome,
                    "team_id": team_id,
                    "points": points,
                }
            )

        def _flush_trip(person: int) -> None:
            trip = pending_trips.pop(person)
            n = min(int(trip["total"]), 3)
            _emit_row(
                trip["rec"],
                "outcome",
                f"ft_trip_{n}",
                int(trip["made"]),
                clock_seconds=trip["clock_seconds"],
                diff=trip["diff"],
                team_id=trip["team_id"],
            )

        for i, rec in enumerate(records):
            action = rec.get("action_type") or ""
            team_id = int(rec["team_id"]) if rec.get("team_id") else 0
            clock_seconds = parse_clock(str(rec.get("clock") or ""))
            offense_is_home = team_id == home_team
            diff = (prev_home - prev_away) if offense_is_home else (prev_away - prev_home)

            if action == "Made Shot":
                outcome = _shot_outcome(int(rec.get("shot_value") or 2), float(rec.get("shot_distance") or 99), True)
                _emit_row(
                    rec,
                    "outcome",
                    outcome,
                    3 if outcome == "three_make" else 2,
                    clock_seconds=clock_seconds,
                    diff=diff,
                    team_id=team_id,
                )
            elif action == "Missed Shot":
                outcome = _shot_outcome(int(rec.get("shot_value") or 2), float(rec.get("shot_distance") or 99), False)
                _emit_row(rec, "outcome", outcome, 0, clock_seconds=clock_seconds, diff=diff, team_id=team_id)
                for nxt in records[i + 1 : i + 6]:
                    if nxt.get("action_type") == "Rebound":
                        oreb = int(nxt.get("team_id") or 0) == team_id
                        _emit_row(
                            rec,
                            "rebound",
                            "oreb" if oreb else "dreb",
                            0,
                            clock_seconds=clock_seconds,
                            diff=diff,
                            team_id=team_id,
                        )
                        break
            elif action == "Free Throw":
                person = int(rec.get("person_id") or 0)
                sub = str(rec.get("sub_type") or "")
                value_match = re.search(r"(\d)PT", sub)
                if value_match:
                    # G-League single-free-throw rule: ONE attempt worth N
                    # points ("Free Throw 2PT"). Encoded as ft_trip_N — the
                    # engine's Binomial(N, ft_pct) matches the mean exactly
                    # (variance differs slightly; acceptable v1 approximation).
                    value = int(value_match.group(1))
                    made_single = not str(rec.get("description") or "").startswith("MISS")
                    _emit_row(
                        rec,
                        "outcome",
                        f"ft_trip_{value}",
                        value if made_single else 0,
                        clock_seconds=clock_seconds,
                        diff=diff,
                        team_id=team_id,
                    )
                    prev_home = _score(rec.get("score_home"), prev_home)
                    prev_away = _score(rec.get("score_away"), prev_away)
                    continue
                match = re.search(r"(\d+) of (\d+)", sub)
                idx, total = (int(match.group(1)), int(match.group(2))) if match else (1, 1)
                made = not str(rec.get("description") or "").startswith("MISS")
                if idx == 1 or person not in pending_trips:
                    if person in pending_trips:
                        _flush_trip(person)  # malformed stream: new trip before close
                    pending_trips[person] = {
                        "rec": rec,
                        "total": total,
                        "made": 0,
                        "seen": 0,
                        "clock_seconds": clock_seconds,
                        "diff": diff,
                        "team_id": team_id,
                    }
                trip = pending_trips[person]
                trip["made"] += int(made)
                trip["seen"] += 1
                if trip["seen"] >= trip["total"]:
                    _flush_trip(person)
            elif action == "Turnover":
                _emit_row(rec, "outcome", "tov", 0, clock_seconds=clock_seconds, diff=diff, team_id=team_id)

            prev_home = _score(rec.get("score_home"), prev_home)
            prev_away = _score(rec.get("score_away"), prev_away)
        for person in list(pending_trips):
            _flush_trip(person)  # end-of-stream stragglers keep their observed makes

    schema = {
        "game_id": pl.Utf8,
        "period": pl.Int64,
        "clock_seconds": pl.Float64,
        "score_diff": pl.Float64,
        "kind": pl.Utf8,
        "outcome": pl.Utf8,
        "team_id": pl.Int64,
        "points": pl.Int64,
    }
    return pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)


@dataclasses.dataclass
class Shelf:
    """Precomputed possession-outcome PMFs with coverage accounting.

    Attributes:
        outcome_pmfs: ``{gamestate_key: {outcome: prob}}``.
        all_pmf: Global fallback PMF over :data:`OUTCOMES`.
        oreb_rate: P(offensive rebound | missed shot), global.
        ft_pct: Free-throw make probability, global.
        mean_possession_seconds: Mean clock burn per possession.
        meta: Provenance (game count, event count, source).
        oreb_rates: Optional per-gamestate-key rebound rates (the fitted
            ReboundNode model's predictions); ``get_oreb`` falls back to the
            global scalar for keys absent here. Fitted in the hand-cut key
            domain — a learned-keyed shelf serves the global rate.
        keyer: Optional learned gamestate keyer the shelf was built with;
            ``key_for`` routes through it so lookups match the build keys.
        aux_rates: Optional per-key expanded-node rate overrides
            (``{key: {rate_name: value}}``) — the fitted aux nodes;
            ``aux_for`` overlays them on the global ``aux`` dict.
        pace_rates: Optional per-key mean possession seconds (the fitted
            pace node); ``pace_for`` falls back to the global scalar.
    """

    outcome_pmfs: Dict[str, Dict[str, float]]
    all_pmf: Dict[str, float]
    oreb_rate: float
    ft_pct: float
    mean_possession_seconds: float
    meta: Dict[str, Any] = dataclasses.field(default_factory=dict)
    oreb_rates: Optional[Dict[str, float]] = None
    aux: Optional[Dict[str, float]] = None
    keyer: Optional[LearnedGamestateKeyer] = None
    aux_rates: Optional[Dict[str, Dict[str, float]]] = None
    pace_rates: Optional[Dict[str, float]] = None
    _hits: int = dataclasses.field(default=0, repr=False)
    _fallbacks: int = dataclasses.field(default=0, repr=False)

    def key_for(self, score_diff: float, period: int, clock_seconds: float) -> str:
        """The shelf's own lookup key for a gamestate.

        Uses the attached learned keyer when the shelf was built with one,
        else the hand-cut :func:`~sportsdataverse.nba.nba_possession_sim.keygen.gamestate_key`
        — sim-time lookups always key exactly the way the shelf was built.

        Args:
            score_diff: Offense-perspective score differential.
            period: Period number.
            clock_seconds: Seconds left in the period.

        Returns:
            The lookup key string.
        """
        if self.keyer is not None:
            return self.keyer.key(score_diff, period, clock_seconds)
        return gamestate_key(score_diff, period, clock_seconds)

    def aux_for(self, key: str) -> Dict[str, float]:
        """Expanded-node rates for a gamestate: fitted per-key overlays on
        the global ``aux`` dict (identical to the global dict when no
        per-key aux was fitted — the empirical default path).

        Args:
            key: The gamestate key.

        Returns:
            The rate dict the expanded nodes consume.
        """
        base = dict(self.aux) if self.aux else {}
        if self.aux_rates is not None:
            base.update(self.aux_rates.get(key, {}))
        return base

    def pace_for(self, key: str) -> float:
        """Mean possession seconds for a gamestate (fitted per-key pace
        when present, else the global scalar).

        Args:
            key: The gamestate key.

        Returns:
            The clock-burn base for this state.
        """
        if self.pace_rates is not None:
            rate = self.pace_rates.get(key)
            if rate is not None:
                return rate
        return self.mean_possession_seconds

    def get_pmf(self, key: str) -> Tuple[Dict[str, float], bool]:
        """Look up a gamestate's outcome PMF, falling back to the global PMF.

        Args:
            key: A :func:`~sportsdataverse.nba.nba_possession_sim.keygen.gamestate_key`.

        Returns:
            ``(pmf, used_fallback)``; every fallback is counted.
        """
        pmf = self.outcome_pmfs.get(key)
        if pmf is None:
            self._fallbacks += 1
            return self.all_pmf, True
        self._hits += 1
        return pmf, False

    def get_oreb(self, key: Optional[str] = None) -> float:
        """P(offensive rebound) for a gamestate (per-key model rate when fitted).

        Args:
            key: Optional gamestate key; None (or an unmodeled key) returns
                the global empirical rate.

        Returns:
            The rebound probability.
        """
        if key is not None and self.oreb_rates is not None:
            rate = self.oreb_rates.get(key)
            if rate is not None:
                return rate
        return self.oreb_rate

    def fallback_rate(self) -> float:
        """Fraction of lookups served by the global fallback (0.0 when unused)."""
        total = self._hits + self._fallbacks
        return self._fallbacks / total if total else 0.0

    def reset_coverage(self) -> None:
        """Zero the hit/fallback counters."""
        self._hits = 0
        self._fallbacks = 0


def build_shelf(possessions: pl.DataFrame, *, keyer: Optional[LearnedGamestateKeyer] = None) -> Shelf:
    """Build the PMF shelf from a classified possession-event frame.

    Args:
        possessions: Output of :func:`possessions_from_pbp` (real data).
        keyer: Optional fitted
            :class:`~sportsdataverse.nba.nba_possession_sim.keygen.LearnedGamestateKeyer`;
            when given, PMFs are keyed by its leaves (and carried on the
            shelf so sim-time lookups key identically). Default is the
            hand-cut :func:`~sportsdataverse.nba.nba_possession_sim.keygen.gamestate_key`.
            Fit the keyer on the SAME (or a superset of the) train games —
            never on evaluation games.

    Returns:
        The populated :class:`Shelf`.

    Raises:
        ValueError: When the frame contains no outcome events.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_possession_sim.shelf import (
                build_shelf, possessions_from_pbp,
            )
            shelf = build_shelf(possessions_from_pbp(actions))
            pmf, fallback = shelf.get_pmf("d0|p1|early")
    """
    outcomes = possessions.filter(pl.col("kind") == "outcome")
    if outcomes.height == 0:
        raise ValueError("no outcome events — cannot build a shelf")

    key_fn = keyer.key if keyer is not None else gamestate_key
    keyed = outcomes.with_columns(
        pl.struct(["score_diff", "period", "clock_seconds"])
        .map_elements(
            lambda s: key_fn(s["score_diff"], s["period"], s["clock_seconds"]),
            return_dtype=pl.Utf8,
        )
        .alias("key")
    )

    def _pmf(frame: pl.DataFrame) -> Dict[str, float]:
        counts = frame.group_by("outcome").agg(pl.len().alias("n"))
        total = int(counts["n"].sum())
        by = {row["outcome"]: row["n"] / total for row in counts.to_dicts()}
        return {o: float(by.get(o, 0.0)) for o in OUTCOMES}

    outcome_pmfs = {str(key[0]): _pmf(group) for key, group in keyed.group_by("key", maintain_order=True)}
    all_pmf = _pmf(keyed)

    rebounds = possessions.filter(pl.col("kind") == "rebound")
    oreb_rate = float((rebounds["outcome"] == "oreb").mean()) if rebounds.height else 0.25

    ft_rows = outcomes.filter(pl.col("outcome").str.starts_with("ft_trip_"))
    ft_att = int(ft_rows["outcome"].str.extract(r"ft_trip_(\d)").cast(pl.Int64).sum() or 0)
    ft_pct = float(ft_rows["points"].sum() / ft_att) if ft_att else 0.78

    # mean clock burn between consecutive outcomes within a game-period
    deltas = (
        outcomes.sort(["game_id", "period", "clock_seconds"], descending=[False, False, True])
        .with_columns(
            (pl.col("clock_seconds").shift(1) - pl.col("clock_seconds")).over(["game_id", "period"]).alias("burn")
        )
        .filter((pl.col("burn") > 0) & (pl.col("burn") < 60))
    )
    mean_possession_seconds = float(deltas["burn"].mean()) if deltas.height else 14.0

    return Shelf(
        outcome_pmfs=outcome_pmfs,
        all_pmf=all_pmf,
        oreb_rate=oreb_rate,
        ft_pct=ft_pct,
        mean_possession_seconds=mean_possession_seconds,
        meta={
            "n_games": int(possessions["game_id"].n_unique()),
            "n_events": int(outcomes.height),
            "n_keys": len(outcome_pmfs),
        },
        keyer=keyer,
    )


def shelf_to_parquet(shelf: Shelf, path: Union[str, Path]) -> Path:
    """Serialize a shelf to one parquet file (PMFs long + meta row).

    Args:
        shelf: The shelf to persist.
        path: Target parquet path.

    Returns:
        The written path.

    Raises:
        ValueError: For a learned-keyed shelf — the keyer does not
            round-trip through the single-file format yet, and silently
            dropping it would make every reloaded lookup miss to the
            global fallback.
    """
    if shelf.keyer is not None:
        # ponytail: single-file format carries no keyer; add a decision-table
        # sidecar when the publish pipeline wants learned-keyed shelves.
        raise ValueError("learned-keyed shelves do not round-trip through parquet yet; rebuild via build_shelf")
    rows = [
        {"key": key, "outcome": outcome, "prob": prob}
        for key, pmf in {**shelf.outcome_pmfs, "all": shelf.all_pmf}.items()
        for outcome, prob in pmf.items()
    ]
    meta = {
        "oreb_rate": shelf.oreb_rate,
        "ft_pct": shelf.ft_pct,
        "mean_possession_seconds": shelf.mean_possession_seconds,
        "meta": shelf.meta,
        "aux": shelf.aux,
    }
    if shelf.oreb_rates is not None:
        rows.extend({"key": key, "outcome": "__oreb__", "prob": rate} for key, rate in shelf.oreb_rates.items())
    if shelf.pace_rates is not None:
        rows.extend({"key": key, "outcome": "__pace__", "prob": rate} for key, rate in shelf.pace_rates.items())
    if shelf.aux_rates is not None:
        rows.extend(
            {"key": key, "outcome": f"__aux__{name}", "prob": value}
            for key, rates in shelf.aux_rates.items()
            for name, value in rates.items()
        )
    rows.append({"key": _META_KEY, "outcome": json.dumps(meta, sort_keys=True), "prob": None})
    out = Path(path)
    pl.DataFrame(rows).write_parquet(out)
    return out


def shelf_from_parquet(path: Union[str, Path]) -> Shelf:
    """Load a shelf persisted by :func:`shelf_to_parquet`.

    Args:
        path: Parquet path.

    Returns:
        The reconstructed :class:`Shelf` (coverage counters zeroed).
    """
    frame = pl.read_parquet(path)
    meta_row = frame.filter(pl.col("key") == _META_KEY)
    meta = json.loads(meta_row["outcome"][0])
    oreb_frame = frame.filter(pl.col("outcome") == "__oreb__")
    oreb_rates: Optional[Dict[str, float]] = None
    if oreb_frame.height:
        oreb_rates = {row["key"]: float(row["prob"]) for row in oreb_frame.to_dicts()}
    pace_frame = frame.filter(pl.col("outcome") == "__pace__")
    pace_rates: Optional[Dict[str, float]] = None
    if pace_frame.height:
        pace_rates = {row["key"]: float(row["prob"]) for row in pace_frame.to_dicts()}
    aux_frame = frame.filter(pl.col("outcome").str.starts_with("__aux__"))
    aux_rates: Optional[Dict[str, Dict[str, float]]] = None
    if aux_frame.height:
        aux_rates = {}
        for row in aux_frame.to_dicts():
            aux_rates.setdefault(row["key"], {})[str(row["outcome"])[len("__aux__") :]] = float(row["prob"])
    pmf_rows = frame.filter(
        (pl.col("key") != _META_KEY) & (pl.col("outcome").str.starts_with("__") == False)  # noqa: E712
    )
    pmfs: Dict[str, Dict[str, float]] = {}
    for row in pmf_rows.to_dicts():
        pmfs.setdefault(row["key"], {})[row["outcome"]] = float(row["prob"])
    all_pmf = pmfs.pop("all")
    return Shelf(
        outcome_pmfs=pmfs,
        all_pmf=all_pmf,
        oreb_rate=float(meta["oreb_rate"]),
        ft_pct=float(meta["ft_pct"]),
        mean_possession_seconds=float(meta["mean_possession_seconds"]),
        meta=dict(meta["meta"]),
        oreb_rates=oreb_rates,
        aux=dict(meta["aux"]) if meta.get("aux") else None,
        pace_rates=pace_rates,
        aux_rates=aux_rates,
    )


# ----------------------------------------------------------------------------
# FeatureSet pilot consumers (WS3): player priors from real game logs
# ----------------------------------------------------------------------------


def player_box_from_boxscorev3(payload: "Dict[str, Any]") -> pl.DataFrame:
    """Official per-player box lines from a ``boxscoretraditionalv3`` payload.

    The independent same-game surface for cross-source validation: the
    official box is aggregated by the league, the pbp-derived logs
    (:func:`player_game_logs_from_pbp`) by our classifier — reconciling the
    two (``modeling.integrity.reconcile``) is the box-vs-pbp agreement gate.

    Args:
        payload: ``boxscoretraditionalv3`` dict (``boxScoreTraditional`` with
            ``homeTeam``/``awayTeam`` player statistics).

    Returns:
        One row per player, matching the log schema: ``game_id`` (Utf8),
        ``player_id``/``team_id`` (Int64), ``fga``, ``fg3a``, ``fta``,
        ``pts``, ``tov``, ``reb``, ``ast``. Empty payloads return the
        zero-row schema.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_possession_sim import player_box_from_boxscorev3
            box = player_box_from_boxscorev3(box_payload)
    """
    game = payload.get("boxScoreTraditional") or payload.get("game") or {}
    rows: "List[Dict[str, Any]]" = []
    for side in ("homeTeam", "awayTeam"):
        team = game.get(side) or {}
        team_id = int(team.get("teamId") or game.get(f"{side}Id") or 0)
        for player in team.get("players") or []:
            stats = player.get("statistics") or {}
            rows.append(
                {
                    "game_id": str(game.get("gameId") or ""),
                    "player_id": int(player.get("personId") or 0),
                    "team_id": team_id,
                    "fga": int(stats.get("fieldGoalsAttempted") or 0),
                    "fg3a": int(stats.get("threePointersAttempted") or 0),
                    "fta": int(stats.get("freeThrowsAttempted") or 0),
                    "ftm": int(stats.get("freeThrowsMade") or 0),
                    "pts": int(stats.get("points") or 0),
                    "tov": int(stats.get("turnovers") or 0),
                    "reb": int(stats.get("reboundsTotal") or 0),
                    "ast": int(stats.get("assists") or 0),
                }
            )
    schema = {
        "game_id": pl.Utf8,
        "player_id": pl.Int64,
        "team_id": pl.Int64,
        "fga": pl.Int64,
        "fg3a": pl.Int64,
        "fta": pl.Int64,
        "ftm": pl.Int64,
        "pts": pl.Int64,
        "tov": pl.Int64,
        "reb": pl.Int64,
        "ast": pl.Int64,
    }
    return pl.DataFrame(rows, schema=schema)


def player_game_logs_from_pbp(actions: pl.DataFrame) -> pl.DataFrame:
    """Per-player per-game shooting/turnover logs from raw actions.

    Args:
        actions: ``playbyplayv3`` action rows (see :func:`possessions_from_pbp`).

    Returns:
        One row per (game_id, player_id): ``team_id``, ``fga``, ``fg3a``,
        ``fta``, ``pts``, ``tov``. ``game_id`` doubles as the chronological ordering
        column for the prior specs (NBA game ids sort by schedule).
    """
    frame = _normalize_actions(actions)
    # team-turnover rows log the TEAM as personId (with teamId=0) — exclude
    # any person_id that appears as a team_id, or team entities become
    # phantom "players" with null team and a turnover
    team_entities = frame.filter(pl.col("team_id") > 0).get_column("team_id").unique().to_list()
    frame = frame.filter(
        (pl.col("person_id") > 0) & (pl.col("person_id").is_in(team_entities) == False)  # noqa: E712
    )
    shots = frame.filter(pl.col("action_type").is_in(["Made Shot", "Missed Shot"]))
    fts = frame.filter(pl.col("action_type") == "Free Throw")
    tovs = frame.filter(pl.col("action_type") == "Turnover")

    shot_stats = shots.group_by(["game_id", "person_id"]).agg(
        pl.len().alias("fga"),
        (pl.col("shot_value") == 3).sum().alias("fg3a"),
        pl.when(pl.col("action_type") == "Made Shot").then(pl.col("shot_value")).otherwise(0).sum().alias("pts_fg"),
    )
    ft_stats = (
        fts.with_columns(
            # v3 quirk: shotResult is EMPTY on free throws — made-ness lives in
            # the description's "MISS " prefix (the established classifier rule)
            (pl.col("description").cast(pl.Utf8).str.starts_with("MISS") == False).alias("_ft_made"),  # noqa: E712
            # G-League single-FT rule: "Free Throw {N}PT" is ONE attempt worth
            # N points; standard trip makes are 1 point each
            pl.col("sub_type").cast(pl.Utf8).str.extract(r"(\d)PT", 1).cast(pl.Int64).fill_null(1).alias("_ft_value"),
        )
        .group_by(["game_id", "person_id"])
        .agg(
            pl.len().alias("fta"),
            pl.col("_ft_made").cast(pl.Int64).sum().alias("ftm"),
            (pl.col("_ft_made").cast(pl.Int64) * pl.col("_ft_value")).sum().alias("ft_pts"),
        )
    )
    tov_stats = tovs.group_by(["game_id", "person_id"]).agg(pl.len().alias("tov"))
    rebs = frame.filter(pl.col("action_type") == "Rebound")
    reb_stats = rebs.group_by(["game_id", "person_id"]).agg(pl.len().alias("reb"))

    # assists: makes carry "(Name N AST)" — resolve the last name to a player
    # id within the scorer's game (name collisions drop the credit, counted)
    ast_rows: List[Dict[str, Any]] = []
    name_map: Dict[Any, Dict[str, int]] = {}
    # index BOTH name forms: bare surnames collide for same-surname teammates
    # (folded to a drop below), but the feed's initialed playerNameI form
    # ("G. Antetokounmpo") is what descriptions use for them — unambiguous
    pid_col = "personId" if "personId" in actions.columns else "person_id"
    name_frames = [
        actions.select(
            pl.col("game_id"),
            pl.col(pid_col).alias("pid"),
            pl.col(col).cast(pl.Utf8).alias("nm"),
        )
        for col in ("playerName", "player_name", "playerNameI", "player_name_i")
        if col in actions.columns
    ]
    names = (
        pl.concat(name_frames, how="vertical")
        .filter((pl.col("pid") > 0) & pl.col("nm").is_not_null() & (pl.col("nm").str.len_chars() > 0))
        .unique()
    )
    for row in names.to_dicts():
        name_map.setdefault(row["game_id"], {})
        # last name = the mapped key; collisions map to -1 (dropped)
        key = str(row["nm"]).strip()
        bucket = name_map[row["game_id"]]
        bucket[key] = -1 if key in bucket and bucket[key] != int(row["pid"]) else int(row["pid"])
    for rec in shots.filter(pl.col("action_type") == "Made Shot").select(["game_id", "description"]).to_dicts():
        match = _AST_RE.search(str(rec.get("description") or ""))
        if not match:
            continue
        assister = name_map.get(rec["game_id"], {}).get(match.group(1).strip(), -1)
        if assister and assister > 0:
            ast_rows.append({"game_id": rec["game_id"], "person_id": assister})
    ast_stats = (
        pl.DataFrame(ast_rows, schema={"game_id": pl.Utf8, "person_id": pl.Int64})
        .group_by(["game_id", "person_id"])
        .agg(pl.len().alias("ast"))
        if ast_rows
        else pl.DataFrame(schema={"game_id": pl.Utf8, "person_id": pl.Int64, "ast": pl.UInt32})
    )

    team_map = (
        frame.filter((pl.col("person_id") > 0) & (pl.col("team_id") > 0))
        .group_by(["game_id", "person_id"])
        .agg(pl.col("team_id").first())
    )
    logs = (
        shot_stats.join(ft_stats, on=["game_id", "person_id"], how="full", coalesce=True)
        .join(tov_stats, on=["game_id", "person_id"], how="full", coalesce=True)
        .join(reb_stats, on=["game_id", "person_id"], how="full", coalesce=True)
        .join(ast_stats, on=["game_id", "person_id"], how="full", coalesce=True)
        .join(team_map, on=["game_id", "person_id"], how="left")
        .with_columns(
            [pl.col(c).fill_null(0) for c in ("fga", "fg3a", "pts_fg", "fta", "ftm", "ft_pts", "tov", "reb", "ast")]
        )
        .with_columns((pl.col("pts_fg") + pl.col("ft_pts")).alias("pts"))
        .rename({"person_id": "player_id"})
        .select("game_id", "player_id", "team_id", "fga", "fg3a", "fta", "ftm", "pts", "tov", "reb", "ast")
        .sort("game_id", "player_id")
    )
    return logs


#: WS3 pilot 1 — trailing usage volume (shots/FT trips/turnovers).
USAGE_SPEC = FeatureSetSpec(
    name="nba_player_usage",
    unit="player_id",
    aggfuncs={"fga": ("sum", "mean"), "fta": ("sum",), "tov": ("sum",)},
    spans=(5, 0),
    date_col="game_id",
)

#: WS3 pilot 2 — trailing shot mix / scoring efficiency.
SHOT_MIX_SPEC = FeatureSetSpec(
    name="nba_player_shot_mix",
    unit="player_id",
    aggfuncs={"fg3a": ("mean",), "fga": ("mean",), "pts": ("mean",)},
    spans=(5, 0),
    date_col="game_id",
)


def player_usage_priors(logs: pl.DataFrame, *, as_of: Optional[str] = None) -> pl.DataFrame:
    """Trailing usage-volume priors (FeatureSet pilot 1).

    Args:
        logs: Output of :func:`player_game_logs_from_pbp`.
        as_of: Optional game-id cutoff — only strictly earlier games
            contribute (point-in-time, leakage-safe).

    Returns:
        One row per player with ``{col}_{agg}___{span}`` prior columns.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_possession_sim.shelf import (
                player_game_logs_from_pbp, player_usage_priors,
            )
            priors = player_usage_priors(player_game_logs_from_pbp(actions))
    """
    return rolling_features(logs, USAGE_SPEC, as_of=as_of)


def player_shot_mix_priors(logs: pl.DataFrame, *, as_of: Optional[str] = None) -> pl.DataFrame:
    """Trailing shot-mix / efficiency priors (FeatureSet pilot 2).

    Args:
        logs: Output of :func:`player_game_logs_from_pbp`.
        as_of: Optional game-id cutoff (strictly earlier games only).

    Returns:
        One row per player with ``{col}_{agg}___{span}`` prior columns.
    """
    return rolling_features(logs, SHOT_MIX_SPEC, as_of=as_of)
