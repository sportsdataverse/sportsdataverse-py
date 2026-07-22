"""Player attribution — who scored the simulated points (WS4 v2).

Attributes each simulated possession's points to a player via outcome-
conditional usage shares built from REAL per-player game logs: three-point
outcomes draw from three-attempt shares, rim/mid outcomes from two-point
attempt shares, free-throw trips from FTA shares, turnovers from TOV
shares. This turns the team-level ensemble into per-player point
distributions — the prop-market surface — while conserving team totals
exactly (every attributed point is a team point).

v1 scope: the terminal scorer of the possession takes all its points
(putback chains attribute to the final event's sampled shooter). Assist /
rebound attribution and minutes-aware availability are the next seams.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Dict, List, Optional, Sequence

import numpy as np
import polars as pl


def _shares(values: np.ndarray) -> np.ndarray:
    total = float(values.sum())
    if total <= 0:
        return np.full(values.shape[0], 1.0 / values.shape[0])
    return values / total


@dataclasses.dataclass
class TeamAttribution:
    """Outcome-conditional scorer shares for one team.

    Attributes:
        team_id: The team.
        player_ids: Players in share order.
        two_shares: P(shooter | two-point attempt) per player.
        three_shares: P(shooter | three-point attempt) per player.
        ft_shares: P(shooter | free-throw trip) per player.
        tov_shares: P(committer | turnover) per player.
    """

    team_id: int
    player_ids: List[int]
    two_shares: np.ndarray
    three_shares: np.ndarray
    ft_shares: np.ndarray
    tov_shares: np.ndarray
    reb_shares: np.ndarray
    ast_shares: np.ndarray
    p_assisted: float

    @classmethod
    def from_logs(cls, logs: pl.DataFrame, team_id: int) -> "TeamAttribution":
        """Build shares from real per-player game logs.

        Args:
            logs: ``player_game_logs_from_pbp`` output (must carry
                ``team_id``).
            team_id: Team to build.

        Returns:
            The team's :class:`TeamAttribution`.

        Raises:
            ValueError: When the team has no logged players.
        """
        team = (
            logs.filter(pl.col("team_id") == team_id)
            .group_by("player_id")
            .agg(
                pl.col("fga").sum(),
                pl.col("fg3a").sum(),
                pl.col("fta").sum(),
                pl.col("tov").sum(),
                pl.col("reb").sum(),
                pl.col("ast").sum(),
                pl.col("pts").sum(),
            )
            .sort("player_id")
        )
        if team.height == 0:
            raise ValueError(f"no logged players for team {team_id}")
        fga = team["fga"].to_numpy().astype(float)
        fg3a = team["fg3a"].to_numpy().astype(float)
        makes_proxy = float(team["pts"].sum())
        total_ast = float(team["ast"].sum())
        # share of scored baskets that were assisted, bounded to sane range
        p_assisted = min(0.85, max(0.35, (2.0 * total_ast) / makes_proxy)) if makes_proxy else 0.6
        return cls(
            team_id=team_id,
            player_ids=[int(p) for p in team["player_id"].to_list()],
            two_shares=_shares(np.clip(fga - fg3a, 0.0, None)),
            three_shares=_shares(fg3a),
            ft_shares=_shares(team["fta"].to_numpy().astype(float)),
            tov_shares=_shares(team["tov"].to_numpy().astype(float)),
            reb_shares=_shares(team["reb"].to_numpy().astype(float)),
            ast_shares=_shares(team["ast"].to_numpy().astype(float)),
            p_assisted=p_assisted,
        )

    def sample_rebounder(self, rng: np.random.Generator) -> int:
        """Draw the credited rebounder from the team's rebound shares."""
        idx = int(np.searchsorted(np.cumsum(self.reb_shares), rng.random()))
        return self.player_ids[min(idx, len(self.player_ids) - 1)]

    def sample_assister(self, scorer: int, rng: np.random.Generator) -> Optional[int]:
        """Draw the assister for a made basket (None = unassisted).

        The assisted probability and assister shares are fitted from the
        real logs; the scorer cannot assist their own basket.
        """
        if rng.random() >= self.p_assisted or len(self.player_ids) < 2:
            return None
        shares = self.ast_shares.copy()
        if scorer in self.player_ids:
            shares[self.player_ids.index(scorer)] = 0.0
        total: float = float(shares.sum())
        if total <= 0:
            return None
        idx = int(np.searchsorted(np.cumsum(shares / total), rng.random()))
        return self.player_ids[min(idx, len(self.player_ids) - 1)]

    def without(self, unavailable: "Sequence[int]") -> "TeamAttribution":
        """Availability mask: remove players and renormalize every share.

        Args:
            unavailable: Player ids to exclude (injury / rest scenarios).

        Returns:
            A new :class:`TeamAttribution` without those players.

        Raises:
            ValueError: When no players would remain.
        """
        keep = [i for i, pid in enumerate(self.player_ids) if pid not in set(unavailable)]
        if not keep:
            raise ValueError("availability mask removed every player")
        idx = np.array(keep, dtype=int)
        return TeamAttribution(
            team_id=self.team_id,
            player_ids=[self.player_ids[i] for i in keep],
            two_shares=_shares(self.two_shares[idx]),
            three_shares=_shares(self.three_shares[idx]),
            ft_shares=_shares(self.ft_shares[idx]),
            tov_shares=_shares(self.tov_shares[idx]),
            reb_shares=_shares(self.reb_shares[idx]),
            ast_shares=_shares(self.ast_shares[idx]),
            p_assisted=self.p_assisted,
        )

    def reweight(self, factors: "Dict[int, float]") -> "TeamAttribution":
        """Minutes-aware reweighting: scale every share by a per-player factor.

        The factor is typically projected minutes over observed minutes
        (:func:`minutes_from_gamerotation` supplies the observed side); a
        factor of 0 removes a player, 2.0 doubles their weight before
        renormalization.

        Args:
            factors: ``{player_id: multiplier}``; absent players keep 1.0.

        Returns:
            A new :class:`TeamAttribution` with renormalized shares.
        """
        mult = np.array([max(0.0, factors.get(pid, 1.0)) for pid in self.player_ids])
        return TeamAttribution(
            team_id=self.team_id,
            player_ids=list(self.player_ids),
            two_shares=_shares(self.two_shares * mult),
            three_shares=_shares(self.three_shares * mult),
            ft_shares=_shares(self.ft_shares * mult),
            tov_shares=_shares(self.tov_shares * mult),
            reb_shares=_shares(self.reb_shares * mult),
            ast_shares=_shares(self.ast_shares * mult),
            p_assisted=self.p_assisted,
        )

    def sample(self, outcome: str, rng: np.random.Generator) -> int:
        """Draw the attributed player for a terminal outcome."""
        if outcome.startswith("three"):
            shares = self.three_shares
        elif outcome.startswith("ft_trip"):
            shares = self.ft_shares
        elif outcome == "tov":
            shares = self.tov_shares
        else:
            shares = self.two_shares
        idx = int(np.searchsorted(np.cumsum(shares), rng.random()))
        return self.player_ids[min(idx, len(self.player_ids) - 1)]


@dataclasses.dataclass
class PlayerAttribution:
    """Home/away attribution pair the engine samples from.

    Attributes:
        home: Home team's shares.
        away: Away team's shares.
        ft_pct: Optional shooter-conditional free-throw rates
            (``{player_id: shrunk FT%}``). OPT-IN: when present, the
            expanded walk samples the FT shooter BEFORE resolving makes and
            uses their rate — shooter identity then AFFECTS the outcome
            stream, deliberately trading the attribution-only-credits
            contract (exact star-out redistribution) for FT realism.
    """

    home: TeamAttribution
    away: TeamAttribution
    ft_pct: Optional[Dict[int, float]] = None

    @classmethod
    def from_logs(
        cls,
        logs: pl.DataFrame,
        *,
        home_team_id: int,
        away_team_id: int,
        with_ft_pct: bool = False,
        ft_shrinkage: float = 10.0,
    ) -> "PlayerAttribution":
        """Build both teams from one logs frame.

        Args:
            logs: ``player_game_logs_from_pbp`` output with ``team_id``.
            home_team_id: Home team.
            away_team_id: Away team.
            with_ft_pct: Fit shooter-conditional FT rates from the logs'
                ``ftm``/``fta`` (empirical-Bayes shrunk toward the pooled
                league rate with ``ft_shrinkage`` pseudo-attempts).
                Default off — see the ``ft_pct`` attribute for the
                contract this trades away.
            ft_shrinkage: Pseudo-attempt weight of the pooled prior.

        Returns:
            The :class:`PlayerAttribution` pair.

        Raises:
            ValueError: When ``with_ft_pct`` is requested but the logs
                carry no ``ftm`` column.

        Example:
            Quick start::

                from sportsdataverse.nba.nba_possession_sim import (
                    PlayerAttribution, player_game_logs_from_pbp, simulate_ensemble,
                )
                logs = player_game_logs_from_pbp(actions)
                att = PlayerAttribution.from_logs(
                    logs, home_team_id=1610612749, away_team_id=1610612751)
                ens = simulate_ensemble(shelf, n_sim=500, seed=7, attribution=att)
                pts_dist = ens["player_points"]  # {player_id: np.ndarray(n_sim)}
        """
        ft_pct: Optional[Dict[int, float]] = None
        if with_ft_pct:
            if "ftm" not in logs.columns:
                raise ValueError("with_ft_pct needs an 'ftm' column in the logs")
            pooled = logs.select(pl.col("ftm").sum().alias("m"), pl.col("fta").sum().alias("a")).to_dicts()[0]
            league = float(pooled["m"] / pooled["a"]) if pooled["a"] else 0.78
            per_player = logs.group_by("player_id").agg(pl.col("ftm").sum(), pl.col("fta").sum())
            ft_pct = {
                int(row["player_id"]): float((row["ftm"] + ft_shrinkage * league) / (row["fta"] + ft_shrinkage))
                for row in per_player.iter_rows(named=True)
            }
        return cls(
            home=TeamAttribution.from_logs(logs, home_team_id),
            away=TeamAttribution.from_logs(logs, away_team_id),
            ft_pct=ft_pct,
        )

    def sample(self, offense_is_home: bool, outcome: str, rng: np.random.Generator) -> int:
        """Draw the attributed player for the offense side's outcome."""
        side = self.home if offense_is_home else self.away
        return side.sample(outcome, rng)

    def without(
        self,
        *,
        home_unavailable: "Sequence[int]" = (),
        away_unavailable: "Sequence[int]" = (),
    ) -> "PlayerAttribution":
        """Availability-masked copy of both teams (see TeamAttribution.without)."""
        return PlayerAttribution(
            home=self.home.without(home_unavailable) if home_unavailable else self.home,
            away=self.away.without(away_unavailable) if away_unavailable else self.away,
            ft_pct=self.ft_pct,
        )

    def all_player_ids(self) -> List[int]:
        """Every attributable player id (home then away)."""
        return [*self.home.player_ids, *self.away.player_ids]


def terminal_outcome(events: List[str], vocabulary: "set[str]") -> Optional[str]:
    """The last sampleable outcome in a possession trail (None if none)."""
    for event in reversed(events):
        if event in vocabulary:
            return event
    return None


def simulate_player_boxscores(
    shelf: Any,
    attribution: PlayerAttribution,
    *,
    n_sim: int = 300,
    seed: Optional[int] = None,
    rules: Optional[Any] = None,
) -> Dict[str, Any]:
    """Monte Carlo per-player boxscore distributions (pts / reb / ast).

    Walks full games with the FULLY EXPANDED possession tree, attributing
    every scored basket (scorer + fitted assisted-probability assister) and
    every rebound (team rebound shares on the correct side) — the complete
    player prop surface: each stat is a per-player sample vector priced by
    ``odds_math``. Team point totals are conserved exactly by construction.

    Args:
        shelf: The PMF shelf (empirical or model-backed).
        attribution: Scorer/rebounder/assister shares (use ``without`` for
            availability scenarios).
        n_sim: Simulated games.
        seed: RNG seed (same seed = identical output).
        rules: League clock structure (defaults to NBA).

    Returns:
        ``{"pts"|"reb"|"ast": {player_id: np.ndarray(n_sim)},
        "score_home"/"score_away": np.ndarray, "n_sim": int}``.

    Example:
        Prop pricing with an injury scenario::

            from sportsdataverse.odds.odds_math import prob_over
            box = simulate_player_boxscores(
                shelf, att.without(home_unavailable=[star_id]), n_sim=500, seed=7)
            p = prob_over(box["pts"][other_id], 24.5)
    """
    from sportsdataverse.nba.nba_possession_sim.expanded_nodes import (
        simulate_possession_expanded,
    )
    from sportsdataverse.nba.nba_possession_sim.rules import NBA_RULES

    league_rules = rules or NBA_RULES
    rng = np.random.default_rng(seed)
    pids = attribution.all_player_ids()
    stats: Dict[str, Dict[int, np.ndarray]] = {
        stat: {pid: np.zeros(n_sim, dtype=np.int64) for pid in pids} for stat in ("pts", "reb", "ast")
    }
    score_home: np.ndarray = np.zeros(n_sim, dtype=np.int64)
    score_away: np.ndarray = np.zeros(n_sim, dtype=np.int64)

    makes = {"rim_make", "mid_make", "three_make"}
    for i in range(n_sim):
        home = away = 0
        offense_is_home = bool(rng.random() < 0.5)
        period, clock = 1, league_rules.period_seconds
        while True:
            while clock > 0:
                diff = float(home - away if offense_is_home else away - home)
                points, trail, scorer = simulate_possession_expanded(
                    shelf,
                    score_diff=diff,
                    period=period,
                    clock_seconds=clock,
                    rng=rng,
                    attribution=attribution,
                    offense_is_home=offense_is_home,
                )
                if offense_is_home:
                    home += points
                else:
                    away += points
                if scorer is not None and points > 0:
                    stats["pts"][scorer][i] += points
                    if any(e in makes for e in trail):
                        side = attribution.home if offense_is_home else attribution.away
                        assister = side.sample_assister(scorer, rng)
                        if assister is not None:
                            stats["ast"][assister][i] += 1
                for event in trail:
                    if event == "oreb":
                        side = attribution.home if offense_is_home else attribution.away
                        stats["reb"][side.sample_rebounder(rng)][i] += 1
                    elif event == "dreb":
                        side = attribution.away if offense_is_home else attribution.home
                        stats["reb"][side.sample_rebounder(rng)][i] += 1
                if shelf.pace_rates is not None:
                    pace_base = shelf.pace_for(shelf.key_for(diff, period, clock))
                else:
                    pace_base = shelf.mean_possession_seconds
                clock -= float(np.clip(rng.uniform(0.5, 1.5) * pace_base, 4.0, 24.0))
                offense_is_home = not offense_is_home
            if period >= league_rules.periods and home != away:
                break
            period += 1
            clock = league_rules.ot_seconds if period > league_rules.periods else league_rules.period_seconds
        score_home[i] = home
        score_away[i] = away
    return {
        "pts": stats["pts"],
        "reb": stats["reb"],
        "ast": stats["ast"],
        "score_home": score_home,
        "score_away": score_away,
        "n_sim": n_sim,
    }


def minutes_from_gamerotation(payload: Dict[str, Any]) -> Dict[int, float]:
    """Real minutes per player from a stats gamerotation payload.

    Args:
        payload: ``GET .../gamerotation`` dict (``resultSets`` with
            ``IN_TIME_REAL`` / ``OUT_TIME_REAL`` stints in tenths of seconds).

    Returns:
        ``{player_id: minutes}`` summed over stints, both teams.

    Example:
        Minutes-aware reweight::

            minutes = minutes_from_gamerotation(rotation_payload)
            factors = {pid: projected[pid] / m for pid, m in minutes.items() if m > 0}
            att = PlayerAttribution(home=att.home.reweight(factors),
                                    away=att.away.reweight(factors))
    """
    minutes: Dict[int, float] = {}
    for result_set in payload.get("resultSets") or []:
        headers = result_set.get("headers") or []
        try:
            pid_i = headers.index("PERSON_ID")
            in_i = headers.index("IN_TIME_REAL")
            out_i = headers.index("OUT_TIME_REAL")
        except ValueError:
            continue
        for row in result_set.get("rowSet") or []:
            pid = int(row[pid_i])
            stint = (float(row[out_i]) - float(row[in_i])) / 10.0 / 60.0
            minutes[pid] = minutes.get(pid, 0.0) + max(0.0, stint)
    return minutes
