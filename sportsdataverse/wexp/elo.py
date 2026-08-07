"""General margin-Elo engine for the bake-off (Axis A1 + baseline oracle #4).

League-agnostic: consumes the oracle-frame game contract
(``season, week, home_team, away_team, neutral_site, home_margin``) and
emits pre-game ratings + P(home). Every constant is a tunable
(:class:`EloConfig`) — the fixed "K=20/HFA=65/1/3-reversion" recipe was
refuted in RESEARCH; seeds here are starting points for the harness sweep
(nfelo: k~9.1, z~402, carryover~0.53).

MOV multiplier (538 form): ``ln(|pd|+1) * 2.2 / ((elo_w - elo_l)*0.001 + 2.2)``
— log-diminishing returns plus the autocorrelation correction that shrinks
credit when big favorites win big.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

import polars as pl

__all__ = ["EloConfig", "elo_ratings"]


@dataclass(frozen=True)
class EloConfig:
    """Tunable Elo parameters (all seeds, none sacred).

    Attributes:
        k: Update step size.
        z: Logistic scale (``p = 1/(10^(-diff/z)+1)``).
        hfa: Home-field advantage in rating points (0 applied at neutral sites).
        init: Rating for a team's first-ever appearance.
        carryover: Fraction of (rating - init) kept across seasons
            (0 = full reset, 1 = no reversion; nfelo ~0.53).
        mov_mult: Apply the margin-of-victory multiplier.
    """

    k: float = 20.0
    z: float = 400.0
    hfa: float = 65.0
    init: float = 1500.0
    carryover: float = 0.67
    mov_mult: bool = True


def elo_ratings(
    games: pl.DataFrame,
    config: EloConfig = EloConfig(),
    season_priors: Optional[pl.DataFrame] = None,
    hfa_by_season: Optional[dict[int, float]] = None,
) -> pl.DataFrame:
    """Walk games chronologically and emit pre-game Elo ratings + P(home).

    Games are processed in ``(season, week)`` order; the emitted rating for
    a game NEVER includes that game's result (walk-forward by construction).
    Ties (margin 0) score as 0.5. Unplayed games (null ``home_margin``)
    still get a pre-game rating and probability but do not update ratings.

    Args:
        games: Frame with ``game_id``, ``season``, ``week``, ``home_team``,
            ``away_team``, ``neutral_site``, ``home_margin``.
        config: Tunable parameters.
        season_priors: Optional continuity-prior table with ``season``,
            ``team``, ``prior_shift`` (rating points). The shift is added
            ONCE per team at its season entry (first appearance or season
            boundary, after the carryover reversion) — the Axis D3/D4
            hook. Teams absent from the table shift by 0. Priors are
            preseason knowledge for their ``season``; never derive them
            from that season's games.
        hfa_by_season: Optional season -> HFA override in rating points
            (Axis F ``per_era``; e.g. ``{2020: 0.0}`` for the no-fans
            COVID season). Seasons absent from the map use ``config.hfa``.
            Values are a-priori/tuned parameters — never fit them from
            the season they apply to inside a walk-forward run.

    Returns:
        The input rows (original order) with ``home_elo_pre``,
        ``away_elo_pre``, ``p_home`` appended.

    Example:
        Quick start::

            from sportsdataverse.wexp.elo import EloConfig, elo_ratings
            rated = elo_ratings(games, EloConfig(k=9.1, z=402, carryover=0.53))
    """
    ratings: dict[str, float] = {}
    last_season: dict[str, int] = {}
    shifts: dict[tuple[int, str], float] = {}
    if season_priors is not None:
        n_dup = season_priors.height - season_priors.unique(subset=["season", "team"]).height
        if n_dup:
            raise ValueError(f"season_priors has {n_dup} duplicate (season, team) row(s)")
        shifts = {
            (int(r["season"]), str(r["team"])): float(r["prior_shift"]) for r in season_priors.iter_rows(named=True)
        }
    ordered = games.with_row_index("__order").sort("season", "week", "__order")

    home_pre: list[float] = []
    away_pre: list[float] = []
    p_homes: list[float] = []

    for row in ordered.iter_rows(named=True):
        season = row["season"]
        for team in (row["home_team"], row["away_team"]):
            if team not in ratings:
                ratings[team] = config.init + shifts.get((season, team), 0.0)
                last_season[team] = season
            elif last_season[team] != season:
                # season boundary: revert toward init, once per team per season
                ratings[team] = (
                    config.init + config.carryover * (ratings[team] - config.init) + shifts.get((season, team), 0.0)
                )
                last_season[team] = season

        h, a = ratings[row["home_team"]], ratings[row["away_team"]]
        season_hfa = config.hfa if hfa_by_season is None else hfa_by_season.get(season, config.hfa)
        hfa = 0.0 if row["neutral_site"] else season_hfa
        diff = h + hfa - a
        p_home = 1.0 / (10.0 ** (-diff / config.z) + 1.0)
        home_pre.append(h)
        away_pre.append(a)
        p_homes.append(p_home)

        margin = row["home_margin"]
        if margin is None:
            continue
        s_home = 1.0 if margin > 0 else 0.0 if margin < 0 else 0.5
        mult = 1.0
        if config.mov_mult:
            winner_diff = diff if margin > 0 else -diff
            mult = math.log(abs(margin) + 1.0) * 2.2 / (winner_diff * 0.001 + 2.2)
            mult = max(mult, 0.0)
        delta = config.k * mult * (s_home - p_home)
        ratings[row["home_team"]] = h + delta
        ratings[row["away_team"]] = a - delta

    return (
        ordered.with_columns(
            home_elo_pre=pl.Series(home_pre, dtype=pl.Float64),
            away_elo_pre=pl.Series(away_pre, dtype=pl.Float64),
            p_home=pl.Series(p_homes, dtype=pl.Float64),
        )
        .sort("__order")
        .drop("__order")
    )
