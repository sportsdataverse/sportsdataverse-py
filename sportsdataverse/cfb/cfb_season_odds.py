"""Ratings-driven season Monte Carlo for college football (T2.1 Phase 4).

Feeds a ratings-based ``compute_results`` closure into the shipped
:func:`sportsdataverse.cfb.cfb_simulations.cfb_simulations` engine (nflseedR-style
season/standings/CFP-bracket machinery) so conference-title / playoff / championship
odds fall out of the same predictors as the pregame model. The engine is reused
whole; this module only supplies the per-game result sampler and (Task 4.2) the
public wrapper.
"""

from __future__ import annotations

import numpy as np
import polars as pl
from scipy.stats import norm

from sportsdataverse.cfb.cfb_prediction_constants import get_constants
from sportsdataverse.cfb.cfb_simulations import ComputeResultsFn

__all__ = ["make_ratings_compute_results"]


def make_ratings_compute_results(ratings: pl.DataFrame, *, era: str = "modern") -> ComputeResultsFn:
    """Build a ``cfb_simulations`` ``compute_results`` closure from fixed ratings.

    The returned closure implements the engine's results contract -- ``(teams, games,
    week_num, *, rng, **kwargs) -> {"teams", "games"}`` -- filling every unplayed
    ``week == week_num`` game's ``result`` with a sampled home margin
    ``round(Normal(exp_margin, margin_sd))``, where ``exp_margin`` is
    :func:`cfb_game_predict.predict_margin` on the two teams' ``adj_net`` (home-field
    applied unless ``neutral``). Unlike the default elo sampler the ratings are
    **fixed**, so ``teams`` passes through unchanged (no elo update). Postseason games
    (``game_type != "REG"``) re-break a sampled tie by win probability.

    Args:
        ratings: A :func:`cfb_ratings.cfb_ratings`-style frame with ``team_id`` and
            ``adj_net``. Teams absent from it are treated as league-average (0.0).
        era: Era key into :data:`cfb_prediction_constants.CFB_CONSTANTS`.

    Returns:
        A ``compute_results`` callable suitable for ``cfb_simulations(...,
        compute_results=...)``.

    Example:
        Quick start::

            import numpy as np, polars as pl
            from sportsdataverse.cfb.cfb_season_odds import make_ratings_compute_results
            cr = make_ratings_compute_results(pl.DataFrame({"team_id": ["A", "B"], "adj_net": [0.3, -0.3]}))
            teams = pl.DataFrame({"sim": [1, 1], "team": ["A", "B"], "conference": ["X", "X"]})
            games = pl.DataFrame({"sim": [1], "week": [1], "home_team": ["A"], "away_team": ["B"],
                                  "neutral": [0], "result": [None]})
            cr(teams, games, 1, rng=np.random.default_rng(0))["games"]

    See Also:
        * `nflseedR <https://nflseedr.com>`_ -- the season-simulation engine reused here.
    """
    c = get_constants(era)
    net = dict(zip(ratings["team_id"].to_list(), ratings["adj_net"].to_list()))
    ns, hfa_epa, md = c.net_points_scale, c.hfa_epa, c.margin_sd

    def compute_results(
        teams: pl.DataFrame, games: pl.DataFrame, week_num: int, *, rng: np.random.Generator, **kwargs: object
    ) -> dict[str, pl.DataFrame]:
        g = games.with_columns(pl.col("result").cast(pl.Float64))
        hn = g["home_team"].replace_strict(net, default=0.0, return_dtype=pl.Float64).to_numpy()
        an = g["away_team"].replace_strict(net, default=0.0, return_dtype=pl.Float64).to_numpy()
        neutral = g["neutral"].to_numpy()

        # exp_margin = predict_margin(home_adj_net, away_adj_net, neutral), vectorized:
        # net_scale * (home_net - away_net + 2*hfa_epa on non-neutral fields).
        exp = ns * ((hn - an) + np.where(neutral == 1, 0.0, 2.0 * hfa_epa))
        n = g.height
        raw = exp + rng.normal(0.0, md, n)
        # round away from zero (matches the engine's _round_out margin convention).
        sim_result = np.where(raw > 0, np.ceil(raw), np.where(raw < 0, np.floor(raw), 0.0))

        if "game_type" in g.columns:
            gt = g["game_type"].to_numpy()
            wp = norm.cdf(exp / md)
            tie = (gt != "REG") & (sim_result == 0.0)
            sim_result = np.where(tie, np.where(rng.uniform(0.0, 1.0, n) < wp, 1.0, -1.0), sim_result)

        g = g.with_columns(pl.Series("_sim_result", sim_result, dtype=pl.Float64)).with_columns(
            pl.when((pl.col("week") == week_num) & pl.col("result").is_null())
            .then(pl.col("_sim_result"))
            .otherwise(pl.col("result"))
            .cast(pl.Float64)
            .alias("result")
        )
        return {"teams": teams, "games": g.select(games.columns)}

    return compute_results
