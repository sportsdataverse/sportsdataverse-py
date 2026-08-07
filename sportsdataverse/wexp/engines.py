"""Vintage-ratings engines (Axis A ridge core) + rating-to-WP maps (Axis E).

``ridge_margin_vintages`` refits the shared opponent-adjusted ridge
(:func:`sportsdataverse._common.ratings.opponent_adjusted_ridge`) per
``(season, week)`` over that season's completed prior games, emitting a
vintage-keyed ratings table for
:class:`~sportsdataverse.wexp.store.VintageStore`. The response column is
an argument: the home margin works from the market oracle alone; the
play-level EPA responses (Axis B weighting) plug in when the pbp vintage
captures land.

``ratings_predictor`` turns any registered ratings vintage into a week
predictor for :func:`~sportsdataverse.wexp.backtest.run_backtest` via the
Axis E map — ``margin_normal`` (E2, normal CDF at a tunable sigma) or
``isotonic`` (E3, refit each week on the history's own leak-free expected
margins).
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import polars as pl

from sportsdataverse._common.ratings import opponent_adjusted_ridge
from sportsdataverse.wexp.backtest import WeekPredictor, elo_predictor, normalize_walk_weeks
from sportsdataverse.wexp.elo import EloConfig
from sportsdataverse.wexp.store import VintageStore
from sportsdataverse.wexp.variants import VariantConfig

__all__ = ["build_predictor", "ratings_predictor", "ridge_margin_vintages"]

_VINTAGE_SCHEMA: dict[str, type[pl.DataType]] = {
    "season": pl.Int32,
    "as_of_week": pl.Int32,
    "team_id": pl.Utf8,
    "off_coef": pl.Float64,
    "def_coef": pl.Float64,
    "intercept": pl.Float64,
    "hfa": pl.Float64,
}


def ridge_margin_vintages(oracle: pl.DataFrame, *, lam: float, resp_col: str = "home_margin") -> pl.DataFrame:
    """Build per-week opponent-adjusted ridge rating vintages from an oracle frame.

    For every ``(season, week)`` present, fits the ridge on that season's
    completed games in strictly earlier (walk-normalized) weeks — so the
    vintage at ``as_of_week = W`` satisfies the store's EXCLUSIVE
    convention by construction. Weeks with no prior completed games (week
    1) emit no rows. Neutral-site games carry no HFA indicator in the fit.

    Args:
        oracle: Market-oracle frame (module contract columns).
        lam: Ridge penalty on the team coefficients.
        resp_col: Response column (default ``"home_margin"``; a team-game
            EPA margin column slots in unchanged).

    Returns:
        A vintage table — ``season`` / ``as_of_week`` / ``team_id`` (Utf8)
        / ``off_coef`` / ``def_coef`` plus the per-vintage ``intercept``
        and ``hfa`` scalars denormalized onto every row. Expected margin
        for (home h, away a): ``h.off_coef + a.def_coef + intercept
        [+ hfa]``.

    Example:
        Quick start::

            from sportsdataverse.wexp.engines import ridge_margin_vintages
            from sportsdataverse.wexp.store import VintageStore
            store = VintageStore()
            store.register("ridge", ridge_margin_vintages(oracle, lam=100.0), entity_key="team_id")
    """
    base = normalize_walk_weeks(oracle).filter(pl.col(resp_col).is_not_null())
    # Symmetrize: each game from both perspectives. With offense always the
    # home team the HFA indicator would be constant 1 and collinear with the
    # unpenalized intercept (singular normal equations); the mirrored away
    # view makes it vary and pins the intercept ~0 by construction.
    games = pl.concat(
        [
            base.select(
                "season",
                "week",
                off=pl.col("home_team"),
                deft=pl.col("away_team"),
                resp=pl.col(resp_col),
                homeflag=pl.when(pl.col("neutral_site") == True)  # noqa: E712
                .then(pl.lit(""))
                .otherwise(pl.col("home_team")),
            ),
            base.select(
                "season",
                "week",
                off=pl.col("away_team"),
                deft=pl.col("home_team"),
                resp=-pl.col(resp_col),
                homeflag=pl.lit(""),
            ),
        ]
    )
    frames: list[pl.DataFrame] = []
    for season, week in (
        normalize_walk_weeks(oracle).select("season", "week").unique().sort("season", "week").iter_rows()
    ):
        fit = games.filter((pl.col("season") == season) & (pl.col("week") < week))
        if fit.height == 0:
            continue
        coefs, intercept, hfa = opponent_adjusted_ridge(
            fit,
            off_col="off",
            def_col="deft",
            home_col="homeflag",
            resp_col="resp",
            lam=lam,
        )
        frames.append(
            coefs.with_columns(
                season=pl.lit(season, dtype=pl.Int32),
                as_of_week=pl.lit(week, dtype=pl.Int32),
                intercept=pl.lit(intercept, dtype=pl.Float64),
                hfa=pl.lit(hfa, dtype=pl.Float64),
            ).select(list(_VINTAGE_SCHEMA))
        )
    if not frames:
        return pl.DataFrame(schema=_VINTAGE_SCHEMA)
    return pl.concat(frames, how="vertical")


def ratings_predictor(
    table: str,
    *,
    wp_map: str = "margin_normal",
    sigma: float = 13.45,
    iso_min_fit: int = 100,
) -> WeekPredictor:
    """Wrap a registered ratings vintage table as a week predictor.

    Joins home and away ratings through the store's leak-free
    ``join_asof``, reconstructs the expected home margin, and maps it to
    P(home) per the Axis E ``wp_map``. Games where either team lacks a
    served vintage get a null probability (scored-on-coverage, never
    imputed).

    Args:
        table: Table name registered on the store
            (:func:`ridge_margin_vintages` schema).
        wp_map: ``"margin_normal"`` (E2) or ``"isotonic"`` (E3,
            walk-forward refit on history each week).
        sigma: Margin SD for the normal link.
        iso_min_fit: Minimum history games with a rated margin before the
            isotonic map will fit; below it the week predicts null.

    Returns:
        A predictor callable for :func:`~sportsdataverse.wexp.backtest.run_backtest`.

    Raises:
        ValueError: If ``wp_map`` is unknown, or the predictor is invoked
            without a store.

    Example:
        Quick start::

            from sportsdataverse.wexp.backtest import run_backtest
            from sportsdataverse.wexp.engines import ratings_predictor
            probs, rows = run_backtest(oracle, ratings_predictor("ridge"),
                                       model_id="ridge_margin", store=store)
    """
    if wp_map not in ("margin_normal", "isotonic"):
        raise ValueError(f"unknown wp_map {wp_map!r}; one of ('margin_normal', 'isotonic')")

    def _expected_margin(games: pl.DataFrame, store: VintageStore) -> pl.DataFrame:
        g = store.join_asof(games, table, on={"home_team": "team_id"}, prefix="rt_home_")
        g = store.join_asof(g, table, on={"away_team": "team_id"}, prefix="rt_away_")
        hfa = pl.when(pl.col("neutral_site") == True).then(0.0).otherwise(pl.col("rt_home_hfa"))  # noqa: E712
        return g.with_columns(
            __exp_margin=pl.col("rt_home_off_coef") + pl.col("rt_away_def_coef") + pl.col("rt_home_intercept") + hfa
        )

    def predict(history: pl.DataFrame, slate: pl.DataFrame, store: Optional[VintageStore]) -> pl.Series:
        if store is None:
            raise ValueError("ratings_predictor requires a VintageStore")
        margins = _expected_margin(slate, store)["__exp_margin"].to_numpy()
        if wp_map == "margin_normal":
            from scipy.stats import norm

            return pl.Series(norm.cdf(margins / sigma)).fill_nan(None)
        fit = _expected_margin(history, store).drop_nulls(["__exp_margin", "home_win"])
        if fit.height < iso_min_fit:
            return pl.Series([None] * slate.height, dtype=pl.Float64)
        from sklearn.isotonic import IsotonicRegression

        iso = IsotonicRegression(y_min=0.01, y_max=0.99, out_of_bounds="clip", increasing=True)
        iso.fit(fit["__exp_margin"].to_numpy(), fit["home_win"].to_numpy())
        # sklearn rejects NaN at predict; unrated games stay null
        out = np.full(len(margins), np.nan)
        rated = ~np.isnan(margins)
        if rated.any():
            out[rated] = iso.predict(margins[rated])
        return pl.Series(out).fill_nan(None)

    return predict


def build_predictor(config: VariantConfig, *, table: str = "ridge", sigma: float = 13.45) -> WeekPredictor:
    """Dispatch a variant config to its implemented week predictor.

    Implemented cells: ``elo_margin`` (prior ``flat`` = full season reset,
    ``carryover`` = the ``carryover`` tunable; ``wp_map`` must be
    ``elo_logistic``) and ``ridge_epa`` with ``response="raw"`` /
    ``opponent_adjust="ridge"`` / ``prior="flat"`` served from a
    registered ridge vintage table with ``wp_map`` in ``margin_normal`` /
    ``isotonic``. Every other valid config cell raises
    ``NotImplementedError`` until its engine lands — never a silent
    fallback to a different model.

    Args:
        config: The variant to build.
        table: Ratings vintage table name (EPA-family cores).
        sigma: Default margin SD for the normal link (``sigma`` in
            ``config.params`` wins).

    Returns:
        A predictor callable for :func:`~sportsdataverse.wexp.backtest.run_backtest`.

    Raises:
        NotImplementedError: For a valid config cell whose engine has not
            landed yet.

    Example:
        Quick start::

            from sportsdataverse.wexp.engines import build_predictor
            from sportsdataverse.wexp.variants import VariantConfig
            predict = build_predictor(VariantConfig(
                core="elo_margin", response="raw", opponent_adjust="none",
                prior="carryover", wp_map="elo_logistic", hfa="fixed"))
    """
    params = dict(config.params)
    if config.core == "elo_margin" and config.wp_map == "elo_logistic" and config.hfa == "fixed":
        carryover = 0.0 if config.prior == "flat" else params.get("carryover", 0.67)
        return elo_predictor(
            EloConfig(
                k=params.get("k", 20.0),
                z=params.get("z", 400.0),
                hfa=params.get("hfa", 65.0),
                carryover=carryover,
            )
        )
    if (
        config.core == "ridge_epa"
        and config.response == "raw"
        and config.opponent_adjust == "ridge"
        and config.prior == "flat"
        and config.hfa == "fixed"
        and config.wp_map in ("margin_normal", "isotonic")
    ):
        return ratings_predictor(table, wp_map=config.wp_map, sigma=params.get("sigma", sigma))
    raise NotImplementedError(f"no engine landed yet for variant {config}")
