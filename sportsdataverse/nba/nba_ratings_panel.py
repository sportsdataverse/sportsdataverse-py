"""Through-date ratings panel — the WP4 daily-foundations engine.

``ratings_as_of`` is the single-date primitive: it filters possessions to
``game_date <= asof`` and reuses the model-validation harness's existing
``_fit_on`` dispatcher, so it works unchanged for every ``AnyModel`` the
harness already supports (RAPM/RidgeCV, BPM, SPM, Bayesian-prior models).
``nba_ratings_panel`` (Task 2) is a thin loop over this primitive per
checkpoint date.
"""

from __future__ import annotations

import datetime

import polars as pl

from sportsdataverse.nba.nba_model_validation import AnyModel, RatingsFit, _fit_on


def ratings_as_of(model: AnyModel, possessions: pl.DataFrame, asof: datetime.date) -> RatingsFit:
    """Fit ``model`` on every possession dated on or before ``asof`` and return ratings.

    This is the through-date primitive: possessions with ``game_date > asof``
    are excluded from the fit entirely (never merely down-weighted), which is
    what makes the panel built from repeated calls to this function leakage-free
    by construction — see ``tests/nba/test_nba_ratings_panel.py::test_ratings_as_of_is_leakage_free_append_invariant``.

    Args:
        model: A harness model conforming to ``nba_model_validation.AnyModel``
            (a ``RapmModel``, ``RatingsModel``, or ``PriorModel``).
        possessions: A possession+lineup frame that MUST carry a ``game_date``
            (``pl.Date``) column (as emitted by ``compile_nba_season``).
        asof: The through-date checkpoint (inclusive).

    Returns:
        ``RatingsFit`` with per-player offense/defense ratings (per-100-possession
        scale, same sign convention as ``nba_rapm``: positive ``d_ratings`` means
        good defense). Empty dicts when no possessions fall on or before ``asof``
        or when ``possessions`` is empty.

    Raises:
        ValueError: If ``possessions`` (non-empty) does not carry a ``game_date``
            column — this is a caller-contract violation, not a normal empty case,
            so it raises rather than silently returning empty ratings.

    Example:
        Through-date RAPM as of a single checkpoint::

            import datetime
            from sportsdataverse.nba.nba_model_validation import RidgeRapmModel
            from sportsdataverse.nba.nba_ratings_panel import ratings_as_of

            rf = ratings_as_of(RidgeRapmModel(), season_poss, datetime.date(2023, 12, 1))
            print(rf.o_ratings[201939])   # per-100 offensive rating through Dec 1

        See Also:
            * `nba_rapm`_ — the per-100 sign convention this function reuses.

        .. _nba_rapm: sportsdataverse.nba.nba_rapm
    """
    if not possessions.is_empty() and "game_date" not in possessions.columns:
        raise ValueError("possessions must carry a game_date column (see compile_nba_season)")
    window = possessions.filter(pl.col("game_date") <= asof) if not possessions.is_empty() else possessions
    fit, pids = _fit_on(model, window)
    if not pids:
        return RatingsFit(o_ratings={}, d_ratings={})
    P = len(pids)
    o_ratings = {int(p): float(fit.coef[k] * 100.0) for k, p in enumerate(pids)}
    d_ratings = {int(p): float(-fit.coef[P + k] * 100.0) for k, p in enumerate(pids)}
    return RatingsFit(o_ratings=o_ratings, d_ratings=d_ratings, posterior=fit.posterior)
