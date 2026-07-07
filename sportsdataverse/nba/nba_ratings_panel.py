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
from typing import Optional, Sequence, Union

import pandas as pd
import polars as pl

from sportsdataverse.nba.nba_model_validation import AnyModel, RatingsFit, _fit_on

#: Schema of the per-(player, date) long frame from :func:`nba_ratings_panel`.
RATINGS_PANEL_SCHEMA: dict[str, pl.DataType] = {
    "player_id": pl.Int64,
    "date": pl.Date,
    "o_rating": pl.Float64,
    "d_rating": pl.Float64,
    "rating": pl.Float64,
}


def ratings_as_of(model: AnyModel, possessions: pl.DataFrame, asof: datetime.date) -> RatingsFit:
    """Fit ``model`` on every possession dated on or before ``asof`` and return ratings.

    This is the through-date primitive: possessions with ``game_date > asof``
    are excluded from the fit entirely (never merely down-weighted), which is
    what makes the panel built from repeated calls to this function leakage-free
    by construction — see ``tests/nba/test_nba_ratings_panel.py::test_ratings_as_of_is_leakage_free_append_invariant``.
    NOTE: the leakage property is proven by the append-invariance test TOGETHER
    with the panel's per-date-parity test — neither alone covers
    cross-checkpoint-window leaks; do not prune one without the other.

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


def _empty_panel_frame() -> pl.DataFrame:
    return pl.DataFrame({c: pl.Series([], dtype=t) for c, t in RATINGS_PANEL_SCHEMA.items()})


def nba_ratings_panel(
    model: AnyModel,
    possessions: pl.DataFrame,
    dates: Optional[Sequence[datetime.date]] = None,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Player-ratings-through-date long panel: one row per (player_id, date).

    Refit-per-checkpoint (v1; no warm-start incrementality) — each date's row
    calls :func:`ratings_as_of` independently, so the panel is leakage-free by
    construction: a possession dated after a given checkpoint can never affect
    that checkpoint's row, no matter what other dates are also being computed
    or what future rows exist in ``possessions``. Cost is a full refit per
    checkpoint date; for a season's sparse RAPM-family design this is seconds
    per date, not minutes — acceptable for a nightly/daily cadence but not for
    live in-game updating (out of scope; see spec non-goals).

    Args:
        model: A harness model conforming to ``nba_model_validation.AnyModel``.
        possessions: A possession+lineup frame with a ``game_date`` (``pl.Date``)
            column (as emitted by ``compile_nba_season``).
        dates: Checkpoint dates to compute. ``None`` (default) uses every
            distinct ``game_date`` present in ``possessions``, sorted ascending
            — a rating for every game day, matching what EPM/LEBRON publish
            nightly. Duplicates are deduped; input order does not matter (the
            output is always sorted by date).
        return_as_pandas: Return pandas instead of polars.

    Returns:
        Long frame with :data:`RATINGS_PANEL_SCHEMA` columns
        (``player_id``, ``date``, ``o_rating``, ``d_rating``, ``rating``).
        Zero-row (that schema) when ``possessions`` is empty or no date yields
        any players.

    Raises:
        ValueError: If ``possessions`` (non-empty) lacks a ``game_date`` column.

    Example:
        Panel over a hand-picked checkpoint grid::

            import datetime
            from sportsdataverse.nba.nba_model_validation import RidgeRapmModel
            from sportsdataverse.nba.nba_ratings_panel import nba_ratings_panel

            checkpoints = [datetime.date(2023, 11, 1), datetime.date(2023, 12, 1)]
            panel = nba_ratings_panel(RidgeRapmModel(), season_poss, dates=checkpoints)
            print(panel.filter(pl.col("player_id") == 201939).sort("date"))

        Every game day, no explicit grid::

            panel = nba_ratings_panel(RidgeRapmModel(), season_poss)

        See Also:
            * `ratings_as_of`_ — the single-date primitive this loops over.

        .. _ratings_as_of: sportsdataverse.nba.nba_ratings_panel.ratings_as_of
    """
    if possessions.is_empty():
        empty = _empty_panel_frame()
        return empty.to_pandas() if return_as_pandas else empty
    if "game_date" not in possessions.columns:
        raise ValueError("possessions must carry a game_date column (see compile_nba_season)")

    checkpoint_dates = (
        sorted(possessions["game_date"].unique().drop_nulls().to_list()) if dates is None else sorted(set(dates))
    )

    rows: list[dict[str, object]] = []
    for d in checkpoint_dates:
        rf = ratings_as_of(model, possessions, d)
        for pid in sorted(set(rf.o_ratings) | set(rf.d_ratings)):
            o = rf.o_ratings.get(pid, 0.0)
            dd = rf.d_ratings.get(pid, 0.0)
            rows.append({"player_id": pid, "date": d, "o_rating": o, "d_rating": dd, "rating": o + dd})

    out = pl.DataFrame(rows, schema=RATINGS_PANEL_SCHEMA) if rows else _empty_panel_frame()
    return out.to_pandas() if return_as_pandas else out
