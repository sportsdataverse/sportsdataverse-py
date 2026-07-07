"""RAPM variants over the shared possession design: luck-adjusted, four-factor, time-decay.

All three variants reuse :func:`sportsdataverse.nba.nba_rapm.build_rapm_design`
unchanged and differ only in the response vector and/or per-possession sample
weights. :mod:`sportsdataverse.nba.nba_rapm` is intentionally left untouched.

**Ridge-schedule divergence from** ``nba_rapm`` **(binding, 2026-07-03 WP2 plan
ruling, decision #8):** the RAPM *variants* (luck-adjusted / four-factor /
time-decay, added in later tasks of this module) fit against the **oracle**
regularization schedule — Ryan Davis's reference RAPM implementation's 3-point
lambda grid ``[0.01, 0.05, 0.1]`` converted to sklearn ``alpha`` via
``alpha = lambda * n_samples / 2`` (see :func:`oracle_rapm_alphas`), combined
with explicit 5-fold cross-validation (``cv=5``, see :data:`ORACLE_RAPM_CV`) —
**not** :data:`sportsdataverse.nba.nba_rapm.DEFAULT_RAPM_ALPHAS`'s 8-point
``logspace(2, 5)`` grid with sklearn's default efficient LOOCV (``cv=None``).
Plain ``nba_rapm`` keeps its own settled convention unchanged.

**``n_samples`` is the design-matrix row count (possessions), NOT the player
count** — verified against the oracle source
(``NBA_Tutorials_Ryan_Davis/rapm/rapm.py:112-125``):
``lambda_to_alpha(lambda_value, samples) = (lambda_value * samples) / 2.0``,
called as ``lambda_to_alpha(l, train_x.shape[0])`` where ``train_x`` is the
possession-level design matrix, so ``samples`` is ``X.shape[0]`` (possessions),
never ``len(player_ids)``.

The shared :func:`_fit_weighted` engine below stays a generic ``(alphas, cv)``
knob so a **single** fitting routine serves both conventions: its defaults
(``alphas=DEFAULT_RAPM_ALPHAS``, ``cv=None``) intentionally reproduce
``nba_rapm`` byte-for-byte (see ``test_fit_weighted_equals_plain_rapm_on_points``
in the test module — a scaffold-only invariant, since plain RAPM has no
weights or alternate response to justify diverging from it); the variant
functions added on top of this scaffold pass ``alphas=oracle_rapm_alphas(n)``
(``n`` = possession count) and ``cv=ORACLE_RAPM_CV`` explicitly, per the
ruling above.
"""

from __future__ import annotations

import datetime
from typing import Optional, Sequence, Union

import numpy as np
import pandas as pd
import polars as pl
from scipy.sparse import csr_matrix
from sklearn.linear_model import RidgeCV

from sportsdataverse.nba.nba_rapm import DEFAULT_RAPM_ALPHAS, build_rapm_design

_OFF: list[str] = [f"off_player_{i}" for i in range(1, 6)]
_DEF: list[str] = [f"def_player_{i}" for i in range(1, 6)]

#: Ryan Davis's oracle RAPM lambda grid (``rapm/rapm.py``), 3 points.
#: Converted to sklearn's ``alpha`` scale per possession (sample) count via
#: :func:`oracle_rapm_alphas` — the oracle's ``lambda_to_alpha(l, n) = l * n / 2``
#: where ``n`` is the number of possessions (design-matrix rows), NOT players.
ORACLE_RAPM_LAMBDAS: tuple[float, ...] = (0.01, 0.05, 0.1)

#: Oracle RidgeCV fold count (explicit 5-fold, NOT sklearn's default LOOCV).
ORACLE_RAPM_CV: int = 5


def oracle_rapm_alphas(
    n_samples: int,
    lambdas: Sequence[float] = ORACLE_RAPM_LAMBDAS,
) -> np.ndarray:
    """Convert the oracle's lambda grid to sklearn ``alpha`` values for *n_samples*.

    Ryan Davis's reference RAPM scales the ridge penalty by the number of
    **possessions** (design-matrix rows, i.e. regression samples) —
    ``NBA_Tutorials_Ryan_Davis/rapm/rapm.py:112-125``:
    ``lambda_to_alpha(lambda_value, samples) = (lambda_value * samples) / 2.0``,
    invoked as ``lambda_to_alpha(l, train_x.shape[0])`` where ``train_x`` is the
    possession-level design matrix. So ``samples`` is ``X.shape[0]``
    (possessions) — **not** the player count ``P``. Unlike
    :data:`~sportsdataverse.nba.nba_rapm.DEFAULT_RAPM_ALPHAS` this grid is
    **not** a fixed array — it must be recomputed per design.

    Args:
        n_samples: Number of possessions (design-matrix rows) in the design,
            i.e. ``X.shape[0]`` / ``len(y)`` from :func:`_prepare` /
            :func:`build_rapm_design`.
        lambdas: Lambda grid to convert. Defaults to :data:`ORACLE_RAPM_LAMBDAS`.

    Returns:
        Float64 array of RidgeCV ``alphas``, same length as *lambdas*.

    Example:
        Oracle-schedule alphas for a 50 000-possession design::

            from sportsdataverse.nba.nba_rapm_variants import oracle_rapm_alphas
            alphas = oracle_rapm_alphas(50_000)
            print(alphas)  # array([ 250., 1250., 2500.])

        See Also:
            * `NBA_Tutorials (Ryan Davis)`_ — the oracle ``rapm.py`` reference implementation
            * `nba_api`_ — upstream play-by-play source

        .. _NBA_Tutorials (Ryan Davis): https://github.com/rd11490/NBA_Tutorials
        .. _nba_api: https://github.com/swar/nba_api
    """
    return np.asarray([lam * n_samples / 2.0 for lam in lambdas], dtype=np.float64)


def decay_weights(
    game_date: pl.Series,
    asof: Optional[datetime.date],
    half_life_days: float,
) -> np.ndarray:
    """Exponential time-decay sample weights ``w = 0.5 ** (days_ago / half_life)``.

    Args:
        game_date: Per-possession game dates (``pl.Date`` Series), aligned
            row-for-row with the design the weights will be applied to.
        asof: Reference "today". ``None`` disables decay (all weights ``1.0``).
            Games dated after ``asof`` are clamped to ``days_ago = 0`` (weight
            ``1.0``); callers that want a strict as-of cutoff must filter first.
        half_life_days: Days at which a possession's weight halves. Must be > 0.

    Returns:
        Float64 array of weights, one per row of ``game_date``.

    Example:
        Down-weight month-old possessions by half::

            import datetime
            import polars as pl
            from sportsdataverse.nba.nba_rapm_variants import decay_weights

            dates = pl.Series("game_date", [datetime.date(2023, 1, 1)])
            w = decay_weights(dates, datetime.date(2023, 1, 31), half_life_days=30.0)
            print(round(float(w[0]), 3))  # 0.5

        See Also:
            * `nba_api`_ — upstream play-by-play source (``game_date`` provenance)

        .. _nba_api: https://github.com/swar/nba_api
    """
    n = game_date.len()
    if asof is None or half_life_days <= 0:
        return np.ones(n, dtype=np.float64)
    days = game_date.to_numpy()
    asof_np = np.datetime64(asof)
    days_ago = (asof_np - days).astype("timedelta64[D]").astype(np.float64)
    days_ago = np.clip(days_ago, 0.0, None)
    return np.power(0.5, days_ago / float(half_life_days)).astype(np.float64)


def _prepare_design(possessions: pl.DataFrame) -> tuple[csr_matrix, pl.DataFrame, list[int]]:
    """Null-drop the lineup columns and build the shared design ``X`` ONCE.

    Factored out of :func:`_prepare` so a caller that needs to fit MULTIPLE
    responses against the identical design (:func:`nba_four_factor_rapm`, four
    factors) can build it a single time and read each response straight off
    the returned ``kept`` frame -- :func:`build_rapm_design` depends only on
    the lineup columns, never on the response, so rebuilding it per response
    is pure waste (it was previously rebuilt once per factor: one useful call
    plus three discarded ones).

    Args:
        possessions: Possession+lineup frame (``off_player_1..5``,
            ``def_player_1..5``, ``points`` -- see :func:`build_rapm_design`).

    Returns:
        ``(X, kept, pids)``. ``kept`` is the null-lineup-dropped frame,
        row-aligned to ``X`` -- callers read any per-possession response/weight
        column directly off of it. Empty / all-null-lineup input ->
        ``(csr_matrix((0, 0)), <empty frame>, [])``.
    """
    if possessions.is_empty():
        return csr_matrix((0, 0)), possessions, []
    kept = possessions.drop_nulls(subset=_OFF + _DEF)
    if kept.is_empty():
        return csr_matrix((0, 0)), kept, []
    X, _y_points, pids = build_rapm_design(kept)
    return X, kept, pids


def _prepare(
    possessions: pl.DataFrame,
    response_col: str,
    *,
    weight_col: Optional[str] = None,
) -> tuple[csr_matrix, np.ndarray, Optional[np.ndarray], list[int]]:
    """Build the shared design ``X`` with an externally-supplied, row-aligned response.

    Drops null-lineup rows ONCE (identical subset to ``build_rapm_design``) so the
    response/weight columns stay aligned to the surviving design rows, then builds
    ``X`` from the dropped frame (its internal re-drop is then a no-op that
    preserves row order). Delegates the null-drop + design build to
    :func:`_prepare_design`.

    Args:
        possessions: Possession+lineup frame carrying ``response_col`` (and
            ``weight_col`` when given) as extra columns.
        response_col: Column name of the per-possession regression target.
        weight_col: Optional per-possession sample-weight column.

    Returns:
        ``(X, y, w, pids)`` where ``w`` is ``None`` when ``weight_col`` is ``None``.
        Empty / all-null-lineup input -> ``(csr_matrix((0, 0)), empty, None, [])``.
    """
    X, kept, pids = _prepare_design(possessions)
    if not pids:
        return X, np.empty(0, dtype=np.float64), None, []
    y = kept[response_col].to_numpy().astype(np.float64)
    w = kept[weight_col].to_numpy().astype(np.float64) if weight_col is not None else None
    assert X.shape[0] == len(y), (X.shape, len(y))  # alignment contract
    return X, y, w, pids


def _fit_weighted(
    X: csr_matrix,
    y: np.ndarray,
    *,
    weights: Optional[np.ndarray] = None,
    alphas: np.ndarray = DEFAULT_RAPM_ALPHAS,
    cv: Optional[int] = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Fit a (optionally weighted) RidgeCV and return per-100 offense/defense + poss counts.

    Sign convention matches :func:`~sportsdataverse.nba.nba_rapm.nba_rapm`:
    ``d`` is the NEGATED raw defense coefficient x 100 (positive = good defender).

    This is the **shared** fitting engine for every RAPM variant in this module.
    Its defaults (``alphas=DEFAULT_RAPM_ALPHAS``, ``cv=None`` i.e. sklearn's
    efficient LOOCV) intentionally reproduce plain ``nba_rapm``'s own fit when
    called unweighted with no overrides. RAPM variants call this with
    ``alphas=oracle_rapm_alphas(n_samples)`` (``n_samples`` = possession /
    design-row count, NOT player count) and ``cv=ORACLE_RAPM_CV`` per the
    binding oracle-ridge-schedule ruling documented in the module docstring.

    Args:
        X: Sparse design ``(n, 2P)`` from :func:`_prepare`.
        y: Row-aligned response ``(n,)``.
        weights: Optional per-possession sample weights ``(n,)``.
        alphas: RidgeCV alpha grid.
        cv: Cross-validation fold count forwarded to ``RidgeCV(cv=...)``.
            ``None`` (default) uses sklearn's efficient LOOCV -- the plain-
            ``nba_rapm`` convention. Pass :data:`ORACLE_RAPM_CV` (``5``) for
            the oracle schedule used by the RAPM variants.

    Returns:
        ``(o_per100, d_per100, off_poss, def_poss)`` each shape ``(P,)``.
    """
    P = X.shape[1] // 2
    model = RidgeCV(alphas=alphas, fit_intercept=True, cv=cv)
    # RidgeCV.fit accepts sample_weight with a sparse csr design under the default
    # ("auto") solver; verified by test_fit_weighted_honors_weights. If a future
    # sklearn drops sparse+weight support, fall back to Ridge + an explicit KFold
    # alpha loop (see spec Sec5 caveat) -- the public surface is unchanged.
    model.fit(X, y, sample_weight=weights)
    coef = np.asarray(model.coef_, dtype=np.float64)
    o = coef[:P] * 100.0
    d = -coef[P:] * 100.0
    col_sums = np.asarray(X.sum(axis=0), dtype=np.float64).ravel()
    return o, d, col_sums[:P].astype(np.int64), col_sums[P:].astype(np.int64)


#: Output schema for :func:`nba_decay_rapm`.
DECAY_RAPM_SCHEMA: dict[str, pl.DataType] = {
    "player_id": pl.Int64,
    "o_decay_rapm": pl.Float64,
    "d_decay_rapm": pl.Float64,
    "decay_rapm": pl.Float64,
    "off_poss": pl.Int64,
    "def_poss": pl.Int64,
}


def _empty(schema: dict[str, pl.DataType]) -> pl.DataFrame:
    """Zero-row frame with exactly ``schema``."""
    return pl.DataFrame({c: pl.Series([], dtype=t) for c, t in schema.items()})


def nba_decay_rapm(
    possessions: pl.DataFrame,
    *,
    asof: Optional[datetime.date] = None,
    half_life_days: float = 180.0,
    alphas: Optional[np.ndarray] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Time-decay RAPM: ridge weighted by ``0.5 ** (days_ago / half_life_days)``.

    ``asof=None`` disables decay: every possession is weighted ``1.0`` and the
    fit uses **exactly** plain :func:`~sportsdataverse.nba.nba_rapm.nba_rapm`'s
    own schedule (``alphas=DEFAULT_RAPM_ALPHAS``, sklearn's efficient default
    LOOCV) so the two agree byte-for-byte (see
    ``test_decay_rapm_asof_none_equals_plain_rapm``). When ``asof`` is set,
    possessions dated after ``asof`` are dropped, the remainder is
    exponentially down-weighted by age, and the fit switches to the
    **oracle** regularization schedule (:func:`oracle_rapm_alphas` evaluated
    at the post-filter possession count, ``cv=`` :data:`ORACLE_RAPM_CV`) per
    the binding WP2 ridge-schedule ruling documented in the module docstring.

    .. note::
        **Deviation from the task interface sketch**: the brief's draft
        signature defaulted ``alphas=DEFAULT_RAPM_ALPHAS`` unconditionally,
        which is exactly the schedule that :func:`_fit_weighted` already uses
        when nothing is overridden -- fine for the ``asof=None`` branch, but
        it would silently skip the oracle schedule the binding ruling
        requires for the decay-weighted branch. This function instead
        defaults ``alphas=None`` and auto-selects the schedule per branch
        (described above); passing an explicit ``alphas`` array overrides
        the auto-selection in either branch.

    Args:
        possessions: Multi-season possession+lineup frame. Must carry a
            ``game_date`` (``pl.Date``) column when ``asof`` is not ``None``.
        asof: Reference date; ``None`` -> unweighted, plain-RAPM-equivalent fit.
        half_life_days: Weight half-life in days (default 180).
        alphas: Optional RidgeCV alpha grid override. ``None`` (default)
            auto-selects :data:`~sportsdataverse.nba.nba_rapm.DEFAULT_RAPM_ALPHAS`
            when ``asof is None`` or :func:`oracle_rapm_alphas` (evaluated at
            the possession count) when ``asof`` is set.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        Frame with :data:`DECAY_RAPM_SCHEMA`. Empty input, or an ``asof`` that
        drops every possession, -> zero-row frame.

    Raises:
        ValueError: ``asof`` is not ``None`` but *possessions* lacks a
            ``game_date`` column.

    Example:
        Recency-weighted ratings as of a date::

            import datetime
            from sportsdataverse.nba.nba_rapm_variants import nba_decay_rapm

            df = nba_decay_rapm(season_poss, asof=datetime.date(2024, 3, 1), half_life_days=120.0)
            print(df.sort("decay_rapm", descending=True).head())

        Plain-RAPM-equivalent (no decay)::

            df = nba_decay_rapm(season_poss)  # asof=None

        See Also:
            * `NBA_Tutorials (Ryan Davis)`_ — the oracle ridge-schedule this variant fits against
            * `nba_api`_ — upstream play-by-play source

        .. _NBA_Tutorials (Ryan Davis): https://github.com/rd11490/NBA_Tutorials
        .. _nba_api: https://github.com/swar/nba_api
    """
    if possessions.is_empty():
        out = _empty(DECAY_RAPM_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    frame = possessions
    weight_col: Optional[str] = None
    if asof is not None:
        if "game_date" not in frame.columns:
            raise ValueError("nba_decay_rapm(asof=...) requires a 'game_date' column")
        frame = frame.filter(pl.col("game_date") <= asof)
        if frame.is_empty():
            out = _empty(DECAY_RAPM_SCHEMA)
            return out.to_pandas() if return_as_pandas else out
        w = decay_weights(frame["game_date"], asof, half_life_days)
        frame = frame.with_columns(pl.Series("_w", w))
        weight_col = "_w"

    X, y, wv, pids = _prepare(frame, "points", weight_col=weight_col)
    if not pids:
        out = _empty(DECAY_RAPM_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    if asof is None:
        # Plain-RAPM-equivalent branch: reproduce nba_rapm's own schedule exactly.
        fit_alphas = alphas if alphas is not None else DEFAULT_RAPM_ALPHAS
        fit_cv: Optional[int] = None
    else:
        # Decay-weighted branch: oracle regularization schedule (binding WP2 ruling),
        # evaluated at the post-filter possession count (X.shape[0]), NOT player count.
        fit_alphas = alphas if alphas is not None else oracle_rapm_alphas(X.shape[0])
        fit_cv = ORACLE_RAPM_CV

    o, d, off_poss, def_poss = _fit_weighted(X, y, weights=wv, alphas=fit_alphas, cv=fit_cv)
    out = pl.DataFrame(
        {
            "player_id": pl.Series(pids, dtype=pl.Int64),
            "o_decay_rapm": pl.Series(o, dtype=pl.Float64),
            "d_decay_rapm": pl.Series(d, dtype=pl.Float64),
            "decay_rapm": pl.Series(o + d, dtype=pl.Float64),
            "off_poss": pl.Series(off_poss, dtype=pl.Int64),
            "def_poss": pl.Series(def_poss, dtype=pl.Int64),
        }
    ).sort("player_id")
    return out.to_pandas() if return_as_pandas else out


def _shrunk_shooter_rates(
    shooting: pl.DataFrame,
    *,
    fg3_k: float = 100.0,
    ft_k: float = 50.0,
) -> pl.DataFrame:
    """Per-shooter 3P% / FT% shrunk toward the pooled league mean (empirical-Bayes flavor).

    ``p̂ = (makes + k * lg_mean) / (attempts + k)`` with pseudo-count ``k`` in shots.
    **DECISION 3**: the ``fg3_k`` / ``ft_k`` defaults and this estimator form are
    the plan's v1 default, not an oracle-verified match.

    Args:
        shooting: Per-(possession, shooter) frame (WP1 ``build_possession_shooting``).
        fg3_k: 3-point shrinkage pseudo-count (shots).
        ft_k: Free-throw shrinkage pseudo-count (shots).

    Returns:
        Frame ``player_id: Int64, p3: Float64, pft: Float64``. Empty input →
        zero-row frame with that schema.

    Example:
        Shrink raw shooter rates toward the league mean::

            from sportsdataverse.nba.nba_rapm_variants import _shrunk_shooter_rates
            rates = _shrunk_shooter_rates(shooting_df, fg3_k=100.0, ft_k=50.0)
            print(rates.columns)  # ['player_id', 'p3', 'pft']
    """
    schema = {"player_id": pl.Int64, "p3": pl.Float64, "pft": pl.Float64}
    if shooting.is_empty():
        return _empty(schema)
    agg = shooting.group_by("player_id").agg(
        pl.col("fg3a").sum(), pl.col("fg3m").sum(), pl.col("fta").sum(), pl.col("ftm").sum()
    )
    tot3a, tot3m = int(agg["fg3a"].sum()), int(agg["fg3m"].sum())
    totfta, totftm = int(agg["fta"].sum()), int(agg["ftm"].sum())
    lg3 = tot3m / tot3a if tot3a else 0.0
    lgft = totftm / totfta if totfta else 0.0
    return agg.select(
        pl.col("player_id").cast(pl.Int64),
        ((pl.col("fg3m") + fg3_k * lg3) / (pl.col("fg3a") + fg3_k)).cast(pl.Float64).alias("p3"),
        ((pl.col("ftm") + ft_k * lgft) / (pl.col("fta") + ft_k)).cast(pl.Float64).alias("pft"),
    )


def luck_adjusted_response(
    possessions: pl.DataFrame,
    shooting: pl.DataFrame,
    player_rates: Optional[dict[int, tuple[float, float]]] = None,
    *,
    fg3_k: float = 100.0,
    ft_k: float = 50.0,
) -> pl.DataFrame:
    """Attach a per-possession ``la_points`` expected-points response.

    **DECISION 2/4**: ``la_points = 2*fg2m + 3*Σ_shooter fg3a·p̂3 + Σ_shooter fta·p̂ft``
    (offense-only, "one_way"). 2-pt makes stay realized. ``p̂`` come from
    ``player_rates`` when given, else :func:`_shrunk_shooter_rates` on ``shooting``.

    **Defense-shooter exclusion (bugfix)**: ``shooting``
    (:func:`~sportsdataverse.nba.nba_possessions.build_possession_shooting`)
    deliberately retains defense-team shooters in a possession group — e.g. a
    defensive technical free throw shooter — because it is a per-shooter
    companion frame, not a team-attributed one (that's why it carries its own
    ``team_id`` column). The expected-points sum is offense-only by
    definition (**DECISION 2**), so *before* aggregating ``exp_extra`` this
    function joins ``possessions[["game_id", "possession_number",
    "offense_team_id"]]`` onto ``shooting`` and filters to
    ``team_id == offense_team_id``, dropping any defense-team shooter row.
    Without this filter a defense tech-FT's ``fta·p̂ft`` term leaks into the
    offense's ``la_points``, inflating it (reproduced: 2.9 vs. the
    offense-only-correct 2.0 for a single offense 2-pt make plus one defense
    tech FT).

    Args:
        possessions: Possession+lineup frame carrying team-level ``fg2m`` and the
            join keys ``game_id`` + ``possession_number`` + ``offense_team_id``.
        shooting: Per-(possession, shooter) frame (``build_possession_shooting``),
            which may include defense-team shooter rows (e.g. technical FTs) —
            filtered out here before aggregation.
        player_rates: Optional ``{player_id: (p3, pft)}`` override (e.g. planted
            truth in tests); ``None`` → shrink from ``shooting``.
        fg3_k: 3-point shrinkage pseudo-count, forwarded to
            :func:`_shrunk_shooter_rates` when ``player_rates`` is ``None``.
        ft_k: Free-throw shrinkage pseudo-count, forwarded to
            :func:`_shrunk_shooter_rates` when ``player_rates`` is ``None``.

    Returns:
        ``possessions`` with an added ``la_points: Float64`` column (same rows,
        same order). Empty ``possessions`` → returned unchanged with an empty
        ``la_points`` column.

    Example:
        Expected-points response with shrunk shooter rates::

            from sportsdataverse.nba.nba_rapm_variants import luck_adjusted_response
            out = luck_adjusted_response(possessions_df, shooting_df)
            print(out["la_points"].mean())

        Planted-truth override for testing::

            out = luck_adjusted_response(possessions_df, shooting_df, {7: (0.4, 0.8)})

        See Also:
            * `nba_api`_ — upstream play-by-play / shooting source
            * `hoopR`_ — R-side possession + shot-detail parity

        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    if possessions.is_empty():
        return possessions.with_columns(pl.Series("la_points", [], dtype=pl.Float64))

    if player_rates is not None:
        rates = pl.DataFrame(
            {
                "player_id": pl.Series([int(k) for k in player_rates], dtype=pl.Int64),
                "p3": pl.Series([float(v[0]) for v in player_rates.values()], dtype=pl.Float64),
                "pft": pl.Series([float(v[1]) for v in player_rates.values()], dtype=pl.Float64),
            }
        )
    else:
        rates = _shrunk_shooter_rates(shooting, fg3_k=fg3_k, ft_k=ft_k)

    assert possessions.schema["game_id"] == pl.Utf8
    if not shooting.is_empty():
        assert shooting.schema["player_id"] == rates.schema["player_id"]  # Int64 both sides

    # per-(game, possession) expected 3pt + ft contributions, OFFENSE SHOOTERS ONLY:
    # shooting (build_possession_shooting) intentionally retains defense-team shooters
    # (e.g. a defensive technical FT) alongside their own team_id -- join the offense
    # team id onto each shooting row and filter to team_id == offense_team_id BEFORE
    # aggregating, so a defense tech-FT never leaks into this offense-only response.
    if shooting.is_empty():
        contrib = pl.DataFrame(schema={"game_id": pl.Utf8, "possession_number": pl.Int64, "exp_extra": pl.Float64})
    else:
        offense_side = possessions.select("game_id", "possession_number", "offense_team_id")
        joined = (
            shooting.join(offense_side, on=["game_id", "possession_number"], how="left")
            .filter(pl.col("team_id") == pl.col("offense_team_id"))
            .join(rates, on="player_id", how="left")
            .with_columns(pl.col("p3").fill_null(0.0), pl.col("pft").fill_null(0.0))
        )
        contrib = joined.group_by(["game_id", "possession_number"]).agg(
            (3.0 * (pl.col("fg3a") * pl.col("p3")).sum() + (pl.col("fta") * pl.col("pft")).sum()).alias("exp_extra")
        )

    out = possessions.join(contrib, on=["game_id", "possession_number"], how="left").with_columns(
        pl.col("exp_extra").fill_null(0.0)
    )
    return out.with_columns((2.0 * pl.col("fg2m") + pl.col("exp_extra")).cast(pl.Float64).alias("la_points")).drop(
        "exp_extra"
    )


#: Output schema for :func:`nba_la_rapm`.
LA_RAPM_SCHEMA: dict[str, pl.DataType] = {
    "player_id": pl.Int64,
    "o_la_rapm": pl.Float64,
    "d_la_rapm": pl.Float64,
    "la_rapm": pl.Float64,
    "off_poss": pl.Int64,
    "def_poss": pl.Int64,
}


def nba_la_rapm(
    possessions: pl.DataFrame,
    shooting: pl.DataFrame,
    player_rates: Optional[dict[int, tuple[float, float]]] = None,
    *,
    alphas: Optional[np.ndarray] = None,
    fg3_k: float = 100.0,
    ft_k: float = 50.0,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Luck-adjusted RAPM: ridge on an expected-points response (high-variance shooting regressed).

    Replaces realized 3-point and free-throw outcomes with the shooter's shrunk
    expected value (:func:`luck_adjusted_response`); 2-pt makes stay realized.
    **DECISION 2/3/4** govern the response recipe and shrinkage constants.

    .. note::
        **Ridge schedule -- the oracle grid (controller ruling, extends the
        binding decision #8 documented in the module docstring)**: this
        function's *operative* fit uses :func:`oracle_rapm_alphas` (evaluated
        at the post-filter possession count, ``X.shape[0]``) with
        ``cv=`` :data:`ORACLE_RAPM_CV`, matching every other WP2 RAPM variant
        -- **not** plain :func:`~sportsdataverse.nba.nba_rapm.nba_rapm`'s own
        ``DEFAULT_RAPM_ALPHAS`` / ``cv=None`` schedule (an earlier revision of
        this function fit at the plain schedule solely to satisfy the
        reduces-to-plain gate; the controller ruling supersedes that).
        Consequently the reduces-to-plain-response correctness gate
        (``test_la_rapm_equals_same_schedule_reference_when_rates_realized``)
        no longer compares against the public ``nba_rapm`` -- doing so would be a
        cross-schedule comparison and could fail on schedule drift alone,
        independent of whether the luck-adjustment recipe is correct. Instead
        it fits a SAME-SCHEDULE internal reference directly --
        ``_fit_weighted(*_prepare(possessions, "points")[:2],
        alphas=oracle_rapm_alphas(n), cv=ORACLE_RAPM_CV)`` on the plain
        realized-``points`` response -- and asserts LA-RAPM equals that
        reference when the luck-adjustment rates are set to the realized
        (non-shrunk) values, so ``la_points == points`` exactly and only the
        response-substitution logic is under test, not the ridge schedule.

    Args:
        possessions: Possession+lineup frame with team-level ``fg2m`` and the
            ten lineup columns; join keys ``game_id`` + ``possession_number``
            + ``offense_team_id`` (the last is required by
            :func:`luck_adjusted_response`'s defense-shooter leak filter).
            Must also carry a ``points`` column even though the LA response
            (``la_points``) supersedes it for fitting --
            :func:`~sportsdataverse.nba.nba_rapm.build_rapm_design` (invoked
            internally via :func:`_prepare`) requires it unconditionally.
        shooting: Per-(possession, shooter) frame from ``build_possession_shooting``.
        player_rates: Optional ``{player_id: (p3, pft)}`` override; ``None`` →
            shrink from ``shooting``.
        alphas: Optional RidgeCV alpha grid override. ``None`` (default)
            auto-selects :func:`oracle_rapm_alphas` evaluated at the
            possession count -- the operative WP2 oracle schedule (``cv=``
            :data:`ORACLE_RAPM_CV` always; there is no plain-schedule mode).
        fg3_k: 3-point shrinkage pseudo-count, forwarded to
            :func:`luck_adjusted_response` when ``player_rates`` is ``None``.
        ft_k: Free-throw shrinkage pseudo-count, forwarded to
            :func:`luck_adjusted_response` when ``player_rates`` is ``None``.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        Frame with :data:`LA_RAPM_SCHEMA`. Empty input → zero-row frame.

    Example:
        Fit LA-RAPM over a compiled season plus its shooting companion::

            from sportsdataverse.nba.nba_rapm_variants import nba_la_rapm
            df = nba_la_rapm(season_poss, season_shooting)
            print(df.sort("la_rapm", descending=True).head())

        Planted-truth shooter rates (e.g. for testing)::

            df = nba_la_rapm(season_poss, season_shooting, {7: (0.4, 0.8)})

        See Also:
            * `NBA_Tutorials (Ryan Davis)`_ — the oracle RAPM reference implementation
            * `nba_api`_ — upstream play-by-play / shooting source

        .. _NBA_Tutorials (Ryan Davis): https://github.com/rd11490/NBA_Tutorials
        .. _nba_api: https://github.com/swar/nba_api
    """
    if possessions.is_empty():
        out = _empty(LA_RAPM_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    enriched = luck_adjusted_response(possessions, shooting, player_rates, fg3_k=fg3_k, ft_k=ft_k)
    X, y, _w, pids = _prepare(enriched, "la_points", weight_col=None)
    if not pids:
        out = _empty(LA_RAPM_SCHEMA)
        return out.to_pandas() if return_as_pandas else out
    fit_alphas = alphas if alphas is not None else oracle_rapm_alphas(X.shape[0])
    o, d, off_poss, def_poss = _fit_weighted(X, y, alphas=fit_alphas, cv=ORACLE_RAPM_CV)
    out = pl.DataFrame(
        {
            "player_id": pl.Series(pids, dtype=pl.Int64),
            "o_la_rapm": pl.Series(o, dtype=pl.Float64),
            "d_la_rapm": pl.Series(d, dtype=pl.Float64),
            "la_rapm": pl.Series(o + d, dtype=pl.Float64),
            "off_poss": pl.Series(off_poss, dtype=pl.Int64),
            "def_poss": pl.Series(def_poss, dtype=pl.Int64),
        }
    ).sort("player_id")
    return out.to_pandas() if return_as_pandas else out


#: Per-possession four-factor response expressions (points-per-possession scale).
#: **DECISION 5/6/7** (binding): ``RA_EFG = 2*fg2m + 3*fg3m``, ``RA_FTR = ftm``,
#: ``RA_ORBD`` = raw ``oreb``, ``RA_TOV`` = **negated** raw ``tov`` (``-tov``) --
#: the plan's v1 defaults, with the TOV sign fixed per the polarity note below.
#:
#: **TOV polarity (bugfix):** :func:`_fit_weighted` assumes points-like polarity
#: -- ``o = +raw_off`` (a higher raw offense coefficient is good: e.g. more
#: points/made-FTs/oreb while on offense) and ``d = -raw_def`` (a defender who
#: SUPPRESSES the response while on the floor gets a POSITIVE rating, since the
#: raw coefficient is negated). That polarity holds for efg/ftr/orbd unmodified
#: -- but turnovers invert it on BOTH sides: *committing* turnovers while on
#: offense is bad (opposite of points, where more is good), and *forcing*
#: turnovers while on defense is good (also opposite of the points convention,
#: where suppressing the response is what's rewarded). Fitting on the raw
#: ``tov`` response as shipped therefore read "more turnovers = higher
#: ``tov__off``" and flipped a good takeaway defender NEGATIVE via ``tov__def``
#: -- backwards on both sides. Fitting on the NEGATED response (``-tov``)
#: restores the module-wide "higher = better" convention symmetrically:
#: least-squares/ridge coefficients are exactly negated under a response sign
#: flip (identical squared error at every alpha, so RidgeCV picks the same
#: alpha and just negates ``coef_``), so this is equivalent to -- and cheaper
#: than -- an explicit post-fit sign flip applied only to the ``tov`` factor's
#: ``o``/``d`` arrays. See ``test_four_factor_tov_directionality_*`` for the
#: planted-turnover-prone-player regression gate.
_FACTOR_RESPONSES: dict[str, pl.Expr] = {
    "efg": (2 * pl.col("fg2m") + 3 * pl.col("fg3m")).cast(pl.Float64),
    "ftr": pl.col("ftm").cast(pl.Float64),
    "orbd": pl.col("oreb").cast(pl.Float64),
    "tov": (-pl.col("tov")).cast(pl.Float64),
}

#: Output schema for :func:`nba_four_factor_rapm`.
FOUR_FACTOR_SCHEMA: dict[str, pl.DataType] = {
    "player_id": pl.Int64,
    **{f"{f}__{side}": pl.Float64 for f in _FACTOR_RESPONSES for side in ("off", "def")},
    "off_poss": pl.Int64,
    "def_poss": pl.Int64,
}


def nba_four_factor_rapm(
    possessions: pl.DataFrame,
    *,
    alphas: Optional[np.ndarray] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Four-factor RAPM: four independent ridge fits (efg/ftr/orbd/tov) on the SAME design.

    Each factor is regressed on the identical offense/defense design matrix,
    differing only in the per-possession response (:data:`_FACTOR_RESPONSES`).
    Output mirrors the oracle's ``RA_*__Off/__Def`` layout. **DECISION 5/6/7**
    govern the response definitions.

    .. note::
        **Ridge schedule -- the oracle grid**: like every other WP2 RAPM
        variant (module docstring's binding schedule ruling, decision #8,
        extended to :func:`nba_la_rapm`), each of the four factors' operative
        fit uses :func:`oracle_rapm_alphas` (evaluated once, at the possession
        count shared by all four fits since they use the identical design)
        with ``cv=`` :data:`ORACLE_RAPM_CV` -- not plain
        :func:`~sportsdataverse.nba.nba_rapm.nba_rapm`'s ``DEFAULT_RAPM_ALPHAS``/
        ``cv=None`` schedule. Pass an explicit ``alphas=`` to override.

    .. note::
        **TOV polarity (bugfix)**: unlike efg/ftr/orbd, ``RA_TOV`` is fit on
        the NEGATED raw ``tov`` response (see :data:`_FACTOR_RESPONSES`) so
        that ``tov__off``/``tov__def`` follow the same module-wide
        "higher = better" convention as the other three factors -- a
        turnover-prone offensive player gets a LOWER ``tov__off``, and a
        takeaway-forcing defender gets a HIGHER ``tov__def``. Fitting on the
        raw (un-negated) response would flip both: more turnovers reading as
        a higher (better) ``tov__off``, and a good takeaway defender reading
        negative on ``tov__def``.

    Args:
        possessions: Possession+lineup frame carrying team-level ``fg2m, fg3m,
            ftm, oreb, tov`` and the ten lineup columns. Must also carry a
            ``points`` column -- :func:`~sportsdataverse.nba.nba_rapm.build_rapm_design`
            (invoked internally) requires it unconditionally even though none
            of the four factor responses use it. ``offense_team_id`` is NOT
            required here (unlike :func:`nba_la_rapm`): none of the four
            factor responses need the offense-only shooter join.
        alphas: Optional RidgeCV alpha grid override, shared by all four
            factor fits. ``None`` (default) auto-selects
            :func:`oracle_rapm_alphas` evaluated at the possession count --
            the operative WP2 oracle schedule.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        Frame with :data:`FOUR_FACTOR_SCHEMA` — ``{factor}__off`` / ``{factor}__def``
        columns per factor, plus possession counts. Empty input → zero-row frame.

    Example:
        Per-player four-factor impact::

            from sportsdataverse.nba.nba_rapm_variants import nba_four_factor_rapm
            ff = nba_four_factor_rapm(season_poss)
            print(ff.sort("efg__off", descending=True).head())

        See Also:
            * `NBA_Tutorials (Ryan Davis)`_ — the oracle RAPM reference implementation
            * `nba_api`_ — upstream play-by-play source

        .. _NBA_Tutorials (Ryan Davis): https://github.com/rd11490/NBA_Tutorials
        .. _nba_api: https://github.com/swar/nba_api
    """
    if possessions.is_empty():
        out = _empty(FOUR_FACTOR_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    enriched = possessions.with_columns([expr.alias(f"_resp_{f}") for f, expr in _FACTOR_RESPONSES.items()])
    # Build the design ONCE (build_rapm_design depends only on the lineup columns,
    # never on the response) and read each factor's response straight off the
    # retained null-dropped frame -- rebuilding the design once per factor (as an
    # earlier revision did via 4 extra _prepare calls) was pure waste.
    X, kept, pids = _prepare_design(enriched)
    if not pids:
        out = _empty(FOUR_FACTOR_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    fit_alphas = alphas if alphas is not None else oracle_rapm_alphas(X.shape[0])
    cols: dict[str, pl.Series] = {"player_id": pl.Series(pids, dtype=pl.Int64)}
    off_poss: np.ndarray = np.empty(0, dtype=np.int64)
    def_poss: np.ndarray = np.empty(0, dtype=np.int64)
    for f in _FACTOR_RESPONSES:
        yf = kept[f"_resp_{f}"].to_numpy().astype(np.float64)
        assert X.shape[0] == len(yf), (X.shape, len(yf))  # alignment sanity (single shared design)
        o, d, off_poss, def_poss = _fit_weighted(X, yf, alphas=fit_alphas, cv=ORACLE_RAPM_CV)
        cols[f"{f}__off"] = pl.Series(o, dtype=pl.Float64)
        cols[f"{f}__def"] = pl.Series(d, dtype=pl.Float64)
    cols["off_poss"] = pl.Series(off_poss, dtype=pl.Int64)
    cols["def_poss"] = pl.Series(def_poss, dtype=pl.Int64)
    out = pl.DataFrame(cols).sort("player_id")
    return out.to_pandas() if return_as_pandas else out
