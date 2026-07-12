"""NHL/PWHL microstat spine -- shared constants, stability metrics, on-demand xG.

Algorithms across the five microstat value models
(:mod:`sportsdataverse.nhl.nhl_faceoff_value`,
:mod:`sportsdataverse.nhl.nhl_penalty_value`,
:mod:`sportsdataverse.nhl.nhl_expected_assists`,
:mod:`sportsdataverse.nhl.nhl_zone_transitions`,
:mod:`sportsdataverse.nhl.nhl_edge_value`) are league-agnostic; every
league-specific number lives in :data:`LEAGUE_CONSTANTS` and is reached only
through :func:`get_constants`. This module is also the single home for the
internal-oracle stability metrics (:func:`spearman_corr`,
:func:`rel_error`, :func:`split_half_stability`,
:func:`season_to_season_stability`) shared by every model's oracle gate, and
for the on-demand shot-xG logistic (:func:`fit_shot_xg`) that feeds model
(3) expected assists and the model (1)/(4) zone-value and faceoff-zone-value
fitters.

No bundled artifact, no first-use download: :func:`fit_shot_xg` fits at call
time on whatever pbp frame is passed in.

Example:
    Quick start::

        from sportsdataverse.nhl.nhl_microstat_constants import get_constants

        c = get_constants("nhl")
        print(c.pp_goal_value)

    PWHL constants::

        pwhl_c = get_constants("pwhl")
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import numpy.typing as npt
import polars as pl
from scipy.stats import rankdata
from sklearn.linear_model import LogisticRegression


@dataclass(frozen=True)
class LeagueConstants:
    """Per-league fitted/seeded constants shared by every microstat model.

    Args:
        pp_goal_value: Expected goals gained per minor penalty drawn
            (net of shorthanded-goals-against per minor). Fit by
            ``dev/nhl_microstat/fit_pp_goal_value.py`` (Task 2.2).
        major_penalty_value: Expected goals gained per major penalty drawn.
        zone_entry_value_controlled: xG generated per controlled zone entry.
            Fit by ``dev/nhl_microstat/fit_zone_entry_value.py`` (Task 4.2).
        zone_entry_value_dump: xG generated per dump-in zone entry.
        zone_exit_value: xG value credited per controlled zone exit.
        edge_component_weights: EDGE z-composite loadings by raw component
            name (``top_speed``, ``distance_km``, ``speed_bursts_20``,
            ``oz_time_pct``).
        faceoff_zone_weights: Post-faceoff xG weight by zone (``"O"``/``"D"``/``"N"``).
            Fit by ``dev/nhl_microstat/fit_faceoff_zone_weights.py`` (Task 1.4).
        controlled_window_s: Seconds after a zone entry within which a
            same-team possession event marks the entry as controlled.
        entry_window_s: Seconds after a zone entry/faceoff over which
            post-event xG is measured for the value fitters.
    """

    pp_goal_value: float
    major_penalty_value: float
    zone_entry_value_controlled: float
    zone_entry_value_dump: float
    zone_exit_value: float
    edge_component_weights: dict[str, float]
    faceoff_zone_weights: dict[str, float]
    controlled_window_s: float = 4.0
    entry_window_s: float = 8.0


# Seeded from published references (NHL PP conversion ~20%, ~0.17-0.20 net
# goals per minor drawn once SH-against is netted out; PWHL seeded slightly
# lower pending its own fit -- smaller sample, lower observed PP rate).
# Task 1.4 / 2.2 / 4.2 / 6.1 overwrite these with values fit from the
# committed oracle corpus via a `dev/nhl_microstat/fit_*.py` script; every
# fit is concrete code, never an invented final number.
LEAGUE_CONSTANTS: dict[str, LeagueConstants] = {
    "nhl": LeagueConstants(
        # Fit from tests/fixtures/nhl_microstat/pbp_2024_slice.parquet via
        # dev/nhl_microstat/fit_pp_goal_value.py (Task 2.2): (PP goals for -
        # SH goals against) per minor penalty, on 988 minors / 210 PP goals /
        # 68 SH goals (120-game slice). major_penalty_value is a
        # time-exposure-scaled estimate (2.5x, a major runs the full 5:00 vs.
        # a minor's 2:00) -- the captured major-penalty sample (86) is still
        # too thin to fit a standalone goals-during-majors ratio.
        pp_goal_value=0.144,
        major_penalty_value=0.359,
        # Fit from the 120-game slice via dev/nhl_microstat/fit_zone_entry_value.py
        # (Task 4.2, re-fit after the T5.2 flesh-out of the event-sequence-aware
        # controlled/dump heuristic + the period-boundary seconds_to_next fix --
        # see nhl_zone_transitions module docstring): mean xG the entering/exiting
        # team generates within entry_window_s after a controlled vs dump entry
        # (controlled ~2x dump) / after an exit. Heuristic controlled/dump labels
        # (see nhl_zone_transitions 🟡), so these remain approximate values; the
        # re-fits barely moved (0.108->0.1101 / 0.053->0.0543 / 0.007->0.0070),
        # confirming the label flesh-out + period-boundary guard changed WHICH
        # events are called controlled at the margin (887->868 controlled) without
        # disturbing the aggregate value split.
        zone_entry_value_controlled=0.1101,
        zone_entry_value_dump=0.0543,
        zone_exit_value=0.0070,
        # NOT fit -- deliberate equal-weight (unweighted-z) composite: each EDGE
        # component contributes its raw league-wide z-score equally. The EDGE
        # concurrent oracle (component rank-corr >= 0.5 in test_edge_value_concurrent)
        # guards against a z-score sign / weight regression. Escalate to fitted
        # PCA first-component loadings only if that gate demands it (see
        # nhl_edge_value docstring). ``oz_dz_time_balance`` (T5.2 flesh-out) is
        # equal-weighted the same way, but only contributes when a caller opts
        # in via ``include_zone_balance=True`` -- ``_edge_zcomposite`` derives
        # the column itself, so this weight is dormant (never looked up) unless
        # that flag is set.
        edge_component_weights={
            "top_speed": 1.0,
            "distance_km": 1.0,
            "speed_bursts_20": 1.0,
            "oz_time_pct": 1.0,
            "oz_dz_time_balance": 1.0,
        },
        # Fit from tests/fixtures/nhl_microstat/pbp_2024_slice.parquet via
        # dev/nhl_microstat/fit_faceoff_zone_weights.py (Task 1.4): mean
        # post-faceoff xG generated by the winning team within
        # entry_window_s, normalized to the O-zone value.
        faceoff_zone_weights={"O": 1.0, "N": 1.03, "D": 0.06},
    ),
    # PWHL constants are SEEDED-PLACEHOLDER (== the NHL fitted values), NOT fit:
    # this spine's models (faceoff/penalty/assist/zone-transition/EDGE) need
    # pbp on the Task-0.1 NHL api-web contract (type_desc_key/zone_code/
    # time_in_period/event_owner_team_id) -- there is no adapter converting
    # sportsdataverse's actual `load_pwhl_pbp` (a differently-shaped HockeyTech
    # feed with event/shot_quality/team_id/time_of_period columns, used by the
    # separate T5.3 `pwhl_xg_proxy` prediction spine) into that contract yet.
    # Capture contract to fit these for real: obtain a PWHL pbp slice on the
    # Task-0.1 contract (e.g. via the fastRhockey PWHL feed, or a future
    # load_pwhl_pbp -> NHL-contract adapter) and re-run
    # dev/nhl_microstat/fit_{faceoff_zone_weights,pp_goal_value,zone_entry_value}.py
    # with league="pwhl", then replace these seeds. Using the NHL fits as the
    # seed is the best available estimate (labeled, not an ungrounded magic
    # number) until that corpus exists. EDGE has no PWHL feed -> zero-row.
    "pwhl": LeagueConstants(
        pp_goal_value=0.144,
        major_penalty_value=0.359,
        zone_entry_value_controlled=0.1101,
        zone_entry_value_dump=0.0543,
        zone_exit_value=0.0070,
        edge_component_weights={
            "top_speed": 1.0,
            "distance_km": 1.0,
            "speed_bursts_20": 1.0,
            "oz_time_pct": 1.0,
            "oz_dz_time_balance": 1.0,
        },
        faceoff_zone_weights={"O": 1.0, "N": 1.03, "D": 0.06},
    ),
}


def get_constants(league: str) -> LeagueConstants:
    """Look up the per-league microstat constants table.

    Args:
        league: League key (``"nhl"`` or ``"pwhl"``).

    Returns:
        The :class:`LeagueConstants` for that league.

    Raises:
        ValueError: If ``league`` is not a known key.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_microstat_constants import get_constants

            get_constants("nhl").pp_goal_value
    """
    try:
        return LEAGUE_CONSTANTS[league]
    except KeyError as exc:
        raise ValueError(f"unknown league {league!r}; expected one of {sorted(LEAGUE_CONSTANTS)}") from exc


def spearman_corr(a: npt.NDArray[np.float64], b: npt.NDArray[np.float64]) -> float:
    """Spearman rank correlation between two equal-length numeric arrays.

    Args:
        a: First array.
        b: Second array, same length as ``a``.

    Returns:
        The Spearman correlation coefficient, or ``nan`` if fewer than 2
        elements are supplied.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nhl.nhl_microstat_constants import spearman_corr

            spearman_corr(np.array([1.0, 2.0]), np.array([10.0, 20.0]))
    """
    ra, rb = rankdata(a), rankdata(b)
    if len(ra) < 2:
        return float("nan")
    return float(np.corrcoef(ra, rb)[0, 1])


def rel_error(a: float, b: float) -> float:
    """Relative error of ``a`` against reference value ``b``.

    Args:
        a: Observed value.
        b: Reference (expected) value.

    Returns:
        ``abs(a - b) / max(abs(b), 1e-9)``.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_microstat_constants import rel_error

            rel_error(1.1, 1.0)
    """
    return float(abs(a - b) / max(abs(b), 1e-9))


def rate_per_60(count: pl.Expr, seconds: pl.Expr) -> pl.Expr:
    """Convert a raw count + elapsed seconds into a per-60-minute rate expression.

    Args:
        count: Column/expression holding the event count.
        seconds: Column/expression holding elapsed seconds of exposure.

    Returns:
        A polars expression for ``count / (seconds / 3600.0)``.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nhl.nhl_microstat_constants import rate_per_60

            df.with_columns(rate_per_60(pl.col("entries"), pl.col("toi_seconds")).alias("rate"))
    """
    return count / (seconds / 3600.0)


def split_half_stability(
    events: pl.DataFrame,
    *,
    id_col: str,
    half_col: str,
    num_col: str,
    den_col: str,
) -> float:
    """Within-sample split-half rate stability (the internal-oracle primitive).

    Aggregates ``sum(num_col)/sum(den_col)`` per ``(id_col, half_col)``,
    pivots the two halves side by side, and returns the Spearman
    correlation of the per-id rate across halves. Only ids present in
    **both** halves are compared -- the strict-split leakage boundary
    (the statistic predicting one half never sees the other half's rows).

    Args:
        events: Per-event frame with an id column, a 2-valued half column,
            a numerator column, and a denominator column.
        id_col: Column identifying the entity being tracked for stability
            (typically ``player_id``).
        half_col: Column with exactly two distinct split values (e.g.
            odd/even by ``event_idx % 2``).
        num_col: Numerator column to sum per half.
        den_col: Denominator column to sum per half.

    Returns:
        Spearman correlation of the per-id rate across the two halves, or
        ``nan`` if fewer than 2 ids have both halves present.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_microstat_constants import split_half_stability

            split_half_stability(events, id_col="player_id", half_col="half",
                                  num_col="won", den_col="one")
    """
    agg = (
        events.group_by([id_col, half_col])
        .agg(pl.col(num_col).sum().alias("n"), pl.col(den_col).sum().alias("d"))
        .with_columns((pl.col("n") / pl.col("d")).alias("rate"))
    )
    wide = agg.pivot(values="rate", index=id_col, on=half_col).drop_nulls()
    cols = [c for c in wide.columns if c != id_col]
    if wide.height < 2 or len(cols) < 2:
        return float("nan")
    return spearman_corr(wide[cols[0]].to_numpy(), wide[cols[1]].to_numpy())


def season_to_season_stability(
    s1: pl.DataFrame,
    s2: pl.DataFrame,
    *,
    id_col: str,
    value_col: str,
) -> float:
    """Season-over-season stability (the strict train->holdout split for annual rates).

    Args:
        s1: Season-1 per-id frame.
        s2: Season-2 per-id frame.
        id_col: Id column present in both frames (must share dtype).
        value_col: Value column to correlate across seasons.

    Returns:
        Spearman correlation of ``value_col`` across the inner-joined ids,
        or ``nan`` if fewer than 2 ids match.

    Raises:
        AssertionError: If ``id_col``'s dtype differs between ``s1`` and ``s2``.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_microstat_constants import season_to_season_stability

            season_to_season_stability(season1_df, season2_df, id_col="player_id", value_col="rate")
    """
    assert s1.schema[id_col] == s2.schema[id_col], "id dtype mismatch across seasons"
    j = s1.select(id_col, pl.col(value_col).alias("v1")).join(
        s2.select(id_col, pl.col(value_col).alias("v2")), on=id_col, how="inner"
    )
    if j.height < 2:
        return float("nan")
    return spearman_corr(j["v1"].to_numpy(), j["v2"].to_numpy())


# ---------------------------------------------------------------------------
# On-demand shot-xG logistic (Task 0.3)
# ---------------------------------------------------------------------------

_XG_GOAL_TYPES = {"goal"}
_XG_SHOT_TYPES = {"goal", "shot-on-goal", "missed-shot", "blocked-shot"}
_XG_SHOT_TYPE_CATEGORIES = ("wrist", "slap", "snap", "backhand", "tip-in", "wrap-around", "deflected")
_XG_MIN_SHOTS = 200
# Rink goal-line x-coordinate in feet (NHL/PWHL-invariant): the net sits at
# x = +/-89 on the api-web coordinate system; shot distance/angle are measured
# to (sign(x_coord)*89, 0).
_NET_X = 89.0


@dataclass(frozen=True)
class ShotXGModel:
    """A fitted (or fallback constant-rate) shot-xG model.

    Args:
        model: Fitted :class:`sklearn.linear_model.LogisticRegression`, or
            ``None`` when the fallback constant-rate path was used.
        features: Ordered feature-column names the model was fit on.
        fallback_rate: Constant goal rate returned by :meth:`predict` when
            ``model`` is ``None`` (insufficient shots to fit).
    """

    model: LogisticRegression | None
    features: list[str]
    fallback_rate: float

    def predict(self, shots: pl.DataFrame) -> pl.Series:
        """Predict shot xG for each row of ``shots``.

        Args:
            shots: Frame with ``x_coord``, ``y_coord``, and (optionally)
                ``shot_type`` columns.

        Returns:
            A ``pl.Series`` named ``"xg"`` of per-row goal probability.

        Example:
            Quick start::

                model.predict(shots_df)
        """
        if shots.height == 0:
            return pl.Series("xg", [], dtype=pl.Float64)
        if self.model is None:
            return pl.Series("xg", [self.fallback_rate] * shots.height, dtype=pl.Float64)
        feat = _shot_features(shots)
        # fill_null mirrors the training path in fit_shot_xg: shot_type is null
        # for some event types (e.g. blocked-shot), which otherwise leaves the
        # is_* one-hot columns null -> NaN feature matrix -> sklearn ValueError.
        x = feat.select(self.features).fill_null(0.0).to_numpy()
        proba = self.model.predict_proba(x)[:, 1]
        return pl.Series("xg", proba, dtype=pl.Float64)


def _shot_features(shots: pl.DataFrame) -> pl.DataFrame:
    shot_type = pl.col("shot_type") if "shot_type" in shots.columns else pl.lit(None, dtype=pl.Utf8)
    out = shots.with_columns(
        ((_NET_X - pl.col("x_coord").abs()).pow(2) + pl.col("y_coord").pow(2)).sqrt().alias("distance"),
        pl.arctan2(pl.col("y_coord").abs(), (_NET_X - pl.col("x_coord").abs())).alias("angle"),
        shot_type.alias("_shot_type"),
    )
    for cat in _XG_SHOT_TYPE_CATEGORIES:
        col_name = f"is_{cat.replace('-', '_')}"
        out = out.with_columns((pl.col("_shot_type") == cat).cast(pl.Float64).alias(col_name))
    return out


def fit_shot_xg(pbp: pl.DataFrame) -> ShotXGModel:
    """Fit a light distance/angle/shot-type logistic xG model on demand.

    Filters to shot/goal events, derives ``distance``/``angle`` from
    ``x_coord``/``y_coord`` (net centered at ``x = +/-89``), one-hot codes
    ``shot_type``, and fits a :class:`~sklearn.linear_model.LogisticRegression`
    with the goal indicator as the label. No bundled artifact -- this fits
    at call time on whatever pbp frame is passed in. Falls back to a
    constant-rate model (the empirical goal rate) when fewer than 200
    qualifying shots are present, so callers never crash on a small frame.

    Args:
        pbp: Parsed pbp frame (Task-0.1 contract) or any frame with
            ``type_desc_key``, ``x_coord``, ``y_coord``, and optionally
            ``shot_type``.

    Returns:
        A fitted :class:`ShotXGModel`.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_microstat_constants import fit_shot_xg

            model = fit_shot_xg(pbp)
            xg = model.predict(pbp.filter(pl.col("type_desc_key") == "shot-on-goal"))
    """
    features = ["distance", "angle"] + [f"is_{c.replace('-', '_')}" for c in _XG_SHOT_TYPE_CATEGORIES]
    if pbp.height == 0 or "type_desc_key" not in pbp.columns:
        return ShotXGModel(model=None, features=features, fallback_rate=0.0)

    shots = pbp.filter(pl.col("type_desc_key").is_in(_XG_SHOT_TYPES)).filter(
        pl.col("x_coord").is_not_null() & pl.col("y_coord").is_not_null()
    )
    if shots.height < _XG_MIN_SHOTS:
        goal_rate = float(shots.filter(pl.col("type_desc_key").is_in(_XG_GOAL_TYPES)).height / max(shots.height, 1))
        return ShotXGModel(model=None, features=features, fallback_rate=goal_rate)

    feat = _shot_features(shots).with_columns(
        pl.col("type_desc_key").is_in(_XG_GOAL_TYPES).cast(pl.Int64).alias("goal")
    )
    x = feat.select(features).fill_null(0.0).to_numpy()
    y = feat["goal"].to_numpy()
    if len(np.unique(y)) < 2:
        return ShotXGModel(model=None, features=features, fallback_rate=float(y.mean()))

    clf = LogisticRegression(max_iter=1000)
    clf.fit(x, y)
    return ShotXGModel(model=clf, features=features, fallback_rate=float(y.mean()))
