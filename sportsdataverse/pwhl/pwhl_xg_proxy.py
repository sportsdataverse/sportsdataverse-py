"""PWHL xG models (coordinate + categorical proxy) + best-effort power ratings (T5.3/T5.3b).

**First-of-its-kind, best-effort.** ``load_pwhl_pbp`` -- the actual
HockeyTech-derived PWHL play-by-play sdv-py ships -- does not carry a numeric
``xg`` column like the NHL api-web contract the rest of the T5.2/T5.3 spine
assumes, but it DOES carry real shot coordinates (``x_coord``/``y_coord``,
feet-scale with the nets at ``x = +/-89`` -- the fastRhockey
``hockeytech_analytics.R`` convention) at ~100% coverage on shot rows, plus a
categorical ``shot_quality`` label (``"Quality on net"`` /
``"Non quality on net"`` / ``"Quality goal"`` / ``"Non quality goal"``).
This module is a **separate, additive** path (it does not touch
:mod:`sportsdataverse.pwhl.pwhl_team_ratings` /
:mod:`sportsdataverse.pwhl.pwhl_market`, whose deferred NHL-contract shims
remain unchanged and still correctly report empty until an NHL-shaped PWHL
pbp adapter exists) that:

1. Fits an xG model on-demand from real shot rows -- no bundled artifact --
   by either of two methods (``xg_method=``):

   - ``"coords"`` (T5.3b, **default**): a real distance/angle logistic xG
     (:func:`fit_pwhl_coord_xg`) whose geometry comes from the shared
     HockeyTech analytics core
     (:func:`sportsdataverse.hockeytech._analytics.add_shot_distance_angle`;
     coordinate frames documented in
     ``docs/superpowers/specs/2026-06-09-hockeytech-multi-league-scraper-analytics-design.md``)
     -- no distance/angle math is re-implemented here.
   - ``"quality"`` (T5.3, the original proxy): an **empirical goal-rate
     weight per shot-quality tier** (:func:`fit_shot_quality_xg`) -- the xG
     proxy IS the observed scoring rate for that tier. Kept working unchanged
     for API stability.
2. Builds a ``team_game_xg_rates``-shaped per-(game, team) frame
   (:func:`pwhl_team_game_xg_rates`) from PWHL's own columns (``event``,
   ``team_id``, ``power_play`` for an even-strength approximation --
   PWHL has no skater-count/goalie-in columns like the NHL contract).
3. Feeds that frame into the **already-shared, league-agnostic** rating core
   (:func:`sportsdataverse.nhl.nhl_team_ratings.adjust_rate_opponent`) with
   PWHL's ``LEAGUE_CONSTANTS`` (:func:`pwhl_ratings_from_proxy`) to produce
   power ratings, then the equally shared
   :func:`sportsdataverse.nhl.nhl_market.nhl_predict_games` /
   :func:`~sportsdataverse.nhl.nhl_market.win_prob_from_margin` for win
   probability -- no re-implementation of the prediction math, only the
   PWHL-shape ingestion is new.

**Honest limitations (documented, not hidden):**

- ``shot_quality`` tautologically encodes outcome for two of its four values
  (``"Quality goal"`` / ``"Non quality goal"`` ARE goals by construction), so
  the quality proxy is really a 2-tier ("quality" vs "non_quality") empirical
  goal-rate, not an independent predictive xG model. The ``"coords"`` method
  has no such tautology -- distance/angle are measured pre-outcome.
- The even-strength filter uses ``power_play != 1``. The loader's
  ``power_play`` tags are already the PP-window back-fill the fastRhockey
  producer applies (the same logic as
  :func:`sportsdataverse.hockeytech._analytics.backfill_power_play`):
  a null tag means "not inside a derived PP window", not missing data.
  Re-running the Python back-fill per game re-derives the tags nearly
  identically (2025: 938 producer PP tags vs 921 re-derived on 5,671 shot
  rows, plus 146 SH tags the ``!= 1`` filter ignores), so the producer tags
  are used as-is.
- Game dates are read from ``load_pwhl_game_info``'s ``game_date_iso``, NOT
  ``load_pwhl_schedules``'s ``game_date`` (a year-less "Wed, May 8" string
  with no reliable chronological parse) -- see :func:`pwhl_team_game_xg_rates`.
- No external PWHL oracle exists (first-of-its-kind); the gate is a
  HELD-OUT realized-outcome backtest (``margin_sd`` fit per method on 2025,
  scored on a held-out 2026, xG models fit on strictly-prior seasons). The
  honest held-out 2026 result (n=107, 2026-07-12): coords Brier 0.2444 vs
  quality 0.2449 vs naive 0.2500; paired coords-minus-quality per-game Brier
  diff -0.0005 (paired SE 0.0006). Coords gates best on every measured axis
  (it also carries real shot-level signal: in-sample AUC 0.6745 vs a 0.0835
  base rate on 10,593 shots) and is therefore the DEFAULT, but the edge over
  naive is still WITHIN sampling noise (~1 SE), so the gate is calibration +
  no-worse-than-naive, NOT a beats-naive magnitude claim (needs more
  seasons). See ``tests/pwhl/test_pwhl_xg_proxy_oracle.py`` +
  ``tests/fixtures/pwhl_prediction/README.md``.

Example:
    Quick start::

        from sportsdataverse.pwhl.pwhl_xg_proxy import pwhl_ratings_from_proxy
        from sportsdataverse.pwhl.pwhl_market import pwhl_predict_games

        ratings = pwhl_ratings_from_proxy(2025)
        games = ratings.select("team").rename({"team": "home_team"})  # illustrative
        # preds = pwhl_predict_games(games, ratings)

See Also:
    * `fastRhockey`_ -- companion R PWHL/NHL client (raw HockeyTech capture).

.. _fastRhockey: https://fastRhockey.sportsdataverse.org
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Union, overload

import pandas as pd
import polars as pl

from sportsdataverse._codegen_runtime import _as_season_list

if TYPE_CHECKING:
    from sklearn.linear_model import LogisticRegression

_QUALITY_TIERS = ("quality", "non_quality")
_MIN_SHOTS_PER_TIER = 30
# Mirrors nhl_microstat_constants._XG_MIN_SHOTS: below this the coord fit
# falls back to a constant-rate model instead of a logistic.
_MIN_COORD_SHOTS = 200

GAME_RATES_SCHEMA: dict[str, pl.PolarsDataType] = {
    "game_id": pl.Utf8,
    "season": pl.Int64,
    "date": pl.Date,
    "team": pl.Utf8,
    "opp_team": pl.Utf8,
    "is_home": pl.Boolean,
    "neutral_site": pl.Boolean,
    "xgf": pl.Float64,
    "xga": pl.Float64,
    "gf": pl.Int64,
    "ga": pl.Int64,
}

_RATINGS_SCHEMA: dict[str, pl.PolarsDataType] = {
    "season": pl.Int64,
    "team": pl.Utf8,
    "adj_xgf": pl.Float64,
    "adj_xga": pl.Float64,
    "adj_xg_net": pl.Float64,
    "adj_gf": pl.Float64,
    "adj_ga": pl.Float64,
    "games": pl.Int64,
    "off_rank": pl.Int64,
    "def_rank": pl.Int64,
    "net_rank": pl.Int64,
    "net_z": pl.Float64,
}


def _quality_tier_expr() -> pl.Expr:
    """Collapse the 4 raw ``shot_quality`` values into a 2-tier signal (Utf8, nullable)."""
    return (
        pl.when(pl.col("shot_quality").is_in(["Quality on net", "Quality goal"]))
        .then(pl.lit("quality"))
        .when(pl.col("shot_quality").is_in(["Non quality on net", "Non quality goal"]))
        .then(pl.lit("non_quality"))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )


def _game_dates(game_info: pl.DataFrame) -> pl.DataFrame:
    """``game_id`` (Int64) -> ``date`` from ``game_date_iso``.

    ``load_pwhl_schedules``'s own ``game_date`` is a year-less string
    (``"Wed, May 8"``) with no reliable chronological parse; ``game_info``'s
    ``game_date_iso`` is a real ISO timestamp. Shared by every date-dependent
    caller so the parse lives in one place.
    """
    return game_info.select(
        pl.col("game_id").cast(pl.Int64),
        pl.col("game_date_iso").str.slice(0, 10).str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("date"),
    ).unique(subset=["game_id"])


def _pbp_shots_before(pbp: pl.DataFrame, game_info: pl.DataFrame, cutoff: _dt.date) -> pl.DataFrame:
    """The narrow (fit-ready) pbp rows for games strictly before ``cutoff``.

    Joins ``game_info`` dates onto ``pbp`` and keeps only rows from games
    dated before the cutoff -- the leakage-safe training set for the tier-weight
    fit in a predictive (as-of) rating.
    """
    cols = ["game_id", "event", "shot_quality", "goal"]
    cols += [c for c in ("x_coord", "y_coord") if c in pbp.columns]  # coords method fit inputs
    dated = pbp.with_columns(pl.col("game_id").cast(pl.Int64)).join(_game_dates(game_info), on="game_id", how="left")
    return dated.filter(pl.col("date").is_not_null() & (pl.col("date") < cutoff)).select(cols)


@dataclass(frozen=True)
class ShotQualityXGModel:
    """A fitted empirical goal-rate-per-quality-tier PWHL xG proxy.

    Args:
        weights: Empirical goal rate (0-1) keyed by tier (:data:`_QUALITY_TIERS`).
        fallback_rate: Overall goal rate used for a tier with too few shots
            to trust its own rate, or for a shot with no resolvable tier.
    """

    weights: dict[str, float]
    fallback_rate: float

    def predict(self, shots: pl.DataFrame) -> pl.Series:
        """Score each row's shot-quality tier to its fitted empirical goal rate.

        Args:
            shots: Frame with a ``shot_quality`` column (PWHL pbp shape).

        Returns:
            A ``pl.Series`` named ``"xg"`` of per-row goal-rate proxy values.

        Example:
            Quick start::

                model.predict(shots_df)
        """
        if shots.height == 0:
            return pl.Series("xg", [], dtype=pl.Float64)
        tier = _quality_tier_expr()
        expr = pl.lit(self.fallback_rate, dtype=pl.Float64)
        for t in _QUALITY_TIERS:
            if t in self.weights:
                expr = pl.when(tier == t).then(pl.lit(self.weights[t])).otherwise(expr)
        return shots.select(expr.alias("xg"))["xg"]


def fit_shot_quality_xg(pbp: pl.DataFrame) -> ShotQualityXGModel:
    """Fit the empirical goal-rate-per-quality-tier PWHL xG proxy on demand.

    No bundled artifact, no first-use download -- mirrors
    :func:`sportsdataverse.nhl.nhl_microstat_constants.fit_shot_xg`'s
    fit-at-call-time contract. Filters to ``event == "shot"`` rows (the
    complete shot-attempt log; goals also appear as a separate, redundant
    ``event == "goal"`` scoring-summary row that this fit does NOT use, to
    avoid double-counting), derives the 2-tier quality signal, and computes
    each tier's realized goal rate. A tier with fewer than
    :data:`_MIN_SHOTS_PER_TIER` shots falls back to the overall rate.

    Args:
        pbp: A frame shaped like ``load_pwhl_pbp`` output (needs ``event``,
            ``shot_quality``, ``goal``).

    Returns:
        A fitted :class:`ShotQualityXGModel`.

    Example:
        Quick start::

            from sportsdataverse.pwhl.pwhl_loaders import load_pwhl_pbp
            from sportsdataverse.pwhl.pwhl_xg_proxy import fit_shot_quality_xg

            model = fit_shot_quality_xg(load_pwhl_pbp(2025))
    """
    if pbp.height == 0 or "event" not in pbp.columns or "shot_quality" not in pbp.columns:
        return ShotQualityXGModel(weights={}, fallback_rate=0.0)

    shots = pbp.filter(pl.col("event") == "shot").with_columns(_quality_tier_expr().alias("_tier"))
    shots = shots.filter(pl.col("_tier").is_not_null())
    if shots.height == 0:
        return ShotQualityXGModel(weights={}, fallback_rate=0.0)

    overall_rate = float(shots["goal"].cast(pl.Float64).mean() or 0.0)
    agg = shots.group_by("_tier").agg(pl.col("goal").cast(pl.Float64).mean().alias("rate"), pl.len().alias("n"))
    weights = {
        row["_tier"]: (row["rate"] if row["n"] >= _MIN_SHOTS_PER_TIER else overall_rate)
        for row in agg.iter_rows(named=True)
    }
    return ShotQualityXGModel(weights=weights, fallback_rate=overall_rate)


#: Rink half-length (feet). A standard rink-feet frame has ``x_coord`` in ~[-100, 100];
#: the RAW HockeyTech feed scale is 0-600. Anything past this + a margin is the RAW
#: scale, which must never be scored with the default ``goal_x=89`` (silent garbage).
_RINK_HALF_LEN_FT = 100.0
_RAW_SCALE_MARGIN_FT = 15.0  # behind-the-net shots reach ~100+; 600-scale can't hide under this


def _assert_rink_feet(frame: pl.DataFrame) -> None:
    """Raise if ``x_coord`` looks like the RAW feed scale (0-600) not rink-feet.

    The dual-frame footgun: ``load_pwhl_pbp`` (release parquet) carries feet-scale
    ``x_coord``, but a ``pwhl_pbp()`` enrich-path frame carries RAW feed-scale
    ``x_coord`` (0-600) where only ``shot_distance``/``shot_angle`` are trustworthy.
    Computing geometry from RAW coords with ``goal_x=89`` produces silent nonsense, so
    this fails loud instead. (No-op when ``x_coord`` is absent or all-null.)
    """
    if "x_coord" not in frame.columns:
        return
    xmax = frame.select(pl.col("x_coord").cast(pl.Float64, strict=False).abs().max()).item()
    if xmax is not None and xmax > _RINK_HALF_LEN_FT + _RAW_SCALE_MARGIN_FT:
        raise ValueError(
            f"x_coord max |{xmax:.0f}| ft exceeds the rink-feet range (~+-100 ft): this frame looks "
            "like RAW HockeyTech feed scale (0-600), not standard rink-feet. Score a load_pwhl_pbp() "
            "frame (feet-scale x_coord), or precompute shot_distance/shot_angle via "
            "add_shot_distance_angle() -- scoring RAW coords with goal_x=89 is a silent bug."
        )


def _shot_geometry(frame: pl.DataFrame) -> pl.DataFrame:
    """``shot_distance``/``shot_angle`` for a shot frame -- always HockeyTech's geometry.

    Reuses the columns when the frame already carries them (an
    :func:`sportsdataverse.hockeytech._analytics.enrich_pbp` output), else
    derives them via the shared
    :func:`sportsdataverse.hockeytech._analytics.add_shot_distance_angle`
    (a ``load_pwhl_pbp`` frame, whose ``x_coord``/``y_coord`` are already the
    standard rink-feet frame -- see the HockeyTech design spec,
    ``docs/superpowers/specs/2026-06-09-hockeytech-multi-league-scraper-analytics-design.md``).
    No geometry is computed in this module.

    When geometry must be derived (the frame lacks ``shot_distance``/``shot_angle``),
    ``x_coord`` is range-checked first (:func:`_assert_rink_feet`) so a RAW-scale
    enrich frame can never be silently scored with the NHL ``goal_x=89``.
    """
    if {"shot_distance", "shot_angle"}.issubset(frame.columns):
        return frame
    from sportsdataverse.hockeytech._analytics import add_shot_distance_angle

    _assert_rink_feet(frame)
    f = frame if "event" in frame.columns else frame.with_columns(pl.lit("shot").alias("event"))
    return add_shot_distance_angle(f)


_XG_BASE_FEATURES = ("shot_distance", "shot_angle")

#: Real on-ice PLAY events, in feed order. The pre-shot-context sequence is
#: restricted to these: PWHL emits a coordless DUPLICATE `goal` row alongside the
#: scoring `shot` (plus `goalie_change` / shootout rows), so an unfiltered
#: `shift(1)` would make every goal its own "prior event" -- a target leak that
#: inflates held-out AUC from 0.707 to 0.755. Do not widen without re-checking that.
_PLAY_EVENTS = ("faceoff", "shot", "blocked_shot", "hit", "penalty")

#: Pre-shot movement features (T5 Phase 4). Added by :func:`_add_preshot_context`,
#: which needs the FULL pbp; :func:`_build_xg_features` only reads them.
_XG_MOVE_FEATURES = (
    "last_shot",
    "last_faceoff",
    "last_hit",
    "last_blocked",
    "last_penalty",
    "time_since_last",
    "distance_from_last",
    "last_x",
    "last_y",
)

_MOVE_CONTEXT_INPUTS = ("game_id", "sec_from_start", "event", "x_coord", "y_coord")
_NO_PRIOR_SECONDS = 999.0  # first play of a game: "nothing happened recently"
_NO_PRIOR_DISTANCE = 200.0  # first play of a game: "far from anything" (rink is 200ft)

#: Even-strength `strength_state` values (home-vs-away skater counts, R4).
_EV_STRENGTH_STATES = ("5v5", "4v4", "3v3")


def _add_preshot_context(pbp: pl.DataFrame) -> pl.DataFrame:
    """Append pre-shot movement context columns (:data:`_XG_MOVE_FEATURES`) to a pbp.

    Must be handed the **FULL** pbp (not a shots-only frame): the features describe
    what happened *before* each shot, so they need the surrounding event sequence.
    Call it before filtering to shots; the columns then ride along on the shot rows
    into both :func:`_build_xg_features` and :meth:`PwhlCoordXGModel.predict`.

    Idempotent (returns the frame unchanged if the columns are already present), and
    a no-op on frames that can't support the features:

    - missing any of :data:`_MOVE_CONTEXT_INPUTS`, or
    - **shots-only frames** (the legacy xG fixture) -- there every shot's "prior
      event" would be another shot, a degenerate feature set. Callers degrade to the
      shot-local features instead.

    The sequence is restricted to :data:`_PLAY_EVENTS` -- see that constant for the
    goal-duplicate target leak this guards against.
    """
    if set(_XG_MOVE_FEATURES).issubset(pbp.columns) or not set(_MOVE_CONTEXT_INPUTS).issubset(pbp.columns):
        return pbp
    is_play = pl.col("event").is_in(_PLAY_EVENTS)
    if pbp.filter(is_play & (pl.col("event") != "shot")).height == 0:
        return pbp
    f = pbp.with_row_index("_ri")
    seq = (
        f.filter(is_play)
        # `_ri` (feed order) is the final tie-break: polars' sort is NOT stable by
        # default, and the feed's clock is 1s-granular, so events tied in the same
        # second would otherwise flip which row counts as "prior" between runs --
        # non-deterministic movement features. Feed order is the right tie-break:
        # within a second it IS the chronological order.
        .sort(["game_id", "sec_from_start", "_ri"])
        .with_columns(
            _pe=pl.col("event").shift(1).over("game_id"),
            _px=pl.col("x_coord").shift(1).over("game_id"),
            _py=pl.col("y_coord").shift(1).over("game_id"),
            _ps=pl.col("sec_from_start").shift(1).over("game_id"),
        )
    )
    pe = pl.col("_pe")
    ctx = seq.select(
        "_ri",
        last_shot=(pe == "shot").fill_null(False).cast(pl.Int64),
        last_faceoff=(pe == "faceoff").fill_null(False).cast(pl.Int64),
        last_hit=(pe == "hit").fill_null(False).cast(pl.Int64),
        last_blocked=(pe == "blocked_shot").fill_null(False).cast(pl.Int64),
        last_penalty=(pe == "penalty").fill_null(False).cast(pl.Int64),
        time_since_last=(pl.col("sec_from_start") - pl.col("_ps"))
        .cast(pl.Float64)
        .fill_null(_NO_PRIOR_SECONDS)
        .clip(0.0, _NO_PRIOR_SECONDS),
        distance_from_last=(
            ((pl.col("x_coord") - pl.col("_px")) ** 2 + (pl.col("y_coord") - pl.col("_py")) ** 2).sqrt()
        )
        .cast(pl.Float64)
        .fill_null(_NO_PRIOR_DISTANCE),
        last_x=pl.col("_px").cast(pl.Float64).fill_null(0.0),
        last_y=pl.col("_py").cast(pl.Float64).fill_null(0.0),
    )
    return f.join(ctx, on="_ri", how="left").sort("_ri").drop("_ri")


def _build_xg_features(
    shots: pl.DataFrame, want: tuple[str, ...] | None = None
) -> tuple[Any, tuple[str, ...], pl.DataFrame]:
    """Build the xG feature matrix (row-order preserved) + the feature names used.

    Always includes ``shot_distance`` / ``shot_angle``. When the source columns
    are present it ALSO adds (T5 Phase 3, LOSO-validated to lift PP/SH AUC):

    - ``rebound`` -- shot within 3s of the prior shot in the game (needs
      ``game_id`` + ``sec_from_start``).
    - ``is_home`` / ``is_pp`` / ``is_sh`` -- shooter-relative home flag and
      power-play/short-handed dummies (needs ``team_id`` + ``home_team_id`` +
      ``skaters_home`` / ``skaters_away``, i.e. the R4 strength columns).

    A frame that lacks those columns (e.g. the legacy xG fixture, or a pre-R4
    pbp) degrades to the 2-feature distance/angle model unchanged. At predict
    time pass ``want`` (the fitted feature tuple) so columns line up; any column
    the frame lacks is filled with 0 (neutral). Returns ``(matrix, names, f)``
    where ``f`` retains null ``shot_distance`` for the caller's null-geometry mask.
    """
    import numpy as np

    f = _shot_geometry(shots).with_row_index("_ri")
    avail = list(_XG_BASE_FEATURES)
    if {"game_id", "sec_from_start"}.issubset(f.columns):
        f = (
            # `_ri` (feed order) is the final sort key for the same reason as in
            # `_add_preshot_context`: polars' sort is unstable and the clock is
            # 1s-granular, so without it the "prior shot" of a rebound flips
            # between runs on tied timestamps.
            f.sort(["game_id", "sec_from_start", "_ri"])
            .with_columns(
                rebound=(pl.col("sec_from_start") - pl.col("sec_from_start").shift(1).over("game_id"))
                .is_between(0, 3)
                .fill_null(False)
                .cast(pl.Int64)
            )
            .sort("_ri")
        )
        avail.append("rebound")
    if {"team_id", "home_team_id", "skaters_home", "skaters_away"}.issubset(f.columns):
        home = pl.col("team_id") == pl.col("home_team_id")
        sk_for = pl.when(home).then(pl.col("skaters_home")).otherwise(pl.col("skaters_away"))
        sk_opp = pl.when(home).then(pl.col("skaters_away")).otherwise(pl.col("skaters_home"))
        f = f.with_columns(
            is_home=home.cast(pl.Int64),
            is_pp=(sk_for > sk_opp).fill_null(False).cast(pl.Int64),
            is_sh=(sk_for < sk_opp).fill_null(False).cast(pl.Int64),
            # defending team shows >=6 skaters => their goalie is pulled => the
            # shooter faces an empty net (the booster's `empty_net`; PWHL's SH gap).
            empty_net_for=(sk_opp >= 6).fill_null(False).cast(pl.Int64),
        )
        avail += ["is_home", "is_pp", "is_sh", "empty_net_for"]
    if "event_type" in f.columns:
        # Shot type -- PWHL's `event_type` on shot rows IS the shot type
        # (Wrist/Snap/Slap/Backhand/Tip; "Default" => all zero). The NHL booster's
        # shot-type one-hots, derived for free from the feed (T5 Phase 4). Self-
        # contained (no prior-event context), so safe on the shots-only predict path.
        et = pl.col("event_type")
        f = f.with_columns(
            is_wrist=(et == "Wrist").cast(pl.Int64),
            is_snap=(et == "Snap").cast(pl.Int64),
            is_slap=(et == "Slap").cast(pl.Int64),
            is_backhand=(et == "Backhand").cast(pl.Int64),
            is_tip=(et == "Tip").cast(pl.Int64),
        )
        avail += ["is_wrist", "is_snap", "is_slap", "is_backhand", "is_tip"]
    if set(_XG_MOVE_FEATURES).issubset(f.columns):
        # Pre-shot movement (T5 Phase 4) -- already computed by _add_preshot_context
        # on the FULL pbp (it cannot be derived here: a shots-only frame has no
        # prior-event context). Read only.
        avail += list(_XG_MOVE_FEATURES)
    feats = list(want) if want is not None else avail
    mats = [(f[c].cast(pl.Float64).fill_null(0.0).to_numpy() if c in f.columns else np.zeros(f.height)) for c in feats]
    return np.column_stack(mats), tuple(feats), f


# ---------------------------------------------------------------------------
# Per-strength Platt recalibration (T5 follow-up). The pooled logistic's `is_pp`/`is_sh`
# dummies already zero each SHOOTER-relative strength bucket's *mean*, but leave residual
# WITHIN-bucket miscalibration (the tails). A 1-logistic-per-bucket Platt on the base
# logit shrinks it at ~zero AUC cost -- held-out LOSO on the model's own shooter-relative
# EV/PP/SH partition: SH 10-bin ECE 0.0130->0.0091, PP 0.0047->0.0036, AUC 0.6962 both.
# It beat isotonic (overfits the thin 2-season curve) and a shallow GBM (hurt SH
# discrimination) in the model-idea sweep (dev/t5_xg_reevaluation/xg_model_ideas.py).
# NB the buckets are SHOOTER-relative (sk_for vs sk_opp), not the home-relative
# `strength_state` string -- so shooter-relative SH includes empty-net shots.
# ---------------------------------------------------------------------------
_STRENGTH_BUCKETS = ("EV", "PP", "SH")
#: A bucket needs at least this many training shots (with both outcome classes present)
#: to earn a Platt calibrator; thinner buckets stay identity (never a degenerate 2-param
#: fit on a handful of special-teams goals).
_MIN_CALIB_SHOTS = 200


def _strength_bucket_from_onehots(x: Any, feats: tuple[str, ...]) -> Any:
    """Map each row to its ``EV``/``PP``/``SH`` bucket via the fitted is_pp/is_sh columns.

    Returns a numpy string array, or ``None`` when the model carries no strength
    one-hots (a 2-feature distance/angle model -- no per-strength calibration possible).
    """
    import numpy as np

    if "is_pp" not in feats or "is_sh" not in feats:
        return None
    ipp = x[:, feats.index("is_pp")]
    ish = x[:, feats.index("is_sh")]
    return np.where(ipp == 1, "PP", np.where(ish == 1, "SH", "EV"))


def _logit(p: Any) -> Any:
    import numpy as np

    p = np.clip(p, 1e-6, 1 - 1e-6)
    return np.log(p / (1.0 - p))


def _fit_strength_calibrators(
    x: Any, y: Any, feats: tuple[str, ...], base_model: Any
) -> dict[str, tuple[float, float]]:
    """Fit a per-EV/PP/SH Platt recalibrator (``(slope, intercept)`` on the base logit).

    Returns ``{bucket: (slope, intercept)}`` for each bucket with
    ``>= _MIN_CALIB_SHOTS`` training shots and both outcome classes present. Empty dict
    when the model has no strength one-hots or every bucket is too thin -- i.e. identity.
    """
    import numpy as np
    from sklearn.linear_model import LogisticRegression

    buckets = _strength_bucket_from_onehots(x, feats)
    if buckets is None:
        return {}
    z = _logit(base_model.predict_proba(x)[:, 1]).reshape(-1, 1)
    cal: dict[str, tuple[float, float]] = {}
    for b in _STRENGTH_BUCKETS:
        m = buckets == b
        if int(m.sum()) >= _MIN_CALIB_SHOTS and len(np.unique(y[m])) > 1:
            lr = LogisticRegression(max_iter=1000).fit(z[m], y[m])
            cal[b] = (float(lr.coef_[0][0]), float(lr.intercept_[0]))
    return cal


def _apply_strength_calibration(
    proba: Any, x: Any, feats: tuple[str, ...], calibrators: dict[str, tuple[float, float]]
) -> Any:
    """Apply per-strength Platt recalibration to ``proba`` in place-safe fashion.

    Rows in a bucket without a calibrator (or in a non-strength model) are returned
    unchanged, so this is exactly identity wherever calibration can't or shouldn't apply.
    """
    import numpy as np

    buckets = _strength_bucket_from_onehots(x, feats)
    if buckets is None:
        return proba
    z = _logit(proba)
    out = np.asarray(proba, dtype=float).copy()
    for b, (a, c) in calibrators.items():
        m = buckets == b
        if m.any():
            out[m] = 1.0 / (1.0 + np.exp(-(a * z[m] + c)))
    return out


@dataclass(frozen=True)
class PwhlCoordXGModel:
    """A fitted (or fallback constant-rate) PWHL coordinate xG model.

    Args:
        model: Fitted :class:`sklearn.linear_model.LogisticRegression` on
            :attr:`features`, or ``None`` when the fallback constant-rate path
            was used (insufficient shots to fit).
        fallback_rate: Constant goal rate returned by :meth:`predict` when
            ``model`` is ``None``, and for rows whose coordinates are null.
        features: The feature names the model was fit on -- ``shot_distance`` /
            ``shot_angle`` plus, when the source pbp carried the R4 strength +
            clock columns, ``rebound`` / ``is_home`` / ``is_pp`` / ``is_sh``
            (T5 Phase 3). :meth:`predict` rebuilds exactly these.
        strength_calibrators: Optional per-strength Platt recalibrators
            ``{"EV"|"PP"|"SH": (slope, intercept)}`` fit on the base logit (T5
            follow-up). ``None`` (or a 2-feature model without ``is_pp``/``is_sh``)
            means no recalibration -- :meth:`predict` is then the raw logistic. When
            present, each row's bucket prediction is Platt-adjusted; buckets without a
            calibrator pass through unchanged.
    """

    model: LogisticRegression | None
    fallback_rate: float
    features: tuple[str, ...] = _XG_BASE_FEATURES
    strength_calibrators: dict[str, tuple[float, float]] | None = None

    def predict(self, shots: pl.DataFrame) -> pl.Series:
        """Predict shot xG for each row of ``shots``.

        Args:
            shots: Frame with ``x_coord``/``y_coord`` (rink-feet) or
                pre-computed ``shot_distance``/``shot_angle`` columns (plus the
                R4 strength/clock columns when :attr:`features` includes them).

        Returns:
            A ``pl.Series`` named ``"xg"`` of per-row goal probability. Rows
            with null geometry (null coordinates) get ``fallback_rate``.

        Example:
            Quick start::

                model.predict(shots_df)
        """
        import numpy as np

        if shots.height == 0:
            return pl.Series("xg", [], dtype=pl.Float64)
        if self.model is None:
            return pl.Series("xg", [self.fallback_rate] * shots.height, dtype=pl.Float64)
        x, _, f = _build_xg_features(shots, want=self.features)
        proba = self.model.predict_proba(x)[:, 1]
        if self.strength_calibrators:
            proba = _apply_strength_calibration(proba, x, self.features, self.strength_calibrators)
        null_geo = f["shot_distance"].is_null().to_numpy()
        out = np.where(null_geo, self.fallback_rate, proba)
        return pl.Series("xg", out, dtype=pl.Float64)


def fit_pwhl_coord_xg(pbp: pl.DataFrame, *, calibrate_strength: bool = True) -> PwhlCoordXGModel:
    """Fit a real distance/angle logistic xG on PWHL shot coordinates on demand.

    Filters to ``load_pwhl_pbp``'s ``event == "shot"`` rows (the complete
    on-net shot log; ``goal`` is the outcome flag), derives
    ``shot_distance``/``shot_angle`` via the shared HockeyTech analytics core
    (:func:`sportsdataverse.hockeytech._analytics.add_shot_distance_angle`;
    coordinate frames documented in the design spec,
    ``docs/superpowers/specs/2026-06-09-hockeytech-multi-league-scraper-analytics-design.md``),
    and fits a :class:`~sklearn.linear_model.LogisticRegression` with the
    goal indicator as the label -- the same fit-at-call-time recipe as
    :func:`sportsdataverse.nhl.nhl_microstat_constants.fit_shot_xg`, with the
    geometry sourced from the HockeyTech core instead of the NHL api-web one.
    The feature set grows with what the frame carries (all LOSO-validated; the
    fitted set is on :attr:`PwhlCoordXGModel.features`, and a frame missing any
    group simply degrades -- see :func:`_build_xg_features`):

    - **R4 strength + clock columns** -> ``rebound`` / ``is_home`` / ``is_pp`` /
      ``is_sh`` / ``empty_net_for`` (T5 Phase 3; ``empty_net_for`` = defending team
      shows >=6 skaters => goalie pulled => shooter faces an empty net).
    - **``event_type``** -> the shot-type one-hots ``is_wrist`` / ``is_snap`` /
      ``is_slap`` / ``is_backhand`` / ``is_tip``. PWHL has no ``shot_type`` column,
      but ``event_type`` on a shot row IS the shot type (T5 Phase 4).
    - **A full pbp (not a shots-only frame)** -> the pre-shot movement features
      (:data:`_XG_MOVE_FEATURES`), derived here by :func:`_add_preshot_context`
      before the shot filter. Pass the FULL pbp to get them; note that
      :meth:`PwhlCoordXGModel.predict` then also needs frames carrying those
      columns (:func:`pwhl_team_game_xg_rates` handles this for you).

    Rows with null coordinates are excluded
    from the fit (~100%
    coverage in the captured seasons, so this drops almost nothing). Fewer
    than :data:`_MIN_COORD_SHOTS` qualifying shots -- or a frame with no
    coordinate columns at all -- falls back to a constant-rate model at the
    observed goal rate (never a silent all-zero xG), mirroring the NHL
    fitter's contract. No bundled artifact, no first-use download.

    Args:
        pbp: A frame shaped like ``load_pwhl_pbp`` output (needs ``event``,
            ``goal``, ``x_coord``, ``y_coord``), or an ``enrich_pbp`` output
            already carrying ``shot_distance``/``shot_angle``.
        calibrate_strength: When ``True`` (default) and the frame carries strength
            columns, also fit a per-EV/PP/SH Platt recalibrator (T5 follow-up) that
            shrinks residual within-bucket per-strength calibration error (held-out SH
            10-bin ECE 0.0130->0.0091) at ~zero AUC cost. Buckets are shooter-relative
            (``is_pp``/``is_sh``). A no-op on 2-feature or thin frames (no strength
            one-hots / too few per-bucket shots). Pass ``False`` for the raw logistic.

    Returns:
        A fitted :class:`PwhlCoordXGModel`.

    Raises:
        ValueError: If a non-empty ``pbp`` has no ``goal`` column (no label
            to fit or fall back on).

    Example:
        Quick start::

            from sportsdataverse.pwhl.pwhl_loaders import load_pwhl_pbp
            from sportsdataverse.pwhl.pwhl_xg_proxy import fit_pwhl_coord_xg

            model = fit_pwhl_coord_xg(load_pwhl_pbp(2025))

    See Also:
        * `fastRhockey`_ -- R source of the HockeyTech coordinate transform.

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    import numpy as np
    from sklearn.linear_model import LogisticRegression

    if pbp.height == 0:
        return PwhlCoordXGModel(model=None, fallback_rate=0.0)
    if "goal" not in pbp.columns:
        raise ValueError("fit_pwhl_coord_xg requires a 'goal' column (load_pwhl_pbp shape)")
    # Pre-shot movement context must be derived BEFORE narrowing to shots (it needs
    # the surrounding event sequence); the columns then ride along on the shot rows.
    pbp = _add_preshot_context(pbp)
    shots = pbp.filter(pl.col("event") == "shot") if "event" in pbp.columns else pbp
    if not {"x_coord", "y_coord"}.issubset(pbp.columns):
        # No coordinate columns at all: an honest constant-rate model at the
        # observed goal rate -- never a silent all-zero xG.
        rate = float(shots["goal"].cast(pl.Float64).mean() or 0.0) if shots.height else 0.0
        return PwhlCoordXGModel(model=None, fallback_rate=rate)
    shots = shots.filter(pl.col("x_coord").is_not_null() & pl.col("y_coord").is_not_null())
    if shots.height < _MIN_COORD_SHOTS:
        goal_rate = float(shots["goal"].cast(pl.Float64).mean() or 0.0) if shots.height else 0.0
        return PwhlCoordXGModel(model=None, fallback_rate=goal_rate)

    x, feats, _ = _build_xg_features(shots)
    y = shots["goal"].cast(pl.Int64).to_numpy()
    if len(np.unique(y)) < 2:
        return PwhlCoordXGModel(model=None, fallback_rate=float(y.mean()), features=feats)

    clf = LogisticRegression(max_iter=1000)
    clf.fit(x, y)
    # Per-strength Platt recalibration (T5 follow-up): shrinks residual within-bucket
    # per-strength calibration error at ~zero AUC cost. Only earns calibrators when the
    # frame carried strength cols (is_pp/is_sh in `feats`) AND each bucket is well-sampled
    # -- else identity (empty dict -> None), so a 2-feature / thin frame is unchanged.
    cal = _fit_strength_calibrators(x, y, feats, clf) if calibrate_strength else {}
    return PwhlCoordXGModel(model=clf, fallback_rate=float(y.mean()), features=feats, strength_calibrators=cal or None)


def _fit_xg(pbp: pl.DataFrame, xg_method: str) -> Union[ShotQualityXGModel, PwhlCoordXGModel]:
    """Dispatch the on-demand xG fit for ``xg_method`` (validated by callers)."""
    return fit_shot_quality_xg(pbp) if xg_method == "quality" else fit_pwhl_coord_xg(pbp)


def pwhl_team_game_xg_rates(
    pbp: pl.DataFrame,
    schedule: pl.DataFrame,
    *,
    game_info: pl.DataFrame | None = None,
    xg_model: Union[ShotQualityXGModel, PwhlCoordXGModel, None] = None,
    xg_method: Literal["coords", "quality"] = "coords",
    even_strength_only: bool = True,
) -> pl.DataFrame:
    """Per-(game, team) xG-for/against + realized goals from PWHL pbp.

    The PWHL-native counterpart to
    :func:`sportsdataverse.nhl.nhl_team_ratings.team_game_xg_rates`: built for
    ``load_pwhl_pbp``'s HockeyTech shape (no pre-computed ``xg``, no
    skater-count/goalie-in columns) rather than the NHL api-web contract.
    ``team``/``opp_team`` are resolved to schedule's clean team-name strings
    (pbp's own ``home_team``/``away_team`` carry a ``"PWHL "`` prefix that
    schedule's do not -- resolved via ``team_id``, never by name-matching
    across the two frames).

    Args:
        pbp: A frame shaped like ``load_pwhl_pbp(season)`` output for ONE
            season (seasons carry different column counts upstream -- see
            ``dev/pwhl_prediction/build_pwhl_xg_fixture.py`` -- so this
            function is single-season; callers loop + concat the output).
        schedule: ``load_pwhl_schedules(season)`` output for the SAME season.
        game_info: Optional ``load_pwhl_game_info(season)`` output, used only
            for its ``game_date_iso`` (schedule's own ``game_date`` has no
            year and cannot be reliably parsed). Without it, ``date`` is null
            and ``as_of_ratings_split`` filtering cannot be applied.
        xg_model: A fitted model (:class:`ShotQualityXGModel` or the NHL
            :class:`PwhlCoordXGModel`
            -- both expose ``predict``); fit on ``pbp`` per ``xg_method``
            when ``None``.
        xg_method: ``"coords"`` (default; real distance/angle logistic via
            :func:`fit_pwhl_coord_xg` -- gates best on the held-out 2026
            backtest) or ``"quality"`` (the T5.3 categorical shot-quality
            proxy, kept working unchanged). Only selects the internal fit
            when ``xg_model`` is ``None``; an explicit ``xg_model`` is used
            as-is regardless of method. Shot rows with null coordinates
            contribute :class:`PwhlCoordXGModel`'s fallback rate (handled
            inside its ``predict``, not here).
        even_strength_only: Best-effort even-strength filter. When the pbp carries
            the R4 shift-derived ``strength_state`` / ``strength_state_valid``
            columns it drops only CONFIDENTLY non-even shots (a valid state outside
            ``5v5`` / ``4v4`` / ``3v3``); rows whose strength is null or invalid are
            kept, so a shift-tracking gap never silently deletes a shot. A pre-R4
            frame falls back to the legacy penalty-window-inferred ``power_play == 1``
            exclusion (see the module docstring's coverage caveat).

    Raises:
        ValueError: If ``xg_method`` is not ``"coords"`` or ``"quality"``.

    Returns:
        One row per (game, team), both home and away.

        |col_name     |type   |
        |:------------|:------|
        |game_id      |String |
        |season       |Int64  |
        |date         |Date   |
        |team         |String |
        |opp_team     |String |
        |is_home      |Boolean|
        |neutral_site |Boolean|
        |xgf          |Float64|
        |xga          |Float64|
        |gf           |Int64  |
        |ga           |Int64  |

    Example:
        Quick start::

            from sportsdataverse.pwhl.pwhl_loaders import (
                load_pwhl_game_info, load_pwhl_pbp, load_pwhl_schedules,
            )
            from sportsdataverse.pwhl.pwhl_xg_proxy import pwhl_team_game_xg_rates

            pbp = load_pwhl_pbp(2025)
            sched = load_pwhl_schedules(2025)
            info = load_pwhl_game_info(2025)
            rates = pwhl_team_game_xg_rates(pbp, sched, game_info=info)
    """
    if xg_method not in ("coords", "quality"):
        raise ValueError(f"xg_method must be 'coords' or 'quality', got {xg_method!r}")
    if pbp.is_empty() or schedule.is_empty():
        return pl.DataFrame(schema=GAME_RATES_SCHEMA)

    model = xg_model if xg_model is not None else _fit_xg(pbp, xg_method)
    # Context BEFORE the shot filter: the movement features need the surrounding
    # event sequence, which a shots-only frame no longer has.
    shots = _add_preshot_context(pbp).filter(pl.col("event") == "shot")
    if even_strength_only and {"strength_state", "strength_state_valid"}.issubset(shots.columns):
        # R4 shift-derived strength: drop only CONFIDENTLY non-even shots (a valid
        # state with unequal skater counts). Null / invalid strength keeps the row,
        # same best-effort philosophy as the legacy power_play path below -- a
        # shift-tracking gap shouldn't silently delete a shot.
        non_even = (
            (pl.col("strength_state_valid") == True) & (pl.col("strength_state").is_in(_EV_STRENGTH_STATES) == False)
        ).fill_null(False)
        shots = shots.filter(non_even == False)
    elif even_strength_only and "power_play" in shots.columns:
        # Legacy pre-R4 fallback: penalty-window-inferred `power_play` (sparse; see
        # the module docstring's coverage caveat). Exclude only confidently-tagged
        # power-play shots. Nulls (the majority) are KEPT -- comparing to null yields
        # null and filter() drops those rows, so null is filled first.
        #
        # Compared AS STRING: load_pwhl_pbp ships `power_play` as Utf8 ("0"/"1"/null),
        # not an int. The old numeric form (`fill_null(0) != 1`) raised ComputeError
        # ("cannot compare string with numeric type") on every real pbp frame -- it
        # only survived tests because the mini fixture hand-builds it as Int32. The
        # cast handles both.
        shots = shots.filter(pl.col("power_play").cast(pl.Utf8).fill_null("0") != "1")
    if shots.height == 0:
        return pl.DataFrame(schema=GAME_RATES_SCHEMA)

    # Null-coord shot rows need no special-casing here: PwhlCoordXGModel.predict
    # already substitutes its fallback_rate (the unconditional goal rate) for
    # rows with null geometry, and realized goals (gf/ga) count every row.
    shots = shots.with_columns(
        model.predict(shots).alias("xg"),
        pl.col("goal").cast(pl.Int64).alias("_goal_i"),
        pl.col("game_id").cast(pl.Int64),
        pl.col("team_id").cast(pl.Int64),
    )
    per_team = shots.group_by(["game_id", "team_id"]).agg(
        pl.col("xg").sum().alias("xgf"), pl.col("_goal_i").sum().alias("gf")
    )

    sched = schedule.filter(pl.col("game_type") == "regular").select(
        pl.col("game_id").cast(pl.Int64),
        pl.col("season").cast(pl.Int64),
        pl.col("home_team_id").cast(pl.Int64),
        pl.col("home_team"),
        pl.col("away_team_id").cast(pl.Int64),
        pl.col("away_team"),
    )
    if sched.is_empty():
        return pl.DataFrame(schema=GAME_RATES_SCHEMA)

    if game_info is not None and not game_info.is_empty():
        sched = sched.join(_game_dates(game_info), on="game_id", how="left")
    else:
        sched = sched.with_columns(pl.lit(None, dtype=pl.Date).alias("date"))

    rows = []
    for is_home, id_col, name_col, opp_id_col, opp_name_col in (
        (True, "home_team_id", "home_team", "away_team_id", "away_team"),
        (False, "away_team_id", "away_team", "home_team_id", "home_team"),
    ):
        side = sched.select(
            "game_id",
            "season",
            "date",
            pl.col(id_col).alias("team_id"),
            pl.col(name_col).alias("team"),
            pl.col(opp_id_col).alias("opp_team_id"),
            pl.col(opp_name_col).alias("opp_team"),
            pl.lit(is_home).alias("is_home"),
            pl.lit(False).alias("neutral_site"),
        )
        side = side.join(per_team, on=["game_id", "team_id"], how="left")
        opp = per_team.rename({"team_id": "opp_team_id", "xgf": "xga", "gf": "ga"})
        side = side.join(opp, on=["game_id", "opp_team_id"], how="left")
        rows.append(side.drop("team_id", "opp_team_id"))

    out = pl.concat(rows, how="vertical_relaxed").with_columns(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("xgf").fill_null(0.0),
        pl.col("xga").fill_null(0.0),
        pl.col("gf").fill_null(0).cast(pl.Int64),
        pl.col("ga").fill_null(0).cast(pl.Int64),
    )
    return out.select(
        "game_id", "season", "date", "team", "opp_team", "is_home", "neutral_site", "xgf", "xga", "gf", "ga"
    )


@overload
def pwhl_ratings_from_proxy(
    seasons: Union[int, list[int]],
    *,
    as_of_date: _dt.date | None = ...,
    xg_model: Union[ShotQualityXGModel, PwhlCoordXGModel, None] = ...,
    xg_method: Literal["coords", "quality"] = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
@overload
def pwhl_ratings_from_proxy(
    seasons: Union[int, list[int]],
    *,
    as_of_date: _dt.date | None = ...,
    xg_model: Union[ShotQualityXGModel, PwhlCoordXGModel, None] = ...,
    xg_method: Literal["coords", "quality"] = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...
def pwhl_ratings_from_proxy(
    seasons: Union[int, list[int]],
    *,
    as_of_date: _dt.date | None = None,
    xg_model: Union[ShotQualityXGModel, PwhlCoordXGModel, None] = None,
    xg_method: Literal["coords", "quality"] = "coords",
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Best-effort PWHL power ratings from an on-demand-fit xG model.

    Loads pbp/schedule/game_info per season (seasons are loaded and processed
    individually -- ``load_pwhl_pbp`` carries a different column count
    season-to-season, so they cannot be concatenated raw), fits (or reuses)
    the ``xg_method`` xG model, builds each season's
    :func:`pwhl_team_game_xg_rates`, concatenates, applies the as-of-date
    leakage split if requested, then opponent-adjusts + shrinks via the
    already-shared :func:`sportsdataverse.nhl.nhl_team_ratings.adjust_rate_opponent`
    with PWHL's fitted constants -- the identical rank-derivation the NHL
    core uses.

    **Leakage boundary:** when ``as_of_date`` is set this is a *predictive*
    rating, so the xG model is fit on shots from games strictly BEFORE the
    cutoff only (never the games being rated). Without a cutoff (a descriptive
    full-season rating) the model is fit per-season on that season's full
    pbp -- there is no future to leak from.

    Args:
        seasons: An int or iterable of PWHL season end-years (``>= 2024``).
        as_of_date: If given, only games strictly before this date are used --
            for BOTH the rating games AND the xG-model fit (the leakage
            boundary for a predictive backtest).
        xg_model: A pre-fit model (:class:`ShotQualityXGModel` or the NHL
            :class:`PwhlCoordXGModel`);
            when supplied it is used as-is (the caller owns its leakage
            boundary). When ``None``, the model is fit internally, leak-safe
            per the ``as_of_date`` note.
        xg_method: ``"coords"`` (default; :func:`fit_pwhl_coord_xg`) or
            ``"quality"`` (:func:`fit_shot_quality_xg`) -- see
            :func:`pwhl_team_game_xg_rates`.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Raises:
        ValueError: If ``xg_method`` is not ``"coords"`` or ``"quality"``.

    Returns:
        A polars (or pandas) DataFrame, one row per (season, team). Same
        shape as :func:`sportsdataverse.nhl.nhl_team_ratings.nhl_team_ratings`.
        Empty input seasons return a zero-row frame with the documented schema.

        |col_name   |type   |
        |:----------|:------|
        |season     |Int64  |
        |team       |String |
        |adj_xgf    |Float64|
        |adj_xga    |Float64|
        |adj_xg_net |Float64|
        |adj_gf     |Float64|
        |adj_ga     |Float64|
        |games      |Int64  |
        |off_rank   |Int64  |
        |def_rank   |Int64  |
        |net_rank   |Int64  |
        |net_z      |Float64|

    Example:
        Quick start::

            from sportsdataverse.pwhl.pwhl_xg_proxy import pwhl_ratings_from_proxy

            ratings = pwhl_ratings_from_proxy(2025)
            print(ratings.sort("net_rank").head())

        Feed the shared NHL/PWHL market core for win probability::

            from sportsdataverse.pwhl.pwhl_market import pwhl_predict_games
            preds = pwhl_predict_games(games, ratings)
    """
    from sportsdataverse.nhl.nhl_prediction_constants import as_of_ratings_split, get_constants
    from sportsdataverse.nhl.nhl_team_ratings import adjust_rate_opponent
    from sportsdataverse.pwhl.pwhl_loaders import load_pwhl_game_info, load_pwhl_pbp, load_pwhl_schedules

    if xg_method not in ("coords", "quality"):
        raise ValueError(f"xg_method must be 'coords' or 'quality', got {xg_method!r}")
    const = get_constants("pwhl")
    loaded = []
    for season in _as_season_list(seasons):
        pbp = load_pwhl_pbp(season)
        sched = load_pwhl_schedules(season)
        if pbp.is_empty() or sched.is_empty():
            continue
        loaded.append((pbp, sched, load_pwhl_game_info(season)))
    if not loaded:
        return _empty_ratings(return_as_pandas)

    # Leakage boundary (MERGE-BLOCKER fix): with an as-of cutoff this is a
    # PREDICTIVE rating, so the xG model must not peek at the games being
    # rated -- fit it on shots from games strictly BEFORE the cutoff only
    # (pooled across the requested seasons). Without a cutoff (a descriptive
    # full-season rating) the per-season full-pbp fit is fine, no boundary to
    # respect. An explicit xg_model always wins.
    fit_model = xg_model
    if fit_model is None and as_of_date is not None:
        pre = [_pbp_shots_before(pbp, info, as_of_date) for pbp, _sched, info in loaded]
        fit_model = _fit_xg(pl.concat(pre, how="vertical_relaxed"), xg_method)

    all_rates = []
    for pbp, sched, info in loaded:
        model = fit_model if fit_model is not None else _fit_xg(pbp, xg_method)
        rates = pwhl_team_game_xg_rates(pbp, sched, game_info=info, xg_model=model, xg_method=xg_method)
        if not rates.is_empty():
            all_rates.append(rates)

    if not all_rates:
        return _empty_ratings(return_as_pandas)
    game_rates = pl.concat(all_rates, how="vertical_relaxed")
    if as_of_date is not None:
        game_rates = as_of_ratings_split(game_rates, as_of_date)
    if game_rates.is_empty():
        return _empty_ratings(return_as_pandas)

    out_frames = []
    for season_val in game_rates["season"].unique().sort().to_list():
        season_rates = game_rates.filter(pl.col("season") == season_val)
        xg_adj = adjust_rate_opponent(
            season_rates, for_col="xgf", against_col="xga", hfa=const.hfa, avg=const.avg_xgf, shrink_k=const.shrink_k
        )
        avg_goals = const.avg_total_goals / 2.0
        goal_adj = adjust_rate_opponent(
            season_rates, for_col="gf", against_col="ga", hfa=const.hfa, avg=avg_goals, shrink_k=const.shrink_k
        )
        assert xg_adj.schema["team"] == goal_adj.schema["team"]
        season_out = xg_adj.join(
            goal_adj.select("team", pl.col("adj_for").alias("adj_gf"), pl.col("adj_against").alias("adj_ga")),
            on="team",
            how="left",
        ).rename({"adj_for": "adj_xgf", "adj_against": "adj_xga", "adj_net": "adj_xg_net"})
        out_frames.append(season_out)

    out = pl.concat(out_frames, how="vertical_relaxed")
    net_mean = out["adj_xg_net"].mean()
    net_std = out["adj_xg_net"].std()
    out = out.with_columns(
        pl.col("adj_xgf").rank(method="ordinal", descending=True).over("season").cast(pl.Int64).alias("off_rank"),
        pl.col("adj_xga").rank(method="ordinal", descending=False).over("season").cast(pl.Int64).alias("def_rank"),
        pl.col("adj_xg_net").rank(method="ordinal", descending=True).over("season").cast(pl.Int64).alias("net_rank"),
        (((pl.col("adj_xg_net") - net_mean) / net_std) if net_std else pl.lit(0.0)).alias("net_z"),
    ).select(
        "season",
        "team",
        "adj_xgf",
        "adj_xga",
        "adj_xg_net",
        "adj_gf",
        "adj_ga",
        "games",
        "off_rank",
        "def_rank",
        "net_rank",
        "net_z",
    )
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out


def _empty_ratings(return_as_pandas: bool) -> Union[pl.DataFrame, pd.DataFrame]:
    out = pl.DataFrame(schema=_RATINGS_SCHEMA)
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out


#: Published `pwhl_xg_pbp` dataset schema: one row per on-net shot, curated
#: identity/geometry/context columns + the model xG. Dtypes are locked here
#: (explicit casts at the boundary) so both input shapes -- `load_pwhl_pbp`
#: output and the pwhl-data committed parquet, whose id dtypes differ --
#: land on one published schema.
_SHOT_XG_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Int32,
    "game_season": pl.Int32,
    "game_date": pl.Utf8,
    "team_id": pl.Int32,
    "player_id": pl.Int32,
    "goalie_id": pl.Int32,
    "period_of_game": pl.Utf8,
    "sec_from_start": pl.Int32,
    "clock": pl.Utf8,
    "x_coord": pl.Float64,
    "y_coord": pl.Float64,
    "shot_distance": pl.Float64,
    "shot_angle": pl.Float64,
    "event_type": pl.Utf8,
    "shot_quality": pl.Utf8,
    "power_play": pl.Int32,
    "short_handed": pl.Utf8,
    "empty_net": pl.Utf8,
    "penalty_shot": pl.Utf8,
    "goal": pl.Boolean,
    "xg": pl.Float64,
}


def pwhl_shot_xg(
    pbp: pl.DataFrame,
    *,
    model: PwhlCoordXGModel | None = None,
    calibrate_strength: bool = True,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Score every on-net PWHL shot with coordinate xG (the `pwhl_xg_pbp` shape).

    The shot-level counterpart to :func:`pwhl_team_game_xg_rates`: runs the
    same pre-shot context derivation (:func:`_add_preshot_context`) over the
    FULL pbp so rebound/movement features exist, filters to
    ``event == "shot"`` rows (the complete on-net shot log; ``goal`` is the
    outcome flag), scores them with :meth:`PwhlCoordXGModel.predict`, and
    returns the curated :data:`_SHOT_XG_SCHEMA` frame -- identity, geometry
    (``shot_distance``/``shot_angle`` derived via the shared HockeyTech
    analytics core inside the model), strength context, outcome, and ``xg``.
    This is the frame the `pwhl_xg_pbp` dataset release publishes per season.

    Args:
        pbp: A frame shaped like ``load_pwhl_pbp`` output (needs ``event``,
            ``goal``, ``x_coord``, ``y_coord``; extra columns are fine and
            improve the feature set). Pass the FULL pbp, not a shots-only
            frame, so the pre-shot movement features can be derived.
        model: A fitted :class:`PwhlCoordXGModel` to score with. Default
            ``None`` fits one from ``pbp`` via :func:`fit_pwhl_coord_xg` --
            pass a model fit on pooled seasons when scoring one season at a
            time so every season is scored by the same model.
        calibrate_strength: Forwarded to :func:`fit_pwhl_coord_xg` when
            ``model`` is ``None``.
        return_as_pandas: If True, return a pandas DataFrame; otherwise polars.

    Returns:
        One row per on-net shot with the :data:`_SHOT_XG_SCHEMA` columns, in
        pbp order. Zero-row (typed) when ``pbp`` is empty or has no shot rows.

    Example:
        Quick start::

            from sportsdataverse.pwhl.pwhl_loaders import load_pwhl_pbp
            from sportsdataverse.pwhl.pwhl_xg_proxy import pwhl_shot_xg

            shots = pwhl_shot_xg(load_pwhl_pbp(2025))
            shots.select("shot_distance", "xg", "goal").describe()

    See Also:
        * :func:`fit_pwhl_coord_xg` -- the model this scores with.
        * :func:`pwhl_team_game_xg_rates` -- the per-(game, team) aggregate.
    """
    if pbp.height == 0:
        out = pl.DataFrame(schema=_SHOT_XG_SCHEMA)
        return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out

    if model is None:
        model = fit_pwhl_coord_xg(pbp, calibrate_strength=calibrate_strength)

    enriched = _add_preshot_context(pbp)
    shots = enriched.filter(pl.col("event") == "shot")
    if shots.height == 0:
        out = pl.DataFrame(schema=_SHOT_XG_SCHEMA)
        return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out

    xg = model.predict(shots)
    # predict() derives shot_distance/shot_angle internally when absent; the
    # published frame must carry them too, so derive on the output path as well.
    shots = _shot_geometry(shots)
    out = shots.with_columns(xg).select([pl.col(c).cast(t) for c, t in _SHOT_XG_SCHEMA.items()])
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out
